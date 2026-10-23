// SPDX-License-Identifier: MulanPSL-2.0
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <expected>
#include <optional>
#include <print>
#include <string>
#include <thread>
#include <utility>

#include "tools/peer_failure_test/peer_failure_test.hpp"
#include "tools/peer_failure_test/process.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(server, false, "Serve a disposable URMA child process");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4 control address; bind on server, connect on client");
DEFINE_uint32(port, 18521, "TCP control port");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_string(fault, "process_exit", "process_exit, inflight_exit (post before SIGKILL), or external");
DEFINE_uint32(bytes, 4096, "Bytes per SEND, 1 to 4096; match peer");
DEFINE_uint32(batch, 32, "Post-fault WR batch, 1 to 256; match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per operation, maximum 600000 ms");
DEFINE_uint32(async_grace_ms, 1000, "Additional async observation after TX observation, maximum 60000 ms");
DEFINE_uint32(fault_delay_ms, 5000, "External fault injection window; match peer; maximum 600000 ms");
DEFINE_uint32(watchdog_ms, 0, "Whole worker deadline; zero computes a bounded deadline from operation limits");

namespace {
using kbsocket::raw::UrmaApi;
using kbsocket::tools::ControlChannel;
using kbsocket::tools::PeerFailureOptions;
using kbsocket::tools::PeerFault;
using kbsocket::tools::ToolError;
constexpr std::uint32_t kStartBaseline = 0x504601;
constexpr std::uint32_t kBaselineTx = 0x504602;
constexpr std::uint32_t kBaselineRx = 0x504603;
constexpr std::uint32_t kPrepareExternal = 0x504604;
constexpr std::uint32_t kExternalReady = 0x504605;
constexpr std::uint32_t kFaultReady = 0x504606;
constexpr std::uint32_t kObserved = 0x504607;
constexpr std::uint32_t kClosed = 0x504608;
constexpr std::uint32_t kProbePosted = 0x504609;
constexpr std::uint32_t kExitConfirmed = 0x50460a;

void PrintError(const ToolError& error) {
    std::println(stderr, "FAIL: {}: {}", error.operation, error.code);
}
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::PeerFailureSession session;
};
std::expected<void, ToolError> OpenHardware(Hardware& hardware, const PeerFailureOptions& options, bool server) {
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return std::unexpected(ToolError{"load URMA", ENOENT});
    }
    auto initialized = hardware.runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        return std::unexpected(ToolError{"initialize raw runtime", initialized.error().provider_error});
    }
    auto* ctx = hardware.runtime.context(0);
    urma_device_attr_t attr{};
    const auto status = UrmaApi::QueryDevice(ctx->dev, &attr);
    if (status != URMA_SUCCESS) {
        return std::unexpected(ToolError{"query device", status});
    }
    return hardware.session.Open(ctx, attr.dev_cap, options, server);
}
bool CloseHardware(Hardware& hardware) {
    auto session = hardware.session.Close();
    if (!session) {
        PrintError(session.error());
        return false;
    }
    auto runtime = hardware.runtime.Close();
    if (!runtime) {
        std::println(stderr, "FAIL: close runtime: {}; retain dependencies until process exit",
                     runtime.error().provider_error);
        return false;
    }
    auto unloaded = UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "FAIL: unload URMA: {}", unloaded.error().message);
        return false;
    }
    return true;
}

int EndpointChild(int fd, const PeerFailureOptions& options) {
    // 子进程只持有 Unix 控制通道，不继承已连接的 TCP fd；parent 在 fork 之后才建立 TCP。
    static kbsocket::NoDestructor<Hardware> hardware;
    ControlChannel channel(fd);
    auto opened = OpenHardware(*hardware, options, true);
    if (!opened) {
        PrintError(opened.error());
        return 1;
    }
    std::println("Disposable URMA endpoint: pid={} jetty_id={}; supervisor owns TCP control", ::getpid(),
                 hardware->session.endpoint().id);
    std::fflush(stdout);
    const auto wire = kbsocket::tools::EncodePeerFailureHello({options, hardware->session.endpoint(), true});
    auto result = channel.Write(wire, options.timeout_ms);
    if (result) {
        result = channel.ExpectMarker(kStartBaseline, options.timeout_ms * 3);
    }
    if (result) {
        result = hardware->session.ReceiveBaseline();
    }
    if (result) {
        result = channel.SendMarker(kBaselineRx, options.timeout_ms);
    }
    if (!result) {
        PrintError(result.error());
        return 1;
    }
    if (options.fault != PeerFault::kProcessExit) {
        result = channel.ExpectMarker(kPrepareExternal, options.timeout_ms);
        if (result) {
            result = hardware->session.PrepareProbeReceives();
        }
        if (result) {
            result = channel.SendMarker(kExternalReady, options.timeout_ms);
        }
        if (!result) {
            PrintError(result.error());
            return 1;
        }
    }
    // 不执行应用层 Close：实验的故障就是持有硬件资源时由 parent SIGKILL。
    // 外层 watchdog 与 PDEATHSIG 约束这个等待，不能用进程内析构“清理”故障现场。
    while (true) {
        ::pause();
    }
}

struct ChildGuard {
    pid_t pid = -1;
    std::expected<void, ToolError> Stop(std::uint32_t timeout_ms, bool require_sigkill) {
        // 即使 waitpid 已回收后语义校验失败，也不能再次向旧 PID 发信号。
        const auto owned = std::exchange(pid, -1);
        return kbsocket::tools::KillAndReapPeer(owned, timeout_ms, require_sigkill);
    }
    ~ChildGuard() {
        if (pid > 0) {
            auto result = Stop(1000, false);
            if (!result) {
                PrintError(result.error());
            }
        }
    }
};

int RunServer(const PeerFailureOptions& options) {
    int sockets[2]{};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        PrintError({"socketpair", errno});
        return 1;
    }
    const auto parent = ::getpid();
    const auto child = ::fork();
    if (child < 0) {
        ::close(sockets[0]);
        ::close(sockets[1]);
        PrintError({"fork endpoint", errno});
        return 1;
    }
    if (child == 0) {
        ::close(sockets[0]);
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) {
            std::_Exit(1);
        }
        int result = 1;
        try {
            result = EndpointChild(sockets[1], options);
        } catch (...) {
            std::println(stderr, "FAIL: endpoint child exception; retaining resources until process exit");
        }
        std::fflush(nullptr);
        std::_Exit(result);
    }
    ::close(sockets[1]);
    ChildGuard guard{child};
    ControlChannel local(sockets[0]);
    ControlChannel peer;
    auto run = [&]() -> std::expected<void, ToolError> {
        // 监督进程不 Load URMA。真实退出证据来自其直接子进程的 waitpid，而不是 TCP EOF。
        auto connected =
            peer.Open(true, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), options.timeout_ms);
        if (!connected) {
            return connected;
        }
        kbsocket::tools::PeerFailureHelloBytes wire{};
        auto io = local.Read(wire, options.timeout_ms);
        if (!io) {
            return io;
        }
        io = peer.Write(wire, options.timeout_ms);
        if (!io) {
            return io;
        }
        io = peer.Read(wire, options.timeout_ms);
        if (!io) {
            return io;
        }
        auto hello = kbsocket::tools::DecodePeerFailureHello(wire);
        if (!hello) {
            return std::unexpected(hello.error());
        }
        io = kbsocket::tools::CheckPeerFailureHello(*hello, options, true);
        if (!io) {
            return io;
        }
        io = local.SendMarker(kStartBaseline, options.timeout_ms);
        if (!io) {
            return io;
        }
        io = peer.ExpectMarker(kBaselineTx, options.timeout_ms);
        if (!io) {
            return io;
        }
        io = local.ExpectMarker(kBaselineRx, options.timeout_ms);
        if (!io) {
            return io;
        }
        if (options.fault == PeerFault::kProcessExit) {
            io = guard.Stop(options.timeout_ms, true);
            if (!io) {
                return io;
            }
            std::println("OBSERVED: peer URMA process SIGKILL and waitpid confirmed; pid={}", child);
        } else {
            io = local.SendMarker(kPrepareExternal, options.timeout_ms);
            if (!io) {
                return io;
            }
            io = local.ExpectMarker(kExternalReady, options.timeout_ms);
            if (!io) {
                return io;
            }
            if (options.fault == PeerFault::kExternal) {
                std::println("INJECT_NOW: external fault window={} ms; endpoint_pid={}; RX posted={}. "
                             "No automatic link operation; TCP control should remain reachable.",
                             options.fault_delay_ms, child, options.batch);
            } else {
                std::println("Post-before-exit ready: endpoint_pid={} RX posted={}; waiting for client post marker",
                             child, options.batch);
            }
        }
        std::fflush(stdout);
        io = peer.SendMarker(kFaultReady, options.timeout_ms);
        if (!io) {
            return io;
        }
        if (options.fault == PeerFault::kInflightExit) {
            io = peer.ExpectMarker(kProbePosted, options.timeout_ms);
            if (!io) {
                return io;
            }
            io = guard.Stop(options.timeout_ms, true);
            if (!io) {
                return io;
            }
            std::println("OBSERVED: SIGKILL/waitpid after client post marker; pid={}; hardware inflight unknown",
                         child);
            io = peer.SendMarker(kExitConfirmed, options.timeout_ms);
            if (!io) {
                return io;
            }
        }
        io = peer.ExpectMarker(kObserved, options.timeout_ms + options.fault_delay_ms + options.async_grace_ms + 5000);
        if (!io) {
            return io;
        }
        if (guard.pid > 0) {
            io = guard.Stop(options.timeout_ms, false);
            if (!io) {
                return io;
            }
        }
        return peer.SendMarker(kClosed, options.timeout_ms);
    }();
    if (!run) {
        PrintError(run.error());
        return 1;
    }
    std::println("PASS: supervisor protocol completed; URMA child reaped; TX/async observations are on client");
    return 0;
}

void PrintStats(const kbsocket::tools::PeerFailureSession& session) {
    const auto& tx = session.tx();
    std::println("Probe TX: post_status={} accepted={} accepted_known={} retired={} first_cqe_ns={} last_cqe_ns={}",
                 tx.post_status, tx.accepted, tx.accepted_known, tx.retired, tx.first_cqe_ns, tx.last_cqe_ns);
    for (std::size_t i = 0; i < tx.statuses.size(); ++i) {
        if (tx.statuses[i]) {
            std::println("  CQE status={} count={}", i, tx.statuses[i]);
        }
    }
    const auto& events = session.async();
    std::println("Async: fd_available={} acknowledged={} other_types={}", events.available, events.acknowledged,
                 events.other);
    for (std::size_t i = 0; i < events.types.size(); ++i) {
        if (events.types[i]) {
            std::println("  async type={} count={}", i, events.types[i]);
        }
    }
    if (!events.acknowledged) {
        std::println("NOT_OBSERVED: async event within observation window; absence does not imply no peer fault");
    }
}

int RunClient(const PeerFailureOptions& options) {
    static kbsocket::NoDestructor<Hardware> hardware;
    auto opened = OpenHardware(*hardware, options, false);
    if (!opened) {
        PrintError(opened.error());
        CloseHardware(*hardware);
        return 1;
    }
    ControlChannel channel;
    bool observation_started = false;
    auto run = [&]() -> std::expected<void, ToolError> {
        auto io =
            channel.Open(false, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), options.timeout_ms);
        if (!io) {
            return io;
        }
        io = channel.Write(kbsocket::tools::EncodePeerFailureHello({options, {}, false}), options.timeout_ms);
        if (!io) {
            return io;
        }
        kbsocket::tools::PeerFailureHelloBytes wire{};
        io = channel.Read(wire, options.timeout_ms);
        if (!io) {
            return io;
        }
        auto hello = kbsocket::tools::DecodePeerFailureHello(wire);
        if (!hello) {
            return std::unexpected(hello.error());
        }
        io = kbsocket::tools::CheckPeerFailureHello(*hello, options, false);
        if (!io) {
            return io;
        }
        io = hardware->session.Import(hello->endpoint);
        if (!io) {
            return io;
        }
        io = hardware->session.Send(1, true);
        if (!io) {
            return io;
        }
        std::println("Baseline: accepted=1 retired=1 success=1");
        io = hardware->session.ObserveAsync(0);
        if (!io) {
            return io;
        }
        std::println("Baseline async acknowledged={}", hardware->session.async().acknowledged);
        hardware->session.ResetAsyncObservation();
        io = channel.SendMarker(kBaselineTx, options.timeout_ms);
        if (!io) {
            return io;
        }
        io = channel.ExpectMarker(kFaultReady, options.timeout_ms);
        if (!io) {
            return io;
        }
        if (options.fault == PeerFault::kExternal) {
            std::println("INJECT_NOW: {} ms external fault window; remote RX is ready; no fault identity inferred",
                         options.fault_delay_ms);
            std::fflush(stdout);
            // 故障注入窗口也推进 async fd，避免遗漏早于新 WR 的事件。
            io = hardware->session.ObserveAsync(options.fault_delay_ms);
            if (!io) {
                return io;
            }
        } else if (options.fault == PeerFault::kProcessExit) {
            std::println("OBSERVED: peer supervisor confirmed SIGKILL/waitpid before new SEND batch");
        }
        observation_started = true;
        auto before = hardware->session.QueryState();
        if (before) {
            std::println("Local SQ before post: state={}", *before);
        } else {
            std::println("NOT_OBSERVED: SQ state before post: {}", before.error().code);
        }
        std::expected<void, ToolError> sent;
        std::optional<ToolError> control_failure;
        if (options.fault == PeerFault::kInflightExit) {
            sent = hardware->session.Submit(options.batch);
            if (sent) {
                sent = hardware->session.PollCompletions();
            }
            const auto before_exit = hardware->session.tx();
            std::println(
                "Post-before-exit: post_status={} accepted={} accepted_known={} retired_before_exit_request={}",
                before_exit.post_status, before_exit.accepted, before_exit.accepted_known, before_exit.retired);
            for (std::size_t i = 0; i < before_exit.statuses.size(); ++i) {
                if (before_exit.statuses[i]) {
                    std::println("  pre-exit CQE status={} count={}", i, before_exit.statuses[i]);
                }
            }
            // 控制等待期间不 poll，软件未退休绝不等于硬件未完成；确认之后继续收齐剩余 CQE。
            io = channel.SendMarker(kProbePosted, options.timeout_ms);
            if (io) {
                io = channel.ExpectMarker(kExitConfirmed, options.timeout_ms);
            }
            if (!io) {
                control_failure = io.error();
            } else if (before_exit.accepted_known && before_exit.retired == before_exit.accepted) {
                std::println("NOT_OBSERVED: inflight_at_confirmation; all accepted WRs retired before exit request");
            } else {
                std::println("Post-before-exit confirmed: software_unretired_at_confirmation={}; "
                             "NOT_OBSERVED: hardware inflight cannot be distinguished from queued CQEs",
                             before_exit.accepted_known ? std::to_string(before_exit.accepted - before_exit.retired)
                                                        : "unknown");
            }
            if (sent) {
                sent = hardware->session.WaitForTerminal();
            }
        } else {
            sent = hardware->session.Send(options.batch, false);
        }
        auto after = hardware->session.QueryState();
        if (after) {
            std::println("Local SQ after observation: state={}", *after);
        } else {
            std::println("NOT_OBSERVED: SQ state after observation: {}", after.error().code);
        }
        auto events = hardware->session.ObserveAsync(options.async_grace_ms);
        PrintStats(hardware->session);
        // 即使未观测到所有终结，也让 supervisor 收尾；这不授权释放本地未知 DMA。
        io = channel.SendMarker(kObserved, options.timeout_ms);
        if (io) {
            io = channel.ExpectMarker(kClosed, options.timeout_ms);
        }
        if (!sent) {
            return sent;
        }
        if (!events) {
            return events;
        }
        if (control_failure) {
            return std::unexpected(*control_failure);
        }
        return io;
    }();
    if (!run) {
        PrintError(run.error());
        if (observation_started && run.error().code == ETIMEDOUT) {
            std::println("NOT_OBSERVED: terminality or control confirmation within deadline; no accepted WR replayed");
        }
    }
    const bool closed = CloseHardware(*hardware);
    if (!run || !closed) {
        return 1;
    }
    std::println("PASS: observation completed; accepted WRs retired once; no replay; client resources closed. "
                 "Reported statuses describe this run, not a required fault status.");
    return 0;
}

int RunWorker(void* argument) {
    const auto& options = *static_cast<PeerFailureOptions*>(argument);
    return FLAGS_server ? RunServer(options) : RunClient(options);
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "--device RAW_DEVICE [--server] --address IPv4 [--fault process_exit|inflight_exit|external] [--batch 32]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    PeerFailureOptions options{
        .bytes = FLAGS_bytes,
        .batch = FLAGS_batch,
        .timeout_ms = FLAGS_timeout_ms,
        .async_grace_ms = FLAGS_async_grace_ms,
        .fault_delay_ms = FLAGS_fault_delay_ms,
        .fault = FLAGS_fault == "external"
                     ? PeerFault::kExternal
                     : (FLAGS_fault == "inflight_exit" ? PeerFault::kInflightExit : PeerFault::kProcessExit)};
    auto valid = kbsocket::tools::ValidatePeerFailureOptions(options);
    if (argc != 1 || FLAGS_device.empty() || FLAGS_library.empty() || FLAGS_port == 0 || FLAGS_port > 65535 ||
        (FLAGS_fault != "process_exit" && FLAGS_fault != "external" && FLAGS_fault != "inflight_exit") || !valid) {
        std::println(stderr, "Invalid options; --device is required; use --help for limits");
        return 2;
    }
    const auto deadline = FLAGS_watchdog_ms
                              ? FLAGS_watchdog_ms
                              : options.timeout_ms * 8 + options.fault_delay_ms + options.async_grace_ms + 10000;
    std::println(
        "{} peer failure observation: fault={} device={} eid_index={} bytes={} batch={} watchdog_ms={} control={}:{}",
        FLAGS_server ? "Server" : "Client", FLAGS_fault, FLAGS_device, FLAGS_eid_index, options.bytes, options.batch,
        deadline, FLAGS_address, FLAGS_port);
    return kbsocket::tools::RunPeerWatchdog(RunWorker, &options, deadline);
}
