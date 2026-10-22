// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <limits>
#include <print>
#include <string>
#include <string_view>

#include "tools/common/control_channel.hpp"
#include "tools/peer_failure_test/process.hpp"
#include "tools/sq_multiplex_test/fault_peer.hpp"
#include "tools/sq_multiplex_test/sq_multiplex_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(urma_debug, false, "Enable URMA debug logging to stderr");
DEFINE_bool(server, false, "Serve independent healthy and empty JFR targets");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18520, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 4096, "Bytes per SEND_WITH_IMM, 32 to 4096 including the test header; must match peer");
DEFINE_uint32(batch, 32, "Messages per target per phase, 1 to 128; mixed phase posts 2*batch WRs; must match peer");
DEFINE_uint32(rounds, 8, "Before/mixed/after rounds, 1 to 1000; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control or data operation, 1 to 3600000 ms");
DEFINE_string(fault, "rnr", "rnr (empty JFR) or peer_exit (SIGKILL a baseline-validated endpoint); match peer");
DEFINE_uint32(watchdog_ms, 0, "Whole worker deadline; zero computes a bounded deadline from operation limits");

namespace {
void UrmaLog(int level, char* message) noexcept {
    const int saved_errno = errno;
    try {
        std::println(stderr, "URMA[{}]: {}", level, message ? message : "");
    } catch (...) {
        // C 日志回调不能传播 C++ 输出异常，也不能覆盖 provider 的 errno。
    }
    errno = saved_errno;
}

struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::SqMultiplexSession session;
};

bool CloseResources(Hardware& hardware) {
    auto closed = hardware.session.Close();
    if (!closed) {
        std::println(stderr, "FAIL: close session: {}: {}; hardware resources retained until process exit",
                     closed.error().operation, closed.error().code);
        return false;
    }
    auto runtime_closed = hardware.runtime.Close();
    if (!runtime_closed) {
        std::println(stderr, "FAIL: close runtime: code={} provider={}; dependencies retained until process exit",
                     static_cast<int>(runtime_closed.error().code), runtime_closed.error().provider_error);
        return false;
    }
    auto unloaded = kbsocket::raw::UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "FAIL: unload URMA: {}", unloaded.error().message);
        return false;
    }
    return true;
}

void PrintTraffic(std::string_view label, const kbsocket::tools::MultiplexTraffic& traffic, bool accepted_known) {
    if (FLAGS_server) {
        // 服务端不消费 TX CQE，只报告实际收到并校验的消息，避免缺省零值被当作发送侧观测。
        std::println("{}: received={}", label, traffic.received);
        return;
    }
    const auto accepted = accepted_known ? std::to_string(traffic.accepted) : "unknown";
    std::println("{}: accepted={} retired={} success={} rnr={} loc_access_err={} remote_access_abort_err={} "
                 "ack_timeout_err={} other_error={} last_other_status={} min_cqe_ns={} max_cqe_ns={}",
                 label, accepted, traffic.retired, traffic.success, traffic.rnr, traffic.loc_access_error,
                 traffic.remote_access_abort_error, traffic.ack_timeout_error, traffic.other_error,
                 traffic.last_other_status, traffic.min_cqe_ns, traffic.max_cqe_ns);
}

std::string_view LaneStateName(const kbsocket::tools::SqMultiplexStats& stats) {
    if (!stats.lane_state) {
        return "unknown";
    }
    using kbsocket::raw::JettyLaneState;
    switch (*stats.lane_state) {
    case JettyLaneState::kReady:
        return "READY";
    case JettyLaneState::kFaulted:
        return "FAULTED";
    case JettyLaneState::kError:
        return "ERROR";
    case JettyLaneState::kFlushReady:
        return "FLUSH_READY";
    case JettyLaneState::kDrained:
        return "DRAINED";
    }
    return "unknown";
}

void PrintStats(const kbsocket::tools::SqMultiplexStats& stats) {
    if (FLAGS_fault == "peer_exit") {
        PrintTraffic("Fault baseline", stats.fault_baseline, stats.accepted_known);
        std::println("Peer exit: confirmed={}; mixed traffic starts only after SIGKILL/waitpid confirmation",
                     stats.peer_exit_confirmed);
    }
    constexpr std::array<std::string_view, 3> kLabels{"Healthy before", "Healthy mixed", "Healthy after"};
    for (std::size_t i = 0; i < kLabels.size(); ++i) {
        PrintTraffic(kLabels[i], stats.healthy[i], stats.accepted_known);
    }
    if (FLAGS_server) {
        std::println("{}; detailed TX statuses and timing are reported by the client.",
                     FLAGS_fault == "peer_exit"
                         ? (stats.peer_exit_confirmed ? "Fault endpoint: disposable URMA child exit confirmed"
                                                      : "Fault endpoint: exit not confirmed")
                         : "Sink JFR: no RX posted");
        return;
    }
    PrintTraffic("Sink mixed", stats.sink, stats.accepted_known);
    std::println("TX completion: completion_path={} processor_events={} processor_retired={} last_lane_state={}",
                 stats.uses_completion_processor ? "processor" : "raw", stats.processor_events, stats.processor_retired,
                 LaneStateName(stats));
    const auto queried_state = stats.queried_state ? std::to_string(*stats.queried_state) : "unknown";
    std::println(
        "Same SQ: jetty_id={} state_checks={} requested_rnr_retry={} requested_err_timeout={} "
        "queried_rnr_retry={} queried_err_timeout={} last_queried_state={} accepted_known={} provider_eagain={}",
        stats.jetty_id, stats.state_checks, stats.requested_rnr_retry, stats.requested_err_timeout,
        stats.queried_rnr_retry, stats.queried_err_timeout, queried_state, stats.accepted_known, stats.provider_eagain);
    for (std::size_t i = 0; i < stats.rounds.size(); ++i) {
        const auto& round = stats.rounds[i];
        if (!round.started) {
            continue;
        }
        std::println("Mixed round {}: healthy_retired={} healthy_success={} sink_retired={} sink_rnr={} "
                     "sink_remote_access_abort={} "
                     "first_healthy_ns={} last_healthy_ns={} first_sink_ns={} last_sink_ns={} "
                     "first_rnr_ns={} last_rnr_ns={} "
                     "healthy_before_first_rnr={} healthy_before_last_rnr={}",
                     i, round.healthy_retired, round.healthy_success, round.sink_retired, round.sink_rnr,
                     round.sink_remote_access_abort, round.first_healthy_ns, round.last_healthy_ns, round.first_sink_ns,
                     round.last_sink_ns, round.first_rnr_ns, round.last_rnr_ns, round.healthy_before_first_rnr,
                     round.healthy_before_last_rnr);
    }
    std::println("Timing is measured from before post to software CQE consumption; CQE order alone does not "
                 "establish hardware head-of-line blocking.");
}

int Run() {
    using kbsocket::raw::UrmaApi;
    const kbsocket::tools::SqMultiplexOptions options{.bytes = FLAGS_bytes,
                                                      .batch = FLAGS_batch,
                                                      .rounds = FLAGS_rounds,
                                                      .timeout_ms = FLAGS_timeout_ms,
                                                      .fault = FLAGS_fault == "peer_exit"
                                                                   ? kbsocket::tools::MultiplexFault::kPeerExit
                                                                   : kbsocket::tools::MultiplexFault::kRnr};
    auto valid = kbsocket::tools::ValidateMultiplexOptions(options);
    if (FLAGS_device.empty() || FLAGS_library.empty() || FLAGS_port == 0 || FLAGS_port > 65535 || !valid ||
        (FLAGS_fault != "rnr" && FLAGS_fault != "peer_exit")) {
        std::println(stderr, "Invalid options; --device is required. Use --help for limits.");
        if (!valid) {
            std::println(stderr, "{}", valid.error().operation);
        }
        return 2;
    }
    // 所有 fork 都在父 worker 加载 provider 前完成，子进程不继承健康端点和 TCP 控制通道。
    kbsocket::tools::MultiplexPeerProcess fault_peer;
    if (FLAGS_server && options.fault == kbsocket::tools::MultiplexFault::kPeerExit) {
        auto opened = fault_peer.Open(FLAGS_library.c_str(), FLAGS_device.c_str(), FLAGS_eid_index, options);
        if (!opened) {
            std::println(stderr, "FAIL: prepare fault endpoint: {}: {}", opened.error().operation, opened.error().code);
            return 1;
        }
    }
    // 未证明所有 DMA 终结与对端静默时，入口保留注册内存、队列、context、动态库的完整依赖链。
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return 1;
    }
    const auto log_status = UrmaApi::RegisterLogFunc(UrmaLog);
    if (log_status != URMA_SUCCESS) {
        std::println(stderr, "URMA log callback registration failed: {}", static_cast<int>(log_status));
    }
    if (FLAGS_urma_debug) {
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    }
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        std::println(stderr, "FAIL: initialize runtime: code={} provider={}",
                     static_cast<int>(initialized.error().code), initialized.error().provider_error);
        CloseResources(*hardware);
        return 1;
    }
    if (FLAGS_urma_debug) {
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    const auto queried = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (queried != URMA_SUCCESS) {
        std::println(stderr, "FAIL: query device: {}", static_cast<int>(queried));
        CloseResources(*hardware);
        return 1;
    }
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server,
                                         FLAGS_server && FLAGS_fault == "peer_exit" ? &fault_peer : nullptr);
    if (!opened) {
        std::println(stderr, "FAIL: open session: {}: {}", opened.error().operation, opened.error().code);
        CloseResources(*hardware);
        return 1;
    }
    std::println("{} ready for same-SQ multiplex: fault={} device={} eid_index={} bytes={} batch={} rounds={} "
                 "mixed_wrs={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_fault, FLAGS_device, FLAGS_eid_index, FLAGS_bytes,
                 FLAGS_batch, FLAGS_rounds, 2 * FLAGS_batch, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel) : connected;
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    }
    PrintStats(hardware->session.stats());
    if (!CloseResources(*hardware) || !connected) {
        return 1;
    }
    // 观测失败与资源未决分别处理：已安全关闭时仍确认对端收尾，但不能因此把实验判为通过。
    auto confirmed = kbsocket::tools::ConfirmMultiplexClosed(channel, FLAGS_timeout_ms);
    if (!confirmed) {
        std::println(stderr, "FAIL: peer cleanup confirmation: {}: {}", confirmed.error().operation,
                     confirmed.error().code);
        return 1;
    }
    if (!result) {
        std::println("FAIL: experiment failed; both endpoints closed resources");
        return 1;
    }
    if (FLAGS_fault == "peer_exit") {
        std::println("PASS: healthy traffic succeeded before, during and after same-SQ REM_ACCESS_ABORT; "
                     "fault baseline validated before SIGKILL/waitpid; old-target attempts retired once with status 8; "
                     "client used TxCompletionProcessor; "
                     "same SQ remained READY without modification or recreation; payload validated; "
                     "both endpoints closed resources");
    } else {
        std::println("PASS: healthy traffic succeeded before, during and after same-SQ RNR; sink attempts retired "
                     "once with RNR; same SQ remained READY without modification or recreation; payload validated; "
                     "both endpoints closed resources");
    }
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device RAW_DEVICE [--server] --address IPv4 [--port 18520] "
                            "[--fault rnr|peer_exit] [--batch 32] [--rounds 8] [--bytes 4096]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        std::println(stderr, "Unexpected positional arguments");
        return 2;
    }
    try {
        if (FLAGS_fault == "peer_exit") {
            const auto computed = static_cast<std::uint64_t>(FLAGS_timeout_ms) * (6ull * FLAGS_rounds + 12);
            const auto deadline = FLAGS_watchdog_ms ? FLAGS_watchdog_ms
                                                    : static_cast<std::uint32_t>(std::min<std::uint64_t>(
                                                          computed, std::numeric_limits<std::uint32_t>::max()));
            return kbsocket::tools::RunPeerWatchdog([](void*) { return Run(); }, nullptr, deadline);
        }
        return Run();
    } catch (const std::exception& error) {
        std::println(stderr, "FAIL: {}; hardware resources retained until process exit", error.what());
        return 1;
    }
}
