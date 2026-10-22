// SPDX-License-Identifier: MulanPSL-2.0
#include <signal.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <exception>
#include <print>
#include <string>

#include "tools/common/control_channel.hpp"
#include "tools/rma_test/rma_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_bool(server, false, "Expose registered memory; mixed_send also receives small SENDs");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18525, "TCP control port; payload uses URMA READ/WRITE");
DEFINE_string(operation, "read", "read or write; must match peer");
DEFINE_uint32(bytes, 4096, "Actual size of one WR, 1 to 64MiB and within device capability");
DEFINE_uint32(rounds, 8, "Number of verified batches, 1 to 10000; must match peer");
DEFINE_uint32(window, 1,
              "Independent READ WRs per batch, 1 to 128; WRITE requires 1; total payload <=256MiB per endpoint");
DEFINE_bool(mixed_send, false, "Interleave each READ with a 64-byte SEND_WITH_IMM on the same SQ");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control/completion operation, 1 to 3600000 ms");
DEFINE_uint32(watchdog_seconds, 60, "Whole process deadline including synchronous provider calls, 1 to 3600 s");
DEFINE_bool(urma_debug, false, "Enable URMA debug logging");

namespace {
void WatchdogExpired(int) {
    // 信号处理仅调用异步信号安全接口，不析构可能仍被 DMA 使用的 MR/vector。
    constexpr char kMessage[] = "FAIL: RMA watchdog expired; process terminated with DMA cleanup unresolved\n";
    const auto ignored = ::write(STDERR_FILENO, kMessage, sizeof(kMessage) - 1);
    static_cast<void>(ignored);
    ::_exit(124);
}

struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::RmaSession session;
};

bool CloseResources(Hardware& hardware) {
    auto closed = hardware.session.Close();
    if (!closed) {
        std::println(stderr, "FAIL: {}: {}; DMA dependencies retained until process exit", closed.error().operation,
                     closed.error().code);
        return false;
    }
    auto runtime_closed = hardware.runtime.Close();
    if (!runtime_closed) {
        std::println(stderr, "FAIL: close runtime: code={} provider={}; remaining dependencies retained",
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

int Run() {
    using kbsocket::raw::UrmaApi;
    using kbsocket::tools::RmaOperation;
    const kbsocket::tools::RmaOptions options{.operation = FLAGS_operation == "read" ? RmaOperation::kRead
                                                                                     : RmaOperation::kWrite,
                                              .bytes = FLAGS_bytes,
                                              .rounds = FLAGS_rounds,
                                              .timeout_ms = FLAGS_timeout_ms,
                                              .window = FLAGS_window,
                                              .mixed_send = FLAGS_mixed_send};
    auto valid = kbsocket::tools::ValidateRmaOptions(options);
    if (!valid) {
        std::println(stderr, "Invalid options: {}. Use --help for limits.", valid.error().operation);
        return 2;
    }
    if ((FLAGS_operation != "read" && FLAGS_operation != "write") || FLAGS_device.empty() || FLAGS_library.empty() ||
        !FLAGS_port || FLAGS_port > 65535) {
        std::println(stderr, "Require --device, nonempty --library, operation=read|write, and port=1..65535.");
        return 2;
    }
    // 远端还可能访问 MR 时，普通 C++ 析构不能释放 vector 或 context。
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return 1;
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
    std::println("RMA capability: max_read_size={} max_write_size={}", attributes.dev_cap.max_read_size,
                 attributes.dev_cap.max_write_size);
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server);
    if (!opened) {
        const bool unsupported = opened.error().code == EOPNOTSUPP;
        std::println(stderr, "{}: {}: {}", unsupported ? "SKIP" : "FAIL", opened.error().operation,
                     opened.error().code);
        const bool closed = CloseResources(*hardware);
        return unsupported && closed ? 77 : 1;
    }
    std::println("{} ready for RM_CTP {}: device={} eid_index={} bytes_per_wr={} rounds={} window={} mixed_send={} "
                 "control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_operation, FLAGS_device, FLAGS_eid_index, FLAGS_bytes,
                 FLAGS_rounds, FLAGS_window, FLAGS_mixed_send, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel) : connected;
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    }
    const auto& stats = hardware->session.stats();
    std::println("RMA traffic: accepted={} retired={} verified={} pending={} post_status={} completion_status={} "
                 "min_cqe_ns={} max_cqe_ns={}",
                 stats.accepted, stats.retired, stats.verified, stats.pending, stats.last_post_status,
                 stats.last_completion_status, stats.min_cqe_ns, stats.max_cqe_ns);
    std::println("RMA concurrency: window={} mixed_send={} accepted_known={} partial_batches={} send_accepted={} "
                 "send_retired={} send_received={}",
                 FLAGS_window, FLAGS_mixed_send, stats.accepted_known, stats.partial_batches, stats.send_accepted,
                 stats.send_retired, stats.send_received);
    if (!CloseResources(*hardware) || !result) {
        return 1;
    }
    constexpr std::uint32_t kClosed = 0x524d434c;
    auto sent = channel.SendMarker(kClosed, FLAGS_timeout_ms);
    auto received = sent ? channel.ExpectMarker(kClosed, FLAGS_timeout_ms) : sent;
    if (!received) {
        std::println(stderr, "FAIL: peer close confirmation: {}: {}", received.error().operation,
                     received.error().code);
        return 1;
    }
    std::println("PASS: {} {} operations of {} bytes in {} batches, window={}; mixed_send={}; "
                 "independent payloads and guards validated; both endpoints closed resources",
                 FLAGS_rounds * FLAGS_window, FLAGS_operation, FLAGS_bytes, FLAGS_rounds, FLAGS_window,
                 FLAGS_mixed_send);
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("Observe raw RM_CTP READ/WRITE sizes, completion and memory visibility");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1 || !FLAGS_watchdog_seconds || FLAGS_watchdog_seconds > 3600 ||
        FLAGS_timeout_ms >= FLAGS_watchdog_seconds * 1000) {
        std::println(stderr, "Invalid arguments: require timeout_ms < watchdog_seconds*1000 <= 3600000; use --help.");
        return 2;
    }
    struct sigaction action{};
    action.sa_handler = WatchdogExpired;
    ::sigemptyset(&action.sa_mask);
    if (::sigaction(SIGALRM, &action, nullptr) < 0) {
        std::println(stderr, "FAIL: install RMA watchdog: {}", errno);
        return 1;
    }
    ::alarm(FLAGS_watchdog_seconds);
    try {
        const int result = Run();
        ::alarm(0);
        return result;
    } catch (const std::exception& error) {
        std::println(stderr, "FAIL: exception: {}; dependencies retained until process exit", error.what());
        return 1;
    }
}
