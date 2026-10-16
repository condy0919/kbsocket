// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <print>
#include <string>
#include <string_view>

#include "tools/backpressure_test/backpressure_test.hpp"
#include "tools/common/control_channel.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(urma_debug, false, "Enable URMA debug logging to stderr");
DEFINE_bool(server, false, "Listen for TCP control and receive client payload over URMA");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18519, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_string(mode, "tx_quota", "Test tx_quota or rx_delay; must match peer");
DEFINE_string(expect, "either", "Expected rx_delay outcome: either, success or error; must match peer");
DEFINE_uint32(bytes, 4096, "Bytes per SEND_WITH_IMM, 32 to 4096 including the test header; must match peer");
DEFINE_uint32(tx_depth, 32, "SQ depth and initial batch size, 1 to 256; must match peer");
DEFINE_uint32(rounds, 8, "tx_quota rounds, 1 to 1000000; must match peer in tx_quota mode");
DEFINE_uint32(rx_delay_us, 1000, "rx_delay wait before posting RX, shorter than timeout; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control or data operation, 1 to 3600000 ms");

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
    kbsocket::tools::BackpressureSession session;
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

void PrintTraffic(std::string_view label, const kbsocket::tools::BackpressureTraffic& traffic, bool accepted_known) {
    const auto accepted = accepted_known ? std::to_string(traffic.accepted) : "unknown";
    if (FLAGS_server) {
        // 服务端只收到终结汇总；详细 CQE 状态由发送端统计，不能打印伪造的零错误分布。
        std::println("{}: peer_accepted={} peer_retired={} peer_success={} peer_flush_done={} received={}", label,
                     accepted, traffic.retired, traffic.success, traffic.flush_done, traffic.received);
        return;
    }
    std::println("{}: accepted={} retired={} success={} flush_err={} unhandled={} loc_access_err={} "
                 "remote_access_abort_err={} ack_timeout_err={} rnr_retry_cnt_exc_err={} other_error={} "
                 "last_other_status={} flush_done={} rejected_sends={}",
                 label, accepted, traffic.retired, traffic.success, traffic.flush_error, traffic.unhandled,
                 traffic.loc_access_error, traffic.remote_access_abort_error, traffic.ack_timeout_error,
                 traffic.rnr_retry_count_exceeded_error, traffic.other_error, traffic.last_other_error,
                 traffic.flush_done, traffic.rejected_sends);
}

void PrintStats(const kbsocket::tools::BackpressureStats& stats) {
    PrintTraffic("Original traffic", stats.traffic, stats.accepted_known);
    if (FLAGS_mode == "rx_delay") {
        PrintTraffic("Cleanup traffic", stats.cleanup, stats.accepted_known);
    }
    if (FLAGS_server) {
        std::println("Detailed TX error counters are reported by the client.");
    } else {
        std::println("Backpressure: quota_rejections={} recovery_sends={} provider_eagain={}", stats.quota_rejections,
                     stats.recovery_sends, stats.provider_eagain);
    }
}

int Run() {
    using kbsocket::raw::UrmaApi;
    using kbsocket::tools::BackpressureExpectation;
    using kbsocket::tools::BackpressureMode;
    if ((FLAGS_mode != "tx_quota" && FLAGS_mode != "rx_delay") ||
        (FLAGS_expect != "either" && FLAGS_expect != "success" && FLAGS_expect != "error")) {
        std::println(stderr, "Invalid --mode or --expect; use --help for supported values.");
        return 2;
    }
    const kbsocket::tools::BackpressureOptions options{
        .mode = FLAGS_mode == "tx_quota" ? BackpressureMode::kTxQuota : BackpressureMode::kRxDelay,
        .expect = FLAGS_expect == "either"    ? BackpressureExpectation::kEither
                  : FLAGS_expect == "success" ? BackpressureExpectation::kSuccess
                                              : BackpressureExpectation::kError,
        .bytes = FLAGS_bytes,
        .tx_depth = FLAGS_tx_depth,
        .rounds = FLAGS_rounds,
        .rx_delay_us = FLAGS_rx_delay_us,
        .timeout_ms = FLAGS_timeout_ms,
    };
    const auto valid = kbsocket::tools::ValidateBackpressureOptions(options);
    if (FLAGS_device.empty() || FLAGS_library.empty() || FLAGS_port == 0 || FLAGS_port > 65535 || !valid) {
        std::println(stderr, "Invalid options; --device is required. Use --help for limits.");
        if (!valid) {
            std::println(stderr, "{}", valid.error().operation);
        }
        return 2;
    }
    // 未证明全部 DMA 终结及对端静默时，保留注册内存、队列、context 与动态库的完整依赖链。
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
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server);
    if (!opened) {
        std::println(stderr, "FAIL: open session: {}: {}", opened.error().operation, opened.error().code);
        CloseResources(*hardware);
        return 1;
    }
    std::println("{} ready for backpressure: mode={} expect={} device={} eid_index={} bytes={} tx_depth={} "
                 "rounds={} rx_delay_us={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_mode, FLAGS_expect, FLAGS_device, FLAGS_eid_index,
                 FLAGS_bytes, FLAGS_tx_depth, FLAGS_rounds, FLAGS_rx_delay_us, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel) : connected;
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    }
    PrintStats(hardware->session.stats());
    if (!CloseResources(*hardware) || !result) {
        return 1;
    }
    auto confirmed = kbsocket::tools::ConfirmBackpressureClosed(channel, FLAGS_timeout_ms);
    if (!confirmed) {
        std::println(stderr, "FAIL: peer cleanup confirmation: {}: {}", confirmed.error().operation,
                     confirmed.error().code);
        return 1;
    }
    const bool terminal_failure = hardware->session.stats().terminal_failure;
    // 期望不匹配仍须完成正常资源关闭，不能把已终结的错误结果变成资源泄漏。
    if ((options.expect == BackpressureExpectation::kSuccess && terminal_failure) ||
        (options.expect == BackpressureExpectation::kError && !terminal_failure)) {
        std::println(stderr, "FAIL: expected {} but observed {}; both endpoints closed resources", FLAGS_expect,
                     terminal_failure ? "terminal failure" : "success");
        return 1;
    }
    if (options.mode == BackpressureMode::kTxQuota) {
        std::println("PASS: outcome=quota_recovery; rejected work was accepted after TX retirement; "
                     "payload validated; both endpoints closed resources");
    } else if (terminal_failure) {
        std::println("PASS: outcome=rx_delay_terminal_failure; original attempts retired without replay; "
                     "RX slots consumed by original or distinct cleanup traffic; both endpoints closed resources");
    } else {
        std::println("PASS: outcome=rx_delay_success; original payload validated after delayed RX posting; "
                     "both endpoints closed resources");
    }
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device RAW_DEVICE [--server] --address IPv4 [--port 18519] "
                            "[--mode tx_quota|rx_delay] [--tx_depth 32] [--rx_delay_us 1000]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        std::println(stderr, "Unexpected positional arguments");
        return 2;
    }
    try {
        return Run();
    } catch (const std::exception& error) {
        std::println(stderr, "FAIL: {}; hardware resources retained until process exit", error.what());
        return 1;
    }
}
