// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdio>
#include <exception>
#include <print>
#include <string>

#include "tools/tx_drain_test/tx_drain_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(urma_debug, false, "Enable URMA debug logging to stderr");
DEFINE_bool(server, false, "Provide a peer jetty without posting receives; omit on the drain endpoint");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18516, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 4096, "Bytes per SEND, 8 to 1048576 and within device capability");
DEFINE_uint32(batch, 32, "WRs to submit before switching the SQ to ERROR, 1 to 256; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control operation or completion batch, 1 to 3600000 ms");

namespace {
// URMA 的 C 日志回调不能传播 C++ 异常，也不修改 errno，避免覆盖原始失败信息。
void UrmaLog(int level, char* message) noexcept {
    const int saved_errno = errno;
    try {
        std::println(stderr, "URMA[{}]: {}", level, message ? message : "");
    } catch (...) {
        // stderr 故障不能使 provider 的日志回调终止测试进程。
    }
    errno = saved_errno;
}
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::TxDrainSession session;
};
int Run() {
    using kbsocket::raw::UrmaApi;
    kbsocket::tools::TxDrainOptions options{FLAGS_bytes, FLAGS_batch, FLAGS_timeout_ms};
    if (FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 ||
        !kbsocket::tools::ValidateDrainOptions(options)) {
        std::println(stderr, "Invalid options; --device is required. Use --help for limits.");
        return 2;
    }
    // 错误路径可能存在挂起的 DMA；只有显式成功 Close 后才释放下层依赖。
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "Load failed: {}", loaded.error().message);
        return 1;
    }
    const auto log_status = UrmaApi::RegisterLogFunc(UrmaLog);
    if (log_status != URMA_SUCCESS) {
        std::println(stderr, "URMA log callback registration failed: {}", static_cast<int>(log_status));
    }
    if (FLAGS_urma_debug)
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        std::println(stderr, "Runtime failed: code={} provider={}", static_cast<int>(initialized.error().code),
                     initialized.error().provider_error);
        return 1;
    }
    if (FLAGS_urma_debug)
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    auto queried = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (queried != URMA_SUCCESS) {
        std::println(stderr, "QueryDevice failed: {}", static_cast<int>(queried));
        return 1;
    }
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server);
    if (!opened) {
        std::println(stderr, "{}: {}", opened.error().operation, opened.error().code);
        return 1;
    }
    std::println("{} ready: device={} eid_index={} bytes={} batch={} control={}:{}",
                 FLAGS_server ? "Peer (no RX WRs)" : "TX drainer", FLAGS_device, FLAGS_eid_index, FLAGS_bytes,
                 FLAGS_batch, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel) : connected;
    if (!result)
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    if (!FLAGS_server) {
        const auto& stats = hardware->session.stats();
        std::println(
            "TX drain: accepted={} retired={} success={} flush_err={} unhandled={} "
            "loc_access_err={} remote_access_abort_err={} ack_timeout_err={} rnr_retry_cnt_exc_err={} other_error={} "
            "last_other_status={} flush_done={} rejected_sends={}",
            stats.accepted_known ? std::to_string(stats.accepted) : "unknown", stats.retired, stats.success,
            stats.flush_error, stats.unhandled, stats.loc_access_error, stats.remote_access_abort_error,
            stats.ack_timeout_error, stats.rnr_retry_count_exceeded_error, stats.other_error, stats.last_other_error,
            stats.flush_done, stats.rejected_sends);
    }
    auto closed = hardware->session.Close();
    if (!closed) {
        std::println(stderr, "{}: {}; hardware resources retained until process exit", closed.error().operation,
                     closed.error().code);
        return 1;
    }
    auto runtime_closed = hardware->runtime.Close();
    if (!runtime_closed) {
        std::println(stderr, "Runtime close failed; retaining dependencies");
        return 1;
    }
    auto unloaded = UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "Unload failed: {}", unloaded.error().message);
        return 1;
    }
    if (!result)
        return 1;
    auto confirmed = kbsocket::tools::ConfirmDrainClosed(channel, FLAGS_timeout_ms);
    if (!confirmed) {
        std::println(stderr, "FAIL: peer cleanup confirmation: {}: {}", confirmed.error().operation,
                     confirmed.error().code);
        return 1;
    }
    std::println("PASS: {}; both endpoints closed resources",
                 FLAGS_server ? "peer TX drain confirmed" : "all attempts retired once; ERROR SQ rejected new sends");
    return 0;
}
} // namespace
int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device RAW_DEVICE [--server] --address IPv4 [--port 18516]");
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
