// SPDX-License-Identifier: MulanPSL-2.0
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <print>
#include <string>
#include <string_view>

#include "tools/common/control_channel.hpp"
#include "tools/sq_isolation_test/sq_isolation_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(urma_debug, false, "Enable URMA debug logging to stderr");
DEFINE_bool(server, false, "Listen for TCP control; both endpoints inject an SQ fault and exchange healthy traffic");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18518, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 4096, "Bytes per SEND_WITH_IMM, 32 to 4096 including the test header; must match peer");
DEFINE_uint32(batch, 32, "Messages per SQ per round, 1 to 256; must match peer");
DEFINE_uint32(rounds, 8, "Healthy traffic rounds after starting the fault, 1 to 1000000; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control operation, data round or complete SQ drain, 1 to 3600000 ms");

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
    kbsocket::tools::SqIsolationSession session;
};

void PrintStats(const kbsocket::tools::SqIsolationStats& stats) {
    constexpr std::array<std::string_view, 3> kPhases{"before", "during", "after"};
    for (std::size_t i = 0; i < kPhases.size(); ++i) {
        const auto& traffic = stats.traffic[i];
        std::println("Traffic {}: accepted={} retired={} received={}", kPhases[i],
                     stats.accepted_known ? std::to_string(traffic.accepted) : "unknown", traffic.retired,
                     traffic.received);
    }
    const auto& fault = stats.fault;
    std::println("Fault SQ: accepted={} retired={} success={} flush_err={} unhandled={} loc_access_err={} "
                 "remote_access_abort_err={} ack_timeout_err={} rnr_retry_cnt_exc_err={} other_error={} "
                 "last_other_status={} flush_done={} rejected_sends={}",
                 stats.accepted_known ? std::to_string(fault.accepted) : "unknown", fault.retired, fault.success,
                 fault.flush_error, fault.unhandled, fault.loc_access_error, fault.remote_access_abort_error,
                 fault.ack_timeout_error, fault.rnr_retry_count_exceeded_error, fault.other_error,
                 fault.last_other_error, fault.flush_done, fault.rejected_sends);
    std::println("Isolation: healthy_ready_checks={} healthy_retired_before_drained={}", stats.healthy_ready_checks,
                 stats.healthy_retired_before_drained);
    if (stats.failed_tx) {
        const auto& cr = *stats.failed_tx;
        // TX 完成只打印具有 TX 语义的字段，避免将未定义的 RX 元信息误当作诊断依据。
        std::println(stderr, "First failed TX CQE: status={} flags={:#04x} local_id={} len={} user_ctx={:#018x}",
                     static_cast<int>(cr.status), cr.flag.value, cr.local_id, cr.completion_len, cr.user_ctx);
    }
    if (stats.failed_rx) {
        const auto& cr = *stats.failed_rx;
        std::print(stderr,
                   "First failed RX CQE: status={} opcode={} flags={:#04x} local_id={} remote_id={} "
                   "len={} user_ctx={:#018x} raw_imm={:#018x} remote_eid=",
                   static_cast<int>(cr.status), static_cast<int>(cr.opcode), cr.flag.value, cr.local_id,
                   cr.remote_id.id, cr.completion_len, cr.user_ctx, cr.imm_data);
        for (const auto byte : cr.remote_id.eid.raw) {
            std::print(stderr, "{:02x}", byte);
        }
        std::println(stderr, "");
    }
}

int Run() {
    using kbsocket::raw::UrmaApi;
    const kbsocket::tools::SqIsolationOptions options{
        .bytes = FLAGS_bytes,
        .batch = FLAGS_batch,
        .rounds = FLAGS_rounds,
        .timeout_ms = FLAGS_timeout_ms,
    };
    if (FLAGS_device.empty() || FLAGS_library.empty() || FLAGS_port == 0 || FLAGS_port > 65535 ||
        !kbsocket::tools::ValidateIsolationOptions(options)) {
        std::println(stderr, "Invalid options; --device is required. Use --help for limits.");
        return 2;
    }
    // 未确认完整排空及双方静默时，保留 DMA 内存、队列及 context 的完整依赖链。
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
        return 1;
    }
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server);
    if (!opened) {
        std::println(stderr, "FAIL: open session: {}: {}; resources retained until process exit",
                     opened.error().operation, opened.error().code);
        return 1;
    }
    std::println("{} ready for SQ isolation: device={} eid_index={} bytes={} batch={} rounds={} "
                 "main_jetties=2 sink_jetties=1 control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, FLAGS_bytes, FLAGS_batch,
                 FLAGS_rounds, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel) : connected;
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    }
    PrintStats(hardware->session.stats());

    auto closed = hardware->session.Close();
    if (!closed) {
        std::println(stderr, "FAIL: close session: {}: {}; hardware resources retained until process exit",
                     closed.error().operation, closed.error().code);
        return 1;
    }
    auto runtime_closed = hardware->runtime.Close();
    if (!runtime_closed) {
        std::println(stderr, "FAIL: close runtime: code={} provider={}; dependencies retained until process exit",
                     static_cast<int>(runtime_closed.error().code), runtime_closed.error().provider_error);
        return 1;
    }
    auto unloaded = UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "FAIL: unload URMA: {}", unloaded.error().message);
        return 1;
    }
    if (!result) {
        return 1;
    }
    auto confirmed = kbsocket::tools::ConfirmIsolationClosed(channel, FLAGS_timeout_ms);
    if (!confirmed) {
        std::println(stderr, "FAIL: peer cleanup confirmation: {}: {}", confirmed.error().operation,
                     confirmed.error().code);
        return 1;
    }
    std::println("PASS: fault SQ drained once and rejected new sends; healthy SQ and shared RX remained usable; "
                 "both endpoints closed resources");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device RAW_DEVICE [--server] --address IPv4 [--port 18518] [--batch 32] [--rounds 8]");
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
