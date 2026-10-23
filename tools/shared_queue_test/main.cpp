// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <print>
#include <string>

#include "tools/common/control_channel.hpp"
#include "tools/shared_queue_test/shared_queue_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(urma_debug, false, "Enable URMA debug logging to stderr");
DEFINE_bool(server, false, "Listen for the TCP control connection; both endpoints send and receive UB data");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18517, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 4096, "Bytes per SEND_WITH_IMM, 32 to 4096 including the test header; must match peer");
DEFINE_uint32(messages, 1024, "Messages sent by each local jetty, at least jetties; must match peer");
DEFINE_uint32(batch, 32, "Messages per jetty per round, 1 to 256; must match peer");
DEFINE_uint32(jetties, 2, "Local jetties sharing a JFR and TX/RX JFCs, 1 to 16; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control operation or data round, 1 to 3600000 ms");

namespace {
// provider 的 C 日志回调必须保持原始 errno，也不能传播 C++ 输出异常。
void UrmaLog(int level, char* message) noexcept {
    const int saved_errno = errno;
    try {
        std::println(stderr, "URMA[{}]: {}", level, message ? message : "");
    } catch (...) {
        // 日志输出失败不能中断 provider。
    }
    errno = saved_errno;
}

struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::SharedQueueSession session;
};

void PrintStats(const kbsocket::tools::SharedQueueStats& stats, std::uint32_t jetties) {
    std::println("Shared queue: accepted={} retired={} received={} imm_mask={:#04x}",
                 stats.accepted_known ? std::to_string(stats.accepted) : "unknown", stats.retired, stats.received,
                 stats.imm_patterns);
    std::println("Pair counts: source=sender jetty, target=receiver jetty; TX is local->peer, RX is peer->local");
    for (std::uint32_t source = 0; source < jetties; ++source) {
        for (std::uint32_t target = 0; target < jetties; ++target) {
            const auto index = source * kbsocket::tools::kSharedMaxJetties + target;
            std::println("  source={} target={} tx={} rx={}", source, target, stats.tx_pairs[index],
                         stats.rx_pairs[index]);
        }
    }
    if (stats.failed_tx) {
        const auto& cr = *stats.failed_tx;
        // TX 的 opcode、remote_id、IMM 未定义，仅打印其有效的完成归属字段。
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
    if (stats.failed_rx_frame) {
        const auto& frame = *stats.failed_rx_frame;
        std::println(stderr,
                     "Failed RX payload route: source={} target={} sequence={} sender_role={} expected_imm={:#018x}",
                     frame.source, frame.target, frame.sequence, frame.server ? "server" : "client",
                     kbsocket::tools::SharedImmediate(frame));
    }
}

int Run() {
    using kbsocket::raw::UrmaApi;
    const kbsocket::tools::SharedQueueOptions options{
        .bytes = FLAGS_bytes,
        .messages = FLAGS_messages,
        .batch = FLAGS_batch,
        .jetties = FLAGS_jetties,
        .timeout_ms = FLAGS_timeout_ms,
    };
    if (FLAGS_device.empty() || FLAGS_library.empty() || FLAGS_port == 0 || FLAGS_port > 65535 ||
        !kbsocket::tools::ValidateSharedOptions(options)) {
        std::println(stderr, "Invalid options; --device is required. Use --help for limits.");
        return 2;
    }
    // 错误路径无法总是证明对端已停止访问，保留完整依赖链，避免退出时析构释放 DMA 内存。
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
    std::println("{} ready for bidirectional SEND_WITH_IMM: device={} eid_index={} jetties={} messages_per_jetty={} "
                 "bytes={} batch={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, FLAGS_jetties, FLAGS_messages,
                 FLAGS_bytes, FLAGS_batch, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel) : connected;
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    }
    PrintStats(hardware->session.stats(), options.jetties);

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
    auto confirmed = kbsocket::tools::ConfirmSharedClosed(channel, FLAGS_timeout_ms);
    if (!confirmed) {
        std::println(stderr, "FAIL: peer cleanup confirmation: {}: {}", confirmed.error().operation,
                     confirmed.error().code);
        return 1;
    }
    std::println(
        "PASS: bidirectional shared queue SEND_WITH_IMM; payload, immediate and completion ownership validated; "
        "both endpoints closed resources");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device RAW_DEVICE [--server] --address IPv4 [--port 18517] [--jetties 2]");
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
