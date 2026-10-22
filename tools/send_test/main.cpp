// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdio>
#include <exception>
#include <print>
#include <string>

#include "tools/send_test/send_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(urma_debug, false, "Enable URMA debug logging to stderr");
DEFINE_bool(server, false, "Receive SEND data; omit on the transmitting endpoint");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18515, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 4096, "Bytes per SEND, 8 to 4096 and within device capability");
DEFINE_uint32(messages, 1024, "Total SEND messages; must match peer");
DEFINE_uint32(batch, 32, "WRs per round, 1 to 256; must match peer");
DEFINE_uint32(sges, 1, "Independent registered source segments per WR, 1 to 8; must match peer");
DEFINE_uint32(header_bytes, 32, "First SGE length when sges > 1; at least 8 bytes; must match peer");
DEFINE_bool(send_imm, false, "Use SEND_WITH_IMM and validate its 64-bit sequence tag; must match peer");
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
    kbsocket::tools::SendTestSession session;
};
int Run() {
    using kbsocket::raw::UrmaApi;
    kbsocket::tools::SendTestOptions options{FLAGS_bytes, FLAGS_messages,     FLAGS_batch,   FLAGS_timeout_ms,
                                             FLAGS_sges,  FLAGS_header_bytes, FLAGS_send_imm};
    if (FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535) {
        std::println(stderr, "Invalid device/library/port; --device is required and port must be 1 to 65535.");
        return 2;
    }
    auto valid = kbsocket::tools::ValidateOptions(options);
    if (!valid) {
        std::println(stderr, "Invalid options: {}. Use --help for limits.", valid.error().operation);
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
    if (FLAGS_urma_debug) {
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    }
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        std::println(stderr, "Runtime failed: code={} provider={}", static_cast<int>(initialized.error().code),
                     initialized.error().provider_error);
        return 1;
    }
    if (FLAGS_urma_debug) {
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    auto queried = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (queried != URMA_SUCCESS) {
        std::println(stderr, "QueryDevice failed: {}", static_cast<int>(queried));
        return 1;
    }
    std::println("SEND capability: max_msg_size={} max_jfs_sge={}", attributes.dev_cap.max_msg_size,
                 attributes.dev_cap.max_jfs_sge);
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server);
    if (!opened) {
        std::println(stderr, "{}: {}", opened.error().operation, opened.error().code);
        return 1;
    }
    std::println("{} ready: device={} eid_index={} bytes={} messages={} batch={} sges={} header_bytes={} "
                 "send_imm={} control={}:{}",
                 FLAGS_server ? "Receiver" : "Sender", FLAGS_device, FLAGS_eid_index, FLAGS_bytes, FLAGS_messages,
                 FLAGS_batch, FLAGS_sges, FLAGS_header_bytes, FLAGS_send_imm, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel, FLAGS_server) : connected;
    const auto& stats = hardware->session.stats();
    std::println("SEND verification: accepted={} accepted_known={} retired={} received={} guarded_segments={}",
                 stats.accepted, stats.accepted_known, stats.retired, stats.received, stats.guarded_segments);
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
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
    if (!result) {
        return 1;
    }
    std::println("PASS: {} messages, {} bytes; {}", FLAGS_messages,
                 static_cast<std::uint64_t>(FLAGS_messages) * FLAGS_bytes,
                 FLAGS_server ? "payload validated" : "TX and remote RX confirmed");
    return 0;
}
} // namespace
int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device RAW_DEVICE [--server] --address IPv4 [--port 18515]");
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
