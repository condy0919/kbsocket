// SPDX-License-Identifier: MulanPSL-2.0
#include <cstdio>
#include <exception>
#include <print>
#include <string>

#include "tools/send_test/send_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(server, false, "Receive SEND data; omit on the transmitting endpoint");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18515, "TCP control port (payload uses URMA)");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 4096, "Bytes per SEND, 8 to 1048576 and within device capability");
DEFINE_uint32(messages, 1024, "Total SEND messages; must match peer");
DEFINE_uint32(batch, 32, "WRs per round, 1 to 256; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control operation or completion batch, 1 to 3600000 ms");

namespace {
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::SendTestSession session;
};
int Run() {
    using kbsocket::raw::UrmaApi;
    kbsocket::tools::SendTestOptions options{FLAGS_bytes, FLAGS_messages, FLAGS_batch, FLAGS_timeout_ms};
    if (FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 ||
        !kbsocket::tools::ValidateOptions(options)) {
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
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        std::println(stderr, "Runtime failed: code={} provider={}", static_cast<int>(initialized.error().code),
                     initialized.error().provider_error);
        return 1;
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    auto queried = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (queried != URMA_SUCCESS) {
        std::println(stderr, "QueryDevice failed: {}", static_cast<int>(queried));
        return 1;
    }
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options);
    if (!opened) {
        std::println(stderr, "{}: {}", opened.error().operation, opened.error().code);
        return 1;
    }
    std::println("{} ready: device={} eid_index={} bytes={} messages={} batch={} control={}:{}",
                 FLAGS_server ? "Receiver" : "Sender", FLAGS_device, FLAGS_eid_index, FLAGS_bytes, FLAGS_messages,
                 FLAGS_batch, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    auto result = connected ? hardware->session.Run(channel, FLAGS_server) : connected;
    if (!result)
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
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
