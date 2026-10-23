// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdio>
#include <exception>
#include <print>

#include "tools/common/control_channel.hpp"
#include "tools/jfr_lifecycle_test/jfr_lifecycle_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_bool(server, false, "Receiver role when consumed > 0; consumed=0 is standalone");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: bind address for server, peer address for client");
DEFINE_uint32(port, 18524, "TCP control port for consumed > 0");
DEFINE_uint32(rx_depth, 32, "Posted RX count, 1 to 256");
DEFINE_uint32(consumed, 0, "RX buffers consumed before deletion, 0 <= consumed < rx_depth");
DEFINE_uint32(timeout_ms, 10000, "Timeout per control or completion operation");
DEFINE_uint32(watchdog_ms, 60000, "Parent deadline for the entire child, including blocking provider calls");
DEFINE_bool(urma_debug, false, "Enable URMA debug logging");

namespace {
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::JfrLifecycleSession session;
};

int RunChild(void*) {
    using kbsocket::raw::UrmaApi;
    // 父进程尚未加载 URMA；失败时完整依赖链由子进程退出回收，绝不提前析构 MR/vector。
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
    const kbsocket::tools::JfrLifecycleOptions options{
        .rx_depth = FLAGS_rx_depth, .consumed = FLAGS_consumed, .timeout_ms = FLAGS_timeout_ms};
    const bool receiver = FLAGS_server || !FLAGS_consumed;
    auto result = hardware->session.Open(ctx, attributes.dev_cap, options, receiver);
    kbsocket::tools::ControlChannel channel;
    bool connected = false;
    if (result) {
        std::println("JFR lifecycle ready: role={} device={} eid_index={} rx_depth={} consumed={} residual={}",
                     receiver ? "receiver" : "sender", FLAGS_device, FLAGS_eid_index, FLAGS_rx_depth, FLAGS_consumed,
                     FLAGS_rx_depth - FLAGS_consumed);
        std::fflush(stdout);
        if (FLAGS_consumed) {
            result = channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port),
                                  FLAGS_timeout_ms);
            connected = result.has_value();
        }
        if (result) {
            result = hardware->session.Run(FLAGS_consumed ? &channel : nullptr);
        }
    }
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
    }
    const auto& stats = hardware->session.stats();
    std::println("Lifecycle traffic: posted={} received={} tx_retired={}", stats.posted, stats.received,
                 stats.tx_retired);
    auto closed = hardware->session.Close();
    if (!closed) {
        std::println(stderr, "FAIL: {}: {}; remaining DMA dependencies retained until child exit",
                     closed.error().operation, closed.error().code);
        return 1;
    }
    auto runtime_closed = hardware->runtime.Close();
    if (!runtime_closed) {
        std::println(stderr, "FAIL: close runtime: code={} provider={}", static_cast<int>(runtime_closed.error().code),
                     runtime_closed.error().provider_error);
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
    if (connected) {
        // 对端确认资源已关闭后才打印双端成功；自身关闭不代表对端也成功。
        constexpr std::uint32_t kClosed = 0x4a4c434c;
        auto sent = channel.SendMarker(kClosed, FLAGS_timeout_ms);
        auto received = sent ? channel.ExpectMarker(kClosed, FLAGS_timeout_ms) : sent;
        if (!received) {
            std::println(stderr, "FAIL: peer close confirmation: {}: {}", received.error().operation,
                         received.error().code);
            return 1;
        }
    }
    if (receiver) {
        std::println("PASS: observed DeleteJfr success with residual_rx={}; both dependent jetties deleted "
                     "first; MR released afterwards; delete_jfr_ns={}",
                     stats.residual_at_delete, stats.delete_jfr_ns);
    } else {
        std::println("PASS: partial RX traffic retired successfully; peer confirmed residual JFR deletion");
    }
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("Observe deleting a shared JFR with residual posted RX under a parent watchdog");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    const auto valid = kbsocket::tools::ValidateJfrLifecycleOptions(
        {.rx_depth = FLAGS_rx_depth, .consumed = FLAGS_consumed, .timeout_ms = FLAGS_timeout_ms});
    if (argc != 1 || FLAGS_device.empty() || FLAGS_library.empty() || !valid || !FLAGS_port || FLAGS_port > 65535 ||
        FLAGS_watchdog_ms <= FLAGS_timeout_ms || FLAGS_watchdog_ms > 3600000) {
        std::println(stderr, "Invalid options: require device, 0<=consumed<rx_depth<=256, "
                             "timeout_ms<watchdog_ms<=3600000. Use --help.");
        return 2;
    }
    const auto result = kbsocket::tools::RunSupervised(RunChild, nullptr, FLAGS_watchdog_ms);
    if (!result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
        return 1;
    }
    if (result->timed_out) {
        std::println(stderr,
                     "FAIL: watchdog expired after {} ms; worker killed and reaped; "
                     "provider shutdown did not finish within the experiment deadline",
                     FLAGS_watchdog_ms);
        return 1;
    }
    if (result->signal) {
        std::println(stderr, "FAIL: hardware worker terminated by signal {}", result->signal);
        return 1;
    }
    return result->exit_code;
}
