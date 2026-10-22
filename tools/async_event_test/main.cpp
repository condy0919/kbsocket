// SPDX-License-Identifier: MulanPSL-2.0
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <print>

#include "tools/async_event_test/async_event_test.hpp"
#include "tools/common/control_channel.hpp"
#include "tools/peer_failure_test/process.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(server, false, "Receiver with shared JFR waterline; omit for SEND client");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4 control bind/peer address");
DEFINE_uint32(port, 18526, "TCP control port");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA library path");
DEFINE_uint32(rx_depth, 128, "RX depth and initially posted buffers, 128..256");
DEFINE_uint32(threshold, 64, "JFR low waterline; this experiment supports only the 64-buffer hardware level");
DEFINE_uint32(bytes, 4096, "SEND payload bytes, 32..4096");
DEFINE_uint32(observe_ms, 100, "Per-phase async observation window, 1..1000 ms");
DEFINE_uint32(timeout_ms, 10000, "Per control/traffic timeout; greater than twice observe_ms");
DEFINE_uint32(watchdog_ms, 60000, "Entire worker deadline including synchronous provider calls");

namespace {
using kbsocket::raw::UrmaApi;
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::JfrLimitSession session;
};
kbsocket::tools::JfrLimitOptions Options() {
    return {.rx_depth = FLAGS_rx_depth,
            .threshold = FLAGS_threshold,
            .bytes = FLAGS_bytes,
            .observe_ms = FLAGS_observe_ms,
            .timeout_ms = FLAGS_timeout_ms};
}
int Fail(const kbsocket::tools::ToolError& error) {
    std::println(stderr, "FAIL: {}: {}; unresolved dependencies retained until worker exit", error.operation,
                 error.code);
    return 1;
}
void PrintStats(const kbsocket::tools::JfrLimitStats& stats) {
    std::println("Waterline traffic: posted={} received={} accepted={} accepted_known={} retired={}", stats.posted,
                 stats.received, stats.accepted, stats.accepted_known, stats.retired);
    if (!FLAGS_server) {
        std::println(
            "Peer waterline observation: first={} repeat={}; event ownership and Get/Ack are reported by server",
            stats.first_observed, stats.repeat_observed);
        return;
    }
    std::println("Waterline configuration: requested_depth={} requested_threshold={} queried_depth={} "
                 "queried_threshold={} threshold_disabled={}",
                 FLAGS_rx_depth, FLAGS_threshold, stats.queried_depth, stats.queried_threshold,
                 stats.threshold_disabled);
    for (std::size_t i = 0; i < stats.events.size(); ++i) {
        const auto& event = stats.events[i];
        std::println("Async phase={}: get_calls={} retrieved={} acked={} jfr_limit={} last_type={} "
                     "last_get_status={} last_context_matches={} last_jfr_matches={}",
                     kbsocket::tools::JfrLimitPhaseName(static_cast<kbsocket::tools::JfrLimitPhase>(i)),
                     event.get_calls, event.retrieved, event.acknowledged, event.limit_events, event.last_type,
                     event.last_get_status, event.last_context_matches, event.last_jfr_matches);
    }
    std::println("{}: first JFR_LIMIT crossing; threshold={}", stats.first_observed ? "OBSERVED" : "NOT_OBSERVED",
                 FLAGS_threshold);
    std::println("{}: repeated JFR_LIMIT observation after refill; no threshold rewrite between crossings",
                 stats.repeat_observed ? "OBSERVED" : "NOT_OBSERVED");
}
int RunWorker(void*) {
    // 监督进程在加载 URMA 前 fork；失败时 NoDestructor 保留队列、MR 与其内存直到 worker 退出。
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
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    auto status = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (status != URMA_SUCCESS) {
        return Fail({"query async device", status});
    }
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, Options(), FLAGS_server);
    if (!opened) {
        return Fail(opened.error());
    }
    std::println("{} ready for shared-JFR waterline: device={} eid_index={} rx_depth={} threshold={} "
                 "bytes={} observe_ms={} jetty_id={} jfr_id={} async_fd={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, FLAGS_rx_depth, FLAGS_threshold,
                 FLAGS_bytes, FLAGS_observe_ms, hardware->session.endpoint().id, hardware->session.jfr_id(),
                 ctx->async_fd, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto result =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    if (result) {
        result = hardware->session.Run(channel);
    }
    if (!result) {
        PrintStats(hardware->session.stats());
        return Fail(result.error());
    }
    auto closed = hardware->session.Close();
    PrintStats(hardware->session.stats());
    if (!closed) {
        return Fail(closed.error());
    }
    auto runtime_closed = hardware->runtime.Close();
    if (!runtime_closed) {
        return Fail({"close async runtime", runtime_closed.error().provider_error});
    }
    auto unloaded = UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "FAIL: unload URMA: {}", unloaded.error().message);
        return 1;
    }
    constexpr std::uint32_t kClosed = 0x4145434c;
    auto sent = channel.SendMarker(kClosed, FLAGS_timeout_ms);
    auto peer_closed = sent ? channel.ExpectMarker(kClosed, FLAGS_timeout_ms) : sent;
    if (!peer_closed) {
        return Fail(peer_closed.error());
    }
    std::println("PASS: first JFR_LIMIT observed and acknowledged; payload and refill validated; "
                 "remaining RX consumed after disabling threshold; both endpoints closed. "
                 "Repeated-event coverage is reported separately.");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "Observe JFR_LIMIT on a shared RM_CTP receive queue, including refill and repeated crossing");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto valid = kbsocket::tools::ValidateJfrLimitOptions(Options());
    if (argc != 1 || FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 || !valid ||
        FLAGS_watchdog_ms <= FLAGS_timeout_ms || FLAGS_watchdog_ms > 3600000) {
        std::println(stderr, "Invalid options: require device, threshold=64, 128<=rx_depth<=256, "
                             "2*observe_ms<timeout_ms<watchdog_ms<=3600000; use --help");
        return 2;
    }
    return kbsocket::tools::RunPeerWatchdog(RunWorker, nullptr, FLAGS_watchdog_ms);
}
