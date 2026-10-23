// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <expected>
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
DEFINE_uint32(crossings, 2, "Number of waterline crossings, 2..1000");
DEFINE_string(rearm, "none", "Before later crossings: none, rewrite (64), or toggle (0 then 64)");
DEFINE_string(traffic, "staged", "Waterline traffic: staged or concurrent (requires rearm=rewrite)");
DEFINE_string(rearm_at, "above", "Concurrent threshold rewrite with occupancy bounded above or below 64");
DEFINE_uint32(send_interval_us, 100, "Concurrent window interval between 32 individual SEND posts, 0..10000 us");
DEFINE_uint32(observe_ms, 100, "Per-phase async observation window, 1..1000 ms");
DEFINE_uint32(timeout_ms, 10000, "Per control/traffic timeout; greater than twice observe_ms");
DEFINE_uint32(watchdog_ms, 0, "Entire worker deadline; 0 derives a budget from crossings and observation windows");

namespace {
using kbsocket::raw::UrmaApi;
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::JfrLimitSession session;
};
struct WorkerOptions {
    kbsocket::tools::JfrLimitOptions test;
    std::uint32_t watchdog_ms = 0;
};
std::expected<WorkerOptions, kbsocket::tools::ToolError> Options() {
    kbsocket::tools::JfrLimitOptions options{.rx_depth = FLAGS_rx_depth,
                                             .threshold = FLAGS_threshold,
                                             .bytes = FLAGS_bytes,
                                             .observe_ms = FLAGS_observe_ms,
                                             .timeout_ms = FLAGS_timeout_ms,
                                             .crossings = FLAGS_crossings};
    using kbsocket::tools::JfrLimitRearm;
    if (FLAGS_rearm == "none") {
        options.rearm = JfrLimitRearm::kNone;
    } else if (FLAGS_rearm == "rewrite") {
        options.rearm = JfrLimitRearm::kRewrite;
    } else if (FLAGS_rearm == "toggle") {
        options.rearm = JfrLimitRearm::kToggle;
    } else {
        return std::unexpected(kbsocket::tools::ToolError{"rearm must be none, rewrite or toggle", EINVAL});
    }
    using kbsocket::tools::JfrLimitRearmAt;
    using kbsocket::tools::JfrLimitTraffic;
    if (FLAGS_traffic == "staged") {
        options.traffic = JfrLimitTraffic::kStaged;
    } else if (FLAGS_traffic == "concurrent") {
        options.traffic = JfrLimitTraffic::kConcurrent;
    } else {
        return std::unexpected(kbsocket::tools::ToolError{"traffic must be staged or concurrent", EINVAL});
    }
    if (FLAGS_rearm_at == "above") {
        options.rearm_at = JfrLimitRearmAt::kAbove;
    } else if (FLAGS_rearm_at == "below") {
        options.rearm_at = JfrLimitRearmAt::kBelow;
    } else {
        return std::unexpected(kbsocket::tools::ToolError{"rearm_at must be above or below", EINVAL});
    }
    options.send_interval_us = FLAGS_send_interval_us;
    if (auto valid = kbsocket::tools::ValidateJfrLimitOptions(options); !valid) {
        return std::unexpected(valid.error());
    }
    // 多轮会累积观察窗口；默认预算随轮数增长，避免合法实验被固定 60 秒上限截断。
    // 它不是每个操作都用满 timeout 的总和；显式值仍允许现场缩短整个实验的截止时间。
    const auto paced_ms = options.traffic == JfrLimitTraffic::kConcurrent
                              ? (32ULL * options.send_interval_us * (options.crossings - 1) + 999) / 1000
                              : 0;
    const auto automatic = std::max<std::uint64_t>(
        60000, 4ULL * options.timeout_ms + (6ULL * options.crossings + 8) * options.observe_ms + paced_ms);
    const std::uint64_t watchdog = FLAGS_watchdog_ms ? FLAGS_watchdog_ms : automatic;
    if (watchdog <= options.timeout_ms || watchdog > 3600000) {
        return std::unexpected(kbsocket::tools::ToolError{
            "require timeout_ms < effective watchdog_ms <= 3600000; reduce observation budget or set watchdog_ms",
            EINVAL});
    }
    return WorkerOptions{options, static_cast<std::uint32_t>(watchdog)};
}
int Fail(const kbsocket::tools::ToolError& error) {
    std::println(stderr, "FAIL: {}: {}; unresolved dependencies retained until worker exit", error.operation,
                 error.code);
    return 1;
}
void PrintEventStats(const kbsocket::tools::JfrLimitEventStats& event) {
    std::println("get_calls={} retrieved={} acked={} jfr_limit={} last_type={} last_get_status={} "
                 "last_context_matches={} last_jfr_matches={}",
                 event.get_calls, event.retrieved, event.acknowledged, event.limit_events, event.last_type,
                 event.last_get_status, event.last_context_matches, event.last_jfr_matches);
}
void PrintSnapshot(std::size_t crossing, const char* point, const kbsocket::tools::JfrThresholdSnapshot& snapshot) {
    std::println("Query crossing={} point={}: queried={} depth={} threshold={}", crossing, point, snapshot.queried,
                 snapshot.depth, snapshot.threshold);
}
void PrintStats(const kbsocket::tools::JfrLimitStats& stats, const kbsocket::tools::JfrLimitOptions& options) {
    const auto expected = kbsocket::tools::ExpectedJfrLimitTraffic(options);
    std::println("Waterline traffic: posted={} received={} accepted={} accepted_known={} retired={} expected={} "
                 "rx_leases_released={}",
                 stats.posted, stats.received, stats.accepted, stats.accepted_known, stats.retired, expected,
                 stats.rx_leases_released);
    if (FLAGS_server) {
        std::println("Waterline configuration: requested_depth={} requested_threshold={} queried_depth={} "
                     "queried_threshold={} threshold_disabled={}",
                     options.rx_depth, options.threshold, stats.queried_depth, stats.queried_threshold,
                     stats.threshold_disabled);
        for (std::size_t i = 0; i < stats.events.size(); ++i) {
            std::print("Async phase={}: ",
                       kbsocket::tools::JfrLimitPhaseName(static_cast<kbsocket::tools::JfrLimitPhase>(i)));
            PrintEventStats(stats.events[i]);
        }
    }
    std::size_t completed = 0;
    std::size_t observed = 0;
    for (std::size_t i = 0; i < stats.rounds.size(); ++i) {
        const auto& round = stats.rounds[i];
        if (!round.started) {
            break;
        }
        completed += round.crossing_completed;
        observed += round.observed;
        const char* outcome = !round.crossing_completed ? "INCOMPLETE" : round.observed ? "OBSERVED" : "NOT_OBSERVED";
        std::println("{}: crossing={} rearm={} completed={} posted={} received={} accepted={} retired={}", outcome,
                     i + 1, i ? kbsocket::tools::JfrLimitRearmName(options.rearm) : "initial", round.crossing_completed,
                     round.posted, round.received, round.accepted, round.retired);
        if (i && options.traffic == kbsocket::tools::JfrLimitTraffic::kConcurrent) {
            std::println("Active window crossing={}: completed={} traffic=concurrent rearm_at={} sends=32 "
                         "send_interval_us={}",
                         i + 1, round.active_window_completed, kbsocket::tools::JfrLimitRearmAtName(options.rearm_at),
                         options.send_interval_us);
            if (!FLAGS_server) {
                std::println("Active sender crossing={}: last_post_ns={}", i + 1, round.last_post_ns);
            }
            if (FLAGS_server) {
                const bool straddled = round.active_received_before_rearm && round.active_received_after_rearm;
                std::println("{}: crossing={} RX completion consumption before/after rewrite; "
                             "before={} after={} software_posted_before={} software_posted_after={} "
                             "attempted={} succeeded={} first_rx_ns={} rearm_begin_ns={} "
                             "rearm_end_ns={} last_rx_ns={}; hardware_overlap=UNPROVEN",
                             straddled ? "OBSERVED" : "NOT_OBSERVED", i + 1, round.active_received_before_rearm,
                             round.active_received_after_rearm, round.software_posted_before_rearm,
                             round.software_posted_after_rearm, round.active_rearm_attempted,
                             round.active_rearm_succeeded, round.first_rx_ns, round.rearm_begin_ns, round.rearm_end_ns,
                             round.last_rx_ns);
                const char* outcome = !round.active_window_completed           ? "INCOMPLETE"
                                      : round.active_rearm_events.limit_events ? "OBSERVED"
                                                                               : "NOT_OBSERVED";
                std::println("{}: crossing={} JFR_LIMIT collected in active rearm window", outcome, i + 1);
                PrintSnapshot(i + 1, "after_active", round.after_active);
                std::print("Async crossing={} phase=active_before_rearm: ", i + 1);
                PrintEventStats(round.active_before_rearm_events);
                std::print("Async crossing={} phase=active_rearm: ", i + 1);
                PrintEventStats(round.active_rearm_events);
            }
        }
        if (FLAGS_server) {
            PrintSnapshot(i + 1, "before_rearm", round.before_rearm);
            PrintSnapshot(i + 1, "disabled", round.disabled);
            PrintSnapshot(i + 1, "after_rearm", round.after_rearm);
            PrintSnapshot(i + 1, "after_crossing", round.after_crossing);
            std::print("Async crossing={} phase=refill_control: ", i + 1);
            PrintEventStats(round.refill_events);
            std::print("Async crossing={} phase=above_water: ", i + 1);
            PrintEventStats(round.above_water_events);
            std::print("Async crossing={} phase=crossing: ", i + 1);
            PrintEventStats(round.crossing_events);
        }
    }
    std::println("Waterline observations: requested={} completed={} observed={} first={} repeat_any={} rearm={}",
                 options.crossings, completed, observed, stats.first_observed, stats.repeat_observed,
                 kbsocket::tools::JfrLimitRearmName(options.rearm));
    if (!FLAGS_server) {
        std::println("Completed crossing observations above are confirmed by the server; detailed ownership and "
                     "Get/Ack are reported by the server.");
    }
}
int RunWorker(void* argument) {
    const auto& settings = *static_cast<const WorkerOptions*>(argument);
    const auto& options = settings.test;
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
    auto opened = hardware->session.Open(ctx, attributes.dev_cap, options, FLAGS_server);
    if (!opened) {
        return Fail(opened.error());
    }
    std::println("{} ready for shared-JFR waterline: device={} eid_index={} rx_depth={} threshold={} "
                 "bytes={} observe_ms={} crossings={} rearm={} traffic={} rearm_at={} send_interval_us={} "
                 "watchdog_ms={} jetty_id={} jfr_id={} async_fd={} "
                 "control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, FLAGS_rx_depth, FLAGS_threshold,
                 FLAGS_bytes, FLAGS_observe_ms, options.crossings, kbsocket::tools::JfrLimitRearmName(options.rearm),
                 kbsocket::tools::JfrLimitTrafficName(options.traffic),
                 kbsocket::tools::JfrLimitRearmAtName(options.rearm_at), options.send_interval_us, settings.watchdog_ms,
                 hardware->session.endpoint().id, hardware->session.jfr_id(), ctx->async_fd, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto result =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    if (result) {
        result = hardware->session.Run(channel);
    }
    if (!result) {
        PrintStats(hardware->session.stats(), options);
        return Fail(result.error());
    }
    auto closed = hardware->session.Close();
    PrintStats(hardware->session.stats(), options);
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
                 "Inspect every crossing and active-window result; PASS does not require repeated events "
                 "or establish hardware overlap.");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        "Observe JFR_LIMIT on a shared RM_CTP receive queue, including refill and repeated crossing");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto options = Options();
    if (argc != 1 || FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 || !options) {
        std::println(stderr, "Invalid options: require device, threshold=64, 128<=rx_depth<=256, "
                             "2<=crossings<=1000, rearm=none/rewrite/toggle; use --help");
        if (!options) {
            std::println(stderr, "{}", options.error().operation);
        }
        return 2;
    }
    return kbsocket::tools::RunPeerWatchdog(RunWorker, &*options, options->watchdog_ms);
}
