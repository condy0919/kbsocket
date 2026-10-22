// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cstdio>
#include <print>

#include "tools/async_error_test/async_error_test.hpp"
#include "tools/peer_failure_test/process.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"

DEFINE_bool(server, false, "Receiver owning the fault injection target");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4 control bind/peer address");
DEFINE_uint32(port, 18527, "TCP control port");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA library path");
DEFINE_string(fault, "jfc_overflow", "jfc_overflow, external_jfc, external_jfr");
DEFINE_uint32(attempts, 512, "Fault SENDs after baseline, even value 128..4096");
DEFINE_uint32(cq_depth, 64, "Requested fault RX JFC depth for overflow candidate");
DEFINE_uint32(healthy_rounds, 16, "Independent healthy SEND checkpoints, 2..256");
DEFINE_uint32(observe_ms, 2000, "Injection observation window, up to 60000 ms");
DEFINE_uint32(timeout_ms, 10000, "Per control/traffic/cleanup deadline");
DEFINE_uint32(watchdog_ms, 0, "Whole-process deadline; 0 derives a conservative budget");

namespace {
using kbsocket::raw::UrmaApi;
using kbsocket::tools::AsyncErrorOptions;
using kbsocket::tools::ToolError;
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::AsyncErrorSession session;
};
int Fail(ToolError error) {
    std::println(stderr, "FAIL: {}: {}; unresolved dependencies retained until worker exit", error.operation,
                 error.code);
    return 1;
}
void PrintEvents(const char* phase, const kbsocket::tools::AsyncErrorEvents& events) {
    std::println("Async phase={}: retrieved={} acked={} fault_jfc={} fault_rx_jfc={} fault_tx_jfc={} fault_jfr={} "
                 "jetty0={} jetty1={} healthy={} "
                 "other={} last_type={}",
                 phase, events.retrieved, events.acknowledged, events.fault_jfc, events.fault_rx_jfc,
                 events.fault_tx_jfc, events.fault_jfr, events.fault_jetty[0], events.fault_jetty[1], events.healthy,
                 events.other, events.last_type);
    for (std::size_t i = 0; i < events.types.size(); ++i) {
        if (events.types[i]) {
            std::println("  async_type[{}]={}", i, events.types[i]);
        }
    }
}
void PrintQueue(const char* name, const kbsocket::tools::AsyncQueueStats& q) {
    std::println(
        "Queue {}: accepted={} accepted_known={} retired={} posted={} received={} rx_retired={} post_status={} "
        "poll_status={} modify_status={} flush_status={} flush_done0={} flush_done1={} "
        "jetty_before={}/{} jetty_after={}/{} jfr_before={} jfr_after={}",
        name, q.accepted, q.accepted_known, q.retired, q.posted, q.received, q.rx_retired, q.post_status, q.poll_status,
        q.modify_status, q.flush_status, q.flush_done[0], q.flush_done[1], q.state_before[0], q.state_before[1],
        q.state_after[0], q.state_after[1], q.jfr_state_before, q.jfr_state_after);
    std::println("  jfr_query_status={} queried_rx_depth={} jetty_query_status={}/{} modify_status={}/{}",
                 q.jfr_query_status, q.queried_rx_depth, q.query_status[0], q.query_status[1], q.modify_statuses[0],
                 q.modify_statuses[1]);
    for (std::size_t i = 0; i < q.statuses.size(); ++i) {
        if (q.statuses[i]) {
            std::println("  terminal_status[{}]={}", i, q.statuses[i]);
        }
    }
}
int RunWorker(void* argument) {
    const auto options = *static_cast<const AsyncErrorOptions*>(argument);
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return 1;
    }
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        return Fail({"initialize async runtime", initialized.error().provider_error});
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    const auto status = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (status != URMA_SUCCESS) {
        return Fail({"query async device", status});
    }
    auto& session = hardware->session;
    if (auto opened = session.Open(ctx, attributes.dev_cap, options, FLAGS_server); !opened) {
        return Fail(opened.error());
    }
    std::println("{} ready for async errors: fault={} device={} eid_index={} attempts={} requested_rx_cq={} "
                 "reported_rx_cq={} async_fd={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", kbsocket::tools::AsyncFaultName(options.fault), FLAGS_device,
                 FLAGS_eid_index, options.attempts, options.cq_depth, session.fault.rx->jfc_cfg.depth, ctx->async_fd,
                 FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto result =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), options.timeout_ms);
    if (result) {
        result = session.Run(channel);
    }
    PrintQueue("fault", session.fault.stats);
    PrintQueue("healthy", session.healthy.stats);
    PrintEvents("baseline", session.stats.baseline);
    PrintEvents("observation", session.stats.observation);
    PrintEvents("after", session.stats.after);
    PrintEvents("cleanup", session.stats.cleanup);
    if (!result) {
        return Fail(result.error());
    }
    const bool observed = session.stats.target_observed || session.stats.peer_target_observed;
    std::println("{}: requested async fault {}; independent healthy traffic validated before/during/after",
                 observed ? "OBSERVED" : "NOT_OBSERVED", kbsocket::tools::AsyncFaultName(options.fault));
    if (!session.stats.can_close) {
        std::println("RETAINED: terminal evidence incomplete; no manual delete/unregister/unload; "
                     "both workers retain all DMA dependencies until process exit");
    } else {
        auto closed = session.Close();
        PrintEvents("cleanup_final", session.stats.cleanup);
        if (!closed) {
            return Fail(closed.error());
        }
        auto runtime_closed = hardware->runtime.Close();
        if (!runtime_closed) {
            return Fail({"close async runtime", runtime_closed.error().provider_error});
        }
        auto unloaded = UrmaApi::Unload();
        if (!unloaded) {
            return Fail({"unload async library", 1});
        }
        constexpr std::uint32_t kClosed = 0x434c4f53;
        if (auto sent = channel.SendMarker(kClosed, options.timeout_ms); !sent) {
            return Fail(sent.error());
        }
        if (auto peer = channel.ExpectMarker(kClosed, options.timeout_ms); !peer) {
            return Fail(peer.error());
        }
        std::println("CLOSED: both endpoints released resources after complete terminal evidence");
    }
    if (!observed) {
        std::println("NOT_OBSERVED: injection did not establish the requested fault; exit=77, not validation PASS");
        return 77;
    }
    std::println("PASS: requested async fault observed and acknowledged; independent healthy traffic succeeded; "
                 "resource disposition reported separately");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("Observe JFC/JFR errors, associated jetty events and independent queue isolation");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    AsyncErrorOptions options{.attempts = FLAGS_attempts,
                              .cq_depth = FLAGS_cq_depth,
                              .healthy_rounds = FLAGS_healthy_rounds,
                              .observe_ms = FLAGS_observe_ms,
                              .timeout_ms = FLAGS_timeout_ms};
    if (FLAGS_fault == "jfc_overflow") {
        options.fault = kbsocket::tools::AsyncFault::kJfcOverflow;
    } else if (FLAGS_fault == "external_jfc") {
        options.fault = kbsocket::tools::AsyncFault::kExternalJfc;
    } else if (FLAGS_fault == "external_jfr") {
        options.fault = kbsocket::tools::AsyncFault::kExternalJfr;
    } else {
        std::println(stderr, "fault must be jfc_overflow, external_jfc or external_jfr");
        return 2;
    }
    const auto watchdog =
        FLAGS_watchdog_ms ? static_cast<std::uint64_t>(FLAGS_watchdog_ms)
                          : std::max<std::uint64_t>(60000, options.observe_ms + (2ULL * options.healthy_rounds + 12) *
                                                                                    options.timeout_ms);
    auto valid = kbsocket::tools::ValidateAsyncErrorOptions(options);
    if (!valid || argc != 1 || FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 ||
        watchdog <= options.timeout_ms || watchdog > 3600000) {
        std::println(stderr, "Invalid async error options; require device and bounded deadlines; use --help");
        return 2;
    }
    return kbsocket::tools::RunPeerWatchdog(RunWorker, &options, static_cast<std::uint32_t>(watchdog));
}
