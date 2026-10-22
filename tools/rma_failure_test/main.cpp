// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <print>

#include "tools/common/control_channel.hpp"
#include "tools/peer_failure_test/process.hpp"
#include "tools/rma_failure_test/fault_peer.hpp"
#include "tools/rma_failure_test/rma_failure_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(server, false, "Own the healthy WRITE target and supervise a disposable fault target process");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4 control bind/peer address");
DEFINE_uint32(port, 18528, "TCP control port; payload uses raw URMA WRITE");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_uint32(bytes, 16384, "Bytes per WRITE, 1..8MiB; 2*batch*bytes must not exceed 256MiB");
DEFINE_uint32(batch, 32, "WRITEs per target per round, 1..128; mixed chain has 2*batch WRs");
DEFINE_uint32(rounds, 8, "Number of rounds per before/mixed/after phase, 1..256; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Per control/completion deadline, 100..120000 milliseconds");
DEFINE_uint32(watchdog_ms, 0, "Whole-worker deadline including provider calls; 0 derives a budget, max 3600000");

namespace {
using kbsocket::raw::UrmaApi;
using kbsocket::tools::RmaFailureOptions;
using kbsocket::tools::ToolError;
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::RmaFailureSession session;
};
int Fail(ToolError error) {
    std::println(stderr, "FAIL: {}: {}", error.operation, error.code);
    std::println(stderr, "RETAINED: no speculative delete/unregister/unload; unresolved DMA dependencies retained "
                         "until worker exit");
    return 1;
}
void PrintCounters(const char* name, const kbsocket::tools::RmaWriteCounters& counters) {
    std::println("{}: accepted={} retired={} verified={}", name, counters.accepted, counters.retired,
                 counters.verified);
    for (std::size_t i = 0; i < counters.statuses.size(); ++i) {
        if (counters.statuses[i]) {
            std::println("  terminal_status[{}]={}", i, counters.statuses[i]);
        }
    }
}
void PrintStats(const kbsocket::tools::RmaFailureStats& stats) {
    PrintCounters("Healthy baseline", stats.healthy_baseline);
    PrintCounters("Fault baseline", stats.fault_baseline);
    PrintCounters("Healthy before", stats.healthy_before);
    PrintCounters("Healthy mixed", stats.healthy_mixed);
    PrintCounters("Fault mixed", stats.fault_mixed);
    PrintCounters("Healthy after", stats.healthy_after);
    std::println("Same SQ: jetty_id={} state_checks={} last_query_status={} last_state={} post_calls={} "
                 "last_post_status={} last_completion_status={} partial_batches={} accepted_known={} "
                 "pending={} peer_exit_confirmed={}",
                 stats.jetty_id, stats.state_checks, stats.last_query_status, stats.last_state, stats.post_calls,
                 stats.last_post_status, stats.last_completion_status, stats.partial_batches, stats.accepted_known,
                 stats.pending, stats.peer_exit_confirmed);
}

int RunWorker(void* argument) {
    const auto options = *static_cast<const RmaFailureOptions*>(argument);
    // fork 发生在本进程第一次 Load 之前，子进程不会继承健康端点或 provider 工作线程。
    kbsocket::tools::RmaFailurePeerProcess fault_peer;
    if (FLAGS_server) {
        auto opened = fault_peer.Open(FLAGS_library.c_str(), FLAGS_device.c_str(), FLAGS_eid_index, options);
        if (!opened) {
            return Fail(opened.error());
        }
    }
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return 1;
    }
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        return Fail({"initialize WRITE failure runtime", initialized.error().provider_error});
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    const auto status = UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (status != URMA_SUCCESS) {
        return Fail({"query WRITE failure device", status});
    }
    auto& session = hardware->session;
    if (auto opened = session.Open(ctx, attributes.dev_cap, options, FLAGS_server); !opened) {
        return Fail(opened.error());
    }
    std::println("{} ready for same-SQ WRITE failure: device={} eid_index={} bytes_per_write={} batch={} rounds={} "
                 "mixed_wrs={} max_write_size={} RX_posted=0 watchdog_scope=worker_and_fault_child control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, options.bytes, options.batch,
                 options.rounds, 2 * options.batch, attributes.dev_cap.max_write_size, FLAGS_address, FLAGS_port);
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto result =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), options.timeout_ms);
    if (result) {
        result = session.Run(channel, FLAGS_server ? &fault_peer : nullptr);
    }
    PrintStats(session.resources.stats);
    if (!result) {
        return Fail(result.error());
    }
    if (!session.can_close()) {
        return Fail({"WRITE failure peer/DMA state unresolved", EBUSY});
    }
    if (auto closed = session.Close(); !closed) {
        return Fail(closed.error());
    }
    if (auto closed = hardware->runtime.Close(); !closed) {
        return Fail({"close WRITE failure runtime", closed.error().provider_error});
    }
    if (auto unloaded = UrmaApi::Unload(); !unloaded) {
        return Fail({"unload WRITE failure URMA", EBUSY});
    }
    constexpr std::uint32_t kClosed = 0x5246434c;
    if (auto sent = channel.SendMarker(kClosed, options.timeout_ms); !sent) {
        std::println(stderr, "FAIL: local resources closed but close marker failed: {}: {}", sent.error().operation,
                     sent.error().code);
        return 1;
    }
    if (auto peer = channel.ExpectMarker(kClosed, options.timeout_ms); !peer) {
        std::println(stderr, "FAIL: local resources closed but peer closure unconfirmed: {}: {}",
                     peer.error().operation, peer.error().code);
        return 1;
    }
    std::println("PASS: old-target WRITEs retired once with observed error statuses after confirmed SIGKILL/waitpid; "
                 "healthy remote payloads and guards validated before/during/after on the same READY SQ; "
                 "no RX buffers posted, SQ modification/recreation or replay; both endpoints closed resources");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("Observe post-exit WRITE failures and healthy remote access on the same raw RM_CTP SQ");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    RmaFailureOptions options{
        .bytes = FLAGS_bytes, .batch = FLAGS_batch, .rounds = FLAGS_rounds, .timeout_ms = FLAGS_timeout_ms};
    const auto watchdog =
        FLAGS_watchdog_ms
            ? static_cast<std::uint64_t>(FLAGS_watchdog_ms)
            : std::clamp<std::uint64_t>((6ULL * options.rounds + 16) * options.timeout_ms, 60000, 3600000);
    const auto valid = kbsocket::tools::ValidateRmaFailureOptions(options);
    if (!valid || argc != 1 || FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 ||
        watchdog <= options.timeout_ms || watchdog > 3600000) {
        std::println(stderr, "Invalid WRITE failure options; require device, bytes/batch/rounds within limits and "
                             "timeout_ms < watchdog_ms <= 3600000. Use --help.");
        return 2;
    }
    return kbsocket::tools::RunPeerWatchdog(RunWorker, &options, static_cast<std::uint32_t>(watchdog));
}
