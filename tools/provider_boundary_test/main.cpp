// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdio>
#include <print>
#include <string>

#include "tools/common/control_channel.hpp"
#include "tools/provider_boundary_test/provider_boundary_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(server, false, "Serve an empty JFR for raw provider boundary observations");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4 bind/peer address");
DEFINE_uint32(port, 18540, "TCP control port");
DEFINE_string(device, "", "Raw UB device name");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA library path");
DEFINE_uint32(tx_depth, 32, "Requested SQ depth, 1 to 4096; must match peer");
DEFINE_uint32(batch, 33, "Direct post batch size, 1 to 256; must match peer");
DEFINE_uint32(attempts, 1024, "Finite WR budget; tx_depth < attempts <= 16384; must match peer");
DEFINE_uint32(timeout_ms, 10000, "Per drain timeout, 1 to 3600000 ms");
DEFINE_uint32(watchdog_ms, 0, "Whole worker deadline; zero computes 6*timeout_ms+10000, including provider cleanup");
DEFINE_bool(error_post, false, "After drain, directly post one WR to ERROR SQ; may retain resources until exit");
DEFINE_bool(urma_debug, false, "Enable URMA debug logging");

namespace {
using kbsocket::raw::UrmaApi;
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    kbsocket::tools::ProviderBoundarySession session;
};
void UrmaLog(int level, char* message) noexcept {
    const int saved = errno;
    try {
        std::println(stderr, "URMA[{}]: {}", level, message ? message : "");
    } catch (...) {
    }
    errno = saved;
}
bool CloseRuntime(Hardware& hardware) {
    auto closed = hardware.runtime.Close();
    if (!closed) {
        std::println(stderr, "FAIL: close runtime: {}; dependencies retained", closed.error().provider_error);
        return false;
    }
    auto unloaded = UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "FAIL: unload URMA: {}", unloaded.error().message);
        return false;
    }
    return true;
}
void PrintStats(const kbsocket::tools::ProviderBoundaryStats& stats) {
    if (FLAGS_server) {
        std::println("Empty target: no RX posted; provider return codes and terminal statuses are on client.");
        return;
    }
    const auto& s = stats.statuses;
    std::println("Provider post: offered={} accepted={} accepted_known={} retired={} calls={} last_status={} "
                 "eagain={} enomem={} partial_batches={}",
                 stats.offered, stats.accepted_known ? std::to_string(stats.accepted) : "unknown", stats.accepted_known,
                 stats.retired, stats.post_calls, stats.last_post_status, stats.eagain, stats.enomem,
                 stats.partial_batches);
    std::println("Terminal: success={} flush_err={} unhandled={} loc_access_err={} remote_access_abort_err={} "
                 "ack_timeout_err={} rnr={} flush_done={} flush_calls={}",
                 s[URMA_CR_SUCCESS], s[URMA_CR_WR_FLUSH_ERR], s[URMA_CR_WR_UNHANDLED], s[URMA_CR_LOC_ACCESS_ERR],
                 s[URMA_CR_REM_ACCESS_ABORT_ERR], s[URMA_CR_ACK_TIMEOUT_ERR], s[URMA_CR_RNR_RETRY_CNT_EXC_ERR],
                 stats.flush_done, stats.flush_calls);
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (s[i]) {
            std::println("  terminal_status[{}]={}", i, s[i]);
        }
    }
    std::println("{}: provider_capacity_rejection", stats.eagain || stats.enomem ? "OBSERVED" : "NOT_OBSERVED");
    std::println("{}: provider_partial_acceptance", stats.partial_batches ? "OBSERVED" : "NOT_OBSERVED");
    std::println("{}: nonzero_WR_UNHANDLED", s[URMA_CR_WR_UNHANDLED] ? "OBSERVED" : "NOT_OBSERVED");
    if (stats.error_post_attempted) {
        std::println("OBSERVED: direct_ERROR_post status={} queried_state={} terminal_pending={}",
                     stats.error_post_status, stats.queried_state, stats.error_post_pending);
    } else {
        std::println("{}: direct_ERROR_post", FLAGS_error_post ? "NOT_OBSERVED" : "SKIP");
    }
}
int Run() {
    const kbsocket::tools::ProviderBoundaryOptions options{.tx_depth = FLAGS_tx_depth,
                                                           .batch = FLAGS_batch,
                                                           .attempts = FLAGS_attempts,
                                                           .timeout_ms = FLAGS_timeout_ms,
                                                           .error_post = FLAGS_error_post};
    if (FLAGS_device.empty() || FLAGS_library.empty() || !FLAGS_port || FLAGS_port > 65535 ||
        !kbsocket::tools::ValidateProviderBoundaryOptions(options)) {
        std::println(stderr, "Invalid options; --device is required. Use --help for limits.");
        return 2;
    }
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return 1;
    }
    UrmaApi::RegisterLogFunc(UrmaLog);
    auto initialized = hardware->runtime.Initialize({{FLAGS_device, FLAGS_eid_index}});
    if (!initialized) {
        std::println(stderr, "FAIL: initialize runtime: {}", initialized.error().provider_error);
        CloseRuntime(*hardware);
        return 1;
    }
    if (FLAGS_urma_debug) {
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_DEBUG);
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attr{};
    const auto queried = UrmaApi::QueryDevice(ctx->dev, &attr);
    if (queried != URMA_SUCCESS) {
        std::println(stderr, "FAIL: query device: {}", static_cast<int>(queried));
        CloseRuntime(*hardware);
        return 1;
    }
    auto opened = hardware->session.Open(ctx, attr.dev_cap, options, FLAGS_server);
    if (!opened) {
        std::println(stderr, "FAIL: open session: {}: {}", opened.error().operation, opened.error().code);
        auto closed = hardware->session.Close();
        if (closed) {
            CloseRuntime(*hardware);
        }
        return 1;
    }
    std::println("{} ready for provider boundary: device={} eid_index={} tx_depth={} batch={} attempts={} "
                 "error_post={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, FLAGS_tx_depth, FLAGS_batch,
                 FLAGS_attempts, FLAGS_error_post, FLAGS_address, FLAGS_port);
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
        if (result && hardware->session.intentionally_retained()) {
            std::println("OBSERVED: ERROR post ownership unresolved; both endpoints retain resources until "
                         "process exit; SUCCESS from post is not terminal completion");
            return 0;
        }
        std::println(stderr, "FAIL: {}: {}; dependencies retained until process exit", closed.error().operation,
                     closed.error().code);
        return 1;
    }
    if (!CloseRuntime(*hardware)) {
        return 1;
    }
    auto confirmed = channel.SendMarker(0x4b425043, FLAGS_timeout_ms);
    if (confirmed) {
        confirmed = channel.ExpectMarker(0x4b425043, FLAGS_timeout_ms);
    }
    if (!result || !confirmed) {
        return 1;
    }
    std::println("PASS: all accepted WRs retired once; both endpoints closed resources; individual "
                 "NOT_OBSERVED results remain unverified");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("Observe raw provider SQ capacity, partial acceptance, software flush and ERROR post.");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1 || !FLAGS_timeout_ms || FLAGS_timeout_ms > 3600000) {
        std::println(stderr, "Invalid command line or timeout_ms; use --help");
        return 2;
    }
    const auto timeout = FLAGS_watchdog_ms ? FLAGS_watchdog_ms : FLAGS_timeout_ms * 6 + 10000;
    // fork 发生在 Load/Initialize 前，父进程完全不访问 URMA，可中止卡在同步删除调用内的子进程。
    const auto result = kbsocket::tools::SuperviseProviderWorker(Run, timeout);
    if (!result) {
        std::println(stderr, "FAIL: watchdog: {}: {}; process/DMA cleanup unresolved", result.error().operation,
                     result.error().code);
        return 1;
    }
    if (result->timed_out) {
        std::println(stderr,
                     "FAIL: watchdog expired after {} ms; provider worker SIGKILL/reap confirmed; "
                     "experiment or provider cleanup did not complete",
                     timeout);
        return 124;
    }
    if (result->signal) {
        std::println(stderr, "FAIL: provider worker terminated by signal {}", result->signal);
        return 1;
    }
    return result->exit_code;
}
