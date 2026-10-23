// SPDX-License-Identifier: MulanPSL-2.0
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <print>
#include <span>
#include <string>

#include "tools/common/control_channel.hpp"
#include "tools/streaming_test/streaming_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_bool(server, false, "Echo server; omit for request sender");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4 control bind/peer address");
DEFINE_uint32(port, 18543, "TCP control port");
DEFINE_string(devices, "",
              "One to four distinct raw name:eid_index pairs, comma separated; same device with different EIDs "
              "allowed; pair by list order");
DEFINE_bool(two_devices_two_eids, false,
            "Require two raw devices with two EIDs each on both endpoints; matching device grouping by list order");
DEFINE_string(library, "liburma.so", "URMA library path");
DEFINE_uint32(bytes, 4096, "Payload bytes, 32..4096");
DEFINE_uint32(messages, 100000, "Messages per context per direction, window..10000000");
DEFINE_uint32(window, 32, "Per-context outstanding request window, 1..256");
DEFINE_bool(events, false, "Use JFCE arm/recheck/fd wait; report actual notification coverage");
DEFINE_uint32(arm_delay_us, 0,
              "Delay between rearm and recheck, events only; recheck progress skips fd wait, 0..1000000");
DEFINE_uint32(timeout_ms, 10000, "No-progress/control timeout, 1..3600000 ms");
DEFINE_uint32(process_timeout_sec, 300, "Hard process deadline including provider calls, 1..86400 seconds");

namespace {
using kbsocket::raw::UrmaApi;
struct Hardware {
    kbsocket::raw::RawRuntime runtime;
    std::array<kbsocket::tools::StreamSession, kbsocket::tools::kStreamMaxContexts> sessions;
};
int Fail(const kbsocket::tools::ToolError& error) {
    std::println(stderr, "FAIL: {}: {}; unresolved resources retained until process exit", error.operation, error.code);
    return 1;
}

void PrintCompletionFields(const urma_cr_t& cr, bool suppress_user_ctx) {
    // 边界伪 CQE 没有 WR 身份，不能把硬件填充的 user_ctx 当作可定位消息的线索。
    const bool suppressed =
        suppress_user_ctx || cr.status == URMA_CR_WR_FLUSH_ERR_DONE || cr.status == URMA_CR_WR_SUSPEND_DONE;
    std::print(stderr, "status={} local_id={} user_ctx={} user_ctx_source={} flags={:#x} opcode={} completion_len={}",
               static_cast<int>(cr.status), cr.local_id, suppressed ? 0 : cr.user_ctx,
               suppressed ? "suppressed" : "raw", static_cast<unsigned>(cr.flag.value), static_cast<int>(cr.opcode),
               cr.completion_len);
}

void PrintTimeoutProbe(std::size_t context, const kbsocket::tools::StreamTimeoutProbe& probe) {
    std::println(stderr, "Stream timeout probe: context={} attempted={} begin_ns={} end_ns={} sampling=diagnostic-only",
                 context, probe.attempted, probe.begin_ns, probe.end_ns);
    if (!probe.attempted) {
        return;
    }
    std::println(stderr,
                 "Stream probe notifications: context={} checked={} result={} errno={} tx={} rx={} foreign={} acked={}",
                 context, probe.notifications_checked, probe.notification_result, probe.notification_errno,
                 probe.tx_notifications, probe.rx_notifications, probe.foreign_notifications, probe.acknowledged);
    const auto print_completions = [&](const char* direction, int result, int saved_errno,
                                       std::span<const urma_cr_t> completions) {
        const bool count_valid = result >= 0 && result <= static_cast<int>(completions.size());
        const auto count = count_valid ? result : 0;
        std::println(stderr,
                     "Stream probe CQ: context={} direction={} result={} errno={} count_valid={} captured={} "
                     "batch_limit={} may_have_more={} sampling=diagnostic-only",
                     context, direction, result, saved_errno, count_valid, count, completions.size(),
                     result == static_cast<int>(completions.size()));
        for (int i = 0; i < count; ++i) {
            std::print(stderr, "Stream probe CQE: context={} direction={} batch_index={} ", context, direction, i);
            PrintCompletionFields(completions[i], false);
            // 探查不走正常身份校验与退休，RX 的 user_ctx 是槽位，TX 的原始值也不声明可信。
            std::println(stderr, " sequence_valid=false sampling=diagnostic-only");
        }
    };
    print_completions("tx", probe.tx_result, probe.tx_errno, probe.tx_completions);
    print_completions("rx", probe.rx_result, probe.rx_errno, probe.rx_completions);
}

void PrintRunDiagnostics(const kbsocket::tools::StreamRunDiagnostics& run,
                         std::span<kbsocket::tools::StreamSession* const> sessions,
                         std::span<const kbsocket::raw::DeviceConfig> devices,
                         const kbsocket::tools::StreamDeviceGroups& groups) {
    std::print(stderr, "Stream failure: phase={} context=", run.phase);
    if (run.context_valid) {
        std::print(stderr, "{}", run.context);
    } else {
        std::print(stderr, "none");
    }
    std::println(stderr, " started_ns={} failure_ns={} failure_unix_ns={}", run.started_ns, run.failure_ns,
                 run.failure_unix_ns);
    std::println(stderr, "Stream clocks: *_ns uses local steady clock except failure_unix_ns; cross-host wall-clock "
                         "comparison requires synchronized clocks");
    std::println(stderr, "Stream last wait: begin_ns={} end_ns={} result={} errno={} waits={} empty_wakes={}",
                 run.last_wait_begin_ns, run.last_wait_end_ns, run.wait_result, run.wait_errno, run.stats.waits,
                 run.stats.empty_wakes);
    for (std::size_t i = 0; i < sessions.size(); ++i) {
        const auto& diagnostics = sessions[i]->diagnostics();
        const auto progress_base = diagnostics.last_progress_ns ? diagnostics.last_progress_ns : diagnostics.started_ns;
        const bool idle_valid = progress_base != 0 && run.failure_ns >= progress_base;
        const char* idle_from = diagnostics.last_progress_ns ? "last_progress" : "start";
        std::println(stderr,
                     "Stream context: context={} device_group={} device={} eid_index={} jetty_id={} jfce_fd={} "
                     "wait_revents={:#x} started_ns={} last_progress_ns={} has_progress={} idle_ns={} idle_ns_valid={} "
                     "idle_from={}",
                     i, groups[i], devices[i].device_name, devices[i].eid_index, sessions[i]->endpoint().id,
                     sessions[i]->event_fd(), static_cast<unsigned short>(run.wait_revents[i]), diagnostics.started_ns,
                     diagnostics.last_progress_ns, diagnostics.last_progress_ns != 0,
                     idle_valid ? run.failure_ns - progress_base : 0, idle_valid,
                     progress_base ? idle_from : "not_started");
        const auto& failure = diagnostics.failure_cqe;
        if (failure.present) {
            std::print(stderr,
                       "Stream failure CQE: context={} direction={} observed_ns={} batch_count={} batch_index={} ", i,
                       failure.receive ? "rx" : "tx", failure.observed_ns, failure.batch_count, failure.batch_index);
            PrintCompletionFields(failure.completion, failure.user_ctx_suppressed);
            std::println(stderr, " sequence_valid={} sequence={}", failure.sequence_valid,
                         failure.sequence_valid ? failure.sequence : 0);
        }
        PrintTimeoutProbe(i, diagnostics.timeout_probe);
    }
}

int Run() {
    kbsocket::tools::StreamOptions options{FLAGS_bytes,
                                           FLAGS_messages,
                                           FLAGS_window,
                                           FLAGS_timeout_ms,
                                           FLAGS_events,
                                           FLAGS_arm_delay_us,
                                           FLAGS_two_devices_two_eids};
    auto devices = kbsocket::tools::ParseStreamDevices(FLAGS_devices);
    if (!devices) {
        std::println(stderr, "Invalid --devices='{}': {}", FLAGS_devices, devices.error().operation);
        return 2;
    }
    auto device_groups = kbsocket::tools::BuildStreamDeviceGroups(*devices, FLAGS_two_devices_two_eids);
    if (!device_groups) {
        std::println(stderr, "Invalid --devices topology: {}", device_groups.error().operation);
        return 2;
    }
    if (auto validated = kbsocket::tools::ValidateStreamOptions(options); !validated) {
        std::println(stderr, "Invalid options: {}; use --help for limits", validated.error().operation);
        return 2;
    }
    if (!FLAGS_port || FLAGS_port > 65535) {
        std::println(stderr, "Invalid --port: expected 1..65535");
        return 2;
    }
    if (FLAGS_library.empty()) {
        std::println(stderr, "Invalid --library: expected a nonempty URMA library path");
        return 2;
    }
    if (!FLAGS_process_timeout_sec || FLAGS_process_timeout_sec > 86400) {
        std::println(stderr, "Invalid --process_timeout_sec: expected 1..86400");
        return 2;
    }
    // 系统默认 SIGALRM 直接终止进程；不在 provider 卡住时触发不安全的 C++ 栈析构。
    // shell 可据退出信号识别超时；普通无进展仍先由 timeout_ms 给出具体错误。
    ::alarm(FLAGS_process_timeout_sec);
    static kbsocket::NoDestructor<Hardware> hardware;
    auto loaded = UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "FAIL: load URMA: {}", loaded.error().message);
        return 1;
    }
    auto initialized = hardware->runtime.Initialize(*devices);
    if (!initialized) {
        std::println(stderr, "FAIL: initialize devices: code={} provider={}",
                     static_cast<int>(initialized.error().code), initialized.error().provider_error);
        return 1;
    }
    kbsocket::tools::StreamHello hello{.options = options,
                                       .server = FLAGS_server,
                                       .count = static_cast<std::uint32_t>(devices->size()),
                                       .device_groups = *device_groups};
    std::array<kbsocket::tools::StreamSession*, kbsocket::tools::kStreamMaxContexts> sessions{};
    for (std::size_t i = 0; i < devices->size(); ++i) {
        urma_device_attr_t attributes{};
        auto* ctx = hardware->runtime.context(i);
        const auto queried = UrmaApi::QueryDevice(ctx->dev, &attributes);
        if (queried != URMA_SUCCESS) {
            return Fail({"query streaming device", queried});
        }
        auto opened = hardware->sessions[i].Open(ctx, attributes.dev_cap, options, FLAGS_server, i);
        if (!opened) {
            return Fail(opened.error());
        }
        sessions[i] = &hardware->sessions[i];
        hello.endpoints[i] = sessions[i]->endpoint();
    }
    std::println("{} ready for continuous SEND: devices={} bytes={} messages={} window={} events={} arm_delay_us={} "
                 "two_devices_two_eids={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_devices, FLAGS_bytes, FLAGS_messages, FLAGS_window,
                 FLAGS_events, FLAGS_arm_delay_us, FLAGS_two_devices_two_eids, FLAGS_address, FLAGS_port);
    for (std::size_t i = 0; i < devices->size(); ++i) {
        const auto endpoint = sessions[i]->endpoint();
        std::println("Context {} mapping: device_group={} device={} eid_index={} jetty_id={} jfce_fd={}", i,
                     (*device_groups)[i], (*devices)[i].device_name, (*devices)[i].eid_index, endpoint.id,
                     sessions[i]->event_fd());
    }
    std::fflush(stdout);
    kbsocket::tools::ControlChannel channel;
    auto connected = channel.Open(FLAGS_server, FLAGS_address.c_str(), FLAGS_port, FLAGS_timeout_ms);
    if (!connected) {
        return Fail(connected.error());
    }
    auto peer = kbsocket::tools::ExchangeStreamHello(channel, hello);
    if (!peer) {
        return Fail(peer.error());
    }
    for (std::size_t i = 0; i < devices->size(); ++i) {
        auto imported = sessions[i]->Connect(peer->endpoints[i]);
        if (!imported) {
            return Fail(imported.error());
        }
        auto started = sessions[i]->Start();
        if (!started) {
            return Fail(started.error());
        }
    }
    auto barrier = [&](std::uint32_t marker) -> std::expected<void, kbsocket::tools::ToolError> {
        auto written = channel.SendMarker(marker, FLAGS_timeout_ms);
        if (!written) {
            return written;
        }
        return channel.ExpectMarker(marker, FLAGS_timeout_ms);
    };
    auto ready = barrier(0x53545259);
    if (!ready) {
        return Fail(ready.error());
    }
    kbsocket::tools::StreamRunDiagnostics diagnostics;
    auto result = kbsocket::tools::RunStreams(std::span(sessions).first(devices->size()), options, &diagnostics);
    for (std::size_t i = 0; i < devices->size(); ++i) {
        const auto& s = sessions[i]->stats();
        std::println(
            "Context {} {}:{}: accepted={} retired={} received={} rx_posted={} rearms={} tx_events={} rx_events={} "
            "acks={} recheck_progress={} rx_reposts={} device_group={}",
            i, (*devices)[i].device_name, (*devices)[i].eid_index, s.accepted, s.retired, s.received, s.rx_posted,
            s.rearms, s.tx_notifications, s.rx_notifications, s.event_acks, s.recheck_progress,
            s.rx_posted >= options.window ? s.rx_posted - options.window : 0, (*device_groups)[i]);
    }
    if (!result) {
        std::fflush(stdout);
        PrintRunDiagnostics(diagnostics, std::span(sessions).first(devices->size()), *devices, *device_groups);
        return Fail(result.error());
    }
    auto stopped = barrier(0x53545354);
    if (!stopped) {
        return Fail(stopped.error());
    }
    for (std::size_t i = 0; i < devices->size(); ++i) {
        sessions[i]->ConfirmPeerQuiescent();
        auto closed = sessions[i]->Close();
        if (!closed) {
            return Fail(closed.error());
        }
    }
    auto runtime_closed = hardware->runtime.Close();
    if (!runtime_closed) {
        return Fail({"close streaming runtime", runtime_closed.error().provider_error});
    }
    auto unloaded = UrmaApi::Unload();
    if (!unloaded) {
        std::println(stderr, "FAIL: unload: {}", unloaded.error().message);
        return 1;
    }
    auto closed = barrier(0x5354434c);
    if (!closed) {
        return Fail(closed.error());
    }
    ::alarm(0);
    if (FLAGS_events) {
        std::uint64_t notifications = 0;
        std::uint64_t fd_wakes = 0;
        for (std::size_t i = 0; i < devices->size(); ++i) {
            const auto& s = sessions[i]->stats();
            notifications += s.event_acks;
            fd_wakes += s.fd_wakes;
            // WaitJfc(0) 可以在未进入 fd 等待时取得通知；两种覆盖分别报告。
            std::println("{}: context {} JFC notifications; tx_events={} rx_events={} acks={}",
                         s.event_acks ? "OBSERVED" : "NOT_OBSERVED", i, s.tx_notifications, s.rx_notifications,
                         s.event_acks);
            std::println("{}: context {} JFCE fd readiness; fd_wakes={}", s.fd_wakes ? "OBSERVED" : "NOT_OBSERVED", i,
                         s.fd_wakes);
        }
        std::println("{}: JFC notifications; events={}", notifications ? "OBSERVED" : "NOT_OBSERVED", notifications);
        std::println("{}: JFCE fd readiness; waits={} fd_wakes={} empty_wakes={}",
                     fd_wakes ? "OBSERVED" : "NOT_OBSERVED", result->waits, fd_wakes, result->empty_wakes);
    }
    std::println("PASS: continuous request/echo on {} raw contexts; all payloads validated; both endpoints closed",
                 devices->size());
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        std::println(stderr, "Unexpected positional arguments");
        return 2;
    }
    try {
        return Run();
    } catch (const std::exception& error) {
        std::println(stderr, "FAIL: {}; resources retained until exit", error.what());
        return 1;
    }
}
