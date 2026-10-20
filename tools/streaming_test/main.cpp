// SPDX-License-Identifier: MulanPSL-2.0
#include <unistd.h>

#include <array>
#include <cstdio>
#include <exception>
#include <print>
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
DEFINE_string(library, "liburma.so", "URMA library path");
DEFINE_uint32(bytes, 4096, "Payload bytes, 32..4096");
DEFINE_uint32(messages, 100000, "Messages per context per direction, window..10000000");
DEFINE_uint32(window, 32, "Per-context outstanding request window, 1..256");
DEFINE_bool(events, false, "Use JFCE arm/recheck/fd wait; report actual notification coverage");
DEFINE_uint32(arm_delay_us, 0, "Delay between rearm and recheck, events only, 0..1000000");
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
int Run() {
    kbsocket::tools::StreamOptions options{FLAGS_bytes,      FLAGS_messages, FLAGS_window,
                                           FLAGS_timeout_ms, FLAGS_events,   FLAGS_arm_delay_us};
    auto devices = kbsocket::tools::ParseStreamDevices(FLAGS_devices);
    if (!devices) {
        std::println(stderr, "Invalid --devices='{}': {}", FLAGS_devices, devices.error().operation);
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
    kbsocket::tools::StreamHello hello{
        .options = options, .server = FLAGS_server, .count = static_cast<std::uint32_t>(devices->size())};
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
    std::println("{} ready for continuous SEND: devices={} bytes={} messages={} window={} events={} control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_devices, FLAGS_bytes, FLAGS_messages, FLAGS_window,
                 FLAGS_events, FLAGS_address, FLAGS_port);
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
    auto result = kbsocket::tools::RunStreams(std::span(sessions).first(devices->size()), options);
    for (std::size_t i = 0; i < devices->size(); ++i) {
        const auto& s = sessions[i]->stats();
        std::println(
            "Context {} {}:{}: accepted={} retired={} received={} rx_posted={} rearms={} tx_events={} rx_events={} "
            "acks={} recheck_progress={}",
            i, (*devices)[i].device_name, (*devices)[i].eid_index, s.accepted, s.retired, s.received, s.rx_posted,
            s.rearms, s.tx_notifications, s.rx_notifications, s.event_acks, s.recheck_progress);
    }
    if (!result) {
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
        for (std::size_t i = 0; i < devices->size(); ++i) {
            const auto& s = sessions[i]->stats();
            notifications += s.event_acks;
            std::println("{}: context {} JFC notification path; fd_wakes={} tx_events={} rx_events={} acks={}",
                         s.fd_wakes && s.event_acks ? "OBSERVED" : "NOT_OBSERVED", i, s.fd_wakes, s.tx_notifications,
                         s.rx_notifications, s.event_acks);
        }
        std::println("{}: JFC notification path; waits={} events={} empty_wakes={}",
                     result->waits && notifications ? "OBSERVED" : "NOT_OBSERVED", result->waits, notifications,
                     result->empty_wakes);
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
