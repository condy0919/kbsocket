// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <numeric>
#include <print>
#include <string>

#include "tools/import_jetty_test/control_plane.hpp"
#include "tools/import_jetty_test/import_test.hpp"
#include <gflags/gflags.h>

#include "kbsocket/base/scope_exit.hpp"

DEFINE_bool(server, false, "Export 100 jettys; omit on importing client");
DEFINE_string(address, "127.0.0.1", "Numeric IPv4: server bind address or client peer address");
DEFINE_uint32(port, 18516, "TCP metadata/control port");
DEFINE_string(device, "", "URMA raw or bonding device name (required)");
DEFINE_uint32(eid_index, 0, "Local EID index");
DEFINE_string(library, "liburma.so", "URMA shared library path");
DEFINE_int32(priority, -1, "CTP priority, -1 selects first advertised CTP priority");
DEFINE_uint32(timeout_ms, 120000, "Timeout per TCP operation; does not interrupt a URMA call");
DEFINE_bool(control_plane, false, "Measure RM_CTP control-plane resource lifecycle on both peers");
DEFINE_bool(all_samples, false, "Print per-call samples as CSV after measurement");
DEFINE_bool(stress, false, "Concurrent import only; retain targets until all workers finish (both peers)");
DEFINE_uint32(import_count, 10000, "Client stress import count, reusing 100 remote jetty descriptors");
DEFINE_uint32(import_threads, 8, "Client stress worker count, sharing one context");

namespace {
using kbsocket::raw::UrmaApi;
using kbsocket::tools::ControlApi;
using kbsocket::tools::ToolError;
auto Error(const char* operation, int code = errno) noexcept {
    return std::unexpected(ToolError{operation, code});
}

// 工具独立支持 bonding，不改变核心 RawRuntime 的设备白名单契约。
class Environment {
public:
    ~Environment() {
        // 报告必须晚于所有释放操作，包括失败路径上的清理。
        kbsocket::ScopeExit report([&]() noexcept {
            if (FLAGS_control_plane) {
                try {
                    std::println("CONTROL_PLANE role={} unit=ns", FLAGS_server ? "server" : "client");
                    timings.Print(stdout, FLAGS_all_samples);
                } catch (...) {
                    std::fprintf(stderr, "Failed to print control-plane report\n");
                }
            }
        });
        if (auto result = Close(); !result) {
            std::fprintf(stderr, "Cleanup failed: %s (%d); dependencies retained until process exit\n",
                         result.error().operation, result.error().code);
        }
    }
    std::expected<void, ToolError> Open() {
        auto loaded = UrmaApi::Load(FLAGS_library.c_str());
        if (!loaded) {
            std::println(stderr, "Load: {}", loaded.error().message);
            return Error("load URMA", EINVAL);
        }
        loaded_ = true;
        if (FLAGS_control_plane) {
            if (auto result = extra.Load(FLAGS_library.c_str()); !result) {
                return result;
            }
        }
        urma_init_attr_t init{};
        auto rc = Call(ControlApi::Init, [&] { return UrmaApi::Init(&init); });
        if (rc != URMA_SUCCESS) {
            return Error("init URMA", rc);
        }
        initialized_ = true;
        UrmaApi::LogSetLevel(URMA_VLOG_LEVEL_ERR);
        int count = 0;
        devices_ = Call(ControlApi::GetDeviceList, [&] { return UrmaApi::GetDeviceList(&count); });
        if (!devices_) {
            return Error("get device list");
        }
        urma_device_t* device = nullptr;
        for (int i = 0; i < count; ++i) {
            if (FLAGS_device == devices_[i]->name) {
                device = devices_[i];
                break;
            }
        }
        if (!device || device->type != URMA_TRANSPORT_UB) {
            return Error("UB device not found", ENODEV);
        }
        urma_device_attr_t attr{};
        rc = Call(ControlApi::QueryDevice, [&] { return UrmaApi::QueryDevice(device, &attr); });
        if (rc != URMA_SUCCESS) {
            return Error("query device", rc);
        }
        auto selected = kbsocket::tools::SelectCtpPriority(attr.dev_cap, FLAGS_priority);
        if (!selected) {
            return std::unexpected(selected.error());
        }
        priority = *selected;
        ctx_ = Call(ControlApi::CreateContext, [&] { return UrmaApi::CreateContext(device, FLAGS_eid_index); });
        if (!ctx_) {
            return Error("create context");
        }
        return session.Open(ctx_, attr.dev_cap, priority);
    }
    std::expected<void, ToolError> Close() noexcept {
        if (auto result = control.Close(); !result) {
            return result;
        }
        if (auto result = session.Close(); !result) {
            return result;
        }
        if (ctx_) {
            const auto rc = Call(ControlApi::DeleteContext, [&] { return UrmaApi::DeleteContext(ctx_); });
            if (rc != URMA_SUCCESS) {
                return Error("delete context", rc);
            }
            ctx_ = nullptr;
        }
        if (devices_) {
            Call(ControlApi::FreeDeviceList, [&] { return UrmaApi::FreeDeviceList(devices_); });
            devices_ = nullptr;
        }
        if (initialized_) {
            const auto rc = Call(ControlApi::Uninit, [&] { return UrmaApi::Uninit(); });
            if (rc != URMA_SUCCESS) {
                return Error("uninit URMA", rc);
            }
            initialized_ = false;
        }
        if (loaded_) {
            if (!UrmaApi::Unload()) {
                return Error("unload URMA", EBUSY);
            }
            loaded_ = false;
        }
        return {};
    }
    kbsocket::tools::ApiTimings timings;
    kbsocket::tools::ControlPlaneApis extra;
    kbsocket::tools::ImportSession session{FLAGS_control_plane ? &timings : nullptr};
    kbsocket::tools::ControlPlaneSession control{timings, extra};
    std::expected<void, ToolError> RunControlPlane(kbsocket::tools::ControlChannel& channel) {
        if (auto result = control.RunLocal(ctx_, priority, session.jettys()); !result) {
            return result;
        }
        return control.RunSegments(channel, FLAGS_server, FLAGS_timeout_ms);
    }
    unsigned priority = 0;

private:
    template <typename F>
    auto Call(ControlApi api, F&& function) -> std::invoke_result_t<F> {
        return kbsocket::tools::MeasureApi(FLAGS_control_plane ? &timings : nullptr, api, std::forward<F>(function));
    }
    bool loaded_ = false;
    bool initialized_ = false;
    urma_device_t** devices_ = nullptr;
    urma_context_t* ctx_ = nullptr;
};

int Run() {
    if (FLAGS_device.empty() || FLAGS_library.empty() || FLAGS_port == 0 || FLAGS_port > 65535 ||
        FLAGS_timeout_ms == 0 || FLAGS_timeout_ms > 3600000 || FLAGS_priority < -1 ||
        FLAGS_priority > URMA_MAX_PRIORITY || (FLAGS_stress && FLAGS_control_plane) || FLAGS_import_count == 0 ||
        FLAGS_import_count > 10000000 || FLAGS_import_threads == 0 || FLAGS_import_threads > 256 ||
        FLAGS_import_threads > FLAGS_import_count ||
        (!FLAGS_stress && (FLAGS_import_count != 10000 || FLAGS_import_threads != 8))) {
        std::println(stderr, "Invalid arguments; --device is required. See --help.");
        return 2;
    }
    // channel 先构造，确保失败展开时本端先清理导入句柄，再关闭 TCP 通知对端。
    kbsocket::tools::ControlChannel channel;
    Environment env;
    if (auto result = env.Open(); !result) {
        std::println(stderr, "FAIL: {}: {}", result.error().operation, result.error().code);
        return 1;
    }
    std::println("{} ready: device={} eid_index={} priority={} mode=RM_CTP local_jettys=100 control={}:{}",
                 FLAGS_server ? "Server" : "Client", FLAGS_device, FLAGS_eid_index, env.priority, FLAGS_address,
                 FLAGS_port);
    std::fflush(stdout);
    auto connected =
        channel.Open(FLAGS_server, FLAGS_address.c_str(), static_cast<std::uint16_t>(FLAGS_port), FLAGS_timeout_ms);
    const kbsocket::tools::StressOptions stress{FLAGS_stress, FLAGS_import_count, FLAGS_import_threads};
    auto result = connected ? env.session.Run(channel, FLAGS_server, FLAGS_timeout_ms, stress) : connected;
    if (FLAGS_stress && !FLAGS_server && connected) {
        env.session.stress().Print(FLAGS_all_samples);
    }
    if (result && FLAGS_control_plane) {
        result = env.RunControlPlane(channel);
    }
    if (!result) {
        std::println(stderr, "FAIL: {}: {}; completed_imports={}", result.error().operation, result.error().code,
                     env.session.completed());
        return 1;
    }
    if (!FLAGS_server && !FLAGS_stress) {
        const auto& samples = env.session.samples();
        auto sorted = samples;
        std::sort(sorted.begin(), sorted.end());
        const double mean = std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size() / 1000.0;
        std::println("IMPORT #1:   {} ns ({:.3f} us)", samples[0], samples[0] / 1000.0);
        std::println("IMPORT #100: {} ns ({:.3f} us)", samples[99], samples[99] / 1000.0);
        std::println("100 calls: min={:.3f} mean={:.3f} p50={:.3f} p95={:.3f} p99={:.3f} max={:.3f} us",
                     sorted.front() / 1000.0, mean, sorted[49] / 1000.0, sorted[94] / 1000.0, sorted[98] / 1000.0,
                     sorted.back() / 1000.0);
        if (FLAGS_all_samples) {
            std::println("import_index,elapsed_ns");
            for (std::size_t i = 0; i < samples.size(); ++i) {
                std::println("{},{}", i + 1, samples[i]);
            }
        }
    }
    if (auto closed = env.Close(); !closed) {
        std::println(stderr, "FAIL cleanup: {}: {}", closed.error().operation, closed.error().code);
        return 1;
    }
    if (FLAGS_control_plane && env.timings.HasFailures()) {
        std::println(stderr, "Control-plane suite completed with API failures; see per-API report");
        return 1;
    }
    if (FLAGS_stress) {
        std::println("PASS: import stress session completed; no application data WRs posted");
    } else {
        std::println("PASS: 100 imports completed; no application data WRs posted");
    }
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("--device DEVICE [--server] --address IPv4 [--all_samples] [--control_plane | --stress "
                            "--import_threads N --import_count N]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        std::fprintf(stderr, "Unexpected positional arguments\n");
        return 2;
    }
    try {
        return Run();
    } catch (const std::exception& error) {
        std::fprintf(stderr, "FAIL: %s\n", error.what());
        return 1;
    }
}
