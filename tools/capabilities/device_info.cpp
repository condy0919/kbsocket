#include <cstdio>
#include <exception>
#include <print>
#include <string>
#include <vector>

#include "tools/capabilities/device_info_output.hpp"
#include <gflags/gflags.h>

#include "kbsocket/transport/raw/device_catalog.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

DEFINE_string(library, "liburma.so", "Path to the URMA shared library");

namespace {
struct UrmaSession {
    ~UrmaSession() {
        kbsocket::raw::UrmaApi::Uninit();
    }
};
} // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("[--library PATH] RAW_DEVICE [RAW_DEVICE ...]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc < 2 || FLAGS_library.empty()) {
        std::println(stderr, "Expected a nonempty library path and at least one RAW_DEVICE; use --help");
        return 2;
    }

    auto loaded = kbsocket::raw::UrmaApi::Load(FLAGS_library.c_str());
    if (!loaded) {
        std::println(stderr, "URMA load failed: {}", loaded.error().message);
        return 1;
    }
    struct LibraryScope {
        ~LibraryScope() {
            const auto result = kbsocket::raw::UrmaApi::Unload();
            if (!result) {
                std::println(stderr, "URMA unload failed: {}", result.error().message);
            }
        }
    } library_scope;

    urma_init_attr_t attr{};
    if (kbsocket::raw::UrmaApi::Init(&attr) != URMA_SUCCESS) {
        std::println(stderr, "urma_init failed");
        return 1;
    }

    UrmaSession session;
    try {
        kbsocket::raw::DeviceCatalog catalog;
        std::vector<std::string> names(argv + 1, argv + argc);
        auto status = catalog.Initialize(names);
        if (!status) {
            std::println(stderr, "Discovery failed: code={} provider_error={}", static_cast<int>(status.error().code),
                         status.error().provider_error);
            return 1;
        }

        for (const auto& device : catalog.devices()) {
            PrintDeviceInfo(device);
            for (const auto& item : device.eids) {
                std::print("  eid_index={} eid=", item.eid_index);
                for (auto byte : item.eid.raw) {
                    std::print("{:02x}", static_cast<unsigned>(byte));
                }
                std::println();
            }
        }
    } catch (const std::exception& error) {
        std::println(stderr, "Discovery failed: {}", error.what());
        return 1;
    }

    return 0;
}
