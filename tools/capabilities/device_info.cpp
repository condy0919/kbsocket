#include <cstring>
#include <exception>
#include <print>
#include <string>
#include <vector>

#include "kbsocket/transport/raw/device_catalog.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace {
struct UrmaSession {
    ~UrmaSession() {
        kbsocket::raw::UrmaApi::Uninit();
    }
};
} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::println(stderr, "Usage: {} [--library PATH] RAW_DEVICE [RAW_DEVICE ...]", argv[0]);
        return 2;
    }
    int first_device = 1;
    const char* library = "liburma.so";
    if (std::strcmp(argv[1], "--library") == 0) {
        if (argc < 4) {
            std::println(stderr, "Expected --library PATH RAW_DEVICE [RAW_DEVICE ...]");
            return 2;
        }
        library = argv[2];
        first_device = 3;
    }
    auto loaded = kbsocket::raw::UrmaApi::Load(library);
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
        std::vector<std::string> names(argv + first_device, argv + argc);
        auto status = catalog.Initialize(names);
        if (!status) {
            std::println(stderr, "Discovery failed: code={} provider_error={}", static_cast<int>(status.error().code),
                         status.error().provider_error);
            return 1;
        }
        for (const auto& device : catalog.devices()) {
            const auto& cap = device.attributes.dev_cap;
            std::println("device={} rm_ctp=reported jfs_depth={} jfr_depth={} "
                         "jfc_depth={} inline={} sge={}",
                         device.name, cap.max_jfs_depth, cap.max_jfr_depth, cap.max_jfc_depth, cap.max_jfs_inline_len,
                         cap.max_jfs_sge);
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
