// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/urma_api.hpp"

#include <dlfcn.h>

#include <cstddef>
#include <cstdio>
#include <iterator>
#include <memory>
#include <string_view>
#include <type_traits>

namespace kbsocket {
namespace raw {
namespace {
template <typename Function>
std::expected<void, LoadError> Resolve(void* handle, const char* name, Function& function) noexcept {
    void* symbol = ::dlsym(handle, name);
    if (!symbol) {
        LoadError failure{
            .code = LoadErrorCode::kSymbolMissing,
        };
        const char* err = ::dlerror();
        std::snprintf(failure.message, sizeof(failure.message), "%s: %s", name, err ? err : "null symbol address");
        return std::unexpected(failure);
    }
    function = reinterpret_cast<Function>(symbol);
    return {};
}

template <auto Member>
std::expected<void, LoadError> ResolveMember(void* handle, const char* name, UrmaFunctions& functions) noexcept {
    return Resolve(handle, name, functions.*Member);
}

struct SymbolBinding {
    const char* name;
    std::expected<void, LoadError> (*resolve)(void*, const char*, UrmaFunctions&) noexcept;
};

// 成员指针保留各 URMA 函数的签名，统一入口供 `UrmaApi::Load()` 循环调用。
constexpr SymbolBinding kSymbolBindings[] = {
    {
        .name = "urma_init",
        .resolve = ResolveMember<&UrmaFunctions::init>,
    },
    {
        .name = "urma_uninit",
        .resolve = ResolveMember<&UrmaFunctions::uninit>,
    },
    {
        .name = "urma_get_device_list",
        .resolve = ResolveMember<&UrmaFunctions::get_devices>,
    },
    {
        .name = "urma_free_device_list",
        .resolve = ResolveMember<&UrmaFunctions::free_devices>,
    },
    {
        .name = "urma_get_eid_list",
        .resolve = ResolveMember<&UrmaFunctions::get_eids>,
    },
    {
        .name = "urma_free_eid_list",
        .resolve = ResolveMember<&UrmaFunctions::free_eids>,
    },
    {
        .name = "urma_query_device",
        .resolve = ResolveMember<&UrmaFunctions::query_device>,
    },
    {
        .name = "urma_create_context",
        .resolve = ResolveMember<&UrmaFunctions::create_context>,
    },
    {
        .name = "urma_delete_context",
        .resolve = ResolveMember<&UrmaFunctions::delete_context>,
    },
    {
        .name = "urma_create_jfc",
        .resolve = ResolveMember<&UrmaFunctions::create_jfc>,
    },
    {
        .name = "urma_delete_jfc",
        .resolve = ResolveMember<&UrmaFunctions::delete_jfc>,
    },
    {
        .name = "urma_rearm_jfc",
        .resolve = ResolveMember<&UrmaFunctions::rearm_jfc>,
    },
    {
        .name = "urma_poll_jfc",
        .resolve = ResolveMember<&UrmaFunctions::poll_jfc>,
    },
    {
        .name = "urma_wait_jfc",
        .resolve = ResolveMember<&UrmaFunctions::wait_jfc>,
    },
    {
        .name = "urma_ack_jfc",
        .resolve = ResolveMember<&UrmaFunctions::ack_jfc>,
    },
    {
        .name = "urma_create_jfce",
        .resolve = ResolveMember<&UrmaFunctions::create_jfce>,
    },
    {
        .name = "urma_delete_jfce",
        .resolve = ResolveMember<&UrmaFunctions::delete_jfce>,
    },
    {
        .name = "urma_create_jfr",
        .resolve = ResolveMember<&UrmaFunctions::create_jfr>,
    },
    {
        .name = "urma_delete_jfr",
        .resolve = ResolveMember<&UrmaFunctions::delete_jfr>,
    },
    {
        .name = "urma_modify_jfr",
        .resolve = ResolveMember<&UrmaFunctions::modify_jfr>,
    },
    {
        .name = "urma_create_jetty",
        .resolve = ResolveMember<&UrmaFunctions::create_jetty>,
    },
    {
        .name = "urma_delete_jetty",
        .resolve = ResolveMember<&UrmaFunctions::delete_jetty>,
    },
    {
        .name = "urma_modify_jetty",
        .resolve = ResolveMember<&UrmaFunctions::modify_jetty>,
    },
    {
        .name = "urma_bind_jetty",
        .resolve = ResolveMember<&UrmaFunctions::bind_jetty>,
    },
    {
        .name = "urma_unbind_jetty",
        .resolve = ResolveMember<&UrmaFunctions::unbind_jetty>,
    },
    {
        .name = "urma_import_jetty",
        .resolve = ResolveMember<&UrmaFunctions::import_jetty>,
    },
    {
        .name = "urma_unimport_jetty",
        .resolve = ResolveMember<&UrmaFunctions::unimport_jetty>,
    },
    {
        .name = "urma_flush_jetty",
        .resolve = ResolveMember<&UrmaFunctions::flush_jetty>,
    },
    {
        .name = "urma_post_jetty_send_wr",
        .resolve = ResolveMember<&UrmaFunctions::post_jetty_send_wr>,
    },
    {
        .name = "urma_post_jetty_recv_wr",
        .resolve = ResolveMember<&UrmaFunctions::post_jetty_recv_wr>,
    },
    {
        .name = "urma_post_jfr_wr",
        .resolve = ResolveMember<&UrmaFunctions::post_jfr_wr>,
    },
    {
        .name = "urma_get_rjetty",
        .resolve = ResolveMember<&UrmaFunctions::get_rjetty>,
    },
    {
        .name = "urma_put_rjetty",
        .resolve = ResolveMember<&UrmaFunctions::put_rjetty>,
    },
    {
        .name = "urma_register_seg",
        .resolve = ResolveMember<&UrmaFunctions::register_seg>,
    },
    {
        .name = "urma_unregister_seg",
        .resolve = ResolveMember<&UrmaFunctions::unregister_seg>,
    },
    {
        .name = "urma_import_seg",
        .resolve = ResolveMember<&UrmaFunctions::import_seg>,
    },
    {
        .name = "urma_unimport_seg",
        .resolve = ResolveMember<&UrmaFunctions::unimport_seg>,
    },
    {
        .name = "urma_get_seg_ctx",
        .resolve = ResolveMember<&UrmaFunctions::get_seg_ctx>,
    },
    {
        .name = "urma_put_seg_ctx",
        .resolve = ResolveMember<&UrmaFunctions::put_seg_ctx>,
    },
    {
        .name = "urma_get_async_event",
        .resolve = ResolveMember<&UrmaFunctions::get_async_event>,
    },
    {
        .name = "urma_ack_async_event",
        .resolve = ResolveMember<&UrmaFunctions::ack_async_event>,
    },
    {
        .name = "urma_log_set_level",
        .resolve = ResolveMember<&UrmaFunctions::log_set_level>,
    },
    {
        .name = "urma_register_log_func",
        .resolve = ResolveMember<&UrmaFunctions::register_log_func>,
    },
    {
        .name = "urma_register_loc_log_func",
        .resolve = ResolveMember<&UrmaFunctions::register_loc_log_func>,
    },
    {
        .name = "urma_unregister_log_func",
        .resolve = ResolveMember<&UrmaFunctions::unregister_log_func>,
    },
    {
        .name = "urma_user_ctl",
        .resolve = ResolveMember<&UrmaFunctions::user_ctl>,
    },
    {
        .name = "urma_str_to_eid",
        .resolve = ResolveMember<&UrmaFunctions::str_to_eid>,
    },
    {
        .name = "urma_start_perf",
        .resolve = ResolveMember<&UrmaFunctions::start_perf>,
    },
    {
        .name = "urma_stop_perf",
        .resolve = ResolveMember<&UrmaFunctions::stop_perf>,
    },
    {
        .name = "urma_get_perf_info",
        .resolve = ResolveMember<&UrmaFunctions::get_perf_info>,
    },
};

consteval bool HasUniqueBindings() {
    for (std::size_t i = 0; i < std::size(kSymbolBindings); ++i) {
        for (std::size_t j = i + 1; j < std::size(kSymbolBindings); ++j) {
            if (kSymbolBindings[i].resolve == kSymbolBindings[j].resolve ||
                std::string_view(kSymbolBindings[i].name) == kSymbolBindings[j].name) {
                return false;
            }
        }
    }
    return true;
}
static_assert(HasUniqueBindings(), "Symbol bindings must not repeat members or symbol names");

} // namespace

// 仅含平凡类型，无退出期析构；dl handle 只能通过显式 `Unload()` 关闭。
constinit void* UrmaApi::handle_ = nullptr;
constinit UrmaFunctions UrmaApi::functions_ = {};
constinit unsigned UrmaApi::override_depth_ = 0;

std::expected<void, LoadError> UrmaApi::Load(const char* path) noexcept {
    if (override_depth_) {
        return std::unexpected(LoadError{
            .code = LoadErrorCode::kInUse,
            .message = "test override is active",
        });
    }

    if (handle_) {
        return std::unexpected(LoadError{
            .code = LoadErrorCode::kAlreadyLoaded,
            .message = "library already loaded",
        });
    }

    if (!path || *path == '\0') {
        return std::unexpected(LoadError{
            .code = LoadErrorCode::kInvalidArgument,
            .message = "empty library path",
        });
    }

    void* handle = ::dlopen(path, RTLD_NOW | RTLD_GLOBAL);
    if (!handle) {
        LoadError failure{
            .code = LoadErrorCode::kOpenFailed,
            .message = "dlopen failed",
        };
        if (const char* err = ::dlerror()) {
            std::snprintf(failure.message, sizeof(failure.message), "%s", err);
        }
        return std::unexpected(failure);
    }

    struct LibraryCloser {
        void operator()(void* handle) const noexcept {
            ::dlclose(handle);
        }
    };

    std::unique_ptr<void, LibraryCloser> pending_handle(handle);
    UrmaFunctions pending;
    for (const auto& [name, resolve] : kSymbolBindings) {
        if (auto result = resolve(handle, name, pending); !result) {
            return result;
        }
    }

    functions_ = pending;
    handle_ = pending_handle.release();
    return {};
}

std::expected<void, LoadError> UrmaApi::Unload() noexcept {
    if (override_depth_) {
        return std::unexpected(LoadError{
            .code = LoadErrorCode::kInUse,
            .message = "test override is active",
        });
    }

    functions_ = {};
    if (handle_) {
        ::dlclose(handle_);
        handle_ = nullptr;
    }
    return {};
}

} // namespace raw
} // namespace kbsocket
