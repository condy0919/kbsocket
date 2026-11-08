// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_runtime.h"

#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <new>
#include <optional>

#include "kbsocket/api/stream_binding.hpp"
#include "kbsocket/api/stream_error.hpp"
#include "kbsocket/runtime/connection_owner.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/runtime_shard.hpp"
#include "kbsocket/transport/tcp/native_tcp_backend.hpp"

namespace {
using kbsocket::api::MapStreamError;
using kbsocket::api::StreamError;
auto Error(int code) noexcept {
    return std::unexpected(StreamError(code));
}
int32_t Report(kbs_stream_error result, kbs_stream_error* error) noexcept {
    if (error) {
        *error = result;
    }
    return result.code;
}
int32_t Invalid(kbs_stream_error* error) noexcept {
    return Report(StreamError(KBS_STREAM_INVALID_ARGUMENT), error);
}
int32_t Finish(const std::expected<void, kbs_stream_error>& result, kbs_stream_error* error) noexcept {
    return Report(result ? kbs_stream_error{} : result.error(), error);
}
bool ValidConfig(const kbs_runtime_config& config) noexcept {
    return config.abi_version == KBS_STREAM_ABI_VERSION && config.struct_size == sizeof(config) && config.table_id &&
           config.stream_capacity && config.stream_capacity <= kbsocket::runtime::ConnectionProgress::kMaxConnections &&
           config.listener_capacity <= 65536 && config.max_io_bytes && config.max_io_bytes <= 1024u * 1024u &&
           config.events <= 1 && config.progress_budget && config.progress_budget <= 64 && config.max_wait_ms <= 1000;
}
} // namespace

// 成员按依赖顺序声明，创建失败时先关 Reactor，再销毁绑定/owner/scheduler。
// 成功实例仅在显式 Close 后 Destroy，不通过析构猜测仍存活的应用责任。
struct kbs_runtime {
    struct Listener {
        std::optional<kbsocket::tcp::NativeTcpListener> socket;
        std::uint64_t generation = 0;
    };
    kbsocket::runtime::ConnectionProgress progress;
    kbsocket::runtime::ConnectionOwner owner;
    kbsocket::api::StreamBinding binding;
    kbsocket::runtime::RuntimeShard shard;
    kbs_runtime_config config{};
    std::unique_ptr<Listener[]> listeners;
    std::unique_ptr<std::uint32_t[]> free_listeners;
    std::size_t listener_count = 0;
    std::uint64_t listener_generation = 0;
    // 非正常发布/回滚失败后停准入，保留唯一未发布槽供 Close 重试，不向应用隐藏 fd 所有权。
    std::optional<kbsocket::runtime::OwnedConnection> unpublished;
    std::optional<kbs_stream_error> callback_error;
    bool accepting = true, closed = false;

    static void Observe(void* cookie, const kbsocket::runtime::ConnectionProgressEvent& event) noexcept {
        auto& self = *static_cast<kbs_runtime*>(cookie);
        const auto recorded = self.binding.RecordProgress(event);
        if (!recorded && !self.callback_error) {
            self.callback_error = recorded.error();
        }
    }
    std::expected<void, kbs_stream_error> Open(const kbs_runtime_config& input) noexcept {
        config = input;
        try {
            listeners = std::make_unique<Listener[]>(config.listener_capacity);
            free_listeners = std::make_unique<std::uint32_t[]>(config.listener_capacity);
        } catch (const std::bad_alloc&) {
            return Error(KBS_STREAM_NO_MEMORY);
        }
        for (std::uint32_t i = 0; i < config.listener_capacity; ++i) {
            free_listeners[i] = config.listener_capacity - 1 - i;
        }
        if (const auto result = progress.Open({.capacity = config.stream_capacity}, &Observe, this); !result) {
            return std::unexpected(MapStreamError(result.error()));
        }
        if (const auto result = owner.Open(progress, {.capacity = config.stream_capacity}); !result) {
            return std::unexpected(MapStreamError(result.error()));
        }
        if (const auto result = binding.OpenTcp(owner, {config.table_id, config.stream_capacity, config.max_io_bytes});
            !result) {
            return result;
        }
        const kbsocket::runtime::RuntimeShardConfig shard_config{
            .reactor = {.events = config.events != 0,
                        .budget = config.progress_budget,
                        .busy_poll_ns = 0,
                        .max_wait_ms = static_cast<int>(config.max_wait_ms)},
            .control = {}};
        if (const auto result = shard.Open({}, shard_config, &progress); !result) {
            return std::unexpected(MapStreamError(result.error()));
        }
        return {};
    }
    std::expected<void, kbs_stream_error> Admit(bool stream) noexcept {
        if (closed) {
            return Error(KBS_STREAM_NOT_READY);
        }
        if (!accepting) {
            return Error(KBS_STREAM_STOPPED);
        }
        if (callback_error) {
            return std::unexpected(*callback_error);
        }
        if (unpublished) {
            return Error(KBS_STREAM_RETAINED);
        }
        if (stream &&
            (binding.published() >= config.stream_capacity || owner.stats().occupied >= config.stream_capacity)) {
            return Error(KBS_STREAM_EXHAUSTED);
        }
        return {};
    }
    std::expected<void, kbs_stream_error> CleanUnpublished() noexcept {
        if (!unpublished) {
            return {};
        }
        if (const auto result = owner.CloseConnection(*unpublished); !result) {
            auto error = MapStreamError(result.error());
            error.code = KBS_STREAM_RETAINED;
            return std::unexpected(error);
        }
        if (const auto result = owner.Release(*unpublished); !result) {
            auto error = MapStreamError(result.error());
            error.code = KBS_STREAM_RETAINED;
            return std::unexpected(error);
        }
        unpublished.reset();
        return {};
    }
    std::expected<kbs_stream_handle, kbs_stream_error>
    Publish(const std::expected<kbsocket::runtime::OwnedConnection, kbsocket::runtime::ConnectionOwnerError>&
                adopted) noexcept {
        if (!adopted) {
            if (adopted.error().retained_handle) {
                unpublished = adopted.error().retained_handle;
                if (const auto cleanup = CleanUnpublished(); !cleanup) {
                    return std::unexpected(cleanup.error());
                }
            }
            return std::unexpected(MapStreamError(adopted.error()));
        }
        const auto published = binding.Publish(*adopted);
        if (!published) {
            unpublished = *adopted;
            if (const auto cleanup = CleanUnpublished(); !cleanup) {
                return std::unexpected(cleanup.error());
            }
        }
        return published;
    }
    std::expected<Listener*, kbs_stream_error> Find(kbs_listener_handle handle) noexcept {
        if (closed) {
            return Error(KBS_STREAM_NOT_READY);
        }
        if (handle.runtime_id != config.table_id || !handle.generation || handle.reserved ||
            handle.slot >= config.listener_capacity) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        auto& slot = listeners[handle.slot];
        if (!slot.socket || slot.generation != handle.generation) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        return &slot;
    }
    std::expected<void, kbs_stream_error> Close() noexcept {
        if (closed) {
            return {};
        }
        accepting = false;
        binding.StopAccepting();
        owner.StopAccepting();
        if (const auto cleanup = CleanUnpublished(); !cleanup) {
            return cleanup;
        }
        if (listener_count || binding.published() || owner.stats().occupied) {
            return Error(KBS_STREAM_BUSY);
        }
        if (const auto result = shard.Close(); !result) {
            return std::unexpected(MapStreamError(result.error()));
        }
        if (const auto result = binding.Close(); !result) {
            return result;
        }
        if (const auto result = owner.Close(); !result) {
            return std::unexpected(MapStreamError(result.error()));
        }
        if (const auto result = progress.Close(); !result) {
            return std::unexpected(MapStreamError(result.error()));
        }
        closed = true;
        return {};
    }
};

extern "C" {
int32_t kbs_runtime_config_init(kbs_runtime_config* config, kbs_stream_error* error) {
    if (!config) {
        return Invalid(error);
    }
    *config = {KBS_STREAM_ABI_VERSION, sizeof(*config), 0, 256, 16, 64u * 1024u, 1, 64, 10};
    return Report({}, error);
}
int32_t kbs_runtime_create(const kbs_runtime_config* config, kbs_runtime** out, kbs_stream_error* error) {
    if (out) {
        *out = nullptr;
    }
    if (!config || !out || !ValidConfig(*config)) {
        return Invalid(error);
    }
    std::unique_ptr<kbs_runtime> instance(new (std::nothrow) kbs_runtime);
    if (!instance) {
        return Report(StreamError(KBS_STREAM_NO_MEMORY), error);
    }
    if (const auto result = instance->Open(*config); !result) {
        return Report(result.error(), error);
    }
    *out = instance.release();
    return Report({}, error);
}
int32_t kbs_runtime_streams(kbs_runtime* runtime, kbs_stream_table** out, kbs_stream_error* error) {
    if (out) {
        *out = nullptr;
    }
    if (!runtime || !out) {
        return Invalid(error);
    }
    if (runtime->closed) {
        return Report(StreamError(KBS_STREAM_NOT_READY), error);
    }
    *out = runtime->binding.table();
    return Report({}, error);
}
uint64_t kbs_runtime_now_ns(void) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
int32_t kbs_runtime_step(kbs_runtime* runtime, uint32_t wait, kbs_runtime_progress* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!runtime || !out || wait > 1) {
        return Invalid(error);
    }
    if (runtime->closed) {
        return Report(StreamError(KBS_STREAM_NOT_READY), error);
    }
    // 前一次 Tick 或同步 Watch 产生的应用事件不在 reactor 的 runnable 中，必须单独禁止等待。
    const auto result = wait && !runtime->binding.events_pending() ? runtime->shard.RunOnce() : runtime->shard.Tick();
    if (!result) {
        return Report(MapStreamError(result.error()), error);
    }
    if (runtime->callback_error) {
        return Report(*runtime->callback_error, error);
    }
    const bool pending = runtime->binding.events_pending();
    *out = {result->processed, result->runnable || pending ? 1u : 0u, pending ? 1u : 0u};
    return Report({}, error);
}
int32_t kbs_runtime_adopt_tcp(kbs_runtime* runtime, int* fd, kbs_stream_handle* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!runtime || !fd || !out) {
        return Invalid(error);
    }
    if (const auto allowed = runtime->Admit(true); !allowed) {
        return Report(allowed.error(), error);
    }
    const auto adopted = runtime->owner.AdoptTcp(*fd);
    if (adopted) {
        *fd = -1;
    }
    const auto published = runtime->Publish(adopted);
    if (!published) {
        return Report(published.error(), error);
    }
    *out = *published;
    return Report({}, error);
}
int32_t kbs_runtime_connect_tcp(kbs_runtime* runtime, const sockaddr* address, socklen_t length, uint64_t deadline_ns,
                                kbs_stream_handle* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!runtime || !out || !address) {
        return Invalid(error);
    }
    if (const auto allowed = runtime->Admit(true); !allowed) {
        return Report(allowed.error(), error);
    }
    const auto result = runtime->Publish(runtime->owner.ConnectTcp(address, length, kbs_runtime_now_ns(), deadline_ns));
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
int32_t kbs_runtime_listen_tcp(kbs_runtime* runtime, const sockaddr* address, socklen_t length, int32_t backlog,
                               kbs_listener_handle* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!runtime || !address || !out) {
        return Invalid(error);
    }
    if (const auto allowed = runtime->Admit(false); !allowed) {
        return Report(allowed.error(), error);
    }
    if (runtime->listener_count == runtime->config.listener_capacity ||
        runtime->listener_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Report(StreamError(KBS_STREAM_EXHAUSTED), error);
    }
    const auto index = runtime->free_listeners[runtime->config.listener_capacity - runtime->listener_count - 1];
    auto& slot = runtime->listeners[index];
    slot.socket.emplace();
    if (const auto result = slot.socket->Open(address, length, backlog); !result) {
        slot.socket.reset();
        return Report(MapStreamError(result.error()), error);
    }
    slot.generation = ++runtime->listener_generation;
    ++runtime->listener_count;
    *out = {runtime->config.table_id, slot.generation, index, 0};
    return Report({}, error);
}
int32_t kbs_runtime_accept_tcp(kbs_runtime* runtime, kbs_listener_handle listener, kbs_stream_handle* out,
                               kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!runtime || !out) {
        return Invalid(error);
    }
    const auto slot = runtime->Find(listener);
    if (!slot) {
        return Report(slot.error(), error);
    }
    if (const auto allowed = runtime->Admit(true); !allowed) {
        return Report(allowed.error(), error);
    }
    const auto accepted = (*slot)->socket->Accept();
    if (!accepted) {
        return Report(MapStreamError(accepted.error()), error);
    }
    int fd = *accepted;
    const auto result = kbs_runtime_adopt_tcp(runtime, &fd, out, error);
    if (fd >= 0) {
        // Accept 产生的内部 fd 尚未转移给 owner，失败时仍由本入口负责关闭。
        (void)::close(fd);
    }
    return result;
}
int32_t kbs_runtime_listener_address(kbs_runtime* runtime, kbs_listener_handle listener, sockaddr* address,
                                     socklen_t* length, kbs_stream_error* error) {
    if (!runtime || !address || !length) {
        return Invalid(error);
    }
    const auto slot = runtime->Find(listener);
    if (!slot) {
        return Report(slot.error(), error);
    }
    if (getsockname((*slot)->socket->fd(), address, length) < 0) {
        return Report(MapStreamError(kbsocket::tcp::TcpError{kbsocket::tcp::TcpErrorCode::kSystemError, errno}), error);
    }
    return Report({}, error);
}
int32_t kbs_runtime_close_listener(kbs_runtime* runtime, kbs_listener_handle listener, kbs_stream_error* error) {
    if (!runtime) {
        return Invalid(error);
    }
    const auto slot = runtime->Find(listener);
    if (!slot) {
        return Report(slot.error(), error);
    }
    const auto result = (*slot)->socket->Close();
    (*slot)->socket.reset();
    (*slot)->generation = 0;
    runtime->free_listeners[runtime->config.listener_capacity - runtime->listener_count--] = listener.slot;
    return Report(result ? kbs_stream_error{} : MapStreamError(result.error()), error);
}
int32_t kbs_runtime_close(kbs_runtime* runtime, kbs_stream_error* error) {
    return runtime ? Finish(runtime->Close(), error) : Invalid(error);
}
int32_t kbs_runtime_destroy(kbs_runtime** runtime, kbs_stream_error* error) {
    if (!runtime) {
        return Invalid(error);
    }
    if (*runtime && !(*runtime)->closed) {
        return Report(StreamError(KBS_STREAM_BUSY), error);
    }
    delete *runtime;
    *runtime = nullptr;
    return Report({}, error);
}
}
