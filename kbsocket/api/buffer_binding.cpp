// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/buffer_binding.hpp"

#include <limits>
#include <new>
#include <utility>

#include "kbsocket/api/stream_error.hpp"

struct kbs_buffer_table {
    kbsocket::api::BufferBinding* binding;
};

namespace kbsocket {
namespace api {
namespace {
constexpr auto kEnd = std::numeric_limits<std::uint32_t>::max();
auto Error(int code) noexcept {
    return std::unexpected(StreamError(code));
}
kbs_stream_error MemoryError(const memory::MemoryError& error) noexcept {
    using Code = memory::MemoryErrorCode;
    int code = KBS_STREAM_TRANSPORT;
    switch (error.code) {
    case Code::kInvalidArgument:
        code = KBS_STREAM_INVALID_ARGUMENT;
        break;
    case Code::kNotReady:
        code = KBS_STREAM_NOT_READY;
        break;
    case Code::kInUse:
    case Code::kNotQuiescent:
        code = KBS_STREAM_BUSY;
        break;
    case Code::kStopped:
        code = KBS_STREAM_STOPPED;
        break;
    case Code::kExhausted:
    case Code::kSequenceExhausted:
    case Code::kQueueFull:
        code = KBS_STREAM_EXHAUSTED;
        break;
    case Code::kNoMemory:
        code = KBS_STREAM_NO_MEMORY;
        break;
    case Code::kInvalidLease:
    case Code::kInvalidReference:
        code = KBS_STREAM_INVALID_HANDLE;
        break;
    case Code::kUnregisterFailed:
        code = KBS_STREAM_RETAINED;
        break;
    default:
        break;
    }
    return {code,
            KBS_STREAM_ORIGIN_MEMORY,
            static_cast<std::int32_t>(error.code),
            error.provider_error,
            error.cleanup_error.value_or(0),
            0};
}
} // namespace

struct BufferBinding::State {
    struct Slot {
        memory::BufferLease lease;
        std::uint64_t generation = 0;
        std::size_t bytes = 0;
        std::uint32_t next = kEnd;
    };
    struct Host {
        explicit Host(std::size_t bytes) noexcept : budget(bytes) {}
        memory::MemoryBudget budget;
        memory::SlabAllocator slab;
    };
    kbs_buffer_table table;
    BufferBindingConfig config;
    std::unique_ptr<Host> host;
    memory::SlabAllocator* slab = nullptr;
    runtime::RawStack* stack = nullptr;
    std::unique_ptr<Slot[]> slots;
    std::uint64_t next_generation = 0;
    std::size_t published = 0;
    std::uint32_t free = kEnd;
    bool accepting = true, closed = false;

    std::expected<memory::SlabAllocator*, kbs_stream_error> Access() noexcept {
        if (closed) {
            return Error(KBS_STREAM_NOT_READY);
        }
        if (stack) {
            // 共享 Slab 属于全部 context，不能缓存指针后绕过任一控制任务的暂停窗口。
            const auto shared = stack->shared();
            if (!shared) {
                return std::unexpected(MapStreamError(shared.error()));
            }
            if (shared->slab != slab) {
                return Error(KBS_STREAM_INVALID_HANDLE);
            }
        }
        return slab;
    }
    std::expected<Slot*, kbs_stream_error> Find(kbs_buffer_handle handle) noexcept {
        if (handle.table_id != config.table_id || !handle.generation || handle.reserved ||
            handle.slot >= config.capacity) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        auto& slot = slots[handle.slot];
        if (!slot.generation || slot.generation != handle.generation) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        return &slot;
    }
};

BufferBinding::BufferBinding() noexcept = default;
BufferBinding::~BufferBinding() {
    if (state_ && !state_->closed) {
        // 不在析构中触碰借用 raw 状态；公开视图或外部责任可能仍存在，保留整份依赖。
        (void)state_.release();
    }
}
std::expected<void, kbs_stream_error> BufferBinding::OpenHost(const kbs_buffer_pool_config& config) noexcept {
    if (config.abi_version != KBS_BUFFER_ABI_VERSION || config.reserved || config.reserved2 || !config.buffer_bytes ||
        config.buffer_bytes > std::numeric_limits<std::size_t>::max() || !config.memory_budget_bytes ||
        config.memory_budget_bytes > std::numeric_limits<std::size_t>::max()) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    return Open(nullptr, nullptr, {config.table_id, config.capacity}, &config);
}
std::expected<void, kbs_stream_error> BufferBinding::OpenSlab(memory::SlabAllocator& slab,
                                                              const BufferBindingConfig& config) noexcept {
    return Open(&slab, nullptr, config, nullptr);
}
std::expected<void, kbs_stream_error> BufferBinding::OpenRaw(runtime::RawStack& stack,
                                                             const BufferBindingConfig& config) noexcept {
    const auto shared = stack.shared();
    if (!shared) {
        return std::unexpected(MapStreamError(shared.error()));
    }
    return Open(shared->slab, &stack, config, nullptr);
}
std::expected<void, kbs_stream_error> BufferBinding::Open(memory::SlabAllocator* slab, runtime::RawStack* stack,
                                                          const BufferBindingConfig& config,
                                                          const kbs_buffer_pool_config* host) noexcept {
    if (opened_once_) {
        return Error(KBS_STREAM_BUSY);
    }
    if (!config.table_id || !config.capacity || config.capacity > memory::SlabAllocator::kMaxBuffers) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    if (slab && !slab->accepting()) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(KBS_STREAM_NO_MEMORY);
    }
    state->slots.reset(new (std::nothrow) State::Slot[config.capacity]);
    if (!state->slots) {
        return Error(KBS_STREAM_NO_MEMORY);
    }
    if (host) {
        state->host.reset(new (std::nothrow) State::Host(static_cast<std::size_t>(host->memory_budget_bytes)));
        if (!state->host) {
            return Error(KBS_STREAM_NO_MEMORY);
        }
        const memory::SlabConfig slab_config{.buffer_count = config.capacity,
                                             .buffer_size = static_cast<std::size_t>(host->buffer_bytes),
                                             .max_references = config.capacity};
        const auto opened = state->host->slab.OpenHost(slab_config, state->host->budget);
        if (!opened) {
            return std::unexpected(MemoryError(opened.error()));
        }
        slab = &state->host->slab;
    }
    state->slab = slab;
    state->stack = stack;
    state->config = config;
    state->table.binding = this;
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->slots[i].next = i + 1 == config.capacity ? kEnd : i + 1;
    }
    state->free = 0;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
kbs_buffer_table* BufferBinding::table() noexcept {
    return state_ ? &state_->table : nullptr;
}
std::size_t BufferBinding::published() const noexcept {
    return state_ ? state_->published : 0;
}
void BufferBinding::StopAccepting() noexcept {
    if (state_) {
        state_->accepting = false;
    }
}
std::expected<void, kbs_stream_error> BufferBinding::Close() noexcept {
    if (!state_ || state_->closed) {
        return {};
    }
    // 暂停拒绝不修改准入；不停止共享 Slab，也不代替宿主释放 CPU/DMA/Grant。
    if (const auto access = state_->Access(); !access) {
        return std::unexpected(access.error());
    }
    StopAccepting();
    if (state_->published) {
        return Error(KBS_STREAM_BUSY);
    }
    if (state_->host) {
        if (const auto closed = state_->host->slab.Close(); !closed) {
            return std::unexpected(MemoryError(closed.error()));
        }
    }
    state_->closed = true;
    return {};
}
std::expected<kbs_buffer_handle, kbs_stream_error> BufferBinding::Acquire(std::size_t bytes) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& state = *state_;
    const auto access = state.Access();
    if (!access) {
        return std::unexpected(access.error());
    }
    if (!state.accepting) {
        return Error(KBS_STREAM_STOPPED);
    }
    if (!bytes) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    // 先预检发布槽；共享池借出成功后不再有可失败的发布步骤。
    if (state.free == kEnd || state.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(KBS_STREAM_EXHAUSTED);
    }
    const auto lease = (*access)->Acquire(bytes);
    if (!lease) {
        return std::unexpected(MemoryError(lease.error()));
    }
    const auto index = state.free;
    auto& slot = state.slots[index];
    state.free = slot.next;
    slot.lease = *lease;
    slot.generation = ++state.next_generation;
    slot.bytes = bytes;
    slot.next = kEnd;
    ++state.published;
    return kbs_buffer_handle{state.config.table_id, slot.generation, index, 0};
}
std::expected<std::span<std::byte>, kbs_stream_error> BufferBinding::Writable(kbs_buffer_handle buffer) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto access = state_->Access();
    if (!access) {
        return std::unexpected(access.error());
    }
    const auto slot = state_->Find(buffer);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    if (!state_->accepting) {
        return Error(KBS_STREAM_STOPPED);
    }
    const auto view = (*access)->Writable((*slot)->lease);
    if (!view) {
        return std::unexpected(MemoryError(view.error()));
    }
    return *view;
}
std::expected<std::span<const std::byte>, kbs_stream_error> BufferBinding::Readable(kbs_buffer_handle buffer) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto access = state_->Access();
    if (!access) {
        return std::unexpected(access.error());
    }
    const auto slot = state_->Find(buffer);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto view = (*access)->Readable((*slot)->lease);
    if (!view) {
        return std::unexpected(MemoryError(view.error()));
    }
    return *view;
}
std::expected<memory::BufferLease, kbs_stream_error> BufferBinding::Resolve(kbs_buffer_handle buffer) noexcept {
    const auto view = Readable(buffer);
    if (!view) {
        return std::unexpected(view.error());
    }
    if (!state_->accepting || !state_->slab->accepting()) {
        return Error(KBS_STREAM_STOPPED);
    }
    return state_->slots[buffer.slot].lease;
}
std::expected<memory::BufferLease, kbs_stream_error> BufferBinding::Resolve(kbs_buffer_table* table,
                                                                            kbs_buffer_handle buffer,
                                                                            std::size_t offset,
                                                                            std::size_t bytes) noexcept {
    if (!table || !bytes) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    const auto view = table->binding->Readable(buffer);
    if (!view) {
        return std::unexpected(view.error());
    }
    if (offset > view->size() || bytes > view->size() - offset) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    return table->binding->Resolve(buffer);
}
std::expected<kbs_buffer_status, kbs_stream_error> BufferBinding::Inspect(kbs_buffer_handle buffer) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto access = state_->Access();
    if (!access) {
        return std::unexpected(access.error());
    }
    const auto slot = state_->Find(buffer);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto view = (*access)->Readable((*slot)->lease);
    if (!view && view.error().code != memory::MemoryErrorCode::kInUse) {
        return std::unexpected(MemoryError(view.error()));
    }
    // 只在通过暂停/身份门禁后，把 Slab 的责任/隔离限制映射为“尚不可复用”。
    // 停准入不改变 DMA 事实；可写访问还须单独通过 Writable 的准入检查。
    return kbs_buffer_status{(*slot)->bytes, view ? 1u : 0u, 0};
}
std::expected<void, kbs_stream_error> BufferBinding::Release(kbs_buffer_handle buffer) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& state = *state_;
    const auto access = state.Access();
    if (!access) {
        return std::unexpected(access.error());
    }
    const auto slot = state.Find(buffer);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto released = (*access)->ReleaseCpu((*slot)->lease);
    if (!released) {
        return std::unexpected(MemoryError(released.error()));
    }
    **slot = {.lease = {}, .next = state.free};
    state.free = buffer.slot;
    --state.published;
    return {};
}
} // namespace api
} // namespace kbsocket

namespace {
int32_t Report(kbs_stream_error value, kbs_stream_error* out) noexcept {
    if (out) {
        *out = value;
    }
    return value.code;
}
int32_t Invalid(kbs_stream_error* error) noexcept {
    return Report(kbsocket::api::StreamError(KBS_STREAM_INVALID_ARGUMENT), error);
}
} // namespace
extern "C" {
int32_t kbs_buffer_acquire(kbs_buffer_table* table, size_t bytes, kbs_buffer_handle* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out) {
        return Invalid(error);
    }
    const auto result = table->binding->Acquire(bytes);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
int32_t kbs_buffer_writable(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_buffer_view* out,
                            kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out) {
        return Invalid(error);
    }
    const auto result = table->binding->Writable(buffer);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = {result->data(), result->size()};
    return Report({}, error);
}
int32_t kbs_buffer_readable(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_buffer_const_view* out,
                            kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out) {
        return Invalid(error);
    }
    const auto result = table->binding->Readable(buffer);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = {result->data(), result->size()};
    return Report({}, error);
}
int32_t kbs_buffer_release(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_stream_error* error) {
    if (!table) {
        return Invalid(error);
    }
    const auto result = table->binding->Release(buffer);
    return Report(result ? kbs_stream_error{} : result.error(), error);
}
int32_t kbs_buffer_inspect(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_buffer_status* out,
                           kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out) {
        return Invalid(error);
    }
    const auto result = table->binding->Inspect(buffer);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
}
