// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_binding.hpp"

#include <sys/epoll.h>

#include <algorithm>
#include <limits>
#include <new>

#include "kbsocket/api/buffer_binding.hpp"
#include "kbsocket/api/stream_error.hpp"

// 不把 C++ 对象布局暴露给 C；指针有效期为绑定对象存活期，Close 后仍能拒绝旧调用。
struct kbs_stream_table {
    kbsocket::api::StreamBinding* binding = nullptr;
    std::size_t max_io_bytes = 0;
};

namespace kbsocket {
namespace api {
namespace {
auto Error(int code) noexcept {
    return std::unexpected(StreamError(code));
}
constexpr auto kUnused = std::numeric_limits<std::uint32_t>::max();
bool SameError(const kbs_stream_error& left, const kbs_stream_error& right) noexcept {
    return left.code == right.code && left.origin == right.origin && left.native_code == right.native_code &&
           left.detail == right.detail && left.underlying == right.underlying;
}
std::expected<void, kbs_stream_error> Active(const runtime::OwnedConnectionStatus& status) noexcept {
    using State = connection::ConnectionState;
    if (status.state == State::kClosed || status.state == State::kClosing) {
        return Error(KBS_STREAM_CLOSED);
    }
    if (status.failure) {
        return std::unexpected(MapStreamError(*status.failure));
    }
    if (status.state != State::kRawActive && status.state != State::kTcpActive) {
        return Error(KBS_STREAM_NOT_READY);
    }
    return {};
}
} // namespace
struct StreamBinding::State {
    struct Slot {
        runtime::OwnedConnection connection;
        std::uint64_t generation = 0;
        std::uint64_t receive_generation = 0, core_receive_generation = 0;
        runtime::ScheduledConnection scheduled;
        kbs_stream_event event{};
        std::uint32_t previous = kUnused, next = kUnused;
        bool pending = false, snapshot_known = false;
        bool watch_read = false, watch_write = false;
    };
    kbs_stream_table table;
    runtime::ConnectionOwner* owner = nullptr;
    runtime::RawStack* stack = nullptr;
    std::size_t context = 0;
    StreamBindingConfig config;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<std::uint32_t[]> free;
    // 按原 owner 槽定位发布，避免 40k 连接时每次 Publish 扫全表。
    std::unique_ptr<std::uint32_t[]> owner_slots;
    std::unique_ptr<std::uint32_t[]> scheduled_slots;
    std::uint32_t scheduled_capacity = 0;
    std::uint32_t pending_head = kUnused, pending_tail = kUnused;
    std::size_t count = 0;
    std::uint64_t next_generation = 0;
    // 整张表共享公开代次，防止不同流的首个内部 token 相同时被移植归还。
    std::uint64_t next_receive_generation = 0;
    bool accepting = true, closed = false;

    std::expected<void, kbs_stream_error> Gate() noexcept {
        if (closed) {
            return Error(KBS_STREAM_NOT_READY);
        }
        if (stack) {
            const auto resources = stack->resources(context);
            if (!resources) {
                return std::unexpected(MapStreamError(resources.error()));
            }
            if (resources->connections != owner) {
                return Error(KBS_STREAM_NOT_READY);
            }
        } else if (!owner->stats().capacity) {
            return Error(KBS_STREAM_NOT_READY);
        }
        return {};
    }
    std::expected<Slot*, kbs_stream_error> Find(kbs_stream_handle handle) noexcept {
        if (const auto ready = Gate(); !ready) {
            return std::unexpected(ready.error());
        }
        if (handle.table_id != config.table_id || handle.reserved || !handle.generation ||
            handle.slot >= config.capacity) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        auto& slot = slots[handle.slot];
        if (!slot.generation || slot.generation != handle.generation) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        return &slot;
    }
    std::expected<runtime::OwnedConnectionStatus, kbs_stream_error> Inspect(Slot& slot) noexcept {
        const auto status = owner->Inspect(slot.connection);
        if (!status) {
            return std::unexpected(MapStreamError(status.error()));
        }
        return *status;
    }
    // 单槽一个 intrusive 节点，重复提示只合并；解除发布时 O(1) 移除，避免陈旧 token 占满队列。
    void Enqueue(Slot& slot) noexcept {
        if (slot.pending) {
            return;
        }
        const auto index = static_cast<std::uint32_t>(&slot - slots.get());
        slot.previous = pending_tail;
        slot.next = kUnused;
        if (pending_tail != kUnused) {
            slots[pending_tail].next = index;
        } else {
            pending_head = index;
        }
        pending_tail = index;
        slot.pending = true;
    }
    void Remove(Slot& slot) noexcept {
        if (!slot.pending) {
            return;
        }
        if (slot.previous != kUnused) {
            slots[slot.previous].next = slot.next;
        } else {
            pending_head = slot.next;
        }
        if (slot.next != kUnused) {
            slots[slot.next].previous = slot.previous;
        } else {
            pending_tail = slot.previous;
        }
        slot.previous = slot.next = kUnused;
        slot.pending = false;
        slot.event.flags = 0;
        slot.event.tx_retired = 0;
    }
    void Observe(Slot& slot, const runtime::ConnectionProgressEvent& update) noexcept {
        auto& event = slot.event;
        const auto state = StreamState(update.state);
        const auto failure = update.failure ? MapStreamError(*update.failure) : kbs_stream_error{};
        const bool changed_error = !SameError(event.failure, failure);
        std::uint32_t flags = 0;
        if (!slot.snapshot_known || event.state != state || changed_error ||
            event.read_eof != update.readiness.read_eof || event.write_closed != update.readiness.write_closed) {
            flags |= KBS_STREAM_EVENT_STATE;
            if (state == KBS_STREAM_STATE_CLOSED) {
                flags |= KBS_STREAM_EVENT_CLOSED;
            }
        }
        if (failure.code && (!slot.snapshot_known || changed_error)) {
            flags |= KBS_STREAM_EVENT_ERROR;
        }
        if (update.rx_changed || event.rx_pending != update.rx_pending) {
            flags |= KBS_STREAM_EVENT_RX_CHANGED;
        }
        if (update.tx_retired) {
            flags |= KBS_STREAM_EVENT_TX_RETIRED;
            const auto max = std::numeric_limits<std::uint64_t>::max();
            if (update.tx_retired > max - event.tx_retired) {
                event.tx_retired = max;
                flags |= KBS_STREAM_EVENT_COUNTER_SATURATED;
            } else {
                event.tx_retired += update.tx_retired;
            }
        }
        // TCP RDHUP/HUP 提示应用继续读取尾部，不能提前报告 EOF 或凭 fd 错误猜底层错误码。
        const bool peer_shutdown = update.fd_events & (EPOLLRDHUP | EPOLLHUP);
        const bool fd_error = update.fd_events & EPOLLERR;
        if (peer_shutdown) {
            flags |= KBS_STREAM_EVENT_PEER_SHUTDOWN;
        }
        if (fd_error) {
            flags |= KBS_STREAM_EVENT_ERROR;
        }
        const bool active = state == KBS_STREAM_RAW_ACTIVE || state == KBS_STREAM_TCP_ACTIVE;
        const bool read_ready = active && slot.watch_read &&
                                (update.readiness.readable || update.readiness.read_eof ||
                                 (update.fd_events & EPOLLIN) || peer_shutdown || fd_error);
        const bool write_ready = active && slot.watch_write && !update.readiness.write_closed &&
                                 (update.readiness.writable || (update.fd_events & (EPOLLOUT | EPOLLHUP)) || fd_error);
        if (read_ready) {
            flags |= KBS_STREAM_EVENT_READABLE;
        }
        if (write_ready) {
            flags |= KBS_STREAM_EVENT_WRITABLE;
        }
        // TCP 底层一次 fd 事件整体消耗 EPOLLONESHOT，不能留下永远不会唤醒的另一方向。
        if (read_ready || write_ready || update.fd_events || !active) {
            slot.watch_read = slot.watch_write = false;
        }
        event.state = state;
        event.failure = failure;
        event.rx_pending = update.rx_pending;
        event.read_eof = update.readiness.read_eof;
        event.write_closed = update.readiness.write_closed;
        event.flags |= flags;
        slot.snapshot_known = true;
        if (event.flags) {
            Enqueue(slot);
        }
    }
    void Observe(Slot& slot, const runtime::OwnedConnectionStatus& status) noexcept {
        Observe(slot, runtime::ConnectionProgressEvent{.connection = status.scheduled,
                                                       .state = status.state,
                                                       .rx_pending = status.rx_pending,
                                                       .readiness = status.readiness,
                                                       .failure = status.failure});
    }
    void Refresh(Slot& slot) noexcept {
        // 仅应用入口调用；RecordProgress 不能重入 owner。刷新失败不覆盖已有成功字节前缀。
        if (const auto status = owner->Inspect(slot.connection); status) {
            Observe(slot, *status);
        }
    }
};
StreamBinding::StreamBinding() noexcept = default;
StreamBinding::~StreamBinding() = default;
std::expected<void, kbs_stream_error> StreamBinding::Open(runtime::ConnectionOwner& owner, runtime::RawStack* stack,
                                                          std::size_t context,
                                                          const StreamBindingConfig& config) noexcept {
    if (opened_once_) {
        return Error(KBS_STREAM_BUSY);
    }
    const auto capacity = owner.stats().capacity;
    const auto scheduled_capacity = owner.scheduler_capacity();
    if (!capacity || !scheduled_capacity || !config.table_id || !config.capacity || config.capacity > capacity ||
        !config.max_io_bytes || config.max_io_bytes > 1024u * 1024u) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    std::unique_ptr<State> state;
    try {
        state = std::make_unique<State>();
        state->slots = std::make_unique<State::Slot[]>(config.capacity);
        state->free = std::make_unique<std::uint32_t[]>(config.capacity);
        state->owner_slots = std::make_unique<std::uint32_t[]>(capacity);
        state->scheduled_slots = std::make_unique<std::uint32_t[]>(scheduled_capacity);
    } catch (const std::bad_alloc&) {
        return Error(KBS_STREAM_NO_MEMORY);
    }
    state->table = {this, config.max_io_bytes};
    state->owner = &owner;
    state->stack = stack;
    state->context = context;
    state->config = config;
    state->scheduled_capacity = scheduled_capacity;
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->free[i] = config.capacity - i - 1;
    }
    std::fill_n(state->owner_slots.get(), capacity, kUnused);
    std::fill_n(state->scheduled_slots.get(), scheduled_capacity, kUnused);
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<void, kbs_stream_error> StreamBinding::OpenTcp(runtime::ConnectionOwner& owner,
                                                             const StreamBindingConfig& config) noexcept {
    if (owner.exclusive_context()) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    return Open(owner, nullptr, 0, config);
}
std::expected<void, kbs_stream_error> StreamBinding::OpenRaw(runtime::RawStack& stack, std::size_t context,
                                                             const StreamBindingConfig& config) noexcept {
    const auto resources = stack.resources(context);
    if (!resources) {
        return std::unexpected(MapStreamError(resources.error()));
    }
    return Open(*resources->connections, &stack, context, config);
}
std::expected<runtime::OwnedConnection, kbs_stream_error> StreamBinding::Resolve(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    return (*slot)->connection;
}
std::expected<void, kbs_stream_error> StreamBinding::CanPublish() noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& s = *state_;
    if (const auto ready = s.Gate(); !ready) {
        return std::unexpected(ready.error());
    }
    if (!s.accepting) {
        return Error(KBS_STREAM_STOPPED);
    }
    if (s.count == s.config.capacity || s.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(KBS_STREAM_EXHAUSTED);
    }
    return {};
}
std::expected<kbs_stream_handle, kbs_stream_error>
StreamBinding::Publish(runtime::OwnedConnection connection) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& s = *state_;
    if (const auto ready = s.Gate(); !ready) {
        return std::unexpected(ready.error());
    }
    if (!s.accepting) {
        return Error(KBS_STREAM_STOPPED);
    }
    // Inspect 先校验 owner/完整代次/范围，再使用 connection.slot 访问索引。
    const auto status = s.owner->Inspect(connection);
    if (!status) {
        return std::unexpected(MapStreamError(status.error()));
    }
    if (s.owner_slots[connection.slot] != kUnused) {
        return Error(KBS_STREAM_BUSY);
    }
    const auto scheduled_index = status->scheduled.slot_index();
    if (!scheduled_index || *scheduled_index >= s.scheduled_capacity) {
        return Error(KBS_STREAM_NOT_READY);
    }
    if (s.scheduled_slots[*scheduled_index] != kUnused) {
        return Error(KBS_STREAM_BUSY);
    }
    if (s.count == s.config.capacity || s.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(KBS_STREAM_EXHAUSTED);
    }
    const auto index = s.free[s.config.capacity - ++s.count];
    auto& slot = s.slots[index];
    slot = {.connection = connection, .generation = ++s.next_generation, .scheduled = status->scheduled};
    s.owner_slots[connection.slot] = index;
    s.scheduled_slots[*scheduled_index] = index;
    slot.event.stream = {s.config.table_id, slot.generation, index, 0};
    s.Observe(slot, *status);
    return slot.event.stream;
}
kbs_stream_table* StreamBinding::table() noexcept {
    return state_ && !state_->closed ? &state_->table : nullptr;
}
std::size_t StreamBinding::published() const noexcept {
    return state_ ? state_->count : 0;
}
bool StreamBinding::events_pending() const noexcept {
    return state_ && !state_->closed && state_->pending_head != kUnused;
}
void StreamBinding::StopAccepting() noexcept {
    if (state_) {
        state_->accepting = false;
    }
}
std::expected<void, kbs_stream_error> StreamBinding::Close() noexcept {
    if (!state_ || state_->closed) {
        return {};
    }
    if (const auto ready = state_->Gate(); !ready) {
        return std::unexpected(ready.error());
    }
    StopAccepting();
    if (state_->count) {
        return Error(KBS_STREAM_BUSY);
    }
    state_->closed = true;
    return {};
}
std::expected<std::size_t, kbs_stream_error> StreamBinding::Write(kbs_stream_handle stream,
                                                                  std::span<const std::byte> input) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return std::unexpected(active.error());
    }
    if (status->tx.write_shutdown) {
        return Error(KBS_STREAM_CLOSED);
    }
    const auto written =
        state_->owner->Write((*slot)->connection, input.first(std::min(input.size(), state_->config.max_io_bytes)));
    state_->Refresh(**slot);
    if (!written) {
        return std::unexpected(MapStreamError(written.error()));
    }
    return *written;
}
std::expected<std::size_t, kbs_stream_error> StreamBinding::WriteLease(kbs_stream_handle stream,
                                                                       kbs_buffer_table* buffers,
                                                                       kbs_buffer_handle buffer, std::size_t offset,
                                                                       std::size_t bytes) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return std::unexpected(active.error());
    }
    if (!state_->stack || status->state != connection::ConnectionState::kRawActive) {
        return Error(KBS_STREAM_NOT_SUPPORTED);
    }
    if (status->tx.write_shutdown) {
        return Error(KBS_STREAM_CLOSED);
    }
    // 源池由全部 context 共享，当前流所在 context 可用不足以允许访问源元数据。
    const auto shared = state_->stack->shared();
    if (!shared) {
        return std::unexpected(MapStreamError(shared.error()));
    }
    const auto source = BufferBinding::Resolve(buffers, buffer, offset, bytes);
    if (!source) {
        return std::unexpected(source.error());
    }
    if (source->allocator != shared->slab) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    const auto written =
        state_->owner->WriteLease((*slot)->connection, *source, offset, std::min(bytes, state_->config.max_io_bytes));
    state_->Refresh(**slot);
    return written ? std::expected<std::size_t, kbs_stream_error>{*written}
                   : std::unexpected(MapStreamError(written.error()));
}
std::expected<kbs_stream_io, kbs_stream_error>
StreamBinding::Read(kbs_stream_handle stream, std::span<std::byte> output, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return std::unexpected(active.error());
    }
    if (output.empty()) {
        return kbs_stream_io{};
    }
    const auto read = state_->owner->Read((*slot)->connection,
                                          output.first(std::min(output.size(), state_->config.max_io_bytes)), now_ns);
    state_->Refresh(**slot);
    if (!read) {
        return std::unexpected(MapStreamError(read.error()));
    }
    return kbs_stream_io{read->bytes, read->eof ? 1u : 0u, 0};
}
std::expected<kbs_receive_view, kbs_stream_error>
StreamBinding::ReadLease(kbs_stream_handle stream, std::size_t max_bytes, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    if (!max_bytes) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return std::unexpected(active.error());
    }
    if (!state_->stack || status->state != connection::ConnectionState::kRawActive) {
        return Error(KBS_STREAM_NOT_SUPPORTED);
    }
    if ((*slot)->receive_generation) {
        return Error(KBS_STREAM_BUSY);
    }
    if (state_->next_receive_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(KBS_STREAM_EXHAUSTED);
    }
    const auto read =
        state_->owner->ReadLease((*slot)->connection, std::min(max_bytes, state_->config.max_io_bytes), now_ns);
    state_->Refresh(**slot);
    if (!read) {
        return std::unexpected(MapStreamError(read.error()));
    }
    kbs_receive_view view{};
    view.eof = read->eof ? 1u : 0u;
    if (read->generation) {
        (*slot)->receive_generation = ++state_->next_receive_generation;
        (*slot)->core_receive_generation = read->generation;
        view.lease = {stream, (*slot)->receive_generation};
        view.data = read->data.data();
        view.size = read->data.size();
    }
    return view;
}
std::expected<void, kbs_stream_error> StreamBinding::ReleaseReadLease(kbs_receive_lease lease) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(lease.stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    if (!lease.generation || lease.generation != (*slot)->receive_generation) {
        return Error(KBS_STREAM_INVALID_HANDLE);
    }
    // 不检查 Active：逻辑取消或 Close 保留时，应用仍拥有必须显式归还的视图。
    const auto released = state_->owner->ReleaseReadLease((*slot)->connection, (*slot)->core_receive_generation);
    if (released) {
        (*slot)->receive_generation = 0;
        (*slot)->core_receive_generation = 0;
    }
    state_->Refresh(**slot);
    return released ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(released.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::ShutdownWrite(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return active;
    }
    const auto result = state_->owner->ShutdownWrite((*slot)->connection);
    state_->Refresh(**slot);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::Cancel(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (status->state == connection::ConnectionState::kClosed ||
        status->state == connection::ConnectionState::kClosing) {
        return Error(KBS_STREAM_CLOSED);
    }
    const auto result = state_->owner->Cancel((*slot)->connection);
    state_->Refresh(**slot);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::Watch(kbs_stream_handle stream, bool readable,
                                                           bool writable) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return active;
    }
    if (status->state == connection::ConnectionState::kTcpActive) {
        const auto result = state_->owner->WatchTcp((*slot)->connection, readable, writable);
        if (!result) {
            return std::unexpected(MapStreamError(result.error()));
        }
    }
    (*slot)->watch_read = readable;
    (*slot)->watch_write = writable;
    state_->Observe(**slot, *status);
    return {};
}
std::expected<void, kbs_stream_error> StreamBinding::WatchTcp(kbs_stream_handle stream, bool readable,
                                                              bool writable) noexcept {
    const auto status = Inspect(stream);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (status->state != KBS_STREAM_TCP_ACTIVE) {
        // 保留旧入口对 raw 的拒绝；统一接口用 Watch。
        if (status->state == KBS_STREAM_STATE_CLOSED || status->state == KBS_STREAM_CLOSING) {
            return Error(KBS_STREAM_CLOSED);
        }
        if (status->failure.code) {
            return std::unexpected(status->failure);
        }
        return Error(KBS_STREAM_NOT_READY);
    }
    return Watch(stream, readable, writable);
}
std::expected<bool, kbs_stream_error>
StreamBinding::RecordProgress(const runtime::ConnectionProgressEvent& event) noexcept {
    if (!state_ || state_->closed) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& s = *state_;
    // 原回调已经在 owner 内；只读顶层暂停标志，绝不访问 ConnectionOwner 或域资源。
    if (s.stack && s.stack->paused(s.context)) {
        return Error(KBS_STREAM_BUSY);
    }
    if (s.stack && s.stack->stage() == runtime::RawStackStage::kClosed) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto index = event.connection.slot_index();
    if (!index || *index >= s.scheduled_capacity) {
        return false;
    }
    const auto published = s.scheduled_slots[*index];
    if (published == kUnused) {
        return false;
    }
    auto& slot = s.slots[published];
    if (slot.scheduled != event.connection) {
        return false;
    }
    s.Observe(slot, event);
    return true;
}
std::expected<std::size_t, kbs_stream_error> StreamBinding::PollEvents(std::span<kbs_stream_event> output) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    if (output.empty()) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    auto& s = *state_;
    if (const auto ready = s.Gate(); !ready) {
        return std::unexpected(ready.error());
    }
    const auto limit = std::min(output.size(), static_cast<std::size_t>(KBS_STREAM_EVENT_BATCH_MAX));
    std::size_t count = 0;
    while (count < limit && s.pending_head != kUnused) {
        auto& slot = s.slots[s.pending_head];
        output[count++] = slot.event;
        s.Remove(slot);
    }
    return count;
}
std::expected<kbs_stream_status, kbs_stream_error> StreamBinding::Inspect(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    return StreamStatus(*status);
}
std::expected<void, kbs_stream_error> StreamBinding::CloseStream(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto result = state_->owner->CloseConnection((*slot)->connection);
    state_->Refresh(**slot);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::RetireSources(kbs_stream_handle stream,
                                                                   const runtime::RxSourceRetireProof& proof) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto result = state_->owner->RetireSources((*slot)->connection, proof);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::Release(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto result = state_->owner->Release((*slot)->connection);
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    auto& s = *state_;
    s.Remove(**slot);
    s.owner_slots[(*slot)->connection.slot] = kUnused;
    s.scheduled_slots[*(*slot)->scheduled.slot_index()] = kUnused;
    **slot = {};
    s.free[s.config.capacity - s.count--] = stream.slot;
    return {};
}
} // namespace api
} // namespace kbsocket

namespace {
int32_t Report(kbs_stream_error result, kbs_stream_error* error) noexcept {
    if (error) {
        *error = result;
    }
    return result.code;
}
int32_t Invalid(kbs_stream_error* error) noexcept {
    return Report(kbsocket::api::StreamError(KBS_STREAM_INVALID_ARGUMENT), error);
}
int32_t Finish(const std::expected<void, kbs_stream_error>& result, kbs_stream_error* error) noexcept {
    return Report(result ? kbs_stream_error{} : result.error(), error);
}
} // namespace
extern "C" {
int32_t kbs_stream_read_lease(kbs_stream_table* table, kbs_stream_handle stream, size_t max_bytes, uint64_t now_ns,
                              kbs_receive_view* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out || !max_bytes) {
        return Invalid(error);
    }
    const auto read = table->binding->ReadLease(stream, max_bytes, now_ns);
    if (!read) {
        return Report(read.error(), error);
    }
    *out = *read;
    return Report({}, error);
}
int32_t kbs_stream_release_read_lease(kbs_stream_table* table, kbs_receive_lease lease, kbs_stream_error* error) {
    if (!table) {
        return Invalid(error);
    }
    return Finish(table->binding->ReleaseReadLease(lease), error);
}
int32_t kbs_stream_write_lease(kbs_stream_table* table, kbs_stream_handle stream, kbs_buffer_table* buffers,
                               kbs_buffer_handle buffer, size_t offset, size_t length, kbs_stream_io* out,
                               kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !buffers || !out || !length) {
        return Invalid(error);
    }
    const auto result = table->binding->WriteLease(stream, buffers, buffer, offset, length);
    if (!result) {
        return Report(result.error(), error);
    }
    out->bytes = *result;
    return Report({}, error);
}
int32_t kbs_stream_write(kbs_stream_table* table, kbs_stream_handle stream, const void* data, size_t length,
                         kbs_stream_io* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out || (!data && length)) {
        return Invalid(error);
    }
    const auto result =
        table->binding->Write(stream, {static_cast<const std::byte*>(data), std::min(length, table->max_io_bytes)});
    if (!result) {
        return Report(result.error(), error);
    }
    out->bytes = *result;
    return Report({}, error);
}
int32_t kbs_stream_read(kbs_stream_table* table, kbs_stream_handle stream, void* data, size_t length, uint64_t now_ns,
                        kbs_stream_io* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out || (!data && length)) {
        return Invalid(error);
    }
    const auto result =
        table->binding->Read(stream, {static_cast<std::byte*>(data), std::min(length, table->max_io_bytes)}, now_ns);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
int32_t kbs_stream_shutdown_write(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->ShutdownWrite(stream), error) : Invalid(error);
}
int32_t kbs_stream_cancel(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->Cancel(stream), error) : Invalid(error);
}
int32_t kbs_stream_watch_tcp(kbs_stream_table* table, kbs_stream_handle stream, uint32_t readable, uint32_t writable,
                             kbs_stream_error* error) {
    if (!table || readable > 1 || writable > 1) {
        return Invalid(error);
    }
    return Finish(table->binding->WatchTcp(stream, readable != 0, writable != 0), error);
}
int32_t kbs_stream_watch(kbs_stream_table* table, kbs_stream_handle stream, uint32_t readable, uint32_t writable,
                         kbs_stream_error* error) {
    if (!table || readable > 1 || writable > 1) {
        return Invalid(error);
    }
    return Finish(table->binding->Watch(stream, readable != 0, writable != 0), error);
}
int32_t kbs_stream_poll_events(kbs_stream_table* table, kbs_stream_event* events, size_t capacity, size_t* count,
                               kbs_stream_error* error) {
    if (count) {
        *count = 0;
    }
    if (!table || !events || !capacity || !count) {
        return Invalid(error);
    }
    const auto result =
        table->binding->PollEvents({events, std::min(capacity, static_cast<size_t>(KBS_STREAM_EVENT_BATCH_MAX))});
    if (!result) {
        return Report(result.error(), error);
    }
    *count = *result;
    return Report({}, error);
}
int32_t kbs_stream_inspect(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_status* out,
                           kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out) {
        return Invalid(error);
    }
    const auto result = table->binding->Inspect(stream);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
int32_t kbs_stream_close(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->CloseStream(stream), error) : Invalid(error);
}
int32_t kbs_stream_release(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->Release(stream), error) : Invalid(error);
}
}
