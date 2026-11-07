// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_progress.hpp"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <limits>
#include <new>

namespace kbsocket {
namespace runtime {
namespace {
auto Error(ProgressErrorCode code, int detail = 0) noexcept {
    return std::unexpected(ProgressError{code, detail});
}
using connection::ConnectionState;
constexpr auto kNone = std::numeric_limits<std::uint32_t>::max();
} // namespace

struct ConnectionProgress::State {
    struct Slot {
        connection::Connection* connection = nullptr;
        std::uint64_t id = 0;
        std::uint32_t previous = kNone, next = kNone, dense = 0;
        std::uint32_t interests = 0, watched_events = 0, events = 0;
        std::uint64_t retired = 0;
        std::size_t reported_rx = 0;
        int watched_fd = -1;
        bool queued = false;
        ConnectionState reported = ConnectionState::kIdle;
    };
    ~State() {
        if (epoll >= 0) {
            ::close(epoll);
        }
        if (wake >= 0) {
            ::close(wake);
        }
    }
    Slot* Lookup(std::uint64_t id) noexcept {
        const auto index = static_cast<std::uint32_t>(id);
        if (!index || index > config.capacity) {
            return nullptr;
        }
        auto& slot = slots[index - 1];
        return slot.connection && slot.id == id ? &slot : nullptr;
    }
    void Enqueue(Slot& slot) noexcept {
        if (slot.queued) {
            return;
        }
        const auto index = static_cast<std::uint32_t>(&slot - slots.get());
        slot.previous = tail;
        slot.next = kNone;
        if (tail != kNone) {
            slots[tail].next = index;
        } else {
            head = index;
        }
        tail = index;
        slot.queued = true;
    }
    void Dequeue(Slot& slot) noexcept {
        if (!slot.queued) {
            return;
        }
        if (slot.previous != kNone) {
            slots[slot.previous].next = slot.next;
        } else {
            head = slot.next;
        }
        if (slot.next != kNone) {
            slots[slot.next].previous = slot.previous;
        } else {
            tail = slot.previous;
        }
        slot.queued = false;
        slot.previous = slot.next = kNone;
    }
    std::expected<void, ProgressError> Signal() noexcept {
        if (wake_pending) {
            return {};
        }
        std::uint64_t value = 1;
        if (::write(wake, &value, sizeof(value)) < 0 && errno != EAGAIN) {
            return Error(ProgressErrorCode::kSystem, errno);
        }
        wake_pending = true;
        return {};
    }
    std::expected<void, ProgressError> Watch(Slot& slot) noexcept {
        const auto phase = slot.connection->state();
        const int fd = slot.connection->fd();
        std::uint32_t mask = 0;
        if (phase == ConnectionState::kTcpActive) {
            if (slot.interests) {
                mask = slot.interests | EPOLLRDHUP | EPOLLONESHOT;
            }
        } else if (phase == ConnectionState::kRawActive) {
            mask = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
            if (slot.connection->control_output_pending()) {
                mask |= EPOLLOUT;
            }
        } else if (phase == ConnectionState::kNegotiating || phase == ConnectionState::kPreparing ||
                   phase == ConnectionState::kWaitingReady) {
            mask = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
            if (slot.connection->handshake().output_pending()) {
                mask |= EPOLLOUT;
            }
        }
        // 不能对可能已复用的 fd 做 DEL。若外部仍有 dup，旧 watch 最多产生一次 oneshot
        // 事件，完整 id 校验后丢弃或报告关闭；不得重新激活旧 open-file description。
        if (fd != slot.watched_fd) {
            slot.watched_fd = -1;
            slot.watched_events = 0;
        }
        if (fd < 0) {
            return {};
        }
        if (!mask) {
            if (slot.watched_fd >= 0 && ::epoll_ctl(epoll, EPOLL_CTL_DEL, fd, nullptr) < 0 && errno != ENOENT) {
                return Error(ProgressErrorCode::kSystem, errno);
            }
            slot.watched_fd = -1;
            slot.watched_events = 0;
            return {};
        }
        if (slot.watched_fd == fd && slot.watched_events == mask) {
            return {};
        }
        epoll_event event{};
        event.events = mask;
        event.data.u64 = slot.id;
        if (::epoll_ctl(epoll, slot.watched_fd < 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, fd, &event) < 0) {
            return Error(ProgressErrorCode::kSystem, errno);
        }
        slot.watched_fd = fd;
        slot.watched_events = mask;
        return {};
    }
    std::expected<std::size_t, ProgressError> Visit(Slot& slot, std::uint64_t now) noexcept {
        ++stats.visits;
        auto& connection = *slot.connection;
        if ((connection.raw_context() && connection.raw_context() != config.context) ||
            (connection.connection_id() && connection.connection_id() != slot.id)) {
            return Error(ProgressErrorCode::kInvalidArgument);
        }
        const auto events = slot.events;
        slot.events = 0;
        if (connection.state() == ConnectionState::kRawActive && events) {
            // EOF 只使本连接失败；连接持有的 DMA 仍由之后的 TX CQE 退休。
            (void)connection.OnControlReady();
        }
        std::size_t processed = 0;
        if (connection.state() != ConnectionState::kIdle && connection.state() != ConnectionState::kFailed &&
            connection.state() != ConnectionState::kClosing && connection.state() != ConnectionState::kClosed) {
            // Reactor 的批次时间可能早于本轮 RX 回调/应用 Read；每次访问刷新同一单调时钟，
            // 防止把旧时间传入已见到较新 RX 的重排窗口。传入值仍作为测试/调度时间下界。
            const auto current = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                std::chrono::steady_clock::now().time_since_epoch())
                                                                .count());
            const auto advanced = connection.Progress(std::max(now, current));
            if (advanced) {
                processed = *advanced;
            }
        }
        if (auto watched = Watch(slot); !watched) {
            return std::unexpected(watched.error());
        }
        const auto pending_rx = connection.rx_pending();
        const bool rx_changed = pending_rx != slot.reported_rx;
        if (slot.reported != connection.state() || rx_changed || slot.retired ||
            (events && connection.state() == ConnectionState::kTcpActive)) {
            if (connection.state() == ConnectionState::kFailed && slot.reported != connection.state()) {
                ++stats.failures;
            }
            slot.reported = connection.state();
            if (callback) {
                callback(cookie, {{owner, slot.id},
                                  slot.reported,
                                  connection.state() == ConnectionState::kTcpActive ? events : 0,
                                  rx_changed,
                                  pending_rx,
                                  slot.retired});
            }
            slot.reported_rx = pending_rx;
            slot.retired = 0;
            ++processed;
        }
        // 仅已取得进展才继续本连接；等待 Credit/资源时交给 CQE、RX 或周期巡检唤醒。
        if (processed && connection.state() == ConnectionState::kRawActive && connection.tx_stats().queued) {
            Enqueue(slot);
        }
        return processed;
    }
    ConnectionProgressConfig config;
    ConnectionProgress* owner = nullptr;
    ConnectionProgressCallback callback = nullptr;
    void* cookie = nullptr;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<std::uint32_t[]> dense, free;
    std::uint32_t free_count = 0, head = kNone, tail = kNone, sweep = 0;
    std::uint64_t next_sweep = 0;
    bool sweeping = false, wake_pending = false;
    std::array<connection::EagerReceiveDomain*, 8> domains{};
    std::size_t domain_count = 0, next_domain = 0;
    ConnectionProgressStats stats;
    int epoll = -1, wake = -1;
};

ConnectionProgress::ConnectionProgress() noexcept = default;
ConnectionProgress::~ConnectionProgress() {
    if (!Close()) {
        // 注册对象与回调仍被外部 Reactor 借用时不得释放内部状态；宿主须显式拆除依赖。
        (void)state_.release();
    }
}
std::expected<void, ProgressError> ConnectionProgress::Open(const ConnectionProgressConfig& config,
                                                            ConnectionProgressCallback callback,
                                                            void* cookie) noexcept {
    if (state_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (!config.capacity || config.capacity > kMaxConnections || !config.sweep_interval_ns) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    state->slots.reset(new (std::nothrow) State::Slot[config.capacity]);
    state->dense.reset(new (std::nothrow) std::uint32_t[config.capacity]);
    state->free.reset(new (std::nothrow) std::uint32_t[config.capacity]);
    if (!state->slots || !state->dense || !state->free) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    state->epoll = ::epoll_create1(EPOLL_CLOEXEC);
    state->wake = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (state->epoll < 0 || state->wake < 0) {
        return Error(ProgressErrorCode::kSystem, errno);
    }
    epoll_event event{};
    event.events = EPOLLIN;
    if (::epoll_ctl(state->epoll, EPOLL_CTL_ADD, state->wake, &event) < 0) {
        return Error(ProgressErrorCode::kSystem, errno);
    }
    state->config = config;
    state->callback = callback;
    state->cookie = cookie;
    state->owner = this;
    state->free_count = config.capacity;
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->free[i] = config.capacity - 1 - i;
    }
    state_ = std::move(state);
    return {};
}
std::expected<ScheduledConnection, ProgressError>
ConnectionProgress::Attach(connection::Connection& connection) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (!state.free_count || generation_ == std::numeric_limits<std::uint32_t>::max()) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    if (connection.connection_id() || connection.state() == ConnectionState::kClosed ||
        connection.state() == ConnectionState::kClosing) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < state.stats.attached; ++i) {
        const auto& other = state.slots[state.dense[i]];
        if (other.connection == &connection || (connection.fd() >= 0 && other.connection->fd() == connection.fd())) {
            return Error(ProgressErrorCode::kInUse);
        }
    }
    const auto index = state.free[--state.free_count];
    auto& slot = state.slots[index];
    slot = {};
    slot.connection = &connection;
    slot.id = (static_cast<std::uint64_t>(++generation_) << 32) | (index + 1);
    slot.dense = state.stats.attached;
    state.dense[state.stats.attached++] = index;
    if (auto watch = state.Watch(slot); !watch) {
        --state.stats.attached;
        state.free[state.free_count++] = index;
        slot = {};
        return std::unexpected(watch.error());
    }
    state.Enqueue(slot);
    if (auto signalled = state.Signal(); !signalled) {
        // Attach 失败不能留下没有返回 handle 的借用；这里尚未提交任何业务 WR。
        state.Dequeue(slot);
        if (slot.watched_fd >= 0) {
            (void)::epoll_ctl(state.epoll, EPOLL_CTL_DEL, slot.watched_fd, nullptr);
        }
        --state.stats.attached;
        state.free[state.free_count++] = index;
        slot = {};
        return std::unexpected(signalled.error());
    }
    return ScheduledConnection{this, slot.id};
}
bool ConnectionProgress::HasFd(int fd) const noexcept {
    if (!state_ || fd < 0) {
        return false;
    }
    for (std::size_t i = 0; i < state_->stats.attached; ++i) {
        const auto& slot = state_->slots[state_->dense[i]];
        if (slot.connection->fd() == fd) {
            return true;
        }
    }
    return false;
}
std::expected<void, ProgressError> ConnectionProgress::Detach(ScheduledConnection handle) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    if (slot->connection->state() != ConnectionState::kClosed) {
        return Error(ProgressErrorCode::kInUse);
    }
    state_->Dequeue(*slot);
    auto& state = *state_;
    const auto last = state.dense[--state.stats.attached];
    state.dense[slot->dense] = last;
    state.slots[last].dense = slot->dense;
    state.free[state.free_count++] = static_cast<std::uint32_t>(slot - state.slots.get());
    *slot = {};
    state.sweeping = false;
    state.next_sweep = 0;
    return {};
}
std::expected<void, ProgressError> ConnectionProgress::Wake(ScheduledConnection handle) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    state_->Enqueue(*slot);
    return state_->Signal();
}
std::expected<void, ProgressError> ConnectionProgress::WatchTcp(ScheduledConnection handle, bool read,
                                                                bool write) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot || slot->connection->state() != ConnectionState::kTcpActive) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    slot->interests = (read ? EPOLLIN : 0u) | (write ? EPOLLOUT : 0u);
    // oneshot 已触发后即使 mask 相同也须 MOD；fd 身份仍保留，避免重复 ADD。
    slot->watched_events = 0;
    return state_->Watch(*slot);
}
std::expected<void, ProgressError> ConnectionProgress::OnTxCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (!event.record) {
        return event.kind == raw::TxCompletionKind::kRejected ? Error(ProgressErrorCode::kProvider)
                                                              : std::expected<void, ProgressError>{};
    }
    auto* slot = state_->Lookup(event.record->metadata.connection_id);
    if (!slot || !event.lane || !event.lane->pool || event.lane->pool->context() != state_->config.context) {
        return Error(ProgressErrorCode::kRetained);
    }
    const auto retired = slot->connection->OnTxCompletion(event);
    state_->Enqueue(*slot);
    if (!retired && (!slot->connection->failure() ||
                     retired.error().detail == static_cast<int>(connection::EagerTxErrorCode::kInvalidCompletion))) {
        return Error(ProgressErrorCode::kRetained, static_cast<int>(retired.error().code));
    }
    ++slot->retired;
    return {};
}
std::expected<void, ProgressError> ConnectionProgress::RequestSweep() noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    state_->next_sweep = 0;
    return state_->Signal();
}
std::expected<void, ProgressError>
ConnectionProgress::AddReceiveDomain(connection::EagerReceiveDomain& domain) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (!state_->config.context || domain.context() != state_->config.context ||
        state_->domain_count == state_->domains.size()) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < state_->domain_count; ++i) {
        if (state_->domains[i] == &domain) {
            return Error(ProgressErrorCode::kInUse);
        }
    }
    state_->domains[state_->domain_count++] = &domain;
    return {};
}
std::expected<ProgressBatch, ProgressError> ConnectionProgress::Progress(ProgressKind kind, std::size_t budget,
                                                                         std::uint64_t now) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    ProgressBatch result;
    if (!budget) {
        return result;
    }
    if (kind == ProgressKind::kRefill && state.domain_count) {
        auto& domain = *state.domains[state.next_domain++ % state.domain_count];
        if (domain.frozen()) {
            // raw 层 Stop/Retain 可能先于连接回调；同步冻结物理共账，禁止继续发布 Credit。
            domain.Freeze();
        } else {
            const auto filled = domain.Refill(std::min(budget, raw::RxBufferPool::kMaxBatch));
            if (!filled) {
                return Error(ProgressErrorCode::kProvider, filled.error().provider_error);
            }
            result.processed = *filled;
            result.runnable = *filled && domain.pending_refill();
            if (*filled) {
                state.next_sweep = 0;
            }
        }
    } else if (kind == ProgressKind::kTimer) {
        if (!state.sweeping && now >= state.next_sweep) {
            state.sweeping = true;
            state.sweep = 0;
        }
        for (std::size_t i = 0; state.sweeping && i < budget; ++i) {
            if (state.sweep == state.stats.attached) {
                state.sweeping = false;
                state.next_sweep = now > std::numeric_limits<std::uint64_t>::max() - state.config.sweep_interval_ns
                                       ? std::numeric_limits<std::uint64_t>::max()
                                       : now + state.config.sweep_interval_ns;
                break;
            }
            auto& slot = state.slots[state.dense[state.sweep++]];
            if (slot.connection->state() != ConnectionState::kTcpActive) {
                state.Enqueue(slot);
            }
        }
        result.runnable = state.sweeping || state.head != kNone;
    } else if (kind == ProgressKind::kMailbox) {
        for (std::size_t i = 0; i < budget && state.head != kNone; ++i) {
            auto& slot = state.slots[state.head];
            state.Dequeue(slot);
            const auto visited = state.Visit(slot, now);
            if (!visited) {
                return std::unexpected(visited.error());
            }
            result.processed += *visited;
        }
        result.runnable = state.head != kNone;
    }
    return result;
}
std::expected<ProgressBatch, ProgressError> ConnectionProgress::CollectNotifications(std::size_t budget) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (!budget) {
        return ProgressBatch{};
    }
    std::array<epoll_event, 64> events{};
    const int count = ::epoll_wait(state_->epoll, events.data(), std::min(budget, events.size()), 0);
    if (count < 0) {
        return errno == EINTR ? std::expected<ProgressBatch, ProgressError>(ProgressBatch{})
                              : Error(ProgressErrorCode::kSystem, errno);
    }
    for (int i = 0; i < count; ++i) {
        if (!events[i].data.u64) {
            std::uint64_t value;
            if (::read(state_->wake, &value, sizeof(value)) < 0 && errno != EAGAIN && errno != EINTR) {
                return Error(ProgressErrorCode::kSystem, errno);
            }
            state_->wake_pending = false;
            continue;
        }
        auto* slot = state_->Lookup(events[i].data.u64);
        if (!slot) {
            ++state_->stats.stale_events;
            continue;
        }
        ++state_->stats.fd_events;
        slot->events |= events[i].events;
        // 所有控制 fd 也是 oneshot；仍活跃时 Visit 重新 MOD，关闭/代次失效不再重装。
        slot->watched_events = 0;
        if (slot->connection->state() == ConnectionState::kTcpActive) {
            slot->interests = 0;
        }
        state_->Enqueue(*slot);
    }
    return ProgressBatch{0, state_->head != kNone || count == static_cast<int>(std::min(budget, events.size()))};
}
std::expected<void, ProgressError> ConnectionProgress::Rearm() noexcept {
    return state_ ? std::expected<void, ProgressError>{} : Error(ProgressErrorCode::kNotReady);
}
std::span<const int> ConnectionProgress::wait_fds() const noexcept {
    return state_ ? std::span<const int>(&state_->epoll, 1) : std::span<const int>{};
}
const void* ConnectionProgress::exclusive_context() const noexcept {
    return state_ ? state_->config.context : nullptr;
}
std::expected<void, ProgressError> ConnectionProgress::Close() noexcept {
    if (state_ && state_->stats.attached) {
        return Error(ProgressErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
const ConnectionProgressStats* ConnectionProgress::stats() const noexcept {
    return state_ ? &state_->stats : nullptr;
}
} // namespace runtime
} // namespace kbsocket
