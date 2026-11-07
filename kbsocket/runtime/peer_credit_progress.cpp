// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/peer_credit_progress.hpp"

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
constexpr auto kNone = std::numeric_limits<std::uint32_t>::max();
std::uint64_t Now(std::uint64_t lower_bound) noexcept {
    const auto current = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
    return std::max(lower_bound, current);
}
} // namespace

struct PeerCreditProgress::State {
    struct Slot {
        std::optional<control::PeerCreditControl> controller;
        std::uint64_t id = 0, last_now = 0;
        std::uint32_t previous = kNone, next = kNone, dense = 0, watched_events = 0;
        int watched_fd = -1;
        bool queued = false;
        PeerWithdrawalState phase = PeerWithdrawalState::kActive;
        std::optional<ProgressError> scheduling_failure;
        PeerWithdrawalSnapshot Snapshot() const noexcept {
            return {phase, controller->stats(), controller->failure(), scheduling_failure};
        }
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
        return slot.controller && slot.id == id ? &slot : nullptr;
    }
    void Enqueue(Slot& slot) noexcept {
        if (slot.queued || slot.phase != PeerWithdrawalState::kActive) {
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
        const std::uint64_t one = 1;
        if (::write(wake, &one, sizeof(one)) < 0 && errno != EAGAIN) {
            return Error(ProgressErrorCode::kSystem, errno);
        }
        wake_pending = true;
        return {};
    }
    std::expected<void, ProgressError> Unwatch(Slot& slot) noexcept {
        // 协议失败会自行 close；此时旧数字可能被复用，禁止 DEL 另一个对象的 fd。
        if (slot.watched_fd >= 0 && slot.controller->fd() == slot.watched_fd) {
            if (::epoll_ctl(epoll, EPOLL_CTL_DEL, slot.watched_fd, nullptr) < 0 && errno != ENOENT) {
                return Error(ProgressErrorCode::kSystem, errno);
            }
        }
        slot.watched_fd = -1;
        slot.watched_events = 0;
        return {};
    }
    std::expected<void, ProgressError> Watch(Slot& slot) noexcept {
        const auto& controller = *slot.controller;
        std::uint32_t mask = 0;
        if (slot.phase == PeerWithdrawalState::kActive && controller.fd() >= 0) {
            // 对端最终 ACK 后不再监听 EOF；本端若仍等待 RX/refill，只由 timer/Wake 推进。
            if (!controller.stats().peer_acknowledged) {
                mask |= EPOLLIN | EPOLLRDHUP;
            }
            if (controller.output_pending()) {
                mask |= EPOLLOUT;
            }
        }
        if (!mask) {
            return Unwatch(slot);
        }
        mask |= EPOLLONESHOT;
        if (slot.watched_events == mask) {
            return {};
        }
        epoll_event event{};
        event.events = mask;
        event.data.u64 = slot.id;
        if (::epoll_ctl(epoll, slot.watched_fd < 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, controller.fd(), &event) < 0) {
            return Error(ProgressErrorCode::kSystem, errno);
        }
        slot.watched_fd = controller.fd();
        slot.watched_events = mask;
        return {};
    }
    std::size_t Visit(Slot& slot, std::uint64_t now) noexcept {
        ++stats.visits;
        // Begin 可发生在本轮 Tick 时间之后；正常暂停也不延长已开始的协议超时。
        slot.last_now = Now(std::max(slot.last_now, now));
        const auto advanced = slot.controller->Progress(slot.last_now);
        std::size_t processed = advanced ? advanced->processed : 0;
        if (!advanced) {
            slot.phase = PeerWithdrawalState::kFailed;
        } else if (advanced->complete) {
            slot.phase = PeerWithdrawalState::kComplete;
        }
        if (const auto watched = Watch(slot); !watched) {
            slot.scheduling_failure = watched.error();
            slot.phase = PeerWithdrawalState::kFailed;
            // 即使摘除失败，oneshot 和终态过滤也不会把该 Peer 重新排入协议队列。
            (void)Unwatch(slot);
        }
        if (slot.phase != PeerWithdrawalState::kActive) {
            if (slot.phase == PeerWithdrawalState::kComplete) {
                ++stats.completed;
            } else {
                ++stats.failures;
            }
            ++processed;
            if (callback) {
                callback(cookie, {{owner, slot.id}, slot.Snapshot()});
            }
        }
        // 未完成和待写不代表现在可推进：等待 EAGAIN、RX receipt 或 post 结论均让出 CPU。
        return processed;
    }
    std::expected<void, ProgressError> Remove(Slot& slot) noexcept {
        if (const auto unwatched = Unwatch(slot); !unwatched) {
            return unwatched;
        }
        if (const auto closed = slot.controller->Close(); !closed) {
            return Error(ProgressErrorCode::kRetained, static_cast<int>(closed.error().code));
        }
        Dequeue(slot);
        const auto index = static_cast<std::uint32_t>(&slot - slots.get());
        const auto moved = dense[--stats.attached];
        dense[slot.dense] = moved;
        slots[moved].dense = slot.dense;
        slot.controller.reset();
        free[free_count++] = index;
        // swap-remove 后重启有界扫描，防止漏掉搬到已扫描位置的 Peer。
        sweeping = false;
        next_sweep = 0;
        return {};
    }
    PeerCreditProgressConfig config;
    PeerCreditProgress* owner = nullptr;
    PeerCreditProgressCallback callback = nullptr;
    void* cookie = nullptr;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<std::uint32_t[]> dense, free;
    std::uint32_t free_count = 0, head = kNone, tail = kNone, sweep = 0;
    std::uint64_t next_sweep = 0;
    bool sweeping = false, wake_pending = false;
    PeerCreditProgressStats stats;
    int epoll = -1, wake = -1;
};

PeerCreditProgress::PeerCreditProgress() noexcept = default;
PeerCreditProgress::~PeerCreditProgress() {
    if (!Close()) {
        // 未显式解绑的控制器仍引用账本；不能靠析构伪造终态或隐式恢复授信。
        (void)state_.release();
    }
}
std::expected<void, ProgressError> PeerCreditProgress::Open(const PeerCreditProgressConfig& config,
                                                            PeerCreditProgressCallback callback,
                                                            void* cookie) noexcept {
    if (state_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (!config.context || !config.capacity || config.capacity > kMaxPeers || !config.sweep_interval_ns) {
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
        state->free[i] = config.capacity - i - 1;
    }
    state_ = std::move(state);
    return {};
}
std::expected<PeerWithdrawal, ProgressError>
PeerCreditProgress::BeginWithdrawal(int fd, const control::PeerCreditControlConfig& config,
                                    std::uint64_t now) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (fd < 0 || fd == state.epoll || fd == state.wake) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < state.stats.attached; ++i) {
        if (state.slots[state.dense[i]].controller->fd() == fd) {
            return Error(ProgressErrorCode::kInUse);
        }
    }
    if (!state.free_count || generation_ == std::numeric_limits<std::uint32_t>::max()) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    const auto index = state.free[state.free_count - 1];
    auto& slot = state.slots[index];
    slot.id = (std::uint64_t{++generation_} << 32) | (index + 1);
    // 注册与唤醒必须早于 Open 的永久 Stop/Freeze；最后一步成功后不再有可能失败的提交操作。
    epoll_event event{};
    event.events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
    event.data.u64 = slot.id;
    if (::epoll_ctl(state.epoll, EPOLL_CTL_ADD, fd, &event) < 0) {
        return Error(ProgressErrorCode::kSystem, errno);
    }
    if (const auto signaled = state.Signal(); !signaled) {
        (void)::epoll_ctl(state.epoll, EPOLL_CTL_DEL, fd, nullptr);
        return std::unexpected(signaled.error());
    }
    slot.controller.emplace();
    slot.last_now = Now(now);
    if (const auto opened = slot.controller->Open(fd, config, slot.last_now); !opened) {
        (void)::epoll_ctl(state.epoll, EPOLL_CTL_DEL, fd, nullptr);
        slot.controller.reset();
        return Error(ProgressErrorCode::kInvalidArgument, static_cast<int>(opened.error().code));
    }
    slot.phase = PeerWithdrawalState::kActive;
    slot.scheduling_failure.reset();
    slot.watched_fd = fd;
    slot.watched_events = event.events;
    slot.dense = static_cast<std::uint32_t>(state.stats.attached);
    state.dense[state.stats.attached++] = index;
    --state.free_count;
    state.Enqueue(slot);
    return PeerWithdrawal{this, slot.id};
}
std::expected<PeerWithdrawalSnapshot, ProgressError> PeerCreditProgress::Inspect(PeerWithdrawal handle) const noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    const auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    return slot->Snapshot();
}
std::expected<void, ProgressError> PeerCreditProgress::Wake(PeerWithdrawal handle) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    if (slot->phase == PeerWithdrawalState::kActive) {
        if (const auto signaled = state_->Signal(); !signaled) {
            return signaled;
        }
        state_->Enqueue(*slot);
    }
    return {};
}
std::expected<void, ProgressError> PeerCreditProgress::Release(PeerWithdrawal handle) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    if (slot->phase == PeerWithdrawalState::kActive) {
        return Error(ProgressErrorCode::kInUse);
    }
    return state_->Remove(*slot);
}
std::expected<void, ProgressError> PeerCreditProgress::Abort(PeerWithdrawal handle) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto* slot = handle.owner == this ? state_->Lookup(handle.id) : nullptr;
    if (!slot) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    const bool active = slot->phase == PeerWithdrawalState::kActive;
    if (const auto removed = state_->Remove(*slot); !removed) {
        return removed;
    }
    state_->stats.aborted += active;
    return {};
}
std::expected<ProgressBatch, ProgressError> PeerCreditProgress::Progress(ProgressKind kind, std::size_t budget,
                                                                         std::uint64_t now) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    ProgressBatch result;
    if (!budget) {
        return result;
    }
    if (kind == ProgressKind::kMailbox) {
        for (std::size_t i = 0; i < budget && state.head != kNone; ++i) {
            auto& slot = state.slots[state.head];
            state.Dequeue(slot);
            result.processed += state.Visit(slot, now);
        }
        result.runnable = state.head != kNone;
    } else if (kind == ProgressKind::kTimer) {
        if (!state.sweeping && now >= state.next_sweep && state.stats.attached) {
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
            state.Enqueue(state.slots[state.dense[state.sweep++]]);
        }
        result.runnable = state.sweeping || state.head != kNone;
    }
    return result;
}
std::expected<ProgressBatch, ProgressError> PeerCreditProgress::CollectNotifications(std::size_t budget) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (!budget) {
        return ProgressBatch{};
    }
    std::array<epoll_event, 64> events{};
    const auto limit = static_cast<int>(std::min(budget, events.size()));
    const int count = ::epoll_wait(state_->epoll, events.data(), limit, 0);
    if (count < 0) {
        return errno == EINTR ? std::expected<ProgressBatch, ProgressError>(ProgressBatch{})
                              : Error(ProgressErrorCode::kSystem, errno);
    }
    for (int i = 0; i < count; ++i) {
        if (!events[i].data.u64) {
            std::uint64_t value;
            if (::read(state_->wake, &value, sizeof(value)) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno != EAGAIN) {
                    return Error(ProgressErrorCode::kSystem, errno);
                }
            }
            state_->wake_pending = false;
            continue;
        }
        auto* slot = state_->Lookup(events[i].data.u64);
        if (!slot || slot->phase != PeerWithdrawalState::kActive) {
            ++state_->stats.stale_events;
            continue;
        }
        ++state_->stats.fd_events;
        slot->watched_events = 0;
        state_->Enqueue(*slot);
    }
    return ProgressBatch{0, state_->head != kNone || count == limit};
}
std::expected<void, ProgressError> PeerCreditProgress::Rearm() noexcept {
    return state_ ? std::expected<void, ProgressError>{} : Error(ProgressErrorCode::kNotReady);
}
std::span<const int> PeerCreditProgress::wait_fds() const noexcept {
    return state_ ? std::span<const int>(&state_->epoll, 1) : std::span<const int>{};
}
const void* PeerCreditProgress::exclusive_context() const noexcept {
    return state_ ? state_->config.context : nullptr;
}
std::expected<void, ProgressError> PeerCreditProgress::Close() noexcept {
    if (state_ && state_->stats.attached) {
        return Error(ProgressErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
const PeerCreditProgressStats* PeerCreditProgress::stats() const noexcept {
    return state_ ? &state_->stats : nullptr;
}
} // namespace runtime
} // namespace kbsocket
