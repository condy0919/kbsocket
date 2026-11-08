// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/listener_events.hpp"

#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace api {
namespace {
using runtime::ProgressErrorCode;
auto Error(ProgressErrorCode code, int detail = 0) noexcept {
    return std::unexpected(runtime::ProgressError{code, detail});
}
constexpr auto kNone = std::numeric_limits<std::uint32_t>::max();
constexpr std::uint64_t kMaxSequence = std::numeric_limits<std::uint64_t>::max() >> 16;
} // namespace

struct ListenerEvents::State {
    struct Slot {
        kbs_listener_handle handle{};
        std::uint64_t tag = 0;
        int fd = -1;
        std::uint32_t previous = kNone, next = kNone, flags = 0;
        bool registered = false, armed = false, queued = false;
    };
    ~State() {
        if (epoll >= 0) {
            (void)::close(epoll);
        }
    }
    std::expected<Slot*, runtime::ProgressError> Find(kbs_listener_handle handle) noexcept {
        if (handle.runtime_id != runtime_id || !handle.generation || handle.reserved || handle.slot >= capacity) {
            return Error(ProgressErrorCode::kInvalidArgument);
        }
        auto& slot = slots[handle.slot];
        if (slot.fd < 0 || slot.handle.generation != handle.generation) {
            return Error(ProgressErrorCode::kInvalidArgument);
        }
        return &slot;
    }
    Slot* Lookup(std::uint64_t tag) noexcept {
        const auto index = static_cast<std::uint32_t>(tag & 0xffffu);
        if (!tag || index >= capacity) {
            return nullptr;
        }
        auto& slot = slots[index];
        return slot.fd >= 0 && slot.tag == tag ? &slot : nullptr;
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
    void Remove(Slot& slot) noexcept {
        if (slot.queued) {
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
        }
        slot.previous = slot.next = kNone;
        slot.flags = 0;
        slot.queued = false;
    }
    std::expected<void, runtime::ProgressError> Unwatch(Slot& slot) noexcept {
        // 必须在借用 fd 关闭前 DEL；其他 dup 仍存活时，单纯 close 不能撤销 epoll 引用。
        if (slot.registered && ::epoll_ctl(epoll, EPOLL_CTL_DEL, slot.fd, nullptr) < 0 && errno != ENOENT) {
            return Error(ProgressErrorCode::kSystem, errno);
        }
        slot.registered = slot.armed = false;
        Remove(slot);
        return {};
    }
    std::unique_ptr<Slot[]> slots;
    std::uint64_t runtime_id = 0, sequence = 0;
    std::uint32_t capacity = 0, attached = 0, head = kNone, tail = kNone;
    int epoll = -1;
    bool stopped = false;
};

ListenerEvents::ListenerEvents() noexcept = default;
ListenerEvents::~ListenerEvents() = default;
std::expected<void, runtime::ProgressError> ListenerEvents::Open(std::uint64_t runtime_id,
                                                                 std::uint32_t capacity) noexcept {
    if (state_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (!runtime_id || capacity > 65536) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    state->slots.reset(new (std::nothrow) State::Slot[capacity]);
    if (!state->slots) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    state->epoll = ::epoll_create1(EPOLL_CLOEXEC);
    if (state->epoll < 0) {
        return Error(ProgressErrorCode::kSystem, errno);
    }
    state->runtime_id = runtime_id;
    state->capacity = capacity;
    state_ = std::move(state);
    return {};
}
std::expected<void, runtime::ProgressError> ListenerEvents::Attach(kbs_listener_handle handle, int fd) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (handle.runtime_id != state.runtime_id || !handle.generation || handle.reserved ||
        handle.slot >= state.capacity || fd < 0 || fd == state.epoll) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    auto& slot = state.slots[handle.slot];
    if (slot.fd >= 0) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (state.sequence == kMaxSequence) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    // tag 与公开 generation 独立；完整 48 位序号加 16 位槽下标，绝不截断公开身份。
    slot.handle = handle;
    slot.tag = (++state.sequence << 16) | handle.slot;
    slot.fd = fd;
    ++state.attached;
    return {};
}
std::expected<void, runtime::ProgressError> ListenerEvents::Detach(kbs_listener_handle handle) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    const auto found = state.Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (const auto removed = state.Unwatch(slot); !removed) {
        return removed;
    }
    slot = {};
    --state.attached;
    return {};
}
std::expected<void, runtime::ProgressError> ListenerEvents::Watch(kbs_listener_handle handle, bool enabled) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    const auto found = state.Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (!enabled) {
        return state.Unwatch(slot);
    }
    if (state.stopped) {
        return Error(ProgressErrorCode::kNotReady);
    }
    epoll_event event{};
    event.events = EPOLLIN | EPOLLONESHOT;
    event.data.u64 = slot.tag;
    if (::epoll_ctl(state.epoll, slot.registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, slot.fd, &event) < 0) {
        return Error(ProgressErrorCode::kSystem, errno);
    }
    // 只有内核订阅成功才替换旧提示；失败保留原队列，应用仍能观察此前的结果。
    slot.registered = slot.armed = true;
    state.Remove(slot);
    return {};
}
std::expected<std::size_t, runtime::ProgressError> ListenerEvents::Poll(std::span<kbs_listener_event> output) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (output.empty() || output.size() > KBS_LISTENER_MAX_EVENT_BATCH) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    std::size_t count = 0;
    while (count < output.size() && state_->head != kNone) {
        auto& slot = state_->slots[state_->head];
        output[count++] = {slot.handle, slot.flags, 0};
        state_->Remove(slot);
    }
    return count;
}
bool ListenerEvents::pending() const noexcept {
    return state_ && state_->head != kNone;
}
void ListenerEvents::Stop() noexcept {
    if (!state_) {
        return;
    }
    state_->stopped = true;
    while (state_->head != kNone) {
        state_->Remove(state_->slots[state_->head]);
    }
}
std::expected<void, runtime::ProgressError> ListenerEvents::Close() noexcept {
    if (state_ && state_->attached) {
        return Error(ProgressErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
std::expected<runtime::ProgressBatch, runtime::ProgressError>
ListenerEvents::Progress(runtime::ProgressKind kind, std::size_t budget, std::uint64_t) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    return kind == runtime::ProgressKind::kMailbox ? CollectNotifications(budget)
                                                   : std::expected<runtime::ProgressBatch, runtime::ProgressError>{};
}
std::expected<runtime::ProgressBatch, runtime::ProgressError>
ListenerEvents::CollectNotifications(std::size_t budget) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped || !budget) {
        return runtime::ProgressBatch{};
    }
    std::array<epoll_event, KBS_LISTENER_MAX_EVENT_BATCH> events{};
    const auto limit = std::min(budget, events.size());
    const int count = ::epoll_wait(state.epoll, events.data(), static_cast<int>(limit), 0);
    if (count < 0) {
        return errno == EINTR ? std::expected<runtime::ProgressBatch, runtime::ProgressError>(
                                    runtime::ProgressBatch{0, state.head != kNone})
                              : Error(ProgressErrorCode::kSystem, errno);
    }
    for (int i = 0; i < count; ++i) {
        auto* slot = state.Lookup(events[i].data.u64);
        if (!slot || !slot->armed) {
            continue;
        }
        slot->armed = false;
        const auto mask = events[i].events;
        if (mask & EPOLLIN) {
            slot->flags |= KBS_LISTENER_EVENT_ACCEPTABLE;
        }
        if (mask & EPOLLERR) {
            slot->flags |= KBS_LISTENER_EVENT_ERROR;
        }
        if (mask & EPOLLHUP) {
            slot->flags |= KBS_LISTENER_EVENT_HANGUP;
        }
        if (slot->flags) {
            state.Enqueue(*slot);
        }
    }
    return runtime::ProgressBatch{static_cast<std::size_t>(count),
                                  state.head != kNone || count == static_cast<int>(limit)};
}
std::expected<void, runtime::ProgressError> ListenerEvents::Rearm() noexcept {
    // 应用 Watch 独占一次性订阅；Reactor rearm 不能使监听器自行重新开始接收提示。
    return state_ ? std::expected<void, runtime::ProgressError>{} : Error(ProgressErrorCode::kNotReady);
}
std::span<const int> ListenerEvents::wait_fds() const noexcept {
    return state_ ? std::span<const int>(&state_->epoll, 1) : std::span<const int>{};
}
bool ListenerEvents::WaitFdEnabled(int fd) const noexcept {
    return state_ && !state_->stopped && fd == state_->epoll;
}
} // namespace api
} // namespace kbsocket
