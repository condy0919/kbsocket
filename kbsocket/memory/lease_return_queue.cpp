// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/memory/lease_return_queue.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace memory {
namespace {
auto Error(MemoryErrorCode code, int status = 0) noexcept {
    return std::unexpected(
        MemoryError{.code = code, .provider_error = status, .context_index = 0, .cleanup_error = {}});
}
void Signal(int fd) noexcept {
    const std::uint64_t one = 1;
    ssize_t count;
    do {
        count = ::write(fd, &one, sizeof(one));
    } while (count < 0 && errno == EINTR);
    // EAGAIN 表示本来已可读。FD 属于队列，其他错误需外部破坏生命周期才会发生。
    // 已发布后不能再返回“提交失败”，否则调用方重试会重复转移同一 CPU 所有权。
}
} // namespace

struct LeaseReturnQueue::State {
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    struct alignas(64) Slot {
        std::atomic<std::uint64_t> sequence{0};
        BufferLease lease;
    };
    struct alignas(64) Producer {
        std::atomic<std::uint64_t> next{0};
        std::atomic<bool> accepting{true};
    } producer;
    struct alignas(64) Consumer {
        std::atomic<std::uint64_t> next{0};
    } consumer;
    std::unique_ptr<Slot[]> slots;
    std::size_t capacity = 0;
    int fd = -1;

    ~State() {
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

LeaseReturnQueue::LeaseReturnQueue() noexcept = default;
LeaseReturnQueue::~LeaseReturnQueue() {
    // 无法从“此刻空”推断 producer 已退出，必须由显式 Close(true) 提供生命周期证明。
    if (state_) {
        (void)state_.release();
    }
}

std::expected<void, MemoryError> LeaseReturnQueue::Open(std::size_t max_outstanding_leases) noexcept {
    if (state_) {
        return Error(MemoryErrorCode::kInUse);
    }
    if (!max_outstanding_leases || max_outstanding_leases > kMaxCapacity) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    // 单槽位不能区分下一轮空槽序号和当前轮发布序号，最小采用两个槽位。
    const auto capacity = std::bit_ceil(std::max(std::size_t{2}, max_outstanding_leases));
    std::unique_ptr<State> state;
    try {
        state = std::make_unique<State>();
        state->slots = std::make_unique<State::Slot[]>(capacity);
    } catch (const std::bad_alloc&) {
        return Error(MemoryErrorCode::kNoMemory);
    }
    state->capacity = capacity;
    for (std::size_t i = 0; i < capacity; ++i) {
        state->slots[i].sequence.store(i, std::memory_order_relaxed);
    }
    state->fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (state->fd < 0) {
        return Error(MemoryErrorCode::kSystem, errno);
    }
    state_ = std::move(state);
    return {};
}

std::expected<void, MemoryError> LeaseReturnQueue::TryPush(BufferLease lease) noexcept {
    if (!state_) {
        return Error(MemoryErrorCode::kNotReady);
    }
    if (!lease.allocator || !lease.generation) {
        return Error(MemoryErrorCode::kInvalidLease);
    }
    auto& state = *state_;
    auto position = state.producer.next.load(std::memory_order_relaxed);
    for (;;) {
        if (!state.producer.accepting.load(std::memory_order_acquire)) {
            return Error(MemoryErrorCode::kStopped);
        }
        // 不让 publish(position+1) 或归还槽位(position+capacity) 回绕，拒绝生命周期内的 ABA。
        if (position >= std::numeric_limits<std::uint64_t>::max() - state.capacity) {
            return Error(MemoryErrorCode::kSequenceExhausted);
        }
        auto& slot = state.slots[position & (state.capacity - 1)];
        const auto sequence = slot.sequence.load(std::memory_order_acquire);
        if (sequence < position) {
            return Error(MemoryErrorCode::kQueueFull);
        }
        if (sequence != position) {
            position = state.producer.next.load(std::memory_order_relaxed);
            continue;
        }
        if (!state.producer.next.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
            continue;
        }
        slot.lease = lease;
        slot.sequence.store(position + 1, std::memory_order_release);
        Signal(state.fd);
        return {};
    }
}

std::expected<std::size_t, MemoryError> LeaseReturnQueue::Poll(std::span<BufferLease> leases) noexcept {
    if (!state_) {
        return Error(MemoryErrorCode::kNotReady);
    }
    if (leases.empty()) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    std::uint64_t notification;
    const auto read = ::read(state.fd, &notification, sizeof(notification));
    if (read < 0 && errno != EAGAIN && errno != EINTR) {
        return Error(MemoryErrorCode::kSystem, errno);
    }
    auto position = state.consumer.next.load(std::memory_order_relaxed);
    std::size_t count = 0;
    while (count < leases.size()) {
        auto& slot = state.slots[position & (state.capacity - 1)];
        if (slot.sequence.load(std::memory_order_acquire) != position + 1) {
            break;
        }
        leases[count++] = slot.lease;
        slot.sequence.store(position + state.capacity, std::memory_order_release);
        ++position;
        state.consumer.next.store(position, std::memory_order_release);
    }
    // 预算内未读完的头部重新置可读；若是 producer 留下的未发布空洞，由其发布后唤醒。
    // 这里不再次清通知，因此扫描期间并发发布的唤醒不会被吞掉。
    auto& head = state.slots[position & (state.capacity - 1)];
    if (head.sequence.load(std::memory_order_acquire) == position + 1) {
        Signal(state.fd);
    }
    return count;
}

void LeaseReturnQueue::StopAccepting() noexcept {
    if (state_) {
        state_->producer.accepting.store(false, std::memory_order_release);
    }
}

std::expected<void, MemoryError> LeaseReturnQueue::Close(bool producers_stopped) noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (!producers_stopped) {
        return Error(MemoryErrorCode::kNotQuiescent);
    }
    if (state_->producer.next.load(std::memory_order_acquire) !=
        state_->consumer.next.load(std::memory_order_acquire)) {
        return Error(MemoryErrorCode::kInUse);
    }
    state_.reset();
    return {};
}

int LeaseReturnQueue::completion_fd() const noexcept {
    return state_ ? state_->fd : -1;
}
std::size_t LeaseReturnQueue::capacity() const noexcept {
    return state_ ? state_->capacity : 0;
}
std::size_t LeaseReturnQueue::pending() const noexcept {
    if (!state_) {
        return 0;
    }
    // 先读消费位置，随后生产位置至少能看到该消费所依赖的发布；并发完整循环时只取上限快照。
    const auto consumed = state_->consumer.next.load(std::memory_order_acquire);
    const auto produced = state_->producer.next.load(std::memory_order_acquire);
    return std::min<std::uint64_t>(produced - consumed, state_->capacity);
}
bool LeaseReturnQueue::accepting() const noexcept {
    return state_ && state_->producer.accepting.load(std::memory_order_acquire);
}

} // namespace memory
} // namespace kbsocket
