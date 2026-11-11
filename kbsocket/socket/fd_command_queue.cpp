// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/socket/fd_command_queue.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace socket {
namespace {
constexpr std::uint64_t kPhaseBits = 3;
constexpr std::uint64_t kPhaseMask = (1u << kPhaseBits) - 1;
constexpr std::uint64_t kFree = 0;
constexpr std::uint64_t kPublished = 1;
constexpr std::uint64_t kExecuting = 2;
constexpr std::uint64_t kComplete = 3;
constexpr std::uint64_t kTaking = 4;
constexpr std::size_t kPayloadAlignment = 64;
constexpr unsigned kSubmitAttempts = 64;
struct PayloadDeleter {
    void operator()(std::byte* pointer) const noexcept {
        ::operator delete[](pointer, std::align_val_t{kPayloadAlignment});
    }
};

auto Error(FdQueueErrorCode code, int status = 0) noexcept {
    return std::unexpected(FdQueueError{.code = code, .system_error = status});
}
std::uint64_t Tag(std::uint64_t position, std::uint64_t phase) noexcept {
    return (position << kPhaseBits) | phase;
}
void Signal(int fd) noexcept {
    const std::uint64_t one = 1;
    ssize_t result;
    do {
        result = ::write(fd, &one, sizeof(one));
    } while (result < 0 && errno == EINTR);
    // EAGAIN 意味着已经可读；发布后不能再报告提交失败，避免调用方重复提交。
}
bool ValidCommand(const FdCommand& command, std::span<const std::byte> input, std::size_t max_io_bytes) noexcept {
    if (command.kind != FdCommandKind::kWatch && (command.readable || command.writable)) {
        return false;
    }
    switch (command.kind) {
    case FdCommandKind::kWrite:
        return command.bytes <= max_io_bytes && input.size() == command.bytes;
    case FdCommandKind::kRead:
        return command.bytes <= max_io_bytes && input.empty();
    case FdCommandKind::kShutdownWrite:
    case FdCommandKind::kInspect:
    case FdCommandKind::kClose:
    case FdCommandKind::kWatch:
        return command.bytes == 0 && input.empty();
    }
    return false;
}
bool ValidReply(const FdCommand& command, const FdCommandResult& result) noexcept {
    if (!result) {
        return result.error().code > 0 && !result.error().reserved;
    }
    if (result->io.reserved || result->io.eof > 1) {
        return false;
    }
    switch (command.kind) {
    case FdCommandKind::kWrite:
        return result->io.bytes <= command.bytes && !result->io.eof;
    case FdCommandKind::kRead:
        return result->io.bytes <= command.bytes && (command.bytes || !result->io.eof);
    case FdCommandKind::kInspect:
    case FdCommandKind::kShutdownWrite:
    case FdCommandKind::kClose:
    case FdCommandKind::kWatch:
        return result->io.bytes == 0 && !result->io.eof;
    }
    return false;
}
} // namespace

struct FdCommandQueue::State {
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    struct alignas(64) Slot {
        std::atomic<std::uint64_t> tag{0};
        FdCommand command;
        FdLease lease;
        FdCommandResult result;
        // Open 后不再改变地址；原有 tag 的发布/领取同步同时保护这一段 payload。
        std::byte* payload = nullptr;
    };
    struct alignas(64) Producer {
        std::atomic<std::uint64_t> next{0};
        std::atomic<bool> accepting{true};
    } producer;
    struct alignas(64) Owner {
        std::uint64_t next = 0;
    } owner;
    struct alignas(64) Accounting {
        std::atomic<std::uint64_t> outstanding{0};
    } accounting;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<std::byte[], PayloadDeleter> payload;
    std::uint64_t queue_id = 0;
    std::size_t capacity = 0, max_io_bytes = 0;
    int fd = -1;

    ~State() {
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

FdCommandQueue::FdCommandQueue() noexcept = default;
FdCommandQueue::~FdCommandQueue() {
    StopAccepting();
    // 空快照不能证明 producer/collector 已退出；只由 Close(true) 释放可并发访问的状态。
    if (state_) {
        (void)state_.release();
    }
}

std::expected<void, FdQueueError> FdCommandQueue::Open(const FdCommandQueueConfig& config) noexcept {
    const auto requested_capacity = config.capacity;
    const auto max_io_bytes = config.max_io_bytes;
    const auto payload_budget_bytes = config.payload_budget_bytes;
    if (state_) {
        return Error(FdQueueErrorCode::kInUse);
    }
    if (opened_once_) {
        return Error(FdQueueErrorCode::kStopped);
    }
    if (!config.queue_id || !requested_capacity || requested_capacity > kMaxCapacity || !max_io_bytes ||
        max_io_bytes > kMaxPayloadBytes || !payload_budget_bytes) {
        return Error(FdQueueErrorCode::kInvalidArgument);
    }
    const auto capacity = std::bit_ceil(std::max(std::size_t{2}, requested_capacity));
    // max_io_bytes 已限制在 1 MiB 以内；整环对齐与每槽对齐避免不同生产者写入同一缓存行。
    const auto stride = (max_io_bytes + kPayloadAlignment - 1) / kPayloadAlignment * kPayloadAlignment;
    // 向上取整后再检查预算，除法同时拒绝乘法溢出，失败不占用单次 Open 身份。
    if (stride > payload_budget_bytes / capacity) {
        return Error(FdQueueErrorCode::kInvalidArgument);
    }
    std::unique_ptr<State> state;
    try {
        state = std::make_unique<State>();
        state->slots = std::make_unique<State::Slot[]>(capacity);
        state->payload.reset(
            static_cast<std::byte*>(::operator new[](capacity * stride, std::align_val_t{kPayloadAlignment})));
        // 控制路径预触页，后续搬运不触发分配或首次清零。
        std::memset(state->payload.get(), 0, capacity * stride);
    } catch (const std::bad_alloc&) {
        return Error(FdQueueErrorCode::kNoMemory);
    }
    state->queue_id = config.queue_id;
    state->capacity = capacity;
    state->max_io_bytes = max_io_bytes;
    for (std::size_t i = 0; i < capacity; ++i) {
        state->slots[i].payload = state->payload.get() + i * stride;
        state->slots[i].tag.store(Tag(i, kFree), std::memory_order_relaxed);
    }
    state->fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (state->fd < 0) {
        return Error(FdQueueErrorCode::kSystem, errno);
    }
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}

std::expected<FdCommandTicket, FdQueueError> FdCommandQueue::TrySubmit(FdLease& lease, const FdCommand& command,
                                                                       std::span<const std::byte> input) noexcept {
    if (!state_) {
        return Error(FdQueueErrorCode::kNotReady);
    }
    if (!lease || !ValidCommand(command, input, state_->max_io_bytes)) {
        return Error(FdQueueErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    auto position = state.producer.next.load(std::memory_order_relaxed);
    for (unsigned attempt = 0; attempt < kSubmitAttempts; ++attempt) {
        if (!state.producer.accepting.load(std::memory_order_acquire)) {
            return Error(FdQueueErrorCode::kStopped);
        }
        // 连同归还阶段一起禁止回绕，旧 ticket 不会重新命中新请求。
        if (position >= (std::numeric_limits<std::uint64_t>::max() >> kPhaseBits) - state.capacity) {
            return Error(FdQueueErrorCode::kSequenceExhausted);
        }
        auto& slot = state.slots[position & (state.capacity - 1)];
        const auto tag = slot.tag.load(std::memory_order_acquire);
        if (tag < Tag(position, kFree)) {
            return Error(FdQueueErrorCode::kFull);
        }
        if (tag != Tag(position, kFree)) {
            position = state.producer.next.load(std::memory_order_relaxed);
            continue;
        }
        if (!state.producer.next.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
            continue;
        }
        state.accounting.outstanding.fetch_add(1, std::memory_order_relaxed);
        slot.command = command;
        slot.lease = std::move(lease);
        if (!input.empty()) {
            std::memcpy(slot.payload, input.data(), input.size());
        }
        slot.tag.store(Tag(position, kPublished), std::memory_order_release);
        Signal(state.fd);
        return FdCommandTicket{.queue_id = state.queue_id, .sequence = position + 1};
    }
    return Error(FdQueueErrorCode::kContended);
}

std::expected<std::size_t, FdQueueError> FdCommandQueue::PollCommands(std::span<FdCommandRequest> requests) noexcept {
    if (!state_) {
        return Error(FdQueueErrorCode::kNotReady);
    }
    if (requests.empty()) {
        return Error(FdQueueErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    std::uint64_t notification;
    const auto result = ::read(state.fd, &notification, sizeof(notification));
    if (result < 0 && errno != EAGAIN && errno != EINTR) {
        return Error(FdQueueErrorCode::kSystem, errno);
    }
    std::size_t count = 0;
    while (count < std::min(requests.size(), kMaxPollBatch)) {
        const auto position = state.owner.next;
        auto& slot = state.slots[position & (state.capacity - 1)];
        if (slot.tag.load(std::memory_order_acquire) != Tag(position, kPublished)) {
            break;
        }
        requests[count++] = FdCommandRequest{.ticket = {.queue_id = state.queue_id, .sequence = position + 1},
                                             .command = slot.command,
                                             .binding = slot.lease.binding(),
                                             .payload = {slot.payload, slot.command.bytes}};
        slot.tag.store(Tag(position, kExecuting), std::memory_order_release);
        ++state.owner.next;
    }
    // 未发布的预留空洞不能阻塞 owner；由其 producer 发布时唤醒。
    // 预算耗尽留下的连续前缀重新保留 readiness，扫描后不再次清空通知。
    if (runnable()) {
        Signal(state.fd);
    }
    return count;
}

std::expected<void, FdQueueError> FdCommandQueue::Complete(FdCommandTicket ticket,
                                                           const FdCommandResult& result) noexcept {
    if (!state_) {
        return Error(FdQueueErrorCode::kNotReady);
    }
    if (ticket.queue_id != state_->queue_id || !ticket.sequence) {
        return Error(FdQueueErrorCode::kInvalidTicket);
    }
    const auto position = ticket.sequence - 1;
    if (position >= state_->owner.next) {
        return Error(FdQueueErrorCode::kInvalidTicket);
    }
    auto& slot = state_->slots[position & (state_->capacity - 1)];
    if (slot.tag.load(std::memory_order_acquire) != Tag(position, kExecuting)) {
        return Error(FdQueueErrorCode::kInvalidTicket);
    }
    if (!ValidReply(slot.command, result)) {
        return Error(FdQueueErrorCode::kInvalidArgument);
    }
    slot.result = result;
    slot.tag.store(Tag(position, kComplete), std::memory_order_release);
    return {};
}

std::expected<FdCommandCompletion, FdQueueError> FdCommandQueue::TakeCompletion(FdCommandTicket ticket,
                                                                                std::span<std::byte> output) noexcept {
    if (!state_) {
        return Error(FdQueueErrorCode::kNotReady);
    }
    if (ticket.queue_id != state_->queue_id || !ticket.sequence) {
        return Error(FdQueueErrorCode::kInvalidTicket);
    }
    auto& state = *state_;
    const auto position = ticket.sequence - 1;
    if (position >= state.producer.next.load(std::memory_order_acquire)) {
        return Error(FdQueueErrorCode::kInvalidTicket);
    }
    auto& slot = state.slots[position & (state.capacity - 1)];
    auto expected = Tag(position, kComplete);
    if (!slot.tag.compare_exchange_strong(expected, Tag(position, kTaking), std::memory_order_acquire)) {
        if ((expected >> kPhaseBits) != position) {
            return Error(FdQueueErrorCode::kInvalidTicket);
        }
        return Error((expected & kPhaseMask) == kFree ? FdQueueErrorCode::kInvalidTicket : FdQueueErrorCode::kPending);
    }
    const auto bytes = slot.command.kind == FdCommandKind::kRead && slot.result ? slot.result->io.bytes : 0;
    if (output.size() < bytes) {
        slot.tag.store(Tag(position, kComplete), std::memory_order_release);
        return Error(FdQueueErrorCode::kOutputTooSmall);
    }
    FdCommandCompletion completion{
        .ticket = ticket, .command = slot.command, .binding = slot.lease.binding(), .result = slot.result};
    if (bytes) {
        std::memcpy(output.data(), slot.payload, bytes);
    }
    slot.lease.Reset();
    state.accounting.outstanding.fetch_sub(1, std::memory_order_relaxed);
    slot.tag.store(Tag(position + state.capacity, kFree), std::memory_order_release);
    return completion;
}

void FdCommandQueue::StopAccepting() noexcept {
    if (state_) {
        state_->producer.accepting.store(false, std::memory_order_release);
    }
}
std::expected<void, FdQueueError> FdCommandQueue::Close(bool clients_stopped) noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (!clients_stopped) {
        return Error(FdQueueErrorCode::kNotQuiescent);
    }
    if (state_->accounting.outstanding.load(std::memory_order_acquire) ||
        state_->producer.next.load(std::memory_order_acquire) != state_->owner.next) {
        return Error(FdQueueErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
int FdCommandQueue::command_fd() const noexcept {
    return state_ ? state_->fd : -1;
}
std::size_t FdCommandQueue::capacity() const noexcept {
    return state_ ? state_->capacity : 0;
}
std::size_t FdCommandQueue::max_io_bytes() const noexcept {
    return state_ ? state_->max_io_bytes : 0;
}
std::size_t FdCommandQueue::outstanding() const noexcept {
    return state_ ? state_->accounting.outstanding.load(std::memory_order_acquire) : 0;
}
bool FdCommandQueue::accepting() const noexcept {
    return state_ && state_->producer.accepting.load(std::memory_order_acquire);
}
bool FdCommandQueue::runnable() const noexcept {
    if (!state_) {
        return false;
    }
    const auto position = state_->owner.next;
    return state_->slots[position & (state_->capacity - 1)].tag.load(std::memory_order_acquire) ==
           Tag(position, kPublished);
}

} // namespace socket
} // namespace kbsocket
