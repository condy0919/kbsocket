// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_command_queue.hpp"

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
namespace runtime {
namespace {
constexpr std::uint64_t kPhaseBits = 3;
constexpr std::uint64_t kPhaseMask = (1u << kPhaseBits) - 1;
constexpr std::uint64_t kFree = 0;
constexpr std::uint64_t kPublished = 1;
constexpr std::uint64_t kExecuting = 2;
constexpr std::uint64_t kComplete = 3;
constexpr std::uint64_t kTaking = 4;
constexpr std::size_t kPayloadAlignment = 64;
struct PayloadDeleter {
    void operator()(std::byte* pointer) const noexcept {
        ::operator delete[](pointer, std::align_val_t{kPayloadAlignment});
    }
};

auto Error(CommandQueueErrorCode code, int status = 0) noexcept {
    return std::unexpected(CommandQueueError{.code = code, .system_error = status});
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
bool ValidCommand(const ConnectionCommand& command, std::span<const std::byte> input,
                  std::size_t max_io_bytes) noexcept {
    if (!command.connection.owner || !command.connection.generation) {
        return false;
    }
    if (command.kind != ConnectionCommandKind::kWatchTcp && (command.readable || command.writable)) {
        return false;
    }
    switch (command.kind) {
    case ConnectionCommandKind::kWrite:
        return command.bytes > 0 && command.bytes <= max_io_bytes && input.size() == command.bytes;
    case ConnectionCommandKind::kRead:
        return command.bytes > 0 && command.bytes <= max_io_bytes && input.empty();
    case ConnectionCommandKind::kShutdownWrite:
    case ConnectionCommandKind::kCancel:
    case ConnectionCommandKind::kInspect:
    case ConnectionCommandKind::kWatchTcp:
        return command.bytes == 0 && input.empty();
    }
    return false;
}
bool ValidReply(const ConnectionCommand& command, const ConnectionCommandResult& result) noexcept {
    if (!result) {
        return true;
    }
    switch (command.kind) {
    case ConnectionCommandKind::kWrite:
        return result->bytes <= command.bytes && !result->eof && !result->status;
    case ConnectionCommandKind::kRead:
        return result->bytes <= command.bytes && !result->status;
    case ConnectionCommandKind::kInspect:
        return result->bytes == 0 && !result->eof && result->status.has_value();
    case ConnectionCommandKind::kShutdownWrite:
    case ConnectionCommandKind::kCancel:
    case ConnectionCommandKind::kWatchTcp:
        return result->bytes == 0 && !result->eof && !result->status;
    }
    return false;
}
} // namespace

struct ConnectionCommandQueue::State {
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    struct alignas(64) Slot {
        std::atomic<std::uint64_t> tag{0};
        ConnectionCommand command;
        ConnectionCommandResult result;
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
    std::size_t capacity = 0, max_io_bytes = 0;
    int fd = -1;

    ~State() {
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

ConnectionCommandQueue::ConnectionCommandQueue() noexcept = default;
ConnectionCommandQueue::~ConnectionCommandQueue() {
    // 空快照不能证明 producer/collector 已退出；只由 Close(true) 释放可并发访问的状态。
    if (state_) {
        (void)state_.release();
    }
}

std::expected<void, CommandQueueError> ConnectionCommandQueue::Open(std::size_t requested_capacity,
                                                                    std::size_t max_io_bytes,
                                                                    std::size_t payload_budget_bytes) noexcept {
    if (state_) {
        return Error(CommandQueueErrorCode::kInUse);
    }
    if (opened_once_) {
        return Error(CommandQueueErrorCode::kStopped);
    }
    if (!requested_capacity || requested_capacity > kMaxCapacity || !max_io_bytes || max_io_bytes > kMaxPayloadBytes ||
        !payload_budget_bytes) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
    }
    const auto capacity = std::bit_ceil(std::max(std::size_t{2}, requested_capacity));
    // max_io_bytes 已限制在 1 MiB 以内；整环对齐与每槽对齐避免不同生产者写入同一缓存行。
    const auto stride = (max_io_bytes + kPayloadAlignment - 1) / kPayloadAlignment * kPayloadAlignment;
    // 向上取整后再检查预算，除法同时拒绝乘法溢出，失败不占用单次 Open 身份。
    if (stride > payload_budget_bytes / capacity) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
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
        return Error(CommandQueueErrorCode::kNoMemory);
    }
    state->capacity = capacity;
    state->max_io_bytes = max_io_bytes;
    for (std::size_t i = 0; i < capacity; ++i) {
        state->slots[i].payload = state->payload.get() + i * stride;
        state->slots[i].tag.store(Tag(i, kFree), std::memory_order_relaxed);
    }
    state->fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (state->fd < 0) {
        return Error(CommandQueueErrorCode::kSystem, errno);
    }
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}

std::expected<ConnectionCommandTicket, CommandQueueError>
ConnectionCommandQueue::TrySubmit(const ConnectionCommand& command, std::span<const std::byte> input) noexcept {
    if (!state_) {
        return Error(CommandQueueErrorCode::kNotReady);
    }
    if (!ValidCommand(command, input, state_->max_io_bytes)) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    auto position = state.producer.next.load(std::memory_order_relaxed);
    for (;;) {
        if (!state.producer.accepting.load(std::memory_order_acquire)) {
            return Error(CommandQueueErrorCode::kStopped);
        }
        // 连同归还阶段一起禁止回绕，旧 ticket 不会重新命中新请求。
        if (position >= (std::numeric_limits<std::uint64_t>::max() >> kPhaseBits) - state.capacity) {
            return Error(CommandQueueErrorCode::kSequenceExhausted);
        }
        auto& slot = state.slots[position & (state.capacity - 1)];
        const auto tag = slot.tag.load(std::memory_order_acquire);
        if (tag < Tag(position, kFree)) {
            return Error(CommandQueueErrorCode::kFull);
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
        if (!input.empty()) {
            std::memcpy(slot.payload, input.data(), input.size());
        }
        slot.tag.store(Tag(position, kPublished), std::memory_order_release);
        Signal(state.fd);
        return ConnectionCommandTicket{.queue = this, .sequence = position + 1};
    }
}

std::expected<std::size_t, CommandQueueError>
ConnectionCommandQueue::PollCommands(std::span<ConnectionCommandRequest> requests) noexcept {
    if (!state_) {
        return Error(CommandQueueErrorCode::kNotReady);
    }
    if (requests.empty()) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    std::uint64_t notification;
    const auto result = ::read(state.fd, &notification, sizeof(notification));
    if (result < 0 && errno != EAGAIN && errno != EINTR) {
        return Error(CommandQueueErrorCode::kSystem, errno);
    }
    std::size_t count = 0;
    while (count < requests.size()) {
        const auto position = state.owner.next;
        auto& slot = state.slots[position & (state.capacity - 1)];
        if (slot.tag.load(std::memory_order_acquire) != Tag(position, kPublished)) {
            break;
        }
        requests[count++] = ConnectionCommandRequest{.ticket = {.queue = this, .sequence = position + 1},
                                                     .command = slot.command,
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

std::expected<void, CommandQueueError>
ConnectionCommandQueue::Complete(ConnectionCommandTicket ticket, const ConnectionCommandResult& result) noexcept {
    if (!state_) {
        return Error(CommandQueueErrorCode::kNotReady);
    }
    if (ticket.queue != this || !ticket.sequence) {
        return Error(CommandQueueErrorCode::kInvalidTicket);
    }
    const auto position = ticket.sequence - 1;
    if (position >= state_->owner.next) {
        return Error(CommandQueueErrorCode::kInvalidTicket);
    }
    auto& slot = state_->slots[position & (state_->capacity - 1)];
    if (slot.tag.load(std::memory_order_acquire) != Tag(position, kExecuting)) {
        return Error(CommandQueueErrorCode::kInvalidTicket);
    }
    if (!ValidReply(slot.command, result)) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
    }
    slot.result = result;
    slot.tag.store(Tag(position, kComplete), std::memory_order_release);
    return {};
}

std::expected<ConnectionCommandCompletion, CommandQueueError>
ConnectionCommandQueue::TakeCompletion(ConnectionCommandTicket ticket, std::span<std::byte> output) noexcept {
    if (!state_) {
        return Error(CommandQueueErrorCode::kNotReady);
    }
    if (ticket.queue != this || !ticket.sequence) {
        return Error(CommandQueueErrorCode::kInvalidTicket);
    }
    auto& state = *state_;
    const auto position = ticket.sequence - 1;
    if (position >= state.producer.next.load(std::memory_order_acquire)) {
        return Error(CommandQueueErrorCode::kInvalidTicket);
    }
    auto& slot = state.slots[position & (state.capacity - 1)];
    auto expected = Tag(position, kComplete);
    if (!slot.tag.compare_exchange_strong(expected, Tag(position, kTaking), std::memory_order_acquire)) {
        if ((expected >> kPhaseBits) != position) {
            return Error(CommandQueueErrorCode::kInvalidTicket);
        }
        return Error((expected & kPhaseMask) == kFree ? CommandQueueErrorCode::kInvalidTicket
                                                      : CommandQueueErrorCode::kPending);
    }
    const auto bytes = slot.command.kind == ConnectionCommandKind::kRead && slot.result ? slot.result->bytes : 0;
    if (output.size() < bytes) {
        slot.tag.store(Tag(position, kComplete), std::memory_order_release);
        return Error(CommandQueueErrorCode::kOutputTooSmall);
    }
    ConnectionCommandCompletion completion{.ticket = ticket, .command = slot.command, .result = slot.result};
    if (bytes) {
        std::memcpy(output.data(), slot.payload, bytes);
    }
    state.accounting.outstanding.fetch_sub(1, std::memory_order_relaxed);
    slot.tag.store(Tag(position + state.capacity, kFree), std::memory_order_release);
    return completion;
}

void ConnectionCommandQueue::StopAccepting() noexcept {
    if (state_) {
        state_->producer.accepting.store(false, std::memory_order_release);
    }
}
std::expected<void, CommandQueueError> ConnectionCommandQueue::Close(bool clients_stopped) noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (!clients_stopped) {
        return Error(CommandQueueErrorCode::kNotQuiescent);
    }
    if (state_->accounting.outstanding.load(std::memory_order_acquire) ||
        state_->producer.next.load(std::memory_order_acquire) != state_->owner.next) {
        return Error(CommandQueueErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
int ConnectionCommandQueue::command_fd() const noexcept {
    return state_ ? state_->fd : -1;
}
std::size_t ConnectionCommandQueue::capacity() const noexcept {
    return state_ ? state_->capacity : 0;
}
std::size_t ConnectionCommandQueue::max_io_bytes() const noexcept {
    return state_ ? state_->max_io_bytes : 0;
}
std::size_t ConnectionCommandQueue::outstanding() const noexcept {
    return state_ ? state_->accounting.outstanding.load(std::memory_order_acquire) : 0;
}
bool ConnectionCommandQueue::accepting() const noexcept {
    return state_ && state_->producer.accepting.load(std::memory_order_acquire);
}
bool ConnectionCommandQueue::runnable() const noexcept {
    if (!state_) {
        return false;
    }
    const auto position = state_->owner.next;
    return state_->slots[position & (state_->capacity - 1)].tag.load(std::memory_order_acquire) ==
           Tag(position, kPublished);
}

} // namespace runtime
} // namespace kbsocket
