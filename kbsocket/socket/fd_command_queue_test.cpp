// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/socket/fd_command_queue.hpp"

#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <limits>
#include <new>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
thread_local int fail_allocation_after = -1;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
    }
    if (fail_allocation_after == 0) {
        fail_allocation_after = -1;
        throw std::bad_alloc();
    }
    if (fail_allocation_after > 0) {
        --fail_allocation_after;
    }
}
void RecordDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocations;
    }
}
} // namespace

void* operator new(std::size_t bytes) {
    RecordAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    RecordAllocation();
    void* pointer = nullptr;
    if (::posix_memalign(&pointer, static_cast<std::size_t>(alignment), bytes ? bytes : 1) != 0) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) {
    return ::operator new(bytes, alignment);
}
void operator delete(void* pointer) noexcept {
    RecordDeallocation(pointer);
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace socket {
namespace {
template <class T>
void ExpectError(const std::expected<T, FdQueueError>& value, FdQueueErrorCode code) {
    ASSERT_FALSE(value);
    EXPECT_EQ(value.error().code, code);
}
bool Ready(int fd) {
    pollfd item{fd, POLLIN, 0};
    return ::poll(&item, 1, 0) == 1 && (item.revents & POLLIN);
}
FdCommandResult Failed(int code = KBS_STREAM_CANCELLED, int underlying = 0) {
    return std::unexpected(kbs_stream_error{code, KBS_STREAM_ORIGIN_CONNECTION, 23, 7, underlying, 0});
}
class FdCommandQueueTest : public ::testing::Test {
protected:
    FdTable table_;
    FdCommandQueue queue_;
    FdBindingId binding_;
    FdReader reader_;
    std::vector<FdCommandTicket> tickets_;
    FdCommandQueueConfig config_{.queue_id = 72, .capacity = 4, .max_io_bytes = 16, .payload_budget_bytes = 256};

    void SetUp() override {
        ASSERT_TRUE(table_.Open({.table_id = 71, .fd_limit = 32, .binding_capacity = 4, .reader_capacity = 8}));
        const auto published = table_.Publish(7, {73, {74, 75, 0, 0}});
        ASSERT_TRUE(published);
        binding_ = *published;
        auto reader = table_.RegisterReader();
        ASSERT_TRUE(reader);
        reader_ = std::move(*reader);
        tickets_.reserve(256);
    }
    void Open() {
        ASSERT_TRUE(queue_.Open(config_));
    }
    std::expected<FdCommandTicket, FdQueueError> Submit(FdCommand command, std::span<const std::byte> input = {}) {
        auto lease = table_.Lookup(reader_, binding_);
        if (!lease) {
            return std::unexpected(FdQueueError{FdQueueErrorCode::kInvalidArgument});
        }
        auto result = queue_.TrySubmit(*lease, command, input);
        if (result) {
            tickets_.push_back(*result);
        }
        return result;
    }
    void TearDown() override {
        count_allocations = false;
        fail_allocation_after = -1;
        queue_.StopAccepting();
        if (queue_.capacity()) {
            std::array<FdCommandRequest, 64> requests;
            for (;;) {
                const auto count = queue_.PollCommands(requests);
                ASSERT_TRUE(count);
                if (!*count) {
                    break;
                }
                for (std::size_t i = 0; i < *count; ++i) {
                    ASSERT_TRUE(queue_.Complete(requests[i].ticket, Failed()));
                }
            }
            std::array<std::byte, 16> output;
            for (const auto ticket : tickets_) {
                auto taken = queue_.TakeCompletion(ticket, output);
                if (!taken && taken.error().code == FdQueueErrorCode::kPending) {
                    // 致命断言后仍可能有已取出而尚未完成的模拟请求；只补全测试取消结果。
                    ASSERT_TRUE(queue_.Complete(ticket, Failed()));
                    taken = queue_.TakeCompletion(ticket, output);
                    ASSERT_TRUE(taken);
                }
            }
            EXPECT_EQ(queue_.outstanding(), 0u);
        }
        EXPECT_TRUE(queue_.Close(true));
        reader_.Reset();
        const auto retired = table_.Unbind(binding_);
        if (retired) {
            ASSERT_TRUE(table_.ConfirmClosed(*retired));
        }
        std::array<FdBinding, 4> output;
        ASSERT_TRUE(table_.Collect(output, 4));
        EXPECT_TRUE(table_.Close(true));
    }
};

TEST_F(FdCommandQueueTest, EmptyQueueRejectsOperationsAndCloseIsIdempotent) {
    FdLease lease;
    std::array<FdCommandRequest, 1> requests;
    ExpectError(queue_.TrySubmit(lease, {}), FdQueueErrorCode::kNotReady);
    ExpectError(queue_.PollCommands(requests), FdQueueErrorCode::kNotReady);
    ExpectError(queue_.Complete({}, Failed()), FdQueueErrorCode::kNotReady);
    ExpectError(queue_.TakeCompletion({}), FdQueueErrorCode::kNotReady);
    EXPECT_EQ(queue_.command_fd(), -1);
    EXPECT_EQ(queue_.capacity(), 0u);
    EXPECT_EQ(queue_.outstanding(), 0u);
    EXPECT_FALSE(queue_.accepting());
    EXPECT_FALSE(queue_.runnable());
    EXPECT_TRUE(queue_.Close());
}

TEST_F(FdCommandQueueTest, InvalidConfigurationAndAlignedBudgetDoNotConsumeOpenIdentity) {
    for (unsigned field = 0; field < 6; ++field) {
        auto invalid = config_;
        switch (field) {
        case 0:
            invalid.queue_id = 0;
            break;
        case 1:
            invalid.capacity = 0;
            break;
        case 2:
            invalid.capacity = FdCommandQueue::kMaxCapacity + 1;
            break;
        case 3:
            invalid.max_io_bytes = 0;
            break;
        case 4:
            invalid.max_io_bytes = FdCommandQueue::kMaxPayloadBytes + 1;
            break;
        case 5:
            invalid.payload_budget_bytes = 0;
            break;
        }
        ExpectError(queue_.Open(invalid), FdQueueErrorCode::kInvalidArgument);
    }
    // 三槽实际取整到四槽；单槽 65 字节实际占 128 字节，不能按原请求绕过硬预算。
    config_.capacity = 3;
    config_.max_io_bytes = 65;
    config_.payload_budget_bytes = 511;
    ExpectError(queue_.Open(config_), FdQueueErrorCode::kInvalidArgument);
    config_.payload_budget_bytes = 512;
    ASSERT_NO_FATAL_FAILURE(Open());
    EXPECT_EQ(queue_.capacity(), 4u);
    EXPECT_EQ(queue_.max_io_bytes(), 65u);
    EXPECT_FALSE(Ready(queue_.command_fd()));
    ExpectError(queue_.Open(config_), FdQueueErrorCode::kInUse);
    ASSERT_TRUE(queue_.Close(true));
    ExpectError(queue_.Open(config_), FdQueueErrorCode::kStopped);
}

TEST_F(FdCommandQueueTest, EachAllocationFailureRollsBackBeforeRetry) {
    // State、槽数组、对齐 payload 三个分配点失败时均不能留下半开的队列或占用 Open 身份。
    for (int allocation = 0; allocation < 3; ++allocation) {
        fail_allocation_after = allocation;
        const auto opened = queue_.Open(config_);
        fail_allocation_after = -1;
        ExpectError(opened, FdQueueErrorCode::kNoMemory);
        EXPECT_EQ(queue_.command_fd(), -1);
        EXPECT_EQ(queue_.capacity(), 0u);
    }
    ASSERT_NO_FATAL_FAILURE(Open());
}

TEST_F(FdCommandQueueTest, SubmissionCopiesInputAndPinsOriginalBindingUntilResultIsTaken) {
    ASSERT_NO_FATAL_FAILURE(Open());
    std::array input{std::byte{1}, std::byte{2}, std::byte{3}};
    auto lease = table_.Lookup(reader_, binding_);
    ASSERT_TRUE(lease);
    const auto ticket = queue_.TrySubmit(*lease, {.kind = FdCommandKind::kWrite, .token = 99, .bytes = 3}, input);
    ASSERT_TRUE(ticket);
    tickets_.push_back(*ticket);
    EXPECT_FALSE(*lease);
    input.fill(std::byte{8});
    EXPECT_EQ(table_.stats().references, 1u);
    EXPECT_EQ(queue_.outstanding(), 1u);
    ExpectError(queue_.TakeCompletion(*ticket), FdQueueErrorCode::kPending);
    const auto retired = table_.Unbind(binding_);
    ASSERT_TRUE(retired);
    ASSERT_TRUE(table_.ConfirmClosed(*retired));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 4), 0u);
    std::array<FdCommandRequest, 1> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 1u);
    EXPECT_EQ(requests[0].binding.id, binding_);
    EXPECT_EQ(requests[0].binding.target.stream.table_id, 74u);
    EXPECT_EQ(requests[0].payload[0], std::byte{1});
    EXPECT_EQ(requests[0].payload[1], std::byte{2});
    EXPECT_EQ(requests[0].payload[2], std::byte{3});
    ASSERT_TRUE(queue_.Complete(*ticket, FdCommandReply{.io = {2, 0, 0}}));
    ASSERT_EQ(table_.Collect(output, 4), 0u);
    const auto completed = queue_.TakeCompletion(*ticket);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(completed->result);
    EXPECT_EQ(completed->result->io.bytes, 2u);
    EXPECT_EQ(completed->command.token, 99u);
    EXPECT_EQ(completed->binding.id, binding_);
    EXPECT_EQ(table_.stats().references, 0u);
    ASSERT_EQ(table_.Collect(output, 4), 1u);
    ExpectError(queue_.TakeCompletion(*ticket), FdQueueErrorCode::kInvalidTicket);
}

TEST_F(FdCommandQueueTest, FailedReservationPreservesLeaseAndCompletedSlotsRemainFull) {
    config_.capacity = 2;
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(Submit({}));
    ASSERT_TRUE(Submit({}));
    auto lease = table_.Lookup(reader_, binding_);
    ASSERT_TRUE(lease);
    ExpectError(queue_.TrySubmit(*lease, {}), FdQueueErrorCode::kFull);
    EXPECT_TRUE(*lease);
    std::array<FdCommandRequest, 2> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 2u);
    ASSERT_TRUE(queue_.Complete(requests[0].ticket, FdCommandReply{}));
    ASSERT_TRUE(queue_.Complete(requests[1].ticket, FdCommandReply{}));
    ExpectError(queue_.TrySubmit(*lease, {}), FdQueueErrorCode::kFull);
    EXPECT_TRUE(*lease);
    ASSERT_TRUE(queue_.TakeCompletion(requests[0].ticket));
    const auto third = queue_.TrySubmit(*lease, {});
    ASSERT_TRUE(third);
    tickets_.push_back(*third);
    EXPECT_FALSE(*lease);
    EXPECT_NE(third->sequence, requests[0].ticket.sequence);
    ExpectError(queue_.TakeCompletion(requests[0].ticket), FdQueueErrorCode::kInvalidTicket);
}

TEST_F(FdCommandQueueTest, ReadResultCanBeRetriedWithLargerOutputWithoutReexecution) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto ticket = Submit({.kind = FdCommandKind::kRead, .bytes = 8});
    ASSERT_TRUE(ticket);
    std::array<FdCommandRequest, 1> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 1u);
    const std::array payload{std::byte{4}, std::byte{5}, std::byte{6}};
    std::copy(payload.begin(), payload.end(), requests[0].payload.begin());
    ASSERT_TRUE(queue_.Complete(*ticket, FdCommandReply{.io = {3, 0, 0}}));
    std::array<std::byte, 2> short_output{};
    ExpectError(queue_.TakeCompletion(*ticket, short_output), FdQueueErrorCode::kOutputTooSmall);
    EXPECT_EQ(table_.stats().references, 1u);
    EXPECT_EQ(queue_.outstanding(), 1u);
    ASSERT_EQ(queue_.PollCommands(requests), 0u);
    std::array<std::byte, 8> output{};
    const auto completed = queue_.TakeCompletion(*ticket, output);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(completed->result);
    EXPECT_EQ(completed->result->io.bytes, 3u);
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), output.begin()));
    EXPECT_EQ(output[3], std::byte{0});
    EXPECT_EQ(table_.stats().references, 0u);
}

TEST_F(FdCommandQueueTest, FailureDiagnosticsAndEofAreDeliveredExactlyOnce) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto failed = Submit({.kind = FdCommandKind::kRead, .bytes = 4});
    const auto eof = Submit({.kind = FdCommandKind::kRead, .bytes = 4});
    ASSERT_TRUE(failed);
    ASSERT_TRUE(eof);
    std::array<FdCommandRequest, 2> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 2u);
    ASSERT_TRUE(queue_.Complete(*failed, Failed(KBS_STREAM_TRANSPORT, 9)));
    ASSERT_TRUE(queue_.Complete(*eof, FdCommandReply{.io = {0, 1, 0}}));
    const auto failure = queue_.TakeCompletion(*failed);
    ASSERT_TRUE(failure);
    ASSERT_FALSE(failure->result);
    EXPECT_EQ(failure->result.error().code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(failure->result.error().underlying, 9);
    EXPECT_EQ(failure->result.error().native_code, 23);
    const auto ended = queue_.TakeCompletion(*eof);
    ASSERT_TRUE(ended);
    ASSERT_TRUE(ended->result);
    EXPECT_EQ(ended->result->io.eof, 1u);
    EXPECT_EQ(ended->result->io.bytes, 0u);
    ExpectError(queue_.Complete(*eof, FdCommandReply{}), FdQueueErrorCode::kInvalidTicket);
    ExpectError(queue_.TakeCompletion(*eof), FdQueueErrorCode::kInvalidTicket);
}

TEST_F(FdCommandQueueTest, ZeroLengthCommandsAreValidAndMalformedCommandsLeaveLeaseUntouched) {
    ASSERT_NO_FATAL_FAILURE(Open());
    for (const auto kind : {FdCommandKind::kRead, FdCommandKind::kWrite}) {
        auto ticket = Submit({.kind = kind});
        ASSERT_TRUE(ticket);
    }
    auto lease = table_.Lookup(reader_, binding_);
    ASSERT_TRUE(lease);
    const std::array input{std::byte{1}};
    for (const auto command :
         {FdCommand{.kind = static_cast<FdCommandKind>(255)}, FdCommand{.kind = FdCommandKind::kRead, .bytes = 17},
          FdCommand{.kind = FdCommandKind::kInspect, .bytes = 1},
          FdCommand{.kind = FdCommandKind::kWrite, .bytes = 1}}) {
        ExpectError(queue_.TrySubmit(*lease, command), FdQueueErrorCode::kInvalidArgument);
        EXPECT_TRUE(*lease);
    }
    ExpectError(queue_.TrySubmit(*lease, {.kind = FdCommandKind::kRead, .bytes = 1}, input),
                FdQueueErrorCode::kInvalidArgument);
    EXPECT_TRUE(*lease);
    FdLease empty;
    ExpectError(queue_.TrySubmit(empty, {}), FdQueueErrorCode::kInvalidArgument);
    std::array<FdCommandRequest, 2> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 2u);
    for (const auto& request : requests) {
        EXPECT_TRUE(request.payload.empty());
        ASSERT_TRUE(queue_.Complete(request.ticket, FdCommandReply{}));
    }
}

TEST_F(FdCommandQueueTest, InvalidRepliesDoNotPublishOrDiscardExecutingRequest) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto write = Submit({.kind = FdCommandKind::kWrite});
    const auto read = Submit({.kind = FdCommandKind::kRead, .bytes = 4});
    ASSERT_TRUE(write);
    ASSERT_TRUE(read);
    std::array<FdCommandRequest, 2> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 2u);
    ExpectError(queue_.Complete(*write, FdCommandReply{.io = {1, 0, 0}}), FdQueueErrorCode::kInvalidArgument);
    ExpectError(queue_.Complete(*write, FdCommandReply{.io = {0, 1, 0}}), FdQueueErrorCode::kInvalidArgument);
    ExpectError(queue_.Complete(*read, FdCommandReply{.io = {5, 0, 0}}), FdQueueErrorCode::kInvalidArgument);
    ExpectError(queue_.Complete(*read, FdCommandReply{.io = {0, 2, 0}}), FdQueueErrorCode::kInvalidArgument);
    ExpectError(queue_.Complete(*read, Failed(0)), FdQueueErrorCode::kInvalidArgument);
    ExpectError(queue_.TakeCompletion(*read), FdQueueErrorCode::kPending);
    ASSERT_TRUE(queue_.Complete(*read, Failed(KBS_STREAM_WOULD_BLOCK)));
    const auto completed = queue_.TakeCompletion(*read);
    ASSERT_TRUE(completed);
    ASSERT_FALSE(completed->result);
    EXPECT_EQ(completed->result.error().code, KBS_STREAM_WOULD_BLOCK);
}

TEST_F(FdCommandQueueTest, ForgedAndCrossQueueTicketsCannotReadOrCompleteRequests) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto ticket = Submit({});
    ASSERT_TRUE(ticket);
    for (const auto invalid :
         {FdCommandTicket{}, FdCommandTicket{99, ticket->sequence}, FdCommandTicket{config_.queue_id, 100},
          FdCommandTicket{config_.queue_id, std::numeric_limits<std::uint64_t>::max()}}) {
        ExpectError(queue_.Complete(invalid, FdCommandReply{}), FdQueueErrorCode::kInvalidTicket);
        ExpectError(queue_.TakeCompletion(invalid), FdQueueErrorCode::kInvalidTicket);
    }
    // 尚未由 owner 取出的已发布命令不能通过伪造完成提前终结。
    ExpectError(queue_.Complete(*ticket, FdCommandReply{}), FdQueueErrorCode::kInvalidTicket);
    ExpectError(queue_.PollCommands({}), FdQueueErrorCode::kInvalidArgument);
}

TEST_F(FdCommandQueueTest, PollBudgetPreservesWakeUntilPublishedPrefixIsDrained) {
    config_.capacity = 128;
    config_.payload_budget_bytes = 8192;
    ASSERT_NO_FATAL_FAILURE(Open());
    for (unsigned i = 0; i < 65; ++i) {
        ASSERT_TRUE(Submit({.token = i}));
    }
    EXPECT_TRUE(Ready(queue_.command_fd()));
    std::array<FdCommandRequest, 128> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 64u);
    EXPECT_TRUE(queue_.runnable());
    EXPECT_TRUE(Ready(queue_.command_fd()));
    for (unsigned i = 0; i < 64; ++i) {
        EXPECT_EQ(requests[i].command.token, i);
        ASSERT_TRUE(queue_.Complete(requests[i].ticket, FdCommandReply{}));
    }
    ASSERT_EQ(queue_.PollCommands(requests), 1u);
    EXPECT_EQ(requests[0].command.token, 64u);
    ASSERT_TRUE(queue_.Complete(requests[0].ticket, FdCommandReply{}));
    EXPECT_FALSE(queue_.runnable());
    EXPECT_FALSE(Ready(queue_.command_fd()));
    ASSERT_TRUE(Submit({}));
    EXPECT_TRUE(Ready(queue_.command_fd()));
}

TEST_F(FdCommandQueueTest, StopRejectsNewWorkButAllowsPendingAndCompletedWorkToDrain) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto ticket = Submit({.kind = FdCommandKind::kClose});
    ASSERT_TRUE(ticket);
    auto lease = table_.Lookup(reader_, binding_);
    ASSERT_TRUE(lease);
    ExpectError(queue_.Close(), FdQueueErrorCode::kNotQuiescent);
    EXPECT_FALSE(queue_.accepting());
    ExpectError(queue_.TrySubmit(*lease, {}), FdQueueErrorCode::kStopped);
    EXPECT_TRUE(*lease);
    ExpectError(queue_.Close(true), FdQueueErrorCode::kInUse);
    std::array<FdCommandRequest, 1> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 1u);
    ASSERT_TRUE(queue_.Complete(*ticket, FdCommandReply{}));
    ExpectError(queue_.Close(true), FdQueueErrorCode::kInUse);
    ASSERT_TRUE(queue_.TakeCompletion(*ticket));
    ASSERT_TRUE(queue_.Close(true));
    EXPECT_TRUE(queue_.Close(true));
}

TEST_F(FdCommandQueueTest, HotPathDoesNotAllocateOrReleaseStorage) {
    ASSERT_NO_FATAL_FAILURE(Open());
    auto lease = table_.Lookup(reader_, binding_);
    ASSERT_TRUE(lease);
    const std::array input{std::byte{1}, std::byte{2}};
    std::array<FdCommandRequest, 1> requests;
    // 计量区间只运行数据路径，不把测试断言和宿主控制路径纳入计数。
    allocations = deallocations = 0;
    count_allocations = true;
    const auto ticket = queue_.TrySubmit(*lease, {.kind = FdCommandKind::kWrite, .bytes = 2}, input);
    const auto count = queue_.PollCommands(requests);
    const bool complete = ticket && queue_.Complete(*ticket, FdCommandReply{.io = {2, 0, 0}}).has_value();
    const auto taken = ticket ? queue_.TakeCompletion(*ticket)
                              : std::expected<FdCommandCompletion, FdQueueError>(
                                    std::unexpected(FdQueueError{FdQueueErrorCode::kNotReady}));
    count_allocations = false;
    ASSERT_TRUE(ticket);
    ASSERT_EQ(count, 1u);
    ASSERT_TRUE(complete);
    ASSERT_TRUE(taken);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    EXPECT_EQ(queue_.outstanding(), 0u);
}

TEST_F(FdCommandQueueTest, CompetingCollectorsReleaseOneReadResultAndOneLease) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto ticket = Submit({.kind = FdCommandKind::kRead, .bytes = 4});
    ASSERT_TRUE(ticket);
    std::array<FdCommandRequest, 1> requests;
    ASSERT_EQ(queue_.PollCommands(requests), 1u);
    requests[0].payload[0] = std::byte{17};
    ASSERT_TRUE(queue_.Complete(*ticket, FdCommandReply{.io = {1, 0, 0}}));
    std::atomic<bool> start{false};
    std::atomic<unsigned> successes{0}, failures{0};
    std::array<std::jthread, 8> collectors;
    // 多领取者竞争同一票据，只有取得 Taking 状态者复制输出并归还 Lease。
    for (auto& collector : collectors) {
        collector = std::jthread([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::array<std::byte, 4> output{};
            const auto result = queue_.TakeCompletion(*ticket, output);
            if (result) {
                ++successes;
                if (!result->result || result->result->io.bytes != 1 || output[0] != std::byte{17}) {
                    ++failures;
                }
            } else if (result.error().code != FdQueueErrorCode::kPending &&
                       result.error().code != FdQueueErrorCode::kInvalidTicket) {
                ++failures;
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& collector : collectors) {
        collector.join();
    }
    EXPECT_EQ(successes.load(), 1u);
    EXPECT_EQ(failures.load(), 0u);
    EXPECT_EQ(queue_.outstanding(), 0u);
    EXPECT_EQ(table_.stats().references, 0u);
}

TEST_F(FdCommandQueueTest, ConcurrentProducersKeepCopiedPayloadAndCompletionOwnership) {
    config_.capacity = 16;
    config_.payload_budget_bytes = 1024;
    ASSERT_NO_FATAL_FAILURE(Open());
    constexpr unsigned kThreads = 4, kPerThread = 500;
    std::atomic<unsigned> finished{0}, completed{0}, failures{0};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::array<std::jthread, kThreads> producers;
    for (unsigned thread = 0; thread < kThreads; ++thread) {
        producers[thread] = std::jthread([&, thread] {
            auto reader = table_.RegisterReader();
            if (!reader) {
                ++failures;
                ++finished;
                return;
            }
            for (unsigned i = 0; i < kPerThread; ++i) {
                const std::uint32_t token = thread * kPerThread + i + 1;
                auto lease = table_.Lookup(*reader, binding_);
                if (!lease) {
                    ++failures;
                    break;
                }
                std::array<std::byte, sizeof(token)> input;
                std::memcpy(input.data(), &token, sizeof(token));
                std::expected<FdCommandTicket, FdQueueError> ticket =
                    std::unexpected(FdQueueError{FdQueueErrorCode::kFull});
                while (!ticket && std::chrono::steady_clock::now() < deadline) {
                    ticket =
                        queue_.TrySubmit(*lease, {.kind = FdCommandKind::kWrite, .token = token, .bytes = 4}, input);
                    if (!ticket && ticket.error().code != FdQueueErrorCode::kFull &&
                        ticket.error().code != FdQueueErrorCode::kContended) {
                        ++failures;
                        break;
                    }
                    std::this_thread::yield();
                }
                if (!ticket) {
                    ++failures;
                    break;
                }
                // 提交成功后立刻改写源，owner 仍应只看到队列自己的副本。
                input.fill(std::byte{0});
                for (;;) {
                    auto result = queue_.TakeCompletion(*ticket);
                    if (result) {
                        if (!result->result || result->result->io.bytes != 4 || result->command.token != token ||
                            result->binding.id != binding_) {
                            ++failures;
                        }
                        ++completed;
                        break;
                    }
                    if (result.error().code != FdQueueErrorCode::kPending ||
                        std::chrono::steady_clock::now() >= deadline) {
                        ++failures;
                        break;
                    }
                    std::this_thread::yield();
                }
            }
            ++finished;
        });
    }
    std::array<FdCommandRequest, 7> requests;
    while (finished.load() < kThreads && std::chrono::steady_clock::now() < deadline) {
        const auto count = queue_.PollCommands(requests);
        if (!count) {
            ++failures;
            break;
        }
        for (std::size_t i = 0; i < *count; ++i) {
            std::uint32_t token = 0;
            std::memcpy(&token, requests[i].payload.data(), sizeof(token));
            if (token != requests[i].command.token || requests[i].binding.id != binding_) {
                ++failures;
            }
            if (!queue_.Complete(requests[i].ticket, FdCommandReply{.io = {4, 0, 0}})) {
                ++failures;
            }
        }
        std::this_thread::yield();
    }
    for (auto& producer : producers) {
        producer.join();
    }
    EXPECT_EQ(failures.load(), 0u);
    EXPECT_EQ(completed.load(), kThreads * kPerThread);
    EXPECT_EQ(queue_.outstanding(), 0u);
    EXPECT_EQ(table_.stats().references, 0u);
}

} // namespace
} // namespace socket
} // namespace kbsocket
