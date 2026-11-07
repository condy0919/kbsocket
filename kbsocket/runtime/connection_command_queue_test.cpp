// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_command_queue.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <thread>

#include <gtest/gtest.h>

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocation_count = 0;
thread_local int fail_allocation_after = -1;
void RecordAllocation() {
    if (count_allocations) {
        ++allocation_count;
    }
    if (fail_allocation_after == 0) {
        fail_allocation_after = -1;
        throw std::bad_alloc();
    }
    if (fail_allocation_after > 0) {
        --fail_allocation_after;
    }
}
} // namespace

void* operator new(std::size_t size) {
    RecordAllocation();
    if (auto* memory = std::malloc(size ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void* operator new(std::size_t size, std::align_val_t alignment) {
    RecordAllocation();
    void* memory = nullptr;
    if (::posix_memalign(&memory, static_cast<std::size_t>(alignment), size ? size : 1) != 0) {
        throw std::bad_alloc();
    }
    return memory;
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return ::operator new(size, alignment);
}
void operator delete(void* memory) noexcept {
    std::free(memory);
}
void operator delete[](void* memory) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}

namespace kbsocket {
namespace runtime {
namespace {
// 仅搬运不解引用 owner 身份，假地址若被执行成 ConnectionOwner 操作会立即暴露边界错误。
alignas(64) const std::array<char, 64> owner_identity{};
ConnectionCommand Command(ConnectionCommandKind kind = ConnectionCommandKind::kCancel, std::uint64_t token = 0,
                          std::uint32_t bytes = 0) {
    return {.kind = kind,
            .connection = {.owner = reinterpret_cast<const ConnectionOwner*>(owner_identity.data()),
                           .slot = 7,
                           .generation = 19},
            .token = token,
            .bytes = bytes};
}
ConnectionCommandResult Failure() {
    return std::unexpected(ConnectionOwnerError{.code = ConnectionOwnerErrorCode::kInvalidHandle});
}
bool Readable(int fd, int timeout_ms = 0) {
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    int result;
    do {
        result = ::poll(&descriptor, 1, timeout_ms);
    } while (result < 0 && errno == EINTR);
    return result == 1 && (descriptor.revents & POLLIN) != 0;
}
class ConnectionCommandQueueTest : public ::testing::Test {
protected:
    void TearDown() override {
        count_allocations = false;
        fail_allocation_after = -1;
        queue_.StopAccepting();
        // 测试退出前线程已 join；即使某个断言失败也把未领取结果显式收走。
        std::array<ConnectionCommandRequest, 64> requests;
        while (queue_.runnable()) {
            const auto polled = queue_.PollCommands(requests);
            ASSERT_TRUE(polled);
            for (std::size_t i = 0; i < *polled; ++i) {
                EXPECT_TRUE(queue_.Complete(requests[i].ticket, Failure()));
            }
        }
        std::array<std::byte, ConnectionCommandQueue::kMaxIoBytes> output;
        for (std::uint64_t sequence = 1; queue_.outstanding() && sequence <= 100000; ++sequence) {
            const ConnectionCommandTicket ticket{.queue = &queue_, .sequence = sequence};
            (void)queue_.Complete(ticket, Failure());
            (void)queue_.TakeCompletion(ticket, output);
        }
        EXPECT_TRUE(queue_.Close(true));
    }
    ConnectionCommandQueue queue_;
};

TEST_F(ConnectionCommandQueueTest, RequiresOpenAndValidCapacity) {
    std::array<ConnectionCommandRequest, 1> requests;
    EXPECT_EQ(queue_.command_fd(), -1);
    EXPECT_EQ(queue_.capacity(), 0u);
    EXPECT_EQ(queue_.outstanding(), 0u);
    EXPECT_FALSE(queue_.accepting());
    EXPECT_FALSE(queue_.runnable());
    EXPECT_EQ(queue_.TrySubmit(Command()).error().code, CommandQueueErrorCode::kNotReady);
    EXPECT_EQ(queue_.PollCommands(requests).error().code, CommandQueueErrorCode::kNotReady);
    EXPECT_EQ(queue_.Complete({}, Failure()).error().code, CommandQueueErrorCode::kNotReady);
    EXPECT_EQ(queue_.TakeCompletion({}).error().code, CommandQueueErrorCode::kNotReady);
    for (const auto capacity :
         {std::size_t{0}, ConnectionCommandQueue::kMaxCapacity + 1, std::numeric_limits<std::size_t>::max()}) {
        const auto opened = queue_.Open(capacity);
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, CommandQueueErrorCode::kInvalidArgument);
    }
    EXPECT_TRUE(queue_.Close());
}

TEST_F(ConnectionCommandQueueTest, CapacityFdAndSingleOpenProtectTicketIdentity) {
    ASSERT_TRUE(queue_.Open(3));
    EXPECT_EQ(queue_.capacity(), 4u);
    EXPECT_TRUE(queue_.accepting());
    const int fd = queue_.command_fd();
    EXPECT_NE(::fcntl(fd, F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(fd, F_GETFD) & FD_CLOEXEC, 0);
    EXPECT_EQ(queue_.Open(2).error().code, CommandQueueErrorCode::kInUse);
    ASSERT_TRUE(queue_.Close(true));
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_EQ(queue_.Open(2).error().code, CommandQueueErrorCode::kStopped);
}

TEST_F(ConnectionCommandQueueTest, ValidatesEveryCommandShapeBeforeReserving) {
    ASSERT_TRUE(queue_.Open(8));
    std::array<std::byte, 4> bytes{};
    auto invalid = Command();
    invalid.connection.owner = nullptr;
    EXPECT_EQ(queue_.TrySubmit(invalid).error().code, CommandQueueErrorCode::kInvalidArgument);
    invalid = Command();
    invalid.connection.generation = 0;
    EXPECT_EQ(queue_.TrySubmit(invalid).error().code, CommandQueueErrorCode::kInvalidArgument);
    invalid = Command(static_cast<ConnectionCommandKind>(255));
    EXPECT_EQ(queue_.TrySubmit(invalid).error().code, CommandQueueErrorCode::kInvalidArgument);
    for (const auto kind : {ConnectionCommandKind::kRead, ConnectionCommandKind::kWrite}) {
        EXPECT_EQ(queue_.TrySubmit(Command(kind)).error().code, CommandQueueErrorCode::kInvalidArgument);
        EXPECT_EQ(queue_.TrySubmit(Command(kind, 0, 4097)).error().code, CommandQueueErrorCode::kInvalidArgument);
        invalid = Command(kind, 0, 4);
        invalid.readable = true;
        EXPECT_EQ(queue_.TrySubmit(invalid, bytes).error().code, CommandQueueErrorCode::kInvalidArgument);
    }
    EXPECT_EQ(queue_.TrySubmit(Command(ConnectionCommandKind::kWrite, 0, 3), bytes).error().code,
              CommandQueueErrorCode::kInvalidArgument);
    EXPECT_EQ(queue_.TrySubmit(Command(ConnectionCommandKind::kRead, 0, 4), bytes).error().code,
              CommandQueueErrorCode::kInvalidArgument);
    for (const auto kind : {ConnectionCommandKind::kCancel, ConnectionCommandKind::kShutdownWrite,
                            ConnectionCommandKind::kInspect, ConnectionCommandKind::kWatchTcp}) {
        EXPECT_EQ(queue_.TrySubmit(Command(kind, 0, 4)).error().code, CommandQueueErrorCode::kInvalidArgument);
        EXPECT_EQ(queue_.TrySubmit(Command(kind), bytes).error().code, CommandQueueErrorCode::kInvalidArgument);
    }
    EXPECT_EQ(queue_.outstanding(), 0u);
    EXPECT_FALSE(Readable(queue_.command_fd()));
    auto watch = Command(ConnectionCommandKind::kWatchTcp);
    watch.readable = true;
    watch.writable = true;
    EXPECT_TRUE(queue_.TrySubmit(watch));
    EXPECT_TRUE(queue_.TrySubmit(Command(ConnectionCommandKind::kRead, 0, 4)));
    EXPECT_TRUE(queue_.TrySubmit(Command(ConnectionCommandKind::kWrite, 0, 4), bytes));
}

TEST_F(ConnectionCommandQueueTest, CopiesWriteInputAndPreservesCommandAndTicket) {
    ASSERT_TRUE(queue_.Open(1));
    EXPECT_EQ(queue_.capacity(), 2u);
    std::array<std::byte, 4> input{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    const auto submitted = queue_.TrySubmit(Command(ConnectionCommandKind::kWrite, 55, 4), input);
    ASSERT_TRUE(submitted);
    input.fill(std::byte{99});
    std::array<ConnectionCommandRequest, 1> requests;
    const auto polled = queue_.PollCommands(requests);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    EXPECT_EQ(requests[0].ticket, *submitted);
    EXPECT_EQ(requests[0].command.connection, Command().connection);
    EXPECT_EQ(requests[0].command.token, 55u);
    ASSERT_EQ(requests[0].payload.size(), 4u);
    EXPECT_EQ(requests[0].payload[0], std::byte{1});
    EXPECT_EQ(requests[0].payload[3], std::byte{4});
    ASSERT_TRUE(queue_.Complete(*submitted, ConnectionCommandReply{.bytes = 2}));
    const auto completion = queue_.TakeCompletion(*submitted);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->bytes, 2u);
    EXPECT_EQ(completion->command.token, 55u);
    EXPECT_EQ(queue_.outstanding(), 0u);
}

TEST_F(ConnectionCommandQueueTest, CompletedResultsKeepCapacityAndOutOfOrderTakeCountsExactly) {
    ASSERT_TRUE(queue_.Open(2));
    const auto first = queue_.TrySubmit(Command(ConnectionCommandKind::kCancel, 1));
    const auto second = queue_.TrySubmit(Command(ConnectionCommandKind::kCancel, 2));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    std::array<ConnectionCommandRequest, 2> requests;
    ASSERT_EQ(*queue_.PollCommands(requests), 2u);
    ASSERT_TRUE(queue_.Complete(*first, ConnectionCommandReply{}));
    ASSERT_TRUE(queue_.Complete(*second, ConnectionCommandReply{}));
    EXPECT_EQ(queue_.TrySubmit(Command()).error().code, CommandQueueErrorCode::kFull);
    EXPECT_EQ(queue_.outstanding(), 2u);
    ASSERT_TRUE(queue_.TakeCompletion(*second));
    EXPECT_EQ(queue_.outstanding(), 1u);
    // FIFO 环头仍被第一份结果占用；后面空槽不能跨过它改变预留顺序。
    EXPECT_EQ(queue_.TrySubmit(Command()).error().code, CommandQueueErrorCode::kFull);
    ASSERT_TRUE(queue_.TakeCompletion(*first));
    EXPECT_EQ(queue_.outstanding(), 0u);
    EXPECT_TRUE(queue_.TrySubmit(Command()));
    EXPECT_TRUE(queue_.TrySubmit(Command()));
    EXPECT_EQ(queue_.outstanding(), 2u);
}

TEST_F(ConnectionCommandQueueTest, ReadOutputTooSmallRetainsResultAndCopiesOnlyReturnedBytes) {
    ASSERT_TRUE(queue_.Open(2));
    const auto ticket = queue_.TrySubmit(Command(ConnectionCommandKind::kRead, 77, 8));
    ASSERT_TRUE(ticket);
    EXPECT_EQ(queue_.TakeCompletion(*ticket).error().code, CommandQueueErrorCode::kPending);
    std::array<ConnectionCommandRequest, 1> requests;
    ASSERT_EQ(*queue_.PollCommands(requests), 1u);
    EXPECT_EQ(queue_.TakeCompletion(*ticket).error().code, CommandQueueErrorCode::kPending);
    std::fill(requests[0].payload.begin(), requests[0].payload.end(), std::byte{42});
    ASSERT_TRUE(queue_.Complete(*ticket, ConnectionCommandReply{.bytes = 3, .eof = true}));
    std::array<std::byte, 8> output;
    output.fill(std::byte{9});
    EXPECT_EQ(queue_.TakeCompletion(*ticket, std::span(output).first(2)).error().code,
              CommandQueueErrorCode::kOutputTooSmall);
    EXPECT_EQ(queue_.outstanding(), 1u);
    EXPECT_EQ(output[0], std::byte{9});
    const auto taken = queue_.TakeCompletion(*ticket, output);
    ASSERT_TRUE(taken);
    ASSERT_TRUE(taken->result);
    EXPECT_TRUE(taken->result->eof);
    EXPECT_EQ(taken->result->bytes, 3u);
    EXPECT_EQ(output[0], std::byte{42});
    EXPECT_EQ(output[2], std::byte{42});
    EXPECT_EQ(output[3], std::byte{9});
    EXPECT_EQ(queue_.TakeCompletion(*ticket, output).error().code, CommandQueueErrorCode::kInvalidTicket);
}

TEST_F(ConnectionCommandQueueTest, CompletionRequiresPolledTicketAndBoundedResult) {
    ASSERT_TRUE(queue_.Open(2));
    const auto ticket = queue_.TrySubmit(Command(ConnectionCommandKind::kRead, 0, 4));
    ASSERT_TRUE(ticket);
    EXPECT_EQ(queue_.Complete(*ticket, ConnectionCommandReply{}).error().code, CommandQueueErrorCode::kInvalidTicket);
    std::array<ConnectionCommandRequest, 1> requests;
    ASSERT_EQ(*queue_.PollCommands(requests), 1u);
    EXPECT_EQ(queue_.Complete(*ticket, ConnectionCommandReply{.bytes = 5}).error().code,
              CommandQueueErrorCode::kInvalidArgument);
    ASSERT_TRUE(queue_.Complete(*ticket, Failure()));
    EXPECT_EQ(queue_.Complete(*ticket, Failure()).error().code, CommandQueueErrorCode::kInvalidTicket);
    const auto taken = queue_.TakeCompletion(*ticket);
    ASSERT_TRUE(taken);
    ASSERT_FALSE(taken->result);
    EXPECT_EQ(taken->result.error().code, ConnectionOwnerErrorCode::kInvalidHandle);
}

TEST_F(ConnectionCommandQueueTest, InspectCopiesStatusAndRejectsMalformedSuccess) {
    ASSERT_TRUE(queue_.Open(2));
    const auto ticket = queue_.TrySubmit(Command(ConnectionCommandKind::kInspect));
    ASSERT_TRUE(ticket);
    std::array<ConnectionCommandRequest, 1> requests;
    ASSERT_EQ(*queue_.PollCommands(requests), 1u);
    EXPECT_EQ(queue_.Complete(*ticket, ConnectionCommandReply{}).error().code, CommandQueueErrorCode::kInvalidArgument);
    OwnedConnectionStatus status{};
    status.rx_pending = 101;
    status.retained_sources = 3;
    ASSERT_TRUE(queue_.Complete(*ticket, ConnectionCommandReply{.status = status}));
    status.rx_pending = 0;
    const auto taken = queue_.TakeCompletion(*ticket);
    ASSERT_TRUE(taken);
    ASSERT_TRUE(taken->result);
    ASSERT_TRUE(taken->result->status);
    EXPECT_EQ(taken->result->status->rx_pending, 101u);
    EXPECT_EQ(taken->result->status->retained_sources, 3u);
}

TEST_F(ConnectionCommandQueueTest, StaleForeignAndUnissuedTicketsNeverAccessOtherRequest) {
    ASSERT_TRUE(queue_.Open(2));
    ConnectionCommandQueue other;
    const auto ticket = queue_.TrySubmit(Command());
    ASSERT_TRUE(ticket);
    EXPECT_EQ(queue_.TakeCompletion({.queue = &other, .sequence = ticket->sequence}).error().code,
              CommandQueueErrorCode::kInvalidTicket);
    EXPECT_EQ(queue_.TakeCompletion({.queue = &queue_, .sequence = 0}).error().code,
              CommandQueueErrorCode::kInvalidTicket);
    EXPECT_EQ(
        queue_.TakeCompletion({.queue = &queue_, .sequence = std::numeric_limits<std::uint64_t>::max()}).error().code,
        CommandQueueErrorCode::kInvalidTicket);
    std::array<ConnectionCommandRequest, 1> requests;
    for (std::size_t i = 0; i < 1000; ++i) {
        if (i) {
            ASSERT_TRUE(queue_.TrySubmit(Command(ConnectionCommandKind::kCancel, i)));
        }
        ASSERT_EQ(*queue_.PollCommands(requests), 1u);
        ASSERT_TRUE(queue_.Complete(requests[0].ticket, ConnectionCommandReply{}));
        ASSERT_TRUE(queue_.TakeCompletion(requests[0].ticket));
        EXPECT_EQ(queue_.TakeCompletion(*ticket).error().code, CommandQueueErrorCode::kInvalidTicket);
    }
}

TEST_F(ConnectionCommandQueueTest, PollBudgetPreservesReadinessAndReservationOrder) {
    ASSERT_TRUE(queue_.Open(4));
    for (std::uint64_t token = 1; token <= 3; ++token) {
        ASSERT_TRUE(queue_.TrySubmit(Command(ConnectionCommandKind::kCancel, token)));
    }
    EXPECT_EQ(queue_.PollCommands({}).error().code, CommandQueueErrorCode::kInvalidArgument);
    std::array<ConnectionCommandRequest, 1> requests;
    for (std::uint64_t token = 1; token <= 3; ++token) {
        EXPECT_TRUE(queue_.runnable());
        EXPECT_TRUE(Readable(queue_.command_fd()));
        ASSERT_EQ(*queue_.PollCommands(requests), 1u);
        EXPECT_EQ(requests[0].command.token, token);
        EXPECT_EQ(requests[0].ticket.sequence, token);
        ASSERT_TRUE(queue_.Complete(requests[0].ticket, ConnectionCommandReply{}));
        ASSERT_TRUE(queue_.TakeCompletion(requests[0].ticket));
    }
    EXPECT_FALSE(queue_.runnable());
    EXPECT_FALSE(Readable(queue_.command_fd()));
    EXPECT_EQ(*queue_.PollCommands(requests), 0u);
}

TEST_F(ConnectionCommandQueueTest, PublicationAfterEmptyPollWakesOwner) {
    ASSERT_TRUE(queue_.Open(2));
    std::atomic<bool> start{false};
    std::atomic<bool> submitted{false};
    std::thread producer([&] {
        start.wait(false, std::memory_order_acquire);
        submitted.store(queue_.TrySubmit(Command()).has_value(), std::memory_order_release);
    });
    std::array<ConnectionCommandRequest, 1> requests;
    const auto polled = queue_.PollCommands(requests);
    start.store(true, std::memory_order_release);
    start.notify_one();
    const bool woke = Readable(queue_.command_fd(), 3000);
    producer.join();
    ASSERT_TRUE(polled);
    EXPECT_EQ(*polled, 0u);
    EXPECT_TRUE(submitted.load(std::memory_order_acquire));
    EXPECT_TRUE(woke);
    EXPECT_TRUE(queue_.runnable());
}

TEST_F(ConnectionCommandQueueTest, StopAndCloseRetainCommandsExecutingRequestsAndCompletions) {
    ASSERT_TRUE(queue_.Open(2));
    const auto ticket = queue_.TrySubmit(Command());
    ASSERT_TRUE(ticket);
    const auto closed = queue_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, CommandQueueErrorCode::kNotQuiescent);
    EXPECT_FALSE(queue_.accepting());
    EXPECT_EQ(queue_.TrySubmit(Command()).error().code, CommandQueueErrorCode::kStopped);
    EXPECT_EQ(queue_.Close(true).error().code, CommandQueueErrorCode::kInUse);
    std::array<ConnectionCommandRequest, 1> requests;
    ASSERT_EQ(*queue_.PollCommands(requests), 1u);
    EXPECT_EQ(queue_.Close(true).error().code, CommandQueueErrorCode::kInUse);
    ASSERT_TRUE(queue_.Complete(*ticket, ConnectionCommandReply{}));
    EXPECT_EQ(queue_.Close(true).error().code, CommandQueueErrorCode::kInUse);
    ASSERT_TRUE(queue_.TakeCompletion(*ticket));
    EXPECT_EQ(queue_.Close().error().code, CommandQueueErrorCode::kNotQuiescent);
    EXPECT_TRUE(queue_.Close(true));
}

TEST_F(ConnectionCommandQueueTest, MultipleProducersAndCollectorsPreserveEveryPayloadAndProducerOrder) {
    constexpr std::size_t kProducers = 4;
    constexpr std::uint64_t kPerProducer = 2048;
    ASSERT_TRUE(queue_.Open(64));
    std::array<std::thread, kProducers> clients;
    std::array<std::uint64_t, kProducers> last{};
    std::atomic<bool> stop{false};
    std::atomic<bool> valid{true};
    std::atomic<std::size_t> done{0};
    std::barrier start(static_cast<std::ptrdiff_t>(kProducers + 1));
    for (std::size_t producer = 0; producer < kProducers; ++producer) {
        clients[producer] = std::thread([&, producer] {
            start.arrive_and_wait();
            for (std::uint64_t number = 1; number <= kPerProducer && !stop.load(std::memory_order_acquire); ++number) {
                const auto token = producer * kPerProducer + number;
                const auto command = Command(ConnectionCommandKind::kWrite, token, sizeof(token));
                ConnectionCommandTicket ticket;
                while (!stop.load(std::memory_order_acquire)) {
                    const auto submitted = queue_.TrySubmit(command, std::as_bytes(std::span(&token, 1)));
                    if (submitted) {
                        ticket = *submitted;
                        break;
                    }
                    if (submitted.error().code != CommandQueueErrorCode::kFull) {
                        valid.store(false, std::memory_order_release);
                        return;
                    }
                    std::this_thread::yield();
                }
                while (ticket.queue && !stop.load(std::memory_order_acquire)) {
                    const auto completion = queue_.TakeCompletion(ticket);
                    if (completion) {
                        if (!completion->result || completion->result->bytes != sizeof(token) ||
                            completion->command.token != token) {
                            valid.store(false, std::memory_order_release);
                        }
                        break;
                    }
                    if (completion.error().code != CommandQueueErrorCode::kPending) {
                        valid.store(false, std::memory_order_release);
                        return;
                    }
                    std::this_thread::yield();
                }
            }
            done.fetch_add(1, std::memory_order_release);
        });
    }
    start.arrive_and_wait();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::array<ConnectionCommandRequest, 3> requests;
    std::uint64_t processed = 0;
    std::uint64_t last_ticket = 0;
    while (done.load(std::memory_order_acquire) != kProducers && std::chrono::steady_clock::now() < deadline) {
        const auto polled = queue_.PollCommands(requests);
        if (!polled) {
            valid.store(false, std::memory_order_release);
            break;
        }
        for (std::size_t i = 0; i < *polled; ++i) {
            const auto token = requests[i].command.token;
            const auto producer = (token - 1) / kPerProducer;
            const auto number = (token - 1) % kPerProducer + 1;
            std::uint64_t payload = 0;
            std::memcpy(&payload, requests[i].payload.data(), sizeof(payload));
            if (producer >= kProducers || number != last[producer] + 1 || payload != token ||
                requests[i].ticket.sequence != last_ticket + 1) {
                valid.store(false, std::memory_order_release);
            } else {
                last[producer] = number;
            }
            last_ticket = requests[i].ticket.sequence;
            if (!queue_.Complete(requests[i].ticket, ConnectionCommandReply{.bytes = sizeof(payload)})) {
                valid.store(false, std::memory_order_release);
            }
            ++processed;
        }
        if (*polled == 0) {
            std::this_thread::yield();
        }
    }
    stop.store(true, std::memory_order_release);
    for (auto& client : clients) {
        client.join();
    }
    EXPECT_TRUE(valid.load(std::memory_order_acquire));
    EXPECT_EQ(done.load(std::memory_order_acquire), kProducers);
    EXPECT_EQ(processed, kProducers * kPerProducer);
    EXPECT_EQ(queue_.outstanding(), 0u);
}

TEST_F(ConnectionCommandQueueTest, ConcurrentCollectorsClaimSameTicketOnceBeforeSlotReuse) {
    ASSERT_TRUE(queue_.Open(2));
    std::array<ConnectionCommandRequest, 2> requests;
    for (std::uint64_t iteration = 1; iteration <= 64; ++iteration) {
        const auto ticket = queue_.TrySubmit(Command(ConnectionCommandKind::kRead, iteration, 4096));
        ASSERT_TRUE(ticket);
        ASSERT_EQ(*queue_.PollCommands(requests), 1u);
        const auto pattern = std::byte{static_cast<unsigned char>(iteration)};
        std::fill(requests[0].payload.begin(), requests[0].payload.end(), pattern);
        ASSERT_TRUE(queue_.Complete(*ticket, ConnectionCommandReply{.bytes = 4096}));
        std::barrier start(3);
        std::atomic<std::size_t> successes{0};
        std::atomic<bool> valid{true};
        std::array<std::thread, 2> collectors;
        for (auto& collector : collectors) {
            collector = std::thread([&] {
                std::array<std::byte, 4096> output{};
                start.arrive_and_wait();
                const auto taken = queue_.TakeCompletion(*ticket, output);
                if (taken) {
                    successes.fetch_add(1, std::memory_order_relaxed);
                    if (!taken->result || taken->result->bytes != output.size() ||
                        !std::all_of(output.begin(), output.end(), [&](auto byte) { return byte == pattern; })) {
                        valid.store(false, std::memory_order_relaxed);
                    }
                } else if (taken.error().code != CommandQueueErrorCode::kPending &&
                           taken.error().code != CommandQueueErrorCode::kInvalidTicket) {
                    valid.store(false, std::memory_order_relaxed);
                }
            });
        }
        start.arrive_and_wait();
        // 槽释放后立即绕环覆盖 payload，另一 collector 必须因 tag 不匹配拒绝旧 ticket。
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (queue_.outstanding() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        const auto filler = queue_.TrySubmit(Command());
        std::array<std::byte, 4096> replacement{};
        auto reused = queue_.TrySubmit(Command(ConnectionCommandKind::kWrite, 0, replacement.size()), replacement);
        while (!reused && reused.error().code == CommandQueueErrorCode::kFull &&
               std::chrono::steady_clock::now() < deadline) {
            reused = queue_.TrySubmit(Command(ConnectionCommandKind::kWrite, 0, replacement.size()), replacement);
        }
        for (auto& collector : collectors) {
            collector.join();
        }
        ASSERT_TRUE(filler);
        ASSERT_TRUE(reused);
        EXPECT_EQ(successes.load(), 1u);
        EXPECT_TRUE(valid.load());
        ASSERT_EQ(*queue_.PollCommands(requests), 2u);
        for (const auto& request : requests) {
            ASSERT_TRUE(queue_.Complete(request.ticket, Failure()));
            ASSERT_TRUE(queue_.TakeCompletion(request.ticket));
        }
    }
}

TEST_F(ConnectionCommandQueueTest, SubmitPollCompleteTakeDoNotAllocate) {
    ASSERT_TRUE(queue_.Open(2));
    std::array<ConnectionCommandRequest, 1> requests;
    std::array<std::byte, 4096> input{};
    bool succeeded = true;
    allocation_count = 0;
    count_allocations = true;
    for (std::size_t i = 0; i < 100; ++i) {
        const auto ticket = queue_.TrySubmit(Command(ConnectionCommandKind::kWrite, i, input.size()), input);
        const auto polled = queue_.PollCommands(requests);
        if (!ticket || !polled || *polled != 1) {
            succeeded = false;
            break;
        }
        succeeded &= queue_.Complete(*ticket, ConnectionCommandReply{.bytes = input.size()}).has_value();
        succeeded &= queue_.TakeCompletion(*ticket).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocation_count, 0u);
}

TEST(ConnectionCommandQueueAllocationTest, FailedOpenCanRetryWithoutConsumingIdentity) {
    for (int allocation = 0; allocation < 2; ++allocation) {
        ConnectionCommandQueue queue;
        fail_allocation_after = allocation;
        const auto opened = queue.Open(4);
        fail_allocation_after = -1;
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, CommandQueueErrorCode::kNoMemory);
        EXPECT_EQ(queue.command_fd(), -1);
        ASSERT_TRUE(queue.Open(4));
        EXPECT_TRUE(queue.Close(true));
    }
}

TEST(ConnectionCommandQueueDeathTest, EventFdFailureLeavesOpenRetryable) {
    EXPECT_EXIT(
        {
            rlimit original;
            if (::getrlimit(RLIMIT_NOFILE, &original) != 0) {
                _exit(1);
            }
            rlimit no_files = original;
            no_files.rlim_cur = 0;
            if (::setrlimit(RLIMIT_NOFILE, &no_files) != 0) {
                _exit(2);
            }
            ConnectionCommandQueue queue;
            const auto opened = queue.Open(2);
            if (::setrlimit(RLIMIT_NOFILE, &original) != 0) {
                _exit(3);
            }
            if (opened || opened.error().code != CommandQueueErrorCode::kSystem ||
                opened.error().system_error != EMFILE || queue.command_fd() != -1) {
                _exit(4);
            }
            if (!queue.Open(2) || !queue.Close(true)) {
                _exit(5);
            }
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace runtime
} // namespace kbsocket
