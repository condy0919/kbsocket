// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/memory/lease_return_queue.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <thread>

#include <gtest/gtest.h>

namespace {
// 只在无 gtest 调用的区段统计分配；Open 故障覆盖两个控制路径分配点。
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
namespace memory {
namespace {

// 队列只搬运身份，不解引用 allocator；用稳定的地址检验这一边界。
alignas(64) const std::array<char, 64> allocator_identity{};
BufferLease Lease(std::uint32_t slot, std::uint64_t generation = 1) {
    return BufferLease{.allocator = reinterpret_cast<const SlabAllocator*>(allocator_identity.data()),
                       .slot = slot,
                       .generation = generation};
}

bool Readable(int fd, int timeout_ms = 0) {
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    int status;
    do {
        status = ::poll(&descriptor, 1, timeout_ms);
    } while (status < 0 && errno == EINTR);
    return status == 1 && (descriptor.revents & POLLIN) != 0;
}

class LeaseReturnQueueTest : public ::testing::Test {
protected:
    void TearDown() override {
        count_allocations = false;
        fail_allocation_after = -1;
        // 所有测试必须先 join producer；失败断言也不能让正常测试路径泄漏 eventfd。
        if (queue_.completion_fd() >= 0) {
            queue_.StopAccepting();
            std::array<BufferLease, 64> leases;
            while (queue_.pending()) {
                auto result = queue_.Poll(leases);
                ASSERT_TRUE(result);
                ASSERT_GT(*result, 0u);
            }
        }
        EXPECT_TRUE(queue_.Close(true));
    }

    LeaseReturnQueue queue_;
};

TEST_F(LeaseReturnQueueTest, RequiresOpenAndRejectsOverflowingCapacity) {
    std::array<BufferLease, 1> output;
    EXPECT_EQ(queue_.completion_fd(), -1);
    EXPECT_EQ(queue_.capacity(), 0u);
    EXPECT_EQ(queue_.pending(), 0u);
    EXPECT_FALSE(queue_.accepting());
    auto pushed = queue_.TryPush(Lease(0));
    ASSERT_FALSE(pushed);
    EXPECT_EQ(pushed.error().code, MemoryErrorCode::kNotReady);
    auto polled = queue_.Poll(output);
    ASSERT_FALSE(polled);
    EXPECT_EQ(polled.error().code, MemoryErrorCode::kNotReady);
    for (const auto capacity :
         {std::size_t{0}, LeaseReturnQueue::kMaxCapacity + 1, std::numeric_limits<std::size_t>::max()}) {
        auto opened = queue_.Open(capacity);
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, MemoryErrorCode::kInvalidArgument);
    }
    EXPECT_TRUE(queue_.Close());
}

TEST_F(LeaseReturnQueueTest, RoundsCapacityAndOwnsNonblockingCloseOnExecFd) {
    ASSERT_TRUE(queue_.Open(3));
    EXPECT_EQ(queue_.capacity(), 4u);
    EXPECT_TRUE(queue_.accepting());
    EXPECT_NE(::fcntl(queue_.completion_fd(), F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(queue_.completion_fd(), F_GETFD) & FD_CLOEXEC, 0);
    auto reopened = queue_.Open(4);
    ASSERT_FALSE(reopened);
    EXPECT_EQ(reopened.error().code, MemoryErrorCode::kInUse);
    ASSERT_TRUE(queue_.Close(true));
    ASSERT_TRUE(queue_.Open(1));
    EXPECT_EQ(queue_.capacity(), 2u);
}

TEST_F(LeaseReturnQueueTest, InvalidLeaseAndEmptyPollDoNotChangeOwnership) {
    ASSERT_TRUE(queue_.Open(2));
    auto invalid = queue_.TryPush(BufferLease{});
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, MemoryErrorCode::kInvalidLease);
    invalid = queue_.TryPush(Lease(0, 0));
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, MemoryErrorCode::kInvalidLease);
    EXPECT_EQ(queue_.pending(), 0u);
    EXPECT_FALSE(Readable(queue_.completion_fd()));

    ASSERT_TRUE(queue_.TryPush(Lease(7)));
    auto polled = queue_.Poll({});
    ASSERT_FALSE(polled);
    EXPECT_EQ(polled.error().code, MemoryErrorCode::kInvalidArgument);
    EXPECT_EQ(queue_.pending(), 1u);
    EXPECT_TRUE(Readable(queue_.completion_fd()));
}

TEST_F(LeaseReturnQueueTest, FullQueueLeavesTokenForRetryAndPreservesIdentity) {
    ASSERT_TRUE(queue_.Open(2));
    const auto first = Lease(17, std::numeric_limits<std::uint64_t>::max());
    const auto second = Lease(91, 29);
    const auto retry = Lease(34, 88);
    ASSERT_TRUE(queue_.TryPush(first));
    ASSERT_TRUE(queue_.TryPush(second));
    auto rejected = queue_.TryPush(retry);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, MemoryErrorCode::kQueueFull);
    EXPECT_EQ(queue_.pending(), 2u);

    // 满额拒绝没有收走第三份 CPU 所有权；释放一个槽位后可原样再次提交。
    std::array<BufferLease, 1> one;
    auto polled = queue_.Poll(one);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    EXPECT_EQ(one[0], first);
    ASSERT_TRUE(queue_.TryPush(retry));
    std::array<BufferLease, 3> output;
    polled = queue_.Poll(output);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 2u);
    EXPECT_EQ(output[0], second);
    EXPECT_EQ(output[1], retry);
    EXPECT_EQ(queue_.pending(), 0u);
}

TEST_F(LeaseReturnQueueTest, BudgetKeepsNotificationReadableUntilLastLease) {
    ASSERT_TRUE(queue_.Open(4));
    for (std::uint32_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(queue_.TryPush(Lease(i)));
    }
    std::array<BufferLease, 1> one;
    for (std::uint32_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(Readable(queue_.completion_fd()));
        auto polled = queue_.Poll(one);
        ASSERT_TRUE(polled);
        ASSERT_EQ(*polled, 1u);
        EXPECT_EQ(one[0], Lease(i));
        EXPECT_EQ(queue_.pending(), 2u - i);
    }
    EXPECT_FALSE(Readable(queue_.completion_fd()));
    auto empty = queue_.Poll(one);
    ASSERT_TRUE(empty);
    EXPECT_EQ(*empty, 0u);
}

TEST_F(LeaseReturnQueueTest, EmptySnapshotIsNotProducerQuiescence) {
    ASSERT_TRUE(queue_.Open(4));
    const int fd = queue_.completion_fd();
    auto closed = queue_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, MemoryErrorCode::kNotQuiescent);
    EXPECT_EQ(queue_.completion_fd(), fd);
    EXPECT_FALSE(queue_.accepting());
    auto rejected = queue_.TryPush(Lease(0));
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, MemoryErrorCode::kStopped);
    ASSERT_TRUE(queue_.Close(true));
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    ASSERT_TRUE(queue_.Open(4));
    EXPECT_TRUE(queue_.accepting());
}

TEST_F(LeaseReturnQueueTest, StopPreservesPublishedReturnsUntilOwnerConsumesThem) {
    ASSERT_TRUE(queue_.Open(2));
    ASSERT_TRUE(queue_.TryPush(Lease(0)));
    queue_.StopAccepting();
    auto rejected = queue_.TryPush(Lease(1));
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, MemoryErrorCode::kStopped);
    auto closed = queue_.Close(true);
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, MemoryErrorCode::kInUse);
    EXPECT_TRUE(Readable(queue_.completion_fd()));
    std::array<BufferLease, 2> output;
    auto polled = queue_.Poll(output);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    EXPECT_EQ(output[0], Lease(0));
    EXPECT_TRUE(queue_.Close(true));
}

TEST_F(LeaseReturnQueueTest, ManyRingCyclesDoNotReuseStaleDescriptors) {
    ASSERT_TRUE(queue_.Open(2));
    std::array<BufferLease, 2> output;
    for (std::uint64_t generation = 1; generation <= 1000; ++generation) {
        ASSERT_TRUE(queue_.TryPush(Lease(0, generation)));
        ASSERT_TRUE(queue_.TryPush(Lease(1, generation)));
        auto polled = queue_.Poll(output);
        ASSERT_TRUE(polled);
        ASSERT_EQ(*polled, 2u);
        EXPECT_EQ(output[0], Lease(0, generation));
        EXPECT_EQ(output[1], Lease(1, generation));
    }
    EXPECT_EQ(queue_.pending(), 0u);
}

TEST_F(LeaseReturnQueueTest, ProducerPublicationAfterEmptyPollWakesOwner) {
    ASSERT_TRUE(queue_.Open(2));
    std::atomic<bool> start{false};
    std::atomic<bool> pushed{false};
    std::thread producer([&] {
        start.wait(false, std::memory_order_acquire);
        pushed.store(queue_.TryPush(Lease(42, 19)).has_value(), std::memory_order_release);
    });
    std::array<BufferLease, 1> output;
    auto empty = queue_.Poll(output);
    start.store(true, std::memory_order_release);
    start.notify_one();
    const bool woke = Readable(queue_.completion_fd(), 3000);
    producer.join();
    ASSERT_TRUE(empty);
    EXPECT_EQ(*empty, 0u);
    ASSERT_TRUE(pushed.load(std::memory_order_acquire));
    ASSERT_TRUE(woke);
    auto polled = queue_.Poll(output);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    EXPECT_EQ(output[0], Lease(42, 19));
}

TEST_F(LeaseReturnQueueTest, MultipleProducersTransferEveryTokenExactlyOnceInProducerOrder) {
    constexpr std::size_t kProducers = 4;
    constexpr std::size_t kPerProducer = 2048;
    ASSERT_TRUE(queue_.Open(64));
    std::array<std::thread, kProducers> producers;
    std::array<std::uint64_t, kProducers> last_generation{};
    std::array<std::array<bool, kPerProducer>, kProducers> seen{};
    std::atomic<bool> stop{false};
    std::atomic<bool> unexpected_error{false};
    std::barrier start(static_cast<std::ptrdiff_t>(kProducers + 1));
    for (std::size_t producer = 0; producer < kProducers; ++producer) {
        producers[producer] = std::thread([&, producer] {
            start.arrive_and_wait();
            for (std::uint64_t generation = 1; generation <= kPerProducer; ++generation) {
                while (!stop.load(std::memory_order_acquire)) {
                    auto pushed = queue_.TryPush(Lease(static_cast<std::uint32_t>(producer), generation));
                    if (pushed) {
                        break;
                    }
                    if (pushed.error().code != MemoryErrorCode::kQueueFull) {
                        unexpected_error.store(true, std::memory_order_release);
                        return;
                    }
                    std::this_thread::yield();
                }
                if (stop.load(std::memory_order_acquire)) {
                    return;
                }
            }
        });
    }
    start.arrive_and_wait();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::array<BufferLease, 7> output;
    std::size_t consumed = 0;
    bool valid = true;
    // 小于队列容量的预算与真实 poll 等待交替，覆盖竞争、绕环及消费预算下的重新通知。
    while (consumed != kProducers * kPerProducer && std::chrono::steady_clock::now() < deadline) {
        auto polled = queue_.Poll(output);
        if (!polled) {
            valid = false;
            break;
        }
        for (std::size_t i = 0; i < *polled; ++i) {
            const auto& lease = output[i];
            if (lease.allocator != Lease(0).allocator || lease.slot >= kProducers || !lease.generation ||
                lease.generation > kPerProducer) {
                valid = false;
                continue;
            }
            valid &= !seen[lease.slot][lease.generation - 1];
            valid &= lease.generation == last_generation[lease.slot] + 1;
            seen[lease.slot][lease.generation - 1] = true;
            last_generation[lease.slot] = lease.generation;
        }
        consumed += *polled;
        if (*polled == 0) {
            (void)Readable(queue_.completion_fd(), 10);
        }
    }
    stop.store(true, std::memory_order_release);
    for (auto& producer : producers) {
        producer.join();
    }
    EXPECT_FALSE(unexpected_error.load(std::memory_order_acquire));
    EXPECT_TRUE(valid);
    EXPECT_EQ(consumed, kProducers * kPerProducer);
    for (const auto generation : last_generation) {
        EXPECT_EQ(generation, kPerProducer);
    }
    EXPECT_EQ(queue_.pending(), 0u);
}

TEST_F(LeaseReturnQueueTest, HotPushAndPollDoNotAllocate) {
    ASSERT_TRUE(queue_.Open(2));
    std::array<BufferLease, 1> output;
    bool succeeded = true;
    allocation_count = 0;
    count_allocations = true;
    for (std::uint64_t generation = 1; generation <= 100; ++generation) {
        succeeded &= queue_.TryPush(Lease(0, generation)).has_value();
        const auto polled = queue_.Poll(output);
        succeeded &= polled && *polled == 1 && output[0] == Lease(0, generation);
    }
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocation_count, 0u);
}

TEST_F(LeaseReturnQueueTest, AllocationFailureLeavesQueueClosedAndRetryable) {
    for (int allocation = 0; allocation < 2; ++allocation) {
        fail_allocation_after = allocation;
        auto opened = queue_.Open(4);
        fail_allocation_after = -1;
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, MemoryErrorCode::kNoMemory);
        EXPECT_EQ(queue_.completion_fd(), -1);
        ASSERT_TRUE(queue_.Open(4));
        EXPECT_TRUE(queue_.Close(true));
    }
}

TEST(LeaseReturnQueueDeathTest, EventFdFailureDoesNotPublishPartiallyOpenedState) {
    // 子进程降低软限制，不影响并行测试进程；恢复后同一对象可以正常 Open。
    EXPECT_EXIT(
        {
            rlimit original;
            if (::getrlimit(RLIMIT_NOFILE, &original) != 0) {
                _exit(1);
            }
            rlimit closed_files = original;
            closed_files.rlim_cur = 0;
            if (::setrlimit(RLIMIT_NOFILE, &closed_files) != 0) {
                _exit(2);
            }
            LeaseReturnQueue queue;
            const auto opened = queue.Open(4);
            if (::setrlimit(RLIMIT_NOFILE, &original) != 0) {
                _exit(3);
            }
            if (opened || opened.error().code != MemoryErrorCode::kSystem || opened.error().provider_error != EMFILE ||
                queue.completion_fd() != -1) {
                _exit(4);
            }
            if (!queue.Open(4) || !queue.Close(true)) {
                _exit(5);
            }
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST(LeaseReturnQueueDeathTest, DestructorWithoutProducerProofRetainsFdAndState) {
    // 未 Close(true) 的析构不能关闭潜在 producer 仍会通知的 FD；仅子进程承担故意保留。
    EXPECT_EXIT(
        {
            auto* queue = new LeaseReturnQueue;
            if (!queue->Open(4) || !queue->TryPush(Lease(0))) {
                _exit(1);
            }
            const int fd = queue->completion_fd();
            delete queue;
            if (::fcntl(fd, F_GETFD) < 0 || !Readable(fd)) {
                _exit(2);
            }
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace memory
} // namespace kbsocket
