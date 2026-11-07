// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/runtime/control_executor.hpp"

#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <new>
#include <span>
#include <thread>

#include <gtest/gtest.h>

namespace {
// 只统计 owner 线程；worker 工作函数本身可以分配，不能污染提交/收割路径的测量。
thread_local bool count_allocations = false;
thread_local bool fail_next_allocation = false;
thread_local std::size_t allocation_count = 0;
thread_local bool track_thread_calls = false;
thread_local int fail_create_after = -1;
thread_local unsigned joined_threads = 0;

void RecordAllocation() {
    if (count_allocations) {
        ++allocation_count;
    }
    if (fail_next_allocation) {
        fail_next_allocation = false;
        throw std::bad_alloc();
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

// 仅本测试通过 --wrap 截获 pthread 调用，生产代码不引入故障注入分支或可替换全局函数表。
extern "C" int __real_pthread_create(pthread_t*, const pthread_attr_t*, void* (*)(void*), void*);
extern "C" int __real_pthread_join(pthread_t, void**);
extern "C" int __wrap_pthread_create(pthread_t* thread, const pthread_attr_t* attributes, void* (*function)(void*),
                                     void* argument) {
    if (track_thread_calls && fail_create_after >= 0) {
        if (fail_create_after == 0) {
            return EAGAIN;
        }
        --fail_create_after;
    }
    return __real_pthread_create(thread, attributes, function, argument);
}
extern "C" int __wrap_pthread_join(pthread_t thread, void** result) {
    const int status = __real_pthread_join(thread, result);
    if (track_thread_calls && status == 0) {
        ++joined_threads;
    }
    return status;
}

namespace kbsocket {
namespace runtime {
namespace {
template <class Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return true;
}

struct WorkState {
    std::atomic<unsigned> calls{0};
    std::atomic<std::uint64_t> last_token{0};
    int error = 0;

    static ControlTaskResult Run(void* argument, std::uint64_t token) noexcept {
        auto& state = *static_cast<WorkState*>(argument);
        state.last_token.store(token, std::memory_order_relaxed);
        state.calls.fetch_add(1, std::memory_order_release);
        if (state.error) {
            return std::unexpected(ControlTaskError{.code = state.error});
        }
        return {};
    }
};

struct Gate {
    std::atomic<bool> released{false};
    std::atomic<unsigned> entered{0};
    std::atomic<unsigned> completed{0};

    void Release() noexcept {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
    static ControlTaskResult Run(void* argument, std::uint64_t) noexcept {
        auto& gate = *static_cast<Gate*>(argument);
        gate.entered.fetch_add(1, std::memory_order_release);
        gate.released.wait(false, std::memory_order_acquire);
        gate.completed.fetch_add(1, std::memory_order_release);
        return {};
    }
};

class ControlExecutorTest : public ::testing::Test {
protected:
    void TearDown() override {
        gate_.Release();
        if (executor_.outstanding()) {
            std::array<ControlTaskCompletion, 16> completions{};
            EXPECT_TRUE(WaitUntil([&] {
                const auto polled = executor_.Poll(completions);
                return polled && executor_.outstanding() == 0;
            }));
        }
        EXPECT_TRUE(executor_.Close());
    }

    bool Collect(std::span<ControlTaskCompletion> completions) {
        std::size_t count = 0;
        return WaitUntil([&] {
            const auto polled = executor_.Poll(completions.subspan(count));
            if (!polled) {
                return false;
            }
            count += *polled;
            return count == completions.size();
        });
    }

    ControlTask Task(std::uint64_t token = 1) {
        return {.function = &WorkState::Run, .argument = &work_, .token = token};
    }
    ControlTask BlockedTask(std::uint64_t token = 1) {
        return {.function = &Gate::Run, .argument = &gate_, .token = token};
    }

    WorkState work_;
    Gate gate_;
    ControlExecutor executor_;
};

TEST_F(ControlExecutorTest, CapacityIncludesCompletedButUncollectedTasks) {
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 2}));
    const auto first = executor_.TrySubmit(Task(11));
    const auto second = executor_.TrySubmit(Task(22));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(WaitUntil([&] { return work_.calls.load(std::memory_order_acquire) == 2; }));
    auto full = executor_.TrySubmit(Task(33));
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, ControlExecutorErrorCode::kResourceExhausted);
    EXPECT_EQ(executor_.outstanding(), 2u);
    auto busy = executor_.Close();
    ASSERT_FALSE(busy);
    EXPECT_EQ(busy.error().code, ControlExecutorErrorCode::kInUse);
    EXPECT_FALSE(executor_.accepting());

    // 完成未被 owner 接管前不能退额度或 join；关闭失败后仍能收割每一个已接受结果。
    std::array<ControlTaskCompletion, 2> completions{};
    ASSERT_TRUE(Collect(completions));
    const auto first_result = std::find_if(completions.begin(), completions.end(),
                                           [&](const auto& completion) { return completion.id == *first; });
    const auto second_result = std::find_if(completions.begin(), completions.end(),
                                            [&](const auto& completion) { return completion.id == *second; });
    ASSERT_NE(first_result, completions.end());
    ASSERT_NE(second_result, completions.end());
    EXPECT_EQ(first_result->token, 11u);
    EXPECT_EQ(second_result->token, 22u);
    EXPECT_EQ(first_result->argument, &work_);
    EXPECT_TRUE(first_result->result);
    EXPECT_TRUE(executor_.Close());
    EXPECT_EQ(executor_.completion_fd(), -1);
}

TEST_F(ControlExecutorTest, LimitedPollLeavesTheCompletionFdReadable) {
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 3}));
    for (std::uint64_t token = 1; token <= 3; ++token) {
        ASSERT_TRUE(executor_.TrySubmit(Task(token)));
    }
    ASSERT_TRUE(WaitUntil([&] { return work_.calls.load(std::memory_order_acquire) == 3; }));
    const int fd = executor_.completion_fd();
    EXPECT_NE(::fcntl(fd, F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(fd, F_GETFD) & FD_CLOEXEC, 0);

    // 即使一次 eventfd read 合并了多个完成，限额收割后剩余结果仍必须能唤醒 Reactor。
    std::array<ControlTaskCompletion, 1> completion{};
    ASSERT_TRUE(Collect(completion));
    EXPECT_EQ(executor_.outstanding(), 2u);
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&descriptor, 1, 1000), 1);
    EXPECT_NE(descriptor.revents & POLLIN, 0);
    ASSERT_TRUE(Collect(completion));
    EXPECT_EQ(executor_.outstanding(), 1u);
    descriptor.revents = 0;
    ASSERT_EQ(::poll(&descriptor, 1, 1000), 1);
    EXPECT_NE(descriptor.revents & POLLIN, 0);
    ASSERT_TRUE(Collect(completion));
    EXPECT_EQ(executor_.outstanding(), 0u);
}

TEST_F(ControlExecutorTest, CancelledSuccessAndFailureAreStillDelivered) {
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 2}));
    work_.error = 73;
    auto task = Task(90);
    task.cancelled = true;
    const auto failed = executor_.TrySubmit(task);
    const auto blocked = executor_.TrySubmit(BlockedTask(91));
    ASSERT_TRUE(failed);
    ASSERT_TRUE(blocked);
    EXPECT_TRUE(executor_.Cancel(*blocked));
    EXPECT_TRUE(executor_.Cancel(*blocked));
    executor_.StopAccepting();
    const auto stopped = executor_.TrySubmit(Task());
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, ControlExecutorErrorCode::kStopped);
    gate_.Release();

    std::array<ControlTaskCompletion, 2> completions{};
    ASSERT_TRUE(Collect(completions));
    const auto failed_result = std::find_if(completions.begin(), completions.end(),
                                            [&](const auto& completion) { return completion.id == *failed; });
    const auto blocked_result = std::find_if(completions.begin(), completions.end(),
                                             [&](const auto& completion) { return completion.id == *blocked; });
    ASSERT_NE(failed_result, completions.end());
    ASSERT_NE(blocked_result, completions.end());
    EXPECT_TRUE(failed_result->cancelled);
    ASSERT_FALSE(failed_result->result);
    EXPECT_EQ(failed_result->result.error().code, 73);
    EXPECT_TRUE(blocked_result->cancelled);
    EXPECT_TRUE(blocked_result->result);
    EXPECT_EQ(gate_.completed.load(), 1u); // 已取消仍实际执行，结果中的半成品必须交回 owner 清理。
    EXPECT_FALSE(executor_.Cancel(*blocked));
}

TEST_F(ControlExecutorTest, SlowWorkerDoesNotBlockOrAllocateInOwnerOperations) {
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 2}));
    std::atomic<bool> owner_finished{false};
    std::atomic<bool> watchdog_fired{false};
    // 如果错误实现等待忙 worker，此保护会释放它并令测试失败，而非让整个测试进程永久挂住。
    std::thread watchdog([&] {
        if (!WaitUntil([&] { return owner_finished.load(std::memory_order_acquire); })) {
            watchdog_fired.store(true, std::memory_order_release);
            gate_.Release();
        }
    });
    std::array<ControlTaskCompletion, 1> completion{};
    allocation_count = 0;
    count_allocations = true;
    const auto first = executor_.TrySubmit(BlockedTask());
    count_allocations = false;
    // 工作线程扫描不承诺 FIFO；明确等待首任务进入 gate，才能断言后续 Poll 尚无完成。
    const bool entered = WaitUntil([&] { return gate_.entered.load(std::memory_order_acquire) == 1; });
    count_allocations = true;
    const auto second = executor_.TrySubmit(Task());
    const auto full = executor_.TrySubmit(Task());
    const auto cancelled = first ? executor_.Cancel(*first) : executor_.Cancel({});
    const auto empty = executor_.Poll(completion);
    executor_.StopAccepting();
    const auto busy = executor_.Close();
    count_allocations = false;
    const auto allocations = allocation_count;
    owner_finished.store(true, std::memory_order_release);
    gate_.Release();
    watchdog.join();

    EXPECT_FALSE(watchdog_fired.load());
    EXPECT_TRUE(entered);
    EXPECT_EQ(allocations, 0u);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_FALSE(full);
    EXPECT_TRUE(cancelled);
    ASSERT_TRUE(empty);
    EXPECT_EQ(*empty, 0u);
    ASSERT_FALSE(busy);
    EXPECT_EQ(busy.error().code, ControlExecutorErrorCode::kInUse);
    std::array<ControlTaskCompletion, 2> completions{};
    ASSERT_TRUE(WaitUntil([&] { return work_.calls.load(std::memory_order_acquire) == 1; }));
    // 非空收割也单独计数，不能只凭空 Poll 的零分配结果推断完成处理路径。
    allocation_count = 0;
    count_allocations = true;
    const auto collected = executor_.Poll(completions);
    count_allocations = false;
    const auto harvest_allocations = allocation_count;
    EXPECT_EQ(harvest_allocations, 0u);
    ASSERT_TRUE(collected);
    EXPECT_GE(*collected, 1u);
    if (*collected < completions.size()) {
        ASSERT_TRUE(Collect(std::span(completions).subspan(*collected)));
    }
    EXPECT_TRUE(completions[0].cancelled);
}

TEST_F(ControlExecutorTest, ConfiguredWorkersRunIndependentlyWithinTheHardLimit) {
    ASSERT_TRUE(executor_.Open({.workers = ControlExecutor::kMaxWorkers, .capacity = 4}));
    for (unsigned i = 0; i < 4; ++i) {
        ASSERT_TRUE(executor_.TrySubmit(BlockedTask(i)));
    }
    // 四个任务都卡在各自工作线程内；必须同时进入，不能在 owner 上执行或串成一个工作线程。
    EXPECT_TRUE(WaitUntil([&] { return gate_.entered.load(std::memory_order_acquire) == 4; }));
    gate_.Release();
    std::array<ControlTaskCompletion, 4> completions{};
    ASSERT_TRUE(Collect(completions));
    std::array<bool, 4> seen{};
    for (const auto& completion : completions) {
        ASSERT_LT(completion.token, seen.size());
        EXPECT_FALSE(seen[completion.token]);
        seen[completion.token] = true;
        EXPECT_TRUE(completion.result);
    }
}

TEST_F(ControlExecutorTest, OldTaskIdCannotCancelReusedSlotOrReopenedExecutor) {
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 1}));
    const auto old = executor_.TrySubmit(Task());
    ASSERT_TRUE(old);
    std::array<ControlTaskCompletion, 1> completion{};
    ASSERT_TRUE(Collect(completion));
    ASSERT_TRUE(executor_.Close());
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 1}));
    const auto current = executor_.TrySubmit(Task(2));
    ASSERT_TRUE(current);
    EXPECT_EQ(current->slot, old->slot);
    EXPECT_GT(current->sequence, old->sequence);
    const auto rejected = executor_.Cancel(*old);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, ControlExecutorErrorCode::kInvalidTask);
    ASSERT_TRUE(Collect(completion));
    EXPECT_EQ(completion[0].id, *current);
    EXPECT_FALSE(completion[0].cancelled);
}

TEST_F(ControlExecutorTest, TaskIdFromAnotherExecutorCannotCancelTheSameSlotAndSequence) {
    ControlExecutor other;
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 1}));
    ASSERT_TRUE(other.Open({.workers = 1, .capacity = 1}));
    const auto mine = executor_.TrySubmit(Task(1));
    const auto theirs = other.TrySubmit(Task(2));
    ASSERT_TRUE(mine);
    ASSERT_TRUE(theirs);
    EXPECT_EQ(mine->slot, theirs->slot);
    EXPECT_EQ(mine->sequence, theirs->sequence);
    EXPECT_NE(*mine, *theirs);
    // 每个 Shard 可拥有独立执行器；取消身份不能仅靠恰好重合的槽位和序号判断。
    EXPECT_FALSE(executor_.Cancel(*theirs));
    EXPECT_FALSE(other.Cancel(*mine));
    ASSERT_TRUE(executor_.Cancel(*mine));
    std::array<ControlTaskCompletion, 1> completion{};
    ASSERT_TRUE(Collect(completion));
    EXPECT_TRUE(completion[0].cancelled);
    ASSERT_TRUE(WaitUntil([&] {
        const auto polled = other.Poll(completion);
        return polled && *polled == 1;
    }));
    EXPECT_EQ(completion[0].id, *theirs);
    EXPECT_FALSE(completion[0].cancelled);
    ASSERT_TRUE(other.Close());
}

TEST_F(ControlExecutorTest, InvalidConfigurationAndRequestsHaveNoOutstandingWork) {
    for (const auto config :
         {ControlExecutorConfig{0, 1}, ControlExecutorConfig{5, 8}, ControlExecutorConfig{1, 0},
          ControlExecutorConfig{2, 1}, ControlExecutorConfig{1, ControlExecutor::kMaxCapacity + 1}}) {
        const auto opened = executor_.Open(config);
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, ControlExecutorErrorCode::kInvalidArgument);
        EXPECT_EQ(executor_.completion_fd(), -1);
    }
    EXPECT_FALSE(executor_.TrySubmit(Task()));
    EXPECT_FALSE(executor_.Cancel({}));
    std::array<ControlTaskCompletion, 1> completion{};
    EXPECT_FALSE(executor_.Poll(completion));
    ASSERT_TRUE(executor_.Open());
    EXPECT_FALSE(executor_.Open());
    EXPECT_FALSE(executor_.TrySubmit({}));
    EXPECT_FALSE(executor_.Cancel({.slot = 999, .sequence = 1}));
    EXPECT_FALSE(executor_.Poll({}));
    EXPECT_EQ(executor_.outstanding(), 0u);
}

TEST_F(ControlExecutorTest, AllocationFailureLeavesOpenRetryable) {
    fail_next_allocation = true;
    const auto failed = executor_.Open();
    fail_next_allocation = false;
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, ControlExecutorErrorCode::kNoMemory);
    EXPECT_EQ(executor_.completion_fd(), -1);
    ASSERT_TRUE(executor_.Open());
}

TEST_F(ControlExecutorTest, PartialWorkerCreationFailureJoinsStartedWorkersBeforeRetry) {
    // 第一个线程已能访问共享槽位，第二个创建失败；返回前必须停掉并 join 第一个，才能释放状态。
    joined_threads = 0;
    fail_create_after = 1;
    track_thread_calls = true;
    const auto opened = executor_.Open({.workers = 2, .capacity = 4});
    track_thread_calls = false;
    fail_create_after = -1;
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, ControlExecutorErrorCode::kThreadError);
    EXPECT_EQ(opened.error().system_error, EAGAIN);
    EXPECT_EQ(joined_threads, 1u);
    EXPECT_EQ(executor_.completion_fd(), -1);
    EXPECT_EQ(executor_.outstanding(), 0u);
    ASSERT_TRUE(executor_.Open({.workers = 2, .capacity = 4}));
    ASSERT_TRUE(executor_.TrySubmit(Task(17)));
    std::array<ControlTaskCompletion, 1> completion{};
    ASSERT_TRUE(Collect(completion));
    EXPECT_EQ(completion[0].token, 17u);
}

TEST_F(ControlExecutorTest, EventFdFailureDoesNotPublishPartialExecutor) {
    EXPECT_EXIT(
        {
            rlimit limit{};
            if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
                ::_exit(1);
            }
            limit.rlim_cur = 0;
            if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) {
                ::_exit(2);
            }
            const auto opened = executor_.Open();
            if (opened || opened.error().code != ControlExecutorErrorCode::kSystemError ||
                opened.error().system_error != EMFILE || executor_.completion_fd() != -1 ||
                executor_.outstanding() != 0 || !executor_.Close()) {
                ::_exit(3);
            }
            ::_exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(ControlExecutorTest, DestructionWithOutstandingWorkRetainsWorkerStorage) {
    // 有意违反 Close 前置条件的进程内保留只在子进程验证，不能在测试主进程伪装为已安全回收。
    EXPECT_EXIT(
        {
            Gate gate;
            int fd = -1;
            {
                ControlExecutor executor;
                if (!executor.Open({.workers = 1, .capacity = 1}) ||
                    !executor.TrySubmit({.function = &Gate::Run, .argument = &gate})) {
                    ::_exit(1);
                }
                fd = executor.completion_fd();
                if (!WaitUntil([&] { return gate.entered.load(std::memory_order_acquire) == 1; })) {
                    ::_exit(2);
                }
            }
            gate.Release();
            if (!WaitUntil([&] { return gate.completed.load(std::memory_order_acquire) == 1; }) ||
                ::fcntl(fd, F_GETFD) < 0) {
                ::_exit(3);
            }
            ::_exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace runtime
} // namespace kbsocket
