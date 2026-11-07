// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/shard_reactor.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <functional>
#include <limits>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace kbsocket {
namespace runtime {
namespace {
constexpr std::size_t kKinds = 8;
constexpr auto Index(ProgressKind kind) noexcept {
    return static_cast<std::size_t>(kind);
}

template <typename Predicate>
bool Until(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    return predicate();
}

struct BlockingTask {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    std::atomic<std::uint64_t> token{0};

    static ControlTaskResult Run(void* argument, std::uint64_t token) noexcept {
        auto& task = *static_cast<BlockingTask*>(argument);
        task.token.store(token, std::memory_order_relaxed);
        task.entered.store(true, std::memory_order_release);
        // 只阻塞控制线程；测试结束或断言失败时 TearDown 都会显式解除等待。
        task.released.wait(false, std::memory_order_acquire);
        return {};
    }
    void Release() noexcept {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
};

class FakeSource final : public ProgressSource {
public:
    FakeSource() : fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)), fds{fd_} {}
    ~FakeSource() override {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind kind, std::size_t budget,
                                                         std::uint64_t now_ns) noexcept override {
        const auto index = Index(kind);
        ++calls[index];
        budgets[index] = budget;
        EXPECT_GT(now_ns, 0u);
        if (trace) {
            trace->emplace_back(kind, label);
        }
        if (on_progress) {
            on_progress(*this, kind);
        }
        if (failure_kind == kind) {
            return std::unexpected(ProgressError{ProgressErrorCode::kProvider, EIO});
        }
        if (hot_tx && kind == ProgressKind::kTx) {
            return ProgressBatch{budget, true};
        }
        const auto processed = std::min(work[index], budget);
        work[index] -= processed;
        return ProgressBatch{processed, force_runnable[index] || work[index] != 0};
    }

    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t budget) noexcept override {
        ++collect_calls;
        if (!collect_notifications) {
            return ProgressBatch{};
        }
        std::uint64_t value = 0;
        const auto read = ::read(fd_, &value, sizeof(value));
        EXPECT_TRUE(read == sizeof(value) || (read == -1 && errno == EAGAIN));
        const auto count = std::min(notifications, budget);
        notifications -= count;
        retrieved += count;
        acked += count;
        if (notifications) {
            EXPECT_TRUE(Signal());
        }
        if (after_collect) {
            after_collect(*this);
        }
        return ProgressBatch{count, notifications != 0 && count == budget};
    }

    std::expected<void, ProgressError> Rearm() noexcept override {
        ++rearms;
        if (on_rearm) {
            on_rearm(*this);
        }
        return {};
    }

    std::span<const int> wait_fds() const noexcept override {
        return fds;
    }
    bool WaitFdEnabled(int) const noexcept override {
        return wait_enabled;
    }

    std::optional<ControlTask> PendingControl() noexcept override {
        ++pending_calls;
        return pending;
    }
    void ControlSubmitted(ControlTaskId id) noexcept override {
        ++submitted;
        submitted_id = id;
        pending.reset();
    }
    void ControlCompleted(const ControlTaskCompletion& completion) noexcept override {
        ++completed;
        last_completion = completion;
    }

    bool Notify(std::size_t count = 1) noexcept {
        notifications += count;
        return Signal();
    }
    std::size_t progress_calls() const noexcept {
        std::size_t total = 0;
        for (const auto count : calls) {
            total += count;
        }
        return total;
    }
    void Request(BlockingTask& task, std::uint64_t token) {
        pending = ControlTask{BlockingTask::Run, &task, token, false};
    }

private:
    bool Signal() noexcept {
        const std::uint64_t one = 1;
        return ::write(fd_, &one, sizeof(one)) == sizeof(one) || errno == EAGAIN;
    }
    // FD 在 fds 之前初始化，整个注册期间保持有效；无效注册测试只替换借用列表。
    int fd_;

public:
    std::vector<int> fds;
    std::size_t label = 0;
    bool hot_tx = false;
    bool wait_enabled = true, collect_notifications = true;
    std::array<std::size_t, kKinds> calls{}, budgets{}, work{};
    std::array<bool, kKinds> force_runnable{};
    std::optional<ProgressKind> failure_kind;
    std::size_t collect_calls = 0, rearms = 0, notifications = 0, retrieved = 0, acked = 0;
    std::size_t pending_calls = 0, submitted = 0, completed = 0;
    std::optional<ControlTask> pending;
    std::optional<ControlTaskId> submitted_id;
    std::optional<ControlTaskCompletion> last_completion;
    std::vector<std::pair<ProgressKind, std::size_t>>* trace = nullptr;
    std::function<void(FakeSource&, ProgressKind)> on_progress;
    std::function<void(FakeSource&)> on_rearm, after_collect;
};

class ShardReactorTest : public ::testing::Test {
public:
    void SetUp() override {
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        for (std::size_t i = 0; i < sources_.size(); ++i) {
            sources_[i].label = i;
            ASSERT_GE(sources_[i].fds[0], 0);
            pointers_[i] = &sources_[i];
        }
    }
    void TearDown() override {
        reactor_.StopAccepting();
        for (auto& task : tasks_) {
            task.Release();
        }
        EXPECT_TRUE(Until([&] {
            if (!executor_.outstanding()) {
                return true;
            }
            (void)reactor_.Tick();
            return !executor_.outstanding();
        }));
        EXPECT_TRUE(reactor_.Close());
        EXPECT_TRUE(executor_.Close());
    }
    std::expected<void, ProgressError> Open(std::size_t count, ReactorConfig config = {}) {
        return reactor_.Open(std::span(pointers_).first(count), executor_, config);
    }
    static ReactorConfig Events(std::size_t budget = 4) {
        ReactorConfig config;
        config.budget = budget;
        config.busy_poll_ns = 0;
        config.max_wait_ms = 1;
        config.events = true;
        return config;
    }
    bool Complete(std::size_t index) {
        return Until([&] {
            const auto tick = reactor_.Tick();
            EXPECT_TRUE(tick);
            return sources_[index].completed != 0;
        });
    }

protected:
    std::array<BlockingTask, 4> tasks_;
    ControlExecutor executor_;
    std::array<FakeSource, 4> sources_;
    std::array<ProgressSource*, 4> pointers_{};
    ShardReactor reactor_;
};

TEST_F(ShardReactorTest, FourHotSourcesRetainEveryProgressClassBudgetAndRotateFirstSource) {
    constexpr std::size_t budget = 3;
    std::vector<std::pair<ProgressKind, std::size_t>> trace;
    for (auto& source : sources_) {
        source.hot_tx = true;
        source.work.fill(4 * budget);
        source.trace = &trace;
    }
    ASSERT_TRUE(Open(4, Events(budget)));
    for (std::size_t round = 0; round < 4; ++round) {
        auto tick = reactor_.RunOnce();
        ASSERT_TRUE(tick);
        EXPECT_EQ(tick->processed, 4 * kKinds * budget);
        EXPECT_TRUE(tick->runnable);
        for (std::size_t kind = 0; kind < kKinds; ++kind) {
            for (std::size_t offset = 0; offset < 4; ++offset) {
                const auto& call = trace[round * kKinds * 4 + kind * 4 + offset];
                EXPECT_EQ(Index(call.first), kind);
                EXPECT_EQ(call.second, (round + offset) % 4);
            }
        }
    }
    for (const auto& source : sources_) {
        for (std::size_t kind = 0; kind < kKinds; ++kind) {
            EXPECT_EQ(source.calls[kind], 4u);
            EXPECT_EQ(source.budgets[kind], budget);
        }
        EXPECT_EQ(source.rearms, 0u);
    }
    EXPECT_EQ(reactor_.stats().wait_calls, 0u);
}

TEST_F(ShardReactorTest, RunnableWithoutProcessedWorkDoesNotArmOrSleep) {
    sources_[0].force_runnable[Index(ProgressKind::kFlush)] = true;
    ASSERT_TRUE(Open(1, Events()));
    const auto tick = reactor_.RunOnce();
    ASSERT_TRUE(tick);
    EXPECT_EQ(tick->processed, 0u);
    EXPECT_TRUE(tick->runnable);
    EXPECT_EQ(sources_[0].rearms, 0u);
    EXPECT_EQ(reactor_.stats().wait_calls, 0u);
}

TEST_F(ShardReactorTest, DefaultPollingModeNeverRearmsOrWaits) {
    ReactorConfig config;
    config.busy_poll_ns = 0;
    ASSERT_TRUE(Open(1, config));
    ASSERT_TRUE(reactor_.RunOnce());
    EXPECT_EQ(sources_[0].rearms, 0u);
    EXPECT_EQ(reactor_.stats().wait_calls, 0u);
}

TEST_F(ShardReactorTest, RearmRecheckConsumesNewCqeAndAcknowledgesNotificationBeforeWaiting) {
    sources_[0].on_rearm = [](FakeSource& source) {
        ++source.work[Index(ProgressKind::kTx)];
        EXPECT_TRUE(source.Notify());
    };
    ASSERT_TRUE(Open(1, Events()));
    const auto tick = reactor_.RunOnce();
    ASSERT_TRUE(tick);
    EXPECT_EQ(tick->processed, 2u);
    EXPECT_EQ(sources_[0].calls[Index(ProgressKind::kTx)], 2u);
    EXPECT_EQ(sources_[0].rearms, 1u);
    EXPECT_EQ(sources_[0].retrieved, 1u);
    EXPECT_EQ(sources_[0].acked, 1u);
    EXPECT_EQ(reactor_.stats().wait_calls, 0u);
}

TEST_F(ShardReactorTest, NotificationAfterFinalCollectionRemainsReadableAndIsAcknowledged) {
    sources_[0].after_collect = [](FakeSource& source) {
        if (source.collect_calls == 2) {
            EXPECT_TRUE(source.Notify());
        }
    };
    ASSERT_TRUE(Open(1, Events()));
    const auto tick = reactor_.RunOnce();
    ASSERT_TRUE(tick);
    EXPECT_EQ(tick->processed, 1u);
    EXPECT_EQ(reactor_.stats().wait_calls, 1u);
    EXPECT_GE(reactor_.stats().fd_wakes, 1u);
    EXPECT_EQ(reactor_.stats().empty_wakes, 0u);
    EXPECT_EQ(sources_[0].retrieved, 1u);
    EXPECT_EQ(sources_[0].acked, 1u);
}

TEST_F(ShardReactorTest, MailboxNotifyAfterLastRecheckCannotBeDrainedBeforeWait) {
    sources_[0].on_progress = [&](FakeSource& source, ProgressKind kind) {
        // 第二次 Tick 的 mailbox 已检查过，最后一个类别才发布 mailbox 并敲门。
        if (kind == ProgressKind::kRetire && source.calls[Index(kind)] == 2) {
            ++source.work[Index(ProgressKind::kMailbox)];
            EXPECT_TRUE(reactor_.Notify());
        }
    };
    ASSERT_TRUE(Open(1, Events()));
    const auto tick = reactor_.RunOnce();
    ASSERT_TRUE(tick);
    EXPECT_EQ(tick->processed, 1u);
    EXPECT_EQ(sources_[0].work[Index(ProgressKind::kMailbox)], 0u);
    EXPECT_EQ(sources_[0].calls[Index(ProgressKind::kMailbox)], 3u);
    EXPECT_EQ(reactor_.stats().wait_calls, 1u);
    EXPECT_GE(reactor_.stats().fd_wakes, 1u);
    EXPECT_EQ(reactor_.stats().empty_wakes, 0u);
}

TEST_F(ShardReactorTest, IdleWaitIsFiniteAndReportsEmptyWake) {
    ASSERT_TRUE(Open(1, Events()));
    const auto begin = std::chrono::steady_clock::now();
    const auto tick = reactor_.RunOnce();
    ASSERT_TRUE(tick);
    EXPECT_EQ(tick->processed, 0u);
    EXPECT_FALSE(tick->runnable);
    EXPECT_EQ(reactor_.stats().wait_calls, 1u);
    EXPECT_EQ(reactor_.stats().empty_wakes, 1u);
    EXPECT_EQ(reactor_.stats().fd_wakes, 0u);
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(2));
}

TEST_F(ShardReactorTest, DisabledReadableFdDoesNotCauseHotWakeAndOtherSourcesStillWake) {
    ASSERT_TRUE(Open(2, Events()));
    sources_[0].wait_enabled = false;
    sources_[0].collect_notifications = false;
    EXPECT_TRUE(sources_[0].Notify()); // 模拟 retained 域不再读取但仍可读的 JFCE。
    sources_[1].after_collect = [](FakeSource& source) {
        if (source.collect_calls == 2) {
            EXPECT_TRUE(source.Notify());
        }
    };
    ASSERT_TRUE(reactor_.RunOnce());
    EXPECT_EQ(reactor_.stats().wait_calls, 1u);
    EXPECT_EQ(reactor_.stats().fd_wakes, 1u);
    EXPECT_EQ(sources_[0].acked, 0u);
    EXPECT_EQ(sources_[0].notifications, 1u);
    EXPECT_EQ(sources_[1].acked, 1u);
    ASSERT_TRUE(reactor_.RunOnce());
    EXPECT_EQ(reactor_.stats().wait_calls, 2u);
    EXPECT_EQ(reactor_.stats().fd_wakes, 1u);
    EXPECT_EQ(reactor_.stats().empty_wakes, 1u);
}

TEST_F(ShardReactorTest, ProgressAndRearmCallbacksCannotReenterOrCloseReactor) {
    const auto rejected = [&] {
        const auto tick = reactor_.Tick();
        const auto run = reactor_.RunOnce();
        const auto close = reactor_.Close();
        EXPECT_FALSE(tick);
        EXPECT_FALSE(run);
        EXPECT_FALSE(close);
        if (!tick && !run && !close) {
            EXPECT_EQ(tick.error().code, ProgressErrorCode::kInUse);
            EXPECT_EQ(run.error().code, ProgressErrorCode::kInUse);
            EXPECT_EQ(close.error().code, ProgressErrorCode::kInUse);
        }
    };
    sources_[0].on_progress = [&](FakeSource&, ProgressKind) { rejected(); };
    sources_[0].on_rearm = [&](FakeSource&) { rejected(); };
    ASSERT_TRUE(Open(1, Events()));
    ASSERT_TRUE(reactor_.RunOnce());
    EXPECT_EQ(sources_[0].rearms, 1u);
    // 关闭回调只在 RunOnce 内被拒绝，返回后仍可正常关闭。
    EXPECT_TRUE(reactor_.Close());
}

TEST_F(ShardReactorTest, SlowControlPausesOnlyItsSourceAndCompletionRestoresProgress) {
    sources_[0].Request(tasks_[0], 71);
    for (std::size_t i = 1; i < sources_.size(); ++i) {
        sources_[i].hot_tx = true;
    }
    ASSERT_TRUE(Open(4, Events()));
    ASSERT_TRUE(reactor_.Tick());
    ASSERT_TRUE(Until([&] { return tasks_[0].entered.load(std::memory_order_acquire); }));
    EXPECT_TRUE(reactor_.paused(0));
    EXPECT_EQ(sources_[0].submitted, 1u);
    const auto paused_calls = sources_[0].calls;
    const auto paused_collects = sources_[0].collect_calls;
    EXPECT_TRUE(sources_[0].Notify());
    auto closed = reactor_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, ProgressErrorCode::kInUse);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(reactor_.RunOnce());
    }
    EXPECT_EQ(sources_[0].calls, paused_calls);
    EXPECT_EQ(sources_[0].collect_calls, paused_collects);
    for (std::size_t i = 1; i < sources_.size(); ++i) {
        EXPECT_FALSE(reactor_.paused(i));
        EXPECT_EQ(sources_[i].calls[Index(ProgressKind::kRx)], 5u);
        EXPECT_EQ(sources_[i].calls[Index(ProgressKind::kTimer)], 5u);
        EXPECT_EQ(sources_[i].calls[Index(ProgressKind::kRetire)], 5u);
    }
    tasks_[0].Release();
    ASSERT_TRUE(Complete(0));
    EXPECT_FALSE(reactor_.paused(0));
    ASSERT_TRUE(sources_[0].last_completion);
    EXPECT_EQ(sources_[0].last_completion->argument, &tasks_[0]);
    EXPECT_EQ(sources_[0].last_completion->token, 71u);
    EXPECT_EQ(sources_[0].last_completion->id, sources_[0].submitted_id);
    EXPECT_TRUE(sources_[0].last_completion->result);
    EXPECT_GT(sources_[0].calls[Index(ProgressKind::kRx)], paused_calls[Index(ProgressKind::kRx)]);
    EXPECT_EQ(sources_[0].acked, 1u);
    EXPECT_EQ(reactor_.stats().controls_submitted, 1u);
    EXPECT_EQ(reactor_.stats().controls_completed, 1u);
    ASSERT_TRUE(reactor_.Close());
}

TEST_F(ShardReactorTest, FullControlQueueReturnsBackpressureWithoutPausingHealthySources) {
    ASSERT_TRUE(executor_.Close());
    ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 1}));
    sources_[0].Request(tasks_[0], 10);
    sources_[1].Request(tasks_[1], 20);
    sources_[2].hot_tx = true;
    ASSERT_TRUE(Open(3, Events()));
    const auto begin = std::chrono::steady_clock::now();
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(2));
    EXPECT_TRUE(reactor_.paused(0));
    EXPECT_FALSE(reactor_.paused(1));
    EXPECT_FALSE(reactor_.paused(2));
    EXPECT_EQ(sources_[0].submitted, 1u);
    EXPECT_EQ(sources_[1].submitted, 0u);
    EXPECT_TRUE(sources_[1].pending);
    EXPECT_EQ(reactor_.stats().control_backpressure, 1u);
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_EQ(sources_[1].calls[Index(ProgressKind::kRx)], 2u);
    EXPECT_EQ(sources_[2].calls[Index(ProgressKind::kTimer)], 2u);
    EXPECT_EQ(reactor_.stats().control_backpressure, 2u);
    tasks_[0].Release();
    ASSERT_TRUE(Until([&] {
        EXPECT_TRUE(reactor_.Tick());
        return sources_[1].submitted == 1;
    }));
    EXPECT_EQ(sources_[0].completed, 1u);
    EXPECT_TRUE(reactor_.paused(1));
    tasks_[1].Release();
    ASSERT_TRUE(Complete(1));
    EXPECT_FALSE(reactor_.paused(1));
    EXPECT_EQ(reactor_.stats().controls_completed, 2u);
}

TEST_F(ShardReactorTest, StopAcceptingStillReapsAcceptedControlAndAdvancesOtherWork) {
    sources_[0].Request(tasks_[0], 10);
    ASSERT_TRUE(Open(2, Events()));
    ASSERT_TRUE(reactor_.Tick());
    ASSERT_TRUE(reactor_.paused(0));
    reactor_.StopAccepting();
    sources_[1].Request(tasks_[1], 20);
    sources_[1].work[Index(ProgressKind::kMailbox)] = 1;
    const auto pending_calls = sources_[1].pending_calls;
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_EQ(sources_[1].work[Index(ProgressKind::kMailbox)], 0u);
    EXPECT_EQ(sources_[1].pending_calls, pending_calls);
    EXPECT_EQ(sources_[1].submitted, 0u);
    tasks_[0].Release();
    ASSERT_TRUE(Complete(0));
    EXPECT_EQ(sources_[1].submitted, 0u);
    EXPECT_TRUE(sources_[1].pending);
    EXPECT_EQ(executor_.outstanding(), 0u);
    EXPECT_TRUE(reactor_.Close());
}

TEST_F(ShardReactorTest, InvalidControlRequestIsAnErrorRatherThanQueueBackpressure) {
    sources_[0].pending = ControlTask{};
    sources_[1].work[Index(ProgressKind::kTimer)] = 1;
    ASSERT_TRUE(Open(2));
    const auto tick = reactor_.Tick();
    EXPECT_FALSE(tick);
    EXPECT_FALSE(reactor_.paused(0));
    EXPECT_EQ(reactor_.stats().control_backpressure, 0u);
    EXPECT_EQ(sources_[0].submitted, 0u);
    EXPECT_EQ(executor_.outstanding(), 0u);
    EXPECT_EQ(sources_[1].work[Index(ProgressKind::kTimer)], 0u);
}

TEST_F(ShardReactorTest, SourceFailureDoesNotStarveOtherSourcesOrLaterProgressClasses) {
    sources_[0].failure_kind = ProgressKind::kTx;
    sources_[1].work[Index(ProgressKind::kRx)] = 1;
    sources_[1].work[Index(ProgressKind::kTimer)] = 1;
    ASSERT_TRUE(Open(2));
    const auto tick = reactor_.Tick();
    ASSERT_FALSE(tick);
    EXPECT_EQ(tick.error().code, ProgressErrorCode::kProvider);
    EXPECT_EQ(tick.error().source, 0u);
    EXPECT_EQ(sources_[1].work[Index(ProgressKind::kRx)], 0u);
    EXPECT_EQ(sources_[1].work[Index(ProgressKind::kTimer)], 0u);
    EXPECT_EQ(sources_[0].calls[Index(ProgressKind::kRetire)], 1u);
}

TEST_F(ShardReactorTest, RejectsDuplicateSourcesInvalidAndDuplicateFdsThenAllowsValidOpen) {
    const auto invalid = [&](std::span<ProgressSource* const> sources) {
        const auto result = reactor_.Open(sources, executor_, Events());
        EXPECT_FALSE(result);
        if (!result) {
            EXPECT_EQ(result.error().code, ProgressErrorCode::kInvalidArgument);
        }
    };
    const std::array<ProgressSource*, 2> duplicate{&sources_[0], &sources_[0]};
    invalid(duplicate);
    const std::array<ProgressSource*, 1> missing{nullptr};
    invalid(missing);
    const auto original = sources_[0].fds;
    sources_[0].fds = {-1};
    invalid(std::span(pointers_).first(1));
    sources_[0].fds = {executor_.completion_fd()};
    invalid(std::span(pointers_).first(1));
    sources_[0].fds = {original[0], original[0]};
    invalid(std::span(pointers_).first(1));
    sources_[0].fds = {sources_[1].fds[0]};
    invalid(std::span(pointers_).first(2));
    // 非负但未打开的 fd 不能注册；失败后不得留下阻碍下一次 Open 的内部 fd/状态。
    sources_[0].fds = {std::numeric_limits<int>::max()};
    const auto unknown = Open(1, Events());
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, ProgressErrorCode::kSystem);
    sources_[0].fds = original;
    ASSERT_TRUE(Open(2, Events()));
    EXPECT_FALSE(Open(2, Events()));
    EXPECT_TRUE(reactor_.Tick());
}

TEST_F(ShardReactorTest, RejectsInvalidLimitsAndCallsOutsideOpenLifecycle) {
    EXPECT_FALSE(reactor_.Tick());
    EXPECT_FALSE(reactor_.RunOnce());
    EXPECT_FALSE(reactor_.Notify());
    EXPECT_FALSE(reactor_.paused(0));
    EXPECT_TRUE(reactor_.Close());
    EXPECT_FALSE(reactor_.Open({}, executor_, Events()));
    auto config = Events();
    for (const auto budget : {0u, 65u}) {
        config.budget = budget;
        EXPECT_FALSE(Open(1, config));
    }
    config = Events();
    for (const int timeout : {-1, 1001}) {
        config.max_wait_ms = timeout;
        EXPECT_FALSE(Open(1, config));
    }
    ControlExecutor unopened;
    EXPECT_FALSE(reactor_.Open(std::span(pointers_).first(1), unopened, Events()));
    ASSERT_TRUE(Open(1, Events()));
    EXPECT_TRUE(reactor_.Notify());
    EXPECT_TRUE(reactor_.Close());
    EXPECT_FALSE(reactor_.Notify());
    EXPECT_FALSE(reactor_.Tick());
}
} // namespace
} // namespace runtime
} // namespace kbsocket
