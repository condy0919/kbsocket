// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/context_progress_group.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/runtime/shard_reactor.hpp"

namespace kbsocket {
namespace runtime {
namespace {

struct BlockingWork {
    std::atomic<bool> entered{false}, released{false};
    static ControlTaskResult Run(void* argument, std::uint64_t) noexcept {
        auto& work = *static_cast<BlockingWork*>(argument);
        work.entered.store(true, std::memory_order_release);
        work.released.wait(false, std::memory_order_acquire);
        return {};
    }
    void Release() {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
};

class Member final : public ProgressSource {
public:
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind, std::size_t budget,
                                                         std::uint64_t) noexcept override {
        ++progress_calls;
        last_budget = budget;
        if (trace && trace_size) {
            (*trace)[(*trace_size)++] = label;
        }
        if (fail_progress) {
            return std::unexpected(ProgressError{ProgressErrorCode::kProvider, 123});
        }
        return ProgressBatch{hot ? budget : 0, hot};
    }
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t budget) noexcept override {
        ++notifications;
        last_budget = budget;
        if (fail_notifications) {
            return std::unexpected(ProgressError{ProgressErrorCode::kSystem, 124});
        }
        return ProgressBatch{hot ? budget : 0, hot};
    }
    std::expected<void, ProgressError> Rearm() noexcept override {
        ++rearms;
        if (fail_rearm) {
            return std::unexpected(ProgressError{ProgressErrorCode::kSystem, 125});
        }
        return {};
    }
    std::span<const int> wait_fds() const noexcept override {
        return std::span(fds).first(fd_count);
    }
    bool WaitFdEnabled(int) const noexcept override {
        return fd_enabled;
    }
    const void* exclusive_context() const noexcept override {
        return context;
    }
    std::optional<ControlTask> PendingControl() noexcept override {
        ++pending_calls;
        return pending;
    }
    void ControlSubmitted(ControlTaskId id) noexcept override {
        ++submitted;
        last_id = id;
        pending.reset();
    }
    void ControlCompleted(const ControlTaskCompletion& completion) noexcept override {
        ++completed;
        EXPECT_EQ(completion.id, last_id);
    }
    std::array<int, 2> fds{};
    std::size_t fd_count = 0;
    std::size_t progress_calls = 0, notifications = 0, rearms = 0, last_budget = 0;
    std::size_t pending_calls = 0, submitted = 0, completed = 0;
    bool hot = false, fd_enabled = true, fail_progress = false, fail_notifications = false, fail_rearm = false;
    std::optional<ControlTask> pending;
    ControlTaskId last_id;
    std::array<std::size_t, 16>* trace = nullptr;
    std::size_t* trace_size = nullptr;
    std::size_t label = 0;
    const void* context = nullptr;
};

TEST(ContextProgressGroupTest, ValidatesMembersAndFdOwnershipBeforeOpen) {
    ContextProgressGroup group;
    Member a, b;
    std::array<ProgressSource*, 2> members{&a, &b};
    EXPECT_FALSE(group.Open({}));
    members[1] = nullptr;
    EXPECT_FALSE(group.Open(members));
    members[1] = &a;
    EXPECT_FALSE(group.Open(members));
    members[1] = &group;
    EXPECT_FALSE(group.Open(members));
    members[1] = &b;
    a.fds[0] = b.fds[0] = 7;
    a.fd_count = b.fd_count = 1;
    EXPECT_FALSE(group.Open(members));
    b.fds[0] = -1;
    EXPECT_FALSE(group.Open(members));
    b.fds[0] = 8;
    ASSERT_TRUE(group.Open(members));
    EXPECT_FALSE(group.Open(members));
    ASSERT_EQ(group.wait_fds().size(), 2u);
    EXPECT_EQ(group.wait_fds()[0], 7);
    EXPECT_EQ(group.wait_fds()[1], 8);
    EXPECT_TRUE(group.Close());
    EXPECT_TRUE(group.wait_fds().empty());
    EXPECT_FALSE(group.Progress(ProgressKind::kTx, 1, 1));
    EXPECT_FALSE(group.CollectNotifications(1));
    EXPECT_FALSE(group.Rearm());
    EXPECT_FALSE(group.PendingControl());
    EXPECT_TRUE(group.Open(members));
    EXPECT_TRUE(group.Close());
}

TEST(ContextProgressGroupTest, IndependentBudgetsRotateFirstMemberAndKeepOtherMembersRunningOnError) {
    ContextProgressGroup group;
    std::array<Member, 3> members;
    std::array<ProgressSource*, 3> sources{};
    std::array<std::size_t, 16> trace{};
    std::size_t trace_size = 0;
    for (std::size_t i = 0; i < members.size(); ++i) {
        sources[i] = &members[i];
        members[i].hot = true;
        members[i].label = i;
        members[i].trace = &trace;
        members[i].trace_size = &trace_size;
    }
    ASSERT_TRUE(group.Open(sources));
    // 一个热成员不能吃掉同 context 的连接、回收和计时推进份额。
    auto step = group.Progress(ProgressKind::kMailbox, 2, 1);
    ASSERT_TRUE(step);
    EXPECT_EQ(step->processed, 6u);
    EXPECT_TRUE(step->runnable);
    ASSERT_TRUE(group.Progress(ProgressKind::kMailbox, 2, 2));
    EXPECT_EQ(trace[0], 0u);
    EXPECT_EQ(trace[1], 1u);
    EXPECT_EQ(trace[2], 2u);
    EXPECT_EQ(trace[3], 1u);
    EXPECT_EQ(trace[4], 2u);
    EXPECT_EQ(trace[5], 0u);
    members[1].fail_progress = true;
    step = group.Progress(ProgressKind::kMailbox, 2, 3);
    ASSERT_FALSE(step);
    EXPECT_EQ(step.error().detail, 123);
    EXPECT_EQ(step.error().source, 1u);
    for (const auto& member : members) {
        EXPECT_EQ(member.last_budget, 2u);
        EXPECT_EQ(member.progress_calls, 3u);
    }
    EXPECT_FALSE(group.Progress(static_cast<ProgressKind>(255), 2, 4));
    EXPECT_FALSE(group.Progress(ProgressKind::kTx, 0, 4));
    EXPECT_TRUE(group.Close());
}

TEST(ContextProgressGroupTest, RejectsDifferentDeviceContextsAndPropagatesSharedIdentity) {
    ContextProgressGroup group;
    std::array<int, 2> contexts{};
    Member raw, connections, cpu;
    raw.context = &contexts[0];
    connections.context = &contexts[1];
    std::array<ProgressSource*, 3> members{&cpu, &raw, &connections};
    // 即使控制任务位于同一组，错误 context 的连接也不能借用另一个 context 的暂停证明。
    EXPECT_FALSE(group.Open(members));
    EXPECT_EQ(group.exclusive_context(), nullptr);
    connections.context = &contexts[0];
    ASSERT_TRUE(group.Open(members));
    EXPECT_EQ(group.exclusive_context(), &contexts[0]);
    ASSERT_TRUE(group.Close());
    EXPECT_EQ(group.exclusive_context(), nullptr);
    raw.context = connections.context = nullptr;
    ASSERT_TRUE(group.Open(members));
    EXPECT_EQ(group.exclusive_context(), nullptr);
    EXPECT_TRUE(group.Close());
}

TEST(ContextProgressGroupTest, NotificationAndRearmFailuresDoNotSkipSiblingAndFdStateUsesOwner) {
    ContextProgressGroup group;
    Member a, b;
    a.fds[0] = 7;
    b.fds[0] = 8;
    a.fd_count = b.fd_count = 1;
    a.hot = b.hot = true;
    std::array<ProgressSource*, 2> members{&a, &b};
    ASSERT_TRUE(group.Open(members));
    auto notified = group.CollectNotifications(3);
    ASSERT_TRUE(notified);
    EXPECT_EQ(notified->processed, 6u);
    EXPECT_TRUE(notified->runnable);
    a.fail_notifications = a.fail_rearm = true;
    EXPECT_FALSE(group.CollectNotifications(3));
    EXPECT_FALSE(group.Rearm());
    EXPECT_EQ(a.notifications, 2u);
    EXPECT_EQ(b.notifications, 2u);
    EXPECT_EQ(a.rearms, 1u);
    EXPECT_EQ(b.rearms, 1u);
    a.fd_enabled = false;
    EXPECT_FALSE(group.WaitFdEnabled(7));
    EXPECT_TRUE(group.WaitFdEnabled(8));
    EXPECT_FALSE(group.WaitFdEnabled(9));
    EXPECT_TRUE(group.Close());
}

TEST(ContextProgressGroupTest, RetriesSameControlAndOnlyReturnsCompletionToSelectedMember) {
    ContextProgressGroup group;
    Member a, b;
    BlockingWork work_a, work_b;
    ControlExecutor identity;
    a.pending = ControlTask{BlockingWork::Run, &work_a, 41};
    b.pending = ControlTask{BlockingWork::Run, &work_b, 42};
    std::array<ProgressSource*, 2> members{&a, &b};
    ASSERT_TRUE(group.Open(members));
    const auto first = group.PendingControl();
    ASSERT_TRUE(first);
    EXPECT_EQ(first->token, 41u);
    // 尚未提交的请求在队列满时原样保留，不再选择其他成员或改写参数。
    const auto retry = group.PendingControl();
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->argument, first->argument);
    EXPECT_EQ(a.pending_calls, 1u);
    EXPECT_EQ(b.pending_calls, 0u);
    const ControlTaskId id{&identity, 0, 1};
    group.ControlSubmitted(id);
    EXPECT_FALSE(group.Close());
    EXPECT_FALSE(group.Progress(ProgressKind::kRx, 1, 1));
    EXPECT_FALSE(group.CollectNotifications(1));
    EXPECT_FALSE(group.Rearm());
    EXPECT_FALSE(group.PendingControl());
    group.ControlCompleted({id, first->argument, first->token, false, {}});
    EXPECT_EQ(a.completed, 1u);
    EXPECT_EQ(b.completed, 0u);
    const auto second = group.PendingControl();
    ASSERT_TRUE(second);
    EXPECT_EQ(second->token, 42u);
    // 未提交时 Close 只解绑本组，不擅自取消借用成员保留的任务。
    EXPECT_TRUE(group.Close());
    EXPECT_TRUE(b.pending);
}

TEST(ContextProgressGroupTest, WrongCompletionRetainsPauseUntilCorrectTaskIsCollected) {
    ContextProgressGroup group;
    Member a;
    BlockingWork work;
    ControlExecutor identity;
    a.pending = ControlTask{BlockingWork::Run, &work, 1};
    std::array<ProgressSource*, 1> members{&a};
    ASSERT_TRUE(group.Open(members));
    ASSERT_TRUE(group.PendingControl());
    const ControlTaskId id{&identity, 0, 1};
    group.ControlSubmitted(id);
    group.ControlCompleted({id, &work, 2, false, {}});
    EXPECT_EQ(a.completed, 0u);
    EXPECT_FALSE(group.Close());
    EXPECT_FALSE(group.Progress(ProgressKind::kRx, 1, 1));
    group.ControlCompleted({id, &work, 1, false, {}});
    EXPECT_EQ(a.completed, 1u);
    EXPECT_TRUE(group.Close());
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

class ContextGroupReactorTest : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(executor_.Open({1, 1}));
        for (auto& member : members_) {
            member.fds[0] = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            member.fd_count = 1;
            ASSERT_GE(member.fds[0], 0);
        }
        std::array<ProgressSource*, 3> same_context{&members_[0], &members_[1], &members_[2]};
        ASSERT_TRUE(group_.Open(same_context));
    }
    void TearDown() override {
        reactor_.StopAccepting();
        for (auto& work : work_) {
            work.Release();
        }
        EXPECT_TRUE(Until([&] {
            if (executor_.outstanding()) {
                (void)reactor_.Tick();
            }
            return !executor_.outstanding();
        }));
        EXPECT_TRUE(reactor_.Close());
        EXPECT_TRUE(group_.Close());
        EXPECT_TRUE(executor_.Close());
        for (auto& member : members_) {
            ::close(member.fds[0]);
        }
    }
    std::array<Member, 4> members_;
    std::array<BlockingWork, 2> work_;
    ContextProgressGroup group_;
    ControlExecutor executor_;
    ShardReactor reactor_;
};

TEST_F(ContextGroupReactorTest, BlockingImportPausesEveryContextMemberButNotOtherContext) {
    std::array<ProgressSource*, 2> sources{&group_, &members_[3]};
    members_[2].pending = ControlTask{BlockingWork::Run, &work_[0], 10};
    ASSERT_TRUE(reactor_.Open(sources, executor_, {.events = true, .budget = 2}));
    ASSERT_TRUE(reactor_.Tick());
    ASSERT_TRUE(Until([&] { return work_[0].entered.load(std::memory_order_acquire); }));
    ASSERT_TRUE(reactor_.paused(0));
    const auto paused_calls = members_[0].progress_calls;
    const auto other_calls = members_[3].progress_calls;
    // 一个 context 的控制线程阻塞时，raw poll、连接发送及事件消费全部停下，另一个继续推进。
    ASSERT_TRUE(reactor_.Tick());
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(members_[i].progress_calls, paused_calls);
        EXPECT_EQ(members_[i].notifications, 1u);
    }
    EXPECT_GT(members_[3].progress_calls, other_calls);
    EXPECT_FALSE(group_.Close());
    work_[0].Release();
    ASSERT_TRUE(Until([&] {
        const auto tick = reactor_.Tick();
        EXPECT_TRUE(tick);
        return members_[2].completed == 1;
    }));
    EXPECT_FALSE(reactor_.paused(0));
    EXPECT_EQ(members_[0].completed, 0u);
    EXPECT_EQ(members_[1].completed, 0u);
    EXPECT_GT(members_[0].progress_calls, paused_calls);
}

TEST_F(ContextGroupReactorTest, ExecutorBackpressureKeepsPreparedControlAndContextRunnable) {
    // 先让独立源占满执行器；组内预备导入等待容量时不应暂停 raw/连接，也不能更换任务所有者。
    std::array<ProgressSource*, 2> sources{&members_[3], &group_};
    members_[3].pending = ControlTask{BlockingWork::Run, &work_[0], 10};
    members_[2].pending = ControlTask{BlockingWork::Run, &work_[1], 20};
    ASSERT_TRUE(reactor_.Open(sources, executor_, {.events = true, .budget = 2}));
    ASSERT_TRUE(reactor_.Tick());
    ASSERT_TRUE(reactor_.paused(0));
    EXPECT_FALSE(reactor_.paused(1));
    const auto calls = members_[0].progress_calls;
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_GT(members_[0].progress_calls, calls);
    EXPECT_EQ(members_[2].pending_calls, 1u);
    EXPECT_EQ(members_[2].submitted, 0u);
    EXPECT_GE(reactor_.stats().control_backpressure, 2u);
    work_[0].Release();
    ASSERT_TRUE(Until([&] {
        EXPECT_TRUE(reactor_.Tick());
        return members_[2].submitted == 1;
    }));
    EXPECT_TRUE(reactor_.paused(1));
    work_[1].Release();
    ASSERT_TRUE(Until([&] {
        EXPECT_TRUE(reactor_.Tick());
        return members_[2].completed == 1;
    }));
    EXPECT_FALSE(reactor_.paused(1));
}

} // namespace
} // namespace runtime
} // namespace kbsocket
