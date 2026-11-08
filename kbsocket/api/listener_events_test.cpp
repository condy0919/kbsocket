// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/listener_events.hpp"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>

#include <gtest/gtest.h>

namespace kbsocket {
namespace api {
namespace {
struct Fd {
    int value = -1;
    ~Fd() {
        if (value >= 0) {
            (void)::close(value);
        }
    }
};
void Signal(int fd) {
    const std::uint64_t value = 1;
    ASSERT_EQ(::write(fd, &value, sizeof(value)), static_cast<ssize_t>(sizeof(value)));
}
constexpr kbs_listener_handle Handle(std::uint32_t slot = 0, std::uint64_t generation = 0x100000007ull) {
    return {42, generation, slot, 0};
}

TEST(ListenerEventsTest, ExplicitWatchOwnsOneshotAndReplacesOrCancelsQueuedHints) {
    ListenerEvents events;
    ASSERT_TRUE(events.Open(42, 1));
    Fd socket{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    ASSERT_GE(socket.value, 0);
    ASSERT_TRUE(events.Attach(Handle(), socket.value));
    Signal(socket.value);
    ASSERT_TRUE(events.CollectNotifications(64));
    EXPECT_FALSE(events.pending());

    ASSERT_TRUE(events.Watch(Handle(), true));
    ASSERT_TRUE(events.Progress(runtime::ProgressKind::kTimer, 1, 0));
    EXPECT_FALSE(events.pending());
    const auto collected = events.Progress(runtime::ProgressKind::kMailbox, 1, 0);
    ASSERT_TRUE(collected);
    EXPECT_EQ(collected->processed, 1u);
    EXPECT_TRUE(collected->runnable);
    std::array<kbs_listener_event, 1> output{};
    ASSERT_EQ(events.Poll(output), 1u);
    EXPECT_EQ(output[0].listener.generation, Handle().generation);
    EXPECT_EQ(output[0].flags, KBS_LISTENER_EVENT_ACCEPTABLE);
    EXPECT_EQ(output[0].reserved, 0u);

    // fd 仍然可读，Reactor 的 Rearm 不得把已消费的应用一次性订阅自动恢复。
    ASSERT_TRUE(events.Rearm());
    ASSERT_TRUE(events.CollectNotifications(64));
    EXPECT_FALSE(events.pending());
    ASSERT_TRUE(events.Watch(Handle(), true));
    ASSERT_TRUE(events.CollectNotifications(64));
    EXPECT_TRUE(events.pending());
    ASSERT_TRUE(events.Watch(Handle(), true));
    EXPECT_FALSE(events.pending());
    ASSERT_TRUE(events.CollectNotifications(64));
    EXPECT_TRUE(events.pending());
    ASSERT_TRUE(events.Watch(Handle(), false));
    EXPECT_FALSE(events.pending());
    ASSERT_TRUE(events.CollectNotifications(64));
    EXPECT_FALSE(events.pending());
    ASSERT_TRUE(events.Detach(Handle()));
    ASSERT_TRUE(events.Close());
    EXPECT_GE(::fcntl(socket.value, F_GETFD), 0);
}

TEST(ListenerEventsTest, CollectionRespectsBudgetAndDetachUnlinksOnePendingListener) {
    ListenerEvents events;
    ASSERT_TRUE(events.Open(42, 4));
    std::array<Fd, 4> sockets;
    for (std::uint32_t i = 0; i < sockets.size(); ++i) {
        sockets[i].value = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        ASSERT_GE(sockets[i].value, 0);
        ASSERT_TRUE(events.Attach(Handle(i), sockets[i].value));
        ASSERT_TRUE(events.Watch(Handle(i), true));
        Signal(sockets[i].value);
    }
    const auto first = events.CollectNotifications(2);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->processed, 2u);
    EXPECT_TRUE(first->runnable);
    std::array<kbs_listener_event, 1> one{};
    ASSERT_EQ(events.Poll(one), 1u);
    ASSERT_TRUE(events.CollectNotifications(2));
    // 不依赖内核不同 fd 的排序；选取尚未交给应用的槽，删除其唯一 intrusive 节点。
    const auto removed = (one[0].listener.slot + 1) % sockets.size();
    ASSERT_TRUE(events.Detach(Handle(removed)));
    std::array<kbs_listener_event, 4> rest{};
    const auto count = events.Poll(rest);
    ASSERT_TRUE(count);
    EXPECT_EQ(*count, 2u);
    for (std::size_t i = 0; i < *count; ++i) {
        EXPECT_NE(rest[i].listener.slot, removed);
        EXPECT_NE(rest[i].listener.slot, one[0].listener.slot);
    }
    EXPECT_FALSE(events.pending());
    for (std::uint32_t i = 0; i < sockets.size(); ++i) {
        if (i != removed) {
            ASSERT_TRUE(events.Detach(Handle(i)));
        }
    }
    ASSERT_TRUE(events.Close());
}

TEST(ListenerEventsTest, StopDropsHintsAndDisablesWaitWithoutClosingBorrowedSockets) {
    ListenerEvents events;
    ASSERT_TRUE(events.Open(42, 1));
    Fd socket{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    ASSERT_GE(socket.value, 0);
    ASSERT_TRUE(events.Attach(Handle(), socket.value));
    ASSERT_TRUE(events.Watch(Handle(), true));
    Signal(socket.value);
    ASSERT_TRUE(events.CollectNotifications(64));
    ASSERT_TRUE(events.pending());
    ASSERT_EQ(events.wait_fds().size(), 1u);
    const auto wait_fd = events.wait_fds()[0];
    EXPECT_TRUE(events.WaitFdEnabled(wait_fd));
    events.Stop();
    EXPECT_FALSE(events.pending());
    EXPECT_FALSE(events.WaitFdEnabled(wait_fd));
    const auto collected = events.CollectNotifications(64);
    ASSERT_TRUE(collected);
    EXPECT_EQ(collected->processed, 0u);
    EXPECT_FALSE(collected->runnable);
    EXPECT_FALSE(events.Watch(Handle(), true));
    ASSERT_TRUE(events.Watch(Handle(), false));
    EXPECT_FALSE(events.Close());
    EXPECT_GE(::fcntl(socket.value, F_GETFD), 0);
    ASSERT_TRUE(events.Detach(Handle()));
    ASSERT_TRUE(events.Close());
}

TEST(ListenerEventsTest, ExplicitDetachRemovesDupAliasBeforeFdAndSlotReuse) {
    ListenerEvents events;
    ASSERT_TRUE(events.Open(42, 1));
    Fd socket{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    ASSERT_GE(socket.value, 0);
    Fd alias{::dup(socket.value)};
    ASSERT_GE(alias.value, 0);
    ASSERT_TRUE(events.Attach(Handle(), socket.value));
    ASSERT_TRUE(events.Watch(Handle(), true));
    Signal(socket.value);
    // 内核已排队但尚未收割。dup 让旧 open-file-description 在原 fd 关闭后仍存活。
    ASSERT_TRUE(events.Detach(Handle()));
    const auto reused_fd = socket.value;
    ASSERT_EQ(::close(socket.value), 0);
    socket.value = -1;
    Fd replacement{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    ASSERT_GE(replacement.value, 0);
    if (replacement.value != reused_fd) {
        ASSERT_EQ(::dup2(replacement.value, reused_fd), reused_fd);
        ASSERT_EQ(::close(replacement.value), 0);
        replacement.value = reused_fd;
    }
    const auto current = Handle(0, Handle().generation + 1);
    ASSERT_TRUE(events.Attach(current, replacement.value));
    ASSERT_TRUE(events.Watch(current, true));
    ASSERT_TRUE(events.CollectNotifications(64));
    EXPECT_FALSE(events.pending());
    EXPECT_FALSE(events.Watch(Handle(), true));
    EXPECT_FALSE(events.Detach(Handle()));
    Signal(replacement.value);
    ASSERT_TRUE(events.CollectNotifications(64));
    std::array<kbs_listener_event, 1> output{};
    ASSERT_EQ(events.Poll(output), 1u);
    EXPECT_EQ(output[0].listener.generation, current.generation);
    ASSERT_TRUE(events.Detach(current));
    ASSERT_TRUE(events.Close());
}

TEST(ListenerEventsTest, FailedDetachPreservesRegistrationAndPendingHintForRetry) {
    ListenerEvents events;
    ASSERT_TRUE(events.Open(42, 1));
    Fd socket{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    ASSERT_GE(socket.value, 0);
    Fd alias{::dup(socket.value)};
    ASSERT_GE(alias.value, 0);
    ASSERT_TRUE(events.Attach(Handle(), socket.value));
    ASSERT_TRUE(events.Watch(Handle(), true));
    Signal(socket.value);
    ASSERT_TRUE(events.CollectNotifications(64));
    ASSERT_TRUE(events.pending());
    // 故意违反借用约定制造 EBADF；还原同一个 open-file-description 后才允许成功撤销。
    ASSERT_EQ(::close(socket.value), 0);
    const auto failed = events.Detach(Handle());
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, runtime::ProgressErrorCode::kSystem);
    EXPECT_EQ(failed.error().detail, EBADF);
    EXPECT_TRUE(events.pending());
    EXPECT_FALSE(events.Close());
    ASSERT_EQ(::dup2(alias.value, socket.value), socket.value);
    ASSERT_TRUE(events.Detach(Handle()));
    EXPECT_FALSE(events.pending());
    ASSERT_TRUE(events.Close());
}

TEST(ListenerEventsTest, HangupIsReportedWithoutInventingAnErrorNumber) {
    ListenerEvents events;
    ASSERT_TRUE(events.Open(42, 1));
    int pipe[2];
    ASSERT_EQ(::pipe2(pipe, O_NONBLOCK | O_CLOEXEC), 0);
    Fd reader{pipe[0]}, writer{pipe[1]};
    ASSERT_TRUE(events.Attach(Handle(), reader.value));
    ASSERT_TRUE(events.Watch(Handle(), true));
    ASSERT_EQ(::close(writer.value), 0);
    writer.value = -1;
    ASSERT_TRUE(events.CollectNotifications(64));
    std::array<kbs_listener_event, 1> output{};
    ASSERT_EQ(events.Poll(output), 1u);
    EXPECT_EQ(output[0].flags, KBS_LISTENER_EVENT_HANGUP);
    EXPECT_EQ(output[0].reserved, 0u);
    ASSERT_TRUE(events.Detach(Handle()));
    ASSERT_TRUE(events.Close());
}

TEST(ListenerEventsTest, ValidatesIdentityCapacityAndPollBoundsWithoutPartialMutation) {
    ListenerEvents events;
    EXPECT_FALSE(events.Open(0, 1));
    EXPECT_FALSE(events.Open(42, 65537));
    ASSERT_TRUE(events.Open(42, 1));
    Fd socket{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    ASSERT_GE(socket.value, 0);
    auto invalid = Handle();
    invalid.reserved = 1;
    EXPECT_FALSE(events.Attach(invalid, socket.value));
    invalid = Handle();
    invalid.runtime_id = 43;
    EXPECT_FALSE(events.Attach(invalid, socket.value));
    EXPECT_FALSE(events.Attach(Handle(1), socket.value));
    ASSERT_TRUE(events.Attach(Handle(), socket.value));
    EXPECT_FALSE(events.Attach(Handle(), socket.value));
    invalid = Handle();
    invalid.generation += std::uint64_t{1} << 32;
    EXPECT_FALSE(events.Watch(invalid, true));
    EXPECT_FALSE(events.Detach(invalid));
    EXPECT_FALSE(events.Poll({}));
    std::array<kbs_listener_event, 65> oversized{};
    EXPECT_FALSE(events.Poll(oversized));
    ASSERT_TRUE(events.Detach(Handle()));
    ASSERT_TRUE(events.Close());
    EXPECT_TRUE(events.wait_fds().empty());
    // 纯客户端配置也提供稳定空 epoll 源，不允许越界添加监听器。
    ASSERT_TRUE(events.Open(43, 0));
    EXPECT_FALSE(events.Attach({43, 1, 0, 0}, socket.value));
    ASSERT_TRUE(events.Close());
}
} // namespace
} // namespace api
} // namespace kbsocket
