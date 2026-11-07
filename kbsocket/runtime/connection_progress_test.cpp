// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_progress.hpp"

#include <netinet/in.h>
#include <poll.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <new>

#include <gtest/gtest.h>

#include "kbsocket/runtime/runtime_shard.hpp"

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
} // namespace
void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void operator delete(void* p) noexcept {
    std::free(p);
}
void operator delete[](void* p) noexcept {
    std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
    std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
    std::free(p);
}

namespace kbsocket {
namespace runtime {
namespace {
using connection::Connection;
using connection::ConnectionState;
std::uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
struct TcpPair {
    int left = -1, right = -1;
    ~TcpPair() {
        if (left >= 0) {
            ::close(left);
        }
        if (right >= 0) {
            ::close(right);
        }
    }
    bool Open() {
        tcp::NativeTcpListener listener;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (!listener.Open(reinterpret_cast<sockaddr*>(&address), sizeof(address))) {
            return false;
        }
        socklen_t length = sizeof(address);
        if (::getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &length) < 0) {
            return false;
        }
        left = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (left < 0) {
            return false;
        }
        if (::connect(left, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 && errno != EINPROGRESS) {
            return false;
        }
        pollfd ready{listener.fd(), POLLIN, 0};
        if (::poll(&ready, 1, 1000) != 1) {
            return false;
        }
        auto fd = listener.Accept();
        if (!fd) {
            return false;
        }
        right = *fd;
        return true;
    }
};
control::ConnectionHandshakeConfig Config(std::uint64_t peer, std::uint64_t deadline) {
    control::ConnectionHandshakeConfig config;
    config.local.capabilities = control::kHandshakeCapabilityTcp;
    config.local.identity = {.runtime = peer,
                             .receive_domain = peer + 100,
                             .endpoint_generation = 1,
                             .authorization = 7,
                             .session = peer + 200,
                             .token = {0, 1}};
    config.expected_peer_authorization = 7;
    config.compatible_peer = true;
    config.deadline_ns = deadline;
    return config;
}
struct Observations {
    std::array<std::uint32_t, 128> ready{};
    std::size_t readiness = 0, failed = 0;
    static void Observe(void* pointer, const ConnectionProgressEvent& event) noexcept {
        auto& self = *static_cast<Observations*>(pointer);
        const auto slot = static_cast<std::uint32_t>(event.connection.id) - 1;
        if (event.fd_events) {
            ++self.readiness;
            if (slot < self.ready.size()) {
                self.ready[slot] |= event.fd_events;
            }
        }
        self.failed += event.state == ConnectionState::kFailed;
    }
};

TEST(ConnectionProgress, DeviceContextIdentityCannotUseAnotherPauseGroupOrStandaloneHooks) {
    ConnectionProgress progress;
    RuntimeShard shard;
    std::array<urma_context_t, 2> contexts{};
    EXPECT_EQ(progress.exclusive_context(), nullptr);
    ASSERT_TRUE(progress.Open({.capacity = 1, .context = &contexts[0]}));
    EXPECT_EQ(progress.exclusive_context(), &contexts[0]);
    const RawContextBinding wrong{.context = &contexts[1], .queues = {}, .connections = &progress};
    // 必须在尝试装配原始队列前拒绝错误关联；不得把 context A 的发送放在 B 的暂停屏障下。
    auto opened = shard.Open(std::span(&wrong, 1));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, ProgressErrorCode::kInvalidArgument);
    EXPECT_FALSE(shard.Open({}, {}, &progress));
    ASSERT_TRUE(progress.Close());
    EXPECT_EQ(progress.exclusive_context(), nullptr);
    ASSERT_TRUE(progress.Open({.capacity = 1}));
    ASSERT_TRUE(shard.Open({}, {}, &progress));
    EXPECT_TRUE(shard.Tick());
    EXPECT_TRUE(shard.Close());
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, BoundedFairQueueAndStaleHandleRejection) {
    ConnectionProgress progress;
    std::array<Connection, 3> connections;
    ASSERT_TRUE(progress.Open({.capacity = 3}));
    std::array<ScheduledConnection, 3> handles;
    for (std::size_t i = 0; i < handles.size(); ++i) {
        auto handle = progress.Attach(connections[i]);
        ASSERT_TRUE(handle);
        handles[i] = *handle;
    }
    // 重复 Wake 合并，预算 1 只访问一个连接，不能让首个热点占住整轮。
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(progress.Wake(handles[0]));
    }
    auto first = progress.Progress(ProgressKind::kMailbox, 1, Now());
    ASSERT_TRUE(first);
    EXPECT_TRUE(first->runnable);
    EXPECT_EQ(progress.stats()->visits, 1u);
    ASSERT_TRUE(progress.Progress(ProgressKind::kMailbox, 2, Now()));
    EXPECT_EQ(progress.stats()->visits, 3u);
    EXPECT_FALSE(progress.Detach(handles[0]));
    ASSERT_TRUE(connections[0].Close());
    ASSERT_TRUE(progress.Detach(handles[0]));
    Connection replacement;
    const auto newer = progress.Attach(replacement);
    ASSERT_TRUE(newer);
    EXPECT_NE(newer->id, handles[0].id);
    EXPECT_FALSE(progress.Wake(handles[0]));
    raw::TxCompletionEvent old;
    old.record.emplace();
    old.record->metadata.connection_id = handles[0].id;
    EXPECT_FALSE(progress.OnTxCompletion(old));
    ASSERT_TRUE(replacement.Close());
    ASSERT_TRUE(progress.Detach(*newer));
    for (std::size_t i = 1; i < handles.size(); ++i) {
        ASSERT_TRUE(connections[i].Close());
        ASSERT_TRUE(progress.Detach(handles[i]));
    }
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, CurrentFdLookupIncludesExternalAndInitiallyIdleConnections) {
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    Connection external, delayed;
    ConnectionProgress progress;
    EXPECT_FALSE(progress.HasFd(first.right));
    ASSERT_TRUE(progress.Open({.capacity = 2}));
    EXPECT_FALSE(progress.HasFd(-1));
    ASSERT_TRUE(external.OpenTcp(first.right));
    first.right = -1;
    const auto existing = progress.Attach(external);
    ASSERT_TRUE(existing);
    EXPECT_TRUE(progress.HasFd(external.fd()));
    EXPECT_FALSE(progress.HasFd(first.left));

    // 入口先 Attach 空 Connection，后接管 fd；查询必须读当前对象而非 Attach 时的缓存。
    const auto pending = progress.Attach(delayed);
    ASSERT_TRUE(pending);
    EXPECT_FALSE(progress.HasFd(second.right));
    ASSERT_TRUE(delayed.OpenTcp(second.right));
    second.right = -1;
    const int owned_fd = delayed.fd();
    allocations = 0;
    count_allocations = true;
    const bool found = progress.HasFd(owned_fd);
    count_allocations = false;
    EXPECT_TRUE(found);
    EXPECT_EQ(allocations, 0u);

    // Closed 槽尚未 Detach，原 fd 已失去所有权，不能继续阻止其数值被正常复用。
    ASSERT_TRUE(delayed.Close());
    EXPECT_FALSE(progress.HasFd(owned_fd));
    ASSERT_TRUE(progress.Detach(*pending));
    ASSERT_TRUE(external.Close());
    ASSERT_TRUE(progress.Detach(*existing));
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, NativeTcpOneShotReadinessDoesNotBusyLoopAndCanRearmExistingData) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    Connection connection;
    ASSERT_TRUE(connection.OpenTcp(pair.right));
    pair.right = -1;
    Observations observations;
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.capacity = 2}, Observations::Observe, &observations));
    const auto handle = progress.Attach(connection);
    ASSERT_TRUE(handle);
    ASSERT_TRUE(progress.WatchTcp(*handle, true, false));
    RuntimeShard shard;
    ASSERT_TRUE(
        shard.Open({}, {.reactor = {.events = true, .budget = 4, .busy_poll_ns = 0, .max_wait_ms = 1}}, &progress));
    const char input = 'x';
    ASSERT_EQ(::send(pair.left, &input, 1, MSG_NOSIGNAL), 1);
    for (int i = 0; i < 20 && !observations.readiness; ++i) {
        ASSERT_TRUE(shard.RunOnce());
    }
    ASSERT_EQ(observations.readiness, 1u);
    EXPECT_NE(observations.ready[0] & EPOLLIN, 0u);
    // 应用暂不读取也不重新订阅，持续可读的 TCP fd 不能把 Reactor 变成忙轮询。
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(shard.RunOnce());
    }
    EXPECT_EQ(observations.readiness, 1u);
    EXPECT_GT(shard.stats()->wait_calls, 0u);
    ASSERT_TRUE(progress.WatchTcp(*handle, true, false));
    for (int i = 0; i < 20 && observations.readiness < 2; ++i) {
        ASSERT_TRUE(shard.RunOnce());
    }
    ASSERT_EQ(observations.readiness, 2u);
    std::array<std::byte, 1> output;
    const auto read = connection.Read(output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 1u);
    EXPECT_EQ(output[0], std::byte{'x'});
    ASSERT_TRUE(shard.Close());
    ASSERT_TRUE(connection.Close());
    ASSERT_TRUE(progress.Detach(*handle));
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, MoreThanOuterReactorFdLimitUsesSingleStableFd) {
    constexpr std::size_t kConnections = 96;
    std::array<TcpPair, kConnections> pairs;
    std::array<Connection, kConnections> connections;
    std::array<ScheduledConnection, kConnections> handles;
    Observations observations;
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.capacity = kConnections}, Observations::Observe, &observations));
    const auto stable = progress.wait_fds().front();
    for (std::size_t i = 0; i < kConnections; ++i) {
        ASSERT_TRUE(pairs[i].Open());
        ASSERT_TRUE(connections[i].OpenTcp(pairs[i].right));
        pairs[i].right = -1;
        auto handle = progress.Attach(connections[i]);
        ASSERT_TRUE(handle);
        handles[i] = *handle;
        ASSERT_TRUE(progress.WatchTcp(*handle, true, false));
        const char byte = 'a';
        ASSERT_EQ(::send(pairs[i].left, &byte, 1, MSG_NOSIGNAL), 1);
    }
    RuntimeShard shard;
    ASSERT_TRUE(
        shard.Open({}, {.reactor = {.events = true, .budget = 7, .busy_poll_ns = 0, .max_wait_ms = 1}}, &progress));
    for (int i = 0; i < 100 && observations.readiness < kConnections; ++i) {
        ASSERT_TRUE(shard.RunOnce());
    }
    ASSERT_EQ(observations.readiness, kConnections);
    EXPECT_EQ(progress.wait_fds().size(), 1u);
    EXPECT_EQ(progress.wait_fds().front(), stable);
    for (std::size_t i = 0; i < kConnections; ++i) {
        EXPECT_NE(observations.ready[i] & EPOLLIN, 0u);
    }
    ASSERT_TRUE(shard.Close());
    for (std::size_t i = 0; i < kConnections; ++i) {
        ASSERT_TRUE(connections[i].Close());
        ASSERT_TRUE(progress.Detach(handles[i]));
    }
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, HandshakePreparationReadyAndTcpTransferAdvanceThroughReactor) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    std::array<connection::ConnectionTable, 2> routes;
    std::array<Connection, 2> connections;
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.capacity = 2}));
    auto now = Now();
    ASSERT_TRUE(routes[0].Open({1, 101}, 2));
    ASSERT_TRUE(routes[1].Open({2, 102}, 2));
    ASSERT_TRUE(connections[0].Open(pair.left, routes[0], Config(1, now + 5'000'000'000), now));
    pair.left = -1;
    ASSERT_TRUE(connections[1].Open(pair.right, routes[1], Config(2, now + 5'000'000'000), now));
    pair.right = -1;
    auto left = progress.Attach(connections[0]);
    auto right = progress.Attach(connections[1]);
    ASSERT_TRUE(left);
    ASSERT_TRUE(right);
    RuntimeShard shard;
    ASSERT_TRUE(
        shard.Open({}, {.reactor = {.events = true, .budget = 2, .busy_poll_ns = 0, .max_wait_ms = 1}}, &progress));
    for (int i = 0; i < 100 && (connections[0].state() != ConnectionState::kPreparing ||
                                connections[1].state() != ConnectionState::kPreparing);
         ++i) {
        ASSERT_TRUE(shard.RunOnce());
    }
    ASSERT_EQ(connections[0].state(), ConnectionState::kPreparing);
    ASSERT_EQ(connections[1].state(), ConnectionState::kPreparing);
    ASSERT_TRUE(connections[0].PrepareTcp());
    ASSERT_TRUE(connections[1].PrepareTcp());
    ASSERT_TRUE(progress.Wake(*left));
    ASSERT_TRUE(progress.Wake(*right));
    for (int i = 0; i < 100 && (connections[0].state() != ConnectionState::kTcpActive ||
                                connections[1].state() != ConnectionState::kTcpActive);
         ++i) {
        ASSERT_TRUE(shard.RunOnce());
    }
    ASSERT_EQ(connections[0].state(), ConnectionState::kTcpActive);
    ASSERT_EQ(connections[1].state(), ConnectionState::kTcpActive);
    std::array<std::byte, 3> input{std::byte{1}, std::byte{2}, std::byte{3}}, output{};
    ASSERT_EQ(connections[0].Write(input), 3u);
    auto read = connections[1].Read(output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 3u);
    EXPECT_EQ(output, input);
    ASSERT_TRUE(shard.Close());
    ASSERT_TRUE(connections[0].Close());
    ASSERT_TRUE(connections[1].Close());
    ASSERT_TRUE(progress.Detach(*left));
    ASSERT_TRUE(progress.Detach(*right));
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, HandshakeTimeoutDoesNotStopHealthyConnection) {
    TcpPair failing, healthy;
    ASSERT_TRUE(failing.Open());
    ASSERT_TRUE(healthy.Open());
    connection::ConnectionTable routes;
    ASSERT_TRUE(routes.Open({1, 101}, 2));
    Connection first, second;
    const auto now = Now();
    ASSERT_TRUE(first.Open(failing.right, routes, Config(1, now + 10), now));
    failing.right = -1;
    ASSERT_TRUE(second.OpenTcp(healthy.right));
    healthy.right = -1;
    Observations observations;
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.capacity = 2}, Observations::Observe, &observations));
    auto a = progress.Attach(first), b = progress.Attach(second);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_TRUE(progress.Progress(ProgressKind::kMailbox, 2, now + 20));
    EXPECT_EQ(first.state(), ConnectionState::kFailed);
    EXPECT_EQ(second.state(), ConnectionState::kTcpActive);
    EXPECT_EQ(observations.failed, 1u);
    EXPECT_EQ(progress.stats()->visits, 2u);
    ASSERT_TRUE(first.Close());
    ASSERT_TRUE(second.Close());
    ASSERT_TRUE(progress.Detach(*a));
    ASSERT_TRUE(progress.Detach(*b));
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, HotProgressAndNotificationCollectionAllocateNothing) {
    Connection connection;
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.capacity = 2}));
    const auto handle = progress.Attach(connection);
    ASSERT_TRUE(handle);
    allocations = 0;
    count_allocations = true;
    const auto wake = progress.Wake(*handle);
    const auto notify = progress.CollectNotifications(2);
    const auto timer = progress.Progress(ProgressKind::kTimer, 2, 10);
    const auto advanced = progress.Progress(ProgressKind::kMailbox, 2, 10);
    const auto rearm = progress.Rearm();
    count_allocations = false;
    EXPECT_TRUE(wake);
    EXPECT_TRUE(notify);
    EXPECT_TRUE(timer);
    EXPECT_TRUE(advanced);
    EXPECT_TRUE(rearm);
    EXPECT_EQ(allocations, 0u);
    ASSERT_TRUE(connection.Close());
    ASSERT_TRUE(progress.Detach(*handle));
    EXPECT_TRUE(progress.Close());
}

TEST(ConnectionProgress, ClosedProtocolFdWithDuplicateCannotKeepWakingReusedSlot) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    Connection connection;
    connection::ConnectionTable routes;
    ASSERT_TRUE(routes.Open({1, 101}, 2));
    const auto now = Now();
    ASSERT_TRUE(connection.Open(pair.right, routes, Config(1, now + 5'000'000'000), now));
    pair.right = -1;
    const int alias = ::dup(connection.fd());
    ASSERT_GE(alias, 0);
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.capacity = 1}));
    const auto old = progress.Attach(connection);
    ASSERT_TRUE(old);
    ASSERT_TRUE(connection.Close());
    ASSERT_TRUE(progress.Detach(*old));
    Connection replacement;
    const auto newer = progress.Attach(replacement);
    ASSERT_TRUE(newer);
    // fd 别名让旧 open-file description 仍存活。旧事件只触发一次，并按完整 id 丢弃。
    const char byte = 'x';
    ASSERT_EQ(::send(pair.left, &byte, 1, MSG_NOSIGNAL), 1);
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(progress.CollectNotifications(4));
        ASSERT_TRUE(progress.Progress(ProgressKind::kMailbox, 4, Now()));
    }
    EXPECT_EQ(progress.stats()->stale_events, 1u);
    EXPECT_EQ(replacement.state(), ConnectionState::kIdle);
    ASSERT_EQ(::close(alias), 0);
    ASSERT_TRUE(replacement.Close());
    ASSERT_TRUE(progress.Detach(*newer));
    EXPECT_TRUE(progress.Close());
}
} // namespace
} // namespace runtime
} // namespace kbsocket
