// SPDX-License-Identifier: MulanPSL-2.0
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection.hpp"

namespace kbsocket {
namespace connection {
namespace {
std::uint64_t Now() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
constexpr std::uint64_t kTimeoutNs = 5'000'000'000;
bool Wait(int fd, short events, std::uint64_t deadline) {
    while (Now() < deadline) {
        pollfd item{fd, events, 0};
        const int result = poll(&item, 1, 10);
        if (result > 0) {
            return (item.revents & (events | POLLHUP | POLLERR)) != 0;
        }
        if (result < 0 && errno != EINTR) {
            return false;
        }
    }
    return false;
}
struct SocketPair {
    int left = -1, right = -1;
    ~SocketPair() {
        if (left >= 0) {
            close(left);
        }
        if (right >= 0) {
            close(right);
        }
    }
    bool Open() {
        tcp::NativeTcpListener listener;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (!listener.Open(reinterpret_cast<const sockaddr*>(&address), sizeof(address))) {
            return false;
        }
        socklen_t size = sizeof(address);
        if (getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &size) < 0) {
            return false;
        }
        left = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (left < 0) {
            return false;
        }
        if (connect(left, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 && errno != EINPROGRESS) {
            return false;
        }
        const auto deadline = Now() + kTimeoutNs;
        if (!Wait(listener.fd(), POLLIN, deadline)) {
            return false;
        }
        auto accepted = listener.Accept();
        if (!accepted) {
            return false;
        }
        right = *accepted;
        if (!Wait(left, POLLOUT, deadline)) {
            return false;
        }
        int error = 0;
        size = sizeof(error);
        return getsockopt(left, SOL_SOCKET, SO_ERROR, &error, &size) == 0 && error == 0;
    }
};
template <class T>
void ExpectError(const std::expected<T, ConnectionError>& result, ConnectionErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
control::ConnectionHandshakeConfig Config(std::uint64_t runtime, std::uint64_t deadline) {
    control::ConnectionHandshakeConfig config;
    config.local.capabilities = control::kHandshakeCapabilityTcp;
    config.local.identity = {.runtime = runtime,
                             .receive_domain = runtime + 100,
                             .endpoint_generation = 1,
                             .authorization = 7,
                             .session = runtime + 200,
                             .token = {.slot = 9, .generation = 1}};
    config.expected_peer_authorization = 7;
    config.compatible_peer = true;
    config.deadline_ns = deadline;
    return config;
}

TEST(ConnectionTcp, DirectOpenDoesNotSendHelloAndUsesTransparentShortIo) {
    SocketPair sockets;
    ASSERT_TRUE(sockets.Open());
    Connection left, right;
    ASSERT_TRUE(left.OpenTcp(sockets.left));
    sockets.left = -1;
    ASSERT_TRUE(right.OpenTcp(sockets.right));
    sockets.right = -1;
    EXPECT_EQ(left.state(), ConnectionState::kTcpActive);
    // TCP 的就绪来源是内核 fd；纯快照只暴露已知 EOF/半关闭，不探测或消费数据。
    EXPECT_EQ(left.readiness(), ConnectionReadiness{});
    std::array<std::byte, 5> source{std::byte{'K'}, std::byte{'B'}, std::byte{0}, std::byte{0xff}, std::byte{7}};
    const auto expected = source;
    std::array<std::byte, 5> output{};
    ExpectError(right.Read(output, Now()), ConnectionErrorCode::kWouldBlock);
    EXPECT_FALSE(right.failure());
    ASSERT_EQ(left.Write(source), source.size());
    source.fill(std::byte{0x77});
    const auto deadline = Now() + kTimeoutNs;
    std::size_t received = 0;
    while (received < output.size() && Now() < deadline) {
        ASSERT_TRUE(Wait(right.fd(), POLLIN, deadline));
        const auto read =
            right.Read(std::span(output).subspan(received, std::min<std::size_t>(2, 5 - received)), Now());
        ASSERT_TRUE(read);
        ASSERT_GT(read->bytes, 0u);
        EXPECT_LE(read->bytes, 2u);
        EXPECT_FALSE(read->eof);
        received += read->bytes;
    }
    EXPECT_EQ(received, output.size());
    EXPECT_EQ(output, expected);
    ASSERT_TRUE(left.ShutdownWrite());
    EXPECT_EQ(left.readiness(), (ConnectionReadiness{.write_closed = true}));
    ASSERT_TRUE(Wait(right.fd(), POLLIN, deadline));
    const auto eof = right.Read(output, Now());
    ASSERT_TRUE(eof);
    EXPECT_EQ(eof->bytes, 0u);
    EXPECT_TRUE(eof->eof);
    EXPECT_EQ(right.readiness(), (ConnectionReadiness{.read_eof = true}));
    EXPECT_FALSE(right.failure());
    // FIN 只关闭一侧写方向；对端仍可回写，且本端不会因写已关闭进入失败状态。
    ExpectError(left.Write(expected), ConnectionErrorCode::kTcp);
    EXPECT_EQ(left.state(), ConnectionState::kTcpActive);
    ASSERT_EQ(right.Write(expected), expected.size());
    ASSERT_TRUE(Wait(left.fd(), POLLIN, deadline));
    auto reply = left.Read(output, Now());
    ASSERT_TRUE(reply);
    EXPECT_EQ(reply->bytes, expected.size());
    EXPECT_EQ(output, expected);
    ASSERT_TRUE(left.Close());
    ASSERT_TRUE(right.Close());
    EXPECT_EQ(left.state(), ConnectionState::kClosed);
}

TEST(ConnectionTcp, DirectConnectPublishesConnectingBeforeTransparentIo) {
    tcp::NativeTcpListener listener;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_TRUE(listener.Open(reinterpret_cast<const sockaddr*>(&address), sizeof(address)));
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &length), 0);
    Connection client, server;
    const auto deadline = Now() + kTimeoutNs;
    // 参数错误尚未分配 fd，不消耗成功打开的一次生命周期。
    ExpectError(client.ConnectTcp(nullptr, 0, Now(), deadline), ConnectionErrorCode::kTcp);
    EXPECT_EQ(client.state(), ConnectionState::kIdle);
    ASSERT_TRUE(client.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), Now(), deadline));
    ASSERT_GE(client.fd(), 0);
    std::array<std::byte, 3> bytes{std::byte{1}, std::byte{2}, std::byte{3}}, output{};
    if (client.state() == ConnectionState::kTcpConnecting) {
        ExpectError(client.Write(bytes), ConnectionErrorCode::kNotReady);
        ExpectError(client.Read(output, Now()), ConnectionErrorCode::kNotReady);
        ExpectError(client.ShutdownWrite(), ConnectionErrorCode::kNotReady);
        EXPECT_EQ(client.readiness(), ConnectionReadiness{});
    }
    ASSERT_TRUE(Wait(listener.fd(), POLLIN, deadline));
    const auto accepted = listener.Accept();
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(server.OpenTcp(*accepted));
    ASSERT_TRUE(Wait(client.fd(), POLLOUT, deadline));
    const bool connecting = client.state() == ConnectionState::kTcpConnecting;
    ASSERT_EQ(client.Progress(Now()), connecting ? 1u : 0u);
    EXPECT_EQ(client.state(), ConnectionState::kTcpActive);
    EXPECT_EQ(client.Progress(Now()), 0u);
    // 直连路径不发送协议 HELLO，首批应用字节必须原样交付。
    ExpectError(server.Read(output, Now()), ConnectionErrorCode::kWouldBlock);
    ASSERT_EQ(client.Write(bytes), bytes.size());
    ASSERT_TRUE(Wait(server.fd(), POLLIN, deadline));
    const auto read = server.Read(output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, bytes.size());
    EXPECT_EQ(output, bytes);
    EXPECT_TRUE(client.Close());
    EXPECT_TRUE(server.Close());
}

TEST(ConnectionTcp, ConnectingDeadlineAndCancelCloseOnlyTheirOwnedSocket) {
    tcp::NativeTcpListener listener;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_TRUE(listener.Open(reinterpret_cast<const sockaddr*>(&address), sizeof(address)));
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &length), 0);
    Connection expired;
    ASSERT_TRUE(expired.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), 10, 20));
    if (expired.state() == ConnectionState::kTcpConnecting) {
        const int fd = expired.fd();
        // 单调时钟回退是参数错误，不应终结连接；到达绝对 deadline 才锁存 Timeout。
        ExpectError(expired.Progress(9), ConnectionErrorCode::kTcp);
        EXPECT_EQ(expired.state(), ConnectionState::kTcpConnecting);
        const auto timed_out = expired.Progress(20);
        ASSERT_FALSE(timed_out);
        EXPECT_EQ(timed_out.error().detail, static_cast<int>(tcp::TcpErrorCode::kTimeout));
        EXPECT_EQ(timed_out.error().underlying_error, ETIMEDOUT);
        EXPECT_EQ(expired.state(), ConnectionState::kFailed);
        EXPECT_EQ(fcntl(fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    }
    ASSERT_TRUE(expired.Close());
    Connection cancelled;
    const auto deadline = Now() + kTimeoutNs;
    ASSERT_TRUE(cancelled.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), Now(), deadline));
    const int fd = cancelled.fd();
    cancelled.Cancel();
    EXPECT_EQ(cancelled.state(), ConnectionState::kFailed);
    ASSERT_TRUE(cancelled.failure());
    EXPECT_EQ(cancelled.failure()->code, ConnectionErrorCode::kCancelled);
    EXPECT_EQ(fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    ASSERT_TRUE(cancelled.Close());
    EXPECT_GE(fcntl(listener.fd(), F_GETFD), 0);
}

TEST(ConnectionTcp, RefusedDirectConnectKeepsNativeErrorAndCannotBeReused) {
    SocketPair reservation;
    reservation.left = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    ASSERT_GE(reservation.left, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    // 保留端口但不 listen，避免“取空闲端口再关闭”与其他测试进程竞争。
    ASSERT_EQ(bind(reservation.left, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(reservation.left, reinterpret_cast<sockaddr*>(&address), &length), 0);
    Connection client;
    const auto deadline = Now() + kTimeoutNs;
    const auto started =
        client.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), Now(), deadline);
    if (started) {
        while (client.state() == ConnectionState::kTcpConnecting && Now() < deadline) {
            ASSERT_TRUE(Wait(client.fd(), POLLOUT, deadline));
            (void)client.Progress(Now());
        }
    }
    EXPECT_EQ(client.state(), ConnectionState::kFailed);
    ASSERT_TRUE(client.failure());
    EXPECT_EQ(client.failure()->code, ConnectionErrorCode::kTcp);
    EXPECT_EQ(client.failure()->underlying_error, ECONNREFUSED);
    EXPECT_EQ(client.fd(), -1);
    ExpectError(client.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), Now(), deadline),
                ConnectionErrorCode::kInUse);
    EXPECT_TRUE(client.Close());
}

TEST(ConnectionTcp, NegotiatedTcpRequiresBothReadyAndPreservesImmediatelyFollowingPayload) {
    SocketPair sockets;
    ASSERT_TRUE(sockets.Open());
    ConnectionTable left_table, right_table;
    ASSERT_TRUE(left_table.Open({1, 101}, 4));
    ASSERT_TRUE(right_table.Open({2, 102}, 4));
    Connection left, right;
    const auto deadline = Now() + kTimeoutNs;
    ASSERT_TRUE(left.Open(sockets.left, left_table, Config(1, deadline), Now()));
    sockets.left = -1;
    ASSERT_TRUE(right.Open(sockets.right, right_table, Config(2, deadline), Now()));
    sockets.right = -1;
    const std::array<std::byte, 7> payload{std::byte{'K'}, std::byte{'B'},  std::byte{'S'}, std::byte{'H'},
                                           std::byte{0},   std::byte{0xff}, std::byte{3}};
    std::array<std::byte, 16> output{};
    ExpectError(left.Write(payload), ConnectionErrorCode::kNotReady);
    ExpectError(left.Read(output, Now()), ConnectionErrorCode::kNotReady);
    ExpectError(left.PrepareTcp(), ConnectionErrorCode::kNotReady);
    while ((left.state() != ConnectionState::kPreparing || right.state() != ConnectionState::kPreparing) &&
           Now() < deadline) {
        ASSERT_TRUE(left.Progress(Now()));
        ASSERT_TRUE(right.Progress(Now()));
        poll(nullptr, 0, 1);
    }
    ASSERT_EQ(left.state(), ConnectionState::kPreparing);
    ASSERT_EQ(right.state(), ConnectionState::kPreparing);
    ASSERT_TRUE(left.PrepareTcp());
    while (right.handshake().peer_ready() == nullptr && Now() < deadline) {
        ASSERT_TRUE(left.Progress(Now()));
        ASSERT_TRUE(right.Progress(Now()));
        poll(nullptr, 0, 1);
    }
    ASSERT_NE(right.handshake().peer_ready(), nullptr);
    EXPECT_EQ(left.state(), ConnectionState::kWaitingReady);
    EXPECT_EQ(right.state(), ConnectionState::kPreparing);
    ExpectError(left.Write(payload), ConnectionErrorCode::kNotReady);
    ExpectError(right.Write(payload), ConnectionErrorCode::kNotReady);
    // 右端先进入 Active，把 READY 与首包连续放入 TCP；左端握手不得吞掉业务前缀。
    ASSERT_TRUE(right.PrepareTcp());
    while (right.state() != ConnectionState::kTcpActive && Now() < deadline) {
        ASSERT_TRUE(right.Progress(Now()));
    }
    ASSERT_EQ(right.state(), ConnectionState::kTcpActive);
    ASSERT_EQ(right.Write(payload), payload.size());
    while (left.state() != ConnectionState::kTcpActive && Now() < deadline) {
        ASSERT_TRUE(left.Progress(Now()));
    }
    ASSERT_EQ(left.state(), ConnectionState::kTcpActive);
    EXPECT_EQ(left.handshake().fd(), -1);
    EXPECT_EQ(right.handshake().fd(), -1);
    EXPECT_GE(left.fd(), 0);
    EXPECT_GE(right.fd(), 0);
    EXPECT_EQ(left_table.published(), 0u);
    EXPECT_EQ(right_table.published(), 0u);
    ASSERT_TRUE(Wait(left.fd(), POLLIN, deadline));
    const auto read = left.Read(output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, payload.size());
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), output.begin()));
    const int owned_fd = left.fd();
    ASSERT_TRUE(left.Close());
    ASSERT_TRUE(right.Close());
    EXPECT_EQ(fcntl(owned_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_EQ(left_table.active(), 0u);
    EXPECT_EQ(right_table.active(), 0u);
    EXPECT_TRUE(left_table.Close());
    EXPECT_TRUE(right_table.Close());
}

TEST(ConnectionTcp, FatalResetChangesConnectionStateAndProgressReportsExactErrno) {
    SocketPair sockets;
    ASSERT_TRUE(sockets.Open());
    Connection client;
    ASSERT_TRUE(client.OpenTcp(sockets.left));
    sockets.left = -1;
    const linger reset{1, 0};
    ASSERT_EQ(setsockopt(sockets.right, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)), 0);
    ASSERT_EQ(close(sockets.right), 0);
    sockets.right = -1;
    ASSERT_TRUE(Wait(client.fd(), POLLIN, Now() + kTimeoutNs));
    std::array<std::byte, 1> output{};
    const auto read = client.Read(output, Now());
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error().code, ConnectionErrorCode::kTcp);
    EXPECT_EQ(read.error().underlying_error, ECONNRESET);
    EXPECT_EQ(client.state(), ConnectionState::kFailed);
    ASSERT_TRUE(client.failure());
    EXPECT_EQ(client.failure()->underlying_error, ECONNRESET);
    EXPECT_EQ(client.fd(), -1);
    const auto progressed = client.Progress(Now());
    ASSERT_FALSE(progressed);
    EXPECT_EQ(progressed.error().underlying_error, ECONNRESET);
    EXPECT_TRUE(client.Close());
}

TEST(ConnectionTcp, FailedOpenKeepsCallerFdAndCancelClosesSuccessfulOwnership) {
    SocketPair sockets;
    ASSERT_TRUE(sockets.Open());
    Connection client;
    const int flags = fcntl(sockets.left, F_GETFL);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(fcntl(sockets.left, F_SETFL, flags & ~O_NONBLOCK), 0);
    ExpectError(client.OpenTcp(sockets.left), ConnectionErrorCode::kTcp);
    EXPECT_EQ(fcntl(sockets.left, F_GETFL), flags & ~O_NONBLOCK);
    EXPECT_EQ(client.state(), ConnectionState::kIdle);
    ASSERT_EQ(fcntl(sockets.left, F_SETFL, flags), 0);
    ASSERT_TRUE(client.OpenTcp(sockets.left));
    const int owned = sockets.left;
    sockets.left = -1;
    client.Cancel();
    EXPECT_EQ(client.state(), ConnectionState::kFailed);
    EXPECT_EQ(fcntl(owned, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    std::array<std::byte, 1> data{};
    ExpectError(client.Read(data, Now()), ConnectionErrorCode::kCancelled);
    ExpectError(client.Write(data), ConnectionErrorCode::kCancelled);
    ExpectError(client.Progress(Now()), ConnectionErrorCode::kCancelled);
    ExpectError(client.OpenTcp(sockets.right), ConnectionErrorCode::kInUse);
    EXPECT_GE(fcntl(sockets.right, F_GETFD), 0);
    EXPECT_TRUE(client.Close());
}

TEST(ConnectionTcp, UnauthorizedNegotiationCannotReuseDirtySocketAsPlainTcp) {
    SocketPair sockets;
    ASSERT_TRUE(sockets.Open());
    ConnectionTable left_table, right_table;
    ASSERT_TRUE(left_table.Open({1, 101}, 4));
    ASSERT_TRUE(right_table.Open({2, 102}, 4));
    Connection left, right;
    const auto deadline = Now() + kTimeoutNs;
    auto bad_policy = Config(1, deadline);
    bad_policy.expected_peer_authorization = 8;
    ASSERT_TRUE(left.Open(sockets.left, left_table, bad_policy, Now()));
    sockets.left = -1;
    ASSERT_TRUE(right.Open(sockets.right, right_table, Config(2, deadline), Now()));
    sockets.right = -1;
    while (left.state() != ConnectionState::kFailed && Now() < deadline) {
        (void)right.Progress(Now());
        (void)left.Progress(Now());
        poll(nullptr, 0, 1);
    }
    ASSERT_EQ(left.state(), ConnectionState::kFailed);
    ASSERT_TRUE(left.failure());
    EXPECT_EQ(left.failure()->code, ConnectionErrorCode::kHandshake);
    EXPECT_EQ(left.fd(), -1);
    ExpectError(left.PrepareTcp(), ConnectionErrorCode::kNotReady);
    ExpectError(left.OpenTcp(right.fd()), ConnectionErrorCode::kInUse);
    EXPECT_TRUE(left.Close());
    EXPECT_TRUE(right.Close());
    EXPECT_TRUE(left_table.Close());
    EXPECT_TRUE(right_table.Close());
}

} // namespace
} // namespace connection
} // namespace kbsocket
