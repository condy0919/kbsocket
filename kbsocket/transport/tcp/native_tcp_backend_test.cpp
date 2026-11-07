// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/tcp/native_tcp_backend.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <vector>

#include <gtest/gtest.h>

namespace kbsocket {
namespace tcp {
namespace {
std::uint64_t Now() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
constexpr std::uint64_t kTimeoutNs = 5'000'000'000;
sockaddr_in Loopback() {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}
const sockaddr* Address(const sockaddr_in& address) {
    return reinterpret_cast<const sockaddr*>(&address);
}
bool LocalAddress(int fd, sockaddr_in& address) {
    socklen_t size = sizeof(address);
    return getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) == 0 && size == sizeof(address);
}
bool Wait(int fd, short events, std::uint64_t deadline) {
    while (Now() < deadline) {
        pollfd item{fd, events, 0};
        const int result = poll(&item, 1, 20);
        if (result > 0) {
            return (item.revents & (events | POLLERR | POLLHUP)) != 0;
        }
        if (result < 0 && errno != EINTR) {
            return false;
        }
    }
    return false;
}
template <class T>
void ExpectError(const std::expected<T, TcpError>& result, TcpErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
struct OwnedFd {
    int fd = -1;
    ~OwnedFd() {
        if (fd >= 0) {
            close(fd);
        }
    }
};
struct TcpPair {
    NativeTcpBackend client;
    NativeTcpBackend server;
    bool Open() {
        NativeTcpListener listener;
        auto address = Loopback();
        const auto listened = listener.Open(Address(address), sizeof(address));
        if (!listened || !LocalAddress(listener.fd(), address)) {
            return false;
        }
        const auto deadline = Now() + kTimeoutNs;
        if (!client.Connect(Address(address), sizeof(address), Now(), deadline)) {
            return false;
        }
        if (!Wait(listener.fd(), POLLIN, deadline)) {
            return false;
        }
        const auto accepted = listener.Accept();
        if (!accepted) {
            return false;
        }
        OwnedFd peer{*accepted};
        if (!server.Open(peer.fd)) {
            return false;
        }
        peer.fd = -1;
        if (!Wait(client.fd(), POLLOUT, deadline)) {
            return false;
        }
        return client.FinishConnect(Now()).has_value();
    }
};

TEST(NativeTcpBackend, AdoptsOnlyConnectedNonblockingTcpAndPreservesFailureOwnership) {
    NativeTcpBackend backend;
    OwnedFd socket_fd{socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
    ASSERT_GE(socket_fd.fd, 0);
    const int original_flags = fcntl(socket_fd.fd, F_GETFL);
    ExpectError(backend.Open(socket_fd.fd), TcpErrorCode::kNotNonblocking);
    EXPECT_EQ(fcntl(socket_fd.fd, F_GETFL), original_flags);
    ASSERT_EQ(fcntl(socket_fd.fd, F_SETFL, original_flags | O_NONBLOCK), 0);
    ExpectError(backend.Open(socket_fd.fd), TcpErrorCode::kNotConnected);
    EXPECT_GE(fcntl(socket_fd.fd, F_GETFD), 0);
    EXPECT_EQ(backend.fd(), -1);
    ExpectError(backend.Open(-1), TcpErrorCode::kInvalidArgument);

    // AF_UNIX 不冒充 TCP；失败仍由调用者持有两个描述符。
    int descriptors[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, descriptors), 0);
    OwnedFd first{descriptors[0]}, second{descriptors[1]};
    ExpectError(backend.Open(first.fd), TcpErrorCode::kInvalidArgument);
    EXPECT_GE(fcntl(first.fd, F_GETFD), 0);
}

TEST(NativeTcpBackend, ListenerAndConnectProduceOwnedNonblockingCloseOnExecSockets) {
    NativeTcpListener listener;
    auto address = Loopback();
    ASSERT_TRUE(listener.Open(Address(address), sizeof(address)));
    ExpectError(listener.Accept(), TcpErrorCode::kWouldBlock);
    ASSERT_TRUE(LocalAddress(listener.fd(), address));
    NativeTcpBackend client;
    const auto deadline = Now() + kTimeoutNs;
    ASSERT_TRUE(client.Connect(Address(address), sizeof(address), Now(), deadline));
    ASSERT_TRUE(Wait(listener.fd(), POLLIN, deadline));
    const auto accepted = listener.Accept();
    ASSERT_TRUE(accepted);
    OwnedFd peer{*accepted};
    EXPECT_NE(fcntl(peer.fd, F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(fcntl(peer.fd, F_GETFD) & FD_CLOEXEC, 0);
    ASSERT_TRUE(Wait(client.fd(), POLLOUT, deadline));
    ASSERT_TRUE(client.FinishConnect(Now()));
    EXPECT_EQ(client.state(), TcpState::kConnected);
    EXPECT_TRUE(client.Interests().readable);
    EXPECT_FALSE(client.Interests().writable);
    EXPECT_TRUE(client.Interests(true).writable);
    ExpectError(client.Open(peer.fd), TcpErrorCode::kInUse);
    EXPECT_GE(fcntl(peer.fd, F_GETFD), 0);
    const int owned_fd = client.fd();
    ASSERT_TRUE(client.Close());
    EXPECT_EQ(fcntl(owned_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    ASSERT_TRUE(client.Close());
}

TEST(NativeTcpBackend, TransparentPartialIoPreservesByteOrderAndCopiesWriteSource) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    std::array<std::byte, 10> source{std::byte{0},   std::byte{0xff}, std::byte{'K'}, std::byte{'B'}, std::byte{'S'},
                                     std::byte{'H'}, std::byte{1},    std::byte{0},   std::byte{2},   std::byte{3}};
    const auto expected = source;
    std::array<std::byte, 10> output{};
    ExpectError(pair.server.Read(output), TcpErrorCode::kWouldBlock);
    // 刻意写入类似私有握手头的业务数据；后端必须原样传送并遵守每次字节预算。
    const auto first = pair.client.Write(source, {.bytes = 3, .syscalls = 1});
    ASSERT_TRUE(first);
    ASSERT_EQ(*first, 3u);
    std::fill_n(source.begin(), 3, std::byte{0x55});
    const auto second = pair.client.Write(std::span(source).subspan(3));
    ASSERT_TRUE(second);
    ASSERT_EQ(*second, 7u);
    source.fill(std::byte{0x66});
    std::size_t received = 0;
    const auto deadline = Now() + kTimeoutNs;
    while (received < output.size() && Now() < deadline) {
        ASSERT_TRUE(Wait(pair.server.fd(), POLLIN, deadline));
        const auto read = pair.server.Read(std::span(output).subspan(received), {.bytes = 2, .syscalls = 1});
        ASSERT_TRUE(read);
        ASSERT_GT(*read, 0u);
        EXPECT_LE(*read, 2u);
        received += *read;
    }
    ASSERT_EQ(received, output.size());
    EXPECT_EQ(output, expected);
    EXPECT_FALSE(pair.server.read_eof());
    EXPECT_EQ(pair.server.Read({}), 0u);
}

TEST(NativeTcpBackend, KernelBackpressureReturnsPartialCountsAndResumesWithoutSourceRetention) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int small_buffer = 4096;
    ASSERT_EQ(setsockopt(pair.client.fd(), SOL_SOCKET, SO_SNDBUF, &small_buffer, sizeof(small_buffer)), 0);
    std::vector<std::byte> expected(2 * 1024 * 1024);
    for (std::size_t i = 0; i < expected.size(); ++i) {
        expected[i] = static_cast<std::byte>(i % 251);
    }
    auto source = expected;
    std::vector<std::byte> output(expected.size());
    std::size_t sent = 0, received = 0;
    bool blocked = false, partial = false;
    auto deadline = Now() + kTimeoutNs;
    // 暂停对端读取直到真实内核发送缓冲耗尽，不能靠 mock 制造 EAGAIN。
    while (sent < source.size() && Now() < deadline) {
        const auto written = pair.client.Write(std::span(source).subspan(sent), {.bytes = 64 * 1024, .syscalls = 1});
        if (!written) {
            ASSERT_EQ(written.error().code, TcpErrorCode::kWouldBlock);
            blocked = true;
            break;
        }
        ASSERT_GT(*written, 0u);
        partial = partial || *written < std::min<std::size_t>(64 * 1024, source.size() - sent);
        std::fill_n(source.begin() + sent, *written, std::byte{0});
        sent += *written;
    }
    ASSERT_TRUE(blocked);
    EXPECT_TRUE(partial);
    ASSERT_GT(sent, 0u);
    ASSERT_LT(sent, source.size());
    deadline = Now() + kTimeoutNs;
    while (received < output.size() && Now() < deadline) {
        if (sent < source.size()) {
            const auto written = pair.client.Write(std::span(source).subspan(sent));
            if (written) {
                std::fill_n(source.begin() + sent, *written, std::byte{0});
                sent += *written;
            } else {
                ASSERT_EQ(written.error().code, TcpErrorCode::kWouldBlock);
            }
        }
        const auto read = pair.server.Read(std::span(output).subspan(received));
        if (read) {
            ASSERT_GT(*read, 0u);
            received += *read;
        } else {
            ASSERT_EQ(read.error().code, TcpErrorCode::kWouldBlock);
            pollfd items[2]{{pair.client.fd(), POLLOUT, 0}, {pair.server.fd(), POLLIN, 0}};
            ASSERT_GE(poll(items, 2, 1), 0);
        }
    }
    EXPECT_EQ(sent, source.size());
    ASSERT_EQ(received, output.size());
    EXPECT_EQ(output, expected);
}

TEST(NativeTcpBackend, HalfCloseDeliversAcceptedBytesBeforeEofAndKeepsReverseDirection) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const std::array<std::byte, 3> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    ASSERT_EQ(pair.client.Write(payload), payload.size());
    ASSERT_TRUE(pair.client.ShutdownWrite());
    ASSERT_TRUE(pair.client.ShutdownWrite());
    EXPECT_TRUE(pair.client.write_shutdown());
    EXPECT_TRUE(pair.client.Interests(true).readable);
    EXPECT_FALSE(pair.client.Interests(true).writable);
    ExpectError(pair.client.Write(payload), TcpErrorCode::kWriteClosed);
    std::array<std::byte, 4> output{};
    std::size_t received = 0;
    const auto deadline = Now() + kTimeoutNs;
    while (!pair.server.read_eof() && Now() < deadline) {
        ASSERT_TRUE(Wait(pair.server.fd(), POLLIN, deadline));
        const auto read = pair.server.Read(std::span(output).subspan(received));
        ASSERT_TRUE(read);
        received += *read;
    }
    ASSERT_TRUE(pair.server.read_eof());
    EXPECT_EQ(received, payload.size());
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), output.begin()));
    EXPECT_FALSE(pair.server.Interests(true).readable);
    EXPECT_TRUE(pair.server.Interests(true).writable);
    ASSERT_EQ(pair.server.Write(payload), payload.size());
    ASSERT_TRUE(Wait(pair.client.fd(), POLLIN, deadline));
    ASSERT_EQ(pair.client.Read(output), payload.size());
    EXPECT_FALSE(pair.client.failure());
}

TEST(NativeTcpBackend, ResetIsStickySystemErrorInsteadOfGracefulEof) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    // 零 linger 强制 RST，验证错误不会伪装成正常 FIN，也不会触发 SIGPIPE。
    const linger reset{1, 0};
    ASSERT_EQ(setsockopt(pair.server.fd(), SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)), 0);
    ASSERT_TRUE(pair.server.Close());
    ASSERT_TRUE(Wait(pair.client.fd(), POLLIN, Now() + kTimeoutNs));
    std::array<std::byte, 4> output{};
    const auto read = pair.client.Read(output);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error().code, TcpErrorCode::kSystemError);
    EXPECT_EQ(read.error().system_error, ECONNRESET);
    EXPECT_FALSE(pair.client.read_eof());
    EXPECT_EQ(pair.client.state(), TcpState::kFailed);
    EXPECT_EQ(pair.client.fd(), -1);
    const auto written = pair.client.Write(output);
    ASSERT_FALSE(written);
    EXPECT_EQ(written.error().system_error, ECONNRESET);
    EXPECT_FALSE(pair.client.Interests(true).readable);
    EXPECT_FALSE(pair.client.Interests(true).writable);
}

TEST(NativeTcpBackend, RefusedConnectRetainsExactErrnoAndDoesNotSendProbe) {
    // bind 但不 listen，端口始终由本测试占有，避免关闭监听端口后的复用竞态。
    OwnedFd reservation{socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
    ASSERT_GE(reservation.fd, 0);
    auto address = Loopback();
    ASSERT_EQ(bind(reservation.fd, Address(address), sizeof(address)), 0);
    ASSERT_TRUE(LocalAddress(reservation.fd, address));
    NativeTcpBackend client;
    const auto deadline = Now() + kTimeoutNs;
    const auto started = client.Connect(Address(address), sizeof(address), Now(), deadline);
    if (started) {
        ASSERT_TRUE(Wait(client.fd(), POLLOUT, deadline));
        const auto finished = client.FinishConnect(Now());
        ASSERT_FALSE(finished);
        EXPECT_EQ(finished.error().system_error, ECONNREFUSED);
    } else {
        EXPECT_EQ(started.error().system_error, ECONNREFUSED);
    }
    ASSERT_TRUE(client.failure());
    EXPECT_EQ(client.failure()->code, TcpErrorCode::kSystemError);
    EXPECT_EQ(client.failure()->system_error, ECONNREFUSED);
    EXPECT_EQ(client.fd(), -1);
}

TEST(NativeTcpBackend, ConnectDeadlineAndClockValidationAreExplicit) {
    NativeTcpListener listener;
    auto address = Loopback();
    ASSERT_TRUE(listener.Open(Address(address), sizeof(address)));
    ASSERT_TRUE(LocalAddress(listener.fd(), address));
    NativeTcpBackend client;
    ExpectError(client.Connect(nullptr, 0, 1, 2), TcpErrorCode::kInvalidArgument);
    ExpectError(client.Connect(Address(address), sizeof(address), 2, 2), TcpErrorCode::kInvalidArgument);
    ASSERT_TRUE(client.Connect(Address(address), sizeof(address), 10, 20));
    // 非阻塞 connect 尚未由 owner 收割时，连接期限仍然生效。
    if (client.state() == TcpState::kConnecting) {
        ExpectError(client.FinishConnect(9), TcpErrorCode::kInvalidArgument);
        EXPECT_EQ(client.state(), TcpState::kConnecting);
        ExpectError(client.FinishConnect(20), TcpErrorCode::kTimeout);
        ASSERT_TRUE(client.failure());
        EXPECT_EQ(client.failure()->system_error, ETIMEDOUT);
        EXPECT_EQ(client.fd(), -1);
    }
}

TEST(NativeTcpBackend, CancelClosesOwnerAndPreventsReopen) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int owned = pair.client.fd();
    pair.client.Cancel();
    EXPECT_EQ(fcntl(owned, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    std::array<std::byte, 1> data{};
    ExpectError(pair.client.Read(data), TcpErrorCode::kCancelled);
    ExpectError(pair.client.Write(data), TcpErrorCode::kCancelled);
    ExpectError(pair.client.ShutdownWrite(), TcpErrorCode::kCancelled);
    ExpectError(pair.client.Open(pair.server.fd()), TcpErrorCode::kCancelled);
    EXPECT_GE(fcntl(pair.server.fd(), F_GETFD), 0);
    EXPECT_TRUE(pair.client.Close());
    NativeTcpBackend unopened;
    unopened.Cancel();
    ExpectError(unopened.Open(pair.server.fd()), TcpErrorCode::kCancelled);
}

TEST(NativeTcpBackend, ZeroBudgetFailsWithoutClosingAndDestructorClosesOwnedFd) {
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    std::array<std::byte, 1> data{};
    ExpectError(pair.client.Read(data, {.bytes = 0}), TcpErrorCode::kInvalidArgument);
    ExpectError(pair.client.Write(data, {.syscalls = 0}), TcpErrorCode::kInvalidArgument);
    EXPECT_EQ(pair.client.state(), TcpState::kConnected);
    EXPECT_FALSE(pair.client.failure());
    NativeTcpBackend unopened;
    ExpectError(unopened.Read(data), TcpErrorCode::kNotConnected);
    ExpectError(unopened.FinishConnect(Now()), TcpErrorCode::kNotConnected);
    int owned = -1;
    {
        TcpPair temporary;
        ASSERT_TRUE(temporary.Open());
        owned = temporary.client.fd();
    }
    EXPECT_EQ(fcntl(owned, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

// 子进程只运行透明 TCP echo，不知道任何 KBSocket 私有握手，证明普通 TCP 兼容性。
int EchoChild(NativeTcpListener& listener) {
    const auto deadline = Now() + kTimeoutNs;
    if (!Wait(listener.fd(), POLLIN, deadline)) {
        return 1;
    }
    const auto accepted = listener.Accept();
    if (!accepted) {
        return 2;
    }
    NativeTcpBackend peer;
    if (!peer.Open(*accepted)) {
        close(*accepted);
        return 3;
    }
    (void)listener.Close();
    std::array<std::byte, 4096> buffer{};
    std::size_t pending = 0, offset = 0;
    while (Now() < deadline) {
        if (pending != 0) {
            const auto write = peer.Write(std::span(buffer).subspan(offset, pending));
            if (write) {
                offset += *write;
                pending -= *write;
            } else if (write.error().code != TcpErrorCode::kWouldBlock) {
                return 4;
            }
        } else if (peer.read_eof()) {
            return peer.ShutdownWrite() ? 0 : 5;
        } else {
            const auto read = peer.Read(buffer);
            if (read) {
                pending = *read;
                offset = 0;
            } else if (read.error().code != TcpErrorCode::kWouldBlock) {
                return 6;
            }
        }
        if (pending != 0 || !peer.read_eof()) {
            if (!Wait(peer.fd(), pending ? POLLOUT : POLLIN, deadline)) {
                return 7;
            }
        }
    }
    return 8;
}
struct ChildProcess {
    pid_t pid = -1;
    ~ChildProcess() {
        if (pid > 0) {
            (void)kill(pid, SIGKILL);
            while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {
            }
        }
    }
};

TEST(NativeTcpBackend, SeparateProcessLoopbackEchoAndHalfClose) {
    NativeTcpListener listener;
    auto address = Loopback();
    ASSERT_TRUE(listener.Open(Address(address), sizeof(address)));
    ASSERT_TRUE(LocalAddress(listener.fd(), address));
    const pid_t child_pid = fork();
    ASSERT_GE(child_pid, 0);
    if (child_pid == 0) {
        _exit(EchoChild(listener));
    }
    ChildProcess child{child_pid};
    // 父子进程之间只共享地址，数据全部经过真实 AF_INET TCP 流。
    ASSERT_TRUE(listener.Close());
    NativeTcpBackend client;
    const auto deadline = Now() + kTimeoutNs;
    ASSERT_TRUE(client.Connect(Address(address), sizeof(address), Now(), deadline));
    ASSERT_TRUE(Wait(client.fd(), POLLOUT, deadline));
    ASSERT_TRUE(client.FinishConnect(Now()));
    std::vector<std::byte> expected(256 * 1024);
    for (std::size_t i = 0; i < expected.size(); ++i) {
        expected[i] = static_cast<std::byte>((i * 7) % 251);
    }
    std::vector<std::byte> output(expected.size() + 1);
    std::size_t sent = 0, received = 0;
    while (!client.read_eof() && Now() < deadline) {
        if (sent < expected.size()) {
            const auto write = client.Write(std::span(expected).subspan(sent), {.bytes = 4093, .syscalls = 1});
            if (write) {
                sent += *write;
            } else {
                ASSERT_EQ(write.error().code, TcpErrorCode::kWouldBlock);
            }
        } else if (!client.write_shutdown()) {
            ASSERT_TRUE(client.ShutdownWrite());
        }
        ASSERT_LT(received, output.size());
        const auto read = client.Read(std::span(output).subspan(received));
        if (read) {
            received += *read;
        } else {
            ASSERT_EQ(read.error().code, TcpErrorCode::kWouldBlock);
            pollfd item{client.fd(), static_cast<short>(POLLIN | (sent < expected.size() ? POLLOUT : 0)), 0};
            ASSERT_GE(poll(&item, 1, 20), 0);
        }
    }
    ASSERT_TRUE(client.read_eof());
    EXPECT_EQ(sent, expected.size());
    ASSERT_EQ(received, expected.size());
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin()));
    int status = 0;
    pid_t waited = 0;
    while (Now() < deadline) {
        waited = waitpid(child.pid, &status, WNOHANG);
        if (waited != 0) {
            break;
        }
        poll(nullptr, 0, 1);
    }
    ASSERT_EQ(waited, child.pid);
    child.pid = -1;
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

} // namespace
} // namespace tcp
} // namespace kbsocket
