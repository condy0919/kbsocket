// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/tcp/native_tcp_backend.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <limits>

namespace kbsocket {
namespace tcp {
namespace {
TcpError SystemError(int error) noexcept {
    return {TcpErrorCode::kSystemError, error};
}
bool WouldBlock(int error) noexcept {
    return error == EAGAIN || error == EWOULDBLOCK;
}
bool ValidAddress(const sockaddr* address, socklen_t length) noexcept {
    if (address == nullptr || length < sizeof(sa_family_t)) {
        return false;
    }
    return (address->sa_family == AF_INET && length == sizeof(sockaddr_in)) ||
           (address->sa_family == AF_INET6 && length == sizeof(sockaddr_in6));
}
std::expected<void, TcpError> CloseFd(int& fd) noexcept {
    const int owned_fd = fd;
    fd = -1;
    // Linux close 即使返回 EINTR 也已释放描述符，不能重试关闭可能复用的 fd。
    if (owned_fd >= 0 && close(owned_fd) < 0) {
        return std::unexpected(SystemError(errno));
    }
    return {};
}
} // namespace

NativeTcpBackend::~NativeTcpBackend() {
    (void)Close();
}

std::expected<void, TcpError> NativeTcpBackend::Open(int fd) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (opened_once_) {
        return std::unexpected(TcpError{TcpErrorCode::kInUse});
    }
    if (fd < 0) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    const int flags = fcntl(fd, F_GETFL);
    if (flags < 0) {
        return std::unexpected(SystemError(errno));
    }
    if ((flags & O_NONBLOCK) == 0) {
        return std::unexpected(TcpError{TcpErrorCode::kNotNonblocking});
    }
    int type = 0;
    socklen_t size = sizeof(type);
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &size) < 0) {
        return std::unexpected(SystemError(errno));
    }
    if (type != SOCK_STREAM) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    sockaddr_storage peer{};
    size = sizeof(peer);
    if (getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &size) < 0) {
        const int error = errno;
        return std::unexpected(
            TcpError{error == ENOTCONN ? TcpErrorCode::kNotConnected : TcpErrorCode::kSystemError, error});
    }
    if (peer.ss_family != AF_INET && peer.ss_family != AF_INET6) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    // 前面的验证均只读；从这里开始才转移所有权。
    fd_ = fd;
    opened_once_ = true;
    state_ = TcpState::kConnected;
    return {};
}

std::expected<void, TcpError> NativeTcpBackend::Connect(const sockaddr* address, socklen_t length, std::uint64_t now_ns,
                                                        std::uint64_t deadline_ns) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (opened_once_) {
        return std::unexpected(TcpError{TcpErrorCode::kInUse});
    }
    if (!ValidAddress(address, length) || deadline_ns <= now_ns) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    const int socket_fd = socket(address->sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (socket_fd < 0) {
        return std::unexpected(SystemError(errno));
    }
    opened_once_ = true;
    fd_ = socket_fd;
    last_now_ns_ = now_ns;
    deadline_ns_ = deadline_ns;
    if (connect(fd_, address, length) == 0) {
        state_ = TcpState::kConnected;
        return {};
    }
    const int error = errno;
    if (error == EINPROGRESS || error == EALREADY || error == EINTR) {
        state_ = TcpState::kConnecting;
        return {};
    }
    return std::unexpected(Fail(SystemError(error)));
}

std::expected<void, TcpError> NativeTcpBackend::FinishConnect(std::uint64_t now_ns) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ == TcpState::kConnected) {
        return {};
    }
    if (state_ != TcpState::kConnecting) {
        return std::unexpected(TcpError{TcpErrorCode::kNotConnected});
    }
    if (now_ns < last_now_ns_) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    last_now_ns_ = now_ns;
    if (now_ns >= deadline_ns_) {
        return std::unexpected(Fail({TcpErrorCode::kTimeout, ETIMEDOUT}));
    }
    int error = 0;
    socklen_t size = sizeof(error);
    if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &size) < 0) {
        return std::unexpected(Fail(SystemError(errno)));
    }
    if (error != 0) {
        return std::unexpected(Fail(SystemError(error)));
    }
    // SO_ERROR==0 也可能是尚未完成的连接，必须检查是否已经绑定远端。
    sockaddr_storage peer{};
    size = sizeof(peer);
    if (getpeername(fd_, reinterpret_cast<sockaddr*>(&peer), &size) < 0) {
        const int peer_error = errno;
        if (peer_error == ENOTCONN) {
            return std::unexpected(TcpError{TcpErrorCode::kWouldBlock, EINPROGRESS});
        }
        return std::unexpected(Fail(SystemError(peer_error)));
    }
    state_ = TcpState::kConnected;
    return {};
}

std::expected<void, TcpError> NativeTcpBackend::CheckConnected() const noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ != TcpState::kConnected) {
        return std::unexpected(TcpError{TcpErrorCode::kNotConnected});
    }
    return {};
}

TcpError NativeTcpBackend::Fail(TcpError error) noexcept {
    if (!failure_) {
        failure_ = error;
    }
    (void)CloseFd(fd_);
    state_ = TcpState::kFailed;
    return *failure_;
}

std::expected<std::size_t, TcpError> NativeTcpBackend::Read(std::span<std::byte> output, TcpIoBudget budget) noexcept {
    const auto connected = CheckConnected();
    if (!connected) {
        return std::unexpected(connected.error());
    }
    if (output.empty() || read_eof_) {
        return 0;
    }
    if (budget.bytes == 0 || budget.syscalls == 0) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    const std::size_t target =
        std::min({output.size(), budget.bytes, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())});
    std::size_t copied = 0;
    for (std::uint32_t calls = 0; calls < budget.syscalls && copied < target; ++calls) {
        const ssize_t result = recv(fd_, output.data() + copied, target - copied, MSG_DONTWAIT);
        if (result > 0) {
            copied += static_cast<std::size_t>(result);
        } else if (result == 0) {
            read_eof_ = true;
            return copied;
        } else {
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (WouldBlock(error)) {
                break;
            }
            const auto failure = Fail(SystemError(error));
            if (copied != 0) {
                return copied;
            }
            return std::unexpected(failure);
        }
    }
    if (copied == 0) {
        return std::unexpected(TcpError{TcpErrorCode::kWouldBlock, EAGAIN});
    }
    return copied;
}

std::expected<std::size_t, TcpError> NativeTcpBackend::Write(std::span<const std::byte> input,
                                                             TcpIoBudget budget) noexcept {
    const auto connected = CheckConnected();
    if (!connected) {
        return std::unexpected(connected.error());
    }
    if (write_shutdown_) {
        return std::unexpected(TcpError{TcpErrorCode::kWriteClosed, EPIPE});
    }
    if (input.empty()) {
        return 0;
    }
    if (budget.bytes == 0 || budget.syscalls == 0) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    const std::size_t target =
        std::min({input.size(), budget.bytes, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())});
    std::size_t copied = 0;
    for (std::uint32_t calls = 0; calls < budget.syscalls && copied < target; ++calls) {
        const ssize_t result = send(fd_, input.data() + copied, target - copied, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (result > 0) {
            copied += static_cast<std::size_t>(result);
        } else if (result == 0) {
            break;
        } else {
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (WouldBlock(error)) {
                break;
            }
            const auto failure = Fail(SystemError(error));
            if (copied != 0) {
                return copied;
            }
            return std::unexpected(failure);
        }
    }
    if (copied == 0) {
        return std::unexpected(TcpError{TcpErrorCode::kWouldBlock, EAGAIN});
    }
    return copied;
}

std::expected<void, TcpError> NativeTcpBackend::ShutdownWrite() noexcept {
    const auto connected = CheckConnected();
    if (!connected) {
        return std::unexpected(connected.error());
    }
    if (write_shutdown_) {
        return {};
    }
    if (shutdown(fd_, SHUT_WR) < 0) {
        return std::unexpected(Fail(SystemError(errno)));
    }
    write_shutdown_ = true;
    return {};
}

void NativeTcpBackend::Cancel() noexcept {
    (void)Fail({TcpErrorCode::kCancelled, ECANCELED});
}

std::expected<void, TcpError> NativeTcpBackend::Close() noexcept {
    const auto result = CloseFd(fd_);
    state_ = failure_ ? TcpState::kFailed : TcpState::kClosed;
    return result;
}

TcpInterests NativeTcpBackend::Interests(bool want_write) const noexcept {
    if (state_ == TcpState::kConnecting) {
        return {false, true};
    }
    if (state_ != TcpState::kConnected) {
        return {};
    }
    return {!read_eof_, want_write && !write_shutdown_};
}

NativeTcpListener::~NativeTcpListener() {
    (void)Close();
}

std::expected<void, TcpError> NativeTcpListener::Open(const sockaddr* address, socklen_t length, int backlog) noexcept {
    if (opened_once_) {
        return std::unexpected(TcpError{TcpErrorCode::kInUse});
    }
    if (!ValidAddress(address, length) || backlog <= 0) {
        return std::unexpected(TcpError{TcpErrorCode::kInvalidArgument});
    }
    int owned_fd = socket(address->sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (owned_fd < 0) {
        return std::unexpected(SystemError(errno));
    }
    const int enabled = 1;
    if (setsockopt(owned_fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0 ||
        bind(owned_fd, address, length) < 0 || listen(owned_fd, backlog) < 0) {
        const int error = errno;
        (void)CloseFd(owned_fd);
        return std::unexpected(SystemError(error));
    }
    fd_ = owned_fd;
    opened_once_ = true;
    return {};
}

std::expected<int, TcpError> NativeTcpListener::Accept() noexcept {
    if (fd_ < 0) {
        return std::unexpected(TcpError{TcpErrorCode::kNotConnected});
    }
    const int accepted = accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (accepted < 0) {
        const int error = errno;
        if (WouldBlock(error) || error == EINTR) {
            return std::unexpected(TcpError{TcpErrorCode::kWouldBlock, error});
        }
        return std::unexpected(SystemError(error));
    }
    return accepted;
}

std::expected<void, TcpError> NativeTcpListener::Close() noexcept {
    return CloseFd(fd_);
}

} // namespace tcp
} // namespace kbsocket
