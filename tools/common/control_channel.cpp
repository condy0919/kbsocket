// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/common/control_channel.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using Clock = std::chrono::steady_clock;
auto Error(const char* operation, int code = errno) noexcept {
    return std::unexpected(ToolError{operation, code});
}
std::expected<void, ToolError> Wait(int fd, short events, Clock::time_point deadline) noexcept {
    while (Clock::now() < deadline) {
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        pollfd item{fd, events, 0};
        const int rc = ::poll(&item, 1, static_cast<int>(std::clamp<std::int64_t>(ms, 1, INT_MAX)));
        if (rc > 0) {
            // HUP 与 POLLIN 同时出现时仍允许读取缓冲中的最后一条控制消息。
            if (item.revents & events)
                return {};
            return Error("control peer disconnected", ECONNRESET);
        }
        if (rc < 0 && errno != EINTR)
            return Error("poll control");
    }
    return Error("control timeout", ETIMEDOUT);
}
void Put32(std::span<std::byte> wire, std::uint32_t value) noexcept {
    for (int i = 3; i >= 0; --i) {
        wire[i] = static_cast<std::byte>(value & 255);
        value >>= 8;
    }
}
std::uint32_t Get32(std::span<const std::byte> wire) noexcept {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i)
        value = (value << 8) | std::to_integer<unsigned>(wire[i]);
    return value;
}
} // namespace

ControlChannel::~ControlChannel() {
    if (fd_ >= 0)
        ::close(fd_);
}
std::expected<void, ToolError> ControlChannel::Open(bool server, const char* ipv4, std::uint16_t port,
                                                    std::uint32_t timeout_ms) noexcept {
    if (fd_ >= 0)
        return Error("control already open", EBUSY);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (::inet_pton(AF_INET, ipv4, &address.sin_addr) != 1)
        return Error("expected numeric IPv4", EINVAL);
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return Error("socket");
    ScopeExit cleanup([fd]() noexcept { ::close(fd); });
    auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    if (server) {
        int enabled = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0)
            return Error("setsockopt");
        if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
            return Error("bind");
        if (::listen(fd, 1) != 0)
            return Error("listen");
        while (true) {
            auto ready = Wait(fd, POLLIN, deadline);
            if (!ready)
                return ready;
            const int accepted = ::accept4(fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (accepted >= 0) {
                fd_ = accepted;
                return {};
            }
            if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
                return Error("accept");
        }
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        if (errno != EINPROGRESS)
            return Error("connect");
        auto ready = Wait(fd, POLLOUT, deadline);
        if (!ready)
            return ready;
        int error = 0;
        socklen_t size = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) != 0)
            return Error("connect status");
        if (error)
            return Error("connect", error);
    }
    fd_ = fd;
    cleanup.Release();
    return {};
}
std::expected<void, ToolError> ControlChannel::Write(std::span<const std::byte> data,
                                                     std::uint32_t timeout_ms) noexcept {
    if (fd_ < 0) {
        return Error("control not open", EBADF);
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!data.empty()) {
        auto ready = Wait(fd_, POLLOUT, deadline);
        if (!ready)
            return ready;
        const auto n = ::send(fd_, data.data(), data.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0)
            data = data.subspan(static_cast<std::size_t>(n));
        else if (n == 0)
            return Error("control send EOF", ECONNRESET);
        else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            return Error("send control");
    }
    return {};
}
std::expected<void, ToolError> ControlChannel::Read(std::span<std::byte> data, std::uint32_t timeout_ms) noexcept {
    if (fd_ < 0) {
        return Error("control not open", EBADF);
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!data.empty()) {
        auto ready = Wait(fd_, POLLIN, deadline);
        if (!ready)
            return ready;
        const auto n = ::recv(fd_, data.data(), data.size(), MSG_DONTWAIT);
        if (n > 0)
            data = data.subspan(static_cast<std::size_t>(n));
        else if (n == 0)
            return Error("control receive EOF", ECONNRESET);
        else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            return Error("receive control");
    }
    return {};
}
std::expected<void, ToolError> ControlChannel::SendMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept {
    std::array<std::byte, 4> data{};
    Put32(data, value);
    return Write(data, timeout_ms);
}
std::expected<void, ToolError> ControlChannel::ExpectMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept {
    std::array<std::byte, 4> data{};
    auto result = Read(data, timeout_ms);
    if (!result)
        return result;
    if (Get32(data) != value)
        return Error("control marker mismatch", EPROTO);
    return {};
}

} // namespace tools
} // namespace kbsocket
