// SPDX-License-Identifier: MulanPSL-2.0
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>

#include "tools/send_test/send_test.hpp"

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using Clock = std::chrono::steady_clock;
auto Error(const char* operation, int code = errno) noexcept {
    return std::unexpected(SendTestError{operation, code});
}
std::expected<void, SendTestError> Wait(int fd, short events, Clock::time_point deadline) noexcept {
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
std::expected<void, SendTestError> ControlChannel::Open(bool server, const char* ipv4, std::uint16_t port,
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
std::expected<void, SendTestError> ControlChannel::Write(std::span<const std::byte> data,
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
std::expected<void, SendTestError> ControlChannel::Read(std::span<std::byte> data, std::uint32_t timeout_ms) noexcept {
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
std::expected<void, SendTestError> ControlChannel::SendMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept {
    std::array<std::byte, 4> data{};
    Put32(data, value);
    return Write(data, timeout_ms);
}
std::expected<void, SendTestError> ControlChannel::ExpectMarker(std::uint32_t value,
                                                                std::uint32_t timeout_ms) noexcept {
    std::array<std::byte, 4> data{};
    auto result = Read(data, timeout_ms);
    if (!result)
        return result;
    if (Get32(data) != value)
        return Error("control marker mismatch", EPROTO);
    return {};
}

std::expected<void, SendTestError> ValidateOptions(const SendTestOptions& o) noexcept {
    if (o.bytes < 8 || o.bytes > 1024 * 1024 || !o.messages || !o.batch || o.batch > raw::TxSender::kMaxBatch ||
        !o.timeout_ms || o.timeout_ms > 3600000)
        return Error("invalid test options", EINVAL);
    return {};
}
HelloBytes EncodeHello(const SendTestHello& hello) noexcept {
    HelloBytes wire{};
    Put32(wire, 0x4b425354); // KBST
    Put32(std::span(wire).subspan(4), 1);
    for (std::size_t i = 0; i < 16; ++i)
        wire[8 + i] = static_cast<std::byte>(hello.endpoint.eid.raw[i]);
    Put32(std::span(wire).subspan(24), hello.endpoint.id);
    Put32(std::span(wire).subspan(28), hello.options.bytes);
    Put32(std::span(wire).subspan(32), hello.options.messages);
    Put32(std::span(wire).subspan(36), hello.options.batch);
    return wire;
}
std::expected<SendTestHello, SendTestError> DecodeHello(const HelloBytes& wire) noexcept {
    if (Get32(wire) != 0x4b425354 || Get32(std::span(wire).subspan(4)) != 1)
        return Error("hello version", EPROTO);
    SendTestHello hello;
    for (std::size_t i = 0; i < 16; ++i)
        hello.endpoint.eid.raw[i] = std::to_integer<std::uint8_t>(wire[8 + i]);
    hello.endpoint.id = Get32(std::span(wire).subspan(24));
    hello.options.bytes = Get32(std::span(wire).subspan(28));
    hello.options.messages = Get32(std::span(wire).subspan(32));
    hello.options.batch = Get32(std::span(wire).subspan(36));
    auto valid = ValidateOptions(hello.options);
    if (!valid)
        return std::unexpected(valid.error());
    return hello;
}
void FillPayload(std::span<std::byte> data, std::uint64_t sequence) noexcept {
    // 调用者已保证长度至少为 8；编码消息序号后填充与序号、偏移有关的确定性内容。
    for (std::size_t i = 0; i < data.size(); ++i) {
        data[i] = i < 8 ? static_cast<std::byte>((sequence >> ((7 - i) * 8)) & 255)
                        : static_cast<std::byte>((sequence * 31 + i * 17) & 255);
    }
}
std::expected<std::uint64_t, SendTestError> CheckPayload(std::span<const std::byte> data) noexcept {
    if (data.size() < 8)
        return Error("short payload", EPROTO);
    std::uint64_t sequence = 0;
    for (std::size_t i = 0; i < 8; ++i)
        sequence = (sequence << 8) | std::to_integer<unsigned>(data[i]);
    for (std::size_t i = 8; i < data.size(); ++i) {
        if (data[i] != static_cast<std::byte>((sequence * 31 + i * 17) & 255))
            return Error("payload mismatch", EILSEQ);
    }
    return sequence;
}
} // namespace tools
} // namespace kbsocket
