// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TRANSPORT_TCP_NATIVE_TCP_BACKEND_HPP_
#define KBSOCKET_TRANSPORT_TCP_NATIVE_TCP_BACKEND_HPP_

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

namespace kbsocket {
namespace tcp {

enum class TcpErrorCode : std::uint8_t {
    kInvalidArgument,
    kInUse,
    kNotConnected,
    kNotNonblocking,
    kWouldBlock,
    kWriteClosed,
    kSystemError,
    kTimeout,
    kCancelled,
};
struct TcpError {
    TcpErrorCode code;
    int system_error = 0;
};
enum class TcpState : std::uint8_t { kIdle, kConnecting, kConnected, kFailed, kClosed };
struct TcpIoBudget {
    std::size_t bytes = 64 * 1024;
    std::uint32_t syscalls = 8;
};
struct TcpInterests {
    bool readable = false;
    bool writable = false;
};

/// 单 owner 的透明 TCP 字节流，无协议探测、内部发送队列或业务 framing。
/// Open 仅接收干净的已连接非阻塞 TCP，或 TakeEstablishedFd 返回的已完成握手 fd；
/// 成功才接管所有权，失败不修改/关闭调用方 fd。每个对象只允许成功打开一次。
/// Read/Write 返回已复制的前缀，零进展 EAGAIN 返回 kWouldBlock；正数返回后即使
/// 后续 syscall 失败也保留该前缀，错误由 failure()/下一次调用报告。
/// Write 返回的前缀已由内核复制，调用方可以立即复用该前缀源内存。
class NativeTcpBackend {
public:
    NativeTcpBackend() = default;
    ~NativeTcpBackend();
    NativeTcpBackend(const NativeTcpBackend&) = delete;
    NativeTcpBackend& operator=(const NativeTcpBackend&) = delete;

    std::expected<void, TcpError> Open(int fd) noexcept;
    /// 只接受数值 AF_INET/AF_INET6 地址，不做 DNS；deadline 由单调时钟给出。
    std::expected<void, TcpError> Connect(const sockaddr* address, socklen_t length, std::uint64_t now_ns,
                                          std::uint64_t deadline_ns) noexcept;
    /// 由 owner 在可写/错误通知或定时推进时调用；尚未建立返回 kWouldBlock。
    std::expected<void, TcpError> FinishConnect(std::uint64_t now_ns) noexcept;
    std::expected<std::size_t, TcpError> Read(std::span<std::byte> output, TcpIoBudget budget = {}) noexcept;
    std::expected<std::size_t, TcpError> Write(std::span<const std::byte> input, TcpIoBudget budget = {}) noexcept;
    /// 不禁止继续读；FIN 排在所有已由 Write 接受的字节之后，不代表远端应用已消费。
    std::expected<void, TcpError> ShutdownWrite() noexcept;
    void Cancel() noexcept;
    std::expected<void, TcpError> Close() noexcept;
    int fd() const noexcept {
        return fd_;
    }
    TcpState state() const noexcept {
        return state_;
    }
    bool read_eof() const noexcept {
        return read_eof_;
    }
    bool write_shutdown() const noexcept {
        return write_shutdown_;
    }
    const std::optional<TcpError>& failure() const noexcept {
        return failure_;
    }
    /// 仅有待写应用数据时传 want_write，避免空闲连接不断触发可写通知。
    TcpInterests Interests(bool want_write = false) const noexcept;

private:
    std::expected<void, TcpError> CheckConnected() const noexcept;
    TcpError Fail(TcpError error) noexcept;
    int fd_ = -1;
    bool opened_once_ = false;
    bool read_eof_ = false;
    bool write_shutdown_ = false;
    TcpState state_ = TcpState::kIdle;
    std::optional<TcpError> failure_;
    std::uint64_t last_now_ns_ = 0;
    std::uint64_t deadline_ns_ = 0;
};

/// 单 owner 非阻塞监听器；Accept 只交出 fd，不发送 HELLO，不替调用方选择协议。
class NativeTcpListener {
public:
    NativeTcpListener() = default;
    ~NativeTcpListener();
    NativeTcpListener(const NativeTcpListener&) = delete;
    NativeTcpListener& operator=(const NativeTcpListener&) = delete;
    std::expected<void, TcpError> Open(const sockaddr* address, socklen_t length, int backlog = 128) noexcept;
    std::expected<int, TcpError> Accept() noexcept;
    std::expected<void, TcpError> Close() noexcept;
    int fd() const noexcept {
        return fd_;
    }

private:
    int fd_ = -1;
    bool opened_once_ = false;
};

} // namespace tcp
} // namespace kbsocket
#endif // KBSOCKET_TRANSPORT_TCP_NATIVE_TCP_BACKEND_HPP_
