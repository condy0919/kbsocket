// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_COMMON_CONTROL_CHANNEL_HPP_
#define KBSOCKET_TOOLS_COMMON_CONTROL_CHANNEL_HPP_
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
namespace kbsocket {
namespace tools {
struct ToolError {
    const char* operation;
    int code = 0;
};
/// 有界 TCP 控制通道，仅传端点和同步消息；不承载测试 payload。所有等待都有截止时间。
class ControlChannel {
public:
    explicit ControlChannel(int fd = -1) noexcept : fd_(fd) {}
    ControlChannel(const ControlChannel&) = delete;
    ControlChannel& operator=(const ControlChannel&) = delete;
    ~ControlChannel();
    std::expected<void, ToolError> Open(bool server, const char* ipv4, std::uint16_t port,
                                        std::uint32_t timeout_ms) noexcept;
    std::expected<void, ToolError> Write(std::span<const std::byte> data, std::uint32_t timeout_ms) noexcept;
    std::expected<void, ToolError> Read(std::span<std::byte> data, std::uint32_t timeout_ms) noexcept;
    std::expected<void, ToolError> SendMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept;
    std::expected<void, ToolError> ExpectMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept;

private:
    int fd_;
};

} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_COMMON_CONTROL_CHANNEL_HPP_
