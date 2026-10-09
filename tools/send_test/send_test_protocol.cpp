// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>

#include "tools/send_test/send_test.hpp"
namespace kbsocket {
namespace tools {
namespace {
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(SendTestError{operation, code});
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
