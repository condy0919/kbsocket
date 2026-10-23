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
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | std::to_integer<unsigned>(wire[i]);
    }
    return value;
}
std::byte PayloadByte(std::uint64_t sequence, std::size_t offset) noexcept {
    // 混合高位，避免间隔 256 字节或 256 条消息时内容重复而漏掉错位/陈旧段。
    auto value = sequence * 0x9e3779b97f4a7c15ULL + offset * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return static_cast<std::byte>((value ^ (value >> 31)) & 255);
}
} // namespace
std::expected<void, SendTestError> ValidateOptions(const SendTestOptions& o) noexcept {
    if (o.bytes < 8 || o.bytes > raw::RxBufferPool::kBufferSize || !o.messages || !o.batch ||
        o.batch > raw::TxSender::kMaxBatch || !o.timeout_ms || o.timeout_ms > 3600000) {
        return Error("invalid test options", EINVAL);
    }
    if (!o.sges || o.sges > kMaxSendSges ||
        (o.sges > 1 && (o.header_bytes < 8 || o.header_bytes >= o.bytes || o.bytes - o.header_bytes < o.sges - 1))) {
        return Error("invalid SGE/header layout", EINVAL);
    }
    return {};
}
HelloBytes EncodeHello(const SendTestHello& hello) noexcept {
    HelloBytes wire{};
    Put32(wire, 0x4b425354); // KBST
    Put32(std::span(wire).subspan(4), 2);
    for (std::size_t i = 0; i < 16; ++i) {
        wire[8 + i] = static_cast<std::byte>(hello.endpoint.eid.raw[i]);
    }
    Put32(std::span(wire).subspan(24), hello.endpoint.id);
    Put32(std::span(wire).subspan(28), hello.options.bytes);
    Put32(std::span(wire).subspan(32), hello.options.messages);
    Put32(std::span(wire).subspan(36), hello.options.batch);
    Put32(std::span(wire).subspan(40), hello.options.sges);
    Put32(std::span(wire).subspan(44), hello.options.header_bytes);
    Put32(std::span(wire).subspan(48), hello.options.send_imm);
    return wire;
}
std::expected<SendTestHello, SendTestError> DecodeHello(const HelloBytes& wire) noexcept {
    if (Get32(wire) != 0x4b425354 || Get32(std::span(wire).subspan(4)) != 2) {
        return Error("hello version", EPROTO);
    }
    SendTestHello hello;
    for (std::size_t i = 0; i < 16; ++i) {
        hello.endpoint.eid.raw[i] = std::to_integer<std::uint8_t>(wire[8 + i]);
    }
    hello.endpoint.id = Get32(std::span(wire).subspan(24));
    hello.options.bytes = Get32(std::span(wire).subspan(28));
    hello.options.messages = Get32(std::span(wire).subspan(32));
    hello.options.batch = Get32(std::span(wire).subspan(36));
    hello.options.sges = Get32(std::span(wire).subspan(40));
    hello.options.header_bytes = Get32(std::span(wire).subspan(44));
    const auto send_imm = Get32(std::span(wire).subspan(48));
    if (send_imm > 1) {
        return Error("hello send_imm", EPROTO);
    }
    hello.options.send_imm = send_imm;
    auto valid = ValidateOptions(hello.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    return hello;
}
void FillPayload(std::span<std::byte> data, std::uint64_t sequence, std::size_t offset) noexcept {
    // 所有分散段使用整个报文中的偏移；交换两段、截断或混入上一条消息均会校验失败。
    for (std::size_t i = 0; i < data.size(); ++i) {
        const auto position = offset + i;
        data[i] = position < 8 ? static_cast<std::byte>((sequence >> ((7 - position) * 8)) & 255)
                               : PayloadByte(sequence, position);
    }
}
std::expected<std::uint64_t, SendTestError> CheckPayload(std::span<const std::byte> data) noexcept {
    if (data.size() < 8) {
        return Error("short payload", EPROTO);
    }
    std::uint64_t sequence = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        sequence = (sequence << 8) | std::to_integer<unsigned>(data[i]);
    }
    for (std::size_t i = 8; i < data.size(); ++i) {
        if (data[i] != PayloadByte(sequence, i)) {
            return Error("payload mismatch", EILSEQ);
        }
    }
    return sequence;
}
std::uint32_t SendSgeLength(const SendTestOptions& options, std::size_t index) noexcept {
    if (options.sges == 1) {
        return options.bytes;
    }
    if (!index) {
        return options.header_bytes;
    }
    const auto payload = options.bytes - options.header_bytes;
    const auto pieces = options.sges - 1;
    return payload / pieces + (index <= payload % pieces ? 1 : 0);
}
std::uint64_t SendImmediate(std::uint64_t sequence) noexcept {
    return 0xa55a123400000000ULL ^ sequence;
}
} // namespace tools
} // namespace kbsocket
