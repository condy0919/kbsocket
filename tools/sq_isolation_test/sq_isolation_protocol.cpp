// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <span>

#include "tools/common/control_channel.hpp"
#include "tools/sq_isolation_test/sq_isolation_test.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kHelloMagic = 0x4b425349;   // KBSI，与其他硬件工具隔离。
constexpr std::uint32_t kPayloadMagic = 0x4b424946; // KBIF
constexpr std::uint32_t kVersion = 1;
constexpr std::uint32_t kClosedMarker = 0x5349434c;
constexpr std::uint32_t kMaxRounds = 1000000;

auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
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

// 正文与整个头部关联，阶段、SQ、轮次及方向串账都不能通过校验。
std::uint64_t PayloadSeed(std::span<const std::byte> header) noexcept {
    std::uint64_t seed = 14695981039346656037ull;
    for (const auto byte : header) {
        seed = (seed ^ std::to_integer<unsigned>(byte)) * 1099511628211ull;
    }
    return seed;
}

std::byte NextPayloadByte(std::uint64_t& state) noexcept {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return static_cast<std::byte>((state * 2685821657736338717ull) >> 56);
}
} // namespace

std::expected<void, ToolError> ValidateIsolationOptions(const SqIsolationOptions& options) noexcept {
    if (options.bytes < kIsolationPayloadHeader || options.bytes > 4096 || options.batch == 0 ||
        options.batch > raw::TxSender::kMaxBatch || options.rounds == 0 || options.rounds > kMaxRounds ||
        options.timeout_ms == 0 || options.timeout_ms > 3600000) {
        return Error("invalid SQ isolation options", EINVAL);
    }
    return {};
}

IsolationHelloBytes EncodeIsolationHello(const SqIsolationHello& hello) noexcept {
    IsolationHelloBytes wire{};
    Put32(wire, kHelloMagic);
    Put32(std::span(wire).subspan(4), kVersion);
    Put32(std::span(wire).subspan(8), hello.server ? 1 : 0);
    Put32(std::span(wire).subspan(12), hello.options.bytes);
    Put32(std::span(wire).subspan(16), hello.options.batch);
    Put32(std::span(wire).subspan(20), hello.options.rounds);
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        const auto offset = 32 + i * 20;
        for (std::size_t j = 0; j < 16; ++j) {
            wire[offset + j] = static_cast<std::byte>(hello.endpoints[i].eid.raw[j]);
        }
        Put32(std::span(wire).subspan(offset + 16), hello.endpoints[i].id);
    }
    return wire;
}

std::expected<SqIsolationHello, ToolError> DecodeIsolationHello(const IsolationHelloBytes& wire) noexcept {
    if (Get32(wire) != kHelloMagic || Get32(std::span(wire).subspan(4)) != kVersion) {
        return Error("SQ isolation hello version", EPROTO);
    }
    const auto role = Get32(std::span(wire).subspan(8));
    if (role > 1 || Get32(std::span(wire).subspan(24)) != 0 || Get32(std::span(wire).subspan(28)) != 0) {
        return Error("SQ isolation hello flags", EPROTO);
    }
    SqIsolationHello hello;
    hello.server = role == 1;
    hello.options.bytes = Get32(std::span(wire).subspan(12));
    hello.options.batch = Get32(std::span(wire).subspan(16));
    hello.options.rounds = Get32(std::span(wire).subspan(20));
    if (!ValidateIsolationOptions(hello.options)) {
        return Error("SQ isolation hello options", EPROTO);
    }
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        const auto offset = 32 + i * 20;
        auto& endpoint = hello.endpoints[i];
        for (std::size_t j = 0; j < 16; ++j) {
            endpoint.eid.raw[j] = std::to_integer<std::uint8_t>(wire[offset + j]);
        }
        endpoint.id = Get32(std::span(wire).subspan(offset + 16));
        for (std::size_t j = 0; j < i; ++j) {
            const auto& previous = hello.endpoints[j];
            if (endpoint.id == previous.id &&
                std::equal(std::begin(endpoint.eid.raw), std::end(endpoint.eid.raw), std::begin(previous.eid.raw))) {
                return Error("duplicate SQ isolation endpoint", EPROTO);
            }
        }
    }
    return hello;
}

std::expected<SqIsolationHello, ToolError> ExchangeIsolationHello(ControlChannel& channel,
                                                                  const SqIsolationHello& local) noexcept {
    const auto valid = ValidateIsolationOptions(local.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    const auto outgoing = EncodeIsolationHello(local);
    const auto decoded_local = DecodeIsolationHello(outgoing);
    if (!decoded_local) {
        return std::unexpected(decoded_local.error());
    }
    auto sent = channel.Write(outgoing, local.options.timeout_ms);
    if (!sent) {
        return std::unexpected(sent.error());
    }
    IsolationHelloBytes incoming{};
    auto received = channel.Read(incoming, local.options.timeout_ms);
    if (!received) {
        return std::unexpected(received.error());
    }
    auto peer = DecodeIsolationHello(incoming);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (peer->server == local.server) {
        return Error("SQ isolation peer role", EPROTO);
    }
    if (peer->options.bytes != local.options.bytes || peer->options.batch != local.options.batch ||
        peer->options.rounds != local.options.rounds) {
        return Error("SQ isolation peer options", EPROTO);
    }
    return peer;
}

std::expected<void, ToolError> IsolationBarrier(ControlChannel& channel, std::uint32_t marker,
                                                std::uint32_t timeout_ms) noexcept {
    auto sent = channel.SendMarker(marker, timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(marker, timeout_ms);
}

std::expected<void, ToolError> ConfirmIsolationClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept {
    return IsolationBarrier(channel, kClosedMarker, timeout_ms);
}

std::uint64_t IsolationImmediate(const IsolationFrame& frame) noexcept {
    // 测试元信息占低 44 位；固定高位用于发现 IMM 截断，不定义正式连接协议。
    const auto identity =
        (static_cast<std::uint64_t>(frame.phase) << 42) | (static_cast<std::uint64_t>(frame.source) << 41) |
        (static_cast<std::uint64_t>(frame.server) << 40) | (static_cast<std::uint64_t>(frame.round) << 8) | frame.index;
    return identity ^ 0xa5c39e271b4068dfull;
}

void FillIsolationPayload(std::span<std::byte> data, const IsolationFrame& frame) noexcept {
    if (data.size() < kIsolationPayloadHeader) {
        return;
    }
    Put32(data, kPayloadMagic);
    Put32(data.subspan(4), kVersion);
    Put32(data.subspan(8), static_cast<std::uint32_t>(frame.phase));
    Put32(data.subspan(12), frame.source);
    Put32(data.subspan(16), frame.round);
    Put32(data.subspan(20), frame.index);
    Put32(data.subspan(24), frame.server ? 1 : 0);
    Put32(data.subspan(28), static_cast<std::uint32_t>(data.size()));
    auto seed = PayloadSeed(data.first(kIsolationPayloadHeader));
    for (auto& byte : data.subspan(kIsolationPayloadHeader)) {
        byte = NextPayloadByte(seed);
    }
}

std::expected<IsolationFrame, ToolError> CheckIsolationPayload(std::span<const std::byte> data) noexcept {
    if (data.size() < kIsolationPayloadHeader || data.size() > 4096) {
        return Error("SQ isolation payload size", EPROTO);
    }
    if (Get32(data) != kPayloadMagic || Get32(data.subspan(4)) != kVersion || Get32(data.subspan(28)) != data.size()) {
        return Error("SQ isolation payload header", EPROTO);
    }
    const auto phase = Get32(data.subspan(8));
    const auto role = Get32(data.subspan(24));
    IsolationFrame frame{.phase = static_cast<IsolationPhase>(phase),
                         .source = Get32(data.subspan(12)),
                         .round = Get32(data.subspan(16)),
                         .index = Get32(data.subspan(20)),
                         .server = role == 1};
    if (phase > static_cast<std::uint32_t>(IsolationPhase::kAfter) || role > 1 || frame.source > 1 ||
        frame.round >= kMaxRounds || frame.index >= raw::TxSender::kMaxBatch) {
        return Error("SQ isolation payload identity", EPROTO);
    }
    auto seed = PayloadSeed(data.first(kIsolationPayloadHeader));
    for (const auto byte : data.subspan(kIsolationPayloadHeader)) {
        if (byte != NextPayloadByte(seed)) {
            return Error("SQ isolation payload mismatch", EILSEQ);
        }
    }
    return frame;
}
} // namespace tools
} // namespace kbsocket
