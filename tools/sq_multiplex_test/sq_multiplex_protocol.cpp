// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <span>

#include "tools/common/control_channel.hpp"
#include "tools/sq_multiplex_test/sq_multiplex_test.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kHelloMagic = 0x4b424d58;   // KBMX
constexpr std::uint32_t kPayloadMagic = 0x4b424d46; // KBMF
constexpr std::uint32_t kVersion = 1;
constexpr std::uint32_t kClosedMarker = 0x4d58434c;
constexpr std::uint32_t kMaxBatch = 128;
constexpr std::uint32_t kMaxRounds = 1000;
constexpr std::uint32_t kMaxTimeoutMs = 3600000;

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

bool SameEndpoint(const urma_jetty_id_t& first, const urma_jetty_id_t& second) noexcept {
    return first.id == second.id &&
           std::equal(std::begin(first.eid.raw), std::end(first.eid.raw), std::begin(second.eid.raw));
}

// 正文关联整个头部，防止合法身份字段被改写后仍通过内容校验。
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

std::expected<void, ToolError> ValidateMultiplexOptions(const SqMultiplexOptions& options) noexcept {
    if (options.bytes < kMultiplexPayloadHeader || options.bytes > 4096 || options.batch == 0 ||
        options.batch > kMaxBatch || options.rounds == 0 || options.rounds > kMaxRounds || options.timeout_ms == 0 ||
        options.timeout_ms > kMaxTimeoutMs) {
        return Error("invalid SQ multiplex limits", EINVAL);
    }
    return {};
}

MultiplexHelloBytes EncodeMultiplexHello(const SqMultiplexHello& hello) noexcept {
    MultiplexHelloBytes wire{};
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

std::expected<SqMultiplexHello, ToolError> DecodeMultiplexHello(const MultiplexHelloBytes& wire) noexcept {
    if (Get32(wire) != kHelloMagic || Get32(std::span(wire).subspan(4)) != kVersion) {
        return Error("SQ multiplex hello version", EPROTO);
    }
    const auto role = Get32(std::span(wire).subspan(8));
    if (role > 1 || Get32(std::span(wire).subspan(24)) != 0 || Get32(std::span(wire).subspan(28)) != 0) {
        return Error("SQ multiplex hello flags", EPROTO);
    }
    SqMultiplexHello hello;
    hello.server = role == 1;
    hello.options.bytes = Get32(std::span(wire).subspan(12));
    hello.options.batch = Get32(std::span(wire).subspan(16));
    hello.options.rounds = Get32(std::span(wire).subspan(20));
    if (!ValidateMultiplexOptions(hello.options)) {
        return Error("SQ multiplex hello options", EPROTO);
    }
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        const auto offset = 32 + i * 20;
        auto& endpoint = hello.endpoints[i];
        for (std::size_t j = 0; j < 16; ++j) {
            endpoint.eid.raw[j] = std::to_integer<std::uint8_t>(wire[offset + j]);
        }
        endpoint.id = Get32(std::span(wire).subspan(offset + 16));
    }
    const urma_jetty_id_t empty{};
    if (hello.server && SameEndpoint(hello.endpoints[0], hello.endpoints[1])) {
        return Error("SQ multiplex targets must be distinct", EPROTO);
    }
    if (!hello.server && !SameEndpoint(hello.endpoints[1], empty)) {
        return Error("SQ multiplex client has only one SQ", EPROTO);
    }
    return hello;
}

std::expected<SqMultiplexHello, ToolError> ExchangeMultiplexHello(ControlChannel& channel,
                                                                  const SqMultiplexHello& local) noexcept {
    auto valid = ValidateMultiplexOptions(local.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    const auto outgoing = EncodeMultiplexHello(local);
    auto checked = DecodeMultiplexHello(outgoing);
    if (!checked) {
        return std::unexpected(checked.error());
    }
    auto sent = channel.Write(outgoing, local.options.timeout_ms);
    if (!sent) {
        return std::unexpected(sent.error());
    }
    MultiplexHelloBytes incoming{};
    auto received = channel.Read(incoming, local.options.timeout_ms);
    if (!received) {
        return std::unexpected(received.error());
    }
    auto peer = DecodeMultiplexHello(incoming);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (peer->server == local.server) {
        return Error("SQ multiplex peer role", EPROTO);
    }
    if (peer->options.bytes != local.options.bytes || peer->options.batch != local.options.batch ||
        peer->options.rounds != local.options.rounds) {
        return Error("SQ multiplex peer options", EPROTO);
    }
    return peer;
}

std::expected<void, ToolError> MultiplexBarrier(ControlChannel& channel, std::uint32_t marker,
                                                std::uint32_t timeout_ms) noexcept {
    auto sent = channel.SendMarker(marker, timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(marker, timeout_ms);
}

std::expected<void, ToolError> ConfirmMultiplexClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept {
    return MultiplexBarrier(channel, kClosedMarker, timeout_ms);
}

std::uint64_t MultiplexImmediate(const MultiplexFrame& frame) noexcept {
    // 各身份字段互不重叠，高位保持非零以检测 64 位 IMM 截断。
    const auto identity = (static_cast<std::uint64_t>(frame.phase) << 20) |
                          (static_cast<std::uint64_t>(frame.round) << 9) |
                          (static_cast<std::uint64_t>(frame.sink) << 8) | frame.index;
    return identity ^ 0xc7a5316e98420bdfull;
}

void FillMultiplexPayload(std::span<std::byte> data, const MultiplexFrame& frame) noexcept {
    if (data.size() < kMultiplexPayloadHeader) {
        return;
    }
    Put32(data, kPayloadMagic);
    Put32(data.subspan(4), kVersion);
    Put32(data.subspan(8), static_cast<std::uint32_t>(frame.phase));
    Put32(data.subspan(12), frame.round);
    Put32(data.subspan(16), frame.index);
    Put32(data.subspan(20), frame.sink ? 1 : 0);
    Put32(data.subspan(24), static_cast<std::uint32_t>(data.size()));
    Put32(data.subspan(28), 0);
    auto seed = PayloadSeed(data.first(kMultiplexPayloadHeader));
    for (auto& byte : data.subspan(kMultiplexPayloadHeader)) {
        byte = NextPayloadByte(seed);
    }
}

std::expected<MultiplexFrame, ToolError> CheckMultiplexPayload(std::span<const std::byte> data) noexcept {
    if (data.size() < kMultiplexPayloadHeader || data.size() > 4096) {
        return Error("SQ multiplex payload length", EPROTO);
    }
    if (Get32(data) != kPayloadMagic || Get32(data.subspan(4)) != kVersion || Get32(data.subspan(24)) != data.size() ||
        Get32(data.subspan(28)) != 0) {
        return Error("SQ multiplex payload header", EPROTO);
    }
    const auto phase = Get32(data.subspan(8));
    const auto sink = Get32(data.subspan(20));
    MultiplexFrame frame{.phase = static_cast<MultiplexPhase>(phase),
                         .round = Get32(data.subspan(12)),
                         .index = Get32(data.subspan(16)),
                         .sink = sink == 1};
    if (phase > static_cast<std::uint32_t>(MultiplexPhase::kAfter) || sink > 1 || frame.round >= kMaxRounds ||
        frame.index >= kMaxBatch || (frame.sink && frame.phase != MultiplexPhase::kMixed)) {
        return Error("SQ multiplex payload identity", EPROTO);
    }
    auto seed = PayloadSeed(data.first(kMultiplexPayloadHeader));
    for (const auto byte : data.subspan(kMultiplexPayloadHeader)) {
        if (byte != NextPayloadByte(seed)) {
            return Error("SQ multiplex payload contents", EPROTO);
        }
    }
    return frame;
}
} // namespace tools
} // namespace kbsocket
