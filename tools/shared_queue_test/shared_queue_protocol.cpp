// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <limits>
#include <span>

#include "tools/common/control_channel.hpp"
#include "tools/shared_queue_test/shared_queue_test.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kHelloMagic = 0x4b425351;   // KBSQ，与其他硬件工具隔离。
constexpr std::uint32_t kPayloadMagic = 0x4b425346; // KBSF
constexpr std::uint32_t kVersion = 1;
constexpr std::uint32_t kClosedMarker = 0x53435143;

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

void Put64(std::span<std::byte> wire, std::uint64_t value) noexcept {
    Put32(wire, static_cast<std::uint32_t>(value >> 32));
    Put32(wire.subspan(4), static_cast<std::uint32_t>(value));
}

std::uint64_t Get64(std::span<const std::byte> wire) noexcept {
    return (static_cast<std::uint64_t>(Get32(wire)) << 32) | Get32(wire.subspan(4));
}

// 正文与整个头部关联，跨方向、跨源/目标、跨序号的错误复用均会改变校验内容。
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

std::expected<void, ToolError> ValidateSharedOptions(const SharedQueueOptions& options) noexcept {
    if (options.bytes < kSharedPayloadHeader || options.bytes > 4096 || options.jetties == 0 ||
        options.jetties > kSharedMaxJetties || options.messages < options.jetties || options.batch == 0 ||
        options.batch > 256 || options.timeout_ms == 0 || options.timeout_ms > 3600000) {
        return Error("invalid shared queue options", EINVAL);
    }
    return {};
}

SharedHelloBytes EncodeSharedHello(const SharedQueueHello& hello) noexcept {
    SharedHelloBytes wire{};
    Put32(wire, kHelloMagic);
    Put32(std::span(wire).subspan(4), kVersion);
    Put32(std::span(wire).subspan(8), hello.server ? 1 : 0);
    Put32(std::span(wire).subspan(12), hello.options.jetties);
    Put32(std::span(wire).subspan(16), hello.options.bytes);
    Put32(std::span(wire).subspan(20), hello.options.messages);
    Put32(std::span(wire).subspan(24), hello.options.batch);
    for (std::size_t i = 0; i < std::min(hello.options.jetties, kSharedMaxJetties); ++i) {
        const auto offset = 32 + i * 20;
        for (std::size_t j = 0; j < 16; ++j) {
            wire[offset + j] = static_cast<std::byte>(hello.endpoints[i].eid.raw[j]);
        }
        Put32(std::span(wire).subspan(offset + 16), hello.endpoints[i].id);
    }
    return wire;
}

std::expected<SharedQueueHello, ToolError> DecodeSharedHello(const SharedHelloBytes& wire) noexcept {
    if (Get32(wire) != kHelloMagic || Get32(std::span(wire).subspan(4)) != kVersion) {
        return Error("shared queue hello version", EPROTO);
    }
    const auto role = Get32(std::span(wire).subspan(8));
    if (role > 1 || Get32(std::span(wire).subspan(28)) != 0) {
        return Error("shared queue hello flags", EPROTO);
    }
    SharedQueueHello hello;
    hello.server = role == 1;
    hello.options.jetties = Get32(std::span(wire).subspan(12));
    hello.options.bytes = Get32(std::span(wire).subspan(16));
    hello.options.messages = Get32(std::span(wire).subspan(20));
    hello.options.batch = Get32(std::span(wire).subspan(24));
    const auto valid = ValidateSharedOptions(hello.options);
    if (!valid) {
        return Error("shared queue hello options", EPROTO);
    }
    for (std::size_t i = 0; i < hello.options.jetties; ++i) {
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
                return Error("duplicate shared queue endpoint", EPROTO);
            }
        }
    }
    const auto unused = std::span(wire).subspan(32 + 20 * hello.options.jetties);
    if (!std::all_of(unused.begin(), unused.end(), [](std::byte byte) { return byte == std::byte{0}; })) {
        return Error("shared queue unused endpoint", EPROTO);
    }
    return hello;
}

std::expected<SharedQueueHello, ToolError> ExchangeSharedHello(ControlChannel& channel,
                                                               const SharedQueueHello& local) noexcept {
    auto valid = ValidateSharedOptions(local.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    const auto outgoing = EncodeSharedHello(local);
    auto decoded_local = DecodeSharedHello(outgoing);
    if (!decoded_local) {
        return std::unexpected(decoded_local.error());
    }
    auto sent = channel.Write(outgoing, local.options.timeout_ms);
    if (!sent) {
        return std::unexpected(sent.error());
    }
    SharedHelloBytes incoming{};
    auto received = channel.Read(incoming, local.options.timeout_ms);
    if (!received) {
        return std::unexpected(received.error());
    }
    auto peer = DecodeSharedHello(incoming);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (peer->server == local.server) {
        return Error("shared queue peer role", EPROTO);
    }
    const auto& options = peer->options;
    if (options.jetties != local.options.jetties || options.bytes != local.options.bytes ||
        options.messages != local.options.messages || options.batch != local.options.batch) {
        return Error("shared queue peer options", EPROTO);
    }
    return peer;
}

std::uint64_t SharedImmediate(const SharedFrame& frame) noexcept {
    constexpr std::array<std::uint64_t, 8> kPatterns{0,
                                                     std::numeric_limits<std::uint64_t>::max(),
                                                     std::uint64_t{1} << 63,
                                                     1,
                                                     0xaaaaaaaaaaaaaaaaull,
                                                     0x5555555555555555ull,
                                                     0x0123456789abcdefull,
                                                     0xfedcba9876543210ull};
    return kPatterns[(frame.sequence + frame.source) % kPatterns.size()];
}

void FillSharedPayload(std::span<std::byte> data, const SharedFrame& frame) noexcept {
    if (data.size() < kSharedPayloadHeader) {
        return;
    }
    Put32(data, kPayloadMagic);
    Put32(data.subspan(4), kVersion);
    Put32(data.subspan(8), frame.source);
    Put32(data.subspan(12), frame.target);
    Put64(data.subspan(16), frame.sequence);
    Put32(data.subspan(24), frame.server ? 1 : 0);
    Put32(data.subspan(28), static_cast<std::uint32_t>(data.size()));
    auto seed = PayloadSeed(data.first(kSharedPayloadHeader));
    for (auto& byte : data.subspan(kSharedPayloadHeader)) {
        byte = NextPayloadByte(seed);
    }
}

std::expected<SharedFrame, ToolError> CheckSharedPayload(std::span<const std::byte> data) noexcept {
    if (data.size() < kSharedPayloadHeader || data.size() > 4096) {
        return Error("shared queue payload size", EPROTO);
    }
    if (Get32(data) != kPayloadMagic || Get32(data.subspan(4)) != kVersion || Get32(data.subspan(28)) != data.size()) {
        return Error("shared queue payload header", EPROTO);
    }
    const auto role = Get32(data.subspan(24));
    SharedFrame frame{.source = Get32(data.subspan(8)),
                      .target = Get32(data.subspan(12)),
                      .sequence = Get64(data.subspan(16)),
                      .server = role == 1};
    if (role > 1 || frame.source >= kSharedMaxJetties || frame.target >= kSharedMaxJetties) {
        return Error("shared queue payload routing", EPROTO);
    }
    auto seed = PayloadSeed(data.first(kSharedPayloadHeader));
    for (const auto byte : data.subspan(kSharedPayloadHeader)) {
        if (byte != NextPayloadByte(seed)) {
            return Error("shared queue payload mismatch", EILSEQ);
        }
    }
    return frame;
}

std::expected<void, ToolError> ConfirmSharedClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept {
    auto sent = channel.SendMarker(kClosedMarker, timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(kClosedMarker, timeout_ms);
}
} // namespace tools
} // namespace kbsocket
