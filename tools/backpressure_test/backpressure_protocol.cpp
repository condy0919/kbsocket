// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <span>

#include "tools/backpressure_test/backpressure_test.hpp"
#include "tools/common/control_channel.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kHelloMagic = 0x4b424250;   // KBBP，与其他硬件工具隔离。
constexpr std::uint32_t kPayloadMagic = 0x4b425046; // KBPF
constexpr std::uint32_t kVersion = 1;
constexpr std::uint32_t kClosedMarker = 0x4250434c;
constexpr std::uint32_t kMaxRounds = 1000000;
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

void Put64(std::span<std::byte> wire, std::uint64_t value) noexcept {
    Put32(wire, static_cast<std::uint32_t>(value >> 32));
    Put32(wire.subspan(4), static_cast<std::uint32_t>(value));
}

std::uint64_t Get64(std::span<const std::byte> wire) noexcept {
    return (static_cast<std::uint64_t>(Get32(wire)) << 32) | Get32(wire.subspan(4));
}

// 正文与全部头部关联，轮次、消息身份或清理标记串账均不能通过校验。
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

bool ValidReport(const BackpressureReport& report, bool rnr_without_drain) noexcept {
    if (rnr_without_drain) {
        // 同一 SQ 续发实验不修改队列状态；只有整批 RNR 已终结、没有成功消息或排空边界才可补投 RX。
        return report.accepted > 0 && report.accepted <= raw::TxSender::kMaxBatch &&
               report.accepted == report.retired && report.success == 0 && report.flush_done == 0 &&
               report.has_success_map &&
               std::all_of(report.success_map.begin(), report.success_map.end(), [](auto word) { return word == 0; });
    }
    if (report.accepted != report.retired || report.success > report.retired ||
        report.flush_done != static_cast<std::uint32_t>(report.success < report.retired)) {
        return false;
    }
    std::uint32_t success_bits = 0;
    for (const auto word : report.success_map) {
        success_bits += static_cast<std::uint32_t>(std::popcount(word));
    }
    if (!report.has_success_map) {
        return success_bits == 0;
    }
    if (report.accepted > raw::TxSender::kMaxBatch || success_bits != report.success) {
        return false;
    }
    for (std::size_t i = report.accepted; i < raw::TxSender::kMaxBatch; ++i) {
        if ((report.success_map[i / 64] & (std::uint64_t{1} << (i % 64))) != 0) {
            return false;
        }
    }
    return true;
}
} // namespace

std::expected<void, ToolError> ValidateBackpressureOptions(const BackpressureOptions& options) noexcept {
    if (options.mode != BackpressureMode::kTxQuota && options.mode != BackpressureMode::kRxDelay &&
        options.mode != BackpressureMode::kRnrContinue) {
        return Error("invalid backpressure mode", EINVAL);
    }
    if (options.expect != BackpressureExpectation::kEither && options.expect != BackpressureExpectation::kSuccess &&
        options.expect != BackpressureExpectation::kError) {
        return Error("invalid backpressure expectation", EINVAL);
    }
    if (options.bytes < kBackpressurePayloadHeader || options.bytes > 4096 || options.tx_depth == 0 ||
        options.tx_depth > raw::TxSender::kMaxBatch || options.rounds == 0 || options.rounds > kMaxRounds ||
        options.timeout_ms == 0 || options.timeout_ms > kMaxTimeoutMs) {
        return Error("invalid backpressure limits", EINVAL);
    }
    if (options.mode != BackpressureMode::kRxDelay && options.expect != BackpressureExpectation::kEither) {
        return Error("--expect applies only to rx_delay", EINVAL);
    }
    if (options.mode == BackpressureMode::kRxDelay &&
        static_cast<std::uint64_t>(options.rx_delay_us) >= static_cast<std::uint64_t>(options.timeout_ms) * 1000) {
        return Error("RX delay must be shorter than timeout", EINVAL);
    }
    return {};
}

BackpressureHelloBytes EncodeBackpressureHello(const BackpressureHello& hello) noexcept {
    BackpressureHelloBytes wire{};
    Put32(wire, kHelloMagic);
    Put32(std::span(wire).subspan(4), kVersion);
    Put32(std::span(wire).subspan(8), hello.server ? 1 : 0);
    Put32(std::span(wire).subspan(12), static_cast<std::uint32_t>(hello.options.mode));
    Put32(std::span(wire).subspan(16), static_cast<std::uint32_t>(hello.options.expect));
    Put32(std::span(wire).subspan(20), hello.options.bytes);
    Put32(std::span(wire).subspan(24), hello.options.tx_depth);
    Put32(std::span(wire).subspan(28), hello.options.rounds);
    Put32(std::span(wire).subspan(32), hello.options.rx_delay_us);
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        const auto offset = 48 + i * 20;
        for (std::size_t j = 0; j < 16; ++j) {
            wire[offset + j] = static_cast<std::byte>(hello.endpoints[i].eid.raw[j]);
        }
        Put32(std::span(wire).subspan(offset + 16), hello.endpoints[i].id);
    }
    return wire;
}

std::expected<BackpressureHello, ToolError> DecodeBackpressureHello(const BackpressureHelloBytes& wire) noexcept {
    if (Get32(wire) != kHelloMagic || Get32(std::span(wire).subspan(4)) != kVersion) {
        return Error("backpressure hello version", EPROTO);
    }
    const auto role = Get32(std::span(wire).subspan(8));
    if (role > 1 || Get32(std::span(wire).subspan(36)) != 0 || Get32(std::span(wire).subspan(40)) != 0 ||
        Get32(std::span(wire).subspan(44)) != 0) {
        return Error("backpressure hello flags", EPROTO);
    }
    BackpressureHello hello;
    hello.server = role == 1;
    hello.options.mode = static_cast<BackpressureMode>(Get32(std::span(wire).subspan(12)));
    hello.options.expect = static_cast<BackpressureExpectation>(Get32(std::span(wire).subspan(16)));
    hello.options.bytes = Get32(std::span(wire).subspan(20));
    hello.options.tx_depth = Get32(std::span(wire).subspan(24));
    hello.options.rounds = Get32(std::span(wire).subspan(28));
    hello.options.rx_delay_us = Get32(std::span(wire).subspan(32));
    // 超时不在线上传输；只按工具允许的最大值校验 wire，实际等待使用各自的本地配置。
    hello.options.timeout_ms = kMaxTimeoutMs;
    if (!ValidateBackpressureOptions(hello.options)) {
        return Error("backpressure hello options", EPROTO);
    }
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        const auto offset = 48 + i * 20;
        auto& endpoint = hello.endpoints[i];
        for (std::size_t j = 0; j < 16; ++j) {
            endpoint.eid.raw[j] = std::to_integer<std::uint8_t>(wire[offset + j]);
        }
        endpoint.id = Get32(std::span(wire).subspan(offset + 16));
    }
    const auto& first = hello.endpoints[0];
    const auto& second = hello.endpoints[1];
    if (first.id == second.id &&
        std::equal(std::begin(first.eid.raw), std::end(first.eid.raw), std::begin(second.eid.raw))) {
        return Error("duplicate backpressure endpoint", EPROTO);
    }
    return hello;
}

std::expected<BackpressureHello, ToolError> ExchangeBackpressureHello(ControlChannel& channel,
                                                                      const BackpressureHello& local) noexcept {
    const auto valid = ValidateBackpressureOptions(local.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    const auto outgoing = EncodeBackpressureHello(local);
    const auto decoded_local = DecodeBackpressureHello(outgoing);
    if (!decoded_local) {
        return std::unexpected(decoded_local.error());
    }
    auto sent = channel.Write(outgoing, local.options.timeout_ms);
    if (!sent) {
        return std::unexpected(sent.error());
    }
    BackpressureHelloBytes incoming{};
    auto received = channel.Read(incoming, local.options.timeout_ms);
    if (!received) {
        return std::unexpected(received.error());
    }
    auto peer = DecodeBackpressureHello(incoming);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (peer->server == local.server) {
        return Error("backpressure peer role", EPROTO);
    }
    const auto& own = local.options;
    const auto& other = peer->options;
    if (own.mode != other.mode || own.bytes != other.bytes || own.tx_depth != other.tx_depth ||
        (own.mode == BackpressureMode::kTxQuota && own.rounds != other.rounds) ||
        (own.mode == BackpressureMode::kRxDelay &&
         (own.expect != other.expect || own.rx_delay_us != other.rx_delay_us))) {
        return Error("backpressure peer options", EPROTO);
    }
    return peer;
}

std::expected<void, ToolError> BackpressureBarrier(ControlChannel& channel, std::uint32_t marker,
                                                   std::uint32_t timeout_ms) noexcept {
    auto sent = channel.SendMarker(marker, timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(marker, timeout_ms);
}

std::expected<void, ToolError> ConfirmBackpressureClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept {
    return BackpressureBarrier(channel, kClosedMarker, timeout_ms);
}

std::uint64_t BackpressureImmediate(const BackpressureFrame& frame) noexcept {
    // 低 30 位存放测试身份；高位保持非零，发现 SEND_WITH_IMM 的 64 位字段被截断。
    const auto identity = (static_cast<std::uint64_t>(frame.round) << 10) |
                          (static_cast<std::uint64_t>(frame.cleanup) << 9) | frame.index;
    return identity ^ 0xa5c39e271b4068dfull;
}

void FillBackpressurePayload(std::span<std::byte> data, const BackpressureFrame& frame) noexcept {
    if (data.size() < kBackpressurePayloadHeader) {
        return;
    }
    Put32(data, kPayloadMagic);
    Put32(data.subspan(4), kVersion);
    Put32(data.subspan(8), frame.round);
    Put32(data.subspan(12), frame.index);
    Put32(data.subspan(16), frame.cleanup ? 1 : 0);
    Put32(data.subspan(20), static_cast<std::uint32_t>(data.size()));
    Put32(data.subspan(24), 0);
    Put32(data.subspan(28), 0);
    auto seed = PayloadSeed(data.first(kBackpressurePayloadHeader));
    for (auto& byte : data.subspan(kBackpressurePayloadHeader)) {
        byte = NextPayloadByte(seed);
    }
}

std::expected<BackpressureFrame, ToolError> CheckBackpressurePayload(std::span<const std::byte> data) noexcept {
    if (data.size() < kBackpressurePayloadHeader || data.size() > 4096) {
        return Error("backpressure payload size", EPROTO);
    }
    if (Get32(data) != kPayloadMagic || Get32(data.subspan(4)) != kVersion || Get32(data.subspan(20)) != data.size() ||
        Get32(data.subspan(24)) != 0 || Get32(data.subspan(28)) != 0) {
        return Error("backpressure payload header", EPROTO);
    }
    const auto cleanup = Get32(data.subspan(16));
    BackpressureFrame frame{.round = Get32(data.subspan(8)), .index = Get32(data.subspan(12)), .cleanup = cleanup == 1};
    if (frame.round >= kMaxRounds || frame.index > raw::TxSender::kMaxBatch || cleanup > 1) {
        return Error("backpressure payload identity", EPROTO);
    }
    auto seed = PayloadSeed(data.first(kBackpressurePayloadHeader));
    for (const auto byte : data.subspan(kBackpressurePayloadHeader)) {
        if (byte != NextPayloadByte(seed)) {
            return Error("backpressure payload mismatch", EILSEQ);
        }
    }
    return frame;
}

std::expected<void, ToolError> WriteBackpressureReport(ControlChannel& channel, const BackpressureReport& report,
                                                       std::uint32_t timeout_ms, bool rnr_without_drain) noexcept {
    if (!ValidReport(report, rnr_without_drain)) {
        return Error("invalid backpressure terminal report", EINVAL);
    }
    std::array<std::byte, 52> wire{};
    Put32(wire, report.accepted);
    Put32(std::span(wire).subspan(4), report.retired);
    Put32(std::span(wire).subspan(8), report.success);
    Put32(std::span(wire).subspan(12), report.flush_done);
    Put32(std::span(wire).subspan(16), report.has_success_map ? 1 : 0);
    for (std::size_t i = 0; i < report.success_map.size(); ++i) {
        Put64(std::span(wire).subspan(20 + 8 * i), report.success_map[i]);
    }
    return channel.Write(wire, timeout_ms);
}

std::expected<BackpressureReport, ToolError> ReadBackpressureReport(ControlChannel& channel, std::uint32_t timeout_ms,
                                                                    bool rnr_without_drain) noexcept {
    std::array<std::byte, 52> wire{};
    auto received = channel.Read(wire, timeout_ms);
    if (!received) {
        return std::unexpected(received.error());
    }
    const auto flags = Get32(std::span(wire).subspan(16));
    if (flags > 1) {
        return Error("invalid peer backpressure report flags", EPROTO);
    }
    BackpressureReport report{.accepted = Get32(wire),
                              .retired = Get32(std::span(wire).subspan(4)),
                              .success = Get32(std::span(wire).subspan(8)),
                              .flush_done = Get32(std::span(wire).subspan(12)),
                              .has_success_map = flags == 1};
    for (std::size_t i = 0; i < report.success_map.size(); ++i) {
        report.success_map[i] = Get64(std::span(wire).subspan(20 + 8 * i));
    }
    if (!ValidReport(report, rnr_without_drain)) {
        return Error("invalid peer backpressure terminal report", EPROTO);
    }
    return report;
}
} // namespace tools
} // namespace kbsocket
