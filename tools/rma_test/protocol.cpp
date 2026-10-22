// SPDX-License-Identifier: MulanPSL-2.0
#include <arpa/inet.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>

#include "tools/rma_test/rma_test.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kMagic = 0x4b42524d;
constexpr std::uint32_t kVersion = 2;
void Put32(RmaHelloBytes& wire, std::size_t offset, std::uint32_t value) {
    value = ::htonl(value);
    std::memcpy(wire.data() + offset, &value, 4);
}
std::uint32_t Get32(const RmaHelloBytes& wire, std::size_t offset) {
    std::uint32_t value;
    std::memcpy(&value, wire.data() + offset, 4);
    return ::ntohl(value);
}
void Put64(RmaHelloBytes& wire, std::size_t offset, std::uint64_t value) {
    Put32(wire, offset, static_cast<std::uint32_t>(value >> 32));
    Put32(wire, offset + 4, static_cast<std::uint32_t>(value));
}
std::uint64_t Get64(const RmaHelloBytes& wire, std::size_t offset) {
    return (static_cast<std::uint64_t>(Get32(wire, offset)) << 32) | Get32(wire, offset + 4);
}
auto Error(const char* operation, int code = EINVAL) {
    return std::unexpected(ToolError{operation, code});
}
} // namespace

std::expected<void, ToolError> ValidateRmaOptions(const RmaOptions& options) noexcept {
    if ((options.operation != RmaOperation::kRead && options.operation != RmaOperation::kWrite) || !options.bytes ||
        options.bytes > 64U * 1024 * 1024 || !options.rounds || options.rounds > 10000 || !options.timeout_ms ||
        options.timeout_ms > 3600000) {
        return Error("require read/write, bytes=1..64MiB, rounds=1..10000, timeout_ms=1..3600000");
    }
    if (!options.window || options.window > kRmaMaxWindow ||
        static_cast<std::uint64_t>(options.bytes) * options.window > 256ULL * 1024 * 1024) {
        return Error("require window=1..128 and bytes*window<=256MiB");
    }
    if (options.operation == RmaOperation::kWrite && (options.mixed_send || options.window != 1)) {
        return Error("write requires window=1 and mixed_send=false");
    }
    return {};
}

std::expected<void, ToolError> CheckRmaCapability(const RmaOptions& options, const urma_device_cap_t& cap) noexcept {
    auto valid = ValidateRmaOptions(options);
    if (!valid) {
        return valid;
    }
    const auto limit = options.operation == RmaOperation::kRead ? cap.max_read_size : cap.max_write_size;
    if (!(cap.trans_mode & URMA_TM_RM) || !cap.rm_tp_cap.bs.ctp || options.bytes > limit ||
        (options.mixed_send && cap.max_msg_size < kRmaSendBytes)) {
        return Error("requested RM_CTP operation/size exceeds advertised device capability", EOPNOTSUPP);
    }
    return {};
}

std::expected<void, ToolError> ValidateRmaHello(const RmaHello& hello) noexcept {
    auto valid = ValidateRmaOptions(hello.options);
    if (!valid) {
        return valid;
    }
    const auto& segment = hello.segment;
    if (!segment.ubva.va || !segment.len || segment.len > std::numeric_limits<std::uint64_t>::max() - segment.ubva.va ||
        hello.payload_address < segment.ubva.va || hello.payload_address - segment.ubva.va > segment.len ||
        (hello.options.window - 1) * RmaSlotStride(hello.options) + hello.options.bytes >
            segment.len - (hello.payload_address - segment.ubva.va)) {
        return Error("remote payload is outside a non-overflowing segment", EPROTO);
    }
    const auto access = hello.options.operation == RmaOperation::kRead ? URMA_ACCESS_READ : URMA_ACCESS_WRITE;
    if (segment.attr.bs.token_policy != URMA_TOKEN_NONE || hello.token.token || segment.attr.bs.has_user_info ||
        segment.attr.bs.user_iova || segment.attr.bs.dsva || segment.attr.bs.reserved ||
        !(segment.attr.bs.access & access) || (segment.attr.bs.access & URMA_ACCESS_LOCAL_ONLY) ||
        segment.token_id == URMA_SEG_TOKEN_ID_INVALID || segment.ubva.uasid >= (1U << 24)) {
        return Error("unsupported or inconsistent raw segment descriptor/token/access", EPROTO);
    }
    if (std::memcmp(segment.ubva.eid.raw, hello.endpoint.eid.raw, 16) != 0 ||
        std::all_of(std::begin(hello.endpoint.eid.raw), std::end(hello.endpoint.eid.raw),
                    [](auto value) { return value == 0; })) {
        return Error("segment and endpoint EID mismatch or empty EID", EPROTO);
    }
    return {};
}

RmaHelloBytes EncodeRmaHello(const RmaHello& hello) noexcept {
    RmaHelloBytes wire{};
    Put32(wire, 0, kMagic);
    Put32(wire, 4, kVersion);
    Put32(wire, 8, hello.server ? 1 : 0);
    Put32(wire, 12, static_cast<std::uint32_t>(hello.options.operation));
    Put32(wire, 16, hello.options.bytes);
    Put32(wire, 20, hello.options.rounds);
    std::memcpy(wire.data() + 24, hello.endpoint.eid.raw, 16);
    Put32(wire, 40, hello.endpoint.id);
    std::memcpy(wire.data() + 44, hello.segment.ubva.eid.raw, 16);
    Put32(wire, 60, hello.segment.ubva.uasid);
    Put64(wire, 64, hello.segment.ubva.va);
    Put64(wire, 72, hello.segment.len);
    Put32(wire, 80, hello.segment.attr.value);
    Put32(wire, 84, hello.segment.token_id);
    Put32(wire, 88, hello.token.token);
    Put64(wire, 96, hello.payload_address);
    Put32(wire, 104, hello.options.window);
    Put32(wire, 108, hello.options.mixed_send ? 1 : 0);
    return wire;
}

std::expected<RmaHello, ToolError> DecodeRmaHello(const RmaHelloBytes& wire) noexcept {
    if (Get32(wire, 0) != kMagic || Get32(wire, 4) != kVersion || Get32(wire, 8) > 1 || Get32(wire, 92) ||
        Get32(wire, 108) > 1) {
        return Error("invalid RMA protocol header/reserved fields", EPROTO);
    }
    RmaHello hello;
    hello.server = Get32(wire, 8) != 0;
    hello.options.operation = static_cast<RmaOperation>(Get32(wire, 12));
    hello.options.bytes = Get32(wire, 16);
    hello.options.rounds = Get32(wire, 20);
    hello.options.window = Get32(wire, 104);
    hello.options.mixed_send = Get32(wire, 108) != 0;
    std::memcpy(hello.endpoint.eid.raw, wire.data() + 24, 16);
    hello.endpoint.id = Get32(wire, 40);
    std::memcpy(hello.segment.ubva.eid.raw, wire.data() + 44, 16);
    hello.segment.ubva.uasid = Get32(wire, 60);
    hello.segment.ubva.va = Get64(wire, 64);
    hello.segment.len = Get64(wire, 72);
    hello.segment.attr.value = Get32(wire, 80);
    hello.segment.token_id = Get32(wire, 84);
    hello.token.token = Get32(wire, 88);
    hello.payload_address = Get64(wire, 96);
    auto valid = ValidateRmaHello(hello);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    return hello;
}
} // namespace tools
} // namespace kbsocket
