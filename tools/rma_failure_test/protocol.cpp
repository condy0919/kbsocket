// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <cstring>

#include "tools/rma_failure_test/rma_failure_test.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kMagic = 0x4b52464c;
constexpr std::uint32_t kVersion = 1;
void Put(RmaFailureHelloBytes& wire, std::size_t offset, std::uint32_t value) noexcept {
    for (std::size_t i = 0; i < 4; ++i) {
        wire[offset + i] = static_cast<std::byte>(value >> (24 - 8 * i));
    }
}
std::uint32_t Get(const RmaFailureHelloBytes& wire, std::size_t offset) noexcept {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | std::to_integer<std::uint32_t>(wire[offset + i]);
    }
    return value;
}
} // namespace

std::expected<void, ToolError> ValidateRmaFailureOptions(const RmaFailureOptions& options) noexcept {
    if (!options.bytes || options.bytes > 8 * 1024 * 1024 || !options.batch || options.batch > 128 || !options.rounds ||
        options.rounds > 256 || options.timeout_ms < 100 || options.timeout_ms > 120000 ||
        2ULL * options.bytes * options.batch > 256ULL * 1024 * 1024) {
        return std::unexpected(ToolError{"invalid READ failure probe options", EINVAL});
    }
    return {};
}

std::expected<void, ToolError> MatchRmaFailureEndpoint(const RmaHello& hello, const RmaFailureOptions& options,
                                                       bool server) noexcept {
    auto valid = ValidateRmaHello(hello);
    if (!valid) {
        return valid;
    }
    if (hello.server != server || hello.options.operation != RmaOperation::kRead ||
        hello.options.bytes != options.bytes || hello.options.window != options.batch ||
        hello.options.rounds != options.rounds || hello.options.mixed_send) {
        return std::unexpected(ToolError{"READ failure endpoint role/options mismatch", EPROTO});
    }
    return {};
}

RmaFailureHelloBytes EncodeRmaFailureHello(const RmaFailureHello& hello) noexcept {
    RmaFailureHelloBytes wire{};
    Put(wire, 0, kMagic);
    Put(wire, 4, kVersion);
    Put(wire, 8, hello.server);
    Put(wire, 12, hello.options.bytes);
    Put(wire, 16, hello.options.batch);
    Put(wire, 20, hello.options.rounds);
    for (std::size_t i = 0; i < (hello.server ? 2U : 1U); ++i) {
        const auto endpoint = EncodeRmaHello(hello.endpoints[i]);
        std::copy(endpoint.begin(), endpoint.end(), wire.begin() + 32 + i * endpoint.size());
    }
    return wire;
}

std::expected<RmaFailureHello, ToolError> DecodeRmaFailureHello(const RmaFailureHelloBytes& wire) noexcept {
    if (Get(wire, 0) != kMagic || Get(wire, 4) != kVersion || Get(wire, 8) > 1 || Get(wire, 24) || Get(wire, 28)) {
        return std::unexpected(ToolError{"invalid READ failure hello header", EPROTO});
    }
    RmaFailureHello hello;
    hello.server = Get(wire, 8) != 0;
    hello.options.bytes = Get(wire, 12);
    hello.options.batch = Get(wire, 16);
    hello.options.rounds = Get(wire, 20);
    auto valid = ValidateRmaFailureOptions(hello.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    for (std::size_t i = 0; i < (hello.server ? 2U : 1U); ++i) {
        RmaHelloBytes endpoint{};
        std::copy_n(wire.begin() + 32 + i * endpoint.size(), endpoint.size(), endpoint.begin());
        auto decoded = DecodeRmaHello(endpoint);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        auto matched = MatchRmaFailureEndpoint(*decoded, hello.options, hello.server);
        if (!matched) {
            return std::unexpected(matched.error());
        }
        hello.endpoints[i] = *decoded;
    }
    if (!hello.server) {
        if (std::any_of(wire.begin() + 144, wire.end(), [](auto byte) { return byte != std::byte{}; })) {
            return std::unexpected(ToolError{"unexpected second client endpoint", EPROTO});
        }
    } else if (hello.endpoints[0].endpoint.id == hello.endpoints[1].endpoint.id &&
               std::memcmp(hello.endpoints[0].endpoint.eid.raw, hello.endpoints[1].endpoint.eid.raw, URMA_EID_SIZE) ==
                   0) {
        return std::unexpected(ToolError{"healthy and fault READ targets must differ", EPROTO});
    }
    return hello;
}
} // namespace tools
} // namespace kbsocket
