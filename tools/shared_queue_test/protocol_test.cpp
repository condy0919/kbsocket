// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>

#include "tools/common/control_channel.hpp"
#include "tools/shared_queue_test/shared_queue_test.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
SharedQueueHello Hello(bool server = false) {
    SharedQueueHello hello;
    hello.server = server;
    hello.options = {.bytes = 64, .messages = 11, .batch = 3, .jetties = 2, .timeout_ms = 100};
    hello.endpoints[0].eid.raw[15] = 42;
    hello.endpoints[0].id = 0x01020304;
    hello.endpoints[1].eid = hello.endpoints[0].eid;
    hello.endpoints[1].id = 0x05060708;
    return hello;
}

std::expected<SharedQueueHello, ToolError> ExchangeWithPeer(const SharedQueueHello& local,
                                                            const SharedQueueHello& remote) {
    int fds[2]{};
    const int status = ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds);
    EXPECT_EQ(status, 0);
    if (status != 0) {
        return std::unexpected(ToolError{"test socketpair", errno});
    }
    ControlChannel channel(fds[0]), peer(fds[1]);
    auto written = peer.Write(EncodeSharedHello(remote), 100);
    if (!written) {
        return std::unexpected(written.error());
    }
    return ExchangeSharedHello(channel, local);
}

TEST(SharedQueueProtocol, OptionsBoundBothPayloadAndSharedQueueResources) {
    EXPECT_TRUE(ValidateSharedOptions({}));
    EXPECT_TRUE(ValidateSharedOptions({.bytes = 32, .messages = 1, .batch = 1, .jetties = 1, .timeout_ms = 1}));
    EXPECT_TRUE(
        ValidateSharedOptions({.bytes = 4096, .messages = 16, .batch = 256, .jetties = 16, .timeout_ms = 3600000}));
    // 不允许绕过 4 KiB 接收槽或 256 WR 批次上限；每个 source 至少轮转所有 target 一次。
    EXPECT_FALSE(ValidateSharedOptions({.bytes = 31}));
    EXPECT_FALSE(ValidateSharedOptions({.bytes = 4097}));
    EXPECT_FALSE(ValidateSharedOptions({.messages = 0}));
    EXPECT_FALSE(ValidateSharedOptions({.messages = 1, .jetties = 2}));
    EXPECT_FALSE(ValidateSharedOptions({.batch = 0}));
    EXPECT_FALSE(ValidateSharedOptions({.batch = 257}));
    EXPECT_FALSE(ValidateSharedOptions({.jetties = 0}));
    EXPECT_FALSE(ValidateSharedOptions({.jetties = 17}));
    EXPECT_FALSE(ValidateSharedOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidateSharedOptions({.timeout_ms = 3600001}));
}

TEST(SharedQueueProtocol, HelloUsesFixedNetworkOrderAndDoesNotNegotiateTimeout) {
    auto hello = Hello(true);
    const auto wire = EncodeSharedHello(hello);
    EXPECT_EQ(wire.size(), 352u);
    EXPECT_EQ(wire[0], std::byte{'K'});
    EXPECT_EQ(wire[3], std::byte{'Q'});
    EXPECT_EQ(wire[11], std::byte{1});
    EXPECT_EQ(wire[15], std::byte{2});
    EXPECT_EQ(wire[47], std::byte{42});
    EXPECT_EQ(wire[48], std::byte{1});
    EXPECT_EQ(wire[51], std::byte{4});
    EXPECT_EQ(wire[68], std::byte{5});
    EXPECT_EQ(wire[71], std::byte{8});
    auto decoded = DecodeSharedHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->options.bytes, hello.options.bytes);
    EXPECT_EQ(decoded->options.messages, hello.options.messages);
    EXPECT_EQ(decoded->options.jetties, hello.options.jetties);
    EXPECT_EQ(decoded->options.batch, hello.options.batch);
    EXPECT_EQ(decoded->endpoints[1].id, hello.endpoints[1].id);
    EXPECT_EQ(decoded->endpoints[1].eid.raw[15], 42);
    hello.options.timeout_ms = 12345;
    EXPECT_EQ(EncodeSharedHello(hello), wire);
}

TEST(SharedQueueProtocol, HelloRejectsWrongToolVersionReservedBitsAndDuplicateEndpoints) {
    const auto valid = EncodeSharedHello(Hello());
    // 每次只破坏一个字段，防止新工具误接旧 SEND 工具或静默接受协议扩展。
    for (const auto offset : {3u, 7u, 11u, 15u, 19u, 23u, 27u, 31u, 72u}) {
        auto wire = valid;
        wire[offset] = std::byte{255};
        if (offset == 19u) {
            wire[18] = std::byte{16}; // bytes=4351，超过单个接收槽。
        }
        if (offset == 23u) {
            wire[offset] = std::byte{0};
        }
        if (offset == 27u) {
            wire[26] = std::byte{1}; // batch=511，低字节 255 本身仍合法。
        }
        EXPECT_FALSE(DecodeSharedHello(wire)) << "offset=" << offset;
    }
    auto duplicate = Hello();
    duplicate.endpoints[1] = duplicate.endpoints[0];
    EXPECT_FALSE(DecodeSharedHello(EncodeSharedHello(duplicate)));
    // 同一 context 下所有 jetty 共享 EID；仅 jetty ID 不同不是重复端点。
    EXPECT_TRUE(DecodeSharedHello(valid));
}

TEST(SharedQueueProtocol, ExchangeRequiresOppositeRoleAndIdenticalDataParameters) {
    const auto local = Hello();
    auto remote = Hello(true);
    remote.options.timeout_ms = 999;
    EXPECT_TRUE(ExchangeWithPeer(local, remote));
    remote.server = false;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    remote.options.bytes = 128;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.messages;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.batch;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    remote.options.jetties = 1;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
}

TEST(SharedQueueProtocol, ImmediatePatternsExerciseEveryBitIncludingZeroAndHighBit) {
    constexpr std::array<std::uint64_t, 8> kExpected{0,
                                                     std::numeric_limits<std::uint64_t>::max(),
                                                     std::uint64_t{1} << 63,
                                                     1,
                                                     0xaaaaaaaaaaaaaaaaull,
                                                     0x5555555555555555ull,
                                                     0x0123456789abcdefull,
                                                     0xfedcba9876543210ull};
    for (std::uint32_t source = 0; source < 2; ++source) {
        for (std::uint64_t sequence = 0; sequence < 8; ++sequence) {
            EXPECT_EQ(SharedImmediate({.source = source, .sequence = sequence}), kExpected[(sequence + source) % 8]);
        }
    }
}

TEST(SharedQueueProtocol, PayloadCarriesRouteDirectionFullSequenceAndLength) {
    std::array<std::byte, 4096> storage{};
    const SharedFrame frame{.source = 3, .target = 1, .sequence = 0xfedcba9876543210ull, .server = true};
    // 最小报文只有测试头；最大报文占满一个 RX 槽，仍不增加额外的片段头。
    for (const auto bytes : {32u, 64u, 4096u}) {
        auto data = std::span(storage).first(bytes);
        FillSharedPayload(data, frame);
        auto decoded = CheckSharedPayload(data);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->source, frame.source);
        EXPECT_EQ(decoded->target, frame.target);
        EXPECT_EQ(decoded->sequence, frame.sequence);
        EXPECT_EQ(decoded->server, frame.server);
        EXPECT_EQ(data[16], std::byte{0xfe});
        EXPECT_EQ(data[23], std::byte{0x10});
    }
    EXPECT_FALSE(CheckSharedPayload(std::span(storage).first(31)));
}

TEST(SharedQueueProtocol, PayloadRejectsHeaderBodyAndRouteCorruption) {
    std::array<std::byte, 64> original{};
    FillSharedPayload(original, {.source = 1, .target = 0, .sequence = 8});
    for (const auto offset : {0u, 7u, 11u, 15u, 20u, 27u, 31u, 40u, 63u}) {
        auto corrupted = original;
        corrupted[offset] ^= std::byte{1};
        EXPECT_FALSE(CheckSharedPayload(corrupted)) << "offset=" << offset;
    }
    // 即便正文按非法字段重新生成，头部本身的角色和路由范围也必须校验。
    auto invalid = original;
    FillSharedPayload(invalid, {.source = kSharedMaxJetties});
    EXPECT_FALSE(CheckSharedPayload(invalid));
    FillSharedPayload(invalid, {.target = kSharedMaxJetties});
    EXPECT_FALSE(CheckSharedPayload(invalid));
    invalid = original;
    invalid[27] = std::byte{2};
    EXPECT_FALSE(CheckSharedPayload(invalid));
    EXPECT_FALSE(CheckSharedPayload(std::span(original).first(63)));
}

TEST(SharedQueueProtocol, CleanupConfirmationRequiresSpecificPeerMarker) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    // 本端关闭成功还不够；对端必须发出专用关闭确认，普通轮次同步不能代替它。
    ASSERT_TRUE(peer.SendMarker(1, 100));
    EXPECT_FALSE(ConfirmSharedClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x53435143, 100));
    ASSERT_TRUE(peer.SendMarker(0x53435143, 100));
    EXPECT_TRUE(ConfirmSharedClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x53435143, 100));
    auto timeout = ConfirmSharedClosed(local, 1);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ETIMEDOUT);
}
} // namespace
} // namespace tools
} // namespace kbsocket
