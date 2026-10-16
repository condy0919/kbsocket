// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "tools/common/control_channel.hpp"
#include "tools/sq_isolation_test/sq_isolation_test.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
SqIsolationHello Hello(bool server = false) {
    SqIsolationHello hello;
    hello.server = server;
    hello.options = {.bytes = 64, .batch = 3, .rounds = 7, .timeout_ms = 100};
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        hello.endpoints[i].eid.raw[15] = 42;
        hello.endpoints[i].id = 0x01020304 + static_cast<std::uint32_t>(i);
    }
    return hello;
}

std::expected<SqIsolationHello, ToolError> ExchangeWithPeer(const SqIsolationHello& local,
                                                            const SqIsolationHello& remote) {
    int fds[2]{};
    const int status = ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds);
    EXPECT_EQ(status, 0);
    if (status != 0) {
        return std::unexpected(ToolError{"test socketpair", errno});
    }
    ControlChannel channel(fds[0]), peer(fds[1]);
    auto written = peer.Write(EncodeIsolationHello(remote), 100);
    if (!written) {
        return std::unexpected(written.error());
    }
    return ExchangeIsolationHello(channel, local);
}

TEST(SqIsolationProtocol, OptionsBoundPayloadBatchAndRoundCount) {
    EXPECT_TRUE(ValidateIsolationOptions({}));
    EXPECT_TRUE(ValidateIsolationOptions({.bytes = 32, .batch = 1, .rounds = 1, .timeout_ms = 1}));
    EXPECT_TRUE(ValidateIsolationOptions({.bytes = 4096, .batch = 256, .rounds = 1000000, .timeout_ms = 3600000}));
    // 数据上限必须同时符合 4 KiB 接收槽和固定 WR 数组；零轮次会漏掉故障后的健康流量。
    EXPECT_FALSE(ValidateIsolationOptions({.bytes = 31}));
    EXPECT_FALSE(ValidateIsolationOptions({.bytes = 4097}));
    EXPECT_FALSE(ValidateIsolationOptions({.batch = 0}));
    EXPECT_FALSE(ValidateIsolationOptions({.batch = 257}));
    EXPECT_FALSE(ValidateIsolationOptions({.rounds = 0}));
    EXPECT_FALSE(ValidateIsolationOptions({.rounds = 1000001}));
    EXPECT_FALSE(ValidateIsolationOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidateIsolationOptions({.timeout_ms = 3600001}));
}

TEST(SqIsolationProtocol, HelloUsesNetworkOrderAndCarriesDistinctSinkEndpoint) {
    auto hello = Hello(true);
    const auto wire = EncodeIsolationHello(hello);
    EXPECT_EQ(wire.size(), 92u);
    EXPECT_EQ(wire[0], std::byte{'K'});
    EXPECT_EQ(wire[3], std::byte{'I'});
    EXPECT_EQ(wire[11], std::byte{1});
    EXPECT_EQ(wire[15], std::byte{64});
    EXPECT_EQ(wire[19], std::byte{3});
    EXPECT_EQ(wire[23], std::byte{7});
    EXPECT_EQ(wire[47], std::byte{42});
    EXPECT_EQ(wire[48], std::byte{1});
    EXPECT_EQ(wire[51], std::byte{4});
    EXPECT_EQ(wire[71], std::byte{5});
    EXPECT_EQ(wire[91], std::byte{6});
    auto decoded = DecodeIsolationHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->options.bytes, hello.options.bytes);
    EXPECT_EQ(decoded->options.batch, hello.options.batch);
    EXPECT_EQ(decoded->options.rounds, hello.options.rounds);
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        EXPECT_EQ(decoded->endpoints[i].id, hello.endpoints[i].id);
        EXPECT_EQ(decoded->endpoints[i].eid.raw[15], 42);
    }
    // 两端可自行决定等待时长，不应因慢机器的超时设置不同拒绝互通。
    hello.options.timeout_ms = 12345;
    EXPECT_EQ(EncodeIsolationHello(hello), wire);
}

TEST(SqIsolationProtocol, HelloRejectsOtherToolsInvalidFlagsAndAliasedSink) {
    const auto valid = EncodeIsolationHello(Hello());
    // 分别破坏 magic、版本、角色及预留位，确保不能误接其他工具或接受未知扩展。
    for (const auto offset : {3u, 7u, 11u, 27u, 31u}) {
        auto wire = valid;
        wire[offset] = std::byte{255};
        EXPECT_FALSE(DecodeIsolationHello(wire)) << "offset=" << offset;
    }
    auto invalid = Hello();
    invalid.options.bytes = 4097;
    EXPECT_FALSE(DecodeIsolationHello(EncodeIsolationHello(invalid)));
    invalid = Hello();
    invalid.options.batch = 257;
    EXPECT_FALSE(DecodeIsolationHello(EncodeIsolationHello(invalid)));
    invalid = Hello();
    invalid.options.rounds = 0;
    EXPECT_FALSE(DecodeIsolationHello(EncodeIsolationHello(invalid)));
    invalid = Hello();
    invalid.options.rounds = 1000001;
    EXPECT_FALSE(DecodeIsolationHello(EncodeIsolationHello(invalid)));
    // sink 必须是独立端点；只比较数字 jetty ID 又会误拒绝不同 EID 上的合法端点。
    auto aliased = Hello();
    aliased.endpoints[2] = aliased.endpoints[1];
    EXPECT_FALSE(DecodeIsolationHello(EncodeIsolationHello(aliased)));
    ++aliased.endpoints[2].eid.raw[15];
    EXPECT_TRUE(DecodeIsolationHello(EncodeIsolationHello(aliased)));
}

TEST(SqIsolationProtocol, ExchangeRequiresOppositeRolesAndMatchingTrafficParameters) {
    const auto local = Hello();
    auto remote = Hello(true);
    remote.options.timeout_ms = 999;
    EXPECT_TRUE(ExchangeWithPeer(local, remote));
    remote.server = false;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.bytes;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.batch;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.rounds;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    auto invalid_local = local;
    invalid_local.options.rounds = 0;
    EXPECT_FALSE(ExchangeWithPeer(invalid_local, Hello(true)));
    invalid_local = local;
    invalid_local.endpoints[2] = invalid_local.endpoints[0];
    EXPECT_FALSE(ExchangeWithPeer(invalid_local, Hello(true)));
}

TEST(SqIsolationProtocol, ImmediateDistinguishesEveryIdentityFieldAndKeepsHighBits) {
    constexpr std::array<IsolationFrame, 6> kFrames{
        IsolationFrame{},
        IsolationFrame{.phase = IsolationPhase::kAfter},
        IsolationFrame{.source = 1},
        IsolationFrame{.round = 999999},
        IsolationFrame{.index = 255},
        IsolationFrame{.server = true},
    };
    EXPECT_EQ(IsolationImmediate({}), 0xa5c39e271b4068dfull);
    for (std::size_t i = 0; i < kFrames.size(); ++i) {
        const auto immediate = IsolationImmediate(kFrames[i]);
        EXPECT_EQ(immediate >> 48, 0xa5c3u);
        for (std::size_t j = 0; j < i; ++j) {
            EXPECT_NE(immediate, IsolationImmediate(kFrames[j]));
        }
    }
}

TEST(SqIsolationProtocol, PayloadCarriesPhaseSourceRoundIndexDirectionAndLength) {
    std::array<std::byte, 4096> storage{};
    const IsolationFrame frame{
        .phase = IsolationPhase::kDuring, .source = 1, .round = 999999, .index = 255, .server = true};
    // 32 字节仅测试头；33 字节覆盖非对齐正文；4096 字节覆盖完整接收槽。
    for (const auto bytes : {32u, 33u, 4096u}) {
        auto data = std::span(storage).first(bytes);
        FillIsolationPayload(data, frame);
        auto decoded = CheckIsolationPayload(data);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->phase, frame.phase);
        EXPECT_EQ(decoded->source, frame.source);
        EXPECT_EQ(decoded->round, frame.round);
        EXPECT_EQ(decoded->index, frame.index);
        EXPECT_EQ(decoded->server, frame.server);
        EXPECT_EQ(data[17], std::byte{0x0f});
        EXPECT_EQ(data[18], std::byte{0x42});
        EXPECT_EQ(data[19], std::byte{0x3f});
        EXPECT_EQ(data[23], std::byte{255});
    }
    EXPECT_FALSE(CheckIsolationPayload(std::span(storage).first(31)));
}

TEST(SqIsolationProtocol, PayloadRejectsDamagedDataAndOutOfRangeIdentity) {
    std::array<std::byte, 64> original{};
    FillIsolationPayload(original, {.phase = IsolationPhase::kDuring, .source = 1, .round = 7, .index = 2});
    for (const auto offset : {0u, 7u, 11u, 15u, 19u, 23u, 27u, 31u, 32u, 63u}) {
        auto corrupted = original;
        corrupted[offset] ^= std::byte{1};
        EXPECT_FALSE(CheckIsolationPayload(corrupted)) << "offset=" << offset;
    }
    // 合法正文也不能掩盖非法身份；这些范围检查还保护 IMM 压位互不重叠。
    auto invalid = original;
    FillIsolationPayload(invalid, {.phase = static_cast<IsolationPhase>(3)});
    EXPECT_FALSE(CheckIsolationPayload(invalid));
    FillIsolationPayload(invalid, {.source = 2});
    EXPECT_FALSE(CheckIsolationPayload(invalid));
    FillIsolationPayload(invalid, {.round = 1000000});
    EXPECT_FALSE(CheckIsolationPayload(invalid));
    FillIsolationPayload(invalid, {.index = 256});
    EXPECT_FALSE(CheckIsolationPayload(invalid));
    invalid = original;
    invalid[27] = std::byte{2};
    EXPECT_FALSE(CheckIsolationPayload(invalid));
    EXPECT_FALSE(CheckIsolationPayload(std::span(original).first(63)));
    std::array<std::byte, 4097> oversized{};
    FillIsolationPayload(oversized, {});
    EXPECT_FALSE(CheckIsolationPayload(oversized));
}

TEST(SqIsolationProtocol, BarrierRequiresMatchingPhaseAndDedicatedCloseConfirmation) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.SendMarker(7, 100));
    EXPECT_TRUE(IsolationBarrier(local, 7, 100));
    ASSERT_TRUE(peer.ExpectMarker(7, 100));
    ASSERT_TRUE(peer.SendMarker(8, 100));
    EXPECT_FALSE(IsolationBarrier(local, 7, 100));
    ASSERT_TRUE(peer.ExpectMarker(7, 100));
    // 普通轮次标记不能证明对端已关闭，只有专用关闭握手才允许入口打印 PASS。
    ASSERT_TRUE(peer.SendMarker(7, 100));
    EXPECT_FALSE(ConfirmIsolationClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x5349434c, 100));
    ASSERT_TRUE(peer.SendMarker(0x5349434c, 100));
    EXPECT_TRUE(ConfirmIsolationClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x5349434c, 100));
    auto timeout = ConfirmIsolationClosed(local, 1);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ETIMEDOUT);
}
} // namespace
} // namespace tools
} // namespace kbsocket
