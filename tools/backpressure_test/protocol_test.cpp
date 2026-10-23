// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>

#include "tools/backpressure_test/backpressure_test.hpp"
#include "tools/common/control_channel.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
BackpressureHello Hello(bool server = false) {
    BackpressureHello hello;
    hello.server = server;
    hello.options = {.bytes = 64, .tx_depth = 3, .rounds = 7, .timeout_ms = 100};
    for (std::size_t i = 0; i < hello.endpoints.size(); ++i) {
        hello.endpoints[i].eid.raw[15] = 42;
        hello.endpoints[i].id = 0x01020304 + static_cast<std::uint32_t>(i);
    }
    return hello;
}

std::expected<BackpressureHello, ToolError> ExchangeWithPeer(const BackpressureHello& local,
                                                             const BackpressureHello& remote) {
    int fds[2]{};
    const int status = ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds);
    EXPECT_EQ(status, 0);
    if (status != 0) {
        return std::unexpected(ToolError{"test socketpair", errno});
    }
    ControlChannel channel(fds[0]), peer(fds[1]);
    auto written = peer.Write(EncodeBackpressureHello(remote), 100);
    if (!written) {
        return std::unexpected(written.error());
    }
    return ExchangeBackpressureHello(channel, local);
}

TEST(BackpressureProtocol, OptionsBoundDepthRoundsAndDelayWithoutOverflow) {
    EXPECT_TRUE(ValidateBackpressureOptions({}));
    EXPECT_TRUE(ValidateBackpressureOptions({.bytes = 32, .tx_depth = 1, .rounds = 1, .timeout_ms = 1}));
    EXPECT_TRUE(ValidateBackpressureOptions({.tx_depth = 256, .rounds = 1000000, .timeout_ms = 3600000}));
    // 固定 WR 数组和 4 KiB RX 槽同时限制参数；超时乘法必须扩大后再计算。
    EXPECT_FALSE(ValidateBackpressureOptions({.bytes = 31}));
    EXPECT_FALSE(ValidateBackpressureOptions({.bytes = 4097}));
    EXPECT_FALSE(ValidateBackpressureOptions({.tx_depth = 0}));
    EXPECT_FALSE(ValidateBackpressureOptions({.tx_depth = 257}));
    EXPECT_FALSE(ValidateBackpressureOptions({.rounds = 0}));
    EXPECT_FALSE(ValidateBackpressureOptions({.rounds = 1000001}));
    EXPECT_FALSE(ValidateBackpressureOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidateBackpressureOptions({.timeout_ms = 3600001}));
    EXPECT_FALSE(ValidateBackpressureOptions({.mode = static_cast<BackpressureMode>(3)}));
    EXPECT_FALSE(ValidateBackpressureOptions({.expect = static_cast<BackpressureExpectation>(3)}));
    EXPECT_FALSE(ValidateBackpressureOptions({.expect = BackpressureExpectation::kSuccess}));
    BackpressureOptions delayed{.mode = BackpressureMode::kRxDelay, .rx_delay_us = 999, .timeout_ms = 1};
    EXPECT_TRUE(ValidateBackpressureOptions(delayed));
    delayed.rx_delay_us = 1000;
    EXPECT_FALSE(ValidateBackpressureOptions(delayed));
    delayed.timeout_ms = 3600000;
    delayed.rx_delay_us = 3599999999u;
    EXPECT_TRUE(ValidateBackpressureOptions(delayed));
    delayed.rx_delay_us = std::numeric_limits<std::uint32_t>::max();
    EXPECT_FALSE(ValidateBackpressureOptions(delayed));
}

TEST(BackpressureProtocol, RnrContinuationSelectsStrictScenarioWithoutDelayOrRoundMatching) {
    auto local = Hello();
    auto remote = Hello(true);
    local.options.mode = remote.options.mode = BackpressureMode::kRnrContinue;
    local.endpoints[1] = remote.endpoints[1] = {};
    // 此模式只有一条 SQ：先确认 RNR，再补投 RX；延迟与轮数不参与阶段同步。
    remote.options.rounds = 1;
    remote.options.rx_delay_us = std::numeric_limits<std::uint32_t>::max();
    EXPECT_TRUE(ValidateBackpressureOptions(remote.options));
    auto exchanged = ExchangeWithPeer(local, remote);
    ASSERT_TRUE(exchanged);
    EXPECT_EQ(exchanged->options.mode, BackpressureMode::kRnrContinue);
    const auto encoded = EncodeBackpressureHello(local);
    EXPECT_EQ(encoded[15], std::byte{2});
    auto decoded = DecodeBackpressureHello(encoded);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->options.mode, BackpressureMode::kRnrContinue);

    // 必须观察“先 RNR、后成功”，不能把只有错误也视作通过，因此不接受 rx_delay 的宽松期望。
    for (const auto expect : {BackpressureExpectation::kSuccess, BackpressureExpectation::kError}) {
        auto invalid = local.options;
        invalid.expect = expect;
        EXPECT_FALSE(ValidateBackpressureOptions(invalid));
    }
    remote.options.mode = BackpressureMode::kTxQuota;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote.options.mode = BackpressureMode::kRnrContinue;
    ++remote.options.tx_depth;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
}

TEST(BackpressureProtocol, HelloUsesNetworkOrderAndOmitsLocalTimeout) {
    auto hello = Hello(true);
    const auto wire = EncodeBackpressureHello(hello);
    EXPECT_EQ(wire.size(), 88u);
    EXPECT_EQ(wire[0], std::byte{'K'});
    EXPECT_EQ(wire[3], std::byte{'P'});
    EXPECT_EQ(wire[11], std::byte{1});
    EXPECT_EQ(wire[23], std::byte{64});
    EXPECT_EQ(wire[27], std::byte{3});
    EXPECT_EQ(wire[31], std::byte{7});
    EXPECT_EQ(wire[63], std::byte{42});
    EXPECT_EQ(wire[64], std::byte{1});
    EXPECT_EQ(wire[67], std::byte{4});
    EXPECT_EQ(wire[87], std::byte{5});
    auto decoded = DecodeBackpressureHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->options.mode, hello.options.mode);
    EXPECT_EQ(decoded->options.bytes, hello.options.bytes);
    EXPECT_EQ(decoded->options.tx_depth, hello.options.tx_depth);
    EXPECT_EQ(decoded->options.rounds, hello.options.rounds);
    EXPECT_EQ(decoded->options.rx_delay_us, hello.options.rx_delay_us);
    EXPECT_EQ(decoded->endpoints[1].id, hello.endpoints[1].id);
    EXPECT_EQ(decoded->endpoints[1].eid.raw[15], 42);
    hello.options.timeout_ms = 12345;
    EXPECT_EQ(EncodeBackpressureHello(hello), wire);
    // 大于默认超时的合法延迟仍可解码；wire 无法决定对端实际使用的本地超时。
    hello.options.mode = BackpressureMode::kRxDelay;
    hello.options.rx_delay_us = 1000000000;
    hello.options.timeout_ms = 3600000;
    EXPECT_TRUE(DecodeBackpressureHello(EncodeBackpressureHello(hello)));
}

TEST(BackpressureProtocol, HelloRejectsUnknownProtocolOptionsAndAliasedEndpoints) {
    const auto valid = EncodeBackpressureHello(Hello());
    // 不允许跨工具误连、未知枚举及非零预留位悄悄改变本次测试语义。
    for (const auto offset : {3u, 7u, 11u, 15u, 19u, 39u, 43u, 47u}) {
        auto wire = valid;
        wire[offset] = std::byte{255};
        EXPECT_FALSE(DecodeBackpressureHello(wire)) << "offset=" << offset;
    }
    auto invalid = Hello();
    invalid.options.tx_depth = 257;
    EXPECT_FALSE(DecodeBackpressureHello(EncodeBackpressureHello(invalid)));
    invalid = Hello();
    invalid.options.rounds = 0;
    EXPECT_FALSE(DecodeBackpressureHello(EncodeBackpressureHello(invalid)));
    auto aliased = Hello();
    aliased.endpoints[1] = aliased.endpoints[0];
    EXPECT_FALSE(DecodeBackpressureHello(EncodeBackpressureHello(aliased)));
    // 数字 jetty ID 可跨 EID 重复，端点身份必须同时包含 EID。
    ++aliased.endpoints[1].eid.raw[15];
    EXPECT_TRUE(DecodeBackpressureHello(EncodeBackpressureHello(aliased)));
}

TEST(BackpressureProtocol, ExchangeChecksOnlyModeRelevantParameters) {
    auto local = Hello();
    auto remote = Hello(true);
    remote.options.timeout_ms = 999;
    ++remote.options.rx_delay_us;
    EXPECT_TRUE(ExchangeWithPeer(local, remote));
    remote.server = false;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.bytes;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.tx_depth;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    ++remote.options.rounds;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote = Hello(true);
    remote.options.mode = BackpressureMode::kRxDelay;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    local.options.mode = BackpressureMode::kRxDelay;
    ++remote.options.rounds;
    EXPECT_TRUE(ExchangeWithPeer(local, remote));
    ++remote.options.rx_delay_us;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    remote.options.rx_delay_us = local.options.rx_delay_us;
    remote.options.expect = BackpressureExpectation::kError;
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
    local.options.expect = BackpressureExpectation::kError;
    EXPECT_TRUE(ExchangeWithPeer(local, remote));
    local.endpoints[1] = local.endpoints[0];
    EXPECT_FALSE(ExchangeWithPeer(local, remote));
}

TEST(BackpressureProtocol, ImmediateSeparatesProbeAndCleanupAndPreservesHighBits) {
    constexpr std::array<BackpressureFrame, 5> kFrames{
        BackpressureFrame{},
        BackpressureFrame{.round = 999999},
        BackpressureFrame{.index = 255},
        BackpressureFrame{.index = 256},
        BackpressureFrame{.cleanup = true},
    };
    EXPECT_EQ(BackpressureImmediate({}), 0xa5c39e271b4068dfull);
    for (std::size_t i = 0; i < kFrames.size(); ++i) {
        const auto immediate = BackpressureImmediate(kFrames[i]);
        EXPECT_EQ(immediate >> 48, 0xa5c3u);
        for (std::size_t j = 0; j < i; ++j) {
            EXPECT_NE(immediate, BackpressureImmediate(kFrames[j]));
        }
    }
}

TEST(BackpressureProtocol, PayloadCoversFullSlotAndAdditionalQuotaProbe) {
    std::array<std::byte, 4096> storage{};
    const BackpressureFrame frame{.round = 999999, .index = 256, .cleanup = true};
    // depth=256 时恢复发送使用 index=256；它必须与原来的 256 个在途消息区分。
    for (const auto bytes : {32u, 33u, 4096u}) {
        auto data = std::span(storage).first(bytes);
        FillBackpressurePayload(data, frame);
        auto decoded = CheckBackpressurePayload(data);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->round, frame.round);
        EXPECT_EQ(decoded->index, frame.index);
        EXPECT_EQ(decoded->cleanup, frame.cleanup);
        EXPECT_EQ(data[9], std::byte{0x0f});
        EXPECT_EQ(data[10], std::byte{0x42});
        EXPECT_EQ(data[11], std::byte{0x3f});
        EXPECT_EQ(data[14], std::byte{1});
        EXPECT_EQ(data[15], std::byte{0});
    }
}

TEST(BackpressureProtocol, PayloadRejectsCorruptionReservedBitsAndInvalidIdentity) {
    std::array<std::byte, 64> original{};
    FillBackpressurePayload(original, {.round = 7, .index = 2});
    // 覆盖每个头部字段及正文首尾；修改合法身份但不修改正文同样必须被识别。
    for (const auto offset : {0u, 7u, 11u, 15u, 19u, 23u, 27u, 31u, 32u, 63u}) {
        auto corrupted = original;
        corrupted[offset] ^= std::byte{1};
        EXPECT_FALSE(CheckBackpressurePayload(corrupted)) << "offset=" << offset;
    }
    auto invalid = original;
    FillBackpressurePayload(invalid, {.round = 1000000});
    EXPECT_FALSE(CheckBackpressurePayload(invalid));
    FillBackpressurePayload(invalid, {.index = 257});
    EXPECT_FALSE(CheckBackpressurePayload(invalid));
    invalid = original;
    invalid[19] = std::byte{2};
    EXPECT_FALSE(CheckBackpressurePayload(invalid));
    EXPECT_FALSE(CheckBackpressurePayload(std::span(original).first(31)));
    EXPECT_FALSE(CheckBackpressurePayload(std::span(original).first(63)));
    std::array<std::byte, 4097> oversized{};
    FillBackpressurePayload(oversized, {});
    EXPECT_FALSE(CheckBackpressurePayload(oversized));
}

TEST(BackpressureProtocol, TerminalReportsRequireRetirementAndMatchingDrainBoundary) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    for (const auto report : {BackpressureReport{}, BackpressureReport{3, 3, 3, 0}, BackpressureReport{3, 3, 1, 1}}) {
        ASSERT_TRUE(WriteBackpressureReport(peer, report, 100));
        auto received = ReadBackpressureReport(local, 100);
        ASSERT_TRUE(received);
        EXPECT_EQ(received->accepted, report.accepted);
        EXPECT_EQ(received->retired, report.retired);
        EXPECT_EQ(received->success, report.success);
        EXPECT_EQ(received->flush_done, report.flush_done);
    }
    // 未排空的账本、重复边界及缺失边界都不能作为关闭许可；读写两侧同时防御。
    for (const auto report :
         {BackpressureReport{3, 2, 2, 0}, BackpressureReport{3, 3, 4, 0}, BackpressureReport{3, 3, 1, 0},
          BackpressureReport{3, 3, 1, 2}, BackpressureReport{3, 3, 3, 1}}) {
        EXPECT_FALSE(WriteBackpressureReport(peer, report, 100));
        std::array<std::byte, 52> invalid{};
        invalid[3] = static_cast<std::byte>(report.accepted);
        invalid[7] = static_cast<std::byte>(report.retired);
        invalid[11] = static_cast<std::byte>(report.success);
        invalid[15] = static_cast<std::byte>(report.flush_done);
        ASSERT_TRUE(peer.Write(invalid, 100));
        auto received = ReadBackpressureReport(local, 100);
        ASSERT_FALSE(received);
        EXPECT_EQ(received.error().code, EPROTO);
    }
    // 汇总可超过单批 256 条，仍按网络字节序完整保留计数。
    ASSERT_TRUE(WriteBackpressureReport(peer, {257000000, 257000000, 257000000, 0}, 100));
    auto large = ReadBackpressureReport(local, 100);
    ASSERT_TRUE(large);
    EXPECT_EQ(large->accepted, 257000000u);
}

TEST(BackpressureProtocol, SuccessMapPreservesExactIdentitiesAndRejectsAmbiguousReports) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    // 相同 success 数量不能证明对应消息真的到达；第 255 条成功的身份必须保留到服务端。
    const BackpressureReport report{.accepted = 256,
                                    .retired = 256,
                                    .success = 1,
                                    .flush_done = 1,
                                    .has_success_map = true,
                                    .success_map = {0, 0, 0, std::uint64_t{1} << 63}};
    ASSERT_TRUE(WriteBackpressureReport(peer, report, 100));
    auto received = ReadBackpressureReport(local, 100);
    ASSERT_TRUE(received);
    EXPECT_TRUE(received->has_success_map);
    EXPECT_EQ(received->success_map, report.success_map);
    ASSERT_TRUE(WriteBackpressureReport(peer, {.has_success_map = true}, 100));
    received = ReadBackpressureReport(local, 100);
    ASSERT_TRUE(received);
    EXPECT_TRUE(received->has_success_map);
    EXPECT_EQ(received->success, 0u);

    auto invalid = report;
    invalid.has_success_map = false;
    EXPECT_FALSE(WriteBackpressureReport(peer, invalid, 100));
    invalid = report;
    invalid.success = 2;
    EXPECT_FALSE(WriteBackpressureReport(peer, invalid, 100));
    invalid = report;
    invalid.accepted = invalid.retired = 255;
    EXPECT_FALSE(WriteBackpressureReport(peer, invalid, 100));
    invalid = report;
    invalid.accepted = invalid.retired = 257;
    EXPECT_FALSE(WriteBackpressureReport(peer, invalid, 100));

    // 构造线上的非法标记、缺失 bit、越界身份及未声明的 bitmap，验证接收侧独立防御。
    std::array<std::byte, 52> valid{};
    valid[3] = valid[7] = std::byte{3};
    valid[11] = valid[15] = valid[19] = std::byte{1};
    valid[27] = std::byte{4};
    for (const auto variant : {0u, 1u, 2u, 3u}) {
        auto wire = valid;
        if (variant == 0) {
            wire[19] = std::byte{2};
        } else if (variant == 1) {
            wire[27] = std::byte{0};
        } else if (variant == 2) {
            wire[27] = std::byte{8};
        } else {
            wire[19] = std::byte{0};
        }
        ASSERT_TRUE(peer.Write(wire, 100));
        received = ReadBackpressureReport(local, 100);
        ASSERT_FALSE(received) << "variant=" << variant;
        EXPECT_EQ(received.error().code, EPROTO);
    }
}

TEST(BackpressureProtocol, RnrReportRequiresExplicitContextAndAllAttemptsTerminalWithoutDrain) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    const BackpressureReport report{.accepted = 3, .retired = 3, .has_success_map = true};
    // 原有错误排空协议仍要求 FLUSH_ERR_DONE；只有双方明确进入 RNR 续发阶段才可省略边界。
    EXPECT_FALSE(WriteBackpressureReport(peer, report, 100));
    ASSERT_TRUE(WriteBackpressureReport(peer, report, 100, true));
    auto received = ReadBackpressureReport(local, 100, true);
    ASSERT_TRUE(received);
    EXPECT_EQ(received->accepted, 3u);
    EXPECT_EQ(received->retired, 3u);
    EXPECT_EQ(received->success, 0u);
    EXPECT_EQ(received->flush_done, 0u);
    EXPECT_TRUE(received->has_success_map);
    ASSERT_TRUE(WriteBackpressureReport(peer, report, 100, true));
    received = ReadBackpressureReport(local, 100);
    ASSERT_FALSE(received);
    EXPECT_EQ(received.error().code, EPROTO);

    // 空批次、未终结、部分成功、主动排空或丢失身份信息都不能授权服务端进入补投 RX 阶段。
    std::array<BackpressureReport, 7> invalid_reports{};
    invalid_reports.fill(report);
    invalid_reports[0].accepted = invalid_reports[0].retired = 0;
    invalid_reports[1].retired = 2;
    invalid_reports[2].success = 1;
    invalid_reports[3].flush_done = 1;
    invalid_reports[4].has_success_map = false;
    invalid_reports[5].success_map[3] = std::uint64_t{1} << 63;
    invalid_reports[6].accepted = invalid_reports[6].retired = 257;
    for (const auto& invalid : invalid_reports) {
        EXPECT_FALSE(WriteBackpressureReport(peer, invalid, 100, true));
    }
    // 从线上的合法报告逐项破坏，同步覆盖接收侧，不能只依赖发送侧的参数检查。
    std::array<std::byte, 52> valid{};
    valid[3] = valid[7] = std::byte{3};
    valid[19] = std::byte{1};
    for (const auto variant : {0u, 1u, 2u, 3u, 4u, 5u, 6u}) {
        auto wire = valid;
        switch (variant) {
        case 0:
            wire[3] = wire[7] = std::byte{0};
            break;
        case 1:
            wire[7] = std::byte{2};
            break;
        case 2:
            wire[11] = std::byte{1};
            break;
        case 3:
            wire[15] = std::byte{1};
            break;
        case 4:
            wire[19] = std::byte{0};
            break;
        case 5:
            wire[44] = std::byte{0x80};
            break;
        case 6:
            wire[2] = wire[3] = wire[6] = wire[7] = std::byte{1};
            break;
        }
        ASSERT_TRUE(peer.Write(wire, 100));
        received = ReadBackpressureReport(local, 100, true);
        ASSERT_FALSE(received) << "variant=" << variant;
        EXPECT_EQ(received.error().code, EPROTO);
    }
    // 最大批次同样可用；续发成功报告则回到原有全成功校验，不沿用允许错误的上下文。
    ASSERT_TRUE(WriteBackpressureReport(peer, {.accepted = 256, .retired = 256, .has_success_map = true}, 100, true));
    EXPECT_TRUE(ReadBackpressureReport(local, 100, true));
    BackpressureReport continuation{
        .accepted = 3, .retired = 3, .success = 3, .has_success_map = true, .success_map = {7}};
    EXPECT_FALSE(WriteBackpressureReport(peer, continuation, 100, true));
    ASSERT_TRUE(WriteBackpressureReport(peer, continuation, 100));
    EXPECT_TRUE(ReadBackpressureReport(local, 100));
}

TEST(BackpressureProtocol, BarrierRequiresDedicatedPeerCloseConfirmation) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.SendMarker(7, 100));
    EXPECT_TRUE(BackpressureBarrier(local, 7, 100));
    ASSERT_TRUE(peer.ExpectMarker(7, 100));
    ASSERT_TRUE(peer.SendMarker(8, 100));
    EXPECT_FALSE(BackpressureBarrier(local, 7, 100));
    ASSERT_TRUE(peer.ExpectMarker(7, 100));
    // 数据阶段结束并不代表硬件资源已关闭，PASS 必须等专用关闭握手。
    ASSERT_TRUE(peer.SendMarker(7, 100));
    EXPECT_FALSE(ConfirmBackpressureClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x4250434c, 100));
    ASSERT_TRUE(peer.SendMarker(0x4250434c, 100));
    EXPECT_TRUE(ConfirmBackpressureClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x4250434c, 100));
    auto timeout = ConfirmBackpressureClosed(local, 1);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ETIMEDOUT);
}
} // namespace
} // namespace tools
} // namespace kbsocket
