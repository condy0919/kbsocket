// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>
#include <thread>

#include "tools/common/control_channel.hpp"
#include "tools/sq_multiplex_test/fault_peer.hpp"
#include "tools/sq_multiplex_test/sq_multiplex_test.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
SqMultiplexHello Hello(bool server = false) {
    SqMultiplexHello hello;
    hello.server = server;
    hello.options = {.bytes = 64, .batch = 3, .rounds = 7, .timeout_ms = 100};
    hello.endpoints[0].eid.raw[15] = 42;
    hello.endpoints[0].id = 0x01020304;
    if (server) {
        hello.endpoints[1] = hello.endpoints[0];
        ++hello.endpoints[1].id;
    }
    return hello;
}

std::expected<SqMultiplexHello, ToolError> ExchangeWithPeer(const SqMultiplexHello& local,
                                                            const SqMultiplexHello& remote) {
    int fds[2]{};
    const int status = ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds);
    EXPECT_EQ(status, 0);
    if (status != 0) {
        return std::unexpected(ToolError{"test socketpair", errno});
    }
    ControlChannel channel(fds[0]), peer(fds[1]);
    auto written = peer.Write(EncodeMultiplexHello(remote), 100);
    if (!written) {
        return std::unexpected(written.error());
    }
    return ExchangeMultiplexHello(channel, local);
}

using PhaseWire = std::array<std::byte, 24>;

PhaseWire PhaseResultWire(std::uint32_t phase_id = 0, std::uint32_t flags = 3, int error_code = 0) {
    const std::array<std::uint32_t, 6> fields{0x4b424d52, 3, phase_id, flags, static_cast<std::uint32_t>(error_code),
                                              0};
    PhaseWire wire{};
    for (std::size_t i = 0; i < fields.size(); ++i) {
        for (std::size_t j = 0; j < 4; ++j) {
            wire[4 * i + j] = static_cast<std::byte>((fields[i] >> (24 - 8 * j)) & 255);
        }
    }
    return wire;
}

std::expected<MultiplexPhaseResult, ToolError>
ExchangePhaseWithWire(const MultiplexPhaseResult& local, const PhaseWire& remote, PhaseWire* outgoing = nullptr) {
    int fds[2]{};
    const int status = ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds);
    EXPECT_EQ(status, 0);
    if (status != 0) {
        return std::unexpected(ToolError{"test socketpair", errno});
    }
    ControlChannel channel(fds[0]), peer(fds[1]);
    auto written = peer.Write(remote, 100);
    if (!written) {
        return std::unexpected(written.error());
    }
    auto result = ExchangeMultiplexPhaseResult(channel, local, 100);
    if (outgoing != nullptr) {
        auto received = peer.Read(*outgoing, 100);
        EXPECT_TRUE(received);
        if (!received) {
            return std::unexpected(received.error());
        }
    }
    return result;
}

TEST(MultiplexProtocol, OptionsBoundTheCombinedMixedBatchAndReceiveSlot) {
    EXPECT_TRUE(ValidateMultiplexOptions({}));
    EXPECT_TRUE(ValidateMultiplexOptions({.bytes = 32, .batch = 1, .rounds = 1, .timeout_ms = 1}));
    EXPECT_TRUE(ValidateMultiplexOptions({.bytes = 4096, .batch = 128, .rounds = 1000, .timeout_ms = 3600000}));
    // 每个目标各 batch 条；mixed 一次提交 2*batch 条，不得超过 TxSender 的固定 256 WR 上限。
    EXPECT_FALSE(ValidateMultiplexOptions({.bytes = 31}));
    EXPECT_FALSE(ValidateMultiplexOptions({.bytes = 4097}));
    EXPECT_FALSE(ValidateMultiplexOptions({.batch = 0}));
    EXPECT_FALSE(ValidateMultiplexOptions({.batch = 129}));
    EXPECT_FALSE(ValidateMultiplexOptions({.rounds = 0}));
    EXPECT_FALSE(ValidateMultiplexOptions({.rounds = 1001}));
    EXPECT_FALSE(ValidateMultiplexOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidateMultiplexOptions({.timeout_ms = 3600001}));
}

TEST(MultiplexProtocol, HelloUsesNetworkOrderAndKeepsTimeoutLocal) {
    auto hello = Hello(true);
    const auto wire = EncodeMultiplexHello(hello);
    EXPECT_EQ(wire.size(), 72u);
    EXPECT_EQ(wire[0], std::byte{'K'});
    EXPECT_EQ(wire[3], std::byte{'X'});
    EXPECT_EQ(wire[7], std::byte{3});
    EXPECT_EQ(wire[11], std::byte{1});
    EXPECT_EQ(wire[15], std::byte{64});
    EXPECT_EQ(wire[19], std::byte{3});
    EXPECT_EQ(wire[23], std::byte{7});
    EXPECT_EQ(wire[47], std::byte{42});
    EXPECT_EQ(wire[48], std::byte{1});
    EXPECT_EQ(wire[51], std::byte{4});
    EXPECT_EQ(wire[71], std::byte{5});
    auto decoded = DecodeMultiplexHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->options.bytes, hello.options.bytes);
    EXPECT_EQ(decoded->options.batch, hello.options.batch);
    EXPECT_EQ(decoded->options.rounds, hello.options.rounds);
    EXPECT_EQ(decoded->endpoints[0].id, hello.endpoints[0].id);
    EXPECT_EQ(decoded->endpoints[1].id, hello.endpoints[1].id);
    EXPECT_EQ(decoded->endpoints[1].eid.raw[15], 42);
    hello.options.timeout_ms = 12345;
    EXPECT_EQ(EncodeMultiplexHello(hello), wire);
}

TEST(MultiplexProtocol, HelloRejectsUnknownVersionFlagsAndOptions) {
    const auto valid = EncodeMultiplexHello(Hello(true));
    // 跨工具误连、未知角色和保留位都必须先于任何导入远端端点被拒绝。
    for (const auto offset : {3u, 7u, 11u, 27u, 31u}) {
        auto wire = valid;
        wire[offset] = std::byte{255};
        EXPECT_FALSE(DecodeMultiplexHello(wire)) << "offset=" << offset;
    }
    for (const auto offset : {15u, 19u, 23u}) {
        auto wire = valid;
        wire[offset] = std::byte{0};
        EXPECT_FALSE(DecodeMultiplexHello(wire)) << "offset=" << offset;
    }
    // 阶段结果引入新报文，旧版本必须在 hello 阶段拒绝，避免后续报文错位。
    auto old_version = valid;
    old_version[7] = std::byte{1};
    EXPECT_FALSE(DecodeMultiplexHello(old_version));
    auto invalid = Hello(true);
    invalid.options.batch = 129;
    EXPECT_FALSE(DecodeMultiplexHello(EncodeMultiplexHello(invalid)));
    invalid = Hello(true);
    invalid.options.rounds = 1001;
    EXPECT_FALSE(DecodeMultiplexHello(EncodeMultiplexHello(invalid)));
}

TEST(MultiplexProtocol, HelloSeparatesTwoServerTargetsAndOneClientSq) {
    auto server = Hello(true);
    server.endpoints[1] = server.endpoints[0];
    EXPECT_FALSE(DecodeMultiplexHello(EncodeMultiplexHello(server)));
    // 数字 jetty ID 可跨 EID 复用，完整身份由 EID 和 ID 共同决定。
    ++server.endpoints[1].eid.raw[15];
    EXPECT_TRUE(DecodeMultiplexHello(EncodeMultiplexHello(server)));
    auto client = Hello();
    EXPECT_TRUE(DecodeMultiplexHello(EncodeMultiplexHello(client)));
    client.endpoints[1].id = 1;
    EXPECT_FALSE(DecodeMultiplexHello(EncodeMultiplexHello(client)));
    client.endpoints[1] = {};
    client.endpoints[1].eid.raw[0] = 1;
    EXPECT_FALSE(DecodeMultiplexHello(EncodeMultiplexHello(client)));
}

TEST(MultiplexProtocol, ExchangeMatchesRoleAndAllTrafficParameters) {
    auto local = Hello();
    auto peer = Hello(true);
    peer.options.timeout_ms = 300;
    EXPECT_TRUE(ExchangeWithPeer(local, peer));
    EXPECT_TRUE(ExchangeWithPeer(peer, local));
    peer = Hello();
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
    peer = Hello(true);
    ++peer.options.bytes;
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
    peer = Hello(true);
    ++peer.options.batch;
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
    peer = Hello(true);
    ++peer.options.rounds;
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
    // 新模式要求先做 baseline 并等待退出证明，不能与旧 RNR 控制序列混用。
    peer = Hello(true);
    peer.options.fault = MultiplexFault::kPeerExit;
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
    local.options.fault = MultiplexFault::kPeerExit;
    EXPECT_TRUE(ExchangeWithPeer(local, peer));
    // 本地配置也必须通过同一校验，不能只防御对端而发送自身非法端点。
    peer = Hello(true);
    local.endpoints[1].id = 1;
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
    local = Hello();
    local.options.timeout_ms = 0;
    EXPECT_FALSE(ExchangeWithPeer(local, peer));
}

TEST(MultiplexProtocol, BothEndpointsExchangeSuccessfulPhaseResults) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel first(fds[0]), second(fds[1]);
    const MultiplexPhaseResult local{.phase_id = 2999, .success = true, .tx_quiescent = true};
    // 两端同时交换，验证双方先写后读，无需依赖固定的客户端/服务端先后顺序。
    std::expected<MultiplexPhaseResult, ToolError> remote;
    std::jthread peer([&] { remote = ExchangeMultiplexPhaseResult(second, local, 1000); });
    const auto result = ExchangeMultiplexPhaseResult(first, local, 1000);
    peer.join();
    ASSERT_TRUE(result);
    ASSERT_TRUE(remote);
    EXPECT_EQ(result->phase_id, 2999u);
    EXPECT_TRUE(result->success);
    EXPECT_TRUE(result->tx_quiescent);
    EXPECT_EQ(result->error_code, 0);
    EXPECT_EQ(remote->phase_id, 2999u);
    EXPECT_TRUE(remote->success);
    EXPECT_TRUE(remote->tx_quiescent);
    EXPECT_EQ(remote->error_code, 0);
}

TEST(MultiplexProtocol, PhaseResultPreservesFailureCodesInBothDirections) {
    const MultiplexPhaseResult success{.phase_id = 256, .success = true, .tx_quiescent = true};
    for (const int code :
         {0, 9, -22, std::numeric_limits<std::int32_t>::min(), std::numeric_limits<std::int32_t>::max()}) {
        for (const bool quiescent : {false, true}) {
            SCOPED_TRACE(code);
            SCOPED_TRACE(quiescent);
            // CQE 状态码与负 provider 返回值必须原样传递；失败不等于 DMA 已经终结。
            PhaseWire outgoing{};
            auto received = ExchangePhaseWithWire(success, PhaseResultWire(256, quiescent ? 2 : 0, code), &outgoing);
            ASSERT_TRUE(received);
            EXPECT_FALSE(received->success);
            EXPECT_EQ(received->tx_quiescent, quiescent);
            EXPECT_EQ(received->error_code, code);
            EXPECT_EQ(outgoing, PhaseResultWire(256));

            const MultiplexPhaseResult failure{
                .phase_id = 256, .success = false, .tx_quiescent = quiescent, .error_code = code};
            received = ExchangePhaseWithWire(failure, PhaseResultWire(256), &outgoing);
            ASSERT_TRUE(received);
            EXPECT_TRUE(received->success);
            EXPECT_TRUE(received->tx_quiescent);
            EXPECT_EQ(received->error_code, 0);
            EXPECT_EQ(outgoing, PhaseResultWire(256, quiescent ? 2 : 0, code));
        }
    }
}

TEST(MultiplexProtocol, PhaseResultRejectsUnknownMagicVersionFlagsAndReservedBits) {
    const MultiplexPhaseResult local{.success = true, .tx_quiescent = true};
    const auto valid = PhaseResultWire();
    for (const auto offset : {0u, 7u, 15u, 23u}) {
        auto corrupted = valid;
        corrupted[offset] = std::byte{255};
        const auto result = ExchangePhaseWithWire(local, corrupted);
        ASSERT_FALSE(result) << "offset=" << offset;
        EXPECT_EQ(result.error().code, EPROTO);
    }
}

TEST(MultiplexProtocol, PhaseResultRejectsDifferentAndOutOfRangePhase) {
    const MultiplexPhaseResult local{.phase_id = 2999, .success = true, .tx_quiescent = true};
    // 不接受上一阶段迟到的结果，也不允许超出 1000 轮、每轮 3 个阶段的身份范围。
    for (const auto phase_id : {2998u, 3000u, 3001u, std::numeric_limits<std::uint32_t>::max()}) {
        const auto result = ExchangePhaseWithWire(local, PhaseResultWire(phase_id));
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, EPROTO);
    }
    const auto invalid_local = ExchangePhaseWithWire({.phase_id = 3002}, PhaseResultWire());
    ASSERT_FALSE(invalid_local);
    EXPECT_EQ(invalid_local.error().code, EPROTO);
}

TEST(MultiplexProtocol, BaselineResultHasDedicatedIdentityAndPreservesExitFailure) {
    // 3000 只用于 baseline/退出确认；不能被普通 mixed/before/after 阶段结果替代。
    const MultiplexPhaseResult local{.phase_id = 3000, .success = true, .tx_quiescent = true};
    auto result = ExchangePhaseWithWire(local, PhaseResultWire(3000, 2, ECHILD));
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->success);
    EXPECT_TRUE(result->tx_quiescent);
    EXPECT_EQ(result->error_code, ECHILD);
    EXPECT_FALSE(ExchangePhaseWithWire(local, PhaseResultWire(0)));
    const MultiplexPhaseResult exited{.phase_id = 3001, .success = true, .tx_quiescent = true};
    EXPECT_TRUE(ExchangePhaseWithWire(exited, PhaseResultWire(3001)));
    EXPECT_FALSE(ExchangePhaseWithWire(exited, PhaseResultWire(3000)));
}

TEST(MultiplexProtocol, FaultBaselineRequiresPayloadIdentityRouteAndImmediate) {
    std::array<std::byte, 64> data{};
    const MultiplexFrame frame{.phase = MultiplexPhase::kBaseline, .sink = true};
    FillMultiplexPayload(data, frame);
    auto decoded = CheckMultiplexPayload(data);
    ASSERT_TRUE(decoded);
    urma_jetty_id_t receiver{}, sender{};
    receiver.id = 11;
    sender.id = 22;
    sender.eid.raw[15] = 3;
    raw::RxBufferEvent event;
    event.lease.emplace();
    event.payload = data;
    event.completion.flag.bs.jetty = 1;
    event.completion.flag.bs.s_r = 1;
    event.completion.opcode = URMA_CR_OPC_SEND_WITH_IMM;
    event.completion.local_id = receiver.id;
    event.completion.remote_id = sender;
    event.completion.imm_data = MultiplexImmediate(frame);
    ASSERT_TRUE(CheckMultiplexBaseline(event, receiver, sender, data.size()));
    // 只有真正发到这个故障进程、来自当前 SQ 的正确消息才能授权 kill 后开展 status 8 实验。
    auto wrong = event;
    ++wrong.completion.remote_id.eid.raw[0];
    EXPECT_FALSE(CheckMultiplexBaseline(wrong, receiver, sender, data.size()));
    wrong = event;
    wrong.completion.imm_data ^= 1;
    EXPECT_FALSE(CheckMultiplexBaseline(wrong, receiver, sender, data.size()));
    wrong = event;
    wrong.lease.reset();
    EXPECT_FALSE(CheckMultiplexBaseline(wrong, receiver, sender, data.size()));
    wrong = event;
    wrong.completion.status = URMA_CR_REM_ACCESS_ABORT_ERR;
    EXPECT_FALSE(CheckMultiplexBaseline(wrong, receiver, sender, data.size()));
    data.back() ^= std::byte{1};
    EXPECT_FALSE(CheckMultiplexBaseline(event, receiver, sender, data.size()));
}

TEST(MultiplexProtocol, PhaseSuccessRequiresQuiescentTxAndZeroError) {
    const MultiplexPhaseResult local{.success = true, .tx_quiescent = true};
    // 成功是下一阶段可安全开始的承诺，不能与未终结 TX 或错误码同时出现。
    for (const auto wire : {PhaseResultWire(0, 1), PhaseResultWire(0, 3, 9), PhaseResultWire(0, 3, -22)}) {
        const auto result = ExchangePhaseWithWire(local, wire);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, EPROTO);
    }
    for (const auto invalid : {MultiplexPhaseResult{.success = true},
                               MultiplexPhaseResult{.success = true, .tx_quiescent = true, .error_code = 9}}) {
        const auto result = ExchangePhaseWithWire(invalid, PhaseResultWire());
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, EPROTO);
    }
}

TEST(MultiplexProtocol, PhaseResultExchangePropagatesControlTimeout) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    const auto result = ExchangeMultiplexPhaseResult(local, {.success = true, .tx_quiescent = true}, 1);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ETIMEDOUT);
    PhaseWire outgoing{};
    ASSERT_TRUE(peer.Read(outgoing, 100));
    EXPECT_EQ(outgoing, PhaseResultWire());
}

TEST(MultiplexProtocol, ImmediateKeepsTargetPhaseRoundAndIndexDistinct) {
    constexpr std::array<MultiplexFrame, 6> kFrames{
        MultiplexFrame{},
        MultiplexFrame{.phase = MultiplexPhase::kMixed},
        MultiplexFrame{.phase = MultiplexPhase::kAfter},
        MultiplexFrame{.round = 999},
        MultiplexFrame{.index = 127},
        MultiplexFrame{.phase = MultiplexPhase::kMixed, .sink = true},
    };
    for (std::size_t i = 0; i < kFrames.size(); ++i) {
        const auto immediate = MultiplexImmediate(kFrames[i]);
        EXPECT_EQ(immediate >> 48, 0xc7a5u);
        for (std::size_t j = 0; j < i; ++j) {
            EXPECT_NE(immediate, MultiplexImmediate(kFrames[j]));
        }
    }
}

TEST(MultiplexProtocol, PayloadAcceptsHeaderOnlyAndFullSlotWithoutLosingIdentity) {
    std::array<std::byte, 4096> storage{};
    const MultiplexFrame frame{.phase = MultiplexPhase::kMixed, .round = 999, .index = 127, .sink = true};
    for (const auto bytes : {32u, 33u, 4096u}) {
        auto data = std::span(storage).first(bytes);
        FillMultiplexPayload(data, frame);
        auto decoded = CheckMultiplexPayload(data);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->phase, frame.phase);
        EXPECT_EQ(decoded->round, frame.round);
        EXPECT_EQ(decoded->index, frame.index);
        EXPECT_TRUE(decoded->sink);
        EXPECT_EQ(data[14], std::byte{3});
        EXPECT_EQ(data[15], std::byte{0xe7});
        EXPECT_EQ(data[19], std::byte{127});
    }
}

TEST(MultiplexProtocol, PayloadRejectsHeaderAndBodyCorruption) {
    std::array<std::byte, 64> original{};
    FillMultiplexPayload(original, {.phase = MultiplexPhase::kMixed, .round = 7, .index = 2});
    // 正文关联全部头部：改成另一个合法 phase/round/index 也不能伪装成另一条消息。
    for (const auto offset : {0u, 7u, 11u, 15u, 19u, 23u, 27u, 31u, 32u, 63u}) {
        auto corrupted = original;
        corrupted[offset] ^= std::byte{1};
        EXPECT_FALSE(CheckMultiplexPayload(corrupted)) << "offset=" << offset;
    }
    EXPECT_FALSE(CheckMultiplexPayload(std::span(original).first(31)));
    EXPECT_FALSE(CheckMultiplexPayload(std::span(original).first(63)));
    std::array<std::byte, 4097> oversized{};
    FillMultiplexPayload(oversized, {});
    EXPECT_FALSE(CheckMultiplexPayload(oversized));
}

TEST(MultiplexProtocol, PayloadRejectsOutOfRangeOrWrongStageTargets) {
    std::array<std::byte, 64> data{};
    for (const auto frame : {MultiplexFrame{.phase = static_cast<MultiplexPhase>(4)}, MultiplexFrame{.round = 1000},
                             MultiplexFrame{.index = 128}, MultiplexFrame{.sink = true},
                             MultiplexFrame{.phase = MultiplexPhase::kAfter, .sink = true}}) {
        FillMultiplexPayload(data, frame);
        EXPECT_FALSE(CheckMultiplexPayload(data));
    }
    FillMultiplexPayload(data, {.phase = MultiplexPhase::kMixed, .sink = true});
    data[23] = std::byte{2};
    EXPECT_FALSE(CheckMultiplexPayload(data));
}

TEST(MultiplexProtocol, CloseConfirmationCannotBeReplacedByDataBarrier) {
    int fds[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.SendMarker(7, 100));
    EXPECT_TRUE(MultiplexBarrier(local, 7, 100));
    ASSERT_TRUE(peer.ExpectMarker(7, 100));
    ASSERT_TRUE(peer.SendMarker(8, 100));
    EXPECT_FALSE(MultiplexBarrier(local, 7, 100));
    ASSERT_TRUE(peer.ExpectMarker(7, 100));
    // 资源关闭与数据终结是两个独立承诺；PASS 只在专用关闭握手后出现。
    ASSERT_TRUE(peer.SendMarker(7, 100));
    EXPECT_FALSE(ConfirmMultiplexClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x4d58434c, 100));
    ASSERT_TRUE(peer.SendMarker(0x4d58434c, 100));
    EXPECT_TRUE(ConfirmMultiplexClosed(local, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x4d58434c, 100));
    auto timeout = ConfirmMultiplexClosed(local, 1);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ETIMEDOUT);
}
} // namespace
} // namespace tools
} // namespace kbsocket
