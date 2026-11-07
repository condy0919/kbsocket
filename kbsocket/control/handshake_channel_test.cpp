// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/handshake_channel.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <utility>

#include <gtest/gtest.h>

namespace kbsocket {
namespace control {
namespace {
HandshakeIdentity Identity(std::uint64_t seed = 1) {
    return {seed, seed + 1, seed + 2, seed + 3, seed + 4, {7, 300}};
}
HandshakeMessage Hello() {
    HandshakeMessage message;
    message.hello.identity = Identity();
    message.hello.credit_epoch = 99;
    message.hello.eid[15] = std::byte{31};
    message.hello.jetty_id = 432;
    message.hello.import_token = 0x01020304;
    message.hello.tx_lanes.pool_epoch = 0x0102030405060708;
    message.hello.tx_lanes.count = 1;
    message.hello.tx_lanes.lanes[0] = {message.hello.eid, 0x11223344, 0x55667788, 0x1112131415161718};
    message.hello.descriptor_bytes = 4;
    message.hello.descriptor[0] = std::byte{0xff};
    message.hello.descriptor[3] = std::byte{0xee};
    return message;
}
HandshakeMessage Ready() {
    HandshakeMessage message;
    message.kind = HandshakeMessageKind::kReady;
    message.ready.sender = Identity();
    message.ready.receiver = Identity(100);
    message.ready.credit_epoch = 99;
    message.ready.max_credit_seq = 1234;
    message.ready.max_control_credit_seq = 12;
    message.ready.max_frame_seq = 64;
    message.ready.max_stream_bytes = 256 * 1024;
    return message;
}
HandshakeMessage LaneUpdate() {
    HandshakeMessage message;
    message.kind = HandshakeMessageKind::kLaneUpdate;
    message.lane_update.sender = Identity();
    message.lane_update.receiver = Identity(100);
    message.lane_update.update_id = 0x0102030405060708;
    message.lane_update.tx_lanes = Hello().hello.tx_lanes;
    return message;
}
HandshakeMessage LaneAck() {
    HandshakeMessage message;
    message.kind = HandshakeMessageKind::kLaneAck;
    message.lane_ack.sender = Identity(100);
    message.lane_ack.receiver = Identity();
    message.lane_ack.update_id = 0x0102030405060708;
    return message;
}
HandshakeMessage CreditStop(HandshakeMessageKind kind = HandshakeMessageKind::kCreditStop) {
    HandshakeMessage message;
    message.kind = kind;
    auto& stop = kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
    stop.sender = Identity();
    stop.receiver = Identity(100);
    stop.sender_credit_epoch = 0x0102030405060708;
    stop.receiver_credit_epoch = 0x1112131415161718;
    stop.spent = {0x2122232425262728, std::numeric_limits<std::uint64_t>::max()};
    return message;
}
template <class T>
void ExpectError(const std::expected<T, HandshakeError>& result, HandshakeErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
struct OwnedFd {
    int fd = -1;
    ~OwnedFd() {
        if (fd >= 0) {
            close(fd);
        }
    }
};

TEST(HandshakeCodec, HelloUsesCanonicalBigEndianAndCopiesOnlyDescriptorLength) {
    auto message = Hello();
    message.hello.identity.runtime = 0x0102030405060708;
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes;
    bytes.fill(std::byte{0xaa});
    const auto encoded = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(encoded);
    ASSERT_EQ(*encoded, 152u);
    // 固定头与完整身份均逐字段大端编码，扩展 token 不允许截断成 8 bit generation。
    const std::array<std::byte, 12> header{std::byte{'K'}, std::byte{'B'}, std::byte{'S'}, std::byte{'H'},
                                           std::byte{0},   std::byte{3},   std::byte{1},   std::byte{0},
                                           std::byte{0},   std::byte{0},   std::byte{0},   std::byte{140}};
    EXPECT_TRUE(std::equal(header.begin(), header.end(), bytes.begin()));
    for (unsigned i = 0; i < 8; ++i) {
        EXPECT_EQ(bytes[16 + i], static_cast<std::byte>(i + 1));
    }
    // 新增列表元数据及每项物理身份也按固定大端布局编码，不依赖结构体对齐。
    for (unsigned i = 0; i < 8; ++i) {
        EXPECT_EQ(bytes[100 + i], static_cast<std::byte>(i + 1));
        EXPECT_EQ(bytes[140 + i], static_cast<std::byte>(0x11 + i));
    }
    EXPECT_EQ(bytes[111], std::byte{1});
    EXPECT_EQ(bytes[112], std::byte{0});
    EXPECT_EQ(bytes[131], std::byte{31});
    EXPECT_EQ(bytes[132], std::byte{0x11});
    EXPECT_EQ(bytes[135], std::byte{0x44});
    EXPECT_EQ(bytes[136], std::byte{0x55});
    EXPECT_EQ(bytes[139], std::byte{0x88});
    EXPECT_EQ(bytes[152], std::byte{0xaa});
    const auto decoded = DecodeHandshakeMessage(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->hello.identity, message.hello.identity);
    EXPECT_EQ(decoded->hello.eid, message.hello.eid);
    EXPECT_EQ(decoded->hello.jetty_id, 432u);
    EXPECT_EQ(decoded->hello.credit_epoch, 99u);
    EXPECT_EQ(decoded->hello.import_token, 0x01020304u);
    EXPECT_EQ(decoded->hello.tx_lanes, message.hello.tx_lanes);
    EXPECT_EQ(decoded->hello.descriptor_bytes, 4u);
    EXPECT_EQ(decoded->hello.descriptor[0], std::byte{0xff});
    EXPECT_EQ(decoded->hello.descriptor[3], std::byte{0xee});
    EXPECT_EQ(decoded->hello.descriptor[4], std::byte{0});
}

TEST(HandshakeCodec, ReadyCarriesBothCompleteIdentitiesAndCumulativeLimits) {
    auto message = Ready();
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto encoded = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(encoded);
    ASSERT_EQ(*encoded, 156u);
    const auto decoded = DecodeHandshakeMessage(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->ready.sender, message.ready.sender);
    EXPECT_EQ(decoded->ready.receiver, message.ready.receiver);
    EXPECT_EQ(decoded->ready.credit_epoch, 99u);
    EXPECT_EQ(decoded->ready.max_credit_seq, 1234u);
    EXPECT_EQ(decoded->ready.max_control_credit_seq, 12u);
    EXPECT_EQ(decoded->ready.max_frame_seq, 64u);
    EXPECT_EQ(decoded->ready.max_stream_bytes, 256u * 1024);
    // 保留字段必须为零，避免未来版本把同一帧解释成两种协议。
    bytes[112] = std::byte{1};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*encoded)), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, RejectAndTcpOnlyMessagesRoundTrip) {
    HandshakeMessage message;
    message.kind = HandshakeMessageKind::kReject;
    message.reject = {12, 34, 5};
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    auto encoded = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(encoded);
    auto decoded = DecodeHandshakeMessage(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->reject.sender_session, 12u);
    EXPECT_EQ(decoded->reject.receiver_session, 34u);
    EXPECT_EQ(decoded->reject.reason, 5u);
    message = {};
    message.hello.identity = Identity();
    message.hello.capabilities = kHandshakeCapabilityTcp;
    encoded = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, 116u);
    decoded = DecodeHandshakeMessage(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->hello.capabilities, kHandshakeCapabilityTcp);
    EXPECT_EQ(decoded->hello.descriptor_bytes, 0u);
    EXPECT_EQ(decoded->hello.tx_lanes, HandshakeTxLanes{});
    message = Ready();
    message.ready.transport = HandshakeTransport::kTcp;
    message.ready.credit_epoch = 0;
    ASSERT_TRUE(EncodeHandshakeMessage(message, bytes));
}

TEST(HandshakeCodec, RejectsInvalidIdentityCapabilitiesAndSizesWithoutOutputMutation) {
    const auto good = Hello();
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes;
    bytes.fill(std::byte{0xcc});
    const auto unchanged = bytes;
    auto bad = good;
    bad.hello.identity.token.generation = 0;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.capabilities = 4;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.descriptor_bytes = kHandshakeMaxDescriptorBytes + 1;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.descriptor_bytes = 0;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.credit_epoch = 0;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.capabilities = kHandshakeCapabilityTcp;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.wire_version = 2;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kVersionMismatch);
    ExpectError(EncodeHandshakeMessage(good, std::span(bytes).first(151)), HandshakeErrorCode::kBufferTooSmall);
    EXPECT_EQ(bytes, unchanged);
}

TEST(HandshakeCodec, RejectsMalformedHeaderBodyAndNonExactFrame) {
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto size = EncodeHandshakeMessage(Hello(), bytes);
    ASSERT_TRUE(size);
    const auto original = bytes;
    for (const std::size_t offset : {std::size_t{0}, std::size_t{7}, std::size_t{8}, std::size_t{96}}) {
        bytes = original;
        bytes[offset] = std::byte{0xff};
        ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    }
    bytes = original;
    bytes[5] = std::byte{1};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kVersionMismatch);
    bytes = original;
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(11)), HandshakeErrorCode::kInvalidFrame);
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size - 1)), HandshakeErrorCode::kInvalidFrame);
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size + 1)), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, MaximumOpaqueDescriptorFitsAndSequenceUpperBoundIsFinite) {
    auto message = Hello();
    message.hello.tx_lanes.count = kHandshakeMaxTxLanes;
    for (std::uint32_t i = 1; i < message.hello.tx_lanes.count; ++i) {
        message.hello.tx_lanes.lanes[i] = message.hello.tx_lanes.lanes[0];
        message.hello.tx_lanes.lanes[i].jetty_id += i;
    }
    message.hello.descriptor_bytes = kHandshakeMaxDescriptorBytes;
    message.hello.descriptor.back() = std::byte{0xbe};
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto size = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(size);
    EXPECT_EQ(*size, bytes.size());
    const auto decoded = DecodeHandshakeMessage(bytes);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->hello.descriptor.back(), std::byte{0xbe});
    EXPECT_EQ(decoded->hello.tx_lanes, message.hello.tx_lanes);
    message = Ready();
    message.ready.max_frame_seq = std::uint64_t{1} << 32;
    ASSERT_TRUE(EncodeHandshakeMessage(message, bytes));
    ++message.ready.max_frame_seq;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, RejectsEmptyOversizedOrUnversionedRawLaneSnapshotsWithoutOutputMutation) {
    const auto good = Hello();
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes;
    bytes.fill(std::byte{0xcc});
    const auto unchanged = bytes;
    // 数量检查先于列表访问；空 RAW 快照和未声明代次都不能建立来源许可。
    auto bad = good;
    bad.hello.tx_lanes.count = 0;
    ExpectError(ValidateHandshakeTxLanes(bad.hello.tx_lanes), HandshakeErrorCode::kInvalidFrame);
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.tx_lanes.count = kHandshakeMaxTxLanes + 1;
    ExpectError(ValidateHandshakeTxLanes(bad.hello.tx_lanes), HandshakeErrorCode::kInvalidFrame);
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.tx_lanes.pool_epoch = 0;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    bad = good;
    bad.hello.tx_lanes.lanes[0].generation = 0;
    ExpectError(EncodeHandshakeMessage(bad, bytes), HandshakeErrorCode::kInvalidFrame);
    EXPECT_EQ(bytes, unchanged);
}

TEST(HandshakeCodec, LaneIdentityRequiresUniquePhysicalSourceAndSeparatePassiveJetty) {
    auto message = Hello();
    auto& lanes = message.hello.tx_lanes;
    lanes.count = 2;
    lanes.lanes[1] = lanes.lanes[0];
    ++lanes.lanes[1].generation;
    // 即便软件 generation 不同，同一个 CQE 物理来源也不能声明两遍。
    ExpectError(ValidateHandshakeTxLanes(lanes), HandshakeErrorCode::kInvalidFrame);
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    ++lanes.lanes[1].uasid;
    ASSERT_TRUE(ValidateHandshakeTxLanes(lanes));
    ASSERT_TRUE(EncodeHandshakeMessage(message, bytes));
    lanes.lanes[1].uasid = lanes.lanes[0].uasid;
    lanes.lanes[1].eid[0] = std::byte{1};
    ASSERT_TRUE(ValidateHandshakeTxLanes(lanes));
    // 被动导入端点没有 uasid 字段，保守禁止相同 EID/id 被登记成主动来源。
    lanes.count = 1;
    lanes.lanes[0].jetty_id = message.hello.jetty_id;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, TcpOnlyHelloRejectsActiveLaneMetadata) {
    HandshakeMessage message;
    message.hello.identity = Identity();
    message.hello.capabilities = kHandshakeCapabilityTcp;
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    message.hello.tx_lanes.pool_epoch = 1;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message.hello.tx_lanes.pool_epoch = 0;
    message.hello.tx_lanes.count = 1;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, RejectsMalformedLaneMetadataBeforeReadingVariableList) {
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto size = EncodeHandshakeMessage(Hello(), bytes);
    ASSERT_TRUE(size);
    const auto original = bytes;
    // 越界数量、保留字段和非法代次必须在解码时同样拒绝。
    bytes[111] = std::byte{65};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    bytes[115] = std::byte{1};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    std::fill_n(bytes.begin() + 140, 8, std::byte{0});
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    std::fill_n(bytes.begin() + 100, 8, std::byte{0});
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    // 同时缩短帧头长度与输入，确保长度一致时也不会越界读取缺失的 Lane。
    bytes = original;
    bytes[11] = std::byte{104};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(116)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    bytes[111] = std::byte{0};
    bytes[11] = std::byte{108};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(120)), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, RejectsDuplicateDecodedLaneDespiteDistinctGeneration) {
    auto message = Hello();
    message.hello.tx_lanes.count = 2;
    message.hello.tx_lanes.lanes[1] = message.hello.tx_lanes.lanes[0];
    ++message.hello.tx_lanes.lanes[1].jetty_id;
    ++message.hello.tx_lanes.lanes[1].generation;
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto size = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(size);
    // 仅把第二项物理来源改为第一项，保留不同代次，覆盖不可信远端列表。
    std::copy_n(bytes.begin() + 116, 24, bytes.begin() + 148);
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, OnlineLaneUpdateAndAckUseCanonicalFullIdentities) {
    const auto update = LaneUpdate();
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    auto encoded = EncodeHandshakeMessage(update, bytes);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, 164u);
    EXPECT_EQ(bytes[5], std::byte{3});
    EXPECT_EQ(bytes[6], std::byte{4});
    // 两份完整身份之后才是更新序号和池代次；二者都不能被截成 32 bit。
    for (unsigned i = 0; i < 8; ++i) {
        EXPECT_EQ(bytes[108 + i], static_cast<std::byte>(i + 1));
        EXPECT_EQ(bytes[116 + i], static_cast<std::byte>(i + 1));
    }
    EXPECT_EQ(bytes[127], std::byte{1});
    EXPECT_EQ(bytes[131], std::byte{0});
    auto decoded = DecodeHandshakeMessage(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->lane_update.sender, update.lane_update.sender);
    EXPECT_EQ(decoded->lane_update.receiver, update.lane_update.receiver);
    EXPECT_EQ(decoded->lane_update.update_id, update.lane_update.update_id);
    EXPECT_TRUE(SameTxLanes(decoded->lane_update.tx_lanes, update.lane_update.tx_lanes));

    const auto ack = LaneAck();
    encoded = EncodeHandshakeMessage(ack, bytes);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, 116u);
    EXPECT_EQ(bytes[6], std::byte{5});
    decoded = DecodeHandshakeMessage(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->lane_ack.sender, ack.lane_ack.sender);
    EXPECT_EQ(decoded->lane_ack.receiver, ack.lane_ack.receiver);
    EXPECT_EQ(decoded->lane_ack.update_id, ack.lane_ack.update_id);
}

TEST(HandshakeCodec, MaximumOnlineLaneUpdateFitsAndUnencodedTailIsIrrelevant) {
    auto message = LaneUpdate();
    auto& lanes = message.lane_update.tx_lanes;
    lanes.count = kHandshakeMaxTxLanes;
    for (std::uint32_t i = 1; i < lanes.count; ++i) {
        lanes.lanes[i] = lanes.lanes[0];
        lanes.lanes[i].jetty_id += i;
    }
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto size = EncodeHandshakeMessage(message, bytes);
    ASSERT_TRUE(size);
    EXPECT_EQ(*size, 2180u);
    const auto decoded = DecodeHandshakeMessage(std::span(bytes).first(*size));
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(SameTxLanes(lanes, decoded->lane_update.tx_lanes));
    // 相等只看实际公告的前缀；尾槽残留不能迫使连接重复发布同一公告。
    lanes.count = 1;
    const auto initial = LaneUpdate().lane_update.tx_lanes;
    EXPECT_TRUE(SameTxLanes(lanes, initial));
    ++lanes.lanes[0].generation;
    EXPECT_FALSE(SameTxLanes(lanes, initial));
    auto oversized = initial;
    oversized.count = kHandshakeMaxTxLanes + 1;
    EXPECT_FALSE(SameTxLanes(oversized, oversized));
}

TEST(HandshakeCodec, RejectsInvalidOnlineIdentitiesIdsAndLanesWithoutOutputMutation) {
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes;
    bytes.fill(std::byte{0xab});
    const auto unchanged = bytes;
    auto message = LaneUpdate();
    message.lane_update.sender.runtime = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneUpdate();
    message.lane_update.receiver.token.generation = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneUpdate();
    message.lane_update.update_id = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneUpdate();
    message.lane_update.tx_lanes.count = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneUpdate();
    message.lane_update.tx_lanes.count = kHandshakeMaxTxLanes + 1;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneUpdate();
    message.lane_update.tx_lanes.count = 2;
    message.lane_update.tx_lanes.lanes[1] = message.lane_update.tx_lanes.lanes[0];
    ++message.lane_update.tx_lanes.lanes[1].generation;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneAck();
    message.lane_ack.sender.authorization = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneAck();
    message.lane_ack.receiver.session = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    message = LaneAck();
    message.lane_ack.update_id = 0;
    ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
    EXPECT_EQ(bytes, unchanged);
}

TEST(HandshakeCodec, OnlineDecodeRejectsOldVersionReservedTruncatedAndUnboundedLists) {
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes{};
    const auto size = EncodeHandshakeMessage(LaneUpdate(), bytes);
    ASSERT_TRUE(size);
    const auto original = bytes;
    // 旧实现不具备在线确认语义，因此连相同布局的 HELLO 也必须显式拒绝旧版本。
    bytes[5] = std::byte{2};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kVersionMismatch);
    bytes = original;
    bytes[131] = std::byte{1};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    bytes[127] = std::byte{65};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    std::fill_n(bytes.begin() + 108, 8, std::byte{0});
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    bytes[11] = std::byte{120};
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(132)), HandshakeErrorCode::kInvalidFrame);
    bytes = original;
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size - 1)), HandshakeErrorCode::kInvalidFrame);
    const auto ack_size = EncodeHandshakeMessage(LaneAck(), bytes);
    ASSERT_TRUE(ack_size);
    std::fill_n(bytes.begin() + 108, 8, std::byte{0});
    ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*ack_size)), HandshakeErrorCode::kInvalidFrame);
}

TEST(HandshakeCodec, CreditStopAndAckPreserveFullIdentitiesEpochsAndLargeFinalCounts) {
    for (const auto kind : {HandshakeMessageKind::kCreditStop, HandshakeMessageKind::kCreditStopAck}) {
        const auto message = CreditStop(kind);
        const auto& expected =
            kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
        std::array<std::byte, 160> bytes;
        bytes.fill(std::byte{0xab});
        const auto size = EncodeHandshakeMessage(message, bytes);
        ASSERT_TRUE(size);
        ASSERT_EQ(*size, 140u);
        EXPECT_EQ(bytes[6], static_cast<std::byte>(kind));
        EXPECT_EQ(bytes[11], std::byte{128});
        // 累计计数可超过 32 bit；逐字节检查大端格式，避免两端共享错误的宿主布局仍往返通过。
        for (unsigned i = 0; i < 8; ++i) {
            EXPECT_EQ(bytes[108 + i], static_cast<std::byte>(i + 1));
            EXPECT_EQ(bytes[116 + i], static_cast<std::byte>(0x11 + i));
            EXPECT_EQ(bytes[124 + i], static_cast<std::byte>(0x21 + i));
            EXPECT_EQ(bytes[132 + i], std::byte{0xff});
        }
        EXPECT_EQ(bytes[140], std::byte{0xab});
        const auto decoded = DecodeHandshakeMessage(std::span(bytes).first(*size));
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->kind, kind);
        const auto& actual =
            kind == HandshakeMessageKind::kCreditStop ? decoded->credit_stop : decoded->credit_stop_ack;
        EXPECT_EQ(actual, expected);
    }
}

TEST(HandshakeCodec, CreditStopAllowsZeroTrafficAndZeroTokenSlot) {
    for (const auto kind : {HandshakeMessageKind::kCreditStop, HandshakeMessageKind::kCreditStopAck}) {
        auto message = CreditStop(kind);
        auto& stop = kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
        stop.spent = {};
        stop.sender.token.slot = stop.receiver.token.slot = 0;
        std::array<std::byte, 140> bytes{};
        const auto size = EncodeHandshakeMessage(message, bytes);
        ASSERT_TRUE(size);
        const auto decoded = DecodeHandshakeMessage(bytes);
        ASSERT_TRUE(decoded);
        const auto& actual =
            kind == HandshakeMessageKind::kCreditStop ? decoded->credit_stop : decoded->credit_stop_ack;
        EXPECT_EQ(actual, stop);
    }
}

TEST(HandshakeCodec, CreditStopRejectsMissingIdentityOrEpochWithoutChangingOutput) {
    for (const auto kind : {HandshakeMessageKind::kCreditStop, HandshakeMessageKind::kCreditStopAck}) {
        std::array<std::byte, 140> bytes;
        bytes.fill(std::byte{0xab});
        const auto unchanged = bytes;
        // 每个方向的完整身份和各自授信代次均不可缺；token.slot=0 本身合法，不在此列。
        for (unsigned field = 0; field < 14; ++field) {
            auto message = CreditStop(kind);
            auto& stop = kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
            auto& id = field < 6 ? stop.sender : stop.receiver;
            switch (field < 12 ? field % 6 : field) {
            case 0:
                id.runtime = 0;
                break;
            case 1:
                id.receive_domain = 0;
                break;
            case 2:
                id.endpoint_generation = 0;
                break;
            case 3:
                id.authorization = 0;
                break;
            case 4:
                id.session = 0;
                break;
            case 5:
                id.token.generation = 0;
                break;
            case 12:
                stop.sender_credit_epoch = 0;
                break;
            case 13:
                stop.receiver_credit_epoch = 0;
                break;
            }
            ExpectError(EncodeHandshakeMessage(message, bytes), HandshakeErrorCode::kInvalidFrame);
            EXPECT_EQ(bytes, unchanged);
        }
        ExpectError(EncodeHandshakeMessage(CreditStop(kind), std::span(bytes).first(139)),
                    HandshakeErrorCode::kBufferTooSmall);
        EXPECT_EQ(bytes, unchanged);
    }
}

TEST(HandshakeCodec, CreditStopDecodeRejectsMalformedLengthIdentityAndUnknownKind) {
    for (const auto kind : {HandshakeMessageKind::kCreditStop, HandshakeMessageKind::kCreditStopAck}) {
        std::array<std::byte, 141> bytes{};
        const auto size = EncodeHandshakeMessage(CreditStop(kind), bytes);
        ASSERT_TRUE(size);
        const auto original = bytes;
        // 固定大小的停止消息必须先通过精确长度门禁，半帧、尾随字节和未知类型都不能推进状态机。
        ExpectError(DecodeHandshakeMessage(std::span(bytes).first(11)), HandshakeErrorCode::kInvalidFrame);
        ExpectError(DecodeHandshakeMessage(std::span(bytes).first(139)), HandshakeErrorCode::kInvalidFrame);
        ExpectError(DecodeHandshakeMessage(bytes), HandshakeErrorCode::kInvalidFrame);
        for (const auto length : {127, 129}) {
            bytes = original;
            bytes[11] = static_cast<std::byte>(length);
            ExpectError(DecodeHandshakeMessage(std::span(bytes).first(12 + length)), HandshakeErrorCode::kInvalidFrame);
        }
        for (const auto offset : {12, 20, 28, 36, 44, 56, 60, 68, 76, 84, 92, 104, 108, 116}) {
            bytes = original;
            const auto width = offset == 56 || offset == 104 ? 4 : 8;
            std::fill_n(bytes.begin() + offset, width, std::byte{0});
            ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
        }
        bytes = original;
        bytes[6] = std::byte{8};
        ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
        bytes = original;
        bytes[7] = std::byte{1};
        ExpectError(DecodeHandshakeMessage(std::span(bytes).first(*size)), HandshakeErrorCode::kInvalidFrame);
    }
}

class HandshakeChannelTest : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, pair_.data()), 0);
    }
    void TearDown() override {
        for (int fd : pair_) {
            if (fd >= 0) {
                close(fd);
            }
        }
    }
    void Open(HandshakeChannel& channel, unsigned index, std::uint64_t deadline = 10000) {
        ASSERT_TRUE(channel.Open(pair_[index], true, 0, deadline));
        pair_[index] = -1;
    }
    std::array<int, 2> pair_{-1, -1};
};

TEST_F(HandshakeChannelTest, RequiresExplicitProtocolAuthorizationAndNonblockingConnectedStream) {
    HandshakeChannel channel;
    ExpectError(channel.Open(pair_[0], false, 0, 100), HandshakeErrorCode::kUnauthorized);
    EXPECT_GE(fcntl(pair_[0], F_GETFL), 0);
    ASSERT_EQ(fcntl(pair_[0], F_SETFL, 0), 0);
    ExpectError(channel.Open(pair_[0], true, 0, 100), HandshakeErrorCode::kNotNonblocking);
    ASSERT_EQ(fcntl(pair_[0], F_SETFL, O_NONBLOCK), 0);
    ExpectError(channel.Open(pair_[0], true, 10, 10), HandshakeErrorCode::kInvalidArgument);
    OwnedFd unconnected{socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    ASSERT_GE(unconnected.fd, 0);
    ExpectError(channel.Open(unconnected.fd, true, 0, 100), HandshakeErrorCode::kNotConnected);
    EXPECT_GE(fcntl(unconnected.fd, F_GETFL), 0);
    Open(channel, 0);
    ASSERT_TRUE(channel.Close());
    ExpectError(channel.Open(pair_[1], true, 0, 100), HandshakeErrorCode::kInUse);
}

TEST_F(HandshakeChannelTest, FragmentedBidirectionalHandshakeRespectsEachIoBudget) {
    HandshakeChannel left, right;
    Open(left, 0);
    Open(right, 1);
    ASSERT_TRUE(left.Queue(Hello()));
    ASSERT_TRUE(right.Queue(Ready()));
    // 每次仅容许 1 字节、1 次调用，跨 header/body 断点仍保留准确进度。
    for (unsigned tick = 0; tick < 1000 && !(left.message_ready() && right.message_ready()); ++tick) {
        const auto a = left.Progress(tick, {1, 1});
        const auto b = right.Progress(tick, {1, 1});
        ASSERT_TRUE(a);
        ASSERT_TRUE(b);
        EXPECT_LE(a->syscalls, 1u);
        EXPECT_LE(b->syscalls, 1u);
        EXPECT_LE(a->sent + a->received, 1u);
        EXPECT_LE(b->sent + b->received, 1u);
    }
    ASSERT_TRUE(left.message_ready());
    ASSERT_TRUE(right.message_ready());
    const auto a = left.TakeMessage();
    const auto b = right.TakeMessage();
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_EQ(a->kind, HandshakeMessageKind::kReady);
    EXPECT_EQ(b->kind, HandshakeMessageKind::kHello);
    EXPECT_EQ(a->ready.receiver, Identity(100));
    EXPECT_TRUE(left.output_flushed());
    EXPECT_TRUE(right.output_flushed());
    ExpectError(left.TakeMessage(), HandshakeErrorCode::kNotReady);
}

TEST_F(HandshakeChannelTest, FragmentedOnlineUpdateAndAckRespectBudgetsBeyondHandshakeDeadline) {
    HandshakeChannel left, right;
    Open(left, 0, std::numeric_limits<std::uint64_t>::max());
    Open(right, 1, std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(left.Queue(LaneUpdate()));
    ASSERT_TRUE(right.Queue(LaneAck()));
    // 已建立控制流保持打开；每次只准一次短 IO，UPDATE/ACK 不会破坏有限推进预算。
    for (std::uint64_t tick = 10000; tick < 11000 && !(left.message_ready() && right.message_ready()); ++tick) {
        const auto a = left.Progress(tick, {7, 1});
        const auto b = right.Progress(tick, {7, 1});
        ASSERT_TRUE(a);
        ASSERT_TRUE(b);
        EXPECT_LE(a->syscalls, 1u);
        EXPECT_LE(b->syscalls, 1u);
        EXPECT_LE(a->sent + a->received, 7u);
        EXPECT_LE(b->sent + b->received, 7u);
    }
    auto ack = left.TakeMessage();
    auto update = right.TakeMessage();
    ASSERT_TRUE(ack);
    ASSERT_TRUE(update);
    EXPECT_EQ(ack->kind, HandshakeMessageKind::kLaneAck);
    EXPECT_EQ(update->kind, HandshakeMessageKind::kLaneUpdate);
    EXPECT_EQ(ack->lane_ack.update_id, update->lane_update.update_id);
}

TEST_F(HandshakeChannelTest, FragmentedCreditStopAndAckUseDedicatedBoundedChannel) {
    HandshakeChannel left, right;
    Open(left, 0);
    Open(right, 1);
    const auto stop = CreditStop();
    auto ack = CreditStop(HandshakeMessageKind::kCreditStopAck);
    std::swap(ack.credit_stop_ack.sender, ack.credit_stop_ack.receiver);
    std::swap(ack.credit_stop_ack.sender_credit_epoch, ack.credit_stop_ack.receiver_credit_epoch);
    ASSERT_TRUE(left.Queue(stop));
    ASSERT_TRUE(right.Queue(ack));
    // ACK 的交换身份由上层协调器验证；底层通道仅保证拆包边界与有限 IO 预算。
    for (unsigned tick = 0; tick < 1000 && !(left.message_ready() && right.message_ready()); ++tick) {
        const auto a = left.Progress(tick, {3, 1});
        const auto b = right.Progress(tick, {3, 1});
        ASSERT_TRUE(a);
        ASSERT_TRUE(b);
        EXPECT_LE(a->sent + a->received, 3u);
        EXPECT_LE(b->sent + b->received, 3u);
        EXPECT_LE(a->syscalls, 1u);
        EXPECT_LE(b->syscalls, 1u);
    }
    const auto received_ack = left.TakeMessage();
    const auto received_stop = right.TakeMessage();
    ASSERT_TRUE(received_ack);
    ASSERT_TRUE(received_stop);
    EXPECT_EQ(received_ack->kind, HandshakeMessageKind::kCreditStopAck);
    EXPECT_EQ(received_stop->kind, HandshakeMessageKind::kCreditStop);
    EXPECT_EQ(received_ack->credit_stop_ack, ack.credit_stop_ack);
    EXPECT_EQ(received_stop->credit_stop, stop.credit_stop);
}

TEST_F(HandshakeChannelTest, EstablishedHandoffPreservesCoalescedFirstLaneUpdate) {
    HandshakeChannel handshake;
    Open(handshake, 0);
    std::array<std::byte, 512> bytes{};
    const auto ready = EncodeHandshakeMessage(Ready(), bytes);
    ASSERT_TRUE(ready);
    const auto update = EncodeHandshakeMessage(LaneUpdate(), std::span(bytes).subspan(*ready));
    ASSERT_TRUE(update);
    ASSERT_EQ(send(pair_[1], bytes.data(), *ready + *update, MSG_NOSIGNAL), static_cast<ssize_t>(*ready + *update));
    const auto progressed = handshake.Progress(1);
    ASSERT_TRUE(progressed);
    EXPECT_EQ(progressed->received, *ready);
    ASSERT_TRUE(handshake.TakeMessage());
    const auto fd = handshake.ReleaseEstablished(true);
    ASSERT_TRUE(fd);
    // READY 与首个在线公告同包到达时，接力的新控制通道必须从公告帧边界开始读取。
    OwnedFd owned{*fd};
    HandshakeChannel control;
    ASSERT_TRUE(control.Open(owned.fd, true, 1, std::numeric_limits<std::uint64_t>::max()));
    owned.fd = -1;
    const auto received = control.Progress(2);
    ASSERT_TRUE(received);
    EXPECT_EQ(received->received, *update);
    const auto taken = control.TakeMessage();
    ASSERT_TRUE(taken);
    EXPECT_EQ(taken->kind, HandshakeMessageKind::kLaneUpdate);
    EXPECT_EQ(taken->lane_update.update_id, LaneUpdate().lane_update.update_id);
}

TEST_F(HandshakeChannelTest, EmptySocketIsNonblockingAndRejectsQueueOverwrite) {
    HandshakeChannel channel;
    Open(channel, 0);
    const auto idle = channel.Progress(1);
    ASSERT_TRUE(idle);
    EXPECT_EQ(idle->received, 0u);
    EXPECT_LE(idle->syscalls, 1u);
    ASSERT_TRUE(channel.Queue(Hello()));
    ExpectError(channel.Queue(Ready()), HandshakeErrorCode::kInUse);
    const auto sent = channel.Progress(2);
    ASSERT_TRUE(sent);
    EXPECT_EQ(sent->sent, 152u);
    EXPECT_TRUE(channel.output_flushed());
    ASSERT_TRUE(channel.Queue(Ready()));
}

TEST_F(HandshakeChannelTest, CoalescedMessagesAndApplicationBytesAreNotConsumedAhead) {
    HandshakeChannel receiver;
    Open(receiver, 0);
    std::array<std::byte, 512> packet{};
    const auto one = EncodeHandshakeMessage(Hello(), packet);
    ASSERT_TRUE(one);
    const auto two = EncodeHandshakeMessage(Ready(), std::span(packet).subspan(*one));
    ASSERT_TRUE(two);
    packet[*one + *two] = std::byte{0x71};
    ASSERT_EQ(send(pair_[1], packet.data(), *one + *two + 1, MSG_NOSIGNAL), static_cast<ssize_t>(*one + *two + 1));
    const auto first = receiver.Progress(1);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->received, *one);
    ASSERT_TRUE(receiver.TakeMessage());
    const auto second = receiver.Progress(2);
    ASSERT_TRUE(second);
    EXPECT_EQ(second->received, *two);
    ASSERT_TRUE(receiver.TakeMessage());
    // 双方 READY 的上层证明是释放条件；通道不能读掉 READY 后首个业务字节。
    ExpectError(receiver.ReleaseEstablished(false), HandshakeErrorCode::kInUse);
    const auto released = receiver.ReleaseEstablished(true);
    ASSERT_TRUE(released);
    OwnedFd fd{*released};
    std::byte business{};
    ASSERT_EQ(recv(fd.fd, &business, 1, MSG_DONTWAIT), 1);
    EXPECT_EQ(business, std::byte{0x71});
    EXPECT_EQ(receiver.fd(), -1);
}

TEST_F(HandshakeChannelTest, IncompleteOrUntakenFramePreventsFdRelease) {
    HandshakeChannel receiver;
    Open(receiver, 0);
    std::array<std::byte, 256> bytes{};
    const auto size = EncodeHandshakeMessage(Hello(), bytes);
    ASSERT_TRUE(size);
    ASSERT_EQ(send(pair_[1], bytes.data(), 5, MSG_NOSIGNAL), 5);
    ASSERT_TRUE(receiver.Progress(1));
    ExpectError(receiver.ReleaseEstablished(true), HandshakeErrorCode::kInUse);
    ASSERT_EQ(send(pair_[1], bytes.data() + 5, *size - 5, MSG_NOSIGNAL), static_cast<ssize_t>(*size - 5));
    ASSERT_TRUE(receiver.Progress(2));
    ExpectError(receiver.ReleaseEstablished(true), HandshakeErrorCode::kInUse);
    ASSERT_TRUE(receiver.TakeMessage());
    ASSERT_TRUE(receiver.Queue(Ready()));
    ExpectError(receiver.ReleaseEstablished(true), HandshakeErrorCode::kInUse);
}

TEST_F(HandshakeChannelTest, TxOnlyProgressLeavesPostReadyBusinessBytesUnread) {
    HandshakeChannel channel;
    Open(channel, 0);
    ASSERT_TRUE(channel.Queue(Ready()));
    const std::byte business{0xab};
    ASSERT_EQ(send(pair_[1], &business, 1, MSG_NOSIGNAL), 1);
    // 上层已取走对端 READY，但本端 READY 尚未发完；本次只能推进 TX。
    const auto progress = channel.Progress(1, {4096, 8, false});
    ASSERT_TRUE(progress);
    EXPECT_EQ(progress->sent, 156u);
    EXPECT_EQ(progress->received, 0u);
    EXPECT_EQ(progress->syscalls, 1u);
    const auto fd = channel.ReleaseEstablished(true);
    ASSERT_TRUE(fd);
    OwnedFd released{*fd};
    std::byte received{};
    ASSERT_EQ(recv(released.fd, &received, 1, MSG_DONTWAIT), 1);
    EXPECT_EQ(received, business);
}

TEST_F(HandshakeChannelTest, TimeoutClosesDirtyFdAndFailureIsSticky) {
    HandshakeChannel channel;
    const int taken_fd = pair_[0];
    Open(channel, 0, 10);
    ASSERT_TRUE(channel.Queue(Hello()));
    ASSERT_TRUE(channel.Progress(1, {2, 1}));
    ExpectError(channel.Progress(10), HandshakeErrorCode::kTimeout);
    EXPECT_EQ(fcntl(taken_fd, F_GETFL), -1);
    EXPECT_EQ(errno, EBADF);
    ExpectError(channel.Queue(Ready()), HandshakeErrorCode::kTimeout);
    ExpectError(channel.Progress(11), HandshakeErrorCode::kTimeout);
    ExpectError(channel.ReleaseEstablished(true), HandshakeErrorCode::kTimeout);
}

TEST_F(HandshakeChannelTest, ClockRegressionAndInvalidBudgetDoNotResetDeadline) {
    HandshakeChannel channel;
    Open(channel, 0, 10);
    ASSERT_TRUE(channel.Progress(5));
    ExpectError(channel.Progress(4), HandshakeErrorCode::kInvalidArgument);
    ExpectError(channel.Progress(6, {0, 1}), HandshakeErrorCode::kInvalidArgument);
    ExpectError(channel.Progress(6, {1, 0}), HandshakeErrorCode::kInvalidArgument);
    ExpectError(channel.Progress(6, {1, 65}), HandshakeErrorCode::kInvalidArgument);
    EXPECT_GE(channel.fd(), 0);
    ExpectError(channel.Progress(10), HandshakeErrorCode::kTimeout);
}

TEST_F(HandshakeChannelTest, TruncatedFrameEofAndMalformedHeaderCloseChannel) {
    HandshakeChannel channel;
    Open(channel, 0);
    std::array<std::byte, 256> bytes{};
    const auto size = EncodeHandshakeMessage(Hello(), bytes);
    ASSERT_TRUE(size);
    ASSERT_EQ(send(pair_[1], bytes.data(), 15, MSG_NOSIGNAL), 15);
    ASSERT_EQ(close(pair_[1]), 0);
    pair_[1] = -1;
    ExpectError(channel.Progress(1), HandshakeErrorCode::kPeerClosed);
    EXPECT_EQ(channel.fd(), -1);
}

TEST_F(HandshakeChannelTest, InvalidLengthFailsAfterHeaderWithoutWaitingForBody) {
    HandshakeChannel channel;
    Open(channel, 0);
    std::array<std::byte, 256> bytes{};
    ASSERT_TRUE(EncodeHandshakeMessage(Hello(), bytes));
    bytes[8] = std::byte{0xff};
    ASSERT_EQ(send(pair_[1], bytes.data(), kHandshakeHeaderBytes, MSG_NOSIGNAL),
              static_cast<ssize_t>(kHandshakeHeaderBytes));
    ExpectError(channel.Progress(1), HandshakeErrorCode::kInvalidFrame);
    EXPECT_EQ(channel.fd(), -1);
}

TEST_F(HandshakeChannelTest, CancelClosesOnceAndPeerClosureCannotRaiseSigpipe) {
    HandshakeChannel channel;
    Open(channel, 0);
    channel.Cancel();
    channel.Cancel();
    ExpectError(channel.Progress(1), HandshakeErrorCode::kCancelled);
    ASSERT_TRUE(channel.Close());
    HandshakeChannel sender;
    Open(sender, 1);
    ASSERT_TRUE(sender.Queue(Hello()));
    // 对端已关闭；send 使用 MSG_NOSIGNAL，返回错误而不终止进程。
    const auto result = sender.Progress(1);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, HandshakeErrorCode::kSystemError);
    EXPECT_EQ(sender.fd(), -1);
}

TEST_F(HandshakeChannelTest, FullSocketSendBufferReturnsWithinBudget) {
    int limit = 1024;
    ASSERT_EQ(setsockopt(pair_[0], SOL_SOCKET, SO_SNDBUF, &limit, sizeof(limit)), 0);
    std::array<std::byte, 4096> filler{};
    while (send(pair_[0], filler.data(), filler.size(), MSG_NOSIGNAL) > 0) {
    }
    ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
    HandshakeChannel channel;
    Open(channel, 0);
    ASSERT_TRUE(channel.Queue(Hello()));
    const auto blocked = channel.Progress(1, {4096, 2});
    ASSERT_TRUE(blocked);
    EXPECT_EQ(blocked->sent, 0u);
    EXPECT_LE(blocked->syscalls, 2u);
    EXPECT_TRUE(blocked->output_pending);
    while (recv(pair_[1], filler.data(), filler.size(), MSG_DONTWAIT) > 0) {
    }
    const auto progress = channel.Progress(2);
    ASSERT_TRUE(progress);
    EXPECT_EQ(progress->sent, 152u);
    EXPECT_TRUE(channel.output_flushed());
}

TEST(HandshakeTcp, RealLoopbackStreamExchangesHelloAndReady) {
    OwnedFd listener{socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    ASSERT_GE(listener.fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(bind(listener.fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    ASSERT_EQ(listen(listener.fd, 1), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address), &length), 0);
    OwnedFd client{socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    ASSERT_GE(client.fd, 0);
    const int connected = connect(client.fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    ASSERT_TRUE(connected == 0 || errno == EINPROGRESS);
    pollfd readiness{listener.fd, POLLIN, 0};
    ASSERT_EQ(poll(&readiness, 1, 1000), 1);
    OwnedFd server{accept4(listener.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC)};
    ASSERT_GE(server.fd, 0);
    HandshakeChannel left, right;
    ASSERT_TRUE(left.Open(client.fd, true, 0, 10000));
    client.fd = -1;
    ASSERT_TRUE(right.Open(server.fd, true, 0, 10000));
    server.fd = -1;
    ASSERT_TRUE(left.Queue(Hello()));
    ASSERT_TRUE(right.Queue(Ready()));
    for (unsigned i = 0; i < 1000 && !(left.message_ready() && right.message_ready()); ++i) {
        ASSERT_TRUE(left.Progress(i));
        ASSERT_TRUE(right.Progress(i));
    }
    ASSERT_TRUE(left.message_ready());
    ASSERT_TRUE(right.message_ready());
    const auto a = left.TakeMessage();
    const auto b = right.TakeMessage();
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_EQ(a->kind, HandshakeMessageKind::kReady);
    EXPECT_EQ(b->kind, HandshakeMessageKind::kHello);
}
} // namespace
} // namespace control
} // namespace kbsocket
