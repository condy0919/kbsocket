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
                                           std::byte{0},   std::byte{2},   std::byte{1},   std::byte{0},
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
