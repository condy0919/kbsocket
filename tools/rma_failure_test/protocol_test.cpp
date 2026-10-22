// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <limits>

#include "tools/rma_failure_test/rma_failure_test.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
RmaFailureHello ValidFailureHello(bool server = true) {
    RmaFailureHello hello;
    hello.server = server;
    hello.options = {.bytes = 16384, .batch = 32, .rounds = 8, .timeout_ms = 1000};
    for (std::size_t i = 0; i < (server ? 2U : 1U); ++i) {
        auto& endpoint = hello.endpoints[i];
        endpoint.server = server;
        endpoint.options = {.operation = RmaOperation::kWrite,
                            .bytes = hello.options.bytes,
                            .rounds = hello.options.rounds,
                            .timeout_ms = hello.options.timeout_ms,
                            .window = 1};
        endpoint.endpoint.eid.raw[0] = static_cast<std::uint8_t>(i + 1);
        endpoint.endpoint.id = 0x10203040U + i;
        endpoint.segment.ubva.eid = endpoint.endpoint.eid;
        endpoint.segment.ubva.uasid = 0xabcdef;
        endpoint.segment.ubva.va = 0x123456780000ULL + i * 0x100000;
        endpoint.segment.len = 64 + (hello.options.batch - 1) * RmaSlotStride(endpoint.options) + hello.options.bytes;
        endpoint.segment.attr.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
        endpoint.segment.token_id = 0x1234 + i;
        endpoint.payload_address = endpoint.segment.ubva.va + 64;
    }
    return hello;
}

TEST(RmaFailureProtocolTest, BothTargetsRoundTripWithoutCopyingAbiStructs) {
    const auto hello = ValidFailureHello();
    const auto wire = EncodeRmaFailureHello(hello);
    EXPECT_EQ(wire.size(), 256U);
    EXPECT_EQ(wire[32 + 40], std::byte{0x10});
    EXPECT_EQ(wire[32 + 43], std::byte{0x40});
    auto decoded = DecodeRmaFailureHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->options.batch, 32U);
    for (std::size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(decoded->endpoints[i].payload_address, hello.endpoints[i].payload_address);
        EXPECT_EQ(decoded->endpoints[i].segment.ubva.va, hello.endpoints[i].segment.ubva.va);
        EXPECT_EQ(decoded->endpoints[i].endpoint.id, hello.endpoints[i].endpoint.id);
        EXPECT_EQ(decoded->endpoints[i].options.window, 1U);
        EXPECT_EQ(decoded->endpoints[i].options.operation, RmaOperation::kWrite);
    }
}

TEST(RmaFailureProtocolTest, ClientWireHasNoSecondTargetAndRejectsHiddenDescriptor) {
    const auto hello = ValidFailureHello(false);
    auto wire = EncodeRmaFailureHello(hello);
    EXPECT_TRUE(std::all_of(wire.begin() + 144, wire.end(), [](std::byte byte) { return byte == std::byte{0}; }));
    ASSERT_TRUE(DecodeRmaFailureHello(wire));
    // 客户端不能偷偷携带第二个远端目标，保留区必须全部为零。
    wire[200] = std::byte{1};
    EXPECT_FALSE(DecodeRmaFailureHello(wire));
}

TEST(RmaFailureProtocolTest, RejectsBadOuterHeaderAndReservedFields) {
    const auto good = EncodeRmaFailureHello(ValidFailureHello());
    for (const auto offset : {0U, 7U, 11U, 24U, 31U}) {
        auto wire = good;
        wire[offset] = std::byte{0xff};
        EXPECT_FALSE(DecodeRmaFailureHello(wire)) << offset;
    }
}

TEST(RmaFailureProtocolTest, RejectsLegacyReadFailureProtocolVersion) {
    auto wire = EncodeRmaFailureHello(ValidFailureHello());
    EXPECT_EQ(wire[7], std::byte{2});
    // WRITE 修改了数据方向与校验屏障，旧 READ 二进制必须在交换描述符时被拒绝。
    wire[7] = std::byte{1};
    EXPECT_FALSE(DecodeRmaFailureHello(wire));
}

TEST(RmaFailureProtocolTest, RejectsCorruptedInnerProtocolForEitherTarget) {
    const auto good = EncodeRmaFailureHello(ValidFailureHello());
    for (const auto base : {32U, 144U}) {
        auto wire = good;
        wire[base + 7] = std::byte{0xff};
        EXPECT_FALSE(DecodeRmaFailureHello(wire));
        wire = good;
        wire[base + 92] = std::byte{1};
        EXPECT_FALSE(DecodeRmaFailureHello(wire));
    }
}

TEST(RmaFailureProtocolTest, EndpointRoleOperationWindowAndOptionsMustMatchOuterHeader) {
    for (int mismatch = 0; mismatch < 6; ++mismatch) {
        auto hello = ValidFailureHello();
        auto& endpoint = hello.endpoints[1];
        switch (mismatch) {
        case 0:
            endpoint.server = false;
            break;
        case 1:
            endpoint.options.operation = RmaOperation::kRead;
            break;
        case 2:
            ++endpoint.options.bytes;
            break;
        case 3:
            ++endpoint.options.rounds;
            break;
        case 4:
            ++endpoint.options.window;
            break;
        case 5:
            endpoint.options.mixed_send = true;
            break;
        }
        EXPECT_FALSE(MatchRmaFailureEndpoint(endpoint, hello.options, true)) << mismatch;
        EXPECT_FALSE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello))) << mismatch;
    }
}

TEST(RmaFailureProtocolTest, HealthyAndFaultTargetsCannotBeTheSameEndpoint) {
    auto hello = ValidFailureHello();
    hello.endpoints[1] = hello.endpoints[0];
    EXPECT_FALSE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
    // ID 在不同 EID 下可以相同，必须以完整端点身份判断是否重复。
    hello = ValidFailureHello();
    hello.endpoints[1].endpoint.id = hello.endpoints[0].endpoint.id;
    EXPECT_TRUE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
}

TEST(RmaFailureProtocolTest, EntireRemoteWindowMustFitAndAddressesMustNotWrap) {
    auto hello = ValidFailureHello();
    ASSERT_TRUE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
    --hello.endpoints[1].segment.len;
    EXPECT_FALSE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
    hello = ValidFailureHello();
    hello.endpoints[1].segment.ubva.va = std::numeric_limits<std::uint64_t>::max() - 64;
    hello.endpoints[1].payload_address = hello.endpoints[1].segment.ubva.va;
    EXPECT_FALSE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
}

TEST(RmaFailureProtocolTest, SegmentAccessAndEndpointEidAreValidatedBeforeImport) {
    auto hello = ValidFailureHello();
    hello.endpoints[1].segment.attr.bs.access = URMA_ACCESS_READ;
    EXPECT_FALSE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
    hello = ValidFailureHello();
    ++hello.endpoints[1].segment.ubva.eid.raw[0];
    EXPECT_FALSE(DecodeRmaFailureHello(EncodeRmaFailureHello(hello)));
}

TEST(RmaFailureProtocolTest, CapsTwoTargetMemoryAndAcceptsBoundaryConfiguration) {
    EXPECT_TRUE(ValidateRmaFailureOptions({.bytes = 1024U * 1024, .batch = 128, .rounds = 256, .timeout_ms = 120000}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.bytes = 1024U * 1024 + 1, .batch = 128}));
    EXPECT_TRUE(ValidateRmaFailureOptions({.bytes = 8U * 1024 * 1024, .batch = 16, .rounds = 1, .timeout_ms = 100}));
}

TEST(RmaFailureProtocolTest, RejectsInvalidOptionsBeforeAnyHardwareAllocation) {
    EXPECT_FALSE(ValidateRmaFailureOptions({.bytes = 0}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.bytes = 8U * 1024 * 1024 + 1}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.batch = 0}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.batch = 129}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.rounds = 0}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.rounds = 257}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.timeout_ms = 99}));
    EXPECT_FALSE(ValidateRmaFailureOptions({.timeout_ms = 120001}));
}
} // namespace
} // namespace tools
} // namespace kbsocket
