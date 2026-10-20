// SPDX-License-Identifier: MulanPSL-2.0
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>

#include "tools/rma_test/rma_test.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
RmaHello ValidHello() {
    RmaHello hello;
    hello.server = true;
    hello.options.bytes = 16384;
    hello.options.rounds = 7;
    hello.endpoint.eid.raw[0] = 0x12;
    hello.endpoint.eid.raw[15] = 0x34;
    hello.endpoint.id = 0x10203040;
    hello.segment.ubva.eid = hello.endpoint.eid;
    hello.segment.ubva.uasid = 0xabcdef;
    hello.segment.ubva.va = 0x123456780000ULL;
    hello.segment.len = 32768;
    hello.segment.attr.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    hello.segment.token_id = 0x98abcdef;
    hello.payload_address = hello.segment.ubva.va + 64;
    return hello;
}

TEST(RmaProtocolTest, RoundTripUsesExplicitNetworkFieldsAndPreservesWideAddress) {
    const auto local = ValidHello();
    const auto wire = EncodeRmaHello(local);
    EXPECT_EQ(wire[40], std::byte{0x10});
    EXPECT_EQ(wire[43], std::byte{0x40});
    EXPECT_EQ(wire[66], std::byte{0x12});
    EXPECT_EQ(wire[67], std::byte{0x34});
    auto decoded = DecodeRmaHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->segment.ubva.va, local.segment.ubva.va);
    EXPECT_EQ(decoded->payload_address, local.payload_address);
    EXPECT_EQ(decoded->segment.ubva.uasid, local.segment.ubva.uasid);
    EXPECT_EQ(decoded->segment.token_id, local.segment.token_id);
    EXPECT_EQ(decoded->segment.attr.value, local.segment.attr.value);
    EXPECT_EQ(decoded->options.bytes, 16384U);
    EXPECT_EQ(decoded->options.rounds, 7U);
    EXPECT_TRUE(decoded->server);
}

TEST(RmaProtocolTest, RejectsAddressOverflowBeforeComputingEnd) {
    auto hello = ValidHello();
    // 不能用 va+len 比较范围：该加法本身会回绕并让恶意地址通过。
    hello.segment.ubva.va = std::numeric_limits<std::uint64_t>::max() - 100;
    hello.payload_address = hello.segment.ubva.va;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.payload_address = hello.segment.ubva.va - 1;
    EXPECT_FALSE(ValidateRmaHello(hello));
}

TEST(RmaProtocolTest, ValidatesEntirePayloadRatherThanJustStartingAddress) {
    auto hello = ValidHello();
    hello.payload_address = hello.segment.ubva.va + hello.segment.len - hello.options.bytes;
    EXPECT_TRUE(ValidateRmaHello(hello));
    ++hello.payload_address;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello.payload_address = hello.segment.ubva.va + hello.segment.len + 1;
    EXPECT_FALSE(ValidateRmaHello(hello));
}

TEST(RmaProtocolTest, RejectsUnsupportedTokenPolicyExtensionsAndAddressModes) {
    auto hello = ValidHello();
    hello.token.token = 1;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.segment.attr.bs.token_policy = URMA_TOKEN_PLAIN_TEXT;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.segment.attr.bs.has_user_info = 1;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.segment.attr.bs.user_iova = 1;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.segment.token_id = URMA_SEG_TOKEN_ID_INVALID;
    EXPECT_FALSE(ValidateRmaHello(hello));
}

TEST(RmaProtocolTest, ChecksPermissionsEidAndUasidBeforeImport) {
    auto hello = ValidHello();
    hello.segment.attr.bs.access = URMA_ACCESS_WRITE;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello.options.operation = RmaOperation::kWrite;
    EXPECT_TRUE(ValidateRmaHello(hello));
    hello.segment.ubva.eid.raw[0] ^= 1;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.segment.ubva.uasid = 1U << 24;
    EXPECT_FALSE(ValidateRmaHello(hello));
    hello = ValidHello();
    hello.segment.attr.bs.access |= URMA_ACCESS_LOCAL_ONLY;
    EXPECT_FALSE(ValidateRmaHello(hello));
}

TEST(RmaProtocolTest, RejectsBadMagicVersionRoleAndReservedBytes) {
    const auto good = EncodeRmaHello(ValidHello());
    for (const auto offset : {0U, 7U, 11U, 92U, 104U}) {
        auto wire = good;
        wire[offset] = std::byte{0xff};
        EXPECT_FALSE(DecodeRmaHello(wire)) << offset;
    }
}

TEST(RmaProtocolTest, CapabilityChecksReadWriteLimitsSeparatelyFromSendLimit) {
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_msg_size = 4096;
    cap.max_read_size = 8U * 1024 * 1024;
    cap.max_write_size = 16384;
    // READ 的单 WR 可以大于 SEND 上限；不能把 SEND 的 4KiB 分片限制套在 RMA 上。
    EXPECT_TRUE(CheckRmaCapability({.bytes = 8U * 1024 * 1024}, cap));
    auto unsupported = CheckRmaCapability({.operation = RmaOperation::kWrite, .bytes = 32768}, cap);
    ASSERT_FALSE(unsupported);
    EXPECT_EQ(unsupported.error().code, EOPNOTSUPP);
    EXPECT_TRUE(CheckRmaCapability({.operation = RmaOperation::kWrite, .bytes = 16384}, cap));
    cap.max_read_size = 0;
    EXPECT_FALSE(CheckRmaCapability({}, cap));
}

TEST(RmaProtocolTest, RejectsInvalidOptions) {
    EXPECT_FALSE(ValidateRmaOptions({.bytes = 0}));
    EXPECT_FALSE(ValidateRmaOptions({.bytes = 64U * 1024 * 1024 + 1}));
    EXPECT_FALSE(ValidateRmaOptions({.rounds = 0}));
    EXPECT_FALSE(ValidateRmaOptions({.rounds = 10001}));
    EXPECT_FALSE(ValidateRmaOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidateRmaOptions({.operation = static_cast<RmaOperation>(0)}));
}
} // namespace
} // namespace tools
} // namespace kbsocket
