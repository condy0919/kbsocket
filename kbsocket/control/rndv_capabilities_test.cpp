// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/rndv_capabilities.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
} // namespace
void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void operator delete(void* pointer) noexcept {
    if (count_allocations && pointer) {
        ++deallocations;
    }
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace control {
namespace {
RndvLimits Limits() {
    return {.version = kRndvVersion,
            .flags = kRndvSuccessfulWriteDone,
            .max_write_bytes = 4096,
            .max_transfer_bytes = 16384,
            .max_grants = 8,
            .window = 128,
            .max_bytes = 131072};
}
RndvOffer Offer() {
    RndvOffer result;
    result.limits = Limits();
    result.arena_count = 1;
    auto& arena = result.arenas[0];
    arena.id = 0x0102030405060708;
    arena.generation = 19;
    arena.address = 0x100000;
    arena.length = 131072;
    arena.max_grant_bytes = 16384;
    arena.eid[15] = std::byte{7};
    arena.uasid = 0x010203;
    arena.token_id = 0x04050607;
    arena.token = 0x08090a0b;
    arena.access = kRndvAccessRead | kRndvAccessWrite;
    arena.token_policy = kRndvTokenPlainText;
    return result;
}

template <class T>
void ExpectError(const std::expected<T, RndvCapabilityError>& result, RndvCapabilityErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

TEST(RndvCapabilities, LimitsRoundTripHasCanonicalEndianAndReservedTail) {
    const auto limits = Limits();
    std::array<std::byte, kRndvLimitsBytes + 1> bytes;
    bytes.fill(std::byte{0xcc});
    const auto encoded = EncodeRndvLimits(limits, bytes);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, 40u);
    EXPECT_EQ(bytes[0], std::byte{0});
    EXPECT_EQ(bytes[1], std::byte{1});
    EXPECT_EQ(bytes[3], std::byte{1});
    EXPECT_EQ(bytes[10], std::byte{0x10});
    EXPECT_EQ(bytes[23], std::byte{8});
    EXPECT_EQ(bytes[27], std::byte{128});
    EXPECT_TRUE(std::all_of(bytes.begin() + 36, bytes.begin() + 40, [](auto b) { return b == std::byte{0}; }));
    EXPECT_EQ(bytes[40], std::byte{0xcc});
    const auto decoded = DecodeRndvLimits(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(*decoded, limits);
}

TEST(RndvCapabilities, OfferRoundTripUsesSemanticFieldsWithoutCppPadding) {
    const auto offer = Offer();
    std::array<std::byte, kRndvMaxOfferBytes> bytes;
    bytes.fill(std::byte{0xee});
    const auto encoded = EncodeRndvOffer(offer, bytes);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, 120u);
    EXPECT_EQ(bytes[43], std::byte{1});
    for (std::size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(bytes[48 + i], static_cast<std::byte>(i + 1));
    }
    EXPECT_EQ(bytes[103], std::byte{7});
    EXPECT_EQ(bytes[105], std::byte{1});
    EXPECT_EQ(bytes[108], std::byte{4});
    EXPECT_EQ(bytes[112], std::byte{8});
    EXPECT_EQ(bytes[116], std::byte{6});
    EXPECT_EQ(bytes[117], std::byte{1});
    EXPECT_EQ(bytes[118], std::byte{0});
    EXPECT_EQ(bytes[119], std::byte{0});
    EXPECT_EQ(bytes[*encoded], std::byte{0xee});
    const auto decoded = DecodeRndvOffer(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(*decoded, offer);
}

TEST(RndvCapabilities, SupportsEightArenasButDoesNotSerializeUnusedSlots) {
    auto offer = Offer();
    offer.arenas[7].token = 99;
    std::array<std::byte, kRndvMaxOfferBytes> bytes{};
    auto encoded = EncodeRndvOffer(offer, bytes);
    ASSERT_TRUE(encoded);
    auto decoded = DecodeRndvOffer(std::span(bytes).first(*encoded));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->arenas[7], RndvArenaDescriptor{});
    // 最大列表仍是固定有界后缀，逐个条目保持独立 Slab 身份与互不重叠范围。
    offer.arena_count = kRndvMaxArenas;
    for (std::size_t i = 1; i < offer.arena_count; ++i) {
        offer.arenas[i] = offer.arenas[0];
        offer.arenas[i].id += i;
        offer.arenas[i].address += i * offer.arenas[0].length;
    }
    encoded = EncodeRndvOffer(offer, bytes);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(*encoded, kRndvMaxOfferBytes);
    decoded = DecodeRndvOffer(bytes);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(*decoded, offer);
}

TEST(RndvCapabilities, DisabledAndUnsupportedVersionsAreNotEncodedAsEnabledSuffix) {
    ExpectError(ValidateRndvLimits({}), RndvCapabilityErrorCode::kVersionMismatch);
    auto limits = Limits();
    limits.version = 2;
    ExpectError(ValidateRndvLimits(limits), RndvCapabilityErrorCode::kVersionMismatch);
    std::array<std::byte, kRndvLimitsBytes> bytes{};
    ASSERT_TRUE(EncodeRndvLimits(Limits(), bytes));
    bytes[1] = std::byte{2};
    ExpectError(DecodeRndvLimits(bytes), RndvCapabilityErrorCode::kVersionMismatch);
}

TEST(RndvCapabilities, RejectsMissingSuccessContractAndUnknownFlags) {
    for (const auto flags : {0u, 2u, 3u, 0xffffu}) {
        auto limits = Limits();
        limits.flags = static_cast<std::uint16_t>(flags);
        ExpectError(ValidateRndvLimits(limits), RndvCapabilityErrorCode::kInvalidArgument);
    }
}

TEST(RndvCapabilities, ValidatesCapacityBoundsWithoutConflatingChunkAndTransfer) {
    const auto good = Limits();
    // 分块可以小于完整事务，单 WR 上限独立限制；全局窗口与活动 Grant 容量须相容。
    ASSERT_TRUE(ValidateRndvLimits(good));
    auto limits = good;
    limits.max_write_bytes = kRndvMaxWriteBytes;
    ASSERT_TRUE(ValidateRndvLimits(limits));
    for (const auto bytes : {std::uint64_t{0}, kRndvMaxWriteBytes + 1}) {
        limits = good;
        limits.max_write_bytes = bytes;
        EXPECT_FALSE(ValidateRndvLimits(limits));
    }
    for (const auto count : {0u, 129u}) {
        limits = good;
        limits.max_grants = count;
        EXPECT_FALSE(ValidateRndvLimits(limits));
    }
    limits = good;
    limits.window = good.max_grants - 1;
    EXPECT_FALSE(ValidateRndvLimits(limits));
    limits.window = 4097;
    EXPECT_FALSE(ValidateRndvLimits(limits));
    limits = good;
    limits.max_transfer_bytes = 0;
    EXPECT_FALSE(ValidateRndvLimits(limits));
    limits = good;
    limits.max_bytes = limits.max_transfer_bytes - 1;
    EXPECT_FALSE(ValidateRndvLimits(limits));
}

TEST(RndvCapabilities, TransferAndGrantSizesRespectActualUint32GrantLimit) {
    auto limits = Limits();
    limits.max_transfer_bytes = std::numeric_limits<std::uint32_t>::max();
    limits.max_write_bytes = kRndvMaxWriteBytes;
    limits.max_bytes = limits.max_transfer_bytes + 1;
    ASSERT_TRUE(ValidateRndvLimits(limits));
    ++limits.max_transfer_bytes;
    EXPECT_FALSE(ValidateRndvLimits(limits));
    auto arena = Offer().arenas[0];
    arena.length = std::uint64_t{1} << 32;
    arena.max_grant_bytes = std::numeric_limits<std::uint32_t>::max();
    ASSERT_TRUE(ValidateRndvArena(arena));
    ++arena.max_grant_bytes;
    EXPECT_FALSE(ValidateRndvArena(arena));
}

TEST(RndvCapabilities, OneTransferMustFitTheBoundedWriteEngineChunkTable) {
    auto limits = Limits();
    // 块大小为 1 也可协商，但最后一个块必须仍在单引擎固定责任表内。
    limits.max_write_bytes = 1;
    limits.max_transfer_bytes = kRndvMaxChunks;
    ASSERT_TRUE(ValidateRndvLimits(limits));
    ++limits.max_transfer_bytes;
    EXPECT_FALSE(ValidateRndvLimits(limits));
    // 非整除的尾块也占一条责任；只比较整数除法会漏掉这个边界。
    limits.max_write_bytes = 3;
    limits.max_transfer_bytes = std::uint64_t{3} * kRndvMaxChunks;
    ASSERT_TRUE(ValidateRndvLimits(limits));
    ++limits.max_transfer_bytes;
    EXPECT_FALSE(ValidateRndvLimits(limits));
}

TEST(RndvCapabilities, RejectsZeroIdentityAndInvalidAddressRanges) {
    const auto good = Offer().arenas[0];
    for (unsigned field = 0; field < 7; ++field) {
        auto arena = good;
        switch (field) {
        case 0:
            arena.id = 0;
            break;
        case 1:
            arena.generation = 0;
            break;
        case 2:
            arena.address = 0;
            break;
        case 3:
            arena.length = 0;
            break;
        case 4:
            arena.max_grant_bytes = 0;
            break;
        case 5:
            arena.eid = {};
            break;
        case 6:
            arena.max_grant_bytes = arena.length + 1;
            break;
        }
        EXPECT_FALSE(ValidateRndvArena(arena)) << field;
    }
    auto arena = good;
    arena.address = std::numeric_limits<std::uint64_t>::max() - arena.length + 1;
    EXPECT_FALSE(ValidateRndvArena(arena));
    arena.address -= 1;
    EXPECT_TRUE(ValidateRndvArena(arena));
}

TEST(RndvCapabilities, RequiresWritePermissionAndSupportedTokenSemantics) {
    const auto good = Offer().arenas[0];
    for (const auto access : {0u, 1u, 2u, 5u, 0x84u}) {
        auto arena = good;
        arena.access = static_cast<std::uint8_t>(access);
        EXPECT_FALSE(ValidateRndvArena(arena));
    }
    auto arena = good;
    arena.access = kRndvAccessWrite | kRndvAccessAtomic;
    ASSERT_TRUE(ValidateRndvArena(arena));
    arena.token_policy = 2;
    EXPECT_FALSE(ValidateRndvArena(arena));
    arena.token_policy = kRndvTokenNone;
    EXPECT_FALSE(ValidateRndvArena(arena));
    arena.token = 0;
    ASSERT_TRUE(ValidateRndvArena(arena));
    arena.token_id = std::numeric_limits<std::uint32_t>::max();
    EXPECT_FALSE(ValidateRndvArena(arena));
    arena = good;
    arena.uasid = 1u << 24;
    EXPECT_FALSE(ValidateRndvArena(arena));
}

TEST(RndvCapabilities, RejectsDuplicateArenaIdentityAcrossGenerations) {
    auto offer = Offer();
    offer.arena_count = 2;
    offer.arenas[1] = offer.arenas[0];
    offer.arenas[1].generation += 1;
    offer.arenas[1].address += offer.arenas[0].length;
    EXPECT_FALSE(ValidateRndvOffer(offer));
    offer.arenas[1].id += 1;
    EXPECT_TRUE(ValidateRndvOffer(offer));
}

TEST(RndvCapabilities, RejectsOverlappingSegmentsButAllowsAdjacentOrDistinctSpaces) {
    auto offer = Offer();
    offer.arena_count = 2;
    offer.arenas[1] = offer.arenas[0];
    offer.arenas[1].id += 1;
    offer.arenas[1].address += offer.arenas[0].length - 1;
    EXPECT_FALSE(ValidateRndvOffer(offer));
    offer.arenas[1].address += 1;
    EXPECT_TRUE(ValidateRndvOffer(offer));
    offer.arenas[1].address = offer.arenas[0].address;
    offer.arenas[1].uasid += 1;
    EXPECT_TRUE(ValidateRndvOffer(offer));
}

TEST(RndvCapabilities, RequiresDirectoryBackingAndSliceCapacityForAdvertisedLimits) {
    auto offer = Offer();
    offer.limits.max_bytes += 1;
    EXPECT_FALSE(ValidateRndvOffer(offer));
    offer = Offer();
    offer.arenas[0].max_grant_bytes -= 1;
    EXPECT_FALSE(ValidateRndvOffer(offer));
    offer = Offer();
    offer.arena_count = 0;
    EXPECT_FALSE(ValidateRndvOffer(offer));
    offer.arena_count = kRndvMaxArenas + 1;
    EXPECT_FALSE(ValidateRndvOffer(offer));
}

TEST(RndvCapabilities, CapacitySumSaturatesInsteadOfWrapping) {
    auto offer = Offer();
    offer.arena_count = 2;
    offer.arenas[0].address = 1;
    offer.arenas[0].length = std::numeric_limits<std::uint64_t>::max() - 1;
    offer.arenas[1] = offer.arenas[0];
    offer.arenas[1].id += 1;
    offer.arenas[1].uasid += 1;
    offer.limits.max_bytes = std::numeric_limits<std::uint64_t>::max();
    EXPECT_TRUE(ValidateRndvOffer(offer));
}

TEST(RndvCapabilities, EncodeFailureLeavesEntireOutputUntouched) {
    std::array<std::byte, kRndvMaxOfferBytes> bytes;
    bytes.fill(std::byte{0xcc});
    const auto original = bytes;
    auto bad = Offer();
    bad.arenas[0].id = 0;
    EXPECT_FALSE(EncodeRndvOffer(bad, bytes));
    EXPECT_EQ(bytes, original);
    EXPECT_FALSE(EncodeRndvOffer(Offer(), std::span(bytes).first(119)));
    EXPECT_EQ(bytes, original);
    EXPECT_FALSE(EncodeRndvLimits({}, bytes));
    EXPECT_FALSE(EncodeRndvLimits(Limits(), std::span(bytes).first(kRndvLimitsBytes - 1)));
    EXPECT_EQ(bytes, original);
}

TEST(RndvCapabilities, RejectsTruncatedAndTrailingBytesBeforeReadingVariableDirectory) {
    std::array<std::byte, kRndvMaxOfferBytes> bytes{};
    const auto encoded = EncodeRndvOffer(Offer(), bytes);
    ASSERT_TRUE(encoded);
    for (std::size_t length = 0; length < *encoded; ++length) {
        EXPECT_FALSE(DecodeRndvOffer(std::span(bytes).first(length))) << length;
    }
    EXPECT_FALSE(DecodeRndvOffer(std::span(bytes).first(*encoded + 1)));
    for (std::size_t length = 0; length < kRndvLimitsBytes; ++length) {
        EXPECT_FALSE(DecodeRndvLimits(std::span(bytes).first(length)));
    }
    EXPECT_FALSE(DecodeRndvLimits(std::span(bytes).first(kRndvLimitsBytes + 1)));
}

TEST(RndvCapabilities, RejectsReservedFieldsAndUntrustedCount) {
    std::array<std::byte, kRndvMaxOfferBytes> bytes{};
    const auto encoded = EncodeRndvOffer(Offer(), bytes);
    ASSERT_TRUE(encoded);
    const auto good = bytes;
    for (const auto offset : {36u, 39u, 44u, 47u, 118u, 119u}) {
        bytes = good;
        bytes[offset] = std::byte{1};
        ExpectError(DecodeRndvOffer(std::span(bytes).first(*encoded)), RndvCapabilityErrorCode::kInvalidFrame);
    }
    bytes = good;
    bytes[40] = std::byte{0xff};
    EXPECT_FALSE(DecodeRndvOffer(std::span(bytes).first(*encoded)));
}

TEST(RndvCapabilities, ValidationAndCodecAllocateAndFreeNothing) {
    const auto offer = Offer();
    std::array<std::byte, kRndvMaxOfferBytes> bytes{};
    // 断言在计数关闭后执行，避免把 gtest 自身的格式化分配计入协议热路径。
    allocations = deallocations = 0;
    count_allocations = true;
    const auto valid = ValidateRndvOffer(offer);
    const auto encoded = EncodeRndvOffer(offer, bytes);
    const auto decoded = DecodeRndvOffer(std::span(bytes).first(120));
    const auto limit_encoded = EncodeRndvLimits(offer.limits, bytes);
    const auto limit_decoded = DecodeRndvLimits(std::span(bytes).first(kRndvLimitsBytes));
    count_allocations = false;
    ASSERT_TRUE(valid);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(limit_encoded);
    ASSERT_TRUE(limit_decoded);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
}
} // namespace
} // namespace control
} // namespace kbsocket
