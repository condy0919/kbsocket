// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_protocol.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

namespace {
// Open 之外才启用计数，避免把 GTest 或控制路径的预分配计入发送热路径。
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
    }
}
void RecordDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocations;
    }
}
} // namespace
void* operator new(std::size_t bytes) {
    RecordAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    RecordAllocation();
    void* pointer = nullptr;
    if (::posix_memalign(&pointer, static_cast<std::size_t>(alignment), bytes ? bytes : 1) != 0) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) {
    return ::operator new(bytes, alignment);
}
void operator delete(void* pointer) noexcept {
    RecordDeallocation(pointer);
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
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace connection {
namespace {
GrantBinding Binding() {
    return {.sender_runtime = 11, .receiver_runtime = 22, .receive_domain = 33, .session = 44, .token = {55, 66}};
}
GrantTransfer Transfer() {
    return {.binding = Binding(), .transfer_id = 77, .seq = 8, .bytes = 1024};
}
GrantIdentity Identity() {
    return {.transfer = Transfer(),
            .arena = {99, 110},
            .slot = 12,
            .generation = (std::uint64_t{1} << 40) + 13,
            .target_address = 0x1234000};
}
RndvDestination Forward() {
    const auto b = Binding();
    return {b.receiver_runtime, b.receive_domain, b.session, b.token};
}
RndvDestination Reverse() {
    // 反向 GRANT 的路由域、Session 与 token 都不同，不能被原始传输身份覆盖。
    return {Binding().sender_runtime, 133, 144, {155, 166}};
}
WireSessionBinding ReceiveBinding(bool reverse = false) {
    const auto target = reverse ? Reverse() : Forward();
    return {.context = {.peer_runtime = reverse ? Binding().receiver_runtime : Binding().sender_runtime,
                        .receive_domain = target.receive_domain,
                        .session = target.session},
            .token = target.token,
            .credit_epoch = 777};
}
std::expected<WireFrameInput, RndvProtocolError> DoneFrame() {
    // 仅为验证器构造网络输入；不能将这个测试帧当作发送端生成成功证明的 API。
    auto frame = BuildRndvGrant(Identity(), Reverse());
    if (!frame) {
        return frame;
    }
    frame->control = WireControlType::kRndvDone;
    frame->session = Forward().session;
    frame->token = Forward().token;
    return frame;
}
std::expected<WireFrameView, WireError> RoundTrip(const WireFrameInput& frame) {
    std::array<std::byte, 112> payload{};
    const auto encoded = EncodeWireFrame(frame, payload);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    // 纯控制帧没有借用 payload 的 data 视图，返回的身份值不依赖临时数组。
    return DecodeWireFrame(encoded->imm_data, std::span(payload).first(encoded->payload_bytes));
}
template <typename T>
void ExpectError(const std::expected<T, RndvProtocolError>& result, RndvProtocolErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

TEST(RndvProtocolTest, RtsRoundTripsOriginalTransferWithoutGrantFields) {
    const auto input = BuildRndvRts(Transfer(), Forward());
    ASSERT_TRUE(input);
    EXPECT_EQ(input->control, WireControlType::kRndvRts);
    EXPECT_EQ(input->seq, Transfer().seq);
    EXPECT_EQ(input->session, Forward().session);
    std::array<std::byte, 112> payload{};
    const auto encoded = EncodeWireFrame(*input, payload);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->payload_bytes, 64u);
    auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding();
    const auto transfer = ValidateRndvRts(*decoded, route, route.context, Binding());
    ASSERT_TRUE(transfer);
    EXPECT_EQ(*transfer, Transfer());
}

TEST(RndvProtocolTest, ReverseGrantKeepsDistinctRoutingAndTransferSessions) {
    const auto input = BuildRndvGrant(Identity(), Reverse());
    ASSERT_TRUE(input);
    EXPECT_EQ(input->session, Reverse().session);
    EXPECT_EQ(input->token, Reverse().token);
    EXPECT_EQ(input->rndv.session, Binding().session);
    EXPECT_EQ(input->rndv.token, Binding().token);
    std::array<std::byte, 112> payload{};
    const auto encoded = EncodeWireFrame(*input, payload);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->payload_bytes, 104u);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding(true);
    const auto grant = ValidateRndvGrant(*decoded, route, route.context, Transfer(), Identity().arena);
    ASSERT_TRUE(grant);
    EXPECT_EQ(*grant, Identity());
    EXPECT_GT(grant->generation, std::numeric_limits<std::uint32_t>::max());
}

TEST(RndvProtocolTest, DoneValidationOnlyMatchesIdentityAndDoesNotProduceTerminalEvidence) {
    const auto input = DoneFrame();
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding();
    EXPECT_TRUE(ValidateRndvDone(*decoded, route, route.context, Identity()));
}

TEST(RndvProtocolTest, ExtendedRouteTokenDoesNotChangeOriginalGrantIdentity) {
    auto destination = Reverse();
    destination.token = {70000, 1024};
    auto route = ReceiveBinding(true);
    route.token = destination.token;
    const auto input = BuildRndvGrant(Identity(), destination);
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->extended_identity);
    EXPECT_EQ(decoded->frame.rndv.token, Binding().token);
    const auto grant = ValidateRndvGrant(*decoded, route, route.context, Transfer(), Identity().arena);
    ASSERT_TRUE(grant);
    EXPECT_EQ(*grant, Identity());
}

TEST(RndvProtocolTest, DoneBuilderRequiresLiveEngineOrPoolTransaction) {
    WriteRndvEngine engine;
    WriteRndvPool pool;
    ExpectError(BuildRndvDone(engine, Forward()), RndvProtocolErrorCode::kInvalidEvidence);
    ExpectError(BuildRndvDone(pool, {}, Forward()), RndvProtocolErrorCode::kInvalidEvidence);
}

TEST(RndvProtocolTest, ForwardBuildersRequireExactReceiverDestination) {
    for (unsigned scenario = 0; scenario < 5; ++scenario) {
        auto target = Forward();
        switch (scenario) {
        case 0:
            ++target.runtime;
            break;
        case 1:
            ++target.receive_domain;
            break;
        case 2:
            ++target.session;
            break;
        case 3:
            ++target.token.slot;
            break;
        case 4:
            ++target.token.generation;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(BuildRndvRts(Transfer(), target));
    }
}

TEST(RndvProtocolTest, GrantReplyRequiresOriginalSenderDestination) {
    auto wrong = Reverse();
    wrong.runtime = Binding().receiver_runtime;
    EXPECT_FALSE(BuildRndvGrant(Identity(), wrong));
    for (unsigned scenario = 0; scenario < 4; ++scenario) {
        auto target = Reverse();
        switch (scenario) {
        case 0:
            target.runtime = 0;
            break;
        case 1:
            target.receive_domain = 0;
            break;
        case 2:
            target.session = 0;
            break;
        case 3:
            target.token.generation = 0;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(BuildRndvGrant(Identity(), target));
    }
}

TEST(RndvProtocolTest, BuildersRejectMalformedTransferBeforeProducingFrame) {
    for (unsigned scenario = 0; scenario < 9; ++scenario) {
        auto transfer = Transfer();
        switch (scenario) {
        case 0:
            transfer.binding.sender_runtime = 0;
            break;
        case 1:
            transfer.binding.receiver_runtime = 0;
            break;
        case 2:
            transfer.binding.receive_domain = 0;
            break;
        case 3:
            transfer.binding.session = 0;
            break;
        case 4:
            transfer.binding.token.generation = 0;
            break;
        case 5:
            transfer.transfer_id = 0;
            break;
        case 6:
            transfer.bytes = 0;
            break;
        case 7:
            transfer.seq = std::numeric_limits<std::uint32_t>::max();
            break;
        case 8:
            transfer.binding.token = {};
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(BuildRndvRts(transfer, Forward()));
        auto grant = Identity();
        grant.transfer = transfer;
        EXPECT_FALSE(BuildRndvGrant(grant, Reverse()));
    }
}

TEST(RndvProtocolTest, BuildersRejectMalformedGrantRangeAndGeneration) {
    for (unsigned scenario = 0; scenario < 5; ++scenario) {
        auto grant = Identity();
        switch (scenario) {
        case 0:
            grant.arena.id = 0;
            break;
        case 1:
            grant.arena.generation = 0;
            break;
        case 2:
            grant.generation = 0;
            break;
        case 3:
            grant.target_address = 0;
            break;
        case 4:
            grant.target_address = std::numeric_limits<std::uint64_t>::max() - grant.transfer.bytes + 1;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(BuildRndvGrant(grant, Reverse()));
    }
}

TEST(RndvProtocolTest, GrantSlotLimitAppliesEvenWhenWireEncodingIsCanonical) {
    auto identity = Identity();
    identity.slot = GrantManager::kMaxWindow - 1;
    const auto input = BuildRndvGrant(identity, Reverse());
    ASSERT_TRUE(input);
    auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto reverse = ReceiveBinding(true);
    EXPECT_TRUE(ValidateRndvGrant(*decoded, reverse, reverse.context, identity.transfer, identity.arena));
    ++identity.slot;
    ExpectError(BuildRndvGrant(identity, Reverse()), RndvProtocolErrorCode::kInvalidArgument);
    // uint32 的 wire 值仍可规范编码，但不能指向实现容量之外的授权槽。
    auto malformed = *input;
    malformed.rndv.grant_slot = identity.slot;
    decoded = RoundTrip(malformed);
    ASSERT_TRUE(decoded);
    ExpectError(ValidateRndvGrant(*decoded, reverse, reverse.context, identity.transfer, identity.arena),
                RndvProtocolErrorCode::kWrongGrant);
    malformed.control = WireControlType::kRndvDone;
    malformed.session = Forward().session;
    malformed.token = Forward().token;
    decoded = RoundTrip(malformed);
    ASSERT_TRUE(decoded);
    const auto forward = ReceiveBinding();
    ExpectError(ValidateRndvDone(*decoded, forward, forward.context, identity), RndvProtocolErrorCode::kWrongGrant);
}

TEST(RndvProtocolTest, ValidatorsRejectWrongObservedSourceWithoutChangingExpectedIdentity) {
    const auto input = BuildRndvRts(Transfer(), Forward());
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding();
    for (unsigned scenario = 0; scenario < 4; ++scenario) {
        auto observed = route.context;
        switch (scenario) {
        case 0:
            ++observed.peer_runtime;
            break;
        case 1:
            ++observed.receive_domain;
            break;
        case 2:
            ++observed.session;
            break;
        case 3:
            ++observed.version;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(ValidateRndvRts(*decoded, route, observed, Binding()));
    }
}

TEST(RndvProtocolTest, ValidatorsCheckFullOriginalBindingEvenWithValidRoute) {
    const auto input = BuildRndvRts(Transfer(), Forward());
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding();
    for (unsigned scenario = 0; scenario < 6; ++scenario) {
        auto expected = Binding();
        switch (scenario) {
        case 0:
            ++expected.sender_runtime;
            break;
        case 1:
            ++expected.receiver_runtime;
            break;
        case 2:
            ++expected.receive_domain;
            break;
        case 3:
            ++expected.session;
            break;
        case 4:
            ++expected.token.slot;
            break;
        case 5:
            ++expected.token.generation;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(ValidateRndvRts(*decoded, route, route.context, expected));
    }
}

TEST(RndvProtocolTest, GrantValidatorChecksFullLocalRequestAndAuthenticatedArena) {
    const auto input = BuildRndvGrant(Identity(), Reverse());
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding(true);
    for (unsigned scenario = 0; scenario < 11; ++scenario) {
        auto expected = Transfer();
        auto arena = Identity().arena;
        switch (scenario) {
        case 0:
            ++expected.binding.sender_runtime;
            break;
        case 1:
            ++expected.binding.receiver_runtime;
            break;
        case 2:
            ++expected.binding.receive_domain;
            break;
        case 3:
            ++expected.binding.session;
            break;
        case 4:
            ++expected.binding.token.slot;
            break;
        case 5:
            ++expected.binding.token.generation;
            break;
        case 6:
            ++expected.transfer_id;
            break;
        case 7:
            ++expected.seq;
            break;
        case 8:
            ++expected.bytes;
            break;
        case 9:
            ++arena.id;
            break;
        case 10:
            ++arena.generation;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(ValidateRndvGrant(*decoded, route, route.context, expected, arena));
    }
}

TEST(RndvProtocolTest, DoneValidatorChecksEveryPublishedGrantIdentityField) {
    const auto input = DoneFrame();
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    const auto route = ReceiveBinding();
    for (unsigned scenario = 0; scenario < 14; ++scenario) {
        auto expected = Identity();
        switch (scenario) {
        case 0:
            ++expected.transfer.binding.sender_runtime;
            break;
        case 1:
            ++expected.transfer.binding.receiver_runtime;
            break;
        case 2:
            ++expected.transfer.binding.receive_domain;
            break;
        case 3:
            ++expected.transfer.binding.session;
            break;
        case 4:
            ++expected.transfer.binding.token.slot;
            break;
        case 5:
            ++expected.transfer.binding.token.generation;
            break;
        case 6:
            ++expected.transfer.transfer_id;
            break;
        case 7:
            ++expected.transfer.seq;
            break;
        case 8:
            ++expected.transfer.bytes;
            break;
        case 9:
            ++expected.arena.id;
            break;
        case 10:
            ++expected.arena.generation;
            break;
        case 11:
            ++expected.slot;
            break;
        case 12:
            ++expected.generation;
            break;
        case 13:
            ++expected.target_address;
            break;
        }
        SCOPED_TRACE(scenario);
        EXPECT_FALSE(ValidateRndvDone(*decoded, route, route.context, expected));
    }
}

TEST(RndvProtocolTest, ValidatorsRejectOtherControlTypesAndMalformedPublicViews) {
    const auto rts = BuildRndvRts(Transfer(), Forward());
    const auto done = DoneFrame();
    ASSERT_TRUE(rts);
    ASSERT_TRUE(done);
    const auto rts_view = RoundTrip(*rts), done_view = RoundTrip(*done);
    ASSERT_TRUE(rts_view);
    ASSERT_TRUE(done_view);
    const auto route = ReceiveBinding();
    ExpectError(ValidateRndvDone(*rts_view, route, route.context, Identity()), RndvProtocolErrorCode::kWrongControl);
    ExpectError(ValidateRndvRts(*done_view, route, route.context, Binding()), RndvProtocolErrorCode::kWrongControl);
    // 即使调用者手工构造 view，不能绕过 codec 的保留字段与规范布局检查。
    auto malformed = *done_view;
    malformed.frame.reset_code = 1;
    EXPECT_FALSE(ValidateRndvDone(malformed, route, route.context, Identity()));
    malformed = *done_view;
    malformed.frame.rndv.grant_generation = 0;
    EXPECT_FALSE(ValidateRndvDone(malformed, route, route.context, Identity()));
}

TEST(RndvProtocolTest, ReverseGrantCannotMasqueradeAsSenderOriginatedControl) {
    const auto input = BuildRndvGrant(Identity(), Reverse());
    ASSERT_TRUE(input);
    const auto decoded = RoundTrip(*input);
    ASSERT_TRUE(decoded);
    auto route = ReceiveBinding(true);
    route.context.peer_runtime = Binding().sender_runtime;
    // 普通 route 校验仍通过，但该帧必须来自授权接收端，不能只检查路由自洽。
    EXPECT_TRUE(ValidateWireIdentity(*decoded, route, route.context));
    EXPECT_FALSE(ValidateRndvGrant(*decoded, route, route.context, Transfer(), Identity().arena));
}

TEST(RndvProtocolTest, ForwardControlCannotUseUnrelatedRouteDespiteConsistentOuterIdentity) {
    const auto input = BuildRndvRts(Transfer(), Forward());
    ASSERT_TRUE(input);
    auto altered = *input;
    auto route = ReceiveBinding();
    ++route.context.session;
    ++route.token.slot;
    altered.session = route.context.session;
    altered.token = route.token;
    auto decoded = RoundTrip(altered);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(ValidateWireIdentity(*decoded, route, route.context));
    EXPECT_FALSE(ValidateRndvRts(*decoded, route, route.context, Binding()));
}

TEST(RndvProtocolTest, BuildersCodecAndValidatorsDoNotAllocateOrFree) {
    const auto forward = ReceiveBinding(), reverse = ReceiveBinding(true);
    bool success = true;
    allocations = deallocations = 0;
    count_allocations = true;
    for (unsigned iteration = 0; iteration < 32; ++iteration) {
        const auto rts = BuildRndvRts(Transfer(), Forward());
        const auto grant = BuildRndvGrant(Identity(), Reverse());
        const auto done = DoneFrame();
        if (!rts || !grant || !done) {
            success = false;
            break;
        }
        const auto a = RoundTrip(*rts), b = RoundTrip(*grant), c = RoundTrip(*done);
        if (!a || !b || !c || !ValidateRndvRts(*a, forward, forward.context, Binding()) ||
            !ValidateRndvGrant(*b, reverse, reverse.context, Transfer(), Identity().arena) ||
            !ValidateRndvDone(*c, forward, forward.context, Identity())) {
            success = false;
            break;
        }
    }
    count_allocations = false;
    EXPECT_TRUE(success);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
}
} // namespace
} // namespace connection
} // namespace kbsocket
