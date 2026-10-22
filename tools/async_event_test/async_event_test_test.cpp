// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/async_event_test/async_event_test.hpp"

#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::Return;

class JfrLimitEventsTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_status_t, Get, (urma_context_t*, urma_async_event_t*));
    MOCK_METHOD(void, Ack, (urma_async_event_t*));

protected:
    void SetUp() override {
        active_ = this;
        ASSERT_EQ(::pipe(pipe_.data()), 0);
        ctx_.async_fd = pipe_[0];
        event_.urma_ctx = &ctx_;
        event_.element.jfr = &jfr_;
        event_.event_type = URMA_EVENT_JFR_LIMIT;
        event_.priv = &cookie_;
    }
    void TearDown() override {
        ::close(pipe_[0]);
        ::close(pipe_[1]);
        active_ = nullptr;
    }
    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions functions;
        functions.get_async_event = [](auto* ctx, auto* event) { return active_->Get(ctx, event); };
        functions.ack_async_event = [](auto* event) { active_->Ack(event); };
        return functions;
    }
    void MakeReadable() {
        const char byte = 'x';
        ASSERT_EQ(::write(pipe_[1], &byte, 1), 1);
    }
    void ExpectRetrievedOriginalEvent() {
        EXPECT_CALL(*this, Get(&ctx_, _)).WillOnce([this](auto*, auto* event) {
            char byte;
            EXPECT_EQ(::read(pipe_[0], &byte, 1), 1);
            *event = event_;
            retrieved_ = event;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Ack(_)).WillOnce([this](auto* event) {
            // 连同 provider 私有信息一起确认原对象；错误事件也不能改写成“本 JFR”的事件再 Ack。
            EXPECT_EQ(event, retrieved_);
            EXPECT_EQ(event->urma_ctx, event_.urma_ctx);
            EXPECT_EQ(event->element.jfr, event_.element.jfr);
            EXPECT_EQ(event->event_type, event_.event_type);
            EXPECT_EQ(event->priv, event_.priv);
        });
    }
    static inline JfrLimitEventsTest* active_ = nullptr;
    std::array<int, 2> pipe_{};
    urma_context_t ctx_{};
    urma_jfr_t jfr_{};
    urma_async_event_t event_{};
    urma_async_event_t* retrieved_ = nullptr;
    int cookie_ = 7;
};

TEST_F(JfrLimitEventsTest, UnreadableFdNeverCallsBlockingGetOrAck) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    EXPECT_CALL(*this, Get(_, _)).Times(0);
    EXPECT_CALL(*this, Ack(_)).Times(0);
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 0U);
    EXPECT_EQ(stats.get_calls, 0U);
    EXPECT_EQ(stats.acknowledged, 0U);
}

TEST_F(JfrLimitEventsTest, ValidLimitEventIsAcknowledgedExactlyOnce) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    MakeReadable();
    ExpectRetrievedOriginalEvent();
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 1U);
    EXPECT_EQ(stats.retrieved, 1U);
    EXPECT_EQ(stats.acknowledged, 1U);
    EXPECT_EQ(stats.limit_events, 1U);
    EXPECT_TRUE(stats.last_context_matches);
    EXPECT_TRUE(stats.last_jfr_matches);
}

TEST_F(JfrLimitEventsTest, UnknownTypeIsAcknowledgedBeforeReportingMismatch) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    event_.event_type = static_cast<urma_async_event_type_t>(123);
    MakeReadable();
    ExpectRetrievedOriginalEvent();
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    EXPECT_EQ(stats.last_type, 123);
    EXPECT_EQ(stats.acknowledged, 1U);
    EXPECT_EQ(stats.limit_events, 0U);
}

TEST_F(JfrLimitEventsTest, WrongContextIsAcknowledgedWithoutReplacingItsOwnership) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    urma_context_t other{};
    event_.urma_ctx = &other;
    MakeReadable();
    ExpectRetrievedOriginalEvent();
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    EXPECT_FALSE(stats.last_context_matches);
    EXPECT_EQ(stats.acknowledged, 1U);
    EXPECT_EQ(stats.limit_events, 0U);
}

TEST_F(JfrLimitEventsTest, WrongJfrIsAcknowledgedWithoutDereferencingForeignPointer) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    event_.element.jfr = reinterpret_cast<urma_jfr_t*>(static_cast<std::uintptr_t>(1));
    MakeReadable();
    ExpectRetrievedOriginalEvent();
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    EXPECT_FALSE(stats.last_jfr_matches);
    EXPECT_EQ(stats.acknowledged, 1U);
    EXPECT_EQ(stats.limit_events, 0U);
}

TEST_F(JfrLimitEventsTest, FailedGetNeverAcknowledgesUnownedEvent) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    MakeReadable();
    EXPECT_CALL(*this, Get(&ctx_, _)).WillOnce(Return(URMA_FAIL));
    EXPECT_CALL(*this, Ack(_)).Times(0);
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_FALSE(result);
    EXPECT_EQ(stats.last_get_status, URMA_FAIL);
    EXPECT_EQ(stats.get_calls, 1U);
    EXPECT_EQ(stats.retrieved, 0U);
    EXPECT_EQ(stats.acknowledged, 0U);
}

TEST_F(JfrLimitEventsTest, ContinuousReadableFdHasBoundedEventConsumption) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    MakeReadable();
    // 故意不消费 pipe，让每次检查都可读；采集仍须有界，不能被事件风暴困住。
    EXPECT_CALL(*this, Get(&ctx_, _)).Times(64).WillRepeatedly([this](auto*, auto* event) {
        *event = event_;
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Ack(_)).Times(64);
    JfrLimitEventStats stats;
    auto result = PollJfrLimitEvents(&ctx_, &jfr_, stats);
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 64U);
    EXPECT_EQ(stats.limit_events, 64U);
    EXPECT_EQ(stats.acknowledged, 64U);
}

TEST_F(JfrLimitEventsTest, ClosedFdFailsWithoutGet) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_EQ(::close(pipe_[1]), 0);
    pipe_[1] = -1;
    EXPECT_CALL(*this, Get(_, _)).Times(0);
    EXPECT_CALL(*this, Ack(_)).Times(0);
    JfrLimitEventStats stats;
    EXPECT_FALSE(PollJfrLimitEvents(&ctx_, &jfr_, stats));
}

TEST(JfrLimitProtocolTest, BoundsWatermarkExperimentAndWatchdogInputs) {
    EXPECT_TRUE(ValidateJfrLimitOptions({}));
    EXPECT_TRUE(ValidateJfrLimitOptions({.rx_depth = 256, .bytes = 32, .observe_ms = 1, .timeout_ms = 100}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.rx_depth = 64}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.rx_depth = 257}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.threshold = 8}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.bytes = 31}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.bytes = 4097}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.observe_ms = 0}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.observe_ms = 1001}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidateJfrLimitOptions({.observe_ms = 100, .timeout_ms = 100}));
}

TEST(JfrLimitProtocolTest, HelloRoundTripUsesNetworkOrderAndKeepsTimeoutLocal) {
    JfrLimitHello hello{.options = {.bytes = 64, .observe_ms = 23}, .server = true};
    hello.endpoint.id = 0x01020304;
    hello.endpoint.eid.raw[15] = 42;
    const auto wire = EncodeJfrLimitHello(hello);
    EXPECT_EQ(wire[11], std::byte{1});
    EXPECT_EQ(wire[15], std::byte{128});
    EXPECT_EQ(wire[19], std::byte{64});
    EXPECT_EQ(wire[23], std::byte{64});
    EXPECT_EQ(wire[27], std::byte{23});
    EXPECT_EQ(wire[43], std::byte{42});
    EXPECT_EQ(wire[44], std::byte{1});
    EXPECT_EQ(wire[47], std::byte{4});
    auto decoded = DecodeJfrLimitHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->endpoint.id, hello.endpoint.id);
    EXPECT_EQ(decoded->endpoint.eid.raw[15], 42);
    EXPECT_EQ(decoded->options.observe_ms, 23U);
    hello.options.timeout_ms = 2000;
    EXPECT_EQ(EncodeJfrLimitHello(hello), wire);
}

TEST(JfrLimitProtocolTest, HelloRejectsForeignProtocolRoleAndMalformedOptions) {
    JfrLimitHello hello{.server = true};
    hello.endpoint.id = 1;
    const auto valid = EncodeJfrLimitHello(hello);
    ASSERT_TRUE(DecodeJfrLimitHello(valid));
    // 错工具、错版本、非法角色都应在远端导入与 WR 投递之前失败。
    for (const auto offset : {0U, 7U, 11U, 12U, 16U, 20U, 24U}) {
        auto corrupted = valid;
        corrupted[offset] = std::byte{0xff};
        EXPECT_FALSE(DecodeJfrLimitHello(corrupted)) << "offset=" << offset;
    }
    hello.endpoint.id = 0;
    EXPECT_FALSE(DecodeJfrLimitHello(EncodeJfrLimitHello(hello)));
}

TEST(JfrLimitProtocolTest, PeerMustMatchOppositeRoleAndAllTrafficParameters) {
    const JfrLimitHello local{};
    JfrLimitHello peer{.server = true};
    peer.options.timeout_ms = 2000;
    EXPECT_TRUE(ValidateJfrLimitPeer(local, peer));
    peer.server = false;
    EXPECT_FALSE(ValidateJfrLimitPeer(local, peer));
    peer = {.server = true};
    peer.options.rx_depth = 256;
    EXPECT_FALSE(ValidateJfrLimitPeer(local, peer));
    peer = {.server = true};
    peer.options.threshold = 32;
    EXPECT_FALSE(ValidateJfrLimitPeer(local, peer));
    peer = {.server = true};
    peer.options.bytes = 64;
    EXPECT_FALSE(ValidateJfrLimitPeer(local, peer));
    peer = {.server = true};
    peer.options.observe_ms = 10;
    EXPECT_FALSE(ValidateJfrLimitPeer(local, peer));
}
} // namespace
} // namespace tools
} // namespace kbsocket
