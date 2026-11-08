// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_error.hpp"

#include <array>
#include <cerrno>
#include <cstdint>

#include "gtest/gtest.h"

namespace kbsocket {
namespace api {
namespace {
using ConnectionCode = connection::ConnectionErrorCode;
using OwnerCode = runtime::ConnectionOwnerErrorCode;
using StackCode = runtime::RawStackErrorCode;

TEST(StreamErrorTest, ApiErrorHasNoInventedNativeEvidence) {
    const auto error = StreamError(KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(error.code, KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(error.origin, KBS_STREAM_ORIGIN_API);
    EXPECT_EQ(error.native_code, 0);
    EXPECT_EQ(error.detail, 0);
    EXPECT_EQ(error.underlying, 0);
    EXPECT_EQ(error.reserved, 0u);
}

TEST(StreamErrorTest, PreservesProviderCompletionWithoutTreatingItAsErrno) {
    // status 9 的错误需要连接层隔离；应用分类不应把它错误解释为系统 errno 9。
    const connection::ConnectionError input{ConnectionCode::kTx,
                                            static_cast<int>(connection::EagerTxErrorCode::kCompletion), 9};
    const auto error = MapStreamError(input);
    EXPECT_EQ(error.code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(error.origin, KBS_STREAM_ORIGIN_CONNECTION);
    EXPECT_EQ(error.native_code, static_cast<int>(ConnectionCode::kTx));
    EXPECT_EQ(error.detail, input.detail);
    EXPECT_EQ(error.underlying, 9);
    EXPECT_EQ(error.reserved, 0u);
}

TEST(StreamErrorTest, TcpLifecycleAndSystemFailuresHaveDistinctCategories) {
    using Code = tcp::TcpErrorCode;
    struct Case {
        Code native;
        int system;
        int expected;
    };
    const std::array cases{
        Case{Code::kWouldBlock, EAGAIN, KBS_STREAM_WOULD_BLOCK},
        Case{Code::kWriteClosed, 0, KBS_STREAM_CLOSED},
        Case{Code::kCancelled, 0, KBS_STREAM_CANCELLED},
        Case{Code::kTimeout, 0, KBS_STREAM_TIMEOUT},
        Case{Code::kNotNonblocking, 0, KBS_STREAM_INVALID_ARGUMENT},
        Case{Code::kSystemError, ECONNRESET, KBS_STREAM_PEER_RESET},
        Case{Code::kSystemError, EPIPE, KBS_STREAM_PEER_RESET},
        Case{Code::kSystemError, ETIMEDOUT, KBS_STREAM_TIMEOUT},
        Case{Code::kSystemError, EIO, KBS_STREAM_TRANSPORT},
    };
    for (const auto& test : cases) {
        const auto error = MapStreamError(
            connection::ConnectionError{ConnectionCode::kTcp, static_cast<int>(test.native), test.system});
        EXPECT_EQ(error.code, test.expected);
        EXPECT_EQ(error.detail, static_cast<int>(test.native));
        EXPECT_EQ(error.underlying, test.system);
    }
}

TEST(StreamErrorTest, FlattenedHandshakeCannotRecoverMissingChannelSubtype) {
    // 当前 ConnectionError 丢失 HandshakeChannel 子类：不从零 errno 猜测 Timeout/PeerClosed。
    const auto channel = MapStreamError(connection::ConnectionError{
        ConnectionCode::kHandshake, static_cast<int>(control::ConnectionHandshakeErrorCode::kChannel), 0});
    EXPECT_EQ(channel.code, KBS_STREAM_PROTOCOL);
    EXPECT_EQ(channel.detail, static_cast<int>(control::ConnectionHandshakeErrorCode::kChannel));
    const auto cancelled = MapStreamError(connection::ConnectionError{
        ConnectionCode::kHandshake, static_cast<int>(control::ConnectionHandshakeErrorCode::kCancelled), 0});
    EXPECT_EQ(cancelled.code, KBS_STREAM_CANCELLED);
    EXPECT_EQ(MapStreamError(connection::ConnectionError{ConnectionCode::kLaneUpdateTimeout}).code, KBS_STREAM_TIMEOUT);
}

TEST(StreamErrorTest, UnknownDetailDoesNotWrapIntoKnownSmallEnum) {
    // detail 是 int，256 不能截断成 uint8_t 后冒充首个合法枚举。
    EXPECT_EQ(MapStreamError(connection::ConnectionError{ConnectionCode::kTcp, 256, ECONNRESET}).code,
              KBS_STREAM_TRANSPORT);
    EXPECT_EQ(MapStreamError(connection::ConnectionError{ConnectionCode::kTx, -1, 9}).code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(MapStreamError(connection::ConnectionError{ConnectionCode::kHandshake, 256}).code, KBS_STREAM_PROTOCOL);
    EXPECT_EQ(MapStreamError(connection::ConnectionError{static_cast<ConnectionCode>(255), 17, 9}).code,
              KBS_STREAM_TRANSPORT);
}

TEST(StreamErrorTest, UnknownAcceptanceAndRetainedRemainDifferentFromBackpressure) {
    using Code = connection::EagerTxErrorCode;
    EXPECT_EQ(
        MapStreamError(connection::ConnectionError{ConnectionCode::kTx, static_cast<int>(Code::kWouldBlock)}).code,
        KBS_STREAM_WOULD_BLOCK);
    EXPECT_EQ(
        MapStreamError(connection::ConnectionError{ConnectionCode::kTx, static_cast<int>(Code::kAcceptanceUnknown)})
            .code,
        KBS_STREAM_RETAINED);
    EXPECT_EQ(MapStreamError(connection::ConnectionError{ConnectionCode::kRetained, 7}).code, KBS_STREAM_RETAINED);
    EXPECT_EQ(MapStreamError(connection::ConnectionError{ConnectionCode::kPeerReset}).code, KBS_STREAM_PEER_RESET);
}

TEST(StreamErrorTest, OwnerRetainedOverridesNestedClassificationAndKeepsCause) {
    // 首个完成错误是 Transport，但 owner 已决定保留，不能因透传子错误而丢掉保留语义。
    const auto error = MapStreamError(runtime::ConnectionOwnerError{
        .code = OwnerCode::kRetained,
        .connection = connection::ConnectionError{ConnectionCode::kTx,
                                                  static_cast<int>(connection::EagerTxErrorCode::kCompletion), 9}});
    EXPECT_EQ(error.code, KBS_STREAM_RETAINED);
    EXPECT_EQ(error.origin, KBS_STREAM_ORIGIN_CONNECTION);
    EXPECT_EQ(error.native_code, static_cast<int>(ConnectionCode::kTx));
    EXPECT_EQ(error.detail, static_cast<int>(connection::EagerTxErrorCode::kCompletion));
    EXPECT_EQ(error.underlying, 9);
}

TEST(StreamErrorTest, OwnerIdentityAndAvailabilityErrorsRemainLocal) {
    const auto invalid = MapStreamError(runtime::ConnectionOwnerError{.code = OwnerCode::kInvalidHandle});
    EXPECT_EQ(invalid.code, KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(invalid.origin, KBS_STREAM_ORIGIN_OWNER);
    EXPECT_EQ(invalid.native_code, static_cast<int>(OwnerCode::kInvalidHandle));
    EXPECT_EQ(MapStreamError(runtime::ConnectionOwnerError{.code = OwnerCode::kExhausted}).code, KBS_STREAM_EXHAUSTED);
    EXPECT_EQ(MapStreamError(runtime::ConnectionOwnerError{.code = OwnerCode::kStopped}).code, KBS_STREAM_STOPPED);
    EXPECT_EQ(MapStreamError(runtime::ConnectionOwnerError{.code = static_cast<OwnerCode>(255)}).code,
              KBS_STREAM_TRANSPORT);
}

TEST(StreamErrorTest, ProgressAndSourceDiagnosticsKeepTheirOrigin) {
    const auto progress = MapStreamError(runtime::ConnectionOwnerError{
        .code = OwnerCode::kProgress, .progress = runtime::ProgressError{runtime::ProgressErrorCode::kSystem, EIO, 2}});
    EXPECT_EQ(progress.code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(progress.origin, KBS_STREAM_ORIGIN_PROGRESS);
    EXPECT_EQ(progress.native_code, static_cast<int>(runtime::ProgressErrorCode::kSystem));
    EXPECT_EQ(progress.detail, EIO);
    const auto source = MapStreamError(runtime::ConnectionOwnerError{
        .code = OwnerCode::kSource, .source = control::RawSourceAdmissionError::kRetained});
    EXPECT_EQ(source.code, KBS_STREAM_RETAINED);
    EXPECT_EQ(source.origin, KBS_STREAM_ORIGIN_SOURCE);
    EXPECT_EQ(source.native_code, static_cast<int>(control::RawSourceAdmissionError::kRetained));
}

TEST(StreamErrorTest, StackPauseAndOriginalProviderFailureStayDiagnosable) {
    const auto paused = MapStreamError(runtime::RawStackError{.code = StackCode::kInUse, .context_index = 2});
    EXPECT_EQ(paused.code, KBS_STREAM_BUSY);
    EXPECT_EQ(paused.origin, KBS_STREAM_ORIGIN_STACK);
    const auto retained = MapStreamError(runtime::RawStackError{
        .code = StackCode::kRetained,
        .domain = runtime::RawDomainError{.code = runtime::RawDomainErrorCode::kReceive, .provider_error = -7}});
    EXPECT_EQ(retained.code, KBS_STREAM_RETAINED);
    EXPECT_EQ(retained.origin, KBS_STREAM_ORIGIN_STACK);
    EXPECT_EQ(retained.native_code, static_cast<int>(StackCode::kRetained));
    EXPECT_EQ(retained.detail, static_cast<int>(runtime::RawDomainErrorCode::kReceive));
    EXPECT_EQ(retained.underlying, -7);
    EXPECT_EQ(MapStreamError(runtime::RawStackError{.code = static_cast<StackCode>(255)}).code, KBS_STREAM_TRANSPORT);
}

TEST(StreamErrorTest, StatusCountsQueuedAndInflightWithoutAcceptedHistory) {
    // accepted/retired 是累计历史；pending 只加未提交与尚未退休两类当前责任。
    runtime::OwnedConnectionStatus input{};
    input.state = connection::ConnectionState::kRawActive;
    input.tx.accepted = 1000;
    input.tx.retired = 993;
    input.tx.queued = 3;
    input.tx.in_flight = 7;
    input.rx_pending = 5;
    input.retained_sources = 2;
    input.lane_update_pending = true;
    const auto status = StreamStatus(input);
    EXPECT_EQ(status.state, KBS_STREAM_RAW_ACTIVE);
    EXPECT_EQ(status.tx_pending, 10u);
    EXPECT_EQ(status.rx_pending, 5u);
    EXPECT_EQ(status.retained_sources, 2u);
    EXPECT_EQ(status.lane_update_pending, 1u);
    EXPECT_EQ(status.failure.code, 0);
    EXPECT_EQ(status.failure.underlying, 0);
}

TEST(StreamErrorTest, ClosingStatusPreservesFailureAndUnknownStateFailsClosed) {
    runtime::OwnedConnectionStatus input{};
    input.state = connection::ConnectionState::kClosing;
    input.failure = connection::ConnectionError{ConnectionCode::kTx, 15, 9};
    const auto status = StreamStatus(input);
    EXPECT_EQ(status.state, KBS_STREAM_CLOSING);
    EXPECT_EQ(status.failure.underlying, 9);
    input.state = connection::ConnectionState::kClosed;
    EXPECT_EQ(StreamStatus(input).state, KBS_STREAM_STATE_CLOSED);
    input.state = connection::ConnectionState::kTcpConnecting;
    EXPECT_EQ(StreamStatus(input).state, KBS_STREAM_TCP_CONNECTING);
    input.state = static_cast<connection::ConnectionState>(255);
    EXPECT_EQ(StreamStatus(input).state, KBS_STREAM_FAILED);
}
} // namespace
} // namespace api
} // namespace kbsocket
