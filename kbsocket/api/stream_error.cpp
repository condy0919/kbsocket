// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_error.hpp"

#include <cerrno>
#include <cstdint>
#include <limits>

namespace kbsocket {
namespace api {
namespace {
template <class Code>
kbs_stream_error NativeError(int code, std::uint32_t origin, Code native, int detail = 0, int underlying = 0) noexcept {
    return {code, origin, static_cast<std::int32_t>(native), detail, underlying, 0};
}
int TcpCode(int detail, int underlying) noexcept {
    using Code = tcp::TcpErrorCode;
    switch (detail) {
    case static_cast<int>(Code::kInvalidArgument):
    case static_cast<int>(Code::kNotNonblocking):
        return KBS_STREAM_INVALID_ARGUMENT;
    case static_cast<int>(Code::kInUse):
        return KBS_STREAM_BUSY;
    case static_cast<int>(Code::kNotConnected):
        return KBS_STREAM_NOT_READY;
    case static_cast<int>(Code::kWouldBlock):
        return KBS_STREAM_WOULD_BLOCK;
    case static_cast<int>(Code::kWriteClosed):
        return KBS_STREAM_CLOSED;
    case static_cast<int>(Code::kTimeout):
        return KBS_STREAM_TIMEOUT;
    case static_cast<int>(Code::kCancelled):
        return KBS_STREAM_CANCELLED;
    case static_cast<int>(Code::kSystemError):
        if (underlying == ECONNRESET || underlying == ECONNABORTED || underlying == EPIPE || underlying == ENOTCONN) {
            return KBS_STREAM_PEER_RESET;
        }
        if (underlying == ETIMEDOUT) {
            return KBS_STREAM_TIMEOUT;
        }
        return KBS_STREAM_TRANSPORT;
    }
    return KBS_STREAM_TRANSPORT;
}
int HandshakeCode(int detail) noexcept {
    using Code = control::ConnectionHandshakeErrorCode;
    switch (detail) {
    case static_cast<int>(Code::kInvalidArgument):
        return KBS_STREAM_INVALID_ARGUMENT;
    case static_cast<int>(Code::kInUse):
        return KBS_STREAM_BUSY;
    case static_cast<int>(Code::kNotReady):
        return KBS_STREAM_NOT_READY;
    case static_cast<int>(Code::kCancelled):
        return KBS_STREAM_CANCELLED;
    case static_cast<int>(Code::kImport):
    case static_cast<int>(Code::kNoCommonTransport):
        return KBS_STREAM_TRANSPORT;
    default:
        // ConnectionError 已扁平化 Channel 子类，不能把未知子类猜成 Timeout 或 PeerReset。
        return KBS_STREAM_PROTOCOL;
    }
}
int TxCode(int detail) noexcept {
    using Code = connection::EagerTxErrorCode;
    switch (detail) {
    case static_cast<int>(Code::kInvalidArgument):
        return KBS_STREAM_INVALID_ARGUMENT;
    case static_cast<int>(Code::kNotReady):
        return KBS_STREAM_NOT_READY;
    case static_cast<int>(Code::kInUse):
        return KBS_STREAM_BUSY;
    case static_cast<int>(Code::kNoMemory):
        return KBS_STREAM_NO_MEMORY;
    case static_cast<int>(Code::kWouldBlock):
        return KBS_STREAM_WOULD_BLOCK;
    case static_cast<int>(Code::kStopped):
        return KBS_STREAM_STOPPED;
    case static_cast<int>(Code::kRetained):
    case static_cast<int>(Code::kAcceptanceUnknown):
        return KBS_STREAM_RETAINED;
    default:
        return KBS_STREAM_TRANSPORT;
    }
}
int RxCode(int detail) noexcept {
    using Code = connection::EagerRxErrorCode;
    switch (detail) {
    case static_cast<int>(Code::kInvalidArgument):
        return KBS_STREAM_INVALID_ARGUMENT;
    case static_cast<int>(Code::kNotReady):
        return KBS_STREAM_NOT_READY;
    case static_cast<int>(Code::kInUse):
    case static_cast<int>(Code::kProofRequired):
        return KBS_STREAM_BUSY;
    case static_cast<int>(Code::kNoMemory):
        return KBS_STREAM_NO_MEMORY;
    case static_cast<int>(Code::kStopped):
        return KBS_STREAM_STOPPED;
    case static_cast<int>(Code::kRetained):
        return KBS_STREAM_RETAINED;
    case static_cast<int>(Code::kReceiveFailed):
    case static_cast<int>(Code::kRefillFailed):
        return KBS_STREAM_TRANSPORT;
    default:
        return KBS_STREAM_PROTOCOL;
    }
}
kbs_stream_error ProgressError(const runtime::ProgressError& error) noexcept {
    using Code = runtime::ProgressErrorCode;
    int code = KBS_STREAM_TRANSPORT;
    switch (error.code) {
    case Code::kInvalidArgument:
        code = KBS_STREAM_INVALID_ARGUMENT;
        break;
    case Code::kNotReady:
        code = KBS_STREAM_NOT_READY;
        break;
    case Code::kInUse:
        code = KBS_STREAM_BUSY;
        break;
    case Code::kRetained:
        code = KBS_STREAM_RETAINED;
        break;
    case Code::kResourceExhausted:
        code = KBS_STREAM_EXHAUSTED;
        break;
    default:
        break;
    }
    return NativeError(code, KBS_STREAM_ORIGIN_PROGRESS, error.code, error.detail);
}
kbs_stream_error SourceError(control::RawSourceAdmissionError error) noexcept {
    using Code = control::RawSourceAdmissionError;
    int code = KBS_STREAM_PROTOCOL;
    switch (error) {
    case Code::kInvalidArgument:
        code = KBS_STREAM_INVALID_ARGUMENT;
        break;
    case Code::kNotReady:
        code = KBS_STREAM_NOT_READY;
        break;
    case Code::kInUse:
        code = KBS_STREAM_BUSY;
        break;
    case Code::kRetained:
        code = KBS_STREAM_RETAINED;
        break;
    case Code::kExhausted:
        code = KBS_STREAM_EXHAUSTED;
        break;
    default:
        break;
    }
    return NativeError(code, KBS_STREAM_ORIGIN_SOURCE, error);
}
std::uint32_t StateCode(connection::ConnectionState state) noexcept {
    using State = connection::ConnectionState;
    switch (state) {
    case State::kIdle:
        return KBS_STREAM_IDLE;
    case State::kNegotiating:
        return KBS_STREAM_NEGOTIATING;
    case State::kPreparing:
        return KBS_STREAM_PREPARING;
    case State::kWaitingReady:
        return KBS_STREAM_WAITING_READY;
    case State::kRawActive:
        return KBS_STREAM_RAW_ACTIVE;
    case State::kTcpActive:
        return KBS_STREAM_TCP_ACTIVE;
    case State::kFailed:
        return KBS_STREAM_FAILED;
    case State::kClosing:
        return KBS_STREAM_CLOSING;
    case State::kClosed:
        return KBS_STREAM_STATE_CLOSED;
    }
    return KBS_STREAM_FAILED;
}
} // namespace

kbs_stream_error StreamError(int code) noexcept {
    return {code, KBS_STREAM_ORIGIN_API, 0, 0, 0, 0};
}
kbs_stream_error MapStreamError(const connection::ConnectionError& error) noexcept {
    using Code = connection::ConnectionErrorCode;
    int code = KBS_STREAM_TRANSPORT;
    switch (error.code) {
    case Code::kInvalidArgument:
        code = KBS_STREAM_INVALID_ARGUMENT;
        break;
    case Code::kNotReady:
        code = KBS_STREAM_NOT_READY;
        break;
    case Code::kInUse:
        code = KBS_STREAM_BUSY;
        break;
    case Code::kWouldBlock:
        code = KBS_STREAM_WOULD_BLOCK;
        break;
    case Code::kClosed:
        code = KBS_STREAM_CLOSED;
        break;
    case Code::kCancelled:
        code = KBS_STREAM_CANCELLED;
        break;
    case Code::kPeerReset:
    case Code::kControlClosed:
        code = KBS_STREAM_PEER_RESET;
        break;
    case Code::kRetained:
        code = KBS_STREAM_RETAINED;
        break;
    case Code::kLaneUpdateTimeout:
        code = KBS_STREAM_TIMEOUT;
        break;
    case Code::kHandshake:
        code = HandshakeCode(error.detail);
        break;
    case Code::kTx:
        code = TxCode(error.detail);
        break;
    case Code::kRx:
        code = RxCode(error.detail);
        break;
    case Code::kTcp:
        code = TcpCode(error.detail, error.underlying_error);
        break;
    case Code::kCredit:
    case Code::kControlProtocol:
    case Code::kSourceAdmission:
        code = KBS_STREAM_PROTOCOL;
        break;
    case Code::kLaneChanged:
        break;
    }
    return NativeError(code, KBS_STREAM_ORIGIN_CONNECTION, error.code, error.detail, error.underlying_error);
}
kbs_stream_error MapStreamError(const runtime::ConnectionOwnerError& error) noexcept {
    using Code = runtime::ConnectionOwnerErrorCode;
    auto mapped = NativeError(KBS_STREAM_TRANSPORT, KBS_STREAM_ORIGIN_OWNER, error.code);
    if ((error.code == Code::kConnection || error.code == Code::kRetained) && error.connection) {
        mapped = MapStreamError(*error.connection);
    } else if ((error.code == Code::kProgress || error.code == Code::kRetained) && error.progress) {
        mapped = ProgressError(*error.progress);
    } else if ((error.code == Code::kSource || error.code == Code::kRetained) && error.source) {
        mapped = SourceError(*error.source);
    }
    switch (error.code) {
    case Code::kInvalidArgument:
        mapped.code = KBS_STREAM_INVALID_ARGUMENT;
        break;
    case Code::kNotReady:
        mapped.code = KBS_STREAM_NOT_READY;
        break;
    case Code::kInUse:
        mapped.code = KBS_STREAM_BUSY;
        break;
    case Code::kNoMemory:
        mapped.code = KBS_STREAM_NO_MEMORY;
        break;
    case Code::kExhausted:
        mapped.code = KBS_STREAM_EXHAUSTED;
        break;
    case Code::kStopped:
        mapped.code = KBS_STREAM_STOPPED;
        break;
    case Code::kInvalidHandle:
        mapped.code = KBS_STREAM_INVALID_HANDLE;
        break;
    case Code::kRetained:
        // 上层保留决定优先，诊断仍保留首个底层失败，不能被普通 Busy/Transport 降级。
        mapped.code = KBS_STREAM_RETAINED;
        break;
    default:
        break;
    }
    return mapped;
}
kbs_stream_error MapStreamError(const runtime::RawStackError& error) noexcept {
    using Code = runtime::RawStackErrorCode;
    auto mapped = NativeError(KBS_STREAM_TRANSPORT, KBS_STREAM_ORIGIN_STACK, error.code);
    if (error.connection) {
        mapped = MapStreamError(*error.connection);
    } else if (error.progress) {
        mapped = ProgressError(*error.progress);
    } else if (error.domain) {
        mapped.detail = static_cast<std::int32_t>(error.domain->code);
        mapped.underlying = error.domain->provider_error;
    } else if (error.memory) {
        mapped.detail = static_cast<std::int32_t>(error.memory->code);
        mapped.underlying = error.memory->provider_error;
    } else if (error.import) {
        mapped.detail = static_cast<std::int32_t>(error.import->code);
        mapped.underlying = error.import->provider_error;
    } else if (error.session) {
        mapped.detail = static_cast<std::int32_t>(error.session->code);
        mapped.underlying = error.session->provider_error;
    } else if (error.session_close) {
        mapped.detail = static_cast<std::int32_t>(error.session_close->code);
        mapped.underlying = error.session_close->provider_error;
    } else if (error.mailbox) {
        mapped.detail = static_cast<std::int32_t>(error.mailbox->code);
        mapped.underlying = error.mailbox->system_error;
    }
    switch (error.code) {
    case Code::kInvalidArgument:
        mapped.code = KBS_STREAM_INVALID_ARGUMENT;
        break;
    case Code::kNotReady:
        mapped.code = KBS_STREAM_NOT_READY;
        break;
    case Code::kInUse:
        mapped.code = KBS_STREAM_BUSY;
        break;
    case Code::kNoMemory:
        mapped.code = KBS_STREAM_NO_MEMORY;
        break;
    case Code::kRetained:
        mapped.code = KBS_STREAM_RETAINED;
        break;
    default:
        break;
    }
    return mapped;
}
std::uint32_t StreamState(connection::ConnectionState state) noexcept {
    return StateCode(state);
}
kbs_stream_status StreamStatus(const runtime::OwnedConnectionStatus& status) noexcept {
    kbs_stream_status result{};
    result.state = StateCode(status.state);
    result.lane_update_pending = status.lane_update_pending ? 1u : 0u;
    if (status.failure) {
        result.failure = MapStreamError(*status.failure);
    }
    // 已接受 DATA 从 queued 移到 in_flight，两者互斥；控制 WR 仅计入 in_flight。
    const auto queued = static_cast<std::uint64_t>(status.tx.queued);
    const auto in_flight = static_cast<std::uint64_t>(status.tx.in_flight);
    const auto max = std::numeric_limits<std::uint64_t>::max();
    result.tx_pending = in_flight > max - queued ? max : queued + in_flight;
    result.rx_pending = status.rx_pending;
    result.retained_sources = status.retained_sources;
    return result;
}
} // namespace api
} // namespace kbsocket
