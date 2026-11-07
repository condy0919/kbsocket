// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/connection.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <limits>

#include "kbsocket/control/tx_lane_snapshot.hpp"

namespace kbsocket {
namespace connection {
namespace {
auto Error(ConnectionErrorCode code, int detail = 0, int underlying = 0) noexcept {
    return std::unexpected(ConnectionError{code, detail, underlying});
}
ConnectionError HandshakeError(const control::ConnectionHandshakeError& error) noexcept {
    return {ConnectionErrorCode::kHandshake, static_cast<int>(error.code), error.underlying_error};
}
ConnectionError TxError(const EagerTxError& error) noexcept {
    return {error.code == EagerTxErrorCode::kWouldBlock ? ConnectionErrorCode::kWouldBlock : ConnectionErrorCode::kTx,
            static_cast<int>(error.code), error.provider_status};
}
ConnectionError RxError(const EagerRxError& error) noexcept {
    return {ConnectionErrorCode::kRx, static_cast<int>(error.code), error.provider_error};
}
ConnectionError TcpError(const tcp::TcpError& error) noexcept {
    return {error.code == tcp::TcpErrorCode::kWouldBlock ? ConnectionErrorCode::kWouldBlock : ConnectionErrorCode::kTcp,
            static_cast<int>(error.code), error.system_error};
}
ConnectionError LaneError(const control::LaneControlError& error) noexcept {
    using Code = control::LaneControlErrorCode;
    ConnectionErrorCode code = ConnectionErrorCode::kControlProtocol;
    if (error.code == Code::kLane) {
        code = ConnectionErrorCode::kLaneChanged;
    } else if (error.code == Code::kTimeout) {
        code = ConnectionErrorCode::kLaneUpdateTimeout;
    } else if (error.code == Code::kSource || error.code == Code::kUnauthorized) {
        code = ConnectionErrorCode::kSourceAdmission;
    } else if (error.code == Code::kChannel &&
               (error.detail == static_cast<int>(control::HandshakeErrorCode::kPeerClosed) ||
                error.detail == static_cast<int>(control::HandshakeErrorCode::kSystemError))) {
        code = ConnectionErrorCode::kControlClosed;
    }
    return {code, error.detail, error.system_error};
}
} // namespace

Connection::~Connection() {
    (void)Close();
}

std::expected<void, ConnectionError> Connection::Open(int fd, ConnectionTable& table,
                                                      const control::ConnectionHandshakeConfig& config,
                                                      std::uint64_t now_ns) noexcept {
    if (opened_once_) {
        return Error(ConnectionErrorCode::kInUse);
    }
    auto opened = handshake_.Open(fd, table, config, now_ns);
    if (!opened) {
        return std::unexpected(HandshakeError(opened.error()));
    }
    opened_once_ = true;
    state_ = ConnectionState::kNegotiating;
    return {};
}
std::expected<void, ConnectionError> Connection::OpenTcp(int fd) noexcept {
    if (opened_once_) {
        return Error(ConnectionErrorCode::kInUse);
    }
    auto opened = tcp_.Open(fd);
    if (!opened) {
        return std::unexpected(TcpError(opened.error()));
    }
    opened_once_ = true;
    state_ = ConnectionState::kTcpActive;
    return {};
}
std::expected<void, ConnectionError> Connection::PrepareRaw(const RawConnectionPreparation& prep) noexcept {
    if (state_ != ConnectionState::kPreparing || raw_prepared_) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    const auto* peer = handshake_.peer();
    if (!peer || !prep.receive || !prep.sender || !prep.slab || !prep.sender_credits || !prep.connection_id ||
        !prep.handshake.credits || !prep.handshake.imports || !prep.sources || !prep.sender->pool() ||
        prep.control_credit_target < 4 || !prep.lane_update_timeout_ns ||
        !prep.receive->Matches(*prep.handshake.credits, *handshake_.route().owner) ||
        prep.receive->context() != prep.handshake.local_context) {
        return Error(ConnectionErrorCode::kInvalidArgument);
    }
    if (!prep.peer_lanes_authorized) {
        return Error(ConnectionErrorCode::kSourceAdmission,
                     static_cast<int>(control::RawSourceAdmissionError::kUnauthorized));
    }
    auto lanes = control::ValidateLocalTxLanes(*prep.sender->pool(), handshake_.local());
    if (!lanes) {
        return std::unexpected(Fail({ConnectionErrorCode::kLaneChanged, static_cast<int>(lanes.error().code)}));
    }
    auto credit = prep.handshake.credits->PeerStats(prep.handshake.credit_peer);
    // 固定 Peer 控制配额与低水线分离；重建逻辑连接不能把当时剩余额度误当新配额。
    const auto control = prep.handshake.credits->stats().control;
    if (!credit || credit->max_control < prep.control_credit_target ||
        prep.control_credit_target > control.free + control.granted + control.unposted) {
        return Error(ConnectionErrorCode::kCredit);
    }
    raw_ = prep;
    tx_pool_ = prep.sender->pool();
    raw_.handshake.route_owner = this;
    auto prepared = handshake_.PrepareRaw(raw_.handshake);
    if (!prepared) {
        return std::unexpected(HandshakeError(prepared.error()));
    }
    raw_prepared_ = true;
    const auto& local = handshake_.local();
    const WireSessionBinding binding{
        {local.wire_version, peer->identity.runtime, local.identity.receive_domain, local.identity.session},
        local.identity.token,
        peer->credit_epoch};
    auto rx_open = rx_.Open(*prep.receive, handshake_.receive_window(), binding);
    if (!rx_open) {
        return std::unexpected(Fail(RxError(rx_open.error())));
    }
    rx_open_ = true;
    tx_session_ = {peer->identity.runtime, peer->identity.receive_domain, peer->identity.session};
    tx_identity_ = {peer->identity.runtime, peer->identity.receive_domain, peer->credit_epoch};
    EagerTxConfig tx_config;
    tx_config.local_context = prep.handshake.local_context;
    tx_config.connection_id = prep.connection_id;
    tx_config.destination = peer->identity.token;
    tx_config.session = tx_session_;
    tx_config.credit_identity = tx_identity_;
    tx_config.capacity = prep.tx_capacity;
    tx_config.advertised_credits = prep.handshake.credits;
    tx_config.advertised_peer = prep.handshake.credit_peer;
    auto tx_open = tx_.Open(tx_config, *prep.sender, *prep.slab, *prep.handshake.imports, handshake_.import_lease(),
                            *prep.sender_credits);
    if (!tx_open) {
        return std::unexpected(Fail(TxError(tx_open.error())));
    }
    tx_open_ = true;
    // 最后一次性安装来源；任何失败都不能发布 READY 或留下已提交的业务 WR。
    auto admitted = prep.sources->Prepare({.local = &local,
                                           .peer = peer,
                                           .context = prep.handshake.local_context,
                                           .buffers = prep.receive->buffers(),
                                           .context_incarnation = prep.handshake.local_context_incarnation,
                                           .local_domain = handshake_.route().owner->domain(),
                                           .credit_peer = prep.handshake.credit_peer,
                                           .peer_lanes_authorized = prep.peer_lanes_authorized});
    if (!admitted) {
        return std::unexpected(Fail({ConnectionErrorCode::kSourceAdmission, static_cast<int>(admitted.error())}));
    }
    const auto window = handshake_.receive_window().stats();
    last_data_ = credit->max_data;
    last_control_ = credit->max_control;
    control_target_ = prep.control_credit_target;
    last_frames_ = window.max_frame_seq;
    last_bytes_ = window.max_stream_bytes;
    return {};
}
std::expected<void, ConnectionError> Connection::CheckRawBindings() const noexcept {
    const auto credit = raw_.handshake.credits->PeerStats(raw_.handshake.credit_peer);
    if (!credit || credit->frozen || credit->retired || raw_.handshake.credits->stats().frozen) {
        return Error(ConnectionErrorCode::kCredit);
    }
    const auto* pool = raw_.sender->pool();
    if (!pool || pool != tx_pool_) {
        return Error(ConnectionErrorCode::kLaneChanged);
    }
    // READY 前仍要求 HELLO 原快照；Active 后由 LaneControl 的公告/确认门禁管理代次变化。
    if (state_ != ConnectionState::kRawActive) {
        auto lanes = control::ValidateLocalTxLanes(*pool, handshake_.local());
        if (!lanes) {
            return Error(ConnectionErrorCode::kLaneChanged, static_cast<int>(lanes.error().code));
        }
    }
    auto sources = raw_.sources->Check();
    if (!sources) {
        return Error(ConnectionErrorCode::kSourceAdmission, static_cast<int>(sources.error()));
    }
    return {};
}
std::expected<void, ConnectionError> Connection::PrepareTcp() noexcept {
    if (state_ != ConnectionState::kPreparing) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    auto prepared = handshake_.PrepareTcp();
    if (!prepared) {
        return std::unexpected(HandshakeError(prepared.error()));
    }
    state_ = ConnectionState::kWaitingReady;
    return {};
}
std::expected<void, ConnectionError> Connection::ApplyPeerReady() noexcept {
    const auto* ready = handshake_.peer_ready();
    const auto* local_ready = handshake_.local_ready();
    if (!ready || !local_ready) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    auto published =
        raw_.handshake.credits->RecordPublishedControl(raw_.handshake.credit_peer, local_ready->max_control_credit_seq);
    if (!published) {
        return Error(ConnectionErrorCode::kCredit);
    }
    auto applied = tx_.ApplyCredit(tx_session_, tx_identity_, ready->max_credit_seq, ready->max_control_credit_seq,
                                   ready->max_frame_seq, ready->max_stream_bytes);
    if (!applied) {
        return std::unexpected(TxError(applied.error()));
    }
    return {};
}
std::expected<void, ConnectionError> Connection::RefreshCredit() noexcept {
    auto& credits = *raw_.handshake.credits;
    auto peer = credits.PeerStats(raw_.handshake.credit_peer);
    if (!peer || peer->frozen || peer->retired || credits.stats().frozen) {
        return Error(ConnectionErrorCode::kCredit);
    }
    if (peer->base.free) {
        auto grant =
            credits.Grant(raw_.handshake.credit_peer, CreditClass::kData, static_cast<std::uint32_t>(peer->base.free));
        if (!grant) {
            return Error(ConnectionErrorCode::kCredit, static_cast<int>(grant.error().code));
        }
    }
    // 控制预算低水线才补满，避免空闲双端仅因 CREDIT 消费一个 RQE 就无限互相通知。
    if (peer->control_granted <= control_target_ / 2) {
        const auto count = std::min(control_target_ - peer->control_granted, credits.stats().control.free);
        if (count) {
            auto grant =
                credits.Grant(raw_.handshake.credit_peer, CreditClass::kControl, static_cast<std::uint32_t>(count));
            if (!grant) {
                return Error(ConnectionErrorCode::kCredit, static_cast<int>(grant.error().code));
            }
        }
    }
    peer = credits.PeerStats(raw_.handshake.credit_peer);
    if (!peer) {
        return Error(ConnectionErrorCode::kCredit);
    }
    const auto window = handshake_.receive_window().stats();
    if (last_data_ == peer->max_data && last_control_ == peer->max_control && last_frames_ == window.max_frame_seq &&
        last_bytes_ == window.max_stream_bytes) {
        return {};
    }
    WireFrameInput frame;
    frame.type = WireFrameType::kControl;
    frame.control = WireControlType::kCreditUpdate;
    frame.session = tx_session_.session;
    frame.credit_epoch = handshake_.local().credit_epoch;
    frame.max_credit_seq = peer->max_data;
    frame.max_control_credit_seq = peer->max_control;
    frame.max_frame_seq = window.max_frame_seq;
    frame.max_stream_bytes = window.max_stream_bytes;
    auto queued = tx_.QueueCredit(frame);
    if (!queued) {
        return std::unexpected(TxError(queued.error()));
    }
    last_data_ = peer->max_data;
    last_control_ = peer->max_control;
    last_frames_ = window.max_frame_seq;
    last_bytes_ = window.max_stream_bytes;
    return {};
}
std::expected<std::size_t, ConnectionError> Connection::Progress(std::uint64_t now_ns) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ == ConnectionState::kTcpActive) {
        return 0;
    }
    if (state_ == ConnectionState::kIdle || state_ == ConnectionState::kClosed || state_ == ConnectionState::kClosing) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    // 单 owner 保证此检查到 post 之间不会重建 Lane；新物理来源必须先完成新的协商。
    if (raw_prepared_) {
        auto bindings = CheckRawBindings();
        if (!bindings) {
            return std::unexpected(Fail(bindings.error()));
        }
    }
    if (state_ != ConnectionState::kRawActive) {
        auto progressed = handshake_.Progress(now_ns);
        if (!progressed) {
            return std::unexpected(Fail(HandshakeError(progressed.error())));
        }
        switch (*progressed) {
        case control::ConnectionHandshakeState::kPreparing:
            state_ = ConnectionState::kPreparing;
            break;
        case control::ConnectionHandshakeState::kWaitingReady:
            state_ = ConnectionState::kWaitingReady;
            break;
        case control::ConnectionHandshakeState::kRawActive: {
            if (!raw_prepared_) {
                return std::unexpected(Fail({ConnectionErrorCode::kNotReady}));
            }
            auto applied = ApplyPeerReady();
            if (!applied) {
                return std::unexpected(Fail(applied.error()));
            }
            auto fd = handshake_.TakeEstablishedFd();
            if (!fd) {
                return std::unexpected(Fail(HandshakeError(fd.error())));
            }
            auto control_open = lane_control_.Open(*fd,
                                                   {.local = &handshake_.local(),
                                                    .peer = handshake_.peer(),
                                                    .pool = raw_.sender->pool(),
                                                    .sources = raw_.sources,
                                                    .authorize_update = raw_.authorize_lane_update,
                                                    .authorization_cookie = raw_.lane_authorization_cookie,
                                                    .timeout_ns = raw_.lane_update_timeout_ns},
                                                   now_ns);
            if (!control_open) {
                ::close(*fd);
                return std::unexpected(Fail(LaneError(control_open.error())));
            }
            state_ = ConnectionState::kRawActive;
            break;
        }
        case control::ConnectionHandshakeState::kTcpActive: {
            auto fd = handshake_.TakeEstablishedFd();
            if (!fd) {
                return std::unexpected(Fail(HandshakeError(fd.error())));
            }
            auto opened = tcp_.Open(*fd);
            if (!opened) {
                ::close(*fd);
                return std::unexpected(Fail(TcpError(opened.error())));
            }
            state_ = ConnectionState::kTcpActive;
            break;
        }
        default:
            break;
        }
        if (state_ != ConnectionState::kRawActive) {
            return 0;
        }
    }
    auto timeout = handshake_.receive_window().CheckTimeout(now_ns);
    if (!timeout) {
        return std::unexpected(Fail({ConnectionErrorCode::kRx, static_cast<int>(timeout.error().code)}));
    }
    auto lane_progress = lane_control_.Progress(now_ns);
    if (!lane_progress) {
        return std::unexpected(Fail(LaneError(lane_progress.error())));
    }
    if (!lane_progress->tx_allowed) {
        return lane_progress->processed;
    }
    auto credit = RefreshCredit();
    if (!credit) {
        return std::unexpected(Fail(credit.error()));
    }
    auto sent = tx_.Progress();
    if (!sent) {
        if (sent.error().code == EagerTxErrorCode::kWouldBlock) {
            return lane_progress->processed;
        }
        return std::unexpected(Fail(TxError(sent.error())));
    }
    return *sent + lane_progress->processed;
}
std::expected<std::size_t, ConnectionError> Connection::Write(std::span<const std::byte> input) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ == ConnectionState::kTcpActive) {
        auto result = tcp_.Write(input);
        if (tcp_.failure()) {
            (void)Fail(TcpError(*tcp_.failure()));
        }
        if (!result) {
            return std::unexpected(TcpError(result.error()));
        }
        return *result;
    }
    if (state_ != ConnectionState::kRawActive) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    auto result = tx_.Write(input);
    if (!result) {
        if (tx_.stats().failed) {
            return std::unexpected(Fail(TxError(result.error())));
        }
        return std::unexpected(TxError(result.error()));
    }
    if (tx_.stats().failed) {
        (void)Fail({ConnectionErrorCode::kTx});
    }
    return *result;
}
std::expected<ConnectionReadResult, ConnectionError> Connection::Read(std::span<std::byte> output,
                                                                      std::uint64_t now_ns) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ == ConnectionState::kTcpActive) {
        auto result = tcp_.Read(output);
        if (tcp_.failure()) {
            (void)Fail(TcpError(*tcp_.failure()));
        }
        if (!result) {
            return std::unexpected(TcpError(result.error()));
        }
        return ConnectionReadResult{*result, tcp_.read_eof()};
    }
    if (state_ != ConnectionState::kRawActive) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    auto result = rx_.Read(output, now_ns);
    if (!result) {
        return std::unexpected(Fail(RxError(result.error())));
    }
    if (rx_.failed()) {
        (void)Fail({ConnectionErrorCode::kRx});
    }
    if (result->would_block) {
        return Error(ConnectionErrorCode::kWouldBlock);
    }
    return ConnectionReadResult{result->bytes, result->eof};
}
std::expected<void, ConnectionError> Connection::ShutdownWrite() noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ == ConnectionState::kTcpActive) {
        auto result = tcp_.ShutdownWrite();
        if (tcp_.failure()) {
            (void)Fail(TcpError(*tcp_.failure()));
        }
        if (!result) {
            return std::unexpected(TcpError(result.error()));
        }
        return {};
    }
    if (state_ != ConnectionState::kRawActive) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    auto result = tx_.ShutdownWrite();
    if (!result) {
        return std::unexpected(TxError(result.error()));
    }
    return {};
}
std::expected<void, ConnectionError> Connection::OnTxCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!tx_open_) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    auto result = tx_.OnCompletion(event);
    if (!result) {
        if (result.error().code == EagerTxErrorCode::kInvalidCompletion) {
            return std::unexpected(TxError(result.error()));
        }
        return std::unexpected(Fail(TxError(result.error())));
    }
    return {};
}
std::expected<void, ConnectionError> Connection::OnControlReady() noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (state_ != ConnectionState::kRawActive) {
        return Error(ConnectionErrorCode::kNotReady);
    }
    std::byte byte;
    const auto count = ::recv(lane_control_.fd(), &byte, 1, MSG_PEEK | MSG_DONTWAIT);
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return {};
    }
    if (count < 0) {
        return std::unexpected(Fail({ConnectionErrorCode::kControlClosed, 0, errno}));
    }
    if (count == 0) {
        return std::unexpected(Fail({ConnectionErrorCode::kControlClosed}));
    }
    lane_control_.NotifyReadable();
    return {};
}
std::expected<EagerRxDisposition, EagerRxError> Connection::DispatchRx(void* owner, EagerReceiveDomain& domain,
                                                                       const WireFrameView& decoded,
                                                                       EagerRxPacket packet,
                                                                       std::uint64_t now_ns) noexcept {
    if (!owner) {
        return std::unexpected(EagerRxError{EagerRxErrorCode::kInvalidArgument});
    }
    auto& self = *static_cast<Connection*>(owner);
    if (!self.raw_prepared_ || self.raw_.receive != &domain || self.failure_ ||
        self.state_ == ConnectionState::kClosing || self.state_ == ConnectionState::kClosed) {
        return std::unexpected(EagerRxError{EagerRxErrorCode::kStopped});
    }
    const auto& frame = decoded.frame;
    if (frame.type == WireFrameType::kDataWithCredit ||
        (frame.type == WireFrameType::kControl && frame.control == WireControlType::kCreditUpdate)) {
        auto updated = self.tx_.ApplyCredit(self.tx_session_, self.tx_identity_, frame.max_credit_seq,
                                            frame.max_control_credit_seq, frame.max_frame_seq, frame.max_stream_bytes);
        if (!updated) {
            (void)self.Fail(TxError(updated.error()));
            return std::unexpected(EagerRxError{EagerRxErrorCode::kCreditFailed});
        }
        if (frame.type == WireFrameType::kControl) {
            return EagerRxDisposition::kRelease;
        }
    }
    if (frame.type == WireFrameType::kControl && frame.control == WireControlType::kReset) {
        (void)self.Fail({ConnectionErrorCode::kPeerReset, static_cast<int>(frame.reset_code)});
        return EagerRxDisposition::kRelease;
    }
    auto accepted = self.rx_.Accept(packet, now_ns);
    if (!accepted) {
        (void)self.Fail(RxError(accepted.error()));
        return std::unexpected(accepted.error());
    }
    return EagerRxDisposition::kHeld;
}
ConnectionError Connection::Fail(ConnectionError error) noexcept {
    if (!failure_) {
        failure_ = error;
    }
    state_ = ConnectionState::kFailed;
    lane_control_.Cancel();
    handshake_.Cancel();
    tx_.Fail();
    rx_.Fail();
    tcp_.Cancel();
    return *failure_;
}
void Connection::Cancel() noexcept {
    if (opened_once_ && state_ != ConnectionState::kClosed) {
        (void)Fail({ConnectionErrorCode::kCancelled});
    }
}
std::expected<void, ConnectionError> Connection::Close() noexcept {
    if (state_ == ConnectionState::kClosed) {
        return {};
    }
    state_ = ConnectionState::kClosing;
    (void)lane_control_.Close();
    handshake_.Cancel();
    tx_.Fail();
    rx_.Fail();
    (void)tcp_.Close();
    if (rx_open_) {
        auto drained = rx_.Drain();
        if (!drained) {
            return std::unexpected(RxError(drained.error()));
        }
        auto closed = rx_.Close();
        if (!closed) {
            return Error(ConnectionErrorCode::kRetained);
        }
        rx_open_ = false;
    }
    if (tx_open_) {
        auto closed = tx_.Close();
        if (!closed) {
            return Error(ConnectionErrorCode::kRetained, static_cast<int>(closed.error().code));
        }
        tx_open_ = false;
    }
    auto closed = handshake_.Close();
    if (!closed) {
        return std::unexpected(HandshakeError(closed.error()));
    }
    state_ = ConnectionState::kClosed;
    return {};
}
int Connection::fd() const noexcept {
    if (state_ == ConnectionState::kTcpActive) {
        return tcp_.fd();
    }
    return state_ == ConnectionState::kRawActive ? lane_control_.fd() : handshake_.fd();
}
} // namespace connection
} // namespace kbsocket
