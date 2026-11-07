// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/peer_credit_control.hpp"

#include <limits>

namespace kbsocket {
namespace control {
namespace {
using Code = PeerCreditControlErrorCode;
auto Error(Code code, int detail = 0, int system_error = 0) noexcept {
    return std::unexpected(PeerCreditControlError{code, detail, system_error});
}
PeerCreditControlError ChannelError(const HandshakeError& error) noexcept {
    return {Code::kChannel, static_cast<int>(error.code), error.system_error};
}
PeerCreditControlError CreditError(const connection::CreditError& error) noexcept {
    return {Code::kCredit, static_cast<int>(error.code)};
}
bool ValidIdentity(const HandshakeIdentity& identity) noexcept {
    return identity.runtime && identity.receive_domain && identity.endpoint_generation && identity.authorization &&
           identity.session && identity.token.generation;
}
} // namespace

PeerCreditControl::~PeerCreditControl() {
    (void)Close();
}

std::expected<void, PeerCreditControlError> PeerCreditControl::Open(int fd, const PeerCreditControlConfig& config,
                                                                    std::uint64_t now_ns) noexcept {
    if (opened_once_) {
        return Error(Code::kInUse);
    }
    if (!config.peer_authorized) {
        return Error(Code::kUnauthorized);
    }
    if (!config.sender || !config.receive || !config.timeout_ns || !config.local_credit_epoch ||
        !config.peer_credit_epoch || !ValidIdentity(config.local) || !ValidIdentity(config.peer)) {
        return Error(Code::kInvalidArgument);
    }
    const connection::CreditIdentity tx_identity{config.peer.runtime, config.peer.receive_domain,
                                                 config.peer_credit_epoch};
    const connection::CreditIdentity rx_identity{config.peer.runtime, config.local.receive_domain,
                                                 config.local_credit_epoch};
    const auto peer = config.receive->PeerStats(config.receive_peer);
    if (!config.sender->ready() || config.sender->identity() != tx_identity || !peer || peer->retired ||
        peer->identity != rx_identity) {
        return Error(Code::kInvalidArgument);
    }
    const auto sender_bound = config.sender->BindWithdrawal(*this);
    if (!sender_bound) {
        return std::unexpected(CreditError(sender_bound.error()));
    }
    const auto receiver_bound = config.receive->BindWithdrawal(*this, config.receive_peer);
    if (!receiver_bound) {
        (void)config.sender->UnbindWithdrawal(*this);
        return std::unexpected(CreditError(receiver_bound.error()));
    }
    const auto opened = channel_.Open(fd, true, now_ns, std::numeric_limits<std::uint64_t>::max());
    if (!opened) {
        (void)config.receive->UnbindWithdrawal(*this, config.receive_peer);
        (void)config.sender->UnbindWithdrawal(*this);
        return std::unexpected(ChannelError(opened.error()));
    }
    const auto frozen = config.receive->FreezePeer(config.receive_peer);
    if (!frozen) {
        // 单 owner 下前面的身份/绑定校验已排除此分支；失败仍把未使用的 fd 交还调用方。
        (void)channel_.ReleaseEstablished(true);
        (void)config.receive->UnbindWithdrawal(*this, config.receive_peer);
        (void)config.sender->UnbindWithdrawal(*this);
        return std::unexpected(CreditError(frozen.error()));
    }
    config.sender->Stop();
    config_ = config;
    started_ns_ = last_now_ns_ = now_ns;
    opened_once_ = bound_ = true;
    return {};
}

PeerCreditControlError PeerCreditControl::Fail(PeerCreditControlError error) noexcept {
    if (!failure_) {
        failure_ = error;
    }
    (void)channel_.Close();
    return *failure_;
}

std::expected<void, PeerCreditControlError> PeerCreditControl::Reconcile() noexcept {
    if (!stats_.local_snapshot) {
        const auto snapshot = config_.sender->StoppedSnapshot(*this);
        if (!snapshot) {
            if (snapshot.error().code == connection::CreditErrorCode::kInUse) {
                // Stop 之前已经 Reserve 的同步 post 必须先交出真实接受前缀；不能当作零发送。
                return {};
            }
            return std::unexpected(CreditError(snapshot.error()));
        }
        stats_.local_spent = snapshot->spent;
        stats_.local_snapshot = true;
    }
    if (stats_.peer_stop_received && !stats_.receive_retired) {
        const auto retired = config_.receive->RetireWithdrawn(*this, config_.receive_peer, stats_.peer_spent);
        if (!retired) {
            if (retired.error().code == connection::CreditErrorCode::kInUse) {
                // 控制 TCP 可以先于数据 CQE 到达；Held 和已 Release 未 refill 的责任也继续等待。
                return {};
            }
            return std::unexpected(CreditError(retired.error()));
        }
        stats_.receive_retired = true;
    }
    return {};
}

std::expected<void, PeerCreditControlError> PeerCreditControl::QueueOutput() noexcept {
    if (!channel_.output_flushed() || output_ != Output::kNone) {
        return {};
    }
    HandshakeMessage message;
    HandshakeCreditStop* frame = nullptr;
    Output output = Output::kNone;
    if (stats_.local_snapshot && !stats_.local_stop_sent) {
        message.kind = HandshakeMessageKind::kCreditStop;
        frame = &message.credit_stop;
        frame->spent = stats_.local_spent;
        output = Output::kStop;
    } else if (stats_.receive_retired && !stats_.ack_sent) {
        message.kind = HandshakeMessageKind::kCreditStopAck;
        frame = &message.credit_stop_ack;
        frame->spent = stats_.peer_spent;
        output = Output::kAck;
    } else {
        return {};
    }
    frame->sender = config_.local;
    frame->receiver = config_.peer;
    frame->sender_credit_epoch = config_.local_credit_epoch;
    frame->receiver_credit_epoch = config_.peer_credit_epoch;
    const auto queued = channel_.Queue(message);
    if (!queued) {
        return std::unexpected(ChannelError(queued.error()));
    }
    output_ = output;
    return {};
}

void PeerCreditControl::ObserveFlushed() noexcept {
    if (!channel_.output_flushed()) {
        return;
    }
    if (output_ == Output::kStop) {
        stats_.local_stop_sent = true;
    } else if (output_ == Output::kAck) {
        stats_.ack_sent = true;
    }
    output_ = Output::kNone;
}

std::expected<void, PeerCreditControlError> PeerCreditControl::Handle(const HandshakeMessage& message) noexcept {
    const HandshakeCreditStop* frame = nullptr;
    if (message.kind == HandshakeMessageKind::kCreditStop) {
        frame = &message.credit_stop;
    } else if (message.kind == HandshakeMessageKind::kCreditStopAck) {
        frame = &message.credit_stop_ack;
    } else {
        return Error(Code::kProtocol);
    }
    if (frame->sender != config_.peer || frame->receiver != config_.local ||
        frame->sender_credit_epoch != config_.peer_credit_epoch ||
        frame->receiver_credit_epoch != config_.local_credit_epoch) {
        return Error(Code::kProtocol);
    }
    if (message.kind == HandshakeMessageKind::kCreditStop) {
        if (stats_.peer_stop_received) {
            return Error(Code::kProtocol);
        }
        stats_.peer_spent = frame->spent;
        stats_.peer_stop_received = true;
    } else {
        if (!stats_.local_stop_sent || !stats_.peer_stop_received || stats_.peer_acknowledged ||
            frame->spent != stats_.local_spent) {
            return Error(Code::kProtocol);
        }
        stats_.peer_acknowledged = true;
    }
    return {};
}

std::expected<PeerCreditControlProgress, PeerCreditControlError>
PeerCreditControl::Progress(std::uint64_t now_ns) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (!bound_) {
        return Error(Code::kNotReady);
    }
    if (now_ns < last_now_ns_) {
        return Error(Code::kInvalidArgument);
    }
    last_now_ns_ = now_ns;
    if (stats_.complete) {
        return PeerCreditControlProgress{0, true};
    }
    if (now_ns - started_ns_ >= config_.timeout_ns) {
        return std::unexpected(Fail({Code::kTimeout}));
    }
    if (const auto reconciled = Reconcile(); !reconciled) {
        return std::unexpected(Fail(reconciled.error()));
    }
    if (const auto queued = QueueOutput(); !queued) {
        return std::unexpected(Fail(queued.error()));
    }
    // 有效 ACK 是对端最后一帧；此后只排尽本端 ACK，避免对端正常 Close 的 EOF
    // 抢在 ObserveFlushed 之前把已经成立的双方撤回误判为失败。
    const auto progressed = channel_.Progress(now_ns, {16384, 8, !stats_.peer_acknowledged});
    if (!progressed) {
        return std::unexpected(Fail(ChannelError(progressed.error())));
    }
    ObserveFlushed();
    if (progressed->message_ready) {
        const auto message = channel_.TakeMessage();
        if (!message) {
            return std::unexpected(Fail(ChannelError(message.error())));
        }
        if (const auto handled = Handle(*message); !handled) {
            return std::unexpected(Fail(handled.error()));
        }
    }
    if (const auto reconciled = Reconcile(); !reconciled) {
        return std::unexpected(Fail(reconciled.error()));
    }
    if (const auto queued = QueueOutput(); !queued) {
        return std::unexpected(Fail(queued.error()));
    }
    stats_.complete = stats_.local_stop_sent && stats_.peer_stop_received && stats_.receive_retired &&
                      stats_.ack_sent && stats_.peer_acknowledged;
    return PeerCreditControlProgress{progressed->sent + progressed->received, stats_.complete};
}

bool PeerCreditControl::output_pending() const noexcept {
    return bound_ && !failure_ &&
           (!channel_.output_flushed() || (stats_.local_snapshot && !stats_.local_stop_sent) ||
            (stats_.receive_retired && !stats_.ack_sent));
}

std::expected<void, PeerCreditControlError> PeerCreditControl::Close() noexcept {
    (void)channel_.Close();
    if (!bound_) {
        return {};
    }
    // 解除的仅是协议控制权。失败/提前 Close 不生成退休证据，冻结账本仍由宿主持有。
    if (const auto unbound = config_.receive->UnbindWithdrawal(*this, config_.receive_peer); !unbound) {
        return std::unexpected(CreditError(unbound.error()));
    }
    if (const auto unbound = config_.sender->UnbindWithdrawal(*this); !unbound) {
        return std::unexpected(CreditError(unbound.error()));
    }
    bound_ = false;
    return {};
}

} // namespace control
} // namespace kbsocket
