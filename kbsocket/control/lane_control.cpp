// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/lane_control.hpp"

#include <cstring>
#include <limits>

#include "kbsocket/control/tx_lane_snapshot.hpp"

namespace kbsocket {
namespace control {
namespace {
using Code = LaneControlErrorCode;
auto Error(Code code, int detail = 0, int system_error = 0) noexcept {
    return std::unexpected(LaneControlError{code, detail, system_error});
}
LaneControlError ChannelError(const HandshakeError& error) noexcept {
    return {Code::kChannel, static_cast<int>(error.code), error.system_error};
}
bool ValidIdentity(const HandshakeIdentity& identity) noexcept {
    return identity.runtime && identity.receive_domain && identity.endpoint_generation && identity.authorization &&
           identity.session && identity.token.generation;
}
} // namespace

std::expected<void, LaneControlError> LaneControl::Open(int fd, const LaneControlConfig& config,
                                                        std::uint64_t now_ns) noexcept {
    if (opened_once_ || !config.local || !config.peer || !config.pool || !config.sources || !config.timeout_ns ||
        !config.pool->context() || !ValidIdentity(config.local->identity) || !ValidIdentity(config.peer->identity) ||
        !(config.local->capabilities & kHandshakeCapabilityRaw) ||
        !(config.peer->capabilities & kHandshakeCapabilityRaw) || !ValidateHandshakeTxLanes(config.local->tx_lanes) ||
        !ValidateHandshakeTxLanes(config.peer->tx_lanes)) {
        return Error(Code::kInvalidArgument);
    }
    const auto sources = config.sources->Check();
    if (!sources) {
        return Error(Code::kSource, static_cast<int>(sources.error()));
    }
    const auto opened = channel_.Open(fd, true, now_ns, std::numeric_limits<std::uint64_t>::max());
    if (!opened) {
        return std::unexpected(ChannelError(opened.error()));
    }
    config_ = config;
    context_ = config.pool->context();
    local_confirmed_ = config.local->tx_lanes;
    peer_current_ = config.peer->tx_lanes;
    last_now_ = now_ns;
    opened_once_ = true;
    return {};
}

LaneControlError LaneControl::Fail(LaneControlError error) noexcept {
    if (!failure_) {
        failure_ = error;
    }
    (void)channel_.Close();
    return *failure_;
}

std::expected<void, LaneControlError> LaneControl::ObserveLocal(std::uint64_t now_ns) noexcept {
    const auto& pool = *config_.pool;
    const auto& hello = *config_.local;
    const auto passive = pool.rx_target_jetty_id();
    const auto first = pool.lane(0);
    // 在线更新仅改变同一池中主动 Lane；域、被动目标或池代次变化必须重新建链。
    if (pool.context() != context_ || pool.retained() || pool.capacity() != hello.tx_lanes.count || !first ||
        first->epoch != hello.tx_lanes.pool_epoch || !passive || passive->id != hello.jetty_id ||
        std::memcmp(passive->eid.raw, hello.eid.data(), hello.eid.size()) != 0) {
        return Error(Code::kLane);
    }
    if (ValidateLocalTxLanes(pool, hello, local_confirmed_)) {
        return {};
    }
    if (!config_.authorize_update) {
        return Error(Code::kLane);
    }
    if (!local_since_) {
        local_since_ = now_ns;
    }
    if (local_pending_) {
        return {};
    }
    const auto captured = CaptureTxLanes(pool, false);
    if (!captured) {
        // Replacing 的空槽只允许有界等待，不猜测新物理身份，更不能继续 post。
        if (captured.error().code == HandshakeErrorCode::kNotReady) {
            return {};
        }
        return Error(Code::kLane, static_cast<int>(captured.error().code));
    }
    const auto valid = ValidateTxLaneUpdate(local_confirmed_, *captured);
    if (!valid || local_update_id_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(Code::kLane);
    }
    local_offered_ = *captured;
    local_pending_ = true;
    local_queued_ = false;
    local_sent_ = false;
    return {};
}

std::expected<void, LaneControlError> LaneControl::QueueOutput() noexcept {
    if (!channel_.output_flushed() || output_ != Output::kNone) {
        return {};
    }
    HandshakeMessage message;
    if (peer_ack_pending_) {
        // 两端同时重建时优先回 ACK，不能被自己的待确认 UPDATE 阻塞。
        message.kind = HandshakeMessageKind::kLaneAck;
        message.lane_ack = {config_.local->identity, config_.peer->identity, peer_update_id_};
    } else if (local_pending_ && !local_queued_) {
        message.kind = HandshakeMessageKind::kLaneUpdate;
        message.lane_update = {config_.local->identity, config_.peer->identity, local_update_id_ + 1, local_offered_};
    } else {
        return {};
    }
    const auto queued = channel_.Queue(message);
    if (!queued) {
        return std::unexpected(ChannelError(queued.error()));
    }
    if (message.kind == HandshakeMessageKind::kLaneAck) {
        output_ = Output::kAck;
    } else {
        output_ = Output::kUpdate;
        local_queued_ = true;
    }
    return {};
}

void LaneControl::ObserveFlushed() noexcept {
    if (!channel_.output_flushed()) {
        return;
    }
    if (output_ == Output::kUpdate) {
        local_sent_ = true;
    } else if (output_ == Output::kAck) {
        peer_ack_pending_ = false;
        ack_since_.reset();
    }
    output_ = Output::kNone;
}

std::expected<void, LaneControlError> LaneControl::Handle(const HandshakeMessage& message,
                                                          std::uint64_t now_ns) noexcept {
    if (message.kind == HandshakeMessageKind::kLaneAck) {
        const auto& ack = message.lane_ack;
        if (ack.sender != config_.peer->identity || ack.receiver != config_.local->identity || !local_pending_ ||
            !local_sent_ || ack.update_id != local_update_id_ + 1) {
            return Error(Code::kProtocol);
        }
        local_confirmed_ = local_offered_;
        ++local_update_id_;
        local_pending_ = local_queued_ = local_sent_ = false;
        local_since_.reset();
        return {};
    }
    if (message.kind != HandshakeMessageKind::kLaneUpdate) {
        return Error(Code::kProtocol);
    }
    const auto& update = message.lane_update;
    if (update.sender != config_.peer->identity || update.receiver != config_.local->identity || peer_ack_pending_ ||
        peer_update_id_ == std::numeric_limits<std::uint64_t>::max() || update.update_id != peer_update_id_ + 1 ||
        !ValidateTxLaneUpdate(peer_current_, update.tx_lanes)) {
        return Error(Code::kProtocol);
    }
    if (!config_.authorize_update ||
        !config_.authorize_update(config_.authorization_cookie, update.sender, update.tx_lanes)) {
        return Error(Code::kUnauthorized);
    }
    const auto installed = config_.sources->Update(update.tx_lanes, true);
    if (!installed) {
        return Error(Code::kSource, static_cast<int>(installed.error()));
    }
    peer_current_ = update.tx_lanes;
    peer_update_id_ = update.update_id;
    peer_ack_pending_ = true;
    ack_since_ = now_ns;
    return {};
}

bool LaneControl::TimedOut(std::uint64_t now_ns) const noexcept {
    const auto expired = [&](const auto& start) { return start && now_ns - *start >= config_.timeout_ns; };
    return expired(local_since_) || expired(ack_since_) || expired(input_since_);
}

std::expected<LaneControlProgress, LaneControlError> LaneControl::Progress(std::uint64_t now_ns) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (channel_.fd() < 0) {
        return Error(Code::kNotReady);
    }
    if (now_ns < last_now_) {
        return Error(Code::kInvalidArgument);
    }
    last_now_ = now_ns;
    if (TimedOut(now_ns)) {
        return std::unexpected(Fail({Code::kTimeout}));
    }
    const auto sources = config_.sources->Check();
    if (!sources) {
        return std::unexpected(Fail({Code::kSource, static_cast<int>(sources.error())}));
    }
    if (auto observed = ObserveLocal(now_ns); !observed) {
        return std::unexpected(Fail(observed.error()));
    }
    if (auto queued = QueueOutput(); !queued) {
        return std::unexpected(Fail(queued.error()));
    }
    LaneControlProgress result;
    const bool receive = readable_ || local_pending_;
    if (!channel_.output_flushed() || receive) {
        constexpr std::size_t kByteBudget = 16384;
        constexpr std::uint32_t kSyscallBudget = 8;
        const auto progressed = channel_.Progress(now_ns, {kByteBudget, kSyscallBudget, receive});
        if (!progressed) {
            return std::unexpected(Fail(ChannelError(progressed.error())));
        }
        result.processed = progressed->sent + progressed->received;
        ObserveFlushed();
        if (progressed->message_ready) {
            const auto message = channel_.TakeMessage();
            if (!message) {
                return std::unexpected(Fail(ChannelError(message.error())));
            }
            input_since_.reset();
            readable_ = true;
            if (auto handled = Handle(*message, now_ns); !handled) {
                return std::unexpected(Fail(handled.error()));
            }
        } else if (receive) {
            // 完整预算耗尽时保留后续读取机会；普通 EAGAIN 等待 readiness，避免空闲 recv 忙轮询。
            readable_ = progressed->syscalls == kSyscallBudget || result.processed == kByteBudget;
            if (channel_.input_pending() && !input_since_) {
                input_since_ = now_ns;
            }
        }
    }
    // ACK 到达期间池可能再次换代；重新校验，绝不能因旧 ACK 开放更新后的未知来源。
    if (auto observed = ObserveLocal(now_ns); !observed) {
        return std::unexpected(Fail(observed.error()));
    }
    if (auto queued = QueueOutput(); !queued) {
        return std::unexpected(Fail(queued.error()));
    }
    result.tx_allowed = !local_since_ && !local_pending_;
    return result;
}

bool LaneControl::output_pending() const noexcept {
    return !channel_.output_flushed() || peer_ack_pending_ || (local_pending_ && !local_queued_);
}
bool LaneControl::pending() const noexcept {
    return local_since_.has_value() || local_pending_ || peer_ack_pending_ || input_since_.has_value();
}
void LaneControl::Cancel() noexcept {
    if (channel_.fd() >= 0) {
        (void)Fail({Code::kChannel, static_cast<int>(HandshakeErrorCode::kCancelled)});
    }
}
std::expected<void, LaneControlError> LaneControl::Close() noexcept {
    const auto closed = channel_.Close();
    if (!closed) {
        return std::unexpected(ChannelError(closed.error()));
    }
    return {};
}

} // namespace control
} // namespace kbsocket
