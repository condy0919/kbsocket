// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_rx_ingress.hpp"

#include <algorithm>
#include <new>
#include <optional>
#include <utility>

namespace kbsocket {
namespace connection {
namespace {
auto Error(RndvIngressErrorCode code) noexcept {
    return std::unexpected(RndvIngressError{code});
}
bool ValidCapacity(std::uint32_t capacity) noexcept {
    return capacity && capacity <= RndvRxIngress::kMaxCapacity;
}
} // namespace

struct RndvRxIngress::Packet {
    EagerRxPacket handle;
    WireControlType type = WireControlType::kRndvRts;
    std::uint32_t sequence = 0;
    bool active = false;
    // 一旦业务副作用发生，只重试物理归还；不得再次解析已释放的 RX backing。
    bool releasing = false;
    bool gap_waiting = false;
    std::uint64_t gap_since = 0;
    std::uint64_t gap_sequence = 0;
};
struct RndvRxIngress::Job {
    RndvReceiveHandle receive;
    GrantReservation grant;
    RndvNotificationHandle notification;
    bool active = false;
    bool prepared = false;
    std::uint32_t sequence = 0;
};
struct RndvRxIngress::State {
    RndvIngressConfig config;
    EagerReceiveDomain* domain = nullptr;
    RndvReceiver* receiver = nullptr;
    GrantManager* grants = nullptr;
    ReorderWindow* reorder = nullptr;
    RndvNotificationTx* notifications = nullptr;
    std::unique_ptr<Packet[]> packets;
    std::unique_ptr<Job[]> jobs;
    std::size_t packet_count = 0;
    std::size_t cursor = 0;
    bool receiver_bound = false;
    bool notifications_bound = false;
    bool stopped = false;
};

RndvRxIngress::RndvRxIngress() noexcept = default;
RndvRxIngress::~RndvRxIngress() {
    if (!Close()) {
        // 元数据保留不能代替外部注册内存/队列生命周期；宿主必须继续持有全部依赖。
        (void)state_.release();
    }
}
std::expected<void, RndvIngressError> RndvRxIngress::Open(const RndvIngressConfig& config, EagerReceiveDomain& domain,
                                                          RndvReceiver& receiver, GrantManager& grants,
                                                          ReorderWindow& reorder,
                                                          RndvNotificationTx& notifications) noexcept {
    if (state_ || opened_once_) {
        return Error(RndvIngressErrorCode::kInUse);
    }
    if (!ValidCapacity(config.rts_packets) || !ValidCapacity(config.grant_packets) ||
        !ValidCapacity(config.done_packets) || !ValidCapacity(config.grant_jobs) || !config.gap_timeout_ns ||
        !domain.context() || domain.frozen()) {
        return Error(RndvIngressErrorCode::kInvalidArgument);
    }
    if (!receiver.Matches(grants, reorder, config.route)) {
        return Error(RndvIngressErrorCode::kWrongIdentity);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(RndvIngressErrorCode::kNoMemory);
    }
    state->packet_count = config.rts_packets + config.grant_packets + config.done_packets;
    state->packets.reset(new (std::nothrow) Packet[state->packet_count]);
    state->jobs.reset(new (std::nothrow) Job[config.grant_jobs]);
    if (!state->packets || !state->jobs) {
        return Error(RndvIngressErrorCode::kNoMemory);
    }
    state->config = config;
    state->domain = &domain;
    state->receiver = &receiver;
    state->grants = &grants;
    state->reorder = &reorder;
    state->notifications = &notifications;
    if (!receiver.BindIngress(*this)) {
        return Error(RndvIngressErrorCode::kInUse);
    }
    state->receiver_bound = true;
    const auto bound = notifications.BindIngress(*this, domain.context(), config.route.context.peer_runtime);
    if (!bound) {
        if (!receiver.UnbindIngress(*this)) {
            state->stopped = true;
            state_ = std::move(state);
            opened_once_ = true;
            return Error(RndvIngressErrorCode::kRetained);
        }
        return Error(bound.error().code == RndvNotificationErrorCode::kWrongIdentity
                         ? RndvIngressErrorCode::kWrongIdentity
                         : RndvIngressErrorCode::kInUse);
    }
    state->notifications_bound = true;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<void, RndvIngressError> RndvRxIngress::Accept(EagerRxPacket handle) noexcept {
    if (!state_) {
        return Error(RndvIngressErrorCode::kNotReady);
    }
    auto& state = *state_;
    const auto view = state.domain->View(handle);
    if (!view) {
        return Error(RndvIngressErrorCode::kInvalidPacket);
    }
    if (!ValidateWireIdentity(view->decoded, state.config.route, view->observed)) {
        (void)Cancel();
        return Error(RndvIngressErrorCode::kWrongIdentity);
    }
    const auto& frame = view->decoded.frame;
    if (frame.type != WireFrameType::kControl ||
        (frame.control != WireControlType::kRndvRts && frame.control != WireControlType::kRndvGrant &&
         frame.control != WireControlType::kRndvDone)) {
        return Error(RndvIngressErrorCode::kUnsupportedFrame);
    }
    // 接收方向正常 FIN 不关闭反向事务的 GRANT 通道；显式取消仍只接受终结证明 DONE。
    const bool receive_eof = state.reorder->eof();
    if ((state.stopped || (state.receiver->stats().stopped && !receive_eof) ||
         (receive_eof && frame.control == WireControlType::kRndvRts)) &&
        frame.control != WireControlType::kRndvDone) {
        return Error(RndvIngressErrorCode::kStopped);
    }
    if (frame.control == WireControlType::kRndvGrant && !state.config.on_grant) {
        (void)Cancel();
        return Error(RndvIngressErrorCode::kUnsupportedFrame);
    }
    for (std::size_t i = 0; i < state.packet_count; ++i) {
        if (state.packets[i].active && state.packets[i].handle == handle) {
            return Error(RndvIngressErrorCode::kInUse);
        }
    }
    const auto begin = frame.control == WireControlType::kRndvRts ? 0u : state.config.rts_packets;
    const auto first = frame.control == WireControlType::kRndvDone ? begin + state.config.grant_packets : begin;
    const auto count = frame.control == WireControlType::kRndvRts     ? state.config.rts_packets
                       : frame.control == WireControlType::kRndvGrant ? state.config.grant_packets
                                                                      : state.config.done_packets;
    for (std::size_t i = first; i < first + count; ++i) {
        auto& packet = state.packets[i];
        if (!packet.active) {
            packet = Packet{};
            packet.handle = handle;
            packet.type = frame.control;
            packet.sequence = frame.seq;
            packet.active = true;
            return {};
        }
    }
    // 配置不足无法凭空保留已经消耗的 RQE；显式失败，当前 packet 仍交还 Domain 的错误路径。
    (void)Cancel();
    return Error(RndvIngressErrorCode::kCapacity);
}
std::expected<bool, RndvIngressError> RndvRxIngress::StepPacket(Packet& packet, std::uint64_t now_ns) noexcept {
    if (!packet.active) {
        return false;
    }
    auto& state = *state_;
    if (state.stopped && packet.type != WireControlType::kRndvDone) {
        packet.releasing = true;
    }
    if (!packet.releasing) {
        const auto view = state.domain->View(packet.handle);
        if (!view) {
            return Error(RndvIngressErrorCode::kInvalidPacket);
        }
        if (packet.type == WireControlType::kRndvRts) {
            const auto window = state.reorder->stats();
            const auto sequence = view->decoded.frame.seq;
            if (sequence < window.rcv_next || sequence >= window.max_frame_seq) {
                packet.releasing = true;
                return Error(RndvIngressErrorCode::kReceive);
            }
            if (sequence > window.observed_prefix) {
                // 硬件 SEND 可乱序。先等前序 Eager/RTS 入窗，不能让后序请求占光前序的事务槽。
                for (std::uint32_t i = 0; i < state.config.rts_packets; ++i) {
                    const auto& earlier = state.packets[i];
                    if (earlier.active && !earlier.releasing && earlier.sequence == window.observed_prefix) {
                        // 前序已收到，只是在等事务/应用容量；不能把正常背压当作缺失帧计时。
                        packet.gap_waiting = false;
                        return false;
                    }
                }
                if (!packet.gap_waiting || packet.gap_sequence != window.observed_prefix) {
                    packet.gap_waiting = true;
                    packet.gap_since = now_ns;
                    packet.gap_sequence = window.observed_prefix;
                } else if (now_ns - packet.gap_since >= state.config.gap_timeout_ns) {
                    packet.releasing = true;
                    return Error(RndvIngressErrorCode::kGapTimeout);
                }
                return false;
            }
            Job* job = nullptr;
            for (std::uint32_t i = 0; i < state.config.grant_jobs; ++i) {
                if (!state.jobs[i].active) {
                    job = &state.jobs[i];
                    break;
                }
            }
            if (!job) {
                return false;
            }
            const auto received = state.receiver->OnRts(view->decoded, view->observed, now_ns);
            if (!received) {
                if (received.error().code == RndvReceiveErrorCode::kWouldBlock) {
                    return false;
                }
                packet.releasing = true;
                return Error(RndvIngressErrorCode::kReceive);
            }
            // 先保存协调器 handle，再归还 RTS；Grant/通知资源耗尽时重试 job，不再次接收同一 RTS。
            *job = Job{};
            job->receive = *received;
            job->active = true;
            job->sequence = sequence;
            packet.releasing = true;
        } else if (packet.type == WireControlType::kRndvGrant) {
            const auto handled = state.config.on_grant(state.config.grant_owner, view->decoded, view->observed, now_ns);
            if (!handled && handled.error().code == RndvIngressErrorCode::kWouldBlock) {
                return false;
            }
            packet.releasing = true;
            if (!handled) {
                return std::unexpected(handled.error());
            }
        } else {
            // OnDone 错误也可能已保存可信终结证明；停止业务后由 Receiver.Progress 完成清理，不能重放。
            const auto done = state.receiver->OnDone(view->decoded, view->observed, now_ns);
            packet.releasing = true;
            if (!done) {
                return Error(RndvIngressErrorCode::kReceive);
            }
        }
    }
    if (!state.domain->Release(packet.handle)) {
        return Error(RndvIngressErrorCode::kRelease);
    }
    packet = Packet{};
    return true;
}
std::expected<bool, RndvIngressError> RndvRxIngress::StepJob(Job& job) noexcept {
    if (!job.active) {
        return false;
    }
    auto& state = *state_;
    if (!job.notification.generation) {
        if (state.stopped) {
            // Receiver.Cancel 已接管未发布/隔离责任；不能从这里虚构远端终结证明。
            job = Job{};
            return true;
        }
        bool progressed = false;
        if (!job.prepared) {
            for (std::uint32_t i = 0; i < state.config.grant_jobs; ++i) {
                const auto& earlier = state.jobs[i];
                if (earlier.active && !earlier.prepared && earlier.sequence < job.sequence) {
                    // 目标紧张时先保障前序事务，避免后序 DONE 占住唯一目标却不能被应用消费。
                    return false;
                }
            }
            const auto grant = state.receiver->PrepareGrant(job.receive);
            if (!grant) {
                if (grant.error().code == RndvReceiveErrorCode::kWouldBlock) {
                    return false;
                }
                return Error(RndvIngressErrorCode::kReceive);
            }
            job.grant = *grant;
            job.prepared = true;
            progressed = true;
        }
        const auto queued = state.notifications->QueueGrant(*state.grants, job.grant);
        if (!queued) {
            if (queued.error().code == RndvNotificationErrorCode::kWouldBlock) {
                return progressed;
            }
            return Error(RndvIngressErrorCode::kNotification);
        }
        job.notification = *queued;
        return true;
    }
    const auto status = state.notifications->status(job.notification);
    if (!status) {
        return Error(RndvIngressErrorCode::kNotification);
    }
    if (!state.stopped && (status->failed || status->cancelled || !status->acceptance_known)) {
        return Error(RndvIngressErrorCode::kNotification);
    }
    const bool success = status->retired && status->acceptance_known && !status->failed && !status->cancelled;
    if (state.stopped && !success) {
        // 只取消本类创建的 GRANT，不触碰同队列其他 owner 的 RTS/DONE。
        if (!state.notifications->Cancel(job.notification)) {
            return false;
        }
    } else if (!success) {
        return false;
    }
    const auto released = state.notifications->Release(job.notification);
    if (!released) {
        if (released.error().code == RndvNotificationErrorCode::kInUse ||
            released.error().code == RndvNotificationErrorCode::kAcceptanceUnknown ||
            released.error().code == RndvNotificationErrorCode::kRetained) {
            return false;
        }
        return Error(RndvIngressErrorCode::kNotification);
    }
    job = Job{};
    return true;
}
std::expected<std::size_t, RndvIngressError> RndvRxIngress::Progress(std::uint64_t now_ns,
                                                                     std::size_t budget) noexcept {
    if (!state_) {
        return Error(RndvIngressErrorCode::kNotReady);
    }
    if (!budget || budget > kMaxBudget) {
        return Error(RndvIngressErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    std::optional<RndvIngressError> error;
    std::size_t progressed = 0;
    const auto received = state.receiver->Progress(now_ns);
    if (!received) {
        error = RndvIngressError{RndvIngressErrorCode::kReceive};
        (void)Cancel();
    } else {
        progressed += *received;
    }
    if (!state.stopped && ((state.receiver->stats().stopped && !state.reorder->eof()) ||
                           state.notifications->stats().failed || state.notifications->stats().stopped)) {
        error = RndvIngressError{RndvIngressErrorCode::kStopped};
        (void)Cancel();
    }
    const auto count = state.packet_count + state.config.grant_jobs;
    const auto scans = std::min(budget, count);
    for (std::size_t i = 0; i < scans; ++i) {
        const auto index = state.cursor;
        state.cursor = (state.cursor + 1) % count;
        const auto result = index < state.packet_count ? StepPacket(state.packets[index], now_ns)
                                                       : StepJob(state.jobs[index - state.packet_count]);
        if (!result) {
            if (!error) {
                error = result.error();
            }
            (void)Cancel();
        } else {
            progressed += *result;
        }
    }
    // 即使本地清理错误，也扫描独立 DONE 槽；否则需要终结证明的保留责任会阻断自身的证明入口。
    if (error) {
        return std::unexpected(*error);
    }
    return progressed;
}
std::expected<void, RndvIngressError> RndvRxIngress::Cancel() noexcept {
    if (!state_) {
        return Error(RndvIngressErrorCode::kNotReady);
    }
    auto& state = *state_;
    state.stopped = true;
    bool settled = state.receiver->Cancel().has_value();
    for (std::uint32_t i = 0; i < state.config.grant_jobs; ++i) {
        const auto& job = state.jobs[i];
        if (job.active && job.notification.generation) {
            const auto status = state.notifications->status(job.notification);
            const bool success =
                status && status->retired && status->acceptance_known && !status->failed && !status->cancelled;
            if (!success && !state.notifications->Cancel(job.notification)) {
                settled = false;
            }
        }
    }
    return settled ? std::expected<void, RndvIngressError>{} : Error(RndvIngressErrorCode::kRetained);
}
std::expected<void, RndvIngressError> RndvRxIngress::Close() noexcept {
    if (!state_) {
        return {};
    }
    const auto pending = stats();
    if (pending.rts_packets || pending.grant_packets || pending.done_packets || pending.grant_jobs ||
        state_->receiver->stats().active) {
        (void)Cancel();
        return Error(RndvIngressErrorCode::kRetained);
    }
    if (state_->notifications_bound) {
        if (!state_->notifications->UnbindIngress(*this)) {
            return Error(RndvIngressErrorCode::kRetained);
        }
        state_->notifications_bound = false;
    }
    if (state_->receiver_bound) {
        if (!state_->receiver->UnbindIngress(*this)) {
            return Error(RndvIngressErrorCode::kRetained);
        }
        state_->receiver_bound = false;
    }
    state_.reset();
    return {};
}
RndvIngressStats RndvRxIngress::stats() const noexcept {
    RndvIngressStats result;
    if (!state_) {
        return result;
    }
    const auto& state = *state_;
    result.stopped = state.stopped;
    for (std::size_t i = 0; i < state.packet_count; ++i) {
        const auto& packet = state.packets[i];
        if (!packet.active) {
            continue;
        }
        switch (packet.type) {
        case WireControlType::kRndvRts:
            ++result.rts_packets;
            break;
        case WireControlType::kRndvGrant:
            ++result.grant_packets;
            break;
        default:
            ++result.done_packets;
            break;
        }
    }
    for (std::uint32_t i = 0; i < state.config.grant_jobs; ++i) {
        result.grant_jobs += state.jobs[i].active;
    }
    return result;
}
} // namespace connection
} // namespace kbsocket
