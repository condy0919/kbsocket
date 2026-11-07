// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/raw_domain.hpp"

#include <algorithm>
#include <array>
#include <new>

namespace kbsocket {
namespace runtime {
namespace {
using Code = RawDomainErrorCode;
auto Error(Code code, int detail = 0, int provider_error = 0) noexcept {
    return std::unexpected(RawDomainError{.code = code, .detail = detail, .provider_error = provider_error});
}
RawDomainError QueueError(const raw::JettyPoolFailure& error) noexcept {
    return {.code = Code::kQueue,
            .detail = static_cast<int>(error.code),
            .provider_error = error.provider_error,
            .queue_error = raw::JettyPoolError{error, {}}};
}
RawDomainError ReceiveError(const raw::RxBufferError& error) noexcept {
    return {.code = Code::kReceive,
            .detail = static_cast<int>(error.code),
            .provider_error = error.provider_error,
            .accepted = error.accepted};
}
bool All(const raw::ReceiveDomainQuiescence& proof) noexcept {
    return proof.peer_stopped && proof.events_acked && proof.queue_users_stopped && proof.completion_queues_healthy;
}
} // namespace

struct RawDomain::State {
    // 地址在整个生命周期内固定；任何初始化/关闭失败均保留整份状态，禁止单独析构底层成员。
    raw::JettyPool pool;
    raw::RxBufferPool rx;
    raw::AttemptLedger ledger;
    raw::TxSender sender;
    connection::ReceiveCreditDomain credits;
    connection::ConnectionTable routes;
    connection::EagerReceiveDomain receive;
    RxSourceRegistry sources;
    ConnectionIoBinding io;
    raw::ReceiveDomainCloser closer;
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> close_events{};
    RawDomainConfig config;
    RawDomainStage stage = RawDomainStage::kOpenFailed;
    std::optional<RawDomainError> open_error;
    std::size_t initial_accepted = 0, retired_rx = 0;
    bool initial_accepted_known = true, opened = false, bound = false;

    std::unexpected<RawDomainError> FailOpen(RawDomainError error) noexcept {
        open_error = error;
        stage = RawDomainStage::kOpenFailed;
        return std::unexpected(error);
    }
    std::expected<void, RawDomainError> RecordInitial(std::size_t count) noexcept {
        const auto remaining_data = credits.stats().data.unposted;
        const auto data = static_cast<std::uint32_t>(std::min<std::size_t>(count, remaining_data));
        const auto control = static_cast<std::uint32_t>(count - data);
        if (data) {
            if (const auto recorded = credits.RecordInitialPosted(connection::CreditClass::kData, data); !recorded) {
                return Error(Code::kCredit, static_cast<int>(recorded.error().code));
            }
        }
        if (control) {
            if (const auto recorded = credits.RecordInitialPosted(connection::CreditClass::kControl, control);
                !recorded) {
                return Error(Code::kCredit, static_cast<int>(recorded.error().code));
            }
        }
        initial_accepted += count;
        return {};
    }
    std::expected<void, RawDomainError> CloseMetadata() noexcept {
        if (receive.buffers()) {
            if (const auto closed = receive.CloseDestroyed(closer); !closed) {
                return Error(Code::kEager, static_cast<int>(closed.error().code));
            }
        } else if (const auto closed = credits.Close(); !closed) {
            // 初始投递失败、尚未打开 Eager 的域没有发布任何授信；仍在物理关闭后才清退元数据。
            return Error(Code::kCredit, static_cast<int>(closed.error().code));
        }
        if (const auto closed = sources.Close(); !closed) {
            return Error(Code::kSource, static_cast<int>(closed.error()));
        }
        if (const auto closed = routes.Close(); !closed) {
            return Error(Code::kRoute, static_cast<int>(closed.error()));
        }
        if (const auto closed = sender.Close(); !closed) {
            return Error(Code::kSender, static_cast<int>(closed.error().code));
        }
        if (const auto closed = ledger.Close(); !closed) {
            return Error(Code::kAttempt, static_cast<int>(closed.error().code));
        }
        stage = RawDomainStage::kClosed;
        return {};
    }
};

RawDomain::RawDomain() noexcept = default;
RawDomain::~RawDomain() {
    if (state_ && state_->stage != RawDomainStage::kClosed) {
        // 析构不能建立远端/事件屏障；同时保留注册内存、队列及所有关联元数据的稳定地址。
        (void)state_.release();
    }
}
std::expected<void, RawDomainError> RawDomain::Open(urma_context_t* context, const urma_device_cap_t& capabilities,
                                                    const RawDomainConfig& config) noexcept {
    if (state_) {
        return Error(Code::kInUse);
    }
    if (!context || !config.context_incarnation || !config.local_domain.runtime_instance ||
        !config.local_domain.receive_domain || !config.tx_attempts || !config.route_capacity ||
        config.route_capacity > connection::ConnectionTable::kMaxSlots || !config.source_capacity ||
        config.source_capacity > RxSourceRegistry::kMaxCapacity || !config.control_slots || !config.peer_capacity ||
        config.peer_capacity > connection::ReceiveCreditDomain::kMaxPeers || !config.base_credits ||
        (config.queues.rx_depth && (config.control_slots >= *config.queues.rx_depth ||
                                    config.base_credits > *config.queues.rx_depth - config.control_slots ||
                                    config.watermark_threshold >= *config.queues.rx_depth))) {
        return Error(Code::kInvalidArgument);
    }
    state_.reset(new (std::nothrow) State);
    if (!state_) {
        return Error(Code::kNoMemory);
    }
    auto& state = *state_;
    state.config = config;
    if (const auto opened = state.routes.Open(config.local_domain, config.route_capacity); !opened) {
        return state.FailOpen({.code = Code::kRoute, .detail = static_cast<int>(opened.error())});
    }
    if (const auto opened = state.pool.Open(context, capabilities, config.queues); !opened) {
        auto error = QueueError(opened.error().failure);
        error.queue_error = opened.error();
        return state.FailOpen(error);
    }
    if (config.watermark_threshold >= state.pool.rx_depth()) {
        return state.FailOpen({.code = Code::kInvalidArgument});
    }
    if (const auto opened = state.credits.Open({config.local_domain.receive_domain, state.pool.rx_depth(),
                                                config.control_slots, config.peer_capacity, config.base_credits});
        !opened) {
        return state.FailOpen({.code = Code::kCredit, .detail = static_cast<int>(opened.error().code)});
    }
    if (const auto opened = state.ledger.Open(state.pool, config.tx_attempts); !opened) {
        return state.FailOpen({.code = Code::kAttempt, .detail = static_cast<int>(opened.error().code)});
    }
    if (const auto opened = state.sender.Open(state.ledger, true); !opened) {
        return state.FailOpen({.code = Code::kSender, .detail = static_cast<int>(opened.error().code)});
    }
    if (const auto opened = state.rx.Open(state.pool); !opened) {
        return state.FailOpen(ReceiveError(opened.error()));
    }
    if (const auto opened =
            state.sources.Open({&state.rx, config.context_incarnation, config.local_domain, config.source_capacity});
        !opened) {
        return state.FailOpen({.code = Code::kSource, .detail = static_cast<int>(opened.error())});
    }
    // 在公开任何端点/授信前完成全部初始 post；部分接受只记真实前缀，不重试、不猜测未知后缀。
    while (state.rx.posted() < state.pool.rx_depth()) {
        const auto posted = state.rx.Refill();
        const auto accepted = posted ? std::optional<std::size_t>(*posted) : posted.error().accepted;
        if (accepted) {
            if (const auto recorded = state.RecordInitial(*accepted); !recorded) {
                return state.FailOpen(recorded.error());
            }
        } else {
            state.initial_accepted_known = false;
        }
        if (!posted) {
            return state.FailOpen(ReceiveError(posted.error()));
        }
        if (!*posted) {
            return state.FailOpen({.code = Code::kReceive});
        }
    }
    if (const auto opened = state.receive.Open(state.rx, state.credits, state.routes); !opened) {
        return state.FailOpen({.code = Code::kEager, .detail = static_cast<int>(opened.error().code)});
    }
    state.opened = true;
    state.stage = RawDomainStage::kReady;
    return {};
}
std::expected<RawQueueBinding, RawDomainError> RawDomain::Bind(ConnectionProgress& progress) noexcept {
    if (!state_ || state_->stage != RawDomainStage::kReady) {
        return Error(Code::kNotReady);
    }
    auto& state = *state_;
    if (state.bound) {
        return Error(Code::kInUse);
    }
    if (const auto opened = state.io.Open(progress, state.receive, state.sources); !opened) {
        return Error(Code::kProgress, static_cast<int>(opened.error().code), opened.error().detail);
    }
    state.bound = true;
    return RawQueueBinding{.pool = &state.pool,
                           .rx = &state.rx,
                           .ledger = &state.ledger,
                           .on_tx = &ConnectionIoBinding::TxCallback,
                           .on_rx = &ConnectionIoBinding::RxCallback,
                           .cookie = &state.io,
                           .on_error = &ConnectionIoBinding::ErrorCallback,
                           .watermark_threshold = state.config.watermark_threshold,
                           .external_refill = true};
}
std::expected<RawDomainResources, RawDomainError> RawDomain::resources() noexcept {
    if (!state_ || !state_->opened || state_->stage == RawDomainStage::kClosed) {
        return Error(Code::kNotReady);
    }
    auto& s = *state_;
    return RawDomainResources{&s.pool, &s.rx, &s.ledger, &s.sender, &s.credits, &s.routes, &s.receive, &s.sources};
}
RawDomainSnapshot RawDomain::Inspect() const noexcept {
    if (!state_) {
        return {};
    }
    const auto& s = *state_;
    return {.stage = s.stage,
            .close_stage = s.closer.stage(),
            .posted = s.rx.posted(),
            .held = s.rx.held(),
            .attempts = s.ledger.size(),
            .routes = s.routes.active(),
            .sources = s.sources.size(),
            .initial_accepted = s.initial_accepted,
            .retired_rx = s.retired_rx,
            .initial_accepted_known = s.initial_accepted_known,
            .bound = s.bound,
            .credits = s.credits.stats(),
            .open_error = s.open_error};
}
std::expected<void, RawDomainError> RawDomain::BeginClose() noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto& state = *state_;
    if (state.stage == RawDomainStage::kClosed) {
        return {};
    }
    state.routes.StopAccepting();
    state.credits.StopAdmission();
    if (state.pool.retained()) {
        state.stage = RawDomainStage::kRetained;
        state.receive.Freeze();
        return Error(Code::kRetained);
    }
    if (state.stage == RawDomainStage::kClosing) {
        return {};
    }
    if (state.rx.pool()) {
        if (const auto begun = state.closer.Begin(state.pool, state.rx); !begun) {
            return std::unexpected(RawDomainError{.code = Code::kClose, .close_error = begun.error()});
        }
    }
    state.stage = RawDomainStage::kClosing;
    return {};
}
std::expected<RawDomainSnapshot, RawDomainError> RawDomain::AdvanceClose(const RawDomainCloseProof& proof) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto& state = *state_;
    if (state.stage == RawDomainStage::kClosed) {
        return Inspect();
    }
    if (state.pool.retained() || state.stage == RawDomainStage::kRetained) {
        state.stage = RawDomainStage::kRetained;
        return Error(Code::kRetained);
    }
    if (state.stage != RawDomainStage::kClosing) {
        return Error(Code::kNotReady);
    }
    // 不能以关闭探测接口代替无副作用预检；Credit.Close 的失败本身就会冻结整个接收域。
    if (!proof.progress_detached || state.routes.active() || state.sources.size() || state.ledger.size() ||
        state.credits.stats().withdrawal_bindings) {
        return Error(Code::kInUse);
    }
    if (const auto drained = state.pool.CheckTxDrained(); !drained) {
        // 空账本不代表 fake CQE/flush 边界已消费；保持回调绑定供原 owner 继续排空。
        return std::unexpected(QueueError(drained.error()));
    }
    if (state.bound) {
        if (const auto closed = state.io.Close(true); !closed) {
            return Error(Code::kProgress, static_cast<int>(closed.error().code), closed.error().detail);
        }
        state.bound = false;
    }
    const auto phase = state.closer.stage();
    if (phase == raw::ReceiveDomainCloseStage::kIdle) {
        // 没有成功打开 RX 的部分创建；仍须确认事件/外部使用者退出后重试底层回滚。
        if (!All(proof.quiescence)) {
            return Error(Code::kInUse);
        }
        if (const auto closed = state.pool.Close(); !closed) {
            return std::unexpected(QueueError(closed.error()));
        }
    } else {
        if (phase != raw::ReceiveDomainCloseStage::kDraining && !All(proof.quiescence)) {
            return Error(Code::kInUse);
        }
        const auto advanced = state.closer.Advance(proof.quiescence, state.close_events);
        if (!advanced) {
            if (state.closer.stage() == raw::ReceiveDomainCloseStage::kRetained) {
                state.stage = RawDomainStage::kRetained;
            }
            return std::unexpected(RawDomainError{.code = Code::kClose, .close_error = advanced.error()});
        }
        state.retired_rx = advanced->retired_rx;
        for (std::size_t i = 0; i < advanced->rx_events; ++i) {
            const auto& event = state.close_events[i];
            const auto held_before = state.rx.held();
            if (event.lease) {
                if (state.receive.buffers()) {
                    // 正常来源/逻辑连接均已退出。迟到 CQE 按未知责任归账并冻结，保留至物理销毁。
                    (void)state.receive.Dispatch(event, {}, nullptr, 0);
                } else {
                    (void)state.rx.Release(*event.lease);
                }
            }
            if (!event.lease || !held_before || state.rx.held() != held_before - 1) {
                state.pool.RetainDependencies();
                state.stage = RawDomainStage::kRetained;
                return Error(Code::kRetained);
            }
        }
        if (advanced->stage != raw::ReceiveDomainCloseStage::kClosed) {
            return Inspect();
        }
    }
    if (const auto closed = state.CloseMetadata(); !closed) {
        return std::unexpected(closed.error());
    }
    return Inspect();
}
} // namespace runtime
} // namespace kbsocket
