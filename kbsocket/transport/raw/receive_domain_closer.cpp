// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/raw/receive_domain_closer.hpp"

#include "kbsocket/base/log.hpp"

namespace kbsocket {
namespace raw {

ReceiveDomainCloser::~ReceiveDomainCloser() {
    if (stage_ != ReceiveDomainCloseStage::kIdle && stage_ != ReceiveDomainCloseStage::kClosed) {
        KBSOCKET_LOG_ERROR("Receive domain close incomplete: stage={}; retain the entire dependency chain",
                           static_cast<unsigned>(stage_));
    }
}

std::expected<void, ReceiveDomainCloseError> ReceiveDomainCloser::Begin(JettyPool& pool,
                                                                        RxBufferPool& buffers) noexcept {
    if (stage_ != ReceiveDomainCloseStage::kIdle || pool.close_owner_) {
        return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kInUse});
    }
    if (!pool.ready() || pool.rx_owner_ != &buffers || buffers.pool_ != &pool) {
        return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kInvalidArgument});
    }
    pool_ = &pool;
    buffers_ = &buffers;
    pool.close_owner_ = this;
    pool.StopAdmission();
    stage_ = ReceiveDomainCloseStage::kDraining;
    return {};
}

ReceiveDomainCloseProgress ReceiveDomainCloser::Progress(std::size_t rx_events) const noexcept {
    return {.stage = stage_, .rx_events = rx_events, .retired_rx = retired_rx_};
}

std::expected<ReceiveDomainCloseProgress, ReceiveDomainCloseError>
ReceiveDomainCloser::Advance(const ReceiveDomainQuiescence& quiescence, std::span<RxBufferEvent> events) noexcept {
    if (stage_ == ReceiveDomainCloseStage::kIdle) {
        return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kNotStarted});
    }
    if (stage_ == ReceiveDomainCloseStage::kClosed) {
        return Progress();
    }
    if (stage_ == ReceiveDomainCloseStage::kRetained) {
        return std::unexpected(
            ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kRxPollFailed, .rx_error = poll_error_});
    }
    if (stage_ == ReceiveDomainCloseStage::kDraining) {
        if (events.empty()) {
            return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kInvalidArgument});
        }
        if (quiescence.peer_stopped) {
            // 等远端静默后才停止补投，避免关闭协商仍在进行时耗尽 RX 而制造 RNR。
            buffers_->Stop();
        }
        const auto polled = buffers_->Poll(events);
        if (!polled) {
            // 无法可信读取 CQ 时不能以删除协议猜测终结，也不能重入正常 polling。
            poll_error_ = polled.error();
            pool_->RetainDependencies();
            buffers_->Stop();
            stage_ = ReceiveDomainCloseStage::kRetained;
            return std::unexpected(
                ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kRxPollFailed, .rx_error = poll_error_});
        }
        if (*polled) {
            return Progress(*polled);
        }
        if (!quiescence.peer_stopped || !quiescence.events_acked || !quiescence.queue_users_stopped ||
            !quiescence.completion_queues_healthy) {
            return Progress();
        }
        if (!pool_->CheckTxDrained()) {
            return Progress();
        }
        // 后续即使 DeleteJetty 失败也不恢复 post/poll：provider 可能已移除对象映射。
        stage_ = ReceiveDomainCloseStage::kDeletingQueues;
    }
    if (stage_ == ReceiveDomainCloseStage::kDeletingQueues) {
        if (const auto closed = pool_->CloseQueues(); !closed) {
            return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kQueueCloseFailed,
                                                           .queue_error = closed.error()});
        }
        // UDMA 正常非 custom CQ 的删除路径在对象仍有效时清除其剩余 CQE。
        // 成功 DeleteJfr 是残留 RX 的 DMA 屏障；不生成接收完成，也不撤销 Held。
        buffers_->OnJfrDeleted();
        retired_rx_ = buffers_->retired_on_delete();
        stage_ = ReceiveDomainCloseStage::kWaitingForLeases;
    }
    if (stage_ == ReceiveDomainCloseStage::kWaitingForLeases) {
        if (buffers_->held()) {
            return Progress();
        }
        if (const auto closed = buffers_->CloseMemory(); !closed) {
            return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kMemoryCloseFailed,
                                                           .rx_error = closed.error()});
        }
        stage_ = ReceiveDomainCloseStage::kDeletingCompletionQueues;
    }
    if (const auto closed = pool_->CloseCompletionQueues(); !closed) {
        return std::unexpected(ReceiveDomainCloseError{.code = ReceiveDomainCloseErrorCode::kQueueCloseFailed,
                                                       .queue_error = closed.error()});
    }
    pool_->close_owner_ = nullptr;
    stage_ = ReceiveDomainCloseStage::kClosed;
    return Progress();
}
} // namespace raw
} // namespace kbsocket
