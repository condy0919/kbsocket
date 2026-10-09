// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/raw/tx_completion_processor.hpp"

#include <algorithm>

namespace kbsocket {
namespace raw {
namespace {
// 仅接受公共 URMA poll 契约中的逐 WR 终结状态。WR_UNHANDLED 来自显式 flush，不在此推断。
bool IsWrCompletion(urma_cr_status_t status) noexcept {
    switch (status) {
    case URMA_CR_SUCCESS:
    case URMA_CR_UNSUPPORTED_OPCODE_ERR:
    case URMA_CR_LOC_LEN_ERR:
    case URMA_CR_LOC_OPERATION_ERR:
    case URMA_CR_LOC_ACCESS_ERR:
    case URMA_CR_REM_RESP_LEN_ERR:
    case URMA_CR_REM_UNSUPPORTED_REQ_ERR:
    case URMA_CR_REM_OPERATION_ERR:
    case URMA_CR_REM_ACCESS_ABORT_ERR:
    case URMA_CR_ACK_TIMEOUT_ERR:
    case URMA_CR_RNR_RETRY_CNT_EXC_ERR:
    case URMA_CR_WR_FLUSH_ERR:
    case URMA_CR_LOC_DATA_POISON:
    case URMA_CR_REM_DATA_POISON:
        return true;
    default:
        return false;
    }
}
} // namespace

std::expected<TxCompletionBatch, TxCompletionError>
TxCompletionProcessor::Poll(std::span<TxCompletionEvent> events) noexcept {
    auto* pool = ledger_.pool();
    if (!pool || !pool->ready()) {
        return std::unexpected(TxCompletionError{.code = TxCompletionErrorCode::kNotReady});
    }
    if (events.empty()) {
        return std::unexpected(TxCompletionError{.code = TxCompletionErrorCode::kInvalidArgument});
    }
    const auto budget = std::min(events.size(), completions_.size());
    // provider 对 fake CQE 不保证填充 user_ctx，清零避免把上一轮残留字段暴露为有效信息。
    std::fill_n(completions_.begin(), budget, urma_cr_t{});
    auto count = pool->PollSend(std::span(completions_).first(budget));
    if (!count) {
        return std::unexpected(
            TxCompletionError{.code = TxCompletionErrorCode::kPollFailed, .pool_error = count.error()});
    }
    TxCompletionBatch batch{.count = static_cast<std::size_t>(*count)};
    for (std::size_t i = 0; i < batch.count; ++i) {
        events[i] = Process(*pool, completions_[i]);
        batch.retired += events[i].record.has_value();
    }
    return batch;
}

TxCompletionEvent TxCompletionProcessor::Process(JettyPool& pool, const urma_cr_t& cr) noexcept {
    TxCompletionEvent event{.completion = cr};
    if (cr.flag.bs.s_r || !cr.flag.bs.jetty) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnexpectedCompletion};
        return event;
    }
    auto lane = pool.FindLane(cr.local_id);
    if (!lane) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnknownLane, .pool_error = lane.error()};
        return event;
    }
    event.lane = *lane;
    // 非成功 CQE 保守隔离整个物理 SQ，但只退休能明确归属的那条 WR。
    // 不把一个连接的错误伪造成同 SQ 上其他连接的完成。
    if (cr.status != URMA_CR_SUCCESS) {
        auto fault = pool.MarkFaulted(*lane);
        if (!fault) {
            event.error = TxCompletionError{.code = TxCompletionErrorCode::kPoolFailure, .pool_error = fault.error()};
            return event;
        }
    }
    if (cr.status == URMA_CR_WR_FLUSH_ERR_DONE || cr.status == URMA_CR_WR_SUSPEND_DONE) {
        event.kind =
            cr.status == URMA_CR_WR_FLUSH_ERR_DONE ? TxCompletionKind::kFlushDone : TxCompletionKind::kSuspendDone;
        event.completion.user_ctx = 0;
        return event;
    }
    if (!IsWrCompletion(cr.status)) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnexpectedCompletion};
        return event;
    }
    auto record = ledger_.Lookup(cr.user_ctx);
    if (!record) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kLedgerFailure, .ledger_error = record.error()};
        return event;
    }
    const auto& recorded_lane = record->ticket.lane;
    if (recorded_lane.pool != lane->pool || recorded_lane.index != lane->index || recorded_lane.epoch != lane->epoch) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kWrongLane};
        return event;
    }
    auto retired = ledger_.Complete(cr.user_ctx);
    if (!retired) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kLedgerFailure, .ledger_error = retired.error()};
        return event;
    }
    event.kind = TxCompletionKind::kCompleted;
    event.record = *retired;
    return event;
}
} // namespace raw
} // namespace kbsocket
