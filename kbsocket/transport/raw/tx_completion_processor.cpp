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

bool SameLane(JettyLane lhs, JettyLane rhs) noexcept {
    return lhs.pool == rhs.pool && lhs.index == rhs.index && lhs.epoch == rhs.epoch && lhs.generation == rhs.generation;
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
        return std::unexpected(TxCompletionError{
            .code = TxCompletionErrorCode::kPollFailed,
            .pool_error = count.error(),
        });
    }

    TxCompletionBatch batch{
        .count = static_cast<std::size_t>(*count),
    };
    for (std::size_t i = 0; i < batch.count; ++i) {
        events[i] = Process(*pool, completions_[i]);
        batch.retired += events[i].record.has_value();
    }
    return batch;
}

std::expected<TxCompletionBatch, TxCompletionError>
TxCompletionProcessor::Flush(JettyLane lane, std::span<TxCompletionEvent> events) noexcept {
    auto* pool = ledger_.pool();
    if (!pool || !pool->ready()) {
        return std::unexpected(TxCompletionError{.code = TxCompletionErrorCode::kNotReady});
    }
    if (events.empty()) {
        return std::unexpected(TxCompletionError{.code = TxCompletionErrorCode::kInvalidArgument});
    }

    const auto budget = std::min(events.size(), completions_.size());
    std::fill_n(completions_.begin(), budget, urma_cr_t{});
    auto count = pool->FlushSend(lane, std::span(completions_).first(budget));
    if (!count) {
        return std::unexpected(TxCompletionError{
            .code = TxCompletionErrorCode::kFlushFailed,
            .pool_error = count.error(),
        });
    }

    TxCompletionBatch batch{
        .count = static_cast<std::size_t>(*count),
    };
    for (std::size_t i = 0; i < batch.count; ++i) {
        events[i] = Process(*pool, completions_[i], lane);
        batch.retired += events[i].record.has_value();
    }
    return batch;
}

TxCompletionEvent TxCompletionProcessor::Process(JettyPool& pool, const urma_cr_t& cr,
                                                 std::optional<JettyLane> flushing_lane) noexcept {
    TxCompletionEvent event{
        .completion = cr,
    };
    // fake CQE 的身份从一开始就不可用，包括方向或通道校验失败的情况。
    const bool boundary = cr.status == URMA_CR_WR_FLUSH_ERR_DONE || cr.status == URMA_CR_WR_SUSPEND_DONE;
    if (boundary) {
        event.completion.user_ctx = 0;
    }
    if (cr.flag.bs.s_r || !cr.flag.bs.jetty) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnexpectedCompletion};
        return event;
    }
    // 未知状态和普通 poll 中的 WR_UNHANDLED 没有可信终结语义，不能据其 local_id 隔离 SQ。
    if (flushing_lane ? cr.status != URMA_CR_WR_UNHANDLED : !boundary && !IsWrCompletion(cr.status)) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnexpectedCompletion};
        return event;
    }
    auto lane = pool.FindLane(cr.local_id);
    if (!lane) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnknownLane, .pool_error = lane.error()};
        return event;
    }

    event.lane = *lane;
    if (flushing_lane) {
        if (!SameLane(*lane, *flushing_lane)) {
            event.error = TxCompletionError{.code = TxCompletionErrorCode::kWrongLane};
            return event;
        }
    } else if (boundary) {
        // 无 user_ctx 的边界只能配合本地排空状态消费，不提供新代次故障证据。
        // 替换前的旧边界清理仍依赖 ReplaceDrainedLane 的显式事件/队列屏障。
        if (cr.status == URMA_CR_WR_FLUSH_ERR_DONE) {
            const auto state = pool.lane_state(*lane);
            // 已隔离的同代 SQ 可能在控制线程切 ERROR 前交付边界，先保存证据，不能提前软件 flush。
            auto observed = defer_drain_ && state && *state == JettyLaneState::kFaulted ? pool.RememberFlushDone(*lane)
                                                                                        : pool.ObserveFlushDone(*lane);
            if (!observed) {
                event.error =
                    TxCompletionError{.code = TxCompletionErrorCode::kPoolFailure, .pool_error = observed.error()};
                return event;
            }
            event.kind = TxCompletionKind::kFlushDone;
            return event;
        }
        auto state = pool.lane_state(*lane);
        if (!state || (*state != JettyLaneState::kFaulted && *state != JettyLaneState::kError &&
                       *state != JettyLaneState::kFlushReady)) {
            event.error = TxCompletionError{.code = TxCompletionErrorCode::kUnexpectedCompletion};
            return event;
        }
        // SUSPEND_DONE 只报告已经隔离的 SQ；不把它当作 FLUSH_DONE，也不自行切 ERROR。
        event.kind = TxCompletionKind::kSuspendDone;
        return event;
    }
    auto record = ledger_.LookupPosted(cr.user_ctx);
    if (!record) {
        event.error = TxCompletionError{
            .code = TxCompletionErrorCode::kLedgerFailure,
            .ledger_error = record.error(),
        };
        return event;
    }
    const auto& recorded_lane = record->ticket.lane;
    if (!SameLane(recorded_lane, *lane)) {
        event.error = TxCompletionError{.code = TxCompletionErrorCode::kWrongLane};
        return event;
    }

    // 只有方向、终结类型、已提交身份及完整通道代次均可信后，才允许错误改变 SQ 状态。
    if (!flushing_lane) {
        if (cr.status != URMA_CR_SUCCESS && cr.status != URMA_CR_REM_ACCESS_ABORT_ERR &&
            cr.status != URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
            // 8/10 仅终结当前 WR；ACK_TIMEOUT 和其他 fatal 错误阻止 SQ 复用并启动 ERROR 排空。
            // 真实 FLUSH_ERR 也可能先于本地 ERROR 转换到达，仍须隔离并退休，不能丢失终结证据。
            // BeginDrain 对 Error/FlushReady 幂等，不重复 modify 或回退已取得的边界。
            // modify 失败仍保留当前 WR 的退休记录，控制路径错误与终结证据同时返回。
            auto fault = defer_drain_ ? pool.MarkFaulted(*lane) : pool.BeginDrain(*lane);
            if (!fault) {
                event.error = TxCompletionError{
                    .code = TxCompletionErrorCode::kPoolFailure,
                    .pool_error = fault.error(),
                };
            }
        }
    }
    auto retired = ledger_.Complete(cr.user_ctx);
    if (!retired) {
        event.error = TxCompletionError{
            .code = TxCompletionErrorCode::kLedgerFailure,
            .ledger_error = retired.error(),
            .pool_error = event.error ? event.error->pool_error : std::nullopt,
        };
        return event;
    }
    event.kind = TxCompletionKind::kCompleted;
    event.record = *retired;
    return event;
}

} // namespace raw
} // namespace kbsocket
