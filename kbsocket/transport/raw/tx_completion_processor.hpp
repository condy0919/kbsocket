// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TRANSPORT_RAW_TX_COMPLETION_PROCESSOR_HPP_
#define KBSOCKET_TRANSPORT_RAW_TX_COMPLETION_PROCESSOR_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
enum class TxCompletionErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kNotReady,
    kPollFailed,
    kUnexpectedCompletion,
    kUnknownLane,
    kWrongLane,
    kLedgerFailure,
    kPoolFailure,
};

struct TxCompletionError {
    TxCompletionErrorCode code;
    std::optional<AttemptLedgerError> ledger_error;
    std::optional<JettyPoolFailure> pool_error;
};

enum class TxCompletionKind : std::uint8_t {
    kCompleted,
    kFlushDone,
    kSuspendDone,
    kRejected,
};

/// 每条取出的 CQE 都有对应结果，包括无效 CQE，调用方不能忽略 kRejected。
struct TxCompletionEvent {
    TxCompletionKind kind = TxCompletionKind::kRejected;
    urma_cr_t completion{};
    std::optional<JettyLane> lane;
    /// 仅 kCompleted 有退休记录；成功或失败由 completion.status 判断。
    /// 租约标识交回上层后，仍需按协议决定 buffer/grant 的回收时机。
    std::optional<AttemptRecord> record;
    std::optional<TxCompletionError> error;
};

struct TxCompletionBatch {
    std::size_t count = 0;
    /// 已退休 WR 数；用于推进等待者，不代表每个连接现在都可写。
    /// 故障 SQ 的额度虽已归还，该 SQ 仍不接受新发送。
    std::size_t retired = 0;
};

/// 单 owner 的全 signal TX CQ 推进器，自身不分配内存、不调用上层回调、不执行重放。
/// 账本须覆盖对象使用期，并拥有所属池全部 TX attempt；同一 TX CQ 只能由此 owner 消费。
/// 与 Send、账本操作及 Open/Close 不得并发或重入。不处理 RX、异步事件和软件 flush。
class TxCompletionProcessor {
public:
    static constexpr std::size_t kMaxPollBatch = 64;
    explicit TxCompletionProcessor(AttemptLedger& ledger) noexcept : ledger_(ledger) {}
    TxCompletionProcessor(const TxCompletionProcessor&) = delete;
    TxCompletionProcessor& operator=(const TxCompletionProcessor&) = delete;

    /// 最多取 min(events.size(), kMaxPollBatch) 条 CQE；每条均输出到调用方缓冲。
    /// poll 失败通过 expected 返回；单条解析失败放在事件中，不中断整批处理。
    /// 边界事件不读取 user_ctx、不清空账本；所有 WR 仍须逐条获得可信终结证据。
    std::expected<TxCompletionBatch, TxCompletionError> Poll(std::span<TxCompletionEvent> events) noexcept;

private:
    TxCompletionEvent Process(JettyPool& pool, const urma_cr_t& cr) noexcept;
    AttemptLedger& ledger_;
    std::array<urma_cr_t, kMaxPollBatch> completions_{};
};
} // namespace raw
} // namespace kbsocket
#endif // KBSOCKET_TRANSPORT_RAW_TX_COMPLETION_PROCESSOR_HPP_
