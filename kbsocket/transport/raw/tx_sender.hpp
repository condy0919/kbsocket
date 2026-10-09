// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_TX_SENDER_HPP_
#define KBSOCKET_TRANSPORT_RAW_TX_SENDER_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/urma_api.hpp" // IWYU pragma: keep

namespace kbsocket {
namespace raw {
/// 首版仅支持普通、非 inline、全 signal SEND，SGE 必须引用已注册的本地内存。
/// 描述符覆盖 Send 调用；目标、注册段和 payload 覆盖对应 attempt 的完整生命周期。
struct TxSendRequest {
    AttemptMetadata metadata;
    std::span<urma_sge_t> sges;
};

enum class TxSendErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kNotReady,
    kInUse,
    kExhausted,
    kLedgerFailure,
    kPoolFailure,
    kPostFailed,
    kProviderContract,
    kAccountingFailure,
};

struct TxSendError {
    TxSendErrorCode code;
    /// 已接受前缀长度；nullopt 表示 provider 未给出可信边界，整批都不得重试或回收。
    std::optional<std::size_t> accepted = 0;
    int provider_status = 0;
    std::optional<AttemptLedgerError> ledger_error;
    std::optional<JettyPoolFailure> pool_error;
};

/// 一个账本对应一个提交器，由相同 owner 调用；post、记账、poll 不得并发或重入。
/// 调用方负责保证上述契约，且不得在 Send 执行期间调用 Open/Close；内部不做重入检查。
/// WR 描述符内嵌固定数组，Open/Send 均不分配；Send 不加锁、不自动重试。
/// 仅借用 ledger/pool；两者必须覆盖提交器使用期，期间不得 Close/Open 重绑。
/// 提交器不拥有在途资源，Close 仅解除绑定；账本与 DMA 资源须继续存活到终结。
class TxSender {
public:
    /// 对齐 UMQ_BATCH_SIZE；这是单次提交上限，不是 SQ 深度。
    static constexpr std::uint32_t kMaxBatch = 256;

    TxSender() = default;

    TxSender(const TxSender&) = delete;
    TxSender& operator=(const TxSender&) = delete;

    /// 绑定已打开的账本与池；单次提交数量由 Send 的请求范围决定，上限为 kMaxBatch。
    std::expected<void, TxSendError> Open(AttemptLedger& ledger) noexcept;
    std::expected<void, TxSendError> Close() noexcept;

    /// 支持一条或多条 SEND，同一批使用一个 SQ，可指定 lane 或由池选择。
    /// post 前预留整批；额度不足则全部撤销，不提交部分预留的请求。
    /// ids 至少与 requests 等长；有效输入下清零对应范围，仅保留未退休记录的 id。
    /// 成功返回请求数；部分失败返回 accepted，前缀 id 留在账本，已取消后缀 id 为 0。
    /// kProviderContract 时整批记录仍为 Prepared，须查明终结状态后处理，不得当作未提交而取消。
    /// kAccountingFailure 时非零 id 必须保留用于人工恢复；提交器停止后续发送。
    std::expected<std::size_t, TxSendError> Send(std::span<const TxSendRequest> requests, std::span<AttemptId> ids,
                                                 std::optional<JettyLane> lane = {}) noexcept;

private:
    std::optional<AttemptLedgerError> Cancel(std::span<AttemptId> ids) noexcept;

    AttemptLedger* ledger_ = nullptr;
    JettyPool* pool_ = nullptr;
    std::array<urma_jfs_wr_t, kMaxBatch> wrs_{};
    std::uint64_t next_sequence_ = 0;
    bool blocked_ = false;
};
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_TX_SENDER_HPP_
