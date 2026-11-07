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
/// 支持非 inline、全 signal 的 SEND、SEND_WITH_IMM 和 WRITE。
/// 本地 SGE 必须引用已注册内存；描述符覆盖 Send 调用，目标、注册段和 payload 覆盖 attempt 生命周期。
struct TxSendRequest {
    AttemptMetadata metadata;
    std::span<urma_sge_t> sges;
    /// 仅 URMA_OPC_SEND_IMM 使用，按 URMA 契约传递主机字节序的 64 位值。
    std::uint64_t imm_data = 0;
    /// 仅 WRITE 使用且必须提供：单个已导入的远端段及目标地址，长度等于本地 SGE 总长。
    /// addr 为对端导出的 VA，按 tseg->seg.ubva.va/len 校验；不接受本地 mva 或相对偏移。
    /// 调用方保持 buffer/grant lease 至可信终结；WRITE 完成不自动发送通知或撤销远端授权。
    std::optional<urma_sge_t> remote;
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
    /// 当前 RM_CTP 单边写入契约；同时受池创建时快照的 max_write_size 限制。
    static constexpr std::uint64_t kMaxWriteSize = 200ULL * 1024 * 1024;

    TxSender() = default;

    TxSender(const TxSender&) = delete;
    TxSender& operator=(const TxSender&) = delete;

    /// 绑定已打开的账本与池；单次提交数量由 Send 的请求范围决定，上限为 kMaxBatch。
    /// defer_drain 在异常 post 时只隔离 SQ；宿主须由控制执行器安排 BeginDrain。
    std::expected<void, TxSendError> Open(AttemptLedger& ledger, bool defer_drain = false) noexcept;
    bool defer_drain() const noexcept {
        return defer_drain_;
    }
    urma_context_t* context() const noexcept {
        return pool_ ? pool_->context() : nullptr;
    }
    std::expected<void, TxSendError> Close() noexcept;

    /// 支持 SEND/SEND_WITH_IMM/WRITE 混合提交，同一批使用一个 SQ，可指定 lane 或由池选择。
    /// 混批不建立 WRITE 与 SEND 通知的可见性依赖；需要通知时须先等全部关联 WRITE 成功退休。
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
    std::array<urma_sge_t, kMaxBatch> remote_sges_{};
    std::uint64_t next_sequence_ = 0;
    bool blocked_ = false;
    bool defer_drain_ = false;
};
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_TX_SENDER_HPP_
