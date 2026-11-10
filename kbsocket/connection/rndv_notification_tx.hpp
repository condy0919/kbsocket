// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_RNDV_NOTIFICATION_TX_HPP_
#define KBSOCKET_CONNECTION_RNDV_NOTIFICATION_TX_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>

#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/connection/tx_sequence_window.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/connection/write_rndv_pool.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace connection {
class RndvNotificationTx;
class RndvRxIngress;
struct RndvNotificationHandle {
    const RndvNotificationTx* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const RndvNotificationHandle&) const = default;
};
struct RndvRtsReservation {
    RndvNotificationHandle rts;
    RndvNotificationHandle done;
};
struct RndvNotificationConfig {
    // TX 完成分发的独立 owner 身份，不能与共用账本的 Eager/WRITE owner 冲突。
    std::uint64_t connection_id = 0;
    RndvDestination destination;
    CreditIdentity credit_identity;
    control::EndpointImportKey endpoint;
    raw::JettyLane lane;
    std::uint32_t capacity = 16;
};
enum class RndvNotificationErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kNoMemory,
    kWouldBlock,
    kWrongIdentity,
    kInvalidHandle,
    kInvalidState,
    kInvalidCompletion,
    kMemory,
    kImport,
    kCredit,
    kSequence,
    kGrant,
    kEvidence,
    kPost,
    kCompletion,
    kAcceptanceUnknown,
    kRetained
};
struct RndvNotificationError {
    RndvNotificationErrorCode code;
    int provider_status = 0;
};
struct RndvNotificationStatus {
    WireControlType type = WireControlType::kRndvRts;
    bool prepared = false;
    bool accepted = false;
    bool acceptance_known = true;
    bool retired = false;
    bool cancelled = false;
    bool failed = false;
    bool grant_published = false;
    int completion_status = 0;
};
struct RndvNotificationStats {
    std::size_t active = 0;
    std::size_t queued = 0;
    std::size_t in_flight = 0;
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    bool stopped = false;
    bool failed = false;
};

/// 单连接单 owner 的有界通知发送器；Open 预留每槽注册 buffer 和独立 endpoint pin。
/// 通知使用固定 Lane、全 signal SEND_WITH_IMM，不重放、不迁移、不排空整个共享 SQ。
/// sender/Slab/cache/credits/序号窗口须覆盖 Open 到 Close；GrantManager 须覆盖其通知记录。
/// 一旦成功 QueueRts，后续只使用同一个共同序号窗口；未终结析构保留依赖。
/// 失败停止本队列的新 post；宿主仍须 Cancel/Close 收束其他已排队通知和远端授权责任。
/// 仅负责通知发送；接收来源认证、RTS 重排占位、Grant 交付和远端静默仍是独立责任。
class RndvNotificationTx {
public:
    static constexpr std::uint32_t kMaxCapacity = 256;
    RndvNotificationTx() noexcept;
    ~RndvNotificationTx();
    RndvNotificationTx(const RndvNotificationTx&) = delete;
    RndvNotificationTx& operator=(const RndvNotificationTx&) = delete;
    std::expected<void, RndvNotificationError> Open(const RndvNotificationConfig& config, raw::TxSender& sender,
                                                    memory::SlabAllocator& slab, control::EndpointImportCache& imports,
                                                    control::ImportLease endpoint,
                                                    SenderCreditAccount& credits) noexcept;
    /// 原子取得两个本地槽、未来 DONE 控制额度和共同 SN；RTS 在 post 时另取 data 额度。
    /// transfer.seq 必须为共同窗口下一 SN；成功后 future DONE 槽绑定此完整事务，不可挪作他用。
    std::expected<RndvRtsReservation, RndvNotificationError> QueueRts(const GrantTransfer& transfer,
                                                                      TxSequenceWindow& sequence) noexcept;
    /// 独占该 Grant 的发布责任；任何可能 post 前内部 CommitPublication，未知接受或取消后隔离。
    /// manager/reservation 必须来自同一真实授权；不接管接收端 ReorderReservation 或应用借用。
    std::expected<RndvNotificationHandle, RndvNotificationError>
    QueueGrant(GrantManager& manager, const GrantReservation& reservation) noexcept;
    /// 仅填充 QueueRts 预留的 DONE 槽；调用时重新读取原事务池的成功证据并核对完整请求。
    /// 取得快照后宿主可释放 WRITE 本地槽；通知源责任仍独立持有至可信 SEND 终结。
    std::expected<void, RndvNotificationError> QueueDone(RndvNotificationHandle handle, const WriteRndvPool& pool,
                                                         WriteRndvHandle write) noexcept;
    /// 每次最多一次单 WR post；0 接受可重试，接受未知永久保留且停止。不会长期占用共账 post reservation。
    std::expected<std::size_t, RndvNotificationError> Progress() noexcept;
    std::expected<void, RndvNotificationError> OnCompletion(const raw::TxCompletionEvent& event) noexcept;
    /// 取消只停止未来 post；已接受通知等待 CQE，不能撤回远端看到的 GRANT/DONE。
    std::expected<void, RndvNotificationError> Cancel(RndvNotificationHandle handle) noexcept;
    std::expected<void, RndvNotificationError> Release(RndvNotificationHandle handle) noexcept;
    std::expected<RndvNotificationStatus, RndvNotificationError> status(RndvNotificationHandle handle) const noexcept;
    RndvNotificationStats stats() const noexcept;
    /// 停止新 RTS/GRANT，已有 WRITE 仍可 QueueDone 和推进；失败由 Cancel/Close 显式处理。
    void Stop() noexcept;
    std::expected<void, RndvNotificationError> Close() noexcept;

private:
    friend class RndvRxIngress;
    std::expected<void, RndvNotificationError> BindIngress(const RndvRxIngress& ingress, urma_context_t* context,
                                                           std::uint64_t peer) noexcept;
    std::expected<void, RndvNotificationError> UnbindIngress(const RndvRxIngress& ingress) noexcept;
    struct Entry;
    struct State;
    std::expected<Entry*, RndvNotificationError> Find(RndvNotificationHandle handle) const noexcept;
    std::expected<void, RndvNotificationError> CancelEntry(Entry& entry) noexcept;
    std::expected<void, RndvNotificationError> Finish(Entry& entry) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_RNDV_NOTIFICATION_TX_HPP_
