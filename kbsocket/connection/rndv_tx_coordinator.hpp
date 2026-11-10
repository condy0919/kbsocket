// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_RNDV_TX_COORDINATOR_HPP_
#define KBSOCKET_CONNECTION_RNDV_TX_COORDINATOR_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/rndv_notification_tx.hpp"
#include "kbsocket/connection/rndv_rx_ingress.hpp"
#include "kbsocket/connection/tx_sequence_window.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/connection/write_rndv_engine.hpp"
#include "kbsocket/connection/write_rndv_pool.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/control/rndv_capabilities.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"

namespace kbsocket {
namespace connection {
class RndvTxCoordinator;
enum class RndvTxErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kStopped,
    kWouldBlock,
    kWrongIdentity,
    kInvalidHandle,
    kInvalidCompletion,
    kProtocol,
    kMemory,
    kImport,
    kNotification,
    kWrite,
    kTimeout,
    kRetained
};
struct RndvTxError {
    RndvTxErrorCode code;
};
struct RndvTxHandle {
    const RndvTxCoordinator* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const RndvTxHandle&) const = default;
};
/// 从已认证、已协商的目录构造请求；可适配 ConnectionHandshake::PrepareRndvImport。
/// 不执行慢 provider、不保存输入引用、不重入协调器。WouldBlock 允许无副作用重试。
using RndvImportResolver =
    std::expected<control::TsegImportRequest, RndvTxError> (*)(void* owner, const GrantIdentity& grant) noexcept;
struct RndvTxConfig {
    WireSessionBinding grant_route;
    control::RndvLimits limits;
    std::uint32_t capacity = 4;
    std::uint64_t timeout_ns = 10000000000;
    RndvImportResolver resolve_import = nullptr;
    void* import_owner = nullptr;
};
struct RndvTxStatus {
    GrantTransfer transfer;
    bool grant_received = false;
    bool rts_accepted = false;
    bool done_queued = false;
    bool sources_released = false;
    bool complete = false;
    bool success = false;
    bool cancelled = false;
    bool acceptance_known = true;
    WriteRndvStats write;
};
struct RndvTxStats {
    std::size_t active = 0;
    std::uint64_t active_bytes = 0;
    bool stopped = false;
    bool failed = false;
};

/// 单 owner、固定连接/Arena/Lane 的发送事务协调器；Open 预分配，提交和进度无分配。
/// 借用通知队列及 WRITE 池并独占绑定；双向接收 ingress 仍可使用同一通知队列。
/// 宿主显式推进通知 Progress、控制导入暂停域、物理 CQE，并保留所有依赖至 Close 成功。
/// 成功通知只能在真实 WRITE 全成功后排队；本地收束不证明远端已消费或撤销 Grant。
class RndvTxCoordinator {
public:
    static constexpr std::uint32_t kMaxCapacity = WriteRndvPool::kMaxSlots;
    static constexpr std::size_t kMaxBudget = 128;
    RndvTxCoordinator() noexcept;
    ~RndvTxCoordinator();
    RndvTxCoordinator(const RndvTxCoordinator&) = delete;
    RndvTxCoordinator& operator=(const RndvTxCoordinator&) = delete;
    std::expected<void, RndvTxError> Open(const RndvTxConfig& config, WriteRndvPool& writes,
                                          RndvNotificationTx& notifications, TxSequenceWindow& sequence,
                                          memory::SlabAllocator& slab, control::EndpointImportCache& endpoints,
                                          control::ImportLease endpoint, control::TsegImportCache& segments) noexcept;
    /// 成功后持有每个源 slice 的独立 Reserved 引用，应用可 ReleaseCpu，但必须停止使用旧 CPU 视图。
    /// 失败且正常回滚不取得源所有权或 SN；Retained 表示内部责任无法核销，宿主须保留依赖。
    std::expected<RndvTxHandle, RndvTxError> Submit(std::span<const WriteRndvSource> sources,
                                                    std::uint64_t now_ns) noexcept;
    /// 验证真实反向来源、原 RTS 和单 Arena；只保存值，不保存 RX span；RTS 已接受即可早于其 CQE。
    std::expected<void, RndvTxError> OnGrant(const WireFrameView& view, const WireReceiveContext& observed,
                                             std::uint64_t now_ns) noexcept;
    /// 可直接作为 RndvIngressConfig::on_grant；任何协议错误同时取消本逻辑发送流。
    static std::expected<void, RndvIngressError> DispatchGrant(void* owner, const WireFrameView& view,
                                                               const WireReceiveContext& observed,
                                                               std::uint64_t now_ns) noexcept;
    /// 每次最多扫描 budget 个槽，每槽最多一次有界 WRITE post；不代替通知或缓存调度源。
    std::expected<std::size_t, RndvTxError> Progress(std::uint64_t now_ns, std::size_t budget = 8) noexcept;
    /// 按独立 connection_id 分发本 WRITE 池或共用通知队列的真实逐 WR 事件。
    std::expected<void, RndvTxError> OnCompletion(const raw::TxCompletionEvent& event) noexcept;
    std::expected<RndvTxStatus, RndvTxError> status(RndvTxHandle handle) const noexcept;
    /// 本地事实只供可信宿主故障清理；未收到 GRANT 或未知 WRITE 接受时没有此证明。
    std::expected<GrantTerminalEvidence, RndvTxError> TerminalEvidence(RndvTxHandle handle) const noexcept;
    std::expected<void, RndvTxError> Release(RndvTxHandle handle) noexcept;
    /// 宿主确认其前全部逻辑 SN 已被远端消费；不能用本地通知/WRITE 完成替代，含 Eager SN。
    std::expected<void, RndvTxError> AdvanceWindow(std::uint64_t consumed_prefix) noexcept;
    void Stop() noexcept;
    /// 取消整个逻辑流并停止新提交；已接受 WR/未知接受继续保留，不排空健康共享 SQ、不重放。
    std::expected<void, RndvTxError> Cancel() noexcept;
    std::expected<void, RndvTxError> Close() noexcept;
    RndvTxStats stats() const noexcept;

private:
    struct Entry;
    struct State;
    std::expected<Entry*, RndvTxError> Find(RndvTxHandle handle) const noexcept;
    std::expected<void, RndvTxError> ObserveTime(std::uint64_t now_ns) noexcept;
    std::expected<bool, RndvTxError> Step(Entry& entry) noexcept;
    std::expected<bool, RndvTxError> Cleanup(Entry& entry) noexcept;
    bool ReleaseSources(Entry& entry) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_RNDV_TX_COORDINATOR_HPP_
