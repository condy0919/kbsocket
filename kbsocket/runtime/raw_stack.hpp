// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_RAW_STACK_HPP_
#define KBSOCKET_RUNTIME_RAW_STACK_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"
#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/runtime/connection_command_queue.hpp"
#include "kbsocket/runtime/connection_mailbox.hpp"
#include "kbsocket/runtime/connection_owner.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/peer_credit_progress.hpp"
#include "kbsocket/runtime/progress_source.hpp"
#include "kbsocket/runtime/raw_domain.hpp"
#include "kbsocket/runtime/runtime_shard.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"

namespace kbsocket {
namespace runtime {
struct RawEndpointConfig {
    raw::DeviceConfig device;
    RawDomainConfig domain;
    ConnectionProgressCallback on_connection = nullptr;
    ConnectionMailboxCallback on_command = nullptr;
    PeerCreditProgressCallback on_withdrawal = nullptr;
    void* cookie = nullptr;
};
struct RawStackConfig {
    std::size_t memory_budget_bytes = 64u * 1024u * 1024u;
    memory::SlabConfig slab;
    control::EndpointImportCacheConfig imports;
    std::uint32_t connections_per_context = 256;
    std::uint32_t withdrawals_per_context = 256;
    std::size_t commands_per_context = 256;
    RuntimeShardConfig runtime;
    // 默认保持 Eager-only；启用后段导入与 endpoint 导入共用每 context 的独占暂停屏障。
    bool enable_rndv = false;
    control::TsegImportCacheConfig segments;
};
enum class RawStackStage : std::uint8_t {
    kEmpty,
    kReady,
    kOpenFailed,
    kStopping,
    kClosingDomains,
    kClosingMemory,
    kClosingContexts,
    kClosed,
    kRetained
};
enum class RawStackErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kSession,
    kDomain,
    kMemory,
    kImport,
    kProgress,
    kConnection,
    kMailbox,
    kRetained
};
struct RawStackError {
    RawStackErrorCode code;
    std::size_t context_index = 0;
    std::optional<raw::RuntimeError> session = {};
    std::optional<raw::RuntimeCloseError> session_close = {};
    std::optional<RawDomainError> domain = {};
    std::optional<memory::MemoryError> memory = {};
    std::optional<control::ImportCacheError> import = {};
    std::optional<ProgressError> progress = {};
    std::optional<ConnectionOwnerError> connection = {};
    std::optional<CommandQueueError> mailbox = {};
    std::optional<control::TsegImportCacheError> segment = {};
};
struct RawStackContextResources {
    urma_context_t* context;
    RawDomainResources domain;
    ConnectionOwner* connections;
    ConnectionMailbox* commands;
    PeerCreditProgress* withdrawals;
};
struct RawStackSharedResources {
    memory::SlabAllocator* slab;
    control::EndpointImportCache* imports;
    control::TsegImportCache* segments = nullptr;
};
struct RawStackContextSnapshot {
    RawDomainSnapshot domain;
    std::size_t connections = 0, scheduled = 0, commands = 0, withdrawals = 0;
};
struct RawStackSnapshot {
    RawStackStage stage = RawStackStage::kEmpty;
    std::size_t context_count = 0;
    bool progress_attached = false;
    std::array<RawStackContextSnapshot, RuntimeShard::kMaxContexts> contexts{};
    memory::SlabStats slab;
    std::size_t budget_bytes = 0, import_entries = 0, import_leases = 0, import_tasks = 0;
    bool rndv_enabled = false;
    std::size_t segment_entries = 0, segment_leases = 0, segment_tasks = 0;
    std::optional<RawStackError> open_error;
    std::optional<RawStackError> close_error;
};
struct RawStackCloseProof {
    // 所有 mailbox producer/collector 已退出；零 outstanding 不是线程静默证明。
    bool clients_stopped = false;
    // 按配置顺序，每个域独立的远端/事件/外部访问/CQ 证明；必须覆盖全部 context。
    std::span<const raw::ReceiveDomainQuiescence> domains;
};

/// 一个进程独占 URMA 会话、最多四 context 的单 owner 资源栈，所有成员地址稳定。
/// 宿主先 Load provider；本类拥有 Init/context/域/Slab/import/连接/mailbox/撤回源/Runtime。
/// Open 全部成功才公开借用；失败或未关闭析构保留整份状态，不能提前 Unload provider。
/// 所有控制入口须在 owner 上串行调用且不得重入；暂停时禁止绕过资源视图访问同 context。
/// 借用视图不得单独 Close/Open 成员；共享 Slab/import 的访问必须避开所有 context 的暂停窗口。
/// Notify 也由 owner 调用；业务线程只使用已取得的 mailbox 指针，并在关闭前退出全部访问。
/// 不代办认证、Peer 发送账本或静默证明，不自动重放、重连、强制关闭连接及故障 CQ 回收。
class RawStack {
public:
    static constexpr std::size_t kMaxContexts = RuntimeShard::kMaxContexts;
    RawStack() noexcept;
    ~RawStack();
    RawStack(const RawStack&) = delete;
    RawStack& operator=(const RawStack&) = delete;
    std::expected<void, RawStackError> Open(std::span<const RawEndpointConfig> endpoints,
                                            const RawStackConfig& config = {}) noexcept;
    std::expected<RawStackContextResources, RawStackError> resources(std::size_t context) noexcept;
    std::expected<RawStackSharedResources, RawStackError> shared() noexcept;
    std::expected<RawStackSnapshot, RawStackError> Inspect() const noexcept;
    RawStackStage stage() const noexcept;
    bool paused(std::size_t context) const noexcept;
    std::expected<ProgressBatch, ProgressError> Tick() noexcept;
    std::expected<ProgressBatch, ProgressError> RunOnce() noexcept;
    std::expected<void, ProgressError> Notify() noexcept;
    std::expected<void, ProgressError> RequestReplacement(std::size_t context, raw::JettyLane lane,
                                                          const raw::JettyLaneQuiescence& proof) noexcept;
    /// 停止新业务/授信/内存准入并使 import 失效，保留 Runtime 控制调度以执行 unimport/排空。
    /// 尚有控制任务暂停 context 时返回 InUse，继续 Tick 后重试；不会部分停止其他域。
    std::expected<void, RawStackError> BeginClose() noexcept;
    /// 调用方先逐连接 Close/RetireSources/Release、撤回 Release/Abort、收割 mailbox 并归还外部 Lease。
    /// 先检查全部责任，再解绑 Runtime；随后逆序关闭域、共享 Slab、context 和 URMA 会话。
    /// 未满足门禁返回 InUse 并保留调度；进入物理关闭后只能归还 Held/重试关闭，不能继续 Tick。
    std::expected<RawStackSnapshot, RawStackError> AdvanceClose(const RawStackCloseProof& proof) noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_RAW_STACK_HPP_
