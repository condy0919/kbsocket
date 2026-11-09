// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_TSEG_IMPORT_CACHE_HPP_
#define KBSOCKET_CONTROL_TSEG_IMPORT_CACHE_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>

#include "kbsocket/runtime/control_executor.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace control {

/// 本地 context 地址不能单独代表实例；远端 Slab 重建必须更新完整代次。
/// Slab 身份由认证宿主绑定到 Grant Arena；每事务 Grant 代次不参与缓存共享键。
struct TsegImportKey {
    urma_context_t* local_context = nullptr;
    std::uint64_t local_context_incarnation = 0;
    std::uint64_t peer_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t slab_id = 0;
    std::uint64_t slab_generation = 0;
    std::array<std::uint8_t, URMA_EID_SIZE> eid{};
    std::uint64_t authorization_identity = 0;
    bool operator==(const TsegImportKey&) const = default;
};

struct TsegImportRequest {
    TsegImportKey key;
    /// 可信同 ABI 的固定段描述，不接受扩展、映射、非固定页或任意网络字节。
    /// 只支持 NONE/PLAIN_TEXT token 与远端 WRITE；ImportSeg 固定 WRITE/NOMAP/addr=0。
    urma_seg_t descriptor{};
    urma_token_t token{};
};

class TsegImportCache;
/// 每份引用有独立票据；复制结构不创建新引用，重复 Release 被拒绝。
struct TsegImportLease {
    const TsegImportCache* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t sequence = 0;
    bool operator==(const TsegImportLease&) const = default;
};

enum class TsegImportOperationKind : std::uint8_t { kImport, kUnimport };
struct TsegImportOperation {
    const TsegImportCache* owner = nullptr;
    std::uint32_t entry = 0;
    std::uint64_t generation = 0;
    TsegImportOperationKind kind = TsegImportOperationKind::kImport;
    urma_context_t* context = nullptr;
    std::uint64_t context_incarnation = 0;
    bool operator==(const TsegImportOperation&) const = default;
};

/// Prepare 后参数被独占保留，排队失败可重试同一 task；reservation 防止旧撤销误伤重新准备的操作。
struct TsegImportPreparedOperation {
    TsegImportOperation operation;
    std::uint64_t reservation = 0;
    runtime::ControlTask task;
};

/// 宿主先暂停该 context 全部 post/poll/事件访问，并排除其他慢控制操作；
/// 屏障保持至 HandleCompletion 收割，不能仅凭任务已完成或 fd 可读恢复。
struct TsegImportContextPause {
    urma_context_t* context = nullptr;
    std::uint64_t incarnation = 0;
    bool users_paused = false;
    bool exclusive_control = false;
};

enum class TsegImportCacheErrorCode : std::uint8_t {
    kInvalidArgument,
    kInvalidDescriptor,
    kDescriptorConflict,
    kNotReady,
    kPending,
    kInvalidated,
    kInvalidLease,
    kInvalidOperation,
    kInvalidCompletion,
    kInUse,
    kStopped,
    kResourceExhausted,
    kSequenceExhausted,
    kNoMemory,
    kProviderError,
    kExecutorError,
};
struct TsegImportCacheError {
    TsegImportCacheErrorCode code;
    int provider_error = 0;
};
struct TsegImportCacheConfig {
    std::uint32_t entries = 64;
    std::uint32_t leases = 4096;
    std::uint32_t in_flight = 16;
};

/// 单 owner 的有界 Single-Flight 缓存，Open 一次分配；其余方法零分配、不调用阻塞 provider。
/// Acquire/Retain 引用既是等待者也是 DMA 依赖 pin；Release 前调用方须停止未来提交并证明所有依赖 WR 已终结。
/// 未知接受、取消、断链或本地队列故障不解除 pin；Invalidate 不撤销远端 Grant，也不产生终结证据。
/// Get 成功仅代表段导入可用；宿主仍须认证目标 Jetty/Grant/Slab 绑定并校验每次 WRITE 在授权范围内。
/// 已保存的裸指针不能绕过失效门禁；每次新提交前须检查 Get，已接受 WR 仍由原 pin 保留至终结。
/// Ready 且零引用的条目作为暖缓存保留；Invalidate/EvictUnused 后才异步 unimport。
/// 缓存不 Poll 借用的 ControlExecutor；宿主把属于本缓存的完成交给 HandleCompletion。
/// context 暂停域须同时覆盖 endpoint/segment 导入与全部数据访问；本缓存的互斥不能替代跨缓存屏障。
/// 驱逐后不保留旧 Slab 高水位；宿主须先拒绝过期 session/Slab 描述，不能把缓存 miss 当作新授权。
/// context、函数表、executor 与缓存须活到所有任务和引用终结；Close 不猜测这些证据。
/// provider 返回错误 context 的非空句柄时永久保留：无法在原 context 暂停域安全 unimport，Close 不会成功。
/// 析构遇到未终结引用/任务/句柄会保留内部存储，调用方仍须保留上述外部依赖。
class TsegImportCache {
public:
    static constexpr std::uint32_t kMaxEntries = 4096;
    static constexpr std::uint32_t kMaxLeases = 65536;
    TsegImportCache() noexcept;
    ~TsegImportCache();
    TsegImportCache(const TsegImportCache&) = delete;
    TsegImportCache& operator=(const TsegImportCache&) = delete;

    std::expected<void, TsegImportCacheError> Open(const TsegImportCacheConfig& config = {}) noexcept;
    std::expected<TsegImportLease, TsegImportCacheError> Acquire(const TsegImportRequest& request) noexcept;
    std::expected<TsegImportLease, TsegImportCacheError> Retain(TsegImportLease lease) noexcept;
    std::expected<TsegImportKey, TsegImportCacheError> GetKey(TsegImportLease lease) const noexcept;
    std::expected<urma_target_seg_t*, TsegImportCacheError> Get(TsegImportLease lease) const noexcept;
    std::expected<void, TsegImportCacheError> Release(TsegImportLease lease) noexcept;
    std::expected<void, TsegImportCacheError> Invalidate(TsegImportLease lease) noexcept;
    std::expected<void, TsegImportCacheError> Invalidate(const TsegImportKey& key) noexcept;
    /// 只使当前零引用暖条目失效；不回收已接受任务，也不使活跃借用失效。
    std::size_t EvictUnused() noexcept;

    /// 返回下一项可调度操作，不代表已经提交；同一 context 在本缓存内至多一个在途任务。
    /// context 非空时只选择该实例的操作，供每 context 共享暂停屏障的调度源使用。
    std::expected<std::optional<TsegImportOperation>, TsegImportCacheError>
    NextOperation(urma_context_t* context = nullptr, std::uint64_t incarnation = 0) noexcept;
    /// 只准备 owner 数据，不调用 provider；保留容量与参数直到 Commit 后收割，或 Discard。
    std::expected<TsegImportPreparedOperation, TsegImportCacheError>
    PrepareOperation(const TsegImportOperation& operation) noexcept;
    /// 调用方须在提交 task 前暂停该 context 的全部使用者，屏障保持到 HandleCompletion。
    /// worker 可已开始，Commit 仅访问 owner 元数据。已提交任务必须 Commit，绝不能 Discard。
    std::expected<void, TsegImportCacheError> CommitOperation(const TsegImportPreparedOperation& prepared,
                                                              runtime::ControlTaskId id) noexcept;
    /// 仅用于确认 task 从未被执行器接受；恢复待调度状态，不销毁已接受操作的依赖。
    std::expected<void, TsegImportCacheError> DiscardOperation(const TsegImportPreparedOperation& prepared) noexcept;
    std::expected<runtime::ControlTaskId, TsegImportCacheError>
    StartOperation(const TsegImportOperation& operation, runtime::ControlExecutor& executor,
                   const TsegImportContextPause& pause) noexcept;
    bool OwnsCompletion(runtime::ControlTaskId id) const noexcept;
    std::expected<void, TsegImportCacheError>
    HandleCompletion(const runtime::ControlTaskCompletion& completion) noexcept;

    void StopAccepting() noexcept;
    /// 停止新引用并使全部条目失效；未结束时返回 kInUse，继续 Release/调度清理/收割后重试。
    std::expected<void, TsegImportCacheError> Close() noexcept;
    std::size_t entries() const noexcept;
    std::size_t leases() const noexcept;
    std::size_t in_flight() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::uint64_t next_entry_ = 0;
    std::uint64_t next_lease_ = 0;
    std::uint64_t next_reservation_ = 0;
    bool opened_once_ = false;
};

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_TSEG_IMPORT_CACHE_HPP_
