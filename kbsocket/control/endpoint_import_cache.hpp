// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_ENDPOINT_IMPORT_CACHE_HPP_
#define KBSOCKET_CONTROL_ENDPOINT_IMPORT_CACHE_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/runtime/control_executor.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace control {

inline constexpr std::size_t kMaxEndpointDescriptorBytes = 4096;

/// 本地 context 地址不能单独代表实例；远端重启或被动端点重建必须更新完整代次。
struct EndpointImportKey {
    urma_context_t* local_context = nullptr;
    std::uint64_t local_context_incarnation = 0;
    std::uint64_t peer_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t endpoint_generation = 0;
    std::array<std::uint8_t, URMA_EID_SIZE> eid{};
    std::uint32_t jetty_id = 0;
    std::uint64_t authorization_identity = 0;
    urma_transport_mode_t transport_mode = URMA_TM_RM;
    urma_tp_type_t tp_type = URMA_CTP;
    bool operator==(const EndpointImportKey&) const = default;
};

struct EndpointImportRequest {
    EndpointImportKey key;
    /// 可信同 ABI 的 GetRjetty 导出；不接受任意网络字节直接调用 provider。
    /// 可含 has_user_info 的长度前缀扩展，拒绝 has_drv_ext 与 share_tp。
    std::span<const std::byte> descriptor;
    urma_token_t token{};
};

class EndpointImportCache;
/// 每份引用有独立票据；复制结构不创建新引用，重复 Release 被拒绝。
struct ImportLease {
    const EndpointImportCache* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t sequence = 0;
    bool operator==(const ImportLease&) const = default;
};

enum class ImportOperationKind : std::uint8_t { kImport, kUnimport };
struct ImportOperation {
    const EndpointImportCache* owner = nullptr;
    std::uint32_t entry = 0;
    std::uint64_t generation = 0;
    ImportOperationKind kind = ImportOperationKind::kImport;
    urma_context_t* context = nullptr;
    std::uint64_t context_incarnation = 0;
    bool operator==(const ImportOperation&) const = default;
};

/// Prepare 后参数被独占保留，排队失败可重试同一 task；reservation 防止旧撤销误伤重新准备的操作。
struct ImportPreparedOperation {
    ImportOperation operation;
    std::uint64_t reservation = 0;
    runtime::ControlTask task;
};

/// 宿主先暂停该 context 全部 post/poll/事件访问，并排除其他慢控制操作；
/// 屏障保持至 HandleCompletion 收割，不能仅凭任务已完成或 fd 可读恢复。
struct ImportContextPause {
    urma_context_t* context = nullptr;
    std::uint64_t incarnation = 0;
    bool users_paused = false;
    bool exclusive_control = false;
};

enum class ImportCacheErrorCode : std::uint8_t {
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
struct ImportCacheError {
    ImportCacheErrorCode code;
    int provider_error = 0;
};
struct EndpointImportCacheConfig {
    std::uint32_t entries = 64;
    std::uint32_t leases = 4096;
    std::uint32_t in_flight = 16;
};

/// 单 owner 的有界 Single-Flight 缓存，Open 一次分配；其余方法零分配、不调用阻塞 provider。
/// Acquire/Retain 引用既是等待者也是 DMA 依赖 pin；Release 前调用方须证明对应任务/WR 已终结。
/// Ready 且零引用的条目作为暖缓存保留；Invalidate/EvictUnused 后才异步 unimport。
/// 缓存不 Poll 借用的 ControlExecutor；宿主把属于本缓存的完成交给 HandleCompletion。
/// context、函数表、executor 与缓存须活到所有任务和引用终结；Close 不猜测这些证据。
/// 析构遇到未终结引用/任务/句柄会保留内部存储，调用方仍须保留上述外部依赖。
class EndpointImportCache {
public:
    static constexpr std::uint32_t kMaxEntries = 4096;
    static constexpr std::uint32_t kMaxLeases = 65536;
    EndpointImportCache() noexcept;
    ~EndpointImportCache();
    EndpointImportCache(const EndpointImportCache&) = delete;
    EndpointImportCache& operator=(const EndpointImportCache&) = delete;

    std::expected<void, ImportCacheError> Open(const EndpointImportCacheConfig& config = {}) noexcept;
    std::expected<ImportLease, ImportCacheError> Acquire(const EndpointImportRequest& request) noexcept;
    std::expected<ImportLease, ImportCacheError> Retain(ImportLease lease) noexcept;
    std::expected<EndpointImportKey, ImportCacheError> GetKey(ImportLease lease) const noexcept;
    std::expected<urma_target_jetty_t*, ImportCacheError> Get(ImportLease lease) const noexcept;
    std::expected<void, ImportCacheError> Release(ImportLease lease) noexcept;
    std::expected<void, ImportCacheError> Invalidate(ImportLease lease) noexcept;
    std::expected<void, ImportCacheError> Invalidate(const EndpointImportKey& key) noexcept;
    /// 只使当前零引用暖条目失效；不回收已接受任务，也不使活跃借用失效。
    std::size_t EvictUnused() noexcept;

    /// 返回下一项可调度操作，不代表已经提交；同一 context 在本缓存内至多一个在途任务。
    /// context 非空时只选择该实例的操作，供每 context 共享暂停屏障的调度源使用。
    std::expected<std::optional<ImportOperation>, ImportCacheError>
    NextOperation(urma_context_t* context = nullptr, std::uint64_t incarnation = 0) noexcept;
    /// 只准备 owner 数据，不调用 provider；保留容量与参数直到 Commit 后收割，或 Discard。
    std::expected<ImportPreparedOperation, ImportCacheError>
    PrepareOperation(const ImportOperation& operation) noexcept;
    /// 调用方须在提交 task 前暂停该 context 的全部使用者，屏障保持到 HandleCompletion。
    /// worker 可已开始，Commit 仅访问 owner 元数据。已提交任务必须 Commit，绝不能 Discard。
    std::expected<void, ImportCacheError> CommitOperation(const ImportPreparedOperation& prepared,
                                                          runtime::ControlTaskId id) noexcept;
    /// 仅用于确认 task 从未被执行器接受；恢复待调度状态，不销毁已接受操作的依赖。
    std::expected<void, ImportCacheError> DiscardOperation(const ImportPreparedOperation& prepared) noexcept;
    std::expected<runtime::ControlTaskId, ImportCacheError> StartOperation(const ImportOperation& operation,
                                                                           runtime::ControlExecutor& executor,
                                                                           const ImportContextPause& pause) noexcept;
    bool OwnsCompletion(runtime::ControlTaskId id) const noexcept;
    std::expected<void, ImportCacheError> HandleCompletion(const runtime::ControlTaskCompletion& completion) noexcept;

    void StopAccepting() noexcept;
    /// 停止新引用并使全部条目失效；未结束时返回 kInUse，继续 Release/调度清理/收割后重试。
    std::expected<void, ImportCacheError> Close() noexcept;
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
#endif // KBSOCKET_CONTROL_ENDPOINT_IMPORT_CACHE_HPP_
