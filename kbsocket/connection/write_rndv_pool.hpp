// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_WRITE_RNDV_POOL_HPP_
#define KBSOCKET_CONNECTION_WRITE_RNDV_POOL_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/connection/write_rndv_engine.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"

namespace kbsocket {
namespace connection {
class WriteRndvPool;
struct WriteRndvHandle {
    const WriteRndvPool* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const WriteRndvHandle&) const = default;
};
struct WriteRndvPoolConfig {
    std::uint64_t connection_id = 0;
    GrantBinding binding;
    GrantArena arena;
    control::EndpointImportKey endpoint;
    control::TsegImportKey segment;
    raw::JettyLane lane;
    std::uint32_t slots = 4;
    std::uint32_t window = 128;
    std::uint32_t initial_seq = 0;
    std::uint32_t max_chunks = 64;
    std::uint32_t post_budget = 32;
    std::uint64_t chunk_bytes = raw::TxSender::kMaxWriteSize;
};
enum class WriteRndvPoolErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kNoMemory,
    kWouldBlock,
    kWrongIdentity,
    kDuplicate,
    kStaleSequence,
    kInvalidHandle,
    kInvalidCompletion,
    kSequenceExhausted,
    kEngine,
    kEndpointCache,
    kSegmentCache,
    kAcceptanceUnknown,
    kRetained
};
struct WriteRndvPoolError {
    WriteRndvPoolErrorCode code;
    std::optional<WriteRndvError> engine_error{};
    std::optional<control::ImportCacheError> endpoint_error{};
    std::optional<control::TsegImportCacheError> segment_error{};
};
struct WriteRndvPoolStats {
    std::uint64_t window_begin = 0;
    std::size_t active = 0;
    std::size_t reserved = 0;
    std::size_t started = 0;
    std::size_t retired = 0;
};

/// 单 owner、固定连接/Arena/Lane 的有界事务池；Open 分配全部引擎与 SN 窗口。
/// Reserve 保留独立的两个缓存引用；调用者原票据仍归调用者，不转移所有权。
/// 源 CPU Lease 至 Start 成功才允许归还。各数据路径不分配，不直接调用慢 provider。
/// connection_id 的命名空间由宿主分配，旧完成可能存在时不得跨池对象复用。
/// 宿主认证 Grant 并独占其写入权；本池防重放不代替跨池授权与连接统一 SN 准入。
/// 缓存/发送器/Slab/context/provider 活到 Close 成功，宿主遵守整个 context 的暂停屏障。
/// 未知接受永久占住本池的槽与缓存 pin；本地清理不构成远端 Grant 终结证据。
class WriteRndvPool {
public:
    static constexpr std::uint32_t kMaxSlots = 128;
    static constexpr std::uint32_t kMaxTotalChunks = 4096;
    WriteRndvPool() noexcept;
    ~WriteRndvPool();
    WriteRndvPool(const WriteRndvPool&) = delete;
    WriteRndvPool& operator=(const WriteRndvPool&) = delete;

    std::expected<void, WriteRndvPoolError> Open(const WriteRndvPoolConfig& config, raw::TxSender& sender,
                                                 memory::SlabAllocator& slab, control::EndpointImportCache& endpoints,
                                                 control::TsegImportCache& segments) noexcept;
    /// 仅接受已 Ready 且完整键匹配的缓存票据；Pending 返回 WouldBlock，不占 SN。
    /// 成功后同 SN 与远端 Grant 代次不能重新准入；Start 失败保留句柄，可重试或 Cancel。
    std::expected<WriteRndvHandle, WriteRndvPoolError> Reserve(const GrantTransfer& expected_transfer,
                                                               const GrantIdentity& grant,
                                                               control::ImportLease endpoint,
                                                               control::TsegImportLease segment) noexcept;
    std::expected<void, WriteRndvPoolError> Start(WriteRndvHandle handle,
                                                  std::span<const WriteRndvSource> sources) noexcept;
    /// 每次至多一次有界 post；每次新提交前重新检查缓存失效，绝不重放已接受前缀。
    std::expected<std::size_t, WriteRndvPoolError> Progress(WriteRndvHandle handle) noexcept;
    std::expected<void, WriteRndvPoolError> OnCompletion(const raw::TxCompletionEvent& event) noexcept;
    std::expected<void, WriteRndvPoolError> Cancel(WriteRndvHandle handle) noexcept;
    std::expected<GrantTerminalEvidence, WriteRndvPoolError> DoneEvidence(WriteRndvHandle handle) const noexcept;
    std::expected<GrantTerminalEvidence, WriteRndvPoolError> TerminalEvidence(WriteRndvHandle handle) const noexcept;
    /// 仅在已知接受且全部责任结束后归还本地缓存 pin 与物理槽；SN tombstone 保留。
    /// 调用前宿主须保存所需证据；不发送通知，也不终结远端 Grant。失败可重试同一 handle。
    std::expected<void, WriteRndvPoolError> Release(WriteRndvHandle handle) noexcept;
    /// 可信宿主确认其前全部逻辑 SN 已消费；遇活跃责任原子拒绝，不按 CQE 到达顺序推进。
    std::expected<void, WriteRndvPoolError> AdvanceWindow(std::uint64_t consumed_prefix) noexcept;
    /// 仅停止新 Reserve；已准入事务可继续 Start/Progress 或取消。
    void Stop() noexcept;
    /// 控制路径停止准入并取消所有事务；失败析构保留整个状态与所有依赖引用。
    std::expected<void, WriteRndvPoolError> Close() noexcept;
    std::expected<WriteRndvStats, WriteRndvPoolError> stats(WriteRndvHandle handle) const noexcept;
    WriteRndvPoolStats stats() const noexcept;

private:
    struct Slot;
    struct State;
    std::expected<Slot*, WriteRndvPoolError> Find(WriteRndvHandle handle) const noexcept;
    std::expected<WriteRndvDestination, WriteRndvPoolError>
    Destination(const GrantIdentity& grant, control::ImportLease endpoint,
                control::TsegImportLease segment) const noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_WRITE_RNDV_POOL_HPP_
