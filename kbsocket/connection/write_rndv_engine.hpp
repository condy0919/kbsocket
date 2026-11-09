// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_WRITE_RNDV_ENGINE_HPP_
#define KBSOCKET_CONNECTION_WRITE_RNDV_ENGINE_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace connection {
enum class WriteRndvErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kNoMemory,
    kWouldBlock,
    kWrongIdentity,
    kMemory,
    kPost,
    kAcceptanceUnknown,
    kInvalidCompletion,
    kCompletion,
    kRetained
};
struct WriteRndvError {
    WriteRndvErrorCode code;
    int provider_status = 0;
};
struct WriteRndvSource {
    memory::BufferLease lease;
    std::size_t offset = 0;
    std::size_t bytes = 0;
};
struct WriteRndvConfig {
    std::uint64_t connection_id = 0;
    std::uint64_t transaction_generation = 0;
    raw::JettyLane lane;
    std::uint64_t chunk_bytes = raw::TxSender::kMaxWriteSize;
    // Open 按此上限预分配固定分块/SGE 元数据；容量由宿主预算，不随消息临时扩容。
    std::uint32_t max_chunks = 256;
    std::uint32_t post_budget = 32;
};
struct WriteRndvDestination {
    GrantIdentity grant;
    urma_target_jetty_t* jetty = nullptr;
    urma_target_seg_t* segment = nullptr;
};
struct WriteRndvStats {
    std::size_t planned = 0;
    std::size_t queued = 0;
    std::size_t in_flight = 0;
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t successful = 0;
    std::uint64_t successful_bytes = 0;
    std::uint64_t post_calls = 0;
    bool cancelled = false;
    bool failed = false;
    bool acceptance_known = true;
};

/// 单 owner WRITE 引擎：独立使用时一次成功 Start 后不能重绑同一对象，也不发送 DONE 控制帧。
/// 只有具备 SN/Grant 防重放账本的 WriteRndvPool 可在全部责任终结后复用内部预分配存储。
/// Open 在控制路径预分配；Start 及其余数据路径无分配，持有每个源 SGE 的独立 DMA 责任。
/// 源 CPU 所有权仍归应用，可在 Start 成功后归还，但所有旧 CPU 视图必须停止使用至相关 DMA 全部结束。
/// 宿主认证 transfer/Grant 与远端 Arena、Jetty、段的绑定，并保证 connection_id/transaction_generation 不复用。
/// 宿主须保证整个 Grant 仅由本引擎拥有，其他引擎或排队任务不得另行 WRITE；对象单次启动不能替代此约束。
/// sender/Slab/导入 Jetty/段/context/provider 由宿主持有至 Close 成功，不能提前 unimport 或重建。
/// 接受未知时保留可能暴露的责任，不重放，也不把空 CQ、flush 边界或 Close 当作终结证据。
class WriteRndvEngine {
public:
    static constexpr std::size_t kMaxSources = 16;
    static constexpr std::uint32_t kMaxChunks = 4096;
    WriteRndvEngine() noexcept;
    ~WriteRndvEngine();
    WriteRndvEngine(const WriteRndvEngine&) = delete;
    WriteRndvEngine& operator=(const WriteRndvEngine&) = delete;
    /// 仅接受开启 defer_drain 的发送器；成功 Open 后对象不可重绑。
    std::expected<void, WriteRndvError> Open(const WriteRndvConfig& config, raw::TxSender& sender,
                                             memory::SlabAllocator& slab) noexcept;
    /// expected_transfer 是已有的可信本地请求；源拼接后恰好等于授权长度，不重建另一份请求身份。
    /// 失败且回滚完成可重试；内部回滚失败则保留 State，只能继续 Close 清理。
    std::expected<void, WriteRndvError> Start(const GrantTransfer& expected_transfer,
                                              const WriteRndvDestination& destination,
                                              std::span<const WriteRndvSource> sources) noexcept;
    /// 每次最多一次有界 post；部分接受只推进可信前缀，保留原偏移的未接受后缀。
    std::expected<std::size_t, WriteRndvError> Progress() noexcept;
    /// 仅消费可信 TxCompletionProcessor 的逐 WR 退休事件；重复/错身份事件不释放源引用。
    std::expected<void, WriteRndvError> OnCompletion(const raw::TxCompletionEvent& event) noexcept;
    /// 停止所有未来提交，仅撤销确定未提交引用；已接受/未知接受继续等待真实逐 WR 终结。
    std::expected<void, WriteRndvError> Cancel() noexcept;
    /// 成功证据只在完整计划全部成功后生成；取消后即使在途 WR 全成功也不生成 DONE。
    /// 返回的是本地快照，宿主负责认证控制通知；后续 Cancel 不能撤回已交出的或已发布的证明。
    std::expected<GrantTerminalEvidence, WriteRndvError> DoneEvidence() const noexcept;
    /// 仅用于远端安全丢弃；接受集合已知、未来提交停止且全部已接受 WR 终结才返回。
    std::expected<GrantTerminalEvidence, WriteRndvError> TerminalEvidence() const noexcept;
    /// 可取消未提交项，不终结远端 Grant；未解决 DMA 保留，失败析构保留内部状态与依赖引用。
    std::expected<void, WriteRndvError> Close() noexcept;
    WriteRndvStats stats() const noexcept;

private:
    friend class WriteRndvPool;
    struct Chunk;
    struct State;
    // 仅供有 SN/Grant 防重放账本的事务池使用，保留预分配存储。
    std::expected<void, WriteRndvError> Recycle(std::uint64_t generation) noexcept;
    std::expected<void, WriteRndvError> CanRecycle() const noexcept;
    std::expected<void, WriteRndvError> RetryCompleted() noexcept;
    std::expected<void, WriteRndvError> CancelQueued() noexcept;
    std::expected<void, WriteRndvError> FinishChunk(Chunk& chunk) noexcept;
    std::expected<void, WriteRndvError> CommitChunk(Chunk& chunk) noexcept;
    void Quarantine(Chunk& chunk) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_WRITE_RNDV_ENGINE_HPP_
