// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_MEMORY_SLAB_ALLOCATOR_HPP_
#define KBSOCKET_MEMORY_SLAB_ALLOCATOR_HPP_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace memory {
/// 可由多个控制任务共享的 backing 硬预算；同一 Slab 的多 context 注册只扣一次。
/// 必须活到全部 Slab 完成注销/释放；不包含 provider 内部 MR 元数据或应用的其他内存。
class MemoryBudget {
public:
    explicit MemoryBudget(std::size_t limit_bytes) noexcept : limit_bytes_(limit_bytes) {}
    std::size_t limit_bytes() const noexcept {
        return limit_bytes_;
    }
    std::size_t reserved_bytes() const noexcept {
        return reserved_bytes_.load(std::memory_order_acquire);
    }

private:
    friend class SlabAllocator;
    bool Reserve(std::size_t bytes) noexcept;
    void Release(std::size_t bytes) noexcept;
    const std::size_t limit_bytes_;
    alignas(64) std::atomic<std::size_t> reserved_bytes_{0};
};

struct SlabConfig {
    std::uint32_t buffer_count = 128;
    std::size_t buffer_size = 4096;
    std::uint32_t max_references = 512;
    // 不使用 LOCAL_ONLY；宿主为整个 Slab 显式选择注册权限。
    std::uint32_t access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
};
struct SlabStats {
    std::size_t backing_bytes = 0;
    std::size_t buffers = 0;
    std::size_t available = 0;
    std::size_t cpu_leases = 0;
    std::size_t local_dma = 0;
    std::size_t remote_grants = 0;
    std::size_t quarantined_buffers = 0;
    std::size_t quarantined_bytes = 0;
};

/// 一个固定容量、页对齐 Slab，显式 CPU 池或最多四 context 注册；所有 Lease/责任操作由同一 owner 串行推进。
/// 数据路径不分配、不注册、不持锁。Open/Close 是可能阻塞的控制路径，不与任何访问并发。
/// CPU、DMA、Grant 是独立责任；全部结束才回收，复制 token 不增引用。
/// Reserve、Quarantine 或 CPU 所有权转移后必须停止使用之前取得的 CPU span；
/// 重新访问须再次取得视图，接口无法撤销外部保存的旧 span。SGE 不能越过相应责任的终结点。
/// 析构失败保留完整状态；context/budget/owner 依赖也须由宿主保留。
class SlabAllocator {
public:
    static constexpr std::size_t kMaxContexts = 4;
    static constexpr std::uint32_t kMaxBuffers = 1u << 20;
    static constexpr std::uint32_t kMaxReferences = 1u << 20;
    SlabAllocator() noexcept;
    ~SlabAllocator();
    SlabAllocator(const SlabAllocator&) = delete;
    SlabAllocator& operator=(const SlabAllocator&) = delete;
    std::expected<void, MemoryError> Open(std::span<urma_context_t* const> contexts, const SlabConfig& config,
                                          MemoryBudget& budget) noexcept;
    /// 显式 CPU 池：沿用固定 backing/预算/Lease 规则，但不注册 MR，也不允许任何 DMA/Grant 责任。
    /// config 的容量、权限位仍按 SlabConfig 校验；权限位不会赋予 Host 内存远端访问能力。
    std::expected<void, MemoryError> OpenHost(const SlabConfig& config, MemoryBudget& budget) noexcept;
    /// 先停止新准入；所有 CPU/DMA/Grant 清零后才逆序注销。失败保留剩余阶段与预算，可重试。
    std::expected<void, MemoryError> Close() noexcept;
    void StopAccepting() noexcept;
    std::expected<BufferLease, MemoryError> Acquire(std::size_t bytes) noexcept;
    /// 仅复制一个可容纳的片段；成功返回前内容已脱离调用方内存。大 write 的部分接受协议待 M5。
    std::expected<BufferLease, MemoryError> Copy(std::span<const std::byte> bytes) noexcept;
    std::expected<std::span<std::byte>, MemoryError> Writable(BufferLease lease) noexcept;
    std::expected<std::span<const std::byte>, MemoryError> Readable(BufferLease lease) const noexcept;
    std::expected<void, MemoryError> ReleaseCpu(BufferLease lease) noexcept;

    /// Reserve 在 post/Grant 发布之前取得责任；Commit 表示可能已暴露，包括接受未知情况。
    /// 只有明确未暴露的 Reserved 责任可 Cancel；不能用取消替代 DMA/远端授权终结证明。
    std::expected<BufferReference, MemoryError> Reserve(BufferLease lease, BufferUse use) noexcept;
    std::expected<void, MemoryError> Commit(BufferReference reference) noexcept;
    std::expected<void, MemoryError> Cancel(BufferReference reference) noexcept;
    /// 从 Reserved 责任构造 SGE，不能用其他 context 的注册句柄。offset/bytes 受 Lease 实际长度限制。
    std::expected<urma_sge_t, MemoryError> Sge(BufferReference reference, urma_context_t* context, std::size_t offset,
                                               std::size_t bytes) const noexcept;
    /// 仅在可信本地逐 WR 终结后调用；不会解除任何远端 Grant。
    std::expected<void, MemoryError> CompleteDma(BufferReference reference) noexcept;
    /// 调用方已禁止该 Grant 后续提交且确认其全部远端访问结束；超时/断链/本地 flush 不是证明。
    std::expected<void, MemoryError> EndGrant(BufferReference reference) noexcept;
    /// 每槽预留隔离记录，覆盖全部可能借出 Lease；隔离不丢责任，不产生虚假完成。
    std::expected<void, MemoryError> Quarantine(BufferLease lease) noexcept;
    SlabStats stats() const noexcept;
    bool accepting() const noexcept;

private:
    struct State;
    std::expected<void, MemoryError> OpenImpl(std::span<urma_context_t* const> contexts, const SlabConfig& config,
                                              MemoryBudget& budget, bool host) noexcept;
    std::expected<std::uint32_t, MemoryError> Find(BufferLease lease) const noexcept;
    std::expected<std::uint32_t, MemoryError> Find(BufferReference reference) const noexcept;
    std::expected<void, MemoryError> Finish(BufferReference reference, BufferUse use, bool committed) noexcept;
    void Recycle(std::uint32_t index) noexcept;
    std::unique_ptr<State> state_;
    std::uint64_t next_lease_ = 0;
    std::uint64_t next_reference_ = 0;
};
} // namespace memory
} // namespace kbsocket
#endif // KBSOCKET_MEMORY_SLAB_ALLOCATOR_HPP_
