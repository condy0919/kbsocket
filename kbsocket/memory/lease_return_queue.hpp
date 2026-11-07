// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_MEMORY_LEASE_RETURN_QUEUE_HPP_
#define KBSOCKET_MEMORY_LEASE_RETURN_QUEUE_HPP_

#include <cstddef>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/memory/buffer_lease.hpp"

namespace kbsocket {
namespace memory {

/// 多生产者、单 owner 消费的有界 Lease 归还队列；Open 预分配，TryPush/Poll 不分配或加锁。
/// 每份 CPU 所有权只能合法提交一次，复制 BufferLease 不增加所有权；队列不解引用 allocator。
/// 成功 TryPush 才转移 CPU 所有权，满额/停止/参数错误时调用方继续持有并可重试。
/// producer 在保留槽位后被抢占时，Poll 不等待其发布；之后该 producer 发布并唤醒 owner。
/// Open/Close 不可与生产者并发；销毁前必须停止生产者、收割归还，再 Close(true)。
/// 未确认 quiescence 就析构会保留整个状态/FD，宿主仍须保留 Lease 指向的 allocator 和 DMA 依赖。
class LeaseReturnQueue {
public:
    static constexpr std::size_t kMaxCapacity = 1u << 20;

    LeaseReturnQueue() noexcept;
    ~LeaseReturnQueue();
    LeaseReturnQueue(const LeaseReturnQueue&) = delete;
    LeaseReturnQueue& operator=(const LeaseReturnQueue&) = delete;

    /// 容量向上取至少 2 的二次幂，必须覆盖宿主可同时借出的全部单 CPU Lease 数量。
    std::expected<void, MemoryError> Open(std::size_t max_outstanding_leases) noexcept;
    std::expected<void, MemoryError> TryPush(BufferLease lease) noexcept;

    /// 仅 owner 调用；先消费通知再读取已发布的连续前缀，预算不足时保留 readiness。
    /// 输出描述符重新交给 owner；由 owner 调用 SlabAllocator::ReleaseCpu 校验身份并解除 CPU 责任。
    std::expected<std::size_t, MemoryError> Poll(std::span<BufferLease> leases) noexcept;
    /// 可与 producer 并发，但已经越过准入检查的 TryPush 仍可能成功，必须等 producer 退出后关闭。
    void StopAccepting() noexcept;
    /// 不凭队列暂空证明 producer 已停止。已保留但未发布的槽位同样阻止关闭。
    std::expected<void, MemoryError> Close(bool producers_stopped = false) noexcept;

    /// 借用 nonblocking eventfd；只供 readiness 观察，不允许外部 read/write/close。
    int completion_fd() const noexcept;
    std::size_t capacity() const noexcept;
    /// 并发近似快照，包含尚未发布的预留，不作为 quiescence 证明。
    std::size_t pending() const noexcept;
    bool accepting() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
};

} // namespace memory
} // namespace kbsocket
#endif // KBSOCKET_MEMORY_LEASE_RETURN_QUEUE_HPP_
