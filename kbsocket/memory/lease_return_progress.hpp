// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_MEMORY_LEASE_RETURN_PROGRESS_HPP_
#define KBSOCKET_MEMORY_LEASE_RETURN_PROGRESS_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/memory/lease_return_queue.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace memory {
struct LeaseReturnStats {
    std::size_t released = 0;
    std::size_t rejected = 0;
};
using LeaseReturnCallback = void (*)(void*, BufferLease, std::optional<MemoryError>) noexcept;

/// 把归还队列接入 M3 的 mailbox 预算；不创建线程，不持有 Slab/queue 所有权。
/// owner 独占队列消费，回调不可重入；生产者只使用 queue.TryPush，成功后不再访问 Lease。
/// 回调不得关闭或销毁本源、queue、Slab；宿主保证借用对象活到源从 reactor 解绑并关闭。
/// 先停止生产者、推进剩余归还并解绑 reactor，再 Close 本源/queue，最后关闭 Slab。
class LeaseReturnProgress final : public runtime::ProgressSource {
public:
    static constexpr std::size_t kMaxSlabs = 16;
    LeaseReturnProgress() noexcept = default;
    LeaseReturnProgress(const LeaseReturnProgress&) = delete;
    LeaseReturnProgress& operator=(const LeaseReturnProgress&) = delete;
    std::expected<void, MemoryError> Open(LeaseReturnQueue& queue, std::span<SlabAllocator* const> slabs,
                                          LeaseReturnCallback callback, void* cookie = nullptr) noexcept;
    std::expected<void, MemoryError> Close(bool producers_stopped = false) noexcept;
    std::expected<runtime::ProgressBatch, runtime::ProgressError>
    Progress(runtime::ProgressKind kind, std::size_t budget, std::uint64_t now_ns) noexcept override;
    std::expected<runtime::ProgressBatch, runtime::ProgressError> CollectNotifications(std::size_t) noexcept override {
        return runtime::ProgressBatch{};
    }
    std::expected<void, runtime::ProgressError> Rearm() noexcept override {
        return {};
    }
    std::span<const int> wait_fds() const noexcept override {
        return queue_ ? std::span(fd_) : std::span<const int>{};
    }
    const LeaseReturnStats& stats() const noexcept {
        return stats_;
    }

private:
    LeaseReturnQueue* queue_ = nullptr;
    std::array<SlabAllocator*, kMaxSlabs> slabs_{};
    std::size_t count_ = 0;
    LeaseReturnCallback callback_ = nullptr;
    void* cookie_ = nullptr;
    std::array<int, 1> fd_{};
    std::array<BufferLease, 64> batch_{};
    LeaseReturnStats stats_{};
};
} // namespace memory
} // namespace kbsocket
#endif // KBSOCKET_MEMORY_LEASE_RETURN_PROGRESS_HPP_
