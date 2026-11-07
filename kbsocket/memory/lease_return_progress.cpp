// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/memory/lease_return_progress.hpp"

#include <algorithm>
#include <limits>

namespace kbsocket {
namespace memory {
std::expected<void, MemoryError> LeaseReturnProgress::Open(LeaseReturnQueue& queue,
                                                           std::span<SlabAllocator* const> slabs,
                                                           LeaseReturnCallback callback, void* cookie) noexcept {
    if (queue_) {
        return std::unexpected(MemoryError{MemoryErrorCode::kInUse});
    }
    if (slabs.empty() || slabs.size() > kMaxSlabs || !callback || queue.completion_fd() < 0 || !queue.accepting() ||
        queue.pending()) {
        return std::unexpected(MemoryError{MemoryErrorCode::kInvalidArgument});
    }
    std::size_t total = 0;
    for (std::size_t i = 0; i < slabs.size(); ++i) {
        if (!slabs[i] || !slabs[i]->accepting()) {
            return std::unexpected(MemoryError{MemoryErrorCode::kInvalidArgument});
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (slabs[i] == slabs[j]) {
                return std::unexpected(MemoryError{MemoryErrorCode::kInvalidArgument});
            }
        }
        const auto buffers = slabs[i]->stats().buffers;
        if (buffers > std::numeric_limits<std::size_t>::max() - total) {
            return std::unexpected(MemoryError{MemoryErrorCode::kInvalidArgument});
        }
        total += buffers;
    }
    if (total > queue.capacity()) {
        return std::unexpected(MemoryError{MemoryErrorCode::kInvalidArgument});
    }
    std::copy(slabs.begin(), slabs.end(), slabs_.begin());
    queue_ = &queue;
    count_ = slabs.size();
    callback_ = callback;
    cookie_ = cookie;
    fd_[0] = queue.completion_fd();
    stats_ = {};
    return {};
}
std::expected<void, MemoryError> LeaseReturnProgress::Close(bool producers_stopped) noexcept {
    if (!queue_) {
        return {};
    }
    if (!producers_stopped) {
        return std::unexpected(MemoryError{MemoryErrorCode::kNotQuiescent});
    }
    queue_->StopAccepting();
    if (queue_->pending()) {
        return std::unexpected(MemoryError{MemoryErrorCode::kInUse});
    }
    queue_ = nullptr;
    count_ = 0;
    callback_ = nullptr;
    cookie_ = nullptr;
    slabs_ = {};
    fd_[0] = -1;
    return {};
}
std::expected<runtime::ProgressBatch, runtime::ProgressError>
LeaseReturnProgress::Progress(runtime::ProgressKind kind, std::size_t budget, std::uint64_t) noexcept {
    if (!queue_) {
        return std::unexpected(runtime::ProgressError{runtime::ProgressErrorCode::kNotReady});
    }
    if (!budget) {
        return std::unexpected(runtime::ProgressError{runtime::ProgressErrorCode::kInvalidArgument});
    }
    if (kind != runtime::ProgressKind::kMailbox) {
        return runtime::ProgressBatch{};
    }
    const auto limit = std::min(budget, batch_.size());
    const auto count = queue_->Poll(std::span(batch_).first(limit));
    if (!count) {
        return std::unexpected(
            runtime::ProgressError{runtime::ProgressErrorCode::kSystem, count.error().provider_error});
    }
    for (std::size_t i = 0; i < *count; ++i) {
        const auto lease = batch_[i];
        std::optional<MemoryError> error = MemoryError{MemoryErrorCode::kInvalidLease};
        // 只比较已登记地址，绝不解引用来源不明的 allocator 指针。
        for (std::size_t j = 0; j < count_; ++j) {
            if (lease.allocator == slabs_[j]) {
                const auto released = slabs_[j]->ReleaseCpu(lease);
                error = released ? std::nullopt : std::optional(released.error());
                break;
            }
        }
        if (error) {
            ++stats_.rejected;
        } else {
            ++stats_.released;
        }
        callback_(cookie_, lease, error);
    }
    return runtime::ProgressBatch{*count, *count == limit};
}
} // namespace memory
} // namespace kbsocket
