// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_SHARD_REACTOR_HPP_
#define KBSOCKET_RUNTIME_SHARD_REACTOR_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace runtime {
struct ReactorConfig {
    bool events = false;
    std::size_t budget = 32;
    std::uint64_t busy_poll_ns = 50000;
    int max_wait_ms = 10;
};
struct ReactorStats {
    std::uint64_t ticks = 0;
    std::uint64_t processed = 0;
    std::uint64_t wait_calls = 0;
    std::uint64_t fd_wakes = 0;
    std::uint64_t empty_wakes = 0;
    std::uint64_t controls_submitted = 0;
    std::uint64_t controls_completed = 0;
    std::uint64_t control_backpressure = 0;
};

/// 单 owner 反应堆；源、执行器及其参数必须覆盖全部异步任务和反应堆的存活期。
/// 注册后独占执行器提交/收割与源进度。外部只能通过 Notify 唤醒，不得并发操作源或 raw 句柄。
/// 回调不可重入反应堆；控制任务暂停整个源并临时移除其 fd，完成后由 owner 恢复。
class ShardReactor {
public:
    static constexpr std::size_t kMaxSources = 8;
    static constexpr std::size_t kMaxFds = 80;
    ShardReactor() = default;
    ~ShardReactor();
    ShardReactor(const ShardReactor&) = delete;
    ShardReactor& operator=(const ShardReactor&) = delete;
    std::expected<void, ProgressError> Open(std::span<ProgressSource* const> sources, ControlExecutor& executor,
                                            ReactorConfig config = {}) noexcept;
    // Close 不删除借用资源；存在控制任务时必须先 Tick 收割，不能丢弃半成品。
    std::expected<void, ProgressError> Close() noexcept;
    void StopAccepting() noexcept {
        stopping_ = true;
    }
    std::expected<ProgressBatch, ProgressError> Tick() noexcept;
    std::expected<ProgressBatch, ProgressError> RunOnce() noexcept;
    // 唯一允许从其他线程调用的方法；FD 生命周期由宿主保证不与 Close 并发。
    std::expected<void, ProgressError> Notify() noexcept;
    bool paused(std::size_t source) const noexcept;
    const ReactorStats& stats() const noexcept {
        return stats_;
    }

private:
    struct SourceSlot {
        ProgressSource* source = nullptr;
        std::optional<ControlTaskId> task;
        bool paused = false;
    };
    struct Watch {
        int fd = -1;
        std::size_t source = 0;
        bool enabled = false;
    };
    std::expected<void, ProgressError> WatchSource(std::size_t source, bool enable) noexcept;
    std::expected<ProgressBatch, ProgressError> Advance() noexcept;
    std::expected<ProgressBatch, ProgressError> TickInternal() noexcept;
    std::expected<void, ProgressError> DrainWake() noexcept;
    static std::uint64_t Now() noexcept;
    void CloseFds() noexcept;
    std::array<SourceSlot, kMaxSources> sources_{};
    std::array<Watch, kMaxFds> watches_{};
    std::array<ControlTaskCompletion, 64> completions_{};
    std::size_t source_count_ = 0;
    std::size_t watch_count_ = 0;
    std::size_t next_source_ = 0;
    ControlExecutor* executor_ = nullptr;
    ReactorConfig config_{};
    ReactorStats stats_{};
    std::uint64_t idle_since_ = 0;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    bool stopping_ = false;
    bool advancing_ = false;
    bool running_ = false;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_SHARD_REACTOR_HPP_
