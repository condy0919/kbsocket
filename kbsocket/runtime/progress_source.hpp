// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_PROGRESS_SOURCE_HPP_
#define KBSOCKET_RUNTIME_PROGRESS_SOURCE_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/runtime/control_executor.hpp"

namespace kbsocket {
namespace runtime {
enum class ProgressKind : std::uint8_t { kAsync, kTx, kRx, kRefill, kFlush, kMailbox, kTimer, kRetire };
enum class ProgressErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kSystem,
    kProvider,
    kRetained,
    kResourceExhausted
};
struct ProgressError {
    ProgressErrorCode code;
    int detail = 0;
    std::size_t source = 0;
};
struct ProgressBatch {
    std::size_t processed = 0;
    // 已知仍有立即可推进的工作；等待 lease/重试期限不应持续置 true。
    bool runnable = false;
};

/// 单 owner、预分配的进度源。实现者须遵守每次调用的预算，回调不得阻塞、重入或销毁源。
/// 独占控制任务执行期间停止全部数据和事件访问及外部借用；独立快照任务可继续 owner 进度。
class ProgressSource {
public:
    virtual ~ProgressSource() = default;
    virtual std::expected<ProgressBatch, ProgressError> Progress(ProgressKind kind, std::size_t budget,
                                                                 std::uint64_t now_ns) noexcept = 0;
    virtual std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t budget) noexcept = 0;
    virtual std::expected<void, ProgressError> Rearm() noexcept = 0;
    // fds 在注册期间保持有效和不变；只观察 readiness，不由 reactor 读取 provider fd。
    virtual std::span<const int> wait_fds() const noexcept = 0;
    // 需要独占暂停的设备 context 身份；nullptr 表示不访问设备（例如 CPU mailbox 或普通 TCP）。
    // 身份在注册期间保持不变；访问 raw 的实现必须声明，不能以独立 hooks 绕过暂停屏障。
    virtual const void* exclusive_context() const noexcept {
        return nullptr;
    }
    // 保持 fd 身份不变，允许故障域退出等待集合；暂停期间 reactor 不调用此方法。
    virtual bool WaitFdEnabled(int) const noexcept {
        return true;
    }
    // 一个源最多一个已接受的控制任务。队列满时保持原请求；被接受后直到完成都不得复用参数。
    virtual std::optional<ControlTask> PendingControl() noexcept {
        return std::nullopt;
    }
    // worker 可能已经开始；此回调只能更新 owner 私有提交记录，不能访问任务参数或 raw 状态。
    virtual void ControlSubmitted(ControlTaskId) noexcept {}
    virtual void ControlCompleted(const ControlTaskCompletion&) noexcept {}
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_PROGRESS_SOURCE_HPP_
