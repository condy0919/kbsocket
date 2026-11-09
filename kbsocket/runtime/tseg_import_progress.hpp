// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_TSEG_IMPORT_PROGRESS_HPP_
#define KBSOCKET_RUNTIME_TSEG_IMPORT_PROGRESS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/control/tseg_import_cache.hpp"
#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace runtime {

/// 每个 context 实例的段导入控制源；须与 endpoint 导入及全部 post/poll/refill 使用者组成同一暂停域。
/// 单独注册在另一反应堆槽并不能暂停 raw 使用者，因此宿主必须使用共享暂停域的组合源。
/// 不拥有缓存、context 或 executor，不消费 fd；缓存与本对象存活至已接受任务被完整收割。
class TsegImportProgress final : public ProgressSource {
public:
    TsegImportProgress() = default;
    ~TsegImportProgress() override;
    TsegImportProgress(const TsegImportProgress&) = delete;
    TsegImportProgress& operator=(const TsegImportProgress&) = delete;

    std::expected<void, ProgressError> Open(control::TsegImportCache& cache, urma_context_t* context,
                                            std::uint64_t incarnation) noexcept;
    /// 撤销尚未提交的准备；已接受任务返回 kInUse，必须继续收割后重试。
    std::expected<void, ProgressError> Close() noexcept;
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind kind, std::size_t budget,
                                                         std::uint64_t now_ns) noexcept override;
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t budget) noexcept override;
    std::expected<void, ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override {
        return {};
    }
    const void* exclusive_context() const noexcept override {
        return context_;
    }
    std::optional<ControlTask> PendingControl() noexcept override;
    void ControlSubmitted(ControlTaskId id) noexcept override;
    void ControlCompleted(const ControlTaskCompletion& completion) noexcept override;

    bool in_flight() const noexcept {
        return submitted_.has_value();
    }
    const std::optional<ProgressError>& failure() const noexcept {
        return failure_;
    }

private:
    control::TsegImportCache* cache_ = nullptr;
    urma_context_t* context_ = nullptr;
    std::uint64_t incarnation_ = 0;
    std::optional<control::TsegImportPreparedOperation> prepared_;
    std::optional<ControlTaskId> submitted_;
    std::optional<ProgressError> failure_;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_TSEG_IMPORT_PROGRESS_HPP_
