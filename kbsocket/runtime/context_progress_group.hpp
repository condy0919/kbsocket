// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_CONTEXT_PROGRESS_GROUP_HPP_
#define KBSOCKET_RUNTIME_CONTEXT_PROGRESS_GROUP_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace runtime {

/// 同一 context 的全部 owner 访问作为一个源注册；独占控制任务共享暂停屏障，快照导入不暂停。
/// 每个成员在每类进度中获得独立 budget，总预算最多 kMaxMembers * budget；轮换首个成员。
/// 固定借用成员和 fd，不分配、不关闭成员；调用者不得把任何成员再独立注册或并发推进。
/// 成员及其控制任务依赖须活到 Close 成功；外部借用者也必须遵守 reactor 的 paused 状态。
class ContextProgressGroup final : public ProgressSource {
public:
    static constexpr std::size_t kMaxMembers = 5;
    static constexpr std::size_t kMaxFds = 80;

    ContextProgressGroup() = default;
    ContextProgressGroup(const ContextProgressGroup&) = delete;
    ContextProgressGroup& operator=(const ContextProgressGroup&) = delete;

    std::expected<void, ProgressError> Open(std::span<ProgressSource* const> members) noexcept;
    /// 已接受控制任务未收割时拒绝解绑；未接受的任务仍归成员所有，本组只放弃选择记录。
    std::expected<void, ProgressError> Close() noexcept;
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind kind, std::size_t budget,
                                                         std::uint64_t now_ns) noexcept override;
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t budget) noexcept override;
    std::expected<void, ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override;
    const void* exclusive_context() const noexcept override {
        return context_;
    }
    bool WaitFdEnabled(int fd) const noexcept override;
    std::optional<ControlTask> PendingControl() noexcept override;
    void ControlSubmitted(ControlTaskId id) noexcept override;
    void ControlCompleted(const ControlTaskCompletion& completion) noexcept override;

private:
    std::array<ProgressSource*, kMaxMembers> members_{};
    std::array<int, kMaxFds> fds_{};
    std::array<std::size_t, kMaxFds> fd_members_{};
    std::array<std::size_t, 8> cursors_{};
    std::size_t member_count_ = 0, fd_count_ = 0;
    std::size_t notification_cursor_ = 0, control_cursor_ = 0, selected_member_ = 0;
    std::optional<ControlTask> selected_;
    std::optional<ControlTaskId> submitted_;
    std::optional<ProgressError> failure_;
    const void* context_ = nullptr;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_CONTEXT_PROGRESS_GROUP_HPP_
