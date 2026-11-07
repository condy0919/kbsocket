// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/context_progress_group.hpp"

#include <limits>

namespace kbsocket {
namespace runtime {
namespace {
auto Error(ProgressErrorCode code) noexcept {
    return std::unexpected(ProgressError{code});
}
} // namespace

std::expected<void, ProgressError> ContextProgressGroup::Open(std::span<ProgressSource* const> members) noexcept {
    if (member_count_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (members.empty() || members.size() > kMaxMembers) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    std::size_t count = 0;
    const void* context = nullptr;
    for (std::size_t i = 0; i < members.size(); ++i) {
        if (!members[i] || members[i] == this) {
            return Error(ProgressErrorCode::kInvalidArgument);
        }
        if (const auto* member_context = members[i]->exclusive_context()) {
            if (context && context != member_context) {
                return Error(ProgressErrorCode::kInvalidArgument);
            }
            context = member_context;
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (members[i] == members[j]) {
                return Error(ProgressErrorCode::kInvalidArgument);
            }
        }
        for (const auto fd : members[i]->wait_fds()) {
            if (fd < 0 || count == kMaxFds) {
                return Error(ProgressErrorCode::kInvalidArgument);
            }
            for (std::size_t j = 0; j < count; ++j) {
                if (fds_[j] == fd) {
                    return Error(ProgressErrorCode::kInvalidArgument);
                }
            }
            fds_[count] = fd;
            fd_members_[count++] = i;
        }
    }
    for (std::size_t i = 0; i < members.size(); ++i) {
        members_[i] = members[i];
    }
    member_count_ = members.size();
    fd_count_ = count;
    context_ = context;
    cursors_ = {};
    notification_cursor_ = control_cursor_ = selected_member_ = 0;
    selected_.reset();
    submitted_.reset();
    failure_.reset();
    return {};
}

std::expected<void, ProgressError> ContextProgressGroup::Close() noexcept {
    if (submitted_) {
        return Error(ProgressErrorCode::kInUse);
    }
    member_count_ = fd_count_ = 0;
    members_ = {};
    context_ = nullptr;
    selected_.reset();
    failure_.reset();
    return {};
}

std::expected<ProgressBatch, ProgressError> ContextProgressGroup::Progress(ProgressKind kind, std::size_t budget,
                                                                           std::uint64_t now_ns) noexcept {
    if (!member_count_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (submitted_) {
        return Error(ProgressErrorCode::kInUse);
    }
    const auto index = static_cast<std::size_t>(kind);
    if (index >= cursors_.size() || !budget || budget > std::numeric_limits<std::size_t>::max() / member_count_) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    ProgressBatch result;
    std::optional<ProgressError> error;
    const auto first = cursors_[index];
    for (std::size_t n = 0; n < member_count_; ++n) {
        const auto i = (first + n) % member_count_;
        auto progress = members_[i]->Progress(kind, budget, now_ns);
        if (!progress) {
            if (!error) {
                error = progress.error();
                error->source = i;
            }
        } else {
            result.processed += progress->processed;
            result.runnable |= progress->runnable;
        }
    }
    cursors_[index] = (first + 1) % member_count_;
    if (error) {
        return std::unexpected(*error);
    }
    return result;
}

std::expected<ProgressBatch, ProgressError> ContextProgressGroup::CollectNotifications(std::size_t budget) noexcept {
    if (!member_count_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (submitted_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (!budget || budget > std::numeric_limits<std::size_t>::max() / member_count_) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    ProgressBatch result;
    std::optional<ProgressError> error;
    for (std::size_t n = 0; n < member_count_; ++n) {
        const auto i = (notification_cursor_ + n) % member_count_;
        auto progress = members_[i]->CollectNotifications(budget);
        if (!progress) {
            if (!error) {
                error = progress.error();
                error->source = i;
            }
        } else {
            result.processed += progress->processed;
            result.runnable |= progress->runnable;
        }
    }
    notification_cursor_ = (notification_cursor_ + 1) % member_count_;
    if (error) {
        return std::unexpected(*error);
    }
    return result;
}

std::expected<void, ProgressError> ContextProgressGroup::Rearm() noexcept {
    if (!member_count_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (submitted_) {
        return Error(ProgressErrorCode::kInUse);
    }
    std::optional<ProgressError> error;
    for (std::size_t i = 0; i < member_count_; ++i) {
        if (auto armed = members_[i]->Rearm(); !armed && !error) {
            error = armed.error();
            error->source = i;
        }
    }
    if (error) {
        return std::unexpected(*error);
    }
    return {};
}

std::span<const int> ContextProgressGroup::wait_fds() const noexcept {
    return std::span(fds_).first(fd_count_);
}
bool ContextProgressGroup::WaitFdEnabled(int fd) const noexcept {
    if (submitted_ || failure_) {
        return false;
    }
    for (std::size_t i = 0; i < fd_count_; ++i) {
        if (fds_[i] == fd) {
            return members_[fd_members_[i]]->WaitFdEnabled(fd);
        }
    }
    return false;
}

std::optional<ControlTask> ContextProgressGroup::PendingControl() noexcept {
    if (!member_count_ || submitted_ || failure_) {
        return std::nullopt;
    }
    if (selected_) {
        return selected_;
    }
    for (std::size_t n = 0; n < member_count_; ++n) {
        const auto i = (control_cursor_ + n) % member_count_;
        if (auto task = members_[i]->PendingControl()) {
            selected_ = task;
            selected_member_ = i;
            return selected_;
        }
    }
    return std::nullopt;
}
void ContextProgressGroup::ControlSubmitted(ControlTaskId id) noexcept {
    if (!selected_ || submitted_ || !id.executor || !id.sequence) {
        failure_ = ProgressError{ProgressErrorCode::kInvalidArgument};
        return;
    }
    submitted_ = id;
    // 只写 owner 私有记录；任务可能已经在 worker 上执行，禁止读取其参数内容。
    members_[selected_member_]->ControlSubmitted(id);
}
void ContextProgressGroup::ControlCompleted(const ControlTaskCompletion& completion) noexcept {
    if (!submitted_ || *submitted_ != completion.id || !selected_ || selected_->argument != completion.argument ||
        selected_->token != completion.token) {
        failure_ = ProgressError{ProgressErrorCode::kInvalidArgument};
        return;
    }
    members_[selected_member_]->ControlCompleted(completion);
    control_cursor_ = (selected_member_ + 1) % member_count_;
    selected_.reset();
    submitted_.reset();
}

} // namespace runtime
} // namespace kbsocket
