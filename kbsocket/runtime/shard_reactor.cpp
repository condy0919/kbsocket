// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/shard_reactor.hpp"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>

namespace kbsocket {
namespace runtime {
namespace {
auto Error(ProgressErrorCode code, int detail = 0, std::size_t source = 0) noexcept {
    return std::unexpected(ProgressError{code, detail, source});
}
bool SameTask(ControlTaskId lhs, ControlTaskId rhs) noexcept {
    return lhs == rhs;
}
} // namespace
ShardReactor::~ShardReactor() {
    CloseFds();
}
std::uint64_t ShardReactor::Now() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
void ShardReactor::CloseFds() noexcept {
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    if (wake_fd_ >= 0) {
        ::close(wake_fd_);
        wake_fd_ = -1;
    }
}
std::expected<void, ProgressError> ShardReactor::Open(std::span<ProgressSource* const> sources,
                                                      ControlExecutor& executor, ReactorConfig config) noexcept {
    if (executor_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (sources.empty() || sources.size() > kMaxSources || !config.budget || config.budget > 64 ||
        config.max_wait_ms < 0 || config.max_wait_ms > 1000 || executor.completion_fd() < 0 || executor.outstanding()) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    // 注册是控制路径，验证全部借用身份后才发布就绪状态。
    std::size_t fd_count = 0;
    for (std::size_t i = 0; i < sources.size(); ++i) {
        if (!sources[i]) {
            return Error(ProgressErrorCode::kInvalidArgument);
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (sources[i] == sources[j]) {
                return Error(ProgressErrorCode::kInvalidArgument);
            }
        }
        for (const auto fd : sources[i]->wait_fds()) {
            if (fd < 0 || fd == executor.completion_fd() || fd_count == kMaxFds) {
                return Error(ProgressErrorCode::kInvalidArgument);
            }
            for (std::size_t j = 0; j < fd_count; ++j) {
                if (watches_[j].fd == fd) {
                    return Error(ProgressErrorCode::kInvalidArgument);
                }
            }
            watches_[fd_count++] = {fd, i, false};
        }
    }
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        return Error(ProgressErrorCode::kSystem, errno);
    }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        const auto saved = errno;
        CloseFds();
        return Error(ProgressErrorCode::kSystem, saved);
    }
    for (const auto fd : {wake_fd_, executor.completion_fd()}) {
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.u64 = kMaxSources;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
            const auto saved = errno;
            CloseFds();
            return Error(ProgressErrorCode::kSystem, saved);
        }
    }
    watch_count_ = fd_count;
    source_count_ = sources.size();
    for (std::size_t i = 0; i < source_count_; ++i) {
        sources_[i] = {sources[i], {}, false};
        if (auto watched = WatchSource(i, true); !watched) {
            CloseFds();
            source_count_ = watch_count_ = 0;
            return watched;
        }
    }
    executor_ = &executor;
    config_ = config;
    stats_ = {};
    stopping_ = false;
    advancing_ = false;
    idle_since_ = 0;
    next_source_ = 0;
    return {};
}
std::expected<void, ProgressError> ShardReactor::WatchSource(std::size_t source, bool enable) noexcept {
    for (std::size_t i = 0; i < watch_count_; ++i) {
        auto& watch = watches_[i];
        if (watch.source != source) {
            continue;
        }
        const bool wanted = enable && sources_[source].source->WaitFdEnabled(watch.fd);
        if (watch.enabled == wanted) {
            continue;
        }
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.u64 = source;
        if (::epoll_ctl(epoll_fd_, wanted ? EPOLL_CTL_ADD : EPOLL_CTL_DEL, watch.fd, &event) < 0) {
            return Error(ProgressErrorCode::kSystem, errno, source);
        }
        watch.enabled = wanted;
    }
    return {};
}
std::expected<void, ProgressError> ShardReactor::Close() noexcept {
    if (advancing_ || running_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (!executor_) {
        CloseFds();
        return {};
    }
    if (executor_->outstanding()) {
        return Error(ProgressErrorCode::kInUse);
    }
    for (std::size_t i = 0; i < source_count_; ++i) {
        if (sources_[i].task) {
            return Error(ProgressErrorCode::kInUse);
        }
    }
    CloseFds();
    sources_ = {};
    source_count_ = watch_count_ = 0;
    executor_ = nullptr;
    return {};
}
bool ShardReactor::paused(std::size_t source) const noexcept {
    return source < source_count_ && sources_[source].paused;
}
std::expected<void, ProgressError> ShardReactor::Notify() noexcept {
    const std::uint64_t one = 1;
    if (wake_fd_ < 0) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (::write(wake_fd_, &one, sizeof(one)) == sizeof(one) || errno == EAGAIN) {
        return {};
    }
    return Error(ProgressErrorCode::kSystem, errno);
}
std::expected<void, ProgressError> ShardReactor::DrainWake() noexcept {
    std::uint64_t value;
    if (::read(wake_fd_, &value, sizeof(value)) == sizeof(value) || errno == EAGAIN || errno == EINTR) {
        return {};
    }
    return Error(ProgressErrorCode::kSystem, errno);
}
std::expected<ProgressBatch, ProgressError> ShardReactor::Advance() noexcept {
    ProgressBatch result;
    std::optional<ProgressError> failure;
    auto record = [&](const std::expected<ProgressBatch, ProgressError>& step, std::size_t source) {
        if (step) {
            result.processed += step->processed;
            result.runnable |= step->runnable;
        } else if (!failure) {
            failure = step.error();
            failure->source = source;
        }
    };
    // 先归还控制任务所有权。完成无论成功/失败/取消，都交回源处理，不能丢弃半成品。
    auto completed = executor_->Poll(std::span(completions_).first(config_.budget));
    if (!completed) {
        return Error(ProgressErrorCode::kSystem);
    }
    result.processed += *completed;
    result.runnable |= *completed == config_.budget;
    for (std::size_t c = 0; c < *completed; ++c) {
        bool matched = false;
        for (std::size_t i = 0; i < source_count_; ++i) {
            auto& slot = sources_[i];
            if (!slot.task || !SameTask(*slot.task, completions_[c].id)) {
                continue;
            }
            slot.source->ControlCompleted(completions_[c]);
            slot.task.reset();
            if (auto watch = WatchSource(i, true); !watch) {
                if (!failure) {
                    failure = watch.error();
                }
            } else {
                slot.paused = false;
            }
            ++stats_.controls_completed;
            matched = true;
            break;
        }
        if (!matched && !failure) {
            failure = ProgressError{ProgressErrorCode::kInvalidArgument};
        }
    }
    const auto now = Now();
    // 按类别遍历所有源，每类均有独立预算；热 TX/错误风暴不能吃掉 RX、timer 或归还预算。
    constexpr std::array kinds{ProgressKind::kAsync,  ProgressKind::kTx,    ProgressKind::kRx,
                               ProgressKind::kRefill, ProgressKind::kFlush, ProgressKind::kMailbox,
                               ProgressKind::kTimer,  ProgressKind::kRetire};
    for (const auto kind : kinds) {
        for (std::size_t n = 0; n < source_count_; ++n) {
            const auto i = (next_source_ + n) % source_count_;
            auto& slot = sources_[i];
            if (!slot.paused) {
                record(slot.source->Progress(kind, config_.budget, now), i);
            }
        }
    }
    for (std::size_t n = 0; n < source_count_; ++n) {
        const auto i = (next_source_ + n) % source_count_;
        auto& slot = sources_[i];
        if (slot.paused) {
            continue;
        }
        record(slot.source->CollectNotifications(config_.budget), i);
        if (auto watch = WatchSource(i, true); !watch) {
            if (!failure) {
                failure = watch.error();
            }
        }
        if (stopping_ || slot.task) {
            continue;
        }
        const auto task = slot.source->PendingControl();
        if (!task) {
            continue;
        }
        // 只有共享队列变更需要在发布前停止访问；快照导入任务保留数据面和 fd 就绪。
        // 工作线程可在 TrySubmit 返回前执行，不能事后才建立独占屏障。
        if (task->pause_progress) {
            slot.paused = true;
            if (auto watch = WatchSource(i, false); !watch) {
                if (!failure) {
                    failure = watch.error();
                }
                continue;
            }
        }
        const auto submitted = executor_->TrySubmit(*task);
        if (!submitted) {
            if (submitted.error().code == ControlExecutorErrorCode::kResourceExhausted) {
                ++stats_.control_backpressure;
            } else if (!failure) {
                failure = ProgressError{ProgressErrorCode::kSystem,
                                        submitted.error().system_error ? submitted.error().system_error : EINVAL, i};
            }
            if (auto watch = WatchSource(i, true); !watch) {
                if (!failure) {
                    failure = watch.error();
                }
            } else {
                slot.paused = false;
            }
            // 已有任务完成会唤醒，有限 wait 也会重试；队列满时不忙等。
            continue;
        }
        slot.task = *submitted;
        slot.source->ControlSubmitted(*submitted);
        ++stats_.controls_submitted;
        ++result.processed;
    }
    next_source_ = (next_source_ + 1) % source_count_;
    ++stats_.ticks;
    stats_.processed += result.processed;
    if (failure) {
        return std::unexpected(*failure);
    }
    return result;
}
std::expected<ProgressBatch, ProgressError> ShardReactor::Tick() noexcept {
    if (!executor_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (advancing_ || running_) {
        return Error(ProgressErrorCode::kInUse);
    }
    return TickInternal();
}
std::expected<ProgressBatch, ProgressError> ShardReactor::TickInternal() noexcept {
    advancing_ = true;
    auto result = Advance();
    advancing_ = false;
    return result;
}
std::expected<ProgressBatch, ProgressError> ShardReactor::RunOnce() noexcept {
    if (!executor_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (advancing_ || running_) {
        return Error(ProgressErrorCode::kInUse);
    }
    running_ = true;
    struct RunningGuard {
        bool& active;
        ~RunningGuard() {
            active = false;
        }
    } guard{running_};
    // 先清 wake，再复查工作；此后的跨线程发布会留下可读 fd，不能在最后一次 recheck 后清除。
    if (auto wake = DrainWake(); !wake) {
        return std::unexpected(wake.error());
    }
    auto result = TickInternal();
    if (!result) {
        return result;
    }
    if (!config_.events) {
        return result;
    }
    if (result->processed || result->runnable) {
        idle_since_ = 0;
        return result;
    }
    const auto now = Now();
    if (!idle_since_) {
        idle_since_ = now;
    }
    if (now - idle_since_ < config_.busy_poll_ns) {
        return result;
    }
    std::optional<ProgressError> failure;
    for (std::size_t i = 0; i < source_count_; ++i) {
        if (sources_[i].paused) {
            continue;
        }
        if (auto arm = sources_[i].source->Rearm(); !arm && !failure) {
            failure = arm.error();
            failure->source = i;
        }
    }
    // 即使 arm 失败也先收割已经取得的通知；后续 Tick 会继续服务其他健康源。
    auto recheck = TickInternal();
    if (failure) {
        return std::unexpected(*failure);
    }
    if (!recheck) {
        return recheck;
    }
    if (recheck->processed || recheck->runnable) {
        idle_since_ = 0;
        return recheck;
    }
    std::array<epoll_event, kMaxFds + 2> events{};
    ++stats_.wait_calls;
    const int count = ::epoll_wait(epoll_fd_, events.data(), events.size(), config_.max_wait_ms);
    if (count < 0) {
        if (errno == EINTR) {
            return ProgressBatch{};
        }
        return Error(ProgressErrorCode::kSystem, errno);
    }
    stats_.fd_wakes += count;
    stats_.empty_wakes += count == 0;
    for (int i = 0; i < count; ++i) {
        if (events[i].events & (EPOLLERR | EPOLLHUP)) {
            return Error(ProgressErrorCode::kSystem, EIO, events[i].data.u64);
        }
    }
    return TickInternal();
}
} // namespace runtime
} // namespace kbsocket
