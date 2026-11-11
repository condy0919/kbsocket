// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/tseg_import_progress.hpp"

namespace kbsocket {
namespace runtime {
namespace {
auto Error(ProgressErrorCode code) noexcept {
    return std::unexpected(ProgressError{code});
}
ProgressError ImportError(control::TsegImportCacheError error) noexcept {
    return {ProgressErrorCode::kProvider, error.provider_error ? error.provider_error : static_cast<int>(error.code)};
}
} // namespace

TsegImportProgress::~TsegImportProgress() {
    (void)Close();
}

std::expected<void, ProgressError> TsegImportProgress::Open(control::TsegImportCache& cache, urma_context_t* context,
                                                            std::uint64_t incarnation) noexcept {
    if (cache_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (!context || !incarnation) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    // 只验证缓存及过滤条件，不准备或提交返回的操作。
    if (auto next = cache.NextOperation(context, incarnation); !next) {
        return std::unexpected(ImportError(next.error()));
    }
    cache_ = &cache;
    context_ = context;
    incarnation_ = incarnation;
    failure_.reset();
    return {};
}

std::expected<void, ProgressError> TsegImportProgress::Close() noexcept {
    if (submitted_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if (prepared_) {
        if (auto discarded = cache_->DiscardOperation(*prepared_); !discarded) {
            return std::unexpected(ImportError(discarded.error()));
        }
        prepared_.reset();
    }
    cache_ = nullptr;
    context_ = nullptr;
    incarnation_ = 0;
    failure_.reset();
    return {};
}

std::expected<ProgressBatch, ProgressError> TsegImportProgress::Progress(ProgressKind, std::size_t budget,
                                                                         std::uint64_t) noexcept {
    if (!cache_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (!budget) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    if (failure_) {
        return std::unexpected(*failure_);
    }
    return ProgressBatch{};
}

std::expected<ProgressBatch, ProgressError> TsegImportProgress::CollectNotifications(std::size_t budget) noexcept {
    return Progress(ProgressKind::kAsync, budget, 0);
}

std::expected<void, ProgressError> TsegImportProgress::Rearm() noexcept {
    if (!cache_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (failure_) {
        return std::unexpected(*failure_);
    }
    return {};
}

std::optional<ControlTask> TsegImportProgress::PendingControl() noexcept {
    if (!cache_ || submitted_ || failure_) {
        return std::nullopt;
    }
    if (!prepared_) {
        auto next = cache_->NextOperation(context_, incarnation_);
        if (!next) {
            failure_ = ImportError(next.error());
            return std::nullopt;
        }
        if (!*next) {
            return std::nullopt;
        }
        auto prepared = cache_->PrepareOperation(**next);
        if (!prepared) {
            failure_ = ImportError(prepared.error());
            return std::nullopt;
        }
        prepared_ = *prepared;
    }
    // queue-full 时反应堆不会调用 ControlSubmitted；参数保持稳定，下一次只重试同一任务。
    // worker 仅访问缓存中的独立 Work 快照；完成收割前目标不会发布给数据面。
    auto task = prepared_->task;
    task.pause_progress = false;
    return task;
}

void TsegImportProgress::ControlSubmitted(ControlTaskId id) noexcept {
    if (!cache_ || !prepared_ || submitted_) {
        failure_ = ProgressError{ProgressErrorCode::kInUse};
        return;
    }
    // 一旦提交便禁止 Close/Discard，即使元数据检查失败也保留全部任务依赖。
    submitted_ = id;
    if (auto committed = cache_->CommitOperation(*prepared_, id); !committed) {
        failure_ = ImportError(committed.error());
    }
}

void TsegImportProgress::ControlCompleted(const ControlTaskCompletion& completion) noexcept {
    if (!cache_ || !submitted_ || completion.id != *submitted_) {
        failure_ = ProgressError{ProgressErrorCode::kInUse};
        return;
    }
    if (auto completed = cache_->HandleCompletion(completion); !completed) {
        failure_ = ImportError(completed.error());
        return;
    }
    submitted_.reset();
    prepared_.reset();
}

} // namespace runtime
} // namespace kbsocket
