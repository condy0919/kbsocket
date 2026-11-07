// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/runtime_shard.hpp"

#include <new>

#include "kbsocket/runtime/context_progress_group.hpp"

namespace kbsocket {
namespace runtime {
namespace {
auto Error(ProgressErrorCode code, int detail = 0) noexcept {
    return std::unexpected(ProgressError{code, detail});
}
} // namespace
struct RuntimeShard::State {
    std::array<RawContextProgress, kMaxContexts> contexts;
    std::array<ContextProgressGroup, kMaxContexts> groups;
    ControlExecutor executor;
    ShardReactor reactor;
    std::size_t count = 0;
    bool stopping = false;
};
RuntimeShard::RuntimeShard() noexcept = default;
RuntimeShard::~RuntimeShard() {
    if (!Close()) {
        (void)state_.release();
    }
}
std::expected<void, ProgressError> RuntimeShard::Open(std::span<const RawContextBinding> contexts,
                                                      const RuntimeShardConfig& config,
                                                      ProgressSource* hooks) noexcept {
    if (state_) {
        return Error(ProgressErrorCode::kInUse);
    }
    if ((contexts.empty() && !hooks) || contexts.size() > kMaxContexts || (hooks && hooks->exclusive_context())) {
        return Error(ProgressErrorCode::kInvalidArgument);
    }
    // 成员不能横跨 context 或同时成为 hooks，否则一个暂停屏障之外仍会推进同一份状态。
    std::array<ProgressSource*, kMaxContexts * 2> companions{};
    std::size_t companion_count = 0;
    for (const auto& context : contexts) {
        for (auto* companion : {context.connections, context.control}) {
            if (!companion) {
                continue;
            }
            if (companion == hooks ||
                (companion->exclusive_context() && companion->exclusive_context() != context.context)) {
                return Error(ProgressErrorCode::kInvalidArgument);
            }
            for (std::size_t i = 0; i < companion_count; ++i) {
                if (companions[i] == companion) {
                    return Error(ProgressErrorCode::kInvalidArgument);
                }
            }
            companions[companion_count++] = companion;
        }
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(ProgressErrorCode::kResourceExhausted);
    }
    for (const auto& context : contexts) {
        if (config.reactor.events) {
            for (const auto& binding : context.queues) {
                if (!binding.pool || !binding.pool->jfce() || binding.pool->jfce()->fd < 0) {
                    return Error(ProgressErrorCode::kInvalidArgument);
                }
            }
        }
        auto opened = state->contexts[state->count].Open(context.context, context.queues);
        if (!opened) {
            return opened;
        }
        std::array<ProgressSource*, 3> members{&state->contexts[state->count]};
        std::size_t member_count = 1;
        if (context.connections) {
            members[member_count++] = context.connections;
        }
        if (context.control) {
            members[member_count++] = context.control;
        }
        if (auto grouped = state->groups[state->count].Open(std::span(members).first(member_count)); !grouped) {
            return grouped;
        }
        ++state->count;
    }
    if (auto opened = state->executor.Open(config.control); !opened) {
        return Error(ProgressErrorCode::kSystem, opened.error().system_error);
    }
    std::array<ProgressSource*, kMaxContexts + 1> sources{};
    for (std::size_t i = 0; i < state->count; ++i) {
        sources[i] = &state->groups[i];
    }
    auto count = state->count;
    if (hooks) {
        sources[count++] = hooks;
    }
    if (auto opened = state->reactor.Open(std::span(sources).first(count), state->executor, config.reactor); !opened) {
        return opened;
    }
    state_ = std::move(state);
    return {};
}
std::expected<ProgressBatch, ProgressError> RuntimeShard::Tick() noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    return state_->reactor.Tick();
}
std::expected<ProgressBatch, ProgressError> RuntimeShard::RunOnce() noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    return state_->reactor.RunOnce();
}
std::expected<void, ProgressError> RuntimeShard::Notify() noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    return state_->reactor.Notify();
}
void RuntimeShard::StopAccepting() noexcept {
    if (state_) {
        state_->stopping = true;
        state_->reactor.StopAccepting();
    }
}
std::expected<void, ProgressError> RuntimeShard::Close() noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (auto closed = state_->reactor.Close(); !closed) {
        return closed;
    }
    // 控制任务都已收割；未提交的水线/替换意图由源解绑放弃，不代表其 raw 资源已释放。
    for (std::size_t i = state_->count; i > 0; --i) {
        if (auto closed = state_->groups[i - 1].Close(); !closed) {
            return closed;
        }
        if (auto closed = state_->contexts[i - 1].Close(); !closed) {
            return closed;
        }
    }
    if (auto closed = state_->executor.Close(); !closed) {
        return Error(ProgressErrorCode::kInUse, closed.error().system_error);
    }
    state_.reset();
    return {};
}
std::expected<void, ProgressError> RuntimeShard::RequestReplacement(std::size_t context, raw::JettyLane lane,
                                                                    const raw::JettyLaneQuiescence& proof) noexcept {
    if (!state_) {
        return Error(ProgressErrorCode::kNotReady);
    }
    if (state_->stopping || context >= state_->count || state_->reactor.paused(context)) {
        return Error(ProgressErrorCode::kInUse);
    }
    return state_->contexts[context].RequestReplacement(lane, proof);
}
bool RuntimeShard::paused(std::size_t context) const noexcept {
    return state_ && context < state_->count && state_->reactor.paused(context);
}
const RawDomainProgressStats* RuntimeShard::domain_stats(std::size_t context, std::size_t domain) const noexcept {
    if (!state_ || context >= state_->count || state_->reactor.paused(context)) {
        return nullptr;
    }
    return state_->contexts[context].domain_stats(domain);
}
const ReactorStats* RuntimeShard::stats() const noexcept {
    return state_ ? &state_->reactor.stats() : nullptr;
}
} // namespace runtime
} // namespace kbsocket
