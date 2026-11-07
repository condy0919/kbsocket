// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/raw_stack.hpp"

#include <new>
#include <stdexcept>
#include <vector>

#include "kbsocket/runtime/import_progress.hpp"

namespace kbsocket {
namespace runtime {
namespace {
using Code = RawStackErrorCode;
auto Error(Code code, std::size_t index = 0) noexcept {
    return std::unexpected(RawStackError{.code = code, .context_index = index});
}
RawStackError ProgressErrorAt(ProgressError error, std::size_t index = 0) noexcept {
    return {.code = Code::kProgress, .context_index = index, .progress = error};
}
RawStackError DomainErrorAt(RawDomainError error, std::size_t index) noexcept {
    return {.code = error.code == RawDomainErrorCode::kRetained ? Code::kRetained : Code::kDomain,
            .context_index = index,
            .domain = error};
}
bool All(const raw::ReceiveDomainQuiescence& proof) noexcept {
    return proof.peer_stopped && proof.events_acked && proof.queue_users_stopped && proof.completion_queues_healthy;
}
} // namespace

struct RawStack::State {
    struct Context {
        RawDomain domain;
        ConnectionProgress progress;
        ConnectionOwner connections;
        ConnectionMailbox commands;
        PeerCreditProgress withdrawals;
        ImportProgress imports;
        RawQueueBinding binding;
    };
    // 未完成的析构保留整份 State；绝不能先析构 session 再留下借用它的域、MR 或 worker。
    raw::RawRuntime session;
    memory::MemoryBudget budget;
    memory::SlabAllocator slab;
    control::EndpointImportCache imports;
    std::array<Context, kMaxContexts> contexts;
    RuntimeShard runtime;
    std::size_t count = 0;
    RawStackStage stage = RawStackStage::kOpenFailed;
    bool published = false, attached = false;
    std::optional<RawStackError> open_error, close_error;

    explicit State(std::size_t bytes) noexcept : budget(bytes) {}
    bool AnyPaused() const noexcept {
        for (std::size_t i = 0; i < count; ++i) {
            if (runtime.paused(i)) {
                return true;
            }
        }
        return false;
    }
    std::unexpected<RawStackError> FailOpen(RawStackError error) noexcept {
        open_error = error;
        stage = RawStackStage::kOpenFailed;
        return std::unexpected(error);
    }
    std::unexpected<RawStackError> FailClose(RawStackError error) noexcept {
        close_error = error;
        if (error.code == Code::kRetained) {
            stage = RawStackStage::kRetained;
        }
        return std::unexpected(error);
    }
};

RawStack::RawStack() noexcept = default;
RawStack::~RawStack() {
    if (state_ && state_->stage != RawStackStage::kClosed) {
        (void)state_.release();
    }
}
std::expected<void, RawStackError> RawStack::Open(std::span<const RawEndpointConfig> endpoints,
                                                  const RawStackConfig& config) noexcept {
    if (state_) {
        return Error(Code::kInUse);
    }
    if (endpoints.empty() || endpoints.size() > kMaxContexts || !config.memory_budget_bytes ||
        !config.connections_per_context || config.connections_per_context > ConnectionProgress::kMaxConnections ||
        !config.withdrawals_per_context || config.withdrawals_per_context > PeerCreditProgress::kMaxPeers ||
        !config.commands_per_context) {
        return Error(Code::kInvalidArgument);
    }
    for (std::size_t i = 0; i < endpoints.size(); ++i) {
        const auto& e = endpoints[i];
        if (!e.domain.context_incarnation || !e.domain.local_domain.runtime_instance ||
            !e.domain.local_domain.receive_domain ||
            e.domain.local_domain.runtime_instance != endpoints[0].domain.local_domain.runtime_instance ||
            (config.runtime.reactor.events && !e.domain.queues.events)) {
            return Error(Code::kInvalidArgument, i);
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (e.domain.context_incarnation == endpoints[j].domain.context_incarnation ||
                e.domain.local_domain.receive_domain == endpoints[j].domain.local_domain.receive_domain ||
                (e.device.device_name == endpoints[j].device.device_name &&
                 e.device.eid_index == endpoints[j].device.eid_index)) {
                return Error(Code::kInvalidArgument, i);
            }
        }
    }
    state_.reset(new (std::nothrow) State(config.memory_budget_bytes));
    if (!state_) {
        return Error(Code::kNoMemory);
    }
    auto& s = *state_;
    s.count = endpoints.size();
    // 生命周期控制路径允许分配；失败也留下单次 State，统一通过显式关闭处理。
    try {
        std::vector<raw::DeviceConfig> devices;
        devices.reserve(endpoints.size());
        for (const auto& endpoint : endpoints) {
            devices.push_back(endpoint.device);
        }
        if (const auto opened = s.session.Initialize(devices); !opened) {
            return s.FailOpen(
                {.code = Code::kSession, .context_index = opened.error().device_index, .session = opened.error()});
        }
    } catch (const std::bad_alloc&) {
        return s.FailOpen({.code = Code::kNoMemory});
    } catch (const std::length_error&) {
        return s.FailOpen({.code = Code::kNoMemory});
    }
    std::array<urma_context_t*, kMaxContexts> handles{};
    for (std::size_t i = 0; i < s.count; ++i) {
        handles[i] = s.session.context(i);
    }
    if (const auto opened = s.slab.Open(std::span(handles).first(s.count), config.slab, s.budget); !opened) {
        return s.FailOpen(
            {.code = Code::kMemory, .context_index = opened.error().context_index, .memory = opened.error()});
    }
    if (const auto opened = s.imports.Open(config.imports); !opened) {
        return s.FailOpen({.code = Code::kImport, .import = opened.error()});
    }
    std::array<RawContextBinding, kMaxContexts> bindings{};
    for (std::size_t i = 0; i < s.count; ++i) {
        auto& c = s.contexts[i];
        const auto& endpoint = endpoints[i];
        const auto* cap = s.session.capabilities(i);
        if (!handles[i] || !cap) {
            return s.FailOpen({.code = Code::kSession, .context_index = i});
        }
        if (const auto opened = c.domain.Open(handles[i], *cap, endpoint.domain); !opened) {
            return s.FailOpen(DomainErrorAt(opened.error(), i));
        }
        if (const auto opened =
                c.progress.Open({config.connections_per_context, handles[i]}, endpoint.on_connection, endpoint.cookie);
            !opened) {
            return s.FailOpen(ProgressErrorAt(opened.error(), i));
        }
        if (const auto opened = c.connections.Open(c.progress, {config.connections_per_context}); !opened) {
            return s.FailOpen({.code = Code::kConnection, .context_index = i, .connection = opened.error()});
        }
        if (const auto opened =
                c.commands.Open(c.connections, {config.commands_per_context}, endpoint.on_command, endpoint.cookie);
            !opened) {
            return s.FailOpen({.code = Code::kMailbox, .context_index = i, .mailbox = opened.error()});
        }
        if (const auto opened = c.withdrawals.Open({config.withdrawals_per_context, handles[i]}, endpoint.on_withdrawal,
                                                   endpoint.cookie);
            !opened) {
            return s.FailOpen(ProgressErrorAt(opened.error(), i));
        }
        if (const auto opened = c.imports.Open(s.imports, handles[i], endpoint.domain.context_incarnation); !opened) {
            return s.FailOpen(ProgressErrorAt(opened.error(), i));
        }
        const auto bound = c.domain.Bind(c.progress);
        if (!bound) {
            return s.FailOpen(DomainErrorAt(bound.error(), i));
        }
        c.binding = *bound;
        bindings[i] = {.context = handles[i],
                       .queues = std::span(&c.binding, 1),
                       .connections = &c.progress,
                       .control = &c.imports,
                       .mailbox = &c.commands,
                       .peers = &c.withdrawals};
    }
    if (const auto opened = s.runtime.Open(std::span(bindings).first(s.count), config.runtime); !opened) {
        return s.FailOpen(ProgressErrorAt(opened.error()));
    }
    s.attached = s.published = true;
    s.stage = RawStackStage::kReady;
    return {};
}
RawStackStage RawStack::stage() const noexcept {
    return state_ ? state_->stage : RawStackStage::kEmpty;
}
bool RawStack::paused(std::size_t context) const noexcept {
    return state_ && context < state_->count && state_->runtime.paused(context);
}
std::expected<RawStackContextResources, RawStackError> RawStack::resources(std::size_t index) noexcept {
    if (!state_ || !state_->published || index >= state_->count || stage() == RawStackStage::kClosed) {
        return Error(Code::kNotReady, index);
    }
    if (paused(index)) {
        return Error(Code::kInUse, index);
    }
    auto& c = state_->contexts[index];
    const auto domain = c.domain.resources();
    if (!domain) {
        return std::unexpected(DomainErrorAt(domain.error(), index));
    }
    return RawStackContextResources{state_->session.context(index), *domain, &c.connections, &c.commands,
                                    &c.withdrawals};
}
std::expected<RawStackSharedResources, RawStackError> RawStack::shared() noexcept {
    if (!state_ || !state_->published || stage() == RawStackStage::kClosed) {
        return Error(Code::kNotReady);
    }
    if (state_->AnyPaused()) {
        return Error(Code::kInUse);
    }
    return RawStackSharedResources{&state_->slab, &state_->imports};
}
std::expected<RawStackSnapshot, RawStackError> RawStack::Inspect() const noexcept {
    if (!state_) {
        return RawStackSnapshot{};
    }
    const auto& s = *state_;
    if (s.AnyPaused()) {
        return Error(Code::kInUse);
    }
    RawStackSnapshot snapshot;
    snapshot.stage = s.stage;
    snapshot.context_count = s.count;
    snapshot.progress_attached = s.attached;
    snapshot.slab = s.slab.stats();
    snapshot.budget_bytes = s.budget.reserved_bytes();
    snapshot.import_entries = s.imports.entries();
    snapshot.import_leases = s.imports.leases();
    snapshot.import_tasks = s.imports.in_flight();
    snapshot.open_error = s.open_error;
    snapshot.close_error = s.close_error;
    for (std::size_t i = 0; i < s.count; ++i) {
        const auto& c = s.contexts[i];
        snapshot.contexts[i] = {.domain = c.domain.Inspect(),
                                .connections = c.connections.stats().occupied,
                                .scheduled = c.progress.stats() ? c.progress.stats()->attached : 0,
                                .commands = c.commands.outstanding(),
                                .withdrawals = c.withdrawals.stats() ? c.withdrawals.stats()->attached : 0};
    }
    return snapshot;
}
std::expected<ProgressBatch, ProgressError> RawStack::Tick() noexcept {
    if (!state_ || !state_->attached) {
        return std::unexpected(ProgressError{ProgressErrorCode::kNotReady});
    }
    return state_->runtime.Tick();
}
std::expected<ProgressBatch, ProgressError> RawStack::RunOnce() noexcept {
    if (!state_ || !state_->attached) {
        return std::unexpected(ProgressError{ProgressErrorCode::kNotReady});
    }
    return state_->runtime.RunOnce();
}
std::expected<void, ProgressError> RawStack::Notify() noexcept {
    if (!state_ || !state_->attached) {
        return std::unexpected(ProgressError{ProgressErrorCode::kNotReady});
    }
    return state_->runtime.Notify();
}
std::expected<void, ProgressError> RawStack::RequestReplacement(std::size_t context, raw::JettyLane lane,
                                                                const raw::JettyLaneQuiescence& proof) noexcept {
    if (!state_ || stage() != RawStackStage::kReady) {
        return std::unexpected(ProgressError{ProgressErrorCode::kNotReady});
    }
    return state_->runtime.RequestReplacement(context, lane, proof);
}
std::expected<void, RawStackError> RawStack::BeginClose() noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto& s = *state_;
    if (s.AnyPaused()) {
        return Error(Code::kInUse);
    }
    if (s.stage == RawStackStage::kRetained) {
        return Error(Code::kRetained);
    }
    if (s.stage != RawStackStage::kReady && s.stage != RawStackStage::kOpenFailed) {
        return {};
    }
    s.slab.StopAccepting();
    s.imports.StopAccepting();
    std::optional<RawStackError> failure;
    for (std::size_t i = 0; i < s.count; ++i) {
        auto& c = s.contexts[i];
        c.commands.StopAccepting();
        c.connections.StopAccepting();
        if (c.domain.Inspect().stage != RawDomainStage::kEmpty) {
            if (const auto begun = c.domain.BeginClose(); !begun && !failure) {
                failure = DomainErrorAt(begun.error(), i);
            }
        }
    }
    // 不停止 Runtime 控制准入：缓存失效后的 unimport 和 ERROR 排空仍须执行/收割。
    if (failure) {
        return s.FailClose(*failure);
    }
    s.stage = RawStackStage::kStopping;
    return {};
}
std::expected<RawStackSnapshot, RawStackError> RawStack::AdvanceClose(const RawStackCloseProof& proof) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto& s = *state_;
    if (s.stage == RawStackStage::kClosed) {
        return Inspect();
    }
    if (s.stage == RawStackStage::kRetained) {
        return Error(Code::kRetained);
    }
    if (s.stage != RawStackStage::kStopping && s.stage != RawStackStage::kClosingDomains &&
        s.stage != RawStackStage::kClosingMemory && s.stage != RawStackStage::kClosingContexts) {
        return Error(Code::kNotReady);
    }
    if (s.AnyPaused() || !proof.clients_stopped || proof.domains.size() != s.count) {
        return Error(Code::kInUse);
    }
    for (std::size_t i = 0; i < s.count; ++i) {
        if (!All(proof.domains[i])) {
            return Error(Code::kInUse, i);
        }
    }
    if (s.stage == RawStackStage::kStopping) {
        // 所有域先做无副作用检查；一域尚忙不能提前停掉其他域为它完成 RX/TX/unimport 的机会。
        for (std::size_t i = 0; i < s.count; ++i) {
            auto& c = s.contexts[i];
            const auto domain = c.domain.Inspect();
            if (domain.stage == RawDomainStage::kRetained) {
                return s.FailClose({.code = Code::kRetained, .context_index = i});
            }
            if (c.connections.stats().occupied || (c.progress.stats() && c.progress.stats()->attached) ||
                c.commands.outstanding() || (c.withdrawals.stats() && c.withdrawals.stats()->attached) ||
                domain.routes || domain.sources || domain.attempts || domain.credits.withdrawal_bindings) {
                return Error(Code::kInUse, i);
            }
            if (const auto resources = c.domain.resources(); resources) {
                if (const auto drained = resources->pool->CheckTxDrained(); !drained) {
                    if (drained.error().code == raw::JettyPoolErrorCode::kRetained) {
                        return s.FailClose({.code = Code::kRetained, .context_index = i});
                    }
                    return Error(Code::kInUse, i);
                }
            }
        }
        const auto memory = s.slab.stats();
        if (memory.available != memory.buffers || s.imports.entries() || s.imports.leases() || s.imports.in_flight()) {
            return Error(Code::kInUse);
        }
        if (const auto closed = s.runtime.Close(); !closed) {
            return s.FailClose(ProgressErrorAt(closed.error()));
        }
        s.attached = false;
        s.stage = RawStackStage::kClosingDomains;
    }
    if (s.stage == RawStackStage::kClosingDomains) {
        for (std::size_t i = s.count; i > 0; --i) {
            auto& c = s.contexts[i - 1];
            // 所有源已脱离 Reactor；关闭不会吞掉业务命令/连接/撤回责任，预检后仍由组件二次核验。
            if (const auto closed = c.commands.Close(true); !closed) {
                return s.FailClose({.code = Code::kMailbox, .context_index = i - 1, .mailbox = closed.error()});
            }
            if (const auto closed = c.connections.Close(); !closed) {
                return s.FailClose({.code = Code::kConnection, .context_index = i - 1, .connection = closed.error()});
            }
            if (const auto closed = c.withdrawals.Close(); !closed) {
                return s.FailClose(ProgressErrorAt(closed.error(), i - 1));
            }
            if (const auto closed = c.imports.Close(); !closed) {
                return s.FailClose(ProgressErrorAt(closed.error(), i - 1));
            }
            if (const auto closed = c.progress.Close(); !closed) {
                return s.FailClose(ProgressErrorAt(closed.error(), i - 1));
            }
            if (c.domain.Inspect().stage == RawDomainStage::kEmpty) {
                continue;
            }
            const auto closed = c.domain.AdvanceClose({true, proof.domains[i - 1]});
            if (!closed) {
                auto error = DomainErrorAt(closed.error(), i - 1);
                if (c.domain.Inspect().stage == RawDomainStage::kRetained) {
                    // 保留底层首个 poll/关闭错误，同时立即公布全栈保留，不能等下一次调用才反映故障。
                    error.code = Code::kRetained;
                }
                return s.FailClose(error);
            }
            if (closed->stage != RawDomainStage::kClosed) {
                return Inspect();
            }
        }
        if (const auto closed = s.imports.Close(); !closed) {
            return s.FailClose({.code = Code::kImport, .import = closed.error()});
        }
        s.stage = RawStackStage::kClosingMemory;
    }
    if (s.stage == RawStackStage::kClosingMemory) {
        if (const auto closed = s.slab.Close(); !closed) {
            return s.FailClose(
                {.code = Code::kMemory, .context_index = closed.error().context_index, .memory = closed.error()});
        }
        s.stage = RawStackStage::kClosingContexts;
    }
    if (const auto closed = s.session.Close(); !closed) {
        return s.FailClose(
            {.code = Code::kSession, .context_index = closed.error().device_index, .session_close = closed.error()});
    }
    s.stage = RawStackStage::kClosed;
    return Inspect();
}
} // namespace runtime
} // namespace kbsocket
