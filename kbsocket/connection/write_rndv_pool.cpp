// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/write_rndv_pool.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace connection {
namespace {
auto Error(WriteRndvPoolErrorCode code) noexcept {
    return std::unexpected(WriteRndvPoolError{.code = code});
}
auto EngineError(WriteRndvError error) noexcept {
    const auto code = error.code == WriteRndvErrorCode::kAcceptanceUnknown ? WriteRndvPoolErrorCode::kAcceptanceUnknown
                      : error.code == WriteRndvErrorCode::kWouldBlock      ? WriteRndvPoolErrorCode::kWouldBlock
                      : error.code == WriteRndvErrorCode::kInUse           ? WriteRndvPoolErrorCode::kInUse
                                                                           : WriteRndvPoolErrorCode::kEngine;
    return std::unexpected(WriteRndvPoolError{.code = code, .engine_error = error});
}
auto EndpointError(control::ImportCacheError error) noexcept {
    return std::unexpected(
        WriteRndvPoolError{.code = error.code == control::ImportCacheErrorCode::kPending ||
                                           error.code == control::ImportCacheErrorCode::kResourceExhausted
                                       ? WriteRndvPoolErrorCode::kWouldBlock
                                       : WriteRndvPoolErrorCode::kEndpointCache,
                           .endpoint_error = error});
}
auto SegmentError(control::TsegImportCacheError error) noexcept {
    return std::unexpected(
        WriteRndvPoolError{.code = error.code == control::TsegImportCacheErrorCode::kPending ||
                                           error.code == control::TsegImportCacheErrorCode::kResourceExhausted
                                       ? WriteRndvPoolErrorCode::kWouldBlock
                                       : WriteRndvPoolErrorCode::kSegmentCache,
                           .segment_error = error});
}
bool ValidBinding(const GrantBinding& binding) noexcept {
    return binding.sender_runtime && binding.receiver_runtime && binding.receive_domain && binding.session &&
           binding.token.generation;
}
bool ValidConfig(const WriteRndvPoolConfig& config, raw::TxSender& sender) noexcept {
    const auto& endpoint = config.endpoint;
    const auto& segment = config.segment;
    return config.connection_id && ValidBinding(config.binding) && config.arena.id && config.arena.generation &&
           config.slots && config.slots <= WriteRndvPool::kMaxSlots && config.window &&
           config.window <= GrantManager::kMaxWindow && config.slots <= config.window &&
           config.initial_seq != std::numeric_limits<std::uint32_t>::max() && config.max_chunks &&
           config.max_chunks <= WriteRndvEngine::kMaxChunks &&
           config.slots <= WriteRndvPool::kMaxTotalChunks / config.max_chunks && endpoint.local_context &&
           endpoint.local_context == sender.context() && endpoint.local_context == segment.local_context &&
           endpoint.local_context_incarnation &&
           endpoint.local_context_incarnation == segment.local_context_incarnation &&
           endpoint.peer_runtime == config.binding.receiver_runtime && endpoint.peer_runtime == segment.peer_runtime &&
           endpoint.receive_domain == config.binding.receive_domain &&
           endpoint.receive_domain == segment.receive_domain && endpoint.endpoint_generation &&
           endpoint.authorization_identity && endpoint.authorization_identity == segment.authorization_identity &&
           endpoint.eid == segment.eid &&
           std::any_of(endpoint.eid.begin(), endpoint.eid.end(), [](auto byte) { return byte != 0; }) &&
           endpoint.transport_mode == URMA_TM_RM && endpoint.tp_type == URMA_CTP &&
           segment.slab_id == config.arena.id && segment.slab_generation == config.arena.generation;
}
} // namespace

struct WriteRndvPool::Slot {
    WriteRndvEngine engine;
    GrantIdentity identity;
    control::ImportLease endpoint;
    control::TsegImportLease segment;
    std::uint64_t generation = 0;
    bool active = false;
    bool started = false;
    bool cancelled = false;
    bool releasing = false;
};
struct WriteRndvPool::State {
    enum class WindowPhase : std::uint8_t { kEmpty, kActive, kRetired };
    WriteRndvPoolConfig config;
    control::EndpointImportCache* endpoints = nullptr;
    control::TsegImportCache* segments = nullptr;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<WindowPhase[]> window;
    // 即使 SN tombstone 已推进，也拒绝同一远端 Grant 槽的旧代次换 SN 重放。
    std::array<std::uint64_t, GrantManager::kMaxWindow> grant_generations{};
    std::uint64_t next_generation = 0;
    std::uint64_t window_begin = 0;
    std::size_t active = 0;
    std::size_t retired = 0;
    bool stopped = false;
};

WriteRndvPool::WriteRndvPool() noexcept = default;
WriteRndvPool::~WriteRndvPool() {
    if (!Close()) {
        // 未终结事务及其独立缓存票据必须一起保留；宿主仍负责外部依赖。
        (void)state_.release();
    }
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::Open(const WriteRndvPoolConfig& config, raw::TxSender& sender,
                                                            memory::SlabAllocator& slab,
                                                            control::EndpointImportCache& endpoints,
                                                            control::TsegImportCache& segments) noexcept {
    if (state_ || opened_once_) {
        return Error(WriteRndvPoolErrorCode::kInUse);
    }
    if (!ValidConfig(config, sender)) {
        return Error(WriteRndvPoolErrorCode::kInvalidArgument);
    }
    if (const auto ready =
            endpoints.NextOperation(config.endpoint.local_context, config.endpoint.local_context_incarnation);
        !ready) {
        return EndpointError(ready.error());
    }
    if (const auto ready =
            segments.NextOperation(config.segment.local_context, config.segment.local_context_incarnation);
        !ready) {
        return SegmentError(ready.error());
    }
    std::unique_ptr<State> state(new (std::nothrow) State);
    if (!state) {
        return Error(WriteRndvPoolErrorCode::kNoMemory);
    }
    state->slots.reset(new (std::nothrow) Slot[config.slots]);
    state->window.reset(new (std::nothrow) State::WindowPhase[config.window]{});
    if (!state->slots || !state->window) {
        return Error(WriteRndvPoolErrorCode::kNoMemory);
    }
    for (std::uint32_t i = 0; i < config.slots; ++i) {
        WriteRndvConfig engine_config{.connection_id = config.connection_id,
                                      .transaction_generation = i + 1,
                                      .lane = config.lane,
                                      .chunk_bytes = config.chunk_bytes,
                                      .max_chunks = config.max_chunks,
                                      .post_budget = config.post_budget};
        if (const auto opened = state->slots[i].engine.Open(engine_config, sender, slab); !opened) {
            return EngineError(opened.error());
        }
    }
    state->config = config;
    state->endpoints = &endpoints;
    state->segments = &segments;
    state->next_generation = config.slots;
    state->window_begin = config.initial_seq;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<WriteRndvPool::Slot*, WriteRndvPoolError> WriteRndvPool::Find(WriteRndvHandle handle) const noexcept {
    if (!state_) {
        return Error(WriteRndvPoolErrorCode::kNotReady);
    }
    if (handle.owner != this || !handle.generation || handle.slot >= state_->config.slots) {
        return Error(WriteRndvPoolErrorCode::kInvalidHandle);
    }
    auto& slot = state_->slots[handle.slot];
    if (!slot.active || slot.generation != handle.generation) {
        return Error(WriteRndvPoolErrorCode::kInvalidHandle);
    }
    return &slot;
}
std::expected<WriteRndvDestination, WriteRndvPoolError>
WriteRndvPool::Destination(const GrantIdentity& grant, control::ImportLease endpoint,
                           control::TsegImportLease segment) const noexcept {
    auto& state = *state_;
    const auto endpoint_key = state.endpoints->GetKey(endpoint);
    if (!endpoint_key) {
        return EndpointError(endpoint_key.error());
    }
    const auto segment_key = state.segments->GetKey(segment);
    if (!segment_key) {
        return SegmentError(segment_key.error());
    }
    if (*endpoint_key != state.config.endpoint || *segment_key != state.config.segment) {
        return Error(WriteRndvPoolErrorCode::kWrongIdentity);
    }
    const auto jetty = state.endpoints->Get(endpoint);
    if (!jetty) {
        return EndpointError(jetty.error());
    }
    const auto target = state.segments->Get(segment);
    if (!target) {
        return SegmentError(target.error());
    }
    const auto& key = state.config.endpoint;
    // 缓存键不能代替 provider 实际返回对象的身份校验，尤其是 EndpointImportCache 的裸 target。
    if (!*jetty || !*target || (*jetty)->urma_ctx != key.local_context || (*target)->urma_ctx != key.local_context ||
        (*jetty)->id.id != key.jetty_id || std::memcmp((*jetty)->id.eid.raw, key.eid.data(), key.eid.size()) ||
        (*jetty)->trans_mode != key.transport_mode || (*jetty)->tp_type != key.tp_type ||
        (*jetty)->type != URMA_JETTY || std::memcmp((*target)->seg.ubva.eid.raw, key.eid.data(), key.eid.size())) {
        return Error(WriteRndvPoolErrorCode::kWrongIdentity);
    }
    const auto& descriptor = (*target)->seg;
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (!descriptor.ubva.va || !descriptor.len || descriptor.len > maximum - descriptor.ubva.va ||
        !(descriptor.attr.bs.access & URMA_ACCESS_WRITE) || (descriptor.attr.bs.access & URMA_ACCESS_LOCAL_ONLY) ||
        !grant.target_address || !grant.transfer.bytes || grant.transfer.bytes > maximum - grant.target_address ||
        grant.target_address < descriptor.ubva.va || grant.target_address - descriptor.ubva.va > descriptor.len ||
        grant.transfer.bytes > descriptor.len - (grant.target_address - descriptor.ubva.va)) {
        return Error(WriteRndvPoolErrorCode::kInvalidArgument);
    }
    return WriteRndvDestination{grant, *jetty, *target};
}
std::expected<WriteRndvHandle, WriteRndvPoolError> WriteRndvPool::Reserve(const GrantTransfer& expected_transfer,
                                                                          const GrantIdentity& grant,
                                                                          control::ImportLease endpoint,
                                                                          control::TsegImportLease segment) noexcept {
    if (!state_) {
        return Error(WriteRndvPoolErrorCode::kNotReady);
    }
    auto& state = *state_;
    const auto& config = state.config;
    if (state.stopped) {
        return Error(WriteRndvPoolErrorCode::kStopped);
    }
    if (expected_transfer.binding != config.binding || grant.transfer != expected_transfer ||
        grant.arena != config.arena) {
        return Error(WriteRndvPoolErrorCode::kWrongIdentity);
    }
    if (!expected_transfer.transfer_id || !expected_transfer.bytes ||
        expected_transfer.seq == std::numeric_limits<std::uint32_t>::max() || !grant.generation ||
        grant.slot >= GrantManager::kMaxWindow) {
        return Error(WriteRndvPoolErrorCode::kInvalidArgument);
    }
    if (expected_transfer.seq < state.window_begin) {
        return Error(WriteRndvPoolErrorCode::kStaleSequence);
    }
    if (expected_transfer.seq - state.window_begin >= config.window) {
        return Error(WriteRndvPoolErrorCode::kWouldBlock);
    }
    if (state.window[expected_transfer.seq % config.window] != State::WindowPhase::kEmpty ||
        grant.generation <= state.grant_generations[grant.slot]) {
        return Error(WriteRndvPoolErrorCode::kDuplicate);
    }
    const auto destination = Destination(grant, endpoint, segment);
    if (!destination) {
        return std::unexpected(destination.error());
    }
    std::uint32_t index = config.slots;
    for (std::uint32_t i = 0; i < config.slots; ++i) {
        const auto& slot = state.slots[i];
        if (!slot.active) {
            if (index == config.slots) {
                index = i;
            }
            continue;
        }
        if (slot.identity.slot == grant.slot ||
            (grant.target_address < slot.identity.target_address + slot.identity.transfer.bytes &&
             slot.identity.target_address < grant.target_address + grant.transfer.bytes)) {
            return Error(WriteRndvPoolErrorCode::kDuplicate);
        }
    }
    if (index == config.slots) {
        return Error(WriteRndvPoolErrorCode::kWouldBlock);
    }
    if (state.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(WriteRndvPoolErrorCode::kSequenceExhausted);
    }
    auto& slot = state.slots[index];
    const auto generation = ++state.next_generation;
    if (const auto recycled = slot.engine.Recycle(generation); !recycled) {
        return EngineError(recycled.error());
    }
    const auto endpoint_pin = state.endpoints->Retain(endpoint);
    if (!endpoint_pin) {
        return EndpointError(endpoint_pin.error());
    }
    const auto segment_pin = state.segments->Retain(segment);
    if (!segment_pin) {
        const auto released = state.endpoints->Release(*endpoint_pin);
        if (released) {
            return SegmentError(segment_pin.error());
        }
        // 理论上只能由缓存账本损坏造成：不能丢失已取得的 pin，保留内部槽供 Close 重试。
        slot.identity = grant;
        slot.endpoint = *endpoint_pin;
        slot.segment = {};
        slot.generation = generation;
        slot.active = true;
        slot.started = false;
        slot.cancelled = true;
        slot.releasing = false;
        state.window[expected_transfer.seq % config.window] = State::WindowPhase::kActive;
        state.grant_generations[grant.slot] = grant.generation;
        ++state.active;
        return Error(WriteRndvPoolErrorCode::kRetained);
    }
    slot.identity = grant;
    slot.endpoint = *endpoint_pin;
    slot.segment = *segment_pin;
    slot.generation = generation;
    slot.active = true;
    slot.started = false;
    slot.cancelled = false;
    slot.releasing = false;
    state.window[expected_transfer.seq % config.window] = State::WindowPhase::kActive;
    state.grant_generations[grant.slot] = grant.generation;
    ++state.active;
    return WriteRndvHandle{this, index, generation};
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::Start(WriteRndvHandle handle,
                                                             std::span<const WriteRndvSource> sources) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (slot.cancelled || slot.releasing) {
        return Error(WriteRndvPoolErrorCode::kStopped);
    }
    if (slot.started) {
        return Error(WriteRndvPoolErrorCode::kInUse);
    }
    const auto destination = Destination(slot.identity, slot.endpoint, slot.segment);
    if (!destination) {
        if (const auto cancelled = Cancel(handle); !cancelled) {
            return cancelled;
        }
        return std::unexpected(destination.error());
    }
    const auto started = slot.engine.Start(slot.identity.transfer, *destination, sources);
    if (!started) {
        return EngineError(started.error());
    }
    slot.started = true;
    return {};
}
std::expected<std::size_t, WriteRndvPoolError> WriteRndvPool::Progress(WriteRndvHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (slot.cancelled || slot.releasing) {
        return Error(WriteRndvPoolErrorCode::kStopped);
    }
    if (!slot.started) {
        return Error(WriteRndvPoolErrorCode::kNotReady);
    }
    if (slot.engine.stats().queued) {
        if (const auto destination = Destination(slot.identity, slot.endpoint, slot.segment); !destination) {
            if (const auto cancelled = Cancel(handle); !cancelled) {
                return std::unexpected(cancelled.error());
            }
            return std::unexpected(destination.error());
        }
    }
    const auto progressed = slot.engine.Progress();
    if (!progressed) {
        return EngineError(progressed.error());
    }
    return *progressed;
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::OnCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_) {
        return Error(WriteRndvPoolErrorCode::kNotReady);
    }
    if (!event.record || event.record->metadata.connection_id != state_->config.connection_id ||
        !event.record->metadata.operation_generation) {
        return Error(WriteRndvPoolErrorCode::kInvalidCompletion);
    }
    // 必须先按完整事务代次路由；operation_id 只是当前引擎内的分块索引。
    for (std::uint32_t i = 0; i < state_->config.slots; ++i) {
        auto& slot = state_->slots[i];
        if (!slot.active || !slot.started || slot.generation != event.record->metadata.operation_generation) {
            continue;
        }
        const auto completed = slot.engine.OnCompletion(event);
        if (!completed) {
            return EngineError(completed.error());
        }
        return {};
    }
    return Error(WriteRndvPoolErrorCode::kInvalidCompletion);
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::Cancel(WriteRndvHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    slot.cancelled = true;
    const auto cancelled = slot.engine.Cancel();
    if (!cancelled) {
        return EngineError(cancelled.error());
    }
    return {};
}
std::expected<GrantTerminalEvidence, WriteRndvPoolError>
WriteRndvPool::TerminalEvidence(WriteRndvHandle handle) const noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    const auto& slot = **found;
    if (slot.started) {
        const auto evidence = slot.engine.TerminalEvidence();
        if (!evidence) {
            return EngineError(evidence.error());
        }
        return *evidence;
    }
    if (!slot.cancelled) {
        return Error(WriteRndvPoolErrorCode::kInUse);
    }
    // 未 Start 的取消可证明没有 WRITE，但部分源引用回滚仍须实际完成。
    if (const auto cleared = slot.engine.CanRecycle(); !cleared) {
        return EngineError(cleared.error());
    }
    return GrantTerminalEvidence{.identity = slot.identity, .no_future_writes = true, .acceptance_known = true};
}
std::expected<GrantTerminalEvidence, WriteRndvPoolError>
WriteRndvPool::DoneEvidence(WriteRndvHandle handle) const noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    const auto& slot = **found;
    if (!slot.started || slot.cancelled) {
        return Error(WriteRndvPoolErrorCode::kStopped);
    }
    const auto evidence = slot.engine.DoneEvidence();
    if (!evidence) {
        return EngineError(evidence.error());
    }
    return *evidence;
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::Release(WriteRndvHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (!slot.engine.stats().acceptance_known) {
        return Error(WriteRndvPoolErrorCode::kAcceptanceUnknown);
    }
    if (const auto completed = slot.engine.RetryCompleted(); !completed) {
        return EngineError(completed.error());
    }
    if (const auto evidence = TerminalEvidence(handle); !evidence) {
        return std::unexpected(evidence.error());
    }
    if (const auto cleared = slot.engine.CanRecycle(); !cleared) {
        return EngineError(cleared.error());
    }
    slot.releasing = true;
    // 保存每一步的释放进度，下一次不能重复释放已归还票据。
    if (slot.segment.sequence) {
        if (const auto released = state_->segments->Release(slot.segment); !released) {
            return SegmentError(released.error());
        }
        slot.segment = {};
    }
    if (slot.endpoint.sequence) {
        if (const auto released = state_->endpoints->Release(slot.endpoint); !released) {
            return EndpointError(released.error());
        }
        slot.endpoint = {};
    }
    state_->window[slot.identity.transfer.seq % state_->config.window] = State::WindowPhase::kRetired;
    ++state_->retired;
    --state_->active;
    slot.active = false;
    return {};
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::AdvanceWindow(std::uint64_t consumed_prefix) noexcept {
    if (!state_) {
        return Error(WriteRndvPoolErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (consumed_prefix < state.window_begin || consumed_prefix > std::numeric_limits<std::uint32_t>::max() ||
        consumed_prefix - state.window_begin > state.config.window) {
        return Error(WriteRndvPoolErrorCode::kInvalidArgument);
    }
    for (auto seq = state.window_begin; seq < consumed_prefix; ++seq) {
        if (state.window[seq % state.config.window] == State::WindowPhase::kActive) {
            return Error(WriteRndvPoolErrorCode::kInUse);
        }
    }
    for (auto seq = state.window_begin; seq < consumed_prefix; ++seq) {
        auto& phase = state.window[seq % state.config.window];
        if (phase == State::WindowPhase::kRetired) {
            --state.retired;
        }
        phase = State::WindowPhase::kEmpty;
    }
    state.window_begin = consumed_prefix;
    return {};
}
void WriteRndvPool::Stop() noexcept {
    if (state_) {
        state_->stopped = true;
    }
}
std::expected<void, WriteRndvPoolError> WriteRndvPool::Close() noexcept {
    if (!state_) {
        return {};
    }
    Stop();
    std::optional<WriteRndvPoolError> failure;
    for (std::uint32_t i = 0; i < state_->config.slots; ++i) {
        auto& slot = state_->slots[i];
        if (!slot.active) {
            continue;
        }
        const WriteRndvHandle handle{this, i, slot.generation};
        if (const auto cancelled = Cancel(handle); !cancelled && !failure) {
            failure = cancelled.error();
        }
        if (const auto released = Release(handle); !released && !failure) {
            failure = released.error();
        }
    }
    if (failure) {
        return std::unexpected(*failure);
    }
    if (state_->active) {
        return Error(WriteRndvPoolErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
std::expected<WriteRndvStats, WriteRndvPoolError> WriteRndvPool::stats(WriteRndvHandle handle) const noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    return (**found).engine.stats();
}
WriteRndvPoolStats WriteRndvPool::stats() const noexcept {
    if (!state_) {
        return {};
    }
    WriteRndvPoolStats result{
        .window_begin = state_->window_begin, .active = state_->active, .retired = state_->retired};
    for (std::uint32_t i = 0; i < state_->config.slots; ++i) {
        const auto& slot = state_->slots[i];
        if (slot.active) {
            if (slot.started) {
                ++result.started;
            } else {
                ++result.reserved;
            }
        }
    }
    return result;
}
} // namespace connection
} // namespace kbsocket
