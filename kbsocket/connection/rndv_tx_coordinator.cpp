// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_tx_coordinator.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <utility>

#include "kbsocket/connection/rndv_protocol.hpp"

namespace kbsocket {
namespace connection {
namespace {
auto Error(RndvTxErrorCode code) noexcept {
    return std::unexpected(RndvTxError{code});
}
bool SameLane(raw::JettyLane a, raw::JettyLane b) noexcept {
    return a.pool == b.pool && a.index == b.index && a.epoch == b.epoch && a.generation == b.generation;
}
bool NotificationSucceeded(const RndvNotificationStatus& status) noexcept {
    return status.acceptance_known && status.accepted && status.retired && !status.failed && !status.cancelled;
}
bool Retryable(control::TsegImportCacheErrorCode code) noexcept {
    return code == control::TsegImportCacheErrorCode::kPending ||
           code == control::TsegImportCacheErrorCode::kResourceExhausted;
}
} // namespace
struct RndvTxCoordinator::Entry {
    RndvTxStatus status;
    GrantIdentity grant;
    std::array<WriteRndvSource, WriteRndvEngine::kMaxSources> sources{};
    std::array<memory::BufferReference, WriteRndvEngine::kMaxSources> pins{};
    std::size_t source_count = 0;
    std::uint64_t generation = 0;
    std::uint64_t started_ns = 0;
    RndvRtsReservation notifications;
    RndvNotificationStatus rts;
    RndvNotificationStatus done;
    control::TsegImportLease segment;
    WriteRndvHandle write;
    std::optional<GrantTerminalEvidence> evidence;
    bool active = false;
    bool write_started = false;
};
struct RndvTxCoordinator::State {
    RndvTxConfig config;
    WriteRndvPoolConfig write_config;
    std::uint64_t notification_id = 0;
    WriteRndvPool* writes = nullptr;
    RndvNotificationTx* notifications = nullptr;
    TxSequenceWindow* sequence = nullptr;
    memory::SlabAllocator* slab = nullptr;
    control::EndpointImportCache* endpoints = nullptr;
    control::TsegImportCache* segments = nullptr;
    control::ImportLease endpoint;
    std::unique_ptr<Entry[]> entries;
    RndvTxStats stats;
    std::uint64_t generation = 0;
    std::uint64_t last_time = 0;
    std::size_t cursor = 0;
    bool pool_bound = false;
    bool notification_bound = false;
    // 无句柄的底层回滚失败仍有责任，禁止 Close 假装已经归还全部借用。
    bool orphaned = false;
};
RndvTxCoordinator::RndvTxCoordinator() noexcept = default;
RndvTxCoordinator::~RndvTxCoordinator() {
    if (!Close()) {
        (void)state_.release();
    }
}
std::expected<void, RndvTxError> RndvTxCoordinator::Open(const RndvTxConfig& config, WriteRndvPool& writes,
                                                         RndvNotificationTx& notifications, TxSequenceWindow& sequence,
                                                         memory::SlabAllocator& slab,
                                                         control::EndpointImportCache& endpoints,
                                                         control::ImportLease endpoint,
                                                         control::TsegImportCache& segments) noexcept {
    if (state_ || opened_once_) {
        return Error(RndvTxErrorCode::kInUse);
    }
    const auto* pool = writes.config();
    const auto* notify = notifications.config();
    if (!pool || !notify) {
        return Error(RndvTxErrorCode::kNotReady);
    }
    if (!control::ValidateRndvLimits(config.limits) || !config.resolve_import || !config.timeout_ns ||
        !config.capacity || config.capacity > kMaxCapacity || config.capacity > pool->slots || notify->capacity < 2 ||
        config.capacity > config.limits.max_grants || config.limits.window != pool->window ||
        config.limits.max_write_bytes < writes.chunk_bytes_limit()) {
        return Error(RndvTxErrorCode::kInvalidArgument);
    }
    const auto& binding = pool->binding;
    const auto& route = config.grant_route;
    if (pool->connection_id == notify->connection_id || pool->endpoint != notify->endpoint ||
        !SameLane(pool->lane, notify->lane) || notify->destination.runtime != binding.receiver_runtime ||
        notify->destination.receive_domain != binding.receive_domain ||
        notify->destination.session != binding.session || notify->destination.token != binding.token ||
        route.context.version != kWireVersion || route.context.peer_runtime != binding.receiver_runtime ||
        !route.context.receive_domain || !route.context.session || !route.token.generation ||
        sequence.session() != SequenceSession{binding.receiver_runtime, binding.receive_domain, binding.session} ||
        sequence.next_sequence() < writes.stats().window_begin) {
        return Error(RndvTxErrorCode::kWrongIdentity);
    }
    const auto key = endpoints.GetKey(endpoint);
    if (!key || *key != pool->endpoint) {
        return Error(RndvTxErrorCode::kWrongIdentity);
    }
    const auto target = endpoints.Get(endpoint);
    if (!target && target.error().code != control::ImportCacheErrorCode::kPending) {
        return Error(RndvTxErrorCode::kImport);
    }
    if (sequence.stopped()) {
        return Error(RndvTxErrorCode::kStopped);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(RndvTxErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) Entry[config.capacity]);
    if (!state->entries) {
        return Error(RndvTxErrorCode::kNoMemory);
    }
    state->config = config;
    state->write_config = *pool;
    state->notification_id = notify->connection_id;
    state->writes = &writes;
    state->notifications = &notifications;
    state->sequence = &sequence;
    state->slab = &slab;
    state->endpoints = &endpoints;
    state->segments = &segments;
    const auto bound = writes.BindCoordinator(*this, slab, endpoints, segments);
    if (!bound) {
        return Error(bound.error().code == WriteRndvPoolErrorCode::kInUse ? RndvTxErrorCode::kInUse
                                                                          : RndvTxErrorCode::kWrongIdentity);
    }
    state->pool_bound = true;
    const auto notification_bound = notifications.BindCoordinator(*this, slab, endpoints, sequence, writes.sender());
    if (!notification_bound) {
        (void)writes.UnbindCoordinator(*this);
        return Error(notification_bound.error().code == RndvNotificationErrorCode::kInUse
                         ? RndvTxErrorCode::kInUse
                         : RndvTxErrorCode::kWrongIdentity);
    }
    state->notification_bound = true;
    const auto pin = endpoints.Retain(endpoint);
    if (!pin) {
        (void)notifications.UnbindCoordinator(*this);
        (void)writes.UnbindCoordinator(*this);
        return Error(RndvTxErrorCode::kImport);
    }
    state->endpoint = *pin;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<RndvTxCoordinator::Entry*, RndvTxError> RndvTxCoordinator::Find(RndvTxHandle handle) const noexcept {
    if (!state_) {
        return Error(RndvTxErrorCode::kNotReady);
    }
    if (handle.owner != this || !handle.generation || handle.slot >= state_->config.capacity) {
        return Error(RndvTxErrorCode::kInvalidHandle);
    }
    auto& entry = state_->entries[handle.slot];
    if (!entry.active || entry.generation != handle.generation) {
        return Error(RndvTxErrorCode::kInvalidHandle);
    }
    return &entry;
}
std::expected<void, RndvTxError> RndvTxCoordinator::ObserveTime(std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(RndvTxErrorCode::kNotReady);
    }
    if (now_ns < state_->last_time) {
        return Error(RndvTxErrorCode::kInvalidArgument);
    }
    state_->last_time = now_ns;
    return {};
}
bool RndvTxCoordinator::ReleaseSources(Entry& entry) noexcept {
    bool released = true;
    for (std::size_t i = 0; i < entry.source_count; ++i) {
        if (entry.pins[i].sequence) {
            if (state_->slab->Cancel(entry.pins[i])) {
                entry.pins[i] = {};
            } else {
                released = false;
            }
        }
    }
    entry.status.sources_released = released && !entry.write.owner;
    return released;
}
std::expected<RndvTxHandle, RndvTxError> RndvTxCoordinator::Submit(std::span<const WriteRndvSource> sources,
                                                                   std::uint64_t now_ns) noexcept {
    if (const auto time = ObserveTime(now_ns); !time) {
        return std::unexpected(time.error());
    }
    auto& state = *state_;
    if (state.stats.stopped || state.sequence->stopped()) {
        return Error(RndvTxErrorCode::kStopped);
    }
    if (sources.empty() || sources.size() > WriteRndvEngine::kMaxSources) {
        return Error(RndvTxErrorCode::kInvalidArgument);
    }
    if (state.generation == std::numeric_limits<std::uint64_t>::max() ||
        state.sequence->next_sequence() >= std::numeric_limits<std::uint32_t>::max()) {
        return Error(RndvTxErrorCode::kStopped);
    }
    std::uint64_t bytes = 0;
    // 先验证所有 CPU 区间，再 Reserve；同一 Lease 的多个 slice 合法。
    for (const auto& source : sources) {
        const auto span = state.slab->Readable(source.lease);
        if (!span || !source.bytes || source.offset > span->size() || source.bytes > span->size() - source.offset ||
            source.bytes > state.config.limits.max_transfer_bytes - bytes) {
            return Error(RndvTxErrorCode::kInvalidArgument);
        }
        bytes += source.bytes;
    }
    // 准入前复算实际 SGE/分块上限，避免先发布 RTS 再发现本地计划无法表达。
    const auto chunk_limit = state.writes->chunk_bytes_limit();
    const auto sge_limit = state.writes->source_reference_budget() / state.write_config.max_chunks;
    std::size_t source_index = 0, offset = 0, chunks = 0;
    while (source_index < sources.size()) {
        std::uint64_t remaining = chunk_limit;
        std::size_t sges = 0;
        while (source_index < sources.size() && remaining && sges < sge_limit) {
            const auto take = std::min<std::uint64_t>(remaining, sources[source_index].bytes - offset);
            remaining -= take;
            offset += take;
            ++sges;
            if (offset == sources[source_index].bytes) {
                ++source_index;
                offset = 0;
            }
        }
        if (++chunks > state.write_config.max_chunks) {
            return Error(RndvTxErrorCode::kInvalidArgument);
        }
    }
    const auto seq = state.sequence->next_sequence();
    const auto window = state.writes->stats().window_begin;
    if (seq < window || seq - window >= state.write_config.window ||
        bytes > state.config.limits.max_bytes - state.stats.active_bytes ||
        state.slab->stats().available_references < sources.size() + state.writes->source_reference_budget() + 1) {
        return Error(RndvTxErrorCode::kWouldBlock);
    }
    std::uint32_t slot = 0;
    while (slot < state.config.capacity && state.entries[slot].active) {
        ++slot;
    }
    if (slot == state.config.capacity) {
        return Error(RndvTxErrorCode::kWouldBlock);
    }
    auto& entry = state.entries[slot];
    entry = {};
    entry.generation = ++state.generation;
    entry.started_ns = now_ns;
    entry.source_count = sources.size();
    std::copy(sources.begin(), sources.end(), entry.sources.begin());
    entry.status.transfer = {state.write_config.binding, entry.generation, static_cast<std::uint32_t>(seq), bytes};
    const auto rollback = [&]() {
        if (!ReleaseSources(entry)) {
            entry.active = true;
            state.stats.active++;
            state.stats.active_bytes += bytes;
            state.orphaned = true;
            (void)Cancel();
            return false;
        }
        return true;
    };
    for (std::size_t i = 0; i < sources.size(); ++i) {
        const auto pin = state.slab->Reserve(sources[i].lease, memory::BufferUse::kLocalDma);
        if (!pin) {
            return Error(rollback() ? RndvTxErrorCode::kMemory : RndvTxErrorCode::kRetained);
        }
        entry.pins[i] = *pin;
    }
    const auto reserved = state.notifications->QueueRts(entry.status.transfer, *state.sequence);
    if (!reserved) {
        const bool clean = rollback();
        if (reserved.error().code == RndvNotificationErrorCode::kRetained || !clean) {
            state.orphaned = true;
            (void)Cancel();
            return Error(RndvTxErrorCode::kRetained);
        }
        return Error(reserved.error().code == RndvNotificationErrorCode::kWouldBlock ? RndvTxErrorCode::kWouldBlock
                                                                                     : RndvTxErrorCode::kNotification);
    }
    entry.notifications = *reserved;
    entry.active = true;
    state.stats.active++;
    state.stats.active_bytes += bytes;
    return RndvTxHandle{this, slot, entry.generation};
}
std::expected<void, RndvTxError> RndvTxCoordinator::OnGrant(const WireFrameView& view,
                                                            const WireReceiveContext& observed,
                                                            std::uint64_t now_ns) noexcept {
    if (const auto time = ObserveTime(now_ns); !time) {
        return time;
    }
    auto& state = *state_;
    if (state.stats.failed) {
        return Error(RndvTxErrorCode::kStopped);
    }
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        auto& entry = state.entries[i];
        if (!entry.active || entry.status.transfer.seq != view.frame.seq) {
            continue;
        }
        if (entry.status.complete || entry.status.grant_received) {
            return Error(RndvTxErrorCode::kProtocol);
        }
        const auto grant = ValidateRndvGrant(view, state.config.grant_route, observed, entry.status.transfer,
                                             state.write_config.arena);
        if (!grant || grant->slot != grant->transfer.seq % state.config.limits.window) {
            return Error(RndvTxErrorCode::kWrongIdentity);
        }
        const auto rts = state.notifications->status(entry.notifications.rts);
        if (!rts || !rts->accepted || !rts->acceptance_known || rts->failed || rts->cancelled) {
            return Error(RndvTxErrorCode::kProtocol);
        }
        entry.rts = *rts;
        entry.status.rts_accepted = true;
        entry.grant = *grant;
        entry.status.grant_received = true;
        return {};
    }
    return Error(RndvTxErrorCode::kWrongIdentity);
}
std::expected<void, RndvIngressError> RndvTxCoordinator::DispatchGrant(void* owner, const WireFrameView& view,
                                                                       const WireReceiveContext& observed,
                                                                       std::uint64_t now_ns) noexcept {
    if (!owner) {
        return std::unexpected(RndvIngressError{RndvIngressErrorCode::kInvalidArgument});
    }
    auto& coordinator = *static_cast<RndvTxCoordinator*>(owner);
    const auto accepted = coordinator.OnGrant(view, observed, now_ns);
    if (accepted) {
        return {};
    }
    if (accepted.error().code == RndvTxErrorCode::kWouldBlock) {
        return std::unexpected(RndvIngressError{RndvIngressErrorCode::kWouldBlock});
    }
    (void)coordinator.Cancel();
    return std::unexpected(RndvIngressError{RndvIngressErrorCode::kWrongIdentity});
}
std::expected<bool, RndvTxError> RndvTxCoordinator::Cleanup(Entry& entry) noexcept {
    auto& state = *state_;
    const bool cancelled = entry.status.cancelled;
    bool clean = true;
    if (entry.write.owner) {
        if (cancelled && !state.writes->Cancel(entry.write)) {
            clean = false;
        }
        const auto stats = state.writes->stats(entry.write);
        if (!stats) {
            return Error(RndvTxErrorCode::kWrite);
        }
        entry.status.write = *stats;
        entry.status.acceptance_known = entry.status.acceptance_known && stats->acceptance_known;
        if (cancelled || entry.status.done_queued) {
            const auto proof = state.writes->TerminalEvidence(entry.write);
            if (proof) {
                entry.evidence = *proof;
                if (state.writes->Release(entry.write)) {
                    entry.write = {};
                } else {
                    clean = false;
                }
            } else {
                clean = false;
            }
        } else {
            clean = false;
        }
    } else if (cancelled && entry.status.grant_received && !entry.evidence && !state.orphaned) {
        // 没有建立 WRITE 计划，唯一 owner 已停止，零接受证明来自本地状态而非网络计数。
        entry.evidence =
            GrantTerminalEvidence{.identity = entry.grant, .no_future_writes = true, .acceptance_known = true};
    }
    if ((cancelled || entry.write_started) && !ReleaseSources(entry)) {
        clean = false;
    }
    if (entry.segment.sequence && !entry.write.owner && (cancelled || entry.status.done_queued)) {
        if (state.segments->Release(entry.segment)) {
            entry.segment = {};
        } else {
            clean = false;
        }
    }
    // 通知与 WRITE 分开核销；RTS 的真实失败即使晚于 GRANT 也不能被 DONE 掩盖。
    const auto finish_notification = [&](RndvNotificationHandle& handle, RndvNotificationStatus& cached) {
        if (!handle.owner) {
            return true;
        }
        const auto before = state.notifications->status(handle);
        if (!before) {
            return false;
        }
        cached = *before;
        entry.status.acceptance_known = entry.status.acceptance_known && cached.acceptance_known;
        if (cancelled && !NotificationSucceeded(cached)) {
            if (!state.notifications->Cancel(handle)) {
                return false;
            }
            const auto after = state.notifications->status(handle);
            if (!after) {
                return false;
            }
            cached = *after;
        }
        entry.status.acceptance_known = entry.status.acceptance_known && cached.acceptance_known;
        if (!cached.acceptance_known || (!cancelled && !NotificationSucceeded(cached))) {
            return false;
        }
        if (!state.notifications->Release(handle)) {
            return false;
        }
        handle = {};
        return true;
    };
    if (cancelled || entry.status.done_queued) {
        if (!finish_notification(entry.notifications.rts, entry.rts)) {
            clean = false;
        }
        if (!finish_notification(entry.notifications.done, entry.done)) {
            clean = false;
        }
    } else {
        clean = false;
    }
    bool pins = false;
    for (std::size_t i = 0; i < entry.source_count; ++i) {
        pins = pins || entry.pins[i].sequence != 0;
    }
    entry.status.rts_accepted = entry.rts.accepted;
    entry.status.sources_released = !pins && !entry.write.owner;
    entry.status.complete = clean && entry.status.sources_released && !entry.segment.sequence &&
                            !entry.notifications.rts.owner && !entry.notifications.done.owner && !state.orphaned;
    entry.status.success = entry.status.complete && !cancelled && entry.status.done_queued &&
                           NotificationSucceeded(entry.rts) && NotificationSucceeded(entry.done);
    return entry.status.complete;
}
std::expected<bool, RndvTxError> RndvTxCoordinator::Step(Entry& entry) noexcept {
    auto& state = *state_;
    if (entry.status.cancelled) {
        return Cleanup(entry);
    }
    for (auto item :
         {std::pair{entry.notifications.rts, &entry.rts}, std::pair{entry.notifications.done, &entry.done}}) {
        if (!item.first.owner) {
            continue;
        }
        const auto status = state.notifications->status(item.first);
        if (!status) {
            return Error(RndvTxErrorCode::kNotification);
        }
        *item.second = *status;
        entry.status.acceptance_known = entry.status.acceptance_known && status->acceptance_known;
        if (status->failed || status->cancelled || !status->acceptance_known) {
            return Error(RndvTxErrorCode::kNotification);
        }
    }
    entry.status.rts_accepted = entry.rts.accepted;
    if (entry.status.done_queued) {
        return Cleanup(entry);
    }
    if (!entry.status.grant_received) {
        return false;
    }
    bool advanced = false;
    if (!entry.segment.sequence) {
        const auto request = state.config.resolve_import(state.config.import_owner, entry.grant);
        if (!request) {
            if (request.error().code == RndvTxErrorCode::kWouldBlock) {
                return false;
            }
            return std::unexpected(request.error());
        }
        const auto& descriptor = request->descriptor;
        const auto address = entry.grant.target_address;
        if (request->key != state.write_config.segment || !descriptor.ubva.va ||
            descriptor.len > std::numeric_limits<std::uint64_t>::max() - descriptor.ubva.va ||
            address < descriptor.ubva.va || address - descriptor.ubva.va > descriptor.len ||
            entry.status.transfer.bytes > descriptor.len - (address - descriptor.ubva.va)) {
            return Error(RndvTxErrorCode::kWrongIdentity);
        }
        const auto segment = state.segments->Acquire(*request);
        if (!segment) {
            return Retryable(segment.error().code) ? std::expected<bool, RndvTxError>{false}
                                                   : Error(RndvTxErrorCode::kImport);
        }
        entry.segment = *segment;
        advanced = true;
    }
    if (!entry.write.owner) {
        const auto handle = state.writes->Reserve(entry.status.transfer, entry.grant, state.endpoint, entry.segment);
        if (!handle) {
            if (handle.error().code == WriteRndvPoolErrorCode::kWouldBlock) {
                return advanced;
            }
            if (handle.error().code == WriteRndvPoolErrorCode::kRetained) {
                state.orphaned = true;
            }
            return Error(RndvTxErrorCode::kWrite);
        }
        entry.write = *handle;
        advanced = true;
    }
    if (!entry.write_started) {
        const auto started = state.writes->StartPinned(entry.write, {entry.sources.data(), entry.source_count},
                                                       {entry.pins.data(), entry.source_count});
        if (!started) {
            if (started.error().code == WriteRndvPoolErrorCode::kWouldBlock) {
                return advanced;
            }
            return Error(RndvTxErrorCode::kWrite);
        }
        // 先派生分块责任，再撤销排队 pin；即使 CPU 已归还也没有可复用的空窗。
        entry.write_started = true;
        advanced = true;
    }
    if (!ReleaseSources(entry)) {
        return Error(RndvTxErrorCode::kMemory);
    }
    const auto posted = state.writes->Progress(entry.write);
    const auto stats = state.writes->stats(entry.write);
    if (!stats) {
        return Error(RndvTxErrorCode::kWrite);
    }
    entry.status.write = *stats;
    entry.status.acceptance_known = entry.status.acceptance_known && stats->acceptance_known;
    if (stats->failed || stats->cancelled || !stats->acceptance_known) {
        return Error(RndvTxErrorCode::kWrite);
    }
    if (!posted) {
        if (posted.error().code == WriteRndvPoolErrorCode::kWouldBlock) {
            return advanced;
        }
        return Error(RndvTxErrorCode::kWrite);
    }
    advanced = advanced || *posted != 0;
    const auto proof = state.writes->DoneEvidence(entry.write);
    if (!proof) {
        return advanced;
    }
    const auto queued = state.notifications->QueueDone(entry.notifications.done, *state.writes, entry.write);
    if (!queued) {
        if (queued.error().code == RndvNotificationErrorCode::kWouldBlock) {
            return advanced;
        }
        return Error(RndvTxErrorCode::kNotification);
    }
    // DONE 已经接管成功证据，清理重试不能再次排队同一通知。
    entry.status.done_queued = true;
    entry.evidence = *proof;
    const auto cleaned = Cleanup(entry);
    if (!cleaned) {
        return std::unexpected(cleaned.error());
    }
    return true;
}
std::expected<std::size_t, RndvTxError> RndvTxCoordinator::Progress(std::uint64_t now_ns, std::size_t budget) noexcept {
    if (const auto time = ObserveTime(now_ns); !time) {
        return std::unexpected(time.error());
    }
    if (!budget || budget > kMaxBudget) {
        return Error(RndvTxErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    if (state.sequence->failed() && !state.stats.failed) {
        (void)Cancel();
    }
    std::size_t progressed = 0;
    const auto count = std::min<std::size_t>(budget, state.config.capacity);
    for (std::size_t i = 0; i < count; ++i) {
        auto& entry = state.entries[state.cursor];
        state.cursor = (state.cursor + 1) % state.config.capacity;
        if (!entry.active || entry.status.complete) {
            continue;
        }
        if (!entry.status.cancelled && now_ns - entry.started_ns >= state.config.timeout_ns) {
            (void)Cancel();
            return Error(RndvTxErrorCode::kTimeout);
        }
        const auto result = Step(entry);
        if (!result) {
            (void)Cancel();
            return std::unexpected(result.error());
        }
        progressed += *result ? 1 : 0;
    }
    return progressed;
}
std::expected<void, RndvTxError> RndvTxCoordinator::OnCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_) {
        return Error(RndvTxErrorCode::kNotReady);
    }
    if (!event.record) {
        return Error(RndvTxErrorCode::kInvalidCompletion);
    }
    auto& state = *state_;
    const auto owner = event.record->metadata.connection_id;
    if (owner == state.notification_id) {
        const auto result = state.notifications->OnCompletion(event);
        if (!result) {
            if (result.error().code == RndvNotificationErrorCode::kInvalidCompletion) {
                return Error(RndvTxErrorCode::kInvalidCompletion);
            }
            (void)Cancel();
            return Error(RndvTxErrorCode::kNotification);
        }
    } else if (owner == state.write_config.connection_id) {
        const auto result = state.writes->OnCompletion(event);
        if (!result) {
            if (result.error().code == WriteRndvPoolErrorCode::kInvalidCompletion ||
                (result.error().engine_error &&
                 result.error().engine_error->code == WriteRndvErrorCode::kInvalidCompletion)) {
                return Error(RndvTxErrorCode::kInvalidCompletion);
            }
            (void)Cancel();
            return Error(RndvTxErrorCode::kWrite);
        }
    } else {
        return Error(RndvTxErrorCode::kInvalidCompletion);
    }
    return {};
}
std::expected<RndvTxStatus, RndvTxError> RndvTxCoordinator::status(RndvTxHandle handle) const noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto result = (*found)->status;
    if ((*found)->write.owner) {
        const auto stats = state_->writes->stats((*found)->write);
        if (!stats) {
            return Error(RndvTxErrorCode::kWrite);
        }
        result.write = *stats;
        result.acceptance_known = result.acceptance_known && stats->acceptance_known;
    }
    return result;
}
std::expected<GrantTerminalEvidence, RndvTxError>
RndvTxCoordinator::TerminalEvidence(RndvTxHandle handle) const noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    const auto& entry = **found;
    if (state_->orphaned || !entry.status.grant_received) {
        return Error(RndvTxErrorCode::kRetained);
    }
    if (entry.write.owner) {
        const auto proof = state_->writes->TerminalEvidence(entry.write);
        if (!proof) {
            return Error(RndvTxErrorCode::kRetained);
        }
        return *proof;
    }
    if (!entry.evidence) {
        return Error(RndvTxErrorCode::kRetained);
    }
    return *entry.evidence;
}
std::expected<void, RndvTxError> RndvTxCoordinator::Release(RndvTxHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& entry = **found;
    if (!entry.status.complete) {
        return Error(RndvTxErrorCode::kInUse);
    }
    state_->stats.active--;
    state_->stats.active_bytes -= entry.status.transfer.bytes;
    entry.active = false;
    return {};
}
std::expected<void, RndvTxError> RndvTxCoordinator::AdvanceWindow(std::uint64_t consumed_prefix) noexcept {
    if (!state_) {
        return Error(RndvTxErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (consumed_prefix > state.sequence->next_sequence()) {
        return Error(RndvTxErrorCode::kInvalidArgument);
    }
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        const auto& entry = state.entries[i];
        if (entry.active && !entry.status.complete && entry.status.transfer.seq < consumed_prefix) {
            return Error(RndvTxErrorCode::kInUse);
        }
    }
    if (!state.writes->AdvanceWindow(consumed_prefix)) {
        return Error(RndvTxErrorCode::kWrite);
    }
    return {};
}
void RndvTxCoordinator::Stop() noexcept {
    if (state_) {
        state_->stats.stopped = true;
    }
}
std::expected<void, RndvTxError> RndvTxCoordinator::Cancel() noexcept {
    if (!state_) {
        return Error(RndvTxErrorCode::kNotReady);
    }
    auto& state = *state_;
    state.stats.stopped = true;
    state.stats.failed = true;
    state.sequence->Fail();
    bool complete = !state.orphaned;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        auto& entry = state.entries[i];
        if (!entry.active || entry.status.complete) {
            continue;
        }
        entry.status.cancelled = true;
        const auto cleaned = Cleanup(entry);
        complete = cleaned && *cleaned && complete;
    }
    return complete ? std::expected<void, RndvTxError>{} : Error(RndvTxErrorCode::kRetained);
}
std::expected<void, RndvTxError> RndvTxCoordinator::Close() noexcept {
    if (!state_) {
        return {};
    }
    auto& state = *state_;
    Stop();
    bool pending = false;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        pending = pending || (state.entries[i].active && !state.entries[i].status.complete);
    }
    if ((pending && !Cancel()) || state.orphaned) {
        return Error(RndvTxErrorCode::kRetained);
    }
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        if (state.entries[i].active && !Release({this, i, state.entries[i].generation})) {
            return Error(RndvTxErrorCode::kRetained);
        }
    }
    if (state.notification_bound) {
        if (!state.notifications->UnbindCoordinator(*this)) {
            return Error(RndvTxErrorCode::kRetained);
        }
        state.notification_bound = false;
    }
    if (state.pool_bound) {
        if (!state.writes->UnbindCoordinator(*this)) {
            return Error(RndvTxErrorCode::kRetained);
        }
        state.pool_bound = false;
    }
    if (state.endpoint.sequence) {
        if (!state.endpoints->Release(state.endpoint)) {
            return Error(RndvTxErrorCode::kRetained);
        }
        state.endpoint = {};
    }
    state_.reset();
    return {};
}
RndvTxStats RndvTxCoordinator::stats() const noexcept {
    return state_ ? state_->stats : RndvTxStats{};
}
} // namespace connection
} // namespace kbsocket
