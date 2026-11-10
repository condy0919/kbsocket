// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_connection.hpp"

#include <algorithm>
#include <array>
#include <new>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/rndv_notification_tx.hpp"
#include "kbsocket/connection/rndv_rx_ingress.hpp"
#include "kbsocket/connection/rndv_tx_coordinator.hpp"
#include "kbsocket/connection/write_rndv_pool.hpp"
#include "kbsocket/control/rndv_arena.hpp"

namespace kbsocket {
namespace connection {
namespace {
auto Error(RndvConnectionError code) noexcept {
    return std::unexpected(code);
}
RndvConnectionError TxError(const RndvTxError& error) noexcept {
    switch (error.code) {
    case RndvTxErrorCode::kWouldBlock:
        return RndvConnectionError::kWouldBlock;
    case RndvTxErrorCode::kInvalidArgument:
        return RndvConnectionError::kInvalidArgument;
    case RndvTxErrorCode::kInvalidCompletion:
        return RndvConnectionError::kInvalidCompletion;
    case RndvTxErrorCode::kRetained:
        return RndvConnectionError::kRetained;
    default:
        return RndvConnectionError::kTx;
    }
}
} // namespace
struct RndvConnection::State {
    RndvConnectionConfig config;
    control::RndvLimits limits;
    control::ConnectionHandshake* handshake = nullptr;
    EagerTxStream* eager = nullptr;
    memory::SlabAllocator* source = nullptr;
    SenderCreditAccount* credits = nullptr;
    GrantManager grants;
    RndvReceiver receiver;
    WriteRndvPool writes;
    RndvNotificationTx notifications;
    RndvTxCoordinator tx;
    RndvRxIngress ingress;
    std::array<RndvTxHandle, RndvTxCoordinator::kMaxCapacity> handles{};
    memory::BufferLease retained_copy;
    std::uint64_t owner = 0;
    std::uint64_t consumed = 0;
    std::uint64_t completions = 0;
    std::uint64_t now = 0;
    std::size_t copy_capacity = 0;
    bool tx_open = false;
    bool ingress_open = false;
    bool receiver_open = false;
    bool stopped = false;
    static std::expected<control::TsegImportRequest, RndvTxError> Resolve(void* owner,
                                                                          const GrantIdentity& grant) noexcept {
        const auto result = static_cast<State*>(owner)->handshake->PrepareRndvImport(grant);
        if (!result) {
            return std::unexpected(RndvTxError{RndvTxErrorCode::kImport});
        }
        return *result;
    }
};
RndvConnection::RndvConnection() noexcept = default;
RndvConnection::~RndvConnection() {
    if (!Close()) {
        (void)state_.release();
    }
}
bool RndvConnection::ValidConfig(const RndvConnectionConfig& config) noexcept {
    return config.tx_capacity && config.tx_capacity <= RndvTxCoordinator::kMaxCapacity && config.receive_capacity &&
           config.receive_capacity <= RndvReceiver::kMaxCapacity &&
           config.notification_capacity >= 2 * config.tx_capacity + 1 &&
           config.notification_capacity <= RndvNotificationTx::kMaxCapacity && config.ingress_packets &&
           config.ingress_packets <= RndvRxIngress::kMaxCapacity && config.max_chunks &&
           config.max_chunks <= WriteRndvPool::kMaxTotalChunks / config.tx_capacity && config.post_budget &&
           config.post_budget <= raw::TxSender::kMaxBatch && config.chunk_bytes &&
           config.chunk_bytes <= raw::TxSender::kMaxWriteSize && config.threshold && config.timeout_ns &&
           config.progress_budget && config.progress_budget <= RndvTxCoordinator::kMaxBudget;
}
std::expected<void, RndvConnectionError>
RndvConnection::Open(const RndvConnectionConfig& config, const RndvConnectionPreparation& prep,
                     control::ConnectionHandshake& handshake, EagerTxStream& eager, EagerReceiveDomain& receive,
                     raw::TxSender& sender, memory::SlabAllocator& source, SenderCreditAccount& credits,
                     control::EndpointImportCache& endpoints, std::uint64_t connection_id) noexcept {
    const auto* limits = handshake.negotiated_rndv();
    const auto* peer = handshake.peer();
    const auto& local = handshake.local();
    if (state_ || !ValidConfig(config) || !limits || !peer || !prep.target_slab || !prep.segments || !connection_id ||
        (connection_id & kOwnerMask) || !eager.sequence_window() || local.rndv.arena_count != 1 ||
        peer->rndv.arena_count != 1 || config.tx_capacity > limits->max_grants ||
        config.receive_capacity < limits->max_grants || config.ingress_packets < limits->max_grants ||
        local.rndv.arenas[0].max_grant_bytes < limits->max_transfer_bytes ||
        peer->rndv.arenas[0].max_grant_bytes < limits->max_transfer_bytes) {
        return Error(RndvConnectionError::kInvalidArgument);
    }
    const auto& arena = local.rndv.arenas[0];
    const auto& remote = peer->rndv.arenas[0];
    const auto exported = control::ExportRndvArena(*prep.target_slab, receive.context(), {arena.id, arena.generation});
    const auto endpoint = endpoints.GetKey(handshake.import_lease());
    const auto snapshot = source.ExportSegment(receive.context());
    const auto chunk = std::min(config.chunk_bytes, limits->max_write_bytes);
    if (!exported || *exported != arena || !endpoint || !snapshot ||
        (limits->max_transfer_bytes - 1) / chunk + 1 > config.max_chunks) {
        return Error(RndvConnectionError::kInvalidArgument);
    }
    state_.reset(new (std::nothrow) State);
    if (!state_) {
        return Error(RndvConnectionError::kMemory);
    }
    auto& state = *state_;
    state.config = config;
    state.limits = *limits;
    state.handshake = &handshake;
    state.eager = &eager;
    state.source = &source;
    state.credits = &credits;
    state.copy_capacity = snapshot->buffer_size;
    state.owner = connection_id;
    const GrantBinding incoming{peer->identity.runtime, local.identity.runtime, local.identity.receive_domain,
                                local.identity.session, local.identity.token};
    const WireSessionBinding route{
        {local.wire_version, peer->identity.runtime, local.identity.receive_domain, local.identity.session},
        local.identity.token,
        peer->credit_epoch};
    if (!state.grants.Open(
            incoming, {arena.id, arena.generation}, *prep.target_slab, receive.context(),
            {.window = limits->window, .max_active = limits->max_grants, .max_bytes = limits->max_bytes})) {
        return Error(RndvConnectionError::kMemory);
    }
    if (!state.receiver.Open({.binding = incoming,
                              .route = route,
                              .capacity = config.receive_capacity,
                              .peer_successful_write_done = true},
                             state.grants, handshake.receive_window())) {
        return Error(RndvConnectionError::kRx);
    }
    state.receiver_open = true;
    control::TsegImportKey segment{.local_context = endpoint->local_context,
                                   .local_context_incarnation = endpoint->local_context_incarnation,
                                   .peer_runtime = endpoint->peer_runtime,
                                   .receive_domain = endpoint->receive_domain,
                                   .slab_id = remote.id,
                                   .slab_generation = remote.generation,
                                   .authorization_identity = endpoint->authorization_identity};
    for (std::size_t i = 0; i < remote.eid.size(); ++i) {
        segment.eid[i] = std::to_integer<std::uint8_t>(remote.eid[i]);
    }
    if (!state.writes.Open({.connection_id = connection_id | kWriteOwner,
                            .binding = {local.identity.runtime, peer->identity.runtime, peer->identity.receive_domain,
                                        peer->identity.session, peer->identity.token},
                            .arena = {remote.id, remote.generation},
                            .endpoint = *endpoint,
                            .segment = segment,
                            .lane = prep.lane,
                            .slots = config.tx_capacity,
                            .window = limits->window,
                            .max_chunks = config.max_chunks,
                            .post_budget = config.post_budget,
                            .chunk_bytes = chunk},
                           sender, source, endpoints, *prep.segments)) {
        return Error(RndvConnectionError::kTx);
    }
    if (!state.notifications.Open(
            {.connection_id = connection_id | kNotificationOwner,
             .destination = {peer->identity.runtime, peer->identity.receive_domain, peer->identity.session,
                             peer->identity.token},
             .credit_identity = {peer->identity.runtime, peer->identity.receive_domain, peer->credit_epoch},
             .endpoint = *endpoint,
             .lane = prep.lane,
             .capacity = config.notification_capacity},
            sender, source, endpoints, handshake.import_lease(), credits)) {
        return Error(RndvConnectionError::kTx);
    }
    if (!state.tx.Open({.grant_route = route,
                        .limits = *limits,
                        .capacity = config.tx_capacity,
                        .timeout_ns = config.timeout_ns,
                        .resolve_import = &State::Resolve,
                        .import_owner = &state},
                       state.writes, state.notifications, *eager.sequence_window(), source, endpoints,
                       handshake.import_lease(), *prep.segments)) {
        return Error(RndvConnectionError::kTx);
    }
    state.tx_open = true;
    if (!state.ingress.Open({.route = route,
                             .rts_packets = config.ingress_packets,
                             .grant_packets = config.ingress_packets,
                             .done_packets = config.ingress_packets,
                             .grant_jobs = config.receive_capacity,
                             .gap_timeout_ns = config.timeout_ns,
                             .on_grant = &RndvTxCoordinator::DispatchGrant,
                             .grant_owner = &state.tx},
                            receive, state.receiver, state.grants, handshake.receive_window(), state.notifications)) {
        return Error(RndvConnectionError::kRx);
    }
    state.ingress_open = true;
    return {};
}
RndvReceiver* RndvConnection::receiver() noexcept {
    return state_ && state_->receiver_open ? &state_->receiver : nullptr;
}
bool RndvConnection::Owns(std::uint64_t owner) const noexcept {
    return state_ && state_->tx_open &&
           (owner == (state_->owner | kWriteOwner) || owner == (state_->owner | kNotificationOwner));
}
bool RndvConnection::pending_sequence() const noexcept {
    return state_ && state_->eager->sequence_window() && !state_->eager->sequence_window()->pending().empty();
}
std::expected<std::size_t, RndvConnectionError> RndvConnection::WriteLease(memory::BufferLease source,
                                                                           std::size_t offset, std::size_t bytes,
                                                                           std::uint64_t now_ns) noexcept {
    if (!state_ || !state_->tx_open || state_->stopped) {
        return Error(RndvConnectionError::kNotReady);
    }
    auto& state = *state_;
    if (!bytes) {
        return 0;
    }
    // 未来 DONE 之外保留反向 GRANT 与最终 CREDIT 的进度额度。
    // 当前 wire 不公告对端控制补投低水线；Peer 共账只允许一个未 Activate 的承诺，
    // 避免多个未来 DONE 使实际空闲为一、但对端尚未达到补投低水线的双向死锁。
    if (state.credits->available(CreditClass::kControl) < 3 || state.credits->stats().promised_control != 0 ||
        !state.eager->can_start_rndv() ||
        state.eager->sequence_window()->next_sequence() >= state.writes.stats().window_begin + state.limits.window) {
        return Error(RndvConnectionError::kWouldBlock);
    }
    const auto slot = std::find_if(state.handles.begin(), state.handles.begin() + state.config.tx_capacity,
                                   [](const auto& handle) { return !handle.owner; });
    if (slot == state.handles.begin() + state.config.tx_capacity) {
        return Error(RndvConnectionError::kWouldBlock);
    }
    const auto count = std::min<std::uint64_t>(bytes, state.limits.max_transfer_bytes);
    const WriteRndvSource slice{source, offset, static_cast<std::size_t>(count)};
    state.now = std::max(state.now, now_ns);
    const auto submitted = state.tx.Submit({&slice, 1}, state.now);
    if (!submitted) {
        return Error(TxError(submitted.error()));
    }
    *slot = *submitted;
    return count;
}
std::expected<std::size_t, RndvConnectionError> RndvConnection::Write(std::span<const std::byte> input,
                                                                      std::uint64_t now_ns) noexcept {
    if (!state_ || state_->stopped) {
        return Error(RndvConnectionError::kNotReady);
    }
    auto& state = *state_;
    if (!state.eager->can_start_rndv()) {
        return Error(RndvConnectionError::kWouldBlock);
    }
    const auto bytes =
        std::min({input.size(), state.copy_capacity, static_cast<std::size_t>(state.limits.max_transfer_bytes)});
    if (!bytes) {
        return 0;
    }
    const auto copy = state.source->Copy(input.first(bytes));
    if (!copy) {
        return Error(RndvConnectionError::kWouldBlock);
    }
    const auto result = WriteLease(*copy, 0, bytes, now_ns);
    if (!state.source->ReleaseCpu(*copy)) {
        state.retained_copy = *copy;
        Cancel();
        return Error(RndvConnectionError::kRetained);
    }
    return result;
}
std::expected<void, RndvConnectionError> RndvConnection::Accept(EagerRxPacket packet) noexcept {
    if (!state_ || !state_->ingress_open) {
        return Error(RndvConnectionError::kNotReady);
    }
    if (!state_->ingress.Accept(packet)) {
        return Error(RndvConnectionError::kRx);
    }
    return {};
}
std::expected<void, RndvConnectionError> RndvConnection::OnCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_ || !state_->tx_open) {
        return Error(RndvConnectionError::kInvalidCompletion);
    }
    const auto result = state_->tx.OnCompletion(event);
    if (!result && result.error().code == RndvTxErrorCode::kInvalidCompletion) {
        return Error(RndvConnectionError::kInvalidCompletion);
    }
    ++state_->completions;
    if (!result) {
        return Error(TxError(result.error()));
    }
    return {};
}
std::expected<std::size_t, RndvConnectionError> RndvConnection::Progress(std::uint64_t now_ns, bool active) noexcept {
    if (!state_) {
        return 0;
    }
    auto& state = *state_;
    state.now = std::max(state.now, now_ns);
    if (!active) {
        Cancel();
    }
    std::size_t count = 0;
    bool failed = false;
    if (state.ingress_open) {
        const auto rx = state.ingress.Progress(state.now, state.config.progress_budget);
        if (rx) {
            count += *rx;
        } else {
            failed = true;
        }
    }
    if (state.tx_open) {
        const auto tx = state.tx.Progress(state.now, state.config.progress_budget);
        if (tx) {
            count += *tx;
        } else {
            failed = true;
        }
        for (std::uint32_t i = 0; i < state.config.tx_capacity; ++i) {
            auto& handle = state.handles[i];
            if (!handle.owner) {
                continue;
            }
            const auto status = state.tx.status(handle);
            if (!status) {
                failed = true;
                continue;
            }
            if (status->complete) {
                if (state.tx.Release(handle)) {
                    handle = {};
                    ++count;
                } else {
                    failed = true;
                }
            }
        }
        if (active && !failed && state.consumed > state.writes.stats().window_begin) {
            const auto end = std::min(state.consumed, state.writes.stats().window_begin + state.limits.window);
            const auto advance = state.tx.AdvanceWindow(end);
            if (!advance && advance.error().code != RndvTxErrorCode::kInUse) {
                failed = true;
            }
        }
    }
    if (active && !state.stopped && !failed) {
        const auto sent = state.notifications.Progress();
        if (sent) {
            count += *sent;
        } else if (sent.error().code != RndvNotificationErrorCode::kWouldBlock) {
            failed = true;
        }
    }
    if (failed && active) {
        Cancel();
        return Error(RndvConnectionError::kTx);
    }
    return count;
}
std::expected<void, RndvConnectionError> RndvConnection::Consumed(std::uint64_t prefix) noexcept {
    if (!state_ || prefix > state_->eager->sequence_window()->next_sequence()) {
        return Error(RndvConnectionError::kProtocol);
    }
    state_->consumed = std::max(state_->consumed, prefix);
    return {};
}
void RndvConnection::Cancel() noexcept {
    if (!state_) {
        return;
    }
    state_->stopped = true;
    if (state_->tx_open) {
        (void)state_->tx.Cancel();
    }
    if (state_->ingress_open) {
        (void)state_->ingress.Cancel();
    }
}
std::expected<void, RndvConnectionError> RndvConnection::Close() noexcept {
    if (!state_) {
        return {};
    }
    Cancel();
    auto& state = *state_;
    // ingress 未解绑前保留共用通知的 CQE 分发器，允许迟到 GRANT/DONE 完成继续收束。
    if (!state.ingress.Close()) {
        return Error(RndvConnectionError::kRetained);
    }
    state.ingress_open = false;
    if (!state.receiver.Close()) {
        return Error(RndvConnectionError::kRetained);
    }
    state.receiver_open = false;
    if (!state.tx.Close()) {
        return Error(RndvConnectionError::kRetained);
    }
    state.tx_open = false;
    if (!state.notifications.Close() || !state.writes.Close() || !state.grants.Close()) {
        return Error(RndvConnectionError::kRetained);
    }
    if (state.retained_copy.allocator && !state.source->ReleaseCpu(state.retained_copy)) {
        return Error(RndvConnectionError::kRetained);
    }
    state_.reset();
    return {};
}
std::size_t RndvConnection::pending_rx() const noexcept {
    if (!state_) {
        return 0;
    }
    const auto stats = state_->ingress.stats();
    return stats.rts_packets + stats.grant_packets + stats.done_packets + stats.grant_jobs +
           state_->receiver.stats().active;
}
std::uint64_t RndvConnection::retired() const noexcept {
    return state_ ? state_->completions : 0;
}
} // namespace connection
} // namespace kbsocket
