// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_notification_tx.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <utility>

namespace kbsocket {
namespace connection {
namespace {
constexpr std::size_t kBufferBytes = 112;
auto Error(RndvNotificationErrorCode code, int status = 0) noexcept {
    return std::unexpected(RndvNotificationError{code, status});
}
auto CreditErrorResult(CreditError error) noexcept {
    return Error(error.code == CreditErrorCode::kExhausted || error.code == CreditErrorCode::kInUse
                     ? RndvNotificationErrorCode::kWouldBlock
                     : RndvNotificationErrorCode::kCredit);
}
auto ImportError(control::ImportCacheError error) noexcept {
    return Error(error.code == control::ImportCacheErrorCode::kPending ||
                         error.code == control::ImportCacheErrorCode::kResourceExhausted
                     ? RndvNotificationErrorCode::kWouldBlock
                     : RndvNotificationErrorCode::kImport,
                 error.provider_error);
}
bool SameLane(raw::JettyLane left, raw::JettyLane right) noexcept {
    return left.pool == right.pool && left.index == right.index && left.epoch == right.epoch &&
           left.generation == right.generation;
}
bool SameMetadata(const raw::AttemptMetadata& left, const raw::AttemptMetadata& right) noexcept {
    return left.connection_id == right.connection_id && left.operation_id == right.operation_id &&
           left.operation_generation == right.operation_generation && left.target == right.target &&
           left.opcode == right.opcode && left.buffer_lease == right.buffer_lease &&
           left.grant_lease == right.grant_lease && left.signaled == right.signaled;
}
bool Retryable(const raw::TxSendError& error) noexcept {
    if (error.code == raw::TxSendErrorCode::kPostFailed) {
        return error.provider_status == URMA_EAGAIN || error.provider_status == URMA_ENOMEM;
    }
    if (error.code == raw::TxSendErrorCode::kLedgerFailure && error.ledger_error) {
        return error.ledger_error->code == raw::AttemptLedgerErrorCode::kExhausted ||
               (error.ledger_error->pool_failure &&
                error.ledger_error->pool_failure->code == raw::JettyPoolErrorCode::kExhausted);
    }
    return error.code == raw::TxSendErrorCode::kExhausted ||
           (error.code == raw::TxSendErrorCode::kPoolFailure && error.pool_error &&
            error.pool_error->code == raw::JettyPoolErrorCode::kExhausted);
}
bool IsTerminal(urma_cr_status_t status) noexcept {
    switch (status) {
    case URMA_CR_SUCCESS:
    case URMA_CR_UNSUPPORTED_OPCODE_ERR:
    case URMA_CR_LOC_LEN_ERR:
    case URMA_CR_LOC_OPERATION_ERR:
    case URMA_CR_LOC_ACCESS_ERR:
    case URMA_CR_REM_RESP_LEN_ERR:
    case URMA_CR_REM_UNSUPPORTED_REQ_ERR:
    case URMA_CR_REM_OPERATION_ERR:
    case URMA_CR_REM_ACCESS_ABORT_ERR:
    case URMA_CR_ACK_TIMEOUT_ERR:
    case URMA_CR_RNR_RETRY_CNT_EXC_ERR:
    case URMA_CR_WR_FLUSH_ERR:
    case URMA_CR_WR_UNHANDLED:
    case URMA_CR_LOC_DATA_POISON:
    case URMA_CR_REM_DATA_POISON:
        return true;
    default:
        return false;
    }
}
} // namespace

struct RndvNotificationTx::Entry {
    memory::BufferLease buffer;
    memory::BufferReference dma;
    ControlCreditLease control;
    CreditReservation pending_credit;
    std::optional<std::size_t> post_acceptance = 0;
    GrantTransfer transfer;
    GrantIdentity grant;
    GrantManager* manager = nullptr;
    GrantHandle grant_handle;
    TxSequenceWindow* sequence = nullptr;
    SequenceTicket sequence_ticket;
    RndvNotificationHandle sibling;
    WireEncodedFrame frame;
    raw::AttemptMetadata metadata;
    raw::AttemptId attempt = 0;
    RndvNotificationStatus status;
    std::uint64_t generation = 0;
    bool active = false;
    bool sequence_pending = false;
    bool sequence_resolution_pending = false;
    bool sequence_cancelled = false;
    bool rts_accepted = false;
    bool dma_committed = false;
    bool possibly_exposed = false;
    bool terminal_seen = false;
    bool grant_cancelled = false;
};
struct RndvNotificationTx::State {
    RndvNotificationConfig config;
    raw::TxSender* sender = nullptr;
    memory::SlabAllocator* slab = nullptr;
    control::EndpointImportCache* imports = nullptr;
    SenderCreditAccount* credits = nullptr;
    TxSequenceWindow* sequence = nullptr;
    control::ImportLease endpoint;
    const RndvRxIngress* ingress = nullptr;
    const RndvTxCoordinator* coordinator = nullptr;
    TxSequenceWindow* coordinator_sequence = nullptr;
    std::unique_ptr<Entry[]> entries;
    std::uint64_t next_generation = 0;
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint32_t cursor = 0;
    bool stopped = false;
    bool failed = false;

    std::expected<urma_target_jetty_t*, RndvNotificationError> Destination() const noexcept {
        const auto key = imports->GetKey(endpoint);
        if (!key) {
            return ImportError(key.error());
        }
        if (*key != config.endpoint) {
            return Error(RndvNotificationErrorCode::kWrongIdentity);
        }
        const auto target = imports->Get(endpoint);
        if (!target) {
            return ImportError(target.error());
        }
        if (!*target || (*target)->urma_ctx != key->local_context || (*target)->id.id != key->jetty_id ||
            std::memcmp((*target)->id.eid.raw, key->eid.data(), key->eid.size()) ||
            (*target)->trans_mode != URMA_TM_RM || (*target)->tp_type != URMA_CTP || (*target)->type != URMA_JETTY) {
            return Error(RndvNotificationErrorCode::kWrongIdentity);
        }
        return *target;
    }
    std::expected<WireEncodedFrame, RndvNotificationError> Encode(Entry& entry, const WireFrameInput& frame) noexcept {
        const auto bytes = slab->Writable(entry.buffer);
        if (!bytes) {
            return Error(RndvNotificationErrorCode::kMemory, bytes.error().provider_error);
        }
        const auto encoded = EncodeWireFrame(frame, *bytes);
        if (!encoded) {
            return Error(RndvNotificationErrorCode::kInvalidArgument);
        }
        return *encoded;
    }
};

RndvNotificationTx::RndvNotificationTx() noexcept = default;
RndvNotificationTx::~RndvNotificationTx() {
    if (!Close()) {
        // 未终结或接受未知时，保留固定 buffer、缓存 pin 与责任记录；宿主仍须保留外部依赖。
        (void)state_.release();
    }
}
std::expected<void, RndvNotificationError> RndvNotificationTx::Open(const RndvNotificationConfig& config,
                                                                    raw::TxSender& sender, memory::SlabAllocator& slab,
                                                                    control::EndpointImportCache& imports,
                                                                    control::ImportLease endpoint,
                                                                    SenderCreditAccount& credits) noexcept {
    if (state_ || opened_once_) {
        return Error(RndvNotificationErrorCode::kInUse);
    }
    const auto& key = config.endpoint;
    const auto* pool = sender.pool();
    if (!config.connection_id || !config.capacity || config.capacity > kMaxCapacity || !config.destination.runtime ||
        !config.destination.receive_domain || !config.destination.session || !config.destination.token.generation ||
        !pool || !pool->ready() || !sender.defer_drain() || !slab.accepting() || !credits.ready() ||
        credits.stats().stopped || credits.identity() != config.credit_identity ||
        config.credit_identity.peer_runtime != config.destination.runtime ||
        config.credit_identity.receive_domain != config.destination.receive_domain || !config.credit_identity.epoch ||
        config.lane.pool != pool || !pool->max_send_sge() || key.local_context != sender.context() ||
        !key.local_context || !key.local_context_incarnation || key.peer_runtime != config.destination.runtime ||
        key.receive_domain != config.destination.receive_domain || !key.endpoint_generation ||
        !key.authorization_identity || key.transport_mode != URMA_TM_RM || key.tp_type != URMA_CTP ||
        !std::any_of(key.eid.begin(), key.eid.end(), [](auto byte) { return byte != 0; })) {
        return Error(RndvNotificationErrorCode::kInvalidArgument);
    }
    const auto lane = pool->lane_state(config.lane);
    if (!lane || *lane != raw::JettyLaneState::kReady) {
        return Error(RndvNotificationErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(RndvNotificationErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) Entry[config.capacity]);
    if (!state->entries) {
        return Error(RndvNotificationErrorCode::kNoMemory);
    }
    state->config = config;
    state->sender = &sender;
    state->slab = &slab;
    state->imports = &imports;
    state->credits = &credits;
    state->endpoint = endpoint;
    const auto destination = state->Destination();
    if (!destination) {
        return std::unexpected(destination.error());
    }
    const auto pin = imports.Retain(endpoint);
    if (!pin) {
        return ImportError(pin.error());
    }
    state->endpoint = *pin;
    state_ = std::move(state);
    // Open 回滚也保留失败阶段；Close 可重试，不能遗失已取得的 CPU/缓存责任。
    const auto rollback = [this](RndvNotificationError error) {
        if (!Close()) {
            return Error(RndvNotificationErrorCode::kRetained);
        }
        return std::unexpected(error);
    };
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        auto& entry = state_->entries[i];
        const auto buffer = slab.Acquire(kBufferBytes);
        if (!buffer) {
            return rollback({RndvNotificationErrorCode::kMemory, buffer.error().provider_error});
        }
        entry.buffer = *buffer;
        const auto dma = slab.Reserve(entry.buffer, memory::BufferUse::kLocalDma);
        if (!dma) {
            return rollback({RndvNotificationErrorCode::kMemory, dma.error().provider_error});
        }
        entry.dma = *dma;
        const auto sge = slab.Sge(*dma, sender.context(), 0, kBufferBytes);
        const auto cancelled = slab.Cancel(*dma);
        if (cancelled) {
            entry.dma = {};
        }
        if (!sge || !cancelled) {
            return rollback({RndvNotificationErrorCode::kMemory});
        }
    }
    opened_once_ = true;
    return {};
}
std::expected<RndvNotificationTx::Entry*, RndvNotificationError>
RndvNotificationTx::Find(RndvNotificationHandle handle) const noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    if (handle.owner != this || !handle.generation || handle.slot >= state_->config.capacity) {
        return Error(RndvNotificationErrorCode::kInvalidHandle);
    }
    auto& entry = state_->entries[handle.slot];
    if (!entry.active || entry.generation != handle.generation) {
        return Error(RndvNotificationErrorCode::kInvalidHandle);
    }
    return &entry;
}
std::expected<RndvRtsReservation, RndvNotificationError>
RndvNotificationTx::QueueRts(const GrantTransfer& transfer, TxSequenceWindow& sequence) noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped || state.failed) {
        return Error(RndvNotificationErrorCode::kStopped);
    }
    const auto& destination = state.config.destination;
    const auto frame = BuildRndvRts(transfer, destination);
    if ((state.sequence && state.sequence != &sequence) ||
        (state.coordinator_sequence && state.coordinator_sequence != &sequence) || !frame ||
        sequence.session() != SequenceSession{destination.runtime, destination.receive_domain, destination.session} ||
        transfer.seq != sequence.next_sequence()) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    std::array<std::uint32_t, 2> slots{};
    std::size_t free = 0;
    for (std::uint32_t i = 0; i < state.config.capacity && free != slots.size(); ++i) {
        if (!state.entries[i].active) {
            slots[free++] = i;
        }
    }
    if (free != slots.size()) {
        return Error(RndvNotificationErrorCode::kWouldBlock);
    }
    if (state.next_generation > std::numeric_limits<std::uint64_t>::max() - 2) {
        return Error(RndvNotificationErrorCode::kInvalidState);
    }
    auto& rts = state.entries[slots[0]];
    auto& done = state.entries[slots[1]];
    const auto encoded = state.Encode(rts, *frame);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    const auto control = state.credits->ReserveControl();
    if (!control) {
        return CreditErrorResult(control.error());
    }
    const std::array<TxFramePlan, 1> plans{{{.bytes = transfer.bytes}}};
    const auto ticket = sequence.Reserve(plans);
    if (!ticket) {
        if (!state.credits->ReleaseControl(*control)) {
            done.active = true;
            done.generation = ++state.next_generation;
            done.control = *control;
            done.status.type = WireControlType::kRndvDone;
            done.status.cancelled = true;
            state.failed = true;
            return Error(RndvNotificationErrorCode::kRetained);
        }
        return Error(ticket.error() == SequenceError::kInUse || ticket.error() == SequenceError::kWindowFull
                         ? RndvNotificationErrorCode::kWouldBlock
                         : RndvNotificationErrorCode::kSequence);
    }
    rts.active = done.active = true;
    rts.generation = ++state.next_generation;
    done.generation = ++state.next_generation;
    RndvRtsReservation reservation{{this, slots[0], rts.generation}, {this, slots[1], done.generation}};
    rts.transfer = done.transfer = transfer;
    rts.status.type = WireControlType::kRndvRts;
    rts.status.prepared = true;
    rts.sequence = &sequence;
    rts.sequence_ticket = *ticket;
    rts.sequence_pending = true;
    rts.frame = *encoded;
    rts.sibling = reservation.done;
    done.sibling = reservation.rts;
    done.sequence = &sequence;
    done.status.type = WireControlType::kRndvDone;
    done.control = *control;
    state.sequence = &sequence;
    return reservation;
}
std::expected<RndvNotificationHandle, RndvNotificationError>
RndvNotificationTx::QueueGrant(GrantManager& manager, const GrantReservation& reservation) noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped || state.failed) {
        return Error(RndvNotificationErrorCode::kStopped);
    }
    std::uint32_t slot = state.config.capacity;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        const auto& entry = state.entries[i];
        if (entry.active && entry.manager == &manager && entry.grant_handle == reservation.handle) {
            return Error(RndvNotificationErrorCode::kInUse);
        }
        if (!entry.active && slot == state.config.capacity) {
            slot = i;
        }
    }
    const auto frame = BuildRndvGrant(reservation.identity, state.config.destination);
    if (!frame) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    if (!manager.ValidatePublication(reservation.handle, reservation.identity)) {
        return Error(RndvNotificationErrorCode::kGrant);
    }
    if (slot == state.config.capacity) {
        return Error(RndvNotificationErrorCode::kWouldBlock);
    }
    if (state.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(RndvNotificationErrorCode::kInvalidState);
    }
    auto& entry = state.entries[slot];
    const auto encoded = state.Encode(entry, *frame);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    const auto control = state.credits->ReserveControl();
    if (!control) {
        return CreditErrorResult(control.error());
    }
    entry.active = true;
    entry.generation = ++state.next_generation;
    entry.status.type = WireControlType::kRndvGrant;
    entry.status.prepared = true;
    entry.manager = &manager;
    entry.grant_handle = reservation.handle;
    entry.grant = reservation.identity;
    entry.transfer = reservation.identity.transfer;
    entry.control = *control;
    entry.frame = *encoded;
    return RndvNotificationHandle{this, slot, entry.generation};
}
std::expected<void, RndvNotificationError> RndvNotificationTx::QueueDone(RndvNotificationHandle handle,
                                                                         const WriteRndvPool& pool,
                                                                         WriteRndvHandle write) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& entry = **found;
    auto& state = *state_;
    if (state.failed || entry.status.cancelled) {
        return Error(RndvNotificationErrorCode::kStopped);
    }
    if (entry.status.type != WireControlType::kRndvDone || entry.status.prepared || !entry.rts_accepted) {
        return Error(RndvNotificationErrorCode::kInvalidState);
    }
    const auto evidence = pool.DoneEvidence(write);
    if (!evidence) {
        return Error(RndvNotificationErrorCode::kEvidence);
    }
    if (evidence->identity.transfer != entry.transfer) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    const auto frame = BuildRndvDone(pool, write, state.config.destination);
    if (!frame) {
        return Error(RndvNotificationErrorCode::kEvidence);
    }
    const auto encoded = state.Encode(entry, *frame);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    entry.grant = evidence->identity;
    entry.frame = *encoded;
    entry.status.prepared = true;
    return {};
}

std::expected<void, RndvNotificationError> RndvNotificationTx::Finish(Entry& entry) noexcept {
    bool settled = true;
    if (entry.pending_credit.id) {
        const auto accepted = entry.post_acceptance;
        const auto resolved = state_->credits->Resolve(
            entry.pending_credit,
            accepted ? std::optional<std::uint32_t>(static_cast<std::uint32_t>(*accepted)) : std::nullopt);
        if (resolved) {
            entry.pending_credit = {};
            if (!accepted || *accepted) {
                entry.control = {};
            }
        } else {
            settled = false;
        }
    }
    if (entry.sequence_resolution_pending) {
        const auto accepted = entry.post_acceptance;
        const auto resolved = entry.sequence->Resolve(entry.sequence_ticket, accepted);
        if (resolved || (!accepted && resolved.error() == SequenceError::kAcceptanceUnknown)) {
            entry.sequence_resolution_pending = false;
            if (!accepted || *accepted) {
                entry.sequence_pending = false;
            }
        } else {
            settled = false;
        }
    }
    if (!entry.dma.sequence) {
        return settled ? std::expected<void, RndvNotificationError>{} : Error(RndvNotificationErrorCode::kRetained);
    }
    auto& slab = *state_->slab;
    if (entry.possibly_exposed) {
        if (!entry.dma_committed) {
            if (const auto committed = slab.Commit(entry.dma); !committed) {
                return Error(RndvNotificationErrorCode::kMemory, committed.error().provider_error);
            }
            entry.dma_committed = true;
        }
        if (!entry.terminal_seen) {
            return settled ? std::expected<void, RndvNotificationError>{} : Error(RndvNotificationErrorCode::kRetained);
        }
        if (const auto completed = slab.CompleteDma(entry.dma); !completed) {
            return Error(RndvNotificationErrorCode::kMemory, completed.error().provider_error);
        }
    } else if (const auto cancelled = slab.Cancel(entry.dma); !cancelled) {
        return Error(RndvNotificationErrorCode::kMemory, cancelled.error().provider_error);
    }
    entry.dma = {};
    entry.dma_committed = false;
    return settled ? std::expected<void, RndvNotificationError>{} : Error(RndvNotificationErrorCode::kRetained);
}
std::expected<std::size_t, RndvNotificationError> RndvNotificationTx::Progress() noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.failed) {
        return Error(RndvNotificationErrorCode::kStopped);
    }
    std::uint32_t slot = state.config.capacity;
    for (std::uint32_t scanned = 0; scanned < state.config.capacity; ++scanned) {
        const auto index = (state.cursor + scanned) % state.config.capacity;
        const auto& entry = state.entries[index];
        if (entry.active && entry.status.prepared && !entry.status.cancelled && !entry.possibly_exposed &&
            !entry.status.retired && !entry.status.failed) {
            slot = index;
            break;
        }
    }
    if (slot == state.config.capacity) {
        return 0;
    }
    state.cursor = (slot + 1) % state.config.capacity;
    auto& entry = state.entries[slot];
    const auto fail = [&](RndvNotificationError error) -> std::expected<std::size_t, RndvNotificationError> {
        state.failed = true;
        entry.status.failed = true;
        if (!CancelEntry(entry)) {
            return Error(RndvNotificationErrorCode::kRetained);
        }
        return std::unexpected(error);
    };
    if (entry.status.type == WireControlType::kRndvRts && entry.sequence->stopped()) {
        return fail({RndvNotificationErrorCode::kSequence});
    }
    if (state.credits->identity() != state.config.credit_identity) {
        return fail({RndvNotificationErrorCode::kWrongIdentity});
    }
    const auto destination = state.Destination();
    if (!destination) {
        if (destination.error().code == RndvNotificationErrorCode::kWouldBlock) {
            return std::unexpected(destination.error());
        }
        return fail(destination.error());
    }
    if (entry.dma.sequence && !Finish(entry)) {
        return fail({RndvNotificationErrorCode::kMemory});
    }
    const auto dma = state.slab->Reserve(entry.buffer, memory::BufferUse::kLocalDma);
    if (!dma) {
        if (dma.error().code == memory::MemoryErrorCode::kExhausted) {
            return Error(RndvNotificationErrorCode::kWouldBlock);
        }
        return fail({RndvNotificationErrorCode::kMemory, dma.error().provider_error});
    }
    entry.dma = *dma;
    auto sge = state.slab->Sge(*dma, state.sender->context(), 0, entry.frame.payload_bytes);
    if (!sge) {
        return fail({RndvNotificationErrorCode::kMemory, sge.error().provider_error});
    }
    const auto credit = entry.status.type == WireControlType::kRndvRts ? state.credits->Reserve(CreditClass::kData, 1)
                                                                       : state.credits->ActivateControl(entry.control);
    if (!credit) {
        if (!Finish(entry)) {
            return fail({RndvNotificationErrorCode::kMemory});
        }
        const auto error = CreditErrorResult(credit.error());
        if (error.error().code == RndvNotificationErrorCode::kWouldBlock) {
            return error;
        }
        return fail(error.error());
    }
    entry.pending_credit = *credit;
    entry.post_acceptance = 0;
    if (entry.manager && !entry.status.grant_published) {
        if (!entry.manager->CommitPublication(entry.grant_handle)) {
            return fail({RndvNotificationErrorCode::kGrant});
        }
        entry.status.grant_published = true;
    }
    entry.metadata = {.connection_id = state.config.connection_id,
                      .operation_id = static_cast<std::uint64_t>(slot) + 1,
                      .operation_generation = entry.generation,
                      .target = *destination,
                      .opcode = URMA_OPC_SEND_IMM,
                      .buffer_lease = entry.buffer.generation,
                      .grant_lease = entry.grant.generation,
                      .signaled = true};
    const std::array<raw::TxSendRequest, 1> requests{{{.metadata = entry.metadata,
                                                       .sges = std::span(&*sge, 1),
                                                       .imm_data = entry.frame.imm_data,
                                                       .remote = std::nullopt}}};
    std::array<raw::AttemptId, 1> ids{};
    const auto posted = state.sender->Send(requests, ids, state.config.lane);
    auto accepted = posted ? std::optional<std::size_t>(*posted) : posted.error().accepted;
    if ((!posted && (posted.error().code == raw::TxSendErrorCode::kAccountingFailure ||
                     posted.error().code == raw::TxSendErrorCode::kProviderContract)) ||
        (accepted && *accepted > 1) || (accepted && *accepted == 1 && !ids[0])) {
        accepted.reset();
    }
    entry.attempt = ids[0];
    entry.possibly_exposed = !accepted || *accepted != 0;
    entry.status.acceptance_known = accepted.has_value();
    entry.status.accepted = accepted && *accepted == 1;
    if (entry.status.accepted) {
        ++state.accepted;
        if (entry.status.type == WireControlType::kRndvRts) {
            const auto done = Find(entry.sibling);
            if (done) {
                (**done).rts_accepted = true;
            }
        }
    }
    entry.post_acceptance = accepted;
    entry.sequence_resolution_pending = entry.sequence && entry.sequence_pending;
    const auto finished = Finish(entry);
    if (!accepted) {
        state.failed = true;
        entry.status.failed = true;
        (void)state.slab->Quarantine(entry.buffer);
        (void)CancelEntry(entry);
        return Error(RndvNotificationErrorCode::kAcceptanceUnknown, posted ? 0 : posted.error().provider_status);
    }
    if (!finished) {
        return fail({RndvNotificationErrorCode::kRetained});
    }
    if (*accepted == 0) {
        entry.attempt = 0;
        if (!posted && Retryable(posted.error())) {
            return Error(RndvNotificationErrorCode::kWouldBlock, posted.error().provider_status);
        }
        return fail({RndvNotificationErrorCode::kPost, posted ? 0 : posted.error().provider_status});
    }
    if (!posted) {
        return fail({RndvNotificationErrorCode::kPost, posted.error().provider_status});
    }
    return 1;
}

std::expected<void, RndvNotificationError>
RndvNotificationTx::OnCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (event.kind != raw::TxCompletionKind::kCompleted || !event.record || !event.lane ||
        event.completion.flag.bs.s_r || !event.completion.flag.bs.jetty || !IsTerminal(event.completion.status) ||
        event.completion.user_ctx != event.record->id || !SameLane(*event.lane, state.config.lane) ||
        !SameLane(event.record->ticket.lane, state.config.lane) || !event.record->metadata.operation_id ||
        event.record->metadata.operation_id > state.config.capacity) {
        return Error(RndvNotificationErrorCode::kInvalidCompletion);
    }
    auto& entry = state.entries[event.record->metadata.operation_id - 1];
    if (!entry.active || !entry.possibly_exposed || entry.terminal_seen || !entry.attempt ||
        entry.attempt != event.record->id || !SameMetadata(entry.metadata, event.record->metadata)) {
        return Error(RndvNotificationErrorCode::kInvalidCompletion);
    }
    // 先保存可信终结事实；清理失败时 Release/Cancel 可以重试，不能靠再次投递获得完成。
    entry.terminal_seen = true;
    entry.status.retired = true;
    entry.status.completion_status = event.completion.status;
    ++state.retired;
    const bool failed = event.completion.status != URMA_CR_SUCCESS || event.error.has_value();
    if (failed) {
        entry.status.failed = true;
        state.failed = true;
    }
    const auto finished = Finish(entry);
    if (!finished) {
        state.failed = true;
        return finished;
    }
    if (failed) {
        if (!CancelEntry(entry)) {
            return Error(RndvNotificationErrorCode::kRetained);
        }
        return Error(RndvNotificationErrorCode::kCompletion, event.completion.status);
    }
    return {};
}
std::expected<void, RndvNotificationError> RndvNotificationTx::CancelEntry(Entry& entry) noexcept {
    entry.status.cancelled = true;
    bool finished = Finish(entry).has_value();
    if (entry.status.type == WireControlType::kRndvRts && entry.sequence && !entry.sequence_cancelled) {
        if (entry.possibly_exposed || !entry.status.acceptance_known) {
            entry.sequence->Fail();
            state_->failed = true;
            entry.sequence_cancelled = true;
        } else if (entry.sequence_pending && !entry.sequence_resolution_pending) {
            if (entry.sequence->Cancel(entry.sequence_ticket)) {
                entry.sequence_pending = false;
                entry.sequence_cancelled = true;
            } else {
                finished = false;
            }
        } else if (!entry.sequence_pending) {
            entry.sequence_cancelled = true;
        }
    }
    if (entry.status.type == WireControlType::kRndvRts) {
        const auto done = Find(entry.sibling);
        if (done && !(**done).possibly_exposed && !CancelEntry(**done)) {
            finished = false;
        }
    } else if (entry.status.type == WireControlType::kRndvDone && entry.sequence) {
        if (entry.rts_accepted) {
            // RTS 的槽位可以已释放；DONE 仍独立承担不可留下共同 SN 空洞的责任。
            entry.sequence->Fail();
            state_->failed = true;
        } else {
            const auto rts = Find(entry.sibling);
            if (rts && !(**rts).status.cancelled && !CancelEntry(**rts)) {
                finished = false;
            }
        }
    }
    if (entry.manager && !entry.grant_cancelled) {
        // 只有唯一发布者确认所有 SEND 均未被接受，才能证明零 WRITE；CQE 成功不终结目标授权。
        // 远端 DONE/归还可能早于本地 GRANT CQE；发布者不得取消已完成或已借出的目标，也不得碰新代次。
        const auto retired = entry.manager->IsRetired(entry.grant_handle, entry.grant);
        const auto cancelled =
            retired && *retired ? std::expected<void, GrantError>{}
            : entry.status.grant_published && !entry.possibly_exposed && entry.status.acceptance_known
                ? entry.manager->ConfirmTerminal(
                      {.identity = entry.grant, .no_future_writes = true, .acceptance_known = true})
                : entry.manager->CancelPublication(entry.grant_handle, entry.grant);
        if (cancelled) {
            entry.grant_cancelled = true;
        } else {
            finished = false;
        }
    }
    if (entry.control.id && !entry.pending_credit.id) {
        if (state_->credits->ReleaseControl(entry.control)) {
            entry.control = {};
        } else {
            finished = false;
        }
    }
    return finished ? std::expected<void, RndvNotificationError>{} : Error(RndvNotificationErrorCode::kRetained);
}
std::expected<void, RndvNotificationError> RndvNotificationTx::Cancel(RndvNotificationHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    return CancelEntry(**found);
}
std::expected<void, RndvNotificationError> RndvNotificationTx::Release(RndvNotificationHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& entry = **found;
    if (!entry.status.acceptance_known) {
        return Error(RndvNotificationErrorCode::kAcceptanceUnknown);
    }
    if (!entry.status.cancelled && !entry.status.retired) {
        return Error(RndvNotificationErrorCode::kInUse);
    }
    if (entry.status.cancelled && !CancelEntry(entry)) {
        return Error(RndvNotificationErrorCode::kRetained);
    }
    if (const auto finished = Finish(entry); !finished) {
        return finished;
    }
    if (entry.dma.sequence || entry.pending_credit.id || entry.sequence_resolution_pending ||
        (entry.possibly_exposed && !entry.status.retired)) {
        return Error(RndvNotificationErrorCode::kInUse);
    }
    if (entry.control.id && !state_->credits->ReleaseControl(entry.control)) {
        return Error(RndvNotificationErrorCode::kRetained);
    }
    const auto buffer = entry.buffer;
    entry = Entry{};
    entry.buffer = buffer;
    return {};
}
std::expected<RndvNotificationStatus, RndvNotificationError>
RndvNotificationTx::status(RndvNotificationHandle handle) const noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    return (**found).status;
}
RndvNotificationStats RndvNotificationTx::stats() const noexcept {
    RndvNotificationStats result;
    if (!state_) {
        return result;
    }
    const auto& state = *state_;
    result.stopped = state.stopped;
    result.failed = state.failed;
    result.accepted = state.accepted;
    result.retired = state.retired;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        const auto& entry = state.entries[i];
        if (entry.active) {
            ++result.active;
            if (entry.status.prepared && !entry.status.cancelled && !entry.possibly_exposed && !entry.status.failed) {
                ++result.queued;
            }
            if (entry.possibly_exposed && !entry.status.retired) {
                ++result.in_flight;
            }
        }
    }
    return result;
}
void RndvNotificationTx::Stop() noexcept {
    if (state_) {
        state_->stopped = true;
    }
}
std::expected<void, RndvNotificationError>
RndvNotificationTx::BindIngress(const RndvRxIngress& ingress, urma_context_t* context, std::uint64_t peer) noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    if (state_->ingress) {
        return Error(RndvNotificationErrorCode::kInUse);
    }
    if (state_->stopped || state_->failed) {
        return Error(RndvNotificationErrorCode::kStopped);
    }
    if (state_->config.endpoint.local_context != context || state_->config.destination.runtime != peer) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    state_->ingress = &ingress;
    return {};
}
std::expected<void, RndvNotificationError> RndvNotificationTx::UnbindIngress(const RndvRxIngress& ingress) noexcept {
    if (!state_ || state_->ingress != &ingress) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    state_->ingress = nullptr;
    return {};
}
const RndvNotificationConfig* RndvNotificationTx::config() const noexcept {
    return state_ ? &state_->config : nullptr;
}
std::expected<void, RndvNotificationError> RndvNotificationTx::BindCoordinator(const RndvTxCoordinator& owner,
                                                                               memory::SlabAllocator& slab,
                                                                               control::EndpointImportCache& endpoints,
                                                                               TxSequenceWindow& sequence,
                                                                               raw::TxSender* sender) noexcept {
    if (!state_) {
        return Error(RndvNotificationErrorCode::kNotReady);
    }
    if (state_->coordinator) {
        return Error(RndvNotificationErrorCode::kInUse);
    }
    if (state_->stopped || state_->failed) {
        return Error(RndvNotificationErrorCode::kStopped);
    }
    if (state_->sender != sender || state_->slab != &slab || state_->imports != &endpoints ||
        (state_->sequence && state_->sequence != &sequence)) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    // 接收端 GRANT 可共存；已存在的本地 RTS/DONE 不可绕过唯一发送协调器。
    for (std::uint32_t i = 0; i < state_->config.capacity; ++i) {
        if (state_->entries[i].active && state_->entries[i].status.type != WireControlType::kRndvGrant) {
            return Error(RndvNotificationErrorCode::kInUse);
        }
    }
    state_->coordinator_sequence = &sequence;
    state_->coordinator = &owner;
    return {};
}
std::expected<void, RndvNotificationError>
RndvNotificationTx::UnbindCoordinator(const RndvTxCoordinator& owner) noexcept {
    if (!state_ || state_->coordinator != &owner) {
        return Error(RndvNotificationErrorCode::kWrongIdentity);
    }
    for (std::uint32_t i = 0; i < state_->config.capacity; ++i) {
        if (state_->entries[i].active && state_->entries[i].status.type != WireControlType::kRndvGrant) {
            return Error(RndvNotificationErrorCode::kInUse);
        }
    }
    state_->coordinator = nullptr;
    state_->coordinator_sequence = nullptr;
    return {};
}
std::expected<void, RndvNotificationError> RndvNotificationTx::Close() noexcept {
    if (!state_) {
        return {};
    }
    if (state_->ingress || state_->coordinator) {
        // ingress 仍可能持有精确通知 handle；不能越过其 RX/远端授权责任关闭或取消整队列。
        return Error(RndvNotificationErrorCode::kInUse);
    }
    Stop();
    auto& state = *state_;
    bool finished = true;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        auto& entry = state.entries[i];
        if (entry.active) {
            // 成功通知的源已终结，远端 Grant 可已被消费；不能倒退调用 Cancel 干预目标责任。
            const bool success = entry.status.acceptance_known && entry.status.retired && !entry.status.failed &&
                                 !entry.status.cancelled;
            if ((!success && !CancelEntry(entry)) || !Release({this, i, entry.generation})) {
                finished = false;
            }
        } else if (entry.dma.sequence && !Finish(entry)) {
            finished = false;
        }
    }
    if (!finished) {
        return Error(RndvNotificationErrorCode::kRetained);
    }
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        auto& entry = state.entries[i];
        if (entry.buffer.generation) {
            const auto released = state.slab->ReleaseCpu(entry.buffer);
            if (!released) {
                return Error(RndvNotificationErrorCode::kMemory, released.error().provider_error);
            }
            entry.buffer = {};
        }
    }
    if (state.endpoint.sequence) {
        if (const auto released = state.imports->Release(state.endpoint); !released) {
            return ImportError(released.error());
        }
        state.endpoint = {};
    }
    state_.reset();
    return {};
}
} // namespace connection
} // namespace kbsocket
