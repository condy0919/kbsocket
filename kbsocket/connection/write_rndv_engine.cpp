// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/write_rndv_engine.hpp"

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
enum class Phase : std::uint8_t { kQueued, kPosted, kRetired, kCancelled };
auto Error(WriteRndvErrorCode code, int status = 0) noexcept {
    return std::unexpected(WriteRndvError{code, status});
}
bool SameLane(raw::JettyLane left, raw::JettyLane right) noexcept {
    return left.pool == right.pool && left.index == right.index && left.epoch == right.epoch &&
           left.generation == right.generation;
}
bool ValidTransfer(const GrantTransfer& transfer) noexcept {
    const auto& binding = transfer.binding;
    return binding.sender_runtime && binding.receiver_runtime && binding.receive_domain && binding.session &&
           binding.token.generation && transfer.transfer_id && transfer.bytes &&
           transfer.seq != std::numeric_limits<std::uint32_t>::max();
}
bool Retryable(const raw::TxSendError& error) noexcept {
    if (error.code == raw::TxSendErrorCode::kPostFailed) {
        return error.provider_status == URMA_EAGAIN || error.provider_status == URMA_ENOMEM;
    }
    if (error.code == raw::TxSendErrorCode::kLedgerFailure && error.ledger_error) {
        if (error.ledger_error->code == raw::AttemptLedgerErrorCode::kExhausted) {
            return true;
        }
        return error.ledger_error->pool_failure &&
               error.ledger_error->pool_failure->code == raw::JettyPoolErrorCode::kExhausted;
    }
    return error.code == raw::TxSendErrorCode::kPoolFailure && error.pool_error &&
           error.pool_error->code == raw::JettyPoolErrorCode::kExhausted;
}
bool SameMetadata(const raw::AttemptMetadata& left, const raw::AttemptMetadata& right) noexcept {
    return left.connection_id == right.connection_id && left.operation_id == right.operation_id &&
           left.operation_generation == right.operation_generation && left.target == right.target &&
           left.opcode == right.opcode && left.buffer_lease == right.buffer_lease &&
           left.grant_lease == right.grant_lease && left.signaled == right.signaled;
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

struct WriteRndvEngine::Chunk {
    struct Source {
        memory::BufferLease lease;
        memory::BufferReference dma;
        memory::BufferReference pin;
        std::size_t offset = 0;
        bool committed = false;
    };
    std::array<Source, kMaxSources> sources{};
    std::array<urma_sge_t, kMaxSources> sges{};
    urma_sge_t remote{};
    raw::AttemptMetadata metadata;
    raw::AttemptId id = 0;
    std::size_t source_count = 0;
    Phase phase = Phase::kQueued;
    bool terminal_seen = false;
    bool terminal_error = false;
    urma_cr_status_t terminal_status = URMA_CR_SUCCESS;
};
struct WriteRndvEngine::State {
    WriteRndvConfig config;
    GrantTransfer transfer;
    WriteRndvDestination destination;
    raw::TxSender* sender = nullptr;
    memory::SlabAllocator* slab = nullptr;
    std::unique_ptr<Chunk[]> chunks;
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests{};
    std::array<raw::AttemptId, raw::TxSender::kMaxBatch> ids{};
    WriteRndvStats stats;
    std::size_t next_submit = 0;
    std::size_t post_limit = 0;
    bool initialized = false;
};

WriteRndvEngine::WriteRndvEngine() noexcept = default;
WriteRndvEngine::~WriteRndvEngine() {
    if (!Close()) {
        // 外部 provider/context/导入对象/Slab 也须由宿主保留，不能用析构代替 DMA 终结。
        (void)state_.release();
    }
}
std::expected<void, WriteRndvError> WriteRndvEngine::Open(const WriteRndvConfig& config, raw::TxSender& sender,
                                                          memory::SlabAllocator& slab) noexcept {
    if (opened_once_ || state_) {
        return Error(WriteRndvErrorCode::kInUse);
    }
    const auto* pool = sender.pool();
    if (!pool || !pool->ready() || !sender.defer_drain() || !slab.accepting() || !config.connection_id ||
        !config.transaction_generation || !config.chunk_bytes || !config.max_chunks || config.max_chunks > kMaxChunks ||
        !config.post_budget || config.post_budget > raw::TxSender::kMaxBatch || config.lane.pool != pool) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    const auto lane_state = pool->lane_state(config.lane);
    if (!lane_state || *lane_state != raw::JettyLaneState::kReady || !pool->max_send_sge() || !pool->max_write_size()) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(WriteRndvErrorCode::kNoMemory);
    }
    state->chunks.reset(new (std::nothrow) Chunk[config.max_chunks]);
    if (!state->chunks) {
        return Error(WriteRndvErrorCode::kNoMemory);
    }
    state->config = config;
    state->sender = &sender;
    state->slab = &slab;
    state->post_limit = config.post_budget;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<void, WriteRndvError> WriteRndvEngine::Start(const GrantTransfer& expected_transfer,
                                                           const WriteRndvDestination& destination,
                                                           std::span<const WriteRndvSource> sources) noexcept {
    return StartImpl(expected_transfer, destination, sources, {});
}
std::expected<void, WriteRndvError>
WriteRndvEngine::StartPinned(const GrantTransfer& expected_transfer, const WriteRndvDestination& destination,
                             std::span<const WriteRndvSource> sources,
                             std::span<const memory::BufferReference> pins) noexcept {
    if (pins.empty() || pins.size() != sources.size()) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    return StartImpl(expected_transfer, destination, sources, pins);
}
std::expected<void, WriteRndvError> WriteRndvEngine::StartImpl(const GrantTransfer& expected_transfer,
                                                               const WriteRndvDestination& destination,
                                                               std::span<const WriteRndvSource> sources,
                                                               std::span<const memory::BufferReference> pins) noexcept {
    if (!state_) {
        return Error(WriteRndvErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.initialized) {
        return Error(WriteRndvErrorCode::kInUse);
    }
    if (state.stats.cancelled || state.stats.failed) {
        return Error(WriteRndvErrorCode::kStopped);
    }
    auto& sender = *state.sender;
    auto& slab = *state.slab;
    const auto& config = state.config;
    const auto* pool = sender.pool();
    if (!pool || pool != config.lane.pool || !pool->ready() || !slab.accepting() || !ValidTransfer(expected_transfer) ||
        sources.empty() || sources.size() > kMaxSources) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    const auto lane_state = pool->lane_state(config.lane);
    if (!lane_state || *lane_state != raw::JettyLaneState::kReady) {
        return Error(WriteRndvErrorCode::kStopped);
    }
    const auto& grant = destination.grant;
    if (grant.transfer != expected_transfer || !grant.arena.id || !grant.arena.generation || !grant.generation ||
        grant.slot >= GrantManager::kMaxWindow || !grant.target_address || !destination.jetty || !destination.segment ||
        destination.jetty->urma_ctx != sender.context() || destination.segment->urma_ctx != sender.context() ||
        std::memcmp(destination.jetty->id.eid.raw, destination.segment->seg.ubva.eid.raw, URMA_EID_SIZE) != 0) {
        return Error(WriteRndvErrorCode::kWrongIdentity);
    }
    const auto& segment = destination.segment->seg;
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (!segment.len || segment.ubva.va > maximum - segment.len ||
        grant.target_address > maximum - grant.transfer.bytes || grant.target_address < segment.ubva.va ||
        grant.target_address - segment.ubva.va > segment.len ||
        grant.transfer.bytes > segment.len - (grant.target_address - segment.ubva.va)) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    std::uint64_t total = 0;
    // 在派生责任前校验全部源区间；同一 Lease 的多个 slice 可以共享原始排队 pin。
    for (std::size_t i = 0; i < sources.size(); ++i) {
        const auto& source = sources[i];
        if (source.lease.allocator != &slab || !source.bytes || source.bytes > grant.transfer.bytes - total) {
            return Error(WriteRndvErrorCode::kInvalidArgument);
        }
        if (pins.empty()) {
            const auto view = slab.Readable(source.lease);
            if (!view || source.offset > view->size() || source.bytes > view->size() - source.offset) {
                return Error(WriteRndvErrorCode::kInvalidArgument);
            }
        } else {
            // 单个源 slice 可大于单 SGE 的 32 位长度；端点核对覆盖完整区间并避免加法溢出。
            if (source.offset > std::numeric_limits<std::size_t>::max() - source.bytes ||
                !slab.Sge(pins[i], sender.context(), source.offset, 1) ||
                !slab.Sge(pins[i], sender.context(), source.offset + source.bytes - 1, 1)) {
                return Error(WriteRndvErrorCode::kInvalidArgument);
            }
        }
        total += source.bytes;
    }
    if (total != grant.transfer.bytes) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    state.destination = destination;
    state.transfer = expected_transfer;
    auto clear_plan = [&]() noexcept {
        for (std::size_t i = 0; i < state.stats.planned; ++i) {
            state.chunks[i] = {};
        }
        state.stats = {};
        state.destination = {};
        state.transfer = {};
        state.next_submit = 0;
    };
    const auto chunk_limit = std::min({config.chunk_bytes, raw::TxSender::kMaxWriteSize, pool->max_write_size()});
    const auto sge_limit = std::min<std::size_t>(kMaxSources, pool->max_send_sge());
    std::size_t source_index = 0;
    std::size_t source_offset = 0;
    std::uint64_t planned_bytes = 0;
    while (planned_bytes < total) {
        if (state.stats.planned == config.max_chunks) {
            clear_plan();
            return Error(WriteRndvErrorCode::kInvalidArgument);
        }
        auto& chunk = state.chunks[state.stats.planned++];
        chunk.remote = {};
        chunk.remote.addr = grant.target_address + planned_bytes;
        chunk.remote.tseg = destination.segment;
        while (chunk.source_count < sge_limit && chunk.remote.len < chunk_limit && source_index < sources.size()) {
            const auto& source = sources[source_index];
            const auto bytes = static_cast<std::size_t>(
                std::min<std::uint64_t>(source.bytes - source_offset, chunk_limit - chunk.remote.len));
            const auto i = chunk.source_count++;
            chunk.sources[i].lease = source.lease;
            chunk.sources[i].pin = pins.empty() ? memory::BufferReference{} : pins[source_index];
            chunk.sources[i].offset = source.offset + source_offset;
            chunk.sges[i].len = static_cast<std::uint32_t>(bytes);
            chunk.remote.len += static_cast<std::uint32_t>(bytes);
            planned_bytes += bytes;
            source_offset += bytes;
            if (source_offset == source.bytes) {
                source_offset = 0;
                ++source_index;
            }
        }
    }
    state.stats.queued = state.stats.planned;
    auto rollback = [&](WriteRndvErrorCode code) -> std::expected<void, WriteRndvError> {
        if (!CancelQueued()) {
            state_->stats.failed = true;
            return Error(WriteRndvErrorCode::kRetained);
        }
        clear_plan();
        return Error(code);
    };
    for (std::size_t index = 0; index < state_->stats.planned; ++index) {
        auto& chunk = state_->chunks[index];
        for (std::size_t i = 0; i < chunk.source_count; ++i) {
            auto& source = chunk.sources[i];
            const auto reference = pins.empty() ? slab.Reserve(source.lease, memory::BufferUse::kLocalDma)
                                                : slab.RetainLocalDma(source.lease, source.pin);
            if (!reference) {
                return rollback(reference.error().code == memory::MemoryErrorCode::kExhausted
                                    ? WriteRndvErrorCode::kWouldBlock
                                    : WriteRndvErrorCode::kMemory);
            }
            source.dma = *reference;
            const auto sge = slab.Sge(source.dma, sender.context(), source.offset, chunk.sges[i].len);
            if (!sge) {
                return rollback(WriteRndvErrorCode::kMemory);
            }
            chunk.sges[i] = *sge;
        }
        chunk.metadata = {.connection_id = config.connection_id,
                          .operation_id = index + 1,
                          .operation_generation = config.transaction_generation,
                          .target = destination.jetty,
                          .opcode = URMA_OPC_WRITE,
                          .buffer_lease = chunk.sources[0].dma.sequence,
                          .grant_lease = grant.generation,
                          .signaled = true};
    }
    state_->initialized = true;
    return {};
}

std::expected<void, WriteRndvError> WriteRndvEngine::CommitChunk(Chunk& chunk) noexcept {
    bool valid = true;
    for (std::size_t i = 0; i < chunk.source_count; ++i) {
        auto& source = chunk.sources[i];
        if (source.dma.sequence && !source.committed) {
            if (state_->slab->Commit(source.dma)) {
                source.committed = true;
            } else {
                valid = false;
            }
        }
    }
    return valid ? std::expected<void, WriteRndvError>{} : Error(WriteRndvErrorCode::kRetained);
}
void WriteRndvEngine::Quarantine(Chunk& chunk) noexcept {
    for (std::size_t i = 0; i < chunk.source_count; ++i) {
        if (chunk.sources[i].dma.sequence) {
            // 标记失败不删除原责任；宿主仍需保留全部底层依赖。
            (void)state_->slab->Quarantine(chunk.sources[i].lease);
        }
    }
}
std::expected<void, WriteRndvError> WriteRndvEngine::CancelQueued() noexcept {
    bool valid = true;
    for (std::size_t index = 0; index < state_->stats.planned; ++index) {
        auto& chunk = state_->chunks[index];
        if (chunk.phase != Phase::kQueued) {
            continue;
        }
        bool cleared = true;
        for (std::size_t i = 0; i < chunk.source_count; ++i) {
            auto& source = chunk.sources[i];
            if (!source.dma.sequence) {
                continue;
            }
            if (!source.committed && state_->slab->Cancel(source.dma)) {
                source.dma = {};
            } else {
                cleared = false;
            }
        }
        if (cleared) {
            chunk.phase = Phase::kCancelled;
            --state_->stats.queued;
        } else {
            valid = false;
        }
    }
    return valid ? std::expected<void, WriteRndvError>{} : Error(WriteRndvErrorCode::kRetained);
}
std::expected<std::size_t, WriteRndvError> WriteRndvEngine::Progress() noexcept {
    if (!state_ || !state_->initialized) {
        return Error(WriteRndvErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stats.cancelled || state.stats.failed) {
        return Error(WriteRndvErrorCode::kStopped);
    }
    if (!state.stats.queued) {
        return std::size_t{0};
    }
    const auto count = std::min(state.stats.queued, state.post_limit);
    for (std::size_t i = 0; i < count; ++i) {
        auto& chunk = state.chunks[state.next_submit + i];
        for (std::size_t j = 0; j < chunk.source_count; ++j) {
            const auto& source = chunk.sources[j];
            // 应用可以已归还 CPU；Reserved 引用仍允许核对注册、范围、停止和隔离状态。
            const auto sge = state.slab->Sge(source.dma, state.sender->context(), source.offset, chunk.sges[j].len);
            if (!sge) {
                state.stats.failed = true;
                if (!CancelQueued()) {
                    return Error(WriteRndvErrorCode::kRetained);
                }
                return Error(WriteRndvErrorCode::kMemory);
            }
            chunk.sges[j] = *sge;
        }
        state.requests[i] = {.metadata = chunk.metadata,
                             .sges = std::span(chunk.sges).first(chunk.source_count),
                             .remote = chunk.remote};
        state.ids[i] = 0;
    }
    ++state.stats.post_calls;
    const auto sent = state.sender->Send(std::span(state.requests).first(count), std::span(state.ids).first(count),
                                         state.config.lane);
    const auto accepted = sent ? std::optional<std::size_t>(*sent) : sent.error().accepted;
    const bool unknown =
        !accepted || *accepted > count || (!sent && sent.error().code == raw::TxSendErrorCode::kAccountingFailure);
    const auto retained = unknown ? count : *accepted;
    bool committed = true;
    for (std::size_t i = 0; i < retained; ++i) {
        auto& chunk = state.chunks[state.next_submit + i];
        chunk.id = state.ids[i];
        chunk.phase = Phase::kPosted;
        ++state.stats.in_flight;
        --state.stats.queued;
        if (!CommitChunk(chunk)) {
            committed = false;
        }
        if (unknown || !committed) {
            Quarantine(chunk);
        }
    }
    state.next_submit += retained;
    if (accepted && *accepted <= count) {
        state.stats.accepted += *accepted;
    }
    if (unknown || !committed) {
        state.stats.failed = true;
        state.stats.acceptance_known = false;
        for (std::size_t i = state.next_submit - retained; i < state.next_submit; ++i) {
            Quarantine(state.chunks[i]);
        }
        (void)CancelQueued();
        return Error(WriteRndvErrorCode::kAcceptanceUnknown, sent ? 0 : sent.error().provider_status);
    }
    if (!sent && !Retryable(sent.error())) {
        state.stats.failed = true;
        if (!CancelQueued()) {
            return Error(WriteRndvErrorCode::kRetained, sent.error().provider_status);
        }
        return Error(WriteRndvErrorCode::kPost, sent.error().provider_status);
    }
    if (!sent && !*accepted) {
        state.post_limit = std::max<std::size_t>(1, count / 2);
        return Error(WriteRndvErrorCode::kWouldBlock, sent.error().provider_status);
    }
    return *accepted;
}
std::expected<void, WriteRndvError> WriteRndvEngine::FinishChunk(Chunk& chunk) noexcept {
    if (!chunk.terminal_seen || chunk.phase != Phase::kPosted) {
        return Error(WriteRndvErrorCode::kInvalidCompletion);
    }
    // 可信终结已保存，即使部分引用清理失败，后续只能重试尚存引用，不能重新提交该分块。
    if (!CommitChunk(chunk)) {
        return Error(WriteRndvErrorCode::kRetained);
    }
    bool valid = true;
    for (std::size_t i = 0; i < chunk.source_count; ++i) {
        auto& source = chunk.sources[i];
        if (!source.dma.sequence) {
            continue;
        }
        if (state_->slab->CompleteDma(source.dma)) {
            source.dma = {};
            source.committed = false;
        } else {
            valid = false;
        }
    }
    if (!valid) {
        return Error(WriteRndvErrorCode::kRetained);
    }
    chunk.phase = Phase::kRetired;
    --state_->stats.in_flight;
    ++state_->stats.retired;
    if (chunk.terminal_status == URMA_CR_SUCCESS) {
        ++state_->stats.successful;
        state_->stats.successful_bytes += chunk.remote.len;
    }
    return {};
}
std::expected<void, WriteRndvError> WriteRndvEngine::OnCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_ || !state_->initialized) {
        return Error(WriteRndvErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (event.kind != raw::TxCompletionKind::kCompleted || !event.record || !event.lane ||
        event.completion.flag.bs.s_r || !event.completion.flag.bs.jetty || !IsTerminal(event.completion.status) ||
        event.completion.user_ctx != event.record->id || !SameLane(*event.lane, state.config.lane) ||
        !SameLane(event.record->ticket.lane, state.config.lane) || !event.record->metadata.operation_id ||
        event.record->metadata.operation_id > state.stats.planned) {
        return Error(WriteRndvErrorCode::kInvalidCompletion);
    }
    auto& chunk = state.chunks[event.record->metadata.operation_id - 1];
    if (chunk.phase != Phase::kPosted || !chunk.id || chunk.id != event.record->id ||
        !SameMetadata(chunk.metadata, event.record->metadata) ||
        (chunk.terminal_seen &&
         (chunk.terminal_status != event.completion.status || chunk.terminal_error != event.error.has_value()))) {
        return Error(WriteRndvErrorCode::kInvalidCompletion);
    }
    chunk.terminal_seen = true;
    chunk.terminal_status = event.completion.status;
    chunk.terminal_error = event.error.has_value();
    const bool failed = chunk.terminal_status != URMA_CR_SUCCESS || chunk.terminal_error;
    if (failed) {
        state.stats.failed = true;
    }
    const auto finished = FinishChunk(chunk);
    if (!finished) {
        state.stats.failed = true;
        (void)CancelQueued();
        return finished;
    }
    if (failed) {
        if (!CancelQueued()) {
            return Error(WriteRndvErrorCode::kRetained);
        }
        return Error(WriteRndvErrorCode::kCompletion, event.completion.status);
    }
    return {};
}
std::expected<void, WriteRndvError> WriteRndvEngine::Cancel() noexcept {
    if (!state_) {
        return {};
    }
    state_->stats.cancelled = true;
    return CancelQueued();
}
std::expected<GrantTerminalEvidence, WriteRndvError> WriteRndvEngine::TerminalEvidence() const noexcept {
    if (!state_ || !state_->initialized) {
        return Error(WriteRndvErrorCode::kNotReady);
    }
    const auto& state = *state_;
    if (!state.stats.acceptance_known) {
        return Error(WriteRndvErrorCode::kAcceptanceUnknown);
    }
    if (state.stats.queued || state.stats.in_flight || state.stats.retired != state.stats.accepted) {
        return Error(WriteRndvErrorCode::kInUse);
    }
    return GrantTerminalEvidence{.identity = state.destination.grant,
                                 .no_future_writes = true,
                                 .acceptance_known = true,
                                 .accepted_writes = state.stats.accepted,
                                 .terminal_writes = state.stats.retired,
                                 .successful_writes = state.stats.successful,
                                 .completed_bytes = state.stats.successful_bytes};
}
std::expected<GrantTerminalEvidence, WriteRndvError> WriteRndvEngine::DoneEvidence() const noexcept {
    const auto terminal = TerminalEvidence();
    if (!terminal) {
        return terminal;
    }
    const auto& state = *state_;
    if (state.stats.cancelled || state.stats.failed || state.stats.accepted != state.stats.planned ||
        state.stats.successful != state.stats.planned || state.stats.successful_bytes != state.transfer.bytes) {
        return Error(WriteRndvErrorCode::kStopped);
    }
    return terminal;
}
std::expected<void, WriteRndvError> WriteRndvEngine::Close() noexcept {
    if (!state_) {
        return {};
    }
    const auto cancelled = Cancel();
    const auto finished = RetryCompleted();
    if (!cancelled || !finished) {
        return Error(WriteRndvErrorCode::kRetained);
    }
    if (state_->stats.in_flight) {
        return Error(WriteRndvErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
WriteRndvStats WriteRndvEngine::stats() const noexcept {
    return state_ ? state_->stats : WriteRndvStats{};
}
std::expected<void, WriteRndvError> WriteRndvEngine::RetryCompleted() noexcept {
    if (!state_) {
        return Error(WriteRndvErrorCode::kNotReady);
    }
    bool finished = true;
    // 只重试已经保存的逐 WR 终结事实，不以取消或空 CQ 推导未见到的完成。
    for (std::size_t i = 0; i < state_->stats.planned; ++i) {
        auto& chunk = state_->chunks[i];
        if (chunk.phase == Phase::kPosted && chunk.terminal_seen && !FinishChunk(chunk)) {
            finished = false;
        }
    }
    return finished ? std::expected<void, WriteRndvError>{} : Error(WriteRndvErrorCode::kRetained);
}
std::expected<void, WriteRndvError> WriteRndvEngine::CanRecycle() const noexcept {
    if (!state_) {
        return Error(WriteRndvErrorCode::kNotReady);
    }
    if (!state_->stats.acceptance_known) {
        return Error(WriteRndvErrorCode::kAcceptanceUnknown);
    }
    if (state_->stats.queued || state_->stats.in_flight || state_->stats.retired != state_->stats.accepted) {
        return Error(WriteRndvErrorCode::kInUse);
    }
    // 计数之外再确认真实引用已清空；失败 Start 的部分回滚不能被槽复用掩盖。
    for (std::size_t i = 0; i < state_->stats.planned; ++i) {
        const auto& chunk = state_->chunks[i];
        for (std::size_t j = 0; j < chunk.source_count; ++j) {
            if (chunk.sources[j].dma.sequence) {
                return Error(WriteRndvErrorCode::kRetained);
            }
        }
    }
    return {};
}
std::expected<void, WriteRndvError> WriteRndvEngine::Recycle(std::uint64_t generation) noexcept {
    if (auto ready = CanRecycle(); !ready) {
        return ready;
    }
    auto& state = *state_;
    if (generation <= state.config.transaction_generation) {
        return Error(WriteRndvErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < state.stats.planned; ++i) {
        state.chunks[i] = {};
    }
    state.config.transaction_generation = generation;
    state.transfer = {};
    state.destination = {};
    state.requests = {};
    state.ids = {};
    state.stats = {};
    state.next_submit = 0;
    state.post_limit = state.config.post_budget;
    state.initialized = false;
    return {};
}
} // namespace connection
} // namespace kbsocket
