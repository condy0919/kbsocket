// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/sq_isolation_test/sq_isolation_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr auto kFaultGeneration = std::numeric_limits<std::uint64_t>::max();

auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
int PoolError(const raw::JettyPoolFailure& error) noexcept {
    return error.provider_error ? error.provider_error : static_cast<int>(error.code);
}
int CompletionError(const raw::TxCompletionError& error) noexcept {
    return error.pool_error ? PoolError(*error.pool_error) : static_cast<int>(error.code);
}
int RxError(const raw::RxBufferError& error) noexcept {
    return error.provider_error ? error.provider_error : static_cast<int>(error.code);
}
bool SameLane(raw::JettyLane a, raw::JettyLane b) noexcept {
    return a.pool == b.pool && a.index == b.index && a.epoch == b.epoch;
}
bool SameEndpoint(const urma_jetty_id_t& a, const urma_jetty_id_t& b) noexcept {
    return a.id == b.id && std::memcmp(a.eid.raw, b.eid.raw, sizeof(a.eid.raw)) == 0;
}
std::uint64_t Generation(IsolationPhase phase, std::uint32_t round) noexcept {
    return (static_cast<std::uint64_t>(phase) << 32) | round;
}
} // namespace

std::size_t SqIsolationSession::Slot(std::uint32_t lane, std::uint32_t index) const noexcept {
    return static_cast<std::size_t>(lane) * options_.batch + index;
}

std::expected<void, ToolError> SqIsolationSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                        const SqIsolationOptions& options, bool server) {
    if (ctx_) {
        return Error("session already open", EBUSY);
    }
    auto valid = ValidateIsolationOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || options.bytes > cap.max_msg_size) {
        return Error("message exceeds device capability", EINVAL);
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return Error("query page size", EINVAL);
    }
    options_ = options;
    local_ = {.options = options, .server = server};
    peer_ = {};
    stats_ = {};
    started_ = false;
    fault_started_ = false;
    drained_ = false;
    peer_quiescent_ = true;
    unsafe_ = false;
    ctx_ = ctx;
    ids_.fill(0);
    tx_seen_.fill(false);
    rx_seen_.fill(false);
    const auto total = 2 * options.batch;
    const auto page = static_cast<std::size_t>(page_size);
    const auto bytes = static_cast<std::size_t>(total) * options.bytes;
    const auto aligned = (bytes + page - 1) / page * page;
    // 两条 SQ 的 TX 区域始终分离；坏 SQ 未排空时健康 SQ 仍可复用自己的 buffer。
    storage_.resize(aligned + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
    registered_ = std::span(storage_).subspan((page - address % page) % page, aligned);

    raw::JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_depth = options.batch;
    config.rx_depth = total;
    config.tx_cq_depth = 2 * (options.batch + 1);
    config.rx_cq_depth = total;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("create main jetty pool (check queue limits)", PoolError(opened.error().failure));
    }
    for (std::uint32_t i = 0; i < 2; ++i) {
        auto ticket = pool_.Reserve();
        if (!ticket) {
            return Error("reserve main endpoint", PoolError(ticket.error()));
        }
        auto jetty = pool_.Get(ticket->lane);
        auto cancelled = pool_.Cancel(*ticket);
        if (!jetty || !cancelled) {
            return Error("read main endpoint", EIO);
        }
        lanes_[i] = ticket->lane;
        local_.endpoints[i] = (*jetty)->jetty_id;
    }
    // sink 只提供合法远端端点，不注册或投递 RX buffer，避免故障流量消耗健康 JFR。
    config.jetty_count = 1;
    config.tx_depth = 1;
    config.rx_depth = 1;
    config.tx_cq_depth = 2;
    config.rx_cq_depth = 1;
    auto sink = sink_.Open(ctx, cap, config);
    if (!sink) {
        return Error("create isolated sink", PoolError(sink.error().failure));
    }
    auto ticket = sink_.Reserve();
    if (!ticket) {
        return Error("reserve sink endpoint", PoolError(ticket.error()));
    }
    auto jetty = sink_.Get(ticket->lane);
    auto cancelled = sink_.Cancel(*ticket);
    if (!jetty || !cancelled) {
        return Error("read sink endpoint", EIO);
    }
    local_.endpoints[2] = (*jetty)->jetty_id;
    auto rx = rx_.Open(pool_);
    if (!rx) {
        return Error("open shared RX buffers", RxError(rx.error()));
    }
    urma_seg_cfg_t segment{};
    segment.va = reinterpret_cast<std::uintptr_t>(registered_.data());
    segment.len = registered_.size();
    segment.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &segment);
    if (!segment_) {
        return Error("register TX memory", errno ? errno : EIO);
    }
    for (std::size_t i = 0; i < total; ++i) {
        sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(registered_.data() + i * options.bytes),
                    .len = options.bytes,
                    .tseg = segment_};
    }
    // 多留一个账本槽，发送门禁探针必须被 SQ 状态拒绝，不能碰巧被账本满拒绝。
    auto ledger = ledger_.Open(pool_, total + 1);
    if (!ledger) {
        return Error("open TX ledger", static_cast<int>(ledger.error().code));
    }
    auto sender = sender_.Open(ledger_);
    if (!sender) {
        return Error("open TX sender", static_cast<int>(sender.error().code));
    }
    ready_ = true;
    return {};
}

std::expected<void, ToolError> SqIsolationSession::Run(ControlChannel& channel) {
    if (!ready_ || started_) {
        return Error("session not ready or already run", EINVAL);
    }
    started_ = true;
    auto peer = ExchangeIsolationHello(channel, local_);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    peer_ = *peer;
    for (std::size_t i = 0; i < remotes_.size(); ++i) {
        urma_rjetty_t remote{};
        remote.jetty_id = peer_.endpoints[i + 1];
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remotes_[i] = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!remotes_[i]) {
            return Error("import isolation peer endpoint", errno ? errno : EIO);
        }
    }
    auto before = RunRound(channel, IsolationPhase::kBefore, 0);
    if (!before) {
        return before;
    }
    for (std::uint32_t round = 0; round < options_.rounds; ++round) {
        auto during = RunRound(channel, IsolationPhase::kDuring, round);
        if (!during) {
            return during;
        }
    }
    // 健康轮次不等待坏 SQ 的账本清空；完成规定轮次后再收敛剩余排空工作。
    while (!drained_) {
        auto tx = ProgressTx();
        auto rx = PollRx();
        if (!tx) {
            return std::unexpected(tx.error());
        }
        if (!rx) {
            return std::unexpected(rx.error());
        }
        if (!*tx && !*rx) {
            std::this_thread::yield();
        }
    }
    auto drained = IsolationBarrier(channel, 0x60000000, options_.timeout_ms);
    if (!drained) {
        return drained;
    }
    for (std::uint32_t round = 0; round < kIsolationAfterRounds; ++round) {
        auto after = RunRound(channel, IsolationPhase::kAfter, round);
        if (!after) {
            return after;
        }
    }
    const std::array<std::uint64_t, 3> expected{2ull * options_.batch,
                                                static_cast<std::uint64_t>(options_.rounds) * options_.batch,
                                                static_cast<std::uint64_t>(kIsolationAfterRounds) * options_.batch};
    for (std::size_t i = 0; i < expected.size(); ++i) {
        const auto& traffic = stats_.traffic[i];
        if (traffic.accepted != expected[i] || traffic.retired != expected[i] || traffic.received != expected[i]) {
            return Error("phase TX/RX totals mismatch", EPROTO);
        }
    }
    if (!stats_.accepted_known || stats_.fault.accepted != options_.batch ||
        stats_.fault.retired != stats_.fault.accepted || stats_.fault.flush_done != 1 ||
        stats_.fault.rejected_sends != 3 || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("isolation retirement or ownership mismatch", EPROTO);
    }
    // 双方都结束健康收发和故障排空后，sink 才能和主池一起销毁。
    auto done = IsolationBarrier(channel, 0x60000001, options_.timeout_ms);
    if (!done) {
        return done;
    }
    peer_quiescent_ = true;
    return {};
}

std::expected<void, ToolError> SqIsolationSession::RunRound(ControlChannel& channel, IsolationPhase phase,
                                                            std::uint32_t round) {
    phase_ = phase;
    round_ = round;
    rx_seen_.fill(false);
    const bool baseline = phase == IsolationPhase::kBefore;
    // 故障槽的 id/退休位跨 during 各轮保留，健康轮次不能抹去尚未完成的坏 SQ 记录。
    const std::size_t first = baseline || !fault_started_ ? 0 : Slot(1, 0);
    std::fill(tx_seen_.begin() + first, tx_seen_.end(), false);
    const auto count = (baseline ? 2 : 1) * options_.batch;
    auto& traffic = stats_.traffic[static_cast<std::size_t>(phase)];
    const auto tx_end = traffic.retired + count;
    const auto rx_end = traffic.received + count;
    auto healthy = CheckHealthy();
    if (!healthy) {
        return healthy;
    }
    for (std::size_t posted = 0; posted < count;) {
        const auto requested = std::min<std::size_t>(count - posted, raw::RxBufferPool::kMaxBatch);
        auto filled = rx_.Refill(requested);
        if (!filled) {
            return Error("post shared RX buffers", RxError(filled.error()));
        }
        if (*filled != requested) {
            return Error("insufficient shared RX capacity", ENOBUFS);
        }
        posted += *filled;
    }
    peer_quiescent_ = false;
    const auto marker = 0x10000000u + (static_cast<std::uint32_t>(phase) << 24) + 2 * round;
    auto ready = IsolationBarrier(channel, marker, options_.timeout_ms);
    if (!ready) {
        return ready;
    }
    if (baseline) {
        auto bad_baseline = Post(0, false);
        if (!bad_baseline) {
            return bad_baseline;
        }
    } else if (!fault_started_) {
        auto fault = StartFault();
        if (!fault) {
            return fault;
        }
    }
    auto posted = Post(1, false);
    if (!posted) {
        return posted;
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while (traffic.retired < tx_end || traffic.received < rx_end) {
        if (Clock::now() >= deadline) {
            return Error("healthy TX/RX completion timeout", ETIMEDOUT);
        }
        auto tx = ProgressTx();
        auto rx = PollRx();
        if (!tx) {
            return std::unexpected(tx.error());
        }
        if (!rx) {
            return std::unexpected(rx.error());
        }
        if (!*tx && !*rx) {
            std::this_thread::yield();
        }
    }
    if (traffic.retired != tx_end || traffic.received != rx_end || rx_.posted() || rx_.held()) {
        return Error("round completion/lease mismatch", EPROTO);
    }
    // 再 poll 一次以拒绝额外完成，且仍推进其他 SQ 的排空，不把健康批结束当成整个 CQ 结束。
    auto tx = ProgressTx();
    auto rx = PollRx();
    if (!tx) {
        return std::unexpected(tx.error());
    }
    if (!rx) {
        return std::unexpected(rx.error());
    }
    healthy = CheckHealthy();
    if (!healthy) {
        return healthy;
    }
    return IsolationBarrier(channel, marker + 1, options_.timeout_ms);
}

std::expected<void, ToolError> SqIsolationSession::Post(std::uint32_t lane, bool fault) {
    for (std::uint32_t i = 0; i < options_.batch; ++i) {
        const auto slot = Slot(lane, i);
        const IsolationFrame frame{phase_, lane, round_, i, local_.server};
        FillIsolationPayload(registered_.subspan(slot * options_.bytes, options_.bytes), frame);
        requests_[i] = {.metadata = {.connection_id = fault ? 2u : 1u,
                                     .operation_id = i + 1,
                                     .operation_generation = fault ? kFaultGeneration : Generation(phase_, round_),
                                     .target = remotes_[fault ? 1 : 0],
                                     .opcode = fault ? URMA_OPC_SEND : URMA_OPC_SEND_IMM,
                                     .buffer_lease = slot + 1},
                        .sges = std::span(&sges_[slot], 1),
                        .imm_data = IsolationImmediate(frame)};
    }
    auto sent = sender_.Send(std::span(requests_).first(options_.batch),
                             std::span(ids_).subspan(Slot(lane, 0), options_.batch), lanes_[lane]);
    const auto accepted = sent ? std::optional<std::size_t>(*sent) : sent.error().accepted;
    if (accepted) {
        if (fault) {
            stats_.fault.accepted += static_cast<std::uint32_t>(*accepted);
        } else {
            stats_.traffic[static_cast<std::size_t>(phase_)].accepted += *accepted;
        }
    } else {
        stats_.accepted_known = false;
    }
    if (!sent || *sent != options_.batch) {
        unsafe_ = true;
        return Error(
            "batch not fully accepted; retaining resources",
            sent ? EPROTO
                 : (sent.error().provider_status ? sent.error().provider_status : static_cast<int>(sent.error().code)));
    }
    return {};
}

std::expected<void, ToolError> SqIsolationSession::StartFault() {
    auto sent = Post(0, true);
    if (!sent) {
        return sent;
    }
    fault_started_ = true;
    drain_deadline_ = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    // 不先 poll，使切 ERROR 时仍有整批在途账本；健康 SQ 的新一批随后才提交。
    auto started = pool_.BeginDrain(lanes_[0]);
    if (!started) {
        return Error("modify fault SQ ERROR", PoolError(started.error()));
    }
    return CheckSendRejected();
}

std::expected<void, ToolError> SqIsolationSession::CheckSendRejected() {
    const auto before = ledger_.size();
    const raw::TxSendRequest probe{.metadata = {.target = remotes_[1]}, .sges = std::span(&sges_[0], 1)};
    std::array<raw::AttemptId, 1> ids{};
    auto sent = sender_.Send(std::span(&probe, 1), ids, lanes_[0]);
    if (sent || sent.error().accepted != 0 || sent.error().code != raw::TxSendErrorCode::kLedgerFailure ||
        !sent.error().ledger_error || !sent.error().ledger_error->pool_failure ||
        sent.error().ledger_error->pool_failure->code != raw::JettyPoolErrorCode::kFaulted || ids[0] ||
        ledger_.size() != before) {
        return Error("fault SQ did not reject send during reservation", EPROTO);
    }
    auto jetty = pool_.Get(lanes_[0]);
    if (jetty || jetty.error().code != raw::JettyPoolErrorCode::kFaulted) {
        return Error("fault SQ exposed a send handle", EPROTO);
    }
    ++stats_.fault.rejected_sends;
    return {};
}

std::expected<void, ToolError> SqIsolationSession::CheckHealthy() {
    auto state = pool_.lane_state(lanes_[1]);
    auto jetty = pool_.Get(lanes_[1]);
    if (!state || *state != raw::JettyLaneState::kReady || !jetty) {
        return Error("healthy SQ no longer READY", EPROTO);
    }
    ++stats_.healthy_ready_checks;
    return {};
}

std::expected<void, ToolError> SqIsolationSession::ObserveTx(const raw::TxCompletionEvent& event, bool software) {
    if (event.error) {
        return Error("invalid TX completion or drain operation", CompletionError(*event.error));
    }
    if (!event.lane || (!SameLane(*event.lane, lanes_[0]) && !SameLane(*event.lane, lanes_[1]))) {
        return Error("TX completion belongs to another pool", EPROTO);
    }
    const bool bad_lane = SameLane(*event.lane, lanes_[0]);
    if (event.kind == raw::TxCompletionKind::kFlushDone) {
        if (!fault_started_ || !bad_lane || software || stats_.fault.flush_done || event.record ||
            event.completion.user_ctx) {
            return Error("invalid fault SQ boundary", EPROTO);
        }
        ++stats_.fault.flush_done;
        return CheckSendRejected();
    }
    if (event.kind != raw::TxCompletionKind::kCompleted || !event.record) {
        return Error("unexpected completion kind", EPROTO);
    }
    const auto& record = *event.record;
    const bool fault = record.metadata.operation_generation == kFaultGeneration;
    if (software && (!fault || !bad_lane || event.completion.status != URMA_CR_WR_UNHANDLED)) {
        return Error("software flush returned another SQ or status", EPROTO);
    }
    if (fault && (!fault_started_ || !bad_lane || (!software && stats_.fault.flush_done) ||
                  (software && stats_.fault.flush_done != 1))) {
        return Error("fault completion violates hardware/software boundary", EPROTO);
    }
    // 边界只约束坏 SQ；共享 CQ 中排在边界之后的健康 CQE 仍须正常退休。
    if (!fault && (event.completion.status != URMA_CR_SUCCESS ||
                   record.metadata.operation_generation != Generation(phase_, round_) ||
                   (phase_ != IsolationPhase::kBefore && bad_lane))) {
        return Error("healthy completion status or generation mismatch", EPROTO);
    }
    const auto lane = bad_lane ? 0u : 1u;
    const auto operation = record.metadata.operation_id;
    if (!operation || operation > options_.batch) {
        return Error("TX operation index out of range", EPROTO);
    }
    const auto slot = Slot(lane, static_cast<std::uint32_t>(operation - 1));
    if (!SameLane(record.ticket.lane, *event.lane) || record.id != ids_[slot] || tx_seen_[slot] ||
        record.metadata.buffer_lease != slot + 1 || event.completion.user_ctx != record.id ||
        record.metadata.target != remotes_[fault ? 1 : 0] ||
        record.metadata.opcode != (fault ? URMA_OPC_SEND : URMA_OPC_SEND_IMM) ||
        record.metadata.connection_id != (fault ? 2u : 1u)) {
        return Error("duplicate or foreign TX attempt", EPROTO);
    }
    tx_seen_[slot] = true;
    if (!fault) {
        ++stats_.traffic[static_cast<std::size_t>(phase_)].retired;
        if (phase_ == IsolationPhase::kDuring && !drained_) {
            ++stats_.healthy_retired_before_drained;
        }
        return {};
    }
    auto& stats = stats_.fault;
    ++stats.retired;
    switch (event.completion.status) {
    case URMA_CR_SUCCESS:
        ++stats.success;
        break;
    case URMA_CR_WR_FLUSH_ERR:
        ++stats.flush_error;
        break;
    case URMA_CR_WR_UNHANDLED:
        ++stats.unhandled;
        break;
    case URMA_CR_LOC_ACCESS_ERR:
        ++stats.loc_access_error;
        break;
    case URMA_CR_REM_ACCESS_ABORT_ERR:
        ++stats.remote_access_abort_error;
        break;
    case URMA_CR_ACK_TIMEOUT_ERR:
        ++stats.ack_timeout_error;
        break;
    case URMA_CR_RNR_RETRY_CNT_EXC_ERR:
        ++stats.rnr_retry_count_exceeded_error;
        break;
    default:
        ++stats.other_error;
        stats.last_other_error = static_cast<int>(event.completion.status);
        break;
    }
    return {};
}

std::expected<std::size_t, ToolError> SqIsolationSession::ProgressTx() {
    if (fault_started_ && !drained_ && Clock::now() >= drain_deadline_) {
        return Error("fault SQ drain timeout", ETIMEDOUT);
    }
    auto polled = processor_.Poll(tx_events_);
    if (!polled) {
        unsafe_ = true;
        return Error("poll shared TX JFC", CompletionError(polled.error()));
    }
    std::optional<ToolError> failure;
    const auto observe = [&](std::size_t count, bool software) {
        for (const auto& event : std::span(tx_events_).first(count)) {
            auto result = ObserveTx(event, software);
            if (!result && !failure) {
                failure = result.error();
                stats_.failed_tx = event.completion;
            }
        }
    };
    std::size_t count = polled->count;
    observe(polled->count, false);
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    if (stats_.fault.flush_done && !drained_) {
        // 每次推进最多 flush 一个有界批次，不能让坏 SQ 排空饿死健康 RX。
        auto flushed = processor_.Flush(lanes_[0], tx_events_);
        if (!flushed) {
            unsafe_ = true;
            return Error("flush fault SQ", CompletionError(flushed.error()));
        }
        count += flushed->count;
        observe(flushed->count, true);
        if (failure) {
            unsafe_ = true;
            return std::unexpected(*failure);
        }
        if (!flushed->count) {
            auto state = pool_.lane_state(lanes_[0]);
            if (!state || *state != raw::JettyLaneState::kDrained || stats_.fault.retired != stats_.fault.accepted) {
                return Error("fault SQ not fully retired", EPROTO);
            }
            auto rejected = CheckSendRejected();
            if (!rejected) {
                return std::unexpected(rejected.error());
            }
            drained_ = true;
        }
    }
    return count;
}

std::expected<void, ToolError> SqIsolationSession::ValidateRx(const raw::RxBufferEvent& event) {
    const auto& cr = event.completion;
    if (event.error || !event.lease || cr.status != URMA_CR_SUCCESS) {
        return Error("invalid RX completion", cr.status ? static_cast<int>(cr.status) : EPROTO);
    }
    if (!cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
        event.payload.size() != options_.bytes) {
        return Error("RX opcode/direction/length mismatch", EPROTO);
    }
    auto frame = CheckIsolationPayload(event.payload);
    if (!frame) {
        return std::unexpected(frame.error());
    }
    if (frame->server == local_.server || frame->phase != phase_ || frame->round != round_ || frame->source >= 2 ||
        frame->index >= options_.batch || (phase_ != IsolationPhase::kBefore && frame->source != 1)) {
        return Error("RX phase/source/sequence mismatch", EPROTO);
    }
    if (cr.local_id != local_.endpoints[1].id || !SameEndpoint(cr.remote_id, peer_.endpoints[frame->source]) ||
        cr.imm_data != IsolationImmediate(*frame)) {
        return Error("RX endpoint/64-bit IMM mismatch", EPROTO);
    }
    const auto slot = Slot(frame->source, frame->index);
    if (rx_seen_[slot]) {
        return Error("duplicate RX message", EPROTO);
    }
    rx_seen_[slot] = true;
    ++stats_.traffic[static_cast<std::size_t>(phase_)].received;
    return {};
}

std::expected<std::size_t, ToolError> SqIsolationSession::PollRx() {
    auto count = rx_.Poll(rx_events_);
    if (!count) {
        unsafe_ = true;
        return Error("poll shared RX JFC", RxError(count.error()));
    }
    std::optional<ToolError> failure;
    for (const auto& event : std::span(rx_events_).first(*count)) {
        auto valid = ValidateRx(event);
        if (!valid && !failure) {
            failure = valid.error();
            stats_.failed_rx = event.completion;
        }
        // 一条校验错误不跳过同批其他 CQE；所有已经交出的 RX lease 都须归还。
        if (event.lease) {
            auto released = rx_.Release(*event.lease);
            if (!released && !failure) {
                failure = ToolError{"release RX lease", RxError(released.error())};
            }
        }
    }
    if (failure) {
        return std::unexpected(*failure);
    }
    return *count;
}

std::expected<void, ToolError> SqIsolationSession::Close() noexcept {
    if (unsafe_ || !peer_quiescent_ || ledger_.size() || rx_.posted() || rx_.held() || (fault_started_ && !drained_)) {
        return Error("DMA/peer state unresolved; retaining isolation session", EBUSY);
    }
    ready_ = false;
    auto sender = sender_.Close();
    if (!sender) {
        return Error("close TX sender", static_cast<int>(sender.error().code));
    }
    auto ledger = ledger_.Close();
    if (!ledger) {
        return Error("close TX ledger", static_cast<int>(ledger.error().code));
    }
    for (std::size_t i = remotes_.size(); i > 0; --i) {
        auto*& remote = remotes_[i - 1];
        if (remote) {
            const auto status = UrmaApi::UnimportJetty(remote);
            if (status != URMA_SUCCESS) {
                return Error("unimport isolation peer", status);
            }
            remote = nullptr;
        }
    }
    auto rx = rx_.Close();
    if (!rx) {
        return Error("close shared RX buffers", RxError(rx.error()));
    }
    auto sink = sink_.Close();
    if (!sink) {
        return Error("close sink pool", PoolError(sink.error()));
    }
    auto pool = pool_.Close();
    if (!pool) {
        return Error("close main pool", PoolError(pool.error()));
    }
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error("unregister TX memory", status);
        }
        segment_ = nullptr;
    }
    registered_ = {};
    std::vector<std::byte>().swap(storage_);
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
