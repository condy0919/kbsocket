// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/sq_multiplex_test/sq_multiplex_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <optional>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr std::uint32_t kStart = 0x4d580000;
constexpr std::uint32_t kRxReady = 0x4d580001;
constexpr std::uint32_t kDone = 0x4d580002;

auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
int PoolError(const raw::JettyPoolFailure& error) noexcept {
    return error.provider_error ? error.provider_error : static_cast<int>(error.code);
}
bool SameEndpoint(const urma_jetty_id_t& a, const urma_jetty_id_t& b) noexcept {
    return a.id == b.id && std::memcmp(a.eid.raw, b.eid.raw, sizeof(a.eid.raw)) == 0;
}
bool SameLane(raw::JettyLane a, raw::JettyLane b) noexcept {
    return a.pool == b.pool && a.index == b.index && a.epoch == b.epoch;
}
bool IsTerminal(urma_cr_status_t status) noexcept {
    // 排除伪 CQE 和软件 flush 记录后才能读取 user_ctx；异常状态不能用来释放 DMA 资源。
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
    case URMA_CR_LOC_DATA_POISON:
    case URMA_CR_REM_DATA_POISON:
        return true;
    default:
        return false;
    }
}
std::expected<urma_jetty_t*, ToolError> ReadEndpoint(raw::JettyPool& pool, raw::JettyLane& lane) {
    auto ticket = pool.Reserve();
    if (!ticket) {
        return Error("reserve local endpoint", PoolError(ticket.error()));
    }
    auto jetty = pool.Get(ticket->lane);
    auto cancelled = pool.Cancel(*ticket);
    if (!jetty || !cancelled) {
        return Error("read local endpoint", EIO);
    }
    lane = ticket->lane;
    return *jetty;
}
} // namespace

std::expected<void, ToolError> SqMultiplexSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                        const SqMultiplexOptions& options, bool server,
                                                        MultiplexFaultPeer* fault_peer) {
    if (ctx_) {
        return Error("session already open", EBUSY);
    }
    auto valid = ValidateMultiplexOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || options.bytes > cap.max_msg_size) {
        return Error("message exceeds device capability", EINVAL);
    }
    if ((server && options.fault == MultiplexFault::kPeerExit) != (fault_peer != nullptr)) {
        return Error("peer_exit server requires a disposable fault endpoint", EINVAL);
    }
    fault_peer_ = fault_peer;
    options_ = options;
    local_ = {.options = options, .server = server};
    peer_ = {};
    stats_ = {};
    if (!server) {
        stats_.rounds.resize(options.rounds);
    }
    started_ = false;
    peer_quiescent_ = true;
    unsafe_ = false;
    ctx_ = ctx;
    raw::JettyPoolConfig config;
    config.jetty_count = 1;
    config.tx_depth = 2 * options.batch;
    config.rx_depth = options.batch;
    config.tx_cq_depth = 2 * options.batch + 1;
    config.rx_cq_depth = options.batch;
    auto pool = pool_.Open(ctx, cap, config);
    if (!pool) {
        return Error("create multiplex pool (check device queue limits)", PoolError(pool.error().failure));
    }
    auto jetty = ReadEndpoint(pool_, lane_);
    if (!jetty) {
        return std::unexpected(jetty.error());
    }
    jetty_ = *jetty;
    local_.endpoints[0] = jetty_->jetty_id;
    stats_.jetty_id = jetty_->jetty_id.id;
    if (server) {
        // 两个目标必须使用独立 JFR，否则健康目标的 RX 也会被 sink 消费。
        if (fault_peer_) {
            local_.endpoints[1] = fault_peer_->endpoint();
        } else {
            config.tx_depth = config.rx_depth = config.rx_cq_depth = 1;
            config.tx_cq_depth = 2;
            auto sink = sink_.Open(ctx, cap, config);
            if (!sink) {
                return Error("create independent empty sink JFR", PoolError(sink.error().failure));
            }
            raw::JettyLane sink_lane;
            auto sink_jetty = ReadEndpoint(sink_, sink_lane);
            if (!sink_jetty) {
                return std::unexpected(sink_jetty.error());
            }
            local_.endpoints[1] = (*sink_jetty)->jetty_id;
        }
        auto rx = rx_.Open(pool_);
        if (!rx) {
            return Error("open healthy RX buffers",
                         rx.error().provider_error ? rx.error().provider_error : static_cast<int>(rx.error().code));
        }
    } else {
        const long page_size = ::sysconf(_SC_PAGESIZE);
        if (page_size <= 0) {
            return Error("query page size", EINVAL);
        }
        const auto page = static_cast<std::size_t>(page_size);
        const auto bytes = static_cast<std::size_t>(2 * options.batch) * options.bytes;
        const auto aligned = (bytes + page - 1) / page * page;
        storage_.resize(aligned + page - 1);
        const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
        registered_ = std::span(storage_).subspan((page - address % page) % page, aligned);
        urma_seg_cfg_t cfg{};
        cfg.va = reinterpret_cast<std::uintptr_t>(registered_.data());
        cfg.len = registered_.size();
        cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
        errno = 0;
        segment_ = UrmaApi::RegisterSeg(ctx, &cfg);
        if (!segment_) {
            return Error("register TX memory", errno ? errno : EIO);
        }
        for (std::uint32_t i = 0; i < 2 * options.batch; ++i) {
            sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(registered_.data() + i * options.bytes),
                        .len = options.bytes,
                        .tseg = segment_};
        }
        auto ledger = ledger_.Open(pool_, 2 * options.batch);
        if (!ledger) {
            return Error("open TX ledger", static_cast<int>(ledger.error().code));
        }
        auto sender = sender_.Open(ledger_);
        if (!sender) {
            return Error("open TX sender", static_cast<int>(sender.error().code));
        }
    }
    ready_ = true;
    return {};
}

std::expected<void, ToolError> SqMultiplexSession::Run(ControlChannel& channel) {
    if (!ready_ || started_) {
        return Error("session not ready or already run", EINVAL);
    }
    started_ = true;
    auto peer = ExchangeMultiplexHello(channel, local_);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    peer_ = *peer;
    if (!local_.server) {
        for (std::size_t i = 0; i < remotes_.size(); ++i) {
            urma_rjetty_t cfg{};
            cfg.jetty_id = peer_.endpoints[i];
            cfg.trans_mode = URMA_TM_RM;
            cfg.type = URMA_JETTY;
            cfg.tp_type = URMA_CTP;
            urma_token_t token{};
            errno = 0;
            remotes_[i] = UrmaApi::ImportJetty(ctx_, &cfg, &token);
            if (!remotes_[i]) {
                return Error("import RM_CTP target", errno ? errno : EIO);
            }
        }
        auto state = CheckState();
        if (!state) {
            return state;
        }
    }
    peer_quiescent_ = false;
    auto start = MultiplexBarrier(channel, kStart, options_.timeout_ms);
    if (!start) {
        return start;
    }
    if (options_.fault == MultiplexFault::kPeerExit) {
        auto baseline = RunFaultBaseline(channel);
        if (!baseline) {
            return baseline;
        }
    }
    for (round_ = 0; round_ < options_.rounds; ++round_) {
        for (auto phase : {MultiplexPhase::kBefore, MultiplexPhase::kMixed, MultiplexPhase::kAfter}) {
            phase_ = phase;
            auto result = RunPhase(channel);
            if (!result) {
                return result;
            }
        }
    }
    const auto expected = static_cast<std::uint64_t>(options_.batch) * options_.rounds;
    for (const auto& healthy : stats_.healthy) {
        if (local_.server
                ? healthy.received != expected
                : (healthy.accepted != expected || healthy.retired != expected || healthy.success != expected)) {
            return Error("healthy traffic totals mismatch", EPROTO);
        }
    }
    const auto fault_count =
        options_.fault == MultiplexFault::kPeerExit ? stats_.sink.remote_access_abort_error : stats_.sink.rnr;
    const auto baseline_checks = options_.fault == MultiplexFault::kPeerExit ? 1u : 0u;
    if (!local_.server &&
        (stats_.sink.accepted != expected || stats_.sink.retired != expected || fault_count != expected ||
         stats_.state_checks != 1 + baseline_checks + 3 * options_.rounds)) {
        return Error("sink traffic or same-SQ state checks mismatch", EPROTO);
    }
    if (!stats_.accepted_known || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("final DMA ownership not fully returned", EBUSY);
    }
    auto done = MultiplexBarrier(channel, kDone, options_.timeout_ms);
    if (!done) {
        return done;
    }
    peer_quiescent_ = true;
    return {};
}

std::expected<void, ToolError> SqMultiplexSession::RunFaultBaseline(ControlChannel& channel) {
    phase_ = MultiplexPhase::kBaseline;
    round_ = 0;
    tx_seen_.fill(false);
    ids_.fill(0);
    phase_error_.reset();
    std::expected<void, ToolError> result;
    if (!local_.server) {
        result = Send();
        if (result) {
            result = FinishTx();
        }
        if (result) {
            result = CheckState();
        }
        if (result && phase_error_) {
            result = std::unexpected(*phase_error_);
        }
    }
    const MultiplexPhaseResult local{
        .phase_id = 3000,
        .success = result.has_value(),
        .tx_quiescent = !unsafe_ && stats_.accepted_known && ledger_.size() == 0,
        .error_code = result ? 0 : result.error().code,
    };
    auto peer = ExchangeMultiplexPhaseResult(channel, local, 3 * options_.timeout_ms);
    if (!peer) {
        return result ? std::unexpected(peer.error()) : result;
    }
    if (!result || !peer->success) {
        peer_quiescent_ = local.tx_quiescent && peer->tx_quiescent;
        return result ? Error("peer rejected fault baseline/exit confirmation", peer->error_code) : result;
    }
    // 首个结果握手证明客户端 baseline 已成功完成；之后才允许校验子进程 RX 并 kill，
    // 避免故障注入抢在 baseline 的成功 CQE 前面，使基线自身被进程退出干扰。
    if (local_.server) {
        result = fault_peer_->FinishBaselineAndExit(peer_.endpoints[0]);
        if (result) {
            stats_.fault_baseline.received = 1;
        }
    }
    const MultiplexPhaseResult exited{
        .phase_id = 3001,
        .success = result.has_value(),
        .tx_quiescent = true,
        .error_code = result ? 0 : result.error().code,
    };
    peer = ExchangeMultiplexPhaseResult(channel, exited, 3 * options_.timeout_ms);
    if (!peer) {
        return result ? std::unexpected(peer.error()) : result;
    }
    if (!result || !peer->success) {
        peer_quiescent_ = peer->tx_quiescent;
        return result ? Error("peer rejected exit confirmation", peer->error_code) : result;
    }
    // 只有收到监督者的成功结果才允许复用已导入的旧 endpoint 发 mixed 流量。
    stats_.peer_exit_confirmed = true;
    return {};
}

std::expected<void, ToolError> SqMultiplexSession::RunPhase(ControlChannel& channel) {
    tx_seen_.fill(false);
    rx_seen_.fill(false);
    ids_.fill(0);
    phase_received_ = mixed_healthy_ = mixed_rnr_ = 0;
    phase_error_.reset();
    bool rx_ready = false;
    const auto result = TransferPhase(channel, rx_ready);
    if (!rx_ready) {
        // 对端还在读取 READY marker 时不能混入阶段结果，否则控制流会错位。
        return result;
    }
    const MultiplexPhaseResult local{
        .phase_id = round_ * 3 + static_cast<std::uint32_t>(phase_),
        .success = result.has_value(),
        .tx_quiescent = !unsafe_ && stats_.accepted_known && ledger_.size() == 0,
        .error_code = result ? 0 : result.error().code,
    };
    // 接收可能比 TX 重试早结束。留出对端数据超时和结果传输的预算，不用断 TCP 传递失败。
    const auto peer = ExchangeMultiplexPhaseResult(channel, local, 2 * options_.timeout_ms);
    if (!peer) {
        // 通知失败不覆盖本地已知的 CQE/超时原因，也不能被当作远端停止 DMA 的证明。
        return result ? std::unexpected(peer.error()) : result;
    }
    if (!result || !peer->success) {
        // 双方都看到失败才停止后续阶段；只有明确的 TX 终结证明允许拆除依赖链。
        // 本地剩余 RX、未知 CQE 等情况仍由 Close 的所有权检查拒绝回收。
        peer_quiescent_ = local.tx_quiescent && peer->tx_quiescent;
        return result ? Error("peer rejected SQ multiplex phase", peer->error_code) : result;
    }
    return {};
}

std::expected<void, ToolError> SqMultiplexSession::TransferPhase(ControlChannel& channel, bool& rx_ready) {
    if (local_.server) {
        auto posted = rx_.Refill(options_.batch);
        if (!posted) {
            return Error("post healthy RX buffers", posted.error().provider_error
                                                        ? posted.error().provider_error
                                                        : static_cast<int>(posted.error().code));
        }
        if (*posted != options_.batch) {
            return Error("insufficient healthy RX buffers", ENOBUFS);
        }
        auto ready = channel.SendMarker(kRxReady, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        rx_ready = true;
        auto received = Receive();
        if (!received) {
            return received;
        }
    } else {
        auto ready = channel.ExpectMarker(kRxReady, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        rx_ready = true;
        auto sent = Send();
        if (!sent) {
            return sent;
        }
        auto finished = FinishTx();
        if (!finished) {
            return finished;
        }
        auto state = CheckState();
        if (!state) {
            return state;
        }
        if (phase_error_) {
            return std::unexpected(*phase_error_);
        }
    }
    return {};
}

std::size_t SqMultiplexSession::RequestCount() const noexcept {
    if (phase_ == MultiplexPhase::kBaseline) {
        return 1;
    }
    return options_.batch * (phase_ == MultiplexPhase::kMixed ? 2 : 1);
}
MultiplexFrame SqMultiplexSession::Frame(std::size_t slot) const noexcept {
    if (phase_ == MultiplexPhase::kBaseline) {
        return {.phase = phase_, .sink = true};
    }
    const bool mixed = phase_ == MultiplexPhase::kMixed;
    return {.phase = phase_,
            .round = round_,
            .index = static_cast<std::uint32_t>(mixed ? slot / 2 : slot),
            .sink = mixed && slot % 2 == 0};
}
MultiplexTraffic& SqMultiplexSession::Traffic(bool sink) noexcept {
    if (phase_ == MultiplexPhase::kBaseline) {
        return stats_.fault_baseline;
    }
    return sink ? stats_.sink : stats_.healthy[static_cast<std::size_t>(phase_)];
}

std::expected<void, ToolError> SqMultiplexSession::Send() {
    const auto count = RequestCount();
    for (std::size_t i = 0; i < count; ++i) {
        const auto frame = Frame(i);
        FillMultiplexPayload(registered_.subspan(i * options_.bytes, options_.bytes), frame);
        requests_[i] = {.metadata = {.connection_id = frame.sink ? 2u : 1u,
                                     .operation_id = i + 1,
                                     .operation_generation = round_ * 3 + static_cast<std::uint32_t>(phase_),
                                     .target = remotes_[frame.sink],
                                     .opcode = URMA_OPC_SEND_IMM,
                                     .buffer_lease = i + 1},
                        .sges = std::span(&sges_[i], 1),
                        .imm_data = MultiplexImmediate(frame)};
    }
    // 同一 post 中交替选择 rjetty，不在每个 WR 间插入 TCP 同步或 poll。
    if (phase_ == MultiplexPhase::kMixed) {
        stats_.rounds[round_].started = true;
    }
    submitted_ = Clock::now();
    auto sent = sender_.Send(std::span(requests_).first(count), std::span(ids_).first(count), lane_);
    const auto accepted = sent ? std::optional<std::size_t>(*sent) : sent.error().accepted;
    if (accepted) {
        for (std::size_t i = 0; i < *accepted; ++i) {
            ++Traffic(Frame(i).sink).accepted;
        }
    } else {
        stats_.accepted_known = false;
    }
    if (!sent || *sent != count) {
        if (!sent && sent.error().provider_status == URMA_EAGAIN) {
            ++stats_.provider_eagain;
        }
        unsafe_ = true;
        return Error(
            "batch not fully accepted; retaining resources",
            sent ? EPROTO
                 : (sent.error().provider_status ? sent.error().provider_status : static_cast<int>(sent.error().code)));
    }
    return {};
}

std::expected<void, ToolError> SqMultiplexSession::CheckState() {
    auto jetty = pool_.Get(lane_);
    if (!jetty || *jetty != jetty_ || !SameEndpoint((*jetty)->jetty_id, local_.endpoints[0])) {
        return Error("sending SQ identity changed or no longer usable", EPROTO);
    }
    if (jetty_->jetty_cfg.jfs_cfg.flag.bs.error_suspend) {
        return Error("multiplex experiment requires error_suspend=0 at creation", EPROTO);
    }
    stats_.requested_rnr_retry = jetty_->jetty_cfg.jfs_cfg.rnr_retry;
    stats_.requested_err_timeout = jetty_->jetty_cfg.jfs_cfg.err_timeout;
    urma_jetty_cfg_t cfg{};
    urma_jetty_attr_t attr{};
    const auto status = UrmaApi::QueryJetty(jetty_, &cfg, &attr);
    if (status != URMA_SUCCESS) {
        return Error("query hardware jetty state", status);
    }
    stats_.queried_rnr_retry = cfg.jfs_cfg.rnr_retry;
    stats_.queried_err_timeout = cfg.jfs_cfg.err_timeout;
    stats_.queried_state = static_cast<std::uint32_t>(attr.state);
    if (attr.state != URMA_JETTY_STATE_READY) {
        return Error("hardware jetty is not READY", static_cast<int>(attr.state));
    }
    // 当前 query 的 flags 可能由内核填零；READY 查询与成功续发须结合判断。
    if (cfg.jfs_cfg.flag.bs.error_suspend) {
        return Error("multiplex experiment requires error_suspend=0", EPROTO);
    }
    ++stats_.state_checks;
    return {};
}

std::expected<void, ToolError> SqMultiplexSession::ObserveTx(const urma_cr_t& cr) {
    if (!IsTerminal(cr.status) || cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.local_id != local_.endpoints[0].id) {
        return Error("unexpected TX CQE status/direction/jetty", static_cast<int>(cr.status));
    }
    auto record = ledger_.Lookup(cr.user_ctx);
    if (!record || !SameLane(record->ticket.lane, lane_) || !record->metadata.buffer_lease ||
        record->metadata.buffer_lease > RequestCount()) {
        return Error("unknown or foreign TX attempt", EPROTO);
    }
    const auto slot = static_cast<std::size_t>(record->metadata.buffer_lease - 1);
    const auto frame = Frame(slot);
    if (record->id != ids_[slot] || tx_seen_[slot] || record->metadata.target != remotes_[frame.sink] ||
        record->metadata.connection_id != (frame.sink ? 2u : 1u) || record->metadata.operation_id != slot + 1 ||
        record->metadata.opcode != URMA_OPC_SEND_IMM ||
        record->metadata.operation_generation != round_ * 3 + static_cast<std::uint32_t>(phase_)) {
        return Error("duplicate or foreign TX identity", EPROTO);
    }
    auto retired = ledger_.Complete(cr.user_ctx);
    if (!retired) {
        return Error("retire TX attempt", static_cast<int>(retired.error().code));
    }
    tx_seen_[slot] = true;
    auto& traffic = Traffic(frame.sink);
    const auto ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - submitted_).count());
    traffic.min_cqe_ns = traffic.retired ? std::min(traffic.min_cqe_ns, ns) : ns;
    traffic.max_cqe_ns = std::max(traffic.max_cqe_ns, ns);
    ++traffic.retired;
    if (cr.status == URMA_CR_SUCCESS) {
        ++traffic.success;
    } else if (cr.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
        ++traffic.rnr;
    } else if (cr.status == URMA_CR_LOC_ACCESS_ERR) {
        ++traffic.loc_access_error;
    } else if (cr.status == URMA_CR_REM_ACCESS_ABORT_ERR) {
        ++traffic.remote_access_abort_error;
    } else if (cr.status == URMA_CR_ACK_TIMEOUT_ERR) {
        ++traffic.ack_timeout_error;
    } else {
        ++traffic.other_error;
        traffic.last_other_status = static_cast<int>(cr.status);
    }
    const auto fault_status =
        options_.fault == MultiplexFault::kPeerExit ? URMA_CR_REM_ACCESS_ABORT_ERR : URMA_CR_RNR_RETRY_CNT_EXC_ERR;
    const auto expected = frame.sink && phase_ != MultiplexPhase::kBaseline ? fault_status : URMA_CR_SUCCESS;
    if (cr.status != expected && !phase_error_) {
        // 状态不符合实验预期，但 CQE 身份与终结证明仍有效。收齐剩余尝试后再 FAIL，不能截断观测。
        const char* operation = "healthy or baseline WR did not succeed";
        if (frame.sink && phase_ != MultiplexPhase::kBaseline) {
            operation = options_.fault == MultiplexFault::kPeerExit
                            ? "exited peer WR did not complete with REM_ACCESS_ABORT"
                            : "empty sink WR did not complete with RNR";
        }
        phase_error_ = ToolError{operation, static_cast<int>(cr.status)};
    }
    if (phase_ == MultiplexPhase::kMixed) {
        auto& round = stats_.rounds[round_];
        if (frame.sink) {
            if (!round.sink_retired) {
                round.first_sink_ns = ns;
            }
            round.last_sink_ns = ns;
            ++round.sink_retired;
            if (cr.status == URMA_CR_REM_ACCESS_ABORT_ERR) {
                ++round.sink_remote_access_abort;
            }
            if (cr.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
                if (!mixed_rnr_) {
                    round.first_rnr_ns = ns;
                    round.healthy_before_first_rnr = mixed_healthy_;
                }
                round.last_rnr_ns = ns;
                round.healthy_before_last_rnr = mixed_healthy_;
                ++mixed_rnr_;
                ++round.sink_rnr;
            }
        } else {
            if (!round.healthy_retired) {
                round.first_healthy_ns = ns;
            }
            round.last_healthy_ns = ns;
            ++round.healthy_retired;
            if (cr.status == URMA_CR_SUCCESS) {
                ++mixed_healthy_;
                ++round.healthy_success;
            }
        }
    }
    return {};
}

std::expected<std::size_t, ToolError> SqMultiplexSession::PollTx() {
    // 生产完成层会在普通错误后主动 BeginDrain；本工具直接消费 CQE，保留硬件 error continue 行为。
    completions_.fill({});
    auto polled = pool_.PollSend(completions_);
    if (!polled) {
        unsafe_ = true;
        return Error("poll TX JFC", PoolError(polled.error()));
    }
    std::optional<ToolError> failure;
    for (const auto& cr : std::span(completions_).first(*polled)) {
        auto observed = ObserveTx(cr);
        if (!observed && !failure) {
            failure = observed.error();
        }
    }
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    return static_cast<std::size_t>(*polled);
}

std::expected<void, ToolError> SqMultiplexSession::FinishTx() {
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while (Clock::now() < deadline) {
        auto polled = PollTx();
        if (!polled) {
            return std::unexpected(polled.error());
        }
        if (!ledger_.size()) {
            // 额外一次 poll 捕获当前已可见的重复 CQE，不能将它错误归给下一批消息。
            auto extra = PollTx();
            if (!extra) {
                return std::unexpected(extra.error());
            }
            return {};
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    return Error("TX completion timeout; retaining resources", ETIMEDOUT);
}

std::expected<void, ToolError> SqMultiplexSession::ValidateRx(const raw::RxBufferEvent& event) {
    const auto& cr = event.completion;
    if (event.error || !event.lease || cr.status != URMA_CR_SUCCESS || cr.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
        event.payload.size() != options_.bytes) {
        return Error("invalid healthy RX completion", EPROTO);
    }
    auto frame = CheckMultiplexPayload(event.payload);
    if (!frame) {
        return std::unexpected(frame.error());
    }
    if (frame->phase != phase_ || frame->round != round_ || frame->sink || frame->index >= options_.batch ||
        cr.local_id != local_.endpoints[0].id || !SameEndpoint(cr.remote_id, peer_.endpoints[0]) ||
        cr.imm_data != MultiplexImmediate(*frame)) {
        return Error("RX route/identity/IMM mismatch", EPROTO);
    }
    if (rx_seen_[frame->index]) {
        return Error("duplicate healthy RX message", EPROTO);
    }
    rx_seen_[frame->index] = true;
    ++Traffic(false).received;
    ++phase_received_;
    return {};
}

std::expected<std::size_t, ToolError> SqMultiplexSession::PollRx() {
    auto polled = rx_.Poll(rx_events_);
    if (!polled) {
        unsafe_ = true;
        return Error("poll healthy RX JFC", polled.error().provider_error ? polled.error().provider_error
                                                                          : static_cast<int>(polled.error().code));
    }
    std::optional<ToolError> failure;
    for (const auto& event : std::span(rx_events_).first(*polled)) {
        auto valid = ValidateRx(event);
        if (!valid && !failure) {
            failure = valid.error();
        }
        // 首条校验失败也归还本批全部有效 lease；未决 posted buffer 仍须保留。
        if (event.lease) {
            auto released = rx_.Release(*event.lease);
            if (!released && !failure) {
                failure = ToolError{"release healthy RX lease", static_cast<int>(released.error().code)};
            }
        }
    }
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    return *polled;
}

std::expected<void, ToolError> SqMultiplexSession::Receive() {
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while (Clock::now() < deadline) {
        auto polled = PollRx();
        if (!polled) {
            return std::unexpected(polled.error());
        }
        if (phase_received_ == options_.batch && !*polled) {
            return {};
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    return Error("healthy RX completion timeout; retaining resources", ETIMEDOUT);
}

std::expected<void, ToolError> SqMultiplexSession::Close() noexcept {
    if (unsafe_ || !peer_quiescent_ || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("DMA/peer state unresolved; retaining multiplex session", EBUSY);
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
    for (auto& remote : remotes_) {
        if (remote) {
            const auto status = UrmaApi::UnimportJetty(remote);
            if (status != URMA_SUCCESS) {
                return Error("unimport target", status);
            }
            remote = nullptr;
        }
    }
    auto rx = rx_.Close();
    if (!rx) {
        return Error("close RX buffers",
                     rx.error().provider_error ? rx.error().provider_error : static_cast<int>(rx.error().code));
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
    storage_.clear();
    jetty_ = nullptr;
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
