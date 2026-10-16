// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/backpressure_test/backpressure_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr std::uint32_t kStart = 0x42500000;
constexpr std::uint32_t kPosted = 0x42500001;
constexpr std::uint32_t kRxReady = 0x42500002;
constexpr std::uint32_t kDone = 0x42500003;

auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
int PoolError(const raw::JettyPoolFailure& error) noexcept {
    return error.provider_error ? error.provider_error : static_cast<int>(error.code);
}
int CompletionError(const raw::TxCompletionError& error) noexcept {
    return error.pool_error ? PoolError(*error.pool_error) : static_cast<int>(error.code);
}
bool SameLane(raw::JettyLane a, raw::JettyLane b) noexcept {
    return a.pool == b.pool && a.index == b.index && a.epoch == b.epoch;
}
bool SameEndpoint(const urma_jetty_id_t& a, const urma_jetty_id_t& b) noexcept {
    return a.id == b.id && std::memcmp(a.eid.raw, b.eid.raw, sizeof(a.eid.raw)) == 0;
}
std::expected<void, ToolError> WriteCount(ControlChannel& channel, std::uint32_t value, std::uint32_t timeout) {
    std::array<std::byte, 4> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<std::byte>(value >> (24 - 8 * i));
    }
    return channel.Write(bytes, timeout);
}
std::expected<std::uint32_t, ToolError> ReadCount(ControlChannel& channel, std::uint32_t timeout) {
    std::array<std::byte, 4> bytes{};
    auto read = channel.Read(bytes, timeout);
    if (!read) {
        return std::unexpected(read.error());
    }
    std::uint32_t value = 0;
    for (auto byte : bytes) {
        value = (value << 8) | std::to_integer<std::uint32_t>(byte);
    }
    return value;
}
} // namespace

std::expected<void, ToolError> BackpressureSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                         const BackpressureOptions& options, bool server) {
    if (ctx_) {
        return Error("session already open", EBUSY);
    }
    auto valid = ValidateBackpressureOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || options.bytes > cap.max_msg_size) {
        return Error("message exceeds device capability", EINVAL);
    }
    options_ = options;
    local_ = {.options = options, .server = server};
    peer_ = {};
    stats_ = {};
    peer_success_ = {};
    started_ = false;
    peer_quiescent_ = true;
    unsafe_ = false;
    round_ = cleanup_count_ = 0;
    ctx_ = ctx;
    const bool delay = options.mode == BackpressureMode::kRxDelay;
    const auto rx_depth = options.tx_depth + (delay ? 0u : 1u);
    raw::JettyPoolConfig config;
    config.jetty_count = delay ? 2 : 1;
    config.tx_depth = options.tx_depth;
    config.rx_depth = rx_depth;
    config.tx_cq_depth = config.jetty_count * (options.tx_depth + 1);
    config.rx_cq_depth = rx_depth;
    auto pool = pool_.Open(ctx, cap, config);
    if (!pool) {
        return Error("create backpressure jetty pool (check device queue limits)", PoolError(pool.error().failure));
    }
    for (std::uint32_t i = 0; i < config.jetty_count; ++i) {
        auto ticket = pool_.Reserve();
        if (!ticket) {
            return Error("reserve local endpoint", PoolError(ticket.error()));
        }
        auto jetty = pool_.Get(ticket->lane);
        auto cancelled = pool_.Cancel(*ticket);
        if (!jetty || !cancelled) {
            return Error("read local endpoint", EIO);
        }
        lanes_[i] = ticket->lane;
        local_.endpoints[i] = (*jetty)->jetty_id;
    }
    if (server) {
        auto rx = rx_.Open(pool_);
        if (!rx) {
            return Error("open RX buffers",
                         rx.error().provider_error ? rx.error().provider_error : static_cast<int>(rx.error().code));
        }
    } else {
        const long page_size = ::sysconf(_SC_PAGESIZE);
        if (page_size <= 0) {
            return Error("query page size", EINVAL);
        }
        const auto page = static_cast<std::size_t>(page_size);
        const auto bytes = static_cast<std::size_t>(options.tx_depth + 1) * options.bytes;
        const auto aligned = (bytes + page - 1) / page * page;
        // 第 D+1 个槽位专供拒绝探测及后续重试，不能覆盖仍在途的前 D 个 payload。
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
        for (std::uint32_t i = 0; i <= options.tx_depth; ++i) {
            sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(registered_.data() + i * options.bytes),
                        .len = options.bytes,
                        .tseg = segment_};
        }
        // 账本故意比 SQ 配额多一个，保证拒绝来自 SQ，而非账本容量耗尽。
        auto ledger = ledger_.Open(pool_, options.tx_depth + 1);
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

std::expected<void, ToolError> BackpressureSession::Run(ControlChannel& channel) {
    if (!ready_ || started_) {
        return Error("session not ready or already run", EINVAL);
    }
    started_ = true;
    auto peer = ExchangeBackpressureHello(channel, local_);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    peer_ = *peer;
    if (!local_.server) {
        urma_rjetty_t cfg{};
        cfg.jetty_id = peer_.endpoints[0];
        cfg.trans_mode = URMA_TM_RM;
        cfg.type = URMA_JETTY;
        cfg.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remote_ = UrmaApi::ImportJetty(ctx_, &cfg, &token);
        if (!remote_) {
            return Error("import RM_CTP receive endpoint", errno ? errno : EIO);
        }
    }
    peer_quiescent_ = false;
    auto start = BackpressureBarrier(channel, kStart, options_.timeout_ms);
    if (!start) {
        return start;
    }
    auto result = options_.mode == BackpressureMode::kTxQuota ? RunQuota(channel) : RunDelay(channel);
    if (!result) {
        return result;
    }
    if (!stats_.accepted_known || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("final DMA ownership not fully returned", EBUSY);
    }
    auto done = BackpressureBarrier(channel, kDone, options_.timeout_ms);
    if (!done) {
        return done;
    }
    peer_quiescent_ = true;
    return {};
}

std::expected<void, ToolError> BackpressureSession::Refill(std::uint32_t count) {
    while (count) {
        const auto requested = std::min<std::size_t>(count, raw::RxBufferPool::kMaxBatch);
        auto posted = rx_.Refill(requested);
        if (!posted) {
            return Error("post RX buffers", posted.error().provider_error ? posted.error().provider_error
                                                                          : static_cast<int>(posted.error().code));
        }
        if (*posted != requested) {
            return Error("insufficient RX buffers", ENOBUFS);
        }
        count -= static_cast<std::uint32_t>(*posted);
    }
    return {};
}

void BackpressureSession::Prepare(std::uint32_t first, std::uint32_t count, bool cleanup) {
    for (std::uint32_t i = first; i < first + count; ++i) {
        const BackpressureFrame frame{.round = round_, .index = i, .cleanup = cleanup};
        FillBackpressurePayload(registered_.subspan(static_cast<std::size_t>(i) * options_.bytes, options_.bytes),
                                frame);
        requests_[i] = {.metadata = {.connection_id = 1,
                                     .operation_id = i + 1,
                                     .operation_generation = (static_cast<std::uint64_t>(round_) << 1) | cleanup,
                                     .target = remote_,
                                     .opcode = URMA_OPC_SEND_IMM,
                                     .buffer_lease = i + 1},
                        .sges = std::span(&sges_[i], 1),
                        .imm_data = BackpressureImmediate(frame)};
    }
}

std::expected<void, ToolError> BackpressureSession::Send(std::uint32_t first, std::uint32_t count, bool cleanup) {
    if (!count) {
        return {};
    }
    auto sent = sender_.Send(std::span(requests_).subspan(first, count), std::span(ids_).subspan(first, count),
                             lanes_[cleanup ? 1 : 0]);
    auto& stats = cleanup ? stats_.cleanup : stats_.traffic;
    const auto accepted = sent ? std::optional<std::size_t>(*sent) : sent.error().accepted;
    if (accepted) {
        stats.accepted += *accepted;
    } else {
        stats_.accepted_known = false;
    }
    if (!sent || *sent != count) {
        if (!sent && sent.error().provider_status == URMA_EAGAIN) {
            ++stats_.provider_eagain;
        }
        // 此测试要求先接受整批，不能边 poll 边补投后仍声称验证了暂停 poll 的满 SQ。
        // 同步失败保留真实 accepted 前缀，不重放已接受工作，也不伪装成本地配额拒绝。
        unsafe_ = true;
        return Error(
            "batch not fully accepted; retaining resources",
            sent ? EPROTO
                 : (sent.error().provider_status ? sent.error().provider_status : static_cast<int>(sent.error().code)));
    }
    return {};
}

std::expected<void, ToolError> BackpressureSession::CheckRejected(bool fault, bool cleanup) {
    const auto before = ledger_.size();
    const auto index = options_.tx_depth;
    Prepare(index, 1, cleanup);
    std::array<raw::AttemptId, 1> id{};
    auto sent = sender_.Send(std::span(requests_).subspan(index, 1), id, lanes_[cleanup ? 1 : 0]);
    const auto expected = fault ? raw::JettyPoolErrorCode::kFaulted : raw::JettyPoolErrorCode::kExhausted;
    if (sent || sent.error().accepted != 0 || sent.error().code != raw::TxSendErrorCode::kLedgerFailure ||
        !sent.error().ledger_error || sent.error().ledger_error->code != raw::AttemptLedgerErrorCode::kPoolFailure ||
        !sent.error().ledger_error->pool_failure || sent.error().ledger_error->pool_failure->code != expected ||
        id[0] || ledger_.size() != before || (!fault && before != options_.tx_depth)) {
        unsafe_ = true;
        return Error("send was not rejected by the expected SQ reservation gate", EPROTO);
    }
    if (fault) {
        ++(cleanup ? stats_.cleanup : stats_.traffic).rejected_sends;
    } else {
        ++stats_.quota_rejections;
    }
    return {};
}

std::expected<void, ToolError> BackpressureSession::RunQuota(ControlChannel& channel) {
    const auto count = options_.tx_depth + 1;
    for (round_ = 0; round_ < options_.rounds; ++round_) {
        tx_seen_.fill(false);
        tx_success_.fill(false);
        for (auto& seen : rx_seen_) {
            seen.fill(false);
        }
        if (local_.server) {
            auto posted = Refill(count);
            if (!posted) {
                return posted;
            }
        }
        auto ready = BackpressureBarrier(channel, 2 * round_ + 1, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        if (local_.server) {
            auto received = Receive(static_cast<std::uint64_t>(round_ + 1) * count);
            if (!received) {
                return received;
            }
        } else {
            Prepare(0, count, false);
            auto sent = Send(0, options_.tx_depth, false);
            if (!sent) {
                return sent;
            }
            // 即便硬件已产生 CQE，尚未 poll/退休的账本仍应占满本地配额。
            auto rejected = CheckRejected(false);
            if (!rejected) {
                return rejected;
            }
            auto finished = FinishTx(false, false);
            if (!finished) {
                return finished;
            }
            // 唯一重试的是 accepted=0 的额外 WR；已经被接受的前 D 条绝不再次发送。
            auto retry = Send(options_.tx_depth, 1, false);
            if (!retry) {
                return retry;
            }
            ++stats_.recovery_sends;
            finished = FinishTx(false, false);
            if (!finished) {
                return finished;
            }
        }
        auto done = BackpressureBarrier(channel, 2 * round_ + 2, options_.timeout_ms);
        if (!done) {
            return done;
        }
    }
    const auto total = options_.rounds * count;
    if (local_.server) {
        auto report = ReadReport(channel, false, total);
        if (!report) {
            return report;
        }
    } else {
        if (stats_.quota_rejections != options_.rounds || stats_.recovery_sends != options_.rounds) {
            return Error("missing quota exhaustion/recovery cycle", EPROTO);
        }
        auto report = ReportTx(channel, false);
        if (!report) {
            return report;
        }
    }
    if (stats_.traffic.accepted != total || stats_.traffic.retired != total || stats_.traffic.success != total ||
        (local_.server && stats_.traffic.received != total)) {
        return Error("quota TX/RX totals mismatch", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> BackpressureSession::RunDelay(ControlChannel& channel) {
    tx_seen_.fill(false);
    tx_success_.fill(false);
    for (auto& seen : rx_seen_) {
        seen.fill(false);
    }
    if (local_.server) {
        auto posted = channel.ExpectMarker(kPosted, options_.timeout_ms);
        if (!posted) {
            return posted;
        }
        // 时间从客户端完成 post 后开始计；不依赖操作系统调度保证某种 RNR 结果。
        std::this_thread::sleep_for(std::chrono::microseconds(options_.rx_delay_us));
        auto refilled = Refill(options_.tx_depth);
        if (!refilled) {
            return refilled;
        }
        auto ready = channel.SendMarker(kRxReady, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        auto report = ReadReport(channel, false, options_.tx_depth);
        if (!report) {
            return report;
        }
        // 成功 ACK 对应的 RX 必须可观察；错误 WR 也可能已经在远端产生了 RX。
        auto received = Receive(stats_.traffic.success);
        if (!received) {
            return received;
        }
        cleanup_count_ = static_cast<std::uint32_t>(rx_.posted());
        auto count = WriteCount(channel, cleanup_count_, options_.timeout_ms);
        if (!count) {
            return count;
        }
        auto cleanup = ReadReport(channel, true, cleanup_count_);
        if (!cleanup) {
            return cleanup;
        }
        // posted 快照不是网络屏障。迟到的原始 CQE 仍可出现，并可能使清理 SQ 遇到 RNR。
        // 只有所有已投 RX 都逐一交还，才允许关闭 MR；空 CQ 或 TX 排空不能替代这个条件。
        received = Receive(options_.tx_depth);
        if (!received) {
            return received;
        }
        for (std::size_t stream = 0; stream < peer_success_.size(); ++stream) {
            for (std::uint32_t index = 0; index < options_.tx_depth; ++index) {
                const bool success = (peer_success_[stream][index / 64] >> (index % 64)) & 1;
                if (success && !rx_seen_[stream][index]) {
                    return Error("successful TX missing matching RX identity", EPROTO);
                }
            }
        }
    } else {
        Prepare(0, options_.tx_depth, false);
        auto sent = Send(0, options_.tx_depth, false);
        if (!sent) {
            return sent;
        }
        auto posted = channel.SendMarker(kPosted, options_.timeout_ms);
        if (!posted) {
            return posted;
        }
        auto ready = channel.ExpectMarker(kRxReady, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        auto finished = FinishTx(false, true);
        if (!finished) {
            return finished;
        }
        auto report = ReportTx(channel, false);
        if (!report) {
            return report;
        }
        auto count = ReadCount(channel, options_.timeout_ms);
        if (!count) {
            return std::unexpected(count.error());
        }
        if (*count > options_.tx_depth) {
            return Error("invalid cleanup count", EPROTO);
        }
        cleanup_count_ = *count;
        // 原始 attempts 已全部终结后才复用 TX 内存；清理使用另一 SQ 和独立身份。
        tx_seen_.fill(false);
        tx_success_.fill(false);
        ids_.fill(0);
        Prepare(0, cleanup_count_, true);
        sent = Send(0, cleanup_count_, true);
        if (!sent) {
            return sent;
        }
        finished = FinishTx(true, true);
        if (!finished) {
            return finished;
        }
        report = ReportTx(channel, true);
        if (!report) {
            return report;
        }
    }
    stats_.terminal_failure = stats_.traffic.retired != stats_.traffic.success;
    return {};
}

std::expected<void, ToolError> BackpressureSession::ReportTx(ControlChannel& channel, bool cleanup) {
    const auto& stats = cleanup ? stats_.cleanup : stats_.traffic;
    BackpressureReport report{.accepted = static_cast<std::uint32_t>(stats.accepted),
                              .retired = static_cast<std::uint32_t>(stats.retired),
                              .success = static_cast<std::uint32_t>(stats.success),
                              .flush_done = static_cast<std::uint32_t>(stats.flush_done),
                              .has_success_map = options_.mode == BackpressureMode::kRxDelay};
    if (report.has_success_map) {
        for (std::uint32_t index = 0; index < options_.tx_depth; ++index) {
            if (tx_success_[index]) {
                report.success_map[index / 64] |= std::uint64_t{1} << (index % 64);
            }
        }
    }
    return WriteBackpressureReport(channel, report, options_.timeout_ms);
}

std::expected<void, ToolError> BackpressureSession::ReadReport(ControlChannel& channel, bool cleanup,
                                                               std::uint32_t expected) {
    auto report = ReadBackpressureReport(channel, options_.timeout_ms);
    if (!report) {
        return std::unexpected(report.error());
    }
    if (report->accepted != expected || report->has_success_map != (options_.mode == BackpressureMode::kRxDelay)) {
        return Error("peer accepted count or success map mismatch", EPROTO);
    }
    auto& stats = cleanup ? stats_.cleanup : stats_.traffic;
    stats.accepted = report->accepted;
    stats.retired = report->retired;
    stats.success = report->success;
    stats.flush_done = report->flush_done;
    peer_success_[cleanup ? 1 : 0] = report->success_map;
    return {};
}

std::expected<void, ToolError> BackpressureSession::ObserveTx(const raw::TxCompletionEvent& event, bool cleanup,
                                                              bool allow_errors) {
    if (event.error) {
        return Error("invalid TX completion or drain operation", CompletionError(*event.error));
    }
    const auto lane = lanes_[cleanup ? 1 : 0];
    auto& stats = cleanup ? stats_.cleanup : stats_.traffic;
    if (!event.lane || !SameLane(*event.lane, lane)) {
        return Error("TX completion belongs to another SQ", EPROTO);
    }
    if (event.kind == raw::TxCompletionKind::kFlushDone) {
        if (!allow_errors || stats.flush_done || event.record || event.completion.user_ctx) {
            return Error("invalid TX flush boundary", EPROTO);
        }
        ++stats.flush_done;
        return {};
    }
    if (event.kind != raw::TxCompletionKind::kCompleted || !event.record) {
        return Error("unexpected TX completion kind", EPROTO);
    }
    const auto& record = *event.record;
    const auto limit =
        cleanup ? cleanup_count_ : options_.tx_depth + (options_.mode == BackpressureMode::kTxQuota ? 1u : 0u);
    if (!record.metadata.buffer_lease || record.metadata.buffer_lease > limit) {
        return Error("TX buffer identity out of range", EPROTO);
    }
    const auto slot = static_cast<std::size_t>(record.metadata.buffer_lease - 1);
    if (record.id != ids_[slot] || tx_seen_[slot] || !SameLane(record.ticket.lane, lane) ||
        record.metadata.target != remote_ || record.metadata.connection_id != 1 ||
        record.metadata.operation_id != slot + 1 || record.metadata.opcode != URMA_OPC_SEND_IMM ||
        record.metadata.operation_generation != ((static_cast<std::uint64_t>(round_) << 1) | cleanup) ||
        event.completion.user_ctx != record.id) {
        return Error("duplicate or foreign TX attempt", EPROTO);
    }
    const auto status = event.completion.status;
    if ((!allow_errors && status != URMA_CR_SUCCESS) || ((status == URMA_CR_WR_UNHANDLED) != (stats.flush_done == 1))) {
        return Error("TX status violates expected mode or flush boundary", static_cast<int>(status));
    }
    tx_seen_[slot] = true;
    tx_success_[slot] = status == URMA_CR_SUCCESS;
    ++stats.retired;
    switch (status) {
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
        stats.last_other_error = static_cast<int>(status);
        break;
    }
    return {};
}

std::expected<std::size_t, ToolError> BackpressureSession::PollTx(bool cleanup, bool allow_errors) {
    auto polled = processor_.Poll(tx_events_);
    if (!polled) {
        unsafe_ = true;
        return Error("poll TX JFC", CompletionError(polled.error()));
    }
    std::optional<ToolError> failure;
    const auto observe = [&](std::size_t count) {
        for (const auto& event : std::span(tx_events_).first(count)) {
            auto result = ObserveTx(event, cleanup, allow_errors);
            if (!result && !failure) {
                failure = result.error();
            }
        }
    };
    std::size_t count = polled->count;
    observe(count);
    auto state = pool_.lane_state(lanes_[cleanup ? 1 : 0]);
    if (!state) {
        unsafe_ = true;
        return Error("read TX lane state", PoolError(state.error()));
    }
    if (!failure && *state == raw::JettyLaneState::kFlushReady) {
        auto flushed = processor_.Flush(lanes_[cleanup ? 1 : 0], tx_events_);
        if (!flushed) {
            unsafe_ = true;
            return Error("flush TX SQ", CompletionError(flushed.error()));
        }
        observe(flushed->count);
        count += flushed->count;
    }
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    return count;
}

std::expected<void, ToolError> BackpressureSession::FinishTx(bool cleanup, bool allow_errors) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while (Clock::now() < deadline) {
        auto progress = PollTx(cleanup, allow_errors);
        if (!progress) {
            return std::unexpected(progress.error());
        }
        auto state = pool_.lane_state(lanes_[cleanup ? 1 : 0]);
        if (!state) {
            return Error("read TX state", PoolError(state.error()));
        }
        if (!ledger_.size() && (*state == raw::JettyLaneState::kReady || *state == raw::JettyLaneState::kDrained)) {
            if (*state == raw::JettyLaneState::kDrained) {
                auto rejected = CheckRejected(true, cleanup);
                if (!rejected) {
                    return rejected;
                }
            }
            // 再取一次 CQ，重复完成不能静默留到后续阶段而被误认为新工作。
            auto extra = PollTx(cleanup, allow_errors);
            if (!extra) {
                return std::unexpected(extra.error());
            }
            return {};
        }
        if (!*progress) {
            std::this_thread::yield();
        }
    }
    return Error("TX completion/drain timeout", ETIMEDOUT);
}

std::expected<void, ToolError> BackpressureSession::ValidateRx(const raw::RxBufferEvent& event) {
    if (event.error || !event.lease || event.completion.status != URMA_CR_SUCCESS) {
        return Error("invalid RX completion",
                     event.completion.status ? static_cast<int>(event.completion.status) : EPROTO);
    }
    const auto& cr = event.completion;
    if (!cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
        event.payload.size() != options_.bytes) {
        return Error("RX opcode/direction/length mismatch", EPROTO);
    }
    auto frame = CheckBackpressurePayload(event.payload);
    if (!frame) {
        return std::unexpected(frame.error());
    }
    const auto limit =
        frame->cleanup ? cleanup_count_ : options_.tx_depth + (options_.mode == BackpressureMode::kTxQuota ? 1u : 0u);
    if (frame->round != round_ || frame->index >= limit ||
        (frame->cleanup && options_.mode != BackpressureMode::kRxDelay) || cr.local_id != local_.endpoints[0].id ||
        !SameEndpoint(cr.remote_id, peer_.endpoints[frame->cleanup ? 1 : 0]) ||
        cr.imm_data != BackpressureImmediate(*frame)) {
        return Error("RX route/identity/IMM mismatch", EPROTO);
    }
    auto& seen = rx_seen_[frame->cleanup ? 1 : 0][frame->index];
    if (seen) {
        return Error("duplicate RX message", EPROTO);
    }
    seen = true;
    ++(frame->cleanup ? stats_.cleanup : stats_.traffic).received;
    return {};
}

std::expected<std::size_t, ToolError> BackpressureSession::PollRx() {
    auto polled = rx_.Poll(rx_events_);
    if (!polled) {
        unsafe_ = true;
        return Error("poll RX JFC", polled.error().provider_error ? polled.error().provider_error
                                                                  : static_cast<int>(polled.error().code));
    }
    std::optional<ToolError> failure;
    for (const auto& event : std::span(rx_events_).first(*polled)) {
        auto valid = ValidateRx(event);
        if (!valid && !failure) {
            failure = valid.error();
        }
        // 同批所有交出的 lease 都必须归还，不能因首条 payload 错误跳过后续记录。
        if (event.lease) {
            auto released = rx_.Release(*event.lease);
            if (!released && !failure) {
                failure = ToolError{"release RX lease", static_cast<int>(released.error().code)};
            }
        }
    }
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    return *polled;
}

std::expected<void, ToolError> BackpressureSession::Receive(std::uint64_t count) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while (Clock::now() < deadline) {
        auto polled = PollRx();
        if (!polled) {
            return std::unexpected(polled.error());
        }
        // 达到下限后仍取到一次空 CQ，观察已可见的错误 WR 对应 RX；这不代表网络屏障。
        if (stats_.traffic.received + stats_.cleanup.received >= count && !*polled) {
            return {};
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    return Error("RX completion timeout; retaining posted buffers", ETIMEDOUT);
}

std::expected<void, ToolError> BackpressureSession::Close() noexcept {
    if (unsafe_ || !peer_quiescent_ || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("DMA/peer state unresolved; retaining backpressure session", EBUSY);
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
    if (remote_) {
        const auto status = UrmaApi::UnimportJetty(remote_);
        if (status != URMA_SUCCESS) {
            return Error("unimport receive endpoint", status);
        }
        remote_ = nullptr;
    }
    auto rx = rx_.Close();
    if (!rx) {
        return Error("close RX buffers",
                     rx.error().provider_error ? rx.error().provider_error : static_cast<int>(rx.error().code));
    }
    auto pool = pool_.Close();
    if (!pool) {
        return Error("close jetty pool", PoolError(pool.error()));
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
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
