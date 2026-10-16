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
bool IsContinueCompletion(urma_cr_status_t status) noexcept {
    // 仅逐 WR 的终结 CQE 可归还额度；伪 CQE 和软件 flush 记录不能用于本实验。
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
    continue_jetty_ = nullptr;
    ctx_ = ctx;
    const bool delay = options.mode == BackpressureMode::kRxDelay;
    const auto rx_depth = options.tx_depth + (options.mode == BackpressureMode::kTxQuota ? 1u : 0u);
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
    std::expected<void, ToolError> result;
    switch (options_.mode) {
    case BackpressureMode::kTxQuota:
        result = RunQuota(channel);
        break;
    case BackpressureMode::kRxDelay:
        result = RunDelay(channel);
        break;
    case BackpressureMode::kRnrContinue:
        result = RunRnrContinue(channel);
        break;
    }
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
    auto& stats = Traffic(cleanup);
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

BackpressureTraffic& BackpressureSession::Traffic(bool cleanup) noexcept {
    if (options_.mode == BackpressureMode::kRnrContinue && round_ == 1) {
        return stats_.continuation;
    }
    return cleanup ? stats_.cleanup : stats_.traffic;
}

std::expected<void, ToolError> BackpressureSession::CheckContinueState() {
    auto jetty = pool_.Get(lanes_[0]);
    if (!jetty) {
        return Error("query continuation SQ", PoolError(jetty.error()));
    }
    if (!continue_jetty_) {
        continue_jetty_ = *jetty;
        stats_.jetty_id = local_.endpoints[0].id;
    }
    if (*jetty != continue_jetty_ || !SameEndpoint((*jetty)->jetty_id, local_.endpoints[0])) {
        return Error("continuation SQ identity changed", EPROTO);
    }
    // 当前 uburma query 路径可能将 jfs_flag 固定返回 0，不能把它作为硬件模式的独立证据。
    // 同时核对创建配置；最终仍以同 SQ 新 WR 的 SUCCESS 和远端校验作为行为证据。
    if ((*jetty)->jetty_cfg.jfs_cfg.flag.bs.error_suspend) {
        return Error("continuation requires error_suspend=0 at creation", EPROTO);
    }
    urma_jetty_cfg_t cfg{};
    urma_jetty_attr_t attr{};
    const auto status = UrmaApi::QueryJetty(*jetty, &cfg, &attr);
    if (status != URMA_SUCCESS) {
        return Error("query hardware jetty state", status);
    }
    stats_.queried_rnr_retry = cfg.jfs_cfg.rnr_retry;
    stats_.queried_err_timeout = cfg.jfs_cfg.err_timeout;
    if (attr.state != URMA_JETTY_STATE_READY) {
        return Error("hardware jetty is not READY", static_cast<int>(attr.state));
    }
    if (cfg.jfs_cfg.flag.bs.error_suspend) {
        return Error("continuation requires error_suspend=0", EPROTO);
    }
    ++stats_.state_checks;
    return {};
}

std::expected<std::size_t, ToolError> BackpressureSession::PollContinueTx() {
    // 本实验只观察硬件行为。生产完成层遇到错误会 BeginDrain，使用它会改变被测条件。
    // 仍逐条核验 CQE 身份并通过账本退休，不允许依据计数释放 DMA 内存。
    continue_completions_.fill({});
    auto polled = pool_.PollSend(continue_completions_);
    if (!polled) {
        return Error("poll continuation TX JFC", PoolError(polled.error()));
    }
    std::optional<ToolError> failure;
    for (const auto& cr : std::span(continue_completions_).first(*polled)) {
        auto consume = [&]() -> std::expected<void, ToolError> {
            // 先排除没有合法 user_ctx 的边界事件；本模式从未请求切 ERROR 或 suspend。
            if (!IsContinueCompletion(cr.status) || cr.flag.bs.s_r || !cr.flag.bs.jetty ||
                cr.local_id != local_.endpoints[0].id) {
                return Error("unexpected continuation CQE status/direction/jetty", static_cast<int>(cr.status));
            }
            auto record = ledger_.Lookup(cr.user_ctx);
            if (!record || !SameLane(record->ticket.lane, lanes_[0])) {
                return Error("unknown or foreign continuation attempt", EPROTO);
            }
            auto retired = ledger_.Complete(cr.user_ctx);
            if (!retired) {
                return Error("retire continuation attempt", static_cast<int>(retired.error().code));
            }
            raw::TxCompletionEvent event{
                .kind = raw::TxCompletionKind::kCompleted, .completion = cr, .lane = lanes_[0], .record = *retired};
            auto observed = ObserveTx(event, false, true);
            if (!observed) {
                return observed;
            }
            const auto expected = round_ == 0 ? URMA_CR_RNR_RETRY_CNT_EXC_ERR : URMA_CR_SUCCESS;
            if (cr.status != expected) {
                return Error(round_ == 0 ? "initial WR did not complete with RNR"
                                         : "new WR on same SQ did not complete successfully",
                             static_cast<int>(cr.status));
            }
            return {};
        };
        auto result = consume();
        if (!result && !failure) {
            failure = result.error();
        }
    }
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    return static_cast<std::size_t>(*polled);
}

std::expected<void, ToolError> BackpressureSession::FinishContinueTx() {
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while (Clock::now() < deadline) {
        auto polled = PollContinueTx();
        if (!polled) {
            return std::unexpected(polled.error());
        }
        if (!ledger_.size()) {
            // 阶段切换前再检查重复 CQE，不能让旧完成释放下一批的额度。
            auto extra = PollContinueTx();
            if (!extra) {
                return std::unexpected(extra.error());
            }
            return {};
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    return Error("continuation TX completion timeout; retaining resources", ETIMEDOUT);
}

std::expected<void, ToolError> BackpressureSession::RunRnrContinue(ControlChannel& channel) {
    tx_seen_.fill(false);
    tx_success_.fill(false);
    for (auto& seen : rx_seen_) {
        seen.fill(false);
    }
    if (local_.server) {
        // 必须先收到全批 RNR 的终结报告再投 RX，排除时延和调度造成的首批偶然成功。
        auto report = ReadReport(channel, false, options_.tx_depth);
        if (!report) {
            return report;
        }
        round_ = 1;
        auto refilled = Refill(options_.tx_depth);
        if (!refilled) {
            return refilled;
        }
        auto ready = channel.SendMarker(kRxReady, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        report = ReadReport(channel, false, options_.tx_depth);
        if (!report) {
            return report;
        }
        auto received = Receive(options_.tx_depth);
        if (!received) {
            return received;
        }
        for (std::uint32_t i = 0; i < options_.tx_depth; ++i) {
            if (!rx_seen_[0][i] || !(peer_success_[0][i / 64] & (std::uint64_t{1} << (i % 64)))) {
                return Error("continuation TX/RX identity mismatch", EPROTO);
            }
        }
    } else {
        auto state = CheckContinueState();
        if (!state) {
            return state;
        }
        Prepare(0, options_.tx_depth, false);
        auto sent = Send(0, options_.tx_depth, false);
        if (!sent) {
            return sent;
        }
        auto finished = FinishContinueTx();
        if (!finished) {
            return finished;
        }
        state = CheckContinueState();
        if (!state) {
            return state;
        }
        auto report = ReportTx(channel, false);
        if (!report) {
            return report;
        }
        auto ready = channel.ExpectMarker(kRxReady, options_.timeout_ms);
        if (!ready) {
            return ready;
        }
        // 旧 WR 已逐条终结，才复用内存。round=1 是全新的数据身份，绝不重放 round=0。
        round_ = 1;
        tx_seen_.fill(false);
        tx_success_.fill(false);
        ids_.fill(0);
        Prepare(0, options_.tx_depth, false);
        sent = Send(0, options_.tx_depth, false);
        if (!sent) {
            return sent;
        }
        finished = FinishContinueTx();
        if (!finished) {
            return finished;
        }
        state = CheckContinueState();
        if (!state) {
            return state;
        }
        report = ReportTx(channel, false);
        if (!report) {
            return report;
        }
    }
    if (stats_.traffic.accepted != options_.tx_depth || stats_.traffic.retired != options_.tx_depth ||
        stats_.traffic.success || stats_.traffic.flush_done || stats_.continuation.accepted != options_.tx_depth ||
        stats_.continuation.retired != options_.tx_depth || stats_.continuation.success != options_.tx_depth ||
        stats_.continuation.flush_done || (local_.server && stats_.continuation.received != options_.tx_depth)) {
        return Error("RNR continuation totals mismatch", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> BackpressureSession::ReportTx(ControlChannel& channel, bool cleanup) {
    const auto& stats = Traffic(cleanup);
    BackpressureReport report{.accepted = static_cast<std::uint32_t>(stats.accepted),
                              .retired = static_cast<std::uint32_t>(stats.retired),
                              .success = static_cast<std::uint32_t>(stats.success),
                              .flush_done = static_cast<std::uint32_t>(stats.flush_done),
                              .has_success_map = options_.mode != BackpressureMode::kTxQuota};
    if (report.has_success_map) {
        for (std::uint32_t index = 0; index < options_.tx_depth; ++index) {
            if (tx_success_[index]) {
                report.success_map[index / 64] |= std::uint64_t{1} << (index % 64);
            }
        }
    }
    return WriteBackpressureReport(channel, report, options_.timeout_ms,
                                   options_.mode == BackpressureMode::kRnrContinue && round_ == 0);
}

std::expected<void, ToolError> BackpressureSession::ReadReport(ControlChannel& channel, bool cleanup,
                                                               std::uint32_t expected) {
    auto report = ReadBackpressureReport(channel, options_.timeout_ms,
                                         options_.mode == BackpressureMode::kRnrContinue && round_ == 0);
    if (!report) {
        return std::unexpected(report.error());
    }
    if (report->accepted != expected || report->has_success_map != (options_.mode != BackpressureMode::kTxQuota)) {
        return Error("peer accepted count or success map mismatch", EPROTO);
    }
    auto& stats = Traffic(cleanup);
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
    auto& stats = Traffic(cleanup);
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
    ++Traffic(frame->cleanup).received;
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
        if (stats_.traffic.received + stats_.cleanup.received + stats_.continuation.received >= count && !*polled) {
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
