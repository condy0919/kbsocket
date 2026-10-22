// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/streaming_test/streaming_test.hpp"

#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <thread>

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
std::uint64_t SteadyNowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}
bool IsBoundary(urma_cr_status_t status) noexcept {
    return status == URMA_CR_WR_FLUSH_ERR_DONE || status == URMA_CR_WR_SUSPEND_DONE;
}
auto Error(const char* operation, int code = 0) {
    return std::unexpected(ToolError{operation, code ? code : (errno ? errno : EIO)});
}
constexpr std::uint64_t kImmediate = 0xa500000000000000ull;
void Fill(std::span<std::byte> data, std::uint64_t sequence, std::uint32_t channel, bool echo) {
    // 字节序明确的测试帧：身份进入每个正文 byte 的生成，错设备/错轮次和旧数据复用都会被检测。
    for (std::size_t i = 0; i < 8; ++i) {
        data[i] = static_cast<std::byte>((sequence >> (56 - i * 8)) & 255);
    }
    data[8] = static_cast<std::byte>(channel);
    data[9] = static_cast<std::byte>(echo);
    for (std::size_t i = 10; i < data.size(); ++i) {
        data[i] = static_cast<std::byte>((sequence * 131 + channel * 29 + echo * 7 + i * 17) & 255);
    }
}
std::expected<std::uint64_t, ToolError> Check(std::span<const std::byte> data, std::uint32_t channel, bool echo) {
    std::uint64_t sequence = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        sequence = (sequence << 8) | std::to_integer<unsigned>(data[i]);
    }
    if (data[8] != static_cast<std::byte>(channel) || data[9] != static_cast<std::byte>(echo)) {
        return Error("stream payload channel/direction", EPROTO);
    }
    for (std::size_t i = 10; i < data.size(); ++i) {
        if (data[i] != static_cast<std::byte>((sequence * 131 + channel * 29 + echo * 7 + i * 17) & 255)) {
            return Error("stream payload mismatch", EILSEQ);
        }
    }
    return sequence;
}
} // namespace

std::expected<void, ToolError> StreamSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                   const StreamOptions& options, bool server, std::uint32_t channel) {
    if (ctx_ || started_ || !ctx || channel >= kStreamMaxContexts || !ValidateStreamOptions(options)) {
        return Error("invalid stream open", EINVAL);
    }
    if (options.window > cap.max_jfs_depth || options.window > cap.max_jfr_depth ||
        options.window + 1 > cap.max_jfc_depth || options.bytes > cap.max_msg_size || cap.max_jfs_sge < 1 ||
        cap.max_jfr_sge < 1) {
        return Error("stream device capability", ENOTSUP);
    }
    options_ = options;
    server_ = server;
    channel_ = channel;
    ctx_ = ctx;
    echo_rx_.fill(-1);
    for (std::size_t i = 0; i < options.window; ++i) {
        sequence_[i] = i;
        reply_received_[i] = true;
    }
    if (options.events) {
        jfce_ = UrmaApi::CreateJfce(ctx);
        if (!jfce_) {
            return Error("create stream JFCE");
        }
    }
    urma_jfc_cfg_t cq{};
    cq.depth = options.window + 1;
    cq.flag.bs.lock_free = 1;
    cq.jfce = jfce_;
    tx_cq_ = UrmaApi::CreateJfc(ctx, &cq);
    if (!tx_cq_) {
        return Error("create stream TX CQ");
    }
    cq.depth = options.window;
    rx_cq_ = UrmaApi::CreateJfc(ctx, &cq);
    if (!rx_cq_) {
        return Error("create stream RX CQ");
    }
    urma_jfr_cfg_t recv{};
    recv.depth = options.window;
    recv.trans_mode = URMA_TM_RM;
    recv.max_sge = 1;
    recv.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    recv.jfc = rx_cq_;
    recv.flag.bs.lock_free = 1;
    jfr_ = UrmaApi::CreateJfr(ctx, &recv);
    if (!jfr_) {
        return Error("create stream JFR");
    }
    urma_jetty_cfg_t cfg{};
    cfg.flag.bs.share_jfr = 1;
    cfg.jfs_cfg.depth = options.window;
    cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    cfg.jfs_cfg.priority = 4;
    cfg.jfs_cfg.max_sge = cfg.jfs_cfg.max_rsge = 1;
    cfg.jfs_cfg.rnr_retry = 6;
    cfg.jfs_cfg.err_timeout = 2;
    cfg.jfs_cfg.flag.bs.lock_free = 1;
    cfg.jfs_cfg.jfc = tx_cq_;
    cfg.shared.jfr = jfr_;
    cfg.shared.jfc = rx_cq_;
    jetty_ = UrmaApi::CreateJetty(ctx, &cfg);
    if (!jetty_) {
        return Error("create stream jetty");
    }
    const auto page_value = ::sysconf(_SC_PAGESIZE);
    if (page_value <= 0) {
        return Error("stream page size", EINVAL);
    }
    const auto page = static_cast<std::size_t>(page_value);
    const auto size = (2 * options.window * 4096 + page - 1) / page * page;
    storage_.resize(size + page - 1);
    const auto addr = reinterpret_cast<std::uintptr_t>(storage_.data());
    memory_ = std::span(storage_).subspan((page - addr % page) % page, size);
    seen_.resize(options.messages);
    urma_seg_cfg_t segment{};
    segment.va = reinterpret_cast<std::uintptr_t>(memory_.data());
    segment.len = memory_.size();
    segment.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    segment_ = UrmaApi::RegisterSeg(ctx, &segment);
    if (!segment_) {
        return Error("register stream buffers");
    }
    for (std::size_t i = 0; i < options.window; ++i) {
        tx_sges_[i] = {reinterpret_cast<std::uintptr_t>(TxBuffer(i).data()), options.bytes, segment_};
        rx_sges_[i] = {reinterpret_cast<std::uintptr_t>(RxBuffer(i).data()), 4096, segment_};
    }
    return {};
}

std::span<std::byte> StreamSession::TxBuffer(std::size_t slot) noexcept {
    return memory_.subspan(slot * 4096, options_.bytes);
}
std::span<std::byte> StreamSession::RxBuffer(std::size_t slot) noexcept {
    return memory_.subspan((options_.window + slot) * 4096, options_.bytes);
}

std::expected<void, ToolError> StreamSession::Connect(const urma_jetty_id_t& peer) {
    if (!segment_ || remote_) {
        return Error("stream connect state", EINVAL);
    }
    urma_rjetty_t remote{};
    remote.jetty_id = peer;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    remote_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
    if (!remote_) {
        return Error("import stream endpoint");
    }
    peer_ = peer;
    return {};
}

std::expected<void, ToolError> StreamSession::PostRx(std::size_t slot) {
    if (stats_.rx_posted == options_.messages) {
        return {};
    }
    urma_jfr_wr_t wr{};
    wr.src = {&rx_sges_[slot], 1};
    wr.user_ctx = slot + 1;
    urma_jfr_wr_t* bad = nullptr;
    const auto status = UrmaApi::PostJfrWr(jfr_, &wr, &bad);
    if (status != URMA_SUCCESS || bad) {
        // 即使只提交一条，也不把不明确的 bad_wr 当作零接受而注销 MR。
        unsafe_ = true;
        return Error("post streaming RX; retaining resources", status ? status : EPROTO);
    }
    rx_posted_[slot] = true;
    ++stats_.rx_posted;
    return {};
}

std::expected<void, ToolError> StreamSession::Start() {
    if (!remote_ || started_) {
        return Error("stream start state", EINVAL);
    }
    peer_quiescent_ = false;
    started_ = true;
    diagnostics_.started_ns = SteadyNowNs();
    const auto posted_before = stats_.rx_posted;
    ScopeExit record_progress([&]() noexcept {
        if (stats_.rx_posted != posted_before) {
            diagnostics_.last_progress_ns = SteadyNowNs();
        }
    });
    for (std::size_t i = 0; i < options_.window; ++i) {
        auto posted = PostRx(i);
        if (!posted) {
            return posted;
        }
    }
    return {};
}

std::expected<void, ToolError> StreamSession::PostTx(std::size_t slot, std::uint64_t sequence) {
    Fill(TxBuffer(slot), sequence, channel_, server_);
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_SEND_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = remote_;
    wr.user_ctx = sequence + 1;
    wr.send.src = {&tx_sges_[slot], 1};
    wr.send.imm_data = kImmediate | (static_cast<std::uint64_t>(channel_) << 48) | sequence;
    urma_jfs_wr_t* bad = nullptr;
    const auto status = UrmaApi::PostJettySendWr(jetty_, &wr, &bad);
    if (status != URMA_SUCCESS || bad) {
        unsafe_ = true;
        return Error("post streaming SEND; no replay", status ? status : EPROTO);
    }
    sequence_[slot] = sequence;
    tx_busy_[slot] = true;
    if (!server_) {
        reply_received_[slot] = false;
    }
    ++stats_.accepted;
    return {};
}

std::expected<std::size_t, ToolError> StreamSession::PollTx() {
    std::array<urma_cr_t, 64> completions{};
    const int count = UrmaApi::PollJfc(tx_cq_, completions.size(), completions.data());
    if (count < 0 || count > static_cast<int>(completions.size())) {
        unsafe_ = true;
        return Error("poll stream TX", count);
    }
    for (int i = 0; i < count; ++i) {
        const auto& cr = completions[i];
        // 先排除伪 CQE/错误，再读取 user_ctx；异常路径不释放未证明终结的依赖链。
        if (cr.status != URMA_CR_SUCCESS || cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.local_id != jetty_->jetty_id.id ||
            !cr.user_ctx || cr.user_ctx > options_.messages) {
            unsafe_ = true;
            RecordCompletionFailure(cr, false, count, i);
            return Error("invalid streaming TX completion", cr.status ? cr.status : EPROTO);
        }
        const auto sequence = cr.user_ctx - 1;
        const auto slot = sequence % options_.window;
        if (!tx_busy_[slot] || sequence_[slot] != sequence) {
            unsafe_ = true;
            RecordCompletionFailure(cr, false, count, i);
            return Error("duplicate/stale streaming TX", EPROTO);
        }
        tx_busy_[slot] = false;
        ++stats_.retired;
    }
    return count;
}

std::expected<std::size_t, ToolError> StreamSession::PollRx() {
    std::array<urma_cr_t, 64> completions{};
    const int count = UrmaApi::PollJfc(rx_cq_, completions.size(), completions.data());
    if (count < 0 || count > static_cast<int>(completions.size())) {
        unsafe_ = true;
        return Error("poll stream RX", count);
    }
    for (int i = 0; i < count; ++i) {
        const auto& cr = completions[i];
        if (cr.status != URMA_CR_SUCCESS || !cr.flag.bs.s_r || cr.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
            cr.completion_len != options_.bytes || !cr.user_ctx || cr.user_ctx > options_.window ||
            cr.local_id != (cr.flag.bs.jetty ? jetty_->jetty_id.id : jfr_->jfr_id.id) || cr.remote_id.id != peer_.id ||
            std::memcmp(cr.remote_id.eid.raw, peer_.eid.raw, 16)) {
            unsafe_ = true;
            RecordCompletionFailure(cr, true, count, i);
            return Error("invalid streaming RX completion", cr.status ? cr.status : EPROTO);
        }
        const auto rx_slot = cr.user_ctx - 1;
        auto checked = Check(RxBuffer(rx_slot), channel_, !server_);
        if (!checked || *checked >= options_.messages || seen_[*checked] || !rx_posted_[rx_slot] ||
            cr.imm_data != (kImmediate | (static_cast<std::uint64_t>(channel_) << 48) | *checked)) {
            unsafe_ = true;
            RecordCompletionFailure(cr, true, count, i);
            return Error("stream RX identity/payload/IMM", EPROTO);
        }
        const auto sequence = *checked;
        const auto slot = sequence % options_.window;
        rx_posted_[rx_slot] = false;
        seen_[sequence] = 1;
        ++stats_.received;
        if (server_) {
            if (echo_rx_[slot] != -1) {
                unsafe_ = true;
                RecordCompletionFailure(cr, true, count, i);
                return Error("stream echo window overflow", EPROTO);
            }
            // 旧 echo 的本地 CQE 可能晚于新请求；保留 RX 槽，绝不能覆盖仍被 DMA 使用的 TX 槽。
            echo_rx_[slot] = static_cast<int>(rx_slot);
        } else {
            if (sequence_[slot] != sequence || reply_received_[slot]) {
                unsafe_ = true;
                RecordCompletionFailure(cr, true, count, i);
                return Error("unexpected stream reply", EPROTO);
            }
            reply_received_[slot] = true;
            auto reposted = PostRx(rx_slot);
            if (!reposted) {
                return std::unexpected(reposted.error());
            }
        }
    }
    return count;
}

std::expected<std::size_t, ToolError> StreamSession::Advance() {
    if (!started_ || unsafe_) {
        return Error("stream advance state", EINVAL);
    }
    const auto before = stats_;
    // 只记录本路真实数据进展；其他 context 活动、通知领取或重复 arm 都不能刷新此时间。
    // 提前返回错误时也保留本轮在错误之前已完成的进展。
    ScopeExit record_progress([&]() noexcept {
        if (stats_.accepted != before.accepted || stats_.retired != before.retired ||
            stats_.received != before.received || stats_.rx_posted != before.rx_posted) {
            diagnostics_.last_progress_ns = SteadyNowNs();
        }
    });
    auto tx = PollTx();
    if (!tx) {
        return tx;
    }
    auto rx = PollRx();
    if (!rx) {
        return rx;
    }
    std::size_t progress = *tx + *rx;
    for (std::size_t slot = 0; slot < options_.window; ++slot) {
        if (tx_busy_[slot]) {
            continue;
        }
        std::uint64_t sequence = sequence_[slot];
        if (server_) {
            if (echo_rx_[slot] < 0) {
                continue;
            }
            const auto rx_slot = static_cast<std::size_t>(echo_rx_[slot]);
            auto checked = Check(RxBuffer(rx_slot), channel_, false);
            if (!checked) {
                unsafe_ = true;
                return std::unexpected(checked.error());
            }
            sequence = *checked;
            auto reposted = PostRx(rx_slot);
            if (!reposted) {
                return std::unexpected(reposted.error());
            }
            echo_rx_[slot] = -1;
        } else {
            if (!reply_received_[slot]) {
                continue;
            }
            if (seen_[sequence]) {
                sequence += options_.window;
            }
            if (sequence >= options_.messages) {
                continue;
            }
        }
        auto sent = PostTx(slot, sequence);
        if (!sent) {
            return std::unexpected(sent.error());
        }
        ++progress;
    }
    return progress;
}

bool StreamSession::done() const noexcept {
    return started_ && !unsafe_ && stats_.accepted == options_.messages && stats_.retired == options_.messages &&
           stats_.received == options_.messages;
}

void StreamSession::RecordCompletionFailure(const urma_cr_t& cr, bool receive, int count, int index) noexcept {
    auto& diagnostic = diagnostics_.failure_cqe;
    if (diagnostic.present) {
        return;
    }
    diagnostic.present = true;
    diagnostic.receive = receive;
    diagnostic.observed_ns = SteadyNowNs();
    diagnostic.batch_count = count;
    diagnostic.batch_index = index;
    diagnostic.completion = cr;
    if (IsBoundary(cr.status)) {
        // 假 CQE 不携带合法 WR 身份，即使只是诊断也不能把垃圾 user_ctx 当作序号。
        diagnostic.user_ctx_suppressed = true;
        diagnostic.completion.user_ctx = 0;
        return;
    }
    // TX 的身份必须同时匹配本地 jetty 与仍在用的发送槽；RX user_ctx 是 buffer 槽，不是序号。
    if (!receive && !cr.flag.bs.s_r && cr.flag.bs.jetty && cr.local_id == jetty_->jetty_id.id && cr.user_ctx &&
        cr.user_ctx <= options_.messages) {
        const auto sequence = cr.user_ctx - 1;
        const auto slot = sequence % options_.window;
        if (tx_busy_[slot] && sequence_[slot] == sequence) {
            diagnostic.sequence_valid = true;
            diagnostic.sequence = sequence;
        }
    }
}

void StreamSession::CaptureTimeoutProbe() noexcept {
    auto& probe = diagnostics_.timeout_probe;
    if (probe.attempted) {
        return;
    }
    // 从这里起只保留现场；探查会消费 provider 队列，绝不能再进入正常推进或释放 DMA 依赖。
    unsafe_ = true;
    probe.attempted = true;
    probe.begin_ns = SteadyNowNs();
    if (jfce_) {
        probe.notifications_checked = true;
        std::array<urma_jfc_t*, 16> events{};
        errno = 0;
        probe.notification_result = UrmaApi::WaitJfc(jfce_, events.size(), 0, events.data());
        probe.notification_errno = errno;
        if (probe.notification_result >= 0 && probe.notification_result <= static_cast<int>(events.size())) {
            std::array<urma_jfc_t*, 16> known{};
            std::array<std::uint32_t, 16> counts{};
            counts.fill(1);
            std::uint32_t known_count = 0;
            for (int i = 0; i < probe.notification_result; ++i) {
                if (events[i] == tx_cq_) {
                    ++probe.tx_notifications;
                } else if (events[i] == rx_cq_) {
                    ++probe.rx_notifications;
                } else {
                    ++probe.foreign_notifications;
                    continue;
                }
                known[known_count++] = events[i];
            }
            // 每个已知通知恰好 ack 一次；未知对象只记录，不能把外来指针交回 provider。
            if (known_count) {
                UrmaApi::AckJfc(known.data(), counts.data(), known_count);
                probe.acknowledged = known_count;
            }
        }
    }
    errno = 0;
    probe.tx_result = UrmaApi::PollJfc(tx_cq_, probe.tx_completions.size(), probe.tx_completions.data());
    probe.tx_errno = errno;
    errno = 0;
    probe.rx_result = UrmaApi::PollJfc(rx_cq_, probe.rx_completions.size(), probe.rx_completions.data());
    probe.rx_errno = errno;
    auto sanitize = [](auto& completions, int count) {
        if (count < 0 || count > static_cast<int>(completions.size())) {
            return;
        }
        for (auto& cr : std::span(completions).first(count)) {
            if (IsBoundary(cr.status)) {
                cr.user_ctx = 0;
            }
        }
    };
    sanitize(probe.tx_completions, probe.tx_result);
    sanitize(probe.rx_completions, probe.rx_result);
    probe.end_ns = SteadyNowNs();
}

std::expected<void, ToolError> StreamSession::Arm() {
    for (auto* cq : {tx_cq_, rx_cq_}) {
        const auto status = UrmaApi::RearmJfc(cq, false);
        if (status != URMA_SUCCESS) {
            return Error("rearm streaming CQ", status);
        }
        ++stats_.rearms;
    }
    return {};
}

std::expected<std::size_t, ToolError> StreamSession::CollectNotifications() {
    if (!jfce_) {
        return std::size_t{0};
    }
    std::array<urma_jfc_t*, 16> events{};
    errno = 0;
    const int count = UrmaApi::WaitJfc(jfce_, events.size(), 0, events.data());
    if (count < 0 || count > static_cast<int>(events.size())) {
        unsafe_ = true;
        return Error("wait streaming JFC", count);
    }
    std::array<std::uint32_t, 16> counts{};
    counts.fill(1);
    bool invalid = false;
    for (int i = 0; i < count; ++i) {
        if (events[i] == tx_cq_) {
            ++stats_.tx_notifications;
        } else if (events[i] == rx_cq_) {
            ++stats_.rx_notifications;
        } else {
            invalid = true;
        }
    }
    // ack 的单位是事件，绝不能用本轮 poll 的 CQE 数替代；相同 CQ 的重复通知也分别确认。
    if (count && !invalid) {
        UrmaApi::AckJfc(events.data(), counts.data(), count);
        stats_.event_acks += count;
    }
    if (invalid) {
        unsafe_ = true;
        return Error("foreign JFC notification; retaining resources", EPROTO);
    }
    return count;
}

std::expected<void, ToolError> StreamSession::Close() {
    if (unsafe_ || !peer_quiescent_ || (started_ && !done())) {
        return Error("stream DMA/peer unresolved; retaining resources", EBUSY);
    }
    // 只有两端都停止投递且所有 TX/RX 已终结，才可收尽残留通知并删除 CQ。
    for (int tries = 0; jfce_ && tries < 1024; ++tries) {
        auto count = CollectNotifications();
        if (!count) {
            return std::unexpected(count.error());
        }
        if (!*count) {
            break;
        }
        if (tries == 1023) {
            return Error("stream notification drain limit", EBUSY);
        }
    }
    if (remote_) {
        const auto status = UrmaApi::UnimportJetty(remote_);
        if (status != URMA_SUCCESS) {
            return Error("unimport stream peer", status);
        }
        remote_ = nullptr;
    }
    if (jetty_) {
        const auto status = UrmaApi::DeleteJetty(jetty_);
        if (status != URMA_SUCCESS) {
            return Error("delete stream jetty", status);
        }
        jetty_ = nullptr;
    }
    if (jfr_) {
        const auto status = UrmaApi::DeleteJfr(jfr_);
        if (status != URMA_SUCCESS) {
            unsafe_ = true;
            return Error("delete stream JFR", status);
        }
        jfr_ = nullptr;
    }
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error("unregister stream MR", status);
        }
        segment_ = nullptr;
    }
    for (auto** cq : {&tx_cq_, &rx_cq_}) {
        if (*cq) {
            const auto status = UrmaApi::DeleteJfc(*cq);
            if (status != URMA_SUCCESS) {
                return Error("delete stream CQ", status);
            }
            *cq = nullptr;
        }
    }
    if (jfce_) {
        const auto status = UrmaApi::DeleteJfce(jfce_);
        if (status != URMA_SUCCESS) {
            return Error("delete stream JFCE", status);
        }
        jfce_ = nullptr;
    }
    ctx_ = nullptr;
    memory_ = {};
    storage_.clear();
    return {};
}

std::expected<StreamRunStats, ToolError> RunStreams(std::span<StreamSession*> sessions, const StreamOptions& options,
                                                    StreamRunDiagnostics* diagnostics) {
    StreamRunDiagnostics local_diagnostics;
    auto& diagnostic = diagnostics ? *diagnostics : local_diagnostics;
    diagnostic = {};
    diagnostic.started_ns = SteadyNowNs();
    StreamRunStats stats;
    auto fail = [&](ToolError error, const char* phase, std::optional<std::size_t> context = std::nullopt,
                    bool probe_timeout = false) -> std::expected<StreamRunStats, ToolError> {
        diagnostic.failed = true;
        diagnostic.phase = phase;
        diagnostic.context_valid = context.has_value();
        diagnostic.context = context.value_or(0);
        diagnostic.failure_ns = SteadyNowNs();
        diagnostic.failure_unix_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        diagnostic.stats = stats;
        if (probe_timeout) {
            // 先固定原始失败和全部正常计数，再做一次有界观察；任何探查结果都不改变本次失败。
            for (auto* session : sessions) {
                session->CaptureTimeoutProbe();
            }
        }
        return std::unexpected(error);
    };
    if (sessions.empty() || sessions.size() > kStreamMaxContexts) {
        return fail({"stream session count", EINVAL}, "validation");
    }
    auto deadline = Clock::now() + std::chrono::milliseconds(options.timeout_ms);
    std::array<pollfd, kStreamMaxContexts> descriptors{};
    for (;;) {
        bool done = true;
        std::size_t progress = 0;
        for (std::size_t i = 0; i < sessions.size(); ++i) {
            auto* session = sessions[i];
            auto notifications = session->CollectNotifications();
            if (!notifications) {
                return fail(notifications.error(), "collect_notifications", i);
            }
            auto advanced = session->Advance();
            if (!advanced) {
                return fail(advanced.error(), "advance", i);
            }
            progress += *advanced;
            done &= session->done();
        }
        if (done) {
            diagnostic.stats = stats;
            return stats;
        }
        if (progress) {
            deadline = Clock::now() + std::chrono::milliseconds(options.timeout_ms);
            continue;
        }
        if (Clock::now() >= deadline) {
            return fail({"stream stalled", ETIMEDOUT}, "stalled", std::nullopt, true);
        }
        if (!options.events) {
            std::this_thread::yield();
            continue;
        }
        std::optional<ToolError> arm_error;
        std::size_t arm_context = 0;
        for (std::size_t i = 0; i < sessions.size(); ++i) {
            auto armed = sessions[i]->Arm();
            if (!armed && !arm_error) {
                arm_error = armed.error();
                arm_context = i;
            }
        }
        if (options.arm_delay_us) {
            std::this_thread::sleep_for(std::chrono::microseconds(options.arm_delay_us));
        }
        // arm 与 sleep 间可能已有 CQE：再 poll 是防丢唤醒的必要步骤；有完成时直接继续推进。
        for (std::size_t i = 0; i < sessions.size(); ++i) {
            auto advanced = sessions[i]->Advance();
            if (!advanced) {
                return fail(advanced.error(), "recheck", i);
            }
            if (*advanced) {
                sessions[i]->RecordRecheckProgress();
            }
            progress += *advanced;
            descriptors[i] = {sessions[i]->event_fd(), POLLIN, 0};
        }
        if (arm_error) {
            return fail(*arm_error, "rearm", arm_context);
        }
        if (progress) {
            deadline = Clock::now() + std::chrono::milliseconds(options.timeout_ms);
            continue;
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (remaining <= 0) {
            return fail({"stream arm/recheck deadline", ETIMEDOUT}, "recheck_deadline", std::nullopt, true);
        }
        ++stats.waits;
        diagnostic.last_wait_begin_ns = SteadyNowNs();
        errno = 0;
        const auto rc = ::poll(descriptors.data(), sessions.size(), static_cast<int>(remaining));
        const auto wait_errno = errno;
        diagnostic.last_wait_end_ns = SteadyNowNs();
        diagnostic.wait_result = rc;
        diagnostic.wait_errno = wait_errno;
        for (std::size_t i = 0; i < sessions.size(); ++i) {
            diagnostic.wait_revents[i] = descriptors[i].revents;
        }
        if (rc < 0 && wait_errno == EINTR) {
            continue;
        }
        if (rc <= 0) {
            return fail({"stream event wait timeout/error; no polling fallback", rc ? wait_errno : ETIMEDOUT},
                        "event_wait", std::nullopt, rc == 0);
        }
        for (std::size_t i = 0; i < sessions.size(); ++i) {
            if (descriptors[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                return fail({"stream JFCE fd error", EIO}, "fd_ready", i);
            }
            if (descriptors[i].revents & POLLIN) {
                sessions[i]->RecordFdWake();
                auto events = sessions[i]->CollectNotifications();
                if (!events) {
                    return fail(events.error(), "fd_notifications", i);
                }
                if (!*events) {
                    ++stats.empty_wakes;
                }
            }
        }
    }
}
} // namespace tools
} // namespace kbsocket
