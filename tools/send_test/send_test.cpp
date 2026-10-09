// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/send_test/send_test.hpp"

#include <algorithm>
#include <cerrno>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
auto Error(const char* operation, int code = errno) noexcept {
    return std::unexpected(SendTestError{operation, code});
}
} // namespace

std::expected<void, SendTestError> SendTestSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                         const SendTestOptions& options) {
    if (ctx_)
        return Error("session already open", EBUSY);
    auto valid = ValidateOptions(options);
    if (!valid)
        return valid;
    if (!ctx || options.bytes > cap.max_msg_size)
        return Error("message exceeds device capability", EINVAL);
    options_ = options;
    ctx_ = ctx;
    raw::JettyPoolConfig config;
    config.tx_depth = config.rx_depth = options.batch;
    config.tx_cq_depth = options.batch + 1;
    config.rx_cq_depth = options.batch;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened)
        return Error("create jetty pool", opened.error().failure.provider_error);
    // 只借用句柄读取公开端点，不占用实际发送额度。
    auto ticket = pool_.Reserve();
    if (!ticket)
        return Error("reserve endpoint", EIO);
    auto jetty = pool_.Get(ticket->lane);
    auto cancelled = pool_.Cancel(*ticket);
    if (!jetty || !cancelled)
        return Error("get endpoint", EIO);
    jetty_ = *jetty;
    buffer_.resize(static_cast<std::size_t>(options.bytes) * options.batch);
    urma_seg_cfg_t segment{};
    segment.va = reinterpret_cast<std::uintptr_t>(buffer_.data());
    segment.len = buffer_.size();
    segment.flag.bs.access = URMA_ACCESS_LOCAL_ONLY;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &segment);
    if (!segment_) {
        const int saved_errno = errno;
        if (!saved_errno) {
            return Error("register local memory: provider returned nullptr without errno; inspect URMA/UDMA logs", EIO);
        }
        return Error("register local memory", saved_errno);
    }
    for (std::size_t i = 0; i < options.batch; ++i) {
        sges_[i].addr = reinterpret_cast<std::uintptr_t>(buffer_.data() + i * options.bytes);
        sges_[i].len = options.bytes;
        sges_[i].tseg = segment_;
    }
    auto ledger = ledger_.Open(pool_, options.batch);
    if (!ledger)
        return Error("open ledger", static_cast<int>(ledger.error().code));
    auto sender = sender_.Open(ledger_);
    if (!sender)
        return Error("open sender", static_cast<int>(sender.error().code));
    return {};
}

std::expected<void, SendTestError> SendTestSession::Run(ControlChannel& channel, bool server) {
    if (!segment_ || !jetty_)
        return Error("session not open", EINVAL);
    auto local = EncodeHello({jetty_->jetty_id, options_});
    auto written = channel.Write(local, options_.timeout_ms);
    if (!written)
        return written;
    HelloBytes wire{};
    auto received = channel.Read(wire, options_.timeout_ms);
    if (!received)
        return received;
    auto peer = DecodeHello(wire);
    if (!peer)
        return std::unexpected(peer.error());
    if (peer->options.bytes != options_.bytes || peer->options.messages != options_.messages ||
        peer->options.batch != options_.batch)
        return Error("peer bytes/messages/batch mismatch", EPROTO);
    if (!server) {
        urma_rjetty_t remote{};
        remote.jetty_id = peer->endpoint;
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remote_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!remote_)
            return Error("import RM_CTP peer");
    }
    for (std::uint32_t base = 0; base < options_.messages;) {
        const auto count = std::min(options_.batch, options_.messages - base);
        auto result = server ? ReceiveBatch(base, count, channel) : SendBatch(base, count, channel);
        if (!result)
            return result;
        base += count;
    }
    // 发送端收到最终接收确认后再通知接收端，两端都结束数据路径才销毁对象。
    if (server)
        return channel.ExpectMarker(0xffffffff, options_.timeout_ms);
    return channel.SendMarker(0xffffffff, options_.timeout_ms);
}

std::expected<void, SendTestError> SendTestSession::ReceiveBatch(std::uint32_t base, std::uint32_t count,
                                                                 ControlChannel& channel) {
    seen_slots_.fill(false);
    seen_messages_.fill(false);
    for (std::uint32_t i = 0; i < count; ++i) {
        recv_wrs_[i] = {};
        recv_wrs_[i].src = {&sges_[i], 1};
        recv_wrs_[i].user_ctx = i + 1;
        recv_wrs_[i].next = i + 1 < count ? &recv_wrs_[i + 1] : nullptr;
    }
    urma_jfr_wr_t* bad = nullptr;
    // 即使 provider 返回部分失败，也保守保留整批内存；失败测试不猜测已挂入 RQ 的数量。
    pending_rx_ = count;
    auto status = UrmaApi::PostJfrWr(pool_.jfr(), recv_wrs_.data(), &bad);
    if (status != URMA_SUCCESS)
        return Error("post receive", status);
    auto ready = channel.SendMarker(base + count, options_.timeout_ms);
    if (!ready)
        return ready;
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<urma_cr_t, 64> completions{};
    while (pending_rx_) {
        if (Clock::now() >= deadline)
            return Error("RX completion timeout", ETIMEDOUT);
        auto n = pool_.PollRecv(completions);
        if (!n)
            return Error("poll RX", n.error().provider_error);
        if (!*n) {
            std::this_thread::yield();
            continue;
        }
        for (int i = 0; i < *n; ++i) {
            const auto& cr = completions[i];
            if (cr.status != URMA_CR_SUCCESS || !cr.flag.bs.s_r || !cr.user_ctx || cr.user_ctx > count ||
                cr.completion_len != options_.bytes)
                return Error("invalid RX completion", static_cast<int>(cr.status));
            const auto slot = static_cast<std::size_t>(cr.user_ctx - 1);
            if (seen_slots_[slot])
                return Error("duplicate RX slot", EPROTO);
            seen_slots_[slot] = true;
            --pending_rx_;
            auto payload = std::span(buffer_).subspan(slot * options_.bytes, options_.bytes);
            auto sequence = CheckPayload(payload);
            if (!sequence)
                return std::unexpected(sequence.error());
            if (*sequence < base || *sequence >= static_cast<std::uint64_t>(base) + count ||
                seen_messages_[*sequence - base])
                return Error("duplicate/out-of-range message", EPROTO);
            seen_messages_[*sequence - base] = true;
        }
    }
    return channel.SendMarker(base + count, options_.timeout_ms);
}

std::expected<void, SendTestError> SendTestSession::SendBatch(std::uint32_t base, std::uint32_t count,
                                                              ControlChannel& channel) {
    auto ready = channel.ExpectMarker(base + count, options_.timeout_ms);
    if (!ready)
        return ready;
    for (std::uint32_t i = 0; i < count; ++i) {
        FillPayload(std::span(buffer_).subspan(static_cast<std::size_t>(i) * options_.bytes, options_.bytes),
                    static_cast<std::uint64_t>(base) + i);
        requests_[i] = {.metadata = {.connection_id = 1,
                                     .operation_id = static_cast<std::uint64_t>(base) + i + 1,
                                     .target = remote_,
                                     .buffer_lease = i + 1},
                        .sges = std::span(&sges_[i], 1)};
    }
    auto submitted = sender_.Send(std::span(requests_).first(count), std::span(ids_).first(count));
    if (!submitted) {
        // 保留部分成功、提交边界不明和记账异常现场，不自动重放。
        unsafe_ = true;
        return Error("post SEND (resources retained)", submitted.error().provider_status);
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<raw::TxCompletionEvent, 64> events{};
    while (ledger_.size()) {
        if (Clock::now() >= deadline)
            return Error("TX completion timeout", ETIMEDOUT);
        auto batch = processor_.Poll(events);
        if (!batch)
            return Error("poll TX", static_cast<int>(batch.error().code));
        for (std::size_t i = 0; i < batch->count; ++i) {
            if (events[i].kind != raw::TxCompletionKind::kCompleted || events[i].completion.status != URMA_CR_SUCCESS) {
                unsafe_ = true;
                return Error("TX error/boundary completion", static_cast<int>(events[i].completion.status));
            }
        }
        if (!batch->count)
            std::this_thread::yield();
    }
    return channel.ExpectMarker(base + count, options_.timeout_ms);
}

std::expected<void, SendTestError> SendTestSession::Close() noexcept {
    if (unsafe_ || pending_rx_ || ledger_.size())
        return Error("DMA state unresolved; retaining session", EBUSY);
    auto sender = sender_.Close();
    if (!sender)
        return Error("close sender", static_cast<int>(sender.error().code));
    auto ledger = ledger_.Close();
    if (!ledger)
        return Error("close ledger", static_cast<int>(ledger.error().code));
    if (remote_) {
        auto status = UrmaApi::UnimportJetty(remote_);
        if (status != URMA_SUCCESS)
            return Error("unimport peer", status);
        remote_ = nullptr;
    }
    auto pool = pool_.Close();
    if (!pool)
        return Error("close jetty pool", pool.error().provider_error);
    jetty_ = nullptr;
    if (segment_) {
        auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS)
            return Error("unregister memory", status);
        segment_ = nullptr;
    }
    std::vector<std::byte>().swap(buffer_);
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
