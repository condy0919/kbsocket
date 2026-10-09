// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/tx_drain_test/tx_drain_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr std::uint32_t kReady = 0x445201;
constexpr std::uint32_t kDrained = 0x445202;
constexpr std::uint32_t kAcknowledged = 0x445203;
constexpr std::uint32_t kClosed = 0x445204;
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
void Put32(std::span<std::byte> wire, std::uint32_t value) noexcept {
    for (int i = 3; i >= 0; --i) {
        wire[i] = static_cast<std::byte>(value & 255);
        value >>= 8;
    }
}
std::uint32_t Get32(std::span<const std::byte> wire) noexcept {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i)
        value = (value << 8) | std::to_integer<unsigned>(wire[i]);
    return value;
}
int CompletionError(const raw::TxCompletionError& error) noexcept {
    return error.pool_error && error.pool_error->provider_error ? error.pool_error->provider_error
                                                                : static_cast<int>(error.code);
}
} // namespace

std::expected<void, ToolError> ValidateDrainOptions(const TxDrainOptions& o) noexcept {
    if (o.bytes < 8 || o.bytes > 1024 * 1024 || !o.batch || o.batch > raw::TxSender::kMaxBatch || !o.timeout_ms ||
        o.timeout_ms > 3600000)
        return Error("invalid drain options", EINVAL);
    return {};
}
TxDrainHelloBytes EncodeDrainHello(const TxDrainHello& hello) noexcept {
    TxDrainHelloBytes wire{};
    Put32(wire, 0x4b424452); // KBDR，与 SEND 工具的 KBST 不互通。
    Put32(std::span(wire).subspan(4), 1);
    Put32(std::span(wire).subspan(8), hello.server ? 1 : 0);
    Put32(std::span(wire).subspan(12), hello.options.bytes);
    Put32(std::span(wire).subspan(16), hello.options.batch);
    for (std::size_t i = 0; i < 16; ++i)
        wire[20 + i] = static_cast<std::byte>(hello.endpoint.eid.raw[i]);
    Put32(std::span(wire).subspan(36), hello.endpoint.id);
    return wire;
}
std::expected<TxDrainHello, ToolError> DecodeDrainHello(const TxDrainHelloBytes& wire) noexcept {
    if (Get32(wire) != 0x4b424452 || Get32(std::span(wire).subspan(4)) != 1)
        return Error("drain protocol magic/version mismatch", EPROTO);
    const auto role = Get32(std::span(wire).subspan(8));
    if (role > 1)
        return Error("invalid peer role", EPROTO);
    TxDrainHello hello;
    hello.server = role == 1;
    hello.options.bytes = Get32(std::span(wire).subspan(12));
    hello.options.batch = Get32(std::span(wire).subspan(16));
    for (std::size_t i = 0; i < 16; ++i)
        hello.endpoint.eid.raw[i] = std::to_integer<std::uint8_t>(wire[20 + i]);
    hello.endpoint.id = Get32(std::span(wire).subspan(36));
    auto valid = ValidateDrainOptions(hello.options);
    if (!valid)
        return std::unexpected(valid.error());
    return hello;
}

std::expected<void, ToolError> TxDrainSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                    const TxDrainOptions& options, bool server) {
    if (ctx_)
        return Error("drain session already open", EBUSY);
    auto valid = ValidateDrainOptions(options);
    if (!valid)
        return valid;
    if (!ctx || options.bytes > cap.max_msg_size)
        return Error("message exceeds device capability", EINVAL);
    ctx_ = ctx;
    options_ = options;
    server_ = server;
    ran_ = false;
    stats_ = {};
    retired_.fill(false);
    raw::JettyPoolConfig config;
    config.tx_depth = server ? 1 : options.batch;
    config.rx_depth = 1;
    config.tx_cq_depth = *config.tx_depth + 1;
    config.rx_cq_depth = 1;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened)
        return Error("create jetty pool", opened.error().failure.provider_error
                                              ? opened.error().failure.provider_error
                                              : static_cast<int>(opened.error().failure.code));
    auto ticket = pool_.Reserve();
    if (!ticket)
        return Error("reserve endpoint", EIO);
    lane_ = ticket->lane;
    auto jetty = pool_.Get(lane_);
    auto cancelled = pool_.Cancel(*ticket);
    if (!jetty || !cancelled)
        return Error("get local endpoint", EIO);
    endpoint_ = (*jetty)->jetty_id;
    if (server) {
        // 对端不投递 RQ，因此没有接收 DMA buffer 或待清理的 RX WR。
        ready_ = true;
        return {};
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0)
        return Error("query page size", EINVAL);
    const auto page = static_cast<std::size_t>(page_size);
    const auto bytes = static_cast<std::size_t>(options.bytes) * options.batch;
    const auto rounded = (bytes + page - 1) / page * page;
    buffer_.resize(rounded + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(buffer_.data());
    registered_buffer_ = std::span(buffer_).subspan((page - address % page) % page, rounded);
    std::ranges::fill(registered_buffer_, std::byte{0x5a});
    urma_seg_cfg_t cfg{};
    cfg.va = reinterpret_cast<std::uintptr_t>(registered_buffer_.data());
    cfg.len = registered_buffer_.size();
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &cfg);
    if (!segment_)
        return Error(errno ? "register local memory" : "register local memory: nullptr without errno",
                     errno ? errno : EIO);
    for (std::size_t i = 0; i < options.batch; ++i)
        sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(registered_buffer_.data() + i * options.bytes),
                    .len = options.bytes,
                    .tseg = segment_};
    // 额外账本槽用于验证故障提交被 SQ 状态拒绝，而不是被账本已满提前挡住。
    auto ledger = ledger_.Open(pool_, options.batch + 1);
    if (!ledger)
        return Error("open ledger", static_cast<int>(ledger.error().code));
    auto sender = sender_.Open(ledger_);
    if (!sender)
        return Error("open sender", static_cast<int>(sender.error().code));
    ready_ = true;
    return {};
}

std::expected<void, ToolError> TxDrainSession::Run(ControlChannel& channel) {
    if (!ready_ || ran_)
        return Error("drain session not ready or already run", EINVAL);
    ran_ = true;
    auto written = channel.Write(EncodeDrainHello({endpoint_, options_, server_}), options_.timeout_ms);
    if (!written)
        return written;
    TxDrainHelloBytes wire{};
    auto received = channel.Read(wire, options_.timeout_ms);
    if (!received)
        return received;
    auto peer = DecodeDrainHello(wire);
    if (!peer)
        return std::unexpected(peer.error());
    if (peer->server == server_ || peer->options.batch != options_.batch || peer->options.bytes != options_.bytes)
        return Error("peer role/bytes/batch mismatch", EPROTO);
    if (server_) {
        auto ready = channel.SendMarker(kReady, options_.timeout_ms);
        if (!ready)
            return ready;
        auto drained = channel.ExpectMarker(kDrained, options_.timeout_ms);
        if (!drained)
            return drained;
        return channel.SendMarker(kAcknowledged, options_.timeout_ms);
    }
    urma_rjetty_t remote{};
    remote.jetty_id = peer->endpoint;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    errno = 0;
    remote_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
    if (!remote_)
        return Error("import RM_CTP peer", errno ? errno : EIO);
    auto ready = channel.ExpectMarker(kReady, options_.timeout_ms);
    if (!ready)
        return ready;
    auto drained = SendAndDrain();
    if (!drained)
        return drained;
    auto notified = channel.SendMarker(kDrained, options_.timeout_ms);
    if (!notified)
        return notified;
    return channel.ExpectMarker(kAcknowledged, options_.timeout_ms);
}

std::expected<void, ToolError> TxDrainSession::CheckSendRejected() {
    const auto before = ledger_.size();
    std::array<raw::AttemptId, 1> probe_ids{};
    auto probe = sender_.Send(std::span(requests_).first(1), probe_ids, lane_);
    if (probe || probe.error().accepted != 0 || probe.error().code != raw::TxSendErrorCode::kLedgerFailure ||
        !probe.error().ledger_error || !probe.error().ledger_error->pool_failure ||
        probe.error().ledger_error->pool_failure->code != raw::JettyPoolErrorCode::kFaulted || probe_ids[0] != 0 ||
        ledger_.size() != before)
        return Error("ERROR SQ did not reject send during reservation", EPROTO);
    auto jetty = pool_.Get(lane_);
    if (jetty || jetty.error().code != raw::JettyPoolErrorCode::kFaulted)
        return Error("ERROR SQ exposed a send handle", EPROTO);
    ++stats_.rejected_sends;
    return {};
}

std::expected<void, ToolError> TxDrainSession::Observe(std::span<const raw::TxCompletionEvent> events, bool software) {
    for (const auto& event : events) {
        if (event.error)
            return Error("completion rejected or modify failed", CompletionError(*event.error));
        if (!event.lane || event.lane->pool != lane_.pool || event.lane->index != lane_.index ||
            event.lane->epoch != lane_.epoch)
            return Error("completion belongs to another SQ", EPROTO);
        if (event.kind == raw::TxCompletionKind::kFlushDone) {
            if (software || stats_.flush_done || event.record || event.completion.user_ctx)
                return Error("invalid FLUSH_ERR_DONE boundary", EPROTO);
            ++stats_.flush_done;
            continue;
        }
        if (event.kind != raw::TxCompletionKind::kCompleted || !event.record)
            return Error("unexpected completion kind", EPROTO);
        if ((!software && stats_.flush_done) || (software && stats_.flush_done != 1))
            return Error("completion violates hardware/software boundary", EPROTO);
        const auto& record = *event.record;
        const auto operation = record.metadata.operation_id;
        if (!operation || operation > options_.batch || ids_[operation - 1] != record.id ||
            event.completion.user_ctx != record.id || record.metadata.buffer_lease != operation ||
            retired_[operation - 1])
            return Error("duplicate or foreign AttemptId", EPROTO);
        if (software && event.completion.status != URMA_CR_WR_UNHANDLED)
            return Error("software flush returned non-UNHANDLED completion", EPROTO);
        retired_[operation - 1] = true;
        ++stats_.retired;
        switch (event.completion.status) {
        case URMA_CR_SUCCESS:
            ++stats_.success;
            break;
        case URMA_CR_WR_FLUSH_ERR:
            ++stats_.flush_error;
            break;
        case URMA_CR_WR_UNHANDLED:
            ++stats_.unhandled;
            break;
        default:
            stats_.last_other_error = static_cast<int>(event.completion.status);
            ++stats_.other_error;
            break;
        }
    }
    return {};
}

std::expected<void, ToolError> TxDrainSession::SendAndDrain() {
    for (std::size_t i = 0; i < options_.batch; ++i)
        requests_[i] = {
            .metadata = {.connection_id = 1, .operation_id = i + 1, .target = remote_, .buffer_lease = i + 1},
            .sges = std::span(&sges_[i], 1)};
    // 只有全部证据验证通过才解除保留。包括部分提交、缺失边界和错误完成在内的失败都不猜测清理。
    unsafe_ = true;
    auto posted =
        sender_.Send(std::span(requests_).first(options_.batch), std::span(ids_).first(options_.batch), lane_);
    if (!posted) {
        stats_.accepted_known = posted.error().accepted.has_value();
        stats_.accepted = static_cast<std::uint32_t>(posted.error().accepted.value_or(0));
    }
    if (!posted || *posted != options_.batch)
        return Error("SEND batch not fully accepted; resources retained",
                     posted ? EPROTO
                            : (posted.error().provider_status ? posted.error().provider_status
                                                              : static_cast<int>(posted.error().code)));
    stats_.accepted = static_cast<std::uint32_t>(*posted);
    // 不先 poll，确保切 ERROR 时账本中的整批请求尚未退休。
    auto started = pool_.BeginDrain(lane_);
    if (!started)
        return Error("modify jetty ERROR", started.error().provider_error);
    auto rejected = CheckSendRejected();
    if (!rejected)
        return rejected;
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<raw::TxCompletionEvent, raw::TxCompletionProcessor::kMaxPollBatch> events{};
    while (!stats_.flush_done) {
        if (Clock::now() >= deadline)
            return Error("timeout waiting for FLUSH_ERR_DONE", ETIMEDOUT);
        auto batch = processor_.Poll(events);
        if (!batch)
            return Error("poll TX", CompletionError(batch.error()));
        auto observed = Observe(std::span(events).first(batch->count), false);
        if (!observed)
            return observed;
        if (!batch->count)
            std::this_thread::yield();
    }
    rejected = CheckSendRejected();
    if (!rejected)
        return rejected;
    while (true) {
        if (Clock::now() >= deadline)
            return Error("software flush timeout", ETIMEDOUT);
        auto batch = processor_.Flush(lane_, events);
        if (!batch)
            return Error("flush TX", CompletionError(batch.error()));
        auto observed = Observe(std::span(events).first(batch->count), true);
        if (!observed)
            return observed;
        if (!batch->count)
            break;
    }
    if (stats_.retired != stats_.accepted || ledger_.size() || pool_.lane_state(lane_) != raw::JettyLaneState::kDrained)
        return Error("incomplete SQ retirement", EPROTO);
    rejected = CheckSendRejected();
    if (!rejected)
        return rejected;
    unsafe_ = false;
    return {};
}

std::expected<void, ToolError> TxDrainSession::Close() noexcept {
    if (unsafe_ || ledger_.size())
        return Error("unresolved DMA/test state; retaining session", EBUSY);
    ready_ = false;
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
        return Error("close jetty pool",
                     pool.error().provider_error ? pool.error().provider_error : static_cast<int>(pool.error().code));
    if (segment_) {
        auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS)
            return Error("unregister memory", status);
        segment_ = nullptr;
    }
    registered_buffer_ = {};
    std::vector<std::byte>().swap(buffer_);
    ctx_ = nullptr;
    return {};
}
std::expected<void, ToolError> ConfirmDrainClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept {
    auto sent = channel.SendMarker(kClosed, timeout_ms);
    if (!sent)
        return sent;
    return channel.ExpectMarker(kClosed, timeout_ms);
}
} // namespace tools
} // namespace kbsocket
