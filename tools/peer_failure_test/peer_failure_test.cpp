// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/peer_failure_test/peer_failure_test.hpp"

#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <span>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
auto Error(const char* operation, int code) {
    return std::unexpected(ToolError{operation, code});
}
void Put32(std::span<std::byte> out, std::uint32_t value) {
    for (int i = 3; i >= 0; --i) {
        out[i] = static_cast<std::byte>(value & 255);
        value >>= 8;
    }
}
std::uint32_t Get32(std::span<const std::byte> in) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | std::to_integer<std::uint32_t>(in[i]);
    }
    return value;
}
bool Terminal(urma_cr_status_t status) {
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
} // namespace

std::expected<void, ToolError> ValidatePeerFailureOptions(const PeerFailureOptions& o) noexcept {
    if (o.bytes < 1 || o.bytes > 4096 || o.batch == 0 || o.batch > raw::TxSender::kMaxBatch || o.timeout_ms == 0 ||
        o.timeout_ms > 600000 || o.async_grace_ms > 60000 || o.fault_delay_ms > 600000 ||
        (o.fault != PeerFault::kProcessExit && o.fault != PeerFault::kExternal &&
         o.fault != PeerFault::kInflightExit)) {
        return Error("invalid peer failure options", EINVAL);
    }
    return {};
}
PeerFailureHelloBytes EncodePeerFailureHello(const PeerFailureHello& hello) noexcept {
    PeerFailureHelloBytes out{};
    Put32(out, 0x4b425046); // KBPF，独立于其他工具的控制协议。
    Put32(std::span(out).subspan(4), 1);
    Put32(std::span(out).subspan(8), hello.server ? 1 : 0);
    Put32(std::span(out).subspan(12), hello.options.bytes);
    Put32(std::span(out).subspan(16), hello.options.batch);
    Put32(std::span(out).subspan(20), static_cast<std::uint32_t>(hello.options.fault));
    Put32(std::span(out).subspan(24), hello.options.fault_delay_ms);
    for (std::size_t i = 0; i < 16; ++i) {
        out[28 + i] = static_cast<std::byte>(hello.endpoint.eid.raw[i]);
    }
    Put32(std::span(out).subspan(44), hello.endpoint.id);
    return out;
}
std::expected<PeerFailureHello, ToolError> DecodePeerFailureHello(const PeerFailureHelloBytes& wire) noexcept {
    if (Get32(wire) != 0x4b425046 || Get32(std::span(wire).subspan(4)) != 1 || Get32(std::span(wire).subspan(8)) > 1) {
        return Error("peer failure protocol mismatch", EPROTO);
    }
    PeerFailureHello hello;
    hello.server = Get32(std::span(wire).subspan(8)) != 0;
    hello.options.bytes = Get32(std::span(wire).subspan(12));
    hello.options.batch = Get32(std::span(wire).subspan(16));
    hello.options.fault = static_cast<PeerFault>(Get32(std::span(wire).subspan(20)));
    hello.options.fault_delay_ms = Get32(std::span(wire).subspan(24));
    for (std::size_t i = 0; i < 16; ++i) {
        hello.endpoint.eid.raw[i] = std::to_integer<std::uint8_t>(wire[28 + i]);
    }
    hello.endpoint.id = Get32(std::span(wire).subspan(44));
    auto valid = ValidatePeerFailureOptions(hello.options);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    return hello;
}
std::expected<void, ToolError> CheckPeerFailureHello(const PeerFailureHello& peer, const PeerFailureOptions& local,
                                                     bool server) noexcept {
    if (peer.server == server || peer.options.bytes != local.bytes || peer.options.batch != local.batch ||
        peer.options.fault != local.fault || peer.options.fault_delay_ms != local.fault_delay_ms) {
        return Error("peer role/options mismatch", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> PollPeerAsync(urma_context_t* ctx, PeerAsyncStats& stats) noexcept {
    if (!ctx) {
        return Error("null async context", EINVAL);
    }
    if (ctx->async_fd < 0) {
        return {};
    }
    stats.available = true;
    // 有界取事件，避免事件风暴饿死 CQ 推进；下一轮会继续读取。
    for (int i = 0; i < 64; ++i) {
        pollfd fd{ctx->async_fd, POLLIN, 0};
        const int ready = ::poll(&fd, 1, 0);
        if (ready < 0) {
            if (errno == EINTR) {
                return {};
            }
            return Error("poll async fd", errno);
        }
        if (ready == 0) {
            return {};
        }
        if (!(fd.revents & POLLIN)) {
            return Error("async fd closed or invalid", EIO);
        }
        urma_async_event_t event{};
        const auto status = UrmaApi::GetAsyncEvent(ctx, &event);
        if (status != URMA_SUCCESS) {
            return Error("get async event", status);
        }
        // 不解引用 event.element 的资源指针，事件仅提供原始类型观测。
        const auto type = static_cast<std::uint32_t>(event.event_type);
        UrmaApi::AckAsyncEvent(&event);
        ++stats.acknowledged;
        if (type < stats.types.size()) {
            ++stats.types[type];
        } else {
            ++stats.other;
        }
    }
    return {};
}

std::expected<void, ToolError> PeerFailureSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                        const PeerFailureOptions& options, bool server) {
    auto valid = ValidatePeerFailureOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || ctx_ || options.bytes > cap.max_msg_size) {
        return Error("invalid or already open session", EINVAL);
    }
    ctx_ = ctx;
    options_ = options;
    server_ = server;
    raw::JettyPoolConfig config;
    config.tx_depth = options.batch;
    config.rx_depth = server ? options.batch : 1;
    config.tx_cq_depth = options.batch + 1;
    config.rx_cq_depth = *config.rx_depth;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("open peer failure queues", opened.error().failure.provider_error);
    }
    auto ticket = pool_.Reserve();
    if (!ticket) {
        return Error("reserve endpoint", EIO);
    }
    lane_ = ticket->lane;
    auto jetty = pool_.Get(lane_);
    auto cancelled = pool_.Cancel(*ticket);
    if (!jetty || !cancelled) {
        return Error("get endpoint", EIO);
    }
    endpoint_ = (*jetty)->jetty_id;
    if (server) {
        auto rx = rx_.Open(pool_);
        if (!rx) {
            return Error("open receiver", rx.error().provider_error);
        }
        auto posted = rx_.Refill(1);
        if (!posted || *posted != 1) {
            return Error("post baseline RX", posted ? EPROTO : posted.error().provider_error);
        }
    } else {
        const long page_size = ::sysconf(_SC_PAGESIZE);
        if (page_size <= 0) {
            return Error("page size", EINVAL);
        }
        const auto page = static_cast<std::size_t>(page_size);
        const auto length = (static_cast<std::size_t>(options.bytes) * options.batch + page - 1) / page * page;
        buffer_.resize(length + page - 1);
        const auto address = reinterpret_cast<std::uintptr_t>(buffer_.data());
        auto memory = std::span(buffer_).subspan((page - address % page) % page, length);
        std::ranges::fill(memory, std::byte{0x5a});
        urma_seg_cfg_t cfg{};
        cfg.va = reinterpret_cast<std::uintptr_t>(memory.data());
        cfg.len = memory.size();
        cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
        segment_ = UrmaApi::RegisterSeg(ctx, &cfg);
        if (!segment_) {
            return Error("register send memory", errno ? errno : EIO);
        }
        for (std::size_t i = 0; i < options.batch; ++i) {
            sges_[i] = {.addr = cfg.va + i * options.bytes, .len = options.bytes, .tseg = segment_};
        }
        auto ledger = ledger_.Open(pool_, options.batch);
        if (!ledger) {
            return Error("open ledger", EIO);
        }
        auto sender = sender_.Open(ledger_);
        if (!sender) {
            return Error("open sender", EIO);
        }
    }
    ready_ = true;
    return {};
}

std::expected<void, ToolError> PeerFailureSession::Import(const urma_jetty_id_t& endpoint) {
    if (!ready_ || server_ || remote_) {
        return Error("invalid import state", EINVAL);
    }
    urma_rjetty_t remote{};
    remote.jetty_id = endpoint;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    remote_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
    if (!remote_) {
        return Error("import peer endpoint", errno ? errno : EIO);
    }
    return {};
}

std::expected<void, ToolError> PeerFailureSession::ReceiveBaseline() {
    if (!ready_ || !server_) {
        return Error("invalid receiver state", EINVAL);
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<raw::RxBufferEvent, 1> events{};
    while (Clock::now() < deadline) {
        auto count = rx_.Poll(events);
        if (!count) {
            return Error("poll baseline RX", count.error().provider_error);
        }
        if (*count == 0) {
            std::this_thread::yield();
            continue;
        }
        const auto& event = events[0];
        const bool valid = !event.error && event.lease && event.payload.size() == options_.bytes &&
                           std::ranges::all_of(event.payload, [](std::byte b) { return b == std::byte{0x5a}; });
        if (event.lease) {
            auto released = rx_.Release(*event.lease);
            if (!released) {
                return Error("release baseline RX", EPROTO);
            }
        }
        if (!valid) {
            return Error("baseline payload invalid", EPROTO);
        }
        return {};
    }
    return Error("baseline RX timeout", ETIMEDOUT);
}

std::expected<void, ToolError> PeerFailureSession::PrepareProbeReceives() {
    if (!ready_ || !server_ || rx_.posted() || rx_.held()) {
        return Error("probe RX not quiescent", EBUSY);
    }
    auto posted = rx_.Refill(options_.batch);
    if (!posted || *posted != options_.batch) {
        return Error("post observation RX", posted ? EPROTO : posted.error().provider_error);
    }
    return {};
}

std::expected<void, ToolError> PeerFailureSession::ObserveTx(const urma_cr_t& cr, std::uint64_t elapsed_ns) {
    // 伪 CQE 不含合法 user_ctx；必须先校验状态、方向和本地 SQ，再查账本。
    if (!Terminal(cr.status) || cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.local_id != endpoint_.id) {
        unsafe_ = true;
        return Error("unexpected TX CQE", static_cast<int>(cr.status));
    }
    auto record = ledger_.Lookup(cr.user_ctx);
    if (!record || record->metadata.operation_generation != generation_ || record->metadata.target != remote_ ||
        record->metadata.buffer_lease == 0 || record->metadata.buffer_lease > tx_.accepted) {
        unsafe_ = true;
        return Error("foreign TX CQE", EPROTO);
    }
    const auto slot = static_cast<std::size_t>(record->metadata.buffer_lease - 1);
    if (seen_[slot] || ids_[slot] != cr.user_ctx || record->ticket.lane.index != lane_.index ||
        record->ticket.lane.epoch != lane_.epoch || record->ticket.lane.pool != lane_.pool) {
        unsafe_ = true;
        return Error("duplicate TX CQE", EPROTO);
    }
    auto retired = ledger_.Complete(cr.user_ctx);
    if (!retired) {
        unsafe_ = true;
        return Error("retire TX", EPROTO);
    }
    seen_[slot] = true;
    if (tx_.retired == 0) {
        tx_.first_cqe_ns = elapsed_ns;
    }
    tx_.last_cqe_ns = elapsed_ns;
    ++tx_.retired;
    ++tx_.statuses[static_cast<std::size_t>(cr.status)];
    return {};
}

std::expected<void, ToolError> PeerFailureSession::Submit(std::uint32_t count) {
    if (!ready_ || server_ || !remote_ || unsafe_ || ledger_.size() || count == 0 || count > options_.batch) {
        return Error("invalid send state", EINVAL);
    }
    tx_ = {};
    seen_.fill(false);
    ++generation_;
    for (std::size_t i = 0; i < count; ++i) {
        requests_[i] = {.metadata = {.connection_id = 1,
                                     .operation_id = i + 1,
                                     .operation_generation = generation_,
                                     .target = remote_,
                                     .buffer_lease = i + 1},
                        .sges = std::span(sges_).subspan(i, 1)};
    }
    requested_ = count;
    submitted_ = Clock::now();
    auto sent = sender_.Send(std::span(requests_).first(count), std::span(ids_).first(count), lane_);
    tx_.post_status = sent ? 0 : sent.error().provider_status;
    if (!sent && !sent.error().accepted) {
        tx_.accepted_known = false;
        unsafe_ = true;
        return Error("unknown accepted prefix; retaining DMA", EPROTO);
    }
    tx_.accepted = static_cast<std::uint32_t>(sent ? *sent : *sent.error().accepted);
    if (!sent && sent.error().code != raw::TxSendErrorCode::kPostFailed) {
        unsafe_ = ledger_.size() != 0;
        return Error("sender accounting/reservation failed", static_cast<int>(sent.error().code));
    }
    return {};
}

std::expected<void, ToolError> PeerFailureSession::PollCompletions() {
    if (!ready_ || server_ || unsafe_ || generation_ == 0) {
        return Error("invalid completion observation state", EINVAL);
    }
    auto events = PollPeerAsync(ctx_, async_);
    if (!events) {
        unsafe_ = true;
        return events;
    }
    std::array<urma_cr_t, 64> completions{};
    auto polled = pool_.PollSend(completions);
    if (!polled) {
        unsafe_ = true;
        return Error("poll peer failure TX", polled.error().provider_error);
    }
    std::optional<ToolError> first_error;
    for (int i = 0; i < *polled; ++i) {
        const auto ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - submitted_).count());
        auto observed = ObserveTx(completions[static_cast<std::size_t>(i)], ns);
        if (!observed && !first_error) {
            first_error = observed.error();
        }
    }
    if (first_error) {
        return std::unexpected(*first_error);
    }
    return {};
}

std::expected<void, ToolError> PeerFailureSession::WaitForTerminal(bool baseline) {
    if (!ready_ || server_ || unsafe_ || generation_ == 0) {
        return Error("invalid terminal wait state", EINVAL);
    }
    const auto deadline = submitted_ + std::chrono::milliseconds(options_.timeout_ms);
    while (ledger_.size()) {
        // 控制确认期间 CQE 可能已排队；即使截止时间刚到，也先消费已可见的终结。
        auto observed = PollCompletions();
        if (!observed) {
            return observed;
        }
        if (ledger_.size() && Clock::now() >= deadline) {
            return Error("accepted WR terminal CQE not observed", ETIMEDOUT);
        }
        std::this_thread::yield();
    }
    if (baseline &&
        (tx_.post_status != 0 || tx_.accepted != requested_ || tx_.statuses[URMA_CR_SUCCESS] != requested_)) {
        return Error("baseline SEND did not succeed", EPROTO);
    }
    // 故障观测允许合法终结错误或明确拒绝；它们都作为原始观测打印，不重放。
    return {};
}

std::expected<void, ToolError> PeerFailureSession::Send(std::uint32_t count, bool baseline) {
    auto submitted = Submit(count);
    if (!submitted) {
        return submitted;
    }
    return WaitForTerminal(baseline);
}

std::expected<std::uint32_t, ToolError> PeerFailureSession::QueryState() {
    auto jetty = pool_.Get(lane_);
    if (!jetty) {
        return Error("local SQ unavailable for query", EBUSY);
    }
    urma_jetty_cfg_t cfg{};
    urma_jetty_attr_t attr{};
    const auto status = UrmaApi::QueryJetty(*jetty, &cfg, &attr);
    if (status != URMA_SUCCESS) {
        return Error("query local SQ", status);
    }
    return static_cast<std::uint32_t>(attr.state);
}

std::expected<void, ToolError> PeerFailureSession::ObserveAsync(std::uint32_t duration_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(duration_ms);
    do {
        auto events = PollPeerAsync(ctx_, async_);
        if (!events) {
            return events;
        }
        if (Clock::now() >= deadline) {
            return {};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (true);
}

std::expected<void, ToolError> PeerFailureSession::Close() noexcept {
    if (unsafe_ || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("DMA state unresolved; retain resources until process exit", EBUSY);
    }
    auto sender = sender_.Close();
    if (!sender) {
        return Error("close sender", EBUSY);
    }
    auto ledger = ledger_.Close();
    if (!ledger) {
        return Error("close ledger", EBUSY);
    }
    auto rx = rx_.Close();
    if (!rx) {
        return Error("close RX pool", rx.error().provider_error);
    }
    if (remote_) {
        const auto status = UrmaApi::UnimportJetty(remote_);
        if (status != URMA_SUCCESS) {
            return Error("unimport peer", status);
        }
        remote_ = nullptr;
    }
    auto pool = pool_.Close();
    if (!pool) {
        return Error("close queues", pool.error().provider_error);
    }
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error("unregister send memory", status);
        }
        segment_ = nullptr;
    }
    buffer_.clear();
    ctx_ = nullptr;
    ready_ = false;
    return {};
}
} // namespace tools
} // namespace kbsocket
