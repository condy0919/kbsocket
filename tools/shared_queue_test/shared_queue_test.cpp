// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/shared_queue_test/shared_queue_test.hpp"

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
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}

std::expected<void, ToolError> Barrier(ControlChannel& channel, std::uint32_t marker,
                                       std::uint32_t timeout_ms) noexcept {
    auto sent = channel.SendMarker(marker, timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(marker, timeout_ms);
}

bool SameEndpoint(const urma_jetty_id_t& a, const urma_jetty_id_t& b) noexcept {
    return a.id == b.id && std::memcmp(a.eid.raw, b.eid.raw, sizeof(a.eid.raw)) == 0;
}
} // namespace

std::size_t SharedQueueSession::Slot(std::uint32_t lane, std::uint32_t offset) const noexcept {
    return static_cast<std::size_t>(lane) * options_.batch + offset;
}

std::expected<void, ToolError> SharedQueueSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                        const SharedQueueOptions& options, bool server) {
    if (ctx_) {
        return Error("session already open", EBUSY);
    }
    auto valid = ValidateSharedOptions(options);
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
    peer_quiescent_ = true;
    unsafe_ = false;
    ctx_ = ctx;
    const auto total = options.jetties * options.batch;
    const auto page = static_cast<std::size_t>(page_size);
    const auto bytes = static_cast<std::size_t>(total) * options.bytes;
    const auto aligned = (bytes + page - 1) / page * page;
    // 数据路径只复用这些槽位；每个 SQ 有独立 TX buffer，不能在其他 SQ 仍 DMA 时覆盖。
    storage_.resize(aligned + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
    registered_ = std::span(storage_).subspan((page - address % page) % page, aligned);
    sges_.resize(total);
    ids_.resize(total);
    tx_seen_.resize(total);
    rx_seen_.resize(total);

    raw::JettyPoolConfig config;
    config.jetty_count = options.jetties;
    config.tx_depth = options.batch;
    config.rx_depth = total;
    config.tx_cq_depth = options.jetties * (options.batch + 1);
    config.rx_cq_depth = total;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        const auto& error = opened.error().failure;
        return Error("create shared jetty pool (check device queue limits)",
                     error.provider_error ? error.provider_error : static_cast<int>(error.code));
    }
    for (std::uint32_t i = 0; i < options.jetties; ++i) {
        // Reserve 的轮转游标让每次看到不同 SQ；这里仅读取端点，立即归还临时票据。
        auto ticket = pool_.Reserve();
        if (!ticket) {
            return Error("reserve local endpoint", static_cast<int>(ticket.error().code));
        }
        auto jetty = pool_.Get(ticket->lane);
        auto cancelled = pool_.Cancel(*ticket);
        if (!jetty || !cancelled) {
            return Error("read local endpoint", EIO);
        }
        lanes_[i] = ticket->lane;
        local_.endpoints[i] = (*jetty)->jetty_id;
    }
    auto rx = rx_.Open(pool_);
    if (!rx) {
        return Error("open shared RX buffers",
                     rx.error().provider_error ? rx.error().provider_error : static_cast<int>(rx.error().code));
    }
    urma_seg_cfg_t segment{};
    segment.va = reinterpret_cast<std::uintptr_t>(registered_.data());
    segment.len = registered_.size();
    segment.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &segment);
    if (!segment_) {
        return Error("register TX memory (inspect URMA/UDMA logs)", errno ? errno : EIO);
    }
    for (std::size_t i = 0; i < total; ++i) {
        sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(registered_.data() + i * options.bytes),
                    .len = options.bytes,
                    .tseg = segment_};
    }
    auto ledger = ledger_.Open(pool_, total);
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

std::expected<void, ToolError> SharedQueueSession::Run(ControlChannel& channel) {
    if (!ready_ || started_) {
        return Error("session not ready or already run", EINVAL);
    }
    started_ = true;
    auto peer = ExchangeSharedHello(channel, local_);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    peer_ = *peer;
    for (std::uint32_t i = 0; i < options_.jetties; ++i) {
        urma_rjetty_t rjetty{};
        rjetty.jetty_id = peer_.endpoints[i];
        rjetty.trans_mode = URMA_TM_RM;
        rjetty.type = URMA_JETTY;
        rjetty.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remotes_[i] = UrmaApi::ImportJetty(ctx_, &rjetty, &token);
        if (!remotes_[i]) {
            return Error("import RM_CTP peer endpoint", errno ? errno : EIO);
        }
    }
    for (std::uint32_t base = 0; base < options_.messages;) {
        const auto count = std::min(options_.batch, options_.messages - base);
        auto result = RunRound(channel, base, count);
        if (!result) {
            return result;
        }
        base += count;
    }
    const auto expected = static_cast<std::uint64_t>(options_.jetties) * options_.messages;
    if (!stats_.accepted_known || stats_.accepted != expected || stats_.retired != expected ||
        stats_.received != expected) {
        return Error("final TX/RX counts mismatch", EPROTO);
    }
    for (std::uint32_t source = 0; source < options_.jetties; ++source) {
        for (std::uint32_t target = 0; target < options_.jetties; ++target) {
            const auto pair = source * kSharedMaxJetties + target;
            if (!stats_.tx_pairs[pair] || stats_.tx_pairs[pair] != stats_.rx_pairs[pair]) {
                return Error("source/target coverage mismatch", EPROTO);
            }
        }
    }
    // 双方已完成所有 TX 和 RX 才允许撤销导入和销毁本地端点。
    auto done = Barrier(channel, 0xffffffff, options_.timeout_ms);
    if (!done) {
        return done;
    }
    peer_quiescent_ = true;
    return {};
}

std::expected<void, ToolError> SharedQueueSession::RunRound(ControlChannel& channel, std::uint32_t base,
                                                            std::uint32_t count) {
    std::fill(tx_seen_.begin(), tx_seen_.end(), 0);
    std::fill(rx_seen_.begin(), rx_seen_.end(), 0);
    const auto total = static_cast<std::size_t>(count) * options_.jetties;
    for (std::size_t posted = 0; posted < total;) {
        const auto requested = std::min(total - posted, raw::RxBufferPool::kMaxBatch);
        auto result = rx_.Refill(requested);
        if (!result) {
            return Error("post shared RX buffers", result.error().provider_error
                                                       ? result.error().provider_error
                                                       : static_cast<int>(result.error().code));
        }
        if (*result != requested) {
            return Error("insufficient shared RX buffers", ENOBUFS);
        }
        posted += *result;
    }
    // READY 只在全量 RX 已挂入共享 JFR 后发送；之后即使本端 post 失败，对端也可能已在 DMA。
    peer_quiescent_ = false;
    auto ready = Barrier(channel, base + count, options_.timeout_ms);
    if (!ready) {
        return ready;
    }
    auto submitted = PostRound(base, count);
    if (!submitted) {
        return submitted;
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    const auto receive_end = stats_.received + total;
    while (ledger_.size() || stats_.received < receive_end) {
        if (Clock::now() >= deadline) {
            return Error("shared TX/RX completion timeout", ETIMEDOUT);
        }
        // 每轮同时推进 TX 和 RX，避免双向通信时一端只等自己的 TX 而饿死 RX 处理。
        auto tx = PollTx(base, count);
        auto rx = PollRx(base, count);
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
    if (rx_.posted() || rx_.held()) {
        return Error("RX ownership not fully returned", EBUSY);
    }
    // 当前批的全部完成已经取到；多出来的 CQE 也不能被当作下一轮的有效工作。
    auto tx = PollTx(base, count);
    auto rx = PollRx(base, count);
    if (!tx) {
        return std::unexpected(tx.error());
    }
    if (!rx) {
        return std::unexpected(rx.error());
    }
    return Barrier(channel, base + count, options_.timeout_ms);
}

std::expected<void, ToolError> SharedQueueSession::PostRound(std::uint32_t base, std::uint32_t count) {
    for (std::uint32_t lane = 0; lane < options_.jetties; ++lane) {
        for (std::uint32_t i = 0; i < count; ++i) {
            SharedFrame frame{
                .source = lane,
                .target = static_cast<std::uint32_t>((static_cast<std::uint64_t>(base) + i + lane) % options_.jetties),
                .sequence = static_cast<std::uint64_t>(base) + i,
                .server = local_.server};
            const auto slot = Slot(lane, i);
            FillSharedPayload(registered_.subspan(slot * options_.bytes, options_.bytes), frame);
            requests_[i] = {
                .metadata = {.connection_id = frame.target + 1,
                             .operation_id = static_cast<std::uint64_t>(lane) * options_.messages + frame.sequence + 1,
                             .target = remotes_[frame.target],
                             .opcode = URMA_OPC_SEND_IMM,
                             .buffer_lease = slot + 1},
                .sges = std::span(&sges_[slot], 1),
                .imm_data = SharedImmediate(frame)};
        }
        auto sent = sender_.Send(std::span(requests_).first(count), std::span(ids_).subspan(Slot(lane, 0), count),
                                 lanes_[lane]);
        const auto accepted = sent ? std::optional<std::size_t>(*sent) : sent.error().accepted;
        if (accepted) {
            stats_.accepted += *accepted;
            for (std::size_t i = 0; i < *accepted; ++i) {
                const auto target = (static_cast<std::uint64_t>(base) + i + lane) % options_.jetties;
                ++stats_.tx_pairs[lane * kSharedMaxJetties + target];
            }
        } else {
            stats_.accepted_known = false;
        }
        if (!sent) {
            // 本工具测试正常双向数据面，不自动重放或把部分接受误报为完整成功。
            unsafe_ = true;
            return Error("post SEND_WITH_IMM (resources retained)", sent.error().provider_status
                                                                        ? sent.error().provider_status
                                                                        : static_cast<int>(sent.error().code));
        }
    }
    return {};
}

std::expected<std::size_t, ToolError> SharedQueueSession::PollTx(std::uint32_t base, std::uint32_t count) {
    auto batch = processor_.Poll(tx_events_);
    if (!batch) {
        unsafe_ = true;
        return Error("poll shared TX JFC", static_cast<int>(batch.error().code));
    }
    std::optional<ToolError> failure;
    for (std::size_t i = 0; i < batch->count; ++i) {
        const auto& event = tx_events_[i];
        if (event.record) {
            ++stats_.retired;
        }
        // 校验失败时记录第一条原始 CQE，正常路径不做格式化。
        const auto remember_failure = [&]() noexcept {
            if (!stats_.failed_tx) {
                stats_.failed_tx = event.completion;
            }
        };
        if (event.error || event.kind != raw::TxCompletionKind::kCompleted || !event.record ||
            event.completion.status != URMA_CR_SUCCESS) {
            if (!failure) {
                failure = ToolError{"invalid TX completion", event.completion.status == URMA_CR_SUCCESS
                                                                 ? EPROTO
                                                                 : static_cast<int>(event.completion.status)};
            }
            remember_failure();
            continue;
        }
        const auto& record = *event.record;
        if (!record.metadata.buffer_lease || record.metadata.buffer_lease > ids_.size()) {
            failure = ToolError{"TX buffer identity out of range", EPROTO};
            remember_failure();
            continue;
        }
        const auto slot = static_cast<std::size_t>(record.metadata.buffer_lease - 1);
        const auto lane = static_cast<std::uint32_t>(slot / options_.batch);
        const auto offset = static_cast<std::uint32_t>(slot % options_.batch);
        const auto sequence = static_cast<std::uint64_t>(base) + offset;
        const auto target = (sequence + lane) % options_.jetties;
        if (offset >= count || ids_[slot] != record.id || tx_seen_[slot] ||
            record.ticket.lane.index != lanes_[lane].index || record.metadata.target != remotes_[target] ||
            record.metadata.operation_id != static_cast<std::uint64_t>(lane) * options_.messages + sequence + 1 ||
            record.metadata.opcode != URMA_OPC_SEND_IMM) {
            failure = ToolError{"TX completion identity mismatch", EPROTO};
            remember_failure();
            continue;
        }
        tx_seen_[slot] = 1;
    }
    if (failure) {
        unsafe_ = true;
        return std::unexpected(*failure);
    }
    return batch->count;
}

std::expected<void, ToolError> SharedQueueSession::ValidateRx(const raw::RxBufferEvent& event, std::uint32_t base,
                                                              std::uint32_t count) {
    if (event.error || !event.lease || event.completion.status != URMA_CR_SUCCESS) {
        return Error("invalid RX completion",
                     event.completion.status ? static_cast<int>(event.completion.status) : EPROTO);
    }
    const auto& cr = event.completion;
    if (!cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
        event.payload.size() != options_.bytes) {
        return Error("RX opcode/direction/length mismatch", EPROTO);
    }
    auto frame = CheckSharedPayload(event.payload);
    if (!frame) {
        return std::unexpected(frame.error());
    }
    if (frame->server == local_.server || frame->source >= options_.jetties || frame->target >= options_.jetties ||
        frame->sequence < base || frame->sequence >= static_cast<std::uint64_t>(base) + count ||
        frame->target != (frame->sequence + frame->source) % options_.jetties) {
        return Error("RX frame route/sequence mismatch", EPROTO);
    }
    if (cr.local_id != local_.endpoints[frame->target].id ||
        !SameEndpoint(cr.remote_id, peer_.endpoints[frame->source])) {
        return Error("RX CQE source/target endpoint mismatch", EPROTO);
    }
    if (cr.imm_data != SharedImmediate(*frame)) {
        return Error("RX 64-bit IMM mismatch", EPROTO);
    }
    const auto slot = Slot(frame->source, static_cast<std::uint32_t>(frame->sequence - base));
    if (rx_seen_[slot]) {
        return Error("duplicate RX message", EPROTO);
    }
    rx_seen_[slot] = 1;
    ++stats_.received;
    ++stats_.rx_pairs[frame->source * kSharedMaxJetties + frame->target];
    stats_.imm_patterns |= 1u << ((frame->sequence + frame->source) % 8);
    return {};
}

std::expected<std::size_t, ToolError> SharedQueueSession::PollRx(std::uint32_t base, std::uint32_t count) {
    auto count_polled = rx_.Poll(rx_events_);
    if (!count_polled) {
        unsafe_ = true;
        return Error("poll shared RX JFC", count_polled.error().provider_error
                                               ? count_polled.error().provider_error
                                               : static_cast<int>(count_polled.error().code));
    }
    std::optional<ToolError> failure;
    for (std::size_t i = 0; i < *count_polled; ++i) {
        const auto& event = rx_events_[i];
        auto valid = ValidateRx(event, base, count);
        if (!valid && !failure) {
            failure = valid.error();
            if (!stats_.failed_rx) {
                stats_.failed_rx = event.completion;
                auto frame = CheckSharedPayload(event.payload);
                if (frame) {
                    stats_.failed_rx_frame = *frame;
                }
            }
        }
        // 即使某条 IMM/payload 错误，同批已交出的全部 lease 仍必须归还。
        if (event.lease) {
            auto released = rx_.Release(*event.lease);
            if (!released && !failure) {
                failure = ToolError{"release shared RX lease", static_cast<int>(released.error().code)};
            }
        }
    }
    if (failure) {
        return std::unexpected(*failure);
    }
    return *count_polled;
}

std::expected<void, ToolError> SharedQueueSession::Close() noexcept {
    if (unsafe_ || !peer_quiescent_ || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("DMA/peer state unresolved; retaining shared session", EBUSY);
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
    for (std::uint32_t i = options_.jetties; i > 0; --i) {
        auto*& remote = remotes_[i - 1];
        if (!remote) {
            continue;
        }
        const auto status = UrmaApi::UnimportJetty(remote);
        if (status != URMA_SUCCESS) {
            return Error("unimport shared peer", status);
        }
        remote = nullptr;
    }
    auto rx = rx_.Close();
    if (!rx) {
        return Error("close shared RX buffers",
                     rx.error().provider_error ? rx.error().provider_error : static_cast<int>(rx.error().code));
    }
    auto pool = pool_.Close();
    if (!pool) {
        return Error("close shared jetty pool",
                     pool.error().provider_error ? pool.error().provider_error : static_cast<int>(pool.error().code));
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
    sges_.clear();
    ids_.clear();
    tx_seen_.clear();
    rx_seen_.clear();
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
