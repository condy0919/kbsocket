// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/eager_tx.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <optional>

namespace kbsocket {
namespace connection {
namespace {
constexpr auto kEnd = std::numeric_limits<std::uint32_t>::max();
auto Error(EagerTxErrorCode code, int status = 0) noexcept {
    return std::unexpected(EagerTxError{code, status});
}
bool SameLane(const raw::JettyLane& a, const raw::JettyLane& b) noexcept {
    return a.pool == b.pool && a.index == b.index && a.epoch == b.epoch && a.generation == b.generation;
}
} // namespace

struct EagerTxStream::State {
    enum class Kind { kData, kFin, kCredit };
    struct Record {
        memory::BufferLease lease{};
        memory::BufferReference dma{};
        memory::BufferLease source{};
        memory::BufferReference source_dma{};
        std::size_t source_offset = 0;
        control::ImportLease endpoint{};
        raw::AttemptMetadata metadata{};
        raw::AttemptId id = 0;
        std::uint64_t generation = 0;
        std::uint64_t imm = 0;
        std::uint32_t next = kEnd;
        std::size_t bytes = 0, wire_bytes = 0;
        Kind kind = Kind::kData;
        bool active = false, cpu = false, committed = false, posted = false;
        bool source_committed = false;
        bool consumed_last_control = false;
    };
    EagerTxConfig config;
    raw::TxSender* sender = nullptr;
    memory::SlabAllocator* slab = nullptr;
    control::EndpointImportCache* imports = nullptr;
    SenderCreditAccount* credits = nullptr;
    control::ImportLease endpoint{};
    memory::BufferLease control_buffer{};
    std::unique_ptr<Record[]> records;
    TxSequenceWindow sequence;
    SequenceTicket ticket;
    std::array<std::uint32_t, raw::TxSender::kMaxBatch> batch{};
    std::array<TxFramePlan, raw::TxSender::kMaxBatch> plans{};
    std::array<std::array<urma_sge_t, 2>, raw::TxSender::kMaxBatch> sges{};
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests{};
    std::array<raw::AttemptId, raw::TxSender::kMaxBatch> ids{};
    WireFrameInput credit;
    EagerTxStats stats;
    std::uint64_t generation = 0, queued_bytes = 0, advertised_control = 0;
    std::uint32_t post_limit = raw::TxSender::kMaxBatch;
    std::uint32_t free = 0, head = kEnd, tail = kEnd;
    std::size_t header = 0;
    bool credit_pending = false, fin_queued = false, closed = false;

    std::expected<std::uint64_t, EagerTxError> PublishedControl() const noexcept {
        if (!config.advertised_credits) {
            return advertised_control;
        }
        const auto published = config.advertised_credits->PublishedControl(config.advertised_peer);
        if (!published) {
            return Error(EagerTxErrorCode::kCredit);
        }
        return std::max(advertised_control, *published);
    }
    std::uint64_t BufferedBytes() const noexcept {
        auto bytes = sequence.accepted_bytes();
        const auto add = [&bytes](std::uint64_t value) {
            bytes += std::min(value, std::numeric_limits<std::uint64_t>::max() - bytes);
        };
        add(queued_bytes);
        // 自己的 pending 已包含于 queued_bytes；外部 RTS 的未接受长度也占共同业务窗口。
        if (!sequence.OwnsReservation(ticket)) {
            for (const auto& frame : sequence.pending()) {
                add(frame.bytes);
            }
        }
        return bytes;
    }
    void Enqueue(std::uint32_t index) noexcept {
        if (tail != kEnd) {
            records[tail].next = index;
        } else {
            head = index;
        }
        tail = index;
        records[index].next = kEnd;
        ++stats.queued;
        queued_bytes += records[index].bytes;
    }
    void Pop() noexcept {
        queued_bytes -= records[head].bytes;
        head = records[head].next;
        if (head == kEnd) {
            tail = kEnd;
        }
        --stats.queued;
    }
};

EagerTxStream::EagerTxStream() noexcept = default;
EagerTxStream::~EagerTxStream() {
    if (!Close()) {
        // 丢弃对象不能替代 DMA 终结；保留槽位与独立 pin，宿主也必须保留借用依赖。
        static_cast<void>(state_.release());
    }
}
std::expected<void, EagerTxError> EagerTxStream::Open(const EagerTxConfig& config, raw::TxSender& sender,
                                                      memory::SlabAllocator& slab,
                                                      control::EndpointImportCache& imports,
                                                      control::ImportLease endpoint,
                                                      SenderCreditAccount& credits) noexcept {
    if (opened_once_) {
        return Error(EagerTxErrorCode::kInUse);
    }
    if (!sender.defer_drain() || !config.local_context || sender.context() != config.local_context ||
        !config.connection_id || !config.destination.generation || !config.capacity || config.capacity > 65536 ||
        config.session.peer_runtime != config.credit_identity.peer_runtime ||
        config.session.receive_domain != config.credit_identity.receive_domain || !slab.accepting()) {
        return Error(EagerTxErrorCode::kInvalidArgument);
    }
    if (config.advertised_credits) {
        if (config.advertised_peer.domain != config.advertised_credits ||
            !config.advertised_credits->PublishedControl(config.advertised_peer)) {
            return Error(EagerTxErrorCode::kWrongIdentity);
        }
    } else if (config.advertised_peer.domain || config.advertised_peer.id) {
        return Error(EagerTxErrorCode::kWrongIdentity);
    }
    if (!credits.ready() || credits.identity() != config.credit_identity) {
        return Error(EagerTxErrorCode::kWrongIdentity);
    }
    const auto key = imports.GetKey(endpoint);
    if (!key || key->local_context != config.local_context || key->peer_runtime != config.session.peer_runtime ||
        key->receive_domain != config.session.receive_domain) {
        return Error(EagerTxErrorCode::kWrongIdentity);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(EagerTxErrorCode::kNoMemory);
    }
    state->records.reset(new (std::nothrow) State::Record[config.capacity + 1]);
    if (!state->records) {
        return Error(EagerTxErrorCode::kNoMemory);
    }
    if (!state->sequence.Open(config.session) ||
        !state->sequence.Update(config.session, config.max_frame_seq, config.max_stream_bytes)) {
        return Error(EagerTxErrorCode::kSequence);
    }
    const auto header = config.destination.slot > 65535 || config.destination.generation > 255 ? 8 : 0;
    const auto control_buffer = slab.Acquire(48 + header);
    if (!control_buffer) {
        return Error(EagerTxErrorCode::kMemory);
    }
    const auto pin = imports.Retain(endpoint);
    if (!pin) {
        const auto released = slab.ReleaseCpu(*control_buffer);
        static_cast<void>(released);
        return Error(EagerTxErrorCode::kImport);
    }
    state->control_buffer = *control_buffer;
    state->config = config;
    state->advertised_control = config.initial_advertised_control;
    state->sender = &sender;
    state->slab = &slab;
    state->imports = &imports;
    state->credits = &credits;
    state->endpoint = *pin;
    state->header = config.destination.slot > 65535 || config.destination.generation > 255 ? 8 : 0;
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->records[i].next = i + 1 == config.capacity ? kEnd : i + 1;
    }
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<std::size_t, EagerTxError> EagerTxStream::Write(std::span<const std::byte> bytes) noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.stats.failed || s.stats.write_shutdown) {
        return Error(EagerTxErrorCode::kStopped);
    }
    std::size_t copied = 0;
    while (copied < bytes.size() && s.free != kEnd) {
        if (s.generation == std::numeric_limits<std::uint64_t>::max()) {
            break;
        }
        // 不能把小字节窗口装成一个永远无法 Reserve 的大帧；超出当前授权的尾部留给调用方。
        const auto buffered = s.BufferedBytes();
        if (buffered >= s.config.max_stream_bytes) {
            break;
        }
        const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(
            std::min(bytes.size() - copied, kWireMaxPayload - s.header), s.config.max_stream_bytes - buffered));
        const auto lease = s.slab->Acquire(length + s.header);
        if (!lease) {
            if (copied) {
                break;
            }
            return Error(lease.error().code == memory::MemoryErrorCode::kExhausted ? EagerTxErrorCode::kWouldBlock
                                                                                   : EagerTxErrorCode::kMemory);
        }
        const auto view = s.slab->Writable(*lease);
        if (!view) {
            const auto released = s.slab->ReleaseCpu(*lease);
            static_cast<void>(released);
            Fail();
            return copied ? std::expected<std::size_t, EagerTxError>(copied) : Error(EagerTxErrorCode::kMemory);
        }
        // 成功返回前脱离用户地址；SN 只在 Progress 预留，排队阶段不消耗远端 credits。
        std::memmove(view->data() + s.header, bytes.data() + copied, length);
        const auto index = s.free;
        auto& record = s.records[index];
        s.free = record.next;
        record = {.lease = *lease,
                  .generation = ++s.generation,
                  .bytes = length,
                  .wire_bytes = length + s.header,
                  .active = true,
                  .cpu = true};
        s.Enqueue(index);
        copied += length;
        s.stats.copied_bytes += length;
    }
    if (!copied && !bytes.empty()) {
        return Error(EagerTxErrorCode::kWouldBlock);
    }
    return copied;
}
std::expected<std::size_t, EagerTxError> EagerTxStream::WriteLease(memory::BufferLease source, std::size_t offset,
                                                                   std::size_t bytes) noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.stats.failed || s.stats.write_shutdown) {
        return Error(EagerTxErrorCode::kStopped);
    }
    if (source.allocator != s.slab || !bytes || !s.sender->pool() ||
        s.sender->pool()->max_send_sge() < (s.header ? 2u : 1u)) {
        return Error(EagerTxErrorCode::kInvalidArgument);
    }
    const auto source_view = s.slab->Readable(source);
    if (!source_view || offset > source_view->size() || bytes > source_view->size() - offset) {
        return Error(EagerTxErrorCode::kInvalidArgument);
    }
    std::size_t queued = 0;
    while (queued < bytes && s.free != kEnd) {
        if (s.generation == std::numeric_limits<std::uint64_t>::max()) {
            break;
        }
        // 给已排队复制帧/控制帧留一个临时责任，避免源 pin 占满引用表后阻塞队首。
        if (s.slab->stats().available_references <= (s.header ? 2u : 1u)) {
            break;
        }
        const auto buffered = s.BufferedBytes();
        if (buffered >= s.config.max_stream_bytes) {
            break;
        }
        const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(
            std::min(bytes - queued, kWireMaxPayload - s.header), s.config.max_stream_bytes - buffered));
        const auto index = s.free;
        auto& r = s.records[index];
        const auto next = r.next;
        // 先占有记录再取得责任；罕见清理失败也有稳定槽记录所有权，不把责任藏在栈变量中。
        s.free = next;
        r = {.source = source,
             .source_offset = offset + queued,
             .generation = ++s.generation,
             .bytes = length,
             .wire_bytes = length + s.header,
             .active = true};
        auto discard = [&](EagerTxErrorCode code) noexcept -> std::expected<std::size_t, EagerTxError> {
            if (!Drop(index)) {
                Fail();
                return queued ? std::expected<std::size_t, EagerTxError>(queued) : Error(EagerTxErrorCode::kRetained);
            }
            return queued ? std::expected<std::size_t, EagerTxError>(queued) : Error(code);
        };
        const auto source_dma = s.slab->Reserve(source, memory::BufferUse::kLocalDma);
        if (!source_dma) {
            return discard(source_dma.error().code == memory::MemoryErrorCode::kExhausted
                               ? EagerTxErrorCode::kWouldBlock
                               : EagerTxErrorCode::kMemory);
        }
        r.source_dma = *source_dma;
        // 在发布入队前核对实际 context 的注册和范围；失败撤销未暴露责任，不接受任何新字节。
        if (!s.slab->Sge(r.source_dma, s.config.local_context, r.source_offset, length)) {
            return discard(EagerTxErrorCode::kInvalidArgument);
        }
        if (s.header) {
            const auto header = s.slab->Acquire(s.header);
            if (!header) {
                return discard(header.error().code == memory::MemoryErrorCode::kExhausted
                                   ? EagerTxErrorCode::kWouldBlock
                                   : EagerTxErrorCode::kMemory);
            }
            r.lease = *header;
            r.cpu = true;
        }
        std::span<std::byte> header_view;
        if (r.lease.generation) {
            const auto writable = s.slab->Writable(r.lease);
            if (!writable) {
                return discard(EagerTxErrorCode::kMemory);
            }
            header_view = *writable;
        }
        WireFrameInput input;
        input.token = s.config.destination;
        // 扩展头只有不可变目标身份；SN 位于 IMM，因此可在准入时编码并独立持有头部责任。
        input.data = source_view->subspan(r.source_offset, r.bytes);
        const auto encoded = EncodeWireHeader(input, header_view);
        if (!encoded) {
            return discard(EagerTxErrorCode::kInvalidArgument);
        }
        r.imm = encoded->imm_data;
        if (r.lease.generation) {
            const auto header_dma = s.slab->Reserve(r.lease, memory::BufferUse::kLocalDma);
            if (!header_dma) {
                return discard(header_dma.error().code == memory::MemoryErrorCode::kExhausted
                                   ? EagerTxErrorCode::kWouldBlock
                                   : EagerTxErrorCode::kMemory);
            }
            r.dma = *header_dma;
        }
        s.Enqueue(index);
        queued += length;
        s.stats.leased_bytes += length;
    }
    return queued ? std::expected<std::size_t, EagerTxError>(queued) : Error(EagerTxErrorCode::kWouldBlock);
}
bool EagerTxStream::writable() const noexcept {
    if (!state_ || state_->closed || state_->stats.failed || state_->stats.write_shutdown) {
        return false;
    }
    const auto& s = *state_;
    return s.free != kEnd && s.generation != std::numeric_limits<std::uint64_t>::max() && s.slab->accepting() &&
           s.slab->stats().available && s.BufferedBytes() < s.config.max_stream_bytes;
}
TxSequenceWindow* EagerTxStream::sequence_window() noexcept {
    return state_ && !state_->closed ? &state_->sequence : nullptr;
}
bool EagerTxStream::can_start_rndv() const noexcept {
    return state_ && !state_->closed && !state_->stats.failed && !state_->stats.write_shutdown &&
           !state_->sequence.stopped() && state_->stats.queued == 0 && state_->sequence.pending().empty();
}
std::expected<void, EagerTxError> EagerTxStream::ShutdownWrite() noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    if (state_->stats.failed) {
        return Error(EagerTxErrorCode::kStopped);
    }
    state_->stats.write_shutdown = true;
    return {};
}
std::expected<void, EagerTxError> EagerTxStream::QueueCredit(const WireFrameInput& credit) noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.stats.failed) {
        return Error(EagerTxErrorCode::kStopped);
    }
    auto frame = credit;
    frame.token = s.config.destination;
    std::array<std::byte, 64> validation{};
    if (frame.type != WireFrameType::kControl || frame.control != WireControlType::kCreditUpdate ||
        frame.session != s.config.session.session || !EncodeWireFrame(frame, validation)) {
        return Error(EagerTxErrorCode::kInvalidArgument);
    }
    if (s.credit_pending) {
        if (frame.credit_epoch != s.credit.credit_epoch) {
            return Error(EagerTxErrorCode::kWrongIdentity);
        }
        frame.max_credit_seq = std::max(frame.max_credit_seq, s.credit.max_credit_seq);
        frame.max_control_credit_seq = std::max(frame.max_control_credit_seq, s.credit.max_control_credit_seq);
        frame.max_frame_seq = std::max(frame.max_frame_seq, s.credit.max_frame_seq);
        frame.max_stream_bytes = std::max(frame.max_stream_bytes, s.credit.max_stream_bytes);
    }
    s.credit = frame;
    s.credit_pending = true;
    return {};
}
std::expected<void, EagerTxError> EagerTxStream::ApplyCredit(SequenceSession session, CreditIdentity identity,
                                                             std::uint64_t max_data, std::uint64_t max_control,
                                                             std::uint64_t max_frames,
                                                             std::uint64_t max_bytes) noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.stats.failed) {
        return Error(EagerTxErrorCode::kStopped);
    }
    if (session != s.config.session || identity != s.config.credit_identity ||
        max_frames > TxSequenceWindow::kSequenceEnd) {
        return Error(EagerTxErrorCode::kWrongIdentity);
    }
    if (!s.sequence.Update(session, max_frames, max_bytes)) {
        return Error(EagerTxErrorCode::kSequence);
    }
    if (!s.credits->Update(identity, max_data, max_control)) {
        return Error(EagerTxErrorCode::kCredit);
    }
    s.config.max_frame_seq = std::max(max_frames, s.config.max_frame_seq);
    s.config.max_stream_bytes = std::max(max_bytes, s.config.max_stream_bytes);
    return {};
}
std::expected<void, EagerTxError> EagerTxStream::Drop(std::uint32_t index) noexcept {
    auto& s = *state_;
    auto& r = s.records[index];
    if (r.source_dma.sequence) {
        if (r.source_committed || !s.slab->Cancel(r.source_dma)) {
            return Error(EagerTxErrorCode::kRetained);
        }
        r.source_dma = {};
    }
    if (r.dma.sequence) {
        if (r.committed || !s.slab->Cancel(r.dma)) {
            return Error(EagerTxErrorCode::kRetained);
        }
        r.dma = {};
    }
    if (r.cpu) {
        if (!s.slab->ReleaseCpu(r.lease)) {
            return Error(EagerTxErrorCode::kRetained);
        }
        r.cpu = false;
    }
    if (r.endpoint.sequence) {
        if (!s.imports->Release(r.endpoint)) {
            return Error(EagerTxErrorCode::kRetained);
        }
        r.endpoint = {};
    }
    r = {};
    if (index < s.config.capacity) {
        r.next = s.free;
        s.free = index;
    }
    return {};
}
std::expected<std::size_t, EagerTxError> EagerTxStream::Progress() noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.stats.failed) {
        return Error(EagerTxErrorCode::kStopped);
    }
    if (!s.credits->ready() || s.credits->identity() != s.config.credit_identity) {
        // 共账撤回后允许以新 epoch 重开；旧流不能借新额度继续向旧会话投递。
        Fail();
        return Error(EagerTxErrorCode::kWrongIdentity);
    }
    if (s.credits->stats().stopped) {
        Fail();
        return Error(EagerTxErrorCode::kCredit);
    }
    // 控制独占一个额外记录，数据窗口耗尽或 FIN 已接受时仍可返还接收额度。
    const auto control_available = s.credits->available(CreditClass::kControl);
    const auto published = s.PublishedControl();
    if (!published) {
        return std::unexpected(published.error());
    }
    const bool advances_control = s.credit.max_control_credit_seq > *published;
    if (s.credit_pending && !s.records[s.config.capacity].active &&
        (control_available > 1 || (control_available == 1 && advances_control))) {
        return Post(true);
    }
    if (s.head == kEnd && s.stats.write_shutdown && !s.fin_queued && !s.stats.fin_accepted) {
        if (s.free == kEnd) {
            return Error(EagerTxErrorCode::kWouldBlock);
        }
        if (s.generation == std::numeric_limits<std::uint64_t>::max()) {
            return Error(EagerTxErrorCode::kStopped);
        }
        const auto lease = s.slab->Acquire(8 + s.header);
        if (!lease) {
            return Error(EagerTxErrorCode::kWouldBlock);
        }
        const auto index = s.free;
        auto& r = s.records[index];
        s.free = r.next;
        r = {.lease = *lease,
             .generation = ++s.generation,
             .wire_bytes = 8 + s.header,
             .kind = State::Kind::kFin,
             .active = true,
             .cpu = true};
        s.Enqueue(index);
        s.fin_queued = true;
    }
    if (s.head == kEnd) {
        return std::size_t{0};
    }
    return Post(false);
}
std::expected<std::size_t, EagerTxError> EagerTxStream::Post(bool control) noexcept {
    auto& s = *state_;
    if (!s.credits->ready() || s.credits->identity() != s.config.credit_identity) {
        Fail();
        return Error(EagerTxErrorCode::kWrongIdentity);
    }
    if (control && s.generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(EagerTxErrorCode::kStopped);
    }
    const auto target = s.imports->Get(s.endpoint);
    if (!target) {
        return Error(target.error().code == control::ImportCacheErrorCode::kPending ? EagerTxErrorCode::kWouldBlock
                                                                                    : EagerTxErrorCode::kImport);
    }
    const auto published = s.PublishedControl();
    if (!published) {
        return std::unexpected(published.error());
    }
    const bool fin = !control && s.records[s.head].kind == State::Kind::kFin;
    const auto kind = control || fin ? CreditClass::kControl : CreditClass::kData;
    auto count = static_cast<std::size_t>(std::min<std::uint64_t>(s.credits->available(kind), s.post_limit));
    // 最后一个控制额度只用于推进对端 control 授权，普通窗口更新或 FIN 不能把双方授信通道耗尽。
    if (!count || (fin && s.credits->available(kind) <= 1) ||
        (control && s.credits->available(kind) == 1 && s.credit.max_control_credit_seq <= *published)) {
        return Error(EagerTxErrorCode::kWouldBlock);
    }
    if (control) {
        count = 1;
        s.batch[0] = s.config.capacity;
        s.records[s.config.capacity] = {.lease = s.control_buffer,
                                        .generation = ++s.generation,
                                        .wire_bytes = 48 + s.header,
                                        .kind = State::Kind::kCredit,
                                        .active = true};
    } else {
        if (!s.sequence.pending().empty() && !s.sequence.OwnsReservation(s.ticket)) {
            return Error(EagerTxErrorCode::kWouldBlock);
        }
        std::size_t n = 0;
        auto index = s.head;
        if (s.sequence.pending().empty()) {
            auto seq = s.sequence.next_sequence();
            auto bytes = s.sequence.accepted_bytes();
            while (n < count && index != kEnd) {
                const auto& r = s.records[index];
                if (seq >= s.config.max_frame_seq || r.bytes > s.config.max_stream_bytes - bytes ||
                    (r.kind == State::Kind::kFin) != fin) {
                    break;
                }
                s.plans[n++] = {r.bytes, fin};
                bytes += r.bytes;
                ++seq;
                index = r.next;
            }
            if (!n) {
                return Error(EagerTxErrorCode::kWouldBlock);
            }
            const auto ticket = s.sequence.Reserve(std::span(s.plans).first(n));
            if (!ticket) {
                return Error(EagerTxErrorCode::kSequence);
            }
            s.ticket = *ticket;
        }
        count = std::min(count, s.sequence.pending().size());
        index = s.head;
        for (std::size_t i = 0; i < count; ++i) {
            s.batch[i] = index;
            index = s.records[index].next;
        }
    }
    const bool consumes_last_control = control && s.credits->available(kind) == 1;
    const auto reservation = s.credits->Reserve(kind, static_cast<std::uint32_t>(count));
    if (!reservation) {
        if (control) {
            if (!Drop(s.batch[0])) {
                Fail();
                return Error(EagerTxErrorCode::kRetained);
            }
        }
        return Error(EagerTxErrorCode::kWouldBlock);
    }
    bool prepared = true;
    bool preparation_exhausted = false;
    for (std::size_t i = 0; i < count; ++i) {
        auto& r = s.records[s.batch[i]];
        if (r.source.generation) {
            // 源及不可变扩展头从准入开始持有独立责任；不重新取得 CPU 视图或改写头部。
            r.imm = (r.imm & 0xffffffff00000000ull) | s.sequence.pending()[i].seq;
        } else {
            const auto writable = s.slab->Writable(r.lease);
            if (!writable) {
                prepared = false;
                break;
            }
            auto view = *writable;
            WireFrameInput input;
            if (control) {
                input = s.credit;
            } else {
                input.token = s.config.destination;
                input.seq = s.sequence.pending()[i].seq;
                if (fin) {
                    input.type = WireFrameType::kControl;
                    input.control = WireControlType::kFin;
                    input.session = s.config.session.session;
                } else {
                    input.data = view.subspan(s.header, r.bytes);
                }
            }
            const auto encoded = EncodeWireHeader(input, view.first(control || fin ? r.wire_bytes : s.header));
            if (!encoded) {
                prepared = false;
                break;
            }
            r.imm = encoded->imm_data;
        }
        const auto pin = s.imports->Retain(s.endpoint);
        if (!pin) {
            prepared = false;
            preparation_exhausted = pin.error().code == control::ImportCacheErrorCode::kResourceExhausted;
            break;
        }
        r.endpoint = *pin;
        std::size_t sge_count = 0;
        if (r.lease.generation) {
            if (!r.source.generation) {
                const auto dma = s.slab->Reserve(r.lease, memory::BufferUse::kLocalDma);
                if (!dma) {
                    prepared = false;
                    preparation_exhausted = dma.error().code == memory::MemoryErrorCode::kExhausted;
                    break;
                }
                r.dma = *dma;
            }
            const auto sge =
                s.slab->Sge(r.dma, s.config.local_context, 0, r.source.generation ? s.header : r.wire_bytes);
            if (!sge) {
                prepared = false;
                break;
            }
            s.sges[i][sge_count++] = *sge;
        }
        if (r.source.generation) {
            // 从入队到 post 始终保留源 Reserved 责任；应用可已归还 CPU，不能重新申请 CPU 视图。
            const auto source_sge = s.slab->Sge(r.source_dma, s.config.local_context, r.source_offset, r.bytes);
            if (!source_sge) {
                prepared = false;
                break;
            }
            s.sges[i][sge_count++] = *source_sge;
        }
        r.metadata = {.connection_id = s.config.connection_id,
                      .operation_id = s.batch[i] + 1,
                      .operation_generation = r.generation,
                      .target = *target,
                      .opcode = URMA_OPC_SEND_IMM,
                      .buffer_lease = r.source.generation ? r.source.generation : r.lease.generation};
        s.requests[i] = {
            .metadata = r.metadata, .sges = std::span(s.sges[i]).first(sge_count), .imm_data = r.imm, .remote = {}};
    }
    auto cancel_prepared = [&]() noexcept {
        bool valid = true;
        for (std::size_t i = 0; i < count; ++i) {
            auto& r = s.records[s.batch[i]];
            if (r.dma.sequence && !r.source.generation) {
                if (s.slab->Cancel(r.dma)) {
                    r.dma = {};
                } else {
                    valid = false;
                }
            }
            if (r.endpoint.sequence) {
                if (s.imports->Release(r.endpoint)) {
                    r.endpoint = {};
                } else {
                    valid = false;
                }
            }
        }
        return valid;
    };
    if (!prepared) {
        const bool cancelled = cancel_prepared();
        const auto resolved = s.credits->Resolve(*reservation, 0);
        const bool dropped = !control || Drop(s.batch[0]).has_value();
        if (!cancelled || !resolved || !dropped) {
            Fail();
            return Error(EagerTxErrorCode::kAccounting);
        }
        // 全批准备尚未暴露 DMA；引用表不足也须缩批，否则原大批会无限重复预留/撤销。
        if (preparation_exhausted && !control && count > 1) {
            s.post_limit = static_cast<std::uint32_t>(count / 2);
        }
        return Error(EagerTxErrorCode::kWouldBlock);
    }
    std::fill_n(s.ids.begin(), count, raw::AttemptId{0});
    const auto sent = s.sender->Send(std::span(s.requests).first(count), std::span(s.ids).first(count));
    const auto accepted = sent ? std::optional<std::size_t>(*sent) : sent.error().accepted;
    bool uncertain =
        !accepted || *accepted > count || (!sent && sent.error().code == raw::TxSendErrorCode::kAccountingFailure);
    // 三份账都按同一可信前缀结算；账本自身失败时不回收任何可能暴露的引用。
    const auto credits_result = s.credits->Resolve(
        *reservation, uncertain ? std::nullopt : std::optional<std::uint32_t>(static_cast<std::uint32_t>(*accepted)));
    const auto sequence_result = control ? std::expected<void, SequenceError>{}
                                         : s.sequence.Resolve(s.ticket, uncertain ? std::nullopt : accepted);
    uncertain = uncertain || !credits_result || !sequence_result;
    const auto retained = uncertain ? count : *accepted;
    bool accounting_ok = true;
    for (std::size_t i = 0; i < count; ++i) {
        auto& r = s.records[s.batch[i]];
        if (i < retained) {
            r.id = s.ids[i];
            r.posted = true;
            r.consumed_last_control = consumes_last_control && accepted && i < *accepted;
            if (r.dma.sequence) {
                if (s.slab->Commit(r.dma)) {
                    r.committed = true;
                } else {
                    accounting_ok = false;
                }
            }
            if (r.source_dma.sequence) {
                if (s.slab->Commit(r.source_dma)) {
                    r.source_committed = true;
                } else {
                    accounting_ok = false;
                }
            }
            if (uncertain || !accounting_ok) {
                if (r.lease.generation) {
                    accounting_ok = s.slab->Quarantine(r.lease).has_value() && accounting_ok;
                }
                if (r.source.generation) {
                    accounting_ok = s.slab->Quarantine(r.source).has_value() && accounting_ok;
                }
            }
            if (r.cpu) {
                if (s.slab->ReleaseCpu(r.lease)) {
                    r.cpu = false;
                } else {
                    accounting_ok = false;
                }
            }
            ++s.stats.in_flight;
            if (!control) {
                s.Pop();
            }
        } else {
            if (r.dma.sequence && !r.source.generation) {
                if (s.slab->Cancel(r.dma)) {
                    r.dma = {};
                } else {
                    accounting_ok = false;
                }
            }
            // 源与扩展头责任仍覆盖未提交后缀，不能随本次 post 的临时预留一起撤销。
            if (s.imports->Release(r.endpoint)) {
                r.endpoint = {};
            } else {
                accounting_ok = false;
            }
        }
    }
    if (accepted && *accepted <= count) {
        s.stats.accepted += *accepted;
        if (control && *accepted) {
            s.advertised_control = std::max(s.advertised_control, s.credit.max_control_credit_seq);
            if (s.config.advertised_credits && !s.config.advertised_credits->RecordPublishedControl(
                                                   s.config.advertised_peer, s.credit.max_control_credit_seq)) {
                accounting_ok = false;
            }
        }
    }
    if (control) {
        if (retained) {
            s.credit_pending = false;
        } else if (!Drop(s.batch[0])) {
            accounting_ok = false;
        }
    }
    s.stats.fin_accepted = s.sequence.fin_accepted();
    if (uncertain || !accounting_ok) {
        // 账务异常也隔离此前已经可信接受的同批槽，避免错误路径复用。
        for (std::size_t i = 0; i < count; ++i) {
            auto& r = s.records[s.batch[i]];
            if (r.active && r.posted) {
                if (r.lease.generation) {
                    const auto quarantined = s.slab->Quarantine(r.lease);
                    static_cast<void>(quarantined);
                }
                if (r.source.generation) {
                    const auto quarantined = s.slab->Quarantine(r.source);
                    static_cast<void>(quarantined);
                }
            }
        }
        Fail();
        return Error(uncertain ? EagerTxErrorCode::kAcceptanceUnknown : EagerTxErrorCode::kAccounting,
                     sent ? 0 : sent.error().provider_status);
    }
    if (!sent && !*accepted) {
        if (sent.error().code == raw::TxSendErrorCode::kExhausted ||
            sent.error().code == raw::TxSendErrorCode::kPostFailed ||
            sent.error().code == raw::TxSendErrorCode::kPoolFailure ||
            sent.error().code == raw::TxSendErrorCode::kLedgerFailure) {
            // raw 层按整批预留。可信零接受时缩小后续尝试，避免槽位容量大于 SQ 深度永久停顿。
            // sequence.pending() 仍保存完整原后缀；缩批只改变本次提交数量。
            if (!control && count > 1) {
                s.post_limit = static_cast<std::uint32_t>(count / 2);
            }
            return Error(EagerTxErrorCode::kWouldBlock, sent.error().provider_status);
        }
        return Error(EagerTxErrorCode::kPost, sent.error().provider_status);
    }
    return *accepted;
}
std::expected<void, EagerTxError> EagerTxStream::OnCompletion(const raw::TxCompletionEvent& event) noexcept {
    if (!state_ || state_->closed) {
        return Error(EagerTxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (event.kind != raw::TxCompletionKind::kCompleted || !event.record || !event.lane ||
        event.record->id != event.completion.user_ctx || !SameLane(event.record->ticket.lane, *event.lane) ||
        event.record->metadata.connection_id != s.config.connection_id || !event.record->metadata.operation_id ||
        event.record->metadata.operation_id > static_cast<std::uint64_t>(s.config.capacity) + 1) {
        return Error(EagerTxErrorCode::kInvalidCompletion);
    }
    const auto index = static_cast<std::uint32_t>(event.record->metadata.operation_id - 1);
    auto& r = s.records[index];
    const auto& metadata = event.record->metadata;
    if (!r.active || !r.posted || !r.id || r.id != event.record->id || metadata.operation_generation != r.generation ||
        metadata.target != r.metadata.target || metadata.opcode != URMA_OPC_SEND_IMM ||
        metadata.buffer_lease != (r.source.generation ? r.source.generation : r.lease.generation) ||
        metadata.grant_lease || !metadata.signaled || (r.lease.generation && !r.committed) ||
        (r.source.generation && !r.source_committed)) {
        return Error(EagerTxErrorCode::kInvalidCompletion);
    }
    const bool terminal_failed = event.completion.status != URMA_CR_SUCCESS || event.error.has_value();
    bool peer_frozen = true;
    if (terminal_failed && r.consumed_last_control) {
        // 最后控制额度的返还报文失败后，不再能保证共账继续前进；禁止其他同 peer 流静默等待。
        // 不返还已消耗额度、不重放；SQ 故障范围仍由完成处理器决定。
        // 旧 WR 的迟到终结只影响其原 epoch，不能停用同一对象上已经重开的新共账。
        if (s.credits->identity() == s.config.credit_identity) {
            s.credits->Stop();
        }
        if (s.config.advertised_credits) {
            peer_frozen = s.config.advertised_credits->FreezePeer(s.config.advertised_peer).has_value();
        }
    }
    if (r.source_dma.sequence) {
        if (!s.slab->CompleteDma(r.source_dma)) {
            Fail();
            return Error(EagerTxErrorCode::kAccounting);
        }
        r.source_dma = {};
        r.source_committed = false;
    }
    if (r.dma.sequence) {
        if (!s.slab->CompleteDma(r.dma)) {
            Fail();
            return Error(EagerTxErrorCode::kAccounting);
        }
        r.dma = {};
        r.committed = false;
    }
    r.posted = false;
    --s.stats.in_flight;
    ++s.stats.retired;
    if (!Drop(index)) {
        Fail();
        return Error(EagerTxErrorCode::kRetained);
    }
    if (!peer_frozen) {
        Fail();
        return Error(EagerTxErrorCode::kAccounting);
    }
    if (terminal_failed) {
        // 普通 DATA status 8/10 只使此流失败；成功 CQE 从不返还远端 credits。
        Fail();
        return Error(EagerTxErrorCode::kCompletion, event.completion.status);
    }
    return {};
}
void EagerTxStream::Fail() noexcept {
    if (!state_ || state_->closed) {
        return;
    }
    auto& s = *state_;
    s.stats.failed = true;
    s.sequence.Fail();
    s.credit_pending = false;
    while (s.head != kEnd) {
        const auto index = s.head;
        s.Pop();
        const auto dropped = Drop(index);
        static_cast<void>(dropped);
    }
    auto& control = s.records[s.config.capacity];
    if (control.active && !control.posted) {
        const auto dropped = Drop(s.config.capacity);
        static_cast<void>(dropped);
    }
}
std::expected<void, EagerTxError> EagerTxStream::Close() noexcept {
    if (!state_ || state_->closed) {
        return {};
    }
    Fail();
    auto& s = *state_;
    for (std::uint32_t i = 0; i <= s.config.capacity; ++i) {
        if (s.records[i].active) {
            return Error(EagerTxErrorCode::kRetained);
        }
    }
    if (s.control_buffer.generation) {
        if (!s.slab->ReleaseCpu(s.control_buffer)) {
            return Error(EagerTxErrorCode::kRetained);
        }
        s.control_buffer = {};
    }
    if (!s.imports->Release(s.endpoint)) {
        return Error(EagerTxErrorCode::kRetained);
    }
    s.endpoint = {};
    s.closed = true;
    return {};
}
EagerTxStats EagerTxStream::stats() const noexcept {
    return state_ ? state_->stats : EagerTxStats{};
}
} // namespace connection
} // namespace kbsocket
