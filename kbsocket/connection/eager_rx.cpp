// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/eager_rx.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <optional>

#include "kbsocket/transport/raw/receive_domain_closer.hpp"

namespace kbsocket {
namespace connection {
namespace {
auto Error(EagerRxErrorCode code) noexcept {
    return std::unexpected(EagerRxError{code});
}
constexpr auto kEnd = std::numeric_limits<std::uint32_t>::max();
} // namespace

struct EagerReceiveDomain::State {
    enum class Phase : std::uint8_t { kFree, kHeld, kLeaseReleased, kQueued };
    struct Entry {
        raw::RxBufferLease lease{};
        RxCreditReceipt receipt{};
        WireFrameView decoded{};
        std::uint64_t id = 0;
        std::uint32_t next = kEnd;
        Phase phase = Phase::kFree;
    };
    raw::RxBufferPool* buffers = nullptr;
    ReceiveCreditDomain* credits = nullptr;
    ConnectionTable* routes = nullptr;
    std::unique_ptr<Entry[]> entries;
    std::unique_ptr<std::uint32_t[]> queue;
    std::unique_ptr<std::uint64_t[]> seen;
    std::size_t capacity = 0, held = 0, queued = 0, queue_head = 0;
    std::uint64_t buffer_incarnation = 0, credit_incarnation = 0;
    std::uint32_t free = kEnd;
    bool frozen = false, dispatching = false;
};

EagerReceiveDomain::EagerReceiveDomain() noexcept = default;
EagerReceiveDomain::~EagerReceiveDomain() {
    if (!Close()) {
        // 未结清的 Lease/receipt 与独占 Credit 绑定必须保留；宿主仍须保留全部借用依赖。
        (void)state_.release();
    }
}

std::expected<void, EagerRxError> EagerReceiveDomain::Open(raw::RxBufferPool& buffers, ReceiveCreditDomain& credits,
                                                           ConnectionTable& routes) noexcept {
    if (state_) {
        return Error(EagerRxErrorCode::kInUse);
    }
    const auto stats = credits.stats();
    const auto capacity = stats.data.free + stats.data.granted + stats.data.unposted + stats.control.free +
                          stats.control.granted + stats.control.unposted;
    // 初始免费槽必须先全部对应真实 Posted，避免 spare buffer 被当作已经归还的物理信用。
    if (!buffers.pool() || buffers.stopped() || buffers.held() || !capacity || stats.frozen || stats.held ||
        stats.released || stats.unattributed || stats.physical_posted != capacity || buffers.posted() != capacity ||
        buffers.pool()->rx_depth() != capacity || !routes.domain().runtime_instance ||
        !routes.domain().receive_domain || credits.receive_domain() != routes.domain().receive_domain) {
        return Error(EagerRxErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(EagerRxErrorCode::kNoMemory);
    }
    state->capacity = buffers.capacity();
    state->entries.reset(new (std::nothrow) State::Entry[state->capacity]);
    state->queue.reset(new (std::nothrow) std::uint32_t[state->capacity]);
    state->seen.reset(new (std::nothrow) std::uint64_t[state->capacity]{});
    if (!state->entries || !state->queue || !state->seen) {
        return Error(EagerRxErrorCode::kNoMemory);
    }
    for (std::uint32_t i = 0; i < state->capacity; ++i) {
        state->entries[i].next = i + 1 < state->capacity ? i + 1 : kEnd;
    }
    state->free = 0;
    state->buffers = &buffers;
    state->credits = &credits;
    state->routes = &routes;
    state->buffer_incarnation = buffers.incarnation();
    state->credit_incarnation = credits.receive_domain();
    if (!credits.BindReceive(*this)) {
        return Error(EagerRxErrorCode::kInUse);
    }
    state_ = std::move(state);
    return {};
}

std::expected<void, EagerRxError> EagerReceiveDomain::Dispatch(const raw::RxBufferEvent& event,
                                                               const EagerReceiveSource& source, EagerRxHandler handler,
                                                               std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    // 无真实 Lease 的异常 CQE 无法证明又消耗一份 RQE；只冻结，不重复虚构物理扣账。
    if (s.dispatching || !event.lease || event.lease->pool != s.buffers || !event.lease->id) {
        Freeze();
        return Error(EagerRxErrorCode::kInvalidPacket);
    }
    const auto residue = event.lease->id % s.capacity;
    if (s.seen[residue] >= event.lease->id || s.free == kEnd ||
        next_packet_ == std::numeric_limits<std::uint64_t>::max()) {
        Freeze();
        return Error(EagerRxErrorCode::kInvalidPacket);
    }
    s.seen[residue] = event.lease->id;
    const auto index = s.free;
    auto& entry = s.entries[index];
    s.free = entry.next;
    entry = {.lease = *event.lease, .id = ++next_packet_, .phase = State::Phase::kHeld};
    ++s.held;
    const EagerRxPacket packet{this, index, entry.id};
    auto decoded = DecodeWireFrame(event.completion.imm_data, event.payload);
    auto peer = s.credits->PeerStats(source.peer);
    const bool source_valid = peer && source.local_domain.runtime_instance == s.routes->domain().runtime_instance &&
                              source.local_domain.receive_domain == s.routes->domain().receive_domain &&
                              source.peer_context.receive_domain == source.local_domain.receive_domain &&
                              source.peer_context.peer_runtime == peer->identity.peer_runtime &&
                              source.peer_context.receive_domain == peer->identity.receive_domain &&
                              source.peer_context.session != 0;
    const bool frame_valid = !event.error && event.completion.status == URMA_CR_SUCCESS &&
                             event.completion.opcode == URMA_CR_OPC_SEND_WITH_IMM &&
                             event.completion.completion_len == event.payload.size() && decoded.has_value();
    std::expected<RxCreditReceipt, CreditError> receipt = std::unexpected(CreditError{CreditErrorCode::kWrongIdentity});
    if (source_valid && frame_valid) {
        const auto kind = decoded->frame.type == WireFrameType::kControl ? CreditClass::kControl : CreditClass::kData;
        receipt = s.credits->Consume(source.peer, kind);
    }
    if (!receipt) {
        // 真实 CQE 已经消费 RQE；无法归属时不能只丢包或猜扣当前连接的授信。
        receipt = s.credits->ConsumeUnknown();
        Freeze();
        if (!receipt) {
            return Error(EagerRxErrorCode::kRetained);
        }
        entry.receipt = *receipt;
        auto released = Release(packet);
        if (!released) {
            return released;
        }
        return Error(!source_valid  ? EagerRxErrorCode::kInvalidSource
                     : !frame_valid ? EagerRxErrorCode::kMalformedFrame
                                    : EagerRxErrorCode::kCreditFailed);
    }
    entry.receipt = *receipt;
    entry.decoded = *decoded;
    if (!handler) {
        auto released = Release(packet);
        if (!released) {
            return released;
        }
        return Error(EagerRxErrorCode::kInvalidArgument);
    }
    // 必须先做物理扣账，再拒绝旧 token；token 回收从不抹除 PeerRuntime 的旧授信责任。
    auto route = s.routes->Resolve(*decoded, source.local_domain, source.peer_context);
    if (!route) {
        auto released = Release(packet);
        if (!released) {
            return released;
        }
        return Error(EagerRxErrorCode::kRouteRejected);
    }
    s.dispatching = true;
    auto handled = handler(route->connection, *this, *decoded, packet, now_ns);
    s.dispatching = false;
    if (!handled || *handled == EagerRxDisposition::kRelease) {
        auto released = Release(packet);
        if (!released) {
            return released;
        }
    }
    if (!handled) {
        return std::unexpected(handled.error());
    }
    return {};
}

std::expected<EagerRxPacketView, EagerRxError> EagerReceiveDomain::View(EagerRxPacket packet) const noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    if (packet.domain != this || !packet.id || packet.slot >= state_->capacity) {
        return Error(EagerRxErrorCode::kInvalidPacket);
    }
    const auto& e = state_->entries[packet.slot];
    if (e.id != packet.id || e.phase != State::Phase::kHeld) {
        return Error(EagerRxErrorCode::kInvalidPacket);
    }
    return EagerRxPacketView{e.lease, e.decoded};
}

std::expected<void, EagerRxError> EagerReceiveDomain::Release(EagerRxPacket packet) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (packet.domain != this || !packet.id || packet.slot >= s.capacity) {
        return Error(EagerRxErrorCode::kInvalidPacket);
    }
    auto& e = s.entries[packet.slot];
    if (e.id != packet.id || (e.phase != State::Phase::kHeld && e.phase != State::Phase::kLeaseReleased)) {
        return Error(EagerRxErrorCode::kInvalidPacket);
    }
    if (e.phase == State::Phase::kHeld) {
        auto released = s.buffers->Release(e.lease);
        if (!released) {
            Freeze();
            return Error(EagerRxErrorCode::kReceiveFailed);
        }
        e.phase = State::Phase::kLeaseReleased;
    }
    auto released = s.credits->Release(e.receipt);
    if (!released) {
        Freeze();
        return Error(EagerRxErrorCode::kCreditFailed);
    }
    s.queue[(s.queue_head + s.queued) % s.capacity] = packet.slot;
    ++s.queued;
    --s.held;
    e.phase = State::Phase::kQueued;
    return {};
}

std::expected<std::size_t, EagerRxError> EagerReceiveDomain::Refill(std::size_t limit) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (!limit || limit > raw::RxBufferPool::kMaxBatch) {
        return Error(EagerRxErrorCode::kInvalidArgument);
    }
    if (frozen()) {
        return Error(EagerRxErrorCode::kStopped);
    }
    const auto count = std::min(limit, s.queued);
    if (!count) {
        return 0;
    }
    auto posted = s.buffers->Refill(count);
    const auto accepted = posted ? std::optional<std::size_t>(*posted) : posted.error().accepted;
    if (!accepted || *accepted > count) {
        Freeze();
        return std::unexpected(
            EagerRxError{EagerRxErrorCode::kRefillFailed, posted ? 0 : posted.error().provider_error, 0, false});
    }
    if (*accepted) {
        std::array<RxCreditReceipt, raw::RxBufferPool::kMaxBatch> receipts;
        for (std::size_t i = 0; i < *accepted; ++i) {
            receipts[i] = s.entries[s.queue[(s.queue_head + i) % s.capacity]].receipt;
        }
        auto recorded = s.credits->RecordRefillAccepted(std::span(receipts).first(*accepted));
        if (!recorded) {
            Freeze();
            return Error(EagerRxErrorCode::kRetained);
        }
        for (std::size_t i = 0; i < *accepted; ++i) {
            const auto index = s.queue[s.queue_head];
            s.queue_head = (s.queue_head + 1) % s.capacity;
            s.entries[index] = {.next = s.free};
            s.free = index;
        }
        s.queued -= *accepted;
    }
    if (!posted) {
        if (s.buffers->stopped()) {
            Freeze();
        }
        return std::unexpected(
            EagerRxError{EagerRxErrorCode::kRefillFailed, posted.error().provider_error, *accepted, true});
    }
    return *accepted;
}

void EagerReceiveDomain::Freeze() noexcept {
    if (state_) {
        state_->frozen = true;
        state_->credits->Freeze();
        state_->buffers->Stop();
    }
}
bool EagerReceiveDomain::frozen() const noexcept {
    return !state_ || state_->frozen || state_->credits->stats().frozen || state_->buffers->stopped();
}
std::size_t EagerReceiveDomain::held() const noexcept {
    return state_ ? state_->held : 0;
}
std::size_t EagerReceiveDomain::pending_refill() const noexcept {
    return state_ ? state_->queued : 0;
}
bool EagerReceiveDomain::Matches(const ReceiveCreditDomain& credits, const ConnectionTable& routes) const noexcept {
    return state_ && state_->credits == &credits && state_->routes == &routes;
}
urma_context_t* EagerReceiveDomain::context() const noexcept {
    return state_ && state_->buffers->pool() ? state_->buffers->pool()->context() : nullptr;
}
raw::RxBufferPool* EagerReceiveDomain::buffers() const noexcept {
    return state_ ? state_->buffers : nullptr;
}
std::expected<void, EagerRxError> EagerReceiveDomain::Close() noexcept {
    if (!state_) {
        return {};
    }
    if (held() || pending_refill() || state_->credits->stats().unattributed || state_->dispatching) {
        return Error(EagerRxErrorCode::kRetained);
    }
    if (!state_->credits->UnbindReceive(*this)) {
        return Error(EagerRxErrorCode::kCreditFailed);
    }
    state_.reset();
    return {};
}
std::expected<void, EagerRxError> EagerReceiveDomain::CloseDestroyed(const raw::ReceiveDomainCloser& closer) noexcept {
    if (!state_) {
        return {};
    }
    auto& s = *state_;
    if (s.dispatching || s.held) {
        return Error(EagerRxErrorCode::kRetained);
    }
    if (!closer.ClosedDomain(*s.buffers, s.buffer_incarnation)) {
        return Error(EagerRxErrorCode::kProofRequired);
    }
    if (s.credits->receive_domain() != s.credit_incarnation) {
        return Error(EagerRxErrorCode::kCreditFailed);
    }
    // 单 owner 控制路径先检查整份借用：任一失效 receipt 均不能留下部分清退结果。
    // 每次 Dispatch 分配唯一 receipt；遍历唯一 Entry，避免依赖可环绕的 refill 队列位置。
    std::size_t released = 0;
    for (std::size_t i = 0; i < s.capacity; ++i) {
        const auto& entry = s.entries[i];
        if (entry.phase == State::Phase::kFree) {
            continue;
        }
        if (entry.phase != State::Phase::kQueued) {
            return Error(EagerRxErrorCode::kRetained);
        }
        if (!s.credits->ValidateReleasedForClose(entry.receipt)) {
            return Error(EagerRxErrorCode::kCreditFailed);
        }
        ++released;
    }
    if (released != s.queued) {
        return Error(EagerRxErrorCode::kRetained);
    }
    const auto closed = s.credits->CloseDestroyed(*this, s.credit_incarnation, released);
    if (!closed) {
        return Error(EagerRxErrorCode::kCreditFailed);
    }
    state_.reset();
    return {};
}

struct EagerRxStream::State {
    struct Entry {
        EagerRxPacket packet;
        bool active = false, physical_released = false;
    };
    EagerReceiveDomain* domain = nullptr;
    ReorderWindow* window = nullptr;
    WireSessionBinding binding;
    std::unique_ptr<Entry[]> entries;
    std::optional<ReorderDelivery> current;
    std::size_t capacity = 0, pending = 0, offset = 0;
    std::size_t lease_bytes = 0;
    std::uint64_t lease_generation = 0;
    bool release_started = false;
    bool failed = false, stopped = false;
};
EagerRxStream::EagerRxStream() noexcept = default;
EagerRxStream::~EagerRxStream() {
    if (!Close()) {
        // 应用视图及真实 Lease/receipt 尚未归还，保留清理元数据；宿主也必须保留底层依赖。
        (void)state_.release();
    }
}

std::expected<void, EagerRxError> EagerRxStream::Open(EagerReceiveDomain& domain, ReorderWindow& window,
                                                      WireSessionBinding binding) noexcept {
    if (state_) {
        return Error(EagerRxErrorCode::kInUse);
    }
    const auto stats = window.stats();
    const auto capacity = stats.max_frame_seq - stats.released_prefix;
    if (domain.frozen() || !window.accepting() || stats.buffered_frames || stats.delivered_frames || !capacity ||
        capacity > ReorderWindow::kMaxWindow || !binding.context.peer_runtime || !binding.context.receive_domain ||
        !binding.context.session || !binding.token.generation) {
        return Error(EagerRxErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(EagerRxErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) State::Entry[capacity]);
    if (!state->entries) {
        return Error(EagerRxErrorCode::kNoMemory);
    }
    state->domain = &domain;
    state->window = &window;
    state->binding = binding;
    state->capacity = capacity;
    state_ = std::move(state);
    return {};
}

std::expected<void, EagerRxError> EagerRxStream::Accept(EagerRxPacket packet, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.stopped || s.failed) {
        return Error(EagerRxErrorCode::kStopped);
    }
    auto view = s.domain->View(packet);
    if (!view) {
        return std::unexpected(view.error());
    }
    const auto& frame = view->decoded.frame;
    auto identity = ValidateWireIdentity(view->decoded, s.binding, s.binding.context);
    if (!identity) {
        return Error(EagerRxErrorCode::kInvalidSource);
    }
    const bool fin = frame.type == WireFrameType::kControl && frame.control == WireControlType::kFin;
    if (frame.type == WireFrameType::kControl && !fin) {
        return Error(EagerRxErrorCode::kUnsupportedFrame);
    }
    auto& entry = s.entries[frame.seq % s.capacity];
    if (entry.active) {
        Fail();
        return Error(EagerRxErrorCode::kReorderFailed);
    }
    const auto& context = s.binding.context;
    ReorderFrame reorder{{context.peer_runtime, context.receive_domain, context.session, s.binding.token.slot,
                          s.binding.token.generation},
                         frame.seq,
                         fin ? ReorderFrameKind::kFin : ReorderFrameKind::kEager,
                         view->lease,
                         frame.data};
    auto pushed = s.window->Push(reorder, now_ns);
    if (!pushed) {
        Fail();
        return Error(EagerRxErrorCode::kReorderFailed);
    }
    entry = {packet, true, false};
    ++s.pending;
    return {};
}

std::expected<void, EagerRxError> EagerRxStream::ReleaseDelivery(const ReorderDelivery& delivery) noexcept {
    auto& s = *state_;
    auto& entry = s.entries[delivery.frame.sequence % s.capacity];
    if (!entry.active) {
        Fail();
        return Error(EagerRxErrorCode::kRetained);
    }
    if (!entry.physical_released) {
        auto released = s.domain->Release(entry.packet);
        if (!released) {
            Fail();
            return released;
        }
        entry.physical_released = true;
    }
    auto released = s.window->Release(delivery.receipt);
    if (!released) {
        Fail();
        return Error(EagerRxErrorCode::kRetained);
    }
    entry = {};
    --s.pending;
    return {};
}

std::expected<EagerReadResult, EagerRxError> EagerRxStream::Read(std::span<std::byte> output,
                                                                 std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.failed || s.stopped) {
        return Error(EagerRxErrorCode::kStopped);
    }
    if (s.lease_generation) {
        return Error(EagerRxErrorCode::kInUse);
    }
    EagerReadResult result;
    if (output.empty()) {
        return EagerReadResult{0, eof(), false};
    }
    for (std::size_t i = 0; i < ReorderWindow::kMaxBatch && result.bytes < output.size(); ++i) {
        if (!s.current) {
            ReorderDelivery delivery;
            auto polled = s.window->Poll(std::span(&delivery, 1), now_ns);
            if (!polled) {
                Fail();
                if (result.bytes) {
                    return result;
                }
                return Error(EagerRxErrorCode::kReorderFailed);
            }
            if (!*polled) {
                break;
            }
            s.current = delivery;
            s.offset = 0;
        }
        auto& delivery = *s.current;
        const auto count = std::min(output.size() - result.bytes, delivery.frame.payload.size() - s.offset);
        if (count) {
            std::memcpy(output.data() + result.bytes, delivery.frame.payload.data() + s.offset, count);
            s.offset += count;
            result.bytes += count;
        }
        if (s.offset != delivery.frame.payload.size()) {
            break;
        }
        auto released = ReleaseDelivery(delivery);
        if (!released) {
            if (result.bytes) {
                return result;
            }
            return std::unexpected(released.error());
        }
        s.current.reset();
    }
    result.eof = eof();
    result.would_block = !result.bytes && !result.eof;
    return result;
}

std::expected<EagerReadLeaseResult, EagerRxError> EagerRxStream::ReadLease(std::size_t max_bytes,
                                                                           std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (s.failed || s.stopped) {
        return Error(EagerRxErrorCode::kStopped);
    }
    if (!max_bytes) {
        return Error(EagerRxErrorCode::kInvalidArgument);
    }
    if (s.lease_generation) {
        return Error(EagerRxErrorCode::kInUse);
    }
    if (next_lease_ == std::numeric_limits<std::uint64_t>::max()) {
        // 永不回绕公开代次；在移动窗口交付位置前停止新的借用。
        return Error(EagerRxErrorCode::kStopped);
    }
    if (!s.current) {
        ReorderDelivery delivery;
        const auto polled = s.window->Poll(std::span(&delivery, 1), now_ns);
        if (!polled) {
            Fail();
            return Error(EagerRxErrorCode::kReorderFailed);
        }
        if (!*polled) {
            return EagerReadLeaseResult{.eof = eof(), .would_block = !eof()};
        }
        s.current = delivery;
        s.offset = 0;
    }
    const auto& delivery = *s.current;
    if (delivery.frame.kind == ReorderFrameKind::kFin) {
        const auto released = ReleaseDelivery(delivery);
        if (!released) {
            return std::unexpected(released.error());
        }
        s.current.reset();
        return EagerReadLeaseResult{.eof = eof()};
    }
    const auto bytes = std::min(max_bytes, delivery.frame.payload.size() - s.offset);
    s.lease_generation = ++next_lease_;
    s.lease_bytes = bytes;
    s.release_started = false;
    return EagerReadLeaseResult{.generation = s.lease_generation,
                                .data = delivery.frame.payload.subspan(s.offset, bytes)};
}

std::expected<void, EagerRxError> EagerRxStream::ReleaseLease(std::uint64_t generation) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (!generation || generation != s.lease_generation) {
        return Error(EagerRxErrorCode::kInvalidLease);
    }
    if (!s.current) {
        Fail();
        return Error(EagerRxErrorCode::kRetained);
    }
    if (!s.release_started) {
        // 此后旧视图不可再访问；先锁存交回意图，底层部分清理失败后只重试责任，不重复消费字节。
        s.release_started = true;
        s.offset += s.lease_bytes;
    }
    if (s.offset == s.current->frame.payload.size()) {
        const auto released = ReleaseDelivery(*s.current);
        if (!released) {
            return released;
        }
        s.current.reset();
    }
    s.lease_generation = 0;
    s.lease_bytes = 0;
    s.release_started = false;
    return {};
}

void EagerRxStream::Fail() noexcept {
    if (state_) {
        state_->failed = true;
        state_->window->Fail();
    }
}
std::expected<std::size_t, EagerRxError> EagerRxStream::Drain(std::size_t budget) noexcept {
    if (!state_) {
        return Error(EagerRxErrorCode::kNotReady);
    }
    auto& s = *state_;
    if (!budget || budget > ReorderWindow::kMaxBatch || (!s.stopped && !s.failed)) {
        return Error(EagerRxErrorCode::kInvalidArgument);
    }
    if (s.lease_generation) {
        // 不抢回应用视图，也不绕过待重试的精确 token；压力仍由固定窗口与物理 RX 容量约束。
        return std::size_t{0};
    }
    std::size_t released_count = 0;
    if (s.current) {
        auto released = ReleaseDelivery(*s.current);
        if (!released) {
            return std::unexpected(released.error());
        }
        s.current.reset();
        ++released_count;
    }
    // 一次只交出一个 receipt，失败时仍保存在 current，不能丢失已经 Delivered 的清理责任。
    for (std::size_t i = released_count; i < budget; ++i) {
        ReorderDelivery delivery;
        auto drained = s.window->Drain(std::span(&delivery, 1));
        if (!drained) {
            return Error(EagerRxErrorCode::kReorderFailed);
        }
        if (!*drained) {
            continue;
        }
        s.current = delivery;
        auto released = ReleaseDelivery(delivery);
        if (!released) {
            return std::unexpected(released.error());
        }
        s.current.reset();
        ++released_count;
    }
    return released_count;
}
std::expected<void, EagerRxError> EagerRxStream::Close() noexcept {
    if (!state_) {
        return {};
    }
    state_->stopped = true;
    state_->window->StopAccepting();
    if (state_->pending || state_->lease_generation) {
        return Error(EagerRxErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
bool EagerRxStream::eof() const noexcept {
    return state_ && !state_->failed && !state_->lease_generation && state_->window->eof();
}
bool EagerRxStream::readable() const noexcept {
    return state_ && !state_->failed && !state_->stopped && !state_->lease_generation && !state_->window->failure() &&
           ((state_->current && state_->offset < state_->current->frame.payload.size()) || state_->window->readable());
}
bool EagerRxStream::failed() const noexcept {
    return state_ && state_->failed;
}
std::size_t EagerRxStream::pending() const noexcept {
    return state_ ? state_->pending : 0;
}

} // namespace connection
} // namespace kbsocket
