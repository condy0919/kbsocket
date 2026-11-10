// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_receiver.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <span>
#include <utility>
#include <variant>

#include "kbsocket/connection/rndv_protocol.hpp"

namespace kbsocket {
namespace connection {
namespace {
auto Error(RndvReceiveErrorCode code) noexcept {
    return std::unexpected(RndvReceiveError{code});
}
ReorderSession Session(const GrantBinding& binding) noexcept {
    return {binding.sender_runtime, binding.receive_domain, binding.session, binding.token.slot,
            binding.token.generation};
}
bool GrantPressure(const GrantError& error) noexcept {
    return error.code == GrantErrorCode::kWindowFull || error.code == GrantErrorCode::kExhausted ||
           (error.code == GrantErrorCode::kMemory && error.memory_error &&
            error.memory_error->code == memory::MemoryErrorCode::kExhausted);
}
} // namespace

struct RndvReceiver::Entry {
    GrantTransfer transfer;
    GrantReservation grant;
    ReorderReservation reorder;
    std::span<const std::byte> payload;
    std::uint64_t generation = 0;
    bool active = false;
    bool has_grant = false;
    bool pending = false;
    bool done_proven = false;
    bool remote_terminal = false;
    bool borrowed = false;
    bool in_reorder = false;
    bool target_returned = false;
    bool cancelled = false;
};
struct RndvReceiver::State {
    RndvReceiverConfig config;
    GrantManager* grants = nullptr;
    ReorderWindow* reorder = nullptr;
    const EagerRxStream* stream = nullptr;
    const RndvRxIngress* ingress = nullptr;
    std::unique_ptr<Entry[]> entries;
    std::uint64_t next_generation = 0;
    bool grants_bound = false;
    bool reorder_bound = false;
    bool stopped = false;
};

RndvReceiver::RndvReceiver() noexcept = default;
RndvReceiver::~RndvReceiver() {
    if (!Close()) {
        // 保留元数据不代替外部依赖的生命周期；宿主仍须保留 Grant/窗口/注册 backing。
        (void)state_.release();
    }
}
std::expected<void, RndvReceiveError> RndvReceiver::Open(const RndvReceiverConfig& config, GrantManager& grants,
                                                         ReorderWindow& reorder) noexcept {
    if (state_ || opened_once_) {
        return Error(RndvReceiveErrorCode::kInUse);
    }
    const auto& binding = config.binding;
    if (!config.capacity || config.capacity > kMaxCapacity || config.capacity > grants.window_size() ||
        !config.peer_successful_write_done || !binding.sender_runtime || !binding.receiver_runtime ||
        !binding.receive_domain || !binding.session || !binding.token.generation ||
        config.route.context.version != kWireVersion || !config.route.credit_epoch ||
        config.route.context.peer_runtime != binding.sender_runtime ||
        config.route.context.receive_domain != binding.receive_domain ||
        config.route.context.session != binding.session || config.route.token != binding.token || !grants.accepting() ||
        !reorder.accepting()) {
        return Error(RndvReceiveErrorCode::kInvalidArgument);
    }
    const auto actual_binding = grants.binding();
    const auto actual_session = reorder.session();
    if (!actual_binding || *actual_binding != binding || !actual_session || *actual_session != Session(binding)) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    const auto grant_stats = grants.stats();
    if (grant_stats.active || grant_stats.window_begin != reorder.stats().released_prefix) {
        return Error(RndvReceiveErrorCode::kInUse);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(RndvReceiveErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) Entry[config.capacity]);
    if (!state->entries) {
        return Error(RndvReceiveErrorCode::kNoMemory);
    }
    state->config = config;
    state->grants = &grants;
    state->reorder = &reorder;
    if (!grants.BindReceiver(*this)) {
        return Error(RndvReceiveErrorCode::kInUse);
    }
    state->grants_bound = true;
    if (!reorder.BindReceiver(*this)) {
        if (!grants.UnbindReceiver(*this)) {
            // 极端回滚失败也保留精确绑定关系，Close 继续拆除，不触碰无关窗口。
            state->stopped = true;
            state_ = std::move(state);
            return Error(RndvReceiveErrorCode::kRetained);
        }
        return Error(RndvReceiveErrorCode::kInUse);
    }
    state->reorder_bound = true;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
bool RndvReceiver::Matches(const ReorderWindow& reorder, WireSessionBinding route) const noexcept {
    return state_ && opened_once_ && state_->reorder == &reorder && state_->config.route.context == route.context &&
           state_->config.route.token == route.token && state_->config.route.credit_epoch == route.credit_epoch;
}
bool RndvReceiver::Matches(const GrantManager& grants, const ReorderWindow& reorder,
                           WireSessionBinding route) const noexcept {
    return Matches(reorder, route) && state_->grants == &grants;
}
std::expected<void, RndvReceiveError>
RndvReceiver::BindStream(const EagerRxStream& stream, const ReorderWindow& reorder, WireSessionBinding route) noexcept {
    if (!Matches(reorder, route)) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    if (state_->stream) {
        return Error(RndvReceiveErrorCode::kInUse);
    }
    if (state_->stopped) {
        return Error(RndvReceiveErrorCode::kStopped);
    }
    state_->stream = &stream;
    return {};
}
std::expected<void, RndvReceiveError> RndvReceiver::UnbindStream(const EagerRxStream& stream) noexcept {
    if (!state_ || state_->stream != &stream) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    state_->stream = nullptr;
    return {};
}
std::expected<void, RndvReceiveError> RndvReceiver::BindIngress(const RndvRxIngress& ingress) noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    if (state_->ingress) {
        return Error(RndvReceiveErrorCode::kInUse);
    }
    if (state_->stopped) {
        return Error(RndvReceiveErrorCode::kStopped);
    }
    state_->ingress = &ingress;
    return {};
}
std::expected<void, RndvReceiveError> RndvReceiver::UnbindIngress(const RndvRxIngress& ingress) noexcept {
    if (!state_ || state_->ingress != &ingress) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    state_->ingress = nullptr;
    return {};
}
std::expected<RndvReceiver::Entry*, RndvReceiveError> RndvReceiver::Find(RndvReceiveHandle handle) const noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    if (handle.owner != this || !handle.generation || handle.slot >= state_->config.capacity) {
        return Error(RndvReceiveErrorCode::kInvalidHandle);
    }
    auto& entry = state_->entries[handle.slot];
    if (!entry.active || entry.generation != handle.generation) {
        return Error(RndvReceiveErrorCode::kInvalidHandle);
    }
    return &entry;
}
std::expected<RndvReceiveHandle, RndvReceiveError>
RndvReceiver::OnRts(const WireFrameView& view, const WireReceiveContext& observed, std::uint64_t now_ns) noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped || !state.reorder->accepting() || !state.grants->accepting()) {
        return Error(RndvReceiveErrorCode::kStopped);
    }
    const auto transfer = ValidateRndvRts(view, state.config.route, observed, state.config.binding);
    if (!transfer) {
        return Error(RndvReceiveErrorCode::kProtocol);
    }
    std::uint32_t free = state.config.capacity;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        const auto& entry = state.entries[i];
        if (entry.active && entry.transfer.seq == transfer->seq) {
            return Error(RndvReceiveErrorCode::kDuplicate);
        }
        if (!entry.active && free == state.config.capacity) {
            free = i;
        }
    }
    if (free == state.config.capacity) {
        return Error(RndvReceiveErrorCode::kWouldBlock);
    }
    if (state.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(RndvReceiveErrorCode::kInvalidState);
    }
    const auto reserved =
        state.reorder->ReserveRndv(Session(state.config.binding), transfer->seq, transfer->bytes, now_ns);
    if (!reserved) {
        return Error(reserved.error().code == ReorderErrorCode::kDuplicate ? RndvReceiveErrorCode::kDuplicate
                                                                           : RndvReceiveErrorCode::kReorder);
    }
    auto& entry = state.entries[free];
    entry = Entry{};
    entry.transfer = *transfer;
    entry.reorder = *reserved;
    entry.generation = ++state.next_generation;
    entry.active = true;
    entry.pending = true;
    return RndvReceiveHandle{this, free, entry.generation};
}
std::expected<void, RndvReceiveError> RndvReceiver::AdvanceGrantWindow() noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    const auto prefix = state_->reorder->stats().released_prefix;
    const auto begin = state_->grants->stats().window_begin;
    if (prefix < begin) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    if (prefix == begin) {
        return {};
    }
    // 单次只扫描原管理器的一个固定窗口；大量 Eager 前缀也不形成无界热循环。
    const auto next = begin + std::min(prefix - begin, static_cast<std::uint64_t>(state_->grants->window_size()));
    if (next == begin || !state_->grants->AdvanceWindow(next)) {
        return Error(RndvReceiveErrorCode::kGrant);
    }
    return {};
}
std::expected<GrantReservation, RndvReceiveError> RndvReceiver::PrepareGrant(RndvReceiveHandle handle) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& entry = **found;
    if (state_->stopped || entry.cancelled || !state_->reorder->accepting()) {
        return Error(RndvReceiveErrorCode::kStopped);
    }
    if (entry.has_grant) {
        if (entry.done_proven) {
            return Error(RndvReceiveErrorCode::kInvalidState);
        }
        return entry.grant;
    }
    if (const auto advanced = AdvanceGrantWindow(); !advanced) {
        return std::unexpected(advanced.error());
    }
    const auto reserved = state_->grants->Reserve(entry.transfer);
    if (!reserved) {
        return Error(GrantPressure(reserved.error()) ? RndvReceiveErrorCode::kWouldBlock
                                                     : RndvReceiveErrorCode::kGrant);
    }
    entry.grant = *reserved;
    entry.has_grant = true;
    return *reserved;
}
std::expected<void, RndvReceiveError> RndvReceiver::Discard(Entry& entry) noexcept {
    entry.cancelled = true;
    if (entry.in_reorder) {
        // 共同窗口或应用实际持有的借用只能通过可信 delivery 归还，不能猜测 receipt。
        return {};
    }
    if (entry.pending) {
        if (!state_->reorder->AbandonRndv(entry.reorder)) {
            return Error(RndvReceiveErrorCode::kRetained);
        }
        entry.pending = false;
    }
    if (!entry.has_grant || entry.target_returned) {
        entry = Entry{};
        return {};
    }
    if (entry.borrowed) {
        if (!state_->grants->Return(entry.grant.handle)) {
            return Error(RndvReceiveErrorCode::kRetained);
        }
        entry = Entry{};
        return {};
    }
    const auto retired = state_->grants->IsRetired(entry.grant.handle, entry.grant.identity);
    if (!retired) {
        return Error(RndvReceiveErrorCode::kRetained);
    }
    if (*retired) {
        entry = Entry{};
        return {};
    }
    const bool unpublished = state_->grants->ValidatePublication(entry.grant.handle, entry.grant.identity).has_value();
    if (!state_->grants->Cancel(entry.grant.handle)) {
        return Error(RndvReceiveErrorCode::kRetained);
    }
    if (unpublished || entry.remote_terminal) {
        entry = Entry{};
    }
    return {};
}
std::expected<void, RndvReceiveError> RndvReceiver::Complete(Entry& entry, std::uint64_t now_ns) noexcept {
    if (!entry.done_proven || !entry.has_grant || entry.in_reorder) {
        return Error(RndvReceiveErrorCode::kInvalidState);
    }
    auto& state = *state_;
    const bool discard = state.stopped || entry.cancelled || !state.reorder->accepting();
    if (!entry.remote_terminal) {
        const auto completed = state.grants->CompleteNotification(entry.grant.identity, discard);
        if (!completed) {
            return Error(RndvReceiveErrorCode::kGrant);
        }
        entry.remote_terminal = true;
        entry.target_returned = !*completed;
    }
    if (discard || entry.target_returned) {
        state.stopped = true;
        state.reorder->Fail();
        state.grants->Stop();
        return Discard(entry);
    }
    if (!entry.borrowed) {
        const auto borrowed = state.grants->Borrow(entry.grant.handle);
        if (!borrowed) {
            return Error(RndvReceiveErrorCode::kGrant);
        }
        entry.payload = *borrowed;
        entry.borrowed = true;
    }
    const ReorderFrame frame{Session(state.config.binding), entry.transfer.seq, ReorderFrameKind::kValidatedRmaDone,
                             entry.grant.handle, entry.payload};
    const auto completed = state.reorder->CompleteRndv(entry.reorder, frame, now_ns);
    if (!completed) {
        if (!state.reorder->accepting()) {
            state.stopped = true;
            state.reorder->Fail();
            state.grants->Stop();
            if (!Discard(entry)) {
                return Error(RndvReceiveErrorCode::kRetained);
            }
        }
        return Error(RndvReceiveErrorCode::kReorder);
    }
    entry.pending = false;
    entry.in_reorder = true;
    return {};
}
std::expected<void, RndvReceiveError>
RndvReceiver::OnDone(const WireFrameView& view, const WireReceiveContext& observed, std::uint64_t now_ns) noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    auto& state = *state_;
    Entry* entry = nullptr;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        if (state.entries[i].active && state.entries[i].transfer.seq == view.frame.seq) {
            entry = &state.entries[i];
            break;
        }
    }
    if (!entry || !entry->has_grant) {
        return Error(RndvReceiveErrorCode::kInvalidHandle);
    }
    if (!ValidateRndvDone(view, state.config.route, observed, entry->grant.identity)) {
        return Error(RndvReceiveErrorCode::kProtocol);
    }
    if (entry->in_reorder || entry->target_returned) {
        return Error(RndvReceiveErrorCode::kDuplicate);
    }
    if (!entry->done_proven) {
        // 完整 wire 身份仍不能令 Reserved 授权提前结束；真正完成入口先检查 Published/Quarantined。
        const auto completed = state.grants->CompleteNotification(
            entry->grant.identity, state.stopped || entry->cancelled || !state.reorder->accepting());
        if (!completed) {
            if (completed.error().code == GrantErrorCode::kMemory) {
                // 可信通知已到达；部分本地清理失败可以由 Progress 重试，不能丢失证明。
                entry->done_proven = true;
            }
            return Error(RndvReceiveErrorCode::kGrant);
        }
        entry->done_proven = true;
        entry->remote_terminal = true;
        entry->target_returned = !*completed;
    }
    return Complete(*entry, now_ns);
}
std::expected<std::size_t, RndvReceiveError> RndvReceiver::Progress(std::uint64_t now_ns) noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (const auto checked = state.reorder->CheckTimeout(now_ns); !checked) {
        if (checked.error().code == ReorderErrorCode::kInvalidArgument) {
            return Error(RndvReceiveErrorCode::kReorder);
        }
        state.stopped = true;
        state.reorder->Fail();
        state.grants->Stop();
    }
    if (!state.reorder->accepting()) {
        state.stopped = true;
        state.grants->Stop();
    }
    std::size_t progressed = 0;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        auto& entry = state.entries[i];
        if (!entry.active || entry.in_reorder) {
            continue;
        }
        if (entry.done_proven) {
            if (const auto completed = Complete(entry, now_ns); !completed) {
                return std::unexpected(completed.error());
            }
            ++progressed;
        } else if (state.stopped || entry.cancelled) {
            const bool was_active = entry.active;
            const bool was_pending = entry.pending;
            if (const auto discarded = Discard(entry); !discarded) {
                return std::unexpected(discarded.error());
            }
            progressed += (was_active != entry.active) || (was_pending != entry.pending);
        }
    }
    if (const auto advanced = AdvanceGrantWindow(); !advanced) {
        return std::unexpected(advanced.error());
    }
    return progressed;
}
std::expected<void, RndvReceiveError> RndvReceiver::ReturnGrant(const ReorderDelivery& delivery) noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    auto& state = *state_;
    const auto* handle = std::get_if<GrantHandle>(&delivery.frame.lease);
    if (!handle || handle->owner != state.grants || delivery.frame.kind != ReorderFrameKind::kValidatedRmaDone ||
        delivery.frame.session != Session(state.config.binding) || !state.reorder->ValidateDelivery(delivery)) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    Entry* entry = nullptr;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        auto& candidate = state.entries[i];
        if (candidate.active && candidate.in_reorder && candidate.grant.handle == *handle) {
            entry = &candidate;
            break;
        }
    }
    if (!entry || entry->transfer.seq != delivery.frame.sequence ||
        entry->payload.data() != delivery.frame.payload.data() ||
        entry->payload.size() != delivery.frame.payload.size()) {
        return Error(RndvReceiveErrorCode::kInvalidHandle);
    }
    if (!entry->target_returned) {
        if (!state.grants->Return(*handle)) {
            return Error(RndvReceiveErrorCode::kRetained);
        }
        entry->target_returned = true;
        entry->borrowed = false;
    }
    if (!state.reorder->Release(delivery.receipt)) {
        return Error(RndvReceiveErrorCode::kRetained);
    }
    *entry = Entry{};
    // receipt 已消费，后续窗口整理失败不能令调用方误以为可重试归还同一 delivery。
    // Grant tombstone 由 Progress/PrepareGrant 单独跟随真实 released_prefix 推进。
    return {};
}
std::expected<void, RndvReceiveError> RndvReceiver::Cancel() noexcept {
    if (!state_ || !opened_once_) {
        return Error(RndvReceiveErrorCode::kNotReady);
    }
    auto& state = *state_;
    state.stopped = true;
    state.reorder->Fail();
    state.reorder->StopAccepting();
    state.grants->Stop();
    bool settled = true;
    for (std::uint32_t i = 0; i < state.config.capacity; ++i) {
        if (state.entries[i].active && !Discard(state.entries[i])) {
            settled = false;
        }
    }
    return settled ? std::expected<void, RndvReceiveError>{} : Error(RndvReceiveErrorCode::kRetained);
}
std::expected<void, RndvReceiveError> RndvReceiver::ConfirmTerminal(RndvReceiveHandle handle,
                                                                    const GrantTerminalEvidence& evidence) noexcept {
    const auto found = Find(handle);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& entry = **found;
    if (!state_->stopped || !entry.has_grant || entry.in_reorder || entry.borrowed) {
        return Error(RndvReceiveErrorCode::kInvalidState);
    }
    if (evidence.identity != entry.grant.identity) {
        return Error(RndvReceiveErrorCode::kWrongIdentity);
    }
    if (!entry.target_returned) {
        if (!state_->grants->ConfirmTerminal(evidence)) {
            return Error(RndvReceiveErrorCode::kGrant);
        }
        entry.target_returned = true;
        entry.remote_terminal = true;
    }
    return Discard(entry);
}
std::expected<void, RndvReceiveError> RndvReceiver::Close() noexcept {
    if (!state_) {
        return {};
    }
    if (state_->stream || state_->ingress) {
        // 即使暂无 Grant，流仍持有本对象指针；拒绝关闭且不取消健康共同窗口。
        return Error(RndvReceiveErrorCode::kInUse);
    }
    if (stats().active) {
        if (!Cancel() || stats().active) {
            return Error(RndvReceiveErrorCode::kRetained);
        }
    }
    if (state_->reorder_bound) {
        if (!state_->reorder->UnbindReceiver(*this)) {
            return Error(RndvReceiveErrorCode::kRetained);
        }
        state_->reorder_bound = false;
    }
    if (state_->grants_bound) {
        if (!state_->grants->UnbindReceiver(*this)) {
            return Error(RndvReceiveErrorCode::kRetained);
        }
        state_->grants_bound = false;
    }
    state_.reset();
    return {};
}
RndvReceiveStats RndvReceiver::stats() const noexcept {
    RndvReceiveStats result;
    if (!state_) {
        return result;
    }
    result.stopped = state_->stopped;
    for (std::uint32_t i = 0; i < state_->config.capacity; ++i) {
        const auto& entry = state_->entries[i];
        if (!entry.active) {
            continue;
        }
        ++result.active;
        if (entry.in_reorder) {
            ++result.in_reorder;
        } else if (!entry.has_grant) {
            ++result.waiting_grant;
        } else {
            ++result.waiting_done;
        }
    }
    return result;
}
} // namespace connection
} // namespace kbsocket
