// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/credit_account.hpp"

#include <algorithm>
#include <limits>
#include <new>

namespace kbsocket {
namespace connection {
namespace {
auto Error(CreditErrorCode code) noexcept {
    return std::unexpected(CreditError{code});
}
bool Valid(CreditClass kind) noexcept {
    return kind == CreditClass::kData || kind == CreditClass::kControl;
}
bool Valid(CreditIdentity identity) noexcept {
    return identity.peer_runtime && identity.receive_domain && identity.epoch;
}
constexpr std::uint32_t kEnd = std::numeric_limits<std::uint32_t>::max();
enum Bucket : std::uint8_t { kBase, kElastic, kControl, kUnknown };
} // namespace

std::expected<void, CreditError> SenderCreditAccount::Open(CreditIdentity identity) noexcept {
    if (ready_) {
        return Error(CreditErrorCode::kInUse);
    }
    if (!Valid(identity)) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    if (identity_.peer_runtime &&
        (identity.peer_runtime != identity_.peer_runtime || identity.receive_domain != identity_.receive_domain ||
         identity.epoch <= identity_.epoch)) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    identity_ = identity;
    limits_ = {};
    spent_ = {};
    pending_ = {};
    stopped_ = false;
    accepted_known_ = true;
    ready_ = true;
    return {};
}
std::expected<void, CreditError> SenderCreditAccount::Update(CreditIdentity identity, std::uint64_t max_data,
                                                             std::uint64_t max_control) noexcept {
    if (!ready_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (identity != identity_) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    limits_[0] = std::max(limits_[0], max_data);
    limits_[1] = std::max(limits_[1], max_control);
    return {};
}
std::uint64_t SenderCreditAccount::available(CreditClass kind) const noexcept {
    if (!ready_ || stopped_ || !Valid(kind)) {
        return 0;
    }
    const auto index = static_cast<std::size_t>(kind);
    const auto reserved = pending_.id && pending_.kind == kind ? pending_.count : 0;
    return limits_[index] - spent_[index] - reserved;
}
SenderCreditStats SenderCreditAccount::stats() const noexcept {
    SenderCreditStats result{
        .limits = limits_, .spent = spent_, .stopped = stopped_, .accepted_known = accepted_known_};
    if (pending_.id) {
        result.reserved[static_cast<std::size_t>(pending_.kind)] = pending_.count;
    }
    return result;
}
std::expected<CreditReservation, CreditError> SenderCreditAccount::Reserve(CreditClass kind,
                                                                           std::uint32_t count) noexcept {
    if (!ready_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (stopped_) {
        return Error(CreditErrorCode::kStopped);
    }
    if (!Valid(kind) || !count) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    if (pending_.id) {
        return Error(CreditErrorCode::kInUse);
    }
    if (next_reservation_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(CreditErrorCode::kSequenceExhausted);
    }
    if (count > available(kind)) {
        return Error(CreditErrorCode::kExhausted);
    }
    pending_ = {this, ++next_reservation_, kind, count};
    return pending_;
}
std::expected<void, CreditError> SenderCreditAccount::Resolve(CreditReservation reservation,
                                                              std::optional<std::uint32_t> accepted_prefix) noexcept {
    if (!ready_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (!pending_.id || reservation.account != this || reservation.id != pending_.id ||
        reservation.kind != pending_.kind || reservation.count != pending_.count) {
        return Error(CreditErrorCode::kInvalidReservation);
    }
    if (accepted_prefix && *accepted_prefix > pending_.count) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    spent_[static_cast<std::size_t>(pending_.kind)] += accepted_prefix.value_or(pending_.count);
    if (!accepted_prefix) {
        stopped_ = true;
        accepted_known_ = false;
    }
    pending_ = {};
    return {};
}
std::expected<void, CreditError> SenderCreditAccount::Close() noexcept {
    if (!ready_) {
        return {};
    }
    stopped_ = true;
    if (pending_.id || withdrawal_owner_) {
        return Error(CreditErrorCode::kInUse);
    }
    ready_ = false;
    return {};
}
std::expected<void, CreditError> SenderCreditAccount::BindWithdrawal(const control::PeerCreditControl& owner) noexcept {
    if (!ready_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (withdrawal_owner_) {
        return Error(CreditErrorCode::kInUse);
    }
    withdrawal_owner_ = &owner;
    return {};
}
std::expected<void, CreditError>
SenderCreditAccount::UnbindWithdrawal(const control::PeerCreditControl& owner) noexcept {
    if (withdrawal_owner_ != &owner) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    withdrawal_owner_ = nullptr;
    return {};
}
std::expected<SenderCreditStats, CreditError>
SenderCreditAccount::StoppedSnapshot(const control::PeerCreditControl& owner) const noexcept {
    if (!ready_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (withdrawal_owner_ != &owner) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    if (!stopped_) {
        return Error(CreditErrorCode::kProofRequired);
    }
    if (pending_.id) {
        return Error(CreditErrorCode::kInUse);
    }
    if (!accepted_known_) {
        return Error(CreditErrorCode::kRetained);
    }
    return stats();
}

struct ReceiveCreditDomain::State {
    struct Peer {
        CreditPeerStats stats;
        std::uint64_t id = 0;
        std::uint64_t published_control = 0;
        const control::PeerCreditControl* withdrawal_owner = nullptr;
    };
    struct Receipt {
        std::uint64_t id = 0;
        std::uint32_t peer = kEnd, next = kEnd;
        std::uint8_t bucket = kUnknown;
        bool active = false, released = false, checked = false;
    };
    CreditDomainConfig config;
    CreditDomainStats stats;
    std::array<std::uint64_t, 2> initial_unposted{};
    std::unique_ptr<Peer[]> peers;
    std::unique_ptr<Receipt[]> receipts;
    const EagerReceiveDomain* receive_owner = nullptr;
    std::uint32_t peer_slots = 0, free_receipt = 0;
};

ReceiveCreditDomain::ReceiveCreditDomain() noexcept = default;
ReceiveCreditDomain::~ReceiveCreditDomain() {
    if (!Close()) {
        // 未结清授权/tombstone 不因析构被清零；外部 RX backing 与域也必须继续保留。
        (void)state_.release();
    }
}
std::expected<void, CreditError> ReceiveCreditDomain::Open(const CreditDomainConfig& config) noexcept {
    if (state_) {
        return Error(CreditErrorCode::kInUse);
    }
    if (!config.receive_domain || config.receive_domain <= last_domain_ || !config.capacity ||
        config.capacity > kMaxCapacity || !config.control_slots || config.control_slots >= config.capacity ||
        !config.peer_capacity || config.peer_capacity > kMaxPeers || !config.base_credits ||
        config.base_credits > config.capacity - config.control_slots) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(CreditErrorCode::kNoMemory);
    }
    state->peers.reset(new (std::nothrow) State::Peer[config.peer_capacity]);
    state->receipts.reset(new (std::nothrow) State::Receipt[config.capacity]);
    if (!state->peers || !state->receipts) {
        return Error(CreditErrorCode::kNoMemory);
    }
    state->config = config;
    state->initial_unposted = {config.capacity - config.control_slots, config.control_slots};
    state->stats.data.unposted = state->initial_unposted[0];
    state->stats.control.unposted = state->initial_unposted[1];
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->receipts[i].next = i + 1 == config.capacity ? kEnd : i + 1;
    }
    state_ = std::move(state);
    last_domain_ = config.receive_domain;
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::Close() noexcept {
    if (!state_) {
        return {};
    }
    state_->stats.frozen = true;
    if (state_->receive_owner) {
        return Error(CreditErrorCode::kInUse);
    }
    if (state_->stats.unattributed) {
        return Error(CreditErrorCode::kRetained);
    }
    if (state_->stats.active_peers || state_->stats.held || state_->stats.released) {
        return Error(CreditErrorCode::kInUse);
    }
    for (std::uint32_t i = 0; i < state_->peer_slots; ++i) {
        if (state_->peers[i].withdrawal_owner) {
            return Error(CreditErrorCode::kInUse);
        }
    }
    state_.reset();
    return {};
}
CreditDomainStats ReceiveCreditDomain::stats() const noexcept {
    if (!state_) {
        return {};
    }
    auto stats = state_->stats;
    const auto accounted_posted = stats.data.free + stats.data.granted + stats.control.free + stats.control.granted;
    stats.physical_posted = accounted_posted - stats.unattributed;
    return stats;
}
std::uint64_t ReceiveCreditDomain::receive_domain() const noexcept {
    return state_ ? state_->config.receive_domain : 0;
}
std::expected<void, CreditError> ReceiveCreditDomain::BindReceive(const EagerReceiveDomain& owner) noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (state_->receive_owner) {
        return Error(CreditErrorCode::kInUse);
    }
    state_->receive_owner = &owner;
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::UnbindReceive(const EagerReceiveDomain& owner) noexcept {
    if (!state_ || state_->receive_owner != &owner) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    state_->receive_owner = nullptr;
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::ValidateReleasedForClose(RxCreditReceipt receipt) const noexcept {
    const auto index = Find(receipt);
    if (!index) {
        return std::unexpected(index.error());
    }
    if (!state_->receipts[*index].released) {
        return Error(CreditErrorCode::kInUse);
    }
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::CloseDestroyed(const EagerReceiveDomain& owner,
                                                                     std::uint64_t receive_domain,
                                                                     std::size_t released_receipts) noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (state_->receive_owner != &owner || state_->config.receive_domain != receive_domain) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    if (state_->stats.held || state_->stats.released != released_receipts) {
        return Error(CreditErrorCode::kRetained);
    }
    for (std::uint32_t i = 0; i < state_->peer_slots; ++i) {
        if (state_->peers[i].withdrawal_owner) {
            return Error(CreditErrorCode::kInUse);
        }
    }
    // 物理域已永久销毁，且桥接器已核验全部 receipt；整体终结元数据，不做 U->F 或重新授信。
    // 未归属的旧 RQE 无需猜测其 Peer/类别，不能把本路径用于仍存活的接收域。
    state_.reset();
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::RecordInitialPosted(CreditClass kind,
                                                                          std::uint32_t accepted) noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (!Valid(kind) || !accepted) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    const auto index = static_cast<std::size_t>(kind);
    if (accepted > state_->initial_unposted[index]) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    // 仅记录外部真实 post 的 accepted 数；即使域已冻结也不能遗漏此前接受的物理 RQE。
    state_->initial_unposted[index] -= accepted;
    auto& pool = kind == CreditClass::kData ? state_->stats.data : state_->stats.control;
    pool.unposted -= accepted;
    pool.free += accepted;
    if (kind == CreditClass::kData) {
        state_->stats.elastic_free += accepted;
    }
    return {};
}
std::expected<std::uint32_t, CreditError> ReceiveCreditDomain::Find(CreditPeerHandle peer) const noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (peer.domain != this || !peer.id || peer.slot >= state_->peer_slots || state_->peers[peer.slot].id != peer.id) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    return peer.slot;
}
std::expected<std::uint32_t, CreditError> ReceiveCreditDomain::Find(RxCreditReceipt receipt) const noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (receipt.domain != this || !receipt.id || receipt.slot >= state_->config.capacity) {
        return Error(CreditErrorCode::kInvalidReceipt);
    }
    const auto& record = state_->receipts[receipt.slot];
    if (!record.active || record.id != receipt.id) {
        return Error(CreditErrorCode::kInvalidReceipt);
    }
    return receipt.slot;
}
std::expected<CreditPeerStats, CreditError> ReceiveCreditDomain::PeerStats(CreditPeerHandle peer) const noexcept {
    auto index = Find(peer);
    if (!index) {
        return std::unexpected(index.error());
    }
    return state_->peers[*index].stats;
}
std::expected<CreditPeerHandle, CreditError> ReceiveCreditDomain::Admit(CreditIdentity identity) noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (state_->stats.frozen) {
        return Error(CreditErrorCode::kStopped);
    }
    if (!Valid(identity) || identity.receive_domain != state_->config.receive_domain) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    for (std::uint32_t i = 0; i < state_->peer_slots; ++i) {
        const auto& old = state_->peers[i].stats;
        if (old.identity.peer_runtime == identity.peer_runtime) {
            if (identity.epoch <= old.identity.epoch) {
                return Error(CreditErrorCode::kWrongIdentity);
            }
            if (!old.retired) {
                return Error(CreditErrorCode::kInUse);
            }
        }
    }
    if (state_->peer_slots == state_->config.peer_capacity ||
        state_->stats.elastic_free < state_->config.base_credits) {
        return Error(CreditErrorCode::kExhausted);
    }
    if (next_peer_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(CreditErrorCode::kSequenceExhausted);
    }
    const auto index = state_->peer_slots++;
    auto& peer = state_->peers[index];
    peer.id = ++next_peer_;
    peer.stats.identity = identity;
    peer.stats.base.free = state_->config.base_credits;
    state_->stats.elastic_free -= state_->config.base_credits;
    ++state_->stats.active_peers;
    return CreditPeerHandle{this, index, peer.id};
}
std::expected<std::uint64_t, CreditError> ReceiveCreditDomain::Grant(CreditPeerHandle handle, CreditClass kind,
                                                                     std::uint32_t count) noexcept {
    auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    if (!Valid(kind) || !count) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    auto& peer = state_->peers[*index].stats;
    if (state_->stats.frozen || peer.frozen || peer.retired) {
        return Error(CreditErrorCode::kStopped);
    }
    auto& maximum = kind == CreditClass::kData ? peer.max_data : peer.max_control;
    if (count > std::numeric_limits<std::uint64_t>::max() - maximum) {
        return Error(CreditErrorCode::kSequenceExhausted);
    }
    auto& pool = kind == CreditClass::kData ? state_->stats.data : state_->stats.control;
    if (kind == CreditClass::kData) {
        if (count > peer.base.free + state_->stats.elastic_free) {
            return Error(CreditErrorCode::kExhausted);
        }
        const auto base = std::min<std::uint64_t>(count, peer.base.free);
        peer.base.free -= base;
        peer.base.granted += base;
        state_->stats.elastic_free -= count - base;
        peer.elastic_granted += count - base;
    } else {
        if (count > pool.free) {
            return Error(CreditErrorCode::kExhausted);
        }
        peer.control_granted += count;
    }
    pool.free -= count;
    pool.granted += count;
    maximum += count;
    return maximum;
}
std::expected<std::uint64_t, CreditError>
ReceiveCreditDomain::PublishedControl(CreditPeerHandle handle) const noexcept {
    auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    return state_->peers[*index].published_control;
}
std::expected<void, CreditError> ReceiveCreditDomain::RecordPublishedControl(CreditPeerHandle handle,
                                                                             std::uint64_t limit) noexcept {
    auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& peer = state_->peers[*index];
    if (limit > peer.stats.max_control) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    peer.published_control = std::max(peer.published_control, limit);
    return {};
}
std::expected<RxCreditReceipt, CreditError> ReceiveCreditDomain::NewReceipt(std::uint32_t peer,
                                                                            std::uint8_t bucket) noexcept {
    if (!stats().physical_posted || state_->free_receipt == kEnd) {
        return Error(CreditErrorCode::kExhausted);
    }
    if (next_receipt_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(CreditErrorCode::kSequenceExhausted);
    }
    const auto index = state_->free_receipt;
    auto& record = state_->receipts[index];
    state_->free_receipt = record.next;
    record = {.id = ++next_receipt_, .peer = peer, .bucket = bucket, .active = true};
    ++state_->stats.held;
    return RxCreditReceipt{this, index, record.id};
}
std::expected<RxCreditReceipt, CreditError> ReceiveCreditDomain::Consume(CreditPeerHandle handle,
                                                                         CreditClass kind) noexcept {
    auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    if (!Valid(kind)) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    auto& peer = state_->peers[*index].stats;
    if (peer.retired) {
        return Error(CreditErrorCode::kStopped);
    }
    std::uint8_t bucket = kControl;
    if (kind == CreditClass::kData) {
        if (peer.base.granted) {
            bucket = kBase;
        } else if (peer.elastic_granted) {
            bucket = kElastic;
        } else {
            return Error(CreditErrorCode::kExhausted);
        }
    } else if (!peer.control_granted) {
        return Error(CreditErrorCode::kExhausted);
    }
    auto receipt = NewReceipt(*index, bucket);
    if (!receipt) {
        return receipt;
    }
    if (bucket == kBase) {
        --peer.base.granted;
        ++peer.base.unposted;
    } else if (bucket == kElastic) {
        --peer.elastic_granted;
    } else {
        --peer.control_granted;
    }
    ++peer.pending_rx;
    auto& pool = kind == CreditClass::kData ? state_->stats.data : state_->stats.control;
    --pool.granted;
    ++pool.unposted;
    return receipt;
}
std::expected<RxCreditReceipt, CreditError> ReceiveCreditDomain::ConsumeUnknown() noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    state_->stats.frozen = true;
    auto receipt = NewReceipt(kEnd, kUnknown);
    if (!receipt) {
        return receipt;
    }
    ++state_->stats.unattributed;
    return receipt;
}
std::expected<void, CreditError> ReceiveCreditDomain::Release(RxCreditReceipt receipt) noexcept {
    auto index = Find(receipt);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& record = state_->receipts[*index];
    if (record.released) {
        return Error(CreditErrorCode::kInvalidReceipt);
    }
    record.released = true;
    --state_->stats.held;
    ++state_->stats.released;
    return {};
}
std::expected<void, CreditError>
ReceiveCreditDomain::RecordRefillAccepted(std::span<const RxCreditReceipt> receipts) noexcept {
    if (!state_) {
        return Error(CreditErrorCode::kNotReady);
    }
    if (receipts.empty() || receipts.size() > kMaxRefillBatch) {
        return Error(CreditErrorCode::kInvalidArgument);
    }
    // 先核验整段 accepted 证据；checked 仅用于本轮去重，任何失败都恢复，计数不部分改变。
    std::array<State::Receipt*, kMaxRefillBatch> records{};
    std::size_t checked = 0;
    std::optional<CreditError> error;
    for (const auto receipt : receipts) {
        auto index = Find(receipt);
        if (!index) {
            error = index.error();
            break;
        }
        auto& record = state_->receipts[*index];
        if (!record.released || record.checked) {
            error = CreditError{CreditErrorCode::kInvalidReceipt};
            break;
        }
        if (record.bucket == kUnknown) {
            error = CreditError{CreditErrorCode::kRetained};
            break;
        }
        record.checked = true;
        records[checked++] = &record;
    }
    for (std::size_t i = 0; i < checked; ++i) {
        records[i]->checked = false;
    }
    if (error) {
        return std::unexpected(*error);
    }
    for (std::size_t i = 0; i < receipts.size(); ++i) {
        auto& record = *records[i];
        auto& peer = state_->peers[record.peer].stats;
        if (record.bucket == kBase) {
            --peer.base.unposted;
            ++peer.base.free;
        } else if (record.bucket == kElastic) {
            ++state_->stats.elastic_free;
        }
        auto& pool = record.bucket == kControl ? state_->stats.control : state_->stats.data;
        --pool.unposted;
        ++pool.free;
        --peer.pending_rx;
        --state_->stats.released;
        record.active = false;
        record.next = state_->free_receipt;
        state_->free_receipt = receipts[i].slot;
    }
    return {};
}
void ReceiveCreditDomain::Freeze() noexcept {
    if (state_) {
        state_->stats.frozen = true;
    }
}
std::expected<void, CreditError> ReceiveCreditDomain::FreezePeer(CreditPeerHandle handle) noexcept {
    auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    state_->peers[*index].stats.frozen = true;
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::BindWithdrawal(const control::PeerCreditControl& owner,
                                                                     CreditPeerHandle handle) noexcept {
    const auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& peer = state_->peers[*index];
    if (peer.withdrawal_owner) {
        return Error(CreditErrorCode::kInUse);
    }
    if (peer.stats.retired) {
        return Error(CreditErrorCode::kStopped);
    }
    peer.withdrawal_owner = &owner;
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::UnbindWithdrawal(const control::PeerCreditControl& owner,
                                                                       CreditPeerHandle handle) noexcept {
    const auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& peer = state_->peers[*index];
    if (peer.withdrawal_owner != &owner) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    peer.withdrawal_owner = nullptr;
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::RetireWithdrawn(const control::PeerCreditControl& owner,
                                                                      CreditPeerHandle handle,
                                                                      std::array<std::uint64_t, 2> spent) noexcept {
    const auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    const auto& record = state_->peers[*index];
    const auto& peer = record.stats;
    if (record.withdrawal_owner != &owner) {
        return Error(CreditErrorCode::kWrongIdentity);
    }
    if (peer.retired || !peer.frozen) {
        return Error(CreditErrorCode::kProofRequired);
    }
    if (state_->stats.unattributed) {
        return Error(CreditErrorCode::kRetained);
    }
    const std::array<std::uint64_t, 2> issued{peer.max_data, peer.max_control};
    const std::array<std::uint64_t, 2> consumed{peer.max_data - peer.base.granted - peer.elastic_granted,
                                                peer.max_control - peer.control_granted};
    // 只接受停止后已确定的累计接受量；数据和控制分别核对，不能用总数掩盖类别错账。
    // 少收的 CQE 仍可能迟到，Held/未补投 receipt 仍占真实接收责任，必须继续推进等待。
    for (std::size_t i = 0; i < spent.size(); ++i) {
        if (spent[i] > issued[i] || consumed[i] > spent[i]) {
            return Error(CreditErrorCode::kWrongIdentity);
        }
    }
    if (consumed != spent || peer.pending_rx) {
        return Error(CreditErrorCode::kInUse);
    }
    FinishRetire(*index);
    return {};
}
std::expected<void, CreditError> ReceiveCreditDomain::RetirePeer(CreditPeerHandle handle,
                                                                 CreditRetireProof proof) noexcept {
    auto index = Find(handle);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& record = state_->peers[*index];
    if (record.withdrawal_owner) {
        return Error(CreditErrorCode::kInUse);
    }
    auto& peer = record.stats;
    if (peer.retired) {
        return {};
    }
    peer.frozen = true;
    if (!proof.sender_stopped || !proof.accepted_set_settled || !proof.unused_grants_revoked) {
        return Error(CreditErrorCode::kProofRequired);
    }
    if (state_->stats.unattributed) {
        return Error(CreditErrorCode::kRetained);
    }
    if (peer.pending_rx) {
        return Error(CreditErrorCode::kInUse);
    }
    FinishRetire(*index);
    return {};
}
void ReceiveCreditDomain::FinishRetire(std::uint32_t index) noexcept {
    auto& peer = state_->peers[index].stats;
    const auto unused_data = peer.base.granted + peer.elastic_granted;
    state_->stats.data.granted -= unused_data;
    state_->stats.data.free += unused_data;
    state_->stats.elastic_free += peer.base.free + unused_data;
    state_->stats.control.granted -= peer.control_granted;
    state_->stats.control.free += peer.control_granted;
    peer.base = {};
    peer.elastic_granted = peer.control_granted = 0;
    peer.retired = true;
    --state_->stats.active_peers;
    ++state_->stats.tombstones;
}

} // namespace connection
} // namespace kbsocket
