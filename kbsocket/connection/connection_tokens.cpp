// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/connection_tokens.hpp"

#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace connection {
std::expected<void, TokenError> ConnectionTokens::Open(TokenDomain domain, std::uint32_t slots,
                                                       std::uint32_t initial_generation) noexcept {
    if (opened_once_) {
        return std::unexpected(TokenError::kInUse);
    }
    if (!domain.runtime_instance || !domain.receive_domain || !slots || slots > kMaxSlots || !initial_generation) {
        return std::unexpected(TokenError::kInvalidArgument);
    }
    auto entries = std::unique_ptr<Entry[]>(new (std::nothrow) Entry[slots]);
    if (!entries) {
        return std::unexpected(TokenError::kNoMemory);
    }
    for (std::uint32_t i = 0; i < slots; ++i) {
        entries[i] = {initial_generation, i + 1, false};
    }
    entries_ = std::move(entries);
    count_ = slots;
    domain_ = domain;
    opened_once_ = true;
    return {};
}
std::expected<ConnectionTokenLease, TokenError> ConnectionTokens::Acquire() noexcept {
    if (!entries_) {
        return std::unexpected(TokenError::kNotReady);
    }
    if (stopped_) {
        return std::unexpected(TokenError::kStopped);
    }
    if (free_ == count_) {
        return std::unexpected(TokenError::kExhausted);
    }
    const auto slot = free_;
    auto& entry = entries_[slot];
    free_ = entry.next;
    entry.active = true;
    ++active_;
    return ConnectionTokenLease{this, WireToken{slot, entry.generation}};
}
bool ConnectionTokens::Contains(WireToken token) const noexcept {
    return entries_ && token.slot < count_ && entries_[token.slot].active &&
           entries_[token.slot].generation == token.generation;
}
std::expected<void, TokenError> ConnectionTokens::Retire(ConnectionTokenLease lease) noexcept {
    if (lease.owner != this || !Contains(lease.token)) {
        return std::unexpected(TokenError::kInvalidToken);
    }
    auto& entry = entries_[lease.token.slot];
    entry.active = false;
    --active_;
    if (entry.generation != std::numeric_limits<std::uint32_t>::max()) {
        ++entry.generation;
        entry.next = free_;
        free_ = lease.token.slot;
    }
    // 完整代次耗尽的槽永久退出 free list，不能退回短 token 或复用最大代次。
    return {};
}
std::expected<void, TokenError> ConnectionTokens::Close() noexcept {
    if (!opened_once_) {
        return {};
    }
    StopAccepting();
    if (active_) {
        return std::unexpected(TokenError::kInUse);
    }
    entries_.reset();
    count_ = free_ = 0;
    return {};
}
} // namespace connection
} // namespace kbsocket
