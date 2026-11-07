// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/connection_table.hpp"

#include <new>
#include <utility>

namespace kbsocket {
namespace connection {
namespace {
ConnectionTableError Convert(TokenError error) noexcept {
    switch (error) {
    case TokenError::kInvalidArgument:
        return ConnectionTableError::kInvalidArgument;
    case TokenError::kNotReady:
        return ConnectionTableError::kNotReady;
    case TokenError::kInUse:
        return ConnectionTableError::kInUse;
    case TokenError::kNoMemory:
        return ConnectionTableError::kNoMemory;
    case TokenError::kExhausted:
        return ConnectionTableError::kExhausted;
    case TokenError::kInvalidToken:
        return ConnectionTableError::kInvalidLease;
    case TokenError::kStopped:
        return ConnectionTableError::kStopped;
    }
    return ConnectionTableError::kInvalidArgument;
}
} // namespace

std::expected<void, ConnectionTableError> ConnectionTable::Open(TokenDomain local_domain, std::uint32_t slots,
                                                                std::uint32_t initial_generation) noexcept {
    if (opened_once_) {
        return std::unexpected(ConnectionTableError::kInUse);
    }
    if (!local_domain.runtime_instance || !local_domain.receive_domain || !slots || slots > kMaxSlots ||
        !initial_generation) {
        return std::unexpected(ConnectionTableError::kInvalidArgument);
    }
    auto entries = std::unique_ptr<Entry[]>(new (std::nothrow) Entry[slots]);
    if (!entries) {
        return std::unexpected(ConnectionTableError::kNoMemory);
    }
    if (auto result = tokens_.Open(local_domain, slots, initial_generation); !result) {
        return std::unexpected(Convert(result.error()));
    }
    entries_ = std::move(entries);
    opened_once_ = true;
    return {};
}

std::expected<ConnectionRouteLease, ConnectionTableError> ConnectionTable::Reserve() noexcept {
    auto acquired = tokens_.Acquire();
    if (!acquired) {
        return std::unexpected(Convert(acquired.error()));
    }
    entries_[acquired->token.slot] = {};
    return ConnectionRouteLease{this, acquired->token};
}

bool ConnectionTable::Contains(ConnectionRouteLease lease) const noexcept {
    return entries_ && lease.owner == this && tokens_.Contains(lease.token);
}

std::expected<void, ConnectionTableError>
ConnectionTable::Publish(ConnectionRouteLease lease, const WireSessionBinding& binding, void* connection) noexcept {
    if (!Contains(lease)) {
        return std::unexpected(ConnectionTableError::kInvalidLease);
    }
    if (stopped_) {
        return std::unexpected(ConnectionTableError::kStopped);
    }
    auto& entry = entries_[lease.token.slot];
    if (entry.state != State::kReserved) {
        return std::unexpected(ConnectionTableError::kInUse);
    }
    if (binding.context.version != kWireVersion) {
        return std::unexpected(ConnectionTableError::kVersionMismatch);
    }
    if (!connection || !binding.context.peer_runtime || !binding.context.session || !binding.credit_epoch ||
        binding.context.receive_domain != domain().receive_domain || binding.token != lease.token) {
        return std::unexpected(ConnectionTableError::kInvalidArgument);
    }
    entry.binding = binding;
    entry.connection = connection;
    entry.state = State::kPublished;
    ++published_;
    return {};
}

std::expected<ConnectionRoute, ConnectionTableError>
ConnectionTable::Resolve(const WireFrameView& decoded, TokenDomain observed_local_domain,
                         const WireReceiveContext& observed_peer) const noexcept {
    if (!entries_) {
        return std::unexpected(ConnectionTableError::kNotReady);
    }
    if (observed_local_domain.runtime_instance != domain().runtime_instance ||
        observed_local_domain.receive_domain != domain().receive_domain) {
        return std::unexpected(ConnectionTableError::kIdentityMismatch);
    }
    const auto token = decoded.frame.token;
    if (!tokens_.Contains(token)) {
        return std::unexpected(ConnectionTableError::kInvalidLease);
    }
    const auto& entry = entries_[token.slot];
    if (entry.state != State::kPublished) {
        return std::unexpected(ConnectionTableError::kNotPublished);
    }
    if (auto result = ValidateWireIdentity(decoded, entry.binding, observed_peer); !result) {
        return std::unexpected(result.error().code == WireErrorCode::kVersionMismatch
                                   ? ConnectionTableError::kVersionMismatch
                                   : ConnectionTableError::kIdentityMismatch);
    }
    return ConnectionRoute{{this, token}, entry.binding, entry.connection};
}

std::expected<void, ConnectionTableError> ConnectionTable::Withdraw(ConnectionRouteLease lease) noexcept {
    if (!Contains(lease)) {
        return std::unexpected(ConnectionTableError::kInvalidLease);
    }
    auto& entry = entries_[lease.token.slot];
    if (entry.state == State::kPublished) {
        --published_;
    }
    entry = {};
    entry.state = State::kWithdrawn;
    return {};
}

std::expected<void, ConnectionTableError> ConnectionTable::Retire(ConnectionRouteLease lease) noexcept {
    if (!Contains(lease)) {
        return std::unexpected(ConnectionTableError::kInvalidLease);
    }
    // token 校验/退休先成功才清除路由；旧 lease 不能撤回已复用槽上的新连接。
    auto result = tokens_.Retire({&tokens_, lease.token});
    if (!result) {
        return std::unexpected(Convert(result.error()));
    }
    auto& entry = entries_[lease.token.slot];
    if (entry.state == State::kPublished) {
        --published_;
    }
    entry = {};
    return {};
}

void ConnectionTable::StopAccepting() noexcept {
    stopped_ = true;
    tokens_.StopAccepting();
}

std::expected<void, ConnectionTableError> ConnectionTable::Close() noexcept {
    if (!opened_once_) {
        return {};
    }
    StopAccepting();
    if (auto result = tokens_.Close(); !result) {
        return std::unexpected(Convert(result.error()));
    }
    entries_.reset();
    return {};
}

} // namespace connection
} // namespace kbsocket
