// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/grant_manager.hpp"

#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace connection {
namespace {
enum class Phase : std::uint8_t { kEmpty, kReserved, kPublished, kReady, kBorrowed, kQuarantined, kRetired };

std::unexpected<GrantError> Error(GrantErrorCode code) noexcept {
    return std::unexpected(GrantError{code, std::nullopt});
}
std::unexpected<GrantError> MemoryError(const memory::MemoryError& error) noexcept {
    return std::unexpected(GrantError{GrantErrorCode::kMemory, error});
}
bool Active(Phase phase) noexcept {
    return phase != Phase::kEmpty && phase != Phase::kRetired;
}
} // namespace

struct GrantManager::Entry {
    GrantIdentity identity;
    memory::BufferLease lease;
    memory::BufferReference reference;
    Phase phase = Phase::kEmpty;
    bool remote = false;
    bool cpu = false;
    bool committed = false;
    bool discarding = false;
    // 准入中途失败不得丢失清理进度；原请求重试时先完成尚未发布的回滚。
    bool rollback = false;
};
struct GrantManager::State {
    GrantBinding binding;
    GrantArena arena;
    memory::SlabAllocator* slab = nullptr;
    urma_context_t* context = nullptr;
    GrantConfig config;
    std::unique_ptr<Entry[]> entries;
    std::uint64_t window_begin = 0;
    std::uint64_t bytes = 0;
    std::size_t active = 0;
    bool stopped = false;
};

GrantManager::GrantManager() noexcept = default;
GrantManager::~GrantManager() {
    if (!Close()) {
        // 账本保留并不延长借用依赖的 C++ 生命周期；宿主必须同时保留 Slab/provider/budget。
        (void)state_.release();
    }
}
std::expected<void, GrantError> GrantManager::Open(GrantBinding binding, GrantArena arena, memory::SlabAllocator& slab,
                                                   urma_context_t* context, const GrantConfig& config) noexcept {
    if (opened_) {
        return Error(GrantErrorCode::kInUse);
    }
    if (!binding.sender_runtime || !binding.receiver_runtime || !binding.receive_domain || !binding.session ||
        !binding.token.generation || !arena.id || !arena.generation || !context || !config.window ||
        config.window > kMaxWindow || !config.max_active || config.max_active > config.window || !config.max_bytes) {
        return Error(GrantErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(GrantErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) Entry[config.window]);
    if (!state->entries) {
        return Error(GrantErrorCode::kNoMemory);
    }
    state->binding = binding;
    state->arena = arena;
    state->slab = &slab;
    state->context = context;
    state->config = config;
    state->window_begin = config.initial_seq;
    state_ = std::move(state);
    opened_ = true;
    return {};
}
std::expected<GrantReservation, GrantError> GrantManager::Reserve(const GrantTransfer& transfer) noexcept {
    if (!state_) {
        return Error(GrantErrorCode::kNotReady);
    }
    if (state_->stopped) {
        return Error(GrantErrorCode::kStopped);
    }
    if (transfer.binding != state_->binding) {
        return Error(GrantErrorCode::kWrongBinding);
    }
    if (!transfer.transfer_id || !transfer.bytes || transfer.bytes > std::numeric_limits<std::size_t>::max() ||
        transfer.bytes > std::numeric_limits<std::uint32_t>::max() ||
        transfer.seq == std::numeric_limits<std::uint32_t>::max()) {
        return Error(GrantErrorCode::kInvalidArgument);
    }
    if (transfer.seq < state_->window_begin) {
        return Error(GrantErrorCode::kStaleSequence);
    }
    if (transfer.seq - state_->window_begin >= state_->config.window) {
        return Error(GrantErrorCode::kWindowFull);
    }
    const auto slot = transfer.seq % state_->config.window;
    auto& entry = state_->entries[slot];
    if (entry.rollback && entry.identity.transfer == transfer) {
        if (auto result = Discard(entry); !result) {
            return std::unexpected(result.error());
        }
        entry = {};
    }
    if (entry.phase != Phase::kEmpty) {
        return Error(GrantErrorCode::kDuplicate);
    }
    if (state_->active >= state_->config.max_active || transfer.bytes > state_->config.max_bytes - state_->bytes) {
        return Error(GrantErrorCode::kWindowFull);
    }
    if (next_generation_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(GrantErrorCode::kExhausted);
    }
    auto lease = state_->slab->Acquire(static_cast<std::size_t>(transfer.bytes));
    if (!lease) {
        return MemoryError(lease.error());
    }
    entry.identity = {transfer, state_->arena, slot, ++next_generation_, 0};
    entry.lease = *lease;
    entry.phase = Phase::kReserved;
    entry.cpu = true;
    ++state_->active;
    state_->bytes += transfer.bytes;
    auto reference = state_->slab->Reserve(*lease, memory::BufferUse::kRemoteGrant);
    if (!reference) {
        entry.rollback = true;
        if (auto cleanup = Discard(entry); !cleanup) {
            return std::unexpected(cleanup.error());
        }
        entry = {};
        return MemoryError(reference.error());
    }
    entry.reference = *reference;
    entry.remote = true;
    auto target = state_->slab->Sge(*reference, state_->context, 0, static_cast<std::size_t>(transfer.bytes));
    if (!target) {
        entry.rollback = true;
        if (auto cleanup = Discard(entry); !cleanup) {
            return std::unexpected(cleanup.error());
        }
        entry = {};
        return MemoryError(target.error());
    }
    entry.identity.target_address = target->addr;
    return GrantReservation{{this, slot, entry.identity.generation}, entry.identity, *target};
}
std::expected<GrantManager::Entry*, GrantError> GrantManager::Find(GrantHandle handle) noexcept {
    if (!state_) {
        return Error(GrantErrorCode::kNotReady);
    }
    if (handle.owner != this || !handle.generation || handle.slot >= state_->config.window) {
        return Error(GrantErrorCode::kInvalidHandle);
    }
    auto& entry = state_->entries[handle.slot];
    if (!Active(entry.phase) || entry.identity.generation != handle.generation || entry.rollback) {
        return Error(GrantErrorCode::kInvalidHandle);
    }
    return &entry;
}
std::expected<GrantManager::Entry*, GrantError> GrantManager::Find(const GrantIdentity& identity) noexcept {
    auto entry = Find({this, identity.slot, identity.generation});
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->identity != identity) {
        return Error(GrantErrorCode::kIdentityMismatch);
    }
    return *entry;
}
std::expected<void, GrantError> GrantManager::ValidatePublication(GrantHandle handle,
                                                                  const GrantIdentity& identity) noexcept {
    auto entry = Find(handle);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->identity != identity) {
        return Error(GrantErrorCode::kIdentityMismatch);
    }
    if ((*entry)->phase != Phase::kReserved || (*entry)->discarding) {
        return Error(GrantErrorCode::kInvalidState);
    }
    return {};
}
std::expected<void, GrantError> GrantManager::CommitPublication(GrantHandle handle) noexcept {
    auto entry = Find(handle);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->phase != Phase::kReserved || (*entry)->discarding) {
        return Error(GrantErrorCode::kInvalidState);
    }
    if (auto result = state_->slab->Commit((*entry)->reference); !result) {
        return MemoryError(result.error());
    }
    (*entry)->committed = true;
    (*entry)->phase = Phase::kPublished;
    return {};
}
std::expected<void, GrantError> GrantManager::EndRemote(Entry& entry) noexcept {
    if (!entry.remote) {
        return {};
    }
    auto result = entry.committed ? state_->slab->EndGrant(entry.reference) : state_->slab->Cancel(entry.reference);
    if (!result) {
        return MemoryError(result.error());
    }
    entry.remote = false;
    return {};
}
void GrantManager::Retire(Entry& entry) noexcept {
    --state_->active;
    state_->bytes -= entry.identity.transfer.bytes;
    entry.phase = Phase::kRetired;
}
std::expected<void, GrantError> GrantManager::Discard(Entry& entry) noexcept {
    // 调用方必须已知未发布，或先完成可信远端终结；本函数不擅自解除 Committed 授权。
    entry.discarding = true;
    if (entry.remote) {
        if (entry.committed) {
            return Error(GrantErrorCode::kInUse);
        }
        if (auto result = EndRemote(entry); !result) {
            return result;
        }
    }
    if (entry.cpu) {
        if (auto result = state_->slab->ReleaseCpu(entry.lease); !result) {
            return MemoryError(result.error());
        }
        entry.cpu = false;
    }
    Retire(entry);
    return {};
}
std::expected<void, GrantError> GrantManager::Cancel(GrantHandle handle) noexcept {
    auto entry = Find(handle);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    switch ((*entry)->phase) {
    case Phase::kReserved:
    case Phase::kReady:
        return Discard(**entry);
    case Phase::kPublished:
        // 先记录取消意图；即使隔离标记失败，后续 DONE 也只能终结并丢弃。
        (*entry)->discarding = true;
        if (auto result = state_->slab->Quarantine((*entry)->lease); !result) {
            return MemoryError(result.error());
        }
        (*entry)->phase = Phase::kQuarantined;
        return {};
    case Phase::kQuarantined:
        return {};
    case Phase::kBorrowed:
        return Error(GrantErrorCode::kInUse);
    default:
        return Error(GrantErrorCode::kInvalidState);
    }
}
std::expected<void, GrantError> GrantManager::ValidateEvidence(const GrantTerminalEvidence& evidence,
                                                               bool success) const noexcept {
    if (!evidence.no_future_writes || !evidence.acceptance_known ||
        evidence.accepted_writes != evidence.terminal_writes || evidence.successful_writes > evidence.terminal_writes ||
        evidence.completed_bytes > evidence.identity.transfer.bytes ||
        (!evidence.successful_writes && evidence.completed_bytes)) {
        return Error(GrantErrorCode::kInvalidEvidence);
    }
    if (success && (!evidence.accepted_writes || evidence.successful_writes != evidence.accepted_writes ||
                    evidence.completed_bytes != evidence.identity.transfer.bytes)) {
        return Error(GrantErrorCode::kInvalidEvidence);
    }
    return {};
}
std::expected<void, GrantError> GrantManager::CompleteDone(const GrantTerminalEvidence& evidence) noexcept {
    auto entry = Find(evidence.identity);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->phase != Phase::kPublished && (*entry)->phase != Phase::kQuarantined) {
        return Error(GrantErrorCode::kInvalidState);
    }
    if (auto valid = ValidateEvidence(evidence, true); !valid) {
        return valid;
    }
    if (auto result = EndRemote(**entry); !result) {
        return result;
    }
    if ((*entry)->phase == Phase::kQuarantined || (*entry)->discarding) {
        return Discard(**entry);
    }
    (*entry)->phase = Phase::kReady;
    return {};
}
std::expected<void, GrantError> GrantManager::ConfirmTerminal(const GrantTerminalEvidence& evidence) noexcept {
    auto entry = Find(evidence.identity);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->phase == Phase::kBorrowed) {
        return Error(GrantErrorCode::kInUse);
    }
    if ((*entry)->phase != Phase::kPublished && (*entry)->phase != Phase::kQuarantined &&
        (*entry)->phase != Phase::kReady) {
        return Error(GrantErrorCode::kInvalidState);
    }
    if (auto valid = ValidateEvidence(evidence, false); !valid) {
        return valid;
    }
    // 一旦采用清理证明，即使后续清理失败，成功 DONE 也不得重新使它可交付。
    (*entry)->discarding = true;
    if (auto result = EndRemote(**entry); !result) {
        return result;
    }
    return Discard(**entry);
}
std::expected<std::span<const std::byte>, GrantError> GrantManager::Borrow(GrantHandle handle) noexcept {
    auto entry = Find(handle);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->phase == Phase::kBorrowed) {
        return Error(GrantErrorCode::kInUse);
    }
    if ((*entry)->phase != Phase::kReady || (*entry)->discarding) {
        return Error(GrantErrorCode::kInvalidState);
    }
    auto view = state_->slab->Readable((*entry)->lease);
    if (!view) {
        return MemoryError(view.error());
    }
    (*entry)->phase = Phase::kBorrowed;
    return *view;
}
std::expected<void, GrantError> GrantManager::Return(GrantHandle handle) noexcept {
    auto entry = Find(handle);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->phase != Phase::kBorrowed) {
        return Error(GrantErrorCode::kInvalidState);
    }
    return Discard(**entry);
}
std::expected<void, GrantError> GrantManager::AdvanceWindow(std::uint64_t consumed_prefix) noexcept {
    if (!state_) {
        return Error(GrantErrorCode::kNotReady);
    }
    constexpr auto sequence_end = std::uint64_t{1} << 32;
    if (consumed_prefix < state_->window_begin || consumed_prefix > sequence_end ||
        consumed_prefix - state_->window_begin > state_->config.window) {
        return Error(GrantErrorCode::kInvalidArgument);
    }
    // 先完整检查再清槽，避免在后半段遇活跃授权时已丢掉前半段的防重放记录。
    for (auto seq = state_->window_begin; seq < consumed_prefix; ++seq) {
        if (Active(state_->entries[seq % state_->config.window].phase)) {
            return Error(GrantErrorCode::kInUse);
        }
    }
    for (auto seq = state_->window_begin; seq < consumed_prefix; ++seq) {
        state_->entries[seq % state_->config.window] = {};
    }
    state_->window_begin = consumed_prefix;
    return {};
}
void GrantManager::Stop() noexcept {
    if (state_) {
        state_->stopped = true;
    }
}
std::expected<void, GrantError> GrantManager::Close() noexcept {
    if (!state_) {
        return {};
    }
    Stop();
    // Reserve 未返回授权时的内部回滚也允许在控制路径重试；绝不清理已交给调用方的活跃授权。
    for (std::uint32_t i = 0; i < state_->config.window; ++i) {
        auto& entry = state_->entries[i];
        if (entry.rollback && Active(entry.phase)) {
            if (auto result = Discard(entry); !result) {
                return result;
            }
        }
    }
    if (state_->active) {
        return Error(GrantErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
GrantStats GrantManager::stats() const noexcept {
    if (!state_) {
        return {};
    }
    GrantStats stats;
    stats.window_begin = state_->window_begin;
    stats.bytes = state_->bytes;
    stats.active = state_->active;
    for (std::uint32_t i = 0; i < state_->config.window; ++i) {
        switch (state_->entries[i].phase) {
        case Phase::kReserved:
            ++stats.reserved;
            break;
        case Phase::kPublished:
            ++stats.published;
            break;
        case Phase::kReady:
            ++stats.ready;
            break;
        case Phase::kBorrowed:
            ++stats.borrowed;
            break;
        case Phase::kQuarantined:
            ++stats.quarantined;
            break;
        case Phase::kRetired:
            ++stats.retired;
            break;
        case Phase::kEmpty:
            break;
        }
    }
    return stats;
}
bool GrantManager::accepting() const noexcept {
    return state_ && !state_->stopped;
}
} // namespace connection
} // namespace kbsocket
