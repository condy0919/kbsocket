// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/tseg_import_cache.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace control {
namespace {
auto Error(TsegImportCacheErrorCode code, int provider = 0) noexcept {
    return std::unexpected(TsegImportCacheError{code, provider});
}

bool SameDescriptor(const urma_seg_t& left, const urma_seg_t& right) noexcept {
    // urma_seg_t 含 ABI padding；只比较语义字段，不能把填充字节当作 Slab 身份。
    return !std::memcmp(left.ubva.eid.raw, right.ubva.eid.raw, URMA_EID_SIZE) && left.ubva.uasid == right.ubva.uasid &&
           left.ubva.va == right.ubva.va && left.len == right.len && left.attr.value == right.attr.value &&
           left.token_id == right.token_id;
}
std::expected<void, TsegImportCacheError> Validate(const TsegImportRequest& request) noexcept {
    const auto& key = request.key;
    if (!key.local_context || !key.local_context_incarnation || !key.peer_runtime || !key.receive_domain ||
        !key.slab_id || !key.slab_generation || !key.authorization_identity ||
        std::all_of(key.eid.begin(), key.eid.end(), [](auto byte) { return byte == 0; })) {
        return Error(TsegImportCacheErrorCode::kInvalidArgument);
    }
    const auto& descriptor = request.descriptor;
    constexpr auto allowed_access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    if (!descriptor.ubva.va || !descriptor.len ||
        descriptor.len > std::numeric_limits<std::uint64_t>::max() - descriptor.ubva.va ||
        descriptor.ubva.uasid >= (1U << 24) || descriptor.token_id == URMA_SEG_TOKEN_ID_INVALID ||
        std::memcmp(descriptor.ubva.eid.raw, key.eid.data(), key.eid.size()) ||
        descriptor.attr.bs.token_policy > URMA_TOKEN_PLAIN_TEXT ||
        (descriptor.attr.bs.token_policy == URMA_TOKEN_NONE && request.token.token != 0) ||
        !(descriptor.attr.bs.access & URMA_ACCESS_WRITE) || (descriptor.attr.bs.access & ~allowed_access) ||
        descriptor.attr.bs.dsva || descriptor.attr.bs.non_pin || descriptor.attr.bs.user_iova ||
        descriptor.attr.bs.has_user_info || descriptor.attr.bs.reserved) {
        return Error(TsegImportCacheErrorCode::kInvalidDescriptor);
    }
    return {};
}
} // namespace

struct TsegImportCache::State {
    enum class Phase : std::uint8_t {
        kFree,
        kPending,
        kPreparedImport,
        kImporting,
        kReady,
        kFailed,
        kPreparedUnimport,
        kUnimporting
    };
    struct Work {
        TsegImportOperationKind kind = TsegImportOperationKind::kImport;
        urma_context_t* context = nullptr;
        urma_token_t token{};
        urma_seg_t descriptor{};
        urma_target_seg_t* target = nullptr;
        bool unimport_safe = true;
    };
    struct Entry {
        TsegImportKey key;
        urma_token_t token{};
        urma_seg_t descriptor{};
        std::uint64_t generation = 0;
        std::size_t references = 0;
        Phase phase = Phase::kFree;
        bool invalidated = false;
        urma_target_seg_t* target = nullptr;
        bool unimport_safe = true;
        int provider_error = 0;
        runtime::ControlTaskId task;
        std::uint64_t reservation = 0;
        Phase prepared_from = Phase::kFree;
        Work work;
    };
    struct Reference {
        std::uint64_t sequence = 0;
        std::uint32_t entry = 0;
        bool active = false;
    };

    Entry* Lookup(TsegImportLease lease, const TsegImportCache* owner) noexcept {
        if (lease.owner != owner || !lease.sequence || lease.slot >= config.leases) {
            return nullptr;
        }
        const auto& reference = references[lease.slot];
        return reference.active && reference.sequence == lease.sequence ? &entries[reference.entry] : nullptr;
    }
    bool Busy(urma_context_t* context) const noexcept {
        // 同 context 最多一个在途任务；只查实际控制并发量，避免 NextOperation 嵌套全表扫描。
        for (std::size_t i = 0; i < outstanding; ++i) {
            if (busy_contexts[i] == context) {
                return true;
            }
        }
        return false;
    }
    void CompleteContext(urma_context_t* context) noexcept {
        for (std::size_t i = 0; i < outstanding; ++i) {
            if (busy_contexts[i] == context) {
                busy_contexts[i] = busy_contexts[--outstanding];
                busy_contexts[outstanding] = nullptr;
                return;
            }
        }
    }
    std::optional<TsegImportOperationKind> Operation(const Entry& entry) const noexcept {
        if (entry.phase == Phase::kPending && !entry.invalidated && entry.references) {
            return TsegImportOperationKind::kImport;
        }
        if (!entry.references && entry.target && entry.unimport_safe &&
            ((entry.phase == Phase::kReady && entry.invalidated) || entry.phase == Phase::kFailed)) {
            return TsegImportOperationKind::kUnimport;
        }
        return std::nullopt;
    }
    void FreeIfUnused(Entry& entry) noexcept {
        if (entry.references || entry.target || entry.phase == Phase::kImporting ||
            entry.phase == Phase::kUnimporting || entry.phase == Phase::kPreparedImport ||
            entry.phase == Phase::kPreparedUnimport || entry.phase == Phase::kFree) {
            return;
        }
        entry.phase = Phase::kFree;
        --entry_count;
    }
    std::expected<TsegImportLease, TsegImportCacheError> NewReference(Entry& entry, const TsegImportCache* owner,
                                                                      std::uint64_t& sequence) noexcept {
        if (sequence == std::numeric_limits<std::uint64_t>::max()) {
            return Error(TsegImportCacheErrorCode::kSequenceExhausted);
        }
        if (lease_count == config.leases) {
            return Error(TsegImportCacheErrorCode::kResourceExhausted);
        }
        for (std::uint32_t i = 0; i < config.leases; ++i) {
            const auto slot = next_reference;
            next_reference = (slot + 1) % config.leases;
            auto& reference = references[slot];
            if (reference.active) {
                continue;
            }
            reference = {
                .sequence = ++sequence, .entry = static_cast<std::uint32_t>(&entry - entries.get()), .active = true};
            ++entry.references;
            ++lease_count;
            return TsegImportLease{owner, slot, sequence};
        }
        return Error(TsegImportCacheErrorCode::kResourceExhausted);
    }
    static runtime::ControlTaskResult Run(void* argument, std::uint64_t) noexcept {
        auto& work = *static_cast<Work*>(argument);
        if (work.kind == TsegImportOperationKind::kImport) {
            errno = 0;
            // 原始快照只属于 owner；provider 可修改入参，worker 使用独立副本。
            auto descriptor = work.descriptor;
            auto token = work.token;
            urma_import_seg_flag_t flag{};
            flag.bs.access = URMA_ACCESS_WRITE;
            flag.bs.mapping = URMA_SEG_NOMAP;
            flag.bs.cacheable = URMA_NON_CACHEABLE;
            work.target = raw::UrmaApi::ImportSeg(work.context, &descriptor, &token, 0, flag);
            if (!work.target) {
                return std::unexpected(runtime::ControlTaskError{errno ? errno : EIO});
            }
            work.unimport_safe = work.target->urma_ctx == work.context;
            // 不发布与已认证描述不符的句柄；非空输出仍须留待异步 unimport，不能在 owner 提前销毁。
            // 错 context 的句柄没有可证明安全的暂停域，只能保留，不能借当前 context 屏障销毁。
            if (!work.unimport_safe || !SameDescriptor(work.target->seg, work.descriptor)) {
                return std::unexpected(runtime::ControlTaskError{EPROTO});
            }
        } else {
            const auto status = raw::UrmaApi::UnimportSeg(work.target);
            if (status != URMA_SUCCESS) {
                return std::unexpected(runtime::ControlTaskError{status});
            }
            work.target = nullptr;
        }
        return {};
    }

    TsegImportCacheConfig config;
    std::unique_ptr<Entry[]> entries;
    std::unique_ptr<Reference[]> references;
    std::unique_ptr<urma_context_t*[]> busy_contexts;
    std::size_t entry_count = 0, lease_count = 0, outstanding = 0;
    std::uint32_t next_reference = 0, next_operation = 0;
    bool stopped = false;
};

TsegImportCache::TsegImportCache() noexcept = default;
TsegImportCache::~TsegImportCache() {
    if (!Close()) {
        // provider 工作参数、已导入句柄及未归还引用仍依赖该存储，不能析构后制造 UAF。
        (void)state_.release();
    }
}

std::expected<void, TsegImportCacheError> TsegImportCache::Open(const TsegImportCacheConfig& config) noexcept {
    if (state_ || opened_once_) {
        return Error(TsegImportCacheErrorCode::kInUse);
    }
    if (!config.entries || config.entries > kMaxEntries || !config.leases || config.leases > kMaxLeases ||
        !config.in_flight || config.in_flight > config.entries) {
        return Error(TsegImportCacheErrorCode::kInvalidArgument);
    }
    try {
        auto state = std::make_unique<State>();
        state->entries = std::make_unique<State::Entry[]>(config.entries);
        state->references = std::make_unique<State::Reference[]>(config.leases);
        state->busy_contexts = std::make_unique<urma_context_t*[]>(config.in_flight);
        state->config = config;
        state_ = std::move(state);
    } catch (const std::bad_alloc&) {
        return Error(TsegImportCacheErrorCode::kNoMemory);
    }
    opened_once_ = true;
    return {};
}

std::expected<TsegImportLease, TsegImportCacheError>
TsegImportCache::Acquire(const TsegImportRequest& request) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped) {
        return Error(TsegImportCacheErrorCode::kStopped);
    }
    if (auto result = Validate(request); !result) {
        return std::unexpected(result.error());
    }
    State::Entry* free = nullptr;
    for (std::uint32_t i = 0; i < state.config.entries; ++i) {
        auto& entry = state.entries[i];
        if (entry.phase == State::Phase::kFree) {
            if (!free) {
                free = &entry;
            }
            continue;
        }
        if (entry.key != request.key) {
            continue;
        }
        // 同一身份不能悄悄替换 descriptor/token，哪怕旧导入已经失败或等待清理。
        if (entry.token.token != request.token.token || !SameDescriptor(entry.descriptor, request.descriptor)) {
            return Error(TsegImportCacheErrorCode::kDescriptorConflict);
        }
        if (entry.invalidated || entry.phase == State::Phase::kUnimporting ||
            entry.phase == State::Phase::kPreparedUnimport) {
            return Error(TsegImportCacheErrorCode::kInvalidated);
        }
        if (entry.phase == State::Phase::kFailed) {
            return Error(TsegImportCacheErrorCode::kProviderError, entry.provider_error);
        }
        return state.NewReference(entry, this, next_lease_);
    }
    if (!free || state.lease_count == state.config.leases) {
        return Error(TsegImportCacheErrorCode::kResourceExhausted);
    }
    if (next_entry_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(TsegImportCacheErrorCode::kSequenceExhausted);
    }
    free->key = request.key;
    free->token = request.token;
    free->descriptor = request.descriptor;
    free->generation = ++next_entry_;
    free->references = 0;
    free->invalidated = false;
    free->target = nullptr;
    free->unimport_safe = true;
    free->provider_error = 0;
    free->phase = State::Phase::kPending;
    ++state.entry_count;
    auto result = state.NewReference(*free, this, next_lease_);
    if (!result) {
        state.FreeIfUnused(*free);
    }
    return result;
}

std::expected<TsegImportLease, TsegImportCacheError> TsegImportCache::Retain(TsegImportLease lease) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    if (state_->stopped) {
        return Error(TsegImportCacheErrorCode::kStopped);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(TsegImportCacheErrorCode::kInvalidLease);
    }
    if (entry->invalidated) {
        return Error(TsegImportCacheErrorCode::kInvalidated);
    }
    if (entry->phase == State::Phase::kFailed) {
        return Error(TsegImportCacheErrorCode::kProviderError, entry->provider_error);
    }
    return state_->NewReference(*entry, this, next_lease_);
}

std::expected<TsegImportKey, TsegImportCacheError> TsegImportCache::GetKey(TsegImportLease lease) const noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(TsegImportCacheErrorCode::kInvalidLease);
    }
    return entry->key;
}

std::expected<urma_target_seg_t*, TsegImportCacheError> TsegImportCache::Get(TsegImportLease lease) const noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(TsegImportCacheErrorCode::kInvalidLease);
    }
    if (entry->invalidated) {
        return Error(TsegImportCacheErrorCode::kInvalidated);
    }
    if (entry->phase == State::Phase::kFailed) {
        return Error(TsegImportCacheErrorCode::kProviderError, entry->provider_error);
    }
    if (entry->phase != State::Phase::kReady) {
        return Error(TsegImportCacheErrorCode::kPending);
    }
    return entry->target;
}

std::expected<void, TsegImportCacheError> TsegImportCache::Release(TsegImportLease lease) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(TsegImportCacheErrorCode::kInvalidLease);
    }
    state_->references[lease.slot].active = false;
    --state_->lease_count;
    --entry->references;
    if (!entry->references &&
        (entry->phase == State::Phase::kImporting || entry->phase == State::Phase::kPreparedImport)) {
        // 所有等待者都取消时仍收割已接受任务，再清理其结果，不发布半成品。
        entry->invalidated = true;
    }
    state_->FreeIfUnused(*entry);
    return {};
}

std::expected<void, TsegImportCacheError> TsegImportCache::Invalidate(TsegImportLease lease) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(TsegImportCacheErrorCode::kInvalidLease);
    }
    entry->invalidated = true;
    return {};
}

std::expected<void, TsegImportCacheError> TsegImportCache::Invalidate(const TsegImportKey& key) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    for (std::uint32_t i = 0; i < state_->config.entries; ++i) {
        auto& entry = state_->entries[i];
        if (entry.phase != State::Phase::kFree && entry.key == key) {
            entry.invalidated = true;
            state_->FreeIfUnused(entry);
        }
    }
    return {};
}

std::size_t TsegImportCache::EvictUnused() noexcept {
    if (!state_) {
        return 0;
    }
    std::size_t count = 0;
    for (std::uint32_t i = 0; i < state_->config.entries; ++i) {
        auto& entry = state_->entries[i];
        if (entry.phase != State::Phase::kFree && !entry.references && !entry.invalidated) {
            entry.invalidated = true;
            ++count;
            state_->FreeIfUnused(entry);
        }
    }
    return count;
}

std::expected<std::optional<TsegImportOperation>, TsegImportCacheError>
TsegImportCache::NextOperation(urma_context_t* context, std::uint64_t incarnation) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    if ((context == nullptr) != (incarnation == 0)) {
        return Error(TsegImportCacheErrorCode::kInvalidArgument);
    }
    if (state.outstanding == state.config.in_flight) {
        return std::nullopt;
    }
    for (std::uint32_t i = 0; i < state.config.entries; ++i) {
        const auto slot = state.next_operation;
        state.next_operation = (slot + 1) % state.config.entries;
        auto& entry = state.entries[slot];
        if (context && (entry.key.local_context != context || entry.key.local_context_incarnation != incarnation)) {
            continue;
        }
        const auto operation = state.Operation(entry);
        if (operation && !state.Busy(entry.key.local_context)) {
            return TsegImportOperation{
                this, slot, entry.generation, *operation, entry.key.local_context, entry.key.local_context_incarnation};
        }
    }
    return std::nullopt;
}

std::expected<TsegImportPreparedOperation, TsegImportCacheError>
TsegImportCache::PrepareOperation(const TsegImportOperation& operation) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (operation.owner != this || operation.entry >= state.config.entries) {
        return Error(TsegImportCacheErrorCode::kInvalidOperation);
    }
    auto& entry = state.entries[operation.entry];
    const auto kind = state.Operation(entry);
    if (!kind || operation.generation != entry.generation || *kind != operation.kind ||
        operation.context != entry.key.local_context ||
        operation.context_incarnation != entry.key.local_context_incarnation) {
        return Error(TsegImportCacheErrorCode::kInvalidOperation);
    }
    if (state.Busy(entry.key.local_context)) {
        return Error(TsegImportCacheErrorCode::kInUse);
    }
    if (state.outstanding == state.config.in_flight) {
        return Error(TsegImportCacheErrorCode::kResourceExhausted);
    }
    if (next_reservation_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(TsegImportCacheErrorCode::kSequenceExhausted);
    }
    entry.work.kind = *kind;
    entry.work.context = entry.key.local_context;
    entry.work.token = entry.token;
    entry.work.target = entry.target;
    entry.work.unimport_safe = entry.unimport_safe;
    entry.work.descriptor = entry.descriptor;
    entry.reservation = ++next_reservation_;
    entry.prepared_from = entry.phase;
    entry.phase =
        *kind == TsegImportOperationKind::kImport ? State::Phase::kPreparedImport : State::Phase::kPreparedUnimport;
    state.busy_contexts[state.outstanding++] = entry.key.local_context;
    return TsegImportPreparedOperation{
        operation, entry.reservation, {.function = &State::Run, .argument = &entry.work, .token = entry.reservation}};
}

std::expected<void, TsegImportCacheError> TsegImportCache::CommitOperation(const TsegImportPreparedOperation& prepared,
                                                                           runtime::ControlTaskId id) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    const auto& operation = prepared.operation;
    if (operation.owner != this || operation.entry >= state_->config.entries || !id.executor || !id.sequence) {
        return Error(TsegImportCacheErrorCode::kInvalidOperation);
    }
    auto& entry = state_->entries[operation.entry];
    const auto phase = operation.kind == TsegImportOperationKind::kImport ? State::Phase::kPreparedImport
                                                                          : State::Phase::kPreparedUnimport;
    if (entry.phase != phase || entry.generation != operation.generation || entry.reservation != prepared.reservation ||
        !prepared.reservation || entry.key.local_context != operation.context ||
        entry.key.local_context_incarnation != operation.context_incarnation || prepared.task.function != &State::Run ||
        prepared.task.argument != &entry.work || prepared.task.token != entry.reservation || prepared.task.cancelled) {
        return Error(TsegImportCacheErrorCode::kInvalidOperation);
    }
    entry.task = id;
    entry.phase =
        operation.kind == TsegImportOperationKind::kImport ? State::Phase::kImporting : State::Phase::kUnimporting;
    return {};
}

std::expected<void, TsegImportCacheError>
TsegImportCache::DiscardOperation(const TsegImportPreparedOperation& prepared) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    const auto& operation = prepared.operation;
    if (operation.owner != this || operation.entry >= state_->config.entries) {
        return Error(TsegImportCacheErrorCode::kInvalidOperation);
    }
    auto& entry = state_->entries[operation.entry];
    const auto phase = operation.kind == TsegImportOperationKind::kImport ? State::Phase::kPreparedImport
                                                                          : State::Phase::kPreparedUnimport;
    if (entry.phase != phase || entry.generation != operation.generation || entry.reservation != prepared.reservation ||
        !prepared.reservation || entry.key.local_context != operation.context ||
        entry.key.local_context_incarnation != operation.context_incarnation || prepared.task.function != &State::Run ||
        prepared.task.argument != &entry.work || prepared.task.token != entry.reservation || prepared.task.cancelled) {
        return Error(TsegImportCacheErrorCode::kInvalidOperation);
    }
    entry.phase = entry.prepared_from;
    state_->CompleteContext(entry.key.local_context);
    state_->FreeIfUnused(entry);
    return {};
}

std::expected<runtime::ControlTaskId, TsegImportCacheError>
TsegImportCache::StartOperation(const TsegImportOperation& operation, runtime::ControlExecutor& executor,
                                const TsegImportContextPause& pause) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    if (!pause.users_paused || !pause.exclusive_control || pause.context != operation.context ||
        pause.incarnation != operation.context_incarnation) {
        return Error(TsegImportCacheErrorCode::kInvalidArgument);
    }
    auto prepared = PrepareOperation(operation);
    if (!prepared) {
        return std::unexpected(prepared.error());
    }
    auto submitted = executor.TrySubmit(prepared->task);
    if (!submitted) {
        if (auto discarded = DiscardOperation(*prepared); !discarded) {
            return std::unexpected(discarded.error());
        }
        return Error(TsegImportCacheErrorCode::kExecutorError, submitted.error().system_error);
    }
    if (auto committed = CommitOperation(*prepared, *submitted); !committed) {
        return std::unexpected(committed.error());
    }
    return *submitted;
}

bool TsegImportCache::OwnsCompletion(runtime::ControlTaskId id) const noexcept {
    if (!state_) {
        return false;
    }
    for (std::uint32_t i = 0; i < state_->config.entries; ++i) {
        const auto& entry = state_->entries[i];
        if ((entry.phase == State::Phase::kImporting || entry.phase == State::Phase::kUnimporting) &&
            entry.task == id) {
            return true;
        }
    }
    return false;
}

std::expected<void, TsegImportCacheError>
TsegImportCache::HandleCompletion(const runtime::ControlTaskCompletion& completion) noexcept {
    if (!state_) {
        return Error(TsegImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    for (std::uint32_t i = 0; i < state.config.entries; ++i) {
        auto& entry = state.entries[i];
        if ((entry.phase != State::Phase::kImporting && entry.phase != State::Phase::kUnimporting) ||
            entry.task != completion.id) {
            continue;
        }
        if (completion.argument != &entry.work || completion.token != entry.reservation) {
            return Error(TsegImportCacheErrorCode::kInvalidCompletion);
        }
        // Executor 的 Poll 已通过 acquire 收割 worker 发布；此前 owner 不读取 work 输出。
        entry.target = entry.work.target;
        entry.unimport_safe = entry.work.unimport_safe;
        entry.provider_error = completion.result ? 0 : completion.result.error().code;
        if (completion.cancelled) {
            entry.invalidated = true;
        }
        entry.phase = completion.result ? State::Phase::kReady : State::Phase::kFailed;
        state.CompleteContext(entry.key.local_context);
        state.FreeIfUnused(entry);
        return {};
    }
    return Error(TsegImportCacheErrorCode::kInvalidCompletion);
}

void TsegImportCache::StopAccepting() noexcept {
    if (!state_) {
        return;
    }
    state_->stopped = true;
    for (std::uint32_t i = 0; i < state_->config.entries; ++i) {
        auto& entry = state_->entries[i];
        entry.invalidated = true;
        state_->FreeIfUnused(entry);
    }
}

std::expected<void, TsegImportCacheError> TsegImportCache::Close() noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (state_->entry_count || state_->lease_count || state_->outstanding) {
        return Error(TsegImportCacheErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
std::size_t TsegImportCache::entries() const noexcept {
    return state_ ? state_->entry_count : 0;
}
std::size_t TsegImportCache::leases() const noexcept {
    return state_ ? state_->lease_count : 0;
}
std::size_t TsegImportCache::in_flight() const noexcept {
    return state_ ? state_->outstanding : 0;
}

} // namespace control
} // namespace kbsocket
