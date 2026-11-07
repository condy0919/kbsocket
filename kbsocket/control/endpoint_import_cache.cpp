// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/endpoint_import_cache.hpp"

#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace control {
namespace {
auto Error(ImportCacheErrorCode code, int provider = 0) noexcept {
    return std::unexpected(ImportCacheError{code, provider});
}

std::expected<void, ImportCacheError> Validate(const EndpointImportRequest& request) noexcept {
    const auto& key = request.key;
    if (!key.local_context || !key.local_context_incarnation || !key.peer_runtime || !key.receive_domain ||
        !key.endpoint_generation || !key.authorization_identity || key.transport_mode != URMA_TM_RM ||
        key.tp_type != URMA_CTP) {
        return Error(ImportCacheErrorCode::kInvalidArgument);
    }
    if (request.descriptor.size() < sizeof(urma_rjetty_t) || request.descriptor.size() > kMaxEndpointDescriptorBytes) {
        return Error(ImportCacheErrorCode::kInvalidDescriptor);
    }
    urma_rjetty_t descriptor{};
    std::memcpy(&descriptor, request.descriptor.data(), sizeof(descriptor));
    if (descriptor.jetty_id.id != key.jetty_id ||
        std::memcmp(&descriptor.jetty_id.eid, key.eid.data(), key.eid.size()) ||
        descriptor.trans_mode != key.transport_mode || descriptor.tp_type != key.tp_type ||
        descriptor.type != URMA_JETTY || descriptor.flag.bs.has_drv_ext || descriptor.flag.bs.share_tp ||
        descriptor.flag.bs.reserved || descriptor.flag.bs.token_policy > URMA_TOKEN_ALL_ENCRYPTED) {
        return Error(ImportCacheErrorCode::kInvalidDescriptor);
    }
    if (!descriptor.flag.bs.has_user_info) {
        if (request.descriptor.size() != sizeof(descriptor)) {
            return Error(ImportCacheErrorCode::kInvalidDescriptor);
        }
    } else {
        // 与本地 URMA ABI 一致的公共用户扩展以原生 uint32_t 长度起始；仅校验边界，不解释 provider 内容。
        std::uint32_t length = 0;
        if (request.descriptor.size() < sizeof(descriptor) + sizeof(length)) {
            return Error(ImportCacheErrorCode::kInvalidDescriptor);
        }
        std::memcpy(&length, request.descriptor.data() + sizeof(descriptor), sizeof(length));
        if (length != request.descriptor.size() - sizeof(descriptor) - sizeof(length)) {
            return Error(ImportCacheErrorCode::kInvalidDescriptor);
        }
    }
    return {};
}
} // namespace

struct EndpointImportCache::State {
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
        ImportOperationKind kind = ImportOperationKind::kImport;
        urma_context_t* context = nullptr;
        urma_token_t token{};
        alignas(urma_rjetty_t) std::array<std::byte, kMaxEndpointDescriptorBytes> descriptor{};
        urma_target_jetty_t* target = nullptr;
    };
    struct Entry {
        EndpointImportKey key;
        urma_token_t token{};
        std::array<std::byte, kMaxEndpointDescriptorBytes> descriptor{};
        std::size_t descriptor_size = 0;
        std::uint64_t generation = 0;
        std::size_t references = 0;
        Phase phase = Phase::kFree;
        bool invalidated = false;
        urma_target_jetty_t* target = nullptr;
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

    Entry* Lookup(ImportLease lease, const EndpointImportCache* owner) noexcept {
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
    std::optional<ImportOperationKind> Operation(const Entry& entry) const noexcept {
        if (entry.phase == Phase::kPending && !entry.invalidated && entry.references) {
            return ImportOperationKind::kImport;
        }
        if (!entry.references && entry.target &&
            ((entry.phase == Phase::kReady && entry.invalidated) || entry.phase == Phase::kFailed)) {
            return ImportOperationKind::kUnimport;
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
    std::expected<ImportLease, ImportCacheError> NewReference(Entry& entry, const EndpointImportCache* owner,
                                                              std::uint64_t& sequence) noexcept {
        if (sequence == std::numeric_limits<std::uint64_t>::max()) {
            return Error(ImportCacheErrorCode::kSequenceExhausted);
        }
        if (lease_count == config.leases) {
            return Error(ImportCacheErrorCode::kResourceExhausted);
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
            return ImportLease{owner, slot, sequence};
        }
        return Error(ImportCacheErrorCode::kResourceExhausted);
    }
    static runtime::ControlTaskResult Run(void* argument, std::uint64_t) noexcept {
        auto& work = *static_cast<Work*>(argument);
        if (work.kind == ImportOperationKind::kImport) {
            errno = 0;
            auto* descriptor = reinterpret_cast<urma_rjetty_t*>(work.descriptor.data());
            work.target = raw::UrmaApi::ImportJetty(work.context, descriptor, &work.token);
            if (!work.target) {
                return std::unexpected(runtime::ControlTaskError{errno ? errno : EIO});
            }
        } else {
            const auto status = raw::UrmaApi::UnimportJetty(work.target);
            if (status != URMA_SUCCESS) {
                return std::unexpected(runtime::ControlTaskError{status});
            }
            work.target = nullptr;
        }
        return {};
    }

    EndpointImportCacheConfig config;
    std::unique_ptr<Entry[]> entries;
    std::unique_ptr<Reference[]> references;
    std::unique_ptr<urma_context_t*[]> busy_contexts;
    std::size_t entry_count = 0, lease_count = 0, outstanding = 0;
    std::uint32_t next_reference = 0, next_operation = 0;
    bool stopped = false;
};

EndpointImportCache::EndpointImportCache() noexcept = default;
EndpointImportCache::~EndpointImportCache() {
    if (!Close()) {
        // provider 工作参数、已导入句柄及未归还引用仍依赖该存储，不能析构后制造 UAF。
        (void)state_.release();
    }
}

std::expected<void, ImportCacheError> EndpointImportCache::Open(const EndpointImportCacheConfig& config) noexcept {
    if (state_ || opened_once_) {
        return Error(ImportCacheErrorCode::kInUse);
    }
    if (!config.entries || config.entries > kMaxEntries || !config.leases || config.leases > kMaxLeases ||
        !config.in_flight || config.in_flight > config.entries) {
        return Error(ImportCacheErrorCode::kInvalidArgument);
    }
    try {
        auto state = std::make_unique<State>();
        state->entries = std::make_unique<State::Entry[]>(config.entries);
        state->references = std::make_unique<State::Reference[]>(config.leases);
        state->busy_contexts = std::make_unique<urma_context_t*[]>(config.in_flight);
        state->config = config;
        state_ = std::move(state);
    } catch (const std::bad_alloc&) {
        return Error(ImportCacheErrorCode::kNoMemory);
    }
    opened_once_ = true;
    return {};
}

std::expected<ImportLease, ImportCacheError>
EndpointImportCache::Acquire(const EndpointImportRequest& request) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (state.stopped) {
        return Error(ImportCacheErrorCode::kStopped);
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
        if (entry.token.token != request.token.token || entry.descriptor_size != request.descriptor.size() ||
            std::memcmp(entry.descriptor.data(), request.descriptor.data(), entry.descriptor_size)) {
            return Error(ImportCacheErrorCode::kDescriptorConflict);
        }
        if (entry.invalidated || entry.phase == State::Phase::kUnimporting ||
            entry.phase == State::Phase::kPreparedUnimport) {
            return Error(ImportCacheErrorCode::kInvalidated);
        }
        if (entry.phase == State::Phase::kFailed) {
            return Error(ImportCacheErrorCode::kProviderError, entry.provider_error);
        }
        return state.NewReference(entry, this, next_lease_);
    }
    if (!free || state.lease_count == state.config.leases) {
        return Error(ImportCacheErrorCode::kResourceExhausted);
    }
    if (next_entry_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(ImportCacheErrorCode::kSequenceExhausted);
    }
    free->key = request.key;
    free->token = request.token;
    free->descriptor_size = request.descriptor.size();
    std::memcpy(free->descriptor.data(), request.descriptor.data(), request.descriptor.size());
    free->generation = ++next_entry_;
    free->references = 0;
    free->invalidated = false;
    free->target = nullptr;
    free->provider_error = 0;
    free->phase = State::Phase::kPending;
    ++state.entry_count;
    auto result = state.NewReference(*free, this, next_lease_);
    if (!result) {
        state.FreeIfUnused(*free);
    }
    return result;
}

std::expected<ImportLease, ImportCacheError> EndpointImportCache::Retain(ImportLease lease) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    if (state_->stopped) {
        return Error(ImportCacheErrorCode::kStopped);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(ImportCacheErrorCode::kInvalidLease);
    }
    if (entry->invalidated) {
        return Error(ImportCacheErrorCode::kInvalidated);
    }
    if (entry->phase == State::Phase::kFailed) {
        return Error(ImportCacheErrorCode::kProviderError, entry->provider_error);
    }
    return state_->NewReference(*entry, this, next_lease_);
}

std::expected<EndpointImportKey, ImportCacheError> EndpointImportCache::GetKey(ImportLease lease) const noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(ImportCacheErrorCode::kInvalidLease);
    }
    return entry->key;
}

std::expected<urma_target_jetty_t*, ImportCacheError> EndpointImportCache::Get(ImportLease lease) const noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(ImportCacheErrorCode::kInvalidLease);
    }
    if (entry->invalidated) {
        return Error(ImportCacheErrorCode::kInvalidated);
    }
    if (entry->phase == State::Phase::kFailed) {
        return Error(ImportCacheErrorCode::kProviderError, entry->provider_error);
    }
    if (entry->phase != State::Phase::kReady) {
        return Error(ImportCacheErrorCode::kPending);
    }
    return entry->target;
}

std::expected<void, ImportCacheError> EndpointImportCache::Release(ImportLease lease) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(ImportCacheErrorCode::kInvalidLease);
    }
    state_->references[lease.slot].active = false;
    --state_->lease_count;
    --entry->references;
    if (!entry->references &&
        (entry->phase == State::Phase::kImporting || entry->phase == State::Phase::kPreparedImport)) {
        // 所有建链者都取消时仍收割已接受任务，再清理其结果，不发布半成品。
        entry->invalidated = true;
    }
    state_->FreeIfUnused(*entry);
    return {};
}

std::expected<void, ImportCacheError> EndpointImportCache::Invalidate(ImportLease lease) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto* entry = state_->Lookup(lease, this);
    if (!entry) {
        return Error(ImportCacheErrorCode::kInvalidLease);
    }
    entry->invalidated = true;
    return {};
}

std::expected<void, ImportCacheError> EndpointImportCache::Invalidate(const EndpointImportKey& key) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
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

std::size_t EndpointImportCache::EvictUnused() noexcept {
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

std::expected<std::optional<ImportOperation>, ImportCacheError>
EndpointImportCache::NextOperation(urma_context_t* context, std::uint64_t incarnation) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    if ((context == nullptr) != (incarnation == 0)) {
        return Error(ImportCacheErrorCode::kInvalidArgument);
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
            return ImportOperation{
                this, slot, entry.generation, *operation, entry.key.local_context, entry.key.local_context_incarnation};
        }
    }
    return std::nullopt;
}

std::expected<ImportPreparedOperation, ImportCacheError>
EndpointImportCache::PrepareOperation(const ImportOperation& operation) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (operation.owner != this || operation.entry >= state.config.entries) {
        return Error(ImportCacheErrorCode::kInvalidOperation);
    }
    auto& entry = state.entries[operation.entry];
    const auto kind = state.Operation(entry);
    if (!kind || operation.generation != entry.generation || *kind != operation.kind ||
        operation.context != entry.key.local_context ||
        operation.context_incarnation != entry.key.local_context_incarnation) {
        return Error(ImportCacheErrorCode::kInvalidOperation);
    }
    if (state.Busy(entry.key.local_context)) {
        return Error(ImportCacheErrorCode::kInUse);
    }
    if (state.outstanding == state.config.in_flight) {
        return Error(ImportCacheErrorCode::kResourceExhausted);
    }
    if (next_reservation_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(ImportCacheErrorCode::kSequenceExhausted);
    }
    entry.work.kind = *kind;
    entry.work.context = entry.key.local_context;
    entry.work.token = entry.token;
    entry.work.target = entry.target;
    std::memcpy(entry.work.descriptor.data(), entry.descriptor.data(), entry.descriptor_size);
    entry.reservation = ++next_reservation_;
    entry.prepared_from = entry.phase;
    entry.phase =
        *kind == ImportOperationKind::kImport ? State::Phase::kPreparedImport : State::Phase::kPreparedUnimport;
    state.busy_contexts[state.outstanding++] = entry.key.local_context;
    return ImportPreparedOperation{
        operation, entry.reservation, {.function = &State::Run, .argument = &entry.work, .token = entry.reservation}};
}

std::expected<void, ImportCacheError> EndpointImportCache::CommitOperation(const ImportPreparedOperation& prepared,
                                                                           runtime::ControlTaskId id) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    const auto& operation = prepared.operation;
    if (operation.owner != this || operation.entry >= state_->config.entries || !id.executor || !id.sequence) {
        return Error(ImportCacheErrorCode::kInvalidOperation);
    }
    auto& entry = state_->entries[operation.entry];
    const auto phase = operation.kind == ImportOperationKind::kImport ? State::Phase::kPreparedImport
                                                                      : State::Phase::kPreparedUnimport;
    if (entry.phase != phase || entry.generation != operation.generation || entry.reservation != prepared.reservation ||
        !prepared.reservation || entry.key.local_context != operation.context ||
        entry.key.local_context_incarnation != operation.context_incarnation || prepared.task.function != &State::Run ||
        prepared.task.argument != &entry.work || prepared.task.token != entry.reservation || prepared.task.cancelled) {
        return Error(ImportCacheErrorCode::kInvalidOperation);
    }
    entry.task = id;
    entry.phase =
        operation.kind == ImportOperationKind::kImport ? State::Phase::kImporting : State::Phase::kUnimporting;
    return {};
}

std::expected<void, ImportCacheError>
EndpointImportCache::DiscardOperation(const ImportPreparedOperation& prepared) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    const auto& operation = prepared.operation;
    if (operation.owner != this || operation.entry >= state_->config.entries) {
        return Error(ImportCacheErrorCode::kInvalidOperation);
    }
    auto& entry = state_->entries[operation.entry];
    const auto phase = operation.kind == ImportOperationKind::kImport ? State::Phase::kPreparedImport
                                                                      : State::Phase::kPreparedUnimport;
    if (entry.phase != phase || entry.generation != operation.generation || entry.reservation != prepared.reservation ||
        !prepared.reservation || entry.key.local_context != operation.context ||
        entry.key.local_context_incarnation != operation.context_incarnation || prepared.task.function != &State::Run ||
        prepared.task.argument != &entry.work || prepared.task.token != entry.reservation || prepared.task.cancelled) {
        return Error(ImportCacheErrorCode::kInvalidOperation);
    }
    entry.phase = entry.prepared_from;
    state_->CompleteContext(entry.key.local_context);
    state_->FreeIfUnused(entry);
    return {};
}

std::expected<runtime::ControlTaskId, ImportCacheError>
EndpointImportCache::StartOperation(const ImportOperation& operation, runtime::ControlExecutor& executor,
                                    const ImportContextPause& pause) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    if (!pause.users_paused || !pause.exclusive_control || pause.context != operation.context ||
        pause.incarnation != operation.context_incarnation) {
        return Error(ImportCacheErrorCode::kInvalidArgument);
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
        return Error(ImportCacheErrorCode::kExecutorError, submitted.error().system_error);
    }
    if (auto committed = CommitOperation(*prepared, *submitted); !committed) {
        return std::unexpected(committed.error());
    }
    return *submitted;
}

bool EndpointImportCache::OwnsCompletion(runtime::ControlTaskId id) const noexcept {
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

std::expected<void, ImportCacheError>
EndpointImportCache::HandleCompletion(const runtime::ControlTaskCompletion& completion) noexcept {
    if (!state_) {
        return Error(ImportCacheErrorCode::kNotReady);
    }
    auto& state = *state_;
    for (std::uint32_t i = 0; i < state.config.entries; ++i) {
        auto& entry = state.entries[i];
        if ((entry.phase != State::Phase::kImporting && entry.phase != State::Phase::kUnimporting) ||
            entry.task != completion.id) {
            continue;
        }
        if (completion.argument != &entry.work || completion.token != entry.reservation) {
            return Error(ImportCacheErrorCode::kInvalidCompletion);
        }
        // Executor 的 Poll 已通过 acquire 收割 worker 发布；此前 owner 不读取 work 输出。
        entry.target = entry.work.target;
        entry.provider_error = completion.result ? 0 : completion.result.error().code;
        if (completion.cancelled) {
            entry.invalidated = true;
        }
        entry.phase = completion.result ? State::Phase::kReady : State::Phase::kFailed;
        state.CompleteContext(entry.key.local_context);
        state.FreeIfUnused(entry);
        return {};
    }
    return Error(ImportCacheErrorCode::kInvalidCompletion);
}

void EndpointImportCache::StopAccepting() noexcept {
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

std::expected<void, ImportCacheError> EndpointImportCache::Close() noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (state_->entry_count || state_->lease_count || state_->outstanding) {
        return Error(ImportCacheErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
std::size_t EndpointImportCache::entries() const noexcept {
    return state_ ? state_->entry_count : 0;
}
std::size_t EndpointImportCache::leases() const noexcept {
    return state_ ? state_->lease_count : 0;
}
std::size_t EndpointImportCache::in_flight() const noexcept {
    return state_ ? state_->outstanding : 0;
}

} // namespace control
} // namespace kbsocket
