// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/memory/slab_allocator.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <new>

#include "kbsocket/memory/quarantine_ledger.hpp"

namespace kbsocket {
namespace memory {
namespace {
constexpr auto kEnd = std::numeric_limits<std::uint32_t>::max();
auto Error(MemoryErrorCode code, int status = 0, std::size_t context = 0) noexcept {
    return std::unexpected(MemoryError{code, status, context, {}});
}
} // namespace
bool MemoryBudget::Reserve(std::size_t bytes) noexcept {
    auto used = reserved_bytes_.load(std::memory_order_relaxed);
    do {
        if (bytes > limit_bytes_ - used) {
            return false;
        }
    } while (!reserved_bytes_.compare_exchange_weak(used, used + bytes, std::memory_order_acq_rel));
    return true;
}
void MemoryBudget::Release(std::size_t bytes) noexcept {
    reserved_bytes_.fetch_sub(bytes, std::memory_order_release);
}
struct SlabAllocator::State {
    struct Entry {
        std::uint64_t generation = 0;
        std::size_t length = 0;
        std::uint32_t local = 0, remote = 0;
        std::uint32_t next = kEnd;
        bool cpu = false, active = false;
    };
    struct Reference {
        BufferLease lease;
        std::uint64_t sequence = 0;
        std::uint32_t next = kEnd;
        BufferUse use = BufferUse::kLocalDma;
        bool active = false, committed = false;
    };
    std::unique_ptr<Entry[]> entries;
    std::unique_ptr<Reference[]> references;
    QuarantineLedger quarantine;
    std::array<urma_context_t*, kMaxContexts> contexts{};
    std::array<urma_target_seg_t*, kMaxContexts> segments{};
    std::size_t context_count = 0;
    MemoryBudget* budget = nullptr;
    std::byte* memory = nullptr;
    SlabConfig config;
    SlabStats stats;
    std::uint32_t free_entry = kEnd, free_reference = kEnd;
    bool ready = false, stopping = false;
};

SlabAllocator::SlabAllocator() noexcept = default;
SlabAllocator::~SlabAllocator() {
    if (!Close()) {
        // 未终结或注销失败时保留元数据/MR/backing/预算，不让析构隐式 free DMA 内存。
        (void)state_.release();
    }
}
std::expected<void, MemoryError> SlabAllocator::Open(std::span<urma_context_t* const> contexts,
                                                     const SlabConfig& config, MemoryBudget& budget) noexcept {
    return OpenImpl(contexts, config, budget, false);
}
std::expected<void, MemoryError> SlabAllocator::OpenHost(const SlabConfig& config, MemoryBudget& budget) noexcept {
    return OpenImpl({}, config, budget, true);
}
std::expected<void, MemoryError> SlabAllocator::OpenImpl(std::span<urma_context_t* const> contexts,
                                                         const SlabConfig& config, MemoryBudget& budget,
                                                         bool host) noexcept {
    if (state_) {
        return Error(MemoryErrorCode::kInUse);
    }
    constexpr auto permissions = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if ((!host && contexts.empty()) || contexts.size() > kMaxContexts || !config.buffer_count ||
        config.buffer_count > kMaxBuffers || !config.buffer_size || !config.max_references ||
        config.max_references > kMaxReferences || !config.access || (config.access & ~permissions) || page_size <= 0 ||
        config.buffer_size > std::numeric_limits<std::size_t>::max() / config.buffer_count) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < contexts.size(); ++i) {
        if (!contexts[i]) {
            return Error(MemoryErrorCode::kInvalidArgument);
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (contexts[i] == contexts[j]) {
                return Error(MemoryErrorCode::kInvalidArgument);
            }
        }
    }
    const auto page = static_cast<std::size_t>(page_size);
    const auto bytes = config.buffer_count * config.buffer_size;
    if (bytes > std::numeric_limits<std::size_t>::max() - (page - 1)) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    const auto allocation_size = (bytes + page - 1) / page * page;
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(MemoryErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) State::Entry[config.buffer_count]);
    state->references.reset(new (std::nothrow) State::Reference[config.max_references]);
    if (!state->entries || !state->references) {
        return Error(MemoryErrorCode::kNoMemory);
    }
    if (auto opened = state->quarantine.Open(config.buffer_count); !opened) {
        return opened;
    }
    if (!budget.Reserve(allocation_size)) {
        return Error(MemoryErrorCode::kExhausted);
    }
    void* memory = nullptr;
    if (const int status = ::posix_memalign(&memory, page, allocation_size)) {
        budget.Release(allocation_size);
        return Error(MemoryErrorCode::kNoMemory, status);
    }
    // 初始化控制路径预触页；此后任何借用不会临时分配或注册未授权内存。
    std::memset(memory, 0, allocation_size);
    state->memory = static_cast<std::byte*>(memory);
    state->budget = &budget;
    state->config = config;
    state->stats.backing_bytes = allocation_size;
    state->context_count = contexts.size();
    std::copy(contexts.begin(), contexts.end(), state->contexts.begin());
    state_ = std::move(state);
    for (std::size_t i = 0; i < contexts.size(); ++i) {
        urma_seg_cfg_t cfg{};
        cfg.va = reinterpret_cast<std::uintptr_t>(memory);
        cfg.len = allocation_size;
        cfg.flag.bs.access = config.access;
        errno = 0;
        state_->segments[i] = raw::UrmaApi::RegisterSeg(contexts[i], &cfg);
        if (!state_->segments[i]) {
            MemoryError error{MemoryErrorCode::kRegisterFailed, errno ? errno : EIO, i, {}};
            if (auto closed = Close(); !closed) {
                error.cleanup_error = closed.error().provider_error;
            }
            return std::unexpected(error);
        }
    }
    for (std::uint32_t i = 0; i < config.buffer_count; ++i) {
        state_->entries[i].next = i + 1 == config.buffer_count ? kEnd : i + 1;
    }
    for (std::uint32_t i = 0; i < config.max_references; ++i) {
        state_->references[i].next = i + 1 == config.max_references ? kEnd : i + 1;
    }
    state_->stats.buffers = state_->stats.available = config.buffer_count;
    state_->free_entry = state_->free_reference = 0;
    state_->ready = true;
    return {};
}
void SlabAllocator::StopAccepting() noexcept {
    if (state_) {
        state_->stopping = true;
    }
}
bool SlabAllocator::accepting() const noexcept {
    return state_ && state_->ready && !state_->stopping;
}
std::expected<SlabSegmentSnapshot, MemoryError> SlabAllocator::ExportSegment(urma_context_t* context) const noexcept {
    if (!state_ || !state_->ready) {
        return Error(MemoryErrorCode::kNotReady);
    }
    if (state_->stopping) {
        return Error(MemoryErrorCode::kStopped);
    }
    if (!context) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < state_->context_count; ++i) {
        if (state_->contexts[i] != context) {
            continue;
        }
        const auto* segment = state_->segments[i];
        if (!segment || segment->urma_ctx != context) {
            return Error(MemoryErrorCode::kRegisterFailed, EPROTO, i);
        }
        const auto& descriptor = segment->seg;
        // provider 的额外区间/权限和未经请求的属性不属于本 Slab，不能悄悄公布。
        // 注册配置固定 NONE/零 token；不能从另一 policy 猜测访问 token。
        if (descriptor.ubva.va != reinterpret_cast<std::uintptr_t>(state_->memory) ||
            descriptor.len != state_->stats.backing_bytes || !descriptor.ubva.va || !descriptor.len ||
            descriptor.len > std::numeric_limits<std::uint64_t>::max() - descriptor.ubva.va ||
            descriptor.ubva.uasid != context->uasid || descriptor.ubva.uasid >= (1U << 24) ||
            std::memcmp(descriptor.ubva.eid.raw, context->eid.raw, URMA_EID_SIZE) ||
            std::all_of(std::begin(descriptor.ubva.eid.raw), std::end(descriptor.ubva.eid.raw),
                        [](auto byte) { return byte == 0; }) ||
            descriptor.token_id == URMA_SEG_TOKEN_ID_INVALID || descriptor.attr.bs.access != state_->config.access ||
            descriptor.attr.bs.token_policy != URMA_TOKEN_NONE || descriptor.attr.bs.cacheable ||
            descriptor.attr.bs.dsva || descriptor.attr.bs.non_pin || descriptor.attr.bs.user_iova ||
            descriptor.attr.bs.user_token_id || descriptor.attr.bs.has_user_info || descriptor.attr.bs.reserved) {
            return Error(MemoryErrorCode::kRegisterFailed, EPROTO, i);
        }
        return SlabSegmentSnapshot{descriptor, {}, state_->config.buffer_size, state_->config.buffer_count};
    }
    // CPU-only backing 或未参与注册的 context 都没有可导出的段。
    return Error(MemoryErrorCode::kInvalidArgument);
}
std::expected<void, MemoryError> SlabAllocator::Close() noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (state_->stats.available != state_->stats.buffers) {
        return Error(MemoryErrorCode::kInUse);
    }
    state_->ready = false;
    for (std::size_t i = state_->context_count; i > 0; --i) {
        auto*& segment = state_->segments[i - 1];
        if (!segment) {
            continue;
        }
        const auto status = raw::UrmaApi::UnregisterSeg(segment);
        if (status != URMA_SUCCESS) {
            return Error(MemoryErrorCode::kUnregisterFailed, status, i - 1);
        }
        segment = nullptr;
    }
    std::free(state_->memory);
    state_->budget->Release(state_->stats.backing_bytes);
    state_.reset();
    return {};
}
SlabStats SlabAllocator::stats() const noexcept {
    if (!state_) {
        return {};
    }
    auto stats = state_->stats;
    stats.quarantined_buffers = state_->quarantine.size();
    stats.quarantined_bytes = state_->quarantine.bytes();
    stats.available_references = state_->config.max_references - stats.local_dma - stats.remote_grants;
    return stats;
}
std::expected<std::uint32_t, MemoryError> SlabAllocator::Find(BufferLease lease) const noexcept {
    if (!state_ || !state_->ready) {
        return Error(MemoryErrorCode::kNotReady);
    }
    if (lease.allocator != this || !lease.generation || lease.slot >= state_->config.buffer_count) {
        return Error(MemoryErrorCode::kInvalidLease);
    }
    const auto& entry = state_->entries[lease.slot];
    if (!entry.active || entry.generation != lease.generation) {
        return Error(MemoryErrorCode::kInvalidLease);
    }
    return lease.slot;
}
std::expected<std::uint32_t, MemoryError> SlabAllocator::Find(BufferReference reference) const noexcept {
    if (!state_ || !state_->ready) {
        return Error(MemoryErrorCode::kNotReady);
    }
    if (reference.allocator != this || !reference.sequence || reference.slot >= state_->config.max_references) {
        return Error(MemoryErrorCode::kInvalidReference);
    }
    const auto& entry = state_->references[reference.slot];
    if (!entry.active || entry.sequence != reference.sequence || !Find(entry.lease)) {
        return Error(MemoryErrorCode::kInvalidReference);
    }
    return reference.slot;
}
std::expected<BufferLease, MemoryError> SlabAllocator::Acquire(std::size_t bytes) noexcept {
    if (!state_ || !state_->ready) {
        return Error(MemoryErrorCode::kNotReady);
    }
    if (state_->stopping) {
        return Error(MemoryErrorCode::kStopped);
    }
    if (!bytes || bytes > state_->config.buffer_size) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    if (next_lease_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(MemoryErrorCode::kSequenceExhausted);
    }
    const auto index = state_->free_entry;
    if (index == kEnd) {
        return Error(MemoryErrorCode::kExhausted);
    }
    auto& entry = state_->entries[index];
    state_->free_entry = entry.next;
    entry = {.generation = ++next_lease_, .length = bytes, .cpu = true, .active = true};
    --state_->stats.available;
    ++state_->stats.cpu_leases;
    return BufferLease{this, index, entry.generation};
}
std::expected<BufferLease, MemoryError> SlabAllocator::Copy(std::span<const std::byte> bytes) noexcept {
    auto lease = Acquire(bytes.size());
    if (!lease) {
        return lease;
    }
    // 来源可以是另一 Slice 的有效视图，memmove 明确定义重叠区间。
    std::memmove(state_->memory + lease->slot * state_->config.buffer_size, bytes.data(), bytes.size());
    return lease;
}
std::expected<std::span<std::byte>, MemoryError> SlabAllocator::Writable(BufferLease lease) noexcept {
    auto index = Find(lease);
    if (!index) {
        return std::unexpected(index.error());
    }
    if (state_->stopping) {
        return Error(MemoryErrorCode::kStopped);
    }
    const auto& entry = state_->entries[*index];
    if (!entry.cpu || entry.local || entry.remote || state_->quarantine.Contains(lease)) {
        return Error(MemoryErrorCode::kInUse);
    }
    return std::span(state_->memory + *index * state_->config.buffer_size, entry.length);
}
std::expected<std::span<const std::byte>, MemoryError> SlabAllocator::Readable(BufferLease lease) const noexcept {
    auto index = Find(lease);
    if (!index) {
        return std::unexpected(index.error());
    }
    const auto& entry = state_->entries[*index];
    if (!entry.cpu || entry.local || entry.remote || state_->quarantine.Contains(lease)) {
        return Error(MemoryErrorCode::kInUse);
    }
    return std::span<const std::byte>(state_->memory + *index * state_->config.buffer_size, entry.length);
}
void SlabAllocator::Recycle(std::uint32_t index) noexcept {
    auto& entry = state_->entries[index];
    if (entry.cpu || entry.local || entry.remote) {
        return;
    }
    state_->quarantine.Remove({this, index, entry.generation});
    entry.active = false;
    entry.next = state_->free_entry;
    state_->free_entry = index;
    ++state_->stats.available;
}
std::expected<void, MemoryError> SlabAllocator::ReleaseCpu(BufferLease lease) noexcept {
    const auto index = Find(lease);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& entry = state_->entries[*index];
    if (!entry.cpu) {
        return Error(MemoryErrorCode::kInvalidLease);
    }
    entry.cpu = false;
    --state_->stats.cpu_leases;
    Recycle(*index);
    return {};
}
std::expected<BufferReference, MemoryError> SlabAllocator::Reserve(BufferLease lease, BufferUse use) noexcept {
    const auto index = Find(lease);
    if (!index) {
        return std::unexpected(index.error());
    }
    if (state_->stopping) {
        return Error(MemoryErrorCode::kStopped);
    }
    if (!state_->context_count || (use != BufferUse::kLocalDma && use != BufferUse::kRemoteGrant) ||
        (use == BufferUse::kRemoteGrant && !(state_->config.access & URMA_ACCESS_WRITE))) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    auto& entry = state_->entries[*index];
    if (!entry.cpu || state_->quarantine.Contains(lease)) {
        return Error(MemoryErrorCode::kInUse);
    }
    if (next_reference_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(MemoryErrorCode::kSequenceExhausted);
    }
    const auto ref_index = state_->free_reference;
    if (ref_index == kEnd) {
        return Error(MemoryErrorCode::kExhausted);
    }
    auto& reference = state_->references[ref_index];
    state_->free_reference = reference.next;
    reference = {.lease = lease, .sequence = ++next_reference_, .use = use, .active = true};
    if (use == BufferUse::kLocalDma) {
        ++entry.local;
        ++state_->stats.local_dma;
    } else {
        ++entry.remote;
        ++state_->stats.remote_grants;
    }
    return BufferReference{this, ref_index, reference.sequence};
}
std::expected<void, MemoryError> SlabAllocator::Commit(BufferReference reference) noexcept {
    const auto index = Find(reference);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& entry = state_->references[*index];
    if (entry.committed) {
        return Error(MemoryErrorCode::kInvalidReference);
    }
    // 即使已 Stop/Quarantine，也须确认此前可能已暴露的责任，绝不能让它随后走 Cancel。
    entry.committed = true;
    return {};
}
std::expected<urma_sge_t, MemoryError> SlabAllocator::Sge(BufferReference reference, urma_context_t* context,
                                                          std::size_t offset, std::size_t bytes) const noexcept {
    const auto index = Find(reference);
    if (!index) {
        return std::unexpected(index.error());
    }
    const auto& ref = state_->references[*index];
    if (state_->stopping) {
        return Error(MemoryErrorCode::kStopped);
    }
    if (ref.committed || state_->quarantine.Contains(ref.lease)) {
        return Error(MemoryErrorCode::kInUse);
    }
    const auto length = state_->entries[ref.lease.slot].length;
    if (!bytes || bytes > std::numeric_limits<std::uint32_t>::max() || offset > length || bytes > length - offset) {
        return Error(MemoryErrorCode::kInvalidArgument);
    }
    for (std::size_t i = 0; i < state_->context_count; ++i) {
        if (state_->contexts[i] == context) {
            urma_sge_t sge{};
            sge.addr =
                reinterpret_cast<std::uintptr_t>(state_->memory + ref.lease.slot * state_->config.buffer_size + offset);
            sge.len = static_cast<std::uint32_t>(bytes);
            sge.tseg = state_->segments[i];
            return sge;
        }
    }
    return Error(MemoryErrorCode::kInvalidArgument);
}
std::expected<void, MemoryError> SlabAllocator::Finish(BufferReference reference, BufferUse use,
                                                       bool committed) noexcept {
    const auto index = Find(reference);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& ref = state_->references[*index];
    if (ref.use != use || ref.committed != committed) {
        return Error(MemoryErrorCode::kInvalidReference);
    }
    auto& entry = state_->entries[ref.lease.slot];
    if (ref.use == BufferUse::kLocalDma) {
        --entry.local;
        --state_->stats.local_dma;
    } else {
        --entry.remote;
        --state_->stats.remote_grants;
    }
    ref.active = false;
    ref.next = state_->free_reference;
    state_->free_reference = *index;
    Recycle(ref.lease.slot);
    return {};
}
std::expected<void, MemoryError> SlabAllocator::Cancel(BufferReference reference) noexcept {
    const auto index = Find(reference);
    if (!index) {
        return std::unexpected(index.error());
    }
    return Finish(reference, state_->references[*index].use, false);
}
std::expected<void, MemoryError> SlabAllocator::CompleteDma(BufferReference reference) noexcept {
    return Finish(reference, BufferUse::kLocalDma, true);
}
std::expected<void, MemoryError> SlabAllocator::EndGrant(BufferReference reference) noexcept {
    return Finish(reference, BufferUse::kRemoteGrant, true);
}
std::expected<void, MemoryError> SlabAllocator::Quarantine(BufferLease lease) noexcept {
    const auto index = Find(lease);
    if (!index) {
        return std::unexpected(index.error());
    }
    state_->quarantine.Mark(lease, state_->config.buffer_size);
    return {};
}
} // namespace memory
} // namespace kbsocket
