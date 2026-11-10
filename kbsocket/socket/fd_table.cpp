// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/socket/fd_table.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <new>
#include <utility>

namespace kbsocket {
namespace socket {
namespace detail {
constexpr std::uint32_t kNone = std::numeric_limits<std::uint32_t>::max();
enum class EntryStage : std::uint8_t { kFree, kPublished, kRetired };
struct alignas(64) FdTableEntry {
    FdBinding binding;
    std::atomic<std::uint32_t> references{0};
    std::uint32_t next = kNone;
    EntryStage stage = EntryStage::kFree;
    bool confirmed = false;
};
struct FdSlot {
    std::atomic<FdTableEntry*> published{nullptr};
    std::uint32_t generation = 0;
};
struct FdPage {
    std::array<FdSlot, FdTable::kPageSize> slots;
};
struct alignas(64) FdHazard {
    std::atomic<FdTableEntry*> pointer{nullptr};
    std::atomic<bool> registered{false};
};
struct FdTableState {
    FdTableConfig config;
    std::unique_ptr<FdPage*[]> directory;
    std::unique_ptr<FdPage[]> pages;
    std::unique_ptr<FdTableEntry[]> entries;
    std::unique_ptr<FdHazard[]> hazards;
    std::atomic<bool> accepting{true};
    std::atomic<std::uint32_t> readers{0};
    std::uint32_t free = kNone;
    std::uint32_t retired_head = kNone, retired_tail = kNone;
    std::size_t active = 0, retired = 0, storage_bytes = 0;
    FdSlot& Slot(std::uint32_t fd) noexcept {
        return directory[fd / FdTable::kPageSize]->slots[fd % FdTable::kPageSize];
    }
    void QueueRetired(std::uint32_t index) noexcept {
        entries[index].next = kNone;
        if (retired_tail == kNone) {
            retired_head = index;
        } else {
            entries[retired_tail].next = index;
        }
        retired_tail = index;
    }
    bool Protected(FdTableEntry* entry) const noexcept {
        for (std::uint32_t i = 0; i < config.reader_capacity; ++i) {
            // 即使 Reader 正在归还注册也扫描指针，不能以独立的 registered 快照跳过保护。
            if (hazards[i].pointer.load(std::memory_order_seq_cst) == entry) {
                return true;
            }
        }
        return false;
    }
};
static_assert(std::atomic<FdTableEntry*>::is_always_lock_free);
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic<bool>::is_always_lock_free);
} // namespace detail
namespace {
constexpr unsigned kLookupAttempts = 8;
auto Error(FdError code) noexcept {
    return std::unexpected(code);
}
bool ValidId(FdBindingId id) noexcept {
    return id.table_id && id.fd >= 0 && id.generation;
}
bool ValidTarget(FdTarget target) noexcept {
    return target.owner_id && target.stream.table_id && target.stream.generation && !target.stream.reserved;
}
} // namespace

FdReader::FdReader(detail::FdTableState* state, std::uint32_t slot) noexcept : state_(state), slot_(slot) {}
FdReader::~FdReader() {
    Reset();
}
FdReader::FdReader(FdReader&& other) noexcept : state_(std::exchange(other.state_, nullptr)), slot_(other.slot_) {}
FdReader& FdReader::operator=(FdReader&& other) noexcept {
    if (this != &other) {
        Reset();
        state_ = std::exchange(other.state_, nullptr);
        slot_ = other.slot_;
    }
    return *this;
}
FdReader::operator bool() const noexcept {
    return state_ != nullptr;
}
void FdReader::Reset() noexcept {
    if (!state_) {
        return;
    }
    auto* state = std::exchange(state_, nullptr);
    state->hazards[slot_].pointer.store(nullptr, std::memory_order_seq_cst);
    state->readers.fetch_sub(1, std::memory_order_seq_cst);
    state->hazards[slot_].registered.store(false, std::memory_order_seq_cst);
}
FdLease::FdLease(detail::FdTableState* state, detail::FdTableEntry* entry) noexcept : state_(state), entry_(entry) {}
FdLease::~FdLease() {
    Reset();
}
FdLease::FdLease(FdLease&& other) noexcept
    : state_(std::exchange(other.state_, nullptr)), entry_(std::exchange(other.entry_, nullptr)) {}
FdLease& FdLease::operator=(FdLease&& other) noexcept {
    if (this != &other) {
        Reset();
        state_ = std::exchange(other.state_, nullptr);
        entry_ = std::exchange(other.entry_, nullptr);
    }
    return *this;
}
FdLease::operator bool() const noexcept {
    return entry_ != nullptr;
}
const FdBinding& FdLease::binding() const noexcept {
    return entry_->binding;
}
bool FdLease::current() const noexcept {
    return entry_ && state_->accepting.load(std::memory_order_seq_cst) &&
           state_->Slot(static_cast<std::uint32_t>(entry_->binding.id.fd)).published.load(std::memory_order_seq_cst) ==
               entry_;
}
void FdLease::Reset() noexcept {
    auto* entry = std::exchange(entry_, nullptr);
    state_ = nullptr;
    if (entry) {
        // 最后使用者只归还 CPU 引用；真正回收由 owner 扫描 Hazard 后完成。
        entry->references.fetch_sub(1, std::memory_order_seq_cst);
    }
}

FdTable::FdTable() noexcept = default;
FdTable::~FdTable() {
    StopAccepting();
    if (!Close()) {
        (void)state_.release();
    }
}
std::expected<void, FdError> FdTable::Open(const FdTableConfig& config) noexcept {
    if (state_ || opened_once_) {
        return Error(FdError::kInUse);
    }
    if (!config.table_id || !config.fd_limit || config.fd_limit > kMaxFdLimit || !config.binding_capacity ||
        config.binding_capacity > kMaxBindings || !config.reader_capacity || config.reader_capacity > kMaxReaders ||
        !config.initial_generation || !config.max_references || !config.memory_budget_bytes) {
        return Error(FdError::kInvalidArgument);
    }
    const auto pages = (config.fd_limit + kPageSize - 1) / kPageSize;
    const auto bytes = sizeof(detail::FdTableState) + pages * (sizeof(detail::FdPage) + sizeof(detail::FdPage*)) +
                       config.binding_capacity * sizeof(detail::FdTableEntry) +
                       config.reader_capacity * sizeof(detail::FdHazard);
    if (bytes > config.memory_budget_bytes) {
        return Error(FdError::kBudgetExceeded);
    }
    auto state = std::unique_ptr<detail::FdTableState>(new (std::nothrow) detail::FdTableState);
    if (!state) {
        return Error(FdError::kNoMemory);
    }
    state->directory.reset(new (std::nothrow) detail::FdPage*[pages]);
    state->pages.reset(new (std::nothrow) detail::FdPage[pages]);
    state->entries.reset(new (std::nothrow) detail::FdTableEntry[config.binding_capacity]);
    state->hazards.reset(new (std::nothrow) detail::FdHazard[config.reader_capacity]);
    if (!state->directory || !state->pages || !state->entries || !state->hazards) {
        return Error(FdError::kNoMemory);
    }
    state->config = config;
    state->storage_bytes = bytes;
    for (std::uint32_t i = 0; i < pages; ++i) {
        state->directory[i] = &state->pages[i];
    }
    for (std::uint32_t i = 0; i < config.fd_limit; ++i) {
        state->Slot(i).generation = config.initial_generation - 1;
    }
    for (std::uint32_t i = 0; i < config.binding_capacity; ++i) {
        state->entries[i].next = i + 1 < config.binding_capacity ? i + 1 : detail::kNone;
    }
    state->free = 0;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<FdBindingId, FdError> FdTable::Publish(int fd, FdTarget target) noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    auto& state = *state_;
    if (fd < 0 || static_cast<std::uint32_t>(fd) >= state.config.fd_limit || !ValidTarget(target)) {
        return Error(FdError::kInvalidArgument);
    }
    if (!state.accepting.load(std::memory_order_seq_cst)) {
        return Error(FdError::kStopped);
    }
    auto& slot = state.Slot(static_cast<std::uint32_t>(fd));
    if (slot.published.load(std::memory_order_seq_cst)) {
        return Error(FdError::kInUse);
    }
    if (slot.generation == std::numeric_limits<std::uint32_t>::max()) {
        return Error(FdError::kGenerationExhausted);
    }
    if (state.free == detail::kNone) {
        return Error(FdError::kFull);
    }
    auto& entry = state.entries[state.free];
    state.free = entry.next;
    entry.next = detail::kNone;
    entry.binding = {{state.config.table_id, fd, ++slot.generation}, target};
    entry.references.store(0, std::memory_order_seq_cst);
    entry.confirmed = false;
    entry.stage = detail::EntryStage::kPublished;
    ++state.active;
    slot.published.store(&entry, std::memory_order_seq_cst);
    return entry.binding.id;
}
std::expected<FdRetireToken, FdError> FdTable::Unbind(FdBindingId id) noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    auto& state = *state_;
    if (!ValidId(id) || static_cast<std::uint32_t>(id.fd) >= state.config.fd_limit) {
        return Error(FdError::kInvalidArgument);
    }
    if (id.table_id != state.config.table_id) {
        return Error(FdError::kStaleBinding);
    }
    auto& slot = state.Slot(static_cast<std::uint32_t>(id.fd));
    auto* entry = slot.published.load(std::memory_order_seq_cst);
    if (!entry) {
        return Error(FdError::kNotBound);
    }
    if (entry->binding.id != id) {
        return Error(FdError::kStaleBinding);
    }
    slot.published.store(nullptr, std::memory_order_seq_cst);
    entry->stage = detail::EntryStage::kRetired;
    const auto index = static_cast<std::uint32_t>(entry - state.entries.get());
    state.QueueRetired(index);
    --state.active;
    ++state.retired;
    return FdRetireToken{id, index};
}
std::expected<void, FdError> FdTable::ConfirmClosed(FdRetireToken token) noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    auto& state = *state_;
    if (token.slot >= state.config.binding_capacity) {
        return Error(FdError::kInvalidRetirement);
    }
    auto& entry = state.entries[token.slot];
    if (entry.stage != detail::EntryStage::kRetired || entry.binding.id != token.id) {
        return Error(FdError::kInvalidRetirement);
    }
    entry.confirmed = true;
    return {};
}
std::expected<std::size_t, FdError> FdTable::Collect(std::span<FdBinding> output, std::size_t budget) noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    if (output.empty() || !budget || budget > kMaxCollectBudget) {
        return Error(FdError::kInvalidArgument);
    }
    auto& state = *state_;
    const auto visits = std::min(budget, state.retired);
    std::size_t count = 0;
    for (std::size_t i = 0; i < visits && count < output.size(); ++i) {
        const auto index = state.retired_head;
        auto& entry = state.entries[index];
        state.retired_head = entry.next;
        if (state.retired_head == detail::kNone) {
            state.retired_tail = detail::kNone;
        }
        // 顺序不能反过来：读者在 HP 保护下增引用，再清 HP。先读 refs==0 再扫 HP
        // 可能同时漏掉交接前的 Hazard 和交接后的长期引用。所有发布/重检/扫描使用 SC。
        if (entry.confirmed && !state.Protected(&entry) && entry.references.load(std::memory_order_seq_cst) == 0) {
            output[count++] = entry.binding;
            entry.stage = detail::EntryStage::kFree;
            entry.next = state.free;
            state.free = index;
            --state.retired;
        } else {
            state.QueueRetired(index);
        }
    }
    return count;
}
std::expected<FdReader, FdError> FdTable::RegisterReader() noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    auto& state = *state_;
    if (!state.accepting.load(std::memory_order_seq_cst)) {
        return Error(FdError::kStopped);
    }
    for (std::uint32_t i = 0; i < state.config.reader_capacity; ++i) {
        bool empty = false;
        if (!state.hazards[i].registered.compare_exchange_strong(empty, true, std::memory_order_seq_cst)) {
            continue;
        }
        state.hazards[i].pointer.store(nullptr, std::memory_order_seq_cst);
        state.readers.fetch_add(1, std::memory_order_seq_cst);
        FdReader reader(&state, i);
        if (!state.accepting.load(std::memory_order_seq_cst)) {
            return Error(FdError::kStopped);
        }
        return reader;
    }
    return Error(FdError::kReaderFull);
}
std::expected<FdLease, FdError> FdTable::Lookup(FdReader& reader, int fd) noexcept {
    return LookupImpl(reader, fd, 0);
}
std::expected<FdLease, FdError> FdTable::Lookup(FdReader& reader, FdBindingId expected) noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    if (!ValidId(expected)) {
        return Error(FdError::kInvalidArgument);
    }
    if (expected.table_id != state_->config.table_id) {
        return Error(FdError::kStaleBinding);
    }
    return LookupImpl(reader, expected.fd, expected.generation);
}
std::expected<FdLease, FdError> FdTable::LookupImpl(FdReader& reader, int fd, std::uint32_t generation) noexcept {
    if (!state_) {
        return Error(FdError::kNotReady);
    }
    auto& state = *state_;
    if (reader.state_ != &state) {
        return Error(FdError::kInvalidReader);
    }
    if (fd < 0 || static_cast<std::uint32_t>(fd) >= state.config.fd_limit) {
        return Error(FdError::kInvalidArgument);
    }
    auto& slot = state.Slot(static_cast<std::uint32_t>(fd));
    auto& hazard = state.hazards[reader.slot_].pointer;
    for (unsigned attempt = 0; attempt < kLookupAttempts; ++attempt) {
        if (!state.accepting.load(std::memory_order_seq_cst)) {
            return Error(FdError::kStopped);
        }
        auto* entry = slot.published.load(std::memory_order_seq_cst);
        if (!entry) {
            return Error(FdError::kNotBound);
        }
        hazard.store(entry, std::memory_order_seq_cst);
        if (slot.published.load(std::memory_order_seq_cst) != entry) {
            hazard.store(nullptr, std::memory_order_seq_cst);
            continue;
        }
        // 指针发布并重检后才能读取不可变绑定；裸 fd 可取得新代，旧事件必须精确匹配。
        if (generation && entry->binding.id.generation != generation) {
            hazard.store(nullptr, std::memory_order_seq_cst);
            return Error(FdError::kStaleBinding);
        }
        auto references = entry->references.load(std::memory_order_seq_cst);
        bool pinned = false;
        for (unsigned pin = 0; pin < kLookupAttempts; ++pin) {
            if (references >= state.config.max_references) {
                hazard.store(nullptr, std::memory_order_seq_cst);
                return Error(FdError::kReferenceLimit);
            }
            if (entry->references.compare_exchange_weak(references, references + 1, std::memory_order_seq_cst)) {
                pinned = true;
                break;
            }
        }
        if (!pinned) {
            hazard.store(nullptr, std::memory_order_seq_cst);
            return Error(FdError::kContended);
        }
        // 取得引用后再检查解绑/停止；后续 Close 只能令 current() 变假，不能改写已固定的身份。
        const bool accepting = state.accepting.load(std::memory_order_seq_cst);
        if (!accepting || slot.published.load(std::memory_order_seq_cst) != entry) {
            entry->references.fetch_sub(1, std::memory_order_seq_cst);
            hazard.store(nullptr, std::memory_order_seq_cst);
            if (!accepting) {
                return Error(FdError::kStopped);
            }
            continue;
        }
        hazard.store(nullptr, std::memory_order_seq_cst);
        return FdLease(&state, entry);
    }
    return Error(FdError::kContended);
}
void FdTable::StopAccepting() noexcept {
    if (state_) {
        state_->accepting.store(false, std::memory_order_seq_cst);
    }
}
std::expected<void, FdError> FdTable::Close(bool clients_stopped) noexcept {
    if (!state_) {
        return {};
    }
    if (!clients_stopped) {
        return Error(FdError::kNotQuiescent);
    }
    StopAccepting();
    if (state_->active || state_->retired || state_->readers.load(std::memory_order_seq_cst)) {
        return Error(FdError::kRetained);
    }
    state_.reset();
    return {};
}
FdTableStats FdTable::stats() const noexcept {
    FdTableStats result;
    if (state_) {
        const auto& state = *state_;
        result.active = state.active;
        result.retired = state.retired;
        result.available = state.config.binding_capacity - state.active - state.retired;
        result.readers = state.readers.load(std::memory_order_seq_cst);
        for (std::uint32_t i = 0; i < state.config.binding_capacity; ++i) {
            result.references += state.entries[i].references.load(std::memory_order_seq_cst);
        }
        result.storage_bytes = state.storage_bytes;
        result.accepting = state.accepting.load(std::memory_order_seq_cst);
    }
    return result;
}
} // namespace socket
} // namespace kbsocket
