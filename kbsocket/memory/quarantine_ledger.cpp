// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/memory/quarantine_ledger.hpp"

#include <new>

namespace kbsocket {
namespace memory {
std::expected<void, MemoryError> QuarantineLedger::Open(std::size_t capacity) noexcept {
    if (!capacity || records_) {
        return std::unexpected(MemoryError{MemoryErrorCode::kInvalidArgument});
    }
    records_.reset(new (std::nothrow) Record[capacity]);
    if (!records_) {
        return std::unexpected(MemoryError{MemoryErrorCode::kNoMemory});
    }
    capacity_ = capacity;
    return {};
}
bool QuarantineLedger::Contains(BufferLease lease) const noexcept {
    return lease.generation && lease.slot < capacity_ && records_[lease.slot].lease == lease;
}
void QuarantineLedger::Mark(BufferLease lease, std::size_t bytes) noexcept {
    // 仅 Slab 对已核验的当前槽调用；每槽一个记录，重复隔离幂等。
    if (Contains(lease)) {
        return;
    }
    records_[lease.slot] = {lease, bytes};
    ++size_;
    bytes_ += bytes;
}
void QuarantineLedger::Remove(BufferLease lease) noexcept {
    if (!Contains(lease)) {
        return;
    }
    bytes_ -= records_[lease.slot].bytes;
    --size_;
    records_[lease.slot] = {};
}
} // namespace memory
} // namespace kbsocket
