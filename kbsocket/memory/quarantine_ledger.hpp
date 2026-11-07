// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_MEMORY_QUARANTINE_LEDGER_HPP_
#define KBSOCKET_MEMORY_QUARANTINE_LEDGER_HPP_

#include <cstddef>
#include <expected>
#include <memory>

#include "kbsocket/memory/buffer_lease.hpp"

namespace kbsocket {
namespace memory {
/// Slab 在 Open 时为每个可借槽预留隔离记录，因此已有风险不会因隔离队列满而丢失。
/// 责任账本由 Slab 唯一 owner 持有；本类只记录隔离占用，不把时间或本地完成当作远端撤权。
class QuarantineLedger {
public:
    std::size_t size() const noexcept {
        return size_;
    }
    std::size_t bytes() const noexcept {
        return bytes_;
    }
    bool Contains(BufferLease lease) const noexcept;

private:
    friend class SlabAllocator;
    struct Record {
        BufferLease lease;
        std::size_t bytes = 0;
    };
    std::expected<void, MemoryError> Open(std::size_t capacity) noexcept;
    void Mark(BufferLease lease, std::size_t bytes) noexcept;
    void Remove(BufferLease lease) noexcept;
    std::unique_ptr<Record[]> records_;
    std::size_t capacity_ = 0;
    std::size_t size_ = 0;
    std::size_t bytes_ = 0;
};
} // namespace memory
} // namespace kbsocket
#endif // KBSOCKET_MEMORY_QUARANTINE_LEDGER_HPP_
