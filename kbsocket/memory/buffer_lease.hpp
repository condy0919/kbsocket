// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_MEMORY_BUFFER_LEASE_HPP_
#define KBSOCKET_MEMORY_BUFFER_LEASE_HPP_

#include <cstddef>
#include <cstdint>
#include <optional>

namespace kbsocket {
namespace memory {
class SlabAllocator;

enum class MemoryErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kExhausted,
    kNoMemory,
    kRegisterFailed,
    kUnregisterFailed,
    kInvalidLease,
    kInvalidReference,
    kSequenceExhausted,
    kQueueFull,
    kSystem,
    kNotQuiescent
};
struct MemoryError {
    MemoryErrorCode code;
    int provider_error = 0;
    std::size_t context_index = 0;
    std::optional<int> cleanup_error;
};

/// 一个 CPU 所有权；复制描述符不会增加引用，成功直接/排队归还后全部副本均不得访问 payload。
/// allocator 必须覆盖所有 Lease、DMA、Grant 和排队归还的生命周期；对象销毁后不能复用旧身份。
struct BufferLease {
    const SlabAllocator* allocator = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const BufferLease&) const = default;
};

enum class BufferUse : std::uint8_t { kLocalDma, kRemoteGrant };

/// 每份 DMA/Grant 责任都有独立、不复用的票据；同一 Lease 可被多个 WR 或 Grant 共同持有。
struct BufferReference {
    const SlabAllocator* allocator = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t sequence = 0;
    bool operator==(const BufferReference&) const = default;
};
} // namespace memory
} // namespace kbsocket
#endif // KBSOCKET_MEMORY_BUFFER_LEASE_HPP_
