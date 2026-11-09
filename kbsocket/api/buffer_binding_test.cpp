// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/buffer_binding.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

extern "C" int KbsBufferCHeaderTest(void);

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
    }
}
} // namespace
void* operator new(std::size_t size) {
    RecordAllocation();
    if (void* pointer = std::malloc(size ? size : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void* operator new(std::size_t size, std::align_val_t alignment) {
    RecordAllocation();
    void* pointer = nullptr;
    if (!::posix_memalign(&pointer, static_cast<std::size_t>(alignment), size ? size : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return ::operator new(size, alignment);
}
void operator delete(void* pointer) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    std::free(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    std::free(pointer);
}

namespace kbsocket {
namespace api {
namespace {
kbs_buffer_pool_config HostConfig(std::uint64_t table_id = 11, std::uint32_t capacity = 2) {
    return {KBS_BUFFER_ABI_VERSION, 0, table_id, capacity, 0, 64, 1u << 20};
}
template <class T>
void ExpectError(const std::expected<T, kbs_stream_error>& result, int code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
void ExpectEmpty(kbs_buffer_handle buffer) {
    EXPECT_EQ(buffer.table_id, 0u);
    EXPECT_EQ(buffer.generation, 0u);
    EXPECT_EQ(buffer.slot, 0u);
    EXPECT_EQ(buffer.reserved, 0u);
}

TEST(BufferBindingTest, CHeaderAndFullCRoundTripUseExactLeaseLength) {
    ASSERT_EQ(KbsBufferCHeaderTest(), 0);
    BufferBinding binding;
    ASSERT_TRUE(binding.OpenHost(HostConfig()));
    kbs_buffer_handle handle{};
    kbs_stream_error error{};
    ASSERT_EQ(kbs_buffer_acquire(binding.table(), 17, &handle, &error), 0);
    EXPECT_EQ(error.code, 0);
    EXPECT_EQ(handle.table_id, 11u);
    EXPECT_NE(handle.generation, 0u);
    EXPECT_EQ(binding.published(), 1u);

    kbs_buffer_view writable{};
    ASSERT_EQ(kbs_buffer_writable(binding.table(), handle, &writable, &error), 0);
    ASSERT_EQ(writable.size, 17u);
    auto* bytes = static_cast<std::byte*>(writable.data);
    for (std::size_t i = 0; i < writable.size; ++i) {
        bytes[i] = static_cast<std::byte>(i + 1);
    }
    kbs_buffer_const_view readonly{};
    ASSERT_EQ(kbs_buffer_readable(binding.table(), handle, &readonly, &error), 0);
    EXPECT_EQ(readonly.data, writable.data);
    ASSERT_EQ(readonly.size, 17u);
    for (std::size_t i = 0; i < readonly.size; ++i) {
        EXPECT_EQ(static_cast<const std::byte*>(readonly.data)[i], static_cast<std::byte>(i + 1));
    }
    // 取多个视图没有复制所有权，一次 Release 立即使全部句柄副本失效。
    ASSERT_EQ(kbs_buffer_release(binding.table(), handle, &error), 0);
    EXPECT_EQ(binding.published(), 0u);
    EXPECT_EQ(kbs_buffer_release(binding.table(), handle, &error), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(kbs_buffer_readable(binding.table(), handle, &readonly, &error), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(readonly.data, nullptr);
    EXPECT_EQ(readonly.size, 0u);
    ASSERT_TRUE(binding.Close());
}

TEST(BufferBindingTest, LengthCapacityAndInvalidArgumentsLeaveEmptyOutputs) {
    BufferBinding binding;
    ASSERT_TRUE(binding.OpenHost(HostConfig(12, 1)));
    kbs_stream_error error{};
    kbs_buffer_handle handle{1, 2, 3, 4};
    EXPECT_EQ(kbs_buffer_acquire(binding.table(), 0, &handle, &error), KBS_STREAM_INVALID_ARGUMENT);
    ExpectEmpty(handle);
    handle = {1, 2, 3, 4};
    EXPECT_EQ(kbs_buffer_acquire(binding.table(), 65, &handle, &error), KBS_STREAM_INVALID_ARGUMENT);
    ExpectEmpty(handle);
    EXPECT_EQ(kbs_buffer_acquire(binding.table(), 8, nullptr, &error), KBS_STREAM_INVALID_ARGUMENT);
    ASSERT_EQ(kbs_buffer_acquire(binding.table(), 64, &handle, &error), 0);
    auto extra = handle;
    EXPECT_EQ(kbs_buffer_acquire(binding.table(), 1, &extra, &error), KBS_STREAM_EXHAUSTED);
    ExpectEmpty(extra);
    EXPECT_EQ(binding.published(), 1u);
    EXPECT_EQ(kbs_buffer_writable(binding.table(), handle, nullptr, &error), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_buffer_readable(binding.table(), handle, nullptr, &error), KBS_STREAM_INVALID_ARGUMENT);
    ASSERT_EQ(kbs_buffer_release(binding.table(), handle, nullptr), 0);
    ASSERT_TRUE(binding.Close());
}

TEST(BufferBindingTest, RejectsCrossTableFullGenerationReservedAndStaleSlotHandles) {
    BufferBinding first, second;
    ASSERT_TRUE(first.OpenHost(HostConfig(13, 1)));
    ASSERT_TRUE(second.OpenHost(HostConfig(14, 1)));
    const auto initial = first.Acquire(20);
    ASSERT_TRUE(initial);
    const auto other = second.Acquire(20);
    ASSERT_TRUE(other);
    ExpectError(second.Release(*initial), KBS_STREAM_INVALID_HANDLE);

    std::array<kbs_buffer_handle, 5> invalid{};
    invalid.fill(*initial);
    invalid[0].table_id = 0;
    invalid[1].generation = 0;
    invalid[2].generation ^= std::uint64_t{1} << 48;
    invalid[3].slot = std::numeric_limits<std::uint32_t>::max();
    invalid[4].reserved = 1;
    for (const auto forged : invalid) {
        ExpectError(first.Readable(forged), KBS_STREAM_INVALID_HANDLE);
        ExpectError(first.Writable(forged), KBS_STREAM_INVALID_HANDLE);
        ExpectError(first.Resolve(forged), KBS_STREAM_INVALID_HANDLE);
        ExpectError(first.Release(forged), KBS_STREAM_INVALID_HANDLE);
    }
    ASSERT_TRUE(first.Release(*initial));
    const auto reused = first.Acquire(21);
    ASSERT_TRUE(reused);
    EXPECT_EQ(reused->slot, initial->slot);
    EXPECT_NE(reused->generation, initial->generation);
    ExpectError(first.Release(*initial), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(first.published(), 1u);
    ASSERT_TRUE(first.Release(*reused));
    ASSERT_TRUE(second.Release(*other));
    ASSERT_TRUE(first.Close());
    ASSERT_TRUE(second.Close());
}

TEST(BufferBindingTest, FailedConfigurationCanRetryButSuccessfulLifetimeCannotReopen) {
    BufferBinding binding;
    auto config = HostConfig();
    config.abi_version = KBS_BUFFER_ABI_VERSION + 1;
    ExpectError(binding.OpenHost(config), KBS_STREAM_INVALID_ARGUMENT);
    config = HostConfig();
    config.reserved2 = 1;
    ExpectError(binding.OpenHost(config), KBS_STREAM_INVALID_ARGUMENT);
    config = HostConfig();
    config.memory_budget_bytes = 1;
    ExpectError(binding.OpenHost(config), KBS_STREAM_EXHAUSTED);
    EXPECT_EQ(binding.table(), nullptr);
    ASSERT_TRUE(binding.OpenHost(HostConfig()));
    auto* table = binding.table();
    ASSERT_NE(table, nullptr);
    ASSERT_TRUE(binding.Close());
    kbs_buffer_handle out{1, 2, 3, 4};
    EXPECT_EQ(kbs_buffer_acquire(table, 1, &out, nullptr), KBS_STREAM_NOT_READY);
    ExpectEmpty(out);
    EXPECT_FALSE(binding.OpenHost(HostConfig(100)));
    ASSERT_TRUE(binding.Close());
}

TEST(BufferBindingTest, BusyCloseStopsWritesButKeepsReadonlyAndReturnAvailable) {
    BufferBinding binding;
    ASSERT_TRUE(binding.OpenHost(HostConfig()));
    const auto handle = binding.Acquire(12);
    ASSERT_TRUE(handle);
    const auto before = binding.Writable(*handle);
    ASSERT_TRUE(before);
    before->front() = std::byte{0x5a};
    auto* table = binding.table();
    ExpectError(binding.Close(), KBS_STREAM_BUSY);
    ExpectError(binding.Acquire(8), KBS_STREAM_STOPPED);
    ExpectError(binding.Writable(*handle), KBS_STREAM_STOPPED);
    const auto readable = binding.Readable(*handle);
    ASSERT_TRUE(readable);
    EXPECT_EQ(readable->front(), std::byte{0x5a});
    ASSERT_TRUE(binding.Release(*handle));
    ASSERT_TRUE(binding.Close());
    kbs_buffer_const_view out{readable->data(), 12};
    EXPECT_EQ(kbs_buffer_readable(table, *handle, &out, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(out.data, nullptr);
    EXPECT_EQ(out.size, 0u);
}

TEST(BufferBindingTest, InspectTracksExactLengthAndSurvivesStopButRejectsStaleAndClosedIdentity) {
    BufferBinding binding;
    ASSERT_TRUE(binding.OpenHost(HostConfig()));
    const auto handle = binding.Acquire(17);
    ASSERT_TRUE(handle);
    auto* table = binding.table();
    kbs_buffer_status status{99, 0, 99};
    kbs_stream_error error{};
    ASSERT_EQ(kbs_buffer_inspect(table, *handle, &status, &error), 0);
    EXPECT_EQ(status.bytes, 17u);
    EXPECT_EQ(status.source_reusable, 1u);
    EXPECT_EQ(status.reserved, 0u);
    EXPECT_EQ(error.code, 0);
    EXPECT_EQ(kbs_buffer_inspect(table, *handle, nullptr, &error), KBS_STREAM_INVALID_ARGUMENT);
    binding.StopAccepting();
    ASSERT_EQ(kbs_buffer_inspect(table, *handle, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    ASSERT_TRUE(binding.Release(*handle));
    status = {99, 1, 99};
    EXPECT_EQ(kbs_buffer_inspect(table, *handle, &status, nullptr), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(status.bytes, 0u);
    EXPECT_EQ(status.source_reusable, 0u);
    EXPECT_EQ(status.reserved, 0u);
    ASSERT_TRUE(binding.Close());
    status = {99, 1, 99};
    EXPECT_EQ(kbs_buffer_inspect(table, *handle, &status, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(status.bytes, 0u);
    EXPECT_EQ(status.source_reusable, 0u);
    EXPECT_EQ(status.reserved, 0u);
}

TEST(BufferBindingTest, PreallocatedFastPathDoesNotAllocate) {
    BufferBinding binding;
    ASSERT_TRUE(binding.OpenHost(HostConfig()));
    allocations = 0;
    bool success = true;
    // 不在计数区间调用 GTest；每次重复真实 Acquire/View/Release，迫使槽位与代次持续复用。
    count_allocations = true;
    for (std::size_t i = 0; i < 256; ++i) {
        kbs_buffer_handle handle{};
        kbs_buffer_view writable{};
        kbs_buffer_const_view readable{};
        kbs_buffer_status status{};
        if (kbs_buffer_acquire(binding.table(), 32, &handle, nullptr) ||
            kbs_buffer_writable(binding.table(), handle, &writable, nullptr) ||
            kbs_buffer_readable(binding.table(), handle, &readable, nullptr) ||
            kbs_buffer_inspect(binding.table(), handle, &status, nullptr) || status.bytes != 32 ||
            status.source_reusable != 1 || kbs_buffer_release(binding.table(), handle, nullptr)) {
            success = false;
            break;
        }
    }
    count_allocations = false;
    EXPECT_TRUE(success);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(binding.published(), 0u);
    ASSERT_TRUE(binding.Close());
}

class RegisteredBufferBindingTest : public ::testing::Test {
protected:
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.register_seg = [](auto* context, auto* config) {
            ++active_->register_calls_;
            active_->segment_.urma_ctx = context;
            active_->segment_.seg.ubva.va = config->va;
            active_->segment_.seg.len = config->len;
            return &active_->segment_;
        };
        functions.unregister_seg = [](auto*) {
            ++active_->unregister_calls_;
            return URMA_SUCCESS;
        };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        const std::array<urma_context_t*, 1> contexts{&context_};
        ASSERT_TRUE(slab_.Open(contexts, {.buffer_count = 3, .buffer_size = 64, .max_references = 8}, budget_));
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(binding_.Close());
        EXPECT_TRUE(slab_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        EXPECT_EQ(register_calls_, 1u);
        EXPECT_EQ(unregister_calls_, 1u);
        active_ = nullptr;
    }
    static inline RegisteredBufferBindingTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_target_seg_t segment_{};
    std::size_t register_calls_ = 0, unregister_calls_ = 0;
    memory::MemoryBudget budget_{1u << 20};
    memory::SlabAllocator slab_;
    BufferBinding binding_;
};

TEST_F(RegisteredBufferBindingTest, PublishedTableCannotAddressPrivateStagingLeaseAndCapacityDoesNotBorrow) {
    // 先让内部 staging 占据物理槽 0，公开槽 0 随后必须映射到另一物理槽，不能透传 token。
    const auto staging = slab_.Acquire(64);
    ASSERT_TRUE(staging);
    ASSERT_TRUE(binding_.OpenSlab(slab_, {21, 1}));
    const auto published = binding_.Acquire(17);
    ASSERT_TRUE(published);
    const auto lease = binding_.Resolve(*published);
    ASSERT_TRUE(lease);
    EXPECT_NE(lease->slot, staging->slot);
    const auto before = slab_.stats();
    ExpectError(binding_.Acquire(1), KBS_STREAM_EXHAUSTED);
    EXPECT_EQ(slab_.stats().cpu_leases, before.cpu_leases);
    EXPECT_EQ(slab_.stats().available, before.available);

    const kbs_buffer_handle fabricated{21, staging->generation, staging->slot, 0};
    // 即使数值恰好命中一个公开身份，也只能释放该公开映射，不能释放私有 staging。
    const auto attempt = binding_.Release(fabricated);
    if (!attempt) {
        EXPECT_EQ(attempt.error().code, KBS_STREAM_INVALID_HANDLE);
        ASSERT_TRUE(binding_.Release(*published));
    }
    EXPECT_TRUE(slab_.Readable(*staging));
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    ASSERT_TRUE(slab_.ReleaseCpu(*staging));
}

TEST_F(RegisteredBufferBindingTest, StoppingBorrowedBindingDoesNotStopSharedSlab) {
    ASSERT_TRUE(binding_.OpenSlab(slab_, {22, 2}));
    const auto published = binding_.Acquire(15);
    ASSERT_TRUE(published);
    binding_.StopAccepting();
    ExpectError(binding_.Acquire(1), KBS_STREAM_STOPPED);
    ExpectError(binding_.Writable(*published), KBS_STREAM_STOPPED);
    EXPECT_TRUE(binding_.Readable(*published));
    EXPECT_TRUE(slab_.accepting());
    const auto staging = slab_.Acquire(8);
    ASSERT_TRUE(staging);
    ASSERT_TRUE(slab_.ReleaseCpu(*staging));
    ASSERT_TRUE(binding_.Release(*published));
    ASSERT_TRUE(binding_.Close());
    EXPECT_TRUE(slab_.accepting());
}

TEST_F(RegisteredBufferBindingTest, CpuReturnDoesNotEndDmaOrRemoteGrant) {
    ASSERT_TRUE(binding_.OpenSlab(slab_, {23, 1}));
    const auto published = binding_.Acquire(32);
    ASSERT_TRUE(published);
    const auto lease = binding_.Resolve(*published);
    ASSERT_TRUE(lease);
    const auto dma = slab_.Reserve(*lease, memory::BufferUse::kLocalDma);
    const auto grant = slab_.Reserve(*lease, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(slab_.Commit(*dma));
    ASSERT_TRUE(slab_.Commit(*grant));
    ExpectError(binding_.Writable(*published), KBS_STREAM_BUSY);
    ExpectError(binding_.Readable(*published), KBS_STREAM_BUSY);
    ExpectError(binding_.Resolve(*published), KBS_STREAM_BUSY);
    ASSERT_TRUE(binding_.Release(*published));
    ExpectError(binding_.Release(*published), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_EQ(slab_.stats().available, 2u);
    // 公共表可以关闭，但底层 Slab 必须继续保留；DMA 结束也不能替代独立 Grant 终结。
    ASSERT_TRUE(binding_.Close());
    ASSERT_TRUE(slab_.CompleteDma(*dma));
    EXPECT_EQ(slab_.stats().available, 2u);
    const auto close = slab_.Close();
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error().code, memory::MemoryErrorCode::kInUse);
    EXPECT_EQ(unregister_calls_, 0u);
    ASSERT_TRUE(slab_.EndGrant(*grant));
    EXPECT_EQ(slab_.stats().available, 3u);
}

TEST_F(RegisteredBufferBindingTest, InspectWaitsForEveryReservedAndCommittedSourceResponsibility) {
    ASSERT_TRUE(binding_.OpenSlab(slab_, {25, 1}));
    const auto handle = binding_.Acquire(32);
    ASSERT_TRUE(handle);
    const auto lease = binding_.Resolve(*handle);
    ASSERT_TRUE(lease);
    const auto dma = slab_.Reserve(*lease, memory::BufferUse::kLocalDma);
    const auto grant = slab_.Reserve(*lease, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(grant);
    kbs_buffer_status status{};
    // 尚未提交的队列责任也禁止源复用；查询成功不等于能够取视图。
    ASSERT_EQ(kbs_buffer_inspect(binding_.table(), *handle, &status, nullptr), 0);
    EXPECT_EQ(status.bytes, 32u);
    EXPECT_EQ(status.source_reusable, 0u);
    ASSERT_TRUE(slab_.Commit(*dma));
    ASSERT_TRUE(slab_.Commit(*grant));
    ASSERT_TRUE(slab_.CompleteDma(*dma));
    ASSERT_EQ(kbs_buffer_inspect(binding_.table(), *handle, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 0u);
    ASSERT_TRUE(slab_.EndGrant(*grant));
    ASSERT_EQ(kbs_buffer_inspect(binding_.table(), *handle, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    EXPECT_TRUE(binding_.Writable(*handle));
    ASSERT_TRUE(binding_.Release(*handle));
}

TEST_F(RegisteredBufferBindingTest, QuarantinedLeaseCannotBeViewedButCpuCanReturn) {
    ASSERT_TRUE(binding_.OpenSlab(slab_, {24, 1}));
    const auto handle = binding_.Acquire(20);
    ASSERT_TRUE(handle);
    const auto lease = binding_.Resolve(*handle);
    ASSERT_TRUE(lease);
    ASSERT_TRUE(slab_.Quarantine(*lease));
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(binding_.table(), *handle, &status, nullptr), 0);
    EXPECT_EQ(status.bytes, 20u);
    EXPECT_EQ(status.source_reusable, 0u);
    ExpectError(binding_.Readable(*handle), KBS_STREAM_BUSY);
    ExpectError(binding_.Writable(*handle), KBS_STREAM_BUSY);
    ASSERT_TRUE(binding_.Release(*handle));
    EXPECT_EQ(slab_.stats().quarantined_buffers, 0u);
    EXPECT_EQ(slab_.stats().available, 3u);
}
} // namespace
} // namespace api
} // namespace kbsocket
