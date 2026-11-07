// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/memory/slab_allocator.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
// 仅在预热后的数据路径打开计数，排除 GTest 和控制路径的分配。
thread_local bool count_allocations = false;
thread_local std::size_t allocation_count = 0;
void RecordAllocation() {
    if (count_allocations) {
        ++allocation_count;
    }
}
} // namespace

void* operator new(std::size_t size) {
    RecordAllocation();
    if (auto* memory = std::malloc(size ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void* operator new(std::size_t size, std::align_val_t alignment) {
    RecordAllocation();
    void* memory = nullptr;
    if (::posix_memalign(&memory, static_cast<std::size_t>(alignment), size ? size : 1) != 0) {
        throw std::bad_alloc();
    }
    return memory;
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return ::operator new(size, alignment);
}
void operator delete(void* memory) noexcept {
    std::free(memory);
}
void operator delete[](void* memory) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}

namespace kbsocket {
namespace memory {
namespace {
class SlabAllocatorTest : public ::testing::Test {
protected:
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.register_seg = [](auto* context, auto* config) { return active_->Register(context, config); };
        functions.unregister_seg = [](auto* segment) { return active_->Unregister(segment); };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        for (std::size_t i = 0; i < contexts_.size(); ++i) {
            contexts_[i] = &context_storage_[i];
        }
    }
    void TearDown() override {
        count_allocations = false;
        fail_unregister_call_ = 0;
        EXPECT_TRUE(slab_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    urma_target_seg_t* Register(urma_context_t* context, urma_seg_cfg_t* config) {
        const auto index = register_calls_++;
        registrations_[index] = {context, *config};
        if (register_calls_ == fail_register_call_) {
            errno = ENOMEM;
            return nullptr;
        }
        segments_[index].urma_ctx = context;
        segments_[index].seg.ubva.va = config->va;
        segments_[index].seg.len = config->len;
        return &segments_[index];
    }
    urma_status_t Unregister(urma_target_seg_t* segment) {
        unregister_order_[unregister_calls_++] = segment;
        return unregister_calls_ == fail_unregister_call_ ? URMA_EAGAIN : URMA_SUCCESS;
    }
    std::span<urma_context_t* const> Contexts(std::size_t count = 4) const {
        return {contexts_.data(), count};
    }
    static SlabConfig Config(std::uint32_t buffers = 1, std::uint32_t references = 8) {
        SlabConfig config;
        config.buffer_count = buffers;
        config.buffer_size = 64;
        config.max_references = references;
        return config;
    }
    struct Registration {
        urma_context_t* context = nullptr;
        urma_seg_cfg_t config{};
    };
    static inline SlabAllocatorTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    std::size_t page_ = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    MemoryBudget budget_{page_ * 2};
    SlabAllocator slab_;
    std::array<urma_context_t, 5> context_storage_{};
    std::array<urma_context_t*, 5> contexts_{};
    std::array<Registration, 64> registrations_{};
    std::array<urma_target_seg_t, 64> segments_{};
    std::array<urma_target_seg_t*, 64> unregister_order_{};
    std::size_t register_calls_ = 0;
    std::size_t unregister_calls_ = 0;
    std::size_t fail_register_call_ = 0;
    std::size_t fail_unregister_call_ = 0;
};

TEST_F(SlabAllocatorTest, FourContextsShareBackingButHaveDistinctRegistrationHandles) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(3), budget_));
    ASSERT_EQ(register_calls_, 4u);
    EXPECT_EQ(budget_.reserved_bytes(), page_);
    EXPECT_EQ(slab_.stats().backing_bytes, page_);
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(registrations_[i].context, contexts_[i]);
        EXPECT_EQ(registrations_[i].config.va, registrations_[0].config.va);
        EXPECT_EQ(registrations_[i].config.va % page_, 0u);
        EXPECT_EQ(registrations_[i].config.len, page_);
        EXPECT_EQ(registrations_[i].config.flag.bs.access, static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE));
    }

    // 两个同时借出的片段必须互不重叠；每个 context 的 SGE 指向它自己的 MR。
    auto first = slab_.Acquire(37);
    auto second = slab_.Acquire(64);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    auto first_memory = slab_.Writable(*first);
    auto second_memory = slab_.Writable(*second);
    ASSERT_TRUE(first_memory);
    ASSERT_TRUE(second_memory);
    EXPECT_EQ(first_memory->size(), 37u);
    const auto first_address = reinterpret_cast<std::uintptr_t>(first_memory->data());
    const auto second_address = reinterpret_cast<std::uintptr_t>(second_memory->data());
    EXPECT_TRUE(first_address + 64 <= second_address || second_address + 64 <= first_address);
    auto reference = slab_.Reserve(*first, BufferUse::kLocalDma);
    ASSERT_TRUE(reference);
    for (std::size_t i = 0; i < 4; ++i) {
        auto sge = slab_.Sge(*reference, contexts_[i], 7, 30);
        ASSERT_TRUE(sge);
        EXPECT_EQ(sge->addr, first_address + 7);
        EXPECT_EQ(sge->len, 30u);
        EXPECT_EQ(sge->tseg, &segments_[i]);
    }
    ASSERT_TRUE(slab_.Cancel(*reference));
    ASSERT_TRUE(slab_.ReleaseCpu(*first));
    ASSERT_TRUE(slab_.ReleaseCpu(*second));
    ASSERT_TRUE(slab_.Close());
    ASSERT_EQ(unregister_calls_, 4u);
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(unregister_order_[i], &segments_[3 - i]);
    }
}

TEST_F(SlabAllocatorTest, SharedBudgetRejectsThirdBackingUntilOneSlabCloses) {
    SlabAllocator second;
    SlabAllocator third;
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    ASSERT_TRUE(second.Open(Contexts(), Config(), budget_));
    EXPECT_EQ(budget_.reserved_bytes(), budget_.limit_bytes());
    auto rejected = third.Open(Contexts(), Config(), budget_);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(register_calls_, 8u);
    EXPECT_FALSE(third.accepting());
    EXPECT_EQ(budget_.reserved_bytes(), page_ * 2);
    ASSERT_TRUE(slab_.Close());
    ASSERT_TRUE(third.Open(Contexts(), Config(), budget_));
    EXPECT_EQ(budget_.reserved_bytes(), page_ * 2);
    ASSERT_TRUE(second.Close());
    ASSERT_TRUE(third.Close());
}

TEST_F(SlabAllocatorTest, EveryLaterRegistrationFailureRollsBackInReverseOrder) {
    // 第 2/3/4 个 context 失败都不能发布半注册 Slab，也不能重复计费或泄漏成功的 MR。
    for (std::size_t failure = 2; failure <= 4; ++failure) {
        SCOPED_TRACE(failure);
        const auto register_start = register_calls_;
        const auto unregister_start = unregister_calls_;
        fail_register_call_ = register_start + failure;
        auto opened = slab_.Open(Contexts(), Config(), budget_);
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, MemoryErrorCode::kRegisterFailed);
        EXPECT_EQ(opened.error().provider_error, ENOMEM);
        EXPECT_EQ(opened.error().context_index, failure - 1);
        EXPECT_FALSE(slab_.accepting());
        EXPECT_EQ(unregister_calls_ - unregister_start, failure - 1);
        for (std::size_t i = 0; i < failure - 1; ++i) {
            EXPECT_EQ(unregister_order_[unregister_start + i], &segments_[register_start + failure - 2 - i]);
        }
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        EXPECT_FALSE(slab_.Acquire(1));
        ASSERT_TRUE(slab_.Close());
    }
    fail_register_call_ = 0;
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
}

TEST_F(SlabAllocatorTest, FailedRollbackRetainsBackingAndRetriesOnlyRemainingRegistrations) {
    fail_register_call_ = 4;
    // 回滚先成功注销 context 2，再在 context 1 停住；context 0 仍依赖同一 backing。
    fail_unregister_call_ = 2;
    auto opened = slab_.Open(Contexts(), Config(), budget_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, MemoryErrorCode::kRegisterFailed);
    EXPECT_EQ(opened.error().provider_error, ENOMEM);
    EXPECT_EQ(opened.error().cleanup_error, URMA_EAGAIN);
    EXPECT_EQ(unregister_calls_, 2u);
    EXPECT_EQ(budget_.reserved_bytes(), page_);
    EXPECT_FALSE(slab_.accepting());
    EXPECT_FALSE(slab_.Open(Contexts(), Config(), budget_));
    fail_unregister_call_ = 0;
    ASSERT_TRUE(slab_.Close());
    ASSERT_EQ(unregister_calls_, 4u);
    EXPECT_EQ(unregister_order_[0], &segments_[2]);
    EXPECT_EQ(unregister_order_[1], &segments_[1]);
    EXPECT_EQ(unregister_order_[2], &segments_[1]);
    EXPECT_EQ(unregister_order_[3], &segments_[0]);
    EXPECT_EQ(budget_.reserved_bytes(), 0u);
    ASSERT_TRUE(slab_.Close());
    EXPECT_EQ(unregister_calls_, 4u);
}

TEST_F(SlabAllocatorTest, CloseFailureRetainsBudgetAndDoesNotUnregisterCompletedStagesAgain) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    fail_unregister_call_ = 2;
    auto closed = slab_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, MemoryErrorCode::kUnregisterFailed);
    EXPECT_EQ(budget_.reserved_bytes(), page_);
    EXPECT_FALSE(slab_.accepting());
    EXPECT_FALSE(slab_.Acquire(1));
    EXPECT_EQ(unregister_calls_, 2u);
    fail_unregister_call_ = 0;
    ASSERT_TRUE(slab_.Close());
    ASSERT_EQ(unregister_calls_, 5u);
    EXPECT_EQ(unregister_order_[0], &segments_[3]);
    EXPECT_EQ(unregister_order_[1], &segments_[2]);
    EXPECT_EQ(unregister_order_[2], &segments_[2]);
    EXPECT_EQ(unregister_order_[3], &segments_[1]);
    EXPECT_EQ(unregister_order_[4], &segments_[0]);
}

TEST_F(SlabAllocatorTest, CloseWaitsForCpuDmaAndGrantInAllSixTerminationOrders) {
    std::array<unsigned, 3> order{0, 1, 2};
    do {
        ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
        auto lease = slab_.Acquire(17);
        ASSERT_TRUE(lease);
        auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
        auto grant = slab_.Reserve(*lease, BufferUse::kRemoteGrant);
        ASSERT_TRUE(dma);
        ASSERT_TRUE(grant);
        ASSERT_TRUE(slab_.Commit(*dma));
        ASSERT_TRUE(slab_.Commit(*grant));
        const auto unregister_before = unregister_calls_;
        // Close 必须先关准入，不能因其中一类责任结束就注销整块 DMA 内存。
        auto close = slab_.Close();
        ASSERT_FALSE(close);
        EXPECT_EQ(close.error().code, MemoryErrorCode::kInUse);
        EXPECT_FALSE(slab_.accepting());
        EXPECT_FALSE(slab_.Acquire(1));
        EXPECT_FALSE(slab_.Writable(*lease));
        for (std::size_t i = 0; i < order.size(); ++i) {
            if (order[i] == 0) {
                ASSERT_TRUE(slab_.ReleaseCpu(*lease));
            } else if (order[i] == 1) {
                ASSERT_TRUE(slab_.CompleteDma(*dma));
            } else {
                ASSERT_TRUE(slab_.EndGrant(*grant));
            }
            if (i < 2) {
                EXPECT_EQ(slab_.stats().available, 0u);
                EXPECT_FALSE(slab_.Close());
                EXPECT_EQ(unregister_calls_, unregister_before);
            }
        }
        EXPECT_EQ(slab_.stats().available, 1u);
        ASSERT_TRUE(slab_.Close());
        EXPECT_EQ(unregister_calls_, unregister_before + 4);
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST_F(SlabAllocatorTest, CopyDetachesCallerStorageAndStoppedReadableLeaseRemainsValid) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    std::array<std::byte, 17> input;
    input.fill(std::byte{0x42});
    auto lease = slab_.Copy(input);
    ASSERT_TRUE(lease);
    input.fill(std::byte{0xff});
    slab_.StopAccepting();
    auto readable = slab_.Readable(*lease);
    ASSERT_TRUE(readable);
    EXPECT_EQ(readable->size(), input.size());
    EXPECT_TRUE(std::all_of(readable->begin(), readable->end(), [](auto byte) { return byte == std::byte{0x42}; }));
    EXPECT_FALSE(slab_.Writable(*lease));
    EXPECT_FALSE(slab_.Reserve(*lease, BufferUse::kLocalDma));
    EXPECT_FALSE(slab_.Copy(input));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
}

TEST_F(SlabAllocatorTest, ReferencesRequireExplicitExposureAndMatchingTerminationType) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    auto lease = slab_.Acquire(32);
    ASSERT_TRUE(lease);
    auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
    auto grant = slab_.Reserve(*lease, BufferUse::kRemoteGrant);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(grant);
    EXPECT_FALSE(slab_.Writable(*lease));
    EXPECT_FALSE(slab_.Readable(*lease));
    // Reserved 尚未交给 provider，终结证据不能替代 Cancel；暴露后则严禁 Cancel。
    EXPECT_FALSE(slab_.CompleteDma(*dma));
    EXPECT_FALSE(slab_.EndGrant(*grant));
    ASSERT_TRUE(slab_.Cancel(*dma));
    EXPECT_FALSE(slab_.Cancel(*dma));
    EXPECT_EQ(slab_.stats().local_dma, 0u);
    ASSERT_TRUE(slab_.Commit(*grant));
    EXPECT_FALSE(slab_.Commit(*grant));
    EXPECT_FALSE(slab_.Cancel(*grant));
    EXPECT_FALSE(slab_.CompleteDma(*grant));
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(slab_.Commit(*dma));
    EXPECT_FALSE(slab_.EndGrant(*dma));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    EXPECT_FALSE(slab_.Reserve(*lease, BufferUse::kLocalDma));
    ASSERT_TRUE(slab_.CompleteDma(*dma));
    EXPECT_EQ(slab_.stats().available, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    ASSERT_TRUE(slab_.EndGrant(*grant));
    EXPECT_EQ(slab_.stats().available, 1u);
    EXPECT_FALSE(slab_.EndGrant(*grant));
}

TEST_F(SlabAllocatorTest, SgeChecksActualLengthContextAndExposureState) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    auto lease = slab_.Acquire(17);
    ASSERT_TRUE(lease);
    auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    EXPECT_FALSE(slab_.Sge(*dma, nullptr, 0, 1));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[4], 0, 1));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], 0, 0));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], 17, 1));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], 1, 17));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], std::numeric_limits<std::size_t>::max(), 2));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], 1, std::numeric_limits<std::size_t>::max()));
    ASSERT_TRUE(slab_.Sge(*dma, contexts_[0], 0, 17));
    ASSERT_TRUE(slab_.Commit(*dma));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], 0, 17));
    ASSERT_TRUE(slab_.CompleteDma(*dma));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
}

TEST_F(SlabAllocatorTest, StaleLeaseAndReferenceCannotReleaseReusedSlotOrReopenedSlab) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    auto old_lease = slab_.Acquire(32);
    ASSERT_TRUE(old_lease);
    auto old_dma = slab_.Reserve(*old_lease, BufferUse::kLocalDma);
    ASSERT_TRUE(old_dma);
    ASSERT_TRUE(slab_.Commit(*old_dma));
    ASSERT_TRUE(slab_.CompleteDma(*old_dma));
    ASSERT_TRUE(slab_.ReleaseCpu(*old_lease));
    for (unsigned iteration = 0; iteration < 2; ++iteration) {
        if (iteration == 1) {
            ASSERT_TRUE(slab_.Close());
            ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
        }
        auto lease = slab_.Acquire(32);
        ASSERT_TRUE(lease);
        EXPECT_NE(lease->generation, old_lease->generation);
        auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
        ASSERT_TRUE(dma);
        EXPECT_NE(dma->sequence, old_dma->sequence);
        EXPECT_FALSE(slab_.Writable(*old_lease));
        EXPECT_FALSE(slab_.ReleaseCpu(*old_lease));
        EXPECT_FALSE(slab_.Quarantine(*old_lease));
        EXPECT_FALSE(slab_.Cancel(*old_dma));
        EXPECT_FALSE(slab_.CompleteDma(*old_dma));
        EXPECT_EQ(slab_.stats().cpu_leases, 1u);
        EXPECT_EQ(slab_.stats().local_dma, 1u);
        ASSERT_TRUE(slab_.Cancel(*dma));
        ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    }
}

TEST_F(SlabAllocatorTest, OtherAllocatorAndMalformedIdentitiesCannotMutateLiveOwnership) {
    SlabAllocator other;
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    ASSERT_TRUE(other.Open(Contexts(), Config(), budget_));
    auto lease = slab_.Acquire(32);
    ASSERT_TRUE(lease);
    auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    EXPECT_FALSE(other.ReleaseCpu(*lease));
    EXPECT_FALSE(other.Cancel(*dma));
    EXPECT_FALSE(other.Sge(*dma, contexts_[0], 0, 1));
    auto forged_lease = *lease;
    forged_lease.slot = std::numeric_limits<std::uint32_t>::max();
    EXPECT_FALSE(slab_.ReleaseCpu(forged_lease));
    auto forged_reference = *dma;
    forged_reference.slot = std::numeric_limits<std::uint32_t>::max();
    EXPECT_FALSE(slab_.Cancel(forged_reference));
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    ASSERT_TRUE(slab_.Cancel(*dma));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    ASSERT_TRUE(other.Close());
}

TEST_F(SlabAllocatorTest, QuarantineCoversEverySlotAndPreservesAllThreeResponsibilities) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(3, 6), budget_));
    std::array<BufferLease, 3> leases{};
    std::array<BufferReference, 3> dmas{};
    std::array<BufferReference, 3> grants{};
    for (std::size_t i = 0; i < leases.size(); ++i) {
        auto lease = slab_.Acquire(i + 1);
        ASSERT_TRUE(lease);
        leases[i] = *lease;
        auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
        auto grant = slab_.Reserve(*lease, BufferUse::kRemoteGrant);
        ASSERT_TRUE(dma);
        ASSERT_TRUE(grant);
        dmas[i] = *dma;
        grants[i] = *grant;
        ASSERT_TRUE(slab_.Commit(*dma));
        ASSERT_TRUE(slab_.Commit(*grant));
        ASSERT_TRUE(slab_.Quarantine(*lease));
        ASSERT_TRUE(slab_.Quarantine(*lease));
        EXPECT_FALSE(slab_.Reserve(*lease, BufferUse::kLocalDma));
        EXPECT_FALSE(slab_.Writable(*lease));
        EXPECT_FALSE(slab_.Readable(*lease));
    }
    // 隔离记录按可借出槽位预留，重复隔离不会重复计数；占用按整个不可复用片段计。
    EXPECT_EQ(slab_.stats().quarantined_buffers, 3u);
    EXPECT_EQ(slab_.stats().quarantined_bytes, 3u * 64);
    EXPECT_EQ(slab_.stats().cpu_leases, 3u);
    EXPECT_EQ(slab_.stats().local_dma, 3u);
    EXPECT_EQ(slab_.stats().remote_grants, 3u);
    EXPECT_FALSE(slab_.Acquire(1));
    for (std::size_t i = 0; i < leases.size(); ++i) {
        ASSERT_TRUE(slab_.ReleaseCpu(leases[i]));
        ASSERT_TRUE(slab_.CompleteDma(dmas[i]));
        EXPECT_EQ(slab_.stats().available, i);
        ASSERT_TRUE(slab_.EndGrant(grants[i]));
        EXPECT_EQ(slab_.stats().available, i + 1);
        EXPECT_EQ(slab_.stats().quarantined_buffers, leases.size() - i - 1);
    }
    EXPECT_EQ(slab_.stats().quarantined_bytes, 0u);
}

TEST_F(SlabAllocatorTest, QuarantineBlocksPreparedSgeButAllowsUnexposedReferenceCancellation) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    auto lease = slab_.Acquire(17);
    ASSERT_TRUE(lease);
    auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(slab_.Quarantine(*lease));
    EXPECT_FALSE(slab_.Sge(*dma, contexts_[0], 0, 17));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    EXPECT_FALSE(slab_.Close());
    ASSERT_TRUE(slab_.Cancel(*dma));
    EXPECT_EQ(slab_.stats().available, 1u);
    EXPECT_EQ(slab_.stats().quarantined_buffers, 0u);
}

TEST_F(SlabAllocatorTest, AcceptanceUnknownReferenceCanBeCommittedAfterStopAndQuarantine) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    auto lease = slab_.Acquire(17);
    ASSERT_TRUE(lease);
    auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(slab_.Sge(*dma, contexts_[0], 0, 17));
    // 模拟 post 返回时发现接受情况未知；停机不能让已可能暴露的责任被当作未投递取消。
    slab_.StopAccepting();
    ASSERT_TRUE(slab_.Quarantine(*lease));
    ASSERT_TRUE(slab_.Commit(*dma));
    EXPECT_FALSE(slab_.Cancel(*dma));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    EXPECT_FALSE(slab_.Close());
    EXPECT_EQ(unregister_calls_, 0u);
    ASSERT_TRUE(slab_.CompleteDma(*dma));
    ASSERT_TRUE(slab_.Close());
}

TEST_F(SlabAllocatorTest, FixedReferenceCapacityDoesNotLoseCpuLeaseOrGrowWhenExhausted) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(1, 2), budget_));
    auto lease = slab_.Acquire(32);
    ASSERT_TRUE(lease);
    auto first = slab_.Reserve(*lease, BufferUse::kLocalDma);
    auto second = slab_.Reserve(*lease, BufferUse::kLocalDma);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    auto exhausted = slab_.Reserve(*lease, BufferUse::kRemoteGrant);
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(slab_.stats().local_dma, 2u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    ASSERT_TRUE(slab_.Cancel(*first));
    auto reused = slab_.Reserve(*lease, BufferUse::kRemoteGrant);
    ASSERT_TRUE(reused);
    EXPECT_NE(reused->sequence, first->sequence);
    EXPECT_FALSE(slab_.Cancel(*first));
    ASSERT_TRUE(slab_.Commit(*second));
    ASSERT_TRUE(slab_.Commit(*reused));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    ASSERT_TRUE(slab_.CompleteDma(*second));
    EXPECT_EQ(slab_.stats().available, 0u);
    ASSERT_TRUE(slab_.EndGrant(*reused));
    EXPECT_EQ(slab_.stats().available, 1u);
}

TEST_F(SlabAllocatorTest, RejectsInvalidContextsDimensionsAndLeaseLengthsBeforeRegistration) {
    EXPECT_FALSE(slab_.Open({}, Config(), budget_));
    EXPECT_FALSE(slab_.Open(Contexts(5), Config(), budget_));
    std::array<urma_context_t*, 2> duplicate{contexts_[0], contexts_[0]};
    EXPECT_FALSE(slab_.Open(duplicate, Config(), budget_));
    std::array<urma_context_t*, 2> missing{contexts_[0], nullptr};
    EXPECT_FALSE(slab_.Open(missing, Config(), budget_));
    EXPECT_FALSE(slab_.Open(Contexts(), Config(0), budget_));
    auto zero_bytes = Config();
    zero_bytes.buffer_size = 0;
    EXPECT_FALSE(slab_.Open(Contexts(), zero_bytes, budget_));
    EXPECT_FALSE(slab_.Open(Contexts(), Config(1, 0), budget_));
    auto overflow = Config(2);
    overflow.buffer_size = std::numeric_limits<std::size_t>::max();
    EXPECT_FALSE(slab_.Open(Contexts(), overflow, budget_));
    EXPECT_EQ(register_calls_, 0u);
    EXPECT_EQ(budget_.reserved_bytes(), 0u);
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    EXPECT_FALSE(slab_.Acquire(0));
    EXPECT_FALSE(slab_.Acquire(65));
    EXPECT_FALSE(slab_.Copy({}));
    EXPECT_EQ(slab_.stats().available, 1u);
}

TEST_F(SlabAllocatorTest, LeaseReferenceAndQuarantineFastPathDoesNotAllocate) {
    ASSERT_TRUE(slab_.Open(Contexts(), Config(), budget_));
    std::array<std::byte, 17> bytes{};
    bool succeeded = true;
    allocation_count = 0;
    count_allocations = true;
    for (unsigned i = 0; i < 64; ++i) {
        auto lease = slab_.Copy(bytes);
        if (!lease) {
            succeeded = false;
            break;
        }
        auto dma = slab_.Reserve(*lease, BufferUse::kLocalDma);
        auto grant = slab_.Reserve(*lease, BufferUse::kRemoteGrant);
        if (!dma || !grant) {
            succeeded = false;
            break;
        }
        succeeded &= slab_.Sge(*dma, contexts_[i % 4], 0, bytes.size()).has_value();
        succeeded &= slab_.Commit(*dma).has_value();
        succeeded &= slab_.Commit(*grant).has_value();
        succeeded &= slab_.Quarantine(*lease).has_value();
        succeeded &= slab_.ReleaseCpu(*lease).has_value();
        succeeded &= slab_.EndGrant(*grant).has_value();
        succeeded &= slab_.CompleteDma(*dma).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(register_calls_, 4u);
    EXPECT_EQ(unregister_calls_, 0u);
    EXPECT_EQ(slab_.stats().available, 1u);
}
} // namespace
} // namespace memory
} // namespace kbsocket
