// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/memory/lease_return_progress.hpp"

#include <poll.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <optional>
#include <span>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/runtime/control_executor.hpp"
#include "kbsocket/runtime/shard_reactor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace memory {
namespace {

bool FdReadable(int fd) {
    pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
    int status;
    do {
        status = ::poll(&descriptor, 1, 0);
    } while (status < 0 && errno == EINTR);
    return status == 1 && (descriptor.revents & POLLIN) != 0;
}

class LeaseReturnProgressTest : public ::testing::Test {
protected:
    struct Returned {
        BufferLease lease;
        std::optional<MemoryError> error;
        std::thread::id thread;
    };
    struct Reference {
        BufferReference token;
        BufferUse use;
    };

    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.register_seg = [](urma_context_t* context, urma_seg_cfg_t* config) {
            auto& segment = active_->segments_[active_->register_calls_++];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            return &segment;
        };
        functions.unregister_seg = [](urma_target_seg_t*) {
            ++active_->unregister_calls_;
            return URMA_SUCCESS;
        };
        return functions;
    }
    static void OnReturned(void* cookie, BufferLease lease, std::optional<MemoryError> error) noexcept {
        auto& self = *static_cast<LeaseReturnProgressTest*>(cookie);
        if (self.returned_count_ < self.returned_.size()) {
            self.returned_[self.returned_count_++] = {lease, error, std::this_thread::get_id()};
        } else {
            self.callback_overflow_ = true;
        }
    }
    void SetUp() override {
        active_ = this;
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        for (std::size_t i = 0; i < slabs_.size(); ++i) {
            contexts_[i] = &context_storage_[i];
            pointers_[i] = &slabs_[i];
            SlabConfig config;
            config.buffer_count = 1;
            config.buffer_size = 64;
            config.max_references = 4;
            ASSERT_TRUE(slabs_[i].Open(std::span(contexts_).subspan(i, 1), config, budget_));
        }
    }
    void TearDown() override {
        // 先从 reactor 移除借用源，再归还所有已接受 token；线程在测试体中已 join。
        EXPECT_TRUE(reactor_.Close());
        queue_.StopAccepting();
        while (queue_.pending()) {
            auto result = progress_.Progress(runtime::ProgressKind::kMailbox, 64, 0);
            ASSERT_TRUE(result);
            ASSERT_GT(result->processed, 0u);
        }
        EXPECT_TRUE(progress_.Close(true));
        EXPECT_TRUE(queue_.Close(true));

        // 失败断言也终结测试自己持有的责任；重复清理被 generation/sequence 拒绝即可。
        for (std::size_t i = 0; i < reference_count_; ++i) {
            auto& reference = references_[i];
            auto* slab = const_cast<SlabAllocator*>(reference.token.allocator);
            if (reference.use == BufferUse::kLocalDma) {
                (void)slab->CompleteDma(reference.token);
            } else {
                (void)slab->EndGrant(reference.token);
            }
            (void)slab->Cancel(reference.token);
        }
        for (std::size_t i = 0; i < acquired_count_; ++i) {
            auto* slab = const_cast<SlabAllocator*>(acquired_[i].allocator);
            (void)slab->ReleaseCpu(acquired_[i]);
        }
        for (auto& slab : slabs_) {
            EXPECT_TRUE(slab.Close());
        }
        EXPECT_TRUE(executor_.Close());
        EXPECT_FALSE(callback_overflow_);
        EXPECT_EQ(unregister_calls_, register_calls_);
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }

    bool Open(std::size_t budget = 2) {
        if (!queue_.Open(slabs_.size()) || !progress_.Open(queue_, pointers_, OnReturned, this)) {
            return false;
        }
        std::array<runtime::ProgressSource*, 1> sources{&progress_};
        return reactor_
            .Open(sources, executor_, {.events = true, .budget = budget, .busy_poll_ns = 0, .max_wait_ms = 1})
            .has_value();
    }
    std::expected<BufferLease, MemoryError> Acquire(std::size_t slab) {
        auto acquired = slabs_[slab].Acquire(32);
        if (acquired) {
            acquired_[acquired_count_++] = *acquired;
        }
        return acquired;
    }
    std::expected<BufferReference, MemoryError> Reserve(std::size_t slab, BufferLease lease, BufferUse use) {
        auto reference = slabs_[slab].Reserve(lease, use);
        if (reference) {
            references_[reference_count_++] = {*reference, use};
        }
        return reference;
    }

    static inline LeaseReturnProgressTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    MemoryBudget budget_{static_cast<std::size_t>(::sysconf(_SC_PAGESIZE)) * 4};
    std::array<urma_context_t, 4> context_storage_{};
    std::array<urma_context_t*, 4> contexts_{};
    std::array<urma_target_seg_t, 4> segments_{};
    std::array<SlabAllocator, 4> slabs_;
    std::array<SlabAllocator*, 4> pointers_{};
    LeaseReturnQueue queue_;
    LeaseReturnProgress progress_;
    runtime::ControlExecutor executor_;
    runtime::ShardReactor reactor_;
    std::array<Returned, 32> returned_{};
    std::array<BufferLease, 16> acquired_{};
    std::array<Reference, 8> references_{};
    std::size_t register_calls_ = 0;
    std::size_t unregister_calls_ = 0;
    std::size_t returned_count_ = 0;
    std::size_t acquired_count_ = 0;
    std::size_t reference_count_ = 0;
    bool callback_overflow_ = false;
};

TEST_F(LeaseReturnProgressTest, ReactorDispatchesFourSlabsAndPreservesReadinessAcrossBudgets) {
    ASSERT_TRUE(Open(2));
    ASSERT_EQ(progress_.wait_fds().size(), 1u);
    EXPECT_EQ(progress_.wait_fds()[0], queue_.completion_fd());
    std::array<BufferLease, 4> leases;
    for (std::size_t i = 0; i < leases.size(); ++i) {
        auto lease = Acquire(i);
        ASSERT_TRUE(lease);
        leases[i] = *lease;
    }

    // 业务线程只提交描述符；真正的 ReleaseCpu 和回调都必须在 reactor owner 执行。
    std::array<std::thread, 4> producers;
    std::atomic<unsigned> submitted{0};
    for (std::size_t i = 0; i < producers.size(); ++i) {
        producers[i] = std::thread([&, i] {
            if (queue_.TryPush(leases[i])) {
                submitted.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    ASSERT_EQ(submitted.load(std::memory_order_relaxed), 4u);
    ASSERT_TRUE(FdReadable(queue_.completion_fd()));
    auto first = reactor_.Tick();
    ASSERT_TRUE(first);
    EXPECT_EQ(first->processed, 2u);
    EXPECT_TRUE(first->runnable);
    EXPECT_EQ(queue_.pending(), 2u);
    EXPECT_EQ(returned_count_, 2u);
    EXPECT_TRUE(FdReadable(queue_.completion_fd()));
    auto second = reactor_.Tick();
    ASSERT_TRUE(second);
    EXPECT_EQ(second->processed, 2u);
    EXPECT_EQ(queue_.pending(), 0u);
    EXPECT_FALSE(FdReadable(queue_.completion_fd()));
    ASSERT_EQ(returned_count_, 4u);
    std::array<bool, 4> seen{};
    for (const auto& slab : slabs_) {
        EXPECT_EQ(slab.stats().cpu_leases, 0u);
        EXPECT_EQ(slab.stats().available, 1u);
    }
    for (std::size_t i = 0; i < returned_count_; ++i) {
        EXPECT_FALSE(returned_[i].error);
        EXPECT_EQ(returned_[i].thread, std::this_thread::get_id());
        for (std::size_t j = 0; j < leases.size(); ++j) {
            if (returned_[i].lease == leases[j]) {
                EXPECT_FALSE(seen[j]);
                seen[j] = true;
            }
        }
    }
    for (bool returned : seen) {
        EXPECT_TRUE(returned);
    }
    EXPECT_EQ(progress_.stats().released, 4u);
    EXPECT_EQ(progress_.stats().rejected, 0u);
}

TEST_F(LeaseReturnProgressTest, CpuReturnDoesNotReleaseLocalDmaOrRemoteGrantResponsibility) {
    ASSERT_TRUE(Open());
    auto lease = Acquire(0);
    ASSERT_TRUE(lease);
    auto dma = Reserve(0, *lease, BufferUse::kLocalDma);
    auto grant = Reserve(0, *lease, BufferUse::kRemoteGrant);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(slabs_[0].Commit(*dma));
    ASSERT_TRUE(slabs_[0].Commit(*grant));
    ASSERT_TRUE(queue_.TryPush(*lease));
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_EQ(slabs_[0].stats().cpu_leases, 0u);
    EXPECT_EQ(slabs_[0].stats().local_dma, 1u);
    EXPECT_EQ(slabs_[0].stats().remote_grants, 1u);
    EXPECT_EQ(slabs_[0].stats().available, 0u);
    auto exhausted = Acquire(0);
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error().code, MemoryErrorCode::kExhausted);

    // 本地 CQE 终结只去掉 DMA 责任，远端 Grant 结束后才允许复用同一槽位。
    ASSERT_TRUE(slabs_[0].CompleteDma(*dma));
    EXPECT_EQ(slabs_[0].stats().available, 0u);
    EXPECT_EQ(slabs_[0].stats().remote_grants, 1u);
    ASSERT_TRUE(slabs_[0].EndGrant(*grant));
    EXPECT_EQ(slabs_[0].stats().available, 1u);
    auto reused = Acquire(0);
    ASSERT_TRUE(reused);
    EXPECT_EQ(reused->slot, lease->slot);
    EXPECT_NE(reused->generation, lease->generation);
    ASSERT_TRUE(queue_.TryPush(*reused));
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_EQ(progress_.stats().released, 2u);
    EXPECT_EQ(progress_.stats().rejected, 0u);
    EXPECT_EQ(unregister_calls_, 0u);
}

TEST_F(LeaseReturnProgressTest, ForeignStaleAndDuplicateTokensAreReportedWithoutReleasingCurrentLease) {
    ASSERT_TRUE(Open(2));
    auto old = Acquire(0);
    ASSERT_TRUE(old);
    ASSERT_TRUE(queue_.TryPush(*old));
    ASSERT_TRUE(reactor_.Tick());
    auto current = Acquire(0);
    ASSERT_TRUE(current);
    ASSERT_EQ(old->slot, current->slot);
    ASSERT_NE(old->generation, current->generation);
    SlabAllocator foreign;
    const BufferLease foreign_lease{.allocator = &foreign, .slot = current->slot, .generation = current->generation};

    // 故意模拟上层损坏/重复描述符；队列不校验 allocator，必须由 owner 路由后拒绝。
    ASSERT_TRUE(queue_.TryPush(*old));
    ASSERT_TRUE(queue_.TryPush(foreign_lease));
    ASSERT_TRUE(queue_.TryPush(*current));
    ASSERT_TRUE(queue_.TryPush(*current));
    auto rejected = reactor_.Tick();
    ASSERT_TRUE(rejected);
    EXPECT_EQ(rejected->processed, 2u);
    EXPECT_EQ(slabs_[0].stats().cpu_leases, 1u);
    EXPECT_EQ(slabs_[0].stats().available, 0u);
    ASSERT_EQ(returned_count_, 3u);
    EXPECT_EQ(returned_[1].lease, *old);
    EXPECT_EQ(returned_[2].lease, foreign_lease);
    ASSERT_TRUE(returned_[1].error);
    ASSERT_TRUE(returned_[2].error);
    EXPECT_EQ(returned_[1].error->code, MemoryErrorCode::kInvalidLease);
    EXPECT_EQ(returned_[2].error->code, MemoryErrorCode::kInvalidLease);
    ASSERT_TRUE(reactor_.Tick());
    ASSERT_EQ(returned_count_, 5u);
    EXPECT_FALSE(returned_[3].error);
    ASSERT_TRUE(returned_[4].error);
    EXPECT_EQ(returned_[4].error->code, MemoryErrorCode::kInvalidLease);
    EXPECT_EQ(slabs_[0].stats().available, 1u);
    EXPECT_EQ(progress_.stats().released, 2u);
    EXPECT_EQ(progress_.stats().rejected, 3u);
}

TEST_F(LeaseReturnProgressTest, CloseRequiresProducerProofAndAllowsRemainingReturnsToProgress) {
    ASSERT_TRUE(Open());
    const int fd = queue_.completion_fd();
    auto no_proof = progress_.Close();
    ASSERT_FALSE(no_proof);
    EXPECT_EQ(no_proof.error().code, MemoryErrorCode::kNotQuiescent);
    ASSERT_EQ(progress_.wait_fds().size(), 1u);
    EXPECT_EQ(progress_.wait_fds()[0], fd);
    auto lease = Acquire(0);
    ASSERT_TRUE(lease);
    ASSERT_TRUE(queue_.TryPush(*lease));

    auto pending = progress_.Close(true);
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error().code, MemoryErrorCode::kInUse);
    EXPECT_FALSE(queue_.accepting());
    EXPECT_EQ(progress_.wait_fds()[0], fd);
    ASSERT_TRUE(reactor_.Tick());
    EXPECT_EQ(progress_.stats().released, 1u);
    EXPECT_EQ(slabs_[0].stats().available, 1u);
    ASSERT_TRUE(reactor_.Close());
    ASSERT_TRUE(progress_.Close(true));
    EXPECT_TRUE(progress_.wait_fds().empty());
    EXPECT_EQ(queue_.completion_fd(), fd);
    ASSERT_TRUE(queue_.Close(true));
    auto closed = progress_.Progress(runtime::ProgressKind::kMailbox, 1, 0);
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, runtime::ProgressErrorCode::kNotReady);
}

TEST_F(LeaseReturnProgressTest, RejectsCapacityBelowTotalSlabBuffersAndDuplicateSlabRegistration) {
    // 每个 Slab 有一个 CPU Lease 槽，queue 必须同时容纳四者全部归还，不能按单 Slab 配额。
    ASSERT_TRUE(queue_.Open(2));
    auto too_small = progress_.Open(queue_, pointers_, OnReturned, this);
    ASSERT_FALSE(too_small);
    EXPECT_EQ(too_small.error().code, MemoryErrorCode::kInvalidArgument);
    EXPECT_TRUE(progress_.wait_fds().empty());
    EXPECT_TRUE(queue_.accepting());
    ASSERT_TRUE(queue_.Close(true));
    ASSERT_TRUE(queue_.Open(4));
    const std::array<SlabAllocator*, 2> duplicates{&slabs_[0], &slabs_[0]};
    auto duplicate = progress_.Open(queue_, duplicates, OnReturned, this);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, MemoryErrorCode::kInvalidArgument);
    EXPECT_TRUE(progress_.wait_fds().empty());
    ASSERT_TRUE(progress_.Open(queue_, pointers_, OnReturned, this));
    EXPECT_EQ(progress_.wait_fds()[0], queue_.completion_fd());
}

} // namespace
} // namespace memory
} // namespace kbsocket
