// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/grant_manager.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <new>
#include <type_traits>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
// 只统计完成 Open 后的热路径；GTest 断言在关闭计数后执行。
thread_local bool count_allocations = false;
thread_local std::size_t allocation_count = 0;
thread_local std::size_t deallocation_count = 0;
void CountAllocation() {
    if (count_allocations) {
        ++allocation_count;
    }
}
void CountDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocation_count;
    }
}
} // namespace

void* operator new(std::size_t bytes) {
    CountAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    CountAllocation();
    void* pointer = nullptr;
    if (::posix_memalign(&pointer, static_cast<std::size_t>(alignment), bytes ? bytes : 1) != 0) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) {
    return ::operator new(bytes, alignment);
}
void operator delete(void* pointer) noexcept {
    CountDeallocation(pointer);
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace connection {
namespace {
template <class T>
void ExpectError(const std::expected<T, GrantError>& result, GrantErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
void ExpectStats(const GrantStats& actual, const GrantStats& expected) {
    EXPECT_EQ(actual.window_begin, expected.window_begin);
    EXPECT_EQ(actual.bytes, expected.bytes);
    EXPECT_EQ(actual.active, expected.active);
    EXPECT_EQ(actual.reserved, expected.reserved);
    EXPECT_EQ(actual.published, expected.published);
    EXPECT_EQ(actual.ready, expected.ready);
    EXPECT_EQ(actual.borrowed, expected.borrowed);
    EXPECT_EQ(actual.quarantined, expected.quarantined);
    EXPECT_EQ(actual.retired, expected.retired);
}
class GrantManagerTest : public ::testing::Test {
protected:
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.register_seg = [](auto* context, auto* config) {
            const auto index = active_->registrations_++;
            auto& segment = active_->segments_[index];
            segment.urma_ctx = context;
            segment.seg.ubva.eid = context->eid;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            segment.seg.attr.value = config->flag.value;
            segment.seg.token_id = static_cast<std::uint32_t>(index + 1);
            return &segment;
        };
        functions.unregister_seg = [](auto*) {
            ++active_->unregistrations_;
            return URMA_SUCCESS;
        };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        contexts_[0] = &storage_[0];
        contexts_[1] = &storage_[1];
        storage_[0].eid.raw[0] = 1;
        storage_[1].eid.raw[0] = 2;
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(manager_.Close());
        EXPECT_EQ(slab_.stats().remote_grants, 0u);
        EXPECT_EQ(slab_.stats().cpu_leases, 0u);
        EXPECT_TRUE(slab_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    static memory::SlabConfig SlabConfig() {
        return {.buffer_count = 8, .buffer_size = 128, .max_references = 16};
    }
    static GrantBinding Binding() {
        return {.sender_runtime = 11, .receiver_runtime = 22, .receive_domain = 33, .session = 44, .token = {55, 66}};
    }
    static GrantArena Arena() {
        return {77, 88};
    }
    static GrantConfig Config() {
        return {.window = 8, .max_active = 4, .max_bytes = 256};
    }
    static GrantTransfer Transfer(std::uint32_t seq = 0, std::uint64_t bytes = 32) {
        return {.binding = Binding(), .transfer_id = 100 + static_cast<std::uint64_t>(seq), .seq = seq, .bytes = bytes};
    }
    static GrantTerminalEvidence Done(const GrantIdentity& identity) {
        // 模拟可信发送 Owner 提供的完整覆盖证明；不是把任意网络计数当作终结证据。
        return {.identity = identity,
                .no_future_writes = true,
                .acceptance_known = true,
                .accepted_writes = 2,
                .terminal_writes = 2,
                .successful_writes = 2,
                .completed_bytes = identity.transfer.bytes};
    }
    void Open(const GrantConfig& config = Config()) {
        ASSERT_TRUE(slab_.Open(contexts_, SlabConfig(), budget_));
        ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], config));
    }
    static void WritePayload(const GrantReservation& reservation, std::byte value) {
        // 通过 mock MR 模拟远端 DMA，业务只能在合法 DONE 后获取只读视图。
        auto* memory = reinterpret_cast<std::byte*>(reservation.target.addr);
        std::fill_n(memory, reservation.target.len, value);
    }
    static inline GrantManagerTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    std::array<urma_context_t, 3> storage_{};
    std::array<urma_context_t*, 2> contexts_{};
    std::array<urma_target_seg_t, 16> segments_{};
    std::size_t registrations_ = 0;
    std::size_t unregistrations_ = 0;
    memory::MemoryBudget budget_{static_cast<std::size_t>(::sysconf(_SC_PAGESIZE)) * 4};
    memory::SlabAllocator slab_;
    GrantManager manager_;
};

TEST_F(GrantManagerTest, RejectsMalformedOpenWithoutConsumingSingleOpenLifetime) {
    ExpectError(manager_.Reserve(Transfer()), GrantErrorCode::kNotReady);
    ASSERT_TRUE(slab_.Open(contexts_, SlabConfig(), budget_));
    for (unsigned field = 0; field < 5; ++field) {
        auto binding = Binding();
        switch (field) {
        case 0:
            binding.sender_runtime = 0;
            break;
        case 1:
            binding.receiver_runtime = 0;
            break;
        case 2:
            binding.receive_domain = 0;
            break;
        case 3:
            binding.session = 0;
            break;
        case 4:
            binding.token.generation = 0;
            break;
        }
        ExpectError(manager_.Open(binding, Arena(), slab_, contexts_[0], Config()), GrantErrorCode::kInvalidArgument);
    }
    ExpectError(manager_.Open(Binding(), {0, 88}, slab_, contexts_[0], Config()), GrantErrorCode::kInvalidArgument);
    ExpectError(manager_.Open(Binding(), {77, 0}, slab_, contexts_[0], Config()), GrantErrorCode::kInvalidArgument);
    ExpectError(manager_.Open(Binding(), Arena(), slab_, nullptr, Config()), GrantErrorCode::kInvalidArgument);
    for (unsigned field = 0; field < 5; ++field) {
        auto config = Config();
        switch (field) {
        case 0:
            config.window = 0;
            break;
        case 1:
            config.window = GrantManager::kMaxWindow + 1;
            break;
        case 2:
            config.max_active = 0;
            break;
        case 3:
            config.max_active = config.window + 1;
            break;
        case 4:
            config.max_bytes = 0;
            break;
        }
        ExpectError(manager_.Open(Binding(), Arena(), slab_, contexts_[0], config), GrantErrorCode::kInvalidArgument);
    }
    ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()));
    ExpectError(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()), GrantErrorCode::kInUse);
    ASSERT_TRUE(manager_.Close());
    EXPECT_FALSE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()));
    EXPECT_EQ(registrations_, 2u);
}

TEST_F(GrantManagerTest, AdmitsOutOfOrderRtsAndRetainsSequenceTombstones) {
    Open();
    auto high = manager_.Reserve(Transfer(3));
    auto low = manager_.Reserve(Transfer(0));
    ASSERT_TRUE(high);
    ASSERT_TRUE(low);
    EXPECT_NE(high->target.addr, low->target.addr);
    EXPECT_EQ(high->target.len, 32u);
    EXPECT_EQ(high->target.tseg, &segments_[0]);
    EXPECT_EQ(high->identity.transfer, Transfer(3));
    EXPECT_EQ(high->identity.arena, Arena());
    EXPECT_EQ(high->identity.target_address, high->target.addr);
    EXPECT_EQ(high->handle.owner, &manager_);
    EXPECT_EQ(slab_.stats().remote_grants, 2u);
    EXPECT_EQ(slab_.stats().local_dma, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 2u);
    const auto stats = manager_.stats();
    ExpectError(manager_.Reserve(Transfer(3)), GrantErrorCode::kDuplicate);
    auto conflicting = Transfer(3, 31);
    conflicting.transfer_id = 9999;
    ExpectError(manager_.Reserve(conflicting), GrantErrorCode::kDuplicate);
    ExpectStats(manager_.stats(), stats);
    ASSERT_TRUE(manager_.Cancel(high->handle));
    // 已撤销授权也不能让同 SN 的旧 RTS 再取得新的目标地址。
    ExpectError(manager_.Reserve(Transfer(3)), GrantErrorCode::kDuplicate);
    ASSERT_TRUE(manager_.Cancel(low->handle));
    EXPECT_EQ(manager_.stats().retired, 2u);
    ASSERT_TRUE(manager_.AdvanceWindow(4));
    ExpectError(manager_.Reserve(Transfer(3)), GrantErrorCode::kStaleSequence);
    EXPECT_EQ(manager_.stats().retired, 0u);
}

TEST_F(GrantManagerTest, CompoundTransferIdentityAllowsDifferentSequencesWithSameTransferId) {
    Open();
    auto first = Transfer(0);
    auto second = Transfer(1);
    second.transfer_id = first.transfer_id;
    auto a = manager_.Reserve(first);
    auto b = manager_.Reserve(second);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_NE(a->identity, b->identity);
    ASSERT_TRUE(manager_.Cancel(a->handle));
    ASSERT_TRUE(manager_.Cancel(b->handle));
}

TEST_F(GrantManagerTest, RejectsEveryWrongBindingAndInvalidRequestWithoutOccupyingMemory) {
    Open();
    for (unsigned field = 0; field < 6; ++field) {
        auto transfer = Transfer();
        switch (field) {
        case 0:
            ++transfer.binding.sender_runtime;
            break;
        case 1:
            ++transfer.binding.receiver_runtime;
            break;
        case 2:
            ++transfer.binding.receive_domain;
            break;
        case 3:
            ++transfer.binding.session;
            break;
        case 4:
            ++transfer.binding.token.slot;
            break;
        case 5:
            ++transfer.binding.token.generation;
            break;
        }
        ExpectError(manager_.Reserve(transfer), GrantErrorCode::kWrongBinding);
    }
    auto invalid = Transfer();
    invalid.transfer_id = 0;
    EXPECT_FALSE(manager_.Reserve(invalid));
    EXPECT_FALSE(manager_.Reserve(Transfer(0, 0)));
    EXPECT_FALSE(manager_.Reserve(Transfer(0, std::numeric_limits<std::uint64_t>::max())));
    ExpectError(manager_.Reserve(Transfer(8)), GrantErrorCode::kWindowFull);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(manager_.stats().retired, 0u);
    EXPECT_EQ(slab_.stats().available, 8u);
    EXPECT_EQ(slab_.stats().available_references, 16u);
    auto accepted = manager_.Reserve(Transfer());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(manager_.Cancel(accepted->handle));
}

TEST_F(GrantManagerTest, PublicationValidationChecksExactIdentityWithoutPublishing) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    const auto before = manager_.stats();
    ASSERT_TRUE(manager_.ValidatePublication(reservation->handle, reservation->identity));
    // 通知队列接管前必须校验完整授权身份；只读核验不能提前发布或解除责任。
    for (unsigned field = 0; field < 4; ++field) {
        auto changed = reservation->identity;
        switch (field) {
        case 0:
            ++changed.transfer.bytes;
            break;
        case 1:
            ++changed.transfer.binding.session;
            break;
        case 2:
            ++changed.generation;
            break;
        case 3:
            ++changed.target_address;
            break;
        }
        ExpectError(manager_.ValidatePublication(reservation->handle, changed), GrantErrorCode::kIdentityMismatch);
        ExpectStats(manager_.stats(), before);
    }
    // 已发布的授权不能再次交给通知队列，取消后仍须独立的远端访问终结证明。
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    ExpectError(manager_.ValidatePublication(reservation->handle, reservation->identity),
                GrantErrorCode::kInvalidState);
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    ASSERT_TRUE(manager_.ConfirmTerminal(Done(reservation->identity)));
    EXPECT_FALSE(manager_.ValidatePublication(reservation->handle, reservation->identity));
}

TEST_F(GrantManagerTest, UnpublishedRollbackReleasesBothResponsibilitiesButDoesNotReuseGeneration) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ExpectError(manager_.Borrow(reservation->handle), GrantErrorCode::kInvalidState);
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_EQ(slab_.stats().quarantined_buffers, 0u);
    EXPECT_EQ(slab_.stats().available, 8u);
    EXPECT_FALSE(manager_.Cancel(reservation->handle));
    ASSERT_TRUE(manager_.AdvanceWindow(8));
    auto replacement = manager_.Reserve(Transfer(8));
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->handle.slot, reservation->handle.slot);
    EXPECT_GT(replacement->handle.generation, reservation->handle.generation);
    EXPECT_FALSE(manager_.CommitPublication(reservation->handle));
    EXPECT_FALSE(manager_.Borrow(reservation->handle));
    EXPECT_FALSE(manager_.Cancel(reservation->handle));
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    ASSERT_TRUE(manager_.Cancel(replacement->handle));
}

TEST_F(GrantManagerTest, PublicationUnknownCancellationRetainsGrantAndBackingUntilTrustedTerminalProof) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    // Commit 在 GRANT 可能暴露前完成；网络投递结果未知也不能再当作 Reserved 回滚。
    EXPECT_FALSE(manager_.CommitPublication(reservation->handle));
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    EXPECT_EQ(manager_.stats().quarantined, 1u);
    EXPECT_EQ(manager_.stats().active, 1u);
    EXPECT_EQ(manager_.stats().bytes, 32u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(slab_.stats().quarantined_buffers, 1u);
    EXPECT_EQ(slab_.stats().quarantined_bytes, 128u);
    EXPECT_EQ(slab_.stats().available, 7u);
    EXPECT_FALSE(manager_.Borrow(reservation->handle));
    auto ended = Done(reservation->identity);
    ended.successful_writes = 1;
    ended.completed_bytes = 16;
    ASSERT_TRUE(manager_.ConfirmTerminal(ended));
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().quarantined_buffers, 0u);
    EXPECT_EQ(slab_.stats().available, 8u);
}

TEST_F(GrantManagerTest, SuccessfulDoneExposesOnlyReadOnlyPayloadAndReturnEndsCpuResponsibility) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    EXPECT_FALSE(manager_.CompleteDone(Done(reservation->identity)));
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    EXPECT_FALSE(manager_.Borrow(reservation->handle));
    WritePayload(*reservation, std::byte{0xa5});
    ASSERT_TRUE(manager_.CompleteDone(Done(reservation->identity)));
    EXPECT_EQ(manager_.stats().ready, 1u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(slab_.stats().available, 7u);
    auto borrowed = manager_.Borrow(reservation->handle);
    ASSERT_TRUE(borrowed);
    static_assert(std::is_same_v<typename decltype(borrowed)::value_type, std::span<const std::byte>>);
    ASSERT_EQ(borrowed->size(), 32u);
    EXPECT_TRUE(std::all_of(borrowed->begin(), borrowed->end(), [](auto value) { return value == std::byte{0xa5}; }));
    EXPECT_EQ(manager_.stats().ready, 0u);
    EXPECT_EQ(manager_.stats().borrowed, 1u);
    EXPECT_FALSE(manager_.Borrow(reservation->handle));
    EXPECT_FALSE(manager_.CompleteDone(Done(reservation->identity)));
    // 已交给应用的视图不可由取消或 Close 抢回。
    EXPECT_FALSE(manager_.Cancel(reservation->handle));
    ExpectError(manager_.Close(), GrantErrorCode::kInUse);
    EXPECT_EQ((*borrowed)[0], std::byte{0xa5});
    ASSERT_TRUE(manager_.Return(reservation->handle));
    EXPECT_FALSE(manager_.Return(reservation->handle));
    EXPECT_EQ(slab_.stats().available, 8u);
    EXPECT_EQ(manager_.stats().active, 0u);
}

TEST_F(GrantManagerTest, BorrowValidationRequiresLiveBorrowAndExactWholeSpan) {
    EXPECT_FALSE(manager_.binding());
    EXPECT_EQ(manager_.window_size(), 0u);
    Open();
    ASSERT_TRUE(manager_.binding());
    EXPECT_EQ(*manager_.binding(), Binding());
    EXPECT_EQ(manager_.window_size(), Config().window);
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    const std::span<const std::byte> target(reinterpret_cast<const std::byte*>(reservation->target.addr),
                                            reservation->target.len);
    // 拥有授权地址还不等于应用借用；Reserved、Published、Ready 都不能直接送入重排窗口。
    EXPECT_FALSE(manager_.ValidateBorrow(reservation->handle, target));
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    EXPECT_FALSE(manager_.ValidateBorrow(reservation->handle, target));
    ASSERT_TRUE(manager_.CompleteDone(Done(reservation->identity)));
    EXPECT_FALSE(manager_.ValidateBorrow(reservation->handle, target));
    const auto borrowed = manager_.Borrow(reservation->handle);
    ASSERT_TRUE(borrowed);
    const auto& read_only = manager_;
    ASSERT_TRUE(read_only.ValidateBorrow(reservation->handle, *borrowed));
    EXPECT_FALSE(read_only.ValidateBorrow(reservation->handle, borrowed->first(borrowed->size() - 1)));
    EXPECT_FALSE(read_only.ValidateBorrow(reservation->handle, borrowed->subspan(1)));
    auto stale = reservation->handle;
    ++stale.generation;
    EXPECT_FALSE(read_only.ValidateBorrow(stale, *borrowed));
    GrantManager foreign;
    EXPECT_FALSE(foreign.ValidateBorrow(reservation->handle, *borrowed));
    // Stop 不抢回真实应用借用；Return 后即使描述符仍在，也不能重新准入。
    manager_.Stop();
    ASSERT_TRUE(read_only.ValidateBorrow(reservation->handle, *borrowed));
    ASSERT_TRUE(manager_.Return(reservation->handle));
    EXPECT_FALSE(read_only.ValidateBorrow(reservation->handle, *borrowed));
    ASSERT_TRUE(manager_.Close());
    EXPECT_FALSE(manager_.binding());
    EXPECT_EQ(manager_.window_size(), 0u);
}

TEST_F(GrantManagerTest, LocalSourceDmaRetirementDoesNotEndIndependentReceiverGrant) {
    Open();
    memory::SlabAllocator source;
    ASSERT_TRUE(source.Open(contexts_, SlabConfig(), budget_));
    auto lease = source.Acquire(32);
    ASSERT_TRUE(lease);
    auto writable = source.Writable(*lease);
    ASSERT_TRUE(writable);
    std::fill(writable->begin(), writable->end(), std::byte{0x7c});
    auto dma = source.Reserve(*lease, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    auto source_sge = source.Sge(*dma, contexts_[0], 0, 32);
    ASSERT_TRUE(source_sge);
    auto target = manager_.Reserve(Transfer());
    ASSERT_TRUE(target);
    ASSERT_TRUE(manager_.CommitPublication(target->handle));
    ASSERT_TRUE(source.Commit(*dma));
    // 两份不同 Slab 的责任独立记账；这里仅模拟已接受 WRITE 完成内存搬运。
    const auto* bytes = reinterpret_cast<const std::byte*>(source_sge->addr);
    auto* destination = reinterpret_cast<std::byte*>(target->target.addr);
    std::copy_n(bytes, source_sge->len, destination);
    ASSERT_TRUE(source.CompleteDma(*dma));
    ASSERT_TRUE(source.ReleaseCpu(*lease));
    ASSERT_TRUE(source.Close());
    EXPECT_EQ(source.stats().local_dma, 0u);
    EXPECT_EQ(source.stats().cpu_leases, 0u);
    EXPECT_EQ(manager_.stats().published, 1u);
    EXPECT_EQ(manager_.stats().active, 1u);
    EXPECT_EQ(manager_.stats().bytes, 32u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(slab_.stats().available, 7u);
    EXPECT_FALSE(manager_.Borrow(target->handle));
    // 本地成功 CQE 只允许发送端复用源；接收端仍须等完整、可信的 DONE。
    ASSERT_TRUE(manager_.CompleteDone(Done(target->identity)));
    auto borrowed = manager_.Borrow(target->handle);
    ASSERT_TRUE(borrowed);
    EXPECT_TRUE(std::all_of(borrowed->begin(), borrowed->end(), [](auto byte) { return byte == std::byte{0x7c}; }));
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    ASSERT_TRUE(manager_.Return(target->handle));
}

TEST_F(GrantManagerTest, CancelReadyDiscardsPayloadWithoutReopeningRemoteResponsibility) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    ASSERT_TRUE(manager_.CompleteDone(Done(reservation->identity)));
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    EXPECT_FALSE(manager_.Borrow(reservation->handle));
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_EQ(slab_.stats().quarantined_buffers, 0u);
}

TEST_F(GrantManagerTest, CancelledGrantLateDoneOnlyDiscardsAndCannotReviveDelivery) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    const auto isolated = manager_.stats();
    EXPECT_FALSE(manager_.CompleteDone({.identity = reservation->identity}));
    ExpectStats(manager_.stats(), isolated);
    ASSERT_TRUE(manager_.CompleteDone(Done(reservation->identity)));
    EXPECT_EQ(manager_.stats().ready, 0u);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_FALSE(manager_.Borrow(reservation->handle));
    EXPECT_FALSE(manager_.CompleteDone(Done(reservation->identity)));
    EXPECT_EQ(slab_.stats().available, 8u);
}

TEST_F(GrantManagerTest, TerminalConfirmationAlwaysDiscardsIncludingAllSuccessAndZeroAccepted) {
    Open();
    for (std::uint32_t seq = 0; seq < 2; ++seq) {
        auto reservation = manager_.Reserve(Transfer(seq));
        ASSERT_TRUE(reservation);
        ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
        auto terminal = Done(reservation->identity);
        if (seq == 1) {
            terminal.accepted_writes = 0;
            terminal.terminal_writes = 0;
            terminal.successful_writes = 0;
            terminal.completed_bytes = 0;
        }
        ASSERT_TRUE(manager_.ConfirmTerminal(terminal));
        EXPECT_FALSE(manager_.Borrow(reservation->handle));
        EXPECT_FALSE(manager_.ConfirmTerminal(terminal));
    }
    EXPECT_EQ(manager_.stats().ready, 0u);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(manager_.stats().retired, 2u);
    EXPECT_EQ(slab_.stats().available, 8u);
}

TEST_F(GrantManagerTest, EveryDoneIdentityFieldMustMatchWithoutChangingResponsibility) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    const auto before = manager_.stats();
    for (unsigned field = 0; field < 14; ++field) {
        SCOPED_TRACE(field);
        auto proof = Done(reservation->identity);
        auto& identity = proof.identity;
        switch (field) {
        case 0:
            ++identity.transfer.binding.sender_runtime;
            break;
        case 1:
            ++identity.transfer.binding.receiver_runtime;
            break;
        case 2:
            ++identity.transfer.binding.receive_domain;
            break;
        case 3:
            ++identity.transfer.binding.session;
            break;
        case 4:
            ++identity.transfer.binding.token.slot;
            break;
        case 5:
            ++identity.transfer.binding.token.generation;
            break;
        case 6:
            ++identity.transfer.transfer_id;
            break;
        case 7:
            ++identity.transfer.seq;
            break;
        case 8:
            ++identity.transfer.bytes;
            break;
        case 9:
            ++identity.arena.id;
            break;
        case 10:
            ++identity.arena.generation;
            break;
        case 11:
            ++identity.slot;
            break;
        case 12:
            ++identity.generation;
            break;
        case 13:
            ++identity.target_address;
            break;
        }
        EXPECT_FALSE(manager_.CompleteDone(proof));
        EXPECT_FALSE(manager_.ConfirmTerminal(proof));
        ExpectStats(manager_.stats(), before);
        EXPECT_EQ(slab_.stats().remote_grants, 1u);
        EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    }
    ASSERT_TRUE(manager_.ConfirmTerminal(Done(reservation->identity)));
}

TEST_F(GrantManagerTest, DoneRequiresFutureSubmissionBanKnownAcceptanceAllSuccessAndExactCoverage) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    const auto before = manager_.stats();
    for (unsigned field = 0; field < 9; ++field) {
        SCOPED_TRACE(field);
        auto proof = Done(reservation->identity);
        switch (field) {
        case 0:
            proof.no_future_writes = false;
            break;
        case 1:
            proof.acceptance_known = false;
            break;
        case 2:
            --proof.terminal_writes;
            break;
        case 3:
            ++proof.terminal_writes;
            break;
        case 4:
            --proof.successful_writes;
            break;
        case 5:
            ++proof.successful_writes;
            break;
        case 6:
            --proof.completed_bytes;
            break;
        case 7:
            ++proof.completed_bytes;
            break;
        case 8:
            proof.accepted_writes = proof.terminal_writes = proof.successful_writes = 0;
            break;
        }
        ExpectError(manager_.CompleteDone(proof), GrantErrorCode::kInvalidEvidence);
        ExpectStats(manager_.stats(), before);
        EXPECT_EQ(slab_.stats().remote_grants, 1u);
    }
    ASSERT_TRUE(manager_.ConfirmTerminal(Done(reservation->identity)));
}

TEST_F(GrantManagerTest, CleanupEvidenceStillRequiresKnownAndCompleteTerminalAccounting) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    const auto before = manager_.stats();
    for (unsigned field = 0; field < 6; ++field) {
        SCOPED_TRACE(field);
        auto proof = Done(reservation->identity);
        switch (field) {
        case 0:
            proof.no_future_writes = false;
            break;
        case 1:
            proof.acceptance_known = false;
            break;
        case 2:
            --proof.terminal_writes;
            break;
        case 3:
            ++proof.terminal_writes;
            break;
        case 4:
            ++proof.successful_writes;
            break;
        case 5:
            ++proof.completed_bytes;
            break;
        }
        ExpectError(manager_.ConfirmTerminal(proof), GrantErrorCode::kInvalidEvidence);
        ExpectStats(manager_.stats(), before);
        EXPECT_EQ(slab_.stats().remote_grants, 1u);
    }
    auto ended = Done(reservation->identity);
    ended.successful_writes = 0;
    ended.completed_bytes = 0;
    ASSERT_TRUE(manager_.ConfirmTerminal(ended));
}

TEST_F(GrantManagerTest, ReadyBorrowedAndQuarantinedBuffersAllConsumeTheByteBudget) {
    auto config = Config();
    config.max_bytes = 96;
    Open(config);
    std::array<GrantReservation, 3> reservations{};
    for (std::uint32_t seq = 0; seq < 3; ++seq) {
        auto reserved = manager_.Reserve(Transfer(seq));
        ASSERT_TRUE(reserved);
        reservations[seq] = *reserved;
        ASSERT_TRUE(manager_.CommitPublication(reserved->handle));
    }
    ASSERT_TRUE(manager_.CompleteDone(Done(reservations[0].identity)));
    ASSERT_TRUE(manager_.CompleteDone(Done(reservations[1].identity)));
    ASSERT_TRUE(manager_.Borrow(reservations[1].handle));
    ASSERT_TRUE(manager_.Cancel(reservations[2].handle));
    EXPECT_EQ(manager_.stats().ready, 1u);
    EXPECT_EQ(manager_.stats().borrowed, 1u);
    EXPECT_EQ(manager_.stats().quarantined, 1u);
    EXPECT_EQ(manager_.stats().bytes, 96u);
    const auto before = manager_.stats();
    EXPECT_FALSE(manager_.Reserve(Transfer(3, 1)));
    ExpectStats(manager_.stats(), before);
    ASSERT_TRUE(manager_.Return(reservations[1].handle));
    auto retry = manager_.Reserve(Transfer(3));
    ASSERT_TRUE(retry);
    EXPECT_EQ(manager_.stats().bytes, 96u);
    ASSERT_TRUE(manager_.Cancel(retry->handle));
    ASSERT_TRUE(manager_.Cancel(reservations[0].handle));
    ASSERT_TRUE(manager_.ConfirmTerminal(Done(reservations[2].identity)));
    EXPECT_EQ(manager_.stats().bytes, 0u);
}

TEST_F(GrantManagerTest, ActiveSlotBudgetPreventsAdmissionEvenWhenBytesRemain) {
    auto config = Config();
    config.max_active = 1;
    Open(config);
    auto held = manager_.Reserve(Transfer(0, 1));
    ASSERT_TRUE(held);
    EXPECT_FALSE(manager_.Reserve(Transfer(1, 1)));
    ASSERT_TRUE(manager_.Cancel(held->handle));
    auto retry = manager_.Reserve(Transfer(1, 1));
    ASSERT_TRUE(retry);
    ASSERT_TRUE(manager_.Cancel(retry->handle));
}

TEST_F(GrantManagerTest, InvalidHandlesCannotMutateForeignOrLiveSlots) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    GrantManager other;
    const auto before = manager_.stats();
    for (unsigned field = 0; field < 3; ++field) {
        auto handle = reservation->handle;
        switch (field) {
        case 0:
            handle.owner = &other;
            break;
        case 1:
            handle.slot = GrantManager::kMaxWindow;
            break;
        case 2:
            ++handle.generation;
            break;
        }
        ExpectError(manager_.CommitPublication(handle), GrantErrorCode::kInvalidHandle);
        ExpectError(manager_.Cancel(handle), GrantErrorCode::kInvalidHandle);
        ExpectError(manager_.Borrow(handle), GrantErrorCode::kInvalidHandle);
        ExpectError(manager_.Return(handle), GrantErrorCode::kInvalidHandle);
        ExpectStats(manager_.stats(), before);
    }
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
}

TEST_F(GrantManagerTest, WrongRegisteredContextRollsBackCpuAndRemoteReservations) {
    ASSERT_TRUE(slab_.Open(contexts_, SlabConfig(), budget_));
    ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, &storage_[2], Config()));
    const auto before = manager_.stats();
    ExpectError(manager_.Reserve(Transfer()), GrantErrorCode::kMemory);
    ExpectStats(manager_.stats(), before);
    EXPECT_EQ(slab_.stats().available, 8u);
    EXPECT_EQ(slab_.stats().available_references, 16u);
}

TEST_F(GrantManagerTest, ReadOnlyRegistrationCannotAuthorizeRemoteWrites) {
    auto config = SlabConfig();
    config.access = URMA_ACCESS_READ;
    ASSERT_TRUE(slab_.Open(contexts_, config, budget_));
    ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()));
    ExpectError(manager_.Reserve(Transfer()), GrantErrorCode::kMemory);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(manager_.stats().retired, 0u);
    EXPECT_EQ(slab_.stats().available, 8u);
    EXPECT_EQ(slab_.stats().available_references, 16u);
}

TEST_F(GrantManagerTest, HostOnlySlabCannotBecomeRemoteGrantBacking) {
    ASSERT_TRUE(slab_.OpenHost(SlabConfig(), budget_));
    ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()));
    ExpectError(manager_.Reserve(Transfer()), GrantErrorCode::kMemory);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().available, 8u);
    EXPECT_EQ(slab_.stats().available_references, 16u);
    EXPECT_EQ(registrations_, 0u);
}

TEST_F(GrantManagerTest, SlabExhaustionDoesNotLeaveTombstoneAndCanRetrySameRts) {
    auto config = SlabConfig();
    config.buffer_count = 1;
    ASSERT_TRUE(slab_.Open(contexts_, config, budget_));
    ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()));
    auto external = slab_.Acquire(16);
    ASSERT_TRUE(external);
    ExpectError(manager_.Reserve(Transfer()), GrantErrorCode::kMemory);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(manager_.stats().retired, 0u);
    ASSERT_TRUE(slab_.ReleaseCpu(*external));
    auto retry = manager_.Reserve(Transfer());
    ASSERT_TRUE(retry);
    ASSERT_TRUE(manager_.Cancel(retry->handle));
}

TEST_F(GrantManagerTest, ReferenceExhaustionRollsBackCpuLeaseAndAllowsRetry) {
    auto config = SlabConfig();
    config.max_references = 1;
    ASSERT_TRUE(slab_.Open(contexts_, config, budget_));
    ASSERT_TRUE(manager_.Open(Binding(), Arena(), slab_, contexts_[0], Config()));
    auto external = slab_.Acquire(16);
    ASSERT_TRUE(external);
    auto dma = slab_.Reserve(*external, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(dma);
    ExpectError(manager_.Reserve(Transfer()), GrantErrorCode::kMemory);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(slab_.stats().available, 7u);
    EXPECT_EQ(manager_.stats().retired, 0u);
    ASSERT_TRUE(slab_.Cancel(*dma));
    ASSERT_TRUE(slab_.ReleaseCpu(*external));
    auto retry = manager_.Reserve(Transfer());
    ASSERT_TRUE(retry);
    ASSERT_TRUE(manager_.Cancel(retry->handle));
}

TEST_F(GrantManagerTest, WindowAdvanceIsAtomicAcrossRetiredAndAllActiveStates) {
    Open();
    auto retired = manager_.Reserve(Transfer(0));
    auto active = manager_.Reserve(Transfer(3));
    ASSERT_TRUE(retired);
    ASSERT_TRUE(active);
    ASSERT_TRUE(manager_.Cancel(retired->handle));
    // 范围前部含可清理 tombstone，后部有活跃 Grant；不能先清前部再返回失败。
    auto before = manager_.stats();
    ExpectError(manager_.AdvanceWindow(4), GrantErrorCode::kInUse);
    ExpectStats(manager_.stats(), before);
    ExpectError(manager_.Reserve(Transfer(0)), GrantErrorCode::kDuplicate);
    ASSERT_TRUE(manager_.AdvanceWindow(3));
    EXPECT_EQ(manager_.stats().window_begin, 3u);
    ASSERT_TRUE(manager_.CommitPublication(active->handle));
    before = manager_.stats();
    EXPECT_FALSE(manager_.AdvanceWindow(4));
    ExpectStats(manager_.stats(), before);
    ASSERT_TRUE(manager_.CompleteDone(Done(active->identity)));
    EXPECT_FALSE(manager_.AdvanceWindow(4));
    ASSERT_TRUE(manager_.Borrow(active->handle));
    EXPECT_FALSE(manager_.AdvanceWindow(4));
    ASSERT_TRUE(manager_.Return(active->handle));
    ASSERT_TRUE(manager_.AdvanceWindow(4));
    EXPECT_FALSE(manager_.AdvanceWindow(3));
    EXPECT_FALSE(manager_.AdvanceWindow(13));
    EXPECT_EQ(manager_.stats().window_begin, 4u);
}

TEST_F(GrantManagerTest, SequenceEndRejectsFinNumberAndNeverWrapsToOldRts) {
    auto config = Config();
    constexpr auto last = std::numeric_limits<std::uint32_t>::max();
    config.initial_seq = last - 1;
    Open(config);
    auto final_data = manager_.Reserve(Transfer(last - 1));
    ASSERT_TRUE(final_data);
    EXPECT_FALSE(manager_.Reserve(Transfer(last)));
    ASSERT_TRUE(manager_.Cancel(final_data->handle));
    ASSERT_TRUE(manager_.AdvanceWindow(std::uint64_t{1} << 32));
    EXPECT_EQ(manager_.stats().window_begin, std::uint64_t{1} << 32);
    EXPECT_FALSE(manager_.Reserve(Transfer(0)));
    EXPECT_FALSE(manager_.Reserve(Transfer(last - 1)));
    EXPECT_FALSE(manager_.AdvanceWindow((std::uint64_t{1} << 32) + 1));
    EXPECT_FALSE(manager_.AdvanceWindow(0));
}

TEST_F(GrantManagerTest, StopAndBusyCloseRetainGrantWithoutStoppingSharedSlab) {
    Open();
    auto reservation = manager_.Reserve(Transfer());
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(manager_.CommitPublication(reservation->handle));
    manager_.Stop();
    EXPECT_FALSE(manager_.accepting());
    ExpectError(manager_.Reserve(Transfer(1)), GrantErrorCode::kStopped);
    ExpectError(manager_.Close(), GrantErrorCode::kInUse);
    EXPECT_TRUE(slab_.accepting());
    EXPECT_EQ(unregistrations_, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_EQ(manager_.stats().published, 1u);
    auto unrelated = slab_.Acquire(16);
    ASSERT_TRUE(unrelated);
    ASSERT_TRUE(slab_.ReleaseCpu(*unrelated));
    // Stop 只是关准入；显式 Cancel 才使连接失败后的迟到 DONE 丢弃。
    ASSERT_TRUE(manager_.Cancel(reservation->handle));
    ExpectError(manager_.Close(), GrantErrorCode::kInUse);
    ASSERT_TRUE(manager_.ConfirmTerminal(Done(reservation->identity)));
    ASSERT_TRUE(manager_.Close());
    EXPECT_TRUE(slab_.accepting());
}

TEST_F(GrantManagerTest, AdmissionCompletionRejectionCancellationAndWindowAdvanceDoNotAllocateOrFree) {
    Open();
    bool succeeded = true;
    allocation_count = 0;
    deallocation_count = 0;
    const auto registrations = registrations_;
    count_allocations = true;
    for (std::uint32_t seq = 0; seq < 64; ++seq) {
        const auto transfer = Transfer(seq);
        auto reserved = manager_.Reserve(transfer);
        if (!reserved) {
            succeeded = false;
            break;
        }
        succeeded &= !manager_.Reserve(transfer).has_value();
        auto wrong = reserved->handle;
        ++wrong.generation;
        succeeded &= !manager_.Cancel(wrong).has_value();
        succeeded &= manager_.CommitPublication(reserved->handle).has_value();
        auto proof = Done(reserved->identity);
        auto invalid = proof;
        invalid.no_future_writes = false;
        succeeded &= !manager_.CompleteDone(invalid).has_value();
        if ((seq & 1) == 0) {
            succeeded &= manager_.CompleteDone(proof).has_value();
            const auto borrowed = manager_.Borrow(reserved->handle);
            succeeded &= borrowed.has_value();
            if (borrowed) {
                succeeded &= manager_.ValidateBorrow(reserved->handle, *borrowed).has_value();
            }
            succeeded &= manager_.Return(reserved->handle).has_value();
        } else {
            succeeded &= manager_.Cancel(reserved->handle).has_value();
            succeeded &= manager_.ConfirmTerminal(proof).has_value();
        }
        succeeded &= !manager_.Reserve(transfer).has_value();
        succeeded &= manager_.AdvanceWindow(static_cast<std::uint64_t>(seq) + 1).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(deallocation_count, 0u);
    EXPECT_EQ(registrations_, registrations);
    EXPECT_EQ(unregistrations_, 0u);
    EXPECT_EQ(manager_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().available, 8u);
}
} // namespace
} // namespace connection
} // namespace kbsocket
