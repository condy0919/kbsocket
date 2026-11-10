// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/socket/fd_table.hpp"

#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <limits>
#include <new>
#include <span>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/api/stream.h"

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
thread_local int fail_allocation_after = -1;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
    }
    if (fail_allocation_after == 0) {
        fail_allocation_after = -1;
        throw std::bad_alloc();
    }
    if (fail_allocation_after > 0) {
        --fail_allocation_after;
    }
}
void RecordDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocations;
    }
}
} // namespace

void* operator new(std::size_t bytes) {
    RecordAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    RecordAllocation();
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
    RecordDeallocation(pointer);
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
namespace socket {
namespace {
template <class T>
void ExpectError(const std::expected<T, FdError>& result, FdError expected) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), expected);
}
FdTableConfig Config() {
    static std::uint64_t next_table = 100;
    FdTableConfig config;
    config.table_id = ++next_table;
    config.fd_limit = 513;
    config.binding_capacity = 8;
    config.reader_capacity = 4;
    return config;
}
// 表只保存路由身份。单测目标没有实际 stream 对象，ConfirmClosed 表示本地假宿主已结束使用。
FdTarget Target(std::uint32_t seed = 1) {
    return {500 + seed, {1000 + seed, 700 + seed, seed, 0}};
}
void ExpectTarget(const FdTarget& actual, const FdTarget& expected) {
    EXPECT_EQ(actual.owner_id, expected.owner_id);
    EXPECT_EQ(actual.stream.table_id, expected.stream.table_id);
    EXPECT_EQ(actual.stream.generation, expected.stream.generation);
    EXPECT_EQ(actual.stream.slot, expected.stream.slot);
    EXPECT_EQ(actual.stream.reserved, expected.stream.reserved);
}
class FdTableTest : public ::testing::Test {
protected:
    FdTable table_;
    FdTableConfig config_ = Config();
    std::vector<FdBindingId> published_;
    std::vector<FdRetireToken> retired_;
    void Open() {
        ASSERT_TRUE(table_.Open(config_));
    }
    std::expected<FdBindingId, FdError> Bind(int fd, FdTarget target = Target()) {
        auto result = table_.Publish(fd, target);
        if (result) {
            published_.push_back(*result);
        }
        return result;
    }
    std::expected<FdRetireToken, FdError> Retire(FdBindingId id) {
        auto result = table_.Unbind(id);
        if (result) {
            retired_.push_back(*result);
        }
        return result;
    }
    void TearDown() override {
        count_allocations = false;
        fail_allocation_after = -1;
        table_.StopAccepting();
        // 局部 Reader/Lease 已销毁；清理剩余纯身份绑定，不借此伪造任何 DMA 终结。
        for (const auto id : published_) {
            auto retired = table_.Unbind(id);
            if (retired) {
                retired_.push_back(*retired);
            }
        }
        for (const auto token : retired_) {
            (void)table_.ConfirmClosed(token);
        }
        std::array<FdBinding, 16> output;
        for (unsigned round = 0; table_.stats().retired && round < 16; ++round) {
            const auto collected = table_.Collect(output, 16);
            ASSERT_TRUE(collected);
            ASSERT_GT(*collected, 0u);
        }
        EXPECT_TRUE(table_.Close(true));
    }
};

TEST(FdTableLifetimeDeathTest, RetainedStateKeepsLateHandlesSafeButClosesTheirAdmissionGate) {
    // 所有对象仅在子进程创建；故意不 Close 的保留状态随进程退出回收，不泄漏到普通单测。
    ASSERT_EXIT(
        {
            [] {
                FdReader reader;
                FdLease lease;
                FdBindingId expected;
                {
                    FdTable table;
                    ASSERT_TRUE(table.Open(Config()));
                    auto registered = table.RegisterReader();
                    ASSERT_TRUE(registered);
                    reader = std::move(*registered);
                    const auto published = table.Publish(7, Target(19));
                    ASSERT_TRUE(published);
                    expected = *published;
                    auto acquired = table.Lookup(reader, expected);
                    ASSERT_TRUE(acquired);
                    lease = std::move(*acquired);
                    ASSERT_TRUE(lease.current());
                }
                // 门面消失不撤销既有身份存储，但必须停止准入；迟归还直接访问被保留的 State。
                ASSERT_TRUE(lease);
                EXPECT_EQ(lease.binding().id, expected);
                ExpectTarget(lease.binding().target, Target(19));
                EXPECT_FALSE(lease.current());
                lease.Reset();
                reader.Reset();
                EXPECT_FALSE(lease);
                EXPECT_FALSE(reader);
            }();
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(FdTableTest, EmptyTableRejectsOperationsWithoutInventingBindings) {
    FdReader reader;
    std::array<FdBinding, 1> output;
    EXPECT_EQ(table_.stats().storage_bytes, 0u);
    EXPECT_FALSE(table_.stats().accepting);
    ExpectError(table_.Publish(1, Target()), FdError::kNotReady);
    ExpectError(table_.RegisterReader(), FdError::kNotReady);
    ExpectError(table_.Lookup(reader, 1), FdError::kNotReady);
    ExpectError(table_.Unbind({101, 1, 1}), FdError::kNotReady);
    ExpectError(table_.ConfirmClosed({{101, 1, 1}, 0}), FdError::kNotReady);
    ExpectError(table_.Collect(output, 1), FdError::kNotReady);
    EXPECT_FALSE(reader);
    reader.Reset();
    FdLease lease;
    EXPECT_FALSE(lease);
    EXPECT_FALSE(lease.current());
    lease.Reset();
}

TEST(FdTableConfigurationTest, RejectsInvalidBoundsAndAllowsCorrectedFirstOpen) {
    auto base = Config();
    std::array<FdTableConfig, 10> cases;
    cases.fill(base);
    cases[0].table_id = 0;
    cases[1].fd_limit = 0;
    cases[2].fd_limit = FdTable::kMaxFdLimit + 1;
    cases[3].binding_capacity = 0;
    cases[4].binding_capacity = FdTable::kMaxBindings + 1;
    cases[5].reader_capacity = 0;
    cases[6].reader_capacity = FdTable::kMaxReaders + 1;
    cases[7].initial_generation = 0;
    cases[8].max_references = 0;
    cases[9].fd_limit = std::numeric_limits<std::uint32_t>::max();
    for (std::size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(i);
        FdTable table;
        const auto opened = table.Open(cases[i]);
        ASSERT_FALSE(opened);
        ExpectError(opened, FdError::kInvalidArgument);
        EXPECT_EQ(table.stats().storage_bytes, 0u);
        ASSERT_TRUE(table.Open(Config()));
        ASSERT_TRUE(table.Close(true));
    }
}

TEST(FdTableConfigurationTest, StorageBudgetIncludesAllReservedMetadata) {
    FdTable probe;
    ASSERT_TRUE(probe.Open(Config()));
    const auto bytes = probe.stats().storage_bytes;
    ASSERT_GT(bytes, 0u);
    ASSERT_TRUE(probe.Close(true));
    auto exact = Config();
    exact.memory_budget_bytes = bytes;
    FdTable table;
    ASSERT_TRUE(table.Open(exact));
    EXPECT_EQ(table.stats().storage_bytes, bytes);
    ASSERT_TRUE(table.Close(true));
    auto short_budget = Config();
    short_budget.memory_budget_bytes = bytes - 1;
    FdTable rejected;
    const auto result = rejected.Open(short_budget);
    ASSERT_FALSE(result);
    ExpectError(result, FdError::kBudgetExceeded);
    EXPECT_EQ(rejected.stats().storage_bytes, 0u);
}

TEST(FdTableConfigurationTest, FailedAllocationDoesNotPublishPartialStateOrConsumeOpenLifetime) {
    FdTable probe;
    allocations = 0;
    const auto config = Config();
    count_allocations = true;
    const auto measured = probe.Open(config);
    count_allocations = false;
    ASSERT_TRUE(measured);
    const auto allocation_points = allocations;
    ASSERT_GT(allocation_points, 0u);
    ASSERT_TRUE(probe.Close(true));
    // 逐一失败实际发生的 Open 分配，而不是依赖实现固定有几个池/目录。
    for (std::size_t point = 0; point < allocation_points; ++point) {
        SCOPED_TRACE(point);
        FdTable table;
        const auto retry_config = Config();
        fail_allocation_after = static_cast<int>(point);
        const auto failed = table.Open(retry_config);
        fail_allocation_after = -1;
        ASSERT_FALSE(failed);
        ExpectError(failed, FdError::kNoMemory);
        EXPECT_EQ(table.stats().storage_bytes, 0u);
        ASSERT_TRUE(table.Open(retry_config));
        ASSERT_TRUE(table.Close(true));
    }
}

TEST_F(FdTableTest, SuccessfullyOpenedObjectCannotResetGenerationsByReopening) {
    ASSERT_NO_FATAL_FAILURE(Open());
    ExpectError(table_.Open(config_), FdError::kInUse);
    ASSERT_TRUE(table_.Close(true));
    auto another_identity = Config();
    ExpectError(table_.Open(another_identity), FdError::kInUse);
}

TEST_F(FdTableTest, TwoLevelDirectoryPreservesFullTargetsAtPageAndRangeBoundaries) {
    ASSERT_NO_FATAL_FAILURE(Open());
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    const std::array<int, 4> fds{0, 255, 256, 512};
    for (const auto fd : fds) {
        const auto target = Target(static_cast<std::uint32_t>(fd));
        const auto id = Bind(fd, target);
        ASSERT_TRUE(id);
        EXPECT_EQ(id->table_id, config_.table_id);
        EXPECT_EQ(id->fd, fd);
        EXPECT_EQ(id->generation, 1u);
        auto exact = table_.Lookup(*reader, *id);
        ASSERT_TRUE(exact);
        EXPECT_EQ(exact->binding().id, *id);
        ExpectTarget(exact->binding().target, target);
        EXPECT_TRUE(exact->current());
        auto current = table_.Lookup(*reader, fd);
        ASSERT_TRUE(current);
        EXPECT_EQ(current->binding().id, *id);
    }
    EXPECT_EQ(table_.stats().active, fds.size());
    EXPECT_EQ(table_.stats().references, 0u);
}

TEST_F(FdTableTest, InvalidDescriptorsAndTargetsDoNotConsumeBindingSlotsOrGeneration) {
    ASSERT_NO_FATAL_FAILURE(Open());
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    ExpectError(Bind(-1), FdError::kInvalidArgument);
    ExpectError(Bind(513), FdError::kInvalidArgument);
    ExpectError(table_.Lookup(*reader, -1), FdError::kInvalidArgument);
    ExpectError(table_.Lookup(*reader, 513), FdError::kInvalidArgument);
    std::array<FdTarget, 4> targets;
    targets.fill(Target());
    targets[0].owner_id = 0;
    targets[1].stream.table_id = 0;
    targets[2].stream.generation = 0;
    targets[3].stream.reserved = 1;
    for (const auto target : targets) {
        ExpectError(Bind(7, target), FdError::kInvalidArgument);
    }
    EXPECT_EQ(table_.stats().available, config_.binding_capacity);
    const auto id = Bind(7);
    ASSERT_TRUE(id);
    EXPECT_EQ(id->generation, 1u);
}

TEST_F(FdTableTest, ConflictingPublishDoesNotReplaceCurrentBindingOrConsumeGeneration) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto id = Bind(7, Target(1));
    ASSERT_TRUE(id);
    ExpectError(Bind(7, Target(2)), FdError::kInUse);
    EXPECT_EQ(table_.stats().active, 1u);
    const auto retired = Retire(*id);
    ASSERT_TRUE(retired);
    ASSERT_TRUE(table_.ConfirmClosed(*retired));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 1), 1u);
    const auto next = Bind(7, Target(3));
    ASSERT_TRUE(next);
    EXPECT_EQ(next->generation, 2u);
}

TEST_F(FdTableTest, ImmediateFdReuseKeepsOldLeaseBoundToOriginalTarget) {
    ASSERT_NO_FATAL_FAILURE(Open());
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    const auto original = Bind(9, Target(1));
    ASSERT_TRUE(original);
    auto old = table_.Lookup(*reader, *original);
    ASSERT_TRUE(old);
    const auto retired = Retire(*original);
    ASSERT_TRUE(retired);
    const auto replacement = Bind(9, Target(2));
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->generation, original->generation + 1);
    EXPECT_FALSE(old->current());
    EXPECT_EQ(old->binding().id, *original);
    ExpectTarget(old->binding().target, Target(1));
    ExpectError(table_.Lookup(*reader, *original), FdError::kStaleBinding);
    auto current = table_.Lookup(*reader, 9);
    ASSERT_TRUE(current);
    EXPECT_EQ(current->binding().id, *replacement);
    ExpectTarget(current->binding().target, Target(2));
    // owner 已释放旧流仍不能回收正在回调/等待中的旧 CPU 引用。
    ASSERT_TRUE(table_.ConfirmClosed(*retired));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 8), 0u);
    old->Reset();
    ASSERT_EQ(table_.Collect(output, 8), 1u);
    EXPECT_EQ(output[0].id, *original);
    EXPECT_TRUE(current->current());
}

TEST_F(FdTableTest, UnboundMetadataWaitsForExplicitOwnerCloseConfirmation) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto id = Bind(12);
    ASSERT_TRUE(id);
    const auto token = Retire(*id);
    ASSERT_TRUE(token);
    std::array<FdBinding, 1> output;
    for (unsigned round = 0; round < 8; ++round) {
        ASSERT_EQ(table_.Collect(output, 8), 0u);
    }
    EXPECT_EQ(table_.stats().retired, 1u);
    EXPECT_FALSE(table_.Close(true));
    ASSERT_TRUE(table_.ConfirmClosed(*token));
    ASSERT_EQ(table_.Collect(output, 8), 1u);
    EXPECT_EQ(output[0].id, *id);
}

TEST_F(FdTableTest, CloseConfirmationDoesNotReplaceLastCpuReferenceRelease) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto id = Bind(12);
    ASSERT_TRUE(id);
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    auto one = table_.Lookup(*reader, *id);
    auto two = table_.Lookup(*reader, *id);
    ASSERT_TRUE(one);
    ASSERT_TRUE(two);
    const auto token = Retire(*id);
    ASSERT_TRUE(token);
    ASSERT_TRUE(table_.ConfirmClosed(*token));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 8), 0u);
    one->Reset();
    EXPECT_EQ(table_.stats().references, 1u);
    ASSERT_EQ(table_.Collect(output, 8), 0u);
    two->Reset();
    ASSERT_EQ(table_.Collect(output, 8), 1u);
    EXPECT_EQ(table_.stats().references, 0u);
}

TEST_F(FdTableTest, StaleAndForeignBindingIdsCannotUnbindCurrentMapping) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto id = Bind(3);
    ASSERT_TRUE(id);
    auto stale = *id;
    ++stale.generation;
    EXPECT_FALSE(table_.Unbind(stale));
    auto foreign = *id;
    ++foreign.table_id;
    EXPECT_FALSE(table_.Unbind(foreign));
    auto invalid = *id;
    invalid.generation = 0;
    EXPECT_FALSE(table_.Unbind(invalid));
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    auto still_current = table_.Lookup(*reader, *id);
    ASSERT_TRUE(still_current);
    EXPECT_TRUE(still_current->current());
    EXPECT_EQ(table_.stats().active, 1u);
}

TEST_F(FdTableTest, OldRetirementTokenCannotConfirmReusedPoolSlot) {
    config_.binding_capacity = 1;
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto first = Bind(3);
    ASSERT_TRUE(first);
    const auto old_token = Retire(*first);
    ASSERT_TRUE(old_token);
    ASSERT_TRUE(table_.ConfirmClosed(*old_token));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 1), 1u);
    // 换一个 fd 时其代次也可为 1，必须同时校验 fd/table 身份，不能只校验池槽与代次。
    const auto second = Bind(4);
    ASSERT_TRUE(second);
    const auto current_token = Retire(*second);
    ASSERT_TRUE(current_token);
    EXPECT_EQ(current_token->slot, old_token->slot);
    EXPECT_EQ(current_token->id.generation, old_token->id.generation);
    ExpectError(table_.ConfirmClosed(*old_token), FdError::kInvalidRetirement);
    ASSERT_EQ(table_.Collect(output, 1), 0u);
    auto forged = *current_token;
    forged.slot = config_.binding_capacity;
    ExpectError(table_.ConfirmClosed(forged), FdError::kInvalidRetirement);
    ASSERT_TRUE(table_.ConfirmClosed(*current_token));
    ASSERT_EQ(table_.Collect(output, 1), 1u);
    EXPECT_EQ(output[0].id, *second);
}

TEST_F(FdTableTest, ReaderAndBindingIdentityAreScopedToTheirTable) {
    ASSERT_NO_FATAL_FAILURE(Open());
    FdTable other;
    ASSERT_TRUE(other.Open(Config()));
    auto own_reader = table_.RegisterReader();
    auto other_reader = other.RegisterReader();
    ASSERT_TRUE(own_reader);
    ASSERT_TRUE(other_reader);
    const auto id = Bind(7);
    ASSERT_TRUE(id);
    ExpectError(table_.Lookup(*other_reader, 7), FdError::kInvalidReader);
    EXPECT_FALSE(other.Lookup(*other_reader, *id));
    own_reader->Reset();
    ExpectError(table_.Lookup(*own_reader, 7), FdError::kInvalidReader);
    other_reader->Reset();
    ASSERT_TRUE(other.Close(true));
}

TEST_F(FdTableTest, ReaderMoveAndResetReleaseExactlyOneRegistration) {
    config_.reader_capacity = 2;
    ASSERT_NO_FATAL_FAILURE(Open());
    auto first = table_.RegisterReader();
    auto second = table_.RegisterReader();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ExpectError(table_.RegisterReader(), FdError::kReaderFull);
    FdReader moved(std::move(*first));
    EXPECT_FALSE(*first);
    EXPECT_TRUE(moved);
    EXPECT_EQ(table_.stats().readers, 2u);
    moved = std::move(*second);
    EXPECT_FALSE(*second);
    EXPECT_EQ(table_.stats().readers, 1u);
    auto recycled = table_.RegisterReader();
    ASSERT_TRUE(recycled);
    moved.Reset();
    moved.Reset();
    recycled->Reset();
    EXPECT_EQ(table_.stats().readers, 0u);
}

TEST_F(FdTableTest, LeaseMoveAndAssignmentDoNotDuplicateOrLeakReferences) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto first = Bind(1, Target(1));
    const auto second = Bind(2, Target(2));
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    auto a = table_.Lookup(*reader, *first);
    auto b = table_.Lookup(*reader, *second);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    FdLease moved(std::move(*a));
    EXPECT_FALSE(*a);
    EXPECT_EQ(table_.stats().references, 2u);
    moved = std::move(*b);
    EXPECT_FALSE(*b);
    EXPECT_EQ(table_.stats().references, 1u);
    EXPECT_EQ(moved.binding().id, *second);
    moved.Reset();
    moved.Reset();
    EXPECT_FALSE(moved.current());
    EXPECT_EQ(table_.stats().references, 0u);
}

TEST_F(FdTableTest, ReferenceLimitRejectsAdditionalPinWithoutLeakingHazardOrCounter) {
    config_.max_references = 2;
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto id = Bind(8);
    ASSERT_TRUE(id);
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    auto first = table_.Lookup(*reader, *id);
    auto second = table_.Lookup(*reader, *id);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ExpectError(table_.Lookup(*reader, *id), FdError::kReferenceLimit);
    EXPECT_EQ(table_.stats().references, 2u);
    first->Reset();
    auto replacement = table_.Lookup(*reader, *id);
    ASSERT_TRUE(replacement);
    EXPECT_EQ(table_.stats().references, 2u);
    replacement->Reset();
    second->Reset();
    const auto token = Retire(*id);
    ASSERT_TRUE(token);
    ASSERT_TRUE(table_.ConfirmClosed(*token));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 1), 1u);
}

TEST_F(FdTableTest, RetiredBindingsContinueToConsumeCapacityUntilSafelyCollected) {
    config_.binding_capacity = 2;
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto first = Bind(5);
    ASSERT_TRUE(first);
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    auto held = table_.Lookup(*reader, *first);
    ASSERT_TRUE(held);
    const auto first_token = Retire(*first);
    ASSERT_TRUE(first_token);
    ASSERT_TRUE(table_.ConfirmClosed(*first_token));
    const auto second = Bind(5);
    ASSERT_TRUE(second);
    const auto second_token = Retire(*second);
    ASSERT_TRUE(second_token);
    ExpectError(Bind(5), FdError::kFull);
    EXPECT_EQ(table_.stats().active, 0u);
    EXPECT_EQ(table_.stats().retired, 2u);
    EXPECT_EQ(table_.stats().available, 0u);
    std::array<FdBinding, 2> output;
    ASSERT_EQ(table_.Collect(output, 8), 0u);
    held->Reset();
    ASSERT_EQ(table_.Collect(output, 8), 1u);
    const auto third = Bind(5);
    ASSERT_TRUE(third);
    EXPECT_EQ(third->generation, 3u);
}

TEST_F(FdTableTest, ExhaustedFdGenerationNeverWrapsAndOtherDescriptorsRemainUsable) {
    config_.initial_generation = std::numeric_limits<std::uint32_t>::max();
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto last = Bind(7);
    ASSERT_TRUE(last);
    EXPECT_EQ(last->generation, config_.initial_generation);
    const auto retired = Retire(*last);
    ASSERT_TRUE(retired);
    ASSERT_TRUE(table_.ConfirmClosed(*retired));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 1), 1u);
    ExpectError(Bind(7), FdError::kGenerationExhausted);
    const auto different_fd = Bind(8);
    ASSERT_TRUE(different_fd);
    EXPECT_EQ(different_fd->generation, config_.initial_generation);
}

TEST_F(FdTableTest, StopRejectsNewAdmissionButAllowsExistingReferencesAndRetirement) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto id = Bind(11);
    ASSERT_TRUE(id);
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    auto held = table_.Lookup(*reader, *id);
    ASSERT_TRUE(held);
    table_.StopAccepting();
    EXPECT_FALSE(table_.stats().accepting);
    EXPECT_FALSE(held->current());
    ExpectTarget(held->binding().target, Target());
    ExpectError(Bind(12), FdError::kStopped);
    ExpectError(table_.RegisterReader(), FdError::kStopped);
    ExpectError(table_.Lookup(*reader, *id), FdError::kStopped);
    const auto token = Retire(*id);
    ASSERT_TRUE(token);
    ASSERT_TRUE(table_.ConfirmClosed(*token));
    std::array<FdBinding, 1> output;
    held->Reset();
    ASSERT_EQ(table_.Collect(output, 1), 1u);
}

TEST_F(FdTableTest, CloseRequiresClientQuiescenceAndNoUnreleasedRegistrationsOrBindings) {
    ASSERT_NO_FATAL_FAILURE(Open());
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    const auto id = Bind(11);
    ASSERT_TRUE(id);
    ExpectError(table_.Close(), FdError::kNotQuiescent);
    EXPECT_FALSE(table_.Close(true));
    const auto token = Retire(*id);
    ASSERT_TRUE(token);
    EXPECT_FALSE(table_.Close(true));
    ASSERT_TRUE(table_.ConfirmClosed(*token));
    std::array<FdBinding, 1> output;
    ASSERT_EQ(table_.Collect(output, 1), 1u);
    EXPECT_FALSE(table_.Close(true));
    reader->Reset();
    EXPECT_TRUE(table_.Close(true));
    EXPECT_EQ(table_.stats().storage_bytes, 0u);
}

TEST_F(FdTableTest, CollectHonorsOutputCapacityAndScanBudgetWithoutLosingRetiredEntries) {
    ASSERT_NO_FATAL_FAILURE(Open());
    for (int fd = 0; fd < 4; ++fd) {
        const auto id = Bind(fd);
        ASSERT_TRUE(id);
        const auto token = Retire(*id);
        ASSERT_TRUE(token);
        ASSERT_TRUE(table_.ConfirmClosed(*token));
    }
    std::array<FdBinding, 4> output;
    ExpectError(table_.Collect(output, 0), FdError::kInvalidArgument);
    ExpectError(table_.Collect(output, FdTable::kMaxCollectBudget + 1), FdError::kInvalidArgument);
    EXPECT_FALSE(table_.Collect({}, 1));
    ASSERT_EQ(table_.Collect(std::span(output).first(1), 4), 1u);
    ASSERT_EQ(table_.Collect(output, 1), 1u);
    ASSERT_EQ(table_.Collect(output, 4), 2u);
    EXPECT_EQ(table_.stats().retired, 0u);
    EXPECT_EQ(table_.stats().available, config_.binding_capacity);
}

TEST_F(FdTableTest, UnconfirmedRetirementDoesNotStarveLaterReadyBindingsUnderSmallBudget) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto blocked = Bind(1);
    ASSERT_TRUE(blocked);
    ASSERT_TRUE(Retire(*blocked));
    const auto ready = Bind(2);
    ASSERT_TRUE(ready);
    const auto ready_token = Retire(*ready);
    ASSERT_TRUE(ready_token);
    ASSERT_TRUE(table_.ConfirmClosed(*ready_token));
    std::array<FdBinding, 1> output;
    std::size_t count = 0;
    for (unsigned round = 0; round < 4 && count == 0; ++round) {
        const auto collected = table_.Collect(output, 1);
        ASSERT_TRUE(collected);
        count += *collected;
    }
    ASSERT_EQ(count, 1u);
    EXPECT_EQ(output[0].id, *ready);
    EXPECT_EQ(table_.stats().retired, 1u);
}

TEST_F(FdTableTest, RapidPoolReuseNeverMakesAnOldBindingIdCurrentAgain) {
    config_.binding_capacity = 1;
    ASSERT_NO_FATAL_FAILURE(Open());
    auto reader = table_.RegisterReader();
    ASSERT_TRUE(reader);
    FdBindingId previous;
    std::array<FdBinding, 1> output;
    for (std::uint32_t generation = 1; generation <= 512; ++generation) {
        const auto id = Bind(7, Target(generation));
        ASSERT_TRUE(id);
        ASSERT_EQ(id->generation, generation);
        if (previous.generation) {
            ExpectError(table_.Lookup(*reader, previous), FdError::kStaleBinding);
        }
        auto current = table_.Lookup(*reader, *id);
        ASSERT_TRUE(current);
        EXPECT_EQ(current->binding().id, *id);
        ExpectTarget(current->binding().target, Target(generation));
        current->Reset();
        const auto token = Retire(*id);
        ASSERT_TRUE(token);
        ASSERT_TRUE(table_.ConfirmClosed(*token));
        ASSERT_EQ(table_.Collect(output, 1), 1u);
        EXPECT_EQ(output[0].id, *id);
        previous = *id;
    }
    EXPECT_EQ(table_.stats().available, 1u);
}

TEST_F(FdTableTest, PublicationLookupReferenceHandoffAndCollectionAllocateNothingAfterOpen) {
    ASSERT_NO_FATAL_FAILURE(Open());
    std::array<FdBinding, 1> output;
    allocations = deallocations = 0;
    // 探针区间不调用 gtest 或测试追踪容器，覆盖注册/发布/查找/移动/归还/退休整个状态循环。
    count_allocations = true;
    const bool valid = [&] {
        auto reader = table_.RegisterReader();
        if (!reader) {
            return false;
        }
        for (unsigned generation = 1; generation <= 128; ++generation) {
            const auto id = table_.Publish(7, Target(generation));
            if (!id) {
                return false;
            }
            auto acquired = table_.Lookup(*reader, *id);
            if (!acquired || acquired->binding().id != *id || !acquired->current()) {
                return false;
            }
            FdLease moved(std::move(*acquired));
            const auto retired = table_.Unbind(*id);
            if (!retired || moved.current() || !table_.ConfirmClosed(*retired)) {
                return false;
            }
            const auto pinned = table_.Collect(output, 1);
            if (!pinned || *pinned != 0) {
                return false;
            }
            moved.Reset();
            const auto collected = table_.Collect(output, 1);
            if (!collected || *collected != 1 || output[0].id != *id) {
                return false;
            }
        }
        reader->Reset();
        table_.StopAccepting();
        return true;
    }();
    count_allocations = false;
    EXPECT_TRUE(valid);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    EXPECT_EQ(table_.stats().available, config_.binding_capacity);
    EXPECT_EQ(table_.stats().references, 0u);
    EXPECT_EQ(table_.stats().readers, 0u);
}

} // namespace
} // namespace socket
} // namespace kbsocket
