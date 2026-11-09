// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/tseg_import_cache.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
// Open 之外才启用计数，避免把 GTest 或控制路径的预分配计入发送热路径。
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
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
namespace control {
namespace {
template <typename T>
void ExpectError(const std::expected<T, TsegImportCacheError>& result, TsegImportCacheErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

enum class BadOutput { kNone, kContext, kEid, kAddress, kLength, kAccess, kToken };
struct Provider {
    struct Snapshot {
        urma_seg_t descriptor{};
        urma_token_t token{};
        urma_import_seg_flag_t flag{};
        std::uint64_t address = 0;
    };
    std::atomic<unsigned> imports{0}, unimports{0};
    std::atomic<bool> block{false}, import_failed{false}, unimport_failed{false}, ran_on_owner{false};
    bool mutate_descriptor = false;
    BadOutput bad_output = BadOutput::kNone;
    std::array<urma_target_seg_t, 64> targets{};
    std::array<Snapshot, 64> snapshots{};
    std::thread::id owner = std::this_thread::get_id();
};
Provider* provider = nullptr;

raw::UrmaFunctions Functions() {
    raw::UrmaFunctions functions;
    functions.import_seg = +[](urma_context_t* context, urma_seg_t* descriptor, urma_token_t* token,
                               std::uint64_t address, urma_import_seg_flag_t flag) -> urma_target_seg_t* {
        auto& state = *provider;
        if (std::this_thread::get_id() == state.owner) {
            state.ran_on_owner.store(true);
        }
        const auto index = state.imports.fetch_add(1) % state.targets.size();
        while (state.block.load()) {
            state.block.wait(true);
        }
        if (state.import_failed.load()) {
            errno = EIO;
            return nullptr;
        }
        state.snapshots[index] = {*descriptor, *token, flag, address};
        auto& target = state.targets[index];
        target = {};
        target.urma_ctx = context;
        target.seg = *descriptor;
        target.handle = index + 1;
        switch (state.bad_output) {
        case BadOutput::kContext:
            target.urma_ctx = nullptr;
            break;
        case BadOutput::kEid:
            target.seg.ubva.eid.raw[15]++;
            break;
        case BadOutput::kAddress:
            target.seg.ubva.va++;
            break;
        case BadOutput::kLength:
            target.seg.len--;
            break;
        case BadOutput::kAccess:
            target.seg.attr.bs.access = URMA_ACCESS_READ;
            break;
        case BadOutput::kToken:
            target.seg.token_id++;
            break;
        case BadOutput::kNone:
            break;
        }
        if (state.mutate_descriptor) {
            descriptor->len = 1;
            descriptor->ubva.va = 0;
            token->token++;
        }
        return &target;
    };
    functions.unimport_seg = +[](urma_target_seg_t*) {
        if (std::this_thread::get_id() == provider->owner) {
            provider->ran_on_owner.store(true);
        }
        provider->unimports.fetch_add(1);
        return provider->unimport_failed.load() ? URMA_FAIL : URMA_SUCCESS;
    };
    return functions;
}

class TsegImportCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        provider = &provider_;
        override_ = std::make_unique<raw::test_support::ScopedUrmaOverride>(Functions());
        ASSERT_TRUE(executor_.Open({.workers = 2, .capacity = 4}));
        ASSERT_TRUE(cache_.Open({.entries = 16, .leases = 1024, .in_flight = 4}));
        key_ = {.local_context = &context_,
                .local_context_incarnation = 1,
                .peer_runtime = 10,
                .receive_domain = 20,
                .slab_id = 30,
                .slab_generation = 40,
                .authorization_identity = 50};
        key_.eid[15] = 7;
        descriptor_.ubva.va = 0x100000;
        descriptor_.len = 1u << 20;
        descriptor_.attr.bs.access = URMA_ACCESS_WRITE;
        descriptor_.attr.bs.token_policy = URMA_TOKEN_PLAIN_TEXT;
        descriptor_.token_id = 77;
        SetEid();
    }
    void TearDown() override {
        count_allocations = false;
        provider_.block.store(false);
        provider_.block.notify_all();
        provider_.unimport_failed.store(false);
        // 每个测试保留全部票据副本，失败路径也能归还；重复票据不得影响新引用。
        for (const auto lease : leases_) {
            (void)cache_.Release(lease);
        }
        cache_.StopAccepting();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (cache_.entries() && std::chrono::steady_clock::now() < deadline) {
            const auto next = cache_.NextOperation();
            if (next && *next) {
                EXPECT_TRUE(Start(**next));
            }
            std::array<runtime::ControlTaskCompletion, 4> completions;
            const auto count = executor_.Poll(completions);
            if (count) {
                for (std::size_t i = 0; i < *count; ++i) {
                    EXPECT_TRUE(cache_.OwnsCompletion(completions[i].id));
                    EXPECT_TRUE(cache_.HandleCompletion(completions[i]));
                }
            }
            std::this_thread::yield();
        }
        EXPECT_TRUE(cache_.Close());
        EXPECT_TRUE(executor_.Close());
        EXPECT_FALSE(provider_.ran_on_owner.load());
        override_.reset();
        provider = nullptr;
    }
    void SetEid() {
        std::memcpy(&descriptor_.ubva.eid, key_.eid.data(), key_.eid.size());
    }
    TsegImportRequest Request() const {
        return {key_, descriptor_, {.token = 123}};
    }
    std::expected<TsegImportLease, TsegImportCacheError> Acquire() {
        auto result = cache_.Acquire(Request());
        if (result) {
            leases_.push_back(*result);
        }
        return result;
    }
    std::expected<runtime::ControlTaskId, TsegImportCacheError> Start(const TsegImportOperation& operation) {
        return cache_.StartOperation(operation, executor_,
                                     {.context = operation.context,
                                      .incarnation = operation.context_incarnation,
                                      .users_paused = true,
                                      .exclusive_control = true});
    }
    std::expected<runtime::ControlTaskId, TsegImportCacheError> StartNext() {
        const auto next = cache_.NextOperation();
        if (!next) {
            return std::unexpected(next.error());
        }
        if (!*next) {
            return std::unexpected(TsegImportCacheError{TsegImportCacheErrorCode::kPending});
        }
        return Start(**next);
    }
    std::optional<runtime::ControlTaskCompletion> PollOne() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            std::array<runtime::ControlTaskCompletion, 1> completions;
            const auto count = executor_.Poll(completions);
            if (!count) {
                return std::nullopt;
            }
            if (*count) {
                return completions[0];
            }
            std::this_thread::yield();
        }
        return std::nullopt;
    }
    bool CompleteOne() {
        auto completion = PollOne();
        if (!completion) {
            return false;
        }
        EXPECT_TRUE(cache_.OwnsCompletion(completion->id));
        return cache_.HandleCompletion(*completion).has_value();
    }

    Provider provider_;
    std::unique_ptr<raw::test_support::ScopedUrmaOverride> override_;
    runtime::ControlExecutor executor_;
    TsegImportCache cache_;
    urma_context_t context_{}, second_context_{};
    TsegImportKey key_;
    urma_seg_t descriptor_{};
    std::vector<TsegImportLease> leases_;
};

TEST_F(TsegImportCacheTest, ThousandWaitersShareOneImportAndWarmReuse) {
    for (int i = 0; i < 1000; ++i) {
        ASSERT_TRUE(Acquire());
    }
    EXPECT_EQ(cache_.entries(), 1u);
    EXPECT_EQ(cache_.leases(), 1000u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    ASSERT_TRUE(StartNext());
    ExpectError(cache_.Get(leases_[0]), TsegImportCacheErrorCode::kPending);
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 1u);
    for (const auto lease : leases_) {
        const auto target = cache_.Get(lease);
        ASSERT_TRUE(target);
        EXPECT_EQ(*target, &provider_.targets[0]);
        ASSERT_TRUE(cache_.Release(lease));
    }
    // 段在零引用时保留为暖缓存，新的 Grant 借用不重新 ImportSeg。
    EXPECT_EQ(cache_.entries(), 1u);
    auto warm = Acquire();
    ASSERT_TRUE(warm);
    EXPECT_TRUE(cache_.Get(*warm));
    EXPECT_EQ(provider_.imports.load(), 1u);
    EXPECT_EQ(provider_.snapshots[0].address, 0u);
    EXPECT_EQ(provider_.snapshots[0].flag.bs.access, static_cast<unsigned>(URMA_ACCESS_WRITE));
    ExpectError(cache_.Release(leases_[0]), TsegImportCacheErrorCode::kInvalidLease);
}

TEST_F(TsegImportCacheTest, InvalidatedTargetWaitsForEveryPossibleDmaPin) {
    const auto original = Acquire();
    ASSERT_TRUE(original);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    const auto dma_pin = cache_.Retain(*original);
    ASSERT_TRUE(dma_pin);
    leases_.push_back(*dma_pin);
    ASSERT_TRUE(cache_.Invalidate(*original));
    ExpectError(cache_.Get(*dma_pin), TsegImportCacheErrorCode::kInvalidated);
    ExpectError(cache_.Retain(*dma_pin), TsegImportCacheErrorCode::kInvalidated);
    ASSERT_TRUE(cache_.Release(*original));
    auto next = cache_.NextOperation();
    ASSERT_TRUE(next);
    EXPECT_FALSE(*next);
    ExpectError(cache_.Close(), TsegImportCacheErrorCode::kInUse);
    EXPECT_EQ(provider_.unimports.load(), 0u);
    // 这份 pin 可以代表接受未知的 WR；只有宿主取得独立终结事实才归还。
    ASSERT_TRUE(cache_.Release(*dma_pin));
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.unimports.load(), 1u);
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(TsegImportCacheTest, CancelBeforeSubmissionDoesNotCallProvider) {
    const auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(cache_.Release(*lease));
    const auto next = cache_.NextOperation();
    ASSERT_TRUE(next);
    EXPECT_FALSE(*next);
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
}

TEST_F(TsegImportCacheTest, CancelDuringImportHarvestsThenUnimports) {
    provider_.block.store(true);
    const auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(cache_.Release(*lease));
    ExpectError(cache_.Close(), TsegImportCacheErrorCode::kInUse);
    provider_.block.store(false);
    provider_.block.notify_all();
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 1u);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 1u);
    EXPECT_EQ(provider_.unimports.load(), 1u);
}

TEST_F(TsegImportCacheTest, InvalidationDuringImportRejectsPublicationAndWaiters) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(cache_.Invalidate(key_));
    ExpectError(Acquire(), TsegImportCacheErrorCode::kInvalidated);
    ASSERT_TRUE(CompleteOne());
    ExpectError(cache_.Get(*lease), TsegImportCacheErrorCode::kInvalidated);
    EXPECT_EQ(provider_.unimports.load(), 0u);
    ASSERT_TRUE(cache_.Release(*lease));
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(TsegImportCacheTest, EveryIdentityComponentSeparatesCacheEntries) {
    const auto original = key_;
    // 同 VA/token 也不能跨 context、远端重启、Slab 重建或授权域共享旧导入。
    for (unsigned i = 0; i < 9; ++i) {
        key_ = original;
        switch (i) {
        case 1:
            key_.local_context = &second_context_;
            break;
        case 2:
            ++key_.local_context_incarnation;
            break;
        case 3:
            ++key_.peer_runtime;
            break;
        case 4:
            ++key_.receive_domain;
            break;
        case 5:
            ++key_.slab_id;
            break;
        case 6:
            ++key_.slab_generation;
            break;
        case 7:
            ++key_.eid[15];
            break;
        case 8:
            ++key_.authorization_identity;
            break;
        default:
            break;
        }
        SetEid();
        auto lease = Acquire();
        ASSERT_TRUE(lease);
        auto actual = cache_.GetKey(*lease);
        ASSERT_TRUE(actual);
        EXPECT_EQ(*actual, key_);
    }
    EXPECT_EQ(cache_.entries(), 9u);
    EXPECT_EQ(cache_.leases(), 9u);
}

TEST_F(TsegImportCacheTest, DescriptorOrTokenCannotChangeUnderSameIdentity) {
    ASSERT_TRUE(Acquire());
    for (unsigned i = 0; i < 7; ++i) {
        auto request = Request();
        switch (i) {
        case 0:
            ++request.token.token;
            break;
        case 1:
            ++request.descriptor.ubva.va;
            break;
        case 2:
            ++request.descriptor.ubva.uasid;
            break;
        case 3:
            ++request.descriptor.len;
            break;
        case 4:
            ++request.descriptor.token_id;
            break;
        case 5:
            request.descriptor.attr.bs.access |= URMA_ACCESS_READ;
            break;
        case 6:
            request.descriptor.attr.bs.cacheable = URMA_CACHEABLE;
            break;
        }
        ExpectError(cache_.Acquire(request), TsegImportCacheErrorCode::kDescriptorConflict);
    }
    EXPECT_EQ(cache_.entries(), 1u);
    EXPECT_EQ(cache_.leases(), 1u);
}

TEST_F(TsegImportCacheTest, MissingKeyComponentsRejectBeforeCapacityAdmission) {
    for (unsigned i = 0; i < 8; ++i) {
        auto request = Request();
        switch (i) {
        case 0:
            request.key.local_context = nullptr;
            break;
        case 1:
            request.key.local_context_incarnation = 0;
            break;
        case 2:
            request.key.peer_runtime = 0;
            break;
        case 3:
            request.key.receive_domain = 0;
            break;
        case 4:
            request.key.slab_id = 0;
            break;
        case 5:
            request.key.slab_generation = 0;
            break;
        case 6:
            request.key.authorization_identity = 0;
            break;
        case 7:
            request.key.eid = {};
            request.descriptor.ubva.eid = {};
            break;
        }
        ExpectError(cache_.Acquire(request), TsegImportCacheErrorCode::kInvalidArgument);
    }
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(cache_.leases(), 0u);
}

TEST_F(TsegImportCacheTest, UnsafeDescriptorsRejectBeforeAnyProviderCall) {
    for (unsigned i = 0; i < 17; ++i) {
        auto request = Request();
        switch (i) {
        case 0:
            request.descriptor.ubva.va = 0;
            break;
        case 1:
            request.descriptor.len = 0;
            break;
        case 2:
            request.descriptor.ubva.va = std::numeric_limits<std::uint64_t>::max() - 15;
            request.descriptor.len = 32;
            break;
        case 3:
            request.descriptor.attr.bs.access = URMA_ACCESS_READ;
            break;
        case 4:
            request.descriptor.attr.bs.access |= URMA_ACCESS_LOCAL_ONLY;
            break;
        case 5:
            request.descriptor.attr.bs.dsva = 1;
            break;
        case 6:
            request.descriptor.attr.bs.non_pin = 1;
            break;
        case 7:
            request.descriptor.attr.bs.user_iova = 1;
            break;
        case 8:
            request.descriptor.attr.bs.has_user_info = 1;
            break;
        case 9:
            request.descriptor.attr.bs.reserved = 1;
            break;
        case 10:
            request.descriptor.attr.bs.token_policy = URMA_TOKEN_SIGNED;
            break;
        case 11:
            request.descriptor.attr.bs.token_policy = URMA_TOKEN_ALL_ENCRYPTED;
            break;
        case 12:
            request.descriptor.attr.bs.token_policy = URMA_TOKEN_NONE;
            break;
        case 13:
            request.descriptor.token_id = URMA_SEG_TOKEN_ID_INVALID;
            break;
        case 14:
            request.descriptor.ubva.uasid = 0x1000000;
            break;
        case 15:
            request.descriptor.ubva.eid.raw[15]++;
            break;
        case 16:
            request.descriptor.attr.bs.access |= 1u << 5;
            break;
        }
        SCOPED_TRACE(i);
        ExpectError(cache_.Acquire(request), TsegImportCacheErrorCode::kInvalidDescriptor);
    }
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
}

TEST_F(TsegImportCacheTest, TokenNoneWithZeroValueAndValidBoundaryAreAccepted) {
    auto request = Request();
    request.descriptor.attr.bs.token_policy = URMA_TOKEN_NONE;
    request.token.token = 0;
    request.descriptor.ubva.va = std::numeric_limits<std::uint64_t>::max() - 4096;
    request.descriptor.len = 4096;
    request.descriptor.ubva.uasid = 0xffffff;
    request.descriptor.token_id = 0;
    auto lease = cache_.Acquire(request);
    ASSERT_TRUE(lease);
    leases_.push_back(*lease);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    ASSERT_TRUE(cache_.Get(*lease));
    EXPECT_EQ(provider_.snapshots[0].token.token, 0u);
}

TEST_F(TsegImportCacheTest, RequestAndWorkerMutationDoNotCorruptCanonicalDescriptor) {
    provider_.mutate_descriptor = true;
    const auto original = Request();
    auto request = original;
    const auto lease = cache_.Acquire(request);
    ASSERT_TRUE(lease);
    leases_.push_back(*lease);
    // 请求是按值接收；发送方复用协议缓冲，或 provider 改写参数均不能污染缓存身份。
    request.descriptor = {};
    request.token = {};
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    const auto target = cache_.Get(*lease);
    ASSERT_TRUE(target);
    EXPECT_EQ((*target)->seg.len, original.descriptor.len);
    EXPECT_EQ(provider_.snapshots[0].descriptor.ubva.va, original.descriptor.ubva.va);
    EXPECT_EQ(provider_.snapshots[0].token.token, original.token.token);
    ASSERT_TRUE(Acquire());
    EXPECT_EQ(cache_.entries(), 1u);
    EXPECT_EQ(provider_.imports.load(), 1u);
}

TEST_F(TsegImportCacheTest, MismatchingProviderOutputIsNeverPublishedAndStillUnimported) {
    constexpr std::array bad_outputs{BadOutput::kEid, BadOutput::kAddress, BadOutput::kLength, BadOutput::kAccess,
                                     BadOutput::kToken};
    for (const auto output : bad_outputs) {
        provider_.bad_output = output;
        const auto lease = Acquire();
        ASSERT_TRUE(lease);
        ASSERT_TRUE(StartNext());
        ASSERT_TRUE(CompleteOne());
        ExpectError(cache_.Get(*lease), TsegImportCacheErrorCode::kProviderError);
        ExpectError(Acquire(), TsegImportCacheErrorCode::kProviderError);
        ASSERT_TRUE(cache_.Release(*lease));
        EXPECT_EQ(cache_.entries(), 1u);
        // provider 返回了非空半成品，不能只因校验失败就遗失句柄。
        ASSERT_TRUE(StartNext());
        ASSERT_TRUE(CompleteOne());
        EXPECT_EQ(cache_.entries(), 0u);
    }
    EXPECT_EQ(provider_.imports.load(), bad_outputs.size());
    EXPECT_EQ(provider_.unimports.load(), bad_outputs.size());
}

TEST_F(TsegImportCacheTest, FailedImportIsSharedAndReleasedWithoutUnimport) {
    provider_.import_failed.store(true);
    auto first = Acquire();
    auto second = Acquire();
    ASSERT_TRUE(first && second);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    const auto result = cache_.Get(*first);
    ExpectError(result, TsegImportCacheErrorCode::kProviderError);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().provider_error, EIO);
    ExpectError(cache_.Get(*second), TsegImportCacheErrorCode::kProviderError);
    ExpectError(Acquire(), TsegImportCacheErrorCode::kProviderError);
    ASSERT_TRUE(cache_.Release(*first));
    ASSERT_TRUE(cache_.Release(*second));
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(provider_.unimports.load(), 0u);
    provider_.import_failed.store(false);
    ASSERT_TRUE(Acquire());
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 2u);
}

TEST_F(TsegImportCacheTest, FailedUnimportRetainsHandleUntilRetrySucceeds) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    ASSERT_TRUE(cache_.Release(*lease));
    EXPECT_EQ(cache_.EvictUnused(), 1u);
    provider_.unimport_failed.store(true);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 1u);
    ExpectError(cache_.Close(), TsegImportCacheErrorCode::kInUse);
    provider_.unimport_failed.store(false);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.unimports.load(), 2u);
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(TsegImportCacheTest, LeaseCapacityAndAntiAbaProtectReusedReferenceSlot) {
    for (unsigned i = 0; i < 1024; ++i) {
        ASSERT_TRUE(Acquire());
    }
    ExpectError(Acquire(), TsegImportCacheErrorCode::kResourceExhausted);
    const auto stale = leases_[0];
    ASSERT_TRUE(cache_.Release(stale));
    const auto replacement = Acquire();
    ASSERT_TRUE(replacement);
    EXPECT_EQ(stale.slot, replacement->slot);
    EXPECT_NE(stale.sequence, replacement->sequence);
    ExpectError(cache_.Release(stale), TsegImportCacheErrorCode::kInvalidLease);
    ExpectError(cache_.Get(stale), TsegImportCacheErrorCode::kInvalidLease);
    auto foreign = *replacement;
    TsegImportCache other;
    foreign.owner = &other;
    ExpectError(cache_.Release(foreign), TsegImportCacheErrorCode::kInvalidLease);
    EXPECT_EQ(cache_.leases(), 1024u);
}

TEST_F(TsegImportCacheTest, EntryCapacityFailureDoesNotCreateWaiter) {
    for (unsigned i = 0; i < 16; ++i) {
        ++key_.slab_generation;
        ASSERT_TRUE(Acquire());
    }
    ++key_.slab_generation;
    ExpectError(Acquire(), TsegImportCacheErrorCode::kResourceExhausted);
    EXPECT_EQ(cache_.entries(), 16u);
    EXPECT_EQ(cache_.leases(), 16u);
}

TEST_F(TsegImportCacheTest, CloseStopsAdmissionWithoutResettingTicketGeneration) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ExpectError(cache_.Close(), TsegImportCacheErrorCode::kInUse);
    ExpectError(Acquire(), TsegImportCacheErrorCode::kStopped);
    ExpectError(cache_.Retain(*lease), TsegImportCacheErrorCode::kStopped);
    ASSERT_TRUE(cache_.Release(*lease));
    ASSERT_TRUE(cache_.Close());
    ExpectError(cache_.Open(), TsegImportCacheErrorCode::kInUse);
}

TEST_F(TsegImportCacheTest, MissingPauseProofRejectsBeforeSubmission) {
    ASSERT_TRUE(Acquire());
    auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    TsegImportContextPause pause{.context = &context_, .incarnation = 1};
    ExpectError(cache_.StartOperation(**next, executor_, pause), TsegImportCacheErrorCode::kInvalidArgument);
    pause.users_paused = true;
    ExpectError(cache_.StartOperation(**next, executor_, pause), TsegImportCacheErrorCode::kInvalidArgument);
    pause.exclusive_control = true;
    pause.incarnation = 2;
    ExpectError(cache_.StartOperation(**next, executor_, pause), TsegImportCacheErrorCode::kInvalidArgument);
    EXPECT_EQ(executor_.outstanding(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    ASSERT_TRUE(Start(**next));
    ASSERT_TRUE(CompleteOne());
}

TEST_F(TsegImportCacheTest, PreparedReservationLivesUntilDiscardEvenAfterLastReferenceCancelled) {
    const auto lease = Acquire();
    ASSERT_TRUE(lease);
    const auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    const auto prepared = cache_.PrepareOperation(**next);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(cache_.Release(*lease));
    ExpectError(cache_.Close(), TsegImportCacheErrorCode::kInUse);
    EXPECT_EQ(cache_.in_flight(), 1u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    ASSERT_TRUE(cache_.DiscardOperation(*prepared));
    EXPECT_EQ(cache_.in_flight(), 0u);
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(TsegImportCacheTest, OldPreparedReservationCannotCommitOrDiscardReplacement) {
    ASSERT_TRUE(Acquire());
    const auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    const auto first = cache_.PrepareOperation(**next);
    ASSERT_TRUE(first);
    ASSERT_TRUE(cache_.DiscardOperation(*first));
    const auto second = cache_.PrepareOperation(**next);
    ASSERT_TRUE(second);
    EXPECT_NE(first->reservation, second->reservation);
    ExpectError(cache_.DiscardOperation(*first), TsegImportCacheErrorCode::kInvalidOperation);
    ExpectError(cache_.CommitOperation(*first, {&executor_, 0, 1}), TsegImportCacheErrorCode::kInvalidOperation);
    EXPECT_EQ(cache_.in_flight(), 1u);
    ASSERT_TRUE(cache_.DiscardOperation(*second));
}

TEST_F(TsegImportCacheTest, RejectedExecutorSubmissionReturnsPreparedCapacity) {
    const auto lease = Acquire();
    ASSERT_TRUE(lease);
    const auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    // 执行器明确未接受任务，缓存必须撤销 reservation，允许宿主稍后重新调度。
    executor_.StopAccepting();
    ExpectError(Start(**next), TsegImportCacheErrorCode::kExecutorError);
    EXPECT_EQ(cache_.in_flight(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    const auto retry = cache_.NextOperation();
    ASSERT_TRUE(retry && *retry);
    EXPECT_EQ(**retry, **next);
    ASSERT_TRUE(cache_.Release(*lease));
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(TsegImportCacheTest, AlteredPreparedTaskCannotCommitOrDiscardLiveReservation) {
    ASSERT_TRUE(Acquire());
    const auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    const auto prepared = cache_.PrepareOperation(**next);
    ASSERT_TRUE(prepared);
    for (unsigned i = 0; i < 5; ++i) {
        auto altered = *prepared;
        switch (i) {
        case 0:
            ++altered.operation.context_incarnation;
            break;
        case 1:
            ++altered.operation.generation;
            break;
        case 2:
            ++altered.task.token;
            break;
        case 3:
            altered.task.argument = nullptr;
            break;
        case 4:
            altered.task.cancelled = true;
            break;
        }
        ExpectError(cache_.DiscardOperation(altered), TsegImportCacheErrorCode::kInvalidOperation);
        ExpectError(cache_.CommitOperation(altered, {&executor_, 0, 1}), TsegImportCacheErrorCode::kInvalidOperation);
    }
    EXPECT_EQ(cache_.in_flight(), 1u);
    ASSERT_TRUE(cache_.DiscardOperation(*prepared));
}

TEST_F(TsegImportCacheTest, WorkerCanFinishBeforeOwnerCommitsWithoutOverwritingResult) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    auto prepared = cache_.PrepareOperation(**next);
    ASSERT_TRUE(prepared);
    auto submitted = executor_.TrySubmit(prepared->task);
    ASSERT_TRUE(submitted);
    auto completion = PollOne();
    ASSERT_TRUE(completion);
    // 明确让 worker 已完成再 Commit，owner 不能覆写工作结果，也不能提前对外发布句柄。
    ExpectError(cache_.Get(*lease), TsegImportCacheErrorCode::kPending);
    ASSERT_TRUE(cache_.CommitOperation(*prepared, *submitted));
    ExpectError(cache_.DiscardOperation(*prepared), TsegImportCacheErrorCode::kInvalidOperation);
    ASSERT_TRUE(cache_.HandleCompletion(*completion));
    EXPECT_TRUE(cache_.Get(*lease));
}

TEST_F(TsegImportCacheTest, ExecutorCancellationStillHarvestsAndCleansResult) {
    provider_.block.store(true);
    const auto lease = Acquire();
    ASSERT_TRUE(lease);
    const auto submitted = StartNext();
    ASSERT_TRUE(submitted);
    ASSERT_TRUE(executor_.Cancel(*submitted));
    provider_.block.store(false);
    provider_.block.notify_all();
    ASSERT_TRUE(CompleteOne());
    ExpectError(cache_.Get(*lease), TsegImportCacheErrorCode::kInvalidated);
    ASSERT_TRUE(cache_.Release(*lease));
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.unimports.load(), 1u);
}

TEST_F(TsegImportCacheTest, PreparedCancellationStillCleansLaterAcceptedTask) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    const auto next = cache_.NextOperation();
    ASSERT_TRUE(next && *next);
    const auto prepared = cache_.PrepareOperation(**next);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(cache_.Release(*lease));
    const auto submitted = executor_.TrySubmit(prepared->task);
    ASSERT_TRUE(submitted);
    ASSERT_TRUE(cache_.CommitOperation(*prepared, *submitted));
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 1u);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(TsegImportCacheTest, SameContextSerializesUntilHarvestWhileAnotherCanProgress) {
    ASSERT_TRUE(Acquire());
    ASSERT_TRUE(StartNext());
    ++key_.slab_id;
    ASSERT_TRUE(Acquire());
    auto blocked = cache_.NextOperation();
    ASSERT_TRUE(blocked);
    EXPECT_FALSE(*blocked);
    key_.local_context = &second_context_;
    ASSERT_TRUE(Acquire());
    ASSERT_TRUE(StartNext());
    EXPECT_EQ(cache_.in_flight(), 2u);
    auto first = PollOne();
    auto second = PollOne();
    ASSERT_TRUE(first && second);
    // Poll 收到结果尚不等于恢复 context 使用；只有缓存收割才解除其内部在途门禁。
    blocked = cache_.NextOperation();
    ASSERT_TRUE(blocked);
    EXPECT_FALSE(*blocked);
    ASSERT_TRUE(cache_.HandleCompletion(*first));
    ASSERT_TRUE(cache_.HandleCompletion(*second));
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 3u);
}

TEST_F(TsegImportCacheTest, ContextFilterAndPreparedReservationPreventConflictingWork) {
    ASSERT_TRUE(Acquire());
    key_.local_context = &second_context_;
    key_.local_context_incarnation = 2;
    ASSERT_TRUE(Acquire());
    ExpectError(cache_.NextOperation(&context_, 0), TsegImportCacheErrorCode::kInvalidArgument);
    ExpectError(cache_.NextOperation(nullptr, 1), TsegImportCacheErrorCode::kInvalidArgument);
    const auto absent = cache_.NextOperation(&context_, 2);
    ASSERT_TRUE(absent);
    EXPECT_FALSE(*absent);
    const auto first = cache_.NextOperation(&context_, 1);
    ASSERT_TRUE(first && *first);
    const auto prepared = cache_.PrepareOperation(**first);
    ASSERT_TRUE(prepared);
    const auto busy = cache_.NextOperation(&context_, 1);
    ASSERT_TRUE(busy);
    EXPECT_FALSE(*busy);
    const auto second = cache_.NextOperation(&second_context_, 2);
    ASSERT_TRUE(second && *second);
    EXPECT_EQ((**second).context, &second_context_);
    ASSERT_TRUE(cache_.DiscardOperation(*prepared));
}

TEST_F(TsegImportCacheTest, StaleOrAlteredCompletionCannotPublishOrReleaseContext) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(StartNext());
    const auto completion = PollOne();
    ASSERT_TRUE(completion);
    auto bad = *completion;
    ++bad.token;
    ExpectError(cache_.HandleCompletion(bad), TsegImportCacheErrorCode::kInvalidCompletion);
    bad = *completion;
    bad.argument = nullptr;
    ExpectError(cache_.HandleCompletion(bad), TsegImportCacheErrorCode::kInvalidCompletion);
    bad = *completion;
    ++bad.id.sequence;
    EXPECT_FALSE(cache_.OwnsCompletion(bad.id));
    ExpectError(cache_.HandleCompletion(bad), TsegImportCacheErrorCode::kInvalidCompletion);
    EXPECT_EQ(cache_.in_flight(), 1u);
    ExpectError(cache_.Get(*lease), TsegImportCacheErrorCode::kPending);
    ASSERT_TRUE(cache_.HandleCompletion(*completion));
    ExpectError(cache_.HandleCompletion(*completion), TsegImportCacheErrorCode::kInvalidCompletion);
    EXPECT_TRUE(cache_.Get(*lease));
}

TEST_F(TsegImportCacheTest, EvictionLeavesActiveEntriesUsable) {
    const auto warm = Acquire();
    ASSERT_TRUE(warm);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    ASSERT_TRUE(cache_.Release(*warm));
    ++key_.slab_id;
    const auto active = Acquire();
    ASSERT_TRUE(active);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.EvictUnused(), 1u);
    EXPECT_TRUE(cache_.Get(*active));
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 1u);
    EXPECT_TRUE(cache_.Get(*active));
}

TEST_F(TsegImportCacheTest, OwnerAdmissionProgressAndReleaseDoNotAllocateOrFree) {
    const auto request = Request();
    allocations = deallocations = 0;
    count_allocations = true;
    const auto lease = cache_.Acquire(request);
    const auto next = cache_.NextOperation();
    std::optional<TsegImportPreparedOperation> prepared;
    std::optional<runtime::ControlTaskId> submitted;
    bool committed = false, harvested = false;
    if (next && *next) {
        const auto result = cache_.PrepareOperation(**next);
        if (result) {
            prepared = *result;
            const auto task = executor_.TrySubmit(prepared->task);
            if (task) {
                submitted = *task;
                committed = cache_.CommitOperation(*prepared, *task).has_value();
            }
        }
    }
    if (committed) {
        const auto completion = PollOne();
        if (completion) {
            harvested = cache_.HandleCompletion(*completion).has_value();
        }
    }
    bool accessed = false, released = false;
    if (lease && harvested) {
        const auto copy = cache_.Retain(*lease);
        accessed = cache_.Get(*lease).has_value() && cache_.GetKey(*lease).has_value();
        if (copy) {
            released = cache_.Release(*copy).has_value();
        }
        released = cache_.Release(*lease).has_value() && released;
        (void)cache_.EvictUnused();
    }
    count_allocations = false;
    if (lease) {
        leases_.push_back(*lease);
    }
    ASSERT_TRUE(lease);
    EXPECT_TRUE(prepared && submitted && committed && harvested && accessed && released);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
}

TEST(TsegImportCacheConfigTest, RejectsInvalidBudgetsAndAllowsRetryBeforeSuccessfulOpen) {
    TsegImportCache cache;
    ExpectError(cache.Open({.entries = 0}), TsegImportCacheErrorCode::kInvalidArgument);
    ExpectError(cache.Open({.entries = TsegImportCache::kMaxEntries + 1}), TsegImportCacheErrorCode::kInvalidArgument);
    ExpectError(cache.Open({.leases = 0}), TsegImportCacheErrorCode::kInvalidArgument);
    ExpectError(cache.Open({.leases = TsegImportCache::kMaxLeases + 1}), TsegImportCacheErrorCode::kInvalidArgument);
    ExpectError(cache.Open({.in_flight = 0}), TsegImportCacheErrorCode::kInvalidArgument);
    ExpectError(cache.Open({.entries = 1, .in_flight = 2}), TsegImportCacheErrorCode::kInvalidArgument);
    ASSERT_TRUE(cache.Open({.entries = 1, .leases = 1, .in_flight = 1}));
    ASSERT_TRUE(cache.Close());
    ExpectError(cache.Open(), TsegImportCacheErrorCode::kInUse);
}

TEST(TsegImportCacheRetentionTest, WrongProviderContextCannotBeCleanedUnderRequestedContextPause) {
    // 故意构造无法安全回收的 provider 违规。假 context/target 和函数存储活至进程退出；
    // cache 析构保留其内部状态，不能在原 context 的屏障中调用另一 context 的 unimport。
    static urma_context_t requested_context{}, foreign_context{};
    static urma_target_seg_t foreign_target{};
    static std::atomic<unsigned> unimports{0};
    unimports.store(0);
    raw::UrmaFunctions functions;
    functions.import_seg = +[](urma_context_t*, urma_seg_t* descriptor, urma_token_t*, std::uint64_t,
                               urma_import_seg_flag_t) -> urma_target_seg_t* {
        foreign_target.seg = *descriptor;
        foreign_target.urma_ctx = &foreign_context;
        return &foreign_target;
    };
    functions.unimport_seg = +[](urma_target_seg_t*) {
        unimports.fetch_add(1);
        return URMA_SUCCESS;
    };
    raw::test_support::ScopedUrmaOverride override(functions);
    runtime::ControlExecutor executor;
    ASSERT_TRUE(executor.Open({.workers = 1, .capacity = 1}));
    TsegImportCache cache;
    ASSERT_TRUE(cache.Open({.entries = 1, .leases = 1, .in_flight = 1}));
    TsegImportRequest request;
    request.key = {.local_context = &requested_context,
                   .local_context_incarnation = 1,
                   .peer_runtime = 2,
                   .receive_domain = 3,
                   .slab_id = 4,
                   .slab_generation = 5,
                   .authorization_identity = 6};
    request.key.eid[15] = 7;
    request.descriptor.ubva.eid.raw[15] = 7;
    request.descriptor.ubva.va = 4096;
    request.descriptor.len = 4096;
    request.descriptor.attr.bs.access = URMA_ACCESS_WRITE;
    const auto lease = cache.Acquire(request);
    ASSERT_TRUE(lease);
    const auto next = cache.NextOperation();
    ASSERT_TRUE(next && *next);
    ASSERT_TRUE(cache.StartOperation(
        **next, executor,
        {.context = &requested_context, .incarnation = 1, .users_paused = true, .exclusive_control = true}));
    std::array<runtime::ControlTaskCompletion, 1> completion;
    std::size_t count = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!count && std::chrono::steady_clock::now() < deadline) {
        auto polled = executor.Poll(completion);
        ASSERT_TRUE(polled);
        count = *polled;
        std::this_thread::yield();
    }
    ASSERT_EQ(count, 1u);
    ASSERT_TRUE(cache.HandleCompletion(completion[0]));
    const auto get = cache.Get(*lease);
    ExpectError(get, TsegImportCacheErrorCode::kProviderError);
    ASSERT_FALSE(get);
    EXPECT_EQ(get.error().provider_error, EPROTO);
    ASSERT_TRUE(cache.Release(*lease));
    ExpectError(cache.Close(), TsegImportCacheErrorCode::kInUse);
    const auto cleanup = cache.NextOperation();
    ASSERT_TRUE(cleanup);
    EXPECT_FALSE(*cleanup);
    EXPECT_EQ(cache.entries(), 1u);
    EXPECT_EQ(unimports.load(), 0u);
    ASSERT_TRUE(executor.Close());
}

} // namespace
} // namespace control
} // namespace kbsocket
