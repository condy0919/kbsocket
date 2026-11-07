// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/endpoint_import_cache.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace control {
namespace {

template <typename T>
void ExpectError(const std::expected<T, ImportCacheError>& result, ImportCacheErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

struct Provider {
    std::atomic<unsigned> imports{0}, unimports{0};
    std::atomic<bool> block{false}, import_failed{false}, unimport_failed{false};
    bool mutate_descriptor = false;
    std::array<urma_target_jetty_t, 64> targets{};
    std::array<std::byte, kMaxEndpointDescriptorBytes> last_descriptor{};
    urma_token_t last_token{};
    std::mutex snapshot_mutex;
    std::thread::id owner = std::this_thread::get_id();
};
Provider* provider = nullptr;

raw::UrmaFunctions Functions() {
    raw::UrmaFunctions functions;
    functions.import_jetty =
        +[](urma_context_t* context, urma_rjetty_t* descriptor, urma_token_t* token) -> urma_target_jetty_t* {
        auto& state = *provider;
        EXPECT_NE(std::this_thread::get_id(), state.owner);
        const auto index = state.imports.fetch_add(1);
        while (state.block.load()) {
            state.block.wait(true);
        }
        if (state.import_failed.load()) {
            errno = EIO;
            return nullptr;
        }
        std::size_t size = sizeof(*descriptor);
        if (descriptor->flag.bs.has_user_info) {
            std::uint32_t length;
            std::memcpy(&length, descriptor + 1, sizeof(length));
            size += sizeof(length) + length;
        }
        {
            std::lock_guard guard(state.snapshot_mutex);
            std::memcpy(state.last_descriptor.data(), descriptor, size);
            state.last_token = *token;
        }
        auto& target = state.targets[index % state.targets.size()];
        target.urma_ctx = context;
        target.id = descriptor->jetty_id;
        target.trans_mode = descriptor->trans_mode;
        target.type = descriptor->type;
        target.tp_type = descriptor->tp_type;
        if (state.mutate_descriptor) {
            descriptor->flag.bs.order_type = 4;
            token->token = 999;
        }
        return &target;
    };
    functions.unimport_jetty = +[](urma_target_jetty_t* target) {
        EXPECT_NE(std::this_thread::get_id(), provider->owner);
        EXPECT_NE(target, nullptr);
        provider->unimports.fetch_add(1);
        return provider->unimport_failed.load() ? URMA_FAIL : URMA_SUCCESS;
    };
    return functions;
}

class ImportCacheTest : public ::testing::Test {
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
                .endpoint_generation = 30,
                .jetty_id = 40,
                .authorization_identity = 50};
        key_.eid[15] = 7;
        SetDescriptor(key_);
    }
    void TearDown() override {
        provider_.block.store(false);
        provider_.block.notify_all();
        provider_.unimport_failed.store(false);
        // 测试中可能已取消/释放；逐票据尝试只用于兜底归还，重复票据的失败不会清其他引用。
        for (const auto lease : leases_) {
            (void)cache_.Release(lease);
        }
        cache_.StopAccepting();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (cache_.entries() && std::chrono::steady_clock::now() < deadline) {
            const auto operation = cache_.NextOperation();
            if (operation && *operation) {
                EXPECT_TRUE(Start(**operation));
            }
            std::array<runtime::ControlTaskCompletion, 8> completions;
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
        override_.reset();
        provider = nullptr;
    }
    void SetDescriptor(const EndpointImportKey& key) {
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = key.jetty_id;
        std::memcpy(&descriptor.jetty_id.eid, key.eid.data(), key.eid.size());
        descriptor.trans_mode = key.transport_mode;
        descriptor.tp_type = key.tp_type;
        descriptor.type = URMA_JETTY;
        std::memcpy(bytes_.data(), &descriptor, sizeof(descriptor));
        size_ = sizeof(descriptor);
    }
    EndpointImportRequest Request() {
        return {key_, std::span(bytes_).first(size_), {.token = 123}};
    }
    std::expected<ImportLease, ImportCacheError> Acquire() {
        auto result = cache_.Acquire(Request());
        if (result) {
            leases_.push_back(*result);
        }
        return result;
    }
    std::expected<runtime::ControlTaskId, ImportCacheError> Start(const ImportOperation& operation) {
        return cache_.StartOperation(operation, executor_,
                                     {.context = operation.context,
                                      .incarnation = operation.context_incarnation,
                                      .users_paused = true,
                                      .exclusive_control = true});
    }
    std::expected<runtime::ControlTaskId, ImportCacheError> StartNext() {
        const auto operation = cache_.NextOperation();
        if (!operation) {
            return std::unexpected(operation.error());
        }
        if (!*operation) {
            return std::unexpected(ImportCacheError{ImportCacheErrorCode::kPending});
        }
        return Start(**operation);
    }
    bool CompleteOne() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            std::array<runtime::ControlTaskCompletion, 1> completions;
            const auto count = executor_.Poll(completions);
            if (!count) {
                return false;
            }
            if (*count) {
                EXPECT_TRUE(cache_.OwnsCompletion(completions[0].id));
                last_completion_ = completions[0];
                return cache_.HandleCompletion(completions[0]).has_value();
            }
            std::this_thread::yield();
        }
        return false;
    }

    Provider provider_;
    std::unique_ptr<raw::test_support::ScopedUrmaOverride> override_;
    runtime::ControlExecutor executor_;
    EndpointImportCache cache_;
    urma_context_t context_{}, second_context_{};
    EndpointImportKey key_;
    std::array<std::byte, sizeof(urma_rjetty_t) + 12> bytes_{};
    std::size_t size_ = 0;
    std::vector<ImportLease> leases_;
    runtime::ControlTaskCompletion last_completion_;
};

TEST_F(ImportCacheTest, ThousandConcurrentRequestsShareOneImportAndWarmHandle) {
    for (int i = 0; i < 1000; ++i) {
        ASSERT_TRUE(Acquire());
    }
    EXPECT_EQ(cache_.entries(), 1u);
    EXPECT_EQ(cache_.leases(), 1000u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    ASSERT_TRUE(StartNext());
    ExpectError(cache_.Get(leases_[0]), ImportCacheErrorCode::kPending);
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 1u);
    for (const auto lease : leases_) {
        const auto target = cache_.Get(lease);
        ASSERT_TRUE(target);
        EXPECT_EQ(*target, &provider_.targets[0]);
        ASSERT_TRUE(cache_.Release(lease));
    }
    EXPECT_EQ(cache_.leases(), 0u);
    EXPECT_EQ(cache_.entries(), 1u);
    // 零引用暖条目继续保留；再次建链不重复 import，也不误将已释放票据当新引用。
    auto warm = Acquire();
    ASSERT_TRUE(warm);
    ASSERT_TRUE(cache_.Get(*warm));
    EXPECT_EQ(provider_.imports.load(), 1u);
    ExpectError(cache_.Release(leases_[0]), ImportCacheErrorCode::kInvalidLease);
}

TEST_F(ImportCacheTest, RetainedDependencyPinsInvalidatedTargetUntilLastRelease) {
    auto original = Acquire();
    ASSERT_TRUE(original);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    auto retained = cache_.Retain(*original);
    ASSERT_TRUE(retained);
    leases_.push_back(*retained);
    EXPECT_NE(*original, *retained);
    ASSERT_TRUE(cache_.Invalidate(*original));
    ExpectError(cache_.Get(*retained), ImportCacheErrorCode::kInvalidated);
    ExpectError(cache_.Retain(*original), ImportCacheErrorCode::kInvalidated);
    ASSERT_TRUE(cache_.Release(*original));
    auto operation = cache_.NextOperation();
    ASSERT_TRUE(operation);
    EXPECT_FALSE(*operation);
    EXPECT_EQ(provider_.unimports.load(), 0u);
    // 代表已接受 WR 的第二份引用归还前，失效不是 unimport 的许可证。
    ASSERT_TRUE(cache_.Release(*retained));
    operation = cache_.NextOperation();
    ASSERT_TRUE(operation);
    ASSERT_TRUE(*operation);
    EXPECT_EQ((**operation).kind, ImportOperationKind::kUnimport);
    ASSERT_TRUE(Start(**operation));
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.unimports.load(), 1u);
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(ImportCacheTest, CancelBeforeSubmissionDoesNotCallProvider) {
    const auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(cache_.Release(*lease));
    auto operation = cache_.NextOperation();
    ASSERT_TRUE(operation);
    EXPECT_FALSE(*operation);
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
}

TEST_F(ImportCacheTest, CancelDuringImportCollectsAndCleansAcceptedResult) {
    provider_.block.store(true);
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(cache_.Release(*lease));
    ExpectError(cache_.Close(), ImportCacheErrorCode::kInUse);
    EXPECT_EQ(cache_.in_flight(), 1u);
    provider_.block.store(false);
    provider_.block.notify_all();
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.entries(), 1u);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 1u);
    EXPECT_EQ(provider_.unimports.load(), 1u);
    ASSERT_TRUE(cache_.Close());
}

TEST_F(ImportCacheTest, InvalidationDuringImportRejectsWaitersAndPublication) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(cache_.Invalidate(key_));
    ExpectError(Acquire(), ImportCacheErrorCode::kInvalidated);
    ASSERT_TRUE(CompleteOne());
    ExpectError(cache_.Get(*lease), ImportCacheErrorCode::kInvalidated);
    const auto key = cache_.GetKey(*lease);
    ASSERT_TRUE(key);
    EXPECT_EQ(*key, key_);
    EXPECT_EQ(provider_.unimports.load(), 0u);
}

TEST_F(ImportCacheTest, EveryIncarnationAndAuthorizationPartSeparatesEntries) {
    const auto original = key_;
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
            ++key_.endpoint_generation;
            break;
        case 6:
            ++key_.eid[15];
            break;
        case 7:
            ++key_.jetty_id;
            break;
        case 8:
            ++key_.authorization_identity;
            break;
        default:
            break;
        }
        SetDescriptor(key_);
        ASSERT_TRUE(Acquire());
    }
    EXPECT_EQ(cache_.entries(), 9u);
}

TEST_F(ImportCacheTest, SameKeyWithDifferentDescriptorOrTokenIsRejected) {
    ASSERT_TRUE(Acquire());
    auto request = Request();
    ++request.token.token;
    ExpectError(cache_.Acquire(request), ImportCacheErrorCode::kDescriptorConflict);
    urma_rjetty_t descriptor{};
    std::memcpy(&descriptor, bytes_.data(), sizeof(descriptor));
    descriptor.flag.bs.order_type = 4;
    std::memcpy(bytes_.data(), &descriptor, sizeof(descriptor));
    ExpectError(Acquire(), ImportCacheErrorCode::kDescriptorConflict);
    EXPECT_EQ(cache_.leases(), 1u);
}

TEST_F(ImportCacheTest, DescriptorAndKeyAreCheckedBeforeAdmission) {
    auto request = Request();
    request.descriptor = request.descriptor.first(4);
    ExpectError(cache_.Acquire(request), ImportCacheErrorCode::kInvalidDescriptor);
    request = Request();
    ++request.key.jetty_id;
    ExpectError(cache_.Acquire(request), ImportCacheErrorCode::kInvalidDescriptor);
    request = Request();
    ++request.key.eid[15];
    ExpectError(cache_.Acquire(request), ImportCacheErrorCode::kInvalidDescriptor);
    request = Request();
    request.key.tp_type = URMA_RTP;
    ExpectError(cache_.Acquire(request), ImportCacheErrorCode::kInvalidArgument);
    request = Request();
    request.key.authorization_identity = 0;
    ExpectError(cache_.Acquire(request), ImportCacheErrorCode::kInvalidArgument);
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(ImportCacheTest, ExtensionCopiedAndProviderMutationDoesNotChangeCanonicalIdentity) {
    urma_rjetty_t descriptor{};
    std::memcpy(&descriptor, bytes_.data(), sizeof(descriptor));
    descriptor.flag.bs.has_user_info = 1;
    std::memcpy(bytes_.data(), &descriptor, sizeof(descriptor));
    const std::array<std::uint32_t, 3> extension{8, 0x12345678, 0xabcdef01};
    std::memcpy(bytes_.data() + sizeof(descriptor), extension.data(), sizeof(extension));
    size_ = bytes_.size();
    const auto original = bytes_;
    provider_.mutate_descriptor = true;
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    // Acquire 返回后原导出缓冲可销毁；worker 只访问缓存预分配的副本。
    bytes_.fill(std::byte{0});
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(std::memcmp(original.data(), provider_.last_descriptor.data(), size_), 0);
    EXPECT_EQ(provider_.last_token.token, 123u);
    bytes_ = original;
    ASSERT_TRUE(Acquire());
    EXPECT_EQ(cache_.entries(), 1u);
}

TEST_F(ImportCacheTest, InvalidExtensionLengthAndDriverExtensionAreRejected) {
    urma_rjetty_t descriptor{};
    std::memcpy(&descriptor, bytes_.data(), sizeof(descriptor));
    descriptor.flag.bs.has_user_info = 1;
    std::memcpy(bytes_.data(), &descriptor, sizeof(descriptor));
    ExpectError(Acquire(), ImportCacheErrorCode::kInvalidDescriptor);
    size_ = bytes_.size();
    const std::uint32_t oversized = 4096;
    std::memcpy(bytes_.data() + sizeof(descriptor), &oversized, sizeof(oversized));
    ExpectError(Acquire(), ImportCacheErrorCode::kInvalidDescriptor);
    SetDescriptor(key_);
    descriptor.flag.bs.has_user_info = 0;
    descriptor.flag.bs.has_drv_ext = 1;
    std::memcpy(bytes_.data(), &descriptor, sizeof(descriptor));
    ExpectError(Acquire(), ImportCacheErrorCode::kInvalidDescriptor);
}

TEST_F(ImportCacheTest, FailedImportIsSharedAndReleasedWithoutUnimport) {
    provider_.import_failed.store(true);
    auto first = Acquire();
    auto second = Acquire();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    const auto result = cache_.Get(*first);
    ExpectError(result, ImportCacheErrorCode::kProviderError);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().provider_error, EIO);
    ExpectError(cache_.Get(*second), ImportCacheErrorCode::kProviderError);
    ExpectError(Acquire(), ImportCacheErrorCode::kProviderError);
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

TEST_F(ImportCacheTest, FailedUnimportRetainsHandleForRetry) {
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
    ExpectError(cache_.Close(), ImportCacheErrorCode::kInUse);
    provider_.unimport_failed.store(false);
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.unimports.load(), 2u);
    EXPECT_EQ(cache_.entries(), 0u);
}

TEST_F(ImportCacheTest, MissingPauseProofRejectsBeforeSubmitting) {
    ASSERT_TRUE(Acquire());
    auto operation = cache_.NextOperation();
    ASSERT_TRUE(operation);
    ASSERT_TRUE(*operation);
    ExpectError(cache_.StartOperation(**operation, executor_, {}), ImportCacheErrorCode::kInvalidArgument);
    EXPECT_EQ(executor_.outstanding(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    ASSERT_TRUE(Start(**operation));
    ExpectError(Start(**operation), ImportCacheErrorCode::kInvalidOperation);
    ASSERT_TRUE(CompleteOne());
}

TEST_F(ImportCacheTest, SameContextSerializedWhileDifferentContextCanProgress) {
    ASSERT_TRUE(Acquire());
    ASSERT_TRUE(StartNext());
    ++key_.jetty_id;
    SetDescriptor(key_);
    ASSERT_TRUE(Acquire());
    auto none = cache_.NextOperation();
    ASSERT_TRUE(none);
    EXPECT_FALSE(*none);
    key_.local_context = &second_context_;
    ASSERT_TRUE(Acquire());
    ASSERT_TRUE(StartNext());
    EXPECT_EQ(cache_.in_flight(), 2u);
    ASSERT_TRUE(CompleteOne());
    ASSERT_TRUE(CompleteOne());
    ASSERT_TRUE(StartNext());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 3u);
}

TEST_F(ImportCacheTest, OnlyHarvestedContextReopensAfterOutOfOrderCompletions) {
    ASSERT_TRUE(Acquire());
    const auto first = StartNext();
    ASSERT_TRUE(first);
    key_.local_context = &second_context_;
    ASSERT_TRUE(Acquire());
    ASSERT_TRUE(StartNext());

    // 每个 context 另有一个等待导入的端点；worker 完成不等于 owner 已取得恢复使用的屏障。
    ++key_.jetty_id;
    SetDescriptor(key_);
    ASSERT_TRUE(Acquire());
    key_.local_context = &context_;
    ASSERT_TRUE(Acquire());
    std::array<runtime::ControlTaskCompletion, 2> completed;
    std::size_t received = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (received != completed.size() && std::chrono::steady_clock::now() < deadline) {
        const auto count = executor_.Poll(std::span(completed).subspan(received));
        ASSERT_TRUE(count);
        received += *count;
        std::this_thread::yield();
    }
    ASSERT_EQ(received, completed.size());
    auto blocked = cache_.NextOperation();
    ASSERT_TRUE(blocked);
    EXPECT_FALSE(*blocked);
    EXPECT_EQ(cache_.in_flight(), 2u);

    // 显式先收割第一个 context；紧凑在途表移动元素后，另一个 context 仍须保持暂停。
    const auto first_index = completed[0].id == *first ? 0u : 1u;
    ASSERT_TRUE(cache_.HandleCompletion(completed[first_index]));
    auto next = cache_.NextOperation();
    ASSERT_TRUE(next);
    ASSERT_TRUE(*next);
    EXPECT_EQ((**next).context, &context_);
    ASSERT_TRUE(Start(**next));
    ASSERT_TRUE(cache_.HandleCompletion(completed[1 - first_index]));
    next = cache_.NextOperation();
    ASSERT_TRUE(next);
    ASSERT_TRUE(*next);
    EXPECT_EQ((**next).context, &second_context_);
    ASSERT_TRUE(Start(**next));
    ASSERT_TRUE(CompleteOne());
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(cache_.in_flight(), 0u);
}

TEST_F(ImportCacheTest, ForeignAndDuplicateCompletionsCannotRetireOtherTasks) {
    ASSERT_TRUE(Acquire());
    const auto task = StartNext();
    ASSERT_TRUE(task);
    EXPECT_TRUE(cache_.OwnsCompletion(*task));
    runtime::ControlTaskCompletion foreign;
    foreign.id = *task;
    ++foreign.id.sequence;
    EXPECT_FALSE(cache_.OwnsCompletion(foreign.id));
    ExpectError(cache_.HandleCompletion(foreign), ImportCacheErrorCode::kInvalidCompletion);
    ASSERT_TRUE(CompleteOne());
    EXPECT_FALSE(cache_.OwnsCompletion(*task));
    ExpectError(cache_.HandleCompletion(last_completion_), ImportCacheErrorCode::kInvalidCompletion);
    EXPECT_EQ(cache_.in_flight(), 0u);
}

TEST_F(ImportCacheTest, ExecutorCancellationStillCollectsButDoesNotPublish) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    const auto task = StartNext();
    ASSERT_TRUE(task);
    ASSERT_TRUE(executor_.Cancel(*task));
    ASSERT_TRUE(CompleteOne());
    EXPECT_EQ(provider_.imports.load(), 1u);
    ExpectError(cache_.Get(*lease), ImportCacheErrorCode::kInvalidated);
    EXPECT_EQ(provider_.unimports.load(), 0u);
}

TEST_F(ImportCacheTest, LeaseCapacityIsBoundedAndStaleReferenceCannotReleaseReuse) {
    for (unsigned i = 0; i < 1024; ++i) {
        ASSERT_TRUE(Acquire());
    }
    ExpectError(Acquire(), ImportCacheErrorCode::kResourceExhausted);
    const auto old = leases_[0];
    ASSERT_TRUE(cache_.Release(old));
    const auto replacement = Acquire();
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->slot, old.slot);
    EXPECT_NE(replacement->sequence, old.sequence);
    ExpectError(cache_.Release(old), ImportCacheErrorCode::kInvalidLease);
    EXPECT_EQ(cache_.leases(), 1024u);
}

TEST_F(ImportCacheTest, EntryCapacityRejectsWithoutCreatingExtraWaiter) {
    for (unsigned i = 0; i < 16; ++i) {
        ++key_.endpoint_generation;
        ASSERT_TRUE(Acquire());
    }
    ++key_.endpoint_generation;
    ExpectError(Acquire(), ImportCacheErrorCode::kResourceExhausted);
    EXPECT_EQ(cache_.entries(), 16u);
    EXPECT_EQ(cache_.leases(), 16u);
}

TEST_F(ImportCacheTest, CloseStopsAdmissionAndCannotResetTicketGeneration) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ExpectError(cache_.Close(), ImportCacheErrorCode::kInUse);
    ExpectError(Acquire(), ImportCacheErrorCode::kStopped);
    ExpectError(cache_.Retain(*lease), ImportCacheErrorCode::kStopped);
    ASSERT_TRUE(cache_.Release(*lease));
    ASSERT_TRUE(cache_.Close());
    ExpectError(cache_.Open(), ImportCacheErrorCode::kInUse);
}

} // namespace
} // namespace control
} // namespace kbsocket
