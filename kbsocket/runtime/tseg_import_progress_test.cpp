// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/tseg_import_progress.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/runtime/context_progress_group.hpp"
#include "kbsocket/runtime/import_progress.hpp"
#include "kbsocket/runtime/shard_reactor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace runtime {
namespace {
struct Provider {
    std::atomic<unsigned> imports{0}, unimports{0};
    std::atomic<bool> block{false};
    std::array<urma_target_seg_t, 8> targets{};
    std::array<urma_target_jetty_t, 8> jetty_targets{};
    std::atomic<unsigned> jetty_imports{0}, jetty_unimports{0};
    std::thread::id owner = std::this_thread::get_id();
};
Provider* provider = nullptr;
raw::UrmaFunctions Functions() {
    raw::UrmaFunctions functions;
    functions.import_seg = +[](urma_context_t* context, urma_seg_t* descriptor, urma_token_t*, uint64_t addr,
                               urma_import_seg_flag_t flags) -> urma_target_seg_t* {
        EXPECT_EQ(addr, 0u);
        EXPECT_EQ(flags.bs.access, static_cast<unsigned>(URMA_ACCESS_WRITE));
        EXPECT_EQ(flags.bs.mapping, static_cast<unsigned>(URMA_SEG_NOMAP));
        EXPECT_NE(provider->owner, std::this_thread::get_id());
        const auto index = provider->imports.fetch_add(1);
        while (provider->block.load()) {
            provider->block.wait(true);
        }
        auto& target = provider->targets[index % provider->targets.size()];
        target.urma_ctx = context;
        target.seg = *descriptor;
        return &target;
    };
    functions.unimport_seg = +[](urma_target_seg_t*) {
        EXPECT_NE(provider->owner, std::this_thread::get_id());
        provider->unimports.fetch_add(1);
        return URMA_SUCCESS;
    };
    functions.import_jetty =
        +[](urma_context_t* context, urma_rjetty_t* descriptor, urma_token_t*) -> urma_target_jetty_t* {
        EXPECT_NE(provider->owner, std::this_thread::get_id());
        const auto index = provider->jetty_imports.fetch_add(1);
        auto& target = provider->jetty_targets[index % provider->jetty_targets.size()];
        target.urma_ctx = context;
        target.id = descriptor->jetty_id;
        return &target;
    };
    functions.unimport_jetty = +[](urma_target_jetty_t*) {
        EXPECT_NE(provider->owner, std::this_thread::get_id());
        provider->jetty_unimports.fetch_add(1);
        return URMA_SUCCESS;
    };
    return functions;
}

class TsegImportProgressTest : public ::testing::Test {
protected:
    void SetUp() override {
        provider = &provider_;
        override_ = std::make_unique<raw::test_support::ScopedUrmaOverride>(Functions());
        ASSERT_TRUE(executor_.Open({.workers = 2, .capacity = 2}));
        ASSERT_TRUE(cache_.Open({.entries = 8, .leases = 16, .in_flight = 2}));
        ASSERT_TRUE(source_.Open(cache_, &context_, 1));
        ASSERT_TRUE(endpoint_cache_.Open({.entries = 4, .leases = 8, .in_flight = 1}));
        ASSERT_TRUE(endpoint_source_.Open(endpoint_cache_, &context_, 1));
    }
    void TearDown() override {
        provider_.block.store(false);
        provider_.block.notify_all();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        if (reactor_open_) {
            reactor_.StopAccepting();
            while ((source_.in_flight() || endpoint_source_.in_flight()) &&
                   std::chrono::steady_clock::now() < deadline) {
                (void)reactor_.Tick();
                std::this_thread::yield();
            }
            EXPECT_TRUE(reactor_.Close());
        } else {
            while ((source_.in_flight() || endpoint_source_.in_flight()) &&
                   std::chrono::steady_clock::now() < deadline) {
                PollSource();
                std::this_thread::yield();
            }
        }
        EXPECT_TRUE(group_.Close());
        EXPECT_TRUE(endpoint_source_.Close());
        EXPECT_TRUE(source_.Close());
        for (auto lease : leases_) {
            (void)cache_.Release(lease);
        }
        for (const auto lease : endpoint_leases_) {
            (void)endpoint_cache_.Release(lease);
        }
        endpoint_cache_.StopAccepting();
        cache_.StopAccepting();
        // 组合源已解绑，兜底清理仍走控制线程，并保留未收割参数到完成。
        while ((cache_.entries() || endpoint_cache_.entries() || executor_.outstanding()) &&
               std::chrono::steady_clock::now() < deadline) {
            std::array<ControlTaskCompletion, 2> completions;
            auto count = executor_.Poll(completions);
            ASSERT_TRUE(count);
            for (std::size_t i = 0; i < *count; ++i) {
                if (cache_.OwnsCompletion(completions[i].id)) {
                    EXPECT_TRUE(cache_.HandleCompletion(completions[i]));
                } else if (endpoint_cache_.OwnsCompletion(completions[i].id)) {
                    EXPECT_TRUE(endpoint_cache_.HandleCompletion(completions[i]));
                }
            }
            // 两种缓存共享 context，清理时也不能并行调用 provider。
            if (cache_.in_flight() || endpoint_cache_.in_flight()) {
                std::this_thread::yield();
                continue;
            }
            auto operation = cache_.NextOperation();
            ASSERT_TRUE(operation);
            if (*operation) {
                const auto& op = **operation;
                EXPECT_TRUE(cache_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true}));
            } else {
                auto endpoint_operation = endpoint_cache_.NextOperation();
                ASSERT_TRUE(endpoint_operation);
                if (*endpoint_operation) {
                    const auto& op = **endpoint_operation;
                    EXPECT_TRUE(endpoint_cache_.StartOperation(op, executor_,
                                                               {op.context, op.context_incarnation, true, true}));
                }
            }
            std::this_thread::yield();
        }
        EXPECT_TRUE(cache_.Close());
        EXPECT_TRUE(endpoint_cache_.Close());
        EXPECT_TRUE(executor_.Close());
        override_.reset();
        provider = nullptr;
    }
    std::expected<control::TsegImportLease, control::TsegImportCacheError> Acquire(urma_context_t* context = nullptr,
                                                                                   std::uint64_t incarnation = 1) {
        urma_seg_t descriptor{};
        descriptor.ubva.va = 0x100000;
        descriptor.ubva.eid.raw[15] = 9;
        descriptor.len = 65536;
        descriptor.attr.bs.access = URMA_ACCESS_WRITE;
        descriptor.token_id = 42;
        control::TsegImportKey key{.local_context = context ? context : &context_,
                                   .local_context_incarnation = incarnation,
                                   .peer_runtime = 2,
                                   .receive_domain = 3,
                                   .slab_id = 4,
                                   .slab_generation = 5,
                                   .authorization_identity = 6};
        std::memcpy(key.eid.data(), &descriptor.ubva.eid, key.eid.size());
        auto lease = cache_.Acquire({key, descriptor, {}});
        if (lease) {
            leases_.push_back(*lease);
        }
        return lease;
    }
    bool PollSource() {
        std::array<ControlTaskCompletion, 2> completions;
        auto count = executor_.Poll(completions);
        if (!count) {
            return false;
        }
        for (std::size_t i = 0; i < *count; ++i) {
            if (cache_.OwnsCompletion(completions[i].id)) {
                source_.ControlCompleted(completions[i]);
            } else if (endpoint_cache_.OwnsCompletion(completions[i].id)) {
                endpoint_source_.ControlCompleted(completions[i]);
            }
        }
        return *count != 0;
    }
    template <typename Predicate>
    bool Until(Predicate predicate) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!predicate() && std::chrono::steady_clock::now() < deadline) {
            if (reactor_open_) {
                const auto tick = reactor_.Tick();
                EXPECT_TRUE(tick);
                if (!tick) {
                    return false;
                }
            } else {
                PollSource();
            }
            std::this_thread::yield();
        }
        return predicate();
    }

    Provider provider_;
    std::unique_ptr<raw::test_support::ScopedUrmaOverride> override_;
    ControlExecutor executor_;
    control::TsegImportCache cache_;
    TsegImportProgress source_;
    control::EndpointImportCache endpoint_cache_;
    ImportProgress endpoint_source_;
    ContextProgressGroup group_;
    std::unique_ptr<ProgressSource> borrowed_queue_;
    std::vector<control::ImportLease> endpoint_leases_;
    ShardReactor reactor_;
    bool reactor_open_ = false;
    urma_context_t context_{}, other_context_{};
    std::vector<control::TsegImportLease> leases_;
};

TEST_F(TsegImportProgressTest, ReactorDrivesImportAndWarmEntryEviction) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    std::array<ProgressSource*, 1> sources{&source_};
    ASSERT_TRUE(reactor_.Open(sources, executor_));
    reactor_open_ = true;
    ASSERT_TRUE(Until([&] { return cache_.Get(*lease).has_value(); }));
    EXPECT_EQ(provider_.imports.load(), 1u);
    EXPECT_FALSE(source_.in_flight());
    ASSERT_TRUE(cache_.Release(*lease));
    EXPECT_EQ(cache_.EvictUnused(), 1u);
    ASSERT_TRUE(Until([&] { return cache_.entries() == 0; }));
    EXPECT_EQ(provider_.unimports.load(), 1u);
    EXPECT_EQ(reactor_.stats().controls_submitted, 2u);
    EXPECT_EQ(reactor_.stats().controls_completed, 2u);
    EXPECT_FALSE(source_.failure());
}

TEST_F(TsegImportProgressTest, AdapterOnlySchedulesItsExactContextIncarnation) {
    auto other = Acquire(&other_context_, 2);
    auto mine = Acquire();
    ASSERT_TRUE(other && mine);
    auto task = source_.PendingControl();
    ASSERT_TRUE(task);
    auto id = executor_.TrySubmit(*task);
    ASSERT_TRUE(id);
    source_.ControlSubmitted(*id);
    ASSERT_TRUE(Until([&] { return !source_.in_flight(); }));
    ASSERT_TRUE(cache_.Get(*mine));
    auto pending = cache_.Get(*other);
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error().code, control::TsegImportCacheErrorCode::kPending);
    EXPECT_FALSE(source_.PendingControl());
    EXPECT_EQ(provider_.imports.load(), 1u);
}

TEST_F(TsegImportProgressTest, QueueFullPreservesOneStableTaskAndCloseDiscardsUnacceptedReservation) {
    ASSERT_TRUE(Acquire());
    // 已完成但尚未收割的执行器槽仍占容量；排队失败不能反复准备/覆盖导入参数。
    ControlTask filler{.function = +[](void*, std::uint64_t) noexcept -> ControlTaskResult { return {}; }};
    ASSERT_TRUE(executor_.TrySubmit(filler));
    ASSERT_TRUE(executor_.TrySubmit(filler));
    auto first = source_.PendingControl();
    ASSERT_TRUE(first);
    auto rejected = executor_.TrySubmit(*first);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, ControlExecutorErrorCode::kResourceExhausted);
    auto retry = source_.PendingControl();
    ASSERT_TRUE(retry);
    EXPECT_EQ(first->argument, retry->argument);
    EXPECT_EQ(first->token, retry->token);
    EXPECT_EQ(cache_.in_flight(), 1u);
    EXPECT_EQ(provider_.imports.load(), 0u);
    ASSERT_TRUE(source_.Close());
    EXPECT_EQ(cache_.in_flight(), 0u);
    auto next = cache_.NextOperation();
    ASSERT_TRUE(next);
    EXPECT_TRUE(*next);
}

TEST_F(TsegImportProgressTest, CancelBeforeSubmissionKeepsPreparedStorageUntilAdapterClose) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(source_.PendingControl());
    ASSERT_TRUE(cache_.Release(*lease));
    cache_.StopAccepting();
    EXPECT_EQ(cache_.entries(), 1u);
    ASSERT_TRUE(source_.Close());
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(provider_.imports.load(), 0u);
}

TEST_F(TsegImportProgressTest, AcceptedTaskCannotCloseOrDisappearOnCancellation) {
    auto lease = Acquire();
    ASSERT_TRUE(lease);
    provider_.block.store(true);
    auto task = source_.PendingControl();
    ASSERT_TRUE(task);
    auto id = executor_.TrySubmit(*task);
    ASSERT_TRUE(id);
    source_.ControlSubmitted(*id);
    auto close = source_.Close();
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error().code, ProgressErrorCode::kInUse);
    ASSERT_TRUE(cache_.Release(*lease));
    EXPECT_EQ(cache_.entries(), 1u);
    EXPECT_FALSE(source_.PendingControl());
    provider_.block.store(false);
    provider_.block.notify_all();
    ASSERT_TRUE(Until([&] { return !source_.in_flight(); }));
    // 取消没有抹掉真实导入结果；后续显式调度 unimport。
    EXPECT_EQ(cache_.entries(), 1u);
    auto cleanup = source_.PendingControl();
    ASSERT_TRUE(cleanup);
    auto cleanup_id = executor_.TrySubmit(*cleanup);
    ASSERT_TRUE(cleanup_id);
    source_.ControlSubmitted(*cleanup_id);
    ASSERT_TRUE(Until([&] { return !source_.in_flight(); }));
    EXPECT_EQ(cache_.entries(), 0u);
    EXPECT_EQ(provider_.unimports.load(), 1u);
}

TEST_F(TsegImportProgressTest, InvalidBindingAndOutOfOrderCallbacksFailExplicitly) {
    TsegImportProgress unopened;
    EXPECT_FALSE(unopened.Open(cache_, nullptr, 1));
    EXPECT_FALSE(unopened.Open(cache_, &context_, 0));
    EXPECT_FALSE(unopened.Progress(ProgressKind::kTimer, 1, 0));
    EXPECT_FALSE(source_.Open(cache_, &context_, 1));
    source_.ControlSubmitted({&executor_, 0, 1});
    EXPECT_TRUE(source_.failure());
    EXPECT_FALSE(source_.PendingControl());
    EXPECT_FALSE(source_.CollectNotifications(1));
    EXPECT_FALSE(source_.Rearm());
    EXPECT_TRUE(source_.Close());
}

// 代表同 context 的 post/poll/refill 使用者；计数用于验证整个暂停域，而非仅导入源本身。
class QueueUser final : public ProgressSource {
public:
    explicit QueueUser(urma_context_t* context) : context_(context) {}
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind, std::size_t, std::uint64_t) noexcept override {
        ++calls;
        return ProgressBatch{};
    }
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t) noexcept override {
        ++calls;
        return ProgressBatch{};
    }
    std::expected<void, ProgressError> Rearm() noexcept override {
        ++calls;
        return {};
    }
    std::span<const int> wait_fds() const noexcept override {
        return {};
    }
    const void* exclusive_context() const noexcept override {
        return context_;
    }
    std::size_t calls = 0;

private:
    urma_context_t* context_;
};

TEST_F(TsegImportProgressTest, SegmentAndEndpointImportsKeepQueuesRunningUntilCompletionIsCollected) {
    const auto segment = Acquire();
    ASSERT_TRUE(segment);
    urma_rjetty_t descriptor{};
    descriptor.jetty_id.id = 42;
    descriptor.jetty_id.eid.raw[15] = 9;
    descriptor.trans_mode = URMA_TM_RM;
    descriptor.tp_type = URMA_CTP;
    descriptor.type = URMA_JETTY;
    control::EndpointImportKey key{.local_context = &context_,
                                   .local_context_incarnation = 1,
                                   .peer_runtime = 2,
                                   .receive_domain = 3,
                                   .endpoint_generation = 4,
                                   .jetty_id = 42,
                                   .authorization_identity = 5};
    std::memcpy(key.eid.data(), &descriptor.jetty_id.eid, key.eid.size());
    auto endpoint = endpoint_cache_.Acquire({key, std::as_bytes(std::span(&descriptor, 1)), {}});
    ASSERT_TRUE(endpoint);
    endpoint_leases_.push_back(*endpoint);
    auto queues_owner = std::make_unique<QueueUser>(&context_);
    auto& queues = *queues_owner;
    borrowed_queue_ = std::move(queues_owner);
    std::array<ProgressSource*, 3> members{&source_, &endpoint_source_, &queues};
    ASSERT_TRUE(group_.Open(members));
    std::array<ProgressSource*, 1> groups{&group_};
    ASSERT_TRUE(reactor_.Open(groups, executor_));
    reactor_open_ = true;

    // 阻塞首个段导入；串行控制提交不应阻塞同 context 的队列使用者。
    provider_.block.store(true);
    ASSERT_TRUE(Until([&] { return provider_.imports.load() == 1; }));
    ASSERT_TRUE(source_.in_flight());
    const auto before = queues.calls;
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(reactor_.Tick());
    }
    EXPECT_GT(queues.calls, before);
    EXPECT_FALSE(reactor_.paused(0));
    EXPECT_EQ(provider_.jetty_imports.load(), 0u);
    EXPECT_FALSE(endpoint_source_.in_flight());
    EXPECT_EQ(reactor_.stats().controls_submitted, 1u);
    EXPECT_FALSE(group_.Close());
    EXPECT_FALSE(source_.Close());
    // 完成由 reactor 收割后才发布段目标并调度 endpoint 导入。
    provider_.block.store(false);
    provider_.block.notify_all();
    ASSERT_TRUE(Until([&] { return cache_.Get(*segment).has_value() && endpoint_cache_.Get(*endpoint).has_value(); }));
    EXPECT_GT(queues.calls, before);
    EXPECT_EQ(provider_.jetty_imports.load(), 1u);
    ASSERT_TRUE(cache_.Release(*segment));
    ASSERT_TRUE(endpoint_cache_.Release(*endpoint));
    EXPECT_EQ(cache_.EvictUnused(), 1u);
    EXPECT_EQ(endpoint_cache_.EvictUnused(), 1u);
    ASSERT_TRUE(Until([&] { return !cache_.entries() && !endpoint_cache_.entries(); }));
    EXPECT_EQ(provider_.unimports.load(), 1u);
    EXPECT_EQ(provider_.jetty_unimports.load(), 1u);
    EXPECT_EQ(reactor_.stats().controls_completed, 4u);
    // 完整解绑后才允许宿主销毁组合源借用的队列使用者。
    ASSERT_TRUE(reactor_.Close());
    reactor_open_ = false;
    EXPECT_TRUE(group_.Close());
}

} // namespace
} // namespace runtime
} // namespace kbsocket
