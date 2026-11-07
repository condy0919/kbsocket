// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/import_progress.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/runtime/shard_reactor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace runtime {
namespace {
struct Provider {
    std::atomic<unsigned> imports{0}, unimports{0};
    std::atomic<bool> block{false};
    std::array<urma_target_jetty_t, 8> targets{};
    std::thread::id owner = std::this_thread::get_id();
};
Provider* provider = nullptr;
raw::UrmaFunctions Functions() {
    raw::UrmaFunctions functions;
    functions.import_jetty =
        +[](urma_context_t* context, urma_rjetty_t* descriptor, urma_token_t*) -> urma_target_jetty_t* {
        EXPECT_NE(provider->owner, std::this_thread::get_id());
        const auto index = provider->imports.fetch_add(1);
        while (provider->block.load()) {
            provider->block.wait(true);
        }
        auto& target = provider->targets[index % provider->targets.size()];
        target.urma_ctx = context;
        target.id = descriptor->jetty_id;
        return &target;
    };
    functions.unimport_jetty = +[](urma_target_jetty_t*) {
        EXPECT_NE(provider->owner, std::this_thread::get_id());
        provider->unimports.fetch_add(1);
        return URMA_SUCCESS;
    };
    return functions;
}

class ImportProgressTest : public ::testing::Test {
protected:
    void SetUp() override {
        provider = &provider_;
        override_ = std::make_unique<raw::test_support::ScopedUrmaOverride>(Functions());
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 2}));
        ASSERT_TRUE(cache_.Open({.entries = 8, .leases = 16, .in_flight = 2}));
        ASSERT_TRUE(source_.Open(cache_, &context_, 1));
    }
    void TearDown() override {
        provider_.block.store(false);
        provider_.block.notify_all();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        if (reactor_open_) {
            reactor_.StopAccepting();
            while (source_.in_flight() && std::chrono::steady_clock::now() < deadline) {
                (void)reactor_.Tick();
                std::this_thread::yield();
            }
            EXPECT_TRUE(reactor_.Close());
        } else {
            while (source_.in_flight() && std::chrono::steady_clock::now() < deadline) {
                PollSource();
                std::this_thread::yield();
            }
        }
        EXPECT_TRUE(source_.Close());
        for (auto lease : leases_) {
            (void)cache_.Release(lease);
        }
        cache_.StopAccepting();
        // 组合源已解绑，兜底清理仍走控制线程，并保留未收割参数到完成。
        while ((cache_.entries() || executor_.outstanding()) && std::chrono::steady_clock::now() < deadline) {
            std::array<ControlTaskCompletion, 2> completions;
            auto count = executor_.Poll(completions);
            ASSERT_TRUE(count);
            for (std::size_t i = 0; i < *count; ++i) {
                if (cache_.OwnsCompletion(completions[i].id)) {
                    EXPECT_TRUE(cache_.HandleCompletion(completions[i]));
                }
            }
            auto operation = cache_.NextOperation();
            ASSERT_TRUE(operation);
            if (*operation) {
                const auto& op = **operation;
                EXPECT_TRUE(cache_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true}));
            }
            std::this_thread::yield();
        }
        EXPECT_TRUE(cache_.Close());
        EXPECT_TRUE(executor_.Close());
        override_.reset();
        provider = nullptr;
    }
    std::expected<control::ImportLease, control::ImportCacheError> Acquire(urma_context_t* context = nullptr,
                                                                           std::uint64_t incarnation = 1) {
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = 42;
        descriptor.jetty_id.eid.raw[15] = 9;
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        control::EndpointImportKey key{.local_context = context ? context : &context_,
                                       .local_context_incarnation = incarnation,
                                       .peer_runtime = 2,
                                       .receive_domain = 3,
                                       .endpoint_generation = 4,
                                       .jetty_id = 42,
                                       .authorization_identity = 5};
        std::memcpy(key.eid.data(), &descriptor.jetty_id.eid, key.eid.size());
        const auto bytes = std::as_bytes(std::span(&descriptor, 1));
        auto lease = cache_.Acquire({key, bytes, {}});
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
    control::EndpointImportCache cache_;
    ImportProgress source_;
    ShardReactor reactor_;
    bool reactor_open_ = false;
    urma_context_t context_{}, other_context_{};
    std::vector<control::ImportLease> leases_;
};

TEST_F(ImportProgressTest, ReactorDrivesImportAndWarmEntryEviction) {
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

TEST_F(ImportProgressTest, AdapterOnlySchedulesItsExactContextIncarnation) {
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
    EXPECT_EQ(pending.error().code, control::ImportCacheErrorCode::kPending);
    EXPECT_FALSE(source_.PendingControl());
    EXPECT_EQ(provider_.imports.load(), 1u);
}

TEST_F(ImportProgressTest, QueueFullPreservesOneStableTaskAndCloseDiscardsUnacceptedReservation) {
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

TEST_F(ImportProgressTest, CancelBeforeSubmissionKeepsPreparedStorageUntilAdapterClose) {
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

TEST_F(ImportProgressTest, AcceptedTaskCannotCloseOrDisappearOnCancellation) {
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

TEST_F(ImportProgressTest, InvalidBindingAndOutOfOrderCallbacksFailExplicitly) {
    ImportProgress unopened;
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

} // namespace
} // namespace runtime
} // namespace kbsocket
