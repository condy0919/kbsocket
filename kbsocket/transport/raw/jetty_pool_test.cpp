// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/jetty_pool.hpp"

#include <array>
#include <cerrno>
#include <limits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;

class MockQueues {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
};

class JettyPoolTest : public ::testing::Test {
public:
    void SetUp() override {
        active_ = &mock_;
        FLAGS_kbsocket_tx_depth = 128;
        FLAGS_kbsocket_rx_depth = 128;
        FLAGS_kbsocket_link_priority = 4;
        cap_.max_jetty = 32;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 1024;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 4;
        cap_.max_jfs_inline_len = 64;
    }

    void TearDown() override {
        active_ = nullptr;
    }

    UrmaFunctions MockedFunctions() {
        UrmaFunctions f{};
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        f.post_jetty_send_wr = [](auto* q, auto* wr, auto** bad) { return active_->Post(q, wr, bad); };
        return f;
    }

    void ExpectCreate(int fail = -1, unsigned tx_depth = 128, unsigned rx_depth = 128, unsigned tx_cq_depth = 256,
                      unsigned priority = 4) {
        ::testing::InSequence order;
        for (int i = 0; i < 2 && (fail < 0 || i <= fail); ++i) {
            EXPECT_CALL(mock_, CreateJfc(&ctx_, _))
                .WillOnce([this, i, fail, tx_cq_depth](auto*, auto* cfg) -> urma_jfc_t* {
                    EXPECT_EQ(cfg->depth, i == 0 ? tx_cq_depth : 256u);
                    EXPECT_EQ(cfg->jfce, nullptr);
                    EXPECT_EQ(cfg->flag.bs.lock_free, 1u);
                    if (fail == i) {
                        errno = ENOMEM;
                        return nullptr;
                    }
                    return i == 0 ? &send_ : &recv_;
                });
        }
        if (fail >= 0 && fail < 2) {
            return;
        }
        EXPECT_CALL(mock_, CreateJfr(&ctx_, _)).WillOnce([this, fail, rx_depth](auto*, auto* cfg) -> urma_jfr_t* {
            EXPECT_EQ(cfg->jfc, &recv_);
            EXPECT_EQ(cfg->trans_mode, URMA_TM_RM);
            EXPECT_EQ(cfg->depth, rx_depth);
            EXPECT_EQ(cfg->max_sge, 1u);
            EXPECT_EQ(cfg->min_rnr_timer, URMA_TYPICAL_MIN_RNR_TIMER);
            if (fail == 2) {
                errno = ENOMEM;
                return nullptr;
            }
            return &jfr_;
        });
        if (fail == 2) {
            return;
        }
        first_create_ = EXPECT_CALL(mock_, CreateJetty(&ctx_, _))
                            .WillOnce([this, fail, tx_depth, priority](auto*, auto* cfg) -> urma_jetty_t* {
                                EXPECT_EQ(cfg->flag.bs.share_jfr, 1u);
                                EXPECT_EQ(cfg->shared.jfr, &jfr_);
                                EXPECT_EQ(cfg->shared.jfc, &recv_);
                                EXPECT_EQ(cfg->jfs_cfg.jfc, &send_);
                                EXPECT_EQ(cfg->jfs_cfg.trans_mode, URMA_TM_RM);
                                EXPECT_EQ(cfg->jfs_cfg.depth, tx_depth);
                                EXPECT_EQ(cfg->jfs_cfg.priority, priority);
                                EXPECT_EQ(cfg->jfs_cfg.max_rsge, 1u);
                                EXPECT_EQ(cfg->jfs_cfg.rnr_retry, 6u);
                                EXPECT_EQ(cfg->jfs_cfg.err_timeout, 2u);
                                if (fail == 3) {
                                    errno = ENOMEM;
                                    return nullptr;
                                }
                                return &jetty_;
                            });
    }

    void ExpectDelete(int count = 4) {
        ::testing::InSequence order;
        if (count >= 4) {
            EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        }
        if (count >= 3) {
            EXPECT_CALL(mock_, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        }
        if (count >= 2) {
            EXPECT_CALL(mock_, DeleteJfc(&recv_)).WillOnce(Return(URMA_SUCCESS));
        }
        if (count >= 1) {
            EXPECT_CALL(mock_, DeleteJfc(&send_)).WillOnce(Return(URMA_SUCCESS));
        }
    }

protected:
    gflags::FlagSaver flag_saver_;
    ::testing::Expectation first_create_;
    static inline MockQueues* active_;
    ::testing::StrictMock<MockQueues> mock_;
    urma_context_t ctx_{};
    urma_device_cap_t cap_{};
    urma_jfc_t send_{}, recv_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
};

TEST_F(JettyPoolTest, LinksQueuesPollsAndClosesInDependencyOrder) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate();
    JettyPool pool;
    ASSERT_TRUE(pool.Open(&ctx_, cap_));
    auto lease = pool.Reserve();
    ASSERT_TRUE(lease);
    EXPECT_EQ(pool.Get(lease->lane), &jetty_);
    ASSERT_TRUE(pool.Cancel(*lease));
    EXPECT_EQ(pool.jfr(), &jfr_);
    EXPECT_FALSE(pool.Open(&ctx_, cap_));
    // 分别路由发送和接收完成，轮询使用调用方缓冲；0 表示无完成而非错误。
    urma_cr_t cr[2]{};
    EXPECT_CALL(mock_, Poll(&send_, 2, cr)).WillOnce(Return(1));
    EXPECT_CALL(mock_, Poll(&recv_, 2, cr)).WillOnce(Return(0));
    EXPECT_EQ(pool.PollSend(cr), 1);
    EXPECT_EQ(pool.PollRecv(cr), 0);
    EXPECT_CALL(mock_, Poll(&send_, 2, cr)).WillOnce(Return(-1));
    auto failed = pool.PollSend(cr);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().provider_error, -1);
    EXPECT_FALSE(pool.PollRecv({}));
    ExpectDelete();
    ASSERT_TRUE(pool.Close());
    EXPECT_FALSE(pool.Reserve());
    EXPECT_FALSE(pool.PollSend(cr));
    EXPECT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, EveryCreationFailureRollsBackAndAllowsRetry) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    constexpr JettyPoolResource resources[] = {JettyPoolResource::kSendJfc, JettyPoolResource::kRecvJfc,
                                               JettyPoolResource::kJfr, JettyPoolResource::kJetty};
    for (int fail = 0; fail < 4; ++fail) {
        // 每个创建阶段均注入失败，只允许删除此前成功的资源，且不能发布半成品。
        SCOPED_TRACE(fail);
        ExpectCreate(fail);
        ExpectDelete(fail);
        JettyPool pool;
        auto result = pool.Open(&ctx_, cap_);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().failure.resource, resources[fail]);
        EXPECT_EQ(result.error().failure.provider_error, ENOMEM);
        EXPECT_FALSE(result.error().cleanup_error);
        EXPECT_FALSE(pool.ready());
        EXPECT_EQ(pool.jfr(), nullptr);
        ExpectCreate();
        ExpectDelete();
        ASSERT_TRUE(pool.Open(&ctx_, cap_));
        ASSERT_TRUE(pool.Close());
    }
}

TEST_F(JettyPoolTest, RollbackFailurePreservesOriginalErrorAndDependencies) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate(3);
    EXPECT_CALL(mock_, DeleteJfr(&jfr_)).WillOnce(Return(URMA_EAGAIN));
    JettyPool pool;
    auto result = pool.Open(&ctx_, cap_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().failure.resource, JettyPoolResource::kJetty);
    EXPECT_EQ(result.error().failure.provider_error, ENOMEM);
    ASSERT_TRUE(result.error().cleanup_error);
    EXPECT_EQ(result.error().cleanup_error->resource, JettyPoolResource::kJfr);
    EXPECT_FALSE(pool.Open(&ctx_, cap_));
    // JFR 删除失败时绝不能删除两个 JFC；保留资源后显式重试。
    ExpectDelete(3);
    EXPECT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, CloseFailureDoesNotRepeatSuccessfulDeletion) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate();
    JettyPool pool;
    ASSERT_TRUE(pool.Open(&ctx_, cap_));
    {
        ::testing::InSequence order;
        EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, DeleteJfr(&jfr_)).WillOnce(Return(URMA_EAGAIN));
    }
    EXPECT_FALSE(pool.Close());
    EXPECT_FALSE(pool.ready());
    // 重试从 JFR 继续，不能重复删除已经释放的 jetty。
    ExpectDelete(3);
    EXPECT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, EveryDeletionFailureStopsBeforeDependenciesAndCanRetry) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    for (int remaining = 4; remaining > 0; --remaining) {
        SCOPED_TRACE(remaining);
        ExpectCreate();
        JettyPool pool;
        ASSERT_TRUE(pool.Open(&ctx_, cap_));
        {
            // 任一删除失败都必须中止后续释放，StrictMock 会捕获越过失败节点的调用。
            ::testing::InSequence order;
            EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(remaining == 4 ? URMA_EAGAIN : URMA_SUCCESS));
            if (remaining < 4) {
                EXPECT_CALL(mock_, DeleteJfr(&jfr_)).WillOnce(Return(remaining == 3 ? URMA_EAGAIN : URMA_SUCCESS));
            }
            if (remaining < 3) {
                EXPECT_CALL(mock_, DeleteJfc(&recv_)).WillOnce(Return(remaining == 2 ? URMA_EAGAIN : URMA_SUCCESS));
            }
            if (remaining < 2) {
                EXPECT_CALL(mock_, DeleteJfc(&send_)).WillOnce(Return(URMA_EAGAIN));
            }
        }
        EXPECT_FALSE(pool.Close());
        EXPECT_FALSE(pool.ready());
        ExpectDelete(remaining);
        ASSERT_TRUE(pool.Close());
    }
}

TEST_F(JettyPoolTest, SharedSqAcceptsDifferentTargetsBeforeEitherCompletes) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate(-1, 2);
    ExpectDelete();
    JettyPool pool;
    JettyPoolConfig config;
    config.tx_depth = 2;
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    urma_target_jetty_t remote_a{}, remote_b{};
    urma_jfs_wr_t wr_a{}, wr_b{};
    wr_a.tjetty = &remote_a;
    wr_b.tjetty = &remote_b;
    auto a = pool.Reserve();
    ASSERT_TRUE(a);
    auto local = pool.Get(a->lane);
    ASSERT_TRUE(local);
    urma_jfs_wr_t* bad = nullptr;
    EXPECT_CALL(mock_, Post(&jetty_, &wr_a, &bad)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_EQ(UrmaApi::PostJettySendWr(*local, &wr_a, &bad), URMA_SUCCESS);
    ASSERT_TRUE(pool.Commit(*a));

    // A 尚未完成，B 已能向同一 SQ 提交不同目标；不能退回按连接独占的模型。
    auto b = pool.Reserve();
    ASSERT_TRUE(b);
    EXPECT_EQ(b->lane.index, a->lane.index);
    EXPECT_EQ(b->lane.epoch, a->lane.epoch);
    EXPECT_EQ(pool.Get(b->lane), *local);
    EXPECT_CALL(mock_, Post(&jetty_, &wr_b, &bad)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_EQ(UrmaApi::PostJettySendWr(*local, &wr_b, &bad), URMA_SUCCESS);
    ASSERT_TRUE(pool.Commit(*b));
    EXPECT_EQ(pool.available(), 0u);
    EXPECT_EQ(pool.Reserve().error().code, JettyPoolErrorCode::kExhausted);
    EXPECT_FALSE(pool.Cancel(*a));
    EXPECT_FALSE(pool.Commit(*a));
    EXPECT_FALSE(pool.Close());
    EXPECT_TRUE(pool.ready());
    urma_cr_t cr{};
    EXPECT_CALL(mock_, Poll(&send_, 1, &cr)).WillOnce(Return(0));
    EXPECT_EQ(pool.PollSend(std::span(&cr, 1)), 0);

    // 独立退休票据，不要求按连接排空；旧票据不能影响复用同一记账槽的新 WR。
    ASSERT_TRUE(pool.Complete(*b));
    auto next = pool.Reserve();
    ASSERT_TRUE(next);
    EXPECT_EQ(next->index, b->index);
    EXPECT_NE(next->sequence, b->sequence);
    EXPECT_FALSE(pool.Complete(*b));
    EXPECT_FALSE(pool.Complete(*next));
    ASSERT_TRUE(pool.Cancel(*next));
    ASSERT_TRUE(pool.Complete(*a));
    ASSERT_TRUE(pool.Close());

    // 物理队列重建才更新 epoch；旧队列身份和旧票据都不能作用于新池。
    ExpectCreate();
    ExpectDelete();
    ASSERT_TRUE(pool.Open(&ctx_, cap_));
    EXPECT_FALSE(pool.ReserveOn(a->lane));
    EXPECT_FALSE(pool.Get(a->lane));
    EXPECT_FALSE(pool.Complete(*a));
    ASSERT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, PartialPostCommitsPrefixAndCancelsSuffix) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate(-1, 3);
    ExpectDelete();
    JettyPool pool;
    JettyPoolConfig config;
    config.tx_depth = 3;
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    auto first = pool.Reserve();
    ASSERT_TRUE(first);
    auto second = pool.ReserveOn(first->lane);
    auto third = pool.ReserveOn(first->lane);
    ASSERT_TRUE(second);
    ASSERT_TRUE(third);
    EXPECT_FALSE(pool.Close());
    EXPECT_FALSE(pool.Complete(*first));
    urma_jfs_wr_t wr[3]{};
    wr[0].next = &wr[1];
    wr[1].next = &wr[2];
    urma_jfs_wr_t* bad = nullptr;
    // 模拟仅接受前两个 WR：取消后缀不会释放仍可能被 DMA 访问的前缀额度。
    EXPECT_CALL(mock_, Post(&jetty_, &wr[0], &bad)).WillOnce([&](auto*, auto*, auto** rejected) {
        *rejected = &wr[2];
        return URMA_EAGAIN;
    });
    auto local = pool.Get(first->lane);
    ASSERT_TRUE(local);
    EXPECT_EQ(UrmaApi::PostJettySendWr(*local, wr, &bad), URMA_EAGAIN);
    ASSERT_EQ(bad, &wr[2]);
    ASSERT_TRUE(pool.Commit(*first));
    ASSERT_TRUE(pool.Commit(*second));
    ASSERT_TRUE(pool.Cancel(*third));
    EXPECT_FALSE(pool.Cancel(*third));
    EXPECT_FALSE(pool.Cancel(*first));
    auto retry = pool.ReserveOn(first->lane);
    ASSERT_TRUE(retry);
    EXPECT_FALSE(pool.Reserve());
    ASSERT_TRUE(pool.Cancel(*retry));
    ASSERT_TRUE(pool.Complete(*first));
    ASSERT_TRUE(pool.Complete(*second));
    ASSERT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, RoundRobinSkipsFullAndFaultedLanesButAllowsDrain) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    urma_jetty_t second{};
    ExpectCreate(-1, 2, 128, 258);
    EXPECT_CALL(mock_, CreateJetty(&ctx_, _)).After(first_create_).WillOnce(Return(&second));
    JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_depth = 2;
    config.tx_cq_depth = 258;
    JettyPool pool;
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    auto a = pool.Reserve();
    auto b = pool.Reserve();
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_EQ(pool.Get(a->lane), &jetty_);
    EXPECT_EQ(pool.Get(b->lane), &second);
    auto c = pool.ReserveOn(a->lane);
    ASSERT_TRUE(c);
    // 第一个 SQ 满了，轮转选择必须跳过它，继续利用第二个 SQ。
    auto d = pool.Reserve();
    ASSERT_TRUE(d);
    EXPECT_EQ(d->lane.index, b->lane.index);
    EXPECT_FALSE(pool.Reserve());
    JettyPool foreign;
    EXPECT_FALSE(foreign.Commit(*a));
    auto invalid = *a;
    invalid.index = 999;
    EXPECT_FALSE(pool.Commit(invalid));
    ASSERT_TRUE(pool.Commit(*a));
    ASSERT_TRUE(pool.MarkFaulted(a->lane));
    EXPECT_FALSE(pool.Get(a->lane));
    EXPECT_FALSE(pool.ReserveOn(a->lane));
    // 故障影响同一 SQ 的所有连接，但仍允许确认已接受 WR、退休和取消未提交工作。
    ASSERT_TRUE(pool.Commit(*c));
    ASSERT_TRUE(pool.Complete(*a));
    ASSERT_TRUE(pool.Complete(*c));
    EXPECT_FALSE(pool.Reserve());
    ASSERT_TRUE(pool.Cancel(*b));
    ASSERT_TRUE(pool.Cancel(*d));
    EXPECT_EQ(pool.available(), 1u);
    auto healthy = pool.Reserve();
    ASSERT_TRUE(healthy);
    EXPECT_EQ(healthy->lane.index, b->lane.index);
    ASSERT_TRUE(pool.Cancel(*healthy));
    {
        ::testing::InSequence order;
        EXPECT_CALL(mock_, DeleteJetty(&second)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, DeleteJfc(&recv_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, DeleteJfc(&send_)).WillOnce(Return(URMA_SUCCESS));
    }
    ASSERT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, SecondJettyFailureRollsBackPoolBeforeSharedResources) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate(-1, 128, 128, 258);
    EXPECT_CALL(mock_, CreateJetty(&ctx_, _)).After(first_create_).WillOnce([](auto*, auto*) -> urma_jetty_t* {
        errno = ENOMEM;
        return nullptr;
    });
    ExpectDelete();
    JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_cq_depth = 258;
    JettyPool pool;
    auto result = pool.Open(&ctx_, cap_, config);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().failure.jetty_index, 1u);
    EXPECT_EQ(result.error().failure.provider_error, ENOMEM);
    EXPECT_FALSE(pool.Reserve());
    EXPECT_FALSE(result.error().cleanup_error);
}

TEST_F(JettyPoolTest, RollbackFailureRetainsJettyAndSharedParents) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate(-1, 128, 128, 258);
    EXPECT_CALL(mock_, CreateJetty(&ctx_, _)).After(first_create_).WillOnce([](auto*, auto*) -> urma_jetty_t* {
        errno = ENOMEM;
        return nullptr;
    });
    // 统一回滚只尝试一次删除；失败时保留 jetty 和共享父资源供显式重试。
    EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(URMA_EAGAIN));
    JettyPool pool;
    JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_cq_depth = 258;
    auto result = pool.Open(&ctx_, cap_, config);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().failure.jetty_index, 1u);
    EXPECT_EQ(result.error().failure.provider_error, ENOMEM);
    ASSERT_TRUE(result.error().cleanup_error);
    EXPECT_EQ(result.error().cleanup_error->jetty_index, 0u);
    EXPECT_EQ(result.error().cleanup_error->provider_error, URMA_EAGAIN);
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    ExpectDelete();
    ASSERT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, PoolDeletionFailureRetainsSharedResourcesUntilRetry) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    urma_jetty_t second{};
    ExpectCreate(-1, 128, 128, 258);
    EXPECT_CALL(mock_, CreateJetty(&ctx_, _)).After(first_create_).WillOnce(Return(&second));
    JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_cq_depth = 258;
    JettyPool pool;
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    {
        ::testing::InSequence order;
        EXPECT_CALL(mock_, DeleteJetty(&second)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(URMA_EAGAIN));
    }
    EXPECT_FALSE(pool.Close());
    EXPECT_FALSE(pool.Reserve());
    // 只重试未成功删除的 jetty，随后才能删除共享 JFR/JFC。
    ExpectDelete();
    ASSERT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, DestructorAttemptsCleanupOnlyOnceAndRetainsDependenciesOnFailure) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    ExpectCreate();
    // 单一所有者析构只尝试一次；jetty 删除失败后不删除共享父资源，也不隐式重试。
    EXPECT_CALL(mock_, DeleteJetty(&jetty_)).WillOnce(Return(URMA_EAGAIN));
    {
        JettyPool pool;
        ASSERT_TRUE(pool.Open(&ctx_, cap_));
    }
}

TEST_F(JettyPoolTest, ParsedDepthFlagsAreReadAtOpenAndSnapshotTheTxLimit) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    JettyPoolConfig config;
    // 配置对象先构造、宿主后解析参数，Open 仍须使用解析后的值。
    char program[] = "jetty_pool_test";
    char tx[] = "--kbsocket_tx_depth=64";
    char rx[] = "--kbsocket_rx_depth=96";
    char* args[] = {program, tx, rx, nullptr};
    char** argv = args;
    int argc = 3;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ExpectCreate(-1, 64, 96);
    ExpectDelete();
    JettyPool pool;
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    // 修改 flag 不能扩大已创建硬件队列的额度。
    FLAGS_kbsocket_tx_depth = 128;
    std::array<JettyTicket, 64> tickets;
    for (auto& ticket : tickets) {
        auto result = pool.Reserve();
        ASSERT_TRUE(result);
        ticket = *result;
    }
    EXPECT_FALSE(pool.Reserve());
    for (auto ticket : tickets) {
        ASSERT_TRUE(pool.Cancel(ticket));
    }
    ASSERT_TRUE(pool.Close());
}

TEST_F(JettyPoolTest, ExplicitDepthsOverrideFlagsAndInvalidDefaultsFailBeforeCreation) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    JettyPool pool;
    FLAGS_kbsocket_tx_depth = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_));
    FLAGS_kbsocket_tx_depth = 128;
    FLAGS_kbsocket_rx_depth = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_));
    FLAGS_kbsocket_rx_depth = 2048;
    EXPECT_FALSE(pool.Open(&ctx_, cap_));
    // 显式配置完全覆盖 flag；即使 flag 无效也不影响显式指定的合法值。
    JettyPoolConfig config;
    config.tx_depth = 32;
    config.rx_depth = 48;
    ExpectCreate(-1, 32, 48);
    ExpectDelete();
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    ASSERT_TRUE(pool.Close());
    config.tx_depth = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
}

TEST_F(JettyPoolTest, TxCqReservesFlushBoundaryWithoutIncreasingWrLimit) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    JettyPool pool;
    JettyPoolConfig config;
    config.jetty_count = 2;
    // 两个 jetty 各需要一个边界 CQE；旧容量 256 和仅多一个的 257 都不足。
    for (unsigned depth : {256u, 257u}) {
        config.tx_cq_depth = depth;
        auto result = pool.Open(&ctx_, cap_, config);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().failure.code, JettyPoolErrorCode::kInvalidArgument);
    }
    config.jetty_count = 1;
    config.tx_cq_depth = 129;
    ExpectCreate(-1, 128, 128, 129);
    ExpectDelete();
    ASSERT_TRUE(pool.Open(&ctx_, cap_, config));
    // 额外 CQE 容量不能成为用户可投递的第 129 个 WR。
    std::array<JettyTicket, 128> tickets;
    for (auto& ticket : tickets) {
        auto result = pool.Reserve();
        ASSERT_TRUE(result);
        ticket = *result;
        ASSERT_TRUE(pool.Commit(ticket));
    }
    EXPECT_FALSE(pool.Reserve());
    for (auto ticket : tickets) {
        ASSERT_TRUE(pool.Complete(ticket));
    }
    ASSERT_TRUE(pool.Close());
    // 先提升到 64 位再加 1，否则 UINT32_MAX 会绕回 0 并通过容量检查。
    config.tx_depth = std::numeric_limits<std::uint32_t>::max();
    config.tx_cq_depth = std::numeric_limits<std::uint32_t>::max();
    cap_.max_jfs_depth = cap_.max_jfc_depth = std::numeric_limits<std::uint32_t>::max();
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
}

TEST_F(JettyPoolTest, LinkPriorityFlagIsParsedAndValidatedBeforeCreatingResources) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    // 验证真实参数解析及两个合法边界；默认值 4 由其他创建测试覆盖。
    char program[] = "jetty_pool_test";
    char priority[] = "--kbsocket_link_priority=15";
    char* args[] = {program, priority, nullptr};
    char** argv = args;
    int argc = 2;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    EXPECT_EQ(FLAGS_kbsocket_link_priority, 15u);
    JettyPool pool;
    for (auto value : {15u, 0u}) {
        FLAGS_kbsocket_link_priority = value;
        ExpectCreate(-1, 128, 128, 256, value);
        ExpectDelete();
        ASSERT_TRUE(pool.Open(&ctx_, cap_));
        ASSERT_TRUE(pool.Close());
    }
    // 256 若先转 uint8_t 会变成 0；必须在任何 provider 调用前拒绝。
    for (auto value : {16u, 256u, std::numeric_limits<std::uint32_t>::max()}) {
        FLAGS_kbsocket_link_priority = value;
        auto result = pool.Open(&ctx_, cap_);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().failure.code, JettyPoolErrorCode::kInvalidArgument);
    }
}

TEST_F(JettyPoolTest, InvalidLimitsNeverCallProvider) {
    test_support::ScopedUrmaOverride scope(MockedFunctions());
    JettyPool pool;
    EXPECT_FALSE(pool.Open(nullptr, cap_));
    JettyPoolConfig config;
    config.jetty_count = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    config.jetty_count = 3;
    // 三条 SQ 的总预算超过 TX JFC 深度，必须在调用 provider 前拒绝。
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    config = {};
    config.tx_depth = cap_.max_jfs_depth + 1;
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    config = {};
    cap_.max_jfr_sge = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    cap_.max_jfr_sge = 4;
    cap_.max_jfs_rsge = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    cap_.max_jfs_rsge = 4;
    config = {};
    config.inline_bytes = cap_.max_jfs_inline_len + 1;
    EXPECT_FALSE(pool.Open(&ctx_, cap_, config));
    cap_.rm_tp_cap.bs.ctp = 0;
    EXPECT_FALSE(pool.Open(&ctx_, cap_));
}

} // namespace
} // namespace raw
} // namespace kbsocket
