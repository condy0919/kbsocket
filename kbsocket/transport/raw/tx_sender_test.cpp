// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/tx_sender.hpp"

#include <array>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;

class TxSenderTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));

    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));

    void SetUp() override {
        active_ = this;
        // 只 mock 控制路径硬件对象；票据与账本均使用真实实现，验证两者同步。
        EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
        EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&jetty_));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 1;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        JettyPoolConfig config;
        config.tx_depth = 3;
        config.rx_depth = 3;
        ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
        ASSERT_TRUE(ledger_.Open(pool_, 3));
        ASSERT_TRUE(sender_.Open(ledger_));
        EXPECT_CALL(*this, Post(_, _, _)).Times(0);
        sge_.addr = reinterpret_cast<std::uintptr_t>(payload_.data());
        sge_.len = payload_.size();
        sge_.tseg = &segment_;
    }

    void TearDown() override {
        EXPECT_TRUE(sender_.Close());
        EXPECT_EQ(ledger_.size(), 0u);
        EXPECT_TRUE(ledger_.Close());
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_TRUE(pool_.Close());
        active_ = nullptr;
    }

    static UrmaFunctions MockedFunctions() {
        UrmaFunctions f{};
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.post_jetty_send_wr = [](auto* q, auto* wr, auto** bad) { return active_->Post(q, wr, bad); };
        return f;
    }

    AttemptMetadata Metadata(std::uint64_t connection) {
        return {.connection_id = connection,
                .operation_id = 42,
                .operation_generation = 7,
                .target = connection == 1 ? &remote_a_ : &remote_b_,
                .opcode = URMA_OPC_SEND,
                .buffer_lease = connection * 10,
                .grant_lease = connection * 100};
    }

protected:
    static inline TxSenderTest* active_;
    test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    urma_context_t ctx_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_jetty_t remote_a_{}, remote_b_{};
    JettyPool pool_;
    AttemptLedger ledger_;
    TxSender sender_;
    urma_target_seg_t segment_{};
    std::array<char, 8> payload_{};
    urma_sge_t sge_{};

    TxSendRequest Request(std::uint64_t connection = 1) {
        return {Metadata(connection), std::span(&sge_, 1)};
    }

    void CheckUncertainBoundary(bool success, bool foreign) {
        std::array requests{Request(), Request(2)};
        std::array<AttemptId, 2> ids{};
        urma_jfs_wr_t alien{};
        // mock 明确不提交任何 WR，但故意提供非法返回，验证发送器不会猜测未提交并释放资源。
        EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
            *bad = success ? wr : (foreign ? &alien : nullptr);
            return success ? URMA_SUCCESS : URMA_EINVAL;
        });
        auto result = sender_.Send(requests, ids);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, TxSendErrorCode::kProviderContract);
        EXPECT_FALSE(result.error().accepted);
        EXPECT_NE(ids[0], 0u);
        EXPECT_NE(ids[1], 0u);
        EXPECT_EQ(ledger_.size(), 2u);
        EXPECT_EQ(ledger_.posted(), 0u);
        auto record = ledger_.Lookup(ids[0]);
        ASSERT_TRUE(record);
        EXPECT_FALSE(pool_.Get(record->ticket.lane));
        EXPECT_FALSE(pool_.Close());
        EXPECT_FALSE(sender_.Send(requests, ids));
        // 此处凭 mock 的“完全未提交”事实恢复；生产中须先获得同等确定的终结/未提交证据。
        ASSERT_TRUE(ledger_.Cancel(ids[0]));
        ASSERT_TRUE(ledger_.Cancel(ids[1]));
    }
};

TEST_F(TxSenderTest, BatchBuildsSignaledWrForDifferentTargetsAndCommitsBeforeReturning) {
    std::array requests{Request(), Request(2), Request()};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
        EXPECT_EQ(*bad, nullptr);
        EXPECT_EQ(ledger_.size(), 3u);
        EXPECT_EQ(ledger_.posted(), 0u);
        // provider 接受前全部记录已经存在；WR 自带目标和稳定身份，不按 socket 独占 SQ。
        for (std::size_t i = 0; i < requests.size(); ++i) {
            EXPECT_NE(wr, nullptr);
            if (!wr) {
                return URMA_EINVAL;
            }
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND);
            EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
            EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
            EXPECT_EQ(wr->tjetty, requests[i].metadata.target);
            EXPECT_EQ(wr->user_ctx, ids[i]);
            EXPECT_EQ(wr->send.src.sge, &sge_);
            EXPECT_EQ(wr->send.src.num_sge, 1u);
            wr = wr->next;
        }
        EXPECT_EQ(wr, nullptr);
        return URMA_SUCCESS;
    });
    EXPECT_EQ(sender_.Send(requests, ids), 3u);
    EXPECT_EQ(ledger_.posted(), 3u);
    // 提交器不拥有在途请求，关闭它不应影响账本和资源归属。
    ASSERT_TRUE(sender_.Close());
    EXPECT_EQ(ledger_.posted(), 3u);
    for (std::size_t i = 0; i < ids.size(); ++i) {
        auto record = ledger_.Complete(ids[i]);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->submission_sequence, i + 1);
        EXPECT_EQ(record->metadata.buffer_lease, requests[i].metadata.buffer_lease);
    }
}

TEST_F(TxSenderTest, SingleSubmissionsShareLaneWhileFirstRequestRemainsInFlight) {
    std::array first{Request()};
    std::array second{Request(2)};
    std::array<AttemptId, 1> first_id{}, second_id{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).Times(2).WillRepeatedly(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(first, first_id));
    auto first_record = ledger_.Lookup(first_id[0]);
    ASSERT_TRUE(first_record);
    // 两次调用的描述符可复用，但第一次的在途记录必须保留，第二次可指定相同 SQ。
    ASSERT_TRUE(sender_.Send(second, second_id, first_record->ticket.lane));
    EXPECT_NE(first_id[0], second_id[0]);
    EXPECT_EQ(ledger_.posted(), 2u);
    auto completed_second = ledger_.Complete(second_id[0]);
    ASSERT_TRUE(completed_second);
    EXPECT_EQ(completed_second->metadata.target, &remote_b_);
    EXPECT_EQ(completed_second->submission_sequence, 2u);
    EXPECT_TRUE(ledger_.Lookup(first_id[0]));
    ASSERT_TRUE(ledger_.Complete(first_id[0]));
}

TEST_F(TxSenderTest, AccountingFailureNeverCancelsAcceptedRequests) {
    std::array requests{Request(), Request(2)};
    std::array<AttemptId, 2> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        // 故意违反不可重入契约，提前确认第一条，制造 post 成功后 Commit 失败。
        // 发送器仍须登记第二条并保留两条资源，绝不能按普通失败回滚已接受 WR。
        EXPECT_TRUE(ledger_.Commit(wr->user_ctx, 1));
        return URMA_SUCCESS;
    });
    auto result = sender_.Send(requests, ids);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, TxSendErrorCode::kAccountingFailure);
    EXPECT_EQ(result.error().accepted, 2u);
    EXPECT_EQ(ledger_.posted(), 2u);
    EXPECT_FALSE(sender_.Send(requests, ids));
    ASSERT_TRUE(ledger_.Complete(ids[0]));
    ASSERT_TRUE(ledger_.Complete(ids[1]));
}

TEST_F(TxSenderTest, MaximumBatchUsesEntireFixedArrayAndRejectsOneMore) {
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(ledger_.Close());
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.Close());
    // SQ 提升到批量上限，TX CQ 另留一个 FLUSH_ERR_DONE 槽，避免被默认深度提前挡住。
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 1;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 512;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    JettyPoolConfig config;
    config.tx_depth = TxSender::kMaxBatch;
    config.rx_depth = 3;
    config.tx_cq_depth = TxSender::kMaxBatch + 1;
    EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&jetty_));
    ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
    ASSERT_TRUE(ledger_.Open(pool_, TxSender::kMaxBatch));
    ASSERT_TRUE(sender_.Open(ledger_));
    std::array<TxSendRequest, TxSender::kMaxBatch + 1> requests;
    requests.fill(Request());
    std::array<AttemptId, TxSender::kMaxBatch + 1> ids{};
    EXPECT_FALSE(sender_.Send(requests, ids));
    EXPECT_EQ(ledger_.size(), 0u);
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto**) {
        std::size_t count = 0;
        for (; wr; wr = wr->next) {
            ++count;
        }
        EXPECT_EQ(count, TxSender::kMaxBatch);
        return URMA_SUCCESS;
    });
    // 正好 256 条必须完整提交，最后一项的 next 必须为 nullptr。
    EXPECT_EQ(sender_.Send(std::span(requests).first(TxSender::kMaxBatch), ids), TxSender::kMaxBatch);
    for (std::size_t i = 0; i < TxSender::kMaxBatch; ++i) {
        ASSERT_TRUE(ledger_.Complete(ids[i]));
    }
}

TEST_F(TxSenderTest, EveryRejectedPositionPreservesOnlyAcceptedPrefixAndAllowsRetry) {
    std::array requests{Request(), Request(2), Request()};
    std::array<AttemptId, 3> ids{};
    std::uint64_t sequence = 0;
    for (std::size_t accepted = 0; accepted < requests.size(); ++accepted) {
        SCOPED_TRACE(accepted);
        EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
            for (std::size_t i = 0; i < accepted; ++i) {
                wr = wr->next;
            }
            *bad = wr;
            return URMA_EAGAIN;
        });
        auto result = sender_.Send(requests, ids);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, TxSendErrorCode::kPostFailed);
        EXPECT_EQ(result.error().accepted, accepted);
        EXPECT_EQ(result.error().provider_status, URMA_EAGAIN);
        EXPECT_EQ(ledger_.size(), accepted);
        EXPECT_EQ(ledger_.posted(), accepted);
        // 只回收已完成前缀；后缀额度已撤销，未获得 attempt 身份，不能伪造其完成。
        for (std::size_t i = 0; i < ids.size(); ++i) {
            if (i < accepted) {
                auto record = ledger_.Complete(ids[i]);
                ASSERT_TRUE(record);
                EXPECT_EQ(record->submission_sequence, ++sequence);
            } else {
                EXPECT_EQ(ids[i], 0u);
            }
        }
    }
    // 同一发送器在普通 post 失败后仍可提交，单条调用不依赖上一批的 next 指针。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->next, nullptr);
        return URMA_SUCCESS;
    });
    EXPECT_EQ(sender_.Send(std::span(requests).first(1), ids), 1u);
    auto record = ledger_.Complete(ids[0]);
    ASSERT_TRUE(record);
    EXPECT_EQ(record->submission_sequence, ++sequence);
}

TEST_F(TxSenderTest, PreflightExhaustionRollsBackWholeBatchWithoutPosting) {
    // 留下一条在途记录，使 SQ 只余两个额度；第三个预留失败必须撤销前两个。
    auto existing = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(existing);
    ASSERT_TRUE(ledger_.Commit(*existing, 0));
    std::array requests{Request(), Request(), Request()};
    std::array<AttemptId, 3> ids{};
    auto failed = sender_.Send(requests, ids);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().accepted, 0u);
    EXPECT_EQ(ledger_.size(), 1u);
    EXPECT_EQ(ledger_.posted(), 1u);
    for (auto id : ids) {
        EXPECT_EQ(id, 0u);
    }
    ASSERT_TRUE(ledger_.Complete(*existing));
    // 分开验证 SQ 容量失败：账本留有空间，失败原因应由池返回。
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(ledger_.Close());
    ASSERT_TRUE(ledger_.Open(pool_, 4));
    ASSERT_TRUE(sender_.Open(ledger_));
    auto held = pool_.Reserve();
    ASSERT_TRUE(held);
    failed = sender_.Send(requests, ids);
    ASSERT_FALSE(failed);
    ASSERT_TRUE(failed.error().ledger_error);
    EXPECT_EQ(failed.error().ledger_error->code, AttemptLedgerErrorCode::kPoolFailure);
    EXPECT_EQ(ledger_.size(), 0u);
    ASSERT_TRUE(pool_.Cancel(*held));
}

TEST_F(TxSenderTest, RejectsInvalidRequestsAndForeignLaneBeforePosting) {
    std::array requests{Request()};
    std::array<AttemptId, 1> ids{};
    EXPECT_FALSE(sender_.Send({}, ids));
    EXPECT_FALSE(sender_.Send(requests, {}));
    std::array<TxSendRequest, TxSender::kMaxBatch + 1> too_many;
    too_many.fill(Request());
    std::array<AttemptId, TxSender::kMaxBatch + 1> large_ids{};
    EXPECT_FALSE(sender_.Send(too_many, large_ids));
    requests[0].metadata.opcode = URMA_OPC_WRITE;
    EXPECT_FALSE(sender_.Send(requests, ids));
    requests[0] = Request();
    requests[0].metadata.signaled = false;
    EXPECT_FALSE(sender_.Send(requests, ids));
    requests[0] = Request();
    requests[0].metadata.target = nullptr;
    EXPECT_FALSE(sender_.Send(requests, ids));
    requests[0] = Request();
    requests[0].sges = {};
    EXPECT_FALSE(sender_.Send(requests, ids));
    requests[0] = Request();
    sge_.tseg = nullptr;
    EXPECT_FALSE(sender_.Send(requests, ids));
    sge_.tseg = &segment_;
    EXPECT_FALSE(sender_.Send(requests, ids, JettyLane{}));
    EXPECT_EQ(ledger_.size(), 0u);
    EXPECT_FALSE(sender_.Open(ledger_));
    ASSERT_TRUE(sender_.Close());
    EXPECT_FALSE(sender_.Send(requests, ids));
    AttemptLedger unopened;
    EXPECT_FALSE(sender_.Open(unopened));
}

TEST_F(TxSenderTest, MissingBadWrRetainsAllResourcesAndBlocksSending) {
    CheckUncertainBoundary(false, false);
}
TEST_F(TxSenderTest, ForeignBadWrRetainsAllResourcesAndBlocksSending) {
    CheckUncertainBoundary(false, true);
}
TEST_F(TxSenderTest, ContradictorySuccessRetainsAllResourcesAndBlocksSending) {
    CheckUncertainBoundary(true, false);
}
} // namespace
} // namespace raw
} // namespace kbsocket
