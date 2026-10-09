// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/tx_completion_processor.hpp"

#include <array>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;

class TxCompletionProcessorTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));

    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));

    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));

    void SetUp() override {
        active_ = this;
        jetty_.jetty_id.id = 7;
        // mock 硬件创建、post 和 poll；队列池、提交器与账本使用真实实现，验证完整额度闭环。
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
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
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
    static inline TxCompletionProcessorTest* active_;
    test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    urma_context_t ctx_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_jetty_t remote_a_{}, remote_b_{};
    JettyPool pool_;
    AttemptLedger ledger_;
    TxSender sender_;
    TxCompletionProcessor processor_{ledger_};
    urma_target_seg_t segment_{};
    std::array<char, 8> payload_{};
    urma_sge_t sge_{};

    urma_cr_t Completion(AttemptId id, urma_cr_status_t status = URMA_CR_SUCCESS) {
        urma_cr_t cr{};
        cr.status = status;
        cr.user_ctx = id;
        cr.local_id = jetty_.jetty_id.id;
        cr.flag.bs.jetty = 1;
        return cr;
    }

    TxSendRequest Request(std::uint64_t connection = 1) {
        return {Metadata(connection), std::span(&sge_, 1)};
    }
};

TEST_F(TxCompletionProcessorTest, FullSqBecomesUsableAfterCompletionRetiresCredit) {
    std::array requests{Request(), Request(2), Request()};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(requests, ids));
    std::array retry{Request(2)};
    std::array<AttemptId, 1> retry_ids{};
    // 已填满三个 SQ 额度：新请求在预留阶段失败，不能影响已在途数据。
    auto full = sender_.Send(retry, retry_ids);
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().accepted, 0u);
    EXPECT_EQ(ledger_.posted(), 3u);
    std::array<TxCompletionEvent, 3> events{};
    EXPECT_CALL(*this, Poll(&tx_, 3, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[1]);
        return 1;
    });
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->count, 1u);
    EXPECT_EQ(batch->retired, 1u);
    ASSERT_TRUE(events[0].record);
    EXPECT_EQ(events[0].record->metadata.connection_id, 2u);
    EXPECT_EQ(events[0].record->metadata.buffer_lease, 20u);
    EXPECT_EQ(events[0].completion.status, URMA_CR_SUCCESS);
    EXPECT_EQ(ledger_.posted(), 2u);
    // 同一物理 SQ 的额度恢复可供任意连接继续使用，不局限于完成所属连接。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(retry, retry_ids));
    EXPECT_CALL(*this, Poll(&tx_, 3, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[0]);
        out[1] = Completion(ids[2]);
        out[2] = Completion(retry_ids[0]);
        return 3;
    });
    batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 3u);
    EXPECT_EQ(ledger_.size(), 0u);
}

TEST_F(TxCompletionProcessorTest, DuplicateAndUnknownIdsDoNotDropLaterValidCqes) {
    std::array requests{Request(), Request(2)};
    std::array<AttemptId, 2> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(requests, ids));
    std::array<TxCompletionEvent, 4> events{};
    EXPECT_CALL(*this, Poll(&tx_, 4, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[0]);
        out[1] = Completion(ids[0]);
        out[2] = Completion(0);
        out[3] = Completion(ids[1]);
        return 4;
    });
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->count, 4u);
    EXPECT_EQ(batch->retired, 2u);
    EXPECT_EQ(events[1].kind, TxCompletionKind::kRejected);
    ASSERT_TRUE(events[1].error);
    EXPECT_EQ(events[1].error->code, TxCompletionErrorCode::kLedgerFailure);
    EXPECT_FALSE(events[1].record);
    EXPECT_EQ(events[2].kind, TxCompletionKind::kRejected);
    ASSERT_TRUE(events[3].record);
    EXPECT_EQ(events[3].record->metadata.connection_id, 2u);
}

TEST_F(TxCompletionProcessorTest, ErrorAndFlushBoundaryNeverRetireOtherConnections) {
    std::array requests{Request(), Request(2)};
    std::array<AttemptId, 2> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(requests, ids));
    std::array<TxCompletionEvent, 2> events{};
    EXPECT_CALL(*this, Poll(&tx_, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[0], URMA_CR_ACK_TIMEOUT_ERR);
        // fake CQE 故意携带另一条有效 id，验证边界事件绝不解释 user_ctx。
        out[1] = Completion(ids[1], URMA_CR_WR_FLUSH_ERR_DONE);
        return 2;
    });
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    ASSERT_TRUE(events[0].record);
    EXPECT_EQ(events[0].completion.status, URMA_CR_ACK_TIMEOUT_ERR);
    EXPECT_EQ(events[1].kind, TxCompletionKind::kFlushDone);
    EXPECT_EQ(events[1].completion.user_ctx, 0u);
    EXPECT_FALSE(events[1].record);
    EXPECT_EQ(ledger_.posted(), 1u);
    EXPECT_FALSE(pool_.Reserve());
    EXPECT_TRUE(ledger_.Lookup(ids[1]));
    // 已隔离 SQ 仍能接收具体 WR 的 FLUSH_ERR 并逐条归还额度。
    EXPECT_CALL(*this, Poll(&tx_, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[1], URMA_CR_WR_FLUSH_ERR);
        return 1;
    });
    batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    EXPECT_EQ(pool_.available(), 0u);
}

TEST_F(TxCompletionProcessorTest, SuspendAndUnhandledDoNotImplyWrRetirement) {
    auto id = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(id);
    ASSERT_TRUE(ledger_.Commit(*id, 1));
    std::array<TxCompletionEvent, 2> events{};
    EXPECT_CALL(*this, Poll(&tx_, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id, URMA_CR_WR_SUSPEND_DONE);
        out[1] = Completion(*id, URMA_CR_WR_UNHANDLED);
        return 2;
    });
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 0u);
    EXPECT_EQ(events[0].kind, TxCompletionKind::kSuspendDone);
    EXPECT_EQ(events[1].kind, TxCompletionKind::kRejected);
    EXPECT_EQ(ledger_.posted(), 1u);
    // 在此测试中另行提供真正的终结 CQE，不能靠异常或 suspend 通知释放资源。
    EXPECT_CALL(*this, Poll(&tx_, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id, URMA_CR_WR_FLUSH_ERR);
        return 1;
    });
    ASSERT_TRUE(processor_.Poll(events));
}

TEST_F(TxCompletionProcessorTest, InvalidDirectionUnknownLaneAndPreparedRecordArePreserved) {
    auto id = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(id);
    std::array<TxCompletionEvent, 4> events{};
    EXPECT_CALL(*this, Poll(&tx_, 4, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id);
        out[0].flag.bs.s_r = 1;
        out[1] = Completion(*id);
        out[1].local_id = 999;
        out[2] = Completion(*id);
        out[2].flag.bs.jetty = 0;
        // user_ctx 正确也不能退休未提交记录。
        out[3] = Completion(*id);
        return 4;
    });
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 0u);
    ASSERT_TRUE(events[1].error);
    EXPECT_EQ(events[1].error->code, TxCompletionErrorCode::kUnknownLane);
    for (const auto& event : events) {
        EXPECT_EQ(event.kind, TxCompletionKind::kRejected);
        EXPECT_FALSE(event.record);
    }
    ASSERT_TRUE(ledger_.Cancel(*id));
}

TEST_F(TxCompletionProcessorTest, ValidIdFromDifferentLaneCannotRetireAttempt) {
    urma_jfc_t tx{}, rx{};
    urma_jfr_t jfr{};
    urma_jetty_t first{}, second{};
    first.jetty_id.id = 17;
    second.jetty_id.id = 18;
    EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx)).WillOnce(Return(&rx));
    EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr));
    EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&first)).WillOnce(Return(&second));
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 2;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_depth = config.rx_depth = 3;
    JettyPool pool;
    AttemptLedger ledger;
    ASSERT_TRUE(pool.Open(&ctx_, cap, config));
    ASSERT_TRUE(ledger.Open(pool, 6));
    TxCompletionProcessor processor(ledger);
    auto id = ledger.Prepare(Metadata(1));
    ASSERT_TRUE(id);
    ASSERT_TRUE(ledger.Commit(*id, 1));
    std::array<TxCompletionEvent, 2> events{};
    EXPECT_CALL(*this, Poll(&tx, 2, _)).WillOnce([&](auto*, int, auto* out) {
        // 即使 user_ctx 命中活跃记录，也必须验证物理通道归属。
        out[0] = Completion(*id);
        out[0].local_id = second.jetty_id.id;
        out[1] = Completion(*id);
        out[1].local_id = first.jetty_id.id;
        return 2;
    });
    auto batch = processor.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    ASSERT_TRUE(events[0].error);
    EXPECT_EQ(events[0].error->code, TxCompletionErrorCode::kWrongLane);
    EXPECT_FALSE(events[0].record);
    ASSERT_TRUE(events[1].record);
    EXPECT_EQ(events[1].record->id, *id);
    ASSERT_TRUE(ledger.Close());
    EXPECT_CALL(*this, DeleteJetty(&second)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJetty(&first)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool.Close());
}

TEST_F(TxCompletionProcessorTest, PollBudgetAndFailuresDoNotChangeLedger) {
    std::array<TxCompletionEvent, TxCompletionProcessor::kMaxPollBatch + 1> events{};
    EXPECT_FALSE(processor_.Poll({}));
    EXPECT_CALL(*this, Poll(&tx_, TxCompletionProcessor::kMaxPollBatch, _)).WillOnce(Return(0));
    auto empty = processor_.Poll(events);
    ASSERT_TRUE(empty);
    EXPECT_EQ(empty->count, 0u);
    EXPECT_EQ(empty->retired, 0u);
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce(Return(-1));
    auto failed = processor_.Poll(std::span(events).first(1));
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, TxCompletionErrorCode::kPollFailed);
    ASSERT_TRUE(failed.error().pool_error);
    EXPECT_EQ(failed.error().pool_error->provider_error, -1);
    ASSERT_TRUE(ledger_.Close());
    EXPECT_FALSE(processor_.Poll(events));
}
} // namespace
} // namespace raw
} // namespace kbsocket
