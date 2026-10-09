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
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));

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
        EXPECT_CALL(*this, Modify(_, _)).Times(0);
        EXPECT_CALL(*this, Flush(_, _, _)).Times(0);
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
        f.modify_jetty = [](auto* q, auto* attr) { return active_->Modify(q, attr); };
        f.flush_jetty = [](auto* q, int n, auto* cr) { return active_->Flush(q, n, cr); };
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

TEST_F(TxCompletionProcessorTest, ErrorTransitionsHardwareBeforeBoundaryAndSoftwareDrain) {
    std::array requests{Request(), Request(2), Request()};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(requests, ids));
    auto lane = pool_.FindLane(jetty_.jetty_id.id);
    ASSERT_TRUE(lane);
    std::array<TxCompletionEvent, 3> events{};
    // error continue 不自动停止硬件。软件先禁止新提交，才发 modify(ERROR)。
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce([&](auto*, auto* attr) {
        EXPECT_EQ(attr->mask, JETTY_STATE);
        EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
        EXPECT_FALSE(pool_.Get(*lane));
        EXPECT_FALSE(pool_.ReserveOn(*lane));
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&tx_, 3, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[0], URMA_CR_ACK_TIMEOUT_ERR);
        return 1;
    });
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kError);
    EXPECT_FALSE(processor_.Flush(*lane, events)); // 尚未取得硬件边界，绝不能调用 provider flush。
    ASSERT_TRUE(pool_.BeginDrain(*lane));          // 幂等：不得重复 modify，制造第二个边界。
    // provider 即使允许 ERROR jetty post 返回 SUCCESS，发送器也不能再调用 post。
    std::array retry{Request()};
    std::array<AttemptId, 1> retry_ids{};
    EXPECT_FALSE(sender_.Send(retry, retry_ids, *lane));

    EXPECT_CALL(*this, Poll(&tx_, 3, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[1], URMA_CR_WR_FLUSH_ERR);
        // 边界只能跟在 FLUSH_ERR 之后；故意填入剩余请求的 id，证明它无效。
        out[1] = Completion(ids[2], URMA_CR_WR_FLUSH_ERR_DONE);
        return 2;
    });
    batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    EXPECT_EQ(events[0].record->id, ids[1]);
    EXPECT_EQ(events[1].kind, TxCompletionKind::kFlushDone);
    EXPECT_EQ(events[1].completion.user_ctx, 0u);
    EXPECT_FALSE(events[1].record);
    EXPECT_EQ(ledger_.size(), 1u);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kFlushReady);
    EXPECT_FALSE(pool_.Close());
    // 剩余 WR 没有硬件 CQE，只能通过显式 flush 的 WR_UNHANDLED 逐条归还。
    EXPECT_CALL(*this, Flush(&jetty_, 3, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[2], URMA_CR_WR_UNHANDLED);
        return 1;
    });
    batch = processor_.Flush(*lane, events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    ASSERT_TRUE(events[0].record);
    EXPECT_EQ(events[0].record->id, ids[2]);
    EXPECT_EQ(events[0].completion.status, URMA_CR_WR_UNHANDLED);
    EXPECT_FALSE(pool_.Close()); // 最后一个非空批次不作为软件队列已空的证据。
    EXPECT_CALL(*this, Flush(&jetty_, 3, _)).WillOnce(Return(0));
    batch = processor_.Flush(*lane, events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->count, 0u);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kDrained);
    EXPECT_EQ(pool_.available(), 0u);
    EXPECT_FALSE(sender_.Send(retry, retry_ids, *lane));
    ASSERT_TRUE(processor_.Flush(*lane, events)); // 已排空后不再访问 provider。
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
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 0u);
    EXPECT_EQ(events[0].kind, TxCompletionKind::kSuspendDone);
    EXPECT_EQ(events[1].kind, TxCompletionKind::kRejected);
    EXPECT_EQ(ledger_.posted(), 1u);
    // 在此测试中另行提供真正的终结 CQE，不能靠异常或 suspend 通知释放资源。
    EXPECT_CALL(*this, Poll(&tx_, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id, URMA_CR_WR_FLUSH_ERR);
        out[1] = Completion(*id, URMA_CR_WR_FLUSH_ERR_DONE);
        return 2;
    });
    ASSERT_TRUE(processor_.Poll(events));
    auto lane = pool_.FindLane(jetty_.jetty_id.id);
    ASSERT_TRUE(lane);
    EXPECT_CALL(*this, Flush(&jetty_, 2, _)).WillOnce(Return(0));
    ASSERT_TRUE(processor_.Flush(*lane, events));
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

TEST_F(TxCompletionProcessorTest, DrainingOneLaneDoesNotStopOtherLaneOrRetireItsRecords) {
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
    auto first_lane = pool.FindLane(first.jetty_id.id);
    auto second_lane = pool.FindLane(second.jetty_id.id);
    ASSERT_TRUE(first_lane);
    ASSERT_TRUE(second_lane);
    auto failed_id = ledger.Prepare(Metadata(1), *first_lane);
    auto healthy_id = ledger.Prepare(Metadata(2), *second_lane);
    ASSERT_TRUE(failed_id);
    ASSERT_TRUE(healthy_id);
    ASSERT_TRUE(ledger.Commit(*failed_id, 1));
    ASSERT_TRUE(ledger.Commit(*healthy_id, 2));
    std::array<TxCompletionEvent, 2> events{};
    EXPECT_CALL(*this, Modify(&first, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool.BeginDrain(*first_lane));
    EXPECT_CALL(*this, Poll(&tx, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*failed_id, URMA_CR_WR_FLUSH_ERR_DONE);
        out[0].local_id = first.jetty_id.id;
        return 1;
    });
    ASSERT_TRUE(processor.Poll(events));
    // 一个 TX JFC 中有多个 SQ。软件 flush 即使命中有效 id，也不能退休另一条 SQ 的请求。
    EXPECT_CALL(*this, Flush(&first, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*healthy_id, URMA_CR_WR_UNHANDLED);
        out[0].local_id = second.jetty_id.id;
        out[1] = Completion(*failed_id, URMA_CR_WR_UNHANDLED);
        out[1].local_id = first.jetty_id.id;
        return 2;
    });
    auto batch = processor.Flush(*first_lane, events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 1u);
    ASSERT_TRUE(events[0].error);
    EXPECT_EQ(events[0].error->code, TxCompletionErrorCode::kWrongLane);
    ASSERT_TRUE(ledger.Lookup(*healthy_id));
    EXPECT_CALL(*this, Flush(&first, 2, _)).WillOnce(Return(0));
    ASSERT_TRUE(processor.Flush(*first_lane, events));
    EXPECT_EQ(pool.available(), 1u);

    // 健康 SQ 的发送不受影响；不能因为共享 JFC 中另一个 SQ 故障而封住全部发送器。
    TxSender sender;
    ASSERT_TRUE(sender.Open(ledger));
    std::array requests{Request(2)};
    std::array<AttemptId, 1> ids{};
    EXPECT_CALL(*this, Post(&second, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender.Send(requests, ids, *second_lane));
    EXPECT_CALL(*this, Poll(&tx, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*healthy_id);
        out[0].local_id = second.jetty_id.id;
        out[1] = Completion(ids[0]);
        out[1].local_id = second.jetty_id.id;
        return 2;
    });
    batch = processor.Poll(events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 2u);
    ASSERT_TRUE(sender.Close());
    ASSERT_TRUE(ledger.Close());
    EXPECT_CALL(*this, DeleteJetty(&second)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJetty(&first)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool.Close());
}

TEST_F(TxCompletionProcessorTest, ModifyFailureRetiresKnownWrButRequiresExplicitRetry) {
    std::array requests{Request()};
    std::array<AttemptId, 1> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(requests, ids));
    auto lane = pool_.FindLane(jetty_.jetty_id.id);
    ASSERT_TRUE(lane);
    std::array<TxCompletionEvent, 1> events{};
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[0], URMA_CR_LOC_ACCESS_ERR);
        return 1;
    });
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_EINVAL));
    auto batch = processor_.Poll(events);
    ASSERT_TRUE(batch);
    // 修改硬件失败不应吞掉已经取出的终结完成；记录和控制路径错误同时返回。
    EXPECT_EQ(batch->retired, 1u);
    EXPECT_EQ(events[0].kind, TxCompletionKind::kCompleted);
    ASSERT_TRUE(events[0].record);
    ASSERT_TRUE(events[0].error);
    ASSERT_TRUE(events[0].error->pool_error);
    EXPECT_EQ(events[0].error->pool_error->code, JettyPoolErrorCode::kModifyFailed);
    EXPECT_EQ(events[0].error->pool_error->provider_error, URMA_EINVAL);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kFaulted);
    EXPECT_FALSE(sender_.Send(requests, ids, *lane));
    EXPECT_FALSE(processor_.Flush(*lane, events));

    // 显式重试成功后仍须等待边界，账本为空也不能跳过。
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.BeginDrain(*lane));
    EXPECT_FALSE(pool_.Close());
    EXPECT_FALSE(processor_.Flush(*lane, events));
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(123, URMA_CR_WR_FLUSH_ERR_DONE);
        return 1;
    });
    ASSERT_TRUE(processor_.Poll(events));
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(-7));
    auto failed = processor_.Flush(*lane, events);
    ASSERT_FALSE(failed);
    ASSERT_TRUE(failed.error().pool_error);
    EXPECT_EQ(failed.error().pool_error->provider_error, -7);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kFlushReady);
    EXPECT_FALSE(pool_.Close());
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
    ASSERT_TRUE(processor_.Flush(*lane, events));
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kDrained);
}

TEST_F(TxCompletionProcessorTest, UnexpectedBoundaryAndEmptyFlushNeverClearResidualRecords) {
    // 只有预留，没有 post。边界或空 flush 都不能作为这条记录可取消的证据。
    auto id = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(id);
    auto lane = pool_.FindLane(jetty_.jetty_id.id);
    ASSERT_TRUE(lane);
    std::array<TxCompletionEvent, 1> events{};
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id, URMA_CR_WR_FLUSH_ERR_DONE);
        return 1;
    });
    ASSERT_TRUE(processor_.Poll(events));
    EXPECT_EQ(events[0].kind, TxCompletionKind::kRejected);
    EXPECT_EQ(events[0].completion.user_ctx, 0u);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kFaulted);
    EXPECT_FALSE(processor_.Flush(*lane, events));
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.BeginDrain(*lane));
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id, URMA_CR_WR_FLUSH_ERR_DONE);
        return 1;
    });
    ASSERT_TRUE(processor_.Poll(events));
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
    auto residual = processor_.Flush(*lane, events);
    ASSERT_FALSE(residual);
    ASSERT_TRUE(residual.error().pool_error);
    EXPECT_EQ(residual.error().pool_error->code, JettyPoolErrorCode::kInUse);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kFlushReady);
    EXPECT_EQ(ledger_.size(), 1u);
    EXPECT_FALSE(pool_.Close());
    // 边界之后再出现 FLUSH_ERR 或重复边界均拒绝，不能回退状态或释放剩余记录。
    std::array<TxCompletionEvent, 2> unexpected{};
    EXPECT_CALL(*this, Poll(&tx_, 2, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*id, URMA_CR_WR_FLUSH_ERR);
        out[1] = Completion(*id, URMA_CR_WR_FLUSH_ERR_DONE);
        return 2;
    });
    auto batch = processor_.Poll(unexpected);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->retired, 0u);
    EXPECT_EQ(unexpected[0].kind, TxCompletionKind::kRejected);
    EXPECT_EQ(unexpected[1].kind, TxCompletionKind::kRejected);
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kFlushReady);
    // 测试明确从未 post，才可以显式 Cancel；生产不得靠空 flush 推断同样结论。
    ASSERT_TRUE(ledger_.Cancel(*id));
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
    ASSERT_TRUE(processor_.Flush(*lane, events));
}

TEST_F(TxCompletionProcessorTest, SoftwareFlushValidatesEveryRecordAndContinuesAfterRejection) {
    std::array requests{Request(), Request(2)};
    std::array<AttemptId, 2> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(sender_.Send(requests, ids));
    auto lane = pool_.FindLane(jetty_.jetty_id.id);
    ASSERT_TRUE(lane);
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.BeginDrain(*lane));
    std::array<TxCompletionEvent, 6> events{};
    EXPECT_CALL(*this, Poll(&tx_, 6, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(999, URMA_CR_WR_FLUSH_ERR_DONE);
        return 1;
    });
    ASSERT_TRUE(processor_.Poll(events));
    EXPECT_FALSE(processor_.Flush(*lane, {}));
    // 非法数量不能让调用方读取越界，也不能归还任何额度。
    EXPECT_CALL(*this, Flush(&jetty_, 6, _)).WillOnce(Return(7));
    EXPECT_FALSE(processor_.Flush(*lane, events));
    EXPECT_EQ(ledger_.size(), 2u);
    EXPECT_CALL(*this, Flush(&jetty_, 6, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids[0]); // 软件 flush 只认 WR_UNHANDLED。
        out[1] = Completion(ids[0], URMA_CR_WR_UNHANDLED);
        out[1].flag.bs.s_r = 1;
        out[2] = Completion(999, URMA_CR_WR_UNHANDLED);
        out[3] = Completion(ids[0], URMA_CR_WR_UNHANDLED);
        out[4] = out[3]; // 重复完成不能再次归还额度。
        out[5] = Completion(ids[1], URMA_CR_WR_UNHANDLED);
        return 6;
    });
    auto batch = processor_.Flush(*lane, events);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->count, 6u);
    EXPECT_EQ(batch->retired, 2u);
    for (const auto i : {0, 1, 2, 4}) {
        EXPECT_EQ(events[i].kind, TxCompletionKind::kRejected);
        EXPECT_FALSE(events[i].record);
    }
    ASSERT_TRUE(events[3].record);
    ASSERT_TRUE(events[5].record);
    EXPECT_EQ(events[3].record->id, ids[0]);
    EXPECT_EQ(events[5].record->id, ids[1]);
    // 分批 flush 直到明确返回 0；调用方输出大于 64 时仍限制 provider 批次。
    std::array<TxCompletionEvent, TxCompletionProcessor::kMaxPollBatch + 1> large{};
    EXPECT_CALL(*this, Flush(&jetty_, TxCompletionProcessor::kMaxPollBatch, _)).WillOnce(Return(0));
    ASSERT_TRUE(processor_.Flush(*lane, large));
    EXPECT_EQ(pool_.lane_state(*lane), JettyLaneState::kDrained);
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
