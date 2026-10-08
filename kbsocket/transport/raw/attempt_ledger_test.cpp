// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/attempt_ledger.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;

class AttemptLedgerTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));

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
    }

    void TearDown() override {
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
    static inline AttemptLedgerTest* active_;
    test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    urma_context_t ctx_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_jetty_t remote_a_{}, remote_b_{};
    JettyPool pool_;
    AttemptLedger ledger_;
};

TEST_F(AttemptLedgerTest, SharedLanePreservesConnectionOperationAndResourceIdentity) {
    auto a = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(a);
    auto record_a = ledger_.Lookup(*a);
    ASSERT_TRUE(record_a);
    auto b = ledger_.Prepare(Metadata(2), record_a->ticket.lane);
    ASSERT_TRUE(b);
    ASSERT_TRUE(ledger_.Commit(*a, 10));
    ASSERT_TRUE(ledger_.Commit(*b, 11));
    EXPECT_EQ(ledger_.posted(), 2u);
    // 连接关闭不能直接清除记录；即使 A 已逻辑关闭，B 的完成也只能退休 B。
    EXPECT_FALSE(ledger_.Cancel(*a));
    EXPECT_FALSE(ledger_.Close());
    EXPECT_FALSE(pool_.Close());
    auto completed_b = ledger_.Complete(*b);
    ASSERT_TRUE(completed_b);
    EXPECT_EQ(completed_b->metadata.connection_id, 2u);
    EXPECT_EQ(completed_b->metadata.target, &remote_b_);
    EXPECT_EQ(completed_b->metadata.buffer_lease, 20u);
    EXPECT_EQ(completed_b->metadata.grant_lease, 200u);
    EXPECT_TRUE(ledger_.Lookup(*a));
    auto completed_a = ledger_.Complete(*a);
    ASSERT_TRUE(completed_a);
    EXPECT_EQ(completed_a->metadata.operation_id, 42u);
    EXPECT_EQ(completed_a->metadata.operation_generation, 7u);
    EXPECT_EQ(completed_a->metadata.opcode, URMA_OPC_SEND);
    EXPECT_EQ(completed_a->submission_sequence, 10u);
    EXPECT_EQ(ledger_.posted(), 0u);
}

TEST_F(AttemptLedgerTest, PartialAcceptanceOnlyPostsPrefixAndReturnsRejectedResources) {
    auto a = ledger_.Prepare(Metadata(1));
    auto b = ledger_.Prepare(Metadata(1));
    auto c = ledger_.Prepare(Metadata(2));
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_TRUE(c);
    EXPECT_EQ(ledger_.posted(), 0u);
    EXPECT_FALSE(ledger_.Complete(*a));
    EXPECT_FALSE(ledger_.Close());
    // 模拟 provider 仅接受前两条；两条 WR 可属于同一个操作，不能以 OpId 唯一索引。
    ASSERT_TRUE(ledger_.Commit(*a, 1));
    ASSERT_TRUE(ledger_.Commit(*b, 2));
    auto rejected = ledger_.Cancel(*c);
    ASSERT_TRUE(rejected);
    EXPECT_EQ(rejected->metadata.buffer_lease, 20u);
    EXPECT_EQ(ledger_.size(), 2u);
    EXPECT_EQ(ledger_.posted(), 2u);
    EXPECT_FALSE(ledger_.Commit(*a, 1));
    EXPECT_FALSE(ledger_.Cancel(*a));
    auto retry = ledger_.Prepare(Metadata(2));
    ASSERT_TRUE(retry);
    EXPECT_EQ(ledger_.Prepare(Metadata(2)).error().code, AttemptLedgerErrorCode::kExhausted);
    ASSERT_TRUE(ledger_.Cancel(*retry));
    ASSERT_TRUE(ledger_.Complete(*a));
    ASSERT_TRUE(ledger_.Complete(*b));
}

TEST_F(AttemptLedgerTest, StaleIdsCannotRetireReusedSlotsOrReopenedLedger) {
    ASSERT_TRUE(ledger_.Close());
    ASSERT_TRUE(ledger_.Open(pool_, 1));
    auto old = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(old);
    ASSERT_TRUE(ledger_.Cancel(*old));
    auto fresh = ledger_.Prepare(Metadata(2));
    ASSERT_TRUE(fresh);
    EXPECT_NE(*old, *fresh);
    // 容量为 1 强制复用同一槽；重复、迟到和无效 id 都不能修改新记录。
    EXPECT_FALSE(ledger_.Cancel(*old));
    EXPECT_FALSE(ledger_.Complete(*old));
    EXPECT_FALSE(ledger_.Lookup(0));
    ASSERT_TRUE(ledger_.Commit(*fresh, 3));
    ASSERT_TRUE(ledger_.Complete(*fresh));
    EXPECT_FALSE(ledger_.Complete(*fresh));
    ASSERT_TRUE(ledger_.Close());
    ASSERT_TRUE(ledger_.Open(pool_, 2));
    auto reopened = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(reopened);
    EXPECT_FALSE(ledger_.Lookup(*fresh));
    ASSERT_TRUE(ledger_.Cancel(*reopened));
}

TEST_F(AttemptLedgerTest, FaultedLaneDrainsWithoutLosingOtherConnectionsRecords) {
    auto a = ledger_.Prepare(Metadata(1));
    auto b = ledger_.Prepare(Metadata(2));
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_TRUE(ledger_.Commit(*a, 1));
    auto record = ledger_.Lookup(*a);
    ASSERT_TRUE(record);
    ASSERT_TRUE(pool_.MarkFaulted(record->ticket.lane));
    // SQ 故障不能自动释放 buffer；未提交后缀可取消，已提交前缀仍须等待终结。
    auto failed = ledger_.Prepare(Metadata(1), record->ticket.lane);
    ASSERT_FALSE(failed);
    ASSERT_TRUE(failed.error().pool_failure);
    EXPECT_EQ(failed.error().pool_failure->code, JettyPoolErrorCode::kFaulted);
    EXPECT_EQ(ledger_.size(), 2u);
    ASSERT_TRUE(ledger_.Cancel(*b));
    EXPECT_EQ(ledger_.posted(), 1u);
    ASSERT_TRUE(ledger_.Complete(*a));
}

TEST_F(AttemptLedgerTest, ValidationAndPoolExhaustionDoNotConsumeLedgerCapacity) {
    EXPECT_FALSE(ledger_.Open(pool_, 1));
    auto invalid = Metadata(1);
    invalid.signaled = false;
    EXPECT_FALSE(ledger_.Prepare(invalid));
    invalid = Metadata(1);
    invalid.target = nullptr;
    EXPECT_FALSE(ledger_.Prepare(invalid));
    EXPECT_EQ(ledger_.size(), 0u);
    // 账本容量大于 SQ：provider 额度失败后，不留下悬空账本记录。
    ASSERT_TRUE(ledger_.Close());
    ASSERT_TRUE(ledger_.Open(pool_, 4));
    auto a = ledger_.Prepare(Metadata(1));
    auto b = ledger_.Prepare(Metadata(1));
    auto c = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    ASSERT_TRUE(c);
    auto failed = ledger_.Prepare(Metadata(2));
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, AttemptLedgerErrorCode::kPoolFailure);
    EXPECT_EQ(ledger_.size(), 3u);
    EXPECT_EQ(ledger_.posted(), 0u);
    ASSERT_TRUE(ledger_.Commit(*a, 1));
    ASSERT_TRUE(ledger_.Complete(*a));
    ASSERT_TRUE(ledger_.Cancel(*b));
    ASSERT_TRUE(ledger_.Cancel(*c));
    ASSERT_TRUE(ledger_.Close());
    EXPECT_FALSE(ledger_.Prepare(Metadata(1)));
    EXPECT_FALSE(ledger_.Lookup(1));
    EXPECT_FALSE(ledger_.Open(pool_, 0));
    JettyPool unopened;
    EXPECT_FALSE(ledger_.Open(unopened, 1));
}
} // namespace
} // namespace raw
} // namespace kbsocket
