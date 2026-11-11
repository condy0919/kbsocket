// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/tx_sender.hpp"

#include <array>
#include <limits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"

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
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));

    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));

    void SetUp() override {
        active_ = this;
        // 只 mock 控制路径硬件对象；票据与账本均使用真实实现，验证两者同步。
        EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
        EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&passive_)).WillOnce(Return(&jetty_));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = 2;
        cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        cap.max_write_size = 256U * 1024 * 1024;
        JettyPoolConfig config;
        config.tx_depth = 3;
        config.rx_depth = 3;
        config.send_sge = 2;
        jetty_.jetty_id.id = 19;
        passive_.jetty_id.id = 23;
        ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
        ASSERT_TRUE(ledger_.Open(pool_, 3));
        ASSERT_TRUE(sender_.Open(ledger_));
        EXPECT_CALL(*this, Post(_, _, _)).Times(0);
        EXPECT_CALL(*this, Modify(_, _)).Times(0);
        EXPECT_CALL(*this, Flush(_, _, _)).Times(0);
        sge_.addr = reinterpret_cast<std::uintptr_t>(payload_.data());
        sge_.len = payload_.size();
        sge_.tseg = &segment_;
        // WRITE 的本地注册段与远端导入段均属于当前 context，远端地址不使用 mva。
        segment_.urma_ctx = remote_segment_.urma_ctx = &ctx_;
        segment_.seg.ubva.va = sge_.addr;
        segment_.seg.len = sge_.len;
        remote_segment_.seg.ubva.va = 0x100000000ULL;
        remote_segment_.seg.len = payload_.size();
        remote_segment_.mva = 0x200000000ULL;
        remote_a_.urma_ctx = remote_b_.urma_ctx = &ctx_;
        remote_sge_ = {.addr = remote_segment_.seg.ubva.va, .len = sge_.len, .tseg = &remote_segment_};
    }

    void TearDown() override {
        EXPECT_TRUE(sender_.Close());
        EXPECT_EQ(ledger_.size(), 0u);
        EXPECT_TRUE(ledger_.Close());
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJetty(&passive_)).WillOnce(Return(URMA_SUCCESS));
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
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
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
    urma_jetty_t jetty_{}, passive_{};
    urma_target_jetty_t remote_a_{}, remote_b_{};
    JettyPool pool_;
    AttemptLedger ledger_;
    TxSender sender_;
    urma_target_seg_t segment_{}, remote_segment_{};
    std::array<char, 8> payload_{};
    urma_sge_t sge_{}, remote_sge_{};

    TxSendRequest Request(std::uint64_t connection = 1) {
        return {Metadata(connection), std::span(&sge_, 1)};
    }

    TxSendRequest ImmediateRequest(std::uint64_t value, std::uint64_t connection = 1) {
        auto request = Request(connection);
        request.metadata.opcode = URMA_OPC_SEND_IMM;
        request.imm_data = value;
        return request;
    }

    TxSendRequest WriteRequest(std::uint64_t connection = 1) {
        auto request = Request(connection);
        request.metadata.opcode = URMA_OPC_WRITE;
        request.remote = remote_sge_;
        return request;
    }

    void CheckUncertainBoundary(bool success, bool foreign, bool write = false) {
        std::array requests{write ? WriteRequest() : Request(), write ? WriteRequest(2) : Request(2)};
        std::array<AttemptId, 2> ids{};
        urma_jfs_wr_t alien{};
        // mock 明确不提交任何 WR，但故意提供非法返回，验证发送器不会猜测未提交并释放资源。
        EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
            *bad = success ? wr : (foreign ? &alien : nullptr);
            return success ? URMA_SUCCESS : URMA_EINVAL;
        });
        EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
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
        // 即使账本已空，也必须先消费硬件边界，再确认软件队列为空。
        ASSERT_TRUE(pool_.ObserveFlushDone(record->ticket.lane));
        EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
        std::array<urma_cr_t, 1> cr{};
        ASSERT_TRUE(pool_.FlushSend(record->ticket.lane, cr));
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

TEST_F(TxSenderTest, ImmediateValuesPreserveAll64BitsAndLedgerOpcode) {
    // 同时检查全零、全一和仅最高位，防止截断为 32 位、字节序转换或把零值当作普通 SEND。
    constexpr std::array<std::uint64_t, 3> values{0, std::numeric_limits<std::uint64_t>::max(), std::uint64_t{1} << 63};
    std::array requests{ImmediateRequest(values[0]), ImmediateRequest(values[1], 2), ImmediateRequest(values[2])};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        for (std::size_t i = 0; i < requests.size(); ++i) {
            if (!wr) {
                ADD_FAILURE() << "missing immediate WR";
                return URMA_EINVAL;
            }
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
            EXPECT_EQ(wr->send.imm_data, values[i]);
            EXPECT_EQ(wr->send.src.sge, &sge_);
            EXPECT_EQ(wr->send.src.num_sge, 1u);
            EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
            EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
            EXPECT_EQ(wr->tjetty, requests[i].metadata.target);
            EXPECT_EQ(wr->user_ctx, ids[i]);
            wr = wr->next;
        }
        EXPECT_EQ(wr, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(requests, ids), requests.size());
    for (auto id : ids) {
        auto record = ledger_.Complete(id);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->metadata.opcode, URMA_OPC_SEND_IMM);
    }
}

TEST_F(TxSenderTest, MixedBatchIgnoresImmediateFieldForPlainSendAndClearsReusedDescriptor) {
    std::array requests{ImmediateRequest(0xFEDCBA9876543210ULL), Request(2), ImmediateRequest(0x8000000000000001ULL)};
    requests[1].imm_data = 0xFFFFFFFFFFFFFFFFULL;
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        for (const auto& request : requests) {
            if (!wr) {
                ADD_FAILURE() << "missing mixed WR";
                return URMA_EINVAL;
            }
            EXPECT_EQ(wr->opcode, request.metadata.opcode);
            EXPECT_EQ(wr->send.imm_data, request.metadata.opcode == URMA_OPC_SEND_IMM ? request.imm_data : 0);
            wr = wr->next;
        }
        EXPECT_EQ(wr, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(requests, ids), requests.size());
    for (std::size_t i = 0; i < ids.size(); ++i) {
        auto record = ledger_.Complete(ids[i]);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->metadata.opcode, requests[i].metadata.opcode);
    }
    // 下一批复用第一个描述符时，普通 SEND 不得携带上一批的 IMM 或 next 指针。
    std::array plain{Request()};
    plain[0].imm_data = 42;
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->opcode, URMA_OPC_SEND);
        EXPECT_EQ(wr->send.imm_data, 0u);
        EXPECT_EQ(wr->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(plain, ids), 1u);
    ASSERT_TRUE(ledger_.Complete(ids[0]));
}

TEST_F(TxSenderTest, MixedPartialPostRetriesOnlyUnacceptedSuffixWithOriginalImmediateData) {
    std::array requests{ImmediateRequest(0x123456789ABCDEF0ULL), Request(2),
                        ImmediateRequest(0x8000000000000000ULL, 2)};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
        EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
        EXPECT_EQ(wr->send.imm_data, requests[0].imm_data);
        *bad = wr->next;
        return URMA_EAGAIN;
    });
    auto first = sender_.Send(requests, ids);
    ASSERT_FALSE(first);
    EXPECT_EQ(first.error().code, TxSendErrorCode::kPostFailed);
    EXPECT_EQ(first.error().accepted, 1u);
    EXPECT_EQ(first.error().provider_status, URMA_EAGAIN);
    EXPECT_EQ(ledger_.posted(), 1u);
    ASSERT_NE(ids[0], 0u);
    EXPECT_EQ(ids[1], 0u);
    EXPECT_EQ(ids[2], 0u);
    const auto accepted_id = ids[0];
    // 前缀保持在途；后缀撤销后可立即重试，并保留原 opcode 和 IMM，不能重放前缀。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->opcode, URMA_OPC_SEND);
        EXPECT_EQ(wr->send.imm_data, 0u);
        EXPECT_NE(wr->user_ctx, accepted_id);
        if (!wr->next) {
            ADD_FAILURE() << "missing retried immediate WR";
            return URMA_EINVAL;
        }
        EXPECT_EQ(wr->next->opcode, URMA_OPC_SEND_IMM);
        EXPECT_EQ(wr->next->send.imm_data, requests[2].imm_data);
        EXPECT_NE(wr->next->user_ctx, accepted_id);
        EXPECT_EQ(wr->next->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(std::span(requests).subspan(1), std::span(ids).subspan(1)), 2u);
    EXPECT_EQ(ids[0], accepted_id);
    EXPECT_EQ(ledger_.posted(), 3u);
    for (std::size_t i = 0; i < ids.size(); ++i) {
        auto record = ledger_.Complete(ids[i]);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->submission_sequence, i + 1);
        EXPECT_EQ(record->metadata.opcode, requests[i].metadata.opcode);
        EXPECT_EQ(record->metadata.target, requests[i].metadata.target);
    }
}

TEST_F(TxSenderTest, UnsupportedOpcodeRejectsWholeMixedBatchBeforePreparing) {
    // 非法 opcode 放在合法 IMM 后面，确保整个批次先检查，不能先提交或预留合法前缀。
    std::array requests{ImmediateRequest(0xFFFFFFFFFFFFFFFFULL), Request()};
    requests[1].metadata.opcode = URMA_OPC_READ;
    std::array<AttemptId, 2> ids{100, 200};
    const auto result = sender_.Send(requests, ids);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, TxSendErrorCode::kInvalidArgument);
    EXPECT_EQ(result.error().accepted, 0u);
    EXPECT_EQ(ledger_.size(), 0u);
    EXPECT_EQ(ids[0], 0u);
    EXPECT_EQ(ids[1], 0u);
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
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    auto result = sender_.Send(requests, ids);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, TxSendErrorCode::kAccountingFailure);
    EXPECT_EQ(result.error().accepted, 2u);
    EXPECT_EQ(ledger_.posted(), 2u);
    EXPECT_FALSE(sender_.Send(requests, ids));
    auto record = ledger_.Lookup(ids[0]);
    ASSERT_TRUE(record);
    ASSERT_TRUE(ledger_.Complete(ids[0]));
    ASSERT_TRUE(ledger_.Complete(ids[1]));
    ASSERT_TRUE(pool_.ObserveFlushDone(record->ticket.lane));
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
    std::array<urma_cr_t, 1> cr{};
    ASSERT_TRUE(pool_.FlushSend(record->ticket.lane, cr));
}

TEST_F(TxSenderTest, ConsecutiveBatchesKeepAffinityAndDriftOnlyForCapacityOrFault) {
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(ledger_.Close());
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJetty(&passive_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.Close());
    // 两条 SQ 各能容纳三条 WR；故意留一个不足以接收整批的首选槽。
    urma_jetty_t second{};
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 3;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_depth = config.rx_depth = 3;
    EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(_, _))
        .WillOnce(Return(&passive_))
        .WillOnce(Return(&jetty_))
        .WillOnce(Return(&second));
    ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
    ASSERT_TRUE(ledger_.Open(pool_, 6));
    ASSERT_TRUE(sender_.Open(ledger_));
    const auto request = Request(2);
    std::array<AttemptId, 2> first{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).Times(2).WillRepeatedly(Return(URMA_SUCCESS));
    for (auto& id : first) {
        ASSERT_EQ(sender_.Send({&request, 1}, {&id, 1}), 1u);
    }
    // 前两批仍在途，第三批因首选 SQ 剩余一槽而完整漂移到另一 SQ。
    std::array batch{request, request};
    std::array<AttemptId, 2> drift{};
    EXPECT_CALL(*this, Post(&second, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_EQ(sender_.Send(batch, drift), 2u);
    for (const auto id : first) {
        const auto record = ledger_.Lookup(id);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->ticket.lane.index, 0u);
        ASSERT_TRUE(ledger_.Complete(id));
    }
    for (const auto id : drift) {
        const auto record = ledger_.Lookup(id);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->ticket.lane.index, 1u);
        ASSERT_TRUE(ledger_.Complete(id));
    }
    // 首选槽恢复后重新使用它；隔离后只能选择健康槽，显式指定故障槽仍拒绝。
    const auto preferred = pool_.lane(0);
    ASSERT_TRUE(preferred);
    const auto restored = pool_.SelectLane(2, 2);
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->index, preferred->index);
    ASSERT_TRUE(pool_.MarkFaulted(*preferred));
    AttemptId id = 0;
    EXPECT_FALSE(sender_.Send({&request, 1}, {&id, 1}, *preferred));
    EXPECT_CALL(*this, Post(&second, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_EQ(sender_.Send({&request, 1}, {&id, 1}), 1u);
    ASSERT_TRUE(ledger_.Complete(id));
    EXPECT_CALL(*this, DeleteJetty(&second)).WillOnce(Return(URMA_SUCCESS));
    // second 是局部对象，必须在作用域结束前关闭池；TearDown 的原对象删除预期在这里满足。
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(ledger_.Close());
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJetty(&passive_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.Close());
    // 恢复 fixture 的单 Lane 资源，供公共 TearDown 验证最终清理。
    config.jetty_count = 1;
    EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&passive_)).WillOnce(Return(&jetty_));
    ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
    ASSERT_TRUE(ledger_.Open(pool_, 3));
    ASSERT_TRUE(sender_.Open(ledger_));
}

TEST_F(TxSenderTest, MaximumBatchUsesEntireFixedArrayAndRejectsOneMore) {
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(ledger_.Close());
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJetty(&passive_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.Close());
    // 主动 SQ 提升到批量上限，TX CQ 为其边界和被动端点各留一槽。
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 2;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 512;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    JettyPoolConfig config;
    config.tx_depth = TxSender::kMaxBatch;
    config.rx_depth = 3;
    config.tx_cq_depth = TxSender::kMaxBatch + 2;
    EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&passive_)).WillOnce(Return(&jetty_));
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

TEST_F(TxSenderTest, WriteBuildsOneRemoteSgeAndRetainsLeasesUntilRealCompletion) {
    std::array requests{WriteRequest()};
    requests[0].imm_data = std::numeric_limits<std::uint64_t>::max();
    std::array<AttemptId, 1> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->opcode, URMA_OPC_WRITE);
        EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
        EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
        EXPECT_EQ(wr->rw.src.sge, &sge_);
        EXPECT_EQ(wr->rw.src.num_sge, 1u);
        EXPECT_EQ(wr->rw.dst.num_sge, 1u);
        EXPECT_EQ(wr->rw.dst.sge->addr, remote_sge_.addr);
        EXPECT_EQ(wr->rw.dst.sge->len, sge_.len);
        EXPECT_EQ(wr->rw.dst.sge->tseg, &remote_segment_);
        EXPECT_EQ(wr->rw.notify_data, 0u);
        EXPECT_EQ(wr->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(requests, ids), 1u);
    const auto record = ledger_.Lookup(ids[0]);
    ASSERT_TRUE(record);
    EXPECT_EQ(record->metadata.buffer_lease, requests[0].metadata.buffer_lease);
    EXPECT_EQ(record->metadata.grant_lease, requests[0].metadata.grant_lease);
    EXPECT_EQ(ledger_.posted(), 1u);
    EXPECT_FALSE(ledger_.Cancel(ids[0]));
    EXPECT_FALSE(ledger_.Close());

    // UDMA 普通 TX CQE 不填 RX opcode。通过真实完成层按 attempt 退休，保留 WRITE 和两类 lease 身份。
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce([&](auto*, int, auto* cr) {
        *cr = {};
        cr->flag.bs.jetty = 1;
        cr->local_id = jetty_.jetty_id.id;
        cr->user_ctx = ids[0];
        cr->status = URMA_CR_SUCCESS;
        return 1;
    });
    TxCompletionProcessor processor(ledger_);
    std::array<TxCompletionEvent, 1> events{};
    const auto completed = processor.Poll(events);
    ASSERT_TRUE(completed);
    EXPECT_EQ(completed->retired, 1u);
    ASSERT_TRUE(events[0].record);
    EXPECT_FALSE(events[0].error);
    EXPECT_EQ(events[0].record->metadata.opcode, URMA_OPC_WRITE);
    EXPECT_EQ(events[0].record->metadata.buffer_lease, requests[0].metadata.buffer_lease);
    EXPECT_EQ(events[0].record->metadata.grant_lease, requests[0].metadata.grant_lease);
    EXPECT_EQ(ledger_.size(), 0u);
}

TEST_F(TxSenderTest, MixedWriteAndSendBatchKeepsIndependentDescriptorsAndMetadata) {
    // 两个本地 SGE 拼接到单个远端区间；混批只检查提交语义，不把 SEND 当作 WRITE 可见性屏障。
    std::array local{sge_, sge_};
    local[0].len = 3;
    local[1].addr += 3;
    local[1].len -= 3;
    auto write = WriteRequest();
    write.sges = local;
    write.imm_data = 0xFFFFFFFFFFFFFFFFULL;
    std::array requests{write, ImmediateRequest(0xFEDCBA9876543210ULL, 2), Request()};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->rw.src.sge, local.data());
        EXPECT_EQ(wr->rw.src.num_sge, 2u);
        EXPECT_EQ(wr->rw.dst.num_sge, 1u);
        EXPECT_EQ(wr->rw.dst.sge->len, 8u);
        EXPECT_EQ(wr->rw.notify_data, 0u);
        EXPECT_EQ(wr->next->opcode, URMA_OPC_SEND_IMM);
        EXPECT_EQ(wr->next->send.imm_data, requests[1].imm_data);
        EXPECT_EQ(wr->next->next->opcode, URMA_OPC_SEND);
        EXPECT_EQ(wr->next->next->send.imm_data, 0u);
        EXPECT_EQ(wr->next->next->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(requests, ids), 3u);
    for (std::size_t i = 0; i < ids.size(); ++i) {
        const auto retired = ledger_.Complete(ids[i]);
        ASSERT_TRUE(retired);
        EXPECT_EQ(retired->metadata.opcode, requests[i].metadata.opcode);
    }
    // 复用 WRITE 的 union 描述符后，SEND IMM 及其无关字段必须重新初始化。
    std::array next{ImmediateRequest(7)};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
        EXPECT_EQ(wr->send.imm_data, 7u);
        EXPECT_EQ(wr->send.tseg, nullptr);
        EXPECT_EQ(wr->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(next, ids), 1u);
    ASSERT_TRUE(ledger_.Complete(ids[0]));
}

TEST_F(TxSenderTest, TwoWritesKeepDistinctRemoteSegmentsAndOffsetsInOneBatch) {
    auto second_segment = remote_segment_;
    second_segment.seg.ubva.va += 0x10000;
    second_segment.seg.token_id = 99;
    auto second_source = sge_;
    second_source.len = 4;
    std::array requests{WriteRequest(), WriteRequest(2)};
    requests[1].sges = std::span(&second_source, 1);
    requests[1].remote = urma_sge_t{.addr = second_segment.seg.ubva.va + 4, .len = 4, .tseg = &second_segment};
    std::array<AttemptId, 2> ids{};

    // 同一批为不同目标写入不同长度和区间，必须保留逐 WR 的远端描述符，不能被后一条覆盖。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* first, auto**) {
        EXPECT_NE(first->next, nullptr);
        if (!first->next) {
            return URMA_EINVAL;
        }
        const auto* second = first->next;
        EXPECT_EQ(first->opcode, URMA_OPC_WRITE);
        EXPECT_EQ(second->opcode, URMA_OPC_WRITE);
        EXPECT_EQ(first->tjetty, &remote_a_);
        EXPECT_EQ(second->tjetty, &remote_b_);
        EXPECT_EQ(first->rw.dst.num_sge, 1u);
        EXPECT_EQ(second->rw.dst.num_sge, 1u);
        EXPECT_NE(first->rw.dst.sge, second->rw.dst.sge);
        EXPECT_EQ(first->rw.dst.sge->tseg, &remote_segment_);
        EXPECT_EQ(first->rw.dst.sge->addr, remote_sge_.addr);
        EXPECT_EQ(first->rw.dst.sge->len, 8u);
        EXPECT_EQ(second->rw.dst.sge->tseg, &second_segment);
        EXPECT_EQ(second->rw.dst.sge->addr, second_segment.seg.ubva.va + 4);
        EXPECT_EQ(second->rw.dst.sge->len, 4u);
        EXPECT_EQ(second->rw.src.sge, &second_source);
        EXPECT_EQ(second->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(requests, ids), 2u);
    for (std::size_t i = 0; i < ids.size(); ++i) {
        const auto record = ledger_.Complete(ids[i]);
        ASSERT_TRUE(record);
        EXPECT_EQ(record->metadata.target, requests[i].metadata.target);
        EXPECT_EQ(record->metadata.grant_lease, requests[i].metadata.grant_lease);
    }
}

TEST_F(TxSenderTest, MixedWritePartialAcceptanceKeepsOnlyPrefixAndNeverReplaysIt) {
    std::array requests{WriteRequest(), ImmediateRequest(42), WriteRequest(2)};
    std::array<AttemptId, 3> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = wr->next;
        return URMA_EAGAIN;
    });
    const auto first = sender_.Send(requests, ids);
    ASSERT_FALSE(first);
    EXPECT_EQ(first.error().accepted, 1u);
    EXPECT_EQ(first.error().code, TxSendErrorCode::kPostFailed);
    const auto accepted_id = ids[0];
    EXPECT_NE(accepted_id, 0u);
    EXPECT_EQ(ids[1], 0u);
    EXPECT_EQ(ids[2], 0u);
    ASSERT_TRUE(ledger_.Lookup(accepted_id));
    EXPECT_EQ(ledger_.posted(), 1u);
    // 调用者只重试确定未提交的后缀；原 WRITE 仍在途且不因后缀失败归还 grant。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
        EXPECT_EQ(wr->send.imm_data, 42u);
        EXPECT_EQ(wr->next->opcode, URMA_OPC_WRITE);
        EXPECT_EQ(wr->next->rw.dst.sge->tseg, &remote_segment_);
        EXPECT_NE(wr->user_ctx, accepted_id);
        EXPECT_NE(wr->next->user_ctx, accepted_id);
        EXPECT_EQ(wr->next->next, nullptr);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(std::span(requests).subspan(1), std::span(ids).subspan(1)), 2u);
    EXPECT_EQ(ids[0], accepted_id);
    for (auto id : ids) {
        ASSERT_TRUE(ledger_.Complete(id));
    }
}

TEST_F(TxSenderTest, WriteWithUnknownAcceptanceRetainsPreparedRecords) {
    CheckUncertainBoundary(false, false, true);
}

TEST_F(TxSenderTest, CompletionCannotAutomaticallyReleaseWriteWithUnknownAcceptance) {
    std::array requests{WriteRequest(), WriteRequest(2)};
    std::array<AttemptId, 2> ids{};
    // mock 接受第一条、未接受第二条，却不提供 bad_wr；生产代码无法据此确认任何前缀。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_EINVAL));
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    const auto sent = sender_.Send(requests, ids);
    ASSERT_FALSE(sent);
    EXPECT_EQ(sent.error().code, TxSendErrorCode::kProviderContract);
    EXPECT_FALSE(sent.error().accepted);
    EXPECT_EQ(ledger_.size(), 2u);
    EXPECT_EQ(ledger_.posted(), 0u);
    const auto first = ledger_.Lookup(ids[0]);
    ASSERT_TRUE(first);

    // 取到第一条真实 CQE 仍不能跳过已提交状态校验，Prepared 记录及两类 lease 均继续保留。
    EXPECT_CALL(*this, Poll(&tx_, 1, _)).WillOnce([&](auto*, int, auto* cr) {
        *cr = {};
        cr->flag.bs.jetty = 1;
        cr->local_id = jetty_.jetty_id.id;
        cr->user_ctx = ids[0];
        cr->status = URMA_CR_SUCCESS;
        return 1;
    });
    TxCompletionProcessor processor(ledger_);
    std::array<TxCompletionEvent, 1> events{};
    const auto completed = processor.Poll(events);
    ASSERT_TRUE(completed);
    EXPECT_EQ(completed->count, 1u);
    EXPECT_EQ(completed->retired, 0u);
    EXPECT_EQ(events[0].kind, TxCompletionKind::kRejected);
    EXPECT_FALSE(events[0].record);
    ASSERT_TRUE(events[0].error);
    EXPECT_EQ(events[0].error->code, TxCompletionErrorCode::kLedgerFailure);
    ASSERT_TRUE(events[0].error->ledger_error);
    EXPECT_EQ(events[0].error->ledger_error->code, AttemptLedgerErrorCode::kInvalidState);
    EXPECT_EQ(ledger_.size(), 2u);
    EXPECT_EQ(ledger_.posted(), 0u);
    EXPECT_EQ(pool_.lane_state(first->ticket.lane), JettyLaneState::kError);
    for (std::size_t i = 0; i < ids.size(); ++i) {
        const auto retained = ledger_.Lookup(ids[i]);
        ASSERT_TRUE(retained);
        EXPECT_EQ(retained->metadata.buffer_lease, requests[i].metadata.buffer_lease);
        EXPECT_EQ(retained->metadata.grant_lease, requests[i].metadata.grant_lease);
    }
    EXPECT_FALSE(sender_.Send(requests, ids));
    EXPECT_FALSE(ledger_.Close());

    // 仅测试凭明确 mock 事实和已取得的真实 CQE 手工结清；未知接受或空 flush 本身不允许这样恢复。
    ASSERT_TRUE(ledger_.Commit(ids[0], 1));
    ASSERT_TRUE(ledger_.Complete(ids[0]));
    ASSERT_TRUE(ledger_.Cancel(ids[1]));
    ASSERT_TRUE(pool_.ObserveFlushDone(first->ticket.lane));
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
    std::array<urma_cr_t, 1> flushed{};
    ASSERT_TRUE(pool_.FlushSend(first->ticket.lane, flushed));
}

TEST_F(TxSenderTest, InvalidWriteRejectsEntireMixedBatchBeforePreparingAnyPrefix) {
    // 用单调 attempt id 观测是否发生过隐藏预留/撤销，不能只看失败后的账本恰好为空。
    const auto before = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(before);
    ASSERT_TRUE(ledger_.Cancel(*before));
    std::array requests{WriteRequest(), ImmediateRequest(42), WriteRequest(2)};
    ++requests[2].remote->addr; // 最后一条越出授权区间，前面的 WRITE/IMM 都不能先行提交。
    std::array<AttemptId, 3> ids{123, 124, 125};
    const auto sent = sender_.Send(requests, ids);
    ASSERT_FALSE(sent);
    EXPECT_EQ(sent.error().code, TxSendErrorCode::kInvalidArgument);
    EXPECT_EQ(sent.error().accepted, 0u);
    EXPECT_EQ(ledger_.size(), 0u);
    EXPECT_EQ(ledger_.posted(), 0u);
    for (const auto id : ids) {
        EXPECT_EQ(id, 0u);
    }
    const auto after = ledger_.Prepare(Metadata(1));
    ASSERT_TRUE(after);
    EXPECT_EQ(*after, *before + 1);
    ASSERT_TRUE(ledger_.Cancel(*after));
}

TEST_F(TxSenderTest, WriteRejectsInvalidSegmentsAndLengthsBeforeAnyReservation) {
    std::array<AttemptId, 1> ids{};
    auto reject = [&](const TxSendRequest& request) {
        ids[0] = 123;
        const auto sent = sender_.Send(std::span(&request, 1), ids);
        ASSERT_FALSE(sent);
        EXPECT_EQ(sent.error().code, TxSendErrorCode::kInvalidArgument);
        EXPECT_EQ(sent.error().accepted, 0u);
        EXPECT_EQ(ids[0], 0u);
        EXPECT_EQ(ledger_.size(), 0u);
    };

    // WRITE 必须提供恰好覆盖源数据的已导入远端区间；SEND 不得意外携带远端描述符。
    auto request = WriteRequest();
    request.remote.reset();
    reject(request);
    request = Request();
    request.remote = remote_sge_;
    reject(request);
    request = WriteRequest();
    --request.remote->len;
    reject(request);
    request.remote->len = 0;
    reject(request);
    request = WriteRequest();
    request.remote->tseg = nullptr;
    reject(request);
    request = WriteRequest();
    request.remote->addr = remote_segment_.mva;
    reject(request);
    request.remote->addr = remote_segment_.seg.ubva.va - 1;
    reject(request);
    request.remote->addr = remote_segment_.seg.ubva.va + 1;
    reject(request);

    // 不同 context 的注册段、导入目标或导入段不能混入当前 SQ。
    urma_context_t foreign{};
    request = WriteRequest();
    remote_segment_.urma_ctx = &foreign;
    reject(request);
    remote_segment_.urma_ctx = &ctx_;
    segment_.urma_ctx = &foreign;
    reject(request);
    segment_.urma_ctx = &ctx_;
    remote_a_.urma_ctx = &foreign;
    reject(request);
    remote_a_.urma_ctx = &ctx_;

    // 本地越界、空区间和整数回绕均在 provider 调用前拒绝；不通过“地址+长度”回绕绕过校验。
    const auto original = sge_;
    ++sge_.addr;
    reject(request);
    sge_ = original;
    sge_.len = 0;
    reject(request);
    sge_ = original;
    sge_.addr = std::numeric_limits<std::uint64_t>::max() - 3;
    segment_.seg.ubva.va = sge_.addr;
    reject(request);
    sge_ = original;
    segment_.seg.ubva.va = sge_.addr;
    remote_segment_.seg.ubva.va = std::numeric_limits<std::uint64_t>::max() - 3;
    request.remote->addr = remote_segment_.seg.ubva.va;
    reject(request);
    remote_segment_.seg.ubva.va = remote_sge_.addr;

    // SGE 数不得超过实际创建配置，即使硬件可能支持更多。
    request = WriteRequest();
    std::array too_many{sge_, sge_, sge_};
    request.sges = too_many;
    reject(request);
}

TEST_F(TxSenderTest, WriteHonorsConfirmed200MiBLimitEvenWhenProviderReportsMore) {
    // mock 不访问 payload，使用虚拟地址覆盖边界值，避免测试分配 200 MiB 内存。
    sge_.addr = segment_.seg.ubva.va = 0x10000000ULL;
    segment_.seg.len = remote_segment_.seg.len = TxSender::kMaxWriteSize + 1;
    sge_.len = static_cast<std::uint32_t>(TxSender::kMaxWriteSize);
    remote_sge_.len = sge_.len;
    std::array requests{WriteRequest()};
    std::array<AttemptId, 1> ids{};
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto**) {
        EXPECT_EQ(wr->rw.dst.sge->len, TxSender::kMaxWriteSize);
        return URMA_SUCCESS;
    });
    ASSERT_EQ(sender_.Send(requests, ids), 1u);
    ASSERT_TRUE(ledger_.Complete(ids[0]));
    ++sge_.len;
    ++requests[0].remote->len;
    const auto too_large = sender_.Send(requests, ids);
    ASSERT_FALSE(too_large);
    EXPECT_EQ(too_large.error().code, TxSendErrorCode::kInvalidArgument);
    EXPECT_EQ(ledger_.size(), 0u);
}

TEST_F(TxSenderTest, WriteHonorsDeviceCapabilitySnapshot) {
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(ledger_.Close());
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJetty(&passive_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.Close());
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 2;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    cap.max_write_size = 4;
    JettyPoolConfig config;
    config.tx_depth = config.rx_depth = 3;
    EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&passive_)).WillOnce(Return(&jetty_));
    ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
    ASSERT_TRUE(ledger_.Open(pool_, 3));
    ASSERT_TRUE(sender_.Open(ledger_));
    // 改写调用者的 cap 不得扩大已创建池的能力；8 B 请求超出快照的 4 B 限制。
    cap.max_write_size = 256U * 1024 * 1024;
    std::array requests{WriteRequest()};
    std::array<AttemptId, 1> ids{};
    EXPECT_FALSE(sender_.Send(requests, ids));
    EXPECT_EQ(ledger_.size(), 0u);
    sge_.len = 4;
    requests[0].remote->len = 4;
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_EQ(sender_.Send(requests, ids), 1u);
    ASSERT_TRUE(ledger_.Complete(ids[0]));
}

TEST_F(TxSenderTest, DeferredContractFaultOnlyMarksLaneUntilOwnerRunsControlWork) {
    ASSERT_TRUE(sender_.Close());
    ASSERT_TRUE(sender_.Open(ledger_, true));
    EXPECT_TRUE(sender_.defer_drain());
    std::array requests{Request()};
    std::array<AttemptId, 1> ids{};
    // provider 明确没有投递，但故意遗漏 bad_wr；提交层只能保留 Prepared，禁止快路径 Modify。
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_EINVAL));
    const auto sent = sender_.Send(requests, ids);
    ASSERT_FALSE(sent);
    EXPECT_EQ(sent.error().code, TxSendErrorCode::kProviderContract);
    EXPECT_FALSE(sent.error().accepted);
    ASSERT_NE(ids[0], 0u);
    const auto record = ledger_.Lookup(ids[0]);
    ASSERT_TRUE(record);
    EXPECT_EQ(pool_.lane_state(record->ticket.lane), JettyLaneState::kFaulted);
    EXPECT_EQ(ledger_.size(), 1u);
    EXPECT_EQ(ledger_.posted(), 0u);
    EXPECT_FALSE(sender_.Send(requests, ids));
    // owner 的慢控制任务在 post 返回后才切 ERROR；以下取消凭本测试明确的未投递事实。
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    ASSERT_TRUE(pool_.BeginDrain(record->ticket.lane));
    ASSERT_TRUE(ledger_.Cancel(record->id));
    ASSERT_TRUE(pool_.ObserveFlushDone(record->ticket.lane));
    EXPECT_CALL(*this, Flush(&jetty_, 1, _)).WillOnce(Return(0));
    std::array<urma_cr_t, 1> flushed{};
    ASSERT_TRUE(pool_.FlushSend(record->ticket.lane, flushed));
}
} // namespace
} // namespace raw
} // namespace kbsocket
