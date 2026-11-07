// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/raw/receive_domain_closer.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;
using ::testing::Sequence;
using ::testing::StrictMock;

class ReceiveDomainUrmaMock {
public:
    MOCK_METHOD(urma_jfce_t*, CreateJfce, (urma_context_t*));
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, DeleteJfce, (urma_jfce_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));

    static UrmaFunctions Functions() {
        UrmaFunctions f{};
        f.create_jfce = [](auto* c) { return active->CreateJfce(c); };
        f.create_jfc = [](auto* c, auto* cfg) { return active->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active->CreateJetty(c, cfg); };
        f.delete_jfc = [](auto* q) { return active->DeleteJfc(q); };
        f.delete_jfce = [](auto* q) { return active->DeleteJfce(q); };
        f.delete_jfr = [](auto* q) { return active->DeleteJfr(q); };
        f.delete_jetty = [](auto* q) { return active->DeleteJetty(q); };
        f.register_seg = [](auto* c, auto* cfg) { return active->Register(c, cfg); };
        f.unregister_seg = [](auto* s) { return active->Unregister(s); };
        f.post_jfr_wr = [](auto* q, auto* wr, auto** bad) { return active->Post(q, wr, bad); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active->Poll(q, n, cr); };
        f.modify_jetty = [](auto* q, auto* attr) { return active->Modify(q, attr); };
        f.flush_jetty = [](auto* q, int n, auto* cr) { return active->Flush(q, n, cr); };
        return f;
    }

    static inline ReceiveDomainUrmaMock* active = nullptr;
};

class ReceiveDomainCloserTest : public ::testing::Test {
protected:
    struct Posted {
        std::uint64_t id;
        std::span<std::byte> memory;
    };

    void SetUp() override {
        ReceiveDomainUrmaMock::active = &api_;
        jfr_.jfr_id.id = 23;
        rx_target_.jetty_id.id = 29;
        jetties_[0].jetty_id.id = 19;
        jetties_[1].jetty_id.id = 20;
        Open(Events());
    }

    void TearDown() override {
        EXPECT_EQ(closer_.stage(), ReceiveDomainCloseStage::kClosed);
        EXPECT_EQ(buffers_.posted(), 0u);
        EXPECT_EQ(buffers_.held(), 0u);
        EXPECT_TRUE(buffers_.Close());
        EXPECT_TRUE(pool_.Close());
        // 正常完成后，析构不应再次接触 provider 资源。
        ReceiveDomainUrmaMock::active = nullptr;
    }

    virtual bool Events() const noexcept {
        return false;
    }

    void Open(bool events = false) {
        ::testing::InSequence creation;
        if (events) {
            EXPECT_CALL(api_, CreateJfce(&ctx_)).WillOnce(Return(&jfce_));
        }
        EXPECT_CALL(api_, CreateJfc(&ctx_, _))
            .WillOnce([&, events](auto*, auto* cfg) {
                EXPECT_EQ(cfg->jfce, events ? &jfce_ : nullptr);
                return &tx_;
            })
            .WillOnce([&, events](auto*, auto* cfg) {
                EXPECT_EQ(cfg->jfce, events ? &jfce_ : nullptr);
                return &rx_;
            });
        EXPECT_CALL(api_, CreateJfr(&ctx_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(api_, CreateJetty(&ctx_, _))
            .WillOnce(Return(&rx_target_))
            .WillOnce(Return(&jetties_[0]))
            .WillOnce(Return(&jetties_[1]));
        EXPECT_CALL(api_, Register(&ctx_, _)).WillOnce(Return(&segment_));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 3;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 1024;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        JettyPoolConfig config;
        config.jetty_count = 2;
        config.tx_depth = 3;
        config.rx_depth = 3;
        config.tx_cq_depth = 9;
        config.rx_cq_depth = 3;
        config.events = events;
        ASSERT_TRUE(pool_.Open(&ctx_, cap, config));
        ASSERT_TRUE(buffers_.Open(pool_, 5));
    }

    static ReceiveDomainQuiescence Quiesced() {
        return {
            .peer_stopped = true, .events_acked = true, .queue_users_stopped = true, .completion_queues_healthy = true};
    }

    void Capture(urma_jfr_wr_t* wr) {
        posted_.clear();
        for (; wr; wr = wr->next) {
            auto* address = reinterpret_cast<std::byte*>(wr->src.sge[0].addr);
            posted_.push_back({wr->user_ctx, {address, wr->src.sge[0].len}});
        }
    }

    void Refill(std::size_t count = 3) {
        EXPECT_CALL(api_, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
            Capture(wr);
            return URMA_SUCCESS;
        });
        auto result = buffers_.Refill(count);
        ASSERT_TRUE(result);
        EXPECT_EQ(*result, count);
    }

    urma_cr_t Completion(std::size_t index) const {
        urma_cr_t cr{};
        cr.flag.bs.s_r = 1;
        cr.flag.bs.jetty = 1;
        cr.local_id = rx_target_.jetty_id.id;
        cr.user_ctx = posted_[index].id;
        cr.completion_len = 17;
        cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        cr.imm_data = 0x123456789abcdef0;
        return cr;
    }

    void ExpectRx(std::vector<urma_cr_t> completions = {}) {
        EXPECT_CALL(api_, Poll(&rx_, static_cast<int>(events_.size()), _))
            .InSequence(cleanup_)
            .WillOnce([completions](auto*, int, auto* out) {
                std::copy(completions.begin(), completions.end(), out);
                return static_cast<int>(completions.size());
            });
    }

    // 用同一顺序约束覆盖所有依赖边界；被动端点同样必须先于共享 JFR 删除。
    void ExpectDeleteStep(int step, urma_status_t status = URMA_SUCCESS) {
        switch (step) {
        case 0:
            EXPECT_CALL(api_, DeleteJetty(&jetties_[1])).InSequence(cleanup_).WillOnce(Return(status));
            break;
        case 1:
            EXPECT_CALL(api_, DeleteJetty(&jetties_[0])).InSequence(cleanup_).WillOnce(Return(status));
            break;
        case 2:
            EXPECT_CALL(api_, DeleteJetty(&rx_target_)).InSequence(cleanup_).WillOnce(Return(status));
            break;
        case 3:
            EXPECT_CALL(api_, DeleteJfr(&jfr_)).InSequence(cleanup_).WillOnce(Return(status));
            break;
        case 4:
            EXPECT_CALL(api_, Unregister(&segment_)).InSequence(cleanup_).WillOnce([&, status](auto*) {
                EXPECT_EQ(buffers_.posted(), 0u);
                EXPECT_EQ(buffers_.held(), 0u);
                return status;
            });
            break;
        case 5:
            EXPECT_CALL(api_, DeleteJfc(&rx_)).InSequence(cleanup_).WillOnce(Return(status));
            break;
        case 6:
            EXPECT_CALL(api_, DeleteJfc(&tx_)).InSequence(cleanup_).WillOnce(Return(status));
            break;
        case 7:
            EXPECT_CALL(api_, DeleteJfce(&jfce_)).InSequence(cleanup_).WillOnce(Return(status));
            break;
        default:
            ADD_FAILURE() << "invalid deletion step";
        }
    }

    void ExpectDeleteSteps(int first = 0, int last = 7) {
        for (int i = first; i < last; ++i) {
            ExpectDeleteStep(i);
        }
    }

    void Finish(std::size_t retired_rx) {
        ExpectRx();
        ExpectDeleteSteps();
        auto result = closer_.Advance(Quiesced(), events_);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kClosed);
        EXPECT_EQ(result->rx_events, 0u);
        EXPECT_EQ(result->retired_rx, retired_rx);
        EXPECT_EQ(buffers_.retired_on_delete(), retired_rx);
    }

    void ExpectExclusiveClose() {
        auto rx_close = buffers_.Close();
        ASSERT_FALSE(rx_close);
        EXPECT_EQ(rx_close.error().code, RxBufferErrorCode::kInUse);
        auto pool_close = pool_.Close();
        ASSERT_FALSE(pool_close);
        EXPECT_EQ(pool_close.error().code, JettyPoolErrorCode::kInUse);
    }

    StrictMock<ReceiveDomainUrmaMock> api_;
    test_support::ScopedUrmaOverride scope_{ReceiveDomainUrmaMock::Functions()};
    urma_context_t ctx_{};
    urma_jfce_t jfce_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t rx_target_{};
    std::array<urma_jetty_t, 2> jetties_{};
    urma_target_seg_t segment_{};
    JettyPool pool_;
    RxBufferPool buffers_;
    ReceiveDomainCloser closer_;
    std::array<RxBufferEvent, 4> events_{};
    std::vector<Posted> posted_;
    Sequence cleanup_;
};

struct ResidualCase {
    std::size_t posted;
    std::size_t consumed;
};

class ReceiveDomainResidualTest : public ReceiveDomainCloserTest, public ::testing::WithParamInterface<ResidualCase> {};

TEST_P(ReceiveDomainResidualTest, DeletesResidualRxOnlyAfterAllDependentJetties) {
    const auto [posted, consumed] = GetParam();
    Refill(posted);
    if (consumed) {
        std::vector<urma_cr_t> completions;
        for (std::size_t i = 0; i < consumed; ++i) {
            completions.push_back(Completion(i));
        }
        ExpectRx(completions);
        auto result = buffers_.Poll(events_);
        ASSERT_TRUE(result);
        ASSERT_EQ(*result, consumed);
        for (std::size_t i = 0; i < consumed; ++i) {
            ASSERT_TRUE(events_[i].lease);
            ASSERT_TRUE(buffers_.Release(*events_[i].lease));
        }
    }
    // 全未消费、部分消费及只剩最后一个 RX 均以 DeleteJfr 成功为终结证据。
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    Finish(posted - consumed);
    auto again = closer_.Advance({}, {});
    ASSERT_TRUE(again);
    EXPECT_EQ(again->stage, ReceiveDomainCloseStage::kClosed);
    EXPECT_EQ(again->rx_events, 0u);
    EXPECT_EQ(again->retired_rx, posted - consumed);
}

INSTANTIATE_TEST_SUITE_P(ResidualRx, ReceiveDomainResidualTest,
                         ::testing::Values(ResidualCase{1, 0}, ResidualCase{3, 0}, ResidualCase{3, 1},
                                           ResidualCase{3, 2}, ResidualCase{3, 3}));

class ReceiveDomainProofTest : public ReceiveDomainCloserTest, public ::testing::WithParamInterface<int> {};

TEST_P(ReceiveDomainProofTest, AnEmptyCqCannotReplaceAnyQuiescenceProof) {
    Refill();
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    auto proof = Quiesced();
    const std::array members{&ReceiveDomainQuiescence::peer_stopped, &ReceiveDomainQuiescence::events_acked,
                             &ReceiveDomainQuiescence::queue_users_stopped,
                             &ReceiveDomainQuiescence::completion_queues_healthy};
    proof.*members[GetParam()] = false;
    ExpectRx();
    auto result = closer_.Advance(proof, events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);
    EXPECT_EQ(result->retired_rx, 0u);
    EXPECT_EQ(buffers_.posted(), 3u);
    ExpectExclusiveClose();
    Finish(3);
}

INSTANTIATE_TEST_SUITE_P(RequiredProof, ReceiveDomainProofTest, ::testing::Range(0, 4));

TEST_F(ReceiveDomainCloserTest, ACompletionAfterAnEmptyPollIsDeliveredBeforeDeletion) {
    Refill();
    posted_[0].memory[0] = std::byte{42};
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    ExpectRx();
    auto result = closer_.Advance({}, events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);

    // 先前的空 CQ 不是屏障：远端停止确认后仍需消费迟到 CQE，并把真实 lease 交给调用者。
    ExpectRx({Completion(0)});
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    ASSERT_EQ(result->rx_events, 1u);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);
    ASSERT_TRUE(events_[0].lease);
    const auto held = *events_[0].lease;
    const auto payload = events_[0].payload;
    ASSERT_EQ(payload.size(), 17u);
    EXPECT_EQ(payload[0], std::byte{42});
    EXPECT_EQ(events_[0].completion.imm_data, 0x123456789abcdef0u);

    ExpectRx();
    ExpectDeleteSteps(0, 4);
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kWaitingForLeases);
    EXPECT_EQ(result->rx_events, 0u);
    EXPECT_EQ(result->retired_rx, 2u);
    EXPECT_EQ(buffers_.posted(), 0u);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(buffers_.retired_on_delete(), 2u);
    EXPECT_EQ(payload[0], std::byte{42});
    ExpectExclusiveClose();

    // 删除 JFR 后只等待已有 lease，不再访问 CQ，不把 Held 当作残留 Posted 清退。
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kWaitingForLeases);
    ASSERT_TRUE(buffers_.Release(held));
    EXPECT_FALSE(buffers_.Release(held));
    ExpectDeleteSteps(4, 7);
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kClosed);
    EXPECT_EQ(result->retired_rx, 2u);
}

TEST_F(ReceiveDomainCloserTest, BeginFreezesAdmissionButAllowsExistingTicketsToFinish) {
    auto reserved = pool_.Reserve();
    auto accepted = pool_.Reserve();
    ASSERT_TRUE(reserved);
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    EXPECT_FALSE(pool_.Reserve());
    EXPECT_FALSE(pool_.ReserveOn(reserved->lane));
    EXPECT_FALSE(pool_.Get(reserved->lane));
    // 远端仍可能推进关闭协商：只停止本地发送准入，继续提供 RX，避免凭空制造 RNR。
    EXPECT_FALSE(buffers_.stopped());
    Refill();
    auto proof = Quiesced();
    proof.peer_stopped = false;
    ExpectRx();
    auto result = closer_.Advance(proof, events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);
    EXPECT_FALSE(buffers_.stopped());
    ExpectExclusiveClose();

    // Begin 之前已获得硬件接受结果的票据仍可 Commit；其余 Reserved 仍可 Cancel。
    ASSERT_TRUE(pool_.Commit(*accepted));
    ExpectRx();
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);
    auto refill = buffers_.Refill();
    ASSERT_FALSE(refill);
    EXPECT_EQ(refill.error().code, RxBufferErrorCode::kStopped);
    ASSERT_TRUE(pool_.Cancel(*reserved));
    ExpectRx();
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);

    std::array<urma_cr_t, 2> tx_events{};
    EXPECT_CALL(api_, Poll(&tx_, 2, _)).WillOnce(Return(0));
    EXPECT_EQ(pool_.PollSend(tx_events), 0);
    ASSERT_TRUE(pool_.Complete(*accepted));
    Finish(3);
}

TEST_F(ReceiveDomainCloserTest, ErrorLaneNeedsFlushBoundaryAndEmptySoftwareFlush) {
    auto ticket = pool_.Reserve();
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(pool_.Commit(*ticket));
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    EXPECT_CALL(api_, Modify(&jetties_[ticket->lane.index], _)).WillOnce([](auto*, auto* attr) {
        EXPECT_EQ(attr->mask, static_cast<unsigned>(JETTY_STATE));
        EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
        return URMA_SUCCESS;
    });
    ASSERT_TRUE(pool_.BeginDrain(ticket->lane));
    ASSERT_TRUE(pool_.Complete(*ticket));

    // 用户 WR 已完成也不能跳过 ERROR 排空协议，边界事件不等价于软件 flush 完成。
    ExpectRx();
    auto result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);
    ASSERT_TRUE(pool_.ObserveFlushDone(ticket->lane));
    ExpectRx();
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kDraining);
    std::array<urma_cr_t, 3> flush_events{};
    EXPECT_CALL(api_, Flush(&jetties_[ticket->lane.index], 3, _)).WillOnce(Return(0));
    ASSERT_EQ(pool_.FlushSend(ticket->lane, flush_events), 0);
    Finish(0);
}

class ReceiveDomainDeleteRetryTest : public ReceiveDomainCloserTest, public ::testing::WithParamInterface<int> {};

TEST_P(ReceiveDomainDeleteRetryTest, FailurePreservesDependenciesAndRetriesOnlyRemainingHandles) {
    Refill();
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    const int failed_step = GetParam();
    ExpectRx();
    ExpectDeleteSteps(0, failed_step);
    ExpectDeleteStep(failed_step, URMA_FAIL);
    auto result = closer_.Advance(Quiesced(), events_);
    ASSERT_FALSE(result);
    if (failed_step == 4) {
        EXPECT_EQ(result.error().code, ReceiveDomainCloseErrorCode::kMemoryCloseFailed);
        ASSERT_TRUE(result.error().rx_error);
        EXPECT_EQ(result.error().rx_error->code, RxBufferErrorCode::kUnregisterFailed);
        EXPECT_EQ(result.error().rx_error->provider_error, URMA_FAIL);
    } else {
        EXPECT_EQ(result.error().code, ReceiveDomainCloseErrorCode::kQueueCloseFailed);
        ASSERT_TRUE(result.error().queue_error);
        const auto resource = failed_step < 2    ? JettyPoolResource::kJetty
                              : failed_step == 2 ? JettyPoolResource::kRxTargetJetty
                              : failed_step == 3 ? JettyPoolResource::kJfr
                              : failed_step == 5 ? JettyPoolResource::kRecvJfc
                                                 : JettyPoolResource::kSendJfc;
        EXPECT_EQ(result.error().queue_error->resource, resource);
        EXPECT_EQ(result.error().queue_error->code, JettyPoolErrorCode::kDeleteFailed);
        EXPECT_EQ(result.error().queue_error->provider_error, URMA_FAIL);
        if (failed_step < 2) {
            EXPECT_EQ(result.error().queue_error->jetty_index, 1u - failed_step);
        }
    }
    const auto expected_stage = failed_step <= 3   ? ReceiveDomainCloseStage::kDeletingQueues
                                : failed_step == 4 ? ReceiveDomainCloseStage::kWaitingForLeases
                                                   : ReceiveDomainCloseStage::kDeletingCompletionQueues;
    EXPECT_EQ(closer_.stage(), expected_stage);
    EXPECT_EQ(buffers_.posted(), failed_step <= 3 ? 3u : 0u);
    EXPECT_EQ(buffers_.retired_on_delete(), failed_step <= 3 ? 0u : 3u);
    auto pool_close = pool_.Close();
    ASSERT_FALSE(pool_close);
    EXPECT_EQ(pool_close.error().code, JettyPoolErrorCode::kInUse);
    if (failed_step <= 3) {
        // 删除已开始时 provider 可能移除了内部映射，所有直接轮询入口也必须失效。
        std::array<urma_cr_t, 2> completions{};
        EXPECT_FALSE(pool_.PollSend(completions));
        EXPECT_FALSE(pool_.PollRecv(completions));
        EXPECT_FALSE(buffers_.Poll(events_));
    }

    // 一旦开始删除，失败重试不得再次 poll；已经成功删除的 Jetty/JFR/MR/JFC 不重复处理。
    ExpectDeleteSteps(failed_step, 7);
    result = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kClosed);
    EXPECT_EQ(result->retired_rx, 3u);
}

INSTANTIATE_TEST_SUITE_P(EachDependency, ReceiveDomainDeleteRetryTest, ::testing::Range(0, 7));

class ReceiveDomainEventCloseTest : public ReceiveDomainCloserTest {
protected:
    bool Events() const noexcept override {
        return true;
    }
};

TEST_F(ReceiveDomainEventCloseTest, HeldLeaseAndFailedJfceDeletionPreserveFinalDependencyUntilRetry) {
    Refill();
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    ExpectRx({Completion(0)});
    auto progress = closer_.Advance({}, events_);
    ASSERT_TRUE(progress);
    ASSERT_EQ(progress->rx_events, 1u);
    ASSERT_TRUE(events_[0].lease);
    const auto lease = *events_[0].lease;

    ExpectRx();
    ExpectDeleteSteps(0, 4);
    progress = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(progress);
    EXPECT_EQ(progress->stage, ReceiveDomainCloseStage::kWaitingForLeases);
    EXPECT_EQ(progress->retired_rx, 2u);
    EXPECT_EQ(buffers_.posted(), 0u);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(pool_.recv_jfc(), &rx_);
    EXPECT_EQ(pool_.send_jfc(), &tx_);
    EXPECT_EQ(pool_.jfce(), &jfce_);
    ExpectExclusiveClose();
    ASSERT_TRUE(buffers_.Release(lease));

    // MR 和两个 CQ 成功删除后，JFCE 删除失败只保留最后一个通知资源。
    ExpectDeleteSteps(4, 7);
    ExpectDeleteStep(7, URMA_EAGAIN);
    progress = closer_.Advance(Quiesced(), events_);
    ASSERT_FALSE(progress);
    EXPECT_EQ(progress.error().code, ReceiveDomainCloseErrorCode::kQueueCloseFailed);
    ASSERT_TRUE(progress.error().queue_error);
    EXPECT_EQ(progress.error().queue_error->resource, JettyPoolResource::kJfce);
    EXPECT_EQ(progress.error().queue_error->provider_error, URMA_EAGAIN);
    EXPECT_EQ(closer_.stage(), ReceiveDomainCloseStage::kDeletingCompletionQueues);
    EXPECT_EQ(pool_.recv_jfc(), nullptr);
    EXPECT_EQ(pool_.send_jfc(), nullptr);
    EXPECT_EQ(pool_.jfce(), &jfce_);
    EXPECT_EQ(buffers_.pool(), nullptr);
    EXPECT_FALSE(pool_.Close()); // 仍由协调器独占，不能跳过其最后一步。

    // StrictMock 确保不再 poll、注销 MR 或删除已释放的队列，仅重试 JFCE。
    ExpectDeleteStep(7);
    progress = closer_.Advance(Quiesced(), events_);
    ASSERT_TRUE(progress);
    EXPECT_EQ(progress->stage, ReceiveDomainCloseStage::kClosed);
    EXPECT_EQ(progress->retired_rx, 2u);
    EXPECT_EQ(pool_.jfce(), nullptr);
    EXPECT_TRUE(closer_.Advance({}, {}));
}

TEST_F(ReceiveDomainCloserTest, UnknownRxPostBoundaryIsResolvedByActualJfrDeletion) {
    EXPECT_CALL(api_, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        Capture(wr);
        return URMA_FAIL; // 无 bad_wr，三条 WR 均可能被接受，必须保留全部 DMA 依赖。
    });
    auto refill = buffers_.Refill();
    ASSERT_FALSE(refill);
    EXPECT_FALSE(refill.error().accepted);
    EXPECT_EQ(buffers_.posted(), 3u);
    EXPECT_TRUE(buffers_.stopped());
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    Finish(3);
}

TEST_F(ReceiveDomainCloserTest, PollFailureRetainsTheDomainWithoutRetryingOrDeleting) {
    Refill();
    std::array<urma_cr_t, 2> completions{};
    // 子进程有意保留整个依赖链直到退出，父进程仍按正常协议关闭，避免把泄漏当作回收。
    EXPECT_EXIT(
        {
            EXPECT_CALL(api_, Poll(&rx_, static_cast<int>(events_.size()), _))
                .WillOnce([&](auto*, int, auto* out) {
                    out[0] = Completion(0);
                    return 1;
                })
                .WillOnce(Return(-1));
            if (!closer_.Begin(pool_, buffers_)) {
                ::_exit(1);
            }
            auto delivered = closer_.Advance({}, events_);
            if (!delivered || delivered->rx_events != 1u || !events_[0].lease) {
                ::_exit(2);
            }
            const auto lease = *events_[0].lease;
            auto failed = closer_.Advance(Quiesced(), events_);
            if (failed || failed.error().code != ReceiveDomainCloseErrorCode::kRxPollFailed ||
                !failed.error().rx_error || failed.error().rx_error->code != RxBufferErrorCode::kPollFailed ||
                failed.error().rx_error->provider_error != -1 ||
                closer_.stage() != ReceiveDomainCloseStage::kRetained || buffers_.posted() != 2u) {
                ::_exit(3);
            }
            auto again = closer_.Advance(Quiesced(), events_);
            if (again || again.error().code != ReceiveDomainCloseErrorCode::kRxPollFailed || !again.error().rx_error ||
                again.error().rx_error->provider_error != -1 || closer_.stage() != ReceiveDomainCloseStage::kRetained ||
                buffers_.posted() != 2u || buffers_.retired_on_delete() || buffers_.Close() || pool_.Close()) {
                ::_exit(4);
            }
            // 协调器和直接调用者都不得重新 poll 故障域，但已有 Held 仍可按 lease 归还。
            if (pool_.PollSend(completions) || pool_.PollRecv(completions) || buffers_.Poll(events_) ||
                buffers_.Refill() || !buffers_.Release(lease) || buffers_.held() != 0u) {
                ::_exit(5);
            }
            const bool verified = ::testing::Mock::VerifyAndClearExpectations(&api_);
            ::_exit(!verified || ::testing::Test::HasFailure() ? 6 : 0);
        },
        ::testing::ExitedWithCode(0), "");
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    Finish(3);
}

TEST_F(ReceiveDomainCloserTest, SecondCloserCannotStealTheDomain) {
    Refill();
    ReceiveDomainCloser second;
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    EXPECT_FALSE(second.Begin(pool_, buffers_));
    EXPECT_EQ(second.stage(), ReceiveDomainCloseStage::kIdle);
    ExpectExclusiveClose();
    Finish(3);
}

TEST_F(ReceiveDomainCloserTest, ClosedCloserAndOldLeaseCannotAffectReopenedObjects) {
    Refill(1);
    ExpectRx({Completion(0)});
    auto polled = buffers_.Poll(events_);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    ASSERT_TRUE(events_[0].lease);
    const auto old = *events_[0].lease;
    ASSERT_TRUE(buffers_.Release(old));
    Refill();
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    Finish(3);

    Open();
    EXPECT_EQ(buffers_.retired_on_delete(), 0u);
    Refill(1);
    ExpectRx({Completion(0)});
    polled = buffers_.Poll(events_);
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    ASSERT_TRUE(events_[0].lease);
    const auto current = *events_[0].lease;
    EXPECT_NE(current.id, old.id);
    EXPECT_FALSE(buffers_.Release(old));
    EXPECT_EQ(buffers_.held(), 1u);

    // 旧 closer 只返回旧生命周期的结果，不重新停止新池，也不接触新队列或 lease。
    auto old_result = closer_.Advance({}, {});
    ASSERT_TRUE(old_result);
    EXPECT_EQ(old_result->stage, ReceiveDomainCloseStage::kClosed);
    EXPECT_EQ(old_result->retired_rx, 3u);
    EXPECT_TRUE(pool_.ready());
    EXPECT_FALSE(buffers_.stopped());
    auto ticket = pool_.Reserve();
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(pool_.Cancel(*ticket));
    ASSERT_TRUE(buffers_.Release(current));

    ReceiveDomainCloser next;
    ASSERT_TRUE(next.Begin(pool_, buffers_));
    ExpectRx();
    ExpectDeleteSteps();
    auto result = next.Advance(Quiesced(), events_);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->stage, ReceiveDomainCloseStage::kClosed);
    EXPECT_EQ(result->retired_rx, 0u);
}

} // namespace
} // namespace raw
} // namespace kbsocket
