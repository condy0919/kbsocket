// SPDX-License-Identifier: MulanPSL-2.0
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <deque>
#include <thread>

#include "tools/peer_failure_test/peer_failure_test.hpp"
#include "tools/peer_failure_test/process.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Return;

TEST(PeerFailureProtocolTest, RoundTripPreservesEndpointAndFaultSelection) {
    PeerFailureHello hello{.options = {.batch = 256, .fault_delay_ms = 123, .fault = PeerFault::kExternal},
                           .server = true};
    hello.endpoint.id = 0x12345678;
    hello.endpoint.eid.raw[15] = 0xfe;
    auto decoded = DecodePeerFailureHello(EncodePeerFailureHello(hello));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->endpoint.id, hello.endpoint.id);
    EXPECT_EQ(decoded->endpoint.eid.raw[15], 0xfe);
    EXPECT_TRUE(CheckPeerFailureHello(*decoded, hello.options, false));
}

TEST(PeerFailureProtocolTest, RejectsForeignProtocolAndInvalidFault) {
    auto wire = EncodePeerFailureHello({});
    wire[0] = std::byte{0};
    EXPECT_FALSE(DecodePeerFailureHello(wire));
    wire = EncodePeerFailureHello({});
    wire[23] = std::byte{3};
    EXPECT_FALSE(DecodePeerFailureHello(wire));
}

TEST(PeerFailureProtocolTest, RejectsRoleAndOptionMismatchBeforePosting) {
    PeerFailureHello peer{.server = true};
    EXPECT_FALSE(CheckPeerFailureHello(peer, {}, true));
    peer.options.batch = 1;
    EXPECT_FALSE(CheckPeerFailureHello(peer, {}, false));
    peer.options = {};
    peer.options.fault = PeerFault::kExternal;
    EXPECT_FALSE(CheckPeerFailureHello(peer, {}, false));
}

TEST(PeerFailureProtocolTest, BoundsHardwareBatchAndWatchdogInputs) {
    EXPECT_TRUE(ValidatePeerFailureOptions({.batch = 1}));
    EXPECT_TRUE(ValidatePeerFailureOptions({.batch = 256}));
    EXPECT_FALSE(ValidatePeerFailureOptions({.batch = 257}));
    EXPECT_FALSE(ValidatePeerFailureOptions({.bytes = 4097}));
    EXPECT_FALSE(ValidatePeerFailureOptions({.timeout_ms = 0}));
    EXPECT_FALSE(ValidatePeerFailureOptions({.async_grace_ms = 60001}));
}

TEST(PeerFailureProtocolTest, DistinguishesPostBeforeExitFromSendAfterExit) {
    PeerFailureHello hello{.options = {.fault = PeerFault::kInflightExit}, .server = true};
    auto decoded = DecodePeerFailureHello(EncodePeerFailureHello(hello));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->options.fault, PeerFault::kInflightExit);
    EXPECT_TRUE(CheckPeerFailureHello(*decoded, hello.options, false));
    // 两个实验的控制 marker 顺序不同，不能仅因设备/批量一致就进入数据阶段。
    EXPECT_FALSE(CheckPeerFailureHello(*decoded, {}, false));
}

class PeerAsyncTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_status_t, Get, (urma_context_t*, urma_async_event_t*));
    MOCK_METHOD(void, Ack, (urma_async_event_t*));

protected:
    void SetUp() override {
        ASSERT_EQ(::pipe(pipe_.data()), 0);
        ctx_.async_fd = pipe_[0];
        active_ = this;
    }
    void TearDown() override {
        ::close(pipe_[0]);
        ::close(pipe_[1]);
        active_ = nullptr;
    }
    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions functions{};
        functions.get_async_event = [](urma_context_t* ctx, urma_async_event_t* event) {
            return active_->Get(ctx, event);
        };
        functions.ack_async_event = [](urma_async_event_t* event) { active_->Ack(event); };
        return functions;
    }
    static inline PeerAsyncTest* active_ = nullptr;
    std::array<int, 2> pipe_{};
    urma_context_t ctx_{};
};

TEST_F(PeerAsyncTest, UnreadableFdNeverCallsPotentiallyBlockingGet) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    // 空 fd 只能得到“尚未观察到事件”，不得调用可能阻塞的 Get，也不能伪造 Ack。
    EXPECT_CALL(*this, Get(_, _)).Times(0);
    EXPECT_CALL(*this, Ack(_)).Times(0);
    PeerAsyncStats stats;
    EXPECT_TRUE(PollPeerAsync(&ctx_, stats));
    EXPECT_TRUE(stats.available);
    EXPECT_EQ(stats.acknowledged, 0u);
}

TEST_F(PeerAsyncTest, EveryRetrievedEventIsAcknowledgedExactlyOnce) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    const char byte = 'x';
    ASSERT_EQ(::write(pipe_[1], &byte, 1), 1);
    EXPECT_CALL(*this, Get(&ctx_, _)).WillOnce([&](urma_context_t*, urma_async_event_t* event) {
        char consumed = 0;
        EXPECT_EQ(::read(pipe_[0], &consumed, 1), 1);
        event->event_type = static_cast<urma_async_event_type_t>(3);
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Ack(_)).WillOnce([](urma_async_event_t* event) {
        EXPECT_EQ(static_cast<int>(event->event_type), 3);
    });
    PeerAsyncStats stats;
    EXPECT_TRUE(PollPeerAsync(&ctx_, stats));
    EXPECT_EQ(stats.types[3], 1u);
    EXPECT_EQ(stats.acknowledged, 1u);
}

TEST_F(PeerAsyncTest, FailedGetDoesNotAckAnUnownedEvent) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    const char byte = 'x';
    ASSERT_EQ(::write(pipe_[1], &byte, 1), 1);
    EXPECT_CALL(*this, Get(_, _)).WillOnce(Return(URMA_FAIL));
    EXPECT_CALL(*this, Ack(_)).Times(0);
    PeerAsyncStats stats;
    EXPECT_FALSE(PollPeerAsync(&ctx_, stats));
    EXPECT_EQ(stats.acknowledged, 0u);
}

TEST(PeerFailureProcessTest, KillRequiresReapedChildBeforeSuccess) {
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        while (true) {
            ::pause();
        }
    }
    ASSERT_TRUE(KillAndReapPeer(child, 1000));
    int status = 0;
    // 成功结果意味着已 waitpid，不是仅发送了 SIGKILL；再次等待必须发现没有该子进程。
    EXPECT_EQ(::waitpid(child, &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

TEST(PeerFailureProcessTest, WatchdogPreservesWorkerResultAndBoundsBlockingCall) {
    EXPECT_EQ(RunPeerWatchdog([](void*) { return 7; }, nullptr, 1000), 7);
    EXPECT_EQ(RunPeerWatchdog(
                  [](void*) {
                      std::this_thread::sleep_for(std::chrono::seconds(10));
                      return 0;
                  },
                  nullptr, 10),
              124);
}

class PeerFailureSessionTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Query, (urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));

protected:
    void SetUp() override {
        active_ = this;
        ctx_.async_fd = -1;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 4;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 1024;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 1;
        cap_.max_msg_size = 4096;
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(2).WillRepeatedly([&](urma_context_t*, urma_jfc_cfg_t* cfg) {
            auto* cq = &cqs_[cq_index_++];
            cq->jfc_cfg = *cfg;
            return cq;
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce([&](urma_context_t*, urma_jfr_cfg_t* cfg) {
            jfr_.jfr_cfg = *cfg;
            return &jfr_;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce([&](urma_context_t*, urma_jetty_cfg_t* cfg) {
            jetty_.jetty_cfg = *cfg;
            jetty_.jetty_id.id = 17;
            return &jetty_;
        });
        EXPECT_CALL(*this, Register(_, _)).WillOnce(Return(&segment_));
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly(Return(&target_));
        EXPECT_CALL(*this, Modify(_, _)).Times(0);
        EXPECT_CALL(*this, Post(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([&](urma_jetty_t*, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
                *bad = nullptr;
                while (wr) {
                    urma_cr_t cr{};
                    cr.status = completion_status_;
                    cr.flag.bs.jetty = 1;
                    cr.local_id = jetty_.jetty_id.id;
                    cr.user_ctx = wr->user_ctx;
                    completions_.push_back(cr);
                    wr = wr->next;
                }
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](urma_jfc_t*, int count, urma_cr_t* cr) {
            int copied = 0;
            while (copied < count && !completions_.empty()) {
                cr[copied++] = completions_.front();
                completions_.pop_front();
            }
            return copied;
        });
        EXPECT_CALL(*this, Query(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([](urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t* attr) {
                attr->state = URMA_JETTY_STATE_READY;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
    }
    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](urma_context_t* c, urma_jfc_cfg_t* x) { return active_->CreateJfc(c, x); };
        f.create_jfr = [](urma_context_t* c, urma_jfr_cfg_t* x) { return active_->CreateJfr(c, x); };
        f.create_jetty = [](urma_context_t* c, urma_jetty_cfg_t* x) { return active_->CreateJetty(c, x); };
        f.register_seg = [](urma_context_t* c, urma_seg_cfg_t* x) { return active_->Register(c, x); };
        f.import_jetty = [](urma_context_t* c, urma_rjetty_t* x, urma_token_t* t) { return active_->Import(c, x, t); };
        f.post_jetty_send_wr = [](urma_jetty_t* j, urma_jfs_wr_t* w, urma_jfs_wr_t** b) {
            return active_->Post(j, w, b);
        };
        f.post_jfr_wr = [](urma_jfr_t* j, urma_jfr_wr_t* w, urma_jfr_wr_t** b) { return active_->PostRecv(j, w, b); };
        f.poll_jfc = [](urma_jfc_t* j, int n, urma_cr_t* c) { return active_->Poll(j, n, c); };
        f.query_jetty = [](urma_jetty_t* j, urma_jetty_cfg_t* c, urma_jetty_attr_t* a) {
            return active_->Query(j, c, a);
        };
        f.modify_jetty = [](urma_jetty_t* j, urma_jetty_attr_t* a) { return active_->Modify(j, a); };
        f.unimport_jetty = [](urma_target_jetty_t* j) { return active_->Unimport(j); };
        f.unregister_seg = [](urma_target_seg_t* s) { return active_->Unregister(s); };
        f.delete_jetty = [](urma_jetty_t* j) { return active_->DeleteJetty(j); };
        f.delete_jfr = [](urma_jfr_t* j) { return active_->DeleteJfr(j); };
        f.delete_jfc = [](urma_jfc_t* j) { return active_->DeleteJfc(j); };
        return f;
    }
    static inline PeerFailureSessionTest* active_ = nullptr;
    urma_context_t ctx_{};
    urma_device_cap_t cap_{};
    std::array<urma_jfc_t, 2> cqs_{};
    std::size_t cq_index_ = 0;
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_seg_t segment_{};
    urma_target_jetty_t target_{};
    std::deque<urma_cr_t> completions_;
    urma_cr_status_t completion_status_ = URMA_CR_SUCCESS;
};

TEST_F(PeerFailureSessionTest, BaselineThenFaultErrorsRetireWithoutModifyingSq) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    PeerFailureSession session;
    ASSERT_TRUE(session.Open(&ctx_, cap_, {}, false));
    ASSERT_TRUE(session.Import({}));
    ASSERT_TRUE(session.Send(1, true));
    // 观察工具必须完整收集错误 CQE；不能像生产 completion processor 一样立即切 ERROR。
    completion_status_ = URMA_CR_ACK_TIMEOUT_ERR;
    ASSERT_TRUE(session.Send(32, false));
    EXPECT_EQ(session.tx().accepted, 32u);
    EXPECT_EQ(session.tx().retired, 32u);
    EXPECT_EQ(session.tx().statuses[URMA_CR_ACK_TIMEOUT_ERR], 32u);
    auto state = session.QueryState();
    ASSERT_TRUE(state);
    EXPECT_EQ(*state, URMA_JETTY_STATE_READY);
    ASSERT_TRUE(session.Close());
}

TEST_F(PeerFailureSessionTest, ProviderPartialPrefixDoesNotReplayRejectedSuffix) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    PeerFailureSession session;
    ASSERT_TRUE(session.Open(&ctx_, cap_, {}, false));
    ASSERT_TRUE(session.Import({}));
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([&](urma_jetty_t*, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
        // 仅第一个 WR 被接受，后缀只取消预留，不重新 post。
        *bad = wr->next;
        urma_cr_t cr{};
        cr.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
        cr.flag.bs.jetty = 1;
        cr.local_id = 17;
        cr.user_ctx = wr->user_ctx;
        completions_.push_back(cr);
        return URMA_EAGAIN;
    });
    ASSERT_TRUE(session.Send(32, false));
    EXPECT_EQ(session.tx().accepted, 1u);
    EXPECT_EQ(session.tx().retired, 1u);
    EXPECT_EQ(session.tx().post_status, URMA_EAGAIN);
    EXPECT_TRUE(session.Close());
}

TEST_F(PeerFailureSessionTest, MissingTerminalCompletionRetainsWholeDmaChain) {
    // 缺少 CQE 时不能注销 MR、unimport 或删除队列；测试也使用 NoDestructor 保留依赖。
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    NoDestructor<PeerFailureSession> session;
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, Unimport(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([](urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t** bad) {
        *bad = nullptr;
        return URMA_SUCCESS;
    });
    ASSERT_TRUE(session->Open(&ctx_, cap_, {.batch = 1, .timeout_ms = 1}, false));
    ASSERT_TRUE(session->Import({}));
    auto sent = session->Send(1, false);
    ASSERT_FALSE(sent);
    EXPECT_EQ(sent.error().code, ETIMEDOUT);
    auto closed = session->Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, EBUSY);
}

TEST_F(PeerFailureSessionTest, ExternalFaultPostsReceiveCapacityOnlyAfterBaselineValidation) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    NoDestructor<PeerFailureSession> session;
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, PostRecv(_, _, _))
        .WillOnce([&](urma_jfr_t*, urma_jfr_wr_t* wr, urma_jfr_wr_t** bad) {
            // baseline 只允许一条 RX；复制 payload 并给出真实接收形状的 CQE。
            EXPECT_EQ(wr->next, nullptr);
            *bad = nullptr;
            std::fill_n(reinterpret_cast<std::byte*>(wr->src.sge[0].addr), 4096, std::byte{0x5a});
            urma_cr_t cr{};
            cr.status = URMA_CR_SUCCESS;
            cr.opcode = URMA_CR_OPC_SEND;
            cr.flag.bs.s_r = 1;
            cr.flag.bs.jetty = 1;
            cr.local_id = 17;
            cr.user_ctx = wr->user_ctx;
            cr.completion_len = 4096;
            completions_.push_back(cr);
            return URMA_SUCCESS;
        })
        .WillOnce([](urma_jfr_t*, urma_jfr_wr_t* wr, urma_jfr_wr_t** bad) {
            // 外部故障窗口之前准备足量 RX，防止无故障对照被 RX 饥饿污染。
            *bad = nullptr;
            std::size_t count = 0;
            while (wr) {
                ++count;
                wr = wr->next;
            }
            EXPECT_EQ(count, 32u);
            return URMA_SUCCESS;
        });
    ASSERT_TRUE(session->Open(&ctx_, cap_, {.fault = PeerFault::kExternal}, true));
    EXPECT_FALSE(session->PrepareProbeReceives());
    ASSERT_TRUE(session->ReceiveBaseline());
    ASSERT_TRUE(session->PrepareProbeReceives());
    EXPECT_FALSE(session->Close());
}

TEST_F(PeerFailureSessionTest, PseudoCompletionCannotRetireAcceptedWr) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    NoDestructor<PeerFailureSession> session;
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, Unimport(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    ASSERT_TRUE(session->Open(&ctx_, cap_, {.batch = 1}, false));
    ASSERT_TRUE(session->Import({}));
    // FLUSH_ERR_DONE 不具有合法 user_ctx，即使伪造为账本 id 也不能当成普通终结。
    completion_status_ = URMA_CR_WR_FLUSH_ERR_DONE;
    EXPECT_FALSE(session->Send(1, false));
    EXPECT_EQ(session->tx().accepted, 1u);
    EXPECT_EQ(session->tx().retired, 0u);
    EXPECT_FALSE(session->Close());
}

TEST_F(PeerFailureSessionTest, SplitSubmissionKeepsUnretiredWrAcrossExitConfirmationWindow) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    PeerFailureSession session;
    ASSERT_TRUE(session.Open(&ctx_, cap_, {.fault = PeerFault::kInflightExit}, false));
    ASSERT_TRUE(session.Import({}));
    ASSERT_TRUE(session.Submit(32));
    EXPECT_EQ(session.tx().accepted, 32u);
    EXPECT_EQ(session.tx().retired, 0u);
    EXPECT_FALSE(session.Close());
    // 模拟退出请求前只消费 8 条 CQE；其余已排队 CQE 仍不能因“软件未退休”被称为硬件在途。
    EXPECT_CALL(*this, Poll(_, _, _))
        .WillOnce([&](urma_jfc_t*, int, urma_cr_t* cr) {
            for (int i = 0; i < 8; ++i) {
                cr[i] = completions_.front();
                completions_.pop_front();
            }
            return 8;
        })
        .RetiresOnSaturation();
    ASSERT_TRUE(session.PollCompletions());
    EXPECT_EQ(session.tx().retired, 8u);
    EXPECT_FALSE(session.Close());
    ASSERT_TRUE(session.WaitForTerminal());
    EXPECT_EQ(session.tx().retired, 32u);
    EXPECT_EQ(session.tx().statuses[URMA_CR_SUCCESS], 32u);
    EXPECT_TRUE(session.Close());
}

TEST_F(PeerFailureSessionTest, UnknownAcceptanceNeverClaimsSafeTerminality) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    NoDestructor<PeerFailureSession> session;
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, Unimport(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    ASSERT_TRUE(session->Open(&ctx_, cap_, {.batch = 1}, false));
    ASSERT_TRUE(session->Import({}));
    // bad_wr 不能界定接受前缀：TxSender 的契约失败隔离仍生效，工具不能把这次结果当普通 post 拒绝。
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([](urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t** bad) {
        *bad = nullptr;
        return URMA_EAGAIN;
    });
    EXPECT_CALL(*this, Modify(_, _)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_FALSE(session->Submit(1));
    EXPECT_FALSE(session->tx().accepted_known);
    EXPECT_FALSE(session->WaitForTerminal());
    EXPECT_FALSE(session->Close());
}
} // namespace
} // namespace tools
} // namespace kbsocket
