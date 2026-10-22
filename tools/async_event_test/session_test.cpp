// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>

#include "tools/async_event_test/async_event_test.hpp"
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

class AsyncSessionTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, ModifyJfr, (urma_jfr_t*, urma_jfr_attr_t*));
    MOCK_METHOD(urma_status_t, QueryJfr, (urma_jfr_t*, urma_jfr_cfg_t*, urma_jfr_attr_t*));
    MOCK_METHOD(urma_status_t, ModifyJetty, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Get, (urma_context_t*, urma_async_event_t*));
    MOCK_METHOD(void, Ack, (urma_async_event_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));

protected:
    struct Buffer {
        urma_sge_t sge;
        std::uint64_t id;
    };
    struct Node {
        urma_context_t ctx{};
        std::array<int, 2> pipe{-1, -1};
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetties{};
        urma_target_seg_t segment{};
        urma_target_jetty_t target{};
        std::uint32_t threshold = 0;
        std::size_t created_cqs = 0;
        std::size_t created_jetties = 0;
        std::size_t deleted_jetties = 0;
        std::size_t threshold_updates = 0;
        std::size_t crossings = 0;
        std::size_t posted = 0;
        std::size_t acknowledged = 0;
        bool deleted_jfr = false;
        bool unregistered = false;
        std::deque<Buffer> buffers;
        std::deque<urma_async_event_t> events;
        std::array<std::deque<urma_cr_t>, 2> completions;
    };

    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f;
        f.create_jfc = [](auto* ctx, auto* cfg) { return current_->CreateJfc(ctx, cfg); };
        f.create_jfr = [](auto* ctx, auto* cfg) { return current_->CreateJfr(ctx, cfg); };
        f.create_jetty = [](auto* ctx, auto* cfg) { return current_->CreateJetty(ctx, cfg); };
        f.register_seg = [](auto* ctx, auto* cfg) { return current_->Register(ctx, cfg); };
        f.import_jetty = [](auto* ctx, auto* cfg, auto* token) { return current_->Import(ctx, cfg, token); };
        f.modify_jfr = [](auto* jfr, auto* attr) { return current_->ModifyJfr(jfr, attr); };
        f.query_jfr = [](auto* jfr, auto* cfg, auto* attr) { return current_->QueryJfr(jfr, cfg, attr); };
        f.modify_jetty = [](auto* jetty, auto* attr) { return current_->ModifyJetty(jetty, attr); };
        f.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) { return current_->PostRecv(jfr, wr, bad); };
        f.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) { return current_->PostSend(jetty, wr, bad); };
        f.poll_jfc = [](auto* cq, int count, auto* cr) { return current_->Poll(cq, count, cr); };
        f.get_async_event = [](auto* ctx, auto* event) { return current_->Get(ctx, event); };
        f.ack_async_event = [](auto* event) { current_->Ack(event); };
        f.delete_jetty = [](auto* jetty) { return current_->DeleteJetty(jetty); };
        f.delete_jfr = [](auto* jfr) { return current_->DeleteJfr(jfr); };
        f.delete_jfc = [](auto* cq) { return current_->DeleteJfc(cq); };
        f.unregister_seg = [](auto* seg) { return current_->Unregister(seg); };
        f.unimport_jetty = [](auto* jetty) { return current_->Unimport(jetty); };
        return f;
    }

    Node& ByContext(const urma_context_t* ctx) {
        return nodes_[ctx == &nodes_[0].ctx ? 0 : 1];
    }

    void SetUp() override {
        current_ = this;
        for (auto& node : nodes_) {
            ASSERT_EQ(::pipe(node.pipe.data()), 0);
            node.ctx.async_fd = node.pipe[0];
        }
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 4;
        cap_.max_jfs_depth = cap_.max_jfr_depth = 256;
        cap_.max_jfc_depth = 1024;
        cap_.max_msg_size = 4096;
        cap_.max_jfs_sge = cap_.max_jfr_sge = cap_.max_jfs_rsge = 1;
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            auto& cq = node.cqs[node.created_cqs++];
            cq.urma_ctx = ctx;
            cq.jfc_cfg = *cfg;
            return &cq;
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            node.jfr.urma_ctx = ctx;
            node.jfr.jfr_cfg = *cfg;
            node.jfr.jfr_id.id = ctx == &nodes_[0].ctx ? 100 : 200;
            return &node.jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            auto& jetty = node.jetties.at(node.created_jetties++);
            jetty.urma_ctx = ctx;
            jetty.jetty_cfg = *cfg;
            jetty.jetty_id.id = node.jfr.jfr_id.id + node.created_jetties;
            jetty.jetty_id.eid.raw[15] = ctx == &nodes_[0].ctx ? 1 : 2;
            return &jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto*) {
            return &ByContext(ctx).segment;
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto*, auto*) {
            return &ByContext(ctx).target;
        });
        EXPECT_CALL(*this, ModifyJfr(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* jfr, auto* attr) {
            auto& node = ByContext(jfr->urma_ctx);
            // 配置水线时 RX 已投满，而且不应夹带状态切换或在重复观察前暗中 rearm。
            EXPECT_EQ(attr->mask, JFR_RX_THRESHOLD);
            if (node.threshold_updates == 0) {
                EXPECT_EQ(node.buffers.size(), node.jfr.jfr_cfg.depth);
                EXPECT_EQ(attr->rx_threshold, 64U);
            } else {
                EXPECT_EQ(node.threshold_updates, 1U);
                EXPECT_EQ(node.buffers.size(), 63U);
                EXPECT_EQ(attr->rx_threshold, 0U);
            }
            ++node.threshold_updates;
            node.threshold = attr->rx_threshold;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, QueryJfr(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* cfg, auto* attr) {
                *cfg = jfr->jfr_cfg;
                attr->rx_threshold = ByContext(jfr->urma_ctx).threshold;
                attr->state = URMA_JFR_STATE_READY;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, PostRecv(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* wr, auto** bad) {
                std::lock_guard lock(mutex_);
                auto& node = ByContext(jfr->urma_ctx);
                for (; wr; wr = wr->next) {
                    EXPECT_EQ(std::count_if(node.buffers.begin(), node.buffers.end(),
                                            [&](const auto& entry) { return entry.sge.addr == wr->src.sge[0].addr; }),
                              0);
                    node.buffers.push_back({wr->src.sge[0], wr->user_ctx});
                    ++node.posted;
                }
                *bad = nullptr;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, PostSend(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jetty, auto* wr, auto** bad) {
                std::lock_guard lock(mutex_);
                auto& sender = ByContext(jetty->urma_ctx);
                auto& receiver = nodes_[0];
                for (; wr; wr = wr->next) {
                    // 反序消耗 RX 槽位，防止工具把 buffer 的 user_ctx 当作 payload 序号。
                    EXPECT_FALSE(receiver.buffers.empty());
                    if (receiver.buffers.empty()) {
                        return URMA_EAGAIN;
                    }
                    const auto buffer = receiver.buffers.back();
                    receiver.buffers.pop_back();
                    const auto length = wr->send.src.sge[0].len;
                    std::memcpy(reinterpret_cast<void*>(buffer.sge.addr),
                                reinterpret_cast<void*>(wr->send.src.sge[0].addr), length);
                    urma_cr_t rx{};
                    rx.status = URMA_CR_SUCCESS;
                    rx.flag.bs.s_r = 1;
                    rx.local_id = receiver.jfr.jfr_id.id;
                    rx.user_ctx = buffer.id;
                    rx.completion_len = length;
                    rx.remote_id = jetty->jetty_id;
                    receiver.completions[1].push_back(rx);
                    if (duplicate_rx_ && sent_ == 0) {
                        receiver.completions[1].push_back(rx);
                    }
                    urma_cr_t tx{};
                    tx.status = URMA_CR_SUCCESS;
                    if (sent_ == 0) {
                        tx.status = first_tx_status_;
                    }
                    tx.flag.bs.jetty = 1;
                    tx.local_id = jetty->jetty_id.id;
                    tx.user_ctx = wr->user_ctx;
                    sender.completions[0].push_back(tx);
                    if (wrong_tx_ && sent_ == 0) {
                        ++sender.completions[0].back().local_id;
                    }
                    ++sent_;
                    if (receiver.buffers.size() + 1 == receiver.threshold) {
                        ++receiver.crossings;
                        if (emit_first_ && (receiver.crossings == 1 || emit_repeat_)) {
                            urma_async_event_t event{};
                            event.urma_ctx = &receiver.ctx;
                            event.element.jfr = &receiver.jfr;
                            event.event_type = URMA_EVENT_JFR_LIMIT;
                            receiver.events.push_back(event);
                            const char byte = 'a';
                            EXPECT_EQ(::write(receiver.pipe[1], &byte, 1), 1);
                        }
                    }
                }
                *bad = nullptr;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* cq, int count, auto* output) {
            std::lock_guard lock(mutex_);
            auto& node = ByContext(cq->urma_ctx);
            auto& completions = node.completions[cq == &node.cqs[0] ? 0 : 1];
            int returned = 0;
            while (!completions.empty() && returned < count) {
                output[returned++] = completions.front();
                completions.pop_front();
            }
            return returned;
        });
        EXPECT_CALL(*this, Get(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* event) {
            std::lock_guard lock(mutex_);
            auto& node = ByContext(ctx);
            if (node.events.empty()) {
                return URMA_EAGAIN;
            }
            char byte;
            EXPECT_EQ(::read(node.pipe[0], &byte, 1), 1);
            *event = node.events.front();
            node.events.pop_front();
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Ack(_)).Times(AnyNumber()).WillRepeatedly([this](auto* event) {
            std::lock_guard lock(mutex_);
            auto& node = ByContext(event->urma_ctx);
            EXPECT_EQ(event->element.jfr, &node.jfr);
            EXPECT_EQ(event->event_type, URMA_EVENT_JFR_LIMIT);
            ++node.acknowledged;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jetty) {
            ++ByContext(jetty->urma_ctx).deleted_jetties;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jfr) {
            auto& node = ByContext(jfr->urma_ctx);
            EXPECT_EQ(node.deleted_jetties, 2U);
            node.deleted_jfr = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([this](auto* segment) {
            auto& node = nodes_[segment == &nodes_[0].segment ? 0 : 1];
            EXPECT_TRUE(node.buffers.empty());
            node.unregistered = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
    }

    void TearDown() override {
        for (auto& node : nodes_) {
            ::close(node.pipe[0]);
            ::close(node.pipe[1]);
        }
        current_ = nullptr;
    }

    static inline AsyncSessionTest* current_ = nullptr;
    std::array<Node, 2> nodes_;
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    bool emit_first_ = true;
    bool emit_repeat_ = true;
    bool duplicate_rx_ = false;
    bool wrong_tx_ = false;
    urma_cr_status_t first_tx_status_ = URMA_CR_SUCCESS;
    std::size_t sent_ = 0;
    // 故障测试不析构生产池或注册内存，不能用测试清理掩盖真实 DMA 生命周期问题。
    NoDestructor<JfrLimitSession> receiver_;
    NoDestructor<JfrLimitSession> sender_;

    void OpenBoth(JfrLimitOptions options = {.bytes = 64, .observe_ms = 1, .timeout_ms = 200}) {
        ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, options, true));
        ASSERT_TRUE(sender_->Open(&nodes_[1].ctx, cap_, options, false));
    }

    void RunBoth() {
        std::array<int, 2> sockets;
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
        ControlChannel left(sockets[0]), right(sockets[1]);
        std::jthread receiver([&] { receiver_result_ = receiver_->Run(left); });
        sender_result_ = sender_->Run(right);
        receiver.join();
    }
    std::expected<void, ToolError> receiver_result_;
    std::expected<void, ToolError> sender_result_;
};

TEST_F(AsyncSessionTest, CrossesWatermarkRefillsAndClosesAfterEveryRxIsConsumed) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenBoth();
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_TRUE(receiver_->stats().first_observed);
    EXPECT_TRUE(receiver_->stats().repeat_observed);
    EXPECT_EQ(receiver_->stats().posted, 194U);
    EXPECT_EQ(receiver_->stats().received, 194U);
    EXPECT_EQ(sender_->stats().accepted, 194U);
    EXPECT_EQ(sender_->stats().retired, 194U);
    EXPECT_EQ(nodes_[0].threshold_updates, 2U);
    EXPECT_EQ(nodes_[0].crossings, 2U);
    EXPECT_EQ(nodes_[0].acknowledged, 2U);
    EXPECT_TRUE(nodes_[0].buffers.empty());
    // 重复跨线前没有重新 Modify64；成功只说明本次 provider 自动再次给出了水线事件。
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
    EXPECT_EQ(nodes_[0].deleted_jetties, 2U);
    EXPECT_TRUE(nodes_[0].deleted_jfr);
    EXPECT_TRUE(nodes_[0].unregistered);
}

TEST_F(AsyncSessionTest, MissingRepeatEventRemainsSuccessfulObservation) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    emit_repeat_ = false;
    OpenBoth();
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_TRUE(receiver_->stats().first_observed);
    EXPECT_FALSE(receiver_->stats().repeat_observed);
    EXPECT_EQ(nodes_[0].acknowledged, 1U);
    EXPECT_EQ(receiver_->stats().received, 194U);
    EXPECT_EQ(sender_->stats().retired, 194U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, LargerJfrCrossingFitsFixedSendBatchAndConsumesCleanupRemainder) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    // 深度 256 时每次跨线发送 193 个 WR，覆盖超过一次 CQ poll 的路径和最后 63 个 cleanup RX。
    OpenBoth({.rx_depth = 256, .bytes = 64, .observe_ms = 1, .timeout_ms = 200});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(receiver_->stats().posted, 450U);
    EXPECT_EQ(receiver_->stats().received, 450U);
    EXPECT_EQ(sender_->stats().accepted, 450U);
    EXPECT_EQ(sender_->stats().retired, 450U);
    EXPECT_EQ(nodes_[0].crossings, 2U);
    EXPECT_TRUE(nodes_[0].buffers.empty());
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, MissingFirstEventFailsWithoutRefillOrResourceRelease) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    emit_first_ = false;
    OpenBoth();
    RunBoth();
    ASSERT_FALSE(receiver_result_);
    EXPECT_FALSE(sender_result_);
    EXPECT_FALSE(receiver_->stats().first_observed);
    EXPECT_EQ(receiver_->stats().posted, 128U);
    EXPECT_EQ(receiver_->stats().received, 65U);
    EXPECT_EQ(nodes_[0].threshold_updates, 1U);
    // 无事件不等于 DMA 已终结：对端控制阶段尚未达成一致，保留所有依赖。
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

TEST_F(AsyncSessionTest, DuplicateRxCompletionCannotReleaseOrRepostSameBufferTwice) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    duplicate_rx_ = true;
    OpenBoth();
    RunBoth();
    EXPECT_FALSE(receiver_result_);
    EXPECT_FALSE(sender_result_);
    EXPECT_EQ(nodes_[0].posted, 128U);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

TEST_F(AsyncSessionTest, WrongTxIdentityCannotBecomeSuccessfulTransfer) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    wrong_tx_ = true;
    OpenBoth();
    RunBoth();
    EXPECT_FALSE(sender_result_);
    EXPECT_FALSE(receiver_result_);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

TEST_F(AsyncSessionTest, RemoteAbortStillCountsEveryRecordRetiredInThePolledBatch) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    first_tx_status_ = URMA_CR_REM_ACCESS_ABORT_ERR;
    OpenBoth();
    EXPECT_CALL(*this, ModifyJetty(_, _)).Times(0);
    RunBoth();
    ASSERT_FALSE(sender_result_);
    EXPECT_EQ(sender_result_.error().code, URMA_CR_REM_ACCESS_ABORT_ERR);
    EXPECT_FALSE(receiver_result_);
    // 首个 status 8 后还有 63 条成功 CQE，processor.Poll 已退休整批，工具不得因首错漏掉统计。
    EXPECT_EQ(sender_->stats().accepted, 65U);
    EXPECT_EQ(sender_->stats().retired, 64U);
    EXPECT_EQ(nodes_[1].completions[0].size(), 1U);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(sender_->Close());
    EXPECT_FALSE(receiver_->Close());
}

TEST_F(AsyncSessionTest, DrainFailureAlongsideRetiredRecordDoesNotHideTheRestOfTheBatch) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    first_tx_status_ = URMA_CR_ACK_TIMEOUT_ERR;
    OpenBoth();
    // BeginDrain 失败不否定该 CQE 的 WR 终结证据：event 可同时有 error 和已退休 record。
    EXPECT_CALL(*this, ModifyJetty(&nodes_[1].jetties[0], _)).WillOnce(Return(URMA_EINVAL));
    RunBoth();
    ASSERT_FALSE(sender_result_);
    EXPECT_EQ(sender_result_.error().code, URMA_CR_ACK_TIMEOUT_ERR);
    EXPECT_FALSE(receiver_result_);
    EXPECT_EQ(sender_->stats().accepted, 65U);
    EXPECT_EQ(sender_->stats().retired, 64U);
    EXPECT_EQ(nodes_[1].completions[0].size(), 1U);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(sender_->Close());
    EXPECT_FALSE(receiver_->Close());
}

TEST_F(AsyncSessionTest, PartialReceivePostRetainsAcceptedPrefixAndMr) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenBoth();
    EXPECT_CALL(*this, PostRecv(_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = wr->next;
        return URMA_EAGAIN;
    });
    RunBoth();
    EXPECT_FALSE(receiver_result_);
    EXPECT_FALSE(sender_result_);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ThresholdQueryMismatchStopsBeforeSend) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenBoth();
    EXPECT_CALL(*this, QueryJfr(&nodes_[0].jfr, _, _)).WillOnce([](auto* jfr, auto* cfg, auto* attr) {
        *cfg = jfr->jfr_cfg;
        attr->rx_threshold = 0;
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    RunBoth();
    EXPECT_FALSE(receiver_result_);
    EXPECT_FALSE(sender_result_);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

} // namespace
} // namespace tools
} // namespace kbsocket
