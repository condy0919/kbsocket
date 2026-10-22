// SPDX-License-Identifier: MulanPSL-2.0
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

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
        std::size_t queries = 0;
        std::size_t crossings = 0;
        std::size_t posted = 0;
        std::size_t acknowledged = 0;
        bool deleted_jfr = false;
        bool unregistered = false;
        std::deque<Buffer> buffers;
        std::deque<urma_async_event_t> events;
        std::vector<std::uint32_t> configured_thresholds;
        std::vector<std::size_t> configured_occupancies;
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

    void EmitLimitEvent(Node& node) {
        urma_async_event_t event{};
        event.urma_ctx = &node.ctx;
        event.element.jfr = &node.jfr;
        event.event_type = URMA_EVENT_JFR_LIMIT;
        node.events.push_back(event);
        const char byte = 'a';
        EXPECT_EQ(::write(node.pipe[1], &byte, 1), 1);
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
            std::lock_guard lock(mutex_);
            auto& node = ByContext(jfr->urma_ctx);
            // 每次重配必须先补满 RX，最后一次清理关水线时才允许保留 63 个 posted 槽位。
            EXPECT_EQ(attr->mask, JFR_RX_THRESHOLD);
            if (concurrent_ && attr->rx_threshold == 64 && node.threshold_updates) {
                EXPECT_LT(node.buffers.size(), node.jfr.jfr_cfg.depth);
            } else if (attr->rx_threshold == 0 && node.buffers.size() != node.jfr.jfr_cfg.depth) {
                EXPECT_EQ(node.buffers.size(), 63U);
            } else {
                EXPECT_EQ(node.buffers.size(), node.jfr.jfr_cfg.depth);
            }
            node.configured_thresholds.push_back(attr->rx_threshold);
            node.configured_occupancies.push_back(node.buffers.size());
            ++node.threshold_updates;
            if (fail_modify_call_ == node.threshold_updates) {
                return URMA_EINVAL;
            }
            node.threshold = attr->rx_threshold;
            if (emit_below_rearm_event_ && node.threshold == 64 && node.buffers.size() < 64) {
                EmitLimitEvent(node);
                node.threshold = 0;
            }
            if (emit_config_event_ && node.threshold_updates > 1 && node.buffers.size() == node.jfr.jfr_cfg.depth) {
                EmitLimitEvent(node);
            }
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, QueryJfr(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* cfg, auto* attr) {
                std::lock_guard lock(mutex_);
                auto& node = ByContext(jfr->urma_ctx);
                ++node.queries;
                if (fail_query_call_ == node.queries) {
                    return URMA_FAIL;
                }
                *cfg = jfr->jfr_cfg;
                attr->rx_threshold = node.threshold;
                attr->state = URMA_JFR_STATE_READY;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, PostRecv(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* wr, auto** bad) {
                std::lock_guard lock(mutex_);
                auto& node = ByContext(jfr->urma_ctx);
                const auto before = node.buffers.size();
                for (; wr; wr = wr->next) {
                    EXPECT_EQ(std::count_if(node.buffers.begin(), node.buffers.end(),
                                            [&](const auto& entry) { return entry.sge.addr == wr->src.sge[0].addr; }),
                              0);
                    node.buffers.push_back({wr->src.sge[0], wr->user_ctx});
                    ++node.posted;
                }
                if (emit_refill_event_ && before == 63) {
                    EmitLimitEvent(node);
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
                    std::uint32_t payload_sequence;
                    std::memcpy(&payload_sequence, reinterpret_cast<void*>(wr->send.src.sge[0].addr + 4),
                                sizeof(payload_sequence));
                    // 包身份在跨线、恢复流量和 cleanup 之间也必须单调递增，不能每轮重新从零编号。
                    EXPECT_EQ(::ntohl(payload_sequence), sent_);
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
                    if (receiver.buffers.size() + 1 == 64) {
                        ++receiver.crossings;
                        if (receiver.threshold == 64 && emit_first_ && (receiver.crossings == 1 || emit_repeat_)) {
                            EmitLimitEvent(receiver);
                        }
                        if (clear_threshold_after_crossing_) {
                            receiver.threshold = 0;
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
            // 模拟接收线程调度延迟：第二轮 32 条已经发完才暴露 RX CQE。
            if (hold_active_rx_ && cq == &nodes_[0].cqs[1] && receiver_->stats().received >= 65 && sent_ < 97) {
                return 0;
            }
            int returned = 0;
            while (!completions.empty() && returned < count) {
                output[returned++] = completions.front();
                completions.pop_front();
            }
            if (corrupt_first_rx_ && cq == &nodes_[0].cqs[1] && returned) {
                // 保留合法 RX user_ctx，让 RX 池正常交出 lease，只破坏报文长度校验。
                output[0].completion_len = 1;
                corrupt_first_rx_ = false;
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
    bool hold_active_rx_ = false;
    bool concurrent_ = false;
    bool emit_below_rearm_event_ = false;
    bool emit_first_ = true;
    bool emit_repeat_ = true;
    bool emit_config_event_ = false;
    bool emit_refill_event_ = false;
    bool clear_threshold_after_crossing_ = false;
    bool duplicate_rx_ = false;
    bool corrupt_first_rx_ = false;
    bool wrong_tx_ = false;
    urma_cr_status_t first_tx_status_ = URMA_CR_SUCCESS;
    std::size_t sent_ = 0;
    std::size_t fail_modify_call_ = 0;
    std::size_t fail_query_call_ = 0;
    // 故障测试不析构生产池或注册内存，不能用测试清理掩盖真实 DMA 生命周期问题。
    NoDestructor<JfrLimitSession> receiver_;
    NoDestructor<JfrLimitSession> sender_;

    void OpenBoth(JfrLimitOptions options = {.bytes = 64, .observe_ms = 1, .timeout_ms = 200}) {
        concurrent_ = options.traffic == JfrLimitTraffic::kConcurrent;
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

TEST_F(AsyncSessionTest, NoneContinuesFiveCrossingsWhenHardwareClearsTheThresholdAfterFirstEvent) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    clear_threshold_after_crossing_ = true;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 5});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 0}));
    EXPECT_EQ(nodes_[0].crossings, 5U);
    EXPECT_EQ(nodes_[0].acknowledged, 1U);
    EXPECT_EQ(receiver_->stats().received, 392U);
    EXPECT_EQ(sender_->stats().retired, 392U);
    ASSERT_EQ(receiver_->stats().rounds.size(), 5U);
    ASSERT_EQ(sender_->stats().rounds.size(), 5U);
    for (std::size_t i = 0; i < 5; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        // 后续查询到硬件清零只记录为观测，不自动重配、更不把未观察到事件当作流量失败。
        EXPECT_TRUE(round.crossing_completed);
        EXPECT_EQ(round.observed, i == 0);
        EXPECT_EQ(sender_->stats().rounds[i].observed, i == 0);
        EXPECT_TRUE(round.before_rearm.queried);
        EXPECT_EQ(round.before_rearm.threshold, 0U);
        EXPECT_TRUE(round.after_rearm.queried);
        EXPECT_EQ(round.after_rearm.threshold, i == 0 ? 64U : 0U);
        EXPECT_TRUE(round.after_crossing.queried);
        EXPECT_EQ(round.after_crossing.threshold, 0U);
        EXPECT_EQ(round.crossing_events.limit_events, i == 0 ? 1U : 0U);
        EXPECT_EQ(round.received, i == 0 ? 65U : 66U);
        EXPECT_EQ(sender_->stats().rounds[i].retired, i == 0 ? 65U : 66U);
    }
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, RewriteKeepsConfigurationEventsOutsideFourCrossingObservations) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    emit_repeat_ = false;
    emit_config_event_ = true;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 4, .rearm = JfrLimitRearm::kRewrite});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 64, 64, 64, 0}));
    EXPECT_EQ(nodes_[0].configured_occupancies, (std::vector<std::size_t>{128, 128, 128, 128, 63}));
    EXPECT_EQ(nodes_[0].acknowledged, 4U);
    EXPECT_EQ(receiver_->stats().received, 326U);
    EXPECT_EQ(sender_->stats().retired, 326U);
    ASSERT_EQ(receiver_->stats().rounds.size(), 4U);
    for (std::size_t i = 0; i < 4; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        // 模拟每次重写配置都会立即产生事件，但真正后续跨线没有事件，不能误报自动/重臂成功。
        EXPECT_EQ(round.above_water_events.limit_events, i == 0 ? 0U : 1U);
        EXPECT_EQ(round.crossing_events.limit_events, i == 0 ? 1U : 0U);
        EXPECT_EQ(round.observed, i == 0);
        EXPECT_EQ(sender_->stats().rounds[i].observed, i == 0);
        EXPECT_FALSE(round.disabled.queried);
        EXPECT_EQ(round.after_rearm.threshold, 64U);
    }
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, DelayedEventsDuringRefillDoNotTurnLaterCrossingsIntoObservedResults) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    emit_repeat_ = false;
    emit_refill_event_ = true;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 3});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(nodes_[0].acknowledged, 3U);
    ASSERT_EQ(receiver_->stats().rounds.size(), 3U);
    for (std::size_t i = 0; i < 3; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        // 补满期间取到的迟到事件必须记录在 refill 窗口，下一轮跨线仍应显示未观察到。
        EXPECT_EQ(round.refill_events.limit_events, i == 0 ? 0U : 1U);
        EXPECT_EQ(round.above_water_events.limit_events, 0U);
        EXPECT_EQ(round.crossing_events.limit_events, i == 0 ? 1U : 0U);
        EXPECT_EQ(round.observed, i == 0);
        EXPECT_EQ(sender_->stats().rounds[i].observed, i == 0);
    }
    EXPECT_EQ(receiver_->stats().received, 260U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ToggleRecordsDisabledReadbackAndSeparatesConfigurationFromThreeCrossings) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    clear_threshold_after_crossing_ = true;
    emit_config_event_ = true;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 3, .rearm = JfrLimitRearm::kToggle});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 0, 64, 0, 64, 0}));
    EXPECT_EQ(nodes_[0].configured_occupancies, (std::vector<std::size_t>{128, 128, 128, 128, 128, 63}));
    EXPECT_EQ(nodes_[0].acknowledged, 7U);
    EXPECT_EQ(receiver_->stats().received, 260U);
    EXPECT_EQ(sender_->stats().retired, 260U);
    ASSERT_EQ(receiver_->stats().rounds.size(), 3U);
    for (std::size_t i = 0; i < 3; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        EXPECT_EQ(round.disabled.queried, i != 0);
        EXPECT_EQ(round.disabled.threshold, 0U);
        EXPECT_EQ(round.after_rearm.threshold, 64U);
        EXPECT_EQ(round.after_crossing.threshold, 0U);
        EXPECT_EQ(round.above_water_events.limit_events, i == 0 ? 0U : 2U);
        EXPECT_EQ(round.crossing_events.limit_events, 1U);
        EXPECT_TRUE(round.observed);
        EXPECT_TRUE(sender_->stats().rounds[i].observed);
    }
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ConcurrentAboveRewritesDuringBoundedTrafficAndStillCrossesAfterwards) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    clear_threshold_after_crossing_ = true;
    emit_refill_event_ = true;
    // 配置窗口总共只发 32 条：即使 CQE 全部排队，硬件 RX 余量仍至少 96，高于 64。
    OpenBoth({.bytes = 64,
              .observe_ms = 1,
              .timeout_ms = 500,
              .crossings = 3,
              .rearm = JfrLimitRearm::kRewrite,
              .traffic = JfrLimitTraffic::kConcurrent,
              .send_interval_us = 1000});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(receiver_->stats().received, 258U);
    EXPECT_EQ(sender_->stats().retired, 258U);
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 64, 64, 0}));
    for (std::size_t i = 1; i < 3; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        EXPECT_TRUE(round.active_window_completed);
        EXPECT_TRUE(round.active_rearm_succeeded);
        EXPECT_GE(nodes_[0].configured_occupancies[i], 96U);
        EXPECT_LT(nodes_[0].configured_occupancies[i], 128U);
        EXPECT_EQ(round.active_received_before_rearm + round.active_received_after_rearm, 32U);
        EXPECT_GT(round.active_received_before_rearm, 0U);
        EXPECT_EQ(round.refill_events.limit_events, 1U);
        EXPECT_EQ(round.active_rearm_events.limit_events, 0U);
        EXPECT_EQ(round.crossing_events.limit_events, 1U);
        EXPECT_TRUE(round.observed);
    }
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ConcurrentWindowDoesNotInventActivityAfterRearmWhenAllCqesWereAlreadyConsumed) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    hold_active_rx_ = true;
    clear_threshold_after_crossing_ = true;
    OpenBoth({.bytes = 64,
              .observe_ms = 1,
              .timeout_ms = 500,
              .crossings = 2,
              .rearm = JfrLimitRearm::kRewrite,
              .traffic = JfrLimitTraffic::kConcurrent,
              .send_interval_us = 0});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    const auto& round = receiver_->stats().rounds[1];
    // 名称 concurrent 不能充当重叠证据；整批在配置前消费时保留 after=0，打印为 NOT_OBSERVED。
    EXPECT_EQ(round.active_received_before_rearm, 32U);
    EXPECT_EQ(round.active_received_after_rearm, 0U);
    EXPECT_TRUE(round.active_rearm_succeeded);
    EXPECT_TRUE(round.active_window_completed);
    EXPECT_TRUE(round.observed);
    EXPECT_EQ(nodes_[0].configured_occupancies[1], 96U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ConcurrentBelowSeparatesImmediateEventFromUnarmedRecoveryCrossing) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    clear_threshold_after_crossing_ = true;
    emit_below_rearm_event_ = true;
    // 模拟低位写入立即触发并清零；随后补满却不重写，所以再次跨线不应被误标成成功。
    OpenBoth({.bytes = 64,
              .observe_ms = 1,
              .timeout_ms = 500,
              .crossings = 3,
              .rearm = JfrLimitRearm::kRewrite,
              .traffic = JfrLimitTraffic::kConcurrent,
              .rearm_at = JfrLimitRearmAt::kBelow,
              .send_interval_us = 1000});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(receiver_->stats().received, 322U);
    EXPECT_EQ(sender_->stats().retired, 322U);
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 64, 64, 0}));
    EXPECT_EQ(nodes_[0].acknowledged, 3U);
    for (std::size_t i = 1; i < 3; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        EXPECT_GE(nodes_[0].configured_occupancies[i], 31U);
        EXPECT_LT(nodes_[0].configured_occupancies[i], 63U);
        EXPECT_TRUE(round.active_window_completed);
        EXPECT_EQ(round.after_rearm.threshold, 0U);
        EXPECT_EQ(round.active_rearm_events.limit_events, 1U);
        EXPECT_EQ(round.crossing_events.limit_events, 0U);
        EXPECT_FALSE(round.observed);
        EXPECT_FALSE(sender_->stats().rounds[i].observed);
    }
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ConcurrentBelowMayKeepThresholdUntilRefillAndNewCrossing) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    clear_threshold_after_crossing_ = true;
    // 另一种 provider 行为：低位重写不立即触发，补到高位再跨线时才产生下一次事件。
    OpenBoth({.bytes = 64,
              .observe_ms = 1,
              .timeout_ms = 500,
              .crossings = 3,
              .rearm = JfrLimitRearm::kRewrite,
              .traffic = JfrLimitTraffic::kConcurrent,
              .rearm_at = JfrLimitRearmAt::kBelow,
              .send_interval_us = 0});
    RunBoth();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    for (std::size_t i = 1; i < 3; ++i) {
        const auto& round = receiver_->stats().rounds[i];
        EXPECT_EQ(round.after_rearm.threshold, 64U);
        EXPECT_EQ(round.active_rearm_events.limit_events, 0U);
        EXPECT_EQ(round.crossing_events.limit_events, 1U);
        EXPECT_TRUE(round.observed);
    }
    EXPECT_EQ(sender_->stats().retired, 322U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncSessionTest, ConcurrentRearmFailureRetainsTheWholeActiveWindowDependencies) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    fail_modify_call_ = 2;
    OpenBoth({.bytes = 64,
              .observe_ms = 1,
              .timeout_ms = 200,
              .crossings = 3,
              .rearm = JfrLimitRearm::kRewrite,
              .traffic = JfrLimitTraffic::kConcurrent,
              .send_interval_us = 0});
    RunBoth();
    ASSERT_FALSE(receiver_result_);
    EXPECT_EQ(receiver_result_.error().code, URMA_EINVAL);
    EXPECT_FALSE(sender_result_);
    const auto& round = receiver_->stats().rounds[1];
    EXPECT_TRUE(round.active_rearm_attempted);
    EXPECT_FALSE(round.active_rearm_succeeded);
    EXPECT_FALSE(round.active_window_completed);
    // 即便发送端已取得部分/全部 CQE，也没有双方阶段确认，不能安全释放注册内存或队列。
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

TEST_F(AsyncSessionTest, RewriteFailureAfterRefillStopsBeforeNextCrossingAndRetainsDependencies) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    fail_modify_call_ = 2;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 3, .rearm = JfrLimitRearm::kRewrite});
    RunBoth();
    ASSERT_FALSE(receiver_result_);
    EXPECT_EQ(receiver_result_.error().code, URMA_EINVAL);
    EXPECT_FALSE(sender_result_);
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 64}));
    EXPECT_EQ(nodes_[0].buffers.size(), 128U);
    EXPECT_EQ(nodes_[0].crossings, 1U);
    EXPECT_EQ(sender_->stats().retired, 66U);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

TEST_F(AsyncSessionTest, ToggleDisabledQueryFailureCannotProceedToReenableOrNextCrossing) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    // 第一轮 before/after/after-cross 查询后，第二轮 before 为第4次，禁用确认是第5次。
    fail_query_call_ = 5;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 3, .rearm = JfrLimitRearm::kToggle});
    RunBoth();
    ASSERT_FALSE(receiver_result_);
    EXPECT_EQ(receiver_result_.error().code, URMA_FAIL);
    EXPECT_FALSE(sender_result_);
    EXPECT_EQ(nodes_[0].configured_thresholds, (std::vector<std::uint32_t>{64, 0}));
    EXPECT_EQ(nodes_[0].buffers.size(), 128U);
    EXPECT_EQ(nodes_[0].crossings, 1U);
    EXPECT_EQ(sender_->stats().retired, 66U);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}

TEST_F(AsyncSessionTest, PostCrossingQueryFailurePreservesTheAlreadyObservedFirstEvent) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    fail_query_call_ = 3;
    OpenBoth({.bytes = 64, .observe_ms = 1, .timeout_ms = 200, .crossings = 3});
    RunBoth();
    ASSERT_FALSE(receiver_result_);
    EXPECT_EQ(receiver_result_.error().code, URMA_FAIL);
    EXPECT_FALSE(sender_result_);
    // 事件已成功 Get/Ack，后续查询失败不能把现场摘要倒退成“首次未观察到”。
    EXPECT_TRUE(receiver_->stats().first_observed);
    EXPECT_TRUE(receiver_->stats().rounds[0].observed);
    EXPECT_EQ(receiver_->stats().rounds[0].crossing_events.limit_events, 1U);
    EXPECT_FALSE(receiver_->stats().rounds[0].after_crossing.queried);
    EXPECT_EQ(nodes_[0].acknowledged, 1U);
    EXPECT_EQ(nodes_[0].crossings, 1U);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
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

TEST_F(AsyncSessionTest, FirstMalformedRxStillReturnsEveryLeaseAndCountsLaterValidCompletions) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    corrupt_first_rx_ = true;
    OpenBoth();
    RunBoth();
    ASSERT_FALSE(receiver_result_);
    EXPECT_FALSE(sender_result_);
    // 首批 64 条 CQE 中首条长度异常，其余 63 条有效；全部 64 个 lease 均应归还，不能提前退出。
    EXPECT_EQ(receiver_->stats().received, 63U);
    EXPECT_EQ(receiver_->stats().rx_leases_released, 64U);
    EXPECT_EQ(receiver_->stats().rounds[0].received, 63U);
    EXPECT_EQ(nodes_[0].posted, 128U);
    EXPECT_EQ(nodes_[0].completions[1].size(), 1U);
    // 批尾仍必须失败且不进入下一轮补投；未消费 CQE、剩余 posted RX 和 MR 保留到进程退出。
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
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
    EXPECT_CALL(*this, QueryJfr(&nodes_[0].jfr, _, _)).Times(2).WillRepeatedly([](auto* jfr, auto* cfg, auto* attr) {
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
