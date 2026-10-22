// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

#include "tools/async_error_test/async_error_test.hpp"
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

class AsyncErrorTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, ModifyJetty, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, QueryJetty, (urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, QueryJfr, (urma_jfr_t*, urma_jfr_cfg_t*, urma_jfr_attr_t*));
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
        std::array<urma_jfc_t, 4> cqs{};
        std::array<urma_jfr_t, 2> jfrs{};
        std::array<urma_jetty_t, 3> jetties{};
        std::array<urma_target_seg_t, 2> segments{};
        std::array<urma_target_jetty_t, 3> targets{};
        std::array<urma_jetty_id_t, 3> imported_ids{};
        std::array<urma_jetty_state_t, 3> states{};
        std::array<std::deque<Buffer>, 2> buffers;
        std::array<std::deque<urma_cr_t>, 4> completions;
        std::deque<urma_async_event_t> events;
        std::vector<int> deleted;
        std::size_t created_cqs = 0;
        std::size_t created_jfrs = 0;
        std::size_t created_jetties = 0;
        std::size_t registered = 0;
        std::size_t imported = 0;
        std::size_t acked = 0;
        std::size_t fault_posts = 0;
    };

    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f;
        f.create_jfc = [](auto* ctx, auto* cfg) { return current_->CreateJfc(ctx, cfg); };
        f.create_jfr = [](auto* ctx, auto* cfg) { return current_->CreateJfr(ctx, cfg); };
        f.create_jetty = [](auto* ctx, auto* cfg) { return current_->CreateJetty(ctx, cfg); };
        f.register_seg = [](auto* ctx, auto* cfg) { return current_->Register(ctx, cfg); };
        f.import_jetty = [](auto* ctx, auto* cfg, auto* token) { return current_->Import(ctx, cfg, token); };
        f.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) { return current_->PostRecv(jfr, wr, bad); };
        f.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) { return current_->PostSend(jetty, wr, bad); };
        f.poll_jfc = [](auto* cq, int count, auto* cr) { return current_->Poll(cq, count, cr); };
        f.flush_jetty = [](auto* jetty, int count, auto* cr) { return current_->Flush(jetty, count, cr); };
        f.modify_jetty = [](auto* jetty, auto* attr) { return current_->ModifyJetty(jetty, attr); };
        f.query_jetty = [](auto* jetty, auto* cfg, auto* attr) { return current_->QueryJetty(jetty, cfg, attr); };
        f.query_jfr = [](auto* jfr, auto* cfg, auto* attr) { return current_->QueryJfr(jfr, cfg, attr); };
        f.get_async_event = [](auto* ctx, auto* event) { return current_->Get(ctx, event); };
        f.ack_async_event = [](auto* event) { current_->Ack(event); };
        f.delete_jetty = [](auto* jetty) { return current_->DeleteJetty(jetty); };
        f.delete_jfr = [](auto* jfr) { return current_->DeleteJfr(jfr); };
        f.delete_jfc = [](auto* cq) { return current_->DeleteJfc(cq); };
        f.unregister_seg = [](auto* segment) { return current_->Unregister(segment); };
        f.unimport_jetty = [](auto* jetty) { return current_->Unimport(jetty); };
        return f;
    }
    Node& ByContext(const urma_context_t* ctx) {
        return nodes_[ctx == &nodes_[0].ctx ? 0 : 1];
    }
    void Emit(Node& node, urma_async_event_type_t type, bool healthy = false) {
        urma_async_event_t event{};
        event.urma_ctx = &node.ctx;
        event.event_type = type;
        if (type == URMA_EVENT_JFR_ERR) {
            event.element.jfr = &node.jfrs[healthy ? 1 : 0];
        } else {
            event.element.jfc = &node.cqs[healthy ? 3 : fault_tx_event_ ? 0 : 1];
        }
        node.events.push_back(event);
        const char byte = 'e';
        EXPECT_EQ(::write(node.pipe[1], &byte, 1), 1);
    }
    urma_cr_t Completion(AsyncErrorQueue& queue, std::uint64_t id, urma_cr_status_t status = URMA_CR_SUCCESS,
                         std::size_t lane = 0) {
        urma_cr_t cr{};
        cr.flag.bs.jetty = 1;
        cr.local_id = queue.jetties[lane]->jetty_id.id;
        cr.user_ctx = id;
        cr.status = status;
        return cr;
    }
    void Enqueue(AsyncErrorQueue& queue, const urma_cr_t& cr, bool receiver = false) {
        auto* cq = receiver ? queue.rx : queue.tx;
        auto& node = ByContext(cq->urma_ctx);
        node.completions[cq - node.cqs.data()].push_back(cr);
    }

    void SetUp() override {
        current_ = this;
        for (auto& node : nodes_) {
            ASSERT_EQ(::pipe(node.pipe.data()), 0);
            node.ctx.async_fd = node.pipe[0];
            node.states.fill(URMA_JETTY_STATE_READY);
        }
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jfs_depth = cap_.max_jfr_depth = 8192;
        cap_.max_jfc_depth = 16384;
        cap_.max_msg_size = 4096;
        cap_.max_jfs_sge = cap_.max_jfr_sge = 1;
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            auto& cq = node.cqs.at(node.created_cqs++);
            cq.urma_ctx = ctx;
            cq.jfc_cfg = *cfg;
            cq.jfc_id.id = node.created_cqs;
            return &cq;
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            auto& jfr = node.jfrs.at(node.created_jfrs++);
            jfr.urma_ctx = ctx;
            jfr.jfr_cfg = *cfg;
            jfr.jfr_id.id = node.created_jfrs;
            return &jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            auto& jetty = node.jetties.at(node.created_jetties++);
            jetty.urma_ctx = ctx;
            jetty.jetty_cfg = *cfg;
            jetty.jetty_id.id = (ctx == &nodes_[0].ctx ? 100 : 200) + node.created_jetties;
            jetty.jetty_id.eid.raw[15] = ctx == &nodes_[0].ctx ? 1 : 2;
            return &jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto*) {
            auto& node = ByContext(ctx);
            return &node.segments.at(node.registered++);
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg, auto*) {
            auto& node = ByContext(ctx);
            const auto index = node.imported++;
            node.imported_ids.at(index) = cfg->jetty_id;
            return &node.targets.at(index);
        });
        EXPECT_CALL(*this, QueryJfr(_, _, _)).Times(AnyNumber()).WillRepeatedly([](auto* jfr, auto* cfg, auto* attr) {
            *cfg = jfr->jfr_cfg;
            attr->state = URMA_JFR_STATE_READY;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, QueryJetty(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jetty, auto* cfg, auto* attr) {
                auto& node = ByContext(jetty->urma_ctx);
                *cfg = jetty->jetty_cfg;
                attr->state = node.states[jetty - node.jetties.data()];
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, ModifyJetty(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* jetty, auto* attr) {
            EXPECT_EQ(attr->mask, JETTY_STATE);
            EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
            auto& node = ByContext(jetty->urma_ctx);
            node.states[jetty - node.jetties.data()] = attr->state;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostRecv(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* wr, auto** bad) {
                std::lock_guard lock(mutex_);
                auto& node = ByContext(jfr->urma_ctx);
                auto& buffers = node.buffers[jfr - node.jfrs.data()];
                for (; wr; wr = wr->next) {
                    buffers.push_back({wr->src.sge[0], wr->user_ctx});
                }
                *bad = nullptr;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, PostSend(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jetty, auto* wr, auto** bad) {
                std::lock_guard lock(mutex_);
                *bad = nullptr;
                if (!automatic_traffic_) {
                    return URMA_SUCCESS;
                }
                auto& sender = ByContext(jetty->urma_ctx);
                auto& receiver = nodes_[0];
                const auto target_index = wr->tjetty - sender.targets.data();
                const auto id = sender.imported_ids.at(target_index);
                auto found = std::find_if(receiver.jetties.begin(), receiver.jetties.end(),
                                          [&](const auto& endpoint) { return endpoint.jetty_id.id == id.id; });
                if (found == receiver.jetties.end()) {
                    ADD_FAILURE() << "unknown imported jetty";
                    return URMA_EINVAL;
                }
                auto* destination = &*found;
                auto* jfr = destination->jetty_cfg.shared.jfr;
                auto& buffers = receiver.buffers[jfr - receiver.jfrs.data()];
                if (buffers.empty()) {
                    ADD_FAILURE() << "receive credit exhausted";
                    return URMA_EAGAIN;
                }
                const auto buffer = buffers.back();
                buffers.pop_back();
                std::memcpy(reinterpret_cast<void*>(buffer.sge.addr), reinterpret_cast<void*>(wr->send.src.sge[0].addr),
                            wr->send.src.sge[0].len);
                urma_cr_t rx{};
                rx.flag.bs.jetty = 1;
                rx.flag.bs.s_r = 1;
                rx.local_id = destination->jetty_id.id;
                rx.remote_id = jetty->jetty_id;
                rx.user_ctx = buffer.id;
                rx.imm_data = wr->send.imm_data;
                rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                rx.status = URMA_CR_SUCCESS;
                rx.completion_len = wr->send.src.sge[0].len;
                receiver.completions[jfr->jfr_cfg.jfc - receiver.cqs.data()].push_back(rx);
                urma_cr_t tx{};
                tx.flag.bs.jetty = 1;
                tx.local_id = jetty->jetty_id.id;
                tx.user_ctx = wr->user_ctx;
                tx.status = URMA_CR_SUCCESS;
                sender.completions[jetty->jetty_cfg.jfs_cfg.jfc - sender.cqs.data()].push_back(tx);
                // fixture 的 healthy_rounds=2，最后探针使用独立 IMM=4；只在该 WR 投递时产生迟到事件。
                if (after_event_ && jfr == &receiver.jfrs[1] && wr->send.imm_data == 4) {
                    Emit(receiver, URMA_EVENT_JFC_ERR);
                }
                if (jfr == &receiver.jfrs[0]) {
                    ++sender.fault_posts;
                    // 两条 baseline 已完成后才允许注入，避免把基线事件混成被测故障。
                    if (inject_event_ && sender.fault_posts == 3) {
                        Emit(receiver, injected_type_, affect_healthy_);
                    }
                }
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* cq, int count, auto* out) {
            std::lock_guard lock(mutex_);
            auto& node = ByContext(cq->urma_ctx);
            auto& completions = node.completions[cq - node.cqs.data()];
            int copied = 0;
            while (copied < count && !completions.empty()) {
                out[copied++] = completions.front();
                completions.pop_front();
            }
            return copied;
        });
        EXPECT_CALL(*this, Flush(_, _, _)).Times(AnyNumber()).WillRepeatedly(Return(0));
        EXPECT_CALL(*this, Get(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* out) {
            std::lock_guard lock(mutex_);
            auto& node = ByContext(ctx);
            if (node.events.empty()) {
                return URMA_EAGAIN;
            }
            char byte;
            EXPECT_EQ(::read(node.pipe[0], &byte, 1), 1);
            *out = node.events.front();
            node.events.pop_front();
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Ack(_)).Times(AnyNumber()).WillRepeatedly([this](auto* event) {
            ++ByContext(event->urma_ctx).acked;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jetty) {
            ByContext(jetty->urma_ctx).deleted.push_back(1);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jfr) {
            ByContext(jfr->urma_ctx).deleted.push_back(2);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([this](auto* cq) {
            ByContext(cq->urma_ctx).deleted.push_back(3);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([this](auto* segment) {
            for (auto& node : nodes_) {
                if (segment == &node.segments[0] || segment == &node.segments[1]) {
                    node.deleted.push_back(4);
                }
            }
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
    }
    void TearDown() override {
        for (auto& node : nodes_) {
            ::close(node.pipe[0]);
            ::close(node.pipe[1]);
        }
        current_ = nullptr;
    }
    void OpenSender(std::uint32_t count = 2, std::uint32_t jets = 1) {
        ASSERT_TRUE(queue_->Open(&nodes_[1].ctx, cap_, count, count, false, jets));
        std::array<urma_jetty_id_t, 2> ids{};
        ids[0].id = 301;
        ids[1].id = 302;
        ASSERT_TRUE(queue_->Import(std::span(ids).first(jets)));
    }
    void RunSessions(AsyncFault fault = AsyncFault::kJfcOverflow) {
        automatic_traffic_ = true;
        AsyncErrorOptions options{
            .fault = fault, .attempts = 128, .cq_depth = 64, .healthy_rounds = 2, .observe_ms = 2, .timeout_ms = 200};
        ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, options, true));
        ASSERT_TRUE(sender_->Open(&nodes_[1].ctx, cap_, options, false));
        std::array<int, 2> sockets;
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
        ControlChannel left(sockets[0]), right(sockets[1]);
        std::jthread receiver([&] { receiver_result_ = receiver_->Run(left); });
        sender_result_ = sender_->Run(right);
        receiver.join();
    }

    static inline AsyncErrorTest* current_ = nullptr;
    std::array<Node, 2> nodes_;
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    bool automatic_traffic_ = false;
    bool fault_tx_event_ = false;
    bool after_event_ = false;
    bool inject_event_ = false;
    bool affect_healthy_ = false;
    urma_async_event_type_t injected_type_ = URMA_EVENT_JFC_ERR;
    // 失败用例保留生产对象，不通过测试析构掩盖未知 DMA 所有权。
    NoDestructor<AsyncErrorQueue> queue_;
    NoDestructor<AsyncErrorSession> receiver_;
    NoDestructor<AsyncErrorSession> sender_;
    std::expected<void, ToolError> receiver_result_;
    std::expected<void, ToolError> sender_result_;
};

TEST(AsyncErrorOptionsTest, RejectsInvalidInjectionBudgetsBeforeHardwareInitialization) {
    EXPECT_TRUE(ValidateAsyncErrorOptions({}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.fault = static_cast<AsyncFault>(3)}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.attempts = 127}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.attempts = 4098}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.attempts = 129}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.cq_depth = 512}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.healthy_rounds = 1}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.observe_ms = 1}));
    EXPECT_FALSE(ValidateAsyncErrorOptions({.timeout_ms = 99}));
}

TEST_F(AsyncErrorTest, ClassifiesFaultAndHealthyObjectsAndAcknowledgesOriginalEvents) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    auto& node = nodes_[0];
    AsyncErrorObjects objects{
        &node.ctx,    &node.cqs[0], &node.cqs[1],  &node.jfrs[0],   {&node.jetties[0], &node.jetties[1]},
        &node.cqs[2], &node.cqs[3], &node.jfrs[1], &node.jetties[2]};
    AsyncErrorEvents stats;
    urma_async_event_t event{};
    event.urma_ctx = &node.ctx;
    int cookie = 42;
    event.priv = &cookie;
    EXPECT_CALL(*this, Ack(&event)).Times(6).WillRepeatedly([&](auto* original) {
        EXPECT_EQ(original->priv, &cookie);
        EXPECT_EQ(original->urma_ctx, &node.ctx);
    });
    event.event_type = URMA_EVENT_JFC_ERR;
    event.element.jfc = objects.fault_rx;
    ASSERT_TRUE(RecordAsyncErrorEvent(event, objects, stats));
    event.element.jfc = objects.fault_tx;
    ASSERT_TRUE(RecordAsyncErrorEvent(event, objects, stats));
    event.event_type = URMA_EVENT_JFR_ERR;
    event.element.jfr = objects.fault_jfr;
    ASSERT_TRUE(RecordAsyncErrorEvent(event, objects, stats));
    event.event_type = URMA_EVENT_JETTY_ERR;
    event.element.jetty = objects.fault_jetties[1];
    ASSERT_TRUE(RecordAsyncErrorEvent(event, objects, stats));
    event.element.jetty = objects.healthy_jetty;
    ASSERT_TRUE(RecordAsyncErrorEvent(event, objects, stats));
    // 不认识的 union 指针必须只做比较，不能解引用或改写后再 Ack。
    event.event_type = URMA_EVENT_JFC_ERR;
    event.element.jfc = reinterpret_cast<urma_jfc_t*>(static_cast<std::uintptr_t>(1));
    ASSERT_TRUE(RecordAsyncErrorEvent(event, objects, stats));
    EXPECT_EQ(stats.retrieved, 6U);
    EXPECT_EQ(stats.acknowledged, 6U);
    EXPECT_EQ(stats.fault_jfc, 2U);
    EXPECT_EQ(stats.fault_rx_jfc, 1U);
    EXPECT_EQ(stats.fault_tx_jfc, 1U);
    EXPECT_EQ(stats.fault_jfr, 1U);
    EXPECT_EQ(stats.fault_jetty[1], 1U);
    EXPECT_EQ(stats.healthy, 1U);
    EXPECT_EQ(stats.other, 1U);
}

TEST_F(AsyncErrorTest, WrongContextIsAcknowledgedBeforeErrorAndUnknownTypeNeverIndexesHistogram) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    AsyncErrorObjects objects{.ctx = &nodes_[0].ctx};
    AsyncErrorEvents stats;
    urma_async_event_t event{};
    event.urma_ctx = &nodes_[1].ctx;
    event.event_type = static_cast<urma_async_event_type_t>(1000);
    EXPECT_CALL(*this, Ack(&event)).Times(1);
    auto result = RecordAsyncErrorEvent(event, objects, stats);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    EXPECT_EQ(stats.acknowledged, 1U);
    EXPECT_EQ(stats.other, 1U);
    EXPECT_EQ(stats.last_type, 1000);
    EXPECT_TRUE(std::all_of(stats.types.begin(), stats.types.end(), [](auto n) { return n == 0; }));
}

TEST_F(AsyncErrorTest, RejectsUnsupportedQueueBeforeCreatingResources) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    cap_.max_jfs_sge = 0;
    EXPECT_CALL(*this, CreateJfc(_, _)).Times(0);
    EXPECT_FALSE(queue_->Open(&nodes_[0].ctx, cap_, 128, 64, true, 2));
}

TEST_F(AsyncErrorTest, UnknownSendAcceptancePreventsQueueClose) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender();
    EXPECT_CALL(*this, PostSend(_, _, _)).WillOnce([](auto*, auto*, auto** bad) {
        *bad = nullptr;
        return URMA_EAGAIN;
    });
    EXPECT_FALSE(queue_->Post(0, 1));
    EXPECT_FALSE(queue_->stats.accepted_known);
    EXPECT_FALSE(queue_->complete());
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(queue_->Close());
}

TEST_F(AsyncErrorTest, UnknownReceiveAcceptanceRetainsRegisteredMemory) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    EXPECT_CALL(*this, PostRecv(_, _, _)).WillOnce([](auto*, auto*, auto** bad) {
        *bad = nullptr;
        return URMA_EAGAIN;
    });
    EXPECT_FALSE(queue_->Open(&nodes_[0].ctx, cap_, 128, 64, true, 2));
    EXPECT_FALSE(queue_->stats.accepted_known);
    EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(queue_->Close());
}

TEST_F(AsyncErrorTest, HealthyErrorStillAccountsForRemainingTrustedCompletionsInTheBatch) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender();
    ASSERT_TRUE(queue_->Post(0, 2));
    Enqueue(*queue_, Completion(*queue_, 1, URMA_CR_ACK_TIMEOUT_ERR));
    Enqueue(*queue_, Completion(*queue_, 2));
    EXPECT_FALSE(queue_->Poll(true));
    EXPECT_EQ(queue_->stats.retired, 2U);
    EXPECT_EQ(queue_->stats.statuses[URMA_CR_ACK_TIMEOUT_ERR], 1U);
    EXPECT_EQ(queue_->stats.statuses[URMA_CR_SUCCESS], 1U);
    EXPECT_TRUE(queue_->complete());
    ASSERT_TRUE(queue_->Close());
    // DMA 已终结后，先删 jetty/JFR/JFC，最后才注销 MR。
    EXPECT_EQ(nodes_[1].deleted, (std::vector<int>{1, 2, 3, 3, 4}));
}

TEST_F(AsyncErrorTest, FakeBoundaryDoesNotReadUserContextAndFlushWaitsForIt) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender(1);
    ASSERT_TRUE(queue_->Post(0, 1));
    ASSERT_TRUE(queue_->BeginDrain());
    EXPECT_CALL(*this, Flush(_, _, _)).Times(0);
    ASSERT_TRUE(queue_->Flush());
    ::testing::Mock::VerifyAndClearExpectations(this);
    // 边界身份故意设为最大值，不能按普通 WR 索引；它只允许后续 flush。
    auto boundary = Completion(*queue_, std::numeric_limits<std::uint64_t>::max(), URMA_CR_WR_FLUSH_ERR_DONE);
    EXPECT_CALL(*this, Poll(queue_->tx, _, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = boundary;
        return 1;
    });
    ASSERT_TRUE(queue_->Poll(false));
    EXPECT_EQ(queue_->stats.retired, 0U);
    EXPECT_EQ(queue_->stats.flush_done[0], 1U);
    EXPECT_CALL(*this, Flush(queue_->jetties[0], _, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(*queue_, 1, URMA_CR_WR_UNHANDLED);
        return 1;
    });
    ASSERT_TRUE(queue_->Flush());
    EXPECT_EQ(queue_->stats.retired, 1U);
    EXPECT_TRUE(queue_->complete());
    EXPECT_FALSE(queue_->Post(0, 1));
}

TEST_F(AsyncErrorTest, FlushMalformedFirstRecordDoesNotHideLaterTrustedRetirements) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender(2);
    ASSERT_TRUE(queue_->Post(0, 2));
    ASSERT_TRUE(queue_->BeginDrain());
    Enqueue(*queue_, Completion(*queue_, std::numeric_limits<std::uint64_t>::max(), URMA_CR_WR_FLUSH_ERR_DONE));
    ASSERT_TRUE(queue_->Poll(false));
    EXPECT_CALL(*this, Flush(queue_->jetties[0], _, _)).WillOnce([&](auto*, int, auto* out) {
        // flush 一次已经取出整批，首项身份损坏不能导致后面合法 WR 永久漏记。
        out[0] = Completion(*queue_, 0, URMA_CR_WR_UNHANDLED);
        out[1] = Completion(*queue_, 1, URMA_CR_WR_UNHANDLED);
        out[2] = Completion(*queue_, 2, URMA_CR_WR_UNHANDLED);
        return 3;
    });
    EXPECT_FALSE(queue_->Flush());
    EXPECT_EQ(queue_->stats.retired, 2U);
    EXPECT_EQ(queue_->stats.statuses[URMA_CR_WR_UNHANDLED], 2U);
}

TEST_F(AsyncErrorTest, FlushCannotRetireRecordsFromAnotherSqEvenWhenIdentityMatchesThatSq) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender(2, 2);
    ASSERT_TRUE(queue_->Post(0, 2));
    ASSERT_TRUE(queue_->BeginDrain());
    Enqueue(*queue_, Completion(*queue_, 0, URMA_CR_WR_FLUSH_ERR_DONE, 0));
    ASSERT_TRUE(queue_->Poll(false));
    EXPECT_CALL(*this, Flush(queue_->jetties[0], _, _)).WillOnce([&](auto*, int, auto* out) {
        // id=2 的确属于 SQ1，但这批由 Flush(SQ0) 返回，不能借此跨 SQ 退休。
        out[0] = Completion(*queue_, 2, URMA_CR_WR_UNHANDLED, 1);
        out[1] = Completion(*queue_, 1, URMA_CR_WR_UNHANDLED, 0);
        return 2;
    });
    EXPECT_FALSE(queue_->Flush());
    EXPECT_EQ(queue_->stats.retired, 1U);
    EXPECT_FALSE(queue_->complete());
}

TEST_F(AsyncErrorTest, ReceiverErrorRetiresOwnershipWithoutCountingSuccessfulPayload) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_TRUE(queue_->Open(&nodes_[0].ctx, cap_, 1, 1, true, 1));
    auto cr = Completion(*queue_, 1, URMA_CR_LOC_ACCESS_ERR);
    cr.flag.bs.s_r = 1;
    Enqueue(*queue_, cr, true);
    ASSERT_TRUE(queue_->Poll(false));
    EXPECT_EQ(queue_->stats.rx_retired, 1U);
    EXPECT_EQ(queue_->stats.received, 0U);
    EXPECT_TRUE(queue_->complete());
    ASSERT_TRUE(queue_->Close());
}

TEST_F(AsyncErrorTest, BeginDrainStillAttemptsSecondSqAfterFirstModifyFails) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender(2, 2);
    EXPECT_CALL(*this, ModifyJetty(queue_->jetties[0], _)).WillOnce(Return(URMA_EINVAL));
    EXPECT_CALL(*this, ModifyJetty(queue_->jetties[1], _)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_FALSE(queue_->BeginDrain());
    EXPECT_EQ(queue_->stats.modify_statuses[0], URMA_EINVAL);
    EXPECT_EQ(queue_->stats.modify_statuses[1], URMA_SUCCESS);
    EXPECT_FALSE(queue_->Post(0, 1));
}

TEST_F(AsyncErrorTest, WrongLaneAndDuplicateIdentitiesDoNotRetireAnotherWr) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender(2, 2);
    ASSERT_TRUE(queue_->Post(0, 2));
    Enqueue(*queue_, Completion(*queue_, 1, URMA_CR_SUCCESS, 1));
    Enqueue(*queue_, Completion(*queue_, 2, URMA_CR_SUCCESS, 1));
    EXPECT_FALSE(queue_->Poll(false));
    EXPECT_EQ(queue_->stats.retired, 1U);
    Enqueue(*queue_, Completion(*queue_, 2, URMA_CR_SUCCESS, 1));
    EXPECT_FALSE(queue_->Poll(false));
    EXPECT_EQ(queue_->stats.retired, 1U);
    EXPECT_FALSE(queue_->complete());
}

TEST_F(AsyncErrorTest, PollFailureDoesNotBecomeDmaCompletionOrPermitClose) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenSender(1);
    ASSERT_TRUE(queue_->Post(0, 1));
    EXPECT_CALL(*this, Poll(queue_->tx, _, _)).WillOnce(Return(-EIO));
    EXPECT_TRUE(queue_->Poll(false));
    EXPECT_EQ(queue_->stats.poll_status, -EIO);
    EXPECT_FALSE(queue_->complete());
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_FALSE(queue_->Close());
}

TEST_F(AsyncErrorTest, SessionWithoutActualEventCompletesTrafficButNeverClaimsTheFaultWasObserved) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    // 模拟 provider 足够大的真实 CQ，没有产生溢出事件；即使流量全完成，也必须保留未观测结果。
    RunSessions();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_FALSE(receiver_->stats.target_observed);
    EXPECT_FALSE(sender_->stats.peer_target_observed);
    EXPECT_EQ(receiver_->stats.observation.retrieved, 0U);
    EXPECT_TRUE(receiver_->stats.can_close);
    EXPECT_TRUE(sender_->stats.can_close);
    EXPECT_EQ(receiver_->fault.stats.received, 130U);
    EXPECT_EQ(sender_->fault.stats.retired, 130U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
    // main 使用 target/peer_target 的 OR 判定；这里为 false，走 NOT_OBSERVED/77 而非 PASS。
}

TEST_F(AsyncErrorTest, SessionRecordsRequestedJfcFaultAndKeepsIndependentTrafficUsable) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    inject_event_ = true;
    RunSessions();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_TRUE(receiver_->stats.target_observed);
    EXPECT_TRUE(sender_->stats.peer_target_observed);
    EXPECT_EQ(receiver_->stats.baseline.retrieved, 0U);
    EXPECT_EQ(receiver_->stats.observation.fault_jfc, 1U);
    EXPECT_EQ(receiver_->stats.observation.acknowledged, 1U);
    EXPECT_EQ(receiver_->healthy.stats.received, 4U);
    EXPECT_EQ(sender_->healthy.stats.retired, 4U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncErrorTest, FaultTxJfcEventDoesNotEstablishTheRequestedRxJfcFault) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    inject_event_ = true;
    fault_tx_event_ = true;
    // 同一个 fault 资源组也有 TX/RX 两个 JFC，不能把任意 JFC_ERR 都认成目标 RX JFC 故障。
    RunSessions();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(receiver_->stats.observation.fault_jfc, 1U);
    EXPECT_EQ(receiver_->stats.observation.fault_tx_jfc, 1U);
    EXPECT_EQ(receiver_->stats.observation.fault_rx_jfc, 0U);
    EXPECT_EQ(receiver_->stats.observation.acknowledged, 1U);
    EXPECT_FALSE(receiver_->stats.target_observed);
    EXPECT_FALSE(sender_->stats.peer_target_observed);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncErrorTest, EventFirstRetrievedDuringAfterProbeDoesNotRetroactivelyEstablishIsolation) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    after_event_ = true;
    // 服务端必须在允许最终 healthy WR 前锁定窗口结果；否则最后探针带来的事件会伪造“故障后成功”。
    RunSessions();
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_EQ(receiver_->stats.observation.retrieved, 0U);
    EXPECT_EQ(receiver_->stats.after.fault_rx_jfc, 1U);
    EXPECT_EQ(receiver_->stats.after.acknowledged, 1U);
    EXPECT_FALSE(receiver_->stats.target_observed);
    EXPECT_FALSE(sender_->stats.peer_target_observed);
    EXPECT_EQ(receiver_->healthy.stats.received, 4U);
    EXPECT_EQ(sender_->healthy.stats.retired, 4U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncErrorTest, ExternalJfrModeRequiresJfrEventAndAlsoExercisesFaultEndpointTraffic) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    inject_event_ = true;
    injected_type_ = URMA_EVENT_JFR_ERR;
    RunSessions(AsyncFault::kExternalJfr);
    ASSERT_TRUE(receiver_result_) << receiver_result_.error().operation;
    ASSERT_TRUE(sender_result_) << sender_result_.error().operation;
    EXPECT_TRUE(receiver_->stats.target_observed);
    EXPECT_EQ(receiver_->stats.observation.fault_jfr, 1U);
    EXPECT_EQ(sender_->fault.stats.accepted, 130U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
}

TEST_F(AsyncErrorTest, EventOnIndependentHealthyQueueFailsIsolationAndRetainsDependencies) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    inject_event_ = true;
    affect_healthy_ = true;
    RunSessions();
    EXPECT_FALSE(receiver_result_);
    EXPECT_FALSE(sender_result_);
    EXPECT_EQ(receiver_->stats.observation.healthy, 1U);
    EXPECT_EQ(receiver_->stats.observation.acknowledged, 1U);
    EXPECT_FALSE(receiver_->stats.can_close);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(sender_->Close());
}
} // namespace
} // namespace tools
} // namespace kbsocket
