// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/streaming_test/streaming_test.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;

TEST(StreamProtocolTest, RejectsInvalidDeviceListsAndOptions) {
    for (auto input : {"", "udma", "udma:-1", "udma:0,", "udma:1x", "udma:4294967296"}) {
        EXPECT_FALSE(ParseStreamDevices(input)) << input;
    }
    StreamOptions options;
    options.messages = 31;
    EXPECT_FALSE(ValidateStreamOptions(options));
    options.messages = 1000;
    options.arm_delay_us = 1;
    EXPECT_FALSE(ValidateStreamOptions(options));
    options.events = true;
    EXPECT_TRUE(ValidateStreamOptions(options));
}

TEST(StreamProtocolTest, MultipleDistinctDevicesPreservePairingOrder) {
    // hello 按列表位置配对；解析两设备及四设备时不得重排设备或混用 EID 索引。
    auto devices = ParseStreamDevices("udmac0d1e4:11,udmac0d1e5:14");
    ASSERT_TRUE(devices);
    ASSERT_EQ(devices->size(), 2u);
    EXPECT_EQ((*devices)[0].device_name, "udmac0d1e4");
    EXPECT_EQ((*devices)[0].eid_index, 11u);
    EXPECT_EQ((*devices)[1].device_name, "udmac0d1e5");
    EXPECT_EQ((*devices)[1].eid_index, 14u);

    // EID 索引是设备内的索引；不同设备使用相同索引不应被视为重复设备。
    devices = ParseStreamDevices("udma3:38,udma1:9,udma2:9,udma0:11");
    ASSERT_TRUE(devices);
    ASSERT_EQ(devices->size(), 4u);
    const std::array names{"udma3", "udma1", "udma2", "udma0"};
    const std::array indices{38u, 9u, 9u, 11u};
    for (std::size_t i = 0; i < devices->size(); ++i) {
        EXPECT_EQ((*devices)[i].device_name, names[i]);
        EXPECT_EQ((*devices)[i].eid_index, indices[i]);
    }
}

TEST(StreamProtocolTest, SameDeviceWithDifferentEidsPreservesEndpointOrder) {
    // 同一个裸设备可以有多个独立 context；不能再以设备名去重而丢掉后面的 EID。
    auto devices = ParseStreamDevices("udmac0d1e4:14,udmac0d1e4:11");
    ASSERT_TRUE(devices);
    ASSERT_EQ(devices->size(), 2u);
    EXPECT_EQ((*devices)[0].device_name, "udmac0d1e4");
    EXPECT_EQ((*devices)[0].eid_index, 14u);
    EXPECT_EQ((*devices)[1].device_name, "udmac0d1e4");
    EXPECT_EQ((*devices)[1].eid_index, 11u);
}

TEST(StreamProtocolTest, DuplicateDeviceEidPairReportsActualError) {
    // 对解析后的索引判重，避免同一端点因数字前导零而被重复配置。
    for (auto input : {"udmac0d1e4:11,udmac0d1e4:11", "udmac0d1e4:11,udmac0d1e4:011"}) {
        auto devices = ParseStreamDevices(input);
        ASSERT_FALSE(devices) << input;
        EXPECT_EQ(devices.error().code, EINVAL);
        EXPECT_STREQ(devices.error().operation, "duplicate device/EID pair");
    }
}

TEST(StreamProtocolTest, TooManyContextsReportsCountLimit) {
    // 工具限制的是会话数；即使全部属于同一设备，第五个端点也超过容量。
    auto devices = ParseStreamDevices("udma0:0,udma0:1,udma0:2,udma0:3,udma0:4");
    ASSERT_FALSE(devices);
    EXPECT_EQ(devices.error().code, EINVAL);
    EXPECT_STREQ(devices.error().operation, "use at most four device/EID pairs");
}

TEST(StreamProtocolTest, FourEndpointsRoundTripAndReservedBytesAreChecked) {
    StreamHello hello{.count = 4};
    hello.options.events = true;
    for (std::size_t i = 0; i < 4; ++i) {
        hello.endpoints[i].eid.raw[15] = i + 1;
        hello.endpoints[i].id = i + 99;
    }
    auto bytes = EncodeStreamHello(hello);
    auto decoded = DecodeStreamHello(bytes);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->count, 4u);
    EXPECT_EQ(decoded->endpoints[3].id, 102u);
    EXPECT_TRUE(decoded->options.events);
    // 未使用端点/保留位必须拒绝，防止参数版本错配悄悄运行不同实验。
    bytes[28] = std::byte{1};
    EXPECT_FALSE(DecodeStreamHello(bytes));
    hello.count = 1;
    bytes = EncodeStreamHello(hello);
    bytes.back() = std::byte{1};
    EXPECT_FALSE(DecodeStreamHello(bytes));
    hello.count = 2;
    hello.endpoints[1] = hello.endpoints[0];
    EXPECT_FALSE(DecodeStreamHello(EncodeStreamHello(hello)));
}

class StreamHardwareTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfce_t*, CreateEvent, (urma_context_t*));
    MOCK_METHOD(urma_jfc_t*, CreateCq, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateRecv, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(int, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Rearm, (urma_jfc_t*, bool));
    MOCK_METHOD(int, Wait, (urma_jfce_t*, std::uint32_t, int, urma_jfc_t**));
    MOCK_METHOD(void, Ack, (urma_jfc_t**, std::uint32_t*, std::uint32_t));
    MOCK_METHOD(int, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(int, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(int, DeleteRecv, (urma_jfr_t*));
    MOCK_METHOD(int, DeleteCq, (urma_jfc_t*));
    MOCK_METHOD(int, DeleteEvent, (urma_jfce_t*));
    MOCK_METHOD(int, Unregister, (urma_target_seg_t*));

protected:
    struct Receive {
        urma_sge_t sge;
        std::uint64_t token;
    };
    struct Node {
        urma_context_t ctx{};
        urma_jfce_t event{};
        std::array<urma_jfc_t, 2> cq{};
        urma_jfr_t recv{};
        urma_jetty_t jetty{};
        urma_target_jetty_t target{};
        urma_target_seg_t segment{};
        std::deque<Receive> buffers;
        std::array<std::deque<urma_cr_t>, 2> completions;
        int cqs = 0;
        std::uint64_t sends = 0;
        bool mr_live = false;
        bool jetty_live = false;
        bool jfr_live = false;
    };
    static raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f{};
        f.create_jfce = [](auto* c) { return active_->CreateEvent(c); };
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateCq(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateRecv(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        f.import_jetty = [](auto* c, auto* r, auto* t) { return active_->Import(c, r, t); };
        f.post_jfr_wr = [](auto* j, auto* w, auto** b) { return active_->PostRecv(j, w, b); };
        f.post_jetty_send_wr = [](auto* j, auto* w, auto** b) { return active_->PostSend(j, w, b); };
        f.poll_jfc = [](auto* j, auto n, auto* cr) { return active_->Poll(j, n, cr); };
        f.rearm_jfc = [](auto* j, bool s) { return active_->Rearm(j, s); };
        f.wait_jfc = [](auto* e, auto n, auto t, auto** c) { return active_->Wait(e, n, t, c); };
        f.ack_jfc = [](auto** c, auto* n, auto count) { active_->Ack(c, n, count); };
        f.unimport_jetty = [](auto* j) { return active_->Unimport(j); };
        f.delete_jetty = [](auto* j) { return active_->DeleteJetty(j); };
        f.delete_jfr = [](auto* j) { return active_->DeleteRecv(j); };
        f.delete_jfc = [](auto* j) { return active_->DeleteCq(j); };
        f.delete_jfce = [](auto* j) { return active_->DeleteEvent(j); };
        f.unregister_seg = [](auto* j) { return active_->Unregister(j); };
        return f;
    }
    Node& Find(urma_context_t* ctx) {
        for (auto& node : nodes_) {
            if (&node.ctx == ctx) {
                return node;
            }
        }
        ADD_FAILURE() << "unknown mock context";
        return nodes_[0];
    }
    void SetUp() override {
        active_ = this;
        override_ = std::make_unique<raw::test_support::ScopedUrmaOverride>(MockedFunctions());
        cap_.max_jfs_depth = cap_.max_jfr_depth = 256;
        cap_.max_jfc_depth = 1024;
        cap_.max_msg_size = 4096;
        cap_.max_jfs_sge = cap_.max_jfr_sge = 1;
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            auto& n = nodes_[i];
            n.ctx.eid.raw[15] = i + 1;
            n.jetty.urma_ctx = &n.ctx;
            n.jetty.jetty_id.eid = n.ctx.eid;
            n.jetty.jetty_id.id = 100 + i;
            n.recv.urma_ctx = &n.ctx;
            n.recv.jfr_id.id = 200 + i;
            n.segment.urma_ctx = &n.ctx;
            n.event.urma_ctx = &n.ctx;
            n.event.fd = -1;
            for (auto& cq : n.cq) {
                cq.urma_ctx = &n.ctx;
            }
        }
        EXPECT_CALL(*this, CreateEvent(_)).Times(AnyNumber()).WillRepeatedly([&](auto* c) { return &Find(c).event; });
        EXPECT_CALL(*this, CreateCq(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* c, auto* cfg) {
            EXPECT_EQ(cfg->flag.bs.non_blocking, 0u);
            auto& n = Find(c);
            return &n.cq[n.cqs++];
        });
        EXPECT_CALL(*this, CreateRecv(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* c, auto*) {
            auto& n = Find(c);
            n.jfr_live = true;
            return &n.recv;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* c, auto* cfg) {
            EXPECT_EQ(cfg->jfs_cfg.flag.bs.error_suspend, 0u);
            auto& n = Find(c);
            n.jetty_live = true;
            return &n.jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* c, auto* cfg) {
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            auto& n = Find(c);
            n.mr_live = true;
            return &n.segment;
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* c, auto* r, auto*) {
            EXPECT_EQ(r->tp_type, URMA_CTP);
            return &Find(c).target;
        });
        EXPECT_CALL(*this, PostRecv(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* j, auto* w, auto**) {
            auto& n = Find(j->urma_ctx);
            EXPECT_TRUE(n.mr_live);
            n.buffers.push_back({w->src.sge[0], w->user_ctx});
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostSend(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* j, auto* w, auto**) {
            auto& source = Find(j->urma_ctx);
            auto& target = nodes_[(j->jetty_id.id - 100) ^ 1];
            EXPECT_TRUE(source.mr_live);
            EXPECT_TRUE(target.mr_live);
            EXPECT_FALSE(target.buffers.empty());
            if (target.buffers.empty()) {
                return URMA_EAGAIN;
            }
            // 逆序消费 RX 槽，防止实现把 WR 序号误当作 buffer 槽号。
            const auto recv = target.buffers.back();
            target.buffers.pop_back();
            std::memcpy(reinterpret_cast<void*>(recv.sge.addr), reinterpret_cast<void*>(w->send.src.sge[0].addr),
                        w->send.src.sge[0].len);
            urma_cr_t tx{};
            tx.status = URMA_CR_SUCCESS;
            tx.user_ctx = w->user_ctx;
            tx.flag.bs.jetty = 1;
            tx.local_id = source.jetty.jetty_id.id;
            source.completions[0].push_back(tx);
            urma_cr_t rx{};
            rx.status = URMA_CR_SUCCESS;
            rx.user_ctx = recv.token;
            rx.flag.bs.s_r = 1;
            rx.flag.bs.jetty = 1;
            rx.local_id = target.jetty.jetty_id.id;
            rx.remote_id = source.jetty.jetty_id;
            rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
            rx.completion_len = w->send.src.sge[0].len;
            rx.imm_data = w->send.imm_data;
            target.completions[1].push_back(rx);
            ++source.sends;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* cq, int max, auto* out) {
            auto& n = Find(cq->urma_ctx);
            auto& pending = n.completions[cq == &n.cq[0] ? 0 : 1];
            const auto count = std::min<int>(max, pending.size());
            for (int i = 0; i < count; ++i) {
                out[i] = pending.back();
                pending.pop_back();
            }
            return count;
        });
        EXPECT_CALL(*this, Wait(_, _, 0, _)).Times(AnyNumber()).WillRepeatedly(::testing::Return(0));
        EXPECT_CALL(*this, Rearm(_, false)).Times(AnyNumber()).WillRepeatedly(::testing::Return(URMA_SUCCESS));
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly(::testing::Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([&](auto* j) {
            Find(j->urma_ctx).jetty_live = false;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteRecv(_)).Times(AnyNumber()).WillRepeatedly([&](auto* j) {
            auto& n = Find(j->urma_ctx);
            EXPECT_FALSE(n.jetty_live);
            EXPECT_TRUE(n.buffers.empty());
            n.jfr_live = false;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([&](auto* s) {
            auto& n = Find(s->urma_ctx);
            EXPECT_FALSE(n.jfr_live);
            EXPECT_FALSE(n.jetty_live);
            n.mr_live = false;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteCq(_)).Times(AnyNumber()).WillRepeatedly(::testing::Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteEvent(_)).Times(AnyNumber()).WillRepeatedly(::testing::Return(URMA_SUCCESS));
    }
    void OpenPair(StreamSession& client, StreamSession& server, const StreamOptions& options) {
        ASSERT_TRUE(client.Open(&nodes_[0].ctx, cap_, options, false, 0));
        ASSERT_TRUE(server.Open(&nodes_[1].ctx, cap_, options, true, 0));
        ASSERT_TRUE(client.Connect(server.endpoint()));
        ASSERT_TRUE(server.Connect(client.endpoint()));
        ASSERT_TRUE(client.Start());
        ASSERT_TRUE(server.Start());
    }
    void FinishPair(StreamSession& client, StreamSession& server, std::uint32_t messages) {
        for (std::uint32_t i = 0; i < messages * 4 && !(client.done() && server.done()); ++i) {
            ASSERT_TRUE(client.Advance());
            ASSERT_TRUE(server.Advance());
        }
        ASSERT_TRUE(client.done());
        ASSERT_TRUE(server.done());
        // 本端计数收齐不能代替对端静默证明，握手前禁止释放导出端点/MR。
        EXPECT_FALSE(client.Close());
        client.ConfirmPeerQuiescent();
        server.ConfirmPeerQuiescent();
        EXPECT_TRUE(client.Close());
        EXPECT_TRUE(server.Close());
    }
    inline static StreamHardwareTest* active_;
    std::unique_ptr<raw::test_support::ScopedUrmaOverride> override_;
    std::array<Node, 4> nodes_;
    urma_device_cap_t cap_{};
};

TEST_F(StreamHardwareTest, ContinuousOutOfOrderTrafficRecyclesEverySlotWithoutBatchBarriers) {
    StreamSession client, server;
    StreamOptions options{.bytes = 4096, .messages = 1001, .window = 32};
    OpenPair(client, server, options);
    FinishPair(client, server, options.messages);
    for (const auto* session : {&client, &server}) {
        EXPECT_EQ(session->stats().accepted, 1001u);
        EXPECT_EQ(session->stats().retired, 1001u);
        EXPECT_EQ(session->stats().received, 1001u);
        EXPECT_EQ(session->stats().rx_posted, 1001u);
    }
}

TEST_F(StreamHardwareTest, ServerWaitsForOldEchoTxCompletionBeforeReusingItsBuffer) {
    StreamSession client, server;
    StreamOptions options{.bytes = 32, .messages = 3, .window = 1};
    OpenPair(client, server, options);
    ASSERT_TRUE(client.Advance());
    ASSERT_TRUE(server.Advance());
    ASSERT_TRUE(client.Advance()); // 收到 echo 后可发下一条，但服务端旧 echo CQE 仍未消费。
    auto delayed = nodes_[1].completions[0];
    nodes_[1].completions[0].clear();
    ASSERT_TRUE(server.Advance());
    EXPECT_EQ(nodes_[1].sends, 1u);
    EXPECT_EQ(server.stats().received, 2u);
    nodes_[1].completions[0] = delayed;
    FinishPair(client, server, options.messages);
}

TEST_F(StreamHardwareTest, MultipleContextsProgressInOneOwnerLoopAndRetainSeparateIdentities) {
    // 多个 context 可以属于同一设备；队列和消息身份仍按 context 独立管理。
    urma_device_t device{};
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        nodes_[i].ctx.dev = &device;
        nodes_[i].ctx.eid_index = i;
    }
    std::array<StreamSession, 4> owners;
    std::array<StreamSession*, 4> sessions;
    StreamOptions options{.bytes = 1024, .messages = 65, .window = 3};
    for (std::size_t i = 0; i < owners.size(); ++i) {
        ASSERT_TRUE(owners[i].Open(&nodes_[i].ctx, cap_, options, i & 1, i / 2));
        sessions[i] = &owners[i];
    }
    for (std::size_t i = 0; i < owners.size(); ++i) {
        ASSERT_TRUE(owners[i].Connect(owners[i ^ 1].endpoint()));
        ASSERT_TRUE(owners[i].Start());
    }
    // 同一 owner 推进两个独立端点配对；payload 的 channel 区分会话，不能只看累计总数。
    auto result = RunStreams(sessions, options);
    ASSERT_TRUE(result);
    for (auto& owner : owners) {
        EXPECT_EQ(owner.stats().received, options.messages);
        EXPECT_EQ(owner.stats().retired, options.messages);
        owner.ConfirmPeerQuiescent();
        EXPECT_TRUE(owner.Close());
    }
}

TEST_F(StreamHardwareTest, DuplicateOrPseudoTxNeverPermitsMemoryRelease) {
    // 使用独立子进程保留故障现场，避免测试栈析构意外释放模拟中的未决 DMA 缓冲。
    EXPECT_EXIT(([&] {
                    NoDestructor<StreamSession> client, server;
                    StreamOptions options{.bytes = 32, .messages = 1, .window = 1};
                    OpenPair(*client, *server, options);
                    auto sent = client->Advance();
                    if (!sent) {
                        std::_Exit(2);
                    }
                    auto& pending = nodes_[0].completions[0];
                    pending.front().status = URMA_CR_WR_FLUSH_ERR_DONE;
                    pending.front().user_ctx = ~std::uint64_t{0};
                    if (client->Advance() || client->Close() || !nodes_[0].mr_live) {
                        std::_Exit(3);
                    }
                    std::_Exit(0);
                }()),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(StreamHardwareTest, AcknowledgesNotificationCountRatherThanCqeCount) {
    StreamSession session;
    StreamOptions options{.events = true};
    ASSERT_TRUE(session.Open(&nodes_[0].ctx, cap_, options, false, 0));
    // 同一 CQ 可以有多个通知；每项 ack=1，不以 CQE 数或唯一 CQ 数来确认。
    EXPECT_CALL(*this, Wait(&nodes_[0].event, 16, 0, _))
        .WillOnce([&](auto*, auto, int, auto** out) {
            out[0] = out[1] = &nodes_[0].cq[0];
            out[2] = &nodes_[0].cq[1];
            return 3;
        })
        .RetiresOnSaturation();
    EXPECT_CALL(*this, Ack(_, _, 3)).WillOnce([&](auto** out, auto* counts, auto) {
        EXPECT_EQ(out[0], &nodes_[0].cq[0]);
        EXPECT_EQ(out[2], &nodes_[0].cq[1]);
        EXPECT_EQ(counts[0], 1u);
        EXPECT_EQ(counts[1], 1u);
        EXPECT_EQ(counts[2], 1u);
    });
    auto count = session.CollectNotifications();
    ASSERT_TRUE(count);
    EXPECT_EQ(*count, 3u);
    EXPECT_EQ(session.stats().event_acks, 3u);
    EXPECT_EQ(session.stats().tx_notifications, 2u);
    EXPECT_EQ(session.stats().rx_notifications, 1u);
    EXPECT_TRUE(session.Close());
}

TEST_F(StreamHardwareTest, EventRecheckFindsCompletionBeforeWaiting) {
    StreamSession client, server;
    StreamOptions options{.bytes = 32, .messages = 1, .window = 1, .timeout_ms = 50, .events = true};
    OpenPair(client, server, options);
    // arm 时注入对端请求，必须被 recheck 消费；若实现直接睡眠，将遇到 -1 的 JFCE fd 并失败。
    EXPECT_CALL(*this, Rearm(&nodes_[1].cq[1], false))
        .WillOnce([&](auto*, bool) {
            EXPECT_TRUE(client.Advance());
            return URMA_SUCCESS;
        })
        .RetiresOnSaturation();
    std::array<StreamSession*, 1> sessions{&server};
    auto result = RunStreams(sessions, options);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->waits, 0u);
    EXPECT_GT(server.stats().recheck_progress, 0u);
    FinishPair(client, server, 1);
}

TEST_F(StreamHardwareTest, DelayedRecheckCollectsNotificationsWithoutFdReadiness) {
    StreamSession client, server;
    StreamOptions options{
        .bytes = 32, .messages = 1, .window = 1, .timeout_ms = 1000, .events = true, .arm_delay_us = 1000};
    OpenPair(client, server, options);
    // 在 arm 时让请求到达，保证延迟后的 recheck 能收到 CQE；此时不应再进入 fd 等待。
    EXPECT_CALL(*this, Rearm(&nodes_[1].cq[1], false))
        .WillOnce([&](auto*, bool) {
            EXPECT_TRUE(client.Advance());
            return URMA_SUCCESS;
        })
        .RetiresOnSaturation();
    // CQE 已在 recheck 中消费，但下一轮 WaitJfc(0) 仍需领取并确认对应通知。
    EXPECT_CALL(*this, Wait(&nodes_[1].event, 16, 0, _))
        .WillOnce(::testing::Return(0))
        .WillOnce([&](auto*, auto, int, auto** out) {
            out[0] = &nodes_[1].cq[1];
            return 1;
        })
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*this, Ack(_, _, 1)).WillOnce([&](auto** out, auto* counts, auto) {
        EXPECT_EQ(out[0], &nodes_[1].cq[1]);
        EXPECT_EQ(counts[0], 1u);
    });
    std::array<StreamSession*, 1> sessions{&server};
    auto result = RunStreams(sessions, options);
    ASSERT_TRUE(result);
    // 通知消费、fd 就绪是独立观测项；前者已验证，不应因为后者未走到而被一并标成未观测。
    EXPECT_EQ(server.stats().recheck_progress, 1u);
    EXPECT_EQ(server.stats().rx_notifications, 1u);
    EXPECT_EQ(server.stats().event_acks, 1u);
    EXPECT_EQ(server.stats().fd_wakes, 0u);
    EXPECT_EQ(result->waits, 0u);
    FinishPair(client, server, 1);
}

TEST_F(StreamHardwareTest, ContinuousProgressDoesNotRequireArmingOrFdReadiness) {
    StreamSession client, server;
    StreamOptions options{.bytes = 32, .messages = 5, .window = 1, .events = true};
    OpenPair(client, server, options);
    // 模拟始终有请求/回包可推进的时序：即使启用 events，也无需为了覆盖标记强行 arm 或等待。
    EXPECT_CALL(*this, Rearm(_, _)).Times(0);
    std::array<StreamSession*, 2> sessions{&client, &server};
    auto result = RunStreams(sessions, options);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->waits, 0u);
    for (auto* session : sessions) {
        EXPECT_TRUE(session->done());
        EXPECT_EQ(session->stats().rearms, 0u);
        EXPECT_EQ(session->stats().event_acks, 0u);
        EXPECT_EQ(session->stats().fd_wakes, 0u);
        session->ConfirmPeerQuiescent();
        EXPECT_TRUE(session->Close());
    }
}

TEST_F(StreamHardwareTest, ReadyFdIsConsumedAndAcknowledgedBeforeNextPoll) {
    StreamSession client, server;
    StreamOptions options{.bytes = 32, .messages = 1, .window = 1, .timeout_ms = 50, .events = true};
    OpenPair(client, server, options);
    nodes_[1].event.fd = ::eventfd(1, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(nodes_[1].event.fd, 0);
    int calls = 0;
    EXPECT_CALL(*this, Wait(&nodes_[1].event, 16, 0, _)).WillRepeatedly([&](auto*, auto, int, auto** out) {
        if (++calls == 2) {
            std::uint64_t value;
            EXPECT_EQ(::read(nodes_[1].event.fd, &value, sizeof(value)), static_cast<ssize_t>(sizeof(value)));
            EXPECT_TRUE(client.Advance());
            out[0] = &nodes_[1].cq[1];
            return 1;
        }
        return 0;
    });
    EXPECT_CALL(*this, Ack(_, _, 1)).WillOnce([&](auto** out, auto* counts, auto) {
        EXPECT_EQ(out[0], &nodes_[1].cq[1]);
        EXPECT_EQ(counts[0], 1u);
    });
    std::array<StreamSession*, 1> sessions{&server};
    auto result = RunStreams(sessions, options);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->waits, 1u);
    EXPECT_EQ(server.stats().fd_wakes, 1u);
    EXPECT_EQ(server.stats().event_acks, 1u);
    FinishPair(client, server, 1);
    ::close(nodes_[1].event.fd);
}

TEST_F(StreamHardwareTest, RearmFailureIsNotHiddenByCompletionDuringRecheck) {
    StreamSession client, server;
    StreamOptions options{.bytes = 32, .messages = 1, .window = 1, .timeout_ms = 50, .events = true};
    OpenPair(client, server, options);
    // 注入 rearm 错误同时让 CQ 前进，不能把“数据能被 poll 到”误判为“事件路径正常”。
    EXPECT_CALL(*this, Rearm(&nodes_[1].cq[1], false))
        .WillOnce([&](auto*, bool) {
            EXPECT_TRUE(client.Advance());
            return URMA_EINVAL;
        })
        .RetiresOnSaturation();
    std::array<StreamSession*, 1> sessions{&server};
    auto result = RunStreams(sessions, options);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, URMA_EINVAL);
    FinishPair(client, server, 1);
}

TEST_F(StreamHardwareTest, SuccessWithBadWrRetainsUnknownOwnership) {
    EXPECT_EXIT(([&] {
                    NoDestructor<StreamSession> client, server;
                    StreamOptions options{.bytes = 32, .messages = 1, .window = 1};
                    OpenPair(*client, *server, options);
                    // 返回码和 bad_wr 自相矛盾时，既不虚构 accepted，也不尝试重放/注销 MR。
                    EXPECT_CALL(*this, PostSend(_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
                        *bad = wr;
                        return URMA_SUCCESS;
                    });
                    if (client->Advance() || client->Close() || !nodes_[0].mr_live || client->stats().accepted) {
                        std::_Exit(2);
                    }
                    std::_Exit(0);
                }()),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(StreamHardwareTest, MissingNotificationFailsWithoutPeriodicPollingFallback) {
    EXPECT_EXIT(([&] {
                    NoDestructor<StreamSession> client, server;
                    StreamOptions options{.bytes = 32, .messages = 1, .window = 1, .timeout_ms = 5, .events = true};
                    OpenPair(*client, *server, options);
                    nodes_[1].event.fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
                    if (nodes_[1].event.fd < 0) {
                        std::_Exit(2);
                    }
                    // 空 fd 永不通知；必须有界失败，不能用超时后的忙 poll 冒充事件唤醒通过。
                    std::array<StreamSession*, 1> sessions{&*server};
                    auto result = RunStreams(sessions, options);
                    if (result || result.error().code != ETIMEDOUT || server->Close() || !nodes_[1].mr_live) {
                        std::_Exit(3);
                    }
                    ::close(nodes_[1].event.fd);
                    std::_Exit(0);
                }()),
                ::testing::ExitedWithCode(0), "");
}
} // namespace
} // namespace tools
} // namespace kbsocket
