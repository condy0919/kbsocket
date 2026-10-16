// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/tx_drain_test/tx_drain_test.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::Return;

TEST(TxDrainProtocol, RejectsOtherToolsVersionsRolesAndInvalidOptions) {
    TxDrainHello hello;
    hello.server = true;
    hello.endpoint.id = 0x01020304;
    hello.endpoint.eid.raw[15] = 42;
    auto wire = EncodeDrainHello(hello);
    EXPECT_EQ(wire[3], std::byte{'R'});
    EXPECT_EQ(wire[36], std::byte{1});
    EXPECT_EQ(wire[39], std::byte{4});
    auto decoded = DecodeDrainHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->server);
    EXPECT_EQ(decoded->endpoint.id, hello.endpoint.id);
    EXPECT_EQ(decoded->endpoint.eid.raw[15], 42);
    wire[3] = std::byte{'T'}; // 不能把基础 SEND 工具的握手当作排空测试对端。
    EXPECT_FALSE(DecodeDrainHello(wire));
    wire = EncodeDrainHello(hello);
    wire[7] = std::byte{2};
    EXPECT_FALSE(DecodeDrainHello(wire));
    wire = EncodeDrainHello(hello);
    wire[11] = std::byte{2};
    EXPECT_FALSE(DecodeDrainHello(wire));
    hello.options.batch = 257;
    EXPECT_FALSE(DecodeDrainHello(EncodeDrainHello(hello)));
    EXPECT_FALSE(ValidateDrainOptions({.bytes = 7}));
    EXPECT_FALSE(ValidateDrainOptions({.bytes = 1048577}));
    EXPECT_FALSE(ValidateDrainOptions({.batch = 0}));
    EXPECT_FALSE(ValidateDrainOptions({.timeout_ms = 0}));
}

TEST(TxDrainProtocol, CleanupRequiresPeerConfirmationAndHasTimeout) {
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    // 本地关闭不意味着对端关闭；尚未收到对端确认时不能报告 PASS。
    auto result = ConfirmDrainClosed(client, 1);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ETIMEDOUT);
    ASSERT_TRUE(peer.ExpectMarker(0x445204, 100));
    ASSERT_TRUE(peer.SendMarker(0x445204, 100));
    ASSERT_TRUE(ConfirmDrainClosed(client, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x445204, 100));
}

class TxDrainHardware : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));

protected:
    static raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        f.import_jetty = [](auto* c, auto* r, auto* t) { return active_->Import(c, r, t); };
        f.post_jetty_send_wr = [](auto* q, auto* w, auto** b) { return active_->Post(q, w, b); };
        f.post_jfr_wr = [](auto* q, auto* w, auto** b) { return active_->PostRecv(q, w, b); };
        f.modify_jetty = [](auto* q, auto* a) { return active_->Modify(q, a); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        f.flush_jetty = [](auto* q, int n, auto* cr) { return active_->Flush(q, n, cr); };
        f.unimport_jetty = [](auto* q) { return active_->Unimport(q); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.unregister_seg = [](auto* q) { return active_->Unregister(q); };
        return f;
    }
    void SetUp() override {
        active_ = this;
        jetty_.jetty_id.id = 7;
        EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
        EXPECT_CALL(*this, Post(_, _, _)).Times(0);
        EXPECT_CALL(*this, Modify(_, _)).Times(0);
        EXPECT_CALL(*this, Poll(_, _, _)).Times(0);
        EXPECT_CALL(*this, Flush(_, _, _)).Times(0);
    }
    void Open(bool server = false) {
        EXPECT_CALL(*this, CreateJfc(&ctx_, _))
            .WillOnce([&](auto*, auto* cfg) {
                EXPECT_EQ(cfg->depth, server ? 2u : options_.batch + 1);
                return &tx_;
            })
            .WillOnce(Return(&rx_));
        EXPECT_CALL(*this, CreateJfr(&ctx_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(*this, CreateJetty(&ctx_, _)).WillOnce([&](auto*, auto* cfg) {
            EXPECT_EQ(cfg->jfs_cfg.flag.bs.error_suspend, 0u);
            EXPECT_EQ(cfg->jfs_cfg.depth, server ? 1u : options_.batch);
            return &jetty_;
        });
        if (server) {
            EXPECT_CALL(*this, Register(_, _)).Times(0);
        } else {
            EXPECT_CALL(*this, Register(&ctx_, _)).WillOnce([&](auto*, auto* cfg) {
                const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
                EXPECT_EQ(cfg->va % page, 0u);
                EXPECT_EQ(cfg->len % page, 0u);
                EXPECT_GE(cfg->len, options_.bytes * options_.batch);
                EXPECT_EQ(cfg->flag.bs.access,
                          static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
                return &segment_;
            });
        }
        urma_device_cap_t cap{};
        cap.max_msg_size = 1048576;
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 1;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 1024;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        ASSERT_TRUE(session_->Open(&ctx_, cap, options_, server));
    }
    void PresetPeer(ControlChannel& peer) {
        TxDrainHello hello{.options = options_, .server = true};
        hello.endpoint.id = 99;
        ASSERT_TRUE(peer.Write(EncodeDrainHello(hello), 100));
        ASSERT_TRUE(peer.SendMarker(0x445201, 100));
        ASSERT_TRUE(peer.SendMarker(0x445203, 100));
        EXPECT_CALL(*this, Import(&ctx_, _, _)).WillOnce([&](auto*, auto* r, auto*) {
            EXPECT_EQ(r->jetty_id.id, 99u);
            EXPECT_EQ(r->tp_type, URMA_CTP);
            EXPECT_EQ(r->trans_mode, URMA_TM_RM);
            return &remote_;
        });
    }
    void ExpectPostAndModify(urma_status_t modify_status = URMA_SUCCESS) {
        EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
            std::size_t i = 0;
            for (; wr && i < options_.batch; wr = wr->next, ++i) {
                ids_[i] = wr->user_ctx;
                EXPECT_NE(ids_[i], 0u);
                EXPECT_EQ(wr->tjetty, &remote_);
                EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
                EXPECT_EQ(wr->send.src.sge->tseg, &segment_);
            }
            EXPECT_EQ(i, options_.batch);
            EXPECT_EQ(wr, nullptr);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce([&, modify_status](auto*, auto* attr) {
            EXPECT_NE(ids_[0], 0u); // 必须先真实 post，再切 ERROR；不能把空队列测试算作成功。
            EXPECT_EQ(attr->mask, JETTY_STATE);
            EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
            return modify_status;
        });
    }
    urma_cr_t Completion(raw::AttemptId id, urma_cr_status_t status) {
        urma_cr_t cr{};
        cr.user_ctx = id;
        cr.status = status;
        cr.local_id = 7;
        cr.flag.bs.jetty = 1;
        return cr;
    }
    void ExpectClose(bool imported = true, bool registered = true) {
        ::testing::InSequence order;
        if (imported) {
            EXPECT_CALL(*this, Unimport(&remote_)).WillOnce(Return(URMA_SUCCESS));
        }
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
        if (registered) {
            EXPECT_CALL(*this, Unregister(&segment_)).WillOnce(Return(URMA_SUCCESS));
        }
    }
    void ExpectRetained() {
        // 故障注入后不伪造额外终结证据；与工具入口相同，保留对象直到测试进程退出。
        EXPECT_CALL(*this, Unimport(_)).Times(0);
        EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
        EXPECT_CALL(*this, Unregister(_)).Times(0);
        auto closed = session_->Close();
        ASSERT_FALSE(closed);
        EXPECT_EQ(closed.error().code, EBUSY);
    }
    static inline TxDrainHardware* active_;
    raw::test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    // 失败测试刻意保留分配的容器；成功测试必须显式 Close 回收。
    NoDestructor<TxDrainSession> session_;
    TxDrainOptions options_{64, 3, 100};
    urma_context_t ctx_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_seg_t segment_{};
    urma_target_jetty_t remote_{};
    std::array<raw::AttemptId, 256> ids_{};
};

class TxDrainDistribution : public TxDrainHardware, public ::testing::WithParamInterface<int> {};
TEST_P(TxDrainDistribution, EveryAttemptRetiresOnceAcrossHardwareAndSoftwareBatches) {
    const int scenario = GetParam();
    if (scenario == 3) {
        options_.batch = 256;
    }
    if (scenario >= 4) {
        options_.batch = 1;
    }
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    // 覆盖混合完成、全软件、全硬件和最大批次；不要求任一类完成必定非零。
    const unsigned hardware = scenario == 0 ? 2 : scenario == 1 ? 0 : scenario == 3 ? 128 : options_.batch;
    unsigned polled = 0, flushed = hardware;
    {
        ::testing::InSequence order;
        ExpectPostAndModify();
        EXPECT_CALL(*this, Poll(&tx_, 64, _))
            .Times((hardware + 1 + 63) / 64)
            .WillRepeatedly([&](auto*, int n, auto* out) {
                int count = 0;
                while (count < n && polled < hardware) {
                    // 四种常见错误分别计数；另用 LOC_LEN_ERR 验证兜底计数仍然有效。
                    constexpr std::array errors{URMA_CR_RNR_RETRY_CNT_EXC_ERR, URMA_CR_LOC_ACCESS_ERR,
                                                URMA_CR_REM_ACCESS_ABORT_ERR, URMA_CR_ACK_TIMEOUT_ERR,
                                                URMA_CR_LOC_LEN_ERR};
                    const auto status = scenario >= 4                  ? errors[scenario - 4]
                                        : scenario == 0 && polled == 0 ? URMA_CR_SUCCESS
                                                                       : URMA_CR_WR_FLUSH_ERR;
                    out[count++] = Completion(ids_[polled++], status);
                }
                if (count < n) {
                    // 填入真实 AttemptId 验证伪 CQE 的 user_ctx 不会造成第二次退休。
                    out[count++] = Completion(ids_[0], URMA_CR_WR_FLUSH_ERR_DONE);
                }
                return count;
            });
        EXPECT_CALL(*this, Flush(&jetty_, 64, _))
            .Times((options_.batch - hardware + 63) / 64 + 1)
            .WillRepeatedly([&](auto*, int n, auto* out) {
                int count = 0;
                while (count < n && flushed < options_.batch) {
                    out[count++] = Completion(ids_[flushed++], URMA_CR_WR_UNHANDLED);
                }
                return count;
            });
    }
    ASSERT_TRUE(session_->Run(client));
    const auto& stats = session_->stats();
    EXPECT_EQ(stats.accepted, options_.batch);
    EXPECT_EQ(stats.retired, options_.batch);
    EXPECT_EQ(stats.success, scenario == 0 ? 1u : 0u);
    EXPECT_EQ(stats.loc_access_error, scenario == 5 ? 1u : 0u);
    EXPECT_EQ(stats.remote_access_abort_error, scenario == 6 ? 1u : 0u);
    EXPECT_EQ(stats.ack_timeout_error, scenario == 7 ? 1u : 0u);
    EXPECT_EQ(stats.rnr_retry_count_exceeded_error, scenario == 4 ? 1u : 0u);
    EXPECT_EQ(stats.flush_error, scenario >= 4 ? 0u : hardware - stats.success);
    EXPECT_EQ(stats.success + stats.flush_error + stats.unhandled + stats.loc_access_error +
                  stats.remote_access_abort_error + stats.ack_timeout_error + stats.rnr_retry_count_exceeded_error +
                  stats.other_error,
              stats.retired);
    EXPECT_EQ(stats.unhandled, options_.batch - hardware);
    EXPECT_EQ(stats.other_error, scenario == 8 ? 1u : 0u);
    EXPECT_EQ(stats.last_other_error, scenario == 8 ? static_cast<int>(URMA_CR_LOC_LEN_ERR) : 0);
    EXPECT_TRUE(stats.accepted_known);
    EXPECT_EQ(stats.flush_done, 1u);
    EXPECT_EQ(stats.rejected_sends, 3u);
    // 三次拒绝检查不应造成额外 provider post，唯一的 Post 期望已在第一批耗尽。
    EXPECT_FALSE(session_->Run(client));
    if (scenario == 0) {
        // 排空成功也不能掩盖清理失败；删除失败时禁止注销内存，随后仅重试剩余资源。
        EXPECT_CALL(*this, Unimport(&remote_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_EAGAIN));
        EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
        EXPECT_CALL(*this, Unregister(_)).Times(0);
        auto closed = session_->Close();
        ASSERT_FALSE(closed);
        EXPECT_EQ(closed.error().code, URMA_EAGAIN);
    }
    ExpectClose(scenario != 0);
    ASSERT_TRUE(session_->Close());
    ASSERT_TRUE(session_->Close());
    TxDrainHelloBytes sent{};
    ASSERT_TRUE(peer.Read(sent, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x445202, 100));
}
INSTANTIATE_TEST_SUITE_P(CompletionDistributions, TxDrainDistribution, ::testing::Values(0, 1, 2, 3, 4, 5, 6, 7, 8));

TEST_F(TxDrainHardware, ServerProvidesEndpointWithoutRegisteringOrPostingReceives) {
    Open(true);
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel server(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeDrainHello({.options = options_}), 100));
    ASSERT_TRUE(peer.SendMarker(0x445202, 100));
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    ASSERT_TRUE(session_->Run(server));
    TxDrainHelloBytes sent{};
    ASSERT_TRUE(peer.Read(sent, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x445201, 100));
    ASSERT_TRUE(peer.ExpectMarker(0x445203, 100));
    ExpectClose(false, false);
    ASSERT_TRUE(session_->Close());
}

TEST_F(TxDrainHardware, MismatchedPeerFailsBeforeImportOrPostAndCanClose) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    auto other = options_;
    ++other.batch;
    ASSERT_TRUE(peer.Write(EncodeDrainHello({.options = other, .server = true}), 100));
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    auto result = session_->Run(client);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    ExpectClose(false);
    ASSERT_TRUE(session_->Close());
}

TEST_F(TxDrainHardware, MissingBoundaryTimesOutEvenWhenAllWrHaveCompleted) {
    options_.timeout_ms = 5;
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    ExpectPostAndModify();
    EXPECT_CALL(*this, Poll(&tx_, 64, _))
        .WillOnce([&](auto*, int, auto* out) {
            for (unsigned i = 0; i < options_.batch; ++i) {
                out[i] = Completion(ids_[i], URMA_CR_SUCCESS);
            }
            return options_.batch;
        })
        .WillRepeatedly(Return(0));
    auto result = session_->Run(client);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ETIMEDOUT);
    EXPECT_EQ(session_->stats().retired, options_.batch);
    EXPECT_EQ(session_->stats().flush_done, 0u);
    ExpectRetained();
}

TEST_F(TxDrainHardware, DuplicateCompletionFailsInsteadOfReportingDrainSuccess) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    ExpectPostAndModify();
    EXPECT_CALL(*this, Poll(&tx_, 64, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids_[0], URMA_CR_WR_FLUSH_ERR);
        out[1] = out[0];
        out[2] = Completion(0, URMA_CR_WR_FLUSH_ERR_DONE);
        return 3;
    });
    EXPECT_FALSE(session_->Run(client));
    EXPECT_EQ(session_->stats().retired, 1u);
    ExpectRetained();
}

TEST_F(TxDrainHardware, EmptySoftwareQueueWithResidualAttemptsFailsAndRetainsMemory) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    ExpectPostAndModify();
    EXPECT_CALL(*this, Poll(&tx_, 64, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(ids_[0], URMA_CR_WR_FLUSH_ERR_DONE);
        return 1;
    });
    EXPECT_CALL(*this, Flush(&jetty_, 64, _)).WillOnce(Return(0));
    EXPECT_FALSE(session_->Run(client));
    EXPECT_EQ(session_->stats().retired, 0u);
    ExpectRetained();
}
TEST_F(TxDrainHardware, ModifyFailureDoesNotPollOrReleaseInFlightMemory) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    ExpectPostAndModify(URMA_EINVAL);
    auto result = session_->Run(client);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, URMA_EINVAL);
    EXPECT_EQ(session_->stats().accepted, options_.batch);
    EXPECT_EQ(session_->stats().retired, 0u);
    ExpectRetained();
}

TEST_F(TxDrainHardware, SoftwareFlushFailureDoesNotDeclareEmptyOrFreeBuffers) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    ExpectPostAndModify();
    EXPECT_CALL(*this, Poll(&tx_, 64, _)).WillOnce([&](auto*, int, auto* out) {
        out[0] = Completion(0, URMA_CR_WR_FLUSH_ERR_DONE);
        return 1;
    });
    EXPECT_CALL(*this, Flush(&jetty_, 64, _)).WillOnce(Return(-7));
    auto result = session_->Run(client);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, -7);
    EXPECT_EQ(session_->stats().flush_done, 1u);
    ExpectRetained();
}

TEST_F(TxDrainHardware, PartialSubmissionReportsAcceptedPrefixAndNeverReplays) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        // 第一条已接受；不能将整批失败解释成没有 DMA，也不能自动再次提交。
        *bad = wr->next;
        return URMA_EAGAIN;
    });
    auto result = session_->Run(client);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, URMA_EAGAIN);
    EXPECT_TRUE(session_->stats().accepted_known);
    EXPECT_EQ(session_->stats().accepted, 1u);
    ExpectRetained();
}

TEST_F(TxDrainHardware, UnknownSubmissionBoundaryIsReportedAsUnknownAndRetained) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    PresetPeer(peer);
    EXPECT_CALL(*this, Post(&jetty_, _, _)).WillOnce(Return(URMA_EINVAL));
    // TxSender 对不可信接受范围会主动隔离并切 ERROR；工具不得把 Prepared 记录当作未提交清除。
    EXPECT_CALL(*this, Modify(&jetty_, _)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_FALSE(session_->Run(client));
    EXPECT_FALSE(session_->stats().accepted_known);
    EXPECT_EQ(session_->stats().retired, 0u);
    ExpectRetained();
}

} // namespace
} // namespace tools
} // namespace kbsocket
