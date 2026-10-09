// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/send_test/send_test.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::Return;

TEST(SendTestProtocol, WireFormatAndPayloadRejectCorruption) {
    SendTestHello hello;
    hello.endpoint.id = 0x01020304;
    hello.endpoint.eid.raw[15] = 42;
    auto wire = EncodeHello(hello);
    EXPECT_EQ(wire[24], std::byte{1});
    EXPECT_EQ(wire[27], std::byte{4});
    auto decoded = DecodeHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->endpoint.id, hello.endpoint.id);
    EXPECT_EQ(decoded->endpoint.eid.raw[15], 42);
    EXPECT_EQ(decoded->options.bytes, 4096u);
    wire[7] = std::byte{2};
    EXPECT_FALSE(DecodeHello(wire));
    hello.options.batch = 257;
    EXPECT_FALSE(DecodeHello(EncodeHello(hello)));
    std::array<std::byte, 64> payload{};
    FillPayload(payload, 1234);
    EXPECT_EQ(CheckPayload(payload), 1234u);
    payload[40] ^= std::byte{1};
    EXPECT_FALSE(CheckPayload(payload));
    EXPECT_FALSE(CheckPayload(std::span(payload).first(7)));
    EXPECT_FALSE(ValidateOptions({.bytes = 7}));
    EXPECT_FALSE(ValidateOptions({.messages = 0}));
    EXPECT_FALSE(ValidateOptions({.timeout_ms = 0}));
}

TEST(SendTestProtocol, ControlTimeoutEofAndMarkerMismatchAreBounded) {
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel reader(fds[0]);
    {
        ControlChannel writer(fds[1]);
        auto timeout = reader.ExpectMarker(1, 1);
        ASSERT_FALSE(timeout);
        EXPECT_EQ(timeout.error().code, ETIMEDOUT);
        ASSERT_TRUE(writer.SendMarker(2, 100));
        EXPECT_FALSE(reader.ExpectMarker(1, 100));
    }
    EXPECT_FALSE(reader.ExpectMarker(1, 100));
}

class SendTestHardware : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        f.import_jetty = [](auto* c, auto* r, auto* t) { return active_->Import(c, r, t); };
        f.post_jetty_send_wr = [](auto* q, auto* w, auto** b) { return active_->PostSend(q, w, b); };
        f.post_jfr_wr = [](auto* q, auto* w, auto** b) { return active_->PostRecv(q, w, b); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        f.unimport_jetty = [](auto* q) { return active_->Unimport(q); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.unregister_seg = [](auto* q) { return active_->Unregister(q); };
        return f;
    }

protected:
    void SetUp() override {
        active_ = this;
        jetty_.jetty_id.id = 7;
        EXPECT_CALL(*this, CreateJfc(&ctx_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
        EXPECT_CALL(*this, CreateJfr(&ctx_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(*this, CreateJetty(&ctx_, _)).WillOnce(Return(&jetty_));
        EXPECT_CALL(*this, Register(&ctx_, _)).WillOnce([&](auto*, auto* cfg) {
            // 小于一页的 payload 仍按完整页注册，地址和长度均不能沿用普通堆分配的对齐。
            const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
            EXPECT_EQ(cfg->va % page, 0u);
            EXPECT_EQ(cfg->len % page, 0u);
            EXPECT_GE(cfg->len, 64u * 3);
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            return &seg_;
        });
        urma_device_cap_t cap{};
        cap.max_msg_size = 4096;
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 1;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        ASSERT_TRUE(session_.Open(&ctx_, cap, options_));
    }
    void ExpectClose(bool imported) {
        // 完成后先解除远端引用、销毁队列，再注销注册段；禁止在队列删除前释放 DMA 内存。
        ::testing::InSequence order;
        if (imported)
            EXPECT_CALL(*this, Unimport(&remote_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, Unregister(&seg_)).WillOnce(Return(URMA_SUCCESS));
    }
    static inline SendTestHardware* active_;
    raw::test_support::ScopedUrmaOverride scope_{Functions()};
    SendTestSession session_;
    SendTestOptions options_{64, 5, 3, 100};
    urma_context_t ctx_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_seg_t seg_{};
    urma_target_jetty_t remote_{};
};

TEST_F(SendTestHardware, ClientRunsMultipleBatchesAndChecksRemoteAcknowledgment) {
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    SendTestHello hello{.options = options_};
    hello.endpoint.id = 99;
    ASSERT_TRUE(peer.Write(EncodeHello(hello), 100));
    // 预置对端两批的 READY/ACK；没有 TCP payload，数据仍经 mock URMA 路径校验。
    for (auto marker : {3u, 3u, 5u, 5u})
        ASSERT_TRUE(peer.SendMarker(marker, 100));
    EXPECT_CALL(*this, Import(&ctx_, _, _)).WillOnce([&](auto*, auto* r, auto*) {
        EXPECT_EQ(r->jetty_id.id, 99u);
        EXPECT_EQ(r->tp_type, URMA_CTP);
        EXPECT_EQ(r->trans_mode, URMA_TM_RM);
        EXPECT_EQ(r->type, URMA_JETTY);
        return &remote_;
    });
    std::array<urma_cr_t, 3> completions{};
    int pending = 0;
    std::uint64_t sequence = 0;
    EXPECT_CALL(*this, PostSend(&jetty_, _, _)).Times(2).WillRepeatedly([&](auto*, auto* wr, auto**) {
        pending = 0;
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->tjetty, &remote_);
            const auto& sge = wr->send.src.sge[0];
            EXPECT_EQ(CheckPayload({reinterpret_cast<const std::byte*>(sge.addr), sge.len}), sequence++);
            auto& cr = completions[pending++];
            cr = {};
            cr.local_id = 7;
            cr.flag.bs.jetty = 1;
            cr.user_ctx = wr->user_ctx;
        }
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&tx_, _, _)).Times(2).WillRepeatedly([&](auto*, int, auto* cr) {
        std::copy_n(completions.begin(), pending, cr);
        return pending;
    });
    ASSERT_TRUE(session_.Run(client, false));
    EXPECT_EQ(sequence, 5u);
    ExpectClose(true);
    ASSERT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, ServerPrepostsAndValidatesOutOfOrderReceiveCompletions) {
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel server(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeHello({.options = options_}), 100));
    ASSERT_TRUE(peer.SendMarker(0xffffffff, 100));
    std::array<urma_cr_t, 3> completions{};
    int pending = 0;
    std::uint64_t sequence = 0;
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostRecv(&jfr_, _, _)).Times(2).WillRepeatedly([&](auto*, auto* wr, auto**) {
        pending = 0;
        for (; wr; wr = wr->next) {
            const auto& sge = wr->src.sge[0];
            FillPayload({reinterpret_cast<std::byte*>(sge.addr), sge.len}, sequence++);
            auto& cr = completions[pending++];
            cr = {};
            cr.flag.bs.s_r = 1;
            cr.completion_len = sge.len;
            cr.user_ctx = wr->user_ctx;
        }
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&rx_, _, _)).Times(2).WillRepeatedly([&](auto*, int, auto* cr) {
        // 完成顺序与投递顺序相反，接收校验按 user_ctx 找 buffer，并独立验证消息序号。
        for (int i = 0; i < pending; ++i)
            cr[i] = completions[pending - 1 - i];
        return pending;
    });
    ASSERT_TRUE(session_.Run(server, true));
    EXPECT_EQ(sequence, 5u);
    ExpectClose(false);
    ASSERT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, RegisterFailureWithoutErrnoIsReportedAndCanCleanUp) {
    ExpectClose(false);
    ASSERT_TRUE(session_.Close());
    ::testing::Mock::VerifyAndClearExpectations(this);
    EXPECT_CALL(*this, CreateJfc(&ctx_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(&ctx_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(&ctx_, _)).WillOnce(Return(&jetty_));
    // 重现 provider 返回 nullptr 但 errno 仍为 0；诊断不能将其显示为无错误。
    EXPECT_CALL(*this, Register(&ctx_, _)).WillOnce([](auto*, auto*) -> urma_target_seg_t* {
        errno = 0;
        return nullptr;
    });
    urma_device_cap_t cap{};
    cap.max_msg_size = 4096;
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 1;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    auto opened = session_.Open(&ctx_, cap, options_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EIO);
    EXPECT_THAT(opened.error().operation, ::testing::HasSubstr("without errno"));
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    ASSERT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, PeerMismatchFailsBeforePostingAndDeleteFailureCanRetry) {
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    auto wrong = options_;
    ++wrong.bytes;
    ASSERT_TRUE(peer.Write(EncodeHello({.options = wrong}), 100));
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
    EXPECT_FALSE(session_.Run(local, false));
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_EAGAIN));
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_FALSE(session_.Close());
    ::testing::Mock::VerifyAndClearExpectations(this);
    ExpectClose(false);
    ASSERT_TRUE(session_.Close());
}
} // namespace
} // namespace tools
} // namespace kbsocket
