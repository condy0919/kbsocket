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
    wire[7] = std::byte{1};
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
    EXPECT_FALSE(ValidateOptions({.bytes = 4097}));
    EXPECT_TRUE(ValidateOptions({.bytes = 4096}));
    EXPECT_FALSE(ValidateOptions({.messages = 0}));
    EXPECT_FALSE(ValidateOptions({.timeout_ms = 0}));
}

TEST(SendTestProtocol, SegmentedPayloadAndWireOptionsPreserveWholeMessageIdentity) {
    SendTestOptions options{.bytes = 4096, .sges = 8, .header_bytes = 33, .send_imm = true};
    ASSERT_TRUE(ValidateOptions(options));
    auto wire = EncodeHello({.options = options});
    auto decoded = DecodeHello(wire);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->options.sges, 8u);
    EXPECT_EQ(decoded->options.header_bytes, 33u);
    EXPECT_TRUE(decoded->options.send_imm);
    wire[51] = std::byte{2};
    EXPECT_FALSE(DecodeHello(wire));

    // 各段独立填充，接收端只按线上的连续报文校验；长度不能被 SGE 数整除也不丢尾部。
    std::array<std::byte, 4096> assembled{};
    std::size_t offset = 0;
    for (std::size_t piece = 0; piece < options.sges; ++piece) {
        const auto length = SendSgeLength(options, piece);
        FillPayload(std::span(assembled).subspan(offset, length), 1001, offset);
        offset += length;
    }
    EXPECT_EQ(offset, options.bytes);
    EXPECT_EQ(CheckPayload(assembled), 1001u);
    // 段偏移错 256 字节或复用相隔 256 条消息的数据都必须被识别。
    FillPayload(std::span(assembled).subspan(512, 256), 1001, 256);
    EXPECT_FALSE(CheckPayload(assembled));
    FillPayload(assembled, 1001);
    FillPayload(std::span(assembled).subspan(512, 256), 745, 512);
    EXPECT_FALSE(CheckPayload(assembled));
    EXPECT_NE(SendImmediate(1), SendImmediate(257));
    EXPECT_NE(SendImmediate(1) >> 32, 0u);

    EXPECT_FALSE(ValidateOptions({.sges = 0}));
    EXPECT_FALSE(ValidateOptions({.sges = 9}));
    EXPECT_FALSE(ValidateOptions({.bytes = 32, .sges = 2}));
    EXPECT_FALSE(ValidateOptions({.bytes = 64, .sges = 2, .header_bytes = 7}));
    EXPECT_FALSE(ValidateOptions({.bytes = 35, .sges = 8, .header_bytes = 32}));
    EXPECT_TRUE(ValidateOptions({.bytes = 8}));
    EXPECT_TRUE(ValidateOptions({.bytes = 9, .sges = 2, .header_bytes = 8}));
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
    }
    void Open(bool server = false) {
        jetty_.jetty_id.id = 7;
        EXPECT_CALL(*this, CreateJfc(&ctx_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
        EXPECT_CALL(*this, CreateJfr(&ctx_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(*this, CreateJetty(&ctx_, _)).WillOnce([&](auto*, auto* cfg) {
            EXPECT_EQ(cfg->jfs_cfg.max_sge, options_.sges);
            return &jetty_;
        });
        std::size_t next_segment = 0;
        EXPECT_CALL(*this, Register(&ctx_, _)).Times(server ? 1 : options_.sges).WillRepeatedly([&](auto*, auto* cfg) {
            // 小于一页的 payload 仍按完整页注册，地址和长度均不能沿用普通堆分配的对齐。
            const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
            EXPECT_EQ(cfg->va % page, 0u);
            EXPECT_EQ(cfg->len % page, 0u);
            EXPECT_GE(cfg->len, 64u * 3);
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            return Segment(next_segment++);
        });
        urma_device_cap_t cap{};
        cap.max_msg_size = 4096;
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 1;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = options_.sges;
        cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        ASSERT_TRUE(session_.Open(&ctx_, cap, options_, server));
    }
    void ExpectClose(bool imported, bool server = false) {
        // RX 池已收到全部终结 CQE 且 lease 已归还，先注销并解绑，才允许关闭父队列。
        ::testing::InSequence order;
        if (imported) {
            EXPECT_CALL(*this, Unimport(&remote_)).WillOnce(Return(URMA_SUCCESS));
        }
        if (server) {
            EXPECT_CALL(*this, Unregister(&seg_)).WillOnce(Return(URMA_SUCCESS));
        }
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
        if (!server) {
            for (std::size_t i = 0; i < options_.sges; ++i) {
                EXPECT_CALL(*this, Unregister(Segment(i))).WillOnce(Return(URMA_SUCCESS));
            }
        }
    }
    urma_target_seg_t* Segment(std::size_t index) {
        return index ? &extra_segments_[index - 1] : &seg_;
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
    std::array<urma_target_seg_t, kMaxSendSges - 1> extra_segments_{};
    urma_target_jetty_t remote_{};
};

TEST_F(SendTestHardware, ClientRunsMultipleBatchesAndChecksRemoteAcknowledgment) {
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    SendTestHello hello{.options = options_};
    hello.endpoint.id = 99;
    ASSERT_TRUE(peer.Write(EncodeHello(hello), 100));
    // 预置对端两批的 READY/ACK；没有 TCP payload，数据仍经 mock URMA 路径校验。
    for (auto marker : {3u, 3u, 5u, 5u}) {
        ASSERT_TRUE(peer.SendMarker(marker, 100));
    }
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

TEST_F(SendTestHardware, MultipleRegisteredSgesProduceOneOrderedMessageAndImmediatePerWr) {
    options_.bytes = 4096;
    options_.sges = 3;
    options_.header_bytes = 33;
    options_.send_imm = true;
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeHello({.options = options_}), 100));
    for (auto marker : {3u, 3u, 5u, 5u}) {
        ASSERT_TRUE(peer.SendMarker(marker, 100));
    }
    EXPECT_CALL(*this, Import(&ctx_, _, _)).WillOnce(Return(&remote_));
    std::array<urma_cr_t, 3> completions{};
    int pending = 0;
    std::uint64_t sequence = 0;
    EXPECT_CALL(*this, PostSend(&jetty_, _, _)).Times(2).WillRepeatedly([&](auto*, auto* wr, auto**) {
        pending = 0;
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
            EXPECT_EQ(wr->send.imm_data, SendImmediate(sequence));
            EXPECT_EQ(wr->send.src.num_sge, 3u);
            std::array<std::byte, 4096> assembled{};
            std::size_t offset = 0;
            for (std::size_t piece = 0; piece < 3; ++piece) {
                const auto& sge = wr->send.src.sge[piece];
                EXPECT_EQ(sge.tseg, Segment(piece));
                EXPECT_EQ(sge.len, SendSgeLength(options_, piece));
                const auto* data = reinterpret_cast<const std::byte*>(sge.addr);
                EXPECT_EQ(data[-1], std::byte{0xa5});
                EXPECT_EQ(data[sge.len], std::byte{0xa5});
                std::copy_n(data, sge.len, assembled.begin() + offset);
                offset += sge.len;
            }
            // 仅测试接收模拟端做拼接；实际工具提交分散 MR 中的原始 SGE，不生成连续 TX 副本。
            EXPECT_EQ(offset, options_.bytes);
            EXPECT_EQ(CheckPayload(assembled), sequence++);
            auto& cr = completions[pending++];
            cr = {};
            cr.local_id = 7;
            cr.flag.bs.jetty = 1;
            cr.user_ctx = wr->user_ctx;
        }
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&tx_, _, _)).Times(2).WillRepeatedly([&](auto*, int, auto* cr) {
        std::reverse_copy(completions.begin(), completions.begin() + pending, cr);
        return pending;
    });
    ASSERT_TRUE(session_.Run(client, false));
    EXPECT_EQ(session_.stats().accepted, 5u);
    EXPECT_EQ(session_.stats().retired, 5u);
    EXPECT_EQ(session_.stats().guarded_segments, 15u);
    ExpectClose(true);
    ASSERT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, GuardCorruptionFailsAfterRetirementWithoutClaimingDmaIsPending) {
    options_.sges = 2;
    options_.messages = 3;
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeHello({.options = options_}), 100));
    ASSERT_TRUE(peer.SendMarker(3, 100));
    EXPECT_CALL(*this, Import(&ctx_, _, _)).WillOnce(Return(&remote_));
    std::array<urma_cr_t, 3> completions{};
    EXPECT_CALL(*this, PostSend(&jetty_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        // 模拟越界破坏第二段尾部，正常 CQE 不能掩盖 guard 失败。
        auto& sge = wr->send.src.sge[1];
        reinterpret_cast<std::byte*>(sge.addr)[sge.len] = std::byte{};
        std::size_t index = 0;
        for (; wr; wr = wr->next, ++index) {
            completions[index].local_id = 7;
            completions[index].flag.bs.jetty = 1;
            completions[index].user_ctx = wr->user_ctx;
        }
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&tx_, _, _)).WillOnce([&](auto*, int, auto* cr) {
        std::copy(completions.begin(), completions.end(), cr);
        return 3;
    });
    auto result = session_.Run(client, false);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EILSEQ);
    EXPECT_THAT(result.error().operation, ::testing::HasSubstr("guard"));
    EXPECT_EQ(session_.stats().retired, 3u);
    ExpectClose(true);
    EXPECT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, UnsupportedSgeCapabilityFailsBeforeCreatingQueues) {
    options_.sges = 2;
    urma_device_cap_t cap{};
    cap.max_msg_size = 4096;
    cap.max_jfs_sge = 1;
    EXPECT_CALL(*this, CreateJfc(_, _)).Times(0);
    EXPECT_CALL(*this, Register(_, _)).Times(0);
    auto result = session_.Open(&ctx_, cap, options_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EINVAL);
    EXPECT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, ServerPrepostsAndValidatesOutOfOrderReceiveCompletions) {
    options_.sges = 2;
    options_.send_imm = true;
    Open(true);
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
            EXPECT_EQ(sge.len, raw::RxBufferPool::kBufferSize);
            FillPayload({reinterpret_cast<std::byte*>(sge.addr), options_.bytes}, sequence++);
            auto& cr = completions[pending++];
            cr = {};
            cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
            cr.imm_data = SendImmediate(sequence - 1);
            cr.flag.bs.s_r = 1;
            cr.completion_len = options_.bytes;
            cr.flag.bs.jetty = 1;
            cr.local_id = jetty_.jetty_id.id;
            cr.user_ctx = wr->user_ctx;
        }
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&rx_, _, _)).Times(2).WillRepeatedly([&](auto*, int, auto* cr) {
        // 完成顺序与投递顺序相反，接收校验按 user_ctx 找 buffer，并独立验证消息序号。
        for (int i = 0; i < pending; ++i) {
            cr[i] = completions[pending - 1 - i];
        }
        return pending;
    });
    ASSERT_TRUE(session_.Run(server, true));
    EXPECT_EQ(sequence, 5u);
    EXPECT_EQ(session_.stats().received, 5u);
    ExpectClose(false, true);
    ASSERT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, ServerReturnsEveryLeaseWhenOnePayloadIsCorrupt) {
    Open(true);
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel server(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeHello({.options = options_}), 100));
    std::array<urma_cr_t, 3> completions{};
    EXPECT_CALL(*this, PostRecv(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        std::size_t index = 0;
        for (; wr; wr = wr->next, ++index) {
            auto* data = reinterpret_cast<std::byte*>(wr->src.sge[0].addr);
            FillPayload({data, options_.bytes}, index);
            if (!index) {
                data[8] ^= std::byte{1};
            }
            auto& cr = completions[index];
            cr.flag.bs.s_r = 1;
            cr.flag.bs.jetty = 1;
            cr.local_id = jetty_.jetty_id.id;
            cr.user_ctx = wr->user_ctx;
            cr.completion_len = options_.bytes;
        }
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Poll(&rx_, _, _)).WillOnce([&](auto*, int, auto* cr) {
        std::copy(completions.begin(), completions.end(), cr);
        return 3;
    });
    // 第一个 payload 失败后，同批其他完成已从 CQ 取走，必须一并归还才能关闭 RX 池。
    auto result = session_.Run(server, true);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EILSEQ);
    ExpectClose(false, true);
    EXPECT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, ServerRejectsWrongImmediateOrSourceAndReleasesWholeCompletionBatch) {
    for (bool wrong_source : {false, true}) {
        options_.sges = 2;
        options_.send_imm = true;
        Open(true);
        int fds[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
        ControlChannel server(fds[0]), peer(fds[1]);
        SendTestHello hello{.options = options_};
        hello.endpoint.id = 99;
        hello.endpoint.eid.raw[15] = 42;
        ASSERT_TRUE(peer.Write(EncodeHello(hello), 100));
        std::array<urma_cr_t, 3> completions{};
        EXPECT_CALL(*this, PostRecv(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
            std::size_t index = 0;
            for (; wr; wr = wr->next, ++index) {
                FillPayload({reinterpret_cast<std::byte*>(wr->src.sge[0].addr), options_.bytes}, index);
                auto& cr = completions[index];
                cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                cr.imm_data = SendImmediate(index);
                cr.remote_id = hello.endpoint;
                cr.flag.bs.s_r = cr.flag.bs.jetty = 1;
                cr.local_id = jetty_.jetty_id.id;
                cr.user_ctx = wr->user_ctx;
                cr.completion_len = options_.bytes;
            }
            // 首条 CQE 的载荷完整，但通知或来源错误；同批后续 lease 仍必须归还。
            if (wrong_source) {
                completions[0].remote_id.eid.raw[15] ^= 1;
            } else {
                completions[0].imm_data ^= 1ULL << 48;
            }
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Poll(&rx_, _, _)).WillOnce([&](auto*, int, auto* cr) {
            std::copy(completions.begin(), completions.end(), cr);
            return 3;
        });
        auto result = session_.Run(server, true);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, EPROTO);
        EXPECT_EQ(session_.stats().received, 3u);
        ExpectClose(false, true);
        EXPECT_TRUE(session_.Close());
        ::testing::Mock::VerifyAndClearExpectations(this);
    }
}

TEST_F(SendTestHardware, PartialPostRetainsEveryRegisteredSegmentAndDoesNotReplay) {
    options_.sges = 2;
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeHello({.options = options_}), 100));
    ASSERT_TRUE(peer.SendMarker(3, 100));
    EXPECT_CALL(*this, Import(&ctx_, _, _)).WillOnce(Return(&remote_));
    EXPECT_CALL(*this, PostSend(&jetty_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        // 只接受第一个 WR，第二个及以后未接受；不得把部分成功当作可以重放整批。
        *bad = wr->next;
        return URMA_ENOMEM;
    });
    EXPECT_CALL(*this, Unimport(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    auto result = session_.Run(client, false);
    ASSERT_FALSE(result);
    EXPECT_EQ(session_.stats().accepted, 1u);
    EXPECT_TRUE(session_.stats().accepted_known);
    EXPECT_EQ(session_.stats().retired, 0u);
    auto closed = session_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, EBUSY);
}

TEST_F(SendTestHardware, UnknownPostBoundaryRetainsAllSgesUntilProcessExit) {
    options_.sges = 2;
    Open();
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel client(fds[0]), peer(fds[1]);
    ASSERT_TRUE(peer.Write(EncodeHello({.options = options_}), 100));
    ASSERT_TRUE(peer.SendMarker(3, 100));
    EXPECT_CALL(*this, Import(&ctx_, _, _)).WillOnce(Return(&remote_));
    EXPECT_CALL(*this, PostSend(&jetty_, _, _)).WillOnce([](auto*, auto*, auto** bad) {
        // 返回失败但没有可信 bad_wr，整批 DMA 归属未知，不能注销任一段。
        *bad = nullptr;
        return URMA_ENOMEM;
    });
    EXPECT_CALL(*this, Unimport(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_FALSE(session_.Run(client, false));
    EXPECT_FALSE(session_.stats().accepted_known);
    auto closed = session_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, EBUSY);
}

TEST_F(SendTestHardware, LayoutMismatchIsRejectedBeforeAnyDataWork) {
    for (unsigned mismatch = 0; mismatch < 3; ++mismatch) {
        options_.sges = 2;
        Open();
        int fds[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
        ControlChannel client(fds[0]), peer(fds[1]);
        auto other = options_;
        if (!mismatch) {
            other.sges = 3;
        } else if (mismatch == 1) {
            other.header_bytes = 31;
        } else {
            other.send_imm = true;
        }
        ASSERT_TRUE(peer.Write(EncodeHello({.options = other}), 100));
        EXPECT_CALL(*this, Import(_, _, _)).Times(0);
        EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
        auto result = session_.Run(client, false);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, EPROTO);
        ExpectClose(false);
        EXPECT_TRUE(session_.Close());
        ::testing::Mock::VerifyAndClearExpectations(this);
    }
}

TEST_F(SendTestHardware, LaterRegistrationFailureCleansOnlyThePreviouslyRegisteredSegments) {
    options_.sges = 2;
    EXPECT_CALL(*this, CreateJfc(&ctx_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
    EXPECT_CALL(*this, CreateJfr(&ctx_, _)).WillOnce(Return(&jfr_));
    EXPECT_CALL(*this, CreateJetty(&ctx_, _)).WillOnce(Return(&jetty_));
    EXPECT_CALL(*this, Register(&ctx_, _)).WillOnce(Return(&seg_)).WillOnce([](auto*, auto*) -> urma_target_seg_t* {
        errno = ENOMEM;
        return nullptr;
    });
    urma_device_cap_t cap{};
    cap.max_msg_size = 4096;
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.max_jetty = 1;
    cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
    cap.max_jfs_sge = 2;
    cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    auto result = session_.Open(&ctx_, cap, options_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ENOMEM);
    // 尚未提交 WR，失败的第二段无注销对象；第一段仍须显式注销后才释放其内存。
    ::testing::InSequence order;
    EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_CALL(*this, Unregister(&seg_)).WillOnce(Return(URMA_SUCCESS));
    EXPECT_TRUE(session_.Close());
}

TEST_F(SendTestHardware, RegisterFailureWithoutErrnoIsReportedAndCanCleanUp) {
    Open();
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
    Open();
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
