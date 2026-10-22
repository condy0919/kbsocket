// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <utility>

#include "tools/rma_test/rma_test.hpp"
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

class RmaHardwareTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_status_t, Export, (urma_target_seg_t*, urma_seg_t**, std::uint32_t*));
    MOCK_METHOD(void, ReleaseExport, (urma_seg_t*));
    MOCK_METHOD(urma_target_seg_t*, ImportSeg,
                (urma_context_t*, urma_seg_t*, urma_token_t*, std::uint64_t, urma_import_seg_flag_t));
    MOCK_METHOD(urma_target_jetty_t*, ImportJetty, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));
    MOCK_METHOD(urma_status_t, UnimportSeg, (urma_target_seg_t*));
    MOCK_METHOD(urma_status_t, UnimportJetty, (urma_target_jetty_t*));

protected:
    enum class Fault {
        kNone,
        kWrongId,
        kWrongUserContext,
        kReceiveCompletion,
        kPseudoCompletion,
        kRnrCompletion,
        kMissingCompletion,
        kDuplicateCompletion,
        kCorruptPayload,
        kCorruptGuard,
        kUnknownAcceptance,
        kRejected,
        kUnknownStatus,
        kWrongRemote,
        kWrongImmediate,
        kBadReceivePayload,
    };
    struct Node {
        urma_context_t ctx{};
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        urma_jetty_t jetty{};
        urma_target_jetty_t target{};
        urma_target_seg_t segment{}, imported{}, rx_segment{};
        urma_seg_t exported{};
        std::size_t created_cqs = 0;
        std::size_t deleted_cqs = 0;
        bool live_memory = false;
        bool exported_released = false;
        bool deleted_jetty = false;
        bool deleted_jfr = false;
        std::deque<urma_cr_t> completions, receive_completions;
        struct Receive {
            urma_sge_t sge;
            std::uint64_t id;
        };
        std::deque<Receive> receives;
        bool rx_live = false;
    };
    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions functions;
        functions.create_jfc = [](auto* ctx, auto* cfg) { return current_->CreateJfc(ctx, cfg); };
        functions.create_jfr = [](auto* ctx, auto* cfg) { return current_->CreateJfr(ctx, cfg); };
        functions.create_jetty = [](auto* ctx, auto* cfg) { return current_->CreateJetty(ctx, cfg); };
        functions.register_seg = [](auto* ctx, auto* cfg) { return current_->Register(ctx, cfg); };
        functions.get_seg_ctx = [](auto* seg, auto** exported, auto* size) {
            return current_->Export(seg, exported, size);
        };
        functions.put_seg_ctx = [](auto* seg) { current_->ReleaseExport(seg); };
        functions.import_seg = [](auto* ctx, auto* seg, auto* token, std::uint64_t addr, urma_import_seg_flag_t flags) {
            return current_->ImportSeg(ctx, seg, token, addr, flags);
        };
        functions.import_jetty = [](auto* ctx, auto* cfg, auto* token) {
            return current_->ImportJetty(ctx, cfg, token);
        };
        functions.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) { return current_->Post(jetty, wr, bad); };
        functions.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) { return current_->PostRecv(jfr, wr, bad); };
        functions.poll_jfc = [](auto* cq, int count, auto* cr) { return current_->Poll(cq, count, cr); };
        functions.delete_jetty = [](auto* jetty) { return current_->DeleteJetty(jetty); };
        functions.delete_jfr = [](auto* jfr) { return current_->DeleteJfr(jfr); };
        functions.delete_jfc = [](auto* cq) { return current_->DeleteJfc(cq); };
        functions.unregister_seg = [](auto* seg) { return current_->Unregister(seg); };
        functions.unimport_seg = [](auto* seg) { return current_->UnimportSeg(seg); };
        functions.unimport_jetty = [](auto* jetty) { return current_->UnimportJetty(jetty); };
        return functions;
    }
    Node& ByContext(urma_context_t* ctx) {
        return nodes_[ctx == &nodes_[0].ctx ? 0 : 1];
    }
    void SetUp() override {
        current_ = this;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 1;
        cap_.max_jfs_depth = cap_.max_jfr_depth = 256;
        cap_.max_jfc_depth = 257;
        cap_.max_msg_size = 4096;
        cap_.max_read_size = cap_.max_write_size = 8U * 1024 * 1024;
        cap_.max_jfs_sge = cap_.max_jfr_sge = cap_.max_jfs_rsge = 1;
        nodes_[0].ctx.eid.raw[0] = 1;
        nodes_[1].ctx.eid.raw[0] = 2;
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
            return &node.jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            node.jetty.urma_ctx = ctx;
            node.jetty.jetty_cfg = *cfg;
            node.jetty.jetty_id.id = ctx == &nodes_[0].ctx ? 101 : 201;
            node.jetty.jetty_id.eid = ctx->eid;
            return &node.jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = ByContext(ctx);
            if (node.live_memory) {
                node.rx_segment.urma_ctx = ctx;
                node.rx_segment.seg.ubva.va = cfg->va;
                node.rx_segment.seg.len = cfg->len;
                node.rx_live = true;
                return &node.rx_segment;
            }
            node.segment.urma_ctx = ctx;
            node.segment.seg.ubva = {.eid = ctx->eid, .uasid = 1, .va = cfg->va};
            node.segment.seg.len = cfg->len;
            node.segment.seg.attr.value = cfg->flag.value;
            node.segment.seg.token_id = ctx == &nodes_[0].ctx ? 123 : 456;
            node.live_memory = true;
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<std::uint32_t>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            return &node.segment;
        });
        EXPECT_CALL(*this, Export(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* seg, auto** out, auto* size) {
                auto& node = ByContext(seg->urma_ctx);
                node.exported = seg->seg;
                *out = &node.exported;
                *size = sizeof(urma_seg_t);
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, ReleaseExport(_)).Times(AnyNumber()).WillRepeatedly([this](auto* seg) {
            auto& node = nodes_[seg == &nodes_[0].exported ? 0 : 1];
            EXPECT_FALSE(node.exported_released);
            node.exported_released = true;
        });
        EXPECT_CALL(*this, ImportSeg(_, _, _, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* ctx, auto* seg, auto* token, auto addr, auto flags) {
                auto& node = ByContext(ctx);
                EXPECT_EQ(seg->ubva.va, nodes_[0].segment.seg.ubva.va);
                EXPECT_EQ(seg->token_id, 123U);
                EXPECT_EQ(token->token, 0U);
                EXPECT_EQ(addr, 0U);
                EXPECT_EQ(flags.bs.mapping, static_cast<std::uint32_t>(URMA_SEG_NOMAP));
                EXPECT_EQ(flags.bs.access,
                          static_cast<std::uint32_t>(options_.operation == RmaOperation::kRead ? URMA_ACCESS_READ
                                                                                               : URMA_ACCESS_WRITE));
                node.imported.seg = *seg;
                return &node.imported;
            });
        EXPECT_CALL(*this, ImportJetty(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg, auto*) {
            EXPECT_EQ(cfg->tp_type, URMA_CTP);
            EXPECT_EQ(cfg->jetty_id.id, nodes_[0].jetty.jetty_id.id);
            return &ByContext(ctx).target;
        });
        ON_CALL(*this, Post(_, _, _)).WillByDefault([this](auto* jetty, auto* first, auto** bad) {
            std::lock_guard lock(mutex_);
            EXPECT_TRUE(nodes_[0].live_memory);
            EXPECT_TRUE(nodes_[1].live_memory);
            if (fault_ == Fault::kUnknownAcceptance || fault_ == Fault::kRejected) {
                *bad = fault_ == Fault::kRejected ? first : nullptr;
                return URMA_EINVAL;
            }
            std::uint32_t index = 0;
            for (auto* wr = first; wr; wr = wr->next, ++index) {
                if (partial_prefix_ >= 0 && index == static_cast<std::uint32_t>(partial_prefix_)) {
                    *bad = wr;
                    return URMA_ENOMEM;
                }
                const auto slot = index / (options_.mixed_send ? 2 : 1);
                const bool send = options_.mixed_send && index % 2 == 1;
                if (send) {
                    EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
                    EXPECT_EQ(wr->send.src.num_sge, 1U);
                    EXPECT_EQ(wr->send.src.sge[0].len, kRmaSendBytes);
                    EXPECT_EQ(wr->send.src.sge[0].tseg, &nodes_[1].segment);
                    auto& server = nodes_[0];
                    if (server.receives.empty()) {
                        ADD_FAILURE() << "SEND requires a posted RX before the ready barrier";
                        *bad = wr;
                        return URMA_EINVAL;
                    }
                    const auto receive = server.receives.front();
                    server.receives.pop_front();
                    auto* payload = reinterpret_cast<std::byte*>(receive.sge.addr);
                    std::memcpy(payload, reinterpret_cast<void*>(wr->send.src.sge[0].addr), kRmaSendBytes);
                    urma_cr_t rx{};
                    rx.flag.bs.s_r = 1;
                    rx.local_id = server.jfr.jfr_id.id;
                    rx.remote_id = jetty->jetty_id;
                    rx.user_ctx = receive.id;
                    rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                    rx.completion_len = kRmaSendBytes;
                    rx.imm_data = wr->send.imm_data;
                    if (fault_ == Fault::kWrongRemote) {
                        ++rx.remote_id.id;
                    } else if (fault_ == Fault::kWrongImmediate) {
                        ++rx.imm_data;
                    } else if (fault_ == Fault::kBadReceivePayload) {
                        payload[0] ^= std::byte{1};
                    }
                    if (reverse_cq_) {
                        server.receive_completions.push_front(rx);
                    } else {
                        server.receive_completions.push_back(rx);
                    }
                } else {
                    EXPECT_EQ(wr->rw.src.num_sge, 1U);
                    EXPECT_EQ(wr->rw.dst.num_sge, 1U);
                    EXPECT_EQ(wr->rw.src.sge[0].len, options_.bytes);
                    EXPECT_EQ(wr->rw.dst.sge[0].len, options_.bytes);
                    const bool read = options_.operation == RmaOperation::kRead;
                    EXPECT_EQ(wr->opcode, read ? URMA_OPC_READ : URMA_OPC_WRITE);
                    auto* source = reinterpret_cast<std::byte*>(wr->rw.src.sge[0].addr);
                    auto* destination = reinterpret_cast<std::byte*>(wr->rw.dst.sge[0].addr);
                    // 地址、长度和 tseg 都按独立槽核验，避免并发 WR 无意覆盖同一块内存。
                    EXPECT_EQ(wr->rw.src.sge[0].tseg, read ? &nodes_[1].imported : &nodes_[1].segment);
                    EXPECT_EQ(wr->rw.dst.sge[0].tseg, read ? &nodes_[1].segment : &nodes_[1].imported);
                    EXPECT_EQ(wr->rw.src.sge[0].addr,
                              (read ? nodes_[0].segment.seg.ubva.va : nodes_[1].segment.seg.ubva.va) + 64 +
                                  slot * RmaSlotStride(options_));
                    EXPECT_EQ(wr->rw.dst.sge[0].addr,
                              (read ? nodes_[1].segment.seg.ubva.va : nodes_[0].segment.seg.ubva.va) + 64 +
                                  slot * RmaSlotStride(options_));
                    std::memcpy(destination, source, options_.bytes);
                    delayed_source_ = source;
                    delayed_destination_ = destination;
                    std::copy_n(source, delayed_snapshot_.size(), delayed_snapshot_.begin());
                    pending_delay_polls_ = delay_polls_;
                    if (index == fault_index_ && fault_ == Fault::kCorruptPayload) {
                        destination[0] ^= std::byte{1};
                    }
                    if (index == fault_index_ && fault_ == Fault::kCorruptGuard) {
                        destination[-1] ^= std::byte{1};
                    }
                }
                urma_cr_t cr{};
                cr.status = URMA_CR_SUCCESS;
                cr.flag.bs.jetty = 1;
                cr.user_ctx = wr->user_ctx;
                cr.local_id = jetty->jetty_id.id;
                if (index == fault_index_) {
                    if (fault_ == Fault::kWrongId) {
                        ++cr.local_id;
                    } else if (fault_ == Fault::kWrongUserContext) {
                        cr.user_ctx += options_.window * 2;
                    } else if (fault_ == Fault::kReceiveCompletion) {
                        cr.flag.bs.s_r = 1;
                    } else if (fault_ == Fault::kPseudoCompletion) {
                        cr.status = URMA_CR_WR_FLUSH_ERR_DONE;
                        cr.user_ctx = 0xffffffffffffffffULL;
                    } else if (fault_ == Fault::kRnrCompletion) {
                        cr.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
                    } else if (fault_ == Fault::kUnknownStatus) {
                        cr.status = static_cast<urma_cr_status_t>(255);
                    }
                }
                auto& node = ByContext(jetty->urma_ctx);
                if (fault_ != Fault::kMissingCompletion || index != fault_index_) {
                    if (reverse_cq_) {
                        node.completions.push_front(cr);
                    } else {
                        node.completions.push_back(cr);
                    }
                }
                if (fault_ == Fault::kDuplicateCompletion && index == fault_index_) {
                    node.completions.push_back(cr);
                }
            }
            EXPECT_EQ(index, options_.window * (options_.mixed_send ? 2 : 1));
            *bad = nullptr;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostRecv(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* first, auto** bad) {
                std::lock_guard lock(mutex_);
                auto& node = ByContext(jfr->urma_ctx);
                EXPECT_TRUE(node.receives.empty());
                for (auto* wr = first; wr; wr = wr->next) {
                    EXPECT_EQ(wr->src.num_sge, 1U);
                    EXPECT_EQ(wr->src.sge[0].tseg, &node.rx_segment);
                    node.receives.push_back({wr->src.sge[0], wr->user_ctx});
                }
                EXPECT_EQ(node.receives.size(), options_.window);
                *bad = nullptr;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, Post(_, _, _)).Times(AnyNumber());
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* cq, int limit, auto* output) {
            std::lock_guard lock(mutex_);
            auto& node = ByContext(cq->urma_ctx);
            auto& completions = cq == &node.cqs[0] ? node.completions : node.receive_completions;
            if (!completions.empty() && pending_delay_polls_) {
                // CQE 未交还前，双方都不能按下一轮 pattern 改写仍在使用的 buffer。
                EXPECT_EQ(std::memcmp(delayed_source_, delayed_snapshot_.data(), delayed_snapshot_.size()), 0);
                EXPECT_EQ(std::memcmp(delayed_destination_, delayed_snapshot_.data(), delayed_snapshot_.size()), 0);
                --pending_delay_polls_;
                ++delay_checks_;
                return 0;
            }
            int count = 0;
            while (!completions.empty() && count < limit) {
                output[count++] = completions.front();
                completions.pop_front();
            }
            return count;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jetty) {
            EXPECT_TRUE(safe_to_close_);
            ByContext(jetty->urma_ctx).deleted_jetty = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jfr) {
            auto& node = ByContext(jfr->urma_ctx);
            EXPECT_TRUE(node.deleted_jetty);
            node.deleted_jfr = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([this](auto* cq) {
            auto& node = ByContext(cq->urma_ctx);
            EXPECT_TRUE(node.deleted_jfr);
            ++node.deleted_cqs;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([this](auto* seg) {
            auto& node = ByContext(seg->urma_ctx);
            if (seg == &node.rx_segment) {
                EXPECT_TRUE(safe_to_close_);
                EXPECT_TRUE(node.receives.empty());
                EXPECT_TRUE(node.receive_completions.empty());
                node.rx_live = false;
                return URMA_SUCCESS;
            }
            // 即便本地未 post，服务端 MR 也必须等双端静默，再等本地队列关闭后释放。
            EXPECT_TRUE(safe_to_close_);
            EXPECT_EQ(node.deleted_cqs, 2U);
            EXPECT_TRUE(node.live_memory);
            node.live_memory = false;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, UnimportSeg(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, UnimportJetty(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
    }

    void OpenPair() {
        ASSERT_TRUE(server_->Open(&nodes_[0].ctx, cap_, options_, true));
        ASSERT_TRUE(client_->Open(&nodes_[1].ctx, cap_, options_, false));
        EXPECT_TRUE(nodes_[0].exported_released);
        EXPECT_TRUE(nodes_[1].exported_released);
    }
    void RunPair() {
        std::array<int, 2> sockets;
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
        ControlChannel left(sockets[0]), right(sockets[1]);
        safe_to_close_ = false;
        std::thread server([&] { server_result_ = server_->Run(left); });
        client_result_ = client_->Run(right);
        server.join();
        safe_to_close_ = server_result_.has_value() && client_result_.has_value();
    }
    void ExpectRetained() {
        EXPECT_CALL(*this, UnimportSeg(_)).Times(0);
        EXPECT_CALL(*this, UnimportJetty(_)).Times(0);
        EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
        EXPECT_CALL(*this, Unregister(_)).Times(0);
        EXPECT_FALSE(client_->Close());
        EXPECT_FALSE(server_->Close());
        EXPECT_TRUE(nodes_[0].live_memory);
        EXPECT_TRUE(nodes_[1].live_memory);
    }
    static inline RmaHardwareTest* current_;
    std::array<Node, 2> nodes_;
    urma_device_cap_t cap_{};
    RmaOptions options_{.rounds = 3, .timeout_ms = 1000};
    Fault fault_ = Fault::kNone;
    std::uint32_t fault_index_ = 0;
    int partial_prefix_ = -1;
    bool reverse_cq_ = false;
    std::mutex mutex_;
    std::array<std::byte, 64> delayed_snapshot_{};
    const std::byte* delayed_source_ = nullptr;
    const std::byte* delayed_destination_ = nullptr;
    std::uint32_t delay_polls_ = 0;
    std::uint32_t pending_delay_polls_ = 0;
    std::uint32_t delay_checks_ = 0;
    bool safe_to_close_ = true;
    NoDestructor<RmaSession> server_, client_;
    std::expected<void, ToolError> server_result_, client_result_;
};

class RmaSizeTest : public RmaHardwareTest,
                    public ::testing::WithParamInterface<std::pair<RmaOperation, std::uint32_t>> {};

TEST_P(RmaSizeTest, OneWrPerRoundTransfersEntireSizeAndClosesAfterBothVerify) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.operation = GetParam().first;
    options_.bytes = GetParam().second;
    options_.timeout_ms = 5000;
    OpenPair();
    // 同一 WR 的 SGE 长度必须是完整 payload；mock Post 的次数同时防止偷偷按 4KiB 拆分。
    EXPECT_CALL(*this, Post(_, _, _)).Times(options_.rounds).WillRepeatedly(::testing::DoDefault());
    RunPair();
    ASSERT_TRUE(client_result_) << client_result_.error().operation;
    ASSERT_TRUE(server_result_) << server_result_.error().operation;
    EXPECT_EQ(client_->stats().accepted, options_.rounds);
    EXPECT_EQ(client_->stats().retired, options_.rounds);
    EXPECT_EQ(client_->stats().verified, options_.rounds);
    EXPECT_EQ(server_->stats().verified, options_.rounds);
    EXPECT_EQ(server_->stats().accepted, 0U);
    ASSERT_TRUE(client_->Close());
    ASSERT_TRUE(server_->Close());
    EXPECT_FALSE(nodes_[0].live_memory);
    EXPECT_FALSE(nodes_[1].live_memory);
}

INSTANTIATE_TEST_SUITE_P(
    ReadAndWrite, RmaSizeTest,
    ::testing::Values(std::pair{RmaOperation::kRead, 4096U}, std::pair{RmaOperation::kRead, 16384U},
                      std::pair{RmaOperation::kRead, 1024U * 1024}, std::pair{RmaOperation::kRead, 8U * 1024 * 1024},
                      std::pair{RmaOperation::kWrite, 4096U}, std::pair{RmaOperation::kWrite, 16384U},
                      std::pair{RmaOperation::kWrite, 1024U * 1024},
                      std::pair{RmaOperation::kWrite, 8U * 1024 * 1024}));

class RmaFailureTest : public RmaHardwareTest, public ::testing::WithParamInterface<int> {};

TEST_P(RmaFailureTest, DoesNotReleasePublishedMemoryOrReplayAfterFailure) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    fault_ = static_cast<Fault>(GetParam());
    options_.timeout_ms = 25;
    OpenPair();
    // CQE 损坏、缺失、payload 越界或 provider 拒绝均终止本轮；不开始第二轮，不重放第一轮。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(::testing::DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    ExpectRetained();
}

INSTANTIATE_TEST_SUITE_P(Failures, RmaFailureTest, ::testing::Range(1, 12));

TEST_F(RmaHardwareTest, DelayedCompletionKeepsBothBuffersUnchangedUntilTerminalProof) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    delay_polls_ = 3;
    options_.operation = RmaOperation::kWrite;
    OpenPair();
    RunPair();
    ASSERT_TRUE(client_result_);
    ASSERT_TRUE(server_result_);
    EXPECT_EQ(delay_checks_, options_.rounds * delay_polls_);
    ASSERT_TRUE(client_->Close());
    ASSERT_TRUE(server_->Close());
}

class RmaWindowTest : public RmaHardwareTest, public ::testing::WithParamInterface<std::pair<std::uint32_t, bool>> {};

TEST_P(RmaWindowTest, IndependentSlotsAllowReversedReadAndMixedSendCompletions) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = GetParam().first;
    options_.mixed_send = GetParam().second;
    options_.bytes = 16384;
    options_.timeout_ms = 5000;
    reverse_cq_ = true;
    OpenPair();
    // CQE 与提交顺序相反，最大窗口还跨多个 poll 批次；所有槽必须按 identity 恰好退休一次。
    EXPECT_CALL(*this, Post(_, _, _)).Times(options_.rounds).WillRepeatedly(::testing::DoDefault());
    RunPair();
    ASSERT_TRUE(client_result_) << client_result_.error().operation;
    ASSERT_TRUE(server_result_) << server_result_.error().operation;
    const auto rma_count = options_.window * options_.rounds;
    const auto send_count = options_.mixed_send ? rma_count : 0;
    EXPECT_EQ(client_->stats().accepted, rma_count + send_count);
    EXPECT_EQ(client_->stats().retired, rma_count + send_count);
    EXPECT_EQ(client_->stats().verified, rma_count);
    EXPECT_EQ(server_->stats().verified, rma_count);
    EXPECT_EQ(client_->stats().send_accepted, send_count);
    EXPECT_EQ(client_->stats().send_retired, send_count);
    EXPECT_EQ(server_->stats().send_received, send_count);
    EXPECT_TRUE(client_->stats().accepted_known);
    EXPECT_FALSE(client_->stats().pending);
    ASSERT_TRUE(client_->Close());
    ASSERT_TRUE(server_->Close());
    EXPECT_FALSE(nodes_[0].rx_live);
}

INSTANTIATE_TEST_SUITE_P(Windows, RmaWindowTest,
                         ::testing::Values(std::pair{2U, false}, std::pair{128U, false}, std::pair{1U, true},
                                           std::pair{4U, true}, std::pair{128U, true}));

TEST_F(RmaHardwareTest, PartialMixedBatchRetiresOnlyAcceptedPrefixAndNeverReplays) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 4;
    options_.mixed_send = true;
    options_.timeout_ms = 25;
    partial_prefix_ = 3;
    reverse_cq_ = true;
    OpenPair();
    // 接受 READ0/SEND0/READ1，拒绝 SEND1 及其后缀；不能把批次错误当成全部未接受。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(::testing::DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    EXPECT_EQ(client_->stats().accepted, 3U);
    EXPECT_EQ(client_->stats().retired, 3U);
    EXPECT_EQ(client_->stats().send_accepted, 1U);
    EXPECT_EQ(client_->stats().send_retired, 1U);
    EXPECT_EQ(client_->stats().partial_batches, 1U);
    EXPECT_TRUE(client_->stats().accepted_known);
    EXPECT_FALSE(client_->stats().pending);
    // 即使本地接受前缀都已完成，未达成双端静默、RX 仍在途，MR 仍必须保留。
    ExpectRetained();
}

TEST_F(RmaHardwareTest, SuccessWithBadWrDoesNotClaimAnyKnownAcceptanceBoundary) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 4;
    options_.mixed_send = true;
    options_.timeout_ms = 25;
    OpenPair();
    // status=SUCCESS 与非空 bad_wr 矛盾，不能把 bad_wr 位置当成可回收的后缀边界。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = wr->next;
        return URMA_SUCCESS;
    });
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(client_->stats().accepted_known);
    EXPECT_TRUE(client_->stats().pending);
    EXPECT_EQ(client_->stats().retired, 0U);
    ExpectRetained();
}

TEST_F(RmaHardwareTest, ForeignBadWrIsNotDereferencedOrTreatedAsRejected) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 4;
    options_.timeout_ms = 25;
    OpenPair();
    urma_jfs_wr_t foreign{};
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce([&](auto*, auto*, auto** bad) {
        *bad = &foreign;
        return URMA_ENOMEM;
    });
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(client_->stats().accepted_known);
    EXPECT_TRUE(client_->stats().pending);
    ExpectRetained();
}

TEST_F(RmaHardwareTest, ErrorCompletionDoesNotLoseOtherTerminalsInTheSamePollBatch) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 32;
    options_.timeout_ms = 25;
    fault_ = Fault::kRnrCompletion;
    OpenPair();
    // 第一个 CQE 报错，后面 31 条已被 poll 消费的成功 CQE 仍应退休，整批随后失败且不复用内存。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(::testing::DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_EQ(client_->stats().accepted, 32U);
    EXPECT_EQ(client_->stats().retired, 32U);
    EXPECT_FALSE(client_->stats().pending);
    EXPECT_EQ(client_->stats().verified, 0U);
    ExpectRetained();
}

TEST_F(RmaHardwareTest, UnknownStatusDoesNotProveDmaCompletion) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 4;
    options_.timeout_ms = 25;
    fault_ = Fault::kUnknownStatus;
    OpenPair();
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(::testing::DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_EQ(client_->stats().accepted, 4U);
    EXPECT_EQ(client_->stats().retired, 3U);
    EXPECT_TRUE(client_->stats().pending);
    ExpectRetained();
}

TEST_F(RmaHardwareTest, LaterSlotGuardCorruptionCannotHideBehindTheFirstSuccessfulSlot) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 4;
    options_.timeout_ms = 25;
    fault_ = Fault::kCorruptGuard;
    fault_index_ = 2;
    OpenPair();
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(::testing::DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_EQ(client_->stats().retired, 4U);
    ExpectRetained();
}

TEST_F(RmaHardwareTest, ConcurrentWindowKeepsLeasesUntilDelayedCompletionAndPeerVerification) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 8;
    delay_polls_ = 5;
    OpenPair();
    RunPair();
    ASSERT_TRUE(client_result_);
    ASSERT_TRUE(server_result_);
    EXPECT_EQ(delay_checks_, options_.rounds * delay_polls_);
    EXPECT_TRUE(client_->Close());
    EXPECT_TRUE(server_->Close());
}

class MixedReceiveFailureTest : public RmaHardwareTest, public ::testing::WithParamInterface<int> {
public:
    static constexpr int kWrongRemote = static_cast<int>(Fault::kWrongRemote);
    static constexpr int kWrongImmediate = static_cast<int>(Fault::kWrongImmediate);
    static constexpr int kBadPayload = static_cast<int>(Fault::kBadReceivePayload);
};

TEST_P(MixedReceiveFailureTest, BadPeerOrImmediateOrPayloadPreventsRoundConfirmation) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 4;
    options_.mixed_send = true;
    options_.timeout_ms = 25;
    fault_ = static_cast<Fault>(GetParam());
    OpenPair();
    // 正常 TX CQE 不能掩盖远端收到的 SEND 身份/数据损坏，服务端不得确认下一轮。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(::testing::DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    EXPECT_EQ(client_->stats().retired, 8U);
    ExpectRetained();
}
INSTANTIATE_TEST_SUITE_P(ReceiveValidation, MixedReceiveFailureTest,
                         ::testing::Values(MixedReceiveFailureTest::kWrongRemote,
                                           MixedReceiveFailureTest::kWrongImmediate,
                                           MixedReceiveFailureTest::kBadPayload));

TEST_F(RmaHardwareTest, PeerWindowMismatchStopsBeforeImportOrDma) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.window = 2;
    options_.timeout_ms = 25;
    ASSERT_TRUE(server_->Open(&nodes_[0].ctx, cap_, options_, true));
    auto client_options = options_;
    client_options.window = 3;
    ASSERT_TRUE(client_->Open(&nodes_[1].ctx, cap_, client_options, false));
    EXPECT_CALL(*this, ImportSeg(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*this, ImportJetty(_, _, _)).Times(0);
    EXPECT_CALL(*this, Post(_, _, _)).Times(0);
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    ExpectRetained();
}

TEST_F(RmaHardwareTest, UnsupportedCapabilityCreatesNoQueuesOrMemory) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    cap_.max_read_size = 0;
    EXPECT_CALL(*this, CreateJfc(_, _)).Times(0);
    EXPECT_CALL(*this, Register(_, _)).Times(0);
    auto opened = server_->Open(&nodes_[0].ctx, cap_, options_, true);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EOPNOTSUPP);
    EXPECT_TRUE(server_->Close());
}

TEST_F(RmaHardwareTest, ExportedDescriptorReleasedEvenWhenUnsupportedExtensionIsRejected) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    EXPECT_CALL(*this, Export(_, _, _)).WillOnce([this](auto* seg, auto** out, auto* size) {
        nodes_[0].exported = seg->seg;
        nodes_[0].exported.attr.bs.has_user_info = 1;
        *out = &nodes_[0].exported;
        *size = sizeof(urma_seg_t) + 8;
        return URMA_SUCCESS;
    });
    auto opened = server_->Open(&nodes_[0].ctx, cap_, options_, true);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EOPNOTSUPP);
    EXPECT_TRUE(nodes_[0].exported_released);
    // 未发布 MR，也未 post；这一阶段可安全关闭，不能因导出失败泄漏 provider 返回的描述符。
    ASSERT_TRUE(server_->Close());
}

TEST_F(RmaHardwareTest, UnimportFailureRetainsLocalQueueAndMemoryAndCannotRetryClose) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    OpenPair();
    RunPair();
    ASSERT_TRUE(client_result_);
    ASSERT_TRUE(server_result_);
    EXPECT_CALL(*this, UnimportSeg(_)).Times(1).WillOnce(Return(URMA_EINVAL));
    EXPECT_CALL(*this, DeleteJetty(&nodes_[1].jetty)).Times(0);
    EXPECT_CALL(*this, Unregister(&nodes_[1].segment)).Times(0);
    EXPECT_FALSE(client_->Close());
    EXPECT_FALSE(client_->Close());
    EXPECT_TRUE(nodes_[1].live_memory);
    EXPECT_TRUE(server_->Close());
}
} // namespace
} // namespace tools
} // namespace kbsocket
