// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>

#include "tools/rma_failure_test/rma_failure_test.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoDefault;
using ::testing::Return;

class MockRmaFailurePeer : public RmaFailurePeer {
public:
    RmaHello descriptor;
    const RmaHello& hello() const noexcept override {
        return descriptor;
    }
    MOCK_METHOD((std::expected<void, ToolError>), FinishBaselineAndExit, (), (override));
};

class RmaFailureHardwareTest : public ::testing::Test {
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
    MOCK_METHOD(urma_status_t, Query, (urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t*));
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
        kHealthyError,
        kDuplicateCompletion,
        kCorruptPayload,
        kCorruptGuard,
        kUnknownAcceptance,
        kUnknownStatus,
        kMissingCompletion,
        kCorruptSourcePayload,
        kCorruptSourceGuard,
        kSkipWrite,
    };
    struct Node {
        urma_context_t ctx{};
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        urma_jetty_t jetty{};
        urma_target_seg_t segment{};
        urma_seg_t exported{};
        std::array<urma_target_seg_t, 2> imported{};
        std::array<urma_target_jetty_t, 2> targets{};
        std::deque<urma_cr_t> completions;
        std::size_t cq_count = 0;
        std::size_t deleted_cqs = 0;
        bool deleted_jetty = false;
        bool deleted_jfr = false;
        bool live_memory = false;
        bool released_export = false;
    };
    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions functions;
        functions.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        functions.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        functions.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        functions.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        functions.get_seg_ctx = [](auto* s, auto** out, auto* len) { return active_->Export(s, out, len); };
        functions.put_seg_ctx = [](auto* s) { active_->ReleaseExport(s); };
        functions.import_seg = [](auto* c, auto* s, auto* t, auto a, auto f) {
            return active_->ImportSeg(c, s, t, a, f);
        };
        functions.import_jetty = [](auto* c, auto* r, auto* t) { return active_->ImportJetty(c, r, t); };
        functions.post_jetty_send_wr = [](auto* q, auto* w, auto** b) { return active_->Post(q, w, b); };
        functions.query_jetty = [](auto* q, auto* c, auto* a) { return active_->Query(q, c, a); };
        functions.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        functions.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        functions.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        functions.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        functions.unregister_seg = [](auto* s) { return active_->Unregister(s); };
        functions.unimport_seg = [](auto* s) { return active_->UnimportSeg(s); };
        functions.unimport_jetty = [](auto* q) { return active_->UnimportJetty(q); };
        return functions;
    }
    Node& Find(urma_context_t* ctx) {
        return nodes_.at(ctx->eid.raw[0] - 1);
    }
    void SetUp() override {
        active_ = this;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 1;
        cap_.max_jfs_depth = cap_.max_jfr_depth = 1024;
        cap_.max_jfc_depth = 1025;
        cap_.max_msg_size = 4096;
        cap_.max_read_size = cap_.max_write_size = 8U * 1024 * 1024;
        cap_.max_jfs_sge = cap_.max_jfr_sge = cap_.max_jfs_rsge = 1;
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            nodes_[i].ctx.eid.raw[0] = static_cast<std::uint8_t>(i + 1);
        }
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = Find(ctx);
            auto& cq = node.cqs.at(node.cq_count++);
            cq.urma_ctx = ctx;
            cq.jfc_cfg = *cfg;
            return &cq;
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = Find(ctx);
            node.jfr.urma_ctx = ctx;
            node.jfr.jfr_cfg = *cfg;
            return &node.jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = Find(ctx);
            node.jetty.urma_ctx = ctx;
            node.jetty.jetty_cfg = *cfg;
            node.jetty.jetty_id.eid = ctx->eid;
            node.jetty.jetty_id.id = ctx->eid.raw[0] * 100;
            return &node.jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg) {
            auto& node = Find(ctx);
            EXPECT_FALSE(node.live_memory);
            node.segment.urma_ctx = ctx;
            node.segment.seg.ubva = {.eid = ctx->eid, .uasid = 1, .va = cfg->va};
            node.segment.seg.len = cfg->len;
            node.segment.seg.attr.value = cfg->flag.value;
            node.segment.seg.token_id = ctx->eid.raw[0];
            node.live_memory = true;
            return &node.segment;
        });
        EXPECT_CALL(*this, Export(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* seg, auto** out, auto* size) {
                auto& node = Find(seg->urma_ctx);
                node.exported = seg->seg;
                *out = &node.exported;
                *size = sizeof(urma_seg_t);
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, ReleaseExport(_)).Times(AnyNumber()).WillRepeatedly([this](auto* seg) {
            auto& node = nodes_.at(seg->ubva.eid.raw[0] - 1);
            EXPECT_FALSE(node.released_export);
            node.released_export = true;
        });
        EXPECT_CALL(*this, ImportSeg(_, _, _, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* ctx, auto* seg, auto* token, auto address, auto flags) {
                EXPECT_EQ(ctx, &nodes_[2].ctx);
                EXPECT_EQ(token->token, 0U);
                EXPECT_EQ(address, 0U);
                EXPECT_EQ(flags.bs.mapping, static_cast<unsigned>(URMA_SEG_NOMAP));
                EXPECT_EQ(flags.bs.access, static_cast<unsigned>(URMA_ACCESS_WRITE));
                auto& target = Find(ctx).imported.at(seg->ubva.eid.raw[0] - 1);
                target.urma_ctx = ctx;
                target.seg = *seg;
                return &target;
            });
        EXPECT_CALL(*this, ImportJetty(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto* cfg, auto*) {
            EXPECT_EQ(ctx, &nodes_[2].ctx);
            EXPECT_EQ(cfg->tp_type, URMA_CTP);
            return &Find(ctx).targets.at(cfg->jetty_id.eid.raw[0] - 1);
        });
        ON_CALL(*this, Post(_, _, _)).WillByDefault([this](auto* jetty, auto* first, auto** bad) {
            std::lock_guard lock(mutex_);
            EXPECT_EQ(jetty, &nodes_[2].jetty);
            if (baseline_post_delay_ms_ && first->next == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds(baseline_post_delay_ms_));
            }
            if (fault_ == Fault::kUnknownAcceptance) {
                *bad = nullptr;
                return URMA_ENOMEM;
            }
            std::size_t index = 0;
            for (auto* wr = first; wr != nullptr; wr = wr->next, ++index) {
                if (partial_prefix_ >= 0 && index == static_cast<std::size_t>(partial_prefix_)) {
                    *bad = wr;
                    return URMA_ENOMEM;
                }
                EXPECT_EQ(wr->opcode, URMA_OPC_WRITE);
                EXPECT_EQ(wr->rw.src.num_sge, 1U);
                EXPECT_EQ(wr->rw.dst.num_sge, 1U);
                const std::size_t target = wr->tjetty == &nodes_[2].targets[1] ? 1 : 0;
                EXPECT_EQ(wr->tjetty, &nodes_[2].targets[target]);
                EXPECT_EQ(wr->rw.src.sge->tseg, &nodes_[2].segment);
                EXPECT_EQ(wr->rw.dst.sge->tseg, &nodes_[2].imported[target]);
                EXPECT_EQ(wr->rw.src.sge->len, options_.bytes);
                EXPECT_EQ(wr->rw.dst.sge->len, options_.bytes);
                EXPECT_GE(wr->rw.src.sge->addr, nodes_[2].segment.seg.ubva.va);
                EXPECT_LE(wr->rw.src.sge->addr + options_.bytes,
                          nodes_[2].segment.seg.ubva.va + nodes_[2].segment.seg.len);
                EXPECT_GE(wr->rw.dst.sge->addr, nodes_[target].segment.seg.ubva.va);
                EXPECT_LE(wr->rw.dst.sge->addr + options_.bytes,
                          nodes_[target].segment.seg.ubva.va + nodes_[target].segment.seg.len);
                if (target == 1 && exited_) {
                    ++fault_writes_after_exit_;
                }
                auto* destination = reinterpret_cast<std::byte*>(wr->rw.dst.sge->addr);
                const bool remote_failed = target == 1 && exited_;
                if ((!remote_failed || fault_status_ == URMA_CR_SUCCESS) &&
                    (fault_ != Fault::kSkipWrite || index != fault_index_)) {
                    std::memcpy(destination, reinterpret_cast<void*>(wr->rw.src.sge->addr), options_.bytes);
                }
                urma_cr_t completion{};
                completion.status = remote_failed ? fault_status_ : URMA_CR_SUCCESS;
                completion.flag.bs.jetty = 1;
                completion.user_ctx = wr->user_ctx;
                completion.local_id = jetty->jetty_id.id;
                if (index == fault_index_) {
                    switch (fault_) {
                    case Fault::kWrongId:
                        ++completion.local_id;
                        break;
                    case Fault::kWrongUserContext:
                        completion.user_ctx = std::numeric_limits<std::uint64_t>::max();
                        break;
                    case Fault::kReceiveCompletion:
                        completion.flag.bs.s_r = 1;
                        break;
                    case Fault::kPseudoCompletion:
                        completion.status = URMA_CR_WR_FLUSH_ERR_DONE;
                        completion.user_ctx = std::numeric_limits<std::uint64_t>::max();
                        break;
                    case Fault::kHealthyError:
                        completion.status = URMA_CR_ACK_TIMEOUT_ERR;
                        break;
                    case Fault::kUnknownStatus:
                        completion.status = static_cast<urma_cr_status_t>(255);
                        break;
                    case Fault::kCorruptPayload:
                        destination[0] ^= std::byte{1};
                        break;
                    case Fault::kCorruptGuard:
                        destination[-1] ^= std::byte{1};
                        break;
                    case Fault::kCorruptSourcePayload:
                        reinterpret_cast<std::byte*>(wr->rw.src.sge->addr)[0] ^= std::byte{1};
                        break;
                    case Fault::kCorruptSourceGuard:
                        reinterpret_cast<std::byte*>(wr->rw.src.sge->addr)[-1] ^= std::byte{1};
                        break;
                    default:
                        break;
                    }
                }
                if (fault_ != Fault::kMissingCompletion || index != fault_index_) {
                    if (reverse_) {
                        nodes_[2].completions.push_front(completion);
                    } else {
                        nodes_[2].completions.push_back(completion);
                    }
                }
                if (fault_ == Fault::kDuplicateCompletion && index == fault_index_) {
                    nodes_[2].completions.push_back(completion);
                }
            }
            *bad = nullptr;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Post(_, _, _)).Times(AnyNumber());
        EXPECT_CALL(*this, Query(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jetty, auto* cfg, auto* attr) {
                *cfg = jetty->jetty_cfg;
                attr->state = query_state_;
                return URMA_SUCCESS;
            });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* cq, int limit, auto* out) {
            std::lock_guard lock(mutex_);
            auto& queue = Find(cq->urma_ctx).completions;
            const auto count = std::min({limit, poll_limit_, static_cast<int>(queue.size())});
            for (int i = 0; i < count; ++i) {
                out[i] = queue.front();
                queue.pop_front();
            }
            return count;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jetty) {
            Find(jetty->urma_ctx).deleted_jetty = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([this](auto* jfr) {
            auto& node = Find(jfr->urma_ctx);
            EXPECT_TRUE(node.deleted_jetty);
            node.deleted_jfr = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([this](auto* cq) {
            auto& node = Find(cq->urma_ctx);
            EXPECT_TRUE(node.deleted_jfr);
            ++node.deleted_cqs;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([this](auto* segment) {
            auto& node = Find(segment->urma_ctx);
            EXPECT_EQ(node.deleted_cqs, 2U);
            node.live_memory = false;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, UnimportSeg(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, UnimportJetty(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
    }
    void Open() {
        ASSERT_TRUE(server_->Open(&nodes_[0].ctx, cap_, options_, true));
        ASSERT_TRUE(fault_target_->Open(&nodes_[1].ctx, cap_, options_, RmaFailureRole::kFaultTarget));
        ASSERT_TRUE(client_->Open(&nodes_[2].ctx, cap_, options_, false));
        fault_peer_.descriptor = fault_target_->hello();
    }
    void Import() {
        ASSERT_TRUE(client_->resources.Import({server_->resources.hello(), fault_target_->hello()}));
    }
    void RunPair() {
        std::array<int, 2> sockets{};
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
        ControlChannel server_channel(sockets[0]), client_channel(sockets[1]);
        std::thread server([&] { server_result_ = server_->Run(server_channel, &fault_peer_); });
        client_result_ = client_->Run(client_channel);
        server.join();
    }
    void ExpectExitAfterBaselines() {
        EXPECT_CALL(fault_peer_, FinishBaselineAndExit()).Times(1).WillOnce([this]() -> std::expected<void, ToolError> {
            // 两条 WRITE 的本地完成不能替代远端校验；先验证故障目标基线，退出确认后客户端才能计入 verified。
            EXPECT_EQ(client_->resources.stats.healthy_baseline.verified, 1U);
            EXPECT_EQ(client_->resources.stats.fault_baseline.verified, 0U);
            EXPECT_EQ(client_->resources.stats.fault_baseline.retired, 1U);
            EXPECT_EQ(client_->resources.stats.fault_mixed.accepted, 0U);
            EXPECT_EQ(fault_writes_after_exit_, 0U);
            EXPECT_TRUE(fault_target_->VerifyTarget(RmaFailurePhase::kFaultBaseline));
            if (exit_confirmation_delay_ms_) {
                std::this_thread::sleep_for(std::chrono::milliseconds(exit_confirmation_delay_ms_));
            }
            exited_ = true;
            return {};
        });
    }
    void ExpectSessionRetained() {
        EXPECT_CALL(*this, UnimportSeg(_)).Times(0);
        EXPECT_CALL(*this, UnimportJetty(_)).Times(0);
        EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
        EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
        EXPECT_CALL(*this, Unregister(_)).Times(0);
        EXPECT_FALSE(client_->can_close());
        EXPECT_FALSE(server_->can_close());
        EXPECT_FALSE(client_->Close());
        EXPECT_FALSE(server_->Close());
        EXPECT_TRUE(nodes_[0].live_memory);
        EXPECT_TRUE(nodes_[2].live_memory);
    }
    static inline RmaFailureHardwareTest* active_ = nullptr;
    std::array<Node, 3> nodes_{};
    urma_device_cap_t cap_{};
    RmaFailureOptions options_{.bytes = 16384, .batch = 4, .rounds = 3, .timeout_ms = 100};
    Fault fault_ = Fault::kNone;
    std::size_t fault_index_ = 0;
    urma_cr_status_t fault_status_ = URMA_CR_REM_ACCESS_ABORT_ERR;
    urma_jetty_state_t query_state_ = URMA_JETTY_STATE_READY;
    bool exited_ = false;
    bool reverse_ = false;
    int partial_prefix_ = -1;
    int poll_limit_ = 256;
    std::uint32_t baseline_post_delay_ms_ = 0;
    std::uint32_t exit_confirmation_delay_ms_ = 0;
    std::uint64_t fault_writes_after_exit_ = 0;
    std::mutex mutex_;
    MockRmaFailurePeer fault_peer_;
    NoDestructor<RmaFailureSession> server_, client_;
    NoDestructor<RmaFailureResources> fault_target_;
    std::expected<void, ToolError> server_result_, client_result_;
};

class RmaFailureStatusTest : public RmaFailureHardwareTest, public ::testing::WithParamInterface<int> {};
TEST_P(RmaFailureStatusTest, SameSqHealthyWritesSurviveObservedRemoteFailureWithoutAssumingStatusEight) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    fault_status_ = static_cast<urma_cr_status_t>(GetParam());
    reverse_ = true;
    poll_limit_ = 3;
    Open();
    ExpectExitAfterBaselines();
    RunPair();
    ASSERT_TRUE(server_result_) << server_result_.error().operation;
    ASSERT_TRUE(client_result_) << client_result_.error().operation;
    const auto& stats = client_->resources.stats;
    const auto count = options_.batch * options_.rounds;
    EXPECT_EQ(stats.healthy_before.verified, count);
    EXPECT_EQ(stats.healthy_mixed.verified, count);
    EXPECT_EQ(stats.healthy_after.verified, count);
    EXPECT_EQ(server_->resources.stats.healthy_baseline.verified, 1U);
    EXPECT_EQ(server_->resources.stats.healthy_before.verified, count);
    EXPECT_EQ(server_->resources.stats.healthy_mixed.verified, count);
    EXPECT_EQ(server_->resources.stats.healthy_after.verified, count);
    EXPECT_EQ(fault_target_->stats.fault_baseline.verified, 1U);
    EXPECT_EQ(stats.fault_mixed.retired, count);
    EXPECT_EQ(stats.fault_mixed.statuses[GetParam()], count);
    EXPECT_TRUE(stats.peer_exit_confirmed);
    EXPECT_EQ(stats.last_state, URMA_JETTY_STATE_READY);
    EXPECT_FALSE(stats.pending);
    EXPECT_EQ(fault_writes_after_exit_, count);
    ASSERT_TRUE(server_->Close());
    ASSERT_TRUE(client_->Close());
    ASSERT_TRUE(fault_target_->Close());
    EXPECT_FALSE(nodes_[0].live_memory);
    EXPECT_FALSE(nodes_[2].live_memory);
}
INSTANTIATE_TEST_SUITE_P(TerminalStatuses, RmaFailureStatusTest,
                         ::testing::Values(static_cast<int>(URMA_CR_REM_ACCESS_ABORT_ERR),
                                           static_cast<int>(URMA_CR_RNR_RETRY_CNT_EXC_ERR)));

TEST_F(RmaFailureHardwareTest, FaultAckTimeoutDrainsAcceptedBatchAndNeverPostsToTheSqAgain) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    fault_status_ = URMA_CR_ACK_TIMEOUT_ERR;
    poll_limit_ = 1;
    Open();
    ExpectExitAfterBaselines();
    // 芯片上 status 9 后 SQ 无法继续使用；即使 query 仍为 READY，也只退休当前批次，禁止后续轮次。
    EXPECT_CALL(*this, Post(_, _, _)).Times(2 + options_.rounds + 1).WillRepeatedly(DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    const auto& stats = client_->resources.stats;
    EXPECT_EQ(stats.healthy_mixed.retired, options_.batch);
    EXPECT_EQ(stats.fault_mixed.retired, options_.batch);
    EXPECT_EQ(stats.fault_mixed.statuses[URMA_CR_ACK_TIMEOUT_ERR], options_.batch);
    EXPECT_EQ(stats.healthy_after.accepted, 0U);
    EXPECT_FALSE(stats.pending);
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyAfter));
    ExpectSessionRetained();
}

TEST_F(RmaFailureHardwareTest, BaselineBarrierAllowsTwoIndividuallyBoundedWritesToFinish) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.rounds = 1;
    baseline_post_delay_ms_ = 70;
    Open();
    ExpectExitAfterBaselines();
    // 每次 WRITE 均少于 100ms，但串行的两个目标基线共需至少 140ms；
    // 服务端不能将整个基线阶段误限为一个 WRITE 的截止时间。
    RunPair();
    ASSERT_TRUE(client_result_) << client_result_.error().operation;
    ASSERT_TRUE(server_result_) << server_result_.error().operation;
    EXPECT_EQ(client_->resources.stats.healthy_baseline.verified, 1U);
    EXPECT_EQ(client_->resources.stats.fault_baseline.verified, 1U);
    EXPECT_EQ(client_->resources.stats.healthy_after.verified, options_.batch);
    EXPECT_TRUE(client_->Close());
    EXPECT_TRUE(server_->Close());
    EXPECT_TRUE(fault_target_->Close());
}

TEST_F(RmaFailureHardwareTest, ExitConfirmationBarrierAllowsTargetVerificationAndReapingToFinish) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.rounds = 1;
    exit_confirmation_delay_ms_ = 150;
    Open();
    ExpectExitAfterBaselines();
    // 监督者需依次确认目标数据与完成 kill/waitpid，整体可超过一次操作的 100ms；
    // 客户端仍必须等待退出确认，而不是提前超时或开始失效目标 WRITE。
    RunPair();
    ASSERT_TRUE(client_result_) << client_result_.error().operation;
    ASSERT_TRUE(server_result_) << server_result_.error().operation;
    EXPECT_TRUE(client_->resources.stats.peer_exit_confirmed);
    EXPECT_EQ(fault_writes_after_exit_, options_.batch);
    EXPECT_EQ(client_->resources.stats.healthy_after.verified, options_.batch);
    EXPECT_TRUE(client_->Close());
    EXPECT_TRUE(server_->Close());
    EXPECT_TRUE(fault_target_->Close());
}

TEST_F(RmaFailureHardwareTest, MaximumMixedBatchCompletesInReverseAcrossPollBoundaries) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    options_.batch = 128;
    options_.rounds = 1;
    options_.timeout_ms = 1000;
    reverse_ = true;
    poll_limit_ = 17;
    Open();
    ExpectExitAfterBaselines();
    RunPair();
    ASSERT_TRUE(client_result_) << client_result_.error().operation;
    ASSERT_TRUE(server_result_) << server_result_.error().operation;
    EXPECT_EQ(client_->resources.stats.healthy_mixed.retired, 128U);
    EXPECT_EQ(client_->resources.stats.fault_mixed.retired, 128U);
    EXPECT_FALSE(client_->resources.stats.pending);
    EXPECT_TRUE(client_->Close());
    EXPECT_TRUE(server_->Close());
    EXPECT_TRUE(fault_target_->Close());
}

TEST_F(RmaFailureHardwareTest, MixedWritesAreRejectedUntilPeerExitIsConfirmed) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    // 两个目标均已导入仍不代表故障生效；没有监督者的退出确认时不得提交混合实验。
    EXPECT_CALL(*this, Post(_, _, _)).Times(0);
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kMixed));
    EXPECT_EQ(client_->resources.stats.fault_mixed.accepted, 0U);
}

TEST_F(RmaFailureHardwareTest, ChangedSqStateRejectsTheNextPostWithoutModificationOrRecreation) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    ASSERT_TRUE(client_->resources.Transfer(RmaFailurePhase::kHealthyBaseline));
    query_state_ = URMA_JETTY_STATE_ERROR;
    EXPECT_CALL(*this, Post(_, _, _)).Times(0);
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.accepted, 0U);
    EXPECT_EQ(client_->resources.stats.last_state, URMA_JETTY_STATE_ERROR);
}

class RmaFailureCqeTest : public RmaFailureHardwareTest, public ::testing::WithParamInterface<int> {};
TEST_P(RmaFailureCqeTest, InvalidCompletionDoesNotHideOtherTerminalsConsumedInTheSamePoll) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = static_cast<Fault>(GetParam());
    // 第一条身份或状态非法，后续三个可信完成仍要退休；无效 user_ctx 永远不能作为指针解引用。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(DoDefault());
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.accepted, 4U);
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, 3U);
    EXPECT_TRUE(client_->resources.stats.pending);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_FALSE(client_->resources.Close());
}
INSTANTIATE_TEST_SUITE_P(IdentityAndStatus, RmaFailureCqeTest, ::testing::Values(1, 2, 3, 4, 10, 11));

TEST_F(RmaFailureHardwareTest, HealthyErrorStillRetiresAllAcceptedTerminalsBeforeReturningFailure) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = Fault::kHealthyError;
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, 4U);
    EXPECT_EQ(client_->resources.stats.healthy_before.statuses[URMA_CR_ACK_TIMEOUT_ERR], 1U);
    EXPECT_FALSE(client_->resources.stats.pending);
}

TEST_F(RmaFailureHardwareTest, HealthyErrorDrainsRemainingAcceptedWritesAcrossLaterPolls) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = Fault::kHealthyError;
    poll_limit_ = 1;
    // 首次 poll 仅返回失败 CQE，后续三个已接受的 WRITE 仍必须取得终结证据，不能提前放弃目的区。
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(DoDefault());
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, 4U);
    EXPECT_EQ(client_->resources.stats.healthy_before.statuses[URMA_CR_SUCCESS], 3U);
    EXPECT_EQ(client_->resources.stats.healthy_before.statuses[URMA_CR_ACK_TIMEOUT_ERR], 1U);
    EXPECT_TRUE(nodes_[2].completions.empty());
    EXPECT_FALSE(client_->resources.stats.pending);
}

TEST_F(RmaFailureHardwareTest, DuplicateCompletionCannotRetireOneWriteTwice) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = Fault::kDuplicateCompletion;
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.accepted, 4U);
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, 4U);
    EXPECT_FALSE(client_->resources.stats.pending);
}

TEST_F(RmaFailureHardwareTest, PartialPostRetiresAcceptedPrefixThenFailsWithoutReplay) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    exited_ = true;
    client_->resources.stats.peer_exit_confirmed = true;
    partial_prefix_ = 3;
    reverse_ = true;
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(DoDefault());
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kMixed));
    const auto& stats = client_->resources.stats;
    EXPECT_EQ(stats.healthy_mixed.accepted + stats.fault_mixed.accepted, 3U);
    EXPECT_EQ(stats.healthy_mixed.retired + stats.fault_mixed.retired, 3U);
    EXPECT_EQ(stats.partial_batches, 1U);
    EXPECT_TRUE(stats.accepted_known);
    EXPECT_FALSE(stats.pending);
}

TEST_F(RmaFailureHardwareTest, NullBadWrWithFailureRetainsUnknownDmaDependencies) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = Fault::kUnknownAcceptance;
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_FALSE(client_->resources.stats.accepted_known);
    EXPECT_TRUE(client_->resources.stats.pending);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_FALSE(client_->resources.Close());
}

TEST_F(RmaFailureHardwareTest, ForeignBadWrCannotBeUsedToInferAcceptance) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    urma_jfs_wr_t foreign{};
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([&](auto*, auto*, auto** bad) {
        *bad = &foreign;
        return URMA_ENOMEM;
    });
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_FALSE(client_->resources.stats.accepted_known);
    EXPECT_TRUE(client_->resources.stats.pending);
}

TEST_F(RmaFailureHardwareTest, SuccessfulPostWithBadWrIsAnUnknownAcceptanceBoundary) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = wr->next;
        return URMA_SUCCESS;
    });
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_FALSE(client_->resources.stats.accepted_known);
    EXPECT_TRUE(client_->resources.stats.pending);
}

class RmaFailureMemoryTest : public RmaFailureHardwareTest, public ::testing::WithParamInterface<bool> {};
TEST_P(RmaFailureMemoryTest, CorruptLaterDestinationIsRejectedDespiteSuccessfulCompletions) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_index_ = 2;
    fault_ = GetParam() ? Fault::kCorruptGuard : Fault::kCorruptPayload;
    // 本地 CQE 成功只能证明 WR 退休；目标进程必须单独验证 payload 与 guard。
    ASSERT_TRUE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.verified, 0U);
    EXPECT_FALSE(server_->resources.VerifyTarget(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, 4U);
    EXPECT_FALSE(client_->resources.stats.pending);
}
INSTANTIATE_TEST_SUITE_P(GuardsAndPayload, RmaFailureMemoryTest, ::testing::Bool());

TEST_F(RmaFailureHardwareTest, GuardOfFailedWriteSourceMustStillRemainIntact) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    exited_ = true;
    client_->resources.stats.peer_exit_confirmed = true;
    fault_index_ = 1;
    fault_ = Fault::kCorruptSourceGuard;
    // 远端目标已经退出也不应改变本地 WRITE 源区；失败 CQE 不能掩盖本地 guard 损坏。
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kMixed));
    EXPECT_EQ(client_->resources.stats.healthy_mixed.retired, 4U);
    EXPECT_EQ(client_->resources.stats.fault_mixed.retired, 4U);
    EXPECT_FALSE(client_->resources.stats.pending);
}

TEST_F(RmaFailureHardwareTest, PollFailureRetainsEveryUnprovenDestinationAndImportedTarget) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    EXPECT_CALL(*this, Poll(_, _, _)).WillOnce(Return(-EIO));
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.accepted, 4U);
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, 0U);
    EXPECT_TRUE(client_->resources.stats.pending);
    EXPECT_CALL(*this, UnimportSeg(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_FALSE(client_->resources.Close());
}

TEST_F(RmaFailureHardwareTest, TargetVerificationRequiresWrittenPayloadAndResetsItToPoison) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    auto* payload = reinterpret_cast<std::byte*>(fault_target_->hello().payload_address);
    ASSERT_TRUE(client_->resources.Transfer(RmaFailurePhase::kFaultBaseline));
    EXPECT_EQ(client_->resources.stats.fault_baseline.verified, 0U);
    ASSERT_TRUE(fault_target_->VerifyTarget(RmaFailurePhase::kFaultBaseline));
    EXPECT_EQ(fault_target_->stats.fault_baseline.verified, 1U);
    EXPECT_TRUE(std::all_of(payload, payload + options_.bytes, [](auto byte) { return byte == std::byte{0xff}; }));
    // 没有新写入不能重复确认上次的数据；清毒使漏写不能靠旧 payload 蒙混过关。
    EXPECT_FALSE(fault_target_->VerifyTarget(RmaFailurePhase::kFaultBaseline));
}

TEST_F(RmaFailureHardwareTest, SkippedRemoteWriteIsRejectedEvenWithSuccessfulCompletion) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = Fault::kSkipWrite;
    fault_index_ = 2;
    ASSERT_TRUE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_FALSE(server_->resources.VerifyTarget(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.verified, 0U);
}

TEST_F(RmaFailureHardwareTest, TargetVerificationRejectsPayloadFromAnotherRound) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    ASSERT_TRUE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore, 0));
    // 正确地址和 slot 的旧轮次 payload 也必须拒绝，防止迟到写入被归给新轮次。
    EXPECT_FALSE(server_->resources.VerifyTarget(RmaFailurePhase::kHealthyBefore, 1));
}

TEST_F(RmaFailureHardwareTest, BaselineVerificationRejectsWritesOutsideTheBaselineSlot) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    ASSERT_TRUE(client_->resources.Transfer(RmaFailurePhase::kHealthyBaseline));
    auto* next_slot = reinterpret_cast<std::byte*>(server_->resources.hello().payload_address) +
                      RmaSlotStride(server_->resources.hello().options);
    next_slot[0] = std::byte{0};
    // 基线只写 slot 0；其余槽仍应保持 poison，校验不能忽略相邻 payload 的越界写入。
    EXPECT_FALSE(server_->resources.VerifyTarget(RmaFailurePhase::kHealthyBaseline));
}

TEST_F(RmaFailureHardwareTest, WriteCompletionCannotHideChangedLocalSourcePayload) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    Import();
    fault_ = Fault::kCorruptSourcePayload;
    fault_index_ = 2;
    EXPECT_FALSE(client_->resources.Transfer(RmaFailurePhase::kHealthyBefore));
    EXPECT_EQ(client_->resources.stats.healthy_before.retired, options_.batch);
    EXPECT_FALSE(client_->resources.stats.pending);
}

TEST_F(RmaFailureHardwareTest, RemoteValidationFailureNeverAcknowledgesVerifiedWrites) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    fault_ = Fault::kCorruptPayload;
    EXPECT_CALL(fault_peer_, FinishBaselineAndExit()).Times(0);
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(DoDefault());
    RunPair();
    EXPECT_FALSE(server_result_);
    EXPECT_FALSE(client_result_);
    EXPECT_EQ(client_->resources.stats.healthy_baseline.retired, 1U);
    EXPECT_EQ(client_->resources.stats.healthy_baseline.verified, 0U);
    ExpectSessionRetained();
}

TEST_F(RmaFailureHardwareTest, ExitFailurePreventsMixedAndAfterTrafficAndRetainsPublishedMemory) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    EXPECT_CALL(fault_peer_, FinishBaselineAndExit()).Times(1).WillOnce([this]() -> std::expected<void, ToolError> {
        EXPECT_EQ(client_->resources.stats.healthy_baseline.verified, 1U);
        EXPECT_EQ(client_->resources.stats.fault_baseline.verified, 0U);
        return std::unexpected(ToolError{"mock waitpid failed", ECHILD});
    });
    RunPair();
    EXPECT_FALSE(server_result_);
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(client_->resources.stats.peer_exit_confirmed);
    EXPECT_EQ(client_->resources.stats.fault_mixed.accepted, 0U);
    EXPECT_EQ(client_->resources.stats.healthy_after.accepted, 0U);
    ExpectSessionRetained();
}

TEST_F(RmaFailureHardwareTest, BaselineErrorNeverTerminatesTheRemoteTargetOrStartsFaultTraffic) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    fault_ = Fault::kHealthyError;
    EXPECT_CALL(fault_peer_, FinishBaselineAndExit()).Times(0);
    EXPECT_CALL(*this, Post(_, _, _)).Times(1).WillOnce(DoDefault());
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    ExpectSessionRetained();
}

TEST_F(RmaFailureHardwareTest, RoleOptionsMismatchRejectsBeforeImportOrPost) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    // 服务端发布的故障目标与主会话配置不一致，必须在任何 DMA 之前拒绝。
    ++fault_peer_.descriptor.options.rounds;
    EXPECT_CALL(*this, ImportSeg(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*this, ImportJetty(_, _, _)).Times(0);
    EXPECT_CALL(*this, Post(_, _, _)).Times(0);
    EXPECT_CALL(fault_peer_, FinishBaselineAndExit()).Times(0);
    RunPair();
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(server_result_);
    ExpectSessionRetained();
}

TEST_F(RmaFailureHardwareTest, WrongExitMarkerNeverAuthorizesMixedWrites) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    Open();
    std::array<int, 2> sockets{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
    ControlChannel client_channel(sockets[1]);
    std::expected<void, ToolError> peer_result;
    EXPECT_CALL(*this, Post(_, _, _)).Times(2 + options_.rounds).WillRepeatedly(DoDefault());
    std::thread peer([&] {
        ControlChannel channel(sockets[0]);
        RmaFailureHelloBytes client_wire{};
        peer_result = channel.Read(client_wire, options_.timeout_ms);
        if (!peer_result) {
            return;
        }
        RmaFailureHello hello;
        hello.server = true;
        hello.options = options_;
        hello.endpoints = {server_->resources.hello(), fault_target_->hello()};
        peer_result = channel.Write(EncodeRmaFailureHello(hello), options_.timeout_ms);
        if (!peer_result) {
            return;
        }
        peer_result = channel.ExpectMarker(0x46410000, options_.timeout_ms);
        if (peer_result) {
            peer_result = server_->resources.VerifyTarget(RmaFailurePhase::kHealthyBaseline);
        }
        if (peer_result) {
            peer_result = channel.SendMarker(0x46410000, options_.timeout_ms);
        }
        if (peer_result) {
            peer_result = channel.ExpectMarker(0x46410001, options_.timeout_ms);
        }
        if (peer_result) {
            peer_result = channel.SendMarker(0x46410001, options_.timeout_ms);
        }
        for (std::uint32_t round = 0; peer_result && round < options_.rounds; ++round) {
            peer_result = channel.ExpectMarker(0x46420000 + round, options_.timeout_ms);
            if (peer_result) {
                peer_result = server_->resources.VerifyTarget(RmaFailurePhase::kHealthyBefore, round);
            }
            if (peer_result) {
                peer_result = channel.SendMarker(0x46420000 + round, options_.timeout_ms);
            }
        }
        if (peer_result) {
            // 基线和 before 都真实完成，但错误的退出标记不能替代监督者的 kill/waitpid 证明。
            peer_result = channel.SendMarker(0x46430001, options_.timeout_ms);
        }
    });
    client_result_ = client_->Run(client_channel);
    peer.join();
    ASSERT_TRUE(peer_result);
    EXPECT_FALSE(client_result_);
    EXPECT_FALSE(client_->resources.stats.peer_exit_confirmed);
    EXPECT_EQ(client_->resources.stats.healthy_before.verified, options_.rounds * options_.batch);
    EXPECT_EQ(client_->resources.stats.fault_mixed.accepted, 0U);
    EXPECT_FALSE(client_->can_close());
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_FALSE(client_->Close());
}

TEST_F(RmaFailureHardwareTest, UnsupportedWriteCapabilityCreatesNoQueuesOrMemory) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    cap_.max_write_size = 0;
    EXPECT_CALL(*this, CreateJfc(_, _)).Times(0);
    EXPECT_CALL(*this, Register(_, _)).Times(0);
    auto result = client_->Open(&nodes_[2].ctx, cap_, options_, false);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EOPNOTSUPP);
}

TEST_F(RmaFailureHardwareTest, WriteFailureProbeDoesNotRequireReadCapability) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    cap_.max_read_size = 0;
    // WRITE 探针仅依赖 max_write_size，READ 能力缺失不能阻止创建。
    ASSERT_TRUE(client_->Open(&nodes_[2].ctx, cap_, options_, false));
    EXPECT_EQ(client_->resources.hello().options.operation, RmaOperation::kWrite);
    EXPECT_TRUE(client_->Close());
}
} // namespace
} // namespace tools
} // namespace kbsocket
