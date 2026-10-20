// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/provider_boundary_test/provider_boundary_test.hpp"

#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <deque>
#include <limits>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::Return;

urma_cr_t Completion(std::uint64_t id, urma_cr_status_t status) {
    urma_cr_t cr{};
    cr.user_ctx = id;
    cr.status = status;
    cr.flag.bs.jetty = 1;
    cr.local_id = 42;
    return cr;
}

TEST(ProviderBoundaryTrackerTest, PartialAcceptanceOnlyOwnsPrefix) {
    ProviderBoundaryTracker tracker;
    tracker.Initialize(4, 42);
    std::array<urma_jfs_wr_t, 4> wrs{};
    for (std::size_t i = 0; i < wrs.size(); ++i) {
        wrs[i].user_ctx = i + 1;
    }
    // bad_wr 指向第三项，前两项已被 provider 接受；不能因为整个调用返回 EAGAIN 而撤销前缀。
    auto posted = tracker.RecordPost(wrs, URMA_EAGAIN, &wrs[2]);
    ASSERT_TRUE(posted);
    EXPECT_EQ(*posted, 2u);
    EXPECT_EQ(tracker.stats.accepted, 2u);
    EXPECT_EQ(tracker.stats.partial_batches, 1u);
    EXPECT_EQ(tracker.stats.eagain, 1u);
    EXPECT_TRUE(tracker.Complete(Completion(1, URMA_CR_RNR_RETRY_CNT_EXC_ERR), false));
    EXPECT_TRUE(tracker.Complete(Completion(2, URMA_CR_SUCCESS), false));
    EXPECT_FALSE(tracker.Complete(Completion(3, URMA_CR_SUCCESS), false));
    EXPECT_FALSE(tracker.pending());
}

TEST(ProviderBoundaryTrackerTest, UnknownBadWrDoesNotGuessAcceptedCount) {
    ProviderBoundaryTracker tracker;
    tracker.Initialize(1, 42);
    std::array<urma_jfs_wr_t, 1> wrs{};
    wrs[0].user_ctx = 1;
    urma_jfs_wr_t foreign{};
    // 不属于本次提交链的 bad_wr 既不能相减求前缀，也不能假设没有任何 WR 被接受。
    EXPECT_FALSE(tracker.RecordPost(wrs, URMA_ENOMEM, &foreign));
    EXPECT_FALSE(tracker.stats.accepted_known);
    EXPECT_EQ(tracker.stats.enomem, 1u);
}

TEST(ProviderBoundaryTrackerTest, SuccessWithBadWrIsUnknownAcceptance) {
    ProviderBoundaryTracker tracker;
    tracker.Initialize(1, 42);
    std::array<urma_jfs_wr_t, 1> wrs{};
    wrs[0].user_ctx = 1;
    EXPECT_FALSE(tracker.RecordPost(wrs, URMA_SUCCESS, wrs.data()));
    EXPECT_FALSE(tracker.stats.accepted_known);
}

TEST(ProviderBoundaryTrackerTest, BoundaryNeverUsesUserCtxAndFlushIsExactlyOnce) {
    ProviderBoundaryTracker tracker;
    tracker.Initialize(1, 42);
    std::array<urma_jfs_wr_t, 1> wrs{};
    wrs[0].user_ctx = 1;
    ASSERT_TRUE(tracker.RecordPost(wrs, URMA_SUCCESS, nullptr));
    tracker.BeginError();
    // 伪 CQE 放入最大整数而非合法 ID，确认它只打开软件 flush 边界、不退休任何请求。
    EXPECT_TRUE(
        tracker.Complete(Completion(std::numeric_limits<std::uint64_t>::max(), URMA_CR_WR_FLUSH_ERR_DONE), false));
    EXPECT_EQ(tracker.stats.retired, 0u);
    EXPECT_FALSE(tracker.Complete(Completion(1, URMA_CR_WR_UNHANDLED), false));
    EXPECT_TRUE(tracker.Complete(Completion(1, URMA_CR_WR_UNHANDLED), true));
    EXPECT_FALSE(tracker.Complete(Completion(1, URMA_CR_WR_FLUSH_ERR), false));
    EXPECT_EQ(tracker.stats.retired, 1u);
    EXPECT_EQ(tracker.stats.statuses[URMA_CR_WR_UNHANDLED], 1u);
}

TEST(ProviderBoundaryTrackerTest, FlushBeforeBoundaryAndDuplicateBoundaryAreRejected) {
    ProviderBoundaryTracker tracker;
    tracker.Initialize(1, 42);
    std::array<urma_jfs_wr_t, 1> wrs{};
    wrs[0].user_ctx = 1;
    ASSERT_TRUE(tracker.RecordPost(wrs, URMA_SUCCESS, nullptr));
    EXPECT_FALSE(tracker.Complete(Completion(1, URMA_CR_WR_UNHANDLED), true));
    EXPECT_FALSE(tracker.Complete(Completion(0, URMA_CR_WR_FLUSH_ERR_DONE), false));
    tracker.BeginError();
    ASSERT_TRUE(tracker.Complete(Completion(0, URMA_CR_WR_FLUSH_ERR_DONE), false));
    EXPECT_FALSE(tracker.Complete(Completion(0, URMA_CR_WR_FLUSH_ERR_DONE), false));
}

TEST(ProviderBoundaryTrackerTest, WrongLaneAndSuspendPseudoCannotRetireWr) {
    ProviderBoundaryTracker tracker;
    tracker.Initialize(1, 42);
    std::array<urma_jfs_wr_t, 1> wrs{};
    wrs[0].user_ctx = 1;
    ASSERT_TRUE(tracker.RecordPost(wrs, URMA_SUCCESS, nullptr));
    auto cr = Completion(1, URMA_CR_SUCCESS);
    cr.local_id = 99;
    EXPECT_FALSE(tracker.Complete(cr, false));
    EXPECT_FALSE(tracker.Complete(Completion(1, URMA_CR_WR_SUSPEND_DONE), false));
    EXPECT_EQ(tracker.stats.retired, 0u);
}

TEST(ProviderBoundaryOptionsTest, LimitsKeepPostBudgetAndCqBounded) {
    EXPECT_TRUE(ValidateProviderBoundaryOptions({}));
    EXPECT_FALSE(ValidateProviderBoundaryOptions({.tx_depth = 32, .attempts = 32}));
    EXPECT_FALSE(ValidateProviderBoundaryOptions({.batch = 257}));
    EXPECT_FALSE(ValidateProviderBoundaryOptions({.attempts = 16385}));
    EXPECT_FALSE(ValidateProviderBoundaryOptions({.timeout_ms = 0}));
}

TEST(ProviderBoundaryWatchdogTest, PreservesChildExitStatus) {
    auto result = SuperviseProviderWorker(+[] { return 7; }, 1000);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->exit_code, 7);
    EXPECT_EQ(result->signal, 0);
    EXPECT_FALSE(result->timed_out);
}

TEST(ProviderBoundaryWatchdogTest, KillsAndReapsBlockedProviderWithoutCallingDestructor) {
    // 模拟同步 provider 删除永久阻塞；父进程必须有界结束，不能等待工具内部的 poll deadline。
    auto result = SuperviseProviderWorker(
        +[]() -> int {
            while (true) {
                ::pause();
            }
        },
        20);
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->timed_out);
    EXPECT_EQ(result->signal, SIGKILL);
    EXPECT_EQ(result->exit_code, -1);
}

TEST(ProviderBoundaryWatchdogTest, ChildCrashIsNotReportedAsSuccessfulObservation) {
    auto result = SuperviseProviderWorker(
        +[] {
            ::raise(SIGTERM);
            return 0;
        },
        1000);
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->timed_out);
    EXPECT_EQ(result->signal, SIGTERM);
    EXPECT_EQ(result->exit_code, -1);
}

class ProviderBoundaryHardwareTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, Query, (urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));

protected:
    static raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f;
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        f.import_jetty = [](auto* c, auto* r, auto* t) { return active_->Import(c, r, t); };
        f.post_jetty_send_wr = [](auto* q, auto* w, auto** b) { return active_->Post(q, w, b); };
        f.modify_jetty = [](auto* q, auto* a) { return active_->Modify(q, a); };
        f.query_jetty = [](auto* q, auto* c, auto* a) { return active_->Query(q, c, a); };
        f.poll_jfc = [](auto* q, int n, auto* c) { return active_->Poll(q, n, c); };
        f.flush_jetty = [](auto* q, int n, auto* c) { return active_->Flush(q, n, c); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.unimport_jetty = [](auto* q) { return active_->Unimport(q); };
        f.unregister_seg = [](auto* q) { return active_->Unregister(q); };
        return f;
    }
    void SetUp() override {
        active_ = this;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_msg_size = 4096;
        cap_.max_jetty = 8;
        cap_.max_jfs_depth = cap_.max_jfr_depth = 256;
        cap_.max_jfc_depth = 4096;
        cap_.max_jfs_sge = cap_.max_jfr_sge = cap_.max_jfs_rsge = 1;
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(4).WillRepeatedly([this](auto*, auto*) {
            return &cqs_[cq_count_++];
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(2).WillRepeatedly([this](auto*, auto*) {
            return &jfrs_[jfr_count_++];
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(2).WillRepeatedly([this](auto*, auto* cfg) {
            auto& jetty = jetties_[jetty_count_++];
            jetty.jetty_id.id = jetty_count_ == 1 ? 42 : 43;
            jetty.jetty_cfg = *cfg;
            return &jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).WillOnce(Return(&segment_));
        EXPECT_CALL(*this, Import(_, _, _)).WillOnce(Return(&remote_));
        EXPECT_CALL(*this, Modify(_, _)).WillOnce([this](auto*, auto* attr) {
            EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
            // 故障边界前两条由硬件终结，第三条留给软件 flush，以验证两种来源的所有权合并。
            completions_.push_back(Completion(1, URMA_CR_WR_FLUSH_ERR));
            completions_.push_back(Completion(2, URMA_CR_ACK_TIMEOUT_ERR));
            if (duplicate_) {
                completions_.push_back(Completion(1, URMA_CR_WR_FLUSH_ERR));
            }
            completions_.push_back(Completion(std::numeric_limits<std::uint64_t>::max(), URMA_CR_WR_FLUSH_ERR_DONE));
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Poll(_, _, _)).WillRepeatedly([this](auto*, int n, auto* output) {
            int count = 0;
            while (!completions_.empty() && count < n) {
                output[count++] = completions_.front();
                completions_.pop_front();
            }
            return count;
        });
        EXPECT_CALL(*this, Flush(_, _, _)).WillRepeatedly([this](auto*, int count, auto* output) {
            EXPECT_LE(count, 4); // flush 上限必须是本 SQ 深度，不能沿用 64 条 poll 缓冲长度。
            if (!flushed_) {
                flushed_ = true;
                output[0] = Completion(3, URMA_CR_WR_UNHANDLED);
                return 1;
            }
            return 0;
        });
    }
    void ExpectCleanup(bool close) {
        // 未终结的 ERROR post 必须同时阻止两端队列拆除，不能仅保留客户端的 payload。
        EXPECT_CALL(*this, DeleteJetty(_)).Times(close ? 2 : 0).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(_)).Times(close ? 2 : 0).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(_)).Times(close ? 4 : 0).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, Unimport(_)).Times(close ? 1 : 0).WillRepeatedly(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, Unregister(_)).Times(close ? 1 : 0).WillRepeatedly(Return(URMA_SUCCESS));
    }
    void Run(bool probe, bool pending, bool success = true) {
        raw::test_support::ScopedUrmaOverride override(MockedFunctions());
        NoDestructor<ProviderBoundarySession> client;
        NoDestructor<ProviderBoundarySession> server;
        const ProviderBoundaryOptions options{
            .tx_depth = 4, .batch = 6, .attempts = 12, .timeout_ms = 50, .error_post = probe};
        ASSERT_TRUE(client->Open(&ctx_, cap_, options, false));
        ASSERT_TRUE(server->Open(&ctx_, cap_, options, true));
        int sockets[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
        ControlChannel client_channel(sockets[0]);
        ControlChannel server_channel(sockets[1]);
        std::expected<void, ToolError> server_result;
        std::thread peer([&] { server_result = server->Run(server_channel); });
        auto client_result = client->Run(client_channel);
        peer.join();
        if (!success) {
            EXPECT_FALSE(client_result);
            EXPECT_FALSE(server_result);
            EXPECT_FALSE(client->Close());
            EXPECT_FALSE(server->Close());
            return;
        }
        ASSERT_TRUE(client_result) << client_result.error().operation;
        ASSERT_TRUE(server_result) << server_result.error().operation;
        EXPECT_EQ(client->stats().partial_batches, 1u);
        EXPECT_EQ(client->stats().flush_done, 1u);
        EXPECT_EQ(client->stats().statuses[URMA_CR_WR_UNHANDLED], 1u);
        EXPECT_EQ(client->stats().error_post_pending, pending);
        EXPECT_EQ(client->intentionally_retained(), pending);
        EXPECT_EQ(server->intentionally_retained(), pending);
        EXPECT_EQ(client->Close().has_value(), !pending);
        EXPECT_EQ(server->Close().has_value(), !pending);
    }
    static inline ProviderBoundaryHardwareTest* active_ = nullptr;
    urma_context_t ctx_{};
    urma_device_cap_t cap_{};
    std::array<urma_jfc_t, 4> cqs_{};
    std::array<urma_jfr_t, 2> jfrs_{};
    std::array<urma_jetty_t, 2> jetties_{};
    urma_target_seg_t segment_{};
    urma_target_jetty_t remote_{};
    std::deque<urma_cr_t> completions_;
    std::size_t cq_count_ = 0;
    std::size_t jfr_count_ = 0;
    std::size_t jetty_count_ = 0;
    bool flushed_ = false;
    bool duplicate_ = false;
};

TEST_F(ProviderBoundaryHardwareTest, PartialProviderAcceptanceDrainsThroughHardwareAndSoftware) {
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = &wr[3];
        return URMA_EAGAIN;
    });
    ExpectCleanup(true);
    Run(false, false);
}

TEST_F(ProviderBoundaryHardwareTest, ErrorPostSuccessWithoutTerminalRetainsBothEndpoints) {
    EXPECT_CALL(*this, Query(_, _, _)).WillOnce([](auto*, auto*, auto* attr) {
        attr->state = URMA_JETTY_STATE_ERROR;
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Post(_, _, _))
        .WillOnce([](auto*, auto* wr, auto** bad) {
            *bad = &wr[3];
            return URMA_ENOMEM;
        })
        .WillOnce([](auto*, auto*, auto** bad) {
            *bad = nullptr;
            return URMA_SUCCESS;
        });
    ExpectCleanup(false);
    Run(true, true);
}

TEST_F(ProviderBoundaryHardwareTest, ErrorPostRejectedAtHeadDoesNotInventPendingWork) {
    EXPECT_CALL(*this, Query(_, _, _)).WillOnce([](auto*, auto*, auto* attr) {
        attr->state = URMA_JETTY_STATE_ERROR;
        return URMA_SUCCESS;
    });
    EXPECT_CALL(*this, Post(_, _, _))
        .WillOnce([](auto*, auto* wr, auto** bad) {
            *bad = &wr[3];
            return URMA_EAGAIN;
        })
        .WillOnce([](auto*, auto* wr, auto** bad) {
            *bad = wr;
            return static_cast<urma_status_t>(EINVAL);
        });
    ExpectCleanup(true);
    Run(true, false);
}

TEST_F(ProviderBoundaryHardwareTest, DuplicateTerminalKeepsEntireDependencyChain) {
    // 硬件重复一条有效 ID 的完成，不能以 accepted==retired 或软件队列空掩盖协议破坏。
    duplicate_ = true;
    EXPECT_CALL(*this, Post(_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = &wr[3];
        return URMA_EAGAIN;
    });
    ExpectCleanup(false);
    Run(false, false, false);
}
} // namespace
} // namespace tools
} // namespace kbsocket
