// SPDX-License-Identifier: MulanPSL-2.0
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>

#include "tools/jfr_lifecycle_test/jfr_lifecycle_test.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Invoke;
using ::testing::Return;

class JfrLifecycleTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
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
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetties{};
        urma_target_seg_t segment{};
        urma_target_jetty_t target{};
        std::size_t created_cqs = 0;
        std::size_t created_jetties = 0;
        std::size_t deleted_jetties = 0;
        bool deleted_jfr = false;
        bool unregistered = false;
        std::deque<Buffer> buffers;
        std::array<std::deque<urma_cr_t>, 2> completions;
    };

    raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions functions;
        functions.create_jfc = [](auto* ctx, auto* cfg) { return current_->CreateJfc(ctx, cfg); };
        functions.create_jfr = [](auto* ctx, auto* cfg) { return current_->CreateJfr(ctx, cfg); };
        functions.create_jetty = [](auto* ctx, auto* cfg) { return current_->CreateJetty(ctx, cfg); };
        functions.register_seg = [](auto* ctx, auto* cfg) { return current_->Register(ctx, cfg); };
        functions.import_jetty = [](auto* ctx, auto* cfg, auto* token) { return current_->Import(ctx, cfg, token); };
        functions.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) { return current_->PostRecv(jfr, wr, bad); };
        functions.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) {
            return current_->PostSend(jetty, wr, bad);
        };
        functions.poll_jfc = [](auto* cq, int count, auto* cr) { return current_->Poll(cq, count, cr); };
        functions.delete_jetty = [](auto* jetty) { return current_->DeleteJetty(jetty); };
        functions.delete_jfr = [](auto* jfr) { return current_->DeleteJfr(jfr); };
        functions.delete_jfc = [](auto* cq) { return current_->DeleteJfc(cq); };
        functions.unregister_seg = [](auto* seg) { return current_->Unregister(seg); };
        functions.unimport_jetty = [](auto* jetty) { return current_->Unimport(jetty); };
        return functions;
    }
    Node& ByContext(urma_context_t* ctx) {
        return nodes_[ctx == &nodes_[0].ctx ? 0 : 1];
    }
    void SetUp() override {
        current_ = this;
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
            auto& jetty = node.jetties[node.created_jetties++];
            jetty.urma_ctx = ctx;
            jetty.jetty_cfg = *cfg;
            jetty.jetty_id.id = node.jfr.jfr_id.id + node.created_jetties;
            return &jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto*) {
            return &ByContext(ctx).segment;
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([this](auto* ctx, auto*, auto*) {
            return &ByContext(ctx).target;
        });
        EXPECT_CALL(*this, PostRecv(_, _, _))
            .Times(AnyNumber())
            .WillRepeatedly([this](auto* jfr, auto* wr, auto** bad) {
                std::lock_guard lock(mutex_);
                auto& node = ByContext(jfr->urma_ctx);
                for (; wr; wr = wr->next) {
                    node.buffers.push_back({wr->src.sge[0], wr->user_ctx});
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
                    // 反向取 RX 槽位，证明完成的 user_ctx 与 payload 序号不能混用。
                    const auto buffer = receiver.buffers.back();
                    receiver.buffers.pop_back();
                    std::memcpy(reinterpret_cast<void*>(buffer.sge.addr),
                                reinterpret_cast<void*>(wr->send.src.sge[0].addr), 4096);
                    urma_cr_t rx{};
                    rx.status = URMA_CR_SUCCESS;
                    rx.flag.bs.s_r = 1;
                    rx.local_id = receiver.jfr.jfr_id.id;
                    rx.user_ctx = buffer.id;
                    rx.completion_len = 4096;
                    receiver.completions[1].push_back(rx);
                    urma_cr_t tx{};
                    tx.status = URMA_CR_SUCCESS;
                    tx.flag.bs.jetty = 1;
                    tx.local_id = jetty->jetty_id.id;
                    tx.user_ctx = wr->user_ctx;
                    sender.completions[0].push_back(tx);
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
        ON_CALL(*this, DeleteJetty(_)).WillByDefault([this](auto* jetty) {
            ++ByContext(jetty->urma_ctx).deleted_jetties;
            return URMA_SUCCESS;
        });
        ON_CALL(*this, DeleteJfr(_)).WillByDefault([this](auto* jfr) {
            auto& node = ByContext(jfr->urma_ctx);
            EXPECT_EQ(node.deleted_jetties, 2U);
            EXPECT_FALSE(node.unregistered);
            node.deleted_jfr = true;
            return URMA_SUCCESS;
        });
        ON_CALL(*this, Unregister(_)).WillByDefault([this](auto* segment) {
            auto& node = nodes_[segment == &nodes_[0].segment ? 0 : 1];
            EXPECT_TRUE(node.deleted_jfr);
            node.unregistered = true;
            return URMA_SUCCESS;
        });
        ON_CALL(*this, DeleteJfc(_)).WillByDefault([this](auto* cq) {
            EXPECT_TRUE(ByContext(cq->urma_ctx).unregistered);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber());
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber());
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber());
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber());
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly(Return(URMA_SUCCESS));
    }

    static inline JfrLifecycleTest* current_;
    std::array<Node, 2> nodes_;
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    // 失败用例也保留实际注册的 vector 内存，不让测试对象析构掩盖 DMA 生命周期错误。
    NoDestructor<JfrLifecycleSession> receiver_;
    NoDestructor<JfrLifecycleSession> sender_;
};

TEST_F(JfrLifecycleTest, DeletesResidualJfrOnlyAfterBothJettiesThenReleasesMemory) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, {.rx_depth = 32}, true));
    ASSERT_TRUE(receiver_->Run(nullptr));
    ASSERT_EQ(receiver_->stats().posted, 32U);
    ASSERT_EQ(nodes_[0].buffers.size(), 32U);
    // 没有 poll/flush 消耗这些 RX；真正的 DeleteJfr 成功后才能注销 MR。
    EXPECT_CALL(*this, Poll(_, _, _)).Times(0);
    {
        ::testing::InSequence sequence;
        EXPECT_CALL(*this, DeleteJetty(&nodes_[0].jetties[1]));
        EXPECT_CALL(*this, DeleteJetty(&nodes_[0].jetties[0]));
        EXPECT_CALL(*this, DeleteJfr(&nodes_[0].jfr));
        EXPECT_CALL(*this, Unregister(&nodes_[0].segment));
        EXPECT_CALL(*this, DeleteJfc(&nodes_[0].cqs[0]));
        EXPECT_CALL(*this, DeleteJfc(&nodes_[0].cqs[1]));
    }
    ASSERT_TRUE(receiver_->Close());
    EXPECT_EQ(receiver_->stats().residual_at_delete, 32U);
    EXPECT_TRUE(receiver_->stats().delete_jfr_attempted);
}

TEST_F(JfrLifecycleTest, PartialConsumptionLeavesRxAtDeletionAndValidatesReorderedSlots) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    const JfrLifecycleOptions options{.rx_depth = 32, .consumed = 7, .timeout_ms = 1000};
    ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, options, true));
    ASSERT_TRUE(sender_->Open(&nodes_[1].ctx, cap_, options, false));
    std::array<int, 2> sockets;
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
    ControlChannel left(sockets[0]), right(sockets[1]);
    std::expected<void, ToolError> receiver_result, sender_result;
    std::thread receiver([&] { receiver_result = receiver_->Run(&left); });
    sender_result = sender_->Run(&right);
    receiver.join();
    ASSERT_TRUE(receiver_result);
    ASSERT_TRUE(sender_result);
    EXPECT_EQ(receiver_->stats().received, 7U);
    EXPECT_EQ(sender_->stats().tx_retired, 7U);
    // 未完成的 25 个 RX 不会补齐 CQE 或重新投递，直接进入受控删除观察。
    EXPECT_EQ(nodes_[0].buffers.size(), 25U);
    ASSERT_TRUE(sender_->Close());
    ASSERT_TRUE(receiver_->Close());
    EXPECT_EQ(receiver_->stats().residual_at_delete, 25U);
}

TEST_F(JfrLifecycleTest, JfrDeleteFailureRetainsMrAndCqsAndCannotRetry) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, {.rx_depth = 1}, true));
    ASSERT_TRUE(receiver_->Run(nullptr));
    // 删除失败可能已破坏 provider 查找表；后续 Close 不能再调用 DeleteJfr，更不能注销 MR。
    EXPECT_CALL(*this, DeleteJfr(_)).Times(1).WillOnce(Return(URMA_EINVAL));
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(receiver_->Close());
    EXPECT_EQ(receiver_->stats().residual_at_delete, 1U);
    EXPECT_EQ(receiver_->stats().delete_jfr_status, URMA_EINVAL);
}

TEST_F(JfrLifecycleTest, JettyDeleteFailurePreventsJfrAndMemoryDeletion) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, {}, true));
    ASSERT_TRUE(receiver_->Run(nullptr));
    // 有任一依赖者删除失败时，整个共享 JFR/MR/CQ 链必须保留。
    EXPECT_CALL(*this, DeleteJetty(_)).Times(1).WillOnce(Return(static_cast<urma_status_t>(-EBUSY)));
    EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
    EXPECT_FALSE(receiver_->Close());
}

TEST_F(JfrLifecycleTest, PartialRxPostFailureRetainsAllResources) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, {.rx_depth = 32}, true));
    EXPECT_CALL(*this, PostRecv(_, _, _)).WillOnce([](auto*, auto* wr, auto** bad) {
        *bad = wr->next;
        return URMA_EAGAIN;
    });
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
    EXPECT_FALSE(receiver_->Run(nullptr));
    EXPECT_FALSE(receiver_->Close());
}

TEST_F(JfrLifecycleTest, MissingPeerQuiescencePreventsDeletion) {
    raw::test_support::ScopedUrmaOverride override(MockedFunctions());
    ASSERT_TRUE(receiver_->Open(&nodes_[0].ctx, cap_, {.rx_depth = 32, .consumed = 1, .timeout_ms = 10}, true));
    std::array<int, 2> sockets;
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
    ControlChannel left(sockets[0]), silent_peer(sockets[1]);
    EXPECT_FALSE(receiver_->Run(&left));
    // 控制超时不等于对端已经终结，不能以退出清理为由删除它可能仍访问的端点。
    EXPECT_CALL(*this, DeleteJetty(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfr(_)).Times(0);
    EXPECT_CALL(*this, Unregister(_)).Times(0);
    EXPECT_CALL(*this, DeleteJfc(_)).Times(0);
    EXPECT_FALSE(receiver_->Close());
}

TEST(JfrLifecycleOptionsTest, RequiresPositiveResidualAndBoundedWork) {
    EXPECT_TRUE(ValidateJfrLifecycleOptions({.rx_depth = 1}));
    EXPECT_TRUE(ValidateJfrLifecycleOptions({.rx_depth = 256, .consumed = 255}));
    EXPECT_FALSE(ValidateJfrLifecycleOptions({.rx_depth = 0}));
    EXPECT_FALSE(ValidateJfrLifecycleOptions({.rx_depth = 257}));
    EXPECT_FALSE(ValidateJfrLifecycleOptions({.rx_depth = 32, .consumed = 32}));
    EXPECT_FALSE(ValidateJfrLifecycleOptions({.timeout_ms = 0}));
}

TEST(JfrLifecycleWatchdogTest, ReturnsChildExitCode) {
    const auto result = RunSupervised([](void*) { return 7; }, nullptr, 1000);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->exit_code, 7);
    EXPECT_FALSE(result->timed_out);
    EXPECT_EQ(result->signal, 0);
}

TEST(JfrLifecycleWatchdogTest, DistinguishesSignalFromTimeout) {
    const auto result = RunSupervised(
        [](void*) {
            ::raise(SIGTERM);
            return 0;
        },
        nullptr, 1000);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->signal, SIGTERM);
    EXPECT_FALSE(result->timed_out);
}

TEST(JfrLifecycleWatchdogTest, KillsAndReapsStuckProviderCall) {
    // 子进程永不返回，模拟 provider DeleteJfr 卡在事件确认等待；poll 超时对此无效。
    const auto start = std::chrono::steady_clock::now();
    const auto result = RunSupervised(
        [](void*) {
            for (;;) {
                ::pause();
            }
            return 0;
        },
        nullptr, 30);
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->timed_out);
    EXPECT_EQ(result->signal, SIGKILL);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
}

int RunParentDeathScenario() {
    // 此函数在独立 death-test 进程中执行；subreaper 只影响该进程，不能改变测试主进程的子进程归属。
    if (::prctl(PR_SET_CHILD_SUBREAPER, 1) != 0) {
        return 10;
    }
    int pipe_fds[2]{};
    if (::pipe(pipe_fds) != 0) {
        return 11;
    }
    const auto owner = ::getpid();
    const auto supervisor = ::fork();
    if (supervisor < 0) {
        return 12;
    }
    if (supervisor == 0) {
        ::close(pipe_fds[0]);
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != owner) {
            std::_Exit(13);
        }
        auto result = RunSupervised(
            [](void* argument) {
                const int fd = *static_cast<int*>(argument);
                const auto worker = ::getpid();
                if (::write(fd, &worker, sizeof(worker)) != static_cast<ssize_t>(sizeof(worker))) {
                    return 14;
                }
                while (true) {
                    ::pause();
                }
                return 0;
            },
            &pipe_fds[1], 10000);
        std::_Exit(result ? result->exit_code : 15);
    }
    ::close(pipe_fds[1]);
    auto wait_for = [](pid_t child) -> std::expected<int, ToolError> {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        while (true) {
            int status = 0;
            const auto result = ::waitpid(child, &status, WNOHANG);
            if (result == child) {
                return status;
            }
            if (result < 0 && errno != EINTR) {
                return std::unexpected(ToolError{"wait test child", errno});
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return std::unexpected(ToolError{"test child still alive", ETIMEDOUT});
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    };
    pollfd pipe_ready{pipe_fds[0], POLLIN, 0};
    pid_t worker = -1;
    const bool started = ::poll(&pipe_ready, 1, 1000) == 1 &&
                         ::read(pipe_fds[0], &worker, sizeof(worker)) == static_cast<ssize_t>(sizeof(worker));
    ::close(pipe_fds[0]);
    // 仅终止本次 fork 得到且尚未 waitpid 的监督进程，PID 仍由本测试拥有。
    if (::kill(supervisor, SIGKILL) != 0) {
        return 16;
    }
    auto supervisor_status = wait_for(supervisor);
    if (!started || !supervisor_status) {
        return 17;
    }
    // 父进程退出后 worker 被本 subreaper 接管。它应由 PDEATHSIG 自动终止，不能等到原来的 10 秒超时。
    auto worker_status = wait_for(worker);
    if (!worker_status) {
        if (worker_status.error().code == ETIMEDOUT) {
            // 仅在 waitpid 已确认是仍存活的自有子进程时补发清理信号；已回收 PID 绝不重用。
            static_cast<void>(::kill(worker, SIGKILL));
            static_cast<void>(wait_for(worker));
        }
        return 18;
    }
    return WIFSIGNALED(*worker_status) && WTERMSIG(*worker_status) == SIGKILL ? 0 : 19;
}

TEST(JfrLifecycleWatchdogTest, ParentDeathKillsAndReparentsBlockedWorker) {
    EXPECT_EXIT(std::_Exit(RunParentDeathScenario()), ::testing::ExitedWithCode(0), "");
}
} // namespace
} // namespace tools
} // namespace kbsocket
