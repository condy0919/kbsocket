// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/wait.h>

#include <cerrno>

#include "tools/peer_failure_test/process.hpp"
#include "tools/rma_failure_test/fault_peer.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
TEST(RmaFailureProcessTest, ChildStartupFailureIsReapedWithoutLoadingParentProvider) {
    const auto run = [](void*) {
        bool failed = false;
        {
            RmaFailurePeerProcess peer;
            const RmaFailureOptions options{.timeout_ms = 100};
            // 使用真实 fork/EOF/kill/waitpid 验证缺库时的有界退出，不依赖硬件或伪造 provider。
            auto opened = peer.Open("/nonexistent-kbsocket-rma-failure-liburma.so", "unused", 0, options);
            failed = !opened;
        }
        int status = 0;
        const auto remaining = ::waitpid(-1, &status, WNOHANG);
        return failed && remaining == -1 && errno == ECHILD ? 0 : 1;
    };
    EXPECT_EQ(RunPeerWatchdog(run, nullptr, 3000), 0);
}

TEST(RmaFailureProcessTest, InvalidConfigurationNeverSpawnsChild) {
    RmaFailurePeerProcess peer;
    const RmaFailureOptions invalid{.batch = 0};
    // 参数错误必须先于 fork；否则调用方可能收到失败却留下正在初始化 URMA 的子进程。
    const auto opened = peer.Open("/nonexistent-kbsocket-rma-failure-liburma.so", "unused", 0, invalid);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EINVAL);
    const auto stopped = peer.FinishBaselineAndExit();
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, EINVAL);
}

TEST(RmaFailureProcessTest, RejectsNullLibraryOrDeviceBeforeFork) {
    RmaFailurePeerProcess peer;
    const RmaFailureOptions options;
    EXPECT_FALSE(peer.Open(nullptr, "unused", 0, options));
    EXPECT_FALSE(peer.Open("unused", nullptr, 0, options));
    EXPECT_FALSE(peer.Open("", "unused", 0, options));
    EXPECT_FALSE(peer.Open("unused", "", 0, options));
}
} // namespace
} // namespace tools
} // namespace kbsocket
