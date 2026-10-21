// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/wait.h>

#include <cerrno>
#include <cstdint>

#include "tools/peer_failure_test/process.hpp"
#include "tools/sq_multiplex_test/fault_peer.hpp"
#include <gtest/gtest.h>

namespace kbsocket {
namespace tools {
namespace {
TEST(MultiplexFaultProcess, ChildStartupFailureIsReapedWithoutLoadingParentProvider) {
    const auto run = [](void*) {
        bool failed = false;
        {
            MultiplexPeerProcess peer;
            const SqMultiplexOptions options{.timeout_ms = 100, .fault = MultiplexFault::kPeerExit};
            // 本机没有 liburma 也能验证真实 fork/EOF/kill/waitpid 路径；失败子进程不能遗留僵尸。
            const auto opened = peer.Open("/nonexistent-kbsocket-test-liburma.so", "unused", 0, options);
            failed = !opened;
        }
        int status = 0;
        const auto remaining = ::waitpid(-1, &status, WNOHANG);
        return failed && remaining == -1 && errno == ECHILD ? 0 : 1;
    };
    EXPECT_EQ(RunPeerWatchdog(run, nullptr, 3000), 0);
}
} // namespace
} // namespace tools
} // namespace kbsocket
