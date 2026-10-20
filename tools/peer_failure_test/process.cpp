// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/peer_failure_test/process.hpp"

#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <print>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
std::expected<int, ToolError> Reap(pid_t child, std::uint32_t timeout_ms) noexcept {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    do {
        int status = 0;
        const auto result = ::waitpid(child, &status, WNOHANG);
        if (result == child) {
            return status;
        }
        if (result < 0 && errno != EINTR) {
            return std::unexpected(ToolError{"waitpid", errno});
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return std::unexpected(ToolError{"waitpid timeout", ETIMEDOUT});
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (true);
}
} // namespace

std::expected<void, ToolError> KillAndReapPeer(pid_t child, std::uint32_t timeout_ms, bool require_sigkill) noexcept {
    if (child <= 0) {
        return std::unexpected(ToolError{"invalid child pid", EINVAL});
    }
    if (::kill(child, SIGKILL) != 0 && errno != ESRCH) {
        return std::unexpected(ToolError{"kill peer child", errno});
    }
    auto reaped = Reap(child, timeout_ms);
    if (!reaped) {
        return std::unexpected(reaped.error());
    }
    if (require_sigkill && (!WIFSIGNALED(*reaped) || WTERMSIG(*reaped) != SIGKILL)) {
        return std::unexpected(ToolError{"peer child exited before fault injection", EPROTO});
    }
    return {};
}

int RunPeerWatchdog(int (*run)(void*), void* argument, std::uint32_t timeout_ms) noexcept {
    if (!run || timeout_ms == 0) {
        return 2;
    }
    std::fflush(nullptr);
    const auto parent = ::getpid();
    const auto child = ::fork();
    if (child < 0) {
        return 1;
    }
    if (child == 0) {
        if (::setpgid(0, 0) != 0 || ::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) {
            std::_Exit(1);
        }
        int result = 1;
        try {
            result = run(argument);
        } catch (const std::exception& exception) {
            std::println(stderr, "FAIL: worker exception: {}", exception.what());
        } catch (...) {
            std::println(stderr, "FAIL: unknown worker exception");
        }
        std::fflush(nullptr);
        std::_Exit(result);
    }
    // 双侧设置进程组，避免父进程过早触发 timeout 时子进程尚未完成 setpgid。
    static_cast<void>(::setpgid(child, child));
    auto reaped = Reap(child, timeout_ms);
    if (!reaped) {
        static_cast<void>(::kill(-child, SIGKILL));
        static_cast<void>(::kill(child, SIGKILL));
        static_cast<void>(Reap(child, 1000));
        std::println(stderr, "NOT_OBSERVED: worker watchdog expired or wait failed; provider/DMA terminality unknown");
        return reaped.error().code == ETIMEDOUT ? 124 : 1;
    }
    // worker 已被 reap，不能再对可能复用的 PID/PGID 发信号；endpoint 子进程由 PDEATHSIG 约束。
    if (WIFEXITED(*reaped)) {
        return WEXITSTATUS(*reaped);
    }
    return 1;
}
} // namespace tools
} // namespace kbsocket
