// SPDX-License-Identifier: MulanPSL-2.0
#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "tools/jfr_lifecycle_test/jfr_lifecycle_test.hpp"

namespace kbsocket {
namespace tools {
std::expected<ChildOutcome, ToolError> RunSupervised(int (*run)(void*), void* argument,
                                                     std::uint32_t timeout_ms) noexcept {
    if (!run || !timeout_ms) {
        return std::unexpected(ToolError{"invalid watchdog options", EINVAL});
    }
    // fork 前排空输出，避免子进程重复刷新父进程继承的 stdio 缓冲。
    std::fflush(nullptr);
    const auto parent = ::getpid();
    const auto pid = ::fork();
    if (pid < 0) {
        return std::unexpected(ToolError{"fork hardware worker", errno});
    }
    if (pid == 0) {
        // 监督者异常退出后仍必须终止硬件 worker；同时覆盖 fork 与 prctl 之间父进程已退出的竞态。
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) {
            std::_Exit(1);
        }
        int result = 1;
        try {
            result = run(argument);
        } catch (...) {
            std::fputs("FAIL: hardware worker threw an exception; dependencies retained until process exit\n", stderr);
        }
        std::fflush(nullptr);
        // provider 失败后不执行静态析构；进程退出由内核回收整个资源域。
        std::_Exit(result);
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    int status = 0;
    bool timed_out = false;
    for (;;) {
        const auto waited = ::waitpid(pid, &status, WNOHANG);
        if (waited == pid) {
            break;
        }
        if (waited < 0 && errno != EINTR) {
            return std::unexpected(ToolError{"wait hardware worker", errno});
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            timed_out = true;
            // 只终止本次 fork 返回的子进程，不触碰其他工具或设备使用者。
            if (::kill(pid, SIGKILL) < 0 && errno != ESRCH) {
                return std::unexpected(ToolError{"kill timed out hardware worker", errno});
            }
            const auto reap_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            for (;;) {
                const auto reaped = ::waitpid(pid, &status, WNOHANG);
                if (reaped == pid) {
                    break;
                }
                if (reaped < 0 && errno != EINTR) {
                    return std::unexpected(ToolError{"reap timed out hardware worker", errno});
                }
                // 不可中断的内核等待也可能延迟 SIGKILL，父进程不能再被无界 waitpid 卡住。
                if (std::chrono::steady_clock::now() >= reap_deadline) {
                    return std::unexpected(
                        ToolError{"worker still alive after SIGKILL; kernel cleanup unresolved", ETIMEDOUT});
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return ChildOutcome{.timed_out = timed_out,
                        .exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1,
                        .signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0};
}
} // namespace tools
} // namespace kbsocket
