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

#include "tools/provider_boundary_test/provider_boundary_test.hpp"

namespace kbsocket {
namespace tools {
std::expected<ProviderWorkerOutcome, ToolError> SuperviseProviderWorker(int (*run)(),
                                                                        std::uint32_t timeout_ms) noexcept {
    if (!run || !timeout_ms) {
        return std::unexpected(ToolError{"invalid provider watchdog options", EINVAL});
    }
    std::fflush(nullptr);
    const auto parent = ::getpid();
    const auto child = ::fork();
    if (child < 0) {
        return std::unexpected(ToolError{"fork provider worker", errno});
    }
    if (!child) {
        // 父进程被终止时不能留下仍持有设备资源的实验进程；检查设置期间父进程退出的竞争。
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) {
            std::_Exit(1);
        }
        int result = 1;
        try {
            result = run();
        } catch (...) {
            std::fputs("FAIL: provider worker exception; dependencies retained until process exit\n", stderr);
        }
        std::fflush(nullptr);
        std::_Exit(result);
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    int status = 0;
    bool timed_out = false;
    for (;;) {
        const auto waited = ::waitpid(child, &status, WNOHANG);
        if (waited == child) {
            break;
        }
        if (waited < 0 && errno != EINTR) {
            return std::unexpected(ToolError{"wait provider worker", errno});
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            timed_out = true;
            if (::kill(child, SIGKILL) != 0 && errno != ESRCH) {
                return std::unexpected(ToolError{"kill timed out provider worker", errno});
            }
            const auto reap_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            for (;;) {
                const auto reaped = ::waitpid(child, &status, WNOHANG);
                if (reaped == child) {
                    break;
                }
                if (reaped < 0 && errno != EINTR) {
                    return std::unexpected(ToolError{"reap provider worker", errno});
                }
                // D 状态可能延迟 SIGKILL，不能在故障实验的父进程中改用无界 waitpid。
                if (std::chrono::steady_clock::now() >= reap_deadline) {
                    return std::unexpected(ToolError{"provider worker still alive after SIGKILL", ETIMEDOUT});
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return ProviderWorkerOutcome{.timed_out = timed_out,
                                 .exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1,
                                 .signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0};
}
} // namespace tools
} // namespace kbsocket
