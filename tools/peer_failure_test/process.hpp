// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_PEER_FAILURE_TEST_PROCESS_HPP_
#define KBSOCKET_TOOLS_PEER_FAILURE_TEST_PROCESS_HPP_
#include <sys/types.h>

#include <cstdint>
#include <expected>

#include "tools/common/control_channel.hpp"

namespace kbsocket {
namespace tools {
/// kill 后 waitpid 确认真实退出；TCP 断开与发送信号本身均不构成退出证据。
std::expected<void, ToolError> KillAndReapPeer(pid_t child, std::uint32_t timeout_ms,
                                               bool require_sigkill = true) noexcept;
/// 必须在任何 URMA Load/Initialize 之前调用。父进程不访问 provider；超时终止整个 worker 进程组。
/// 124 表示 watchdog 超时，不能被解释为实验通过；回收也有时间上限，避免内核 D 状态阻塞监督者。
int RunPeerWatchdog(int (*run)(void*), void* argument, std::uint32_t timeout_ms) noexcept;
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_PEER_FAILURE_TEST_PROCESS_HPP_
