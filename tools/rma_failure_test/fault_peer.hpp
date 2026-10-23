// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_RMA_FAILURE_TEST_FAULT_PEER_HPP_
#define KBSOCKET_TOOLS_RMA_FAILURE_TEST_FAULT_PEER_HPP_

#include <sys/types.h>

#include <cstdint>
#include <expected>
#include <memory>

#include "tools/common/control_channel.hpp"
#include "tools/rma_failure_test/rma_failure_test.hpp"
#include "tools/rma_test/rma_test.hpp"

namespace kbsocket {
namespace tools {
/// 必须先于任何 URMA Load 创建；子进程独占 WRITE 目标 MR，父进程持有健康端点和 TCP。
class RmaFailurePeerProcess final : public RmaFailurePeer {
public:
    ~RmaFailurePeerProcess() override;
    std::expected<void, ToolError> Open(const char* library, const char* device, std::uint32_t eid_index,
                                        const RmaFailureOptions& options);
    const RmaHello& hello() const noexcept override {
        return hello_;
    }
    std::expected<void, ToolError> FinishBaselineAndExit() override;

private:
    std::expected<void, ToolError> Stop(bool require_sigkill);
    pid_t pid_ = -1;
    std::unique_ptr<ControlChannel> channel_;
    RmaFailureOptions options_;
    RmaHello hello_;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_RMA_FAILURE_TEST_FAULT_PEER_HPP_
