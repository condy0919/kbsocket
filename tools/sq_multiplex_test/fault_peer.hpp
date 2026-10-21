// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_SQ_MULTIPLEX_TEST_FAULT_PEER_HPP_
#define KBSOCKET_TOOLS_SQ_MULTIPLEX_TEST_FAULT_PEER_HPP_

#include <sys/types.h>

#include <cstdint>
#include <expected>
#include <memory>

#include "tools/common/control_channel.hpp"
#include "tools/sq_multiplex_test/sq_multiplex_test.hpp"

#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
std::expected<void, ToolError> CheckMultiplexBaseline(const raw::RxBufferEvent& event, const urma_jetty_id_t& receiver,
                                                      const urma_jetty_id_t& sender, std::uint32_t bytes) noexcept;

/// 必须先于任何 URMA Load 创建；独立进程拥有故障 JFR/MR，健康端点只存在于父进程。
class MultiplexPeerProcess final : public MultiplexFaultPeer {
public:
    ~MultiplexPeerProcess() override;
    std::expected<void, ToolError> Open(const char* library, const char* device, std::uint32_t eid_index,
                                        const SqMultiplexOptions& options);
    const urma_jetty_id_t& endpoint() const noexcept override {
        return endpoint_;
    }
    std::expected<void, ToolError> FinishBaselineAndExit(const urma_jetty_id_t& sender) override;

private:
    std::expected<void, ToolError> Stop(bool require_sigkill);
    pid_t pid_ = -1;
    std::unique_ptr<ControlChannel> channel_;
    SqMultiplexOptions options_;
    urma_jetty_id_t endpoint_{};
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_SQ_MULTIPLEX_TEST_FAULT_PEER_HPP_
