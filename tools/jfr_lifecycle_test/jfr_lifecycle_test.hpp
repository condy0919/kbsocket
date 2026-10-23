// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_JFR_LIFECYCLE_TEST_JFR_LIFECYCLE_TEST_HPP_
#define KBSOCKET_TOOLS_JFR_LIFECYCLE_TEST_JFR_LIFECYCLE_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
struct JfrLifecycleOptions {
    std::uint32_t rx_depth = 32;
    std::uint32_t consumed = 0;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateJfrLifecycleOptions(const JfrLifecycleOptions& options) noexcept;

struct ChildOutcome {
    bool timed_out = false;
    int exit_code = 0;
    int signal = 0;
};
/// 必须在加载 URMA、创建 provider 线程前调用；父进程监督整个实验，而非只监督 poll。
std::expected<ChildOutcome, ToolError> RunSupervised(int (*run)(void*), void* argument,
                                                     std::uint32_t timeout_ms) noexcept;

struct JfrLifecycleStats {
    std::uint32_t posted = 0;
    std::uint32_t received = 0;
    std::uint32_t tx_retired = 0;
    std::uint32_t residual_at_delete = 0;
    std::uint64_t delete_jfr_ns = 0;
    bool delete_jfr_attempted = false;
    int delete_jfr_status = 0;
};

/// 专用原始资源观察器，刻意保留 posted RX 至 DeleteJfr；不能用于替代生产 RX 池关闭协议。
/// 入口必须保持对象存活至显式 Close 成功；故障后不重试删除或释放其余 DMA 依赖。
class JfrLifecycleSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const JfrLifecycleOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel* channel);
    std::expected<void, ToolError> Close();
    const JfrLifecycleStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> PostReceive();
    std::expected<void, ToolError> Send();
    std::expected<void, ToolError> Poll(bool receive);
    std::expected<void, ToolError> Exchange(ControlChannel& channel);

    JfrLifecycleOptions options_;
    JfrLifecycleStats stats_;
    urma_context_t* ctx_ = nullptr;
    std::array<urma_jfc_t*, 2> cqs_{};
    urma_jfr_t* jfr_ = nullptr;
    std::array<urma_jetty_t*, 2> jetties_{};
    urma_target_jetty_t* target_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    std::vector<std::byte> storage_;
    std::span<std::byte> memory_;
    std::array<urma_sge_t, 256> sges_{};
    std::array<urma_jfr_wr_t, 256> rx_wrs_{};
    std::array<urma_jfs_wr_t, 256> tx_wrs_{};
    std::array<bool, 256> seen_{};
    std::array<bool, 256> payload_seen_{};
    bool server_ = false;
    bool ready_ = false;
    bool started_ = false;
    bool quiescent_ = true;
    bool failed_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_JFR_LIFECYCLE_TEST_JFR_LIFECYCLE_TEST_HPP_
