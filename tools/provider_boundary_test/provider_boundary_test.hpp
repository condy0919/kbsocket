// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_PROVIDER_BOUNDARY_TEST_PROVIDER_BOUNDARY_TEST_HPP_
#define KBSOCKET_TOOLS_PROVIDER_BOUNDARY_TEST_PROVIDER_BOUNDARY_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
struct ProviderBoundaryOptions {
    std::uint32_t tx_depth = 32;
    std::uint32_t batch = 32;
    std::uint32_t attempts = 1024;
    std::uint32_t timeout_ms = 10000;
    bool error_post = false;
};

std::expected<void, ToolError> ValidateProviderBoundaryOptions(const ProviderBoundaryOptions& options) noexcept;

struct ProviderWorkerOutcome {
    bool timed_out = false;
    int exit_code = -1;
    int signal = 0;
};

/// 必须在加载 URMA 前调用；独立父进程为同步 provider 调用提供硬截止，不运行资源析构。
std::expected<ProviderWorkerOutcome, ToolError> SuperviseProviderWorker(int (*run)(),
                                                                        std::uint32_t timeout_ms) noexcept;

struct ProviderBoundaryStats {
    std::uint64_t offered = 0;
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t post_calls = 0;
    std::uint64_t partial_batches = 0;
    std::uint64_t eagain = 0;
    std::uint64_t enomem = 0;
    std::uint64_t flush_done = 0;
    std::uint64_t flush_calls = 0;
    std::array<std::uint64_t, 17> statuses{};
    int last_post_status = 0;
    int error_post_status = 0;
    int queried_state = -1;
    bool accepted_known = true;
    bool error_post_attempted = false;
    bool error_post_pending = false;
};

/// 独立实验账本：不预扣 JettyPool quota，严格按 provider 的 bad_wr 前缀和逐条完成回收所有权。
class ProviderBoundaryTracker {
public:
    void Initialize(std::size_t max_id, std::uint32_t jetty_id);
    std::expected<std::size_t, ToolError> RecordPost(std::span<urma_jfs_wr_t> wrs, urma_status_t status,
                                                     urma_jfs_wr_t* bad_wr) noexcept;
    std::expected<void, ToolError> Complete(const urma_cr_t& cr, bool software_flush) noexcept;
    void BeginError() noexcept {
        error_ = true;
    }
    bool pending() const noexcept {
        return stats.accepted != stats.retired;
    }
    ProviderBoundaryStats stats;

private:
    std::vector<std::uint8_t> states_;
    std::uint32_t jetty_id_ = 0;
    bool error_ = false;
};

/// 必须由 NoDestructor 持有。发生未知接受、缺失 CQE 或 ERROR post 无终结时保留完整 DMA 依赖链。
class ProviderBoundarySession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const ProviderBoundaryOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close();
    const ProviderBoundaryStats& stats() const noexcept {
        return tracker_.stats;
    }
    bool intentionally_retained() const noexcept {
        return intentional_retention_;
    }

private:
    std::expected<void, ToolError> ExchangeHello(ControlChannel& channel);
    std::expected<void, ToolError> RunClient();
    std::expected<void, ToolError> Drain(bool probe);
    std::expected<std::size_t, ToolError> Post(std::size_t first, std::size_t count);
    ProviderBoundaryOptions options_;
    ProviderBoundaryTracker tracker_;
    raw::JettyPool pool_;
    urma_context_t* ctx_ = nullptr;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    std::vector<std::byte> storage_;
    std::vector<urma_jfs_wr_t> wrs_;
    urma_sge_t sge_{};
    bool server_ = false;
    bool started_ = false;
    bool published_ = false;
    bool quiescent_ = true;
    bool unsafe_ = false;
    bool intentional_retention_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_PROVIDER_BOUNDARY_TEST_PROVIDER_BOUNDARY_TEST_HPP_
