// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_ASYNC_ERROR_TEST_ASYNC_ERROR_TEST_HPP_
#define KBSOCKET_TOOLS_ASYNC_ERROR_TEST_ASYNC_ERROR_TEST_HPP_

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
enum class AsyncFault : std::uint32_t { kJfcOverflow, kExternalJfc, kExternalJfr };
const char* AsyncFaultName(AsyncFault fault) noexcept;
struct AsyncErrorOptions {
    AsyncFault fault = AsyncFault::kJfcOverflow;
    std::uint32_t attempts = 512;
    std::uint32_t cq_depth = 64;
    std::uint32_t healthy_rounds = 16;
    std::uint32_t observe_ms = 2000;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateAsyncErrorOptions(const AsyncErrorOptions& options) noexcept;

struct AsyncErrorObjects {
    urma_context_t* ctx = nullptr;
    urma_jfc_t* fault_tx = nullptr;
    urma_jfc_t* fault_rx = nullptr;
    urma_jfr_t* fault_jfr = nullptr;
    std::array<urma_jetty_t*, 2> fault_jetties{};
    urma_jfc_t* healthy_tx = nullptr;
    urma_jfc_t* healthy_rx = nullptr;
    urma_jfr_t* healthy_jfr = nullptr;
    urma_jetty_t* healthy_jetty = nullptr;
};
struct AsyncErrorEvents {
    std::uint64_t retrieved = 0;
    std::uint64_t acknowledged = 0;
    std::uint64_t fault_jfc = 0;
    std::uint64_t fault_rx_jfc = 0;
    std::uint64_t fault_tx_jfc = 0;
    std::uint64_t fault_jfr = 0;
    std::array<std::uint64_t, 2> fault_jetty{};
    std::uint64_t healthy = 0;
    std::uint64_t other = 0;
    int last_type = -1;
    std::array<std::uint64_t, 32> types{};
};
/// 先快照类型与归属，再原样 Ack；未知对象只记录，不解引用。Ack 不归还任何 DMA 所有权。
std::expected<void, ToolError> RecordAsyncErrorEvent(urma_async_event_t& event, const AsyncErrorObjects& objects,
                                                     AsyncErrorEvents& stats) noexcept;

struct AsyncQueueStats {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t posted = 0;
    std::uint64_t received = 0;
    std::uint64_t rx_retired = 0;
    std::array<std::uint64_t, 17> statuses{};
    std::array<std::uint64_t, 2> flush_done{};
    std::array<int, 2> state_before{-1, -1};
    std::array<int, 2> state_after{-1, -1};
    int post_status = 0;
    int poll_status = 0;
    int modify_status = 0;
    int flush_status = 0;
    int jfr_state_before = -1;
    int jfr_state_after = -1;
    int jfr_query_status = 0;
    std::uint32_t queried_rx_depth = 0;
    std::array<int, 2> query_status{};
    std::array<int, 2> modify_statuses{};
    bool accepted_known = true;
};

/// 专用故障资源，不使用生产池的最小 CQ 容量保护来制造正常路径非法配置。
/// 每条 WR 使用独立 buffer 与身份；错误 CQ 不假装成有效 RX，不重放未知接受范围。
class AsyncErrorQueue {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap, std::uint32_t count,
                                        std::uint32_t rx_cq_depth, bool server, std::uint32_t jets);
    std::expected<void, ToolError> Import(std::span<const urma_jetty_id_t> peers);
    std::expected<void, ToolError> Post(std::uint32_t first, std::uint32_t count);
    std::expected<void, ToolError> Poll(bool require_success);
    std::expected<void, ToolError> BeginDrain();
    std::expected<void, ToolError> Flush();
    void Query(bool after);
    std::expected<void, ToolError> Close();
    bool complete() const noexcept;
    AsyncQueueStats stats;
    urma_jfc_t* tx = nullptr;
    urma_jfc_t* rx = nullptr;
    urma_jfr_t* jfr = nullptr;
    std::array<urma_jetty_t*, 2> jetties{};

private:
    std::expected<void, ToolError> Complete(const urma_cr_t& cr, bool require_success, bool flush);
    urma_context_t* ctx_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    std::array<urma_target_jetty_t*, 2> peers_{};
    std::array<urma_jetty_id_t, 2> peer_ids_{};
    std::vector<std::byte> storage_;
    std::span<std::byte> memory_;
    std::vector<urma_sge_t> sges_;
    std::vector<urma_jfs_wr_t> sends_;
    std::vector<urma_jfr_wr_t> recvs_;
    std::vector<std::uint8_t> states_;
    std::vector<bool> received_ids_;
    std::uint32_t jets_ = 0;
    bool server_ = false;
    bool draining_ = false;
    bool poll_failed_ = false;
};

struct AsyncErrorStats {
    AsyncErrorEvents baseline;
    AsyncErrorEvents observation;
    AsyncErrorEvents after;
    AsyncErrorEvents cleanup;
    bool target_observed = false;
    bool peer_target_observed = false;
    bool can_close = false;
};
/// 入口须用 NoDestructor，无法证明终结时双方都保留依赖，整个进程受外部 watchdog 监督。
class AsyncErrorSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const AsyncErrorOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close();
    AsyncErrorQueue fault;
    AsyncErrorQueue healthy;
    AsyncErrorStats stats;

private:
    std::expected<void, ToolError> Collect(AsyncErrorEvents& events);
    std::expected<void, ToolError> Exchange(ControlChannel& channel);
    std::expected<void, ToolError> HealthyRound(ControlChannel& channel, std::uint32_t round, std::uint32_t hold_ms);
    std::expected<void, ToolError> Baseline(ControlChannel& channel);
    AsyncErrorObjects objects_;
    AsyncErrorOptions options_;
    bool server_ = false;
    bool started_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_ASYNC_ERROR_TEST_ASYNC_ERROR_TEST_HPP_
