// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_PEER_FAILURE_TEST_PEER_FAILURE_TEST_HPP_
#define KBSOCKET_TOOLS_PEER_FAILURE_TEST_PEER_FAILURE_TEST_HPP_

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace tools {
enum class PeerFault : std::uint32_t { kProcessExit, kExternal, kInflightExit };
struct PeerFailureOptions {
    std::uint32_t bytes = 4096;
    std::uint32_t batch = 32;
    std::uint32_t timeout_ms = 10000;
    std::uint32_t async_grace_ms = 1000;
    std::uint32_t fault_delay_ms = 5000;
    PeerFault fault = PeerFault::kProcessExit;
};
std::expected<void, ToolError> ValidatePeerFailureOptions(const PeerFailureOptions& options) noexcept;
struct PeerFailureHello {
    PeerFailureOptions options;
    urma_jetty_id_t endpoint{};
    bool server = false;
};
using PeerFailureHelloBytes = std::array<std::byte, 48>;
PeerFailureHelloBytes EncodePeerFailureHello(const PeerFailureHello& hello) noexcept;
std::expected<PeerFailureHello, ToolError> DecodePeerFailureHello(const PeerFailureHelloBytes& wire) noexcept;
std::expected<void, ToolError> CheckPeerFailureHello(const PeerFailureHello& peer, const PeerFailureOptions& local,
                                                     bool server) noexcept;

struct PeerAsyncStats {
    std::array<std::uint32_t, 32> types{};
    std::uint32_t other = 0;
    std::uint32_t acknowledged = 0;
    bool available = false;
};
/// 仅在 async_fd 可读后调用 Get；每次成功取出的事件恰好 Ack 一次，不把无事件解释为无故障。
std::expected<void, ToolError> PollPeerAsync(urma_context_t* ctx, PeerAsyncStats& stats) noexcept;

struct PeerTxStats {
    std::uint32_t accepted = 0;
    std::uint32_t retired = 0;
    bool accepted_known = true;
    int post_status = 0;
    std::array<std::uint32_t, 32> statuses{};
    std::uint64_t first_cqe_ns = 0;
    std::uint64_t last_cqe_ns = 0;
};

/// 工具专用单 owner 会话：不因普通错误 CQE 主动 modify ERROR，不重放 accepted WR。
/// 未证明终结的 WR、RX 和依赖资源由入口 NoDestructor 保留至进程退出。
class PeerFailureSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const PeerFailureOptions& options, bool server);
    std::expected<void, ToolError> Import(const urma_jetty_id_t& endpoint);
    std::expected<void, ToolError> ReceiveBaseline();
    std::expected<void, ToolError> PrepareProbeReceives();
    std::expected<void, ToolError> Send(std::uint32_t count, bool baseline);
    /// 分离 post 与 poll，允许控制面在 post 之后发起进程退出；不据此推断硬件仍然在途。
    std::expected<void, ToolError> Submit(std::uint32_t count);
    std::expected<void, ToolError> PollCompletions();
    std::expected<void, ToolError> WaitForTerminal(bool baseline = false);
    std::expected<std::uint32_t, ToolError> QueryState();
    std::expected<void, ToolError> ObserveAsync(std::uint32_t duration_ms);
    void ResetAsyncObservation() noexcept {
        async_ = {};
    }
    std::expected<void, ToolError> Close() noexcept;
    const urma_jetty_id_t& endpoint() const noexcept {
        return endpoint_;
    }
    const PeerTxStats& tx() const noexcept {
        return tx_;
    }
    const PeerAsyncStats& async() const noexcept {
        return async_;
    }

private:
    std::expected<void, ToolError> ObserveTx(const urma_cr_t& cr, std::uint64_t elapsed_ns);
    PeerFailureOptions options_;
    urma_context_t* ctx_ = nullptr;
    urma_jetty_id_t endpoint_{};
    urma_target_jetty_t* remote_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    raw::JettyPool pool_;
    raw::JettyLane lane_;
    raw::RxBufferPool rx_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    std::vector<std::byte> buffer_;
    std::array<urma_sge_t, raw::TxSender::kMaxBatch> sges_{};
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests_{};
    std::array<raw::AttemptId, raw::TxSender::kMaxBatch> ids_{};
    std::array<bool, raw::TxSender::kMaxBatch> seen_{};
    PeerTxStats tx_;
    PeerAsyncStats async_;
    bool ready_ = false;
    bool server_ = false;
    bool unsafe_ = false;
    std::uint64_t generation_ = 0;
    std::uint32_t requested_ = 0;
    std::chrono::steady_clock::time_point submitted_;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_PEER_FAILURE_TEST_PEER_FAILURE_TEST_HPP_
