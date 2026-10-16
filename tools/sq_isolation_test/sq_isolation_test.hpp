// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_SQ_ISOLATION_TEST_SQ_ISOLATION_TEST_HPP_
#define KBSOCKET_TOOLS_SQ_ISOLATION_TEST_SQ_ISOLATION_TEST_HPP_

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
inline constexpr std::size_t kIsolationPayloadHeader = 32;
inline constexpr std::uint32_t kIsolationAfterRounds = 2;

struct SqIsolationOptions {
    std::uint32_t bytes = 4096;
    std::uint32_t batch = 32;
    // 故障启动后健康 SQ 的收发轮数；排空完成后另运行固定两轮。
    std::uint32_t rounds = 8;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateIsolationOptions(const SqIsolationOptions& options) noexcept;

struct SqIsolationHello {
    SqIsolationOptions options;
    bool server = false;
    // 依次为主池的故障候选 SQ、健康 SQ、独立且不投接收 WR 的 sink。
    std::array<urma_jetty_id_t, 3> endpoints{};
};
using IsolationHelloBytes = std::array<std::byte, 92>;
IsolationHelloBytes EncodeIsolationHello(const SqIsolationHello& hello) noexcept;
std::expected<SqIsolationHello, ToolError> DecodeIsolationHello(const IsolationHelloBytes& wire) noexcept;
std::expected<SqIsolationHello, ToolError> ExchangeIsolationHello(ControlChannel& channel,
                                                                  const SqIsolationHello& local) noexcept;
std::expected<void, ToolError> IsolationBarrier(ControlChannel& channel, std::uint32_t marker,
                                                std::uint32_t timeout_ms) noexcept;
std::expected<void, ToolError> ConfirmIsolationClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept;

enum class IsolationPhase : std::uint32_t { kBefore = 0, kDuring = 1, kAfter = 2 };
struct IsolationFrame {
    IsolationPhase phase = IsolationPhase::kBefore;
    std::uint32_t source = 0;
    std::uint32_t round = 0;
    std::uint32_t index = 0;
    bool server = false;
};
std::uint64_t IsolationImmediate(const IsolationFrame& frame) noexcept;
void FillIsolationPayload(std::span<std::byte> data, const IsolationFrame& frame) noexcept;
std::expected<IsolationFrame, ToolError> CheckIsolationPayload(std::span<const std::byte> data) noexcept;

struct IsolationTrafficStats {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t received = 0;
};
struct IsolationFaultStats {
    std::uint32_t accepted = 0;
    std::uint32_t retired = 0;
    std::uint32_t success = 0;
    std::uint32_t flush_error = 0;
    std::uint32_t unhandled = 0;
    std::uint32_t loc_access_error = 0;
    std::uint32_t remote_access_abort_error = 0;
    std::uint32_t ack_timeout_error = 0;
    std::uint32_t rnr_retry_count_exceeded_error = 0;
    std::uint32_t other_error = 0;
    int last_other_error = 0;
    std::uint32_t flush_done = 0;
    std::uint32_t rejected_sends = 0;
};
struct SqIsolationStats {
    // before 包括两条 SQ 的基线；during/after 只有健康 SQ 的正常消息。
    std::array<IsolationTrafficStats, 3> traffic{};
    IsolationFaultStats fault;
    bool accepted_known = true;
    std::uint32_t healthy_ready_checks = 0;
    // 仅记录实际观察，不要求非零：硬件可能在健康消息完成前迅速排空故障 SQ。
    std::uint64_t healthy_retired_before_drained = 0;
    std::optional<urma_cr_t> failed_tx;
    std::optional<urma_cr_t> failed_rx;
};

/// 双端均由单 owner 推进；主池的两个 SQ 共用 JFR/TX CQ/RX CQ，sink 使用独立空 JFR。
/// 无法证明 DMA 或对端已停止时保留完整依赖链；入口用 NoDestructor，正常路径显式 Close。
class SqIsolationSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const SqIsolationOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close() noexcept;
    const SqIsolationStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> RunRound(ControlChannel& channel, IsolationPhase phase, std::uint32_t round);
    std::expected<void, ToolError> Post(std::uint32_t lane, bool fault);
    std::expected<void, ToolError> StartFault();
    std::expected<void, ToolError> CheckSendRejected();
    std::expected<void, ToolError> CheckHealthy();
    std::expected<std::size_t, ToolError> ProgressTx();
    std::expected<void, ToolError> ObserveTx(const raw::TxCompletionEvent& event, bool software);
    std::expected<std::size_t, ToolError> PollRx();
    std::expected<void, ToolError> ValidateRx(const raw::RxBufferEvent& event);
    std::size_t Slot(std::uint32_t lane, std::uint32_t index) const noexcept;

    SqIsolationOptions options_;
    SqIsolationHello local_, peer_;
    SqIsolationStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    // 仅导入健康接收目标和 sink；故障候选 SQ 不作为远端 RX 目标。
    std::array<urma_target_jetty_t*, 2> remotes_{};
    std::array<raw::JettyLane, 2> lanes_{};
    raw::JettyPool pool_;
    raw::JettyPool sink_;
    raw::RxBufferPool rx_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_;
    static constexpr std::size_t kSlots = 2 * raw::TxSender::kMaxBatch;
    std::array<urma_sge_t, kSlots> sges_{};
    std::array<raw::AttemptId, kSlots> ids_{};
    std::array<bool, kSlots> tx_seen_{}, rx_seen_{};
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests_{};
    std::array<raw::TxCompletionEvent, raw::TxCompletionProcessor::kMaxPollBatch> tx_events_{};
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> rx_events_{};
    IsolationPhase phase_ = IsolationPhase::kBefore;
    std::uint32_t round_ = 0;
    std::chrono::steady_clock::time_point drain_deadline_{};
    bool fault_started_ = false;
    bool drained_ = false;
    bool ready_ = false;
    bool started_ = false;
    bool peer_quiescent_ = true;
    bool unsafe_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_SQ_ISOLATION_TEST_SQ_ISOLATION_TEST_HPP_
