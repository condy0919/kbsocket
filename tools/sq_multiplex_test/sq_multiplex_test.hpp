// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_SQ_MULTIPLEX_TEST_SQ_MULTIPLEX_TEST_HPP_
#define KBSOCKET_TOOLS_SQ_MULTIPLEX_TEST_SQ_MULTIPLEX_TEST_HPP_

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
#include "kbsocket/transport/raw/tx_sender.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
inline constexpr std::size_t kMultiplexPayloadHeader = 32;
struct SqMultiplexOptions {
    std::uint32_t bytes = 4096;
    // 每个目标的消息数；混合阶段合计 2 * batch，一次 post 到同一 SQ。
    std::uint32_t batch = 32;
    std::uint32_t rounds = 8;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateMultiplexOptions(const SqMultiplexOptions& options) noexcept;

struct SqMultiplexHello {
    SqMultiplexOptions options;
    bool server = false;
    // 服务端为 healthy/sink；客户端仅第一个为发送 SQ，第二个在线上必须全零。
    std::array<urma_jetty_id_t, 2> endpoints{};
};
using MultiplexHelloBytes = std::array<std::byte, 72>;
MultiplexHelloBytes EncodeMultiplexHello(const SqMultiplexHello& hello) noexcept;
std::expected<SqMultiplexHello, ToolError> DecodeMultiplexHello(const MultiplexHelloBytes& wire) noexcept;
std::expected<SqMultiplexHello, ToolError> ExchangeMultiplexHello(ControlChannel& channel,
                                                                  const SqMultiplexHello& local) noexcept;
std::expected<void, ToolError> MultiplexBarrier(ControlChannel& channel, std::uint32_t marker,
                                                std::uint32_t timeout_ms) noexcept;
std::expected<void, ToolError> ConfirmMultiplexClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept;

struct MultiplexPhaseResult {
    std::uint32_t phase_id = 0;
    bool success = false;
    // 本阶段已接受 TX 均有终结证明；任一端报告失败后双方不再发送，断连不代替此确认。
    bool tx_quiescent = false;
    int error_code = 0;
};
std::expected<MultiplexPhaseResult, ToolError> ExchangeMultiplexPhaseResult(ControlChannel& channel,
                                                                            const MultiplexPhaseResult& local,
                                                                            std::uint32_t timeout_ms) noexcept;

enum class MultiplexPhase : std::uint32_t { kBefore = 0, kMixed = 1, kAfter = 2 };
struct MultiplexFrame {
    MultiplexPhase phase = MultiplexPhase::kBefore;
    std::uint32_t round = 0;
    std::uint32_t index = 0;
    bool sink = false;
};
std::uint64_t MultiplexImmediate(const MultiplexFrame& frame) noexcept;
void FillMultiplexPayload(std::span<std::byte> data, const MultiplexFrame& frame) noexcept;
std::expected<MultiplexFrame, ToolError> CheckMultiplexPayload(std::span<const std::byte> data) noexcept;

struct MultiplexTraffic {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t success = 0;
    std::uint64_t rnr = 0;
    std::uint64_t loc_access_error = 0;
    std::uint64_t remote_access_abort_error = 0;
    std::uint64_t ack_timeout_error = 0;
    std::uint64_t received = 0;
    std::uint64_t other_error = 0;
    int last_other_status = 0;
    // 各批 post 调用前到软件消费 CQE 的耗时，跨轮取最小/最大值；不是硬件时间戳。
    std::uint64_t min_cqe_ns = 0;
    std::uint64_t max_cqe_ns = 0;
};
struct MultiplexRound {
    bool started = false;
    std::uint32_t healthy_retired = 0;
    std::uint32_t sink_retired = 0;
    std::uint32_t healthy_success = 0;
    std::uint32_t sink_rnr = 0;
    // healthy 时序包含其所有合法终结状态，不能把错误完成漏掉后仍称为最后完成。
    std::uint64_t first_healthy_ns = 0;
    std::uint64_t last_healthy_ns = 0;
    std::uint64_t first_rnr_ns = 0;
    std::uint64_t last_rnr_ns = 0;
    std::uint64_t first_sink_ns = 0;
    std::uint64_t last_sink_ns = 0;
    std::uint32_t healthy_before_first_rnr = 0;
    std::uint32_t healthy_before_last_rnr = 0;
};
struct SqMultiplexStats {
    // 三阶段健康流量与混合阶段的空 JFR 目标分别计数。
    std::array<MultiplexTraffic, 3> healthy{};
    MultiplexTraffic sink;
    std::vector<MultiplexRound> rounds;
    std::uint32_t jetty_id = 0;
    std::uint32_t state_checks = 0;
    std::uint32_t requested_rnr_retry = 0;
    std::uint32_t requested_err_timeout = 0;
    std::uint32_t queried_rnr_retry = 0;
    std::uint32_t queried_err_timeout = 0;
    std::optional<std::uint32_t> queried_state;
    std::uint64_t provider_eagain = 0;
    bool accepted_known = true;
};

/// 单 owner 硬件实验：客户端一条 SQ，服务端两个独立 JFR，普通 RNR 不触发 ERROR 排空。
/// 正常关闭前须证明所有 TX/RX 终结并完成双端屏障；未决资源由入口常驻保留。
class SqMultiplexSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const SqMultiplexOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close() noexcept;
    const SqMultiplexStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> CheckState();
    std::expected<void, ToolError> RunPhase(ControlChannel& channel);
    std::expected<void, ToolError> TransferPhase(ControlChannel& channel, bool& rx_ready);
    std::expected<void, ToolError> Send();
    std::expected<void, ToolError> FinishTx();
    std::expected<std::size_t, ToolError> PollTx();
    std::expected<void, ToolError> ObserveTx(const urma_cr_t& completion);
    std::expected<void, ToolError> Receive();
    std::expected<std::size_t, ToolError> PollRx();
    std::expected<void, ToolError> ValidateRx(const raw::RxBufferEvent& event);
    std::size_t RequestCount() const noexcept;
    MultiplexFrame Frame(std::size_t slot) const noexcept;
    MultiplexTraffic& Traffic(bool sink) noexcept;

    SqMultiplexOptions options_;
    SqMultiplexHello local_, peer_;
    SqMultiplexStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    std::array<urma_target_jetty_t*, 2> remotes_{};
    raw::JettyPool pool_;
    raw::JettyPool sink_;
    raw::RxBufferPool rx_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::JettyLane lane_{};
    urma_jetty_t* jetty_ = nullptr;
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_;
    std::array<urma_sge_t, raw::TxSender::kMaxBatch> sges_{};
    std::array<raw::AttemptId, raw::TxSender::kMaxBatch> ids_{};
    std::array<bool, raw::TxSender::kMaxBatch> tx_seen_{};
    std::array<bool, raw::TxSender::kMaxBatch / 2> rx_seen_{};
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests_{};
    std::array<urma_cr_t, 64> completions_{};
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> rx_events_{};
    MultiplexPhase phase_ = MultiplexPhase::kBefore;
    std::uint32_t round_ = 0;
    std::uint32_t phase_received_ = 0;
    std::uint32_t mixed_healthy_ = 0;
    std::uint32_t mixed_rnr_ = 0;
    // 预期状态不匹配不破坏逐 WR 终结证明；延迟到整批收齐后才上报阶段失败。
    std::optional<ToolError> phase_error_;
    std::chrono::steady_clock::time_point submitted_{};
    bool ready_ = false;
    bool started_ = false;
    bool peer_quiescent_ = true;
    bool unsafe_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_SQ_MULTIPLEX_TEST_SQ_MULTIPLEX_TEST_HPP_
