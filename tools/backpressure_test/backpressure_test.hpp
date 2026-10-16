// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_BACKPRESSURE_TEST_BACKPRESSURE_TEST_HPP_
#define KBSOCKET_TOOLS_BACKPRESSURE_TEST_BACKPRESSURE_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
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
enum class BackpressureMode : std::uint32_t { kTxQuota = 0, kRxDelay = 1, kRnrContinue = 2 };
enum class BackpressureExpectation : std::uint32_t { kEither = 0, kSuccess = 1, kError = 2 };
struct BackpressureOptions {
    BackpressureMode mode = BackpressureMode::kTxQuota;
    BackpressureExpectation expect = BackpressureExpectation::kEither;
    std::uint32_t bytes = 4096;
    std::uint32_t tx_depth = 32;
    // 仅 tx_quota 使用 rounds；其余模式各测试一组固定阶段。
    std::uint32_t rounds = 8;
    std::uint32_t rx_delay_us = 1000;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateBackpressureOptions(const BackpressureOptions& options) noexcept;
struct BackpressureHello {
    BackpressureOptions options;
    bool server = false;
    // rx_delay 的第二条 SQ 只发送清理消息，不重放原始数据。
    std::array<urma_jetty_id_t, 2> endpoints{};
};
using BackpressureHelloBytes = std::array<std::byte, 88>;
BackpressureHelloBytes EncodeBackpressureHello(const BackpressureHello& hello) noexcept;
std::expected<BackpressureHello, ToolError> DecodeBackpressureHello(const BackpressureHelloBytes& wire) noexcept;
std::expected<BackpressureHello, ToolError> ExchangeBackpressureHello(ControlChannel& channel,
                                                                      const BackpressureHello& local) noexcept;
std::expected<void, ToolError> BackpressureBarrier(ControlChannel& channel, std::uint32_t marker,
                                                   std::uint32_t timeout_ms) noexcept;
std::expected<void, ToolError> ConfirmBackpressureClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept;

inline constexpr std::size_t kBackpressurePayloadHeader = 32;
struct BackpressureFrame {
    std::uint32_t round = 0;
    std::uint32_t index = 0;
    bool cleanup = false;
};
void FillBackpressurePayload(std::span<std::byte> data, const BackpressureFrame& frame) noexcept;
std::expected<BackpressureFrame, ToolError> CheckBackpressurePayload(std::span<const std::byte> data) noexcept;
std::uint64_t BackpressureImmediate(const BackpressureFrame& frame) noexcept;

struct BackpressureTraffic {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t received = 0;
    std::uint64_t success = 0;
    std::uint64_t flush_error = 0;
    std::uint64_t unhandled = 0;
    std::uint64_t loc_access_error = 0;
    std::uint64_t remote_access_abort_error = 0;
    std::uint64_t ack_timeout_error = 0;
    std::uint64_t rnr_retry_count_exceeded_error = 0;
    std::uint64_t other_error = 0;
    std::uint64_t flush_done = 0;
    std::uint64_t rejected_sends = 0;
    int last_other_error = 0;
};
// 控制面交换终结计数；不把错误 TX 推断为一定没有发生远端 RX。
struct BackpressureReport {
    std::uint32_t accepted = 0;
    std::uint32_t retired = 0;
    std::uint32_t success = 0;
    std::uint32_t flush_done = 0;
    // rx_delay 与 rnr_continue 逐批交换成功身份，防止错误 WR 的 RX 抵消另一条成功 WR 的缺失。
    bool has_success_map = false;
    std::array<std::uint64_t, 4> success_map{};
};
std::expected<void, ToolError> WriteBackpressureReport(ControlChannel& channel, const BackpressureReport& report,
                                                       std::uint32_t timeout_ms,
                                                       bool rnr_without_drain = false) noexcept;
std::expected<BackpressureReport, ToolError> ReadBackpressureReport(ControlChannel& channel, std::uint32_t timeout_ms,
                                                                    bool rnr_without_drain = false) noexcept;
struct BackpressureStats {
    BackpressureTraffic traffic;
    BackpressureTraffic cleanup;
    BackpressureTraffic continuation;
    std::uint32_t jetty_id = 0;
    std::uint32_t queried_rnr_retry = 0;
    // 保留 query 的原始值；不同 provider 的编码不能直接解释为微秒。
    std::uint32_t queried_err_timeout = 0;
    std::uint32_t state_checks = 0;
    std::uint64_t quota_rejections = 0;
    std::uint64_t recovery_sends = 0;
    std::uint64_t provider_eagain = 0;
    bool accepted_known = true;
    bool terminal_failure = false;
};

/// 客户端单 owner 发送，服务端接收；TCP 仅同步测试阶段。
/// 入口须常驻完整依赖链，正常完成后显式 Close，未证明 DMA 终结时保留资源。
class BackpressureSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const BackpressureOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close() noexcept;
    const BackpressureStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> RunQuota(ControlChannel& channel);
    std::expected<void, ToolError> RunDelay(ControlChannel& channel);
    std::expected<void, ToolError> RunRnrContinue(ControlChannel& channel);
    std::expected<void, ToolError> CheckContinueState();
    std::expected<void, ToolError> FinishContinueTx();
    std::expected<std::size_t, ToolError> PollContinueTx();
    BackpressureTraffic& Traffic(bool cleanup) noexcept;
    std::expected<void, ToolError> Refill(std::uint32_t count);
    void Prepare(std::uint32_t first, std::uint32_t count, bool cleanup);
    std::expected<void, ToolError> Send(std::uint32_t first, std::uint32_t count, bool cleanup);
    std::expected<void, ToolError> CheckRejected(bool fault, bool cleanup = false);
    std::expected<void, ToolError> FinishTx(bool cleanup, bool allow_errors);
    std::expected<std::size_t, ToolError> PollTx(bool cleanup, bool allow_errors);
    std::expected<void, ToolError> ObserveTx(const raw::TxCompletionEvent& event, bool cleanup, bool allow_errors);
    std::expected<std::size_t, ToolError> PollRx();
    std::expected<void, ToolError> ValidateRx(const raw::RxBufferEvent& event);
    std::expected<void, ToolError> Receive(std::uint64_t count);
    std::expected<void, ToolError> ReportTx(ControlChannel& channel, bool cleanup);
    std::expected<void, ToolError> ReadReport(ControlChannel& channel, bool cleanup, std::uint32_t expected);

    BackpressureOptions options_;
    BackpressureHello local_, peer_;
    BackpressureStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    raw::JettyPool pool_;
    raw::RxBufferPool rx_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    std::array<raw::JettyLane, 2> lanes_{};
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_;
    static constexpr std::size_t kSlots = raw::TxSender::kMaxBatch + 1;
    std::array<urma_sge_t, kSlots> sges_{};
    std::array<raw::AttemptId, kSlots> ids_{};
    std::array<bool, kSlots> tx_seen_{};
    std::array<bool, kSlots> tx_success_{};
    std::array<std::array<bool, kSlots>, 2> rx_seen_{};
    std::array<std::array<std::uint64_t, 4>, 2> peer_success_{};
    std::array<raw::TxSendRequest, kSlots> requests_{};
    std::array<raw::TxCompletionEvent, raw::TxCompletionProcessor::kMaxPollBatch> tx_events_{};
    std::array<urma_cr_t, raw::TxCompletionProcessor::kMaxPollBatch> continue_completions_{};
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> rx_events_{};
    std::uint32_t round_ = 0;
    std::uint32_t cleanup_count_ = 0;
    urma_jetty_t* continue_jetty_ = nullptr;
    bool ready_ = false;
    bool started_ = false;
    bool peer_quiescent_ = true;
    bool unsafe_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_BACKPRESSURE_TEST_BACKPRESSURE_TEST_HPP_
