// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_TX_DRAIN_TEST_TX_DRAIN_TEST_HPP_
#define KBSOCKET_TOOLS_TX_DRAIN_TEST_TX_DRAIN_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace tools {
struct TxDrainOptions {
    std::uint32_t bytes = 4096;
    std::uint32_t batch = 32;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateDrainOptions(const TxDrainOptions& options) noexcept;

struct TxDrainHello {
    urma_jetty_id_t endpoint{};
    TxDrainOptions options;
    bool server = false;
};
using TxDrainHelloBytes = std::array<std::byte, 40>;
TxDrainHelloBytes EncodeDrainHello(const TxDrainHello& hello) noexcept;
std::expected<TxDrainHello, ToolError> DecodeDrainHello(const TxDrainHelloBytes& wire) noexcept;

struct TxDrainStats {
    std::uint32_t accepted = 0;
    bool accepted_known = true;
    std::uint32_t retired = 0;
    std::uint32_t success = 0;
    std::uint32_t flush_error = 0;
    std::uint32_t unhandled = 0;
    std::uint32_t other_error = 0;
    int last_other_error = 0;
    std::uint32_t flush_done = 0;
    std::uint32_t rejected_sends = 0;
};

/// 单 SQ 的故障排空工具。服务端仅提供有效目标，不投递 RQ，避免残留 RX WR 影响 TX 验证。
/// Run 只允许一次；任何可能遗留 DMA 的失败都要求保留本对象及 context 到进程退出。
/// 入口使用 NoDestructor，正常路径显式 Close 后才关闭 runtime 和库。
class TxDrainSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const TxDrainOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close() noexcept;
    const TxDrainStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> SendAndDrain();
    std::expected<void, ToolError> CheckSendRejected();
    std::expected<void, ToolError> Observe(std::span<const raw::TxCompletionEvent> events, bool software);
    static constexpr std::size_t kMaxBatch = raw::TxSender::kMaxBatch;
    TxDrainOptions options_;
    bool server_ = false;
    bool ready_ = false;
    bool ran_ = false;
    bool unsafe_ = false;
    urma_context_t* ctx_ = nullptr;
    urma_jetty_id_t endpoint_{};
    urma_target_seg_t* segment_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    raw::JettyLane lane_;
    raw::JettyPool pool_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    std::vector<std::byte> buffer_;
    std::span<std::byte> registered_buffer_;
    std::array<urma_sge_t, kMaxBatch> sges_{};
    std::array<raw::TxSendRequest, kMaxBatch> requests_{};
    std::array<raw::AttemptId, kMaxBatch> ids_{};
    std::array<bool, kMaxBatch> retired_{};
    TxDrainStats stats_;
};

/// 只有本地 session、runtime 和库都已成功关闭后，才与对端交换此确认。
std::expected<void, ToolError> ConfirmDrainClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept;
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_TX_DRAIN_TEST_TX_DRAIN_TEST_HPP_
