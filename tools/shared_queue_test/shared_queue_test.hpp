// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_SHARED_QUEUE_TEST_SHARED_QUEUE_TEST_HPP_
#define KBSOCKET_TOOLS_SHARED_QUEUE_TEST_SHARED_QUEUE_TEST_HPP_

#include <array>
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
inline constexpr std::uint32_t kSharedMaxJetties = 16;
inline constexpr std::size_t kSharedPayloadHeader = 32;

struct SharedQueueOptions {
    std::uint32_t bytes = 4096;
    // 每个本地 jetty 的消息数；每端总发送量为 jetties * messages。
    std::uint32_t messages = 1024;
    std::uint32_t batch = 32;
    std::uint32_t jetties = 2;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateSharedOptions(const SharedQueueOptions& options) noexcept;

struct SharedQueueHello {
    SharedQueueOptions options;
    bool server = false;
    std::array<urma_jetty_id_t, kSharedMaxJetties> endpoints{};
};
using SharedHelloBytes = std::array<std::byte, 32 + 20 * kSharedMaxJetties>;
SharedHelloBytes EncodeSharedHello(const SharedQueueHello& hello) noexcept;
std::expected<SharedQueueHello, ToolError> DecodeSharedHello(const SharedHelloBytes& wire) noexcept;
std::expected<SharedQueueHello, ToolError> ExchangeSharedHello(ControlChannel& channel,
                                                               const SharedQueueHello& local) noexcept;

/// 专用测试 payload 元信息，不定义 kbsocket 的正式 IMM/连接协议。
struct SharedFrame {
    std::uint32_t source = 0;
    std::uint32_t target = 0;
    std::uint64_t sequence = 0;
    bool server = false;
};
std::uint64_t SharedImmediate(const SharedFrame& frame) noexcept;
void FillSharedPayload(std::span<std::byte> data, const SharedFrame& frame) noexcept;
std::expected<SharedFrame, ToolError> CheckSharedPayload(std::span<const std::byte> data) noexcept;
/// 完成双方成功关闭后的最后握手；只有它成功才允许入口打印 PASS。
std::expected<void, ToolError> ConfirmSharedClosed(ControlChannel& channel, std::uint32_t timeout_ms) noexcept;

struct SharedQueueStats {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t received = 0;
    // 各 bit 表示收到过哪一种 IMM 模式（共八种）；独立于逻辑连接/序号编码。
    std::uint32_t imm_patterns = 0;
    bool accepted_known = true;
    // 保留首个异常完成，方便硬件环境中比对 provider 返回与预期路由。
    std::optional<urma_cr_t> failed_tx;
    std::optional<urma_cr_t> failed_rx;
    std::optional<SharedFrame> failed_rx_frame;
    // 固定 stride=kSharedMaxJetties；source 表示发送方本地 jetty 索引，target 表示接收方 jetty 索引。
    std::array<std::uint64_t, kSharedMaxJetties * kSharedMaxJetties> tx_pairs{};
    std::array<std::uint64_t, kSharedMaxJetties * kSharedMaxJetties> rx_pairs{};
};

/// 每端单 context/单 owner，多个 jetty 共用一套 JFR 和 TX/RX JFC，双向 SEND_WITH_IMM。
/// 进程入口以 NoDestructor 持有完整依赖链；失败且 DMA/远端使用尚未确认终结时保留资源。
class SharedQueueSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const SharedQueueOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close() noexcept;
    const SharedQueueStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> RunRound(ControlChannel& channel, std::uint32_t base, std::uint32_t count);
    std::expected<void, ToolError> PostRound(std::uint32_t base, std::uint32_t count);
    std::expected<std::size_t, ToolError> PollTx(std::uint32_t base, std::uint32_t count);
    std::expected<std::size_t, ToolError> PollRx(std::uint32_t base, std::uint32_t count);
    std::expected<void, ToolError> ValidateRx(const raw::RxBufferEvent& event, std::uint32_t base, std::uint32_t count);
    std::size_t Slot(std::uint32_t lane, std::uint32_t offset) const noexcept;

    SharedQueueOptions options_;
    SharedQueueHello local_, peer_;
    SharedQueueStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    std::array<urma_target_jetty_t*, kSharedMaxJetties> remotes_{};
    std::array<raw::JettyLane, kSharedMaxJetties> lanes_{};
    raw::JettyPool pool_;
    raw::RxBufferPool rx_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_;
    std::vector<urma_sge_t> sges_;
    std::vector<raw::AttemptId> ids_;
    std::vector<std::uint8_t> tx_seen_, rx_seen_;
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests_{};
    std::array<raw::TxCompletionEvent, raw::TxCompletionProcessor::kMaxPollBatch> tx_events_{};
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> rx_events_{};
    bool ready_ = false;
    bool started_ = false;
    // 完成相互确认前可能仍有对端访问，不凭本端 CQ 空就销毁本地端点。
    bool peer_quiescent_ = true;
    bool unsafe_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_SHARED_QUEUE_TEST_SHARED_QUEUE_TEST_HPP_
