// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_ASYNC_EVENT_TEST_ASYNC_EVENT_TEST_HPP_
#define KBSOCKET_TOOLS_ASYNC_EVENT_TEST_ASYNC_EVENT_TEST_HPP_

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
struct JfrLimitOptions {
    std::uint32_t rx_depth = 128;
    std::uint32_t threshold = 64;
    std::uint32_t bytes = 4096;
    std::uint32_t observe_ms = 100;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateJfrLimitOptions(const JfrLimitOptions& options) noexcept;

enum class JfrLimitPhase : std::uint8_t {
    kBaseline,
    kFirstCrossing,
    kRefillControl,
    kRepeatCrossing,
    kCleanup,
    kCount
};
const char* JfrLimitPhaseName(JfrLimitPhase phase) noexcept;
struct JfrLimitEventStats {
    std::uint64_t get_calls = 0;
    std::uint64_t retrieved = 0;
    std::uint64_t acknowledged = 0;
    std::uint64_t limit_events = 0;
    int last_type = -1;
    int last_get_status = 0;
    bool last_context_matches = false;
    bool last_jfr_matches = false;
};
/// 单 owner 有界采集；只在 fd 可读时 Get，成功取得的原事件恰好 Ack 一次，再报告类型或归属错误。
/// 不解引用事件对象，不把事件确认当作任何 WR 的完成证明。
std::expected<std::size_t, ToolError> PollJfrLimitEvents(urma_context_t* ctx, urma_jfr_t* jfr,
                                                         JfrLimitEventStats& stats) noexcept;

struct JfrLimitHello {
    JfrLimitOptions options;
    bool server = false;
    urma_jetty_id_t endpoint{};
};
using JfrLimitHelloBytes = std::array<std::byte, 48>;
JfrLimitHelloBytes EncodeJfrLimitHello(const JfrLimitHello& hello) noexcept;
std::expected<JfrLimitHello, ToolError> DecodeJfrLimitHello(const JfrLimitHelloBytes& wire) noexcept;
std::expected<void, ToolError> ValidateJfrLimitPeer(const JfrLimitHello& local, const JfrLimitHello& peer) noexcept;

struct JfrLimitStats {
    std::uint64_t posted = 0;
    std::uint64_t received = 0;
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    bool accepted_known = true;
    std::uint32_t queried_threshold = 0;
    std::uint32_t queried_depth = 0;
    bool first_observed = false;
    bool repeat_observed = false;
    bool threshold_disabled = false;
    std::array<JfrLimitEventStats, static_cast<std::size_t>(JfrLimitPhase::kCount)> events{};
};

/// 专用 JFR 水线实验；使用真实共享 JFR、生产 RX 池及发送完成层，不注入队列故障。
/// 入口用 NoDestructor 保留对象；失败时不得析构任何可能仍由 DMA 使用的依赖。
class JfrLimitSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const JfrLimitOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close();
    const JfrLimitStats& stats() const noexcept {
        return stats_;
    }
    urma_jetty_id_t endpoint() const noexcept {
        return jetty_->jetty_id;
    }
    std::uint32_t jfr_id() const noexcept {
        return pool_.jfr()->jfr_id.id;
    }

private:
    std::expected<void, ToolError> ConfigureThreshold(std::uint32_t threshold);
    std::expected<void, ToolError> Refill();
    std::expected<void, ToolError> Observe(JfrLimitPhase phase);
    std::expected<void, ToolError> Transfer(ControlChannel& channel, std::uint32_t sequence, std::uint32_t count,
                                            JfrLimitPhase phase);
    std::expected<void, ToolError> Exchange(ControlChannel& channel);

    JfrLimitOptions options_;
    JfrLimitStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_jetty_t* jetty_ = nullptr;
    raw::JettyLane lane_;
    urma_jetty_id_t peer_{};
    urma_target_jetty_t* remote_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    raw::JettyPool pool_;
    raw::RxBufferPool rx_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    std::vector<std::byte> storage_;
    std::span<std::byte> memory_;
    std::array<urma_sge_t, raw::TxSender::kMaxBatch> sges_{};
    std::array<raw::TxSendRequest, raw::TxSender::kMaxBatch> requests_{};
    std::array<raw::AttemptId, raw::TxSender::kMaxBatch> ids_{};
    std::array<bool, raw::TxSender::kMaxBatch> seen_{};
    bool server_ = false;
    bool opened_ = false;
    bool started_ = false;
    bool unsafe_ = false;
    bool quiescent_ = true;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_ASYNC_EVENT_TEST_ASYNC_EVENT_TEST_HPP_
