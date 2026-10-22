// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_SEND_TEST_SEND_TEST_HPP_
#define KBSOCKET_TOOLS_SEND_TEST_SEND_TEST_HPP_

#include <array>
#include <chrono>
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

namespace kbsocket {
namespace tools {
using SendTestError = ToolError;
inline constexpr std::size_t kMaxSendSges = 8;
inline constexpr std::size_t kSendGuardBytes = 32;
struct SendTestOptions {
    std::uint32_t bytes = 4096;
    std::uint32_t messages = 1024;
    std::uint32_t batch = 32;
    std::uint32_t timeout_ms = 10000;
    std::uint32_t sges = 1;
    std::uint32_t header_bytes = 32;
    bool send_imm = false;
};
std::expected<void, SendTestError> ValidateOptions(const SendTestOptions& options) noexcept;

struct SendTestHello {
    urma_jetty_id_t endpoint{};
    SendTestOptions options;
};
using HelloBytes = std::array<std::byte, 52>;
HelloBytes EncodeHello(const SendTestHello& hello) noexcept;
std::expected<SendTestHello, SendTestError> DecodeHello(const HelloBytes& wire) noexcept;
/// offset 指向整个报文中的位置，允许直接填充多个独立注册段而不拼接复制。
void FillPayload(std::span<std::byte> data, std::uint64_t sequence, std::size_t offset = 0) noexcept;
std::expected<std::uint64_t, SendTestError> CheckPayload(std::span<const std::byte> data) noexcept;
std::uint32_t SendSgeLength(const SendTestOptions& options, std::size_t index) noexcept;
std::uint64_t SendImmediate(std::uint64_t sequence) noexcept;

struct SendTestStats {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t received = 0;
    std::uint64_t guarded_segments = 0;
    bool accepted_known = true;
};

/// 单设备单 jetty 的硬件测试会话；context 必须覆盖会话生命周期。
/// 若 Run 失败，先调用 Close；Close 拒绝或失败时必须保留整个会话及其内存到进程退出。
/// 工具入口用 NoDestructor 持有会话，避免失败路径上的析构提前回收 DMA buffer。
class SendTestSession {
public:
    std::expected<void, SendTestError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                            const SendTestOptions& options, bool server = false);
    std::expected<void, SendTestError> Run(ControlChannel& channel, bool server);
    std::expected<void, SendTestError> Close() noexcept;
    const SendTestStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, SendTestError> ReceiveBatch(std::uint32_t base, std::uint32_t count, ControlChannel& channel);
    std::expected<void, SendTestError> SendBatch(std::uint32_t base, std::uint32_t count, ControlChannel& channel);
    std::expected<void, SendTestError> CheckGuards(std::uint32_t count) noexcept;
    static constexpr std::size_t kBatch = raw::TxSender::kMaxBatch;
    struct SendMemory {
        std::vector<std::byte> buffer;
        std::span<std::byte> registered_buffer;
        urma_target_seg_t* segment = nullptr;
        std::size_t stride = 0;
    };
    SendTestOptions options_;
    SendTestStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    urma_jetty_id_t peer_endpoint_{};
    raw::JettyPool pool_;
    raw::RxBufferPool rx_buffers_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    // 每个 SGE 使用独立注册段；段内每条 WR 的数据前后均保留 guard。
    std::array<SendMemory, kMaxSendSges> memory_;
    std::array<std::array<urma_sge_t, kMaxSendSges>, kBatch> sges_{};
    std::array<raw::TxSendRequest, kBatch> requests_{};
    std::array<raw::AttemptId, kBatch> ids_{};
    std::array<bool, kBatch> seen_messages_{};
    bool server_ = false;
    bool unsafe_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_SEND_TEST_SEND_TEST_HPP_
