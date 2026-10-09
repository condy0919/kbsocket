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

#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace tools {
struct SendTestError {
    const char* operation;
    int code = 0;
};
struct SendTestOptions {
    std::uint32_t bytes = 4096;
    std::uint32_t messages = 1024;
    std::uint32_t batch = 32;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, SendTestError> ValidateOptions(const SendTestOptions& options) noexcept;

/// 有界 TCP 控制通道，仅传端点和同步消息；不承载测试 payload。所有等待都有截止时间。
class ControlChannel {
public:
    explicit ControlChannel(int fd = -1) noexcept : fd_(fd) {}
    ControlChannel(const ControlChannel&) = delete;
    ControlChannel& operator=(const ControlChannel&) = delete;
    ~ControlChannel();
    std::expected<void, SendTestError> Open(bool server, const char* ipv4, std::uint16_t port,
                                            std::uint32_t timeout_ms) noexcept;
    std::expected<void, SendTestError> Write(std::span<const std::byte> data, std::uint32_t timeout_ms) noexcept;
    std::expected<void, SendTestError> Read(std::span<std::byte> data, std::uint32_t timeout_ms) noexcept;
    std::expected<void, SendTestError> SendMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept;
    std::expected<void, SendTestError> ExpectMarker(std::uint32_t value, std::uint32_t timeout_ms) noexcept;

private:
    int fd_;
};

struct SendTestHello {
    urma_jetty_id_t endpoint{};
    SendTestOptions options;
};
using HelloBytes = std::array<std::byte, 40>;
HelloBytes EncodeHello(const SendTestHello& hello) noexcept;
std::expected<SendTestHello, SendTestError> DecodeHello(const HelloBytes& wire) noexcept;
void FillPayload(std::span<std::byte> data, std::uint64_t sequence) noexcept;
std::expected<std::uint64_t, SendTestError> CheckPayload(std::span<const std::byte> data) noexcept;

/// 单设备单 jetty 的硬件测试会话；context 必须覆盖会话生命周期。
/// 若 Run 失败，先调用 Close；Close 拒绝或失败时必须保留整个会话及其内存到进程退出。
/// 工具入口用 NoDestructor 持有会话，避免失败路径上的析构提前回收 DMA buffer。
class SendTestSession {
public:
    std::expected<void, SendTestError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                            const SendTestOptions& options);
    std::expected<void, SendTestError> Run(ControlChannel& channel, bool server);
    std::expected<void, SendTestError> Close() noexcept;

private:
    std::expected<void, SendTestError> ReceiveBatch(std::uint32_t base, std::uint32_t count, ControlChannel& channel);
    std::expected<void, SendTestError> SendBatch(std::uint32_t base, std::uint32_t count, ControlChannel& channel);
    static constexpr std::size_t kBatch = raw::TxSender::kMaxBatch;
    SendTestOptions options_;
    urma_context_t* ctx_ = nullptr;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    raw::JettyPool pool_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    std::vector<std::byte> buffer_;
    std::array<urma_sge_t, kBatch> sges_{};
    std::array<urma_jfr_wr_t, kBatch> recv_wrs_{};
    std::array<raw::TxSendRequest, kBatch> requests_{};
    std::array<raw::AttemptId, kBatch> ids_{};
    std::array<bool, kBatch> seen_slots_{};
    std::array<bool, kBatch> seen_messages_{};
    std::uint32_t pending_rx_ = 0;
    bool unsafe_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_SEND_TEST_SEND_TEST_HPP_
