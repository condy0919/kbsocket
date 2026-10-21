// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_STREAMING_TEST_STREAMING_TEST_HPP_
#define KBSOCKET_TOOLS_STREAMING_TEST_STREAMING_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
inline constexpr std::size_t kStreamMaxContexts = 4;
inline constexpr std::size_t kStreamMaxWindow = 256;

struct StreamOptions {
    std::uint32_t bytes = 4096;
    std::uint32_t messages = 100000;
    std::uint32_t window = 32;
    std::uint32_t timeout_ms = 10000;
    bool events = false;
    // 只扰动 arm/recheck 间隔，不把软件时间解释为硬件完成时间。
    std::uint32_t arm_delay_us = 0;
    bool two_devices_two_eids = false;
};
std::expected<void, ToolError> ValidateStreamOptions(const StreamOptions& options) noexcept;
std::expected<std::vector<raw::DeviceConfig>, ToolError> ParseStreamDevices(std::string_view text);
using StreamDeviceGroups = std::array<std::uint8_t, kStreamMaxContexts>;
/// 按设备首次出现顺序分组；严格模式要求两个设备各配置两个不同 EID。
std::expected<StreamDeviceGroups, ToolError> BuildStreamDeviceGroups(std::span<const raw::DeviceConfig> devices,
                                                                     bool two_devices_two_eids);

struct StreamHello {
    StreamOptions options;
    bool server = false;
    std::uint32_t count = 1;
    StreamDeviceGroups device_groups{};
    std::array<urma_jetty_id_t, kStreamMaxContexts> endpoints{};
};
using StreamHelloBytes = std::array<std::byte, 112>;
StreamHelloBytes EncodeStreamHello(const StreamHello& hello) noexcept;
std::expected<StreamHello, ToolError> DecodeStreamHello(const StreamHelloBytes& wire) noexcept;
std::expected<void, ToolError> ValidateStreamPeer(const StreamHello& local, const StreamHello& peer) noexcept;
std::expected<StreamHello, ToolError> ExchangeStreamHello(ControlChannel& channel, const StreamHello& hello);

struct StreamStats {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t received = 0;
    std::uint64_t rx_posted = 0;
    std::uint64_t rearms = 0;
    std::uint64_t tx_notifications = 0;
    std::uint64_t rx_notifications = 0;
    std::uint64_t event_acks = 0;
    // poll 返回该 fd 的 POLLIN 次数；不表示 poll 一定曾阻塞，也不等于通知条数。
    std::uint64_t fd_wakes = 0;
    std::uint64_t recheck_progress = 0;
};

/// 单 context 的持续 request/echo 流。双方先补投 RX 再交还窗口额度；运行中不交换 TCP 批次屏障。
/// 所有缓冲在 Open 预分配，只有逐消息 TX CQE 和回包均到达后才复用发送槽。
/// 外层必须保留完整对象到显式 Close 成功，未知 DMA 状态不能由析构清理。
class StreamSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap, const StreamOptions& options,
                                        bool server, std::uint32_t channel);
    std::expected<void, ToolError> Connect(const urma_jetty_id_t& peer);
    std::expected<void, ToolError> Start();
    std::expected<std::size_t, ToolError> Advance();
    std::expected<void, ToolError> Arm();
    std::expected<std::size_t, ToolError> CollectNotifications();
    std::expected<void, ToolError> Close();
    void ConfirmPeerQuiescent() noexcept {
        peer_quiescent_ = true;
    }
    bool done() const noexcept;
    urma_jetty_id_t endpoint() const noexcept {
        return jetty_->jetty_id;
    }
    int event_fd() const noexcept {
        return jfce_ ? jfce_->fd : -1;
    }
    const StreamStats& stats() const noexcept {
        return stats_;
    }
    void RecordRecheckProgress() noexcept {
        ++stats_.recheck_progress;
    }
    void RecordFdWake() noexcept {
        ++stats_.fd_wakes;
    }

private:
    std::expected<void, ToolError> PostRx(std::size_t slot);
    std::expected<void, ToolError> PostTx(std::size_t slot, std::uint64_t sequence);
    std::expected<std::size_t, ToolError> PollTx();
    std::expected<std::size_t, ToolError> PollRx();
    std::span<std::byte> TxBuffer(std::size_t slot) noexcept;
    std::span<std::byte> RxBuffer(std::size_t slot) noexcept;

    StreamOptions options_;
    StreamStats stats_;
    urma_context_t* ctx_ = nullptr;
    urma_jfce_t* jfce_ = nullptr;
    urma_jfc_t* tx_cq_ = nullptr;
    urma_jfc_t* rx_cq_ = nullptr;
    urma_jfr_t* jfr_ = nullptr;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    urma_jetty_id_t peer_{};
    std::vector<std::byte> storage_;
    std::span<std::byte> memory_;
    std::vector<std::uint8_t> seen_;
    std::array<bool, kStreamMaxWindow> tx_busy_{};
    std::array<bool, kStreamMaxWindow> rx_posted_{};
    std::array<bool, kStreamMaxWindow> reply_received_{};
    std::array<std::uint64_t, kStreamMaxWindow> sequence_{};
    std::array<int, kStreamMaxWindow> echo_rx_{};
    std::array<urma_sge_t, kStreamMaxWindow> tx_sges_{}, rx_sges_{};
    std::uint32_t channel_ = 0;
    bool server_ = false;
    bool started_ = false;
    bool peer_quiescent_ = true;
    bool unsafe_ = false;
};

struct StreamRunStats {
    // 调用 poll 等待 fd 的次数；是否返回就绪由各 context 的 fd_wakes 单独记录。
    std::uint64_t waits = 0;
    std::uint64_t empty_wakes = 0;
};
/// 空 CQ -> arm 所有 CQ -> 再 poll -> 等待 fd；每个 WaitJfc 返回项按事件数恰好 ack 一次。
std::expected<StreamRunStats, ToolError> RunStreams(std::span<StreamSession*> sessions, const StreamOptions& options);
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_STREAMING_TEST_STREAMING_TEST_HPP_
