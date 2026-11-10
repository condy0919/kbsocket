// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_HANDSHAKE_CHANNEL_HPP_
#define KBSOCKET_CONTROL_HANDSHAKE_CHANNEL_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/control/rndv_capabilities.hpp"

namespace kbsocket {
namespace control {

inline constexpr std::size_t kHandshakeMaxDescriptorBytes = 4096;
inline constexpr std::size_t kHandshakeMaxTxLanes = 64;
inline constexpr std::size_t kHandshakeHeaderBytes = 12;
inline constexpr std::size_t kHandshakeMaxFrameBytes =
    kHandshakeHeaderBytes + 104 + 32 * kHandshakeMaxTxLanes + kHandshakeMaxDescriptorBytes + kRndvMaxOfferBytes;
inline constexpr std::uint16_t kHandshakeVersion = 3;
inline constexpr std::uint16_t kHandshakeCapabilityRaw = 1;
inline constexpr std::uint16_t kHandshakeCapabilityTcp = 2;
inline constexpr std::uint16_t kHandshakeCapabilityWriteRndv = 4;

enum class HandshakeMessageKind : std::uint8_t {
    kHello = 1,
    kReady = 2,
    kReject = 3,
    kLaneUpdate = 4,
    kLaneAck = 5,
    kCreditStop = 6,
    kCreditStopAck = 7
};
enum class HandshakeTransport : std::uint32_t { kRaw = 1, kTcp = 2 };

/// session 为本端 HELLO 的不可复用 nonce；双方 READY 必须完整回显两个 identity。
/// authorization 只是宿主已验证的策略身份，不是认证凭证；此通道不做身份认证。
struct HandshakeIdentity {
    std::uint64_t runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t endpoint_generation = 0;
    std::uint64_t authorization = 0;
    std::uint64_t session = 0;
    connection::WireToken token;
    bool operator==(const HandshakeIdentity&) const = default;
};
/// 主动 TX Lane 的物理来源和软件代次；独立于供对端导入的被动接收 Jetty。
struct HandshakeTxLane {
    std::array<std::byte, 16> eid{};
    std::uint32_t uasid = 0;
    std::uint32_t jetty_id = 0;
    std::uint64_t generation = 0;
    bool operator==(const HandshakeTxLane&) const = default;
};
/// 握手绑定同一池代次的完整有界快照；仅前 count 项参与编码。
struct HandshakeTxLanes {
    std::uint64_t pool_epoch = 0;
    std::uint32_t count = 0;
    std::array<HandshakeTxLane, kHandshakeMaxTxLanes> lanes{};
    bool operator==(const HandshakeTxLanes&) const = default;
};
struct HandshakeHello {
    std::uint16_t wire_version = connection::kWireVersion;
    std::uint16_t capabilities = kHandshakeCapabilityRaw;
    HandshakeIdentity identity;
    /// 本端接收域授信代次；RAW 必须在 READY 前确定，TCP-only 为零。
    std::uint64_t credit_epoch = 0;
    std::array<std::byte, 16> eid{};
    std::uint32_t jetty_id = 0;
    std::uint32_t import_token = 0;
    std::uint32_t descriptor_bytes = 0;
    HandshakeTxLanes tx_lanes;
    /// 只搬运 provider 导出的有界 opaque 描述符；宿主必须另行验证 ABI、来源与端点字段。
    std::array<std::byte, kHandshakeMaxDescriptorBytes> descriptor{};
    /// 仅 WRITE RNDV capability 声明时编码；未启用时必须全零。
    RndvOffer rndv{};
};
struct HandshakeReady {
    HandshakeIdentity sender;
    HandshakeIdentity receiver;
    HandshakeTransport transport = HandshakeTransport::kRaw;
    std::uint64_t credit_epoch = 0;
    std::uint64_t max_credit_seq = 0;
    std::uint64_t max_control_credit_seq = 0;
    std::uint64_t max_frame_seq = 0;
    std::uint64_t max_stream_bytes = 0;
    /// RAW 双方声明时回显确定性协商结果；全零时保留原 v3 READY 字节。
    RndvLimits rndv{};
};
struct HandshakeReject {
    std::uint64_t sender_session = 0;
    std::uint64_t receiver_session = 0;
    std::uint32_t reason = 0;
};
/// 在线公告绑定既有双向会话；ACK 只确认该公告已安装，不证明旧 RX 已经排空。
struct HandshakeLaneUpdate {
    HandshakeIdentity sender;
    HandshakeIdentity receiver;
    std::uint64_t update_id = 0;
    HandshakeTxLanes tx_lanes;
};
struct HandshakeLaneAck {
    HandshakeIdentity sender;
    HandshakeIdentity receiver;
    std::uint64_t update_id = 0;
};
/// 独立 Peer 授信撤回通道的停止快照；spent 顺序为 DATA、CONTROL，零流量也允许协商。
/// ACK 交换双方 identity/epoch 并回显原 STOP 的 spent；它不属于初始握手或 LaneControl。
struct HandshakeCreditStop {
    HandshakeIdentity sender;
    HandshakeIdentity receiver;
    std::uint64_t sender_credit_epoch = 0;
    std::uint64_t receiver_credit_epoch = 0;
    std::array<std::uint64_t, 2> spent{};
    bool operator==(const HandshakeCreditStop&) const = default;
};
struct HandshakeMessage {
    HandshakeMessageKind kind = HandshakeMessageKind::kHello;
    HandshakeHello hello;
    HandshakeReady ready;
    HandshakeReject reject;
    HandshakeLaneUpdate lane_update;
    HandshakeLaneAck lane_ack;
    HandshakeCreditStop credit_stop;
    HandshakeCreditStop credit_stop_ack;
};
enum class HandshakeErrorCode : std::uint8_t {
    kInvalidArgument,
    kInvalidFrame,
    kVersionMismatch,
    kBufferTooSmall,
    kNotReady,
    kInUse,
    kNotConnected,
    kNotNonblocking,
    kUnauthorized,
    kSystemError,
    kPeerClosed,
    kTimeout,
    kCancelled,
};
struct HandshakeError {
    HandshakeErrorCode code;
    int system_error = 0;
};

/// 验证 RAW 主动 Lane 快照的数量、代次和物理来源唯一性；无分配。
std::expected<void, HandshakeError> ValidateHandshakeTxLanes(const HandshakeTxLanes& lanes) noexcept;
/// 只比较 wire 有效前缀，未使用数组槽不参与公告身份；超界数量一律不相等。
bool SameTxLanes(const HandshakeTxLanes& left, const HandshakeTxLanes& right) noexcept;

/// 固定大端字段；失败不写 output。Decode 只接受恰好一帧，不读取 C++ 结构体布局。
std::expected<std::size_t, HandshakeError> EncodeHandshakeMessage(const HandshakeMessage& message,
                                                                  std::span<std::byte> output) noexcept;
std::expected<HandshakeMessage, HandshakeError> DecodeHandshakeMessage(std::span<const std::byte> frame) noexcept;

struct HandshakeIoBudget {
    std::size_t bytes = 16384;
    std::uint32_t syscalls = 8;
    /// 对端 READY 已消费时仅排空本端输出，不能把首个业务字节解释成下一帧握手。
    bool receive = true;
};
struct HandshakeProgress {
    std::size_t sent = 0;
    std::size_t received = 0;
    std::uint32_t syscalls = 0;
    bool message_ready = false;
    bool output_pending = false;
};

/// 单 owner、每次 Progress 有界，固定数组、无分配、无 wait/poll；不负责 connect 或握手状态机。
/// 仅对已明确支持本协议的专用/约定入口调用 Open，不能向普通 TCP 服务试探发送 HELLO。
/// 成功 Open 才接管 fd，失败调用方仍持有；对象只允许成功 Open 一次。Progress 时钟必须单调。
/// 收到完整一帧便停止读，不吞掉合并到 TCP 中的下一帧或业务字节；错误/取消关闭已接管 fd。
class HandshakeChannel {
public:
    HandshakeChannel() = default;
    ~HandshakeChannel();
    HandshakeChannel(const HandshakeChannel&) = delete;
    HandshakeChannel& operator=(const HandshakeChannel&) = delete;

    std::expected<void, HandshakeError> Open(int fd, bool compatible_peer, std::uint64_t now_ns,
                                             std::uint64_t deadline_ns) noexcept;
    std::expected<void, HandshakeError> Queue(const HandshakeMessage& message) noexcept;
    std::expected<HandshakeProgress, HandshakeError> Progress(std::uint64_t now_ns,
                                                              HandshakeIoBudget budget = {}) noexcept;
    std::expected<HandshakeMessage, HandshakeError> TakeMessage() noexcept;
    /// 只能由上层完整双方 READY 状态机确认 established 后调用；不允许在脏流上失败回退。
    /// 要求无未发送握手、无未取走消息及半帧；成功后 fd 交给后端，析构不再关闭它。
    std::expected<int, HandshakeError> ReleaseEstablished(bool established) noexcept;
    void Cancel() noexcept;
    std::expected<void, HandshakeError> Close() noexcept;
    int fd() const noexcept {
        return fd_;
    }
    bool output_flushed() const noexcept {
        return tx_offset_ == tx_size_;
    }
    bool message_ready() const noexcept {
        return message_ready_;
    }
    bool input_pending() const noexcept {
        return rx_size_ != 0;
    }
    const std::optional<HandshakeError>& failure() const noexcept {
        return failure_;
    }

private:
    HandshakeError Fail(HandshakeError error) noexcept;
    std::expected<void, HandshakeError> CheckOpen() const noexcept;
    int fd_ = -1;
    bool opened_once_ = false;
    bool message_ready_ = false;
    std::uint64_t last_now_ns_ = 0, deadline_ns_ = 0;
    std::optional<HandshakeError> failure_;
    std::array<std::byte, kHandshakeMaxFrameBytes> tx_{}, rx_{};
    std::size_t tx_size_ = 0, tx_offset_ = 0, rx_size_ = 0, rx_target_ = kHandshakeHeaderBytes;
};

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_HANDSHAKE_CHANNEL_HPP_
