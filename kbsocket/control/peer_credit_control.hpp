// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_PEER_CREDIT_CONTROL_HPP_
#define KBSOCKET_CONTROL_PEER_CREDIT_CONTROL_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>

#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/control/handshake_channel.hpp"

namespace kbsocket {
namespace control {

struct PeerCreditControlConfig {
    HandshakeIdentity local;
    HandshakeIdentity peer;
    std::uint64_t local_credit_epoch = 0, peer_credit_epoch = 0;
    connection::SenderCreditAccount* sender = nullptr;
    connection::ReceiveCreditDomain* receive = nullptr;
    connection::CreditPeerHandle receive_peer;
    std::uint64_t timeout_ns = 5'000'000'000;
    // 宿主已认证双方完整身份，且此专用 fd 约定承载整个 Peer 共账的撤回协议。
    bool peer_authorized = false;
};
enum class PeerCreditControlErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kUnauthorized,
    kChannel,
    kProtocol,
    kCredit,
    kTimeout
};
struct PeerCreditControlError {
    PeerCreditControlErrorCode code;
    int detail = 0;
    int system_error = 0;
};
struct PeerCreditControlStats {
    std::array<std::uint64_t, 2> local_spent{}, peer_spent{};
    bool local_snapshot = false, local_stop_sent = false, peer_stop_received = false;
    bool receive_retired = false, ack_sent = false, peer_acknowledged = false, complete = false;
};
struct PeerCreditControlProgress {
    std::size_t processed = 0;
    bool complete = false;
};

/// 整个 PeerRuntime/接收域授信关系的双向撤回，不是某条逻辑连接的 Close。
/// 成功 Open 接管独立的已认证非阻塞 fd、冻结本端 Peer 授信并停止共账发送；
/// 同 Peer 的所有发送者必须共用 sender，禁止绕过它直接提交消费该授信的 WR。
/// Progress 有界、无分配、不调用 provider。宿主继续 RX 消费/Release/真实 refill，
/// 并关闭受影响的连接以归还 Held；不能提前 Freeze 整个 Eager RX 域。
/// 完成仅表示双方消费计数对齐和旧授信退休，不证明本地 TX DMA、来源或 M1 可销毁。
class PeerCreditControl {
public:
    PeerCreditControl() = default;
    ~PeerCreditControl();
    PeerCreditControl(const PeerCreditControl&) = delete;
    PeerCreditControl& operator=(const PeerCreditControl&) = delete;
    /// 失败不接管 fd，也不停止或冻结账本；成功后不可恢复本代发送/授信。
    std::expected<void, PeerCreditControlError> Open(int fd, const PeerCreditControlConfig& config,
                                                     std::uint64_t now_ns) noexcept;
    std::expected<PeerCreditControlProgress, PeerCreditControlError> Progress(std::uint64_t now_ns) noexcept;
    /// 中止或完成后解除控制器绑定并关闭 fd；不回收未核实的授权、不清空旧责任或恢复发送。
    /// 所有借用账本必须存活至 Close 成功；失败后仍可保留账本并走真实整域关闭。
    std::expected<void, PeerCreditControlError> Close() noexcept;
    int fd() const noexcept {
        return channel_.fd();
    }
    bool output_pending() const noexcept;
    PeerCreditControlStats stats() const noexcept {
        return stats_;
    }
    const std::optional<PeerCreditControlError>& failure() const noexcept {
        return failure_;
    }

private:
    enum class Output : std::uint8_t { kNone, kStop, kAck };
    PeerCreditControlError Fail(PeerCreditControlError error) noexcept;
    std::expected<void, PeerCreditControlError> Reconcile() noexcept;
    std::expected<void, PeerCreditControlError> QueueOutput() noexcept;
    std::expected<void, PeerCreditControlError> Handle(const HandshakeMessage& message) noexcept;
    void ObserveFlushed() noexcept;
    HandshakeChannel channel_;
    PeerCreditControlConfig config_;
    PeerCreditControlStats stats_;
    std::optional<PeerCreditControlError> failure_;
    std::uint64_t started_ns_ = 0, last_now_ns_ = 0;
    Output output_ = Output::kNone;
    bool opened_once_ = false, bound_ = false;
};

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_PEER_CREDIT_CONTROL_HPP_
