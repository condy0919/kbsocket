// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_CONNECTION_HPP_
#define KBSOCKET_CONNECTION_CONNECTION_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/connection/eager_tx.hpp"
#include "kbsocket/control/connection_handshake.hpp"
#include "kbsocket/control/raw_source_admission.hpp"
#include "kbsocket/transport/tcp/native_tcp_backend.hpp"

namespace kbsocket {
namespace connection {

enum class ConnectionState : std::uint8_t {
    kIdle,
    kNegotiating,
    kPreparing,
    kWaitingReady,
    kRawActive,
    kTcpActive,
    kFailed,
    kClosing,
    kClosed
};
enum class ConnectionErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kWouldBlock,
    kClosed,
    kHandshake,
    kTx,
    kRx,
    kCredit,
    kTcp,
    kPeerReset,
    kCancelled,
    kRetained,
    kControlClosed,
    kControlProtocol,
    kSourceAdmission,
    kLaneChanged
};
struct ConnectionError {
    ConnectionErrorCode code;
    int detail = 0;
    int underlying_error = 0;
};
struct ConnectionReadResult {
    std::size_t bytes = 0;
    bool eof = false;
};
struct RawConnectionPreparation {
    control::RawHandshakePreparation handshake;
    EagerReceiveDomain* receive = nullptr;
    raw::TxSender* sender = nullptr;
    memory::SlabAllocator* slab = nullptr;
    SenderCreditAccount* sender_credits = nullptr;
    std::uint64_t connection_id = 0;
    std::uint32_t tx_capacity = 256;
    // 同一 Peer 共账的连接须使用同一个控制配额；至少 4，初始实际授信不得少于该值。
    std::uint32_t control_credit_target = 4;
    // 在 READY 前登记可信远端主动 Lane；对象及绑定覆盖连接和迟到 RX 的生命周期。
    control::RawSourceAdmission* sources = nullptr;
    // 宿主已认证并授权 peer HELLO 中的整份 Lane 公告；远端自报字段不构成认证。
    bool peer_lanes_authorized = false;
};

/// 单 owner 的有界 copy-stream 接口；Write 返回的前缀可立即复用，Read 只交付连续字节。
/// 不拥有 raw 队列、RX 域、注册 Slab、缓存及共账；这些依赖必须存活到 Close 成功。
/// 宿主独占 poll，通过 DispatchRx/OnTxCompletion 分发；实际 RX 补投只走 EagerReceiveDomain。
/// 本层不自动回退、重放、撤销远端授信或删除共享队列；零拷贝 C ABI 留待独立交付。
class Connection {
public:
    Connection() = default;
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    std::expected<void, ConnectionError> Open(int fd, ConnectionTable& table,
                                              const control::ConnectionHandshakeConfig& config,
                                              std::uint64_t now_ns) noexcept;
    /// 普通 TCP 服务不探测私有协议；只接管干净、已连接的非阻塞 TCP fd。
    std::expected<void, ConnectionError> OpenTcp(int fd) noexcept;
    std::expected<void, ConnectionError> PrepareRaw(const RawConnectionPreparation& preparation) noexcept;
    std::expected<void, ConnectionError> PrepareTcp() noexcept;
    /// 单次有界握手/一个 TX 批次；物理域 refill 后调用，发布累计 Credit 与应用消费窗口。
    std::expected<std::size_t, ConnectionError> Progress(std::uint64_t now_ns) noexcept;
    std::expected<std::size_t, ConnectionError> Write(std::span<const std::byte> input) noexcept;
    std::expected<ConnectionReadResult, ConnectionError> Read(std::span<std::byte> output,
                                                              std::uint64_t now_ns) noexcept;
    std::expected<void, ConnectionError> ShutdownWrite() noexcept;
    std::expected<void, ConnectionError> OnTxCompletion(const raw::TxCompletionEvent& event) noexcept;
    /// 宿主收到 raw 控制 fd readiness 后调用；EOF/错误显式失败，不能转成 DMA 终结证明。
    std::expected<void, ConnectionError> OnControlReady() noexcept;
    static std::expected<EagerRxDisposition, EagerRxError> DispatchRx(void* owner, EagerReceiveDomain& domain,
                                                                      const WireFrameView& frame, EagerRxPacket packet,
                                                                      std::uint64_t now_ns) noexcept;
    void Cancel() noexcept;
    /// 撤路由并有界清理未提交/未读数据；有在途 DMA 时返回 Retained，继续分发终结再重试。
    std::expected<void, ConnectionError> Close() noexcept;
    ConnectionState state() const noexcept {
        return state_;
    }
    const std::optional<ConnectionError>& failure() const noexcept {
        return failure_;
    }
    const control::ConnectionHandshake& handshake() const noexcept {
        return handshake_;
    }
    EagerTxStats tx_stats() const noexcept {
        return tx_.stats();
    }
    int fd() const noexcept;
    // Reactor 用于身份分发和 context 暂停约束，不转移底层资源所有权。
    std::uint64_t connection_id() const noexcept {
        return raw_prepared_ ? raw_.connection_id : 0;
    }
    urma_context_t* raw_context() const noexcept {
        return raw_prepared_ ? raw_.handshake.local_context : nullptr;
    }
    std::size_t rx_pending() const noexcept {
        return rx_.pending();
    }

private:
    ConnectionError Fail(ConnectionError error) noexcept;
    std::expected<void, ConnectionError> RefreshCredit() noexcept;
    std::expected<void, ConnectionError> ApplyPeerReady() noexcept;
    std::expected<void, ConnectionError> CheckRawBindings() const noexcept;
    control::ConnectionHandshake handshake_;
    EagerTxStream tx_;
    EagerRxStream rx_;
    tcp::NativeTcpBackend tcp_;
    RawConnectionPreparation raw_;
    SequenceSession tx_session_;
    CreditIdentity tx_identity_;
    std::uint64_t last_data_ = 0, last_control_ = 0, last_frames_ = 0, last_bytes_ = 0;
    std::uint64_t control_target_ = 0;
    bool opened_once_ = false, raw_prepared_ = false, tx_open_ = false, rx_open_ = false;
    ConnectionState state_ = ConnectionState::kIdle;
    std::optional<ConnectionError> failure_;
};

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_CONNECTION_HPP_
