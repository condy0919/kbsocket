// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_CONNECTION_HANDSHAKE_HPP_
#define KBSOCKET_CONTROL_CONNECTION_HANDSHAKE_HPP_

#include <cstdint>
#include <expected>
#include <optional>

#include "kbsocket/connection/connection_table.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/control/handshake_channel.hpp"
#include "kbsocket/control/rndv_capabilities.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"

namespace kbsocket {
namespace control {

enum class ConnectionHandshakeState : std::uint8_t {
    kIdle,
    kNegotiating,
    kPreparing,
    kWaitingReady,
    kRawActive,
    kTcpActive,
    kFailed,
    kClosed
};
enum class ConnectionHandshakeErrorCode : std::uint8_t {
    kInvalidArgument,
    kInUse,
    kNotReady,
    kProtocol,
    kUnauthorized,
    kNoCommonTransport,
    kChannel,
    kImport,
    kCredit,
    kRoute,
    kReorder,
    kPeerRejected,
    kCancelled
};
struct ConnectionHandshakeError {
    ConnectionHandshakeErrorCode code;
    int detail = 0;
    /// 原始系统 errno/provider 状态；不覆盖 detail 中的组件错误分类。
    int underlying_error = 0;
};
struct ConnectionHandshakeConfig {
    HandshakeHello local;
    /// 来自宿主认证/策略校验；HELLO 自报 authorization 不能证明来源可信。
    std::uint64_t expected_peer_authorization = 0;
    /// 非零时固定远端 runtime 实例，零允许策略身份下的新实例。
    std::uint64_t expected_peer_runtime = 0;
    bool compatible_peer = false;
    std::uint64_t deadline_ns = 0;
};
struct RawHandshakePreparation {
    EndpointImportCache* imports = nullptr;
    urma_context_t* local_context = nullptr;
    std::uint64_t local_context_incarnation = 0;
    /// 由宿主确认对端 GetRjetty ABI 与本端一致，并授权导入已校验的描述符。
    bool descriptor_compatible = false;
    connection::ReceiveCreditDomain* credits = nullptr;
    connection::CreditPeerHandle credit_peer;
    connection::ReorderConfig reorder;
    /// 路由借用的完整连接对象；准备完成后即可接收首包，不能等本端 Active 才初始化。
    void* route_owner = nullptr;
    /// 宿主已独立认证对端成功 DONE 契约，并授权按规范字段导入其 Arena；网络 capability 不能替代该许可。
    bool rndv_peer_authorized = false;
};

/// 单 owner 建链协调器。Open 接管已连接协议 fd 并预留 token；不执行 connect 或创建 raw 队列。
/// PrepareRaw 只复制请求、预分配乱序窗口；宿主暂停 context 后用控制执行器驱动缓存 import。
/// import 完成、Peer 授信和接收窗口就绪后先 Publish，再排队 READY；双方 READY 且本地 READY
/// 完整写出才 Active。等待对端 READY 时已发布路由可收首包；失败立即撤回路由，不回收旧授信。
/// 对象、table、cache、credit 域及 route_owner 由同一 owner 管理并活到 Close 成功。
/// 后续每个已接受 WR 必须另持 ImportLease，不能借用本对象的引用覆盖其 DMA 生命周期。
/// 本对象不是完整 Connection API，不消费 RX Credit、不发送业务 WR、不回退/重放旧流。
class ConnectionHandshake {
public:
    ConnectionHandshake() = default;
    ~ConnectionHandshake();
    ConnectionHandshake(const ConnectionHandshake&) = delete;
    ConnectionHandshake& operator=(const ConnectionHandshake&) = delete;

    /// 本方法任何失败都不接管 fd；成功后错误、Cancel、Close 均关闭脏协议 fd。
    std::expected<void, ConnectionHandshakeError> Open(int fd, connection::ConnectionTable& table,
                                                       const ConnectionHandshakeConfig& config,
                                                       std::uint64_t now_ns) noexcept;
    std::expected<ConnectionHandshakeState, ConnectionHandshakeError> Progress(std::uint64_t now_ns,
                                                                               HandshakeIoBudget budget = {}) noexcept;
    std::expected<void, ConnectionHandshakeError> PrepareRaw(const RawHandshakePreparation& preparation) noexcept;
    /// 仅双方协商支持 TCP 的专用协议流；不自动把失败的 UB 握手 fd 当普通 TCP 业务流。
    std::expected<void, ConnectionHandshakeError> PrepareTcp() noexcept;
    void Cancel() noexcept;
    /// 停止/撤路由后仍有 RX Lease 时 kInUse，先 Drain/归还后重试；不撤销 PeerRuntime 授信。
    std::expected<void, ConnectionHandshakeError> Close() noexcept;
    /// Active 后把协议字节恰好读完的 fd 交给后续控制/NativeTCP 后端；之后不再由本对象读写。
    std::expected<int, ConnectionHandshakeError> TakeEstablishedFd() noexcept;

    /// 双方 READY 完成后才返回共同 RNDV 上限；限额不是动态空闲量，也不代表目标 Grant 已授予。
    const RndvLimits* rndv() const noexcept;
    /// 只从当前已确认的 peer 目录与完整 Grant 构造请求，不调用 provider、不取得缓存引用或授权 WRITE。
    /// 调用方仍须核验对应 RTS、经共享暂停域驱动导入，并由 WRITE pool 持有逐 WR 所需引用。
    std::expected<TsegImportRequest, ConnectionHandshakeError>
    PrepareRndvImport(const connection::GrantIdentity& grant) const noexcept;

    ConnectionHandshakeState state() const noexcept {
        return state_;
    }
    HandshakeTransport transport() const noexcept {
        return transport_;
    }
    const HandshakeHello& local() const noexcept {
        return local_;
    }
    const HandshakeHello* peer() const noexcept {
        return peer_hello_ ? &peer_ : nullptr;
    }
    const HandshakeReady* peer_ready() const noexcept {
        return peer_ready_ ? &remote_ready_ : nullptr;
    }
    const HandshakeReady* local_ready() const noexcept {
        return ready_queued_ ? &local_ready_ : nullptr;
    }
    connection::ConnectionRouteLease route() const noexcept {
        return route_;
    }
    connection::ReorderWindow& receive_window() noexcept {
        return receive_;
    }
    ImportLease import_lease() const noexcept {
        return import_;
    }
    int fd() const noexcept {
        return channel_.fd();
    }
    bool output_pending() const noexcept {
        return !channel_.output_flushed();
    }
    const std::optional<ConnectionHandshakeError>& failure() const noexcept {
        return failure_;
    }

private:
    ConnectionHandshakeError Fail(ConnectionHandshakeError error) noexcept;
    std::expected<void, ConnectionHandshakeError> Handle(const HandshakeMessage& message) noexcept;
    std::expected<void, ConnectionHandshakeError> PublishRaw() noexcept;
    std::expected<void, ConnectionHandshakeError> CheckRawDependencies() noexcept;
    bool active() const noexcept;

    HandshakeChannel channel_;
    connection::ConnectionTable* table_ = nullptr;
    connection::ConnectionRouteLease route_;
    HandshakeHello local_, peer_;
    HandshakeReady local_ready_, remote_ready_;
    RndvLimits rndv_;
    connection::ReorderWindow receive_;
    RawHandshakePreparation raw_;
    ImportLease import_;
    ConnectionHandshakeState state_ = ConnectionHandshakeState::kIdle;
    HandshakeTransport transport_ = HandshakeTransport::kRaw;
    std::uint64_t expected_authorization_ = 0, expected_runtime_ = 0;
    bool hello_queued_ = false, peer_hello_ = false, prepared_ = false, published_ = false;
    bool ready_queued_ = false, peer_ready_ = false;
    std::optional<ConnectionHandshakeError> failure_;
};

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_CONNECTION_HANDSHAKE_HPP_
