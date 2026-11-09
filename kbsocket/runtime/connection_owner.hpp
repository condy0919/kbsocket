// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_CONNECTION_OWNER_HPP_
#define KBSOCKET_RUNTIME_CONNECTION_OWNER_HPP_

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/connection/connection.hpp"
#include "kbsocket/connection/connection_table.hpp"
#include "kbsocket/control/connection_handshake.hpp"
#include "kbsocket/control/raw_source_admission.hpp"
#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/handshake_rx_sources.hpp"
#include "kbsocket/runtime/rx_source_registry.hpp"

namespace kbsocket {
namespace runtime {

class ConnectionOwner;
struct OwnedConnection {
    const ConnectionOwner* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const OwnedConnection&) const = default;
};
enum class ConnectionOwnerErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kExhausted,
    kStopped,
    kInvalidHandle,
    kConnection,
    kProgress,
    kSource,
    kRetained
};
struct ConnectionOwnerError {
    ConnectionOwnerErrorCode code;
    std::optional<connection::ConnectionError> connection = std::nullopt;
    std::optional<ProgressError> progress = std::nullopt;
    std::optional<control::RawSourceAdmissionError> source = std::nullopt;
    // 入口回滚若无法撤回调度注册，交还保留槽供调用方关闭；失败 fd 仍属调用方。
    std::optional<OwnedConnection> retained_handle = std::nullopt;
};
struct ConnectionOwnerConfig {
    std::uint32_t capacity = 256;
};
struct OwnedConnectionStatus {
    connection::ConnectionState state;
    std::optional<connection::ConnectionError> failure;
    ScheduledConnection scheduled;
    connection::EagerTxStats tx;
    std::size_t rx_pending = 0;
    std::size_t retained_sources = 0;
    bool lane_update_pending = false;
    connection::ConnectionReadiness readiness = {};
};
struct ConnectionOwnerStats {
    std::size_t capacity = 0;
    std::size_t occupied = 0;
    std::uint64_t adopted = 0;
    bool accepting = false;
};

/// 单 owner 的有界连接所有者，拥有稳定地址的 Connection 与来源适配器，借用调度器及 raw 依赖。
/// Open 预分配槽；Adopt 成功才接管 fd，失败保留调用方所有权。槽句柄不以 fd 标识连接。
/// Prepare 为控制路径，可初始化连接内存；Read/Write/Inspect 和正常调度不分配。
/// 所有入口遵守所属 context 暂停边界，跨线程命令须由宿主转交，回调不得重入。
/// CloseConnection 不删除来源；RetireSources 的独立证明不能由 EOF/ACK/Close 推导。
/// 析构前必须逐槽 Release 并 Close；否则保留整份内部状态，宿主仍须保留调度器和全部借用依赖。
class ConnectionOwner {
public:
    ConnectionOwner() noexcept;
    ~ConnectionOwner();
    ConnectionOwner(const ConnectionOwner&) = delete;
    ConnectionOwner& operator=(const ConnectionOwner&) = delete;
    std::expected<void, ConnectionOwnerError> Open(ConnectionProgress& progress,
                                                   const ConnectionOwnerConfig& config = {}) noexcept;
    std::expected<OwnedConnection, ConnectionOwnerError> AdoptTcp(int fd) noexcept;
    /// 先预留有界槽/调度身份，再非阻塞直连；返回句柄不代表已连通，状态/错误由原调度回调交付。
    std::expected<OwnedConnection, ConnectionOwnerError>
    ConnectTcp(const sockaddr* address, socklen_t length, std::uint64_t now_ns, std::uint64_t deadline_ns) noexcept;
    std::expected<OwnedConnection, ConnectionOwnerError> AdoptProtocol(int fd, connection::ConnectionTable& routes,
                                                                       const control::ConnectionHandshakeConfig& config,
                                                                       std::uint64_t now_ns) noexcept;
    /// preparation 的 connection_id/sources 必须留空，由本所有者注入真实调度身份与槽内来源。
    /// Peer 信用、Slab、import、接收域仍由宿主共用，不能为每条连接复制授信。
    /// 来源适配器首次 Open 后固定 registry；准备失败可纠正授权重试，但换 registry 须重新建链。
    std::expected<void, ConnectionOwnerError> PrepareRaw(OwnedConnection handle,
                                                         const connection::RawConnectionPreparation& preparation,
                                                         RxSourceRegistry& sources) noexcept;
    std::expected<void, ConnectionOwnerError> PrepareTcp(OwnedConnection handle) noexcept;
    std::expected<std::size_t, ConnectionOwnerError> Write(OwnedConnection handle,
                                                           std::span<const std::byte> input) noexcept;
    std::expected<std::size_t, ConnectionOwnerError> WriteLease(OwnedConnection handle, memory::BufferLease source,
                                                                std::size_t offset, std::size_t bytes) noexcept;
    std::expected<connection::ConnectionReadResult, ConnectionOwnerError>
    Read(OwnedConnection handle, std::span<std::byte> output, std::uint64_t now_ns) noexcept;
    std::expected<void, ConnectionOwnerError> ShutdownWrite(OwnedConnection handle) noexcept;
    std::expected<void, ConnectionOwnerError> WatchTcp(OwnedConnection handle, bool read, bool write) noexcept;
    std::expected<void, ConnectionOwnerError> Cancel(OwnedConnection handle) noexcept;
    std::expected<OwnedConnectionStatus, ConnectionOwnerError> Inspect(OwnedConnection handle) const noexcept;
    /// 只借用已校验的 HELLO；Preparing 前返回 kNotReady，指针仅有效至本槽 Release。
    std::expected<const control::HandshakeHello*, ConnectionOwnerError> Peer(OwnedConnection handle) const noexcept;
    std::expected<void, ConnectionOwnerError> CloseConnection(OwnedConnection handle) noexcept;
    /// 仅允许 Connection 已 Closed；证明不足不移除任何来源。
    std::expected<void, ConnectionOwnerError> RetireSources(OwnedConnection handle,
                                                            const RxSourceRetireProof& proof) noexcept;
    /// 仅在 Close 成功且全部来源退休后 Detach 并复用槽；Retained 保持原 id 以接收迟到 TX。
    std::expected<void, ConnectionOwnerError> Release(OwnedConnection handle) noexcept;
    void StopAccepting() noexcept;
    /// 停止新入口；有任一占用槽时返回 kInUse，不自动销毁连接或停止借用的调度器。
    std::expected<void, ConnectionOwnerError> Close() noexcept;
    ConnectionOwnerStats stats() const noexcept;
    /// 控制路径取得真实调度 context，供业务 mailbox 加入同一暂停组；不授予跨线程访问 owner。
    const void* exclusive_context() const noexcept;
    std::uint32_t scheduler_capacity() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_CONNECTION_OWNER_HPP_
