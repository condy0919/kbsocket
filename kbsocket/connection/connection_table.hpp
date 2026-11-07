// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_CONNECTION_TABLE_HPP_
#define KBSOCKET_CONNECTION_CONNECTION_TABLE_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>

#include "kbsocket/connection/connection_tokens.hpp"
#include "kbsocket/connection/wire_frame.hpp"

namespace kbsocket {
namespace connection {

enum class ConnectionTableError : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kExhausted,
    kInvalidLease,
    kStopped,
    kNotPublished,
    kIdentityMismatch,
    kVersionMismatch,
};

class ConnectionTable;
struct ConnectionRouteLease {
    const ConnectionTable* owner = nullptr;
    WireToken token;
    bool operator==(const ConnectionRouteLease&) const = default;
};

struct ConnectionRoute {
    ConnectionRouteLease lease;
    WireSessionBinding binding;
    void* connection = nullptr;
};

/// 每个本地接收域 incarnation 的唯一连接路由表；单 owner，无并发或重入。
/// Open 一次预分配 token/路由槽，Reserve/Publish/Resolve/Withdraw/Retire 为 O(1)，不分配。
/// Publish 前宿主须完成 endpoint import、初始 Credit 准入和接收状态初始化，之后才可发送 READY。
/// connection 为借用指针，必须活到 Withdraw/Retire 后所有已返回路由的使用者退出。
/// 本表不消费/归还 Credit，不持有 RX Lease，不证明 DMA 或远端已静默；即使拒绝旧帧，宿主仍须
/// 按实际来源结账物理 RQE。Retire 不得替代旧 Peer Credit tombstone 或 Grant 的保留责任。
class ConnectionTable {
public:
    static constexpr std::uint32_t kMaxSlots = ConnectionTokens::kMaxSlots;

    ConnectionTable() = default;
    ConnectionTable(const ConnectionTable&) = delete;
    ConnectionTable& operator=(const ConnectionTable&) = delete;

    std::expected<void, ConnectionTableError> Open(TokenDomain local_domain, std::uint32_t slots,
                                                   std::uint32_t initial_generation = 1) noexcept;
    std::expected<ConnectionRouteLease, ConnectionTableError> Reserve() noexcept;
    /// binding 的 receive_domain 是本地接收域，peer_runtime/session 来自已校验的握手来源。
    /// binding.credit_epoch 是对端接收域授信的代次，用于入站 CREDIT_UPDATE 校验；
    /// 本次 RX 消耗的本地 RQE 必须按本地信用账本的独立 epoch 结账，不能混用两个方向。
    /// 只能发布 Reserved 槽，不能更换已发布身份，撤回后的槽须 Retire 并取得新代次。
    std::expected<void, ConnectionTableError> Publish(ConnectionRouteLease lease, const WireSessionBinding& binding,
                                                      void* connection) noexcept;
    /// decoded 必须来自 DecodeWireFrame；两个 observed 参数必须取自实际接收路径的已验证事实，
    /// 不能拿预期绑定代替来源。返回值只借用连接；下一次 owner 修改表前应完成使用。
    std::expected<ConnectionRoute, ConnectionTableError>
    Resolve(const WireFrameView& decoded, TokenDomain observed_local_domain,
            const WireReceiveContext& observed_peer) const noexcept;
    /// 不可逆撤回路由，保留 token 供尚未完成的握手/清理任务定位；重复撤回幂等。
    std::expected<void, ConnectionTableError> Withdraw(ConnectionRouteLease lease) noexcept;
    /// Reserved/Published/Withdrawn 都可退休；完整 generation 耗尽时永久退出槽位，不回绕。
    std::expected<void, ConnectionTableError> Retire(ConnectionRouteLease lease) noexcept;
    void StopAccepting() noexcept;
    /// 隐含停止 Reserve/Publish；仍有任何保留 token 则 kInUse，已发布路由仍可用于排空。
    /// Close 成功后不可重新 Open，也不能另建表重置同一个接收域 incarnation 的 token。
    std::expected<void, ConnectionTableError> Close() noexcept;

    TokenDomain domain() const noexcept {
        return tokens_.domain();
    }
    std::size_t active() const noexcept {
        return tokens_.active();
    }
    std::size_t published() const noexcept {
        return published_;
    }

private:
    enum class State : std::uint8_t { kReserved, kPublished, kWithdrawn };
    struct Entry {
        WireSessionBinding binding;
        void* connection = nullptr;
        State state = State::kReserved;
    };
    bool Contains(ConnectionRouteLease lease) const noexcept;

    ConnectionTokens tokens_;
    std::unique_ptr<Entry[]> entries_;
    std::size_t published_ = 0;
    bool opened_once_ = false;
    bool stopped_ = false;
};

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_CONNECTION_TABLE_HPP_
