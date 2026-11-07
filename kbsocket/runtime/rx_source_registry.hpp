// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_RX_SOURCE_REGISTRY_HPP_
#define KBSOCKET_RUNTIME_RX_SOURCE_REGISTRY_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/connection/eager_rx.hpp"

namespace kbsocket {
namespace runtime {

enum class RxSourceError : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kExhausted,
    kSequenceExhausted,
    kDuplicate,
    kIdentityConflict,
    kInvalidLease,
    kProofRequired,
    kInvalidCompletion,
    kMalformedFrame,
    kUnknownSource
};
struct RxSourceRegistryConfig {
    raw::RxBufferPool* buffers = nullptr;
    std::uint64_t context_incarnation = 0;
    connection::TokenDomain local_domain;
    std::uint32_t capacity = 4096;
};
struct RxSourceRoute {
    std::uint32_t local_id = 0;
    bool is_jetty = true;
    // 必须是远端主动发送 Lane 的身份，不能拿 HELLO 中被动接收 Jetty 的 ID 代替。
    urma_jetty_id_t remote_id{};
    connection::WireToken token;
    connection::EagerReceiveSource source;
};
class RxSourceRegistry;
struct RxSourceLease {
    const RxSourceRegistry* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t id = 0;
    bool operator==(const RxSourceLease&) const = default;
};
struct RxSourceRetireProof {
    std::uint64_t context_incarnation = 0;
    bool remote_sender_stopped = false;
    bool accepted_traffic_settled = false;
    bool receive_events_drained = false;
};

/// 每个真实 RX pool 的单 owner 来源表；Open 预分配，Resolve 不分配、不调用 provider。
/// Register 只接纳宿主通过可信控制路径确认的发送 Lane/会话/物理 Credit 归属；本表不认证远端。
/// CQE 的 local_id/type、remote EID/ID 与帧 token 共同选取完整来源；token 本身不证明 Peer 身份。
/// 同一物理发送 Lane 可承载不同逻辑 session，但不能在旧绑定仍存在时更换 PeerRuntime/Credit 账。
/// 逻辑连接撤路由不应 Remove；迟到旧帧仍须找到旧来源并结账，清除须证明远端静默及 RX 事件排空。
/// Resolve 失败时宿主仍须把真实 RX Lease 交给 EagerReceiveDomain 的未知来源路径，不能只丢帧。
class RxSourceRegistry {
public:
    static constexpr std::uint32_t kMaxCapacity = 1u << 20;
    RxSourceRegistry() noexcept;
    ~RxSourceRegistry();
    RxSourceRegistry(const RxSourceRegistry&) = delete;
    RxSourceRegistry& operator=(const RxSourceRegistry&) = delete;
    std::expected<void, RxSourceError> Open(const RxSourceRegistryConfig& config) noexcept;
    std::expected<RxSourceLease, RxSourceError> Register(const RxSourceRoute& route) noexcept;
    /// 控制路径原子准入：任一条失败时表和 output 都不改变；调用方提供等长输出。
    std::expected<void, RxSourceError> RegisterBatch(std::span<const RxSourceRoute> routes,
                                                     std::span<RxSourceLease> output) noexcept;
    std::expected<connection::EagerReceiveSource, RxSourceError>
    Resolve(const raw::RxBufferEvent& event) const noexcept;
    std::expected<void, RxSourceError> Remove(RxSourceLease lease, const RxSourceRetireProof& proof) noexcept;
    std::expected<void, RxSourceError> RemoveBatch(std::span<const RxSourceLease> leases,
                                                   const RxSourceRetireProof& proof) noexcept;
    std::expected<void, RxSourceError> Check(RxSourceLease lease) const noexcept;
    std::expected<void, RxSourceError> Close() noexcept;
    std::size_t size() const noexcept;
    raw::RxBufferPool* buffers() const noexcept;
    std::uint64_t context_incarnation() const noexcept;
    connection::TokenDomain local_domain() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::uint64_t next_id_ = 0;
    bool opened_once_ = false;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_RX_SOURCE_REGISTRY_HPP_
