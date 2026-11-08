// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_RAW_STREAM_RUNTIME_HPP_
#define KBSOCKET_API_RAW_STREAM_RUNTIME_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/api/stream_binding.hpp"
#include "kbsocket/connection/connection.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/control/handshake_channel.hpp"
#include "kbsocket/runtime/raw_stack.hpp"

namespace kbsocket {
namespace api {
struct RawStreamEndpointConfig {
    runtime::RawEndpointConfig endpoint;
    StreamBindingConfig streams;
    // 宿主提供不可复用的接收端点代次和授信代次；授权 ID 不是认证凭证。
    std::uint64_t endpoint_generation = 0;
    std::uint64_t authorization = 0;
    std::uint64_t credit_epoch = 0;
};
struct RawStreamConnectOptions {
    std::uint64_t session = 0;
    std::uint64_t expected_peer_authorization = 0;
    std::uint64_t expected_peer_runtime = 0;
    bool compatible_peer = false;
    std::uint64_t deadline_ns = 0;
};
struct RawStreamPreparation {
    // 宿主按 Peer 管理并跨连接共用，不能逐连接新建或重置同一物理授信账户。
    connection::CreditPeerHandle credit_peer;
    connection::SenderCreditAccount* sender_credits = nullptr;
    connection::ReorderConfig reorder;
    std::uint32_t tx_capacity = 256;
    std::uint32_t control_credit_target = 4;
    // Peer() 返回的 HELLO 必须经过宿主独立认证、描述符 ABI 和完整 Lane 授权。
    bool descriptor_compatible = false;
    bool peer_lanes_authorized = false;
    connection::LaneUpdateAuthorizer authorize_lane_update = nullptr;
    void* lane_authorization_cookie = nullptr;
    std::uint64_t lane_update_timeout_ns = 5'000'000'000;
};

/// 单 owner raw 应用编排：拥有 RawStack、每 context 的 C stream 表及同步事件转发。
/// Open/Adopt/Prepare/Close 属于控制路径；Step 与 C stream I/O 复用既有有界数据路径。
/// 宿主先 Load provider，负责协议 fd 建立/认证、Peer 共账和独立关闭证明；不自动 TCP 回退。
/// 未完整 Close 时析构保留全部状态和回调桥；外部账户/回调 cookie/provider 也必须继续存活。
class RawStreamRuntime {
public:
    RawStreamRuntime() noexcept;
    ~RawStreamRuntime();
    RawStreamRuntime(const RawStreamRuntime&) = delete;
    RawStreamRuntime& operator=(const RawStreamRuntime&) = delete;
    std::expected<void, kbs_stream_error> Open(std::span<const RawStreamEndpointConfig> endpoints,
                                               const runtime::RawStackConfig& config = {}) noexcept;
    kbs_stream_table* table(std::size_t context) noexcept;
    runtime::RawStackStage stage() const noexcept;
    bool paused(std::size_t context) const noexcept;
    std::expected<runtime::RawStackSnapshot, kbs_stream_error> Inspect() const noexcept;
    // 仅供宿主 Peer 授信/撤回/mailbox 等编排；不得独立关闭成员或绕过暂停访问缓存视图。
    std::expected<runtime::RawStackContextResources, kbs_stream_error> HostResources(std::size_t context) noexcept;
    // fd 接管后立即置 -1（包括随后发布失败）；失败且 fd 非负时仍由调用者拥有。
    std::expected<kbs_stream_handle, kbs_stream_error>
    AdoptProtocol(std::size_t context, int* fd, const RawStreamConnectOptions& options, std::uint64_t now_ns) noexcept;
    // 返回副本，允许宿主在 Preparing 时进行认证；不会自动信任 HELLO 的自报字段。
    std::expected<control::HandshakeHello, kbs_stream_error> Peer(kbs_stream_handle stream) noexcept;
    std::expected<void, kbs_stream_error> PrepareRaw(kbs_stream_handle stream,
                                                     const RawStreamPreparation& preparation) noexcept;
    std::expected<void, kbs_stream_error> RetireSources(kbs_stream_handle stream,
                                                        const runtime::RxSourceRetireProof& proof) noexcept;
    // 有待交付应用事件时 wait=true 仍只 Tick，避免应用事件被底层等待饿死。
    std::expected<runtime::ProgressBatch, kbs_stream_error> Step(bool wait = false) noexcept;
    std::expected<void, kbs_stream_error> RequestReplacement(std::size_t context, raw::JettyLane lane,
                                                             const raw::JettyLaneQuiescence& proof) noexcept;
    std::expected<void, kbs_stream_error> BeginClose() noexcept;
    // 先逐流 Close/RetireSources/Release，再给出真实的 context 域证明；BUSY 时继续推进。
    std::expected<runtime::RawStackSnapshot, kbs_stream_error> Close(const runtime::RawStackCloseProof& proof) noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
};
} // namespace api
} // namespace kbsocket
#endif // KBSOCKET_API_RAW_STREAM_RUNTIME_HPP_
