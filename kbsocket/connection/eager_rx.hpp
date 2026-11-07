// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_EAGER_RX_HPP_
#define KBSOCKET_CONNECTION_EAGER_RX_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/connection/connection_table.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"

namespace kbsocket {
namespace raw {
class ReceiveDomainCloser;
}
namespace connection {

enum class EagerRxErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kInvalidPacket,
    kInvalidSource,
    kMalformedFrame,
    kRouteRejected,
    kUnsupportedFrame,
    kReceiveFailed,
    kCreditFailed,
    kRefillFailed,
    kReorderFailed,
    kStopped,
    kRetained,
    kProofRequired
};
struct EagerRxError {
    EagerRxErrorCode code;
    int provider_error = 0;
    // Refill 失败时仅可信 provider 前缀可非零；该前缀已经记入物理信用账。
    std::size_t accepted = 0;
    bool accepted_known = true;
};
class EagerReceiveDomain;
struct EagerRxPacket {
    const EagerReceiveDomain* domain = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t id = 0;
    bool operator==(const EagerRxPacket&) const = default;
};
struct EagerReceiveSource {
    TokenDomain local_domain;
    WireReceiveContext peer_context;
    CreditPeerHandle peer;
};
struct EagerRxPacketView {
    raw::RxBufferLease lease;
    WireFrameView decoded;
};
enum class EagerRxDisposition : std::uint8_t { kHeld, kRelease };
using EagerRxHandler = std::expected<EagerRxDisposition, EagerRxError> (*)(void* connection, EagerReceiveDomain&,
                                                                           const WireFrameView&, EagerRxPacket,
                                                                           std::uint64_t now_ns) noexcept;

/// 共享 JFR 的单 owner RX 接入；不拥有 poll 权，Dispatch 只接收唯一 poll owner 的真实事件。
/// 来源必须由接收路径独立校验，不能从 IMM/token 或预期路由反推 PeerRuntime/session/epoch。
/// Open 要求整份初始物理容量已实际 post 并记账，此后本对象独占 refill/release/receipt 结账。
/// 一个 ReceiveCreditDomain 只能绑定一个 EagerReceiveDomain，解绑前拒绝直接关闭信用账。
/// callback 成功 kHeld 才转移 packet；kRelease 或失败由本对象归还。callback 不可重入 Dispatch。
class EagerReceiveDomain {
public:
    EagerReceiveDomain() noexcept;
    ~EagerReceiveDomain();
    EagerReceiveDomain(const EagerReceiveDomain&) = delete;
    EagerReceiveDomain& operator=(const EagerReceiveDomain&) = delete;
    std::expected<void, EagerRxError> Open(raw::RxBufferPool& buffers, ReceiveCreditDomain& credits,
                                           ConnectionTable& routes) noexcept;
    std::expected<void, EagerRxError> Dispatch(const raw::RxBufferEvent& event, const EagerReceiveSource& source,
                                               EagerRxHandler handler, std::uint64_t now_ns) noexcept;
    std::expected<EagerRxPacketView, EagerRxError> View(EagerRxPacket packet) const noexcept;
    /// 真实 RX Lease -> physical receipt -> refill 队列，禁止直接把应用消费当作已补投。
    std::expected<void, EagerRxError> Release(EagerRxPacket packet) noexcept;
    std::expected<std::size_t, EagerRxError> Refill(std::size_t limit = raw::RxBufferPool::kMaxBatch) noexcept;
    /// 冻结只停止授信和投递，不伪造终结；现有持有 Lease 仍可 Release。
    void Freeze() noexcept;
    /// 只解除本层借用，不关闭真实队列/信用账；held/released/未知责任未解决时拒绝。
    std::expected<void, EagerRxError> Close() noexcept;
    /// 控制路径整域清退：实际 M1 Closed 且绑定代次一致、全部 Lease/receipt 已归还后，
    /// 原子关闭本层及 ReceiveCreditDomain，不补投、不重新授信。失败保留全部元数据。
    /// 宿主先解绑 Runtime/来源及连接访问；不能以断链、空 CQ 或手工布尔值代替物理关闭。
    std::expected<void, EagerRxError> CloseDestroyed(const raw::ReceiveDomainCloser& closer) noexcept;
    bool frozen() const noexcept;
    std::size_t held() const noexcept;
    std::size_t pending_refill() const noexcept;
    bool Matches(const ReceiveCreditDomain& credits, const ConnectionTable& routes) const noexcept;
    urma_context_t* context() const noexcept;
    raw::RxBufferPool* buffers() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::uint64_t next_packet_ = 0;
};

struct EagerReadResult {
    std::size_t bytes = 0;
    bool eof = false;
    bool would_block = false;
};

/// 借用握手已经初始化的重排窗口。Read 复制数据，短读继续持有真实 Lease 与两份 receipt。
/// 只有完整消费一帧才归还全部驻留责任；FIN 也按序消费后才可见 EOF。
class EagerRxStream {
public:
    EagerRxStream() noexcept;
    ~EagerRxStream();
    EagerRxStream(const EagerRxStream&) = delete;
    EagerRxStream& operator=(const EagerRxStream&) = delete;
    std::expected<void, EagerRxError> Open(EagerReceiveDomain& domain, ReorderWindow& window,
                                           WireSessionBinding binding) noexcept;
    /// 成功接管 packet；失败时仍归调用方/Dispatch 清理。
    std::expected<void, EagerRxError> Accept(EagerRxPacket packet, std::uint64_t now_ns) noexcept;
    /// 已复制前缀后遇错仍返回该前缀并置 failed()；宿主保留本次读取结果，同时终止后续业务推进。
    std::expected<EagerReadResult, EagerRxError> Read(std::span<std::byte> output, std::uint64_t now_ns) noexcept;
    void Fail() noexcept;
    /// 已停止后按预算释放本流持有的真实 Lease；不触碰仍 Posted 的 RX。
    std::expected<std::size_t, EagerRxError> Drain(std::size_t budget = ReorderWindow::kMaxBatch) noexcept;
    std::expected<void, EagerRxError> Close() noexcept;
    bool eof() const noexcept;
    bool failed() const noexcept;
    std::size_t pending() const noexcept;

private:
    struct State;
    std::expected<void, EagerRxError> ReleaseDelivery(const ReorderDelivery& delivery) noexcept;
    std::unique_ptr<State> state_;
};

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_EAGER_RX_HPP_
