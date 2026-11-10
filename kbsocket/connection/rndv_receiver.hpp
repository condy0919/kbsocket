// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_RNDV_RECEIVER_HPP_
#define KBSOCKET_CONNECTION_RNDV_RECEIVER_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/connection/wire_frame.hpp"

namespace kbsocket {
namespace connection {
class RndvReceiver;
class EagerRxStream;
class RndvRxIngress;
struct RndvReceiveHandle {
    const RndvReceiver* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const RndvReceiveHandle&) const = default;
};
struct RndvReceiverConfig {
    GrantBinding binding;
    WireSessionBinding route;
    std::uint32_t capacity = 128;
    // 宿主已认证并确认对端只在全部 WRITE 成功且不再写入该 Grant 后发送 DONE。
    // 不是从网络 payload 自动推导的能力，也不是密码学认证机制。
    bool peer_successful_write_done = false;
};
enum class RndvReceiveErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kNoMemory,
    kWouldBlock,
    kWrongIdentity,
    kInvalidHandle,
    kInvalidState,
    kDuplicate,
    kProtocol,
    kGrant,
    kReorder,
    kRetained
};
struct RndvReceiveError {
    RndvReceiveErrorCode code;
};
struct RndvReceiveStats {
    std::size_t active = 0;
    std::size_t waiting_grant = 0;
    std::size_t waiting_done = 0;
    std::size_t in_reorder = 0;
    bool stopped = false;
};

/// 单连接单 owner 的 RTS/DONE 接收协调器，不发送 GRANT，也不代替物理 RQE 扣账与补投。
/// 独占管理器的事务准入/终结/借用；唯一通知发布者仍通过原 GrantManager 发布 GRANT。
/// Open 原子绑定 GrantManager 与共同窗口；绑定期间禁止另一个协调器接管或直接关闭依赖。
/// observed 必须来自可信 RX 来源登记，不能复制预期 route 冒充真实来源。
/// grants/reorder/Slab 必须覆盖全部责任；宿主通过共同窗口 Poll/Drain，并将 Grant 项交给 ReturnGrant。
/// 可由唯一 EagerRxStream 接管共同窗口交付；外部不得直接 Return/Cancel 本协调器管理的 Grant。
class RndvReceiver {
public:
    static constexpr std::uint32_t kMaxCapacity = GrantManager::kMaxWindow;
    RndvReceiver() noexcept;
    ~RndvReceiver();
    RndvReceiver(const RndvReceiver&) = delete;
    RndvReceiver& operator=(const RndvReceiver&) = delete;
    std::expected<void, RndvReceiveError> Open(const RndvReceiverConfig& config, GrantManager& grants,
                                               ReorderWindow& reorder) noexcept;
    /// 只占共同 SN/总长度与固定本地槽；Grant 不足时保留已接收请求，不撤销活跃流的占位。
    std::expected<RndvReceiveHandle, RndvReceiveError>
    OnRts(const WireFrameView& view, const WireReceiveContext& observed, std::uint64_t now_ns) noexcept;
    /// 在已接收请求上重试目标准入；返回原授权供唯一通知队列 QueueGrant，不能自行重复发布。
    std::expected<GrantReservation, RndvReceiveError> PrepareGrant(RndvReceiveHandle handle) noexcept;
    /// 验证当前已发布 Grant 的完整身份及来源，依据可信成功通知契约终结远端访问，不虚构 WR 计数。
    /// 取消后有效的迟到 DONE 只清理；重复或错误身份不改变当前授权。
    std::expected<void, RndvReceiveError> OnDone(const WireFrameView& view, const WireReceiveContext& observed,
                                                 std::uint64_t now_ns) noexcept;
    /// 重试已证实 DONE 的本地转换/取消清理；有界推进 Grant tombstone 到共同 released_prefix。
    std::expected<std::size_t, RndvReceiveError> Progress(std::uint64_t now_ns) noexcept;
    /// 仅接收共同窗口真实 Poll/Drain 交出的完整 Grant delivery；先归还目标，再释放共同 receipt。
    /// 合法归还开始后停止访问所有 payload 副本；中途失败仅保留清理阶段供重试，不恢复借用。
    /// 成功不包含 Grant tombstone 整理；该窗口由 Progress/PrepareGrant 独立推进。
    std::expected<void, RndvReceiveError> ReturnGrant(const ReorderDelivery& delivery) noexcept;
    /// 失败整个逻辑流；已发布授权隔离，已借用目标仍须真实 Poll/Drain 与 ReturnGrant。
    std::expected<void, RndvReceiveError> Cancel() noexcept;
    /// 仅取消后接受独立可信终结证明，不产生业务交付；身份必须对应保留的同一事务。
    std::expected<void, RndvReceiveError> ConfirmTerminal(RndvReceiveHandle handle,
                                                          const GrantTerminalEvidence& evidence) noexcept;
    /// 入口或流绑定期间拒绝关闭；解绑且无活动责任时仅释放本地状态，不使健康共同流失败。
    /// 其余情况取消并等待真实终结/归还。
    std::expected<void, RndvReceiveError> Close() noexcept;
    RndvReceiveStats stats() const noexcept;
    bool Matches(const ReorderWindow& reorder, WireSessionBinding route) const noexcept;
    bool Matches(const GrantManager& grants, const ReorderWindow& reorder, WireSessionBinding route) const noexcept;

private:
    friend class EagerRxStream;
    friend class RndvRxIngress;
    std::expected<void, RndvReceiveError> BindIngress(const RndvRxIngress& ingress) noexcept;
    std::expected<void, RndvReceiveError> UnbindIngress(const RndvRxIngress& ingress) noexcept;
    std::expected<void, RndvReceiveError> BindStream(const EagerRxStream& stream, const ReorderWindow& reorder,
                                                     WireSessionBinding route) noexcept;
    std::expected<void, RndvReceiveError> UnbindStream(const EagerRxStream& stream) noexcept;
    struct Entry;
    struct State;
    std::expected<Entry*, RndvReceiveError> Find(RndvReceiveHandle handle) const noexcept;
    std::expected<void, RndvReceiveError> AdvanceGrantWindow() noexcept;
    std::expected<void, RndvReceiveError> Complete(Entry& entry, std::uint64_t now_ns) noexcept;
    std::expected<void, RndvReceiveError> Discard(Entry& entry) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_RNDV_RECEIVER_HPP_
