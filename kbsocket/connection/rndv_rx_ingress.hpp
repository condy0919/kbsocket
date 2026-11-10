// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_RNDV_RX_INGRESS_HPP_
#define KBSOCKET_CONNECTION_RNDV_RX_INGRESS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>

#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/connection/rndv_notification_tx.hpp"
#include "kbsocket/connection/rndv_receiver.hpp"
#include "kbsocket/connection/wire_frame.hpp"

namespace kbsocket {
namespace connection {
enum class RndvIngressErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kStopped,
    kInvalidPacket,
    kWrongIdentity,
    kUnsupportedFrame,
    kCapacity,
    kGapTimeout,
    kWouldBlock,
    kReceive,
    kNotification,
    kRelease,
    kRetained
};
struct RndvIngressError {
    RndvIngressErrorCode code;
};
/// 成功表示已复制/接管 GRANT 事务信息；WouldBlock 必须无副作用，不得保存 payload span。
/// 回调负责核对完整 GRANT 与本地未完成 RTS，不能仅凭通知接收来源批准远端写入。
/// 只能推进自身事务状态，不得同步重入 ingress 的 Accept/Progress/Cancel/Close。
using RndvGrantHandler = std::expected<void, RndvIngressError> (*)(void* owner, const WireFrameView& view,
                                                                   const WireReceiveContext& observed,
                                                                   std::uint64_t now_ns) noexcept;
struct RndvIngressConfig {
    WireSessionBinding route;
    std::uint32_t rts_packets = 16;
    std::uint32_t grant_packets = 16;
    std::uint32_t done_packets = 16;
    std::uint32_t grant_jobs = 16;
    // 尚未进入共同窗口的乱序 RTS 也必须有缺口期限；不因重试而重置。
    std::uint64_t gap_timeout_ns = 1000000000;
    RndvGrantHandler on_grant = nullptr;
    void* grant_owner = nullptr;
};
struct RndvIngressStats {
    std::size_t rts_packets = 0;
    std::size_t grant_packets = 0;
    std::size_t done_packets = 0;
    std::size_t grant_jobs = 0;
    bool stopped = false;
};

/// 真实 RX 域与 RNDV 协调器之间的有界暂存及重试层；Open 后快路径不分配内存。
/// Accept 成功后宿主必须返回 EagerRxDisposition::kHeld；失败时仍由 RX 域归还该 packet。
/// RTS/GRANT/DONE 分别保留固定槽，容量须覆盖宿主允许的在途帧数；满槽是逻辑流失败，不能丢弃重试。
/// 接收器/通知队列独占绑定本 ingress；domain、依赖对象及回调 owner 须存活至 Close 成功。
/// 宿主继续单独推进 notifications.Progress 并分发其 TX CQE；本类只释放自己创建的 GRANT 通知。
/// 失败后仍需路由迟到 DONE、推进 RX refill 与应用归还，不将取消或释放 RX 当作远端 DMA 终结。
class RndvRxIngress {
public:
    static constexpr std::uint32_t kMaxCapacity = RndvReceiver::kMaxCapacity;
    static constexpr std::size_t kMaxBudget = 128;
    RndvRxIngress() noexcept;
    ~RndvRxIngress();
    RndvRxIngress(const RndvRxIngress&) = delete;
    RndvRxIngress& operator=(const RndvRxIngress&) = delete;
    std::expected<void, RndvIngressError> Open(const RndvIngressConfig& config, EagerReceiveDomain& domain,
                                               RndvReceiver& receiver, GrantManager& grants, ReorderWindow& reorder,
                                               RndvNotificationTx& notifications) noexcept;
    std::expected<void, RndvIngressError> Accept(EagerRxPacket packet) noexcept;
    /// 单次最多扫描 budget 个 packet/job 槽，游标跨调用轮转；容量压力返回零进度。
    std::expected<std::size_t, RndvIngressError> Progress(std::uint64_t now_ns, std::size_t budget = 64) noexcept;
    std::expected<void, RndvIngressError> Cancel() noexcept;
    /// 取消后等待全部 packet、通知 SEND、接收 Grant 与应用借用真实终结，再解除绑定。
    std::expected<void, RndvIngressError> Close() noexcept;
    RndvIngressStats stats() const noexcept;

private:
    struct Packet;
    struct Job;
    struct State;
    std::expected<bool, RndvIngressError> StepPacket(Packet& packet, std::uint64_t now_ns) noexcept;
    std::expected<bool, RndvIngressError> StepJob(Job& job) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_RNDV_RX_INGRESS_HPP_
