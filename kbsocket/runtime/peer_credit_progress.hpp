// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_PEER_CREDIT_PROGRESS_HPP_
#define KBSOCKET_RUNTIME_PEER_CREDIT_PROGRESS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/control/peer_credit_control.hpp"
#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace runtime {
class PeerCreditProgress;
struct PeerWithdrawal {
    const PeerCreditProgress* owner = nullptr;
    std::uint64_t id = 0;
    bool operator==(const PeerWithdrawal&) const = default;
};
struct PeerCreditProgressConfig {
    std::uint32_t capacity = 4096;
    // 必须指定账本所属 raw context，并作为其组成员注册；不允许作为独立 hooks。
    const void* context = nullptr;
    std::uint64_t sweep_interval_ns = 1'000'000;
};
enum class PeerWithdrawalState : std::uint8_t { kActive, kComplete, kFailed };
struct PeerWithdrawalSnapshot {
    PeerWithdrawalState state = PeerWithdrawalState::kActive;
    control::PeerCreditControlStats protocol;
    std::optional<control::PeerCreditControlError> failure;
    std::optional<ProgressError> scheduling_failure;
};
struct PeerCreditProgressEvent {
    PeerWithdrawal withdrawal;
    PeerWithdrawalSnapshot snapshot;
};
using PeerCreditProgressCallback = void (*)(void*, const PeerCreditProgressEvent&) noexcept;
struct PeerCreditProgressStats {
    std::uint64_t visits = 0, fd_events = 0, stale_events = 0, completed = 0, failures = 0, aborted = 0;
    std::size_t attached = 0;
};

/// 单 owner 的 Peer 共账撤回调度器，固定槽拥有控制器/专用 fd，借用账本。
/// 所有入口与 raw RX/refill 共用 context 暂停屏障；跨线程调用须先交给 owner。
/// 独立 fd 的认证与所有权由宿主保证，不能复用连接握手/LaneControl fd 或其 dup。
/// Begin 成功才停止整个 Peer 的发送并冻结授信；宿主继续归还 RX、真实补投及退休 TX。
/// 完成不代表 DMA、来源或 M1 已关闭；失败只终结该撤回，不恢复或推测回收授信。
/// 终态回调仅报告一次，不得重入；账本须存活至 Release/Abort，关闭源前先从 Reactor 解绑。
class PeerCreditProgress final : public ProgressSource {
public:
    static constexpr std::uint32_t kMaxPeers = 65536;
    PeerCreditProgress() noexcept;
    ~PeerCreditProgress() override;
    std::expected<void, ProgressError> Open(const PeerCreditProgressConfig& config,
                                            PeerCreditProgressCallback callback = nullptr,
                                            void* cookie = nullptr) noexcept;
    /// 失败不接管 fd 或改变账本；控制配置校验失败的 detail 为 PeerCreditControlErrorCode。
    std::expected<PeerWithdrawal, ProgressError> BeginWithdrawal(int fd, const control::PeerCreditControlConfig& config,
                                                                 std::uint64_t now_ns) noexcept;
    std::expected<PeerWithdrawalSnapshot, ProgressError> Inspect(PeerWithdrawal withdrawal) const noexcept;
    // 待决 post/Release/refill 后可主动唤醒；不唤醒也由有限预算的定时巡检推进。
    std::expected<void, ProgressError> Wake(PeerWithdrawal withdrawal) noexcept;
    /// 只释放完成/失败的控制器与 fd。失败残留授权仍属于冻结账本，不生成 M1 证明。
    std::expected<void, ProgressError> Release(PeerWithdrawal withdrawal) noexcept;
    /// 显式中止并释放控制器槽，保留账本原有冻结、停止及未核实责任；旧 handle 失效。
    std::expected<void, ProgressError> Abort(PeerWithdrawal withdrawal) noexcept;
    /// 有任何槽尚未显式 Release/Abort 时拒绝关闭，包括已完成的槽。
    std::expected<void, ProgressError> Close() noexcept;
    const PeerCreditProgressStats* stats() const noexcept;

    // now_ns 为单调时钟下界；Begin/实际协议访问取不早于 steady_clock 和该槽上次访问的时间。
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind, std::size_t, std::uint64_t) noexcept override;
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t) noexcept override;
    std::expected<void, ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override;
    const void* exclusive_context() const noexcept override;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::uint32_t generation_ = 0;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_PEER_CREDIT_PROGRESS_HPP_
