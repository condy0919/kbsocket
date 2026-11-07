// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TRANSPORT_RAW_RECEIVE_DOMAIN_CLOSER_HPP_
#define KBSOCKET_TRANSPORT_RAW_RECEIVE_DOMAIN_CLOSER_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"

namespace kbsocket {
namespace raw {

enum class ReceiveDomainCloseStage : std::uint8_t {
    kIdle,
    kDraining,
    kDeletingQueues,
    kWaitingForLeases,
    kDeletingCompletionQueues,
    kClosed,
    kRetained,
};

/// 由资源 owner 建立的销毁屏障，不是检测器；空 CQ、断链或超时均不能代替这些证明。
/// peer_stopped 一旦确认就会停止补投，不得撤销；其余条件在进入删除阶段后须持续成立。
/// RX lease 由 held() 单独跟踪。
struct ReceiveDomainQuiescence {
    /// 已撤回准入并确认全部远端停止访问此接收域；不能仅凭控制连接断开置 true。
    bool peer_stopped = false;
    /// JFC 通知和 context async 事件已 Get/Ack，且没有待访问这些队列的事件引用。
    bool events_acked = false;
    /// 外部 post/poll、缓存的队列句柄和软件引用已停用；MR 未被独立 RMA Grant 等继续借用。
    bool queue_users_stopped = false;
    /// 两个 JFC 均正常；CQ 错误/溢出后的隔离回收不属于本关闭流程。
    bool completion_queues_healthy = false;
};

enum class ReceiveDomainCloseErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotStarted,
    kInUse,
    kRxPollFailed,
    kQueueCloseFailed,
    kMemoryCloseFailed,
};

struct ReceiveDomainCloseError {
    ReceiveDomainCloseErrorCode code;
    std::optional<JettyPoolFailure> queue_error;
    std::optional<RxBufferError> rx_error;
};

struct ReceiveDomainCloseProgress {
    ReceiveDomainCloseStage stage;
    /// 本次 Advance 消费并交出的 RX 事件；调用方须检查每个事件并归还其 lease。
    std::size_t rx_events = 0;
    /// 累计由成功 DeleteJfr 终结的残留 Posted，不是成功接收数。
    std::size_t retired_rx = 0;
};

/// 单 owner、单次生命周期的接收域关闭协调器，不拥有 pool、buffers 或 context。
/// Begin 只停止本地 TX 准入；确认远端静默前仍允许 RX Refill，避免关闭协商期间耗尽 RX。
/// 调用方继续通过原 TX 完成层退休所有票据/ERROR 边界；Advance 有界消费 RX。
/// RX 事件非空时该次 Advance 不删除对象，留给调用方处理事件和重新确认屏障。
/// 所有 TX 退休、RX CQ 读空且销毁屏障成立后：
/// DeleteJetty -> DeleteJfr -> 清退 Posted -> 等待 Held -> UnregisterSeg -> DeleteJfc。
/// 删除失败后仅重试删除，不再 post/poll；RX poll 失败则封禁整个域的队列推进并保留全部依赖。
/// kRetained 只允许归还已有 RX lease，不尝试故障 CQ 回收或继续 TX 退休。
/// Advance 的 provider 删除/注销可能阻塞，只能在控制路径调用，不在数据面 Tick 内调用。
/// 未完成时必须保留协调器及整个依赖链；析构不强制释放、不推测 DMA 终结，也不自动重试。
class ReceiveDomainCloser {
public:
    ReceiveDomainCloser() = default;
    ReceiveDomainCloser(const ReceiveDomainCloser&) = delete;
    ReceiveDomainCloser& operator=(const ReceiveDomainCloser&) = delete;
    ~ReceiveDomainCloser();

    std::expected<void, ReceiveDomainCloseError> Begin(JettyPool& pool, RxBufferPool& buffers) noexcept;

    /// Draining 阶段 events 必须非空，最多消费 RxBufferPool::kPollBatch 条完成。
    /// 缺少证明或尚有 TX/Held 时返回对应未完成阶段；provider 失败返回保留原始错误的 expected。
    /// 已完成后幂等，即使原 pool 已重新 Open 也不访问新生命周期。
    std::expected<ReceiveDomainCloseProgress, ReceiveDomainCloseError>
    Advance(const ReceiveDomainQuiescence& quiescence, std::span<RxBufferEvent> events) noexcept;

    ReceiveDomainCloseStage stage() const noexcept {
        return stage_;
    }
    /// 仅整域实际关闭成功才成立；匹配本对象捕获的 RX Open 代次，拒绝另一对象或重新打开的池。
    /// 查询不替代宿主证明远端/队列使用者静默；buffers 对象须持续存活，不能复用其对象地址。
    bool ClosedDomain(const RxBufferPool& buffers, std::uint64_t incarnation) const noexcept;

private:
    ReceiveDomainCloseProgress Progress(std::size_t rx_events = 0) const noexcept;

    JettyPool* pool_ = nullptr;
    RxBufferPool* buffers_ = nullptr;
    std::uint64_t buffer_incarnation_ = 0;
    ReceiveDomainCloseStage stage_ = ReceiveDomainCloseStage::kIdle;
    std::size_t retired_rx_ = 0;
    std::optional<RxBufferError> poll_error_;
};
} // namespace raw
} // namespace kbsocket
#endif // KBSOCKET_TRANSPORT_RAW_RECEIVE_DOMAIN_CLOSER_HPP_
