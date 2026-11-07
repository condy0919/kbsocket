// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_RAW_CONTEXT_PROGRESS_HPP_
#define KBSOCKET_RUNTIME_RAW_CONTEXT_PROGRESS_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "kbsocket/runtime/progress_source.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"

namespace kbsocket {
namespace runtime {
struct RawAsyncObservation {
    urma_async_event_t event{};
    bool context_matches = false;
    bool object_matches = false;
    std::optional<raw::JettyLane> lane;
};

/// 回调在 owner 上同步执行，不得阻塞、重入或关闭依赖。RX 回调必须接收并最终归还所有 lease。
struct RawQueueBinding {
    raw::JettyPool* pool = nullptr;
    raw::RxBufferPool* rx = nullptr;
    raw::AttemptLedger* ledger = nullptr;
    void (*on_tx)(void*, const raw::TxCompletionEvent&) noexcept = nullptr;
    void (*on_rx)(void*, const raw::RxBufferEvent&) noexcept = nullptr;
    void* cookie = nullptr;
    void (*on_async)(void*, const RawAsyncObservation&) noexcept = nullptr;
    void (*on_error)(void*, const ProgressError&) noexcept = nullptr;
    // 0 禁用水线重写；非 0 时必须小于 JFR 深度。仅 posted 严格高于水线才交给控制线程 rearm。
    std::uint32_t watermark_threshold = 0;
};

struct RawDomainProgressStats {
    std::uint64_t tx_cqes = 0;
    std::uint64_t tx_retired = 0;
    std::uint64_t rx_cqes = 0;
    std::uint64_t rx_posted = 0;
    std::uint64_t tx_events = 0;
    std::uint64_t rx_events = 0;
    std::uint64_t events = 0;
    std::uint64_t acks = 0;
    std::uint64_t async_events = 0;
    std::uint64_t async_acks = 0;
    std::uint64_t rearm_calls = 0;
    std::uint64_t watermark_rearms = 0;
    std::uint64_t refill_retries = 0;
    bool watermark_pending = false;
    std::optional<ProgressError> last_error;
};

/// 一个 URMA context 的唯一 async 消费者；最多绑定八个独立接收域。Open/Close 只绑定，不拥有 raw 资源。
/// 所有绑定对象与 fd 必须活得更久；控制任务期间包括外部借用者在内必须暂停整个 context 的队列访问。
class RawContextProgress final : public ProgressSource {
public:
    static constexpr std::size_t kMaxDomains = 8;
    static constexpr std::size_t kMaxLanes = 64;
    static constexpr std::size_t kBatch = 64;
    static constexpr std::uint64_t kRetryNs = 1000000;

    RawContextProgress() = default;
    RawContextProgress(const RawContextProgress&) = delete;
    RawContextProgress& operator=(const RawContextProgress&) = delete;
    ~RawContextProgress() override;

    std::expected<void, ProgressError> Open(urma_context_t* context,
                                            std::span<const RawQueueBinding> bindings) noexcept;
    /// 有已提交控制任务时拒绝解绑；撤销尚未提交的请求，不删除 Jetty/JFR/JFC 或释放 lease。
    std::expected<void, ProgressError> Close() noexcept;
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind kind, std::size_t budget,
                                                         std::uint64_t now_ns) noexcept override;
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t budget) noexcept override;
    std::expected<void, ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override {
        return std::span(fds_).first(fd_count_);
    }
    bool WaitFdEnabled(int fd) const noexcept override;
    std::optional<ControlTask> PendingControl() noexcept override;
    void ControlSubmitted(ControlTaskId id) noexcept override;
    void ControlCompleted(const ControlTaskCompletion& completion) noexcept override;

    /// 不从空 CQ 推断旧代次消失；显式证明通过后才允许控制线程替换 Lane。
    std::expected<void, ProgressError> RequestReplacement(raw::JettyLane lane,
                                                          const raw::JettyLaneQuiescence& proof) noexcept;
    const RawDomainProgressStats* domain_stats(std::size_t domain) const noexcept;
    std::uint64_t unknown_notifications() const noexcept {
        return unknown_notifications_;
    }
    std::uint64_t unknown_async_events() const noexcept {
        return unknown_async_events_;
    }

private:
    enum class JobKind : std::uint8_t { kDrain, kWatermark, kReplace };
    struct Domain {
        RawQueueBinding binding{};
        // 生命周期内不变的借用对象，用于 retained 后仍识别并 Ack 已排队的 JFR 事件。
        urma_jfr_t* event_jfr = nullptr;
        std::optional<raw::TxCompletionProcessor> processor;
        RawDomainProgressStats stats{};
        std::uint64_t refill_after = 0;
        std::uint64_t control_after = 0;
        bool start_control_retry = false;
        std::size_t flush_lane = 0;
        std::size_t control_lane = 0;
        std::optional<raw::JettyLane> replacement;
        raw::JettyLaneQuiescence replacement_proof{};
    };
    struct Job {
        JobKind kind = JobKind::kDrain;
        std::size_t domain = 0;
        raw::JettyLane lane{};
        raw::JettyLaneQuiescence proof{};
        std::uint64_t token = 0;
    };

    bool Usable(const Domain& domain) const noexcept;
    void Report(std::size_t domain, ProgressError error, bool retain) noexcept;
    std::expected<ProgressBatch, ProgressError> Async(std::size_t budget) noexcept;
    void DeliverTx(Domain& domain, std::size_t count) noexcept;
    static std::expected<void, ControlTaskError> RunControl(void* argument, std::uint64_t token) noexcept;

    urma_context_t* context_ = nullptr;
    std::array<Domain, kMaxDomains> domains_{};
    std::size_t domain_count_ = 0;
    std::array<int, kMaxDomains + 1> fds_{};
    std::size_t fd_count_ = 0;
    std::array<std::size_t, 8> cursors_{};
    std::size_t notification_cursor_ = 0;
    std::size_t control_cursor_ = 0;
    std::array<raw::TxCompletionEvent, kBatch> tx_events_{};
    std::array<raw::RxBufferEvent, kBatch> rx_events_{};
    std::array<urma_jfc_t*, kBatch> notifications_{};
    std::optional<Job> job_;
    std::optional<ControlTaskId> submitted_;
    std::uint64_t next_token_ = 0;
    std::uint64_t now_ns_ = 0;
    std::uint64_t unknown_notifications_ = 0;
    std::uint64_t unknown_async_events_ = 0;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_RAW_CONTEXT_PROGRESS_HPP_
