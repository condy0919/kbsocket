// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/raw_context_progress.hpp"

#include <poll.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <mutex>

namespace kbsocket {
namespace runtime {
namespace {
// 仅 Open/Close 使用控制面锁；避免两个进度源同时对同一个阻塞 async fd 执行 Get。
struct AsyncOwner {
    urma_context_t* context = nullptr;
    RawContextProgress* owner = nullptr;
};
std::mutex owner_mutex;
std::array<AsyncOwner, 64> owners;

std::uint64_t RetryAt(std::uint64_t now) noexcept {
    return now > std::numeric_limits<std::uint64_t>::max() - RawContextProgress::kRetryNs
               ? std::numeric_limits<std::uint64_t>::max()
               : now + RawContextProgress::kRetryNs;
}
ProgressError Error(ProgressErrorCode code, int detail = 0, std::size_t domain = 0) noexcept {
    return {.code = code, .detail = detail, .source = domain};
}
} // namespace

RawContextProgress::~RawContextProgress() {
    // 仅解绑；在途控制任务仍引用 this，调用方必须保留整个源及 raw 依赖到收割完成。
    (void)Close();
}

std::expected<void, ProgressError> RawContextProgress::Open(urma_context_t* context,
                                                            std::span<const RawQueueBinding> bindings) noexcept {
    if (context_) {
        return std::unexpected(Error(ProgressErrorCode::kInUse));
    }
    if (!context || bindings.empty() || bindings.size() > kMaxDomains) {
        return std::unexpected(Error(ProgressErrorCode::kInvalidArgument));
    }
    for (std::size_t i = 0; i < bindings.size(); ++i) {
        const auto& b = bindings[i];
        if (!b.pool || !b.rx || !b.ledger || !b.on_tx || !b.on_rx || !b.pool->ready() || b.pool->retained() ||
            b.pool->context() != context || b.rx->pool() != b.pool || b.ledger->pool() != b.pool ||
            b.watermark_threshold >= b.pool->rx_depth() || b.pool->capacity() > kMaxLanes) {
            return std::unexpected(Error(ProgressErrorCode::kInvalidArgument, 0, i));
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (bindings[j].pool == b.pool) {
                return std::unexpected(Error(ProgressErrorCode::kInvalidArgument, 0, i));
            }
        }
    }
    {
        const std::lock_guard lock(owner_mutex);
        AsyncOwner* free = nullptr;
        for (auto& entry : owners) {
            if (entry.context == context) {
                return std::unexpected(Error(ProgressErrorCode::kInUse));
            }
            if (!entry.context && !free) {
                free = &entry;
            }
        }
        if (!free) {
            return std::unexpected(Error(ProgressErrorCode::kResourceExhausted));
        }
        *free = {context, this};
    }
    context_ = context;
    domain_count_ = bindings.size();
    fd_count_ = 0;
    if (context->async_fd >= 0) {
        fds_[fd_count_++] = context->async_fd;
    }
    for (std::size_t i = 0; i < domain_count_; ++i) {
        auto& domain = domains_[i];
        domain.binding = bindings[i];
        domain.event_jfr = bindings[i].pool->jfr();
        domain.processor.emplace(*bindings[i].ledger, true);
        domain.stats.watermark_pending = bindings[i].watermark_threshold != 0;
        const auto* jfce = bindings[i].pool->jfce();
        if (jfce && jfce->fd >= 0 &&
            std::find(fds_.begin(), fds_.begin() + fd_count_, jfce->fd) == fds_.begin() + fd_count_) {
            fds_[fd_count_++] = jfce->fd;
        }
    }
    return {};
}

std::expected<void, ProgressError> RawContextProgress::Close() noexcept {
    if (submitted_) {
        return std::unexpected(Error(ProgressErrorCode::kInUse));
    }
    if (context_) {
        const std::lock_guard lock(owner_mutex);
        for (auto& entry : owners) {
            if (entry.owner == this) {
                entry = {};
            }
        }
    }
    for (auto& domain : domains_) {
        domain.processor.reset();
        domain.binding = {};
        domain.event_jfr = nullptr;
        domain.stats = {};
        domain.refill_after = domain.control_after = 0;
        domain.start_control_retry = false;
        domain.flush_lane = 0;
        domain.control_lane = 0;
        domain.replacement.reset();
        domain.replacement_proof = {};
    }
    context_ = nullptr;
    domain_count_ = fd_count_ = 0;
    cursors_ = {};
    notification_cursor_ = control_cursor_ = 0;
    job_.reset();
    now_ns_ = unknown_notifications_ = unknown_async_events_ = 0;
    return {};
}

bool RawContextProgress::Usable(const Domain& domain) const noexcept {
    return domain.binding.pool->ready() && !domain.binding.pool->retained();
}

const RawDomainProgressStats* RawContextProgress::domain_stats(std::size_t index) const noexcept {
    return index < domain_count_ ? &domains_[index].stats : nullptr;
}

bool RawContextProgress::WaitFdEnabled(int fd) const noexcept {
    if (!context_ || submitted_) {
        return false;
    }
    if (fd >= 0 && fd == context_->async_fd) {
        return true;
    }
    for (std::size_t i = 0; i < domain_count_; ++i) {
        const auto& domain = domains_[i];
        const auto* jfce = domain.binding.pool->jfce();
        if (jfce && jfce->fd == fd && Usable(domain)) {
            return true;
        }
    }
    return false;
}

void RawContextProgress::Report(std::size_t index, ProgressError error, bool retain) noexcept {
    auto& domain = domains_[index];
    error.source = index;
    domain.stats.last_error = error;
    if (retain) {
        domain.binding.pool->RetainDependencies();
        domain.binding.rx->Stop();
    }
    if (domain.binding.on_error) {
        domain.binding.on_error(domain.binding.cookie, error);
    }
}

void RawContextProgress::DeliverTx(Domain& domain, std::size_t count) noexcept {
    domain.stats.tx_cqes += count;
    for (std::size_t i = 0; i < count; ++i) {
        domain.stats.tx_retired += tx_events_[i].record.has_value();
        domain.binding.on_tx(domain.binding.cookie, tx_events_[i]);
    }
}

std::expected<ProgressBatch, ProgressError> RawContextProgress::Progress(ProgressKind kind, std::size_t budget,
                                                                         std::uint64_t now_ns) noexcept {
    if (!context_) {
        return std::unexpected(Error(ProgressErrorCode::kNotReady));
    }
    if (submitted_) {
        return std::unexpected(Error(ProgressErrorCode::kInUse));
    }
    if (!budget || static_cast<std::size_t>(kind) >= cursors_.size()) {
        return std::unexpected(Error(ProgressErrorCode::kInvalidArgument));
    }
    now_ns_ = now_ns;
    // 控制调用可能耗时很长，不能用提交前的旧时刻计算重试截止；由 owner 下一次 Tick 起算。
    for (std::size_t i = 0; i < domain_count_; ++i) {
        if (domains_[i].start_control_retry) {
            domains_[i].control_after = RetryAt(now_ns);
            domains_[i].start_control_retry = false;
        }
    }
    if (kind == ProgressKind::kAsync) {
        return Async(budget);
    }
    ProgressBatch result;
    auto& cursor = cursors_[static_cast<std::size_t>(kind)];
    for (std::size_t visited = 0; visited < domain_count_ && result.processed < budget; ++visited) {
        const auto index = cursor;
        cursor = (cursor + 1) % domain_count_;
        auto& domain = domains_[index];
        if (!Usable(domain)) {
            continue;
        }
        const auto remaining = std::min(kBatch, budget - result.processed);
        if (kind == ProgressKind::kTx) {
            const auto batch = domain.processor->Poll(std::span(tx_events_).first(remaining));
            if (!batch) {
                Report(index,
                       Error(ProgressErrorCode::kProvider,
                             batch.error().pool_error ? batch.error().pool_error->provider_error : EIO),
                       true);
                continue;
            }
            DeliverTx(domain, batch->count);
            result.processed += batch->count;
            result.runnable |= batch->count == remaining;
        } else if (kind == ProgressKind::kRx) {
            const auto count = domain.binding.rx->Poll(std::span(rx_events_).first(remaining));
            if (!count) {
                Report(index, Error(ProgressErrorCode::kProvider, count.error().provider_error), true);
                continue;
            }
            domain.stats.rx_cqes += *count;
            result.processed += *count;
            result.runnable |= *count == remaining;
            for (std::size_t i = 0; i < *count; ++i) {
                domain.binding.on_rx(domain.binding.cookie, rx_events_[i]);
            }
        } else if (kind == ProgressKind::kRefill) {
            if (domain.binding.external_refill) {
                continue;
            }
            auto& rx = *domain.binding.rx;
            if (rx.stopped() || now_ns < domain.refill_after || !rx.available() ||
                rx.posted() >= domain.binding.pool->rx_depth()) {
                continue;
            }
            const auto count = rx.Refill(remaining);
            if (!count) {
                const auto& error = count.error();
                const auto accepted = error.accepted.value_or(0);
                domain.stats.rx_posted += accepted;
                result.processed += accepted;
                if (error.code == raw::RxBufferErrorCode::kPostFailed && error.provider_error == URMA_EAGAIN &&
                    error.accepted) {
                    domain.refill_after = RetryAt(now_ns);
                    ++domain.stats.refill_retries;
                    Report(index, Error(ProgressErrorCode::kProvider, error.provider_error), false);
                } else {
                    Report(index, Error(ProgressErrorCode::kProvider, error.provider_error), true);
                }
                continue;
            }
            domain.refill_after = 0;
            domain.stats.rx_posted += *count;
            result.processed += *count;
            result.runnable |= *count && rx.available() && rx.posted() < domain.binding.pool->rx_depth();
        } else if (kind == ProgressKind::kFlush) {
            const auto capacity = domain.binding.pool->capacity();
            for (std::size_t scanned = 0; scanned < std::min(capacity, budget); ++scanned) {
                const auto lane_index = domain.flush_lane;
                domain.flush_lane = (domain.flush_lane + 1) % capacity;
                const auto lane = domain.binding.pool->lane(static_cast<std::uint32_t>(lane_index));
                if (!lane) {
                    continue;
                }
                const auto state = domain.binding.pool->lane_state(*lane);
                if (!state || *state != raw::JettyLaneState::kFlushReady) {
                    continue;
                }
                const auto batch = domain.processor->Flush(*lane, std::span(tx_events_).first(remaining));
                if (!batch) {
                    Report(index,
                           Error(ProgressErrorCode::kProvider,
                                 batch.error().pool_error ? batch.error().pool_error->provider_error : EIO),
                           true);
                } else {
                    DeliverTx(domain, batch->count);
                    result.processed += batch->count;
                    result.runnable |= batch->count != 0;
                }
                break;
            }
        }
    }
    return result;
}

std::expected<ProgressBatch, ProgressError> RawContextProgress::CollectNotifications(std::size_t budget) noexcept {
    if (!context_) {
        return std::unexpected(Error(ProgressErrorCode::kNotReady));
    }
    if (submitted_) {
        return std::unexpected(Error(ProgressErrorCode::kInUse));
    }
    if (!budget) {
        return std::unexpected(Error(ProgressErrorCode::kInvalidArgument));
    }
    ProgressBatch result;
    for (std::size_t visited = 0; visited < domain_count_ && result.processed < budget; ++visited) {
        const auto index = notification_cursor_;
        notification_cursor_ = (notification_cursor_ + 1) % domain_count_;
        auto& domain = domains_[index];
        auto* jfce = domain.binding.pool->jfce();
        if (!Usable(domain) || !jfce || jfce->fd < 0) {
            continue;
        }
        pollfd fd{.fd = jfce->fd, .events = POLLIN, .revents = 0};
        const auto ready = ::poll(&fd, 1, 0);
        if (ready < 0 && errno == EINTR) {
            continue;
        }
        if (ready < 0 || (fd.revents & (POLLERR | POLLHUP | POLLNVAL))) {
            Report(index, Error(ProgressErrorCode::kSystem, ready < 0 ? errno : EIO), true);
            continue;
        }
        const auto remaining = std::min(kBatch, budget - result.processed);
        const auto count = raw::UrmaApi::WaitJfc(jfce, static_cast<std::uint32_t>(remaining), 0, notifications_.data());
        if (count < 0 || static_cast<std::size_t>(count) > remaining) {
            Report(index, Error(ProgressErrorCode::kProvider, count), true);
            continue;
        }
        result.processed += count;
        result.runnable |= static_cast<std::size_t>(count) == remaining;
        for (int i = 0; i < count; ++i) {
            auto* jfc = notifications_[i];
            if (!jfc) {
                Report(index, Error(ProgressErrorCode::kProvider, EPROTO), true);
                continue;
            }
            // Wait 的每一项都独立 Ack，重复项不能去重；未知归属仍按 provider 返回的有效对象 Ack。
            std::uint32_t one = 1;
            raw::UrmaApi::AckJfc(&jfc, &one, 1);
            bool matched = false;
            for (std::size_t j = 0; j < domain_count_; ++j) {
                auto& owner = domains_[j];
                const bool tx = jfc == owner.binding.pool->send_jfc();
                const bool rx = jfc == owner.binding.pool->recv_jfc();
                if (tx || rx) {
                    ++owner.stats.events;
                    ++owner.stats.acks;
                    owner.stats.tx_events += tx;
                    owner.stats.rx_events += rx;
                    matched = true;
                    break;
                }
            }
            unknown_notifications_ += !matched;
        }
    }
    return result;
}

std::expected<void, ProgressError> RawContextProgress::Rearm() noexcept {
    if (!context_) {
        return std::unexpected(Error(ProgressErrorCode::kNotReady));
    }
    if (submitted_) {
        return std::unexpected(Error(ProgressErrorCode::kInUse));
    }
    for (std::size_t i = 0; i < domain_count_; ++i) {
        auto& domain = domains_[i];
        if (!Usable(domain) || !domain.binding.pool->jfce()) {
            continue;
        }
        for (auto* jfc : {domain.binding.pool->send_jfc(), domain.binding.pool->recv_jfc()}) {
            const auto status = raw::UrmaApi::RearmJfc(jfc, false);
            ++domain.stats.rearm_calls;
            if (status != URMA_SUCCESS) {
                Report(i, Error(ProgressErrorCode::kProvider, status), true);
                break;
            }
        }
    }
    return {};
}

std::expected<ProgressBatch, ProgressError> RawContextProgress::Async(std::size_t budget) noexcept {
    ProgressBatch result;
    if (context_->async_fd < 0) {
        return result;
    }
    for (; result.processed < budget; ++result.processed) {
        pollfd fd{.fd = context_->async_fd, .events = POLLIN, .revents = 0};
        const auto ready = ::poll(&fd, 1, 0);
        if (ready < 0 && errno == EINTR) {
            return result;
        }
        if (ready < 0 || (fd.revents & (POLLERR | POLLHUP | POLLNVAL))) {
            const auto error = Error(ProgressErrorCode::kSystem, ready < 0 ? errno : EIO);
            for (std::size_t i = 0; i < domain_count_; ++i) {
                Report(i, error, true);
            }
            return std::unexpected(error);
        }
        if (!ready || !(fd.revents & POLLIN)) {
            return result;
        }
        urma_async_event_t provider_event{};
        const auto status = raw::UrmaApi::GetAsyncEvent(context_, &provider_event);
        if (status != URMA_SUCCESS) {
            if (status == URMA_EAGAIN) {
                return result;
            }
            for (std::size_t i = 0; i < domain_count_; ++i) {
                Report(i, Error(ProgressErrorCode::kProvider, status), true);
            }
            return std::unexpected(Error(ProgressErrorCode::kProvider, status));
        }
        // Ack 可能改写调用方结构；先复制字段，Get 成功后无论归属都恰好 Ack 一次再路由副本。
        const auto event = provider_event;
        raw::UrmaApi::AckAsyncEvent(&provider_event);
        RawAsyncObservation observation{.event = event, .context_matches = event.urma_ctx == context_};
        std::optional<std::size_t> match;
        bool passive = false;
        if (observation.context_matches) {
            for (std::size_t i = 0; i < domain_count_ && !match; ++i) {
                auto* pool = domains_[i].binding.pool;
                if (event.event_type == URMA_EVENT_JFC_ERR) {
                    if (event.element.jfc &&
                        (event.element.jfc == pool->send_jfc() || event.element.jfc == pool->recv_jfc())) {
                        match = i;
                    }
                } else if (event.event_type == URMA_EVENT_JFR_ERR || event.event_type == URMA_EVENT_JFR_LIMIT) {
                    if (event.element.jfr && event.element.jfr == domains_[i].event_jfr) {
                        match = i;
                    }
                } else if (event.event_type == URMA_EVENT_JETTY_ERR) {
                    if (event.element.jetty && event.element.jetty == pool->rx_target_jetty()) {
                        match = i;
                        passive = true;
                    } else {
                        for (std::size_t j = 0; j < pool->capacity(); ++j) {
                            // 未确认 context 与指针相等前不读取 event union 对象内的任何字段。
                            if (event.element.jetty && event.element.jetty == pool->lane_jetty(j)) {
                                match = i;
                                const auto lane = pool->lane(j);
                                if (lane) {
                                    observation.lane = *lane;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
        observation.object_matches = match.has_value();
        if (match) {
            auto& domain = domains_[*match];
            ++domain.stats.async_events;
            ++domain.stats.async_acks;
            if (Usable(domain)) {
                if (event.event_type == URMA_EVENT_JFR_LIMIT) {
                    domain.stats.watermark_pending = domain.binding.watermark_threshold != 0;
                } else if (event.event_type == URMA_EVENT_JETTY_ERR && !passive && observation.lane) {
                    (void)domain.binding.pool->MarkFaulted(*observation.lane);
                } else {
                    Report(*match, Error(ProgressErrorCode::kRetained, static_cast<int>(event.event_type)), true);
                }
            }
            if (domain.binding.on_async) {
                domain.binding.on_async(domain.binding.cookie, observation);
            }
        } else {
            ++unknown_async_events_;
            for (std::size_t i = 0; i < domain_count_; ++i) {
                if (domains_[i].binding.on_async) {
                    domains_[i].binding.on_async(domains_[i].binding.cookie, observation);
                }
            }
        }
    }
    result.runnable = true;
    return result;
}

std::expected<void, ProgressError>
RawContextProgress::RequestReplacement(raw::JettyLane lane, const raw::JettyLaneQuiescence& proof) noexcept {
    if (!context_ || submitted_) {
        return std::unexpected(Error(context_ ? ProgressErrorCode::kInUse : ProgressErrorCode::kNotReady));
    }
    if (!proof.events_acked || !proof.queue_users_stopped || !proof.completions_drained) {
        return std::unexpected(Error(ProgressErrorCode::kInvalidArgument));
    }
    for (std::size_t i = 0; i < domain_count_; ++i) {
        auto& domain = domains_[i];
        if (domain.binding.pool != lane.pool) {
            continue;
        }
        const auto state = domain.binding.pool->lane_state(lane);
        if (!Usable(domain) || !state ||
            (*state != raw::JettyLaneState::kDrained && *state != raw::JettyLaneState::kReplacing)) {
            return std::unexpected(Error(ProgressErrorCode::kInvalidArgument, 0, i));
        }
        if (domain.replacement || (job_ && job_->domain == i)) {
            return std::unexpected(Error(ProgressErrorCode::kInUse, 0, i));
        }
        domain.replacement = lane;
        domain.replacement_proof = proof;
        return {};
    }
    return std::unexpected(Error(ProgressErrorCode::kInvalidArgument));
}

std::optional<ControlTask> RawContextProgress::PendingControl() noexcept {
    if (!context_ || submitted_) {
        return std::nullopt;
    }
    if (job_ && !Usable(domains_[job_->domain])) {
        job_.reset();
    }
    for (std::size_t visited = 0; !job_ && visited < domain_count_; ++visited) {
        const auto index = control_cursor_;
        control_cursor_ = (control_cursor_ + 1) % domain_count_;
        auto& domain = domains_[index];
        if (!Usable(domain) || domain.start_control_retry || now_ns_ < domain.control_after) {
            continue;
        }
        const auto capacity = domain.binding.pool->capacity();
        for (std::size_t i = 0; i < capacity; ++i) {
            const auto lane_index = domain.control_lane;
            domain.control_lane = (domain.control_lane + 1) % capacity;
            const auto lane = domain.binding.pool->lane(lane_index);
            if (lane && domain.binding.pool->lane_state(*lane) == raw::JettyLaneState::kFaulted) {
                job_ = Job{.kind = JobKind::kDrain, .domain = index, .lane = *lane};
                break;
            }
        }
        if (!job_ && domain.replacement) {
            job_ = Job{.kind = JobKind::kReplace,
                       .domain = index,
                       .lane = *domain.replacement,
                       .proof = domain.replacement_proof};
        }
        if (!job_ && domain.stats.watermark_pending && !domain.binding.rx->stopped() &&
            domain.binding.rx->posted() > domain.binding.watermark_threshold) {
            job_ = Job{.kind = JobKind::kWatermark, .domain = index};
        }
        if (job_) {
            if (next_token_ == std::numeric_limits<std::uint64_t>::max()) {
                Report(index, Error(ProgressErrorCode::kResourceExhausted), true);
                job_.reset();
            } else {
                job_->token = ++next_token_;
            }
        }
    }
    if (!job_) {
        return std::nullopt;
    }
    return ControlTask{.function = RunControl, .argument = this, .token = job_->token};
}

void RawContextProgress::ControlSubmitted(ControlTaskId id) noexcept {
    // TrySubmit 之后 worker 可能已运行；这里只写 owner 独占 bookkeeping，不改 job 或 raw 对象。
    if (job_ && !submitted_) {
        submitted_ = id;
    }
}

std::expected<void, ControlTaskError> RawContextProgress::RunControl(void* argument, std::uint64_t token) noexcept {
    auto& self = *static_cast<RawContextProgress*>(argument);
    if (!self.job_ || self.job_->token != token) {
        return std::unexpected(ControlTaskError{ESTALE});
    }
    const auto job = *self.job_;
    auto& domain = self.domains_[job.domain];
    if (!self.Usable(domain)) {
        return std::unexpected(ControlTaskError{ECANCELED});
    }
    if (job.kind == JobKind::kWatermark) {
        if (domain.binding.rx->stopped() || domain.binding.rx->posted() <= domain.binding.watermark_threshold) {
            return std::unexpected(ControlTaskError{EAGAIN});
        }
        urma_jfr_attr_t attr{};
        attr.mask = JFR_RX_THRESHOLD;
        attr.rx_threshold = domain.binding.watermark_threshold;
        const auto status = raw::UrmaApi::ModifyJfr(domain.binding.pool->jfr(), &attr);
        if (status != URMA_SUCCESS) {
            return std::unexpected(ControlTaskError{status});
        }
    } else if (job.kind == JobKind::kDrain) {
        const auto drained = domain.binding.pool->BeginDrain(job.lane);
        if (!drained) {
            return std::unexpected(
                ControlTaskError{drained.error().provider_error ? drained.error().provider_error : EIO});
        }
    } else {
        const auto replaced = domain.binding.pool->ReplaceDrainedLane(job.lane, job.proof);
        if (!replaced) {
            return std::unexpected(
                ControlTaskError{replaced.error().provider_error ? replaced.error().provider_error : EIO});
        }
    }
    return {};
}

void RawContextProgress::ControlCompleted(const ControlTaskCompletion& completion) noexcept {
    if (!submitted_ || !job_ || completion.id != *submitted_ || completion.argument != this ||
        completion.token != job_->token) {
        return;
    }
    const auto job = *job_;
    auto& domain = domains_[job.domain];
    submitted_.reset();
    job_.reset();
    if (completion.cancelled || !Usable(domain)) {
        // 取消不撤销已执行的硬件修改/创建；不能恢复数据面使用可能已变更的依赖。
        Report(job.domain, Error(ProgressErrorCode::kRetained, ECANCELED), true);
        return;
    }
    if (!completion.result) {
        domain.start_control_retry = true;
        Report(job.domain, Error(ProgressErrorCode::kProvider, completion.result.error().code), false);
        return;
    }
    domain.control_after = 0;
    if (job.kind == JobKind::kWatermark) {
        domain.stats.watermark_pending = false;
        ++domain.stats.watermark_rearms;
    } else if (job.kind == JobKind::kReplace) {
        domain.replacement.reset();
    }
}
} // namespace runtime
} // namespace kbsocket
