// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/raw_context_progress.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <deque>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
class JettyPoolTestAccess {
public:
    // 只构造 Open 的软件规模拒绝边界；测试立即恢复，不对虚构槽位执行任何 provider 操作。
    static void SetLaneCount(JettyPool& pool, std::size_t count) {
        pool.slots_.resize(count);
    }
};
} // namespace raw
namespace runtime {
namespace {
class RawContextProgressTest : public ::testing::Test {
protected:
    struct Cookie {
        RawContextProgressTest* test;
        std::size_t index;
    };
    static inline RawContextProgressTest* self;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    std::array<urma_jfc_t, 4> cq_{};
    std::array<urma_jfce_t, 2> jfce_{};
    std::array<urma_jfr_t, 2> jfr_{};
    std::array<urma_jetty_t, 6> jetty_{};
    std::array<urma_target_seg_t, 2> segment_{};
    std::array<raw::JettyPool, 2> pools_;
    std::array<raw::RxBufferPool, 2> buffers_;
    std::array<raw::AttemptLedger, 2> ledgers_;
    std::array<Cookie, 2> cookies_{{{this, 0}, {this, 1}}};
    std::array<RawQueueBinding, 2> bindings_{};
    RawContextProgress source_;
    urma_target_jetty_t target_{};
    std::array<std::deque<std::uint64_t>, 2> posted_;
    std::array<std::deque<urma_cr_t>, 4> completions_;
    std::array<std::deque<urma_jfc_t*>, 2> notifications_;
    std::deque<urma_async_event_t> async_;
    std::array<std::vector<raw::RxBufferLease>, 2> held_;
    std::array<std::size_t, 2> rx_callbacks_{}, tx_callbacks_{}, async_callbacks_{}, error_callbacks_{};
    std::array<std::size_t, 4> poll_calls_{};
    std::array<std::size_t, 6> flush_calls_{};
    std::array<std::size_t, 2> post_calls_{};
    std::vector<urma_jfc_t*> acked_jfcs_;
    std::vector<urma_jetty_t*> modified_jetties_;
    std::size_t async_acks_ = 0, async_gets_ = 0, modifies_ = 0, watermark_modifies_ = 0;
    std::size_t create_cq_ = 0, create_jfr_ = 0, create_jetty_ = 0, create_jfce_ = 0, register_ = 0;
    std::size_t deletes_ = 0;
    bool hold_rx_ = false;
    bool control_scope_ = false;
    bool overwrite_acked_event_ = false;
    int modify_status_ = URMA_SUCCESS;
    std::optional<std::size_t> eagain_prefix_;
    std::optional<std::size_t> failed_cq_;

    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfce = [](auto* ctx) {
            auto* value = &self->jfce_[self->create_jfce_++];
            value->urma_ctx = ctx;
            return value;
        };
        f.create_jfc = [](auto* ctx, auto* config) {
            auto* value = &self->cq_[self->create_cq_++];
            value->urma_ctx = ctx;
            value->jfc_cfg = *config;
            return value;
        };
        f.create_jfr = [](auto* ctx, auto* config) {
            auto* value = &self->jfr_[self->create_jfr_++];
            value->urma_ctx = ctx;
            value->jfr_cfg = *config;
            value->jfr_id.id = 50 + self->create_jfr_;
            return value;
        };
        f.create_jetty = [](auto* ctx, auto* config) {
            auto* value = &self->jetty_[self->create_jetty_++];
            value->urma_ctx = ctx;
            value->jetty_cfg = *config;
            value->jetty_id.id = 100 + self->create_jetty_;
            return value;
        };
        f.register_seg = [](auto* ctx, auto* config) {
            auto* value = &self->segment_[self->register_++];
            value->urma_ctx = ctx;
            value->seg.ubva.va = config->va;
            value->seg.len = config->len;
            return value;
        };
        f.unregister_seg = [](auto*) {
            ++self->deletes_;
            return URMA_SUCCESS;
        };
        f.delete_jetty = [](auto*) {
            ++self->deletes_;
            return URMA_SUCCESS;
        };
        f.delete_jfr = [](auto*) {
            ++self->deletes_;
            return URMA_SUCCESS;
        };
        f.delete_jfc = [](auto*) {
            ++self->deletes_;
            return URMA_SUCCESS;
        };
        f.delete_jfce = [](auto*) {
            ++self->deletes_;
            return URMA_SUCCESS;
        };
        f.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) {
            const auto index = static_cast<std::size_t>(jfr - self->jfr_.data());
            ++self->post_calls_[index];
            std::size_t accepted = 0;
            *bad = nullptr;
            for (; wr; wr = wr->next) {
                if (self->eagain_prefix_ && accepted == *self->eagain_prefix_) {
                    *bad = wr;
                    self->eagain_prefix_.reset();
                    return URMA_EAGAIN;
                }
                self->posted_[index].push_back(wr->user_ctx);
                ++accepted;
            }
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* cq, int limit, auto* cr) {
            const auto index = static_cast<std::size_t>(cq - self->cq_.data());
            ++self->poll_calls_[index];
            if (self->failed_cq_ == index) {
                return -1;
            }
            int count = 0;
            auto& queue = self->completions_[index];
            while (count < limit && !queue.empty()) {
                cr[count++] = queue.front();
                queue.pop_front();
            }
            return count;
        };
        f.wait_jfc = [](auto* jfce, std::uint32_t limit, int timeout, auto** out) {
            EXPECT_EQ(timeout, 0);
            auto& queue = self->notifications_[jfce - self->jfce_.data()];
            std::uint32_t count = 0;
            while (count < limit && !queue.empty()) {
                out[count++] = queue.front();
                queue.pop_front();
            }
            return static_cast<int>(count);
        };
        f.ack_jfc = [](auto** jfc, auto* events, std::uint32_t count) {
            for (std::uint32_t i = 0; i < count; ++i) {
                EXPECT_EQ(events[i], 1u);
                self->acked_jfcs_.push_back(jfc[i]);
            }
        };
        f.rearm_jfc = [](auto*, bool solicited_only) {
            EXPECT_FALSE(solicited_only);
            return URMA_SUCCESS;
        };
        f.get_async_event = [](auto* ctx, auto* out) {
            ++self->async_gets_;
            std::uint64_t one = 0;
            EXPECT_EQ(::read(ctx->async_fd, &one, sizeof(one)), static_cast<ssize_t>(sizeof(one)));
            if (self->async_.empty()) {
                return URMA_EAGAIN;
            }
            *out = self->async_.front();
            self->async_.pop_front();
            return URMA_SUCCESS;
        };
        f.ack_async_event = [](auto* event) {
            ++self->async_acks_;
            if (self->overwrite_acked_event_) {
                *event = {};
            }
        };
        f.modify_jfr = [](auto*, auto* attr) {
            EXPECT_TRUE(self->control_scope_);
            EXPECT_EQ(attr->mask, JFR_RX_THRESHOLD);
            EXPECT_EQ(attr->rx_threshold, 2u);
            ++self->watermark_modifies_;
            return self->modify_status_;
        };
        f.modify_jetty = [](auto* jetty, auto* attr) {
            EXPECT_TRUE(self->control_scope_);
            EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
            ++self->modifies_;
            self->modified_jetties_.push_back(jetty);
            return self->modify_status_;
        };
        f.flush_jetty = [](auto* jetty, int limit, auto*) {
            EXPECT_LE(limit, 4);
            ++self->flush_calls_[jetty - self->jetty_.data()];
            return 0;
        };
        return f;
    }

    void SetUp() override {
        self = this;
        context_.async_fd = ::eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE | EFD_CLOEXEC);
        ASSERT_GE(context_.async_fd, 0);
        for (auto& channel : jfce_) {
            channel.fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            ASSERT_GE(channel.fd, 0);
        }
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = LaneCount() + 1;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        raw::JettyPoolConfig config;
        config.jetty_count = LaneCount();
        config.tx_depth = config.rx_depth = 4;
        config.events = true;
        for (std::size_t i = 0; i < 2; ++i) {
            ASSERT_TRUE(pools_[i].Open(&context_, cap, config));
            ASSERT_TRUE(buffers_[i].Open(pools_[i]));
            ASSERT_TRUE(ledgers_[i].Open(pools_[i], 4));
            bindings_[i] = {.pool = &pools_[i],
                            .rx = &buffers_[i],
                            .ledger = &ledgers_[i],
                            .on_tx =
                                [](void* data, const raw::TxCompletionEvent&) noexcept {
                                    auto& c = *static_cast<Cookie*>(data);
                                    ++c.test->tx_callbacks_[c.index];
                                },
                            .on_rx =
                                [](void* data, const raw::RxBufferEvent& event) noexcept {
                                    auto& c = *static_cast<Cookie*>(data);
                                    ++c.test->rx_callbacks_[c.index];
                                    if (event.lease) {
                                        if (c.test->hold_rx_) {
                                            c.test->held_[c.index].push_back(*event.lease);
                                        } else {
                                            EXPECT_TRUE(c.test->buffers_[c.index].Release(*event.lease));
                                        }
                                    }
                                },
                            .cookie = &cookies_[i],
                            .on_async =
                                [](void* data, const RawAsyncObservation&) noexcept {
                                    auto& c = *static_cast<Cookie*>(data);
                                    // 回调发生前必须已 Ack，错误归属亦不能漏 Ack。
                                    EXPECT_EQ(c.test->async_gets_, c.test->async_acks_);
                                    ++c.test->async_callbacks_[c.index];
                                },
                            .on_error =
                                [](void* data, const ProgressError&) noexcept {
                                    auto& c = *static_cast<Cookie*>(data);
                                    ++c.test->error_callbacks_[c.index];
                                }};
        }
    }

    void TearDown() override {
        EXPECT_TRUE(source_.Close());
        for (std::size_t i = 0; i < 2; ++i) {
            // 故障域模拟不可证明 DMA 终结，测试也不得伪造注销；普通域逐 CQE/lease 清理。
            if (pools_[i].retained()) {
                continue;
            }
            buffers_[i].Stop();
            while (!posted_[i].empty()) {
                QueueRx(i);
            }
            std::array<raw::RxBufferEvent, 8> events{};
            while (buffers_[i].posted()) {
                auto count = buffers_[i].Poll(events);
                ASSERT_TRUE(count);
                ASSERT_GT(*count, 0u);
                for (std::size_t j = 0; j < *count; ++j) {
                    ASSERT_TRUE(events[j].lease);
                    EXPECT_TRUE(buffers_[i].Release(*events[j].lease));
                }
            }
            for (auto lease : held_[i]) {
                EXPECT_TRUE(buffers_[i].Release(lease));
            }
            for (std::size_t j = 0; j < LaneCount(); ++j) {
                auto lane = pools_[i].lane(j);
                ASSERT_TRUE(lane);
                const auto state = pools_[i].lane_state(*lane);
                if (state == raw::JettyLaneState::kFaulted) {
                    control_scope_ = true;
                    modify_status_ = URMA_SUCCESS;
                    ASSERT_TRUE(pools_[i].BeginDrain(*lane));
                    control_scope_ = false;
                }
                if (pools_[i].lane_state(*lane) == raw::JettyLaneState::kError) {
                    ASSERT_TRUE(pools_[i].ObserveFlushDone(*lane));
                }
                if (pools_[i].lane_state(*lane) == raw::JettyLaneState::kFlushReady) {
                    std::array<urma_cr_t, 4> cr{};
                    ASSERT_EQ(pools_[i].FlushSend(*lane, cr), 0);
                }
            }
            EXPECT_TRUE(ledgers_[i].Close());
            EXPECT_TRUE(buffers_[i].Close());
            EXPECT_TRUE(pools_[i].Close());
        }
        for (auto& channel : jfce_) {
            ::close(channel.fd);
        }
        ::close(context_.async_fd);
    }

    void Open() {
        ASSERT_TRUE(source_.Open(&context_, bindings_));
    }
    virtual std::size_t LaneCount() const noexcept {
        return 1;
    }
    urma_jetty_t* ActiveJetty(std::size_t domain, std::size_t lane = 0) {
        return &jetty_[domain * (LaneCount() + 1) + 1 + lane];
    }
    void QueueRx(std::size_t domain) {
        ASSERT_FALSE(posted_[domain].empty());
        urma_cr_t cr{};
        cr.user_ctx = posted_[domain].front();
        posted_[domain].pop_front();
        cr.flag.bs.s_r = 1;
        cr.flag.bs.jetty = 1;
        cr.local_id = jetty_[domain * (LaneCount() + 1)].jetty_id.id;
        cr.opcode = URMA_CR_OPC_SEND;
        cr.completion_len = 8;
        completions_[domain * 2 + 1].push_back(cr);
    }
    void QueueAsync(urma_async_event_t event) {
        async_.push_back(event);
        const std::uint64_t one = 1;
        ASSERT_EQ(::write(context_.async_fd, &one, sizeof(one)), static_cast<ssize_t>(sizeof(one)));
    }
    void RunJob(bool cancelled = false) {
        const auto task = source_.PendingControl();
        ASSERT_TRUE(task);
        const ControlTaskId id{.slot = 0, .sequence = task->token};
        source_.ControlSubmitted(id);
        // 任务已交给 worker 后 source 自己也拒绝 owner 数据/通知访问，防止调用方漏冻结。
        EXPECT_FALSE(source_.Progress(ProgressKind::kRx, 1, 0));
        EXPECT_FALSE(source_.CollectNotifications(1));
        EXPECT_FALSE(source_.Rearm());
        EXPECT_FALSE(source_.Close());
        control_scope_ = true;
        auto result = task->function(task->argument, task->token);
        control_scope_ = false;
        source_.ControlCompleted(
            {.id = id, .argument = task->argument, .token = task->token, .cancelled = cancelled, .result = result});
    }
};

TEST_F(RawContextProgressTest, ValidatesBindingsAndOwnsAsyncConsumptionUntilExplicitUnbind) {
    auto invalid = bindings_;
    invalid[0].on_rx = nullptr;
    EXPECT_FALSE(source_.Open(&context_, invalid));
    invalid = bindings_;
    invalid[1] = invalid[0];
    EXPECT_FALSE(source_.Open(&context_, invalid));
    urma_context_t wrong{};
    EXPECT_FALSE(source_.Open(&wrong, bindings_));
    Open();
    EXPECT_EQ(source_.wait_fds().size(), 3u);
    RawContextProgress duplicate;
    EXPECT_FALSE(duplicate.Open(&context_, bindings_));
    const auto deleted_before = deletes_;
    ASSERT_TRUE(source_.Close());
    EXPECT_EQ(deletes_, deleted_before);
    ASSERT_TRUE(duplicate.Open(&context_, bindings_));
    ASSERT_TRUE(duplicate.Close());
}

TEST_F(RawContextProgressTest, OversizedDomainIsRejectedBeforeAsyncOwnerIsRegistered) {
    raw::JettyPoolTestAccess::SetLaneCount(pools_[0], RawContextProgress::kMaxLanes + 1);
    const auto rejected = source_.Open(&context_, bindings_);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, ProgressErrorCode::kInvalidArgument);
    raw::JettyPoolTestAccess::SetLaneCount(pools_[0], 1);
    // 拒绝不能留下 context owner 占用；恢复原有效配置后可立即由另一 source 绑定。
    RawContextProgress other;
    ASSERT_TRUE(other.Open(&context_, bindings_));
    ASSERT_TRUE(other.Close());
}

TEST_F(RawContextProgressTest, RefillBudgetRotatesDomainsAndDefersEagainWithoutBusyRetry) {
    Open();
    eagain_prefix_ = 1;
    auto result = source_.Progress(ProgressKind::kRefill, 2, 10);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->processed, 2u);
    EXPECT_EQ(buffers_[0].posted(), 1u);
    EXPECT_EQ(buffers_[1].posted(), 1u);
    EXPECT_EQ(source_.domain_stats(0)->refill_retries, 1u);
    const auto calls = post_calls_[0];
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 2, 11));
    EXPECT_EQ(post_calls_[0], calls);
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 2, 10 + RawContextProgress::kRetryNs));
    EXPECT_GT(post_calls_[0], calls);
    EXPECT_LE(buffers_[0].posted(), 4u);
}

TEST_F(RawContextProgressTest, UnsubmittedControlIntentCanBeDetachedWithoutDeletingRawResources) {
    bindings_[0].watermark_threshold = 2;
    Open();
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 0));
    ASSERT_TRUE(source_.PendingControl());
    const auto before = deletes_;
    ASSERT_TRUE(source_.Close());
    EXPECT_EQ(deletes_, before);
    EXPECT_EQ(watermark_modifies_, 0u);
    EXPECT_EQ(buffers_[0].posted(), 4u);
}

TEST_F(RawContextProgressTest, WaterlineDebtWaitsForReturnedLeasesBeforeControlRearm) {
    bindings_[0].watermark_threshold = 2;
    Open();
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 1));
    RunJob();
    EXPECT_EQ(watermark_modifies_, 1u);
    hold_rx_ = true;
    QueueRx(0);
    QueueRx(0);
    QueueRx(0);
    ASSERT_TRUE(source_.Progress(ProgressKind::kRx, 8, 2));
    urma_async_event_t limit{};
    limit.urma_ctx = &context_;
    limit.event_type = URMA_EVENT_JFR_LIMIT;
    limit.element.jfr = &jfr_[0];
    QueueAsync(limit);
    ASSERT_TRUE(source_.Progress(ProgressKind::kAsync, 8, 3));
    EXPECT_TRUE(source_.domain_stats(0)->watermark_pending);
    EXPECT_FALSE(source_.PendingControl());
    auto refill = source_.Progress(ProgressKind::kRefill, 8, 4);
    ASSERT_TRUE(refill);
    EXPECT_FALSE(refill->runnable);
    EXPECT_EQ(buffers_[0].posted(), 1u);
    // 归还上层 Held lease 才恢复补槽；仅低水位事件本身不能生成新缓冲或提前 rearm。
    for (auto lease : held_[0]) {
        ASSERT_TRUE(buffers_[0].Release(lease));
    }
    held_[0].clear();
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 5));
    EXPECT_EQ(buffers_[0].posted(), 4u);
    RunJob();
    EXPECT_EQ(watermark_modifies_, 2u);
    EXPECT_FALSE(source_.domain_stats(0)->watermark_pending);
}

TEST_F(RawContextProgressTest, NotificationDuplicatesAndUnknownOwnersAreAckedWithoutWaiting) {
    Open();
    urma_jfc_t unknown{};
    notifications_[0] = {&cq_[0], &cq_[0], &cq_[1], &unknown};
    ASSERT_TRUE(source_.Rearm());
    // 即使 recheck 已经拿到 RX CQE，通知仍须独立 Get/Ack；不能依赖进入睡眠才处理通知。
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 0));
    QueueRx(0);
    ASSERT_TRUE(source_.Progress(ProgressKind::kRx, 8, 0));
    auto events = source_.CollectNotifications(8);
    ASSERT_TRUE(events);
    EXPECT_EQ(events->processed, 4u);
    EXPECT_EQ(acked_jfcs_.size(), 4u);
    EXPECT_EQ(source_.domain_stats(0)->tx_events, 2u);
    EXPECT_EQ(source_.domain_stats(0)->rx_events, 1u);
    EXPECT_EQ(source_.domain_stats(0)->acks, 3u);
    EXPECT_EQ(source_.unknown_notifications(), 1u);
    EXPECT_EQ(source_.domain_stats(0)->rx_cqes, 1u);
}

TEST_F(RawContextProgressTest, AsyncOwnershipIsCheckedBeforeDereferenceAndEveryGetIsAcked) {
    Open();
    ASSERT_TRUE(source_.Progress(ProgressKind::kAsync, 4, 0));
    EXPECT_EQ(async_gets_, 0u);
    urma_context_t other{};
    urma_async_event_t unknown{};
    unknown.urma_ctx = &other;
    unknown.event_type = URMA_EVENT_JETTY_ERR;
    unknown.element.jetty = reinterpret_cast<urma_jetty_t*>(1);
    QueueAsync(unknown);
    unknown.urma_ctx = &context_;
    QueueAsync(unknown);
    ASSERT_TRUE(source_.Progress(ProgressKind::kAsync, 4, 0));
    EXPECT_EQ(async_gets_, 2u);
    EXPECT_EQ(async_acks_, 2u);
    EXPECT_EQ(source_.unknown_async_events(), 2u);
    EXPECT_FALSE(pools_[0].retained());
    EXPECT_FALSE(pools_[1].retained());
    EXPECT_FALSE(source_.PendingControl());
}

TEST_F(RawContextProgressTest, JfcErrorPermanentlyRetainsOnlyItsDomain) {
    Open();
    urma_async_event_t error{};
    error.urma_ctx = &context_;
    error.event_type = URMA_EVENT_JFC_ERR;
    error.element.jfc = &cq_[0];
    QueueAsync(error);
    ASSERT_TRUE(source_.Progress(ProgressKind::kAsync, 4, 0));
    EXPECT_TRUE(pools_[0].retained());
    EXPECT_TRUE(buffers_[0].stopped());
    EXPECT_FALSE(pools_[1].retained());
    EXPECT_FALSE(source_.WaitFdEnabled(jfce_[0].fd));
    EXPECT_TRUE(source_.WaitFdEnabled(jfce_[1].fd));
    EXPECT_TRUE(source_.WaitFdEnabled(context_.async_fd));
    ASSERT_TRUE(source_.Progress(ProgressKind::kTx, 8, 1));
    ASSERT_TRUE(source_.Progress(ProgressKind::kRx, 8, 1));
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 1));
    EXPECT_EQ(poll_calls_[0], 0u);
    EXPECT_EQ(poll_calls_[1], 0u);
    EXPECT_EQ(post_calls_[0], 0u);
    EXPECT_GT(poll_calls_[2], 0u);
    EXPECT_EQ(buffers_[1].posted(), 4u);
    EXPECT_EQ(source_.domain_stats(0)->async_events, 1u);
    EXPECT_EQ(source_.domain_stats(0)->async_acks, 1u);
}

TEST_F(RawContextProgressTest, CopiedAsyncEventKeepsOwnershipAfterAckAndDomainRetention) {
    Open();
    overwrite_acked_event_ = true;
    urma_async_event_t event{};
    event.urma_ctx = &context_;
    event.event_type = URMA_EVENT_JFC_ERR;
    event.element.jfc = &cq_[0];
    QueueAsync(event);
    event.event_type = URMA_EVENT_JETTY_ERR;
    event.element.jetty = ActiveJetty(0);
    QueueAsync(event);
    // 第一事件封域后 lane() 不再返回活动身份；第二事件仍由已知对象指针归属到原域。
    ASSERT_TRUE(source_.Progress(ProgressKind::kAsync, 4, 0));
    EXPECT_TRUE(pools_[0].retained());
    EXPECT_EQ(async_acks_, 2u);
    EXPECT_EQ(source_.domain_stats(0)->async_events, 2u);
    EXPECT_EQ(source_.domain_stats(0)->async_acks, 2u);
    EXPECT_EQ(async_callbacks_[0], 2u);
    EXPECT_EQ(async_callbacks_[1], 0u);
    EXPECT_EQ(source_.unknown_async_events(), 0u);
    EXPECT_EQ(modifies_, 0u);
}

TEST_F(RawContextProgressTest, PollFailureRetainsDomainButProgressesHealthyPool) {
    Open();
    failed_cq_ = 0;
    ASSERT_TRUE(source_.Progress(ProgressKind::kTx, 8, 0));
    EXPECT_TRUE(pools_[0].retained());
    EXPECT_EQ(poll_calls_[2], 1u);
    ASSERT_TRUE(source_.Progress(ProgressKind::kTx, 8, 0));
    EXPECT_EQ(poll_calls_[0], 1u);
    EXPECT_EQ(poll_calls_[2], 2u);
}

TEST_F(RawContextProgressTest, BadNotificationFdIsRetainedWithoutTouchingHealthyDomain) {
    Open();
    ASSERT_EQ(::close(jfce_[0].fd), 0);
    ASSERT_TRUE(source_.CollectNotifications(8));
    EXPECT_TRUE(pools_[0].retained());
    EXPECT_FALSE(pools_[1].retained());
    EXPECT_FALSE(source_.WaitFdEnabled(jfce_[0].fd));
}

TEST_F(RawContextProgressTest, TxBudgetRotatesAndBothFlushBoundariesAreDrained) {
    Open();
    // 两个域都有待消费边界，单条预算不能让第一域持续占用后续 Tick。
    control_scope_ = true;
    for (std::size_t i = 0; i < 2; ++i) {
        auto lane = pools_[i].lane(0);
        ASSERT_TRUE(lane);
        ASSERT_TRUE(pools_[i].BeginDrain(*lane));
        urma_cr_t done{};
        done.flag.bs.jetty = 1;
        done.local_id = jetty_[i * 2 + 1].jetty_id.id;
        done.status = URMA_CR_WR_FLUSH_ERR_DONE;
        completions_[i * 2].push_back(done);
    }
    control_scope_ = false;
    auto batch = source_.Progress(ProgressKind::kTx, 1, 0);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->processed, 1u);
    EXPECT_EQ(tx_callbacks_[0], 1u);
    EXPECT_EQ(tx_callbacks_[1], 0u);
    batch = source_.Progress(ProgressKind::kTx, 1, 1);
    ASSERT_TRUE(batch);
    EXPECT_EQ(batch->processed, 1u);
    EXPECT_EQ(tx_callbacks_[1], 1u);
    ASSERT_TRUE(source_.Progress(ProgressKind::kFlush, 1, 2));
    EXPECT_EQ(flush_calls_[1], 1u);
    EXPECT_EQ(flush_calls_[3], 1u);
    for (std::size_t i = 0; i < 2; ++i) {
        const auto lane = pools_[i].lane(0);
        ASSERT_TRUE(lane);
        EXPECT_EQ(pools_[i].lane_state(*lane), raw::JettyLaneState::kDrained);
    }
}

TEST_F(RawContextProgressTest, ActiveJettyErrorDefersModifyAndRetriesThroughControlOwner) {
    Open();
    urma_async_event_t error{};
    error.urma_ctx = &context_;
    error.event_type = URMA_EVENT_JETTY_ERR;
    error.element.jetty = &jetty_[1];
    QueueAsync(error);
    ASSERT_TRUE(source_.Progress(ProgressKind::kAsync, 8, 10));
    const auto lane = pools_[0].lane(0);
    ASSERT_TRUE(lane);
    EXPECT_EQ(pools_[0].lane_state(*lane), raw::JettyLaneState::kFaulted);
    EXPECT_EQ(modifies_, 0u);
    modify_status_ = URMA_EAGAIN;
    RunJob();
    EXPECT_EQ(modifies_, 1u);
    EXPECT_EQ(pools_[0].lane_state(*lane), raw::JettyLaneState::kFaulted);
    EXPECT_FALSE(source_.PendingControl());
    ASSERT_TRUE(source_.Progress(ProgressKind::kTimer, 1, 10 + RawContextProgress::kRetryNs));
    EXPECT_FALSE(source_.PendingControl());
    ASSERT_TRUE(source_.Progress(ProgressKind::kTimer, 1, 10 + 2 * RawContextProgress::kRetryNs));
    modify_status_ = URMA_SUCCESS;
    RunJob();
    EXPECT_EQ(pools_[0].lane_state(*lane), raw::JettyLaneState::kError);
    EXPECT_FALSE(source_.PendingControl());
}

TEST_F(RawContextProgressTest, FatalTxCompletionRetiresWithoutRunningModifyInDataProgress) {
    Open();
    const auto attempt = ledgers_[0].Prepare({.target = &target_});
    ASSERT_TRUE(attempt);
    ASSERT_TRUE(ledgers_[0].Commit(*attempt, 1));
    urma_cr_t completion{};
    completion.flag.bs.jetty = 1;
    completion.local_id = jetty_[1].jetty_id.id;
    completion.user_ctx = *attempt;
    completion.status = URMA_CR_ACK_TIMEOUT_ERR;
    completions_[0].push_back(completion);
    ASSERT_TRUE(source_.Progress(ProgressKind::kTx, 1, 0));
    EXPECT_EQ(modifies_, 0u);
    EXPECT_EQ(ledgers_[0].size(), 0u);
    EXPECT_EQ(source_.domain_stats(0)->tx_retired, 1u);
    RunJob();
    EXPECT_EQ(modifies_, 1u);
}

TEST_F(RawContextProgressTest, CancelledControlCompletionDoesNotResumeChangedDomain) {
    bindings_[0].watermark_threshold = 2;
    Open();
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 0));
    RunJob(true);
    EXPECT_TRUE(pools_[0].retained());
    EXPECT_FALSE(pools_[1].retained());
    EXPECT_EQ(source_.domain_stats(0)->watermark_rearms, 0u);
    EXPECT_FALSE(source_.PendingControl());
}

TEST_F(RawContextProgressTest, ForeignOrStaleControlCompletionsCannotReleaseThePausedSource) {
    bindings_[0].watermark_threshold = 2;
    Open();
    ASSERT_TRUE(source_.Progress(ProgressKind::kRefill, 8, 0));
    const auto task = source_.PendingControl();
    ASSERT_TRUE(task);
    const ControlTaskId id{.slot = 2, .sequence = 17};
    source_.ControlSubmitted(id);
    // 返回的 executor 槽位序号、argument 和源 token 任一不匹配，都不得解除 owner 暂停。
    const std::array invalid{
        ControlTaskCompletion{
            .id = {.slot = 2, .sequence = 18}, .argument = task->argument, .token = task->token, .result = {}},
        ControlTaskCompletion{.id = id, .argument = &context_, .token = task->token, .result = {}},
        ControlTaskCompletion{.id = id, .argument = task->argument, .token = task->token + 1, .result = {}}};
    for (const auto& completion : invalid) {
        source_.ControlCompleted(completion);
        EXPECT_FALSE(source_.Progress(ProgressKind::kTimer, 1, 0));
        EXPECT_FALSE(source_.WaitFdEnabled(context_.async_fd));
    }
    control_scope_ = true;
    const auto result = task->function(task->argument, task->token);
    control_scope_ = false;
    source_.ControlCompleted({.id = id, .argument = task->argument, .token = task->token, .result = result});
    ASSERT_TRUE(source_.Progress(ProgressKind::kTimer, 1, 1));
    EXPECT_TRUE(source_.WaitFdEnabled(context_.async_fd));
    EXPECT_EQ(source_.domain_stats(0)->watermark_rearms, 1u);
    // 已收割完成的重复交付同样无效，不得重复确认水线或撤销后续状态。
    source_.ControlCompleted({.id = id, .argument = task->argument, .token = task->token, .result = result});
    EXPECT_EQ(source_.domain_stats(0)->watermark_rearms, 1u);
}

class RawContextTwoLaneProgressTest : public RawContextProgressTest {
protected:
    std::size_t LaneCount() const noexcept override {
        return 2;
    }
};

TEST_F(RawContextTwoLaneProgressTest, FailingFirstLaneDoesNotStarveAnotherFaultedLane) {
    Open();
    const auto first = pools_[0].lane(0);
    const auto second = pools_[0].lane(1);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(pools_[0].MarkFaulted(*first));
    ASSERT_TRUE(pools_[0].MarkFaulted(*second));
    ASSERT_TRUE(source_.Progress(ProgressKind::kTimer, 1, 100));
    modify_status_ = URMA_EAGAIN;
    RunJob();
    ASSERT_EQ(modified_jetties_.size(), 1u);
    EXPECT_EQ(modified_jetties_[0], ActiveJetty(0, 0));
    ASSERT_TRUE(source_.Progress(ProgressKind::kTimer, 1, 1000));
    ASSERT_TRUE(source_.Progress(ProgressKind::kTimer, 1, 1000 + RawContextProgress::kRetryNs));
    modify_status_ = URMA_SUCCESS;
    RunJob();
    ASSERT_EQ(modified_jetties_.size(), 2u);
    EXPECT_EQ(modified_jetties_[1], ActiveJetty(0, 1));
    EXPECT_EQ(pools_[0].lane_state(*second), raw::JettyLaneState::kError);
    EXPECT_EQ(pools_[0].lane_state(*first), raw::JettyLaneState::kFaulted);
    // 第二条 Lane 成功后仍回到第一条完成其重试，不能遗忘故障债务。
    RunJob();
    EXPECT_EQ(pools_[0].lane_state(*first), raw::JettyLaneState::kError);
}
} // namespace
} // namespace runtime
} // namespace kbsocket
