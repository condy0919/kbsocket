// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/runtime_shard.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace runtime {
namespace {
class Companion final : public ProgressSource {
public:
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind, std::size_t, std::uint64_t) noexcept override {
        ++calls;
        return ProgressBatch{1, false};
    }
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t) noexcept override {
        ++notifications;
        return ProgressBatch{};
    }
    std::expected<void, ProgressError> Rearm() noexcept override {
        return {};
    }
    std::span<const int> wait_fds() const noexcept override {
        return {};
    }
    const void* exclusive_context() const noexcept override {
        return context;
    }
    std::optional<ControlTask> PendingControl() noexcept override {
        return pending;
    }
    void ControlSubmitted(ControlTaskId) noexcept override {
        ++submitted;
        pending.reset();
    }
    void ControlCompleted(const ControlTaskCompletion&) noexcept override {
        ++completed;
    }
    std::size_t calls = 0, notifications = 0, submitted = 0, completed = 0;
    std::optional<ControlTask> pending;
    const void* context = nullptr;
};

class RuntimeShardTest : public ::testing::Test {
protected:
    struct Domain {
        urma_context_t context{};
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfce_t jfce{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetties{};
        urma_target_seg_t segment{};
        urma_target_jetty_t target{};
        raw::JettyPool pool;
        raw::RxBufferPool rx;
        raw::AttemptLedger ledger;
        RawQueueBinding queue;
        std::size_t next_cq = 0, next_jetty = 0, polls = 0, received = 0, deleted = 0, retired = 0, acked = 0;
        std::deque<std::uint64_t> posted;
        std::deque<urma_cr_t> rx_cq;
        std::deque<urma_cr_t> tx_cq;
        std::deque<urma_jfc_t*> notifications;
    };
    static inline RuntimeShardTest* self;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    std::array<Domain, 4> domains_;
    std::array<RawContextBinding, 4> bindings_{};
    std::array<Companion, 4> companions_;
    RuntimeShard shard_;
    std::atomic<bool> control_entered_{false}, control_release_{false};

    static Domain& Find(urma_context_t* context) {
        for (auto& domain : self->domains_) {
            if (&domain.context == context) {
                return domain;
            }
        }
        ADD_FAILURE() << "unknown context";
        return self->domains_[0];
    }
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfce = [](auto* c) {
            auto& d = Find(c);
            d.jfce.urma_ctx = c;
            return &d.jfce;
        };
        f.create_jfc = [](auto* c, auto* cfg) {
            auto& d = Find(c);
            auto* q = &d.cqs[d.next_cq++];
            q->urma_ctx = c;
            q->jfc_cfg = *cfg;
            return q;
        };
        f.create_jfr = [](auto* c, auto* cfg) {
            auto& d = Find(c);
            d.jfr.urma_ctx = c;
            d.jfr.jfr_cfg = *cfg;
            return &d.jfr;
        };
        f.create_jetty = [](auto* c, auto* cfg) {
            auto& d = Find(c);
            auto* q = &d.jetties[d.next_jetty++];
            q->urma_ctx = c;
            q->jetty_cfg = *cfg;
            q->jetty_id.id = d.next_jetty;
            return q;
        };
        f.register_seg = [](auto* c, auto* cfg) {
            auto& d = Find(c);
            d.segment.urma_ctx = c;
            d.segment.seg.ubva.va = cfg->va;
            d.segment.seg.len = cfg->len;
            return &d.segment;
        };
        f.post_jfr_wr = [](auto* q, auto* wr, auto** bad) {
            auto& d = Find(q->urma_ctx);
            for (; wr; wr = wr->next) {
                d.posted.push_back(wr->user_ctx);
            }
            *bad = nullptr;
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* q, int count, auto* out) {
            auto& d = Find(q->urma_ctx);
            ++d.polls;
            auto& cq = q == &d.cqs[0] ? d.tx_cq : d.rx_cq;
            int n = 0;
            while (n < count && !cq.empty()) {
                out[n++] = cq.front();
                cq.pop_front();
            }
            return n;
        };
        f.wait_jfc = [](auto* jfce, std::uint32_t limit, int, auto** out) {
            auto& d = Find(jfce->urma_ctx);
            std::uint64_t value = 0;
            (void)::read(jfce->fd, &value, sizeof(value));
            int n = 0;
            while (static_cast<std::uint32_t>(n) < limit && !d.notifications.empty()) {
                out[n++] = d.notifications.front();
                d.notifications.pop_front();
            }
            return n;
        };
        f.ack_jfc = [](auto** cq, auto* counts, std::uint32_t size) {
            for (std::uint32_t i = 0; i < size; ++i) {
                Find(cq[i]->urma_ctx).acked += counts[i];
            }
        };
        f.rearm_jfc = [](auto*, bool) { return URMA_SUCCESS; };
        f.modify_jfr = [](auto*, auto*) {
            self->control_entered_.store(true, std::memory_order_release);
            // 模拟阻塞的 provider 控制调用；主线程应继续其他三个 context 的 RX/refill。
            self->control_release_.wait(false, std::memory_order_acquire);
            return URMA_SUCCESS;
        };
        f.delete_jetty = [](auto* q) {
            ++Find(q->urma_ctx).deleted;
            return URMA_SUCCESS;
        };
        f.delete_jfr = [](auto* q) {
            ++Find(q->urma_ctx).deleted;
            return URMA_SUCCESS;
        };
        f.delete_jfc = [](auto* q) {
            ++Find(q->urma_ctx).deleted;
            return URMA_SUCCESS;
        };
        f.delete_jfce = [](auto* q) {
            ++Find(q->urma_ctx).deleted;
            return URMA_SUCCESS;
        };
        f.unregister_seg = [](auto* s) {
            ++Find(s->urma_ctx).deleted;
            return URMA_SUCCESS;
        };
        return f;
    }
    void SetUp() override {
        self = this;
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        raw::JettyPoolConfig config;
        config.tx_depth = config.rx_depth = 4;
        config.events = true;
        for (std::size_t i = 0; i < domains_.size(); ++i) {
            auto& d = domains_[i];
            d.context.async_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            d.jfce.fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            ASSERT_GE(d.context.async_fd, 0);
            ASSERT_GE(d.jfce.fd, 0);
            ASSERT_TRUE(d.pool.Open(&d.context, cap, config));
            ASSERT_TRUE(d.rx.Open(d.pool));
            ASSERT_TRUE(d.ledger.Open(d.pool, 4));
            d.queue = {.pool = &d.pool,
                       .rx = &d.rx,
                       .ledger = &d.ledger,
                       .on_tx =
                           [](void* cookie, const raw::TxCompletionEvent& event) noexcept {
                               EXPECT_TRUE(event.record);
                               if (event.record) {
                                   ++static_cast<Domain*>(cookie)->retired;
                               }
                           },
                       .on_rx =
                           [](void* cookie, const raw::RxBufferEvent& event) noexcept {
                               auto& domain = *static_cast<Domain*>(cookie);
                               EXPECT_TRUE(event.lease);
                               if (event.lease) {
                                   EXPECT_TRUE(domain.rx.Release(*event.lease));
                                   ++domain.received;
                               }
                           },
                       .cookie = &d};
            bindings_[i] = {&d.context, std::span(&d.queue, 1)};
        }
    }
    void ReleaseControl() {
        control_release_.store(true, std::memory_order_release);
        control_release_.notify_one();
    }
    static ControlTaskResult RunCompanionControl(void* argument, std::uint64_t) noexcept {
        auto& fixture = *static_cast<RuntimeShardTest*>(argument);
        fixture.control_entered_.store(true, std::memory_order_release);
        fixture.control_release_.wait(false, std::memory_order_acquire);
        return {};
    }
    void TearDown() override {
        ReleaseControl();
        // 先收割控制任务再解绑，域实际销毁另走 M1 证明门禁；shard.Close 本身不删除 URMA。
        auto closed = shard_.Close();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!closed && std::chrono::steady_clock::now() < deadline) {
            EXPECT_TRUE(shard_.Tick());
            std::this_thread::yield();
            closed = shard_.Close();
        }
        ASSERT_TRUE(closed);
        for (auto& d : domains_) {
            EXPECT_TRUE(d.ledger.Close());
            raw::ReceiveDomainCloser closer;
            ASSERT_TRUE(closer.Begin(d.pool, d.rx));
            raw::ReceiveDomainQuiescence proof{true, true, true, true};
            std::array<raw::RxBufferEvent, 4> events{};
            auto progress = closer.Advance(proof, events);
            ASSERT_TRUE(progress);
            EXPECT_EQ(progress->stage, raw::ReceiveDomainCloseStage::kClosed);
            EXPECT_EQ(d.deleted, 7u); // 两 Jetty、JFR、MR、两 JFC 最后 JFCE。
            ::close(d.context.async_fd);
            ::close(d.jfce.fd);
        }
        self = nullptr;
    }
    void CompleteOne(std::size_t index) {
        auto& d = domains_[index];
        ASSERT_FALSE(d.posted.empty());
        urma_cr_t cr{};
        cr.status = URMA_CR_SUCCESS;
        cr.user_ctx = d.posted.front();
        d.posted.pop_front();
        cr.local_id = d.jetties[0].jetty_id.id;
        cr.flag.bs.jetty = 1;
        cr.flag.bs.s_r = 1;
        cr.completion_len = 1;
        d.rx_cq.push_back(cr);
    }
    void CompleteTxAndNotify(std::size_t index, urma_cr_status_t status) {
        auto& d = domains_[index];
        // 由 Mock 的接受边界提交账本；这一层测试真实 processor 的跨 context 完成与配额归还。
        auto id = d.ledger.Prepare({.connection_id = index + 1, .target = &d.target});
        ASSERT_TRUE(id);
        ASSERT_TRUE(d.ledger.Commit(*id, *id));
        urma_cr_t cr{};
        cr.status = status;
        cr.user_ctx = *id;
        cr.local_id = d.jetties[1].jetty_id.id;
        cr.flag.bs.jetty = 1;
        d.tx_cq.push_back(cr);
        d.notifications.push_back(&d.cqs[0]);
        d.notifications.push_back(&d.cqs[1]);
        const std::uint64_t one = 1;
        ASSERT_EQ(::write(d.jfce.fd, &one, sizeof(one)), static_cast<ssize_t>(sizeof(one)));
    }
};

TEST_F(RuntimeShardTest, FourContextsProgressRefillAndDetachWithoutDeletingResources) {
    RuntimeShardConfig config;
    config.reactor.events = true;
    config.reactor.budget = 2;
    ASSERT_TRUE(shard_.Open(bindings_, config));
    ASSERT_TRUE(shard_.Tick());
    ASSERT_TRUE(shard_.Tick());
    for (const auto& d : domains_) {
        EXPECT_EQ(d.rx.posted(), 4u);
    }
    for (std::size_t round = 0; round < 3; ++round) {
        for (std::size_t i = 0; i < domains_.size(); ++i) {
            CompleteOne(i);
            CompleteTxAndNotify(i, round == 1 ? URMA_CR_REM_ACCESS_ABORT_ERR : URMA_CR_SUCCESS);
        }
        ASSERT_TRUE(shard_.RunOnce());
        for (std::size_t i = 0; i < domains_.size(); ++i) {
            EXPECT_EQ(domains_[i].received, round + 1);
            EXPECT_EQ(domains_[i].retired, round + 1);
            EXPECT_EQ(domains_[i].acked, 2 * (round + 1));
            EXPECT_EQ(domains_[i].ledger.size(), 0u);
            EXPECT_EQ(domains_[i].rx.posted(), 4u);
            ASSERT_NE(shard_.domain_stats(i, 0), nullptr);
            EXPECT_EQ(shard_.domain_stats(i, 0)->rx_posted, round + 5);
        }
    }
    ASSERT_TRUE(shard_.Close());
    for (const auto& d : domains_) {
        EXPECT_EQ(d.deleted, 0u);
    }
    // 已解绑 owner 可重新注册，同一批 raw 资源仍有效；水线/槽位不被 Close 推测终结。
    ASSERT_TRUE(shard_.Open(bindings_, config));
    ASSERT_TRUE(shard_.Close());
}

TEST_F(RuntimeShardTest, SlowControlPausesOneContextAndCloseWaitsForItsCompletion) {
    domains_[0].queue.watermark_threshold = 1;
    for (std::size_t i = 0; i < bindings_.size(); ++i) {
        bindings_[i].connections = &companions_[i];
    }
    RuntimeShardConfig config;
    config.reactor.budget = 2;
    ASSERT_TRUE(shard_.Open(bindings_, config));
    ASSERT_TRUE(shard_.Tick());
    ASSERT_TRUE(shard_.paused(0));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!control_entered_.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(control_entered_.load(std::memory_order_acquire));
    const auto polls = domains_[0].polls;
    const auto connection_calls = companions_[0].calls;
    const auto connection_notifications = companions_[0].notifications;
    for (std::size_t i = 1; i < domains_.size(); ++i) {
        CompleteOne(i);
    }
    ASSERT_TRUE(shard_.Tick());
    EXPECT_EQ(domains_[0].polls, polls);
    EXPECT_EQ(companions_[0].calls, connection_calls);
    EXPECT_EQ(companions_[0].notifications, connection_notifications);
    EXPECT_EQ(shard_.domain_stats(0, 0), nullptr);
    for (std::size_t i = 1; i < domains_.size(); ++i) {
        EXPECT_EQ(domains_[i].received, 1u);
        EXPECT_GT(companions_[i].calls, connection_calls);
    }
    auto closed = shard_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, ProgressErrorCode::kInUse);
    ReleaseControl();
    const auto reap_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (shard_.paused(0) && std::chrono::steady_clock::now() < reap_deadline) {
        ASSERT_TRUE(shard_.Tick());
        std::this_thread::yield();
    }
    ASSERT_FALSE(shard_.paused(0));
    ASSERT_NE(shard_.domain_stats(0, 0), nullptr);
    EXPECT_EQ(shard_.domain_stats(0, 0)->watermark_rearms, 1u);
    EXPECT_EQ(shard_.stats()->controls_completed, 1u);
    ASSERT_TRUE(shard_.Close());
}

TEST_F(RuntimeShardTest, CompanionControlPausesRawConnectionAndMailboxWork) {
    bindings_[0].connections = &companions_[0];
    bindings_[0].control = &companions_[1];
    bindings_[0].mailbox = &companions_[2];
    bindings_[1].mailbox = &companions_[3];
    companions_[1].pending =
        ControlTask{.function = &RuntimeShardTest::RunCompanionControl, .argument = this, .token = 1};
    ASSERT_TRUE(shard_.Open(bindings_));
    ASSERT_TRUE(shard_.Tick());
    ASSERT_TRUE(shard_.paused(0));
    const auto polls = domains_[0].polls;
    const auto calls = companions_[0].calls;
    const auto mailbox_calls = companions_[2].calls;
    const auto mailbox_notifications = companions_[2].notifications;
    const auto other_mailbox_calls = companions_[3].calls;
    // 暂停必须覆盖第四个成员：业务命令不能借 mailbox 在导入期间访问同 context 的连接。
    CompleteOne(1);
    ASSERT_TRUE(shard_.Tick());
    EXPECT_EQ(domains_[0].polls, polls);
    EXPECT_EQ(companions_[0].calls, calls);
    EXPECT_EQ(companions_[2].calls, mailbox_calls);
    EXPECT_EQ(companions_[2].notifications, mailbox_notifications);
    EXPECT_GT(companions_[3].calls, other_mailbox_calls);
    EXPECT_EQ(domains_[1].received, 1u);
    EXPECT_EQ(companions_[1].submitted, 1u);
    EXPECT_FALSE(shard_.Close());
    ReleaseControl();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (shard_.paused(0) && std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(shard_.Tick());
        std::this_thread::yield();
    }
    EXPECT_FALSE(shard_.paused(0));
    EXPECT_EQ(companions_[1].completed, 1u);
    EXPECT_EQ(companions_[0].completed, 0u);
    EXPECT_GT(companions_[2].calls, mailbox_calls);
    EXPECT_TRUE(shard_.Close());
}

TEST_F(RuntimeShardTest, RejectsCompanionSharedAcrossPauseDomainsOrHooks) {
    bindings_[0].connections = &companions_[0];
    bindings_[1].control = &companions_[0];
    EXPECT_FALSE(shard_.Open(bindings_));
    bindings_[1].control = nullptr;
    EXPECT_FALSE(shard_.Open(bindings_, {}, &companions_[0]));
    bindings_[0].control = &companions_[0];
    EXPECT_FALSE(shard_.Open(bindings_));
    // 注册失败前不能泄漏任何 raw owner 登记，修正后可以完整打开。
    bindings_[0].control = &companions_[1];
    bindings_[0].mailbox = &companions_[0];
    EXPECT_FALSE(shard_.Open(bindings_));
    bindings_[0].mailbox = &companions_[2];
    bindings_[1].mailbox = &companions_[2];
    EXPECT_FALSE(shard_.Open(bindings_));
    bindings_[1].mailbox = nullptr;
    EXPECT_FALSE(shard_.Open(bindings_, {}, &companions_[2]));
    EXPECT_TRUE(shard_.Open(bindings_));
    EXPECT_TRUE(shard_.Close());
}

TEST_F(RuntimeShardTest, RejectsWrongContextCompanionAndDeviceHooksBeforeBindingOwners) {
    bindings_[0].connections = &companions_[0];
    companions_[0].context = &domains_[1].context;
    EXPECT_FALSE(shard_.Open(bindings_));
    bindings_[0].connections = nullptr;
    bindings_[0].mailbox = &companions_[0];
    EXPECT_FALSE(shard_.Open(bindings_));
    bindings_[0].mailbox = nullptr;
    // 有设备关联的源必须进入所属 context 的组，不能单独注册为 hooks 或伪装纯 TCP shard。
    EXPECT_FALSE(shard_.Open(bindings_, {}, &companions_[0]));
    EXPECT_FALSE(shard_.Open({}, {}, &companions_[0]));
    bindings_[1].connections = &companions_[0];
    ASSERT_TRUE(shard_.Open(bindings_));
    EXPECT_TRUE(shard_.Tick());
    EXPECT_TRUE(shard_.Close());
}

TEST(RuntimeShardStandaloneTest, TcpHooksOnlyNeedsNoRawContext) {
    Companion connections;
    RuntimeShard shard;
    EXPECT_FALSE(shard.Open({}));
    ASSERT_TRUE(shard.Open({}, {.reactor = {.events = true}}, &connections));
    auto tick = shard.RunOnce();
    ASSERT_TRUE(tick);
    EXPECT_EQ(connections.calls, 8u);
    EXPECT_FALSE(shard.paused(0));
    EXPECT_EQ(shard.domain_stats(0, 0), nullptr);
    EXPECT_TRUE(shard.Close());
}

TEST_F(RuntimeShardTest, PartialOpenFailureUnregistersOwnersAndRejectsSecondShard) {
    auto invalid = bindings_;
    invalid[3].context = bindings_[0].context;
    invalid[3].queues = bindings_[0].queues;
    EXPECT_FALSE(shard_.Open(invalid));
    // 前三个 context 的登记必须回滚；原始四 context 随后可完整绑定。
    ASSERT_TRUE(shard_.Open(bindings_));
    RuntimeShard duplicate;
    auto opened = duplicate.Open(bindings_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, ProgressErrorCode::kInUse);
    ASSERT_TRUE(shard_.Close());
    ASSERT_TRUE(duplicate.Open(bindings_));
    ASSERT_TRUE(duplicate.Close());
}
} // namespace
} // namespace runtime
} // namespace kbsocket
