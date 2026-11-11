// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection.hpp"
#include "kbsocket/control/tx_lane_snapshot.hpp"
#include "kbsocket/runtime/connection_io.hpp"
#include "kbsocket/runtime/connection_mailbox.hpp"
#include "kbsocket/runtime/connection_owner.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/handshake_rx_sources.hpp"
#include "kbsocket/runtime/import_progress.hpp"
#include "kbsocket/runtime/runtime_shard.hpp"
#include "kbsocket/runtime/rx_source_registry.hpp"
#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace connection {
namespace {
class ConnectionRuntimeTest : public ::testing::Test {
protected:
    struct Receive {
        std::uint64_t id;
        urma_sge_t sge;
    };
    struct Packet {
        std::size_t to;
        urma_jetty_id_t source;
        std::uint64_t imm;
        std::vector<std::byte> bytes;
    };
    struct Endpoint {
        urma_context_t context{};
        std::array<urma_jfc_t, 2> cq{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 4> jetty{};
        std::array<urma_target_seg_t, 2> segments{};
        urma_target_jetty_t target{};
        std::size_t cq_created = 0, jetty_created = 0, registered = 0;
        raw::JettyPool pool;
        raw::RxBufferPool buffers;
        raw::AttemptLedger ledger;
        raw::TxSender sender;
        memory::SlabAllocator slab;
        ReceiveCreditDomain credits;
        SenderCreditAccount send_credits;
        CreditPeerHandle credit_peer;
        ConnectionTable routes;
        EagerReceiveDomain receive;
        Connection connection;
        runtime::ConnectionProgress scheduler;
        runtime::ImportProgress imports;
        runtime::RxSourceRegistry sources;
        runtime::HandshakeRxSources handshake_sources;
        runtime::ConnectionIoBinding io;
        runtime::ScheduledConnection scheduled;
        bool original_attached = false;
        runtime::RawQueueBinding binding;
        std::uint64_t polls = 0;
        std::array<std::uint64_t, 4> posts_by_jetty{};
        std::uint64_t callback_tx_retired = 0, callback_closing_tx = 0, callback_failed_tx = 0;
        std::uint64_t initial_accepted = 0, initial_retired = 0, initial_callback_retired = 0;
        std::uint64_t callback_rx_changes = 0;
        std::size_t callback_rx_pending = 0;
        std::uint64_t callback_count = 0;
        std::uint64_t callback_readiness_changes = 0;
        ConnectionReadiness callback_readiness;
        std::optional<ConnectionError> callback_failure;
        bool callback_bad_id = false;
        bool lane_updates_enabled = false, reject_lane_update = false;
        std::uint64_t lane_authorizations = 0;
        std::uint64_t lane_update_timeout_ns = 5'000'000'000ull;
        std::atomic<unsigned> importing{0};
        std::atomic<bool> block_import{false};
        std::deque<Receive> posted;
        std::array<std::deque<urma_cr_t>, 2> completions;
    };
    static inline ConnectionRuntimeTest* self;
    std::array<Endpoint, 2> endpoints_;
    std::deque<Packet> packets_;
    memory::MemoryBudget budget_{1024 * 1024};
    control::EndpointImportCache imports_;
    runtime::RuntimeShard shard_;
    bool shard_open_ = false;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    static std::uint64_t Now() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
    std::uint64_t now_ = Now();
    static Endpoint& Find(urma_context_t* context) {
        return context == &self->endpoints_[0].context ? self->endpoints_[0] : self->endpoints_[1];
    }
    static void OnProgress(void* cookie, const runtime::ConnectionProgressEvent& event) noexcept {
        auto& endpoint = *static_cast<Endpoint*>(cookie);
        ++endpoint.callback_count;
        endpoint.callback_readiness_changes += endpoint.callback_readiness != event.readiness;
        endpoint.callback_readiness = event.readiness;
        endpoint.callback_failure = event.failure;
        endpoint.callback_bad_id |= event.connection != endpoint.scheduled;
        endpoint.callback_tx_retired += event.tx_retired;
        if (event.state == ConnectionState::kClosing) {
            endpoint.callback_closing_tx += event.tx_retired;
        } else if (event.state == ConnectionState::kFailed) {
            endpoint.callback_failed_tx += event.tx_retired;
        }
        if (event.rx_changed) {
            ++endpoint.callback_rx_changes;
            endpoint.callback_rx_pending = event.rx_pending;
        }
    }
    static bool AuthorizeLaneUpdate(void* cookie, const control::HandshakeIdentity& peer,
                                    const control::HandshakeTxLanes& lanes) noexcept {
        auto& endpoint = *static_cast<Endpoint*>(cookie);
        ++endpoint.lane_authorizations;
        const auto* known_peer = endpoint.connection.handshake().peer();
        if (endpoint.reject_lane_update || !known_peer || peer != known_peer->identity) {
            return false;
        }
        // 测试宿主通过独立可信拓扑验证整份公告，不能把对端自报的 Lane 当成授权依据。
        const auto other = &endpoint == &self->endpoints_[0] ? 1u : 0u;
        const auto actual = control::CaptureTxLanes(self->endpoints_[other].pool);
        return actual && control::SameTxLanes(*actual, lanes);
    }
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f;
        f.create_jfc = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto* cq = &e.cq[e.cq_created++];
            cq->urma_ctx = context;
            cq->jfc_cfg = *config;
            return cq;
        };
        f.create_jfr = [](auto* context, auto* config) {
            auto& e = Find(context);
            e.jfr.urma_ctx = context;
            e.jfr.jfr_cfg = *config;
            return &e.jfr;
        };
        f.create_jetty = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto* jetty = &e.jetty[e.jetty_created++];
            jetty->urma_ctx = context;
            jetty->jetty_cfg = *config;
            jetty->jetty_id.id = 100 + (&e == &self->endpoints_[0] ? 0 : 10) + e.jetty_created;
            jetty->jetty_id.eid.raw[0] = &e == &self->endpoints_[0] ? 1 : 2;
            return jetty;
        };
        f.register_seg = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto* seg = &e.segments[e.registered++];
            seg->urma_ctx = context;
            seg->seg.ubva.va = config->va;
            seg->seg.len = config->len;
            return seg;
        };
        f.import_jetty = [](auto* context, auto* remote, auto*) {
            auto& endpoint = Find(context);
            endpoint.importing.fetch_add(1);
            while (endpoint.block_import.load()) {
                endpoint.block_import.wait(true);
            }
            auto& target = endpoint.target;
            target.urma_ctx = context;
            target.id = remote->jetty_id;
            return &target;
        };
        f.unimport_jetty = [](auto*) { return URMA_SUCCESS; };
        f.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        f.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) {
            auto& e = Find(jfr->urma_ctx);
            *bad = nullptr;
            for (; wr; wr = wr->next) {
                e.posted.push_back({wr->user_ctx, wr->src.sge[0]});
            }
            return URMA_SUCCESS;
        };
        f.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) {
            auto& e = Find(jetty->urma_ctx);
            *bad = nullptr;
            for (; wr; wr = wr->next) {
                ++e.posts_by_jetty[jetty - e.jetty.data()];
                Packet packet{wr->tjetty->id.id >= 110 ? 1u : 0u, jetty->jetty_id, wr->send.imm_data, {}};
                for (std::uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
                    const auto& sge = wr->send.src.sge[i];
                    auto* data = reinterpret_cast<const std::byte*>(sge.addr);
                    packet.bytes.insert(packet.bytes.end(), data, data + sge.len);
                }
                self->packets_.push_back(std::move(packet));
                urma_cr_t cr{};
                cr.status = URMA_CR_SUCCESS;
                cr.flag.bs.jetty = 1;
                cr.local_id = jetty->jetty_id.id;
                cr.user_ctx = wr->user_ctx;
                cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                e.completions[0].push_back(cr);
            }
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* cq, int limit, auto* output) {
            auto& e = Find(cq->urma_ctx);
            ++e.polls;
            auto& queue = e.completions[cq == &e.cq[0] ? 0 : 1];
            int count = 0;
            while (count < limit && !queue.empty()) {
                output[count++] = queue.front();
                queue.pop_front();
            }
            return count;
        };
        return f;
    }
    control::HandshakeHello Hello(std::size_t side) {
        control::HandshakeHello hello;
        hello.capabilities = control::kHandshakeCapabilityRaw;
        hello.identity = {.runtime = side + 1,
                          .receive_domain = side + 101,
                          .endpoint_generation = 1,
                          .authorization = 7,
                          .session = side + 201,
                          .token = {0, 1}};
        hello.credit_epoch = side + 301;
        hello.jetty_id = endpoints_[side].jetty[0].jetty_id.id;
        hello.eid[0] = std::byte(side + 1);
        const auto lanes = control::CaptureTxLanes(endpoints_[side].pool);
        EXPECT_TRUE(lanes);
        if (lanes) {
            hello.tx_lanes = *lanes;
        }
        urma_rjetty_t remote{};
        remote.jetty_id.id = hello.jetty_id;
        std::memcpy(&remote.jetty_id.eid, hello.eid.data(), hello.eid.size());
        remote.trans_mode = URMA_TM_RM;
        remote.tp_type = URMA_CTP;
        remote.type = URMA_JETTY;
        hello.descriptor_bytes = sizeof(remote);
        std::memcpy(hello.descriptor.data(), &remote, sizeof(remote));
        return hello;
    }
    virtual std::uint32_t SourceCapacity(std::size_t) const {
        return 8;
    }
    void SetUp() override {
        self = this;
        override_.emplace(Functions());
        ASSERT_TRUE(imports_.Open({.entries = 4, .leases = 128, .in_flight = 2}));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 3;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            e.context.async_fd = -1;
            raw::JettyPoolConfig config;
            config.jetty_count = 2;
            config.tx_depth = 16;
            config.rx_depth = 8;
            ASSERT_TRUE(e.pool.Open(&e.context, cap, config));
            ASSERT_TRUE(e.buffers.Open(e.pool));
            ASSERT_TRUE(e.buffers.Refill(8));
            ASSERT_TRUE(e.ledger.Open(e.pool, 16));
            ASSERT_TRUE(e.sender.Open(e.ledger, true));
            const std::array contexts{&e.context};
            ASSERT_TRUE(e.slab.Open(contexts, {.buffer_count = 16, .max_references = 32}, budget_));
            ASSERT_TRUE(e.routes.Open({i + 1, i + 101}, 8));
            ASSERT_TRUE(e.credits.Open(
                {.receive_domain = i + 101, .capacity = 8, .control_slots = 4, .peer_capacity = 2, .base_credits = 4}));
            ASSERT_TRUE(e.credits.RecordInitialPosted(CreditClass::kData, 4));
            ASSERT_TRUE(e.credits.RecordInitialPosted(CreditClass::kControl, 4));
            auto peer = e.credits.Admit({2 - i, i + 101, i + 301});
            ASSERT_TRUE(peer);
            e.credit_peer = *peer;
            ASSERT_TRUE(e.credits.Grant(*peer, CreditClass::kData, 4));
            ASSERT_TRUE(e.credits.Grant(*peer, CreditClass::kControl, 4));
            ASSERT_TRUE(e.send_credits.Open({2 - i, 102 - i, 302 - i}));
            ASSERT_TRUE(e.receive.Open(e.buffers, e.credits, e.routes));
            ASSERT_TRUE(e.connection.Open(sockets[i], e.routes,
                                          {.local = Hello(i),
                                           .expected_peer_authorization = 7,
                                           .compatible_peer = true,
                                           .deadline_ns = now_ + 10'000'000'000ull},
                                          now_));
            ASSERT_TRUE(e.scheduler.Open({.capacity = 8, .context = &e.context}, &OnProgress, &e));
            auto attached = e.scheduler.Attach(e.connection);
            ASSERT_TRUE(attached);
            e.scheduled = *attached;
            e.original_attached = true;
            ASSERT_TRUE(e.imports.Open(imports_, &e.context, 1));
            ASSERT_TRUE(e.sources.Open({&e.buffers, 1, {i + 1, i + 101}, SourceCapacity(i)}));
            ASSERT_TRUE(e.handshake_sources.Open(e.sources));
            ASSERT_TRUE(e.io.Open(e.scheduler, e.receive, e.sources));
            e.binding = {.pool = &e.pool,
                         .rx = &e.buffers,
                         .ledger = &e.ledger,
                         .on_tx = &runtime::ConnectionIoBinding::TxCallback,
                         .on_rx = &runtime::ConnectionIoBinding::RxCallback,
                         .cookie = &e.io,
                         .on_error = &runtime::ConnectionIoBinding::ErrorCallback,
                         .external_refill = true};
        }
        std::array<runtime::RawContextBinding, 2> contexts;
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            contexts[i] = {.context = &e.context,
                           .queues = std::span(&e.binding, 1),
                           .connections = &e.scheduler,
                           .control = &e.imports};
        }
        ASSERT_TRUE(shard_.Open(contexts, {.reactor = {.budget = 8}, .control = {.workers = 2, .capacity = 4}}));
        shard_open_ = true;
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].connection.state() == ConnectionState::kPreparing &&
                   endpoints_[1].connection.state() == ConnectionState::kPreparing;
        }));
    }
    RawConnectionPreparation Preparation(std::size_t i) {
        auto& e = endpoints_[i];
        RawConnectionPreparation preparation{.handshake = {.imports = &imports_,
                                                           .local_context = &e.context,
                                                           .local_context_incarnation = 1,
                                                           .descriptor_compatible = true,
                                                           .credits = &e.credits,
                                                           .credit_peer = e.credit_peer,
                                                           .reorder = {.window = 4, .max_resident_bytes = 16384}},
                                             .receive = &e.receive,
                                             .sender = &e.sender,
                                             .slab = &e.slab,
                                             .sender_credits = &e.send_credits,
                                             .connection_id = e.scheduled.id,
                                             .tx_capacity = 8,
                                             .sources = &e.handshake_sources,
                                             .peer_lanes_authorized = true,
                                             .rndv = {}};
        preparation.authorize_lane_update = e.lane_updates_enabled ? &AuthorizeLaneUpdate : nullptr;
        preparation.lane_authorization_cookie = &e;
        preparation.lane_update_timeout_ns = e.lane_update_timeout_ns;
        return preparation;
    }
    void Deliver() {
        while (!packets_.empty()) {
            auto packet = std::move(packets_.front());
            packets_.pop_front();
            auto& endpoint = endpoints_[packet.to];
            ASSERT_FALSE(endpoint.posted.empty()) << "physical credit overrun";
            const auto receive = endpoint.posted.front();
            endpoint.posted.pop_front();
            ASSERT_LE(packet.bytes.size(), receive.sge.len);
            std::memcpy(reinterpret_cast<void*>(receive.sge.addr), packet.bytes.data(), packet.bytes.size());
            urma_cr_t completion{};
            completion.flag.bs.jetty = 1;
            completion.flag.bs.s_r = 1;
            completion.local_id = endpoint.jetty[0].jetty_id.id;
            completion.remote_id = packet.source;
            completion.status = URMA_CR_SUCCESS;
            completion.opcode = URMA_CR_OPC_SEND_WITH_IMM;
            completion.user_ctx = receive.id;
            completion.imm_data = packet.imm;
            completion.completion_len = packet.bytes.size();
            endpoint.completions[1].push_back(completion);
        }
    }
    bool Step() {
        // 宿主只驱动统一 owner；投递、CQE、补投、握手和导入收割全部由 RuntimeShard 负责。
        const auto advanced = shard_.Tick();
        EXPECT_TRUE(advanced);
        if (!advanced) {
            return false;
        }
        Deliver();
        return true;
    }
    template <typename Predicate>
    bool Until(Predicate predicate) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!predicate() && std::chrono::steady_clock::now() < deadline) {
            if (!Step()) {
                return false;
            }
            std::this_thread::yield();
        }
        return predicate();
    }
    void Prepare() {
        for (std::size_t i = 0; i < endpoints_.size(); ++i) {
            auto& endpoint = endpoints_[i];
            ASSERT_TRUE(endpoint.connection.PrepareRaw(Preparation(i)));
            ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
        }
    }
    void DrainInitialCredits() {
        // READY 不是本端 CREDIT 的成功 CQE；通过真正的 Runtime Tick/传递/poll 排空初始公告。
        // 连续两次空闲才建立业务计数基线，不清队列、不清 DMA 或篡改回调累计数。
        unsigned idle = 0;
        ASSERT_TRUE(Until([&] {
            const bool quiet = packets_.empty() && std::all_of(endpoints_.begin(), endpoints_.end(), [](const auto& e) {
                                   return e.slab.stats().local_dma == 0 && e.completions[0].empty() &&
                                          e.completions[1].empty() && e.receive.pending_refill() == 0;
                               });
            idle = quiet ? idle + 1 : 0;
            return idle >= 2;
        }));
        for (auto& endpoint : endpoints_) {
            endpoint.initial_accepted = endpoint.connection.tx_stats().accepted;
            endpoint.initial_retired = endpoint.connection.tx_stats().retired;
            endpoint.initial_callback_retired = endpoint.callback_tx_retired;
        }
    }
    void Activate() {
        Prepare();
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].connection.state() == ConnectionState::kRawActive &&
                   endpoints_[1].connection.state() == ConnectionState::kRawActive;
        }));
        DrainInitialCredits();
    }
    void EnableLaneUpdates() {
        for (auto& endpoint : endpoints_) {
            endpoint.lane_updates_enabled = true;
        }
    }
    // 初始 CREDIT 已真实使用 Lane 0，接下来的业务 WR 使用轮转到的 Lane 1；替换它以覆盖新来源。
    void ReplaceEmptyLane(std::size_t side, std::uint32_t index = 1) {
        auto& endpoint = endpoints_[side];
        ASSERT_EQ(endpoint.slab.stats().local_dma, 0u);
        ASSERT_TRUE(endpoint.completions[0].empty());
        const auto old_lane = endpoint.pool.lane(index);
        ASSERT_TRUE(old_lane);
        const auto old_id = endpoint.pool.lane_jetty(index)->jetty_id.id;
        // owner 未运行；已消费全部真实 TX CQE，空 SQ 再经 ERROR/边界/空 flush 后才替换。
        ASSERT_TRUE(endpoint.pool.BeginDrain(*old_lane));
        ASSERT_TRUE(endpoint.pool.ObserveFlushDone(*old_lane));
        std::array<urma_cr_t, 1> flushed{};
        const auto count = endpoint.pool.FlushSend(*old_lane, flushed);
        ASSERT_TRUE(count);
        ASSERT_EQ(*count, 0);
        const auto replacement = endpoint.pool.ReplaceDrainedLane(*old_lane, {true, true, true});
        ASSERT_TRUE(replacement);
        EXPECT_EQ(replacement->generation, old_lane->generation + 1);
        EXPECT_NE(endpoint.pool.lane_jetty(index)->jetty_id.id, old_id);
    }
    void TearDown() override {
        for (auto& endpoint : endpoints_) {
            endpoint.block_import.store(false);
            endpoint.block_import.notify_all();
        }
        if (!shard_open_) {
            return;
        }
        ASSERT_TRUE(Until([&] { return !shard_.paused(0) && !shard_.paused(1); }));
        for (auto& endpoint : endpoints_) {
            endpoint.connection.Cancel();
            if (endpoint.original_attached) {
                ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
            }
        }
        Deliver();
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].slab.stats().local_dma == 0 && endpoints_[1].slab.stats().local_dma == 0 &&
                   endpoints_[0].completions[1].empty() && endpoints_[1].completions[1].empty();
        }));
        for (auto& endpoint : endpoints_) {
            ASSERT_TRUE(endpoint.connection.Close());
            if (endpoint.original_attached) {
                ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
            }
        }
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].receive.pending_refill() == 0 && endpoints_[1].receive.pending_refill() == 0;
        }));
        for (auto& endpoint : endpoints_) {
            if (endpoint.original_attached) {
                ASSERT_TRUE(endpoint.scheduler.Detach(endpoint.scheduled));
                endpoint.original_attached = false;
            }
        }
        imports_.StopAccepting();
        ASSERT_TRUE(Until([&] { return imports_.entries() == 0; }));
        ASSERT_TRUE(shard_.Close());
        shard_open_ = false;
        for (auto& endpoint : endpoints_) {
            ASSERT_TRUE(endpoint.imports.Close());
            ASSERT_TRUE(endpoint.scheduler.Close());
            // 双端停止且已排空真实 RX/CQE 后，才允许撤销 HELLO 自动登记的来源。
            if (endpoint.handshake_sources.size() != 0) {
                ASSERT_TRUE(endpoint.handshake_sources.Retire({1, true, true, true}));
            }
            ASSERT_TRUE(endpoint.handshake_sources.Close());
            ASSERT_TRUE(endpoint.sources.Close());
            ASSERT_TRUE(endpoint.receive.Close());
            ASSERT_TRUE(endpoint.credits.RetirePeer(endpoint.credit_peer, {true, true, true}));
            ASSERT_TRUE(endpoint.credits.Close());
            ASSERT_TRUE(endpoint.send_credits.Close());
            ASSERT_TRUE(endpoint.routes.Close());
            ASSERT_TRUE(endpoint.sender.Close());
            ASSERT_TRUE(endpoint.ledger.Close());
            ASSERT_TRUE(endpoint.slab.Close());
            raw::ReceiveDomainCloser closer;
            ASSERT_TRUE(closer.Begin(endpoint.pool, endpoint.buffers));
            std::array<raw::RxBufferEvent, 64> events;
            auto done = closer.Advance({true, true, true, true}, events);
            ASSERT_TRUE(done);
            EXPECT_EQ(done->stage, raw::ReceiveDomainCloseStage::kClosed);
        }
        EXPECT_TRUE(imports_.Close());
        override_.reset();
        self = nullptr;
    }
};

class OwnedConnectionRuntimeTest : public ConnectionRuntimeTest {
protected:
    std::array<runtime::ConnectionOwner, 2> owners_;
    std::array<runtime::OwnedConnection, 2> owned_{};
    std::array<bool, 2> owner_open_{};
    std::array<bool, 2> adopted_{};

    void SetUp() override {
        ConnectionRuntimeTest::SetUp();
        if (HasFatalFailure()) {
            return;
        }
        // 基础 fixture 的连接尚未 PrepareRaw；先撤销空握手，再复用同一组真实队列与 Runtime。
        for (auto& endpoint : endpoints_) {
            ASSERT_TRUE(endpoint.connection.Close());
            ASSERT_TRUE(endpoint.scheduler.Detach(endpoint.scheduled));
            endpoint.original_attached = false;
        }
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        for (std::size_t side = 0; side < owners_.size(); ++side) {
            auto& endpoint = endpoints_[side];
            ASSERT_TRUE(owners_[side].Open(endpoint.scheduler, {.capacity = 1}));
            owner_open_[side] = true;
            auto hello = Hello(side);
            hello.identity.session += 1000;
            const auto adopted = owners_[side].AdoptProtocol(sockets[side], endpoint.routes,
                                                             {.local = hello,
                                                              .expected_peer_authorization = 7,
                                                              .compatible_peer = true,
                                                              .deadline_ns = Now() + 10'000'000'000ull},
                                                             Now());
            ASSERT_TRUE(adopted);
            owned_[side] = *adopted;
            adopted_[side] = true;
            const auto status = owners_[side].Inspect(owned_[side]);
            ASSERT_TRUE(status);
            endpoint.scheduled = status->scheduled;
        }
        ASSERT_TRUE(Until([&] {
            const auto left = owners_[0].Inspect(owned_[0]);
            const auto right = owners_[1].Inspect(owned_[1]);
            return left && right && left->state == ConnectionState::kPreparing &&
                   right->state == ConnectionState::kPreparing;
        }));
    }

    RawConnectionPreparation OwnedPreparation(std::size_t side) {
        auto preparation = Preparation(side);
        preparation.connection_id = 0;
        preparation.sources = nullptr;
        return preparation;
    }

    void ActivateOwned() {
        for (std::size_t side = 0; side < owners_.size(); ++side) {
            ASSERT_TRUE(owners_[side].PrepareRaw(owned_[side], OwnedPreparation(side), endpoints_[side].sources));
        }
        ASSERT_TRUE(Until([&] {
            const auto left = owners_[0].Inspect(owned_[0]);
            const auto right = owners_[1].Inspect(owned_[1]);
            return left && right && left->state == ConnectionState::kRawActive &&
                   right->state == ConnectionState::kRawActive;
        }));
        DrainInitialCredits();
    }

    void RetireOwned(std::size_t side) {
        const auto status = owners_[side].Inspect(owned_[side]);
        ASSERT_TRUE(status);
        ASSERT_EQ(status->state, ConnectionState::kClosed);
        if (status->retained_sources != 0) {
            ASSERT_TRUE(owners_[side].RetireSources(owned_[side], {1, true, true, true}));
        }
        ASSERT_TRUE(owners_[side].Release(owned_[side]));
        adopted_[side] = false;
    }

    void TearDown() override {
        if (shard_open_) {
            ASSERT_TRUE(Until([&] { return !shard_.paused(0) && !shard_.paused(1); }));
            for (std::size_t side = 0; side < owners_.size(); ++side) {
                if (adopted_[side]) {
                    const auto status = owners_[side].Inspect(owned_[side]);
                    ASSERT_TRUE(status);
                    if (status->state != ConnectionState::kClosed) {
                        ASSERT_TRUE(owners_[side].Cancel(owned_[side]));
                    }
                }
            }
            // 关闭请求不替代真实 CQE；保留 owner 槽到 Runtime 退休 DMA、归还并补投所有 RX。
            ASSERT_TRUE(Until([&] {
                bool closed = true;
                for (std::size_t side = 0; side < owners_.size(); ++side) {
                    if (adopted_[side]) {
                        closed &= owners_[side].CloseConnection(owned_[side]).has_value();
                    }
                }
                return closed && endpoints_[0].slab.stats().local_dma == 0 &&
                       endpoints_[1].slab.stats().local_dma == 0 && endpoints_[0].completions[1].empty() &&
                       endpoints_[1].completions[1].empty() && endpoints_[0].receive.pending_refill() == 0 &&
                       endpoints_[1].receive.pending_refill() == 0 && packets_.empty();
            }));
            for (std::size_t side = 0; side < owners_.size(); ++side) {
                if (adopted_[side]) {
                    RetireOwned(side);
                }
            }
        }
        for (std::size_t side = 0; side < owners_.size(); ++side) {
            if (owner_open_[side]) {
                ASSERT_TRUE(owners_[side].Close());
                owner_open_[side] = false;
            }
        }
        ConnectionRuntimeTest::TearDown();
    }
};

class MailboxConnectionRuntimeTest : public OwnedConnectionRuntimeTest {
protected:
    std::array<runtime::ConnectionMailbox, 2> mailboxes_;
    std::array<std::vector<runtime::ConnectionMailboxNotice>, 2> notices_;
    std::array<bool, 2> mailbox_open_{};

    void BindMailboxes(bool enabled) {
        ASSERT_TRUE(shard_.Close());
        shard_open_ = false;
        std::array<runtime::RawContextBinding, 2> contexts;
        for (std::size_t side = 0; side < contexts.size(); ++side) {
            auto& endpoint = endpoints_[side];
            contexts[side] = {.context = &endpoint.context,
                              .queues = std::span(&endpoint.binding, 1),
                              .connections = &endpoint.scheduler,
                              .control = &endpoint.imports,
                              .mailbox = enabled ? &mailboxes_[side] : nullptr};
        }
        ASSERT_TRUE(shard_.Open(contexts, {.reactor = {.budget = 8}, .control = {.workers = 2, .capacity = 4}}));
        shard_open_ = true;
    }

    void SetUp() override {
        OwnedConnectionRuntimeTest::SetUp();
        if (HasFatalFailure()) {
            return;
        }
        for (std::size_t side = 0; side < mailboxes_.size(); ++side) {
            ASSERT_TRUE(mailboxes_[side].Open(
                owners_[side], {.capacity = 16},
                [](void* cookie, const runtime::ConnectionMailboxNotice& notice) noexcept {
                    static_cast<std::vector<runtime::ConnectionMailboxNotice>*>(cookie)->push_back(notice);
                },
                &notices_[side]));
            mailbox_open_[side] = true;
            EXPECT_EQ(mailboxes_[side].exclusive_context(), &endpoints_[side].context);
        }
        BindMailboxes(true);
    }

    std::optional<runtime::ConnectionCommandCompletion>
    AwaitCompletion(std::size_t side, runtime::ConnectionCommandTicket ticket, std::span<std::byte> output = {}) {
        std::optional<runtime::ConnectionCommandCompletion> completion;
        EXPECT_TRUE(Until([&] {
            if (completion) {
                return true;
            }
            const auto result = mailboxes_[side].TakeCompletion(ticket, output);
            if (result) {
                completion = *result;
            } else {
                EXPECT_EQ(result.error().code, runtime::CommandQueueErrorCode::kPending);
            }
            return completion.has_value();
        }));
        return completion;
    }

    void TearDown() override {
        for (auto& endpoint : endpoints_) {
            endpoint.block_import.store(false);
            endpoint.block_import.notify_all();
        }
        if (shard_open_) {
            for (auto& mailbox : mailboxes_) {
                mailbox.StopAccepting();
            }
            // 测试失败也先执行并领取剩余命令；解绑 mailbox 后才能关闭其 fd 和借用的 owner。
            ASSERT_TRUE(Until([&] {
                std::array<std::byte, runtime::ConnectionCommandQueue::kMaxIoBytes> output{};
                for (std::size_t side = 0; side < mailboxes_.size(); ++side) {
                    for (const auto& notice : notices_[side]) {
                        (void)mailboxes_[side].TakeCompletion(notice.ticket, output);
                    }
                }
                return !shard_.paused(0) && !shard_.paused(1) && mailboxes_[0].outstanding() == 0 &&
                       mailboxes_[1].outstanding() == 0;
            }));
            BindMailboxes(false);
        }
        for (std::size_t side = 0; side < mailboxes_.size(); ++side) {
            if (mailbox_open_[side]) {
                ASSERT_TRUE(mailboxes_[side].Close(true));
                mailbox_open_[side] = false;
            }
        }
        OwnedConnectionRuntimeTest::TearDown();
    }
};

TEST_F(MailboxConnectionRuntimeTest, CopiedCommandsDriveRawDataRefillAndOrderedFin) {
    ActivateOwned();
    std::array<std::byte, 4096> input;
    input.fill(std::byte{0x6b});
    const auto write = mailboxes_[0].TrySubmit(
        {.kind = runtime::ConnectionCommandKind::kWrite, .connection = owned_[0], .token = 11, .bytes = input.size()},
        input);
    ASSERT_TRUE(write);
    input.fill(std::byte{0});
    const auto written = AwaitCompletion(0, *write);
    ASSERT_TRUE(written);
    ASSERT_TRUE(written->result);
    EXPECT_EQ(written->result->bytes, input.size());
    EXPECT_EQ(written->command.token, 11u);
    ASSERT_EQ(notices_[0].size(), 1u);
    EXPECT_EQ(notices_[0][0].ticket, *write);
    EXPECT_EQ(notices_[0][0].connection, owned_[0]);
    EXPECT_EQ(notices_[0][0].token, 11u);
    ASSERT_TRUE(Until([&] {
        const auto status = owners_[1].Inspect(owned_[1]);
        return status && status->rx_pending != 0;
    }));

    // Write 入队时已经复制；Read 完成区必须跨越领取时间保持数据，短读不提前归还物理 RX。
    std::array<std::byte, 4096> output{};
    const auto first = mailboxes_[1].TrySubmit(
        {.kind = runtime::ConnectionCommandKind::kRead, .connection = owned_[1], .token = 12, .bytes = 17});
    ASSERT_TRUE(first);
    const auto prefix = AwaitCompletion(1, *first, std::span(output).first(17));
    ASSERT_TRUE(prefix);
    ASSERT_TRUE(prefix->result);
    EXPECT_EQ(prefix->result->bytes, 17u);
    EXPECT_FALSE(prefix->result->eof);
    EXPECT_NE(endpoints_[1].buffers.held(), 0u);
    std::size_t received = prefix->result->bytes;
    while (received < output.size()) {
        const auto read = mailboxes_[1].TrySubmit({.kind = runtime::ConnectionCommandKind::kRead,
                                                   .connection = owned_[1],
                                                   .bytes = static_cast<std::uint32_t>(output.size() - received)});
        ASSERT_TRUE(read);
        const auto suffix = AwaitCompletion(1, *read, std::span(output).subspan(received));
        ASSERT_TRUE(suffix);
        ASSERT_TRUE(suffix->result);
        ASSERT_GT(suffix->result->bytes, 0u);
        received += suffix->result->bytes;
    }
    EXPECT_TRUE(std::all_of(output.begin(), output.end(), [](auto byte) { return byte == std::byte{0x6b}; }));
    ASSERT_TRUE(Until([&] {
        return endpoints_[1].buffers.held() == 0 && endpoints_[1].receive.pending_refill() == 0 &&
               endpoints_[0].send_credits.available(CreditClass::kData) == 4;
    }));

    const auto shutdown = mailboxes_[0].TrySubmit(
        {.kind = runtime::ConnectionCommandKind::kShutdownWrite, .connection = owned_[0], .token = 13});
    ASSERT_TRUE(shutdown);
    const auto stopped = AwaitCompletion(0, *shutdown);
    ASSERT_TRUE(stopped);
    ASSERT_TRUE(stopped->result);
    // FIN 按序到达前允许 WouldBlock，但不得以命令入队成功伪造 EOF。
    bool eof = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!eof && std::chrono::steady_clock::now() < deadline) {
        const auto read = mailboxes_[1].TrySubmit(
            {.kind = runtime::ConnectionCommandKind::kRead, .connection = owned_[1], .bytes = 1});
        ASSERT_TRUE(read);
        const auto completion = AwaitCompletion(1, *read, std::span(output).first(1));
        ASSERT_TRUE(completion);
        if (completion->result) {
            EXPECT_EQ(completion->result->bytes, 0u);
            eof = completion->result->eof;
        } else {
            ASSERT_TRUE(completion->result.error().connection);
            ASSERT_EQ(completion->result.error().connection->code, ConnectionErrorCode::kWouldBlock);
        }
    }
    EXPECT_TRUE(eof);
    for (const auto& endpoint : endpoints_) {
        EXPECT_FALSE(endpoint.callback_bad_id);
        EXPECT_FALSE(endpoint.io.first_error());
    }
}

TEST_F(MailboxConnectionRuntimeTest, ConcurrentSubmissionProgressesDuringContextImport) {
    endpoints_[0].block_import.store(true);
    for (std::size_t side = 0; side < owners_.size(); ++side) {
        ASSERT_TRUE(owners_[side].PrepareRaw(owned_[side], OwnedPreparation(side), endpoints_[side].sources));
    }
    ASSERT_TRUE(Until([&] { return endpoints_[0].importing.load() == 1; }));
    ASSERT_TRUE(Until([&] { return !shard_.paused(1) && endpoints_[1].importing.load() == 1; }));
    const auto polls_before = endpoints_[0].polls;
    std::array<std::optional<runtime::ConnectionCommandTicket>, 2> tickets;
    std::array<std::thread, 2> producers;
    // 慢导入仅阻塞对应握手；producer 提交后同 context 的 owner 仍须执行命令。
    for (std::size_t side = 0; side < producers.size(); ++side) {
        producers[side] = std::thread([&, side] {
            const auto submitted = mailboxes_[side].TrySubmit(
                {.kind = runtime::ConnectionCommandKind::kInspect, .connection = owned_[side], .token = side + 21});
            EXPECT_TRUE(submitted);
            if (submitted) {
                tickets[side] = *submitted;
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    ASSERT_TRUE(tickets[0]);
    ASSERT_TRUE(tickets[1]);
    const auto other = AwaitCompletion(1, *tickets[1]);
    ASSERT_TRUE(other);
    ASSERT_TRUE(other->result);
    ASSERT_TRUE(other->result->status);
    EXPECT_EQ(other->command.token, 22u);
    // ImportJetty 仍未返回，此时读取状态命令已经可以完成，raw CQ 也继续轮询。
    const auto resumed = AwaitCompletion(0, *tickets[0]);
    ASSERT_TRUE(resumed);
    ASSERT_TRUE(resumed->result);
    ASSERT_TRUE(resumed->result->status);
    EXPECT_EQ(resumed->command.token, 21u);
    ASSERT_EQ(notices_[0].size(), 1u);
    EXPECT_EQ(notices_[0][0].ticket, *tickets[0]);
    EXPECT_FALSE(shard_.paused(0));
    EXPECT_GT(endpoints_[0].polls, polls_before);
    endpoints_[0].block_import.store(false);
    endpoints_[0].block_import.notify_all();
    ASSERT_TRUE(Until([&] {
        const auto left = owners_[0].Inspect(owned_[0]);
        const auto right = owners_[1].Inspect(owned_[1]);
        return left && right && left->state == ConnectionState::kRawActive &&
               right->state == ConnectionState::kRawActive;
    }));
}

TEST_F(MailboxConnectionRuntimeTest, ReadRefreshesStaleBatchTimestampAfterRawReceive) {
    ActivateOwned();
    const auto batch_started = Now();
    const std::array payload{std::byte{0x42}};
    ASSERT_EQ(owners_[0].Write(owned_[0], payload), payload.size());
    ASSERT_TRUE(Until([&] {
        const auto status = owners_[1].Inspect(owned_[1]);
        return status && status->rx_pending != 0;
    }));
    const auto read =
        mailboxes_[1].TrySubmit({.kind = runtime::ConnectionCommandKind::kRead, .connection = owned_[1], .bytes = 1});
    ASSERT_TRUE(read);
    // 同一轮 RX 已使用更新的单调时间；命令执行不能把旧批次时间送入重排窗口导致连接失败。
    ASSERT_TRUE(mailboxes_[1].Progress(runtime::ProgressKind::kMailbox, 8, batch_started));
    std::array<std::byte, 1> output{};
    const auto completion = mailboxes_[1].TakeCompletion(*read, output);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->bytes, 1u);
    EXPECT_EQ(output, payload);
    const auto status = owners_[1].Inspect(owned_[1]);
    ASSERT_TRUE(status);
    EXPECT_EQ(status->state, ConnectionState::kRawActive);
    EXPECT_FALSE(status->failure);
}

TEST_F(OwnedConnectionRuntimeTest, OwnedRawConnectionsUseRuntimeForBidirectionalDataRefillAndFin) {
    ActivateOwned();
    EXPECT_EQ(shard_.stats()->controls_completed, 2u);
    std::array<std::vector<std::byte>, 2> payloads{std::vector<std::byte>(4096, std::byte{0x31}),
                                                   std::vector<std::byte>(4096, std::byte{0x72})};
    for (std::size_t side = 0; side < owners_.size(); ++side) {
        const auto peer = owners_[side].Peer(owned_[side]);
        ASSERT_TRUE(peer);
        EXPECT_EQ((*peer)->identity.runtime, 2 - side);
        ASSERT_EQ(owners_[side].Write(owned_[side], payloads[side]), payloads[side].size());
        std::fill(payloads[side].begin(), payloads[side].end(), std::byte{0});
    }
    // 所有入口由 owner 自动 Wake；应用无需接触底层 Connection 或手工推进 import/CQ。
    ASSERT_TRUE(Until([&] {
        const auto left = owners_[0].Inspect(owned_[0]);
        const auto right = owners_[1].Inspect(owned_[1]);
        return left && right && left->rx_pending != 0 && right->rx_pending != 0;
    }));
    for (std::size_t side = 0; side < owners_.size(); ++side) {
        std::vector<std::byte> output(4096);
        const auto first = owners_[side].Read(owned_[side], std::span(output).first(17), Now());
        ASSERT_TRUE(first);
        ASSERT_EQ(first->bytes, 17u);
        EXPECT_NE(endpoints_[side].buffers.held(), 0u);
        std::size_t offset = first->bytes;
        ASSERT_TRUE(Until([&] {
            const auto read = owners_[side].Read(owned_[side], std::span(output).subspan(offset), Now());
            if (read) {
                offset += read->bytes;
            }
            return offset == output.size();
        }));
        const auto expected = side == 0 ? std::byte{0x72} : std::byte{0x31};
        EXPECT_TRUE(std::all_of(output.begin(), output.end(), [&](auto byte) { return byte == expected; }));
    }
    ASSERT_TRUE(Until([&] {
        return endpoints_[0].receive.pending_refill() == 0 && endpoints_[1].receive.pending_refill() == 0 &&
               endpoints_[0].send_credits.available(CreditClass::kData) == 4 &&
               endpoints_[1].send_credits.available(CreditClass::kData) == 4;
    }));
    for (std::size_t side = 0; side < owners_.size(); ++side) {
        ASSERT_TRUE(owners_[side].ShutdownWrite(owned_[side]));
    }
    std::array<bool, 2> eof{};
    ASSERT_TRUE(Until([&] {
        for (std::size_t side = 0; side < owners_.size(); ++side) {
            std::array<std::byte, 1> output{};
            const auto read = owners_[side].Read(owned_[side], output, Now());
            if (read) {
                EXPECT_EQ(read->bytes, 0u);
                eof[side] = read->eof;
            }
        }
        return eof[0] && eof[1];
    }));
    for (const auto& endpoint : endpoints_) {
        EXPECT_FALSE(endpoint.callback_bad_id);
        EXPECT_FALSE(endpoint.io.first_error());
        EXPECT_GT(endpoint.callback_tx_retired, 0u);
    }
}

TEST_F(OwnedConnectionRuntimeTest, ClosingOwnedConnectionsRetainsDmaAndLateReceiveSourcesUntilProof) {
    ActivateOwned();
    const std::array payload{std::byte{0x42}, std::byte{0x53}};
    ASSERT_EQ(owners_[0].Write(owned_[0], payload), payload.size());
    ASSERT_TRUE(shard_.Tick());
    ASSERT_EQ(endpoints_[0].slab.stats().local_dma, 1u);
    ASSERT_EQ(packets_.size(), 1u);
    auto delayed = std::move(packets_.front());
    packets_.pop_front();

    // 发送已接受但尚未 poll CQE，不能 Release 槽或删除调度身份；远端尚无 RX，因此可先 Close。
    const auto sender_close = owners_[0].CloseConnection(owned_[0]);
    ASSERT_FALSE(sender_close);
    EXPECT_EQ(sender_close.error().code, runtime::ConnectionOwnerErrorCode::kRetained);
    ASSERT_FALSE(owners_[0].Release(owned_[0]));
    EXPECT_EQ(endpoints_[0].scheduler.stats()->attached, 1u);
    ASSERT_TRUE(owners_[1].CloseConnection(owned_[1]));
    const auto receiver_status = owners_[1].Inspect(owned_[1]);
    ASSERT_TRUE(receiver_status);
    EXPECT_EQ(receiver_status->state, ConnectionState::kClosed);
    EXPECT_EQ(receiver_status->retained_sources, 2u);
    EXPECT_FALSE(owners_[1].Release(owned_[1]));
    EXPECT_FALSE(owners_[1].RetireSources(owned_[1], {1, true, true, false}));
    EXPECT_EQ(endpoints_[1].sources.size(), 2u);
    ASSERT_TRUE(Until([&] { return endpoints_[0].slab.stats().local_dma == 0; }));
    EXPECT_EQ(endpoints_[0].callback_tx_retired, endpoints_[0].initial_callback_retired + 1);
    EXPECT_EQ(endpoints_[0].callback_closing_tx, 1u);
    ASSERT_TRUE(owners_[0].CloseConnection(owned_[0]));

    // 控制 fd 和逻辑路由已关闭，延迟到达的真实 RX 仍归原 Peer 结账，而不是 unknown 冻结。
    packets_.push_back(std::move(delayed));
    Deliver();
    ASSERT_TRUE(
        Until([&] { return endpoints_[1].completions[1].empty() && endpoints_[1].receive.pending_refill() == 0; }));
    EXPECT_EQ(endpoints_[1].io.stats().stale_route_drops, 1u);
    EXPECT_FALSE(endpoints_[1].io.first_error());
    EXPECT_FALSE(endpoints_[1].receive.frozen());
    EXPECT_EQ(endpoints_[1].buffers.held(), 0u);
    EXPECT_EQ(endpoints_[1].sources.size(), 2u);
    for (std::size_t side = 0; side < owners_.size(); ++side) {
        RetireOwned(side);
        EXPECT_EQ(owners_[side].stats().occupied, 0u);
        EXPECT_EQ(endpoints_[side].scheduler.stats()->attached, 0u);
        EXPECT_EQ(endpoints_[side].sources.size(), 0u);
        EXPECT_FALSE(owners_[side].Inspect(owned_[side]));
    }
}

TEST_F(OwnedConnectionRuntimeTest, RawPreparationRejectsInjectedBindingsAndUnauthorizedSources) {
    auto preparation = OwnedPreparation(0);
    preparation.connection_id = endpoints_[0].scheduled.id;
    auto rejected = owners_[0].PrepareRaw(owned_[0], preparation, endpoints_[0].sources);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, runtime::ConnectionOwnerErrorCode::kInvalidArgument);
    preparation.connection_id = 0;
    preparation.sources = &endpoints_[0].handshake_sources;
    rejected = owners_[0].PrepareRaw(owned_[0], preparation, endpoints_[0].sources);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, runtime::ConnectionOwnerErrorCode::kInvalidArgument);
    EXPECT_EQ(endpoints_[0].sources.size(), 0u);

    // 来源授权仍由宿主提供；自动接线不能把存在 owner handle 当作远端公告已经获准。
    preparation.sources = nullptr;
    preparation.peer_lanes_authorized = false;
    rejected = owners_[0].PrepareRaw(owned_[0], preparation, endpoints_[0].sources);
    ASSERT_FALSE(rejected);
    ASSERT_TRUE(rejected.error().connection);
    EXPECT_EQ(rejected.error().connection->code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(rejected.error().connection->detail, static_cast<int>(control::RawSourceAdmissionError::kUnauthorized));
    const auto status = owners_[0].Inspect(owned_[0]);
    ASSERT_TRUE(status);
    EXPECT_EQ(status->state, ConnectionState::kPreparing);
    EXPECT_EQ(status->retained_sources, 0u);
    EXPECT_EQ(status->tx.accepted, 0u);
    EXPECT_TRUE(packets_.empty());

    // 授权属于 Prepare 前置条件，尚未发布 READY；纠正后须复用已经打开的来源适配器正常建链。
    preparation.peer_lanes_authorized = true;
    ASSERT_TRUE(owners_[0].PrepareRaw(owned_[0], preparation, endpoints_[0].sources));
    ASSERT_TRUE(owners_[1].PrepareRaw(owned_[1], OwnedPreparation(1), endpoints_[1].sources));
    ASSERT_TRUE(Until([&] {
        const auto left = owners_[0].Inspect(owned_[0]);
        const auto right = owners_[1].Inspect(owned_[1]);
        return left && right && left->state == ConnectionState::kRawActive &&
               right->state == ConnectionState::kRawActive;
    }));
    EXPECT_EQ(endpoints_[0].sources.size(), 2u);
    EXPECT_EQ(endpoints_[1].sources.size(), 2u);
}

TEST_F(ConnectionRuntimeTest, FinOnlyReportsReadOpportunityThenKnownEofWithoutRepeatedReadinessChanges) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    ASSERT_TRUE(Until([&] { return sender.callback_readiness.writable && receiver.callback_readiness.writable; }));
    EXPECT_FALSE(receiver.callback_readiness.readable);
    ASSERT_TRUE(sender.connection.ShutdownWrite());
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    // FIN 没有业务字节，仍须通知应用消费它；首次提示不能提前宣称已经读到了 EOF。
    ASSERT_TRUE(Until([&] { return receiver.callback_readiness.readable; }));
    EXPECT_TRUE(sender.callback_readiness.write_closed);
    EXPECT_FALSE(sender.callback_readiness.writable);
    EXPECT_FALSE(receiver.callback_readiness.read_eof);
    EXPECT_EQ(receiver.connection.rx_pending(), 1u);
    std::array<std::byte, 1> output;
    const auto read = receiver.connection.Read(output, Now());
    ASSERT_TRUE(read);
    EXPECT_TRUE(read->eof);
    ASSERT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
    ASSERT_TRUE(Until([&] { return receiver.callback_readiness.read_eof; }));
    EXPECT_TRUE(receiver.callback_readiness.readable);
    const auto changes = receiver.callback_readiness_changes;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(Step());
    }
    EXPECT_EQ(receiver.callback_readiness_changes, changes);
}

TEST_F(ConnectionRuntimeTest, SharedSlabReturnChangesReadinessWithoutOwnTxCompletion) {
    Activate();
    auto& endpoint = endpoints_[0];
    ASSERT_TRUE(Until([&] { return endpoint.callback_readiness.writable; }));
    std::array<memory::BufferLease, 16> external{};
    std::size_t held = 0;
    while (endpoint.slab.stats().available) {
        const auto lease = endpoint.slab.Acquire(1);
        ASSERT_TRUE(lease);
        external[held++] = *lease;
    }
    ASSERT_TRUE(endpoint.scheduler.RequestSweep());
    ASSERT_TRUE(Until([&] { return !endpoint.callback_readiness.writable; }));
    const auto tx_retired = endpoint.callback_tx_retired;
    const auto callbacks = endpoint.callback_count;
    ASSERT_GT(held, 0u);
    ASSERT_TRUE(endpoint.slab.ReleaseCpu(external[--held]));
    // 资源由另一个 Slab 使用者归还，周期巡检仍需发出恢复通知，不能只依赖本连接 CQE。
    ASSERT_TRUE(Until([&] { return endpoint.callback_readiness.writable; }));
    EXPECT_EQ(endpoint.callback_count, callbacks + 1);
    EXPECT_EQ(endpoint.callback_tx_retired, tx_retired);
    const auto recovered = endpoint.callback_count;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(endpoint.scheduler.RequestSweep());
        ASSERT_TRUE(Step());
    }
    EXPECT_EQ(endpoint.callback_count, recovered);
    while (held) {
        ASSERT_TRUE(endpoint.slab.ReleaseCpu(external[--held]));
    }
}

TEST_F(ConnectionRuntimeTest, HelloAutomaticallyAdmitsBothActiveLanesBeforeReady) {
    for (const auto& endpoint : endpoints_) {
        EXPECT_EQ(endpoint.sources.size(), 0u);
        EXPECT_EQ(endpoint.handshake_sources.size(), 0u);
    }
    Prepare();
    // 准备阶段原子安装整份 HELLO 快照，尚未发布 READY 时已可识别两个主动来源。
    for (const auto& endpoint : endpoints_) {
        EXPECT_EQ(endpoint.sources.size(), 2u);
        EXPECT_EQ(endpoint.handshake_sources.size(), 2u);
        EXPECT_EQ(endpoint.connection.handshake().local_ready(), nullptr);
    }
    ASSERT_TRUE(Until([&] {
        return endpoints_[0].connection.state() == ConnectionState::kRawActive &&
               endpoints_[1].connection.state() == ConnectionState::kRawActive;
    }));
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    // 两次独立投递触发真实 TxSender 的 Lane 轮转，CQE 携带实际 Jetty 来源，不能靠手工路由兜底。
    for (unsigned round = 0; round < 2; ++round) {
        const std::array<std::byte, 16> input{std::byte(round + 1)};
        ASSERT_TRUE(sender.connection.Write(input));
        ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
        ASSERT_TRUE(Until([&] { return receiver.connection.rx_pending() != 0; }));
        std::array<std::byte, 16> output{};
        const auto read = receiver.connection.Read(output, Now());
        ASSERT_TRUE(read);
        ASSERT_EQ(read->bytes, input.size());
        EXPECT_EQ(output, input);
        ASSERT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
        ASSERT_TRUE(Until([&] { return sender.slab.stats().local_dma == 0; }));
    }
    EXPECT_EQ(sender.posts_by_jetty[0], 0u);
    EXPECT_GT(sender.posts_by_jetty[1], 0u);
    EXPECT_GT(sender.posts_by_jetty[2], 0u);
    EXPECT_FALSE(receiver.io.first_error());
    EXPECT_EQ(receiver.io.stats().failures, 0u);
}

TEST_F(ConnectionRuntimeTest, BatchTimestampCannotRegressAfterReceiveOrApplicationRead) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const auto batch_started = Now();
    const std::array payload{std::byte{0x42}};
    ASSERT_EQ(sender.connection.Write(payload), payload.size());
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(Until([&] { return receiver.connection.rx_pending() != 0; }));
    std::array<std::byte, 1> output{};
    ASSERT_TRUE(receiver.connection.Read(output, Now()));
    EXPECT_EQ(output, payload);
    // 批次开始之后已发生真实 RX/Read；强制复用旧批次时间，不能让 CheckTimeout 误报倒退。
    ASSERT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
    ASSERT_TRUE(receiver.scheduler.Progress(runtime::ProgressKind::kMailbox, 8, batch_started));
    EXPECT_EQ(receiver.connection.state(), ConnectionState::kRawActive);
    EXPECT_FALSE(receiver.connection.failure());
}

class ConstrainedSourceRuntimeTest : public ConnectionRuntimeTest {
protected:
    std::uint32_t SourceCapacity(std::size_t side) const override {
        return side == 0 ? 1 : 8;
    }
};

TEST_F(ConstrainedSourceRuntimeTest, InsufficientRegistryCapacityRejectsWholeHelloBeforeReady) {
    auto& endpoint = endpoints_[0];
    // 远端两条 Lane 而本地仅一个空位；失败不能留下第一条来源或发布半准备的 READY。
    auto prepared = endpoint.connection.PrepareRaw(Preparation(0));
    ASSERT_FALSE(prepared);
    EXPECT_EQ(prepared.error().code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(endpoint.connection.state(), ConnectionState::kFailed);
    EXPECT_EQ(endpoint.connection.handshake().local_ready(), nullptr);
    EXPECT_EQ(endpoint.sources.size(), 0u);
    EXPECT_EQ(endpoint.handshake_sources.size(), 0u);
    EXPECT_EQ(endpoint.connection.tx_stats().accepted, endpoint.initial_accepted);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRuntimeTest, ReplacingActiveLaneRejectsQueuedWriteUnderOldHello) {
    Activate();
    auto& endpoint = endpoints_[0];
    const std::array<std::byte, 16> payload{};
    ASSERT_TRUE(endpoint.connection.Write(payload));
    ASSERT_TRUE(packets_.empty());
    const auto old_lane = endpoint.pool.lane(0);
    ASSERT_TRUE(old_lane);
    const auto old_jetty_id = endpoint.pool.lane_jetty(0)->jetty_id.id;
    // 业务 WR 尚未提交且初始 CREDIT 已终结；真实完成 ERROR/边界/空 flush/替换以重建空 SQ。
    ASSERT_TRUE(endpoint.pool.BeginDrain(*old_lane));
    ASSERT_TRUE(endpoint.pool.ObserveFlushDone(*old_lane));
    std::array<urma_cr_t, 1> completions{};
    auto flushed = endpoint.pool.FlushSend(*old_lane, completions);
    ASSERT_TRUE(flushed);
    ASSERT_EQ(*flushed, 0);
    const auto replacement = endpoint.pool.ReplaceDrainedLane(*old_lane, {true, true, true});
    ASSERT_TRUE(replacement);
    EXPECT_NE(endpoint.pool.lane_jetty(0)->jetty_id.id, old_jetty_id);
    EXPECT_EQ(replacement->generation, old_lane->generation + 1);

    // 即使另一个 Lane 仍 READY，也不能绕过旧 HELLO 的完整快照校验继续发送。
    auto progressed = endpoint.connection.Progress(Now());
    ASSERT_FALSE(progressed);
    EXPECT_EQ(progressed.error().code, ConnectionErrorCode::kLaneChanged);
    EXPECT_EQ(endpoint.connection.state(), ConnectionState::kFailed);
    EXPECT_EQ(endpoint.connection.tx_stats().accepted, endpoint.initial_accepted);
    EXPECT_EQ(endpoint.slab.stats().local_dma, 0u);
    EXPECT_TRUE(packets_.empty());
    EXPECT_EQ(endpoint.sources.size(), 2u);
}

TEST_F(ConnectionRuntimeTest, RebuiltLaneWaitsForPeerAckBeforeSubmittingQueuedData) {
    EnableLaneUpdates();
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const auto passive = sender.pool.rx_target_jetty_id();
    ASSERT_TRUE(passive);
    ReplaceEmptyLane(0);
    const std::array payload{std::byte{0x31}, std::byte{0x72}};
    ASSERT_EQ(sender.connection.Write(payload), payload.size());

    // 只推进发送方，接收方尚未读公告/安装来源/返回 ACK；业务数据必须始终留在本地队列。
    for (unsigned i = 0; i < 4; ++i) {
        ASSERT_TRUE(sender.connection.Progress(Now()));
    }
    EXPECT_TRUE(sender.connection.lane_update_pending());
    EXPECT_EQ(sender.connection.local_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.tx_stats().accepted, sender.initial_accepted);
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
    EXPECT_EQ(receiver.sources.size(), 2u);
    EXPECT_TRUE(packets_.empty());
    // 对端已安装新来源并写出 ACK，但本端尚未消费确认；也不能提前放行注册 Slab 的业务 WR。
    for (unsigned i = 0; i < 4; ++i) {
        ASSERT_TRUE(receiver.connection.OnControlReady());
        ASSERT_TRUE(receiver.connection.Progress(Now()));
    }
    EXPECT_EQ(receiver.connection.peer_lane_update_id(), 1u);
    EXPECT_EQ(receiver.sources.size(), 3u);
    EXPECT_EQ(sender.connection.local_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.tx_stats().accepted, sender.initial_accepted);
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
    ASSERT_TRUE(Until([&] {
        return sender.connection.local_lane_update_id() == 1 && receiver.connection.peer_lane_update_id() == 1 &&
               receiver.connection.rx_pending() != 0;
    }));
    EXPECT_FALSE(sender.connection.lane_update_pending());
    EXPECT_EQ(receiver.sources.size(), 3u);
    EXPECT_EQ(receiver.lane_authorizations, 1u);
    EXPECT_GT(sender.posts_by_jetty[3], 0u);
    EXPECT_EQ(sender.pool.rx_target_jetty_id()->id, passive->id);
    std::array<std::byte, 2> output{};
    const auto read = receiver.connection.Read(output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, output.size());
    EXPECT_EQ(output, payload);
    EXPECT_FALSE(receiver.io.first_error());
}

TEST_F(ConnectionRuntimeTest, SimultaneousLaneUpdatesCompleteInBothDirections) {
    EnableLaneUpdates();
    Activate();
    ReplaceEmptyLane(0);
    ReplaceEmptyLane(1);
    const std::array payloads{std::array{std::byte{0x51}}, std::array{std::byte{0x62}}};
    for (std::size_t side = 0; side < endpoints_.size(); ++side) {
        auto& endpoint = endpoints_[side];
        ASSERT_EQ(endpoint.connection.Write(payloads[side]), 1u);
        ASSERT_TRUE(endpoint.connection.Progress(Now()));
        EXPECT_TRUE(endpoint.connection.lane_update_pending());
        EXPECT_EQ(endpoint.connection.tx_stats().accepted, endpoint.initial_accepted);
        ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
    }
    // 双向都处于等待 ACK 状态时也须处理对端 UPDATE，不能让单发送缓冲造成互相等待。
    ASSERT_TRUE(Until([&] {
        return std::all_of(endpoints_.begin(), endpoints_.end(), [](const auto& endpoint) {
            return endpoint.connection.local_lane_update_id() == 1 && endpoint.connection.peer_lane_update_id() == 1 &&
                   endpoint.connection.rx_pending() != 0;
        });
    }));
    for (std::size_t side = 0; side < endpoints_.size(); ++side) {
        auto& endpoint = endpoints_[side];
        EXPECT_EQ(endpoint.connection.state(), ConnectionState::kRawActive);
        EXPECT_FALSE(endpoint.connection.lane_update_pending());
        EXPECT_EQ(endpoint.sources.size(), 3u);
        EXPECT_EQ(endpoint.lane_authorizations, 1u);
        EXPECT_GT(endpoint.posts_by_jetty[3], 0u);
        std::array<std::byte, 1> output{};
        ASSERT_TRUE(endpoint.connection.Read(output, Now()));
        EXPECT_EQ(output, payloads[1 - side]);
        EXPECT_FALSE(endpoint.io.first_error());
    }
}

TEST_F(ConnectionRuntimeTest, LaneAckDoesNotRetireOldSourceWithDelayedReceive) {
    EnableLaneUpdates();
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const std::array payload{std::byte{0x3b}};
    ASSERT_EQ(sender.connection.Write(payload), payload.size());
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(shard_.Tick());
    ASSERT_EQ(packets_.size(), 1u);
    auto delayed = std::move(packets_.front());
    packets_.pop_front();
    ASSERT_EQ(delayed.source.id, sender.pool.lane_jetty(1)->jetty_id.id);
    // 本地 TX 先终结，但远端 RX 尚未交付；这只能证明本地替换安全，不能撤销旧来源。
    ASSERT_TRUE(Until([&] { return sender.slab.stats().local_dma == 0; }));
    ReplaceEmptyLane(0);
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(Until([&] { return sender.connection.local_lane_update_id() == 1; }));
    EXPECT_EQ(receiver.sources.size(), 3u);
    EXPECT_FALSE(receiver.handshake_sources.Retire({1, true, true, false}));
    EXPECT_EQ(receiver.sources.size(), 3u);
    packets_.push_back(std::move(delayed));
    Deliver();
    ASSERT_TRUE(Until([&] { return receiver.connection.rx_pending() != 0; }));
    std::array<std::byte, 1> output{};
    ASSERT_TRUE(receiver.connection.Read(output, Now()));
    EXPECT_EQ(output, payload);
    EXPECT_FALSE(receiver.io.first_error());
    EXPECT_FALSE(receiver.receive.frozen());
    EXPECT_EQ(receiver.sources.size(), 3u);
}

TEST_F(ConnectionRuntimeTest, RejectedLanePolicyNeverAcknowledgesOrPostsFromNewSource) {
    EnableLaneUpdates();
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    receiver.reject_lane_update = true;
    ReplaceEmptyLane(0);
    const std::array payload{std::byte{0x41}};
    ASSERT_TRUE(sender.connection.Write(payload));
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(Until([&] {
        return sender.connection.state() == ConnectionState::kFailed &&
               receiver.connection.state() == ConnectionState::kFailed;
    }));
    ASSERT_TRUE(receiver.connection.failure());
    EXPECT_EQ(receiver.connection.failure()->code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(receiver.lane_authorizations, 1u);
    EXPECT_EQ(receiver.connection.peer_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.local_lane_update_id(), 0u);
    EXPECT_EQ(receiver.sources.size(), 2u);
    EXPECT_EQ(sender.connection.tx_stats().accepted, sender.initial_accepted);
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
    EXPECT_TRUE(packets_.empty());
}

class FullSourceRuntimeTest : public ConnectionRuntimeTest {
protected:
    std::uint32_t SourceCapacity(std::size_t side) const override {
        return side == 1 ? 2 : 8;
    }
};

TEST_F(FullSourceRuntimeTest, FailedOnlineSourceInstallCannotAcknowledgeReplacement) {
    EnableLaneUpdates();
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    ASSERT_EQ(receiver.sources.size(), 2u);
    ReplaceEmptyLane(0);
    const std::array payload{std::byte{0x19}};
    ASSERT_TRUE(sender.connection.Write(payload));
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    // 初始两个来源已占满表，更新不能提前删除仍可能有迟到 RX 的旧键腾出空间。
    ASSERT_TRUE(Until([&] {
        return sender.connection.state() == ConnectionState::kFailed &&
               receiver.connection.state() == ConnectionState::kFailed;
    }));
    ASSERT_TRUE(receiver.connection.failure());
    EXPECT_EQ(receiver.connection.failure()->code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(receiver.lane_authorizations, 1u);
    EXPECT_EQ(receiver.sources.size(), 2u);
    EXPECT_EQ(receiver.connection.peer_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.local_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.tx_stats().accepted, sender.initial_accepted);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRuntimeTest, ControlCloseWhileWaitingForLaneAckKeepsSourcesAndStopsData) {
    EnableLaneUpdates();
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    ReplaceEmptyLane(0);
    const std::array payload{std::byte{0x41}};
    ASSERT_TRUE(sender.connection.Write(payload));
    ASSERT_TRUE(sender.connection.Progress(Now()));
    ASSERT_TRUE(sender.connection.lane_update_pending());
    // 对端未处理 UPDATE 即关闭控制连接，EOF 不能当作 ACK 或远端静默证明。
    receiver.connection.Cancel();
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(Until([&] { return sender.connection.state() == ConnectionState::kFailed; }));
    ASSERT_TRUE(sender.connection.failure());
    EXPECT_EQ(sender.connection.failure()->code, ConnectionErrorCode::kControlClosed);
    EXPECT_EQ(sender.connection.local_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.tx_stats().accepted, sender.initial_accepted);
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
    EXPECT_EQ(sender.sources.size(), 2u);
    EXPECT_EQ(receiver.sources.size(), 2u);
    EXPECT_FALSE(receiver.handshake_sources.Close());
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRuntimeTest, UnacknowledgedLaneUpdateHasBoundedDeadline) {
    EnableLaneUpdates();
    endpoints_[0].lane_update_timeout_ns = 1'000'000;
    Activate();
    auto& sender = endpoints_[0];
    ReplaceEmptyLane(0);
    const std::array payload{std::byte{0x77}};
    ASSERT_TRUE(sender.connection.Write(payload));
    const auto start = Now();
    ASSERT_TRUE(sender.connection.Progress(start));
    ASSERT_TRUE(sender.connection.lane_update_pending());
    // 仅推进本端并使用确定的截止时间，无需睡眠或依赖对端处理速度。
    auto timed_out = sender.connection.Progress(start + sender.lane_update_timeout_ns + 1);
    ASSERT_FALSE(timed_out);
    EXPECT_EQ(timed_out.error().code, ConnectionErrorCode::kLaneUpdateTimeout);
    EXPECT_EQ(sender.connection.state(), ConnectionState::kFailed);
    EXPECT_EQ(sender.connection.local_lane_update_id(), 0u);
    EXPECT_EQ(sender.connection.tx_stats().accepted, sender.initial_accepted);
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
    EXPECT_EQ(endpoints_[1].sources.size(), 2u);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRuntimeTest, CancelRetainsHelloSourcesUntilQueuedReceiveHasSettled) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const std::array<std::byte, 16> input{};
    ASSERT_TRUE(sender.connection.Write(input));
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    ASSERT_TRUE(shard_.Tick());
    Deliver();
    ASSERT_EQ(receiver.completions[1].size(), 1u);
    ASSERT_EQ(sender.slab.stats().local_dma, 1u);

    // RX 已被硬件接受但 owner 尚未消费；撤逻辑路由必须保留来源表以完成旧 Peer 的物理账。
    receiver.connection.Cancel();
    ASSERT_TRUE(receiver.connection.Close());
    EXPECT_EQ(receiver.sources.size(), 2u);
    auto early_close = receiver.handshake_sources.Close();
    ASSERT_FALSE(early_close);
    EXPECT_EQ(early_close.error(), control::RawSourceAdmissionError::kRetained);
    auto early_retire = receiver.handshake_sources.Retire({1, true, true, false});
    EXPECT_FALSE(early_retire);
    EXPECT_EQ(receiver.sources.size(), 2u);
    ASSERT_TRUE(Until([&] { return receiver.completions[1].empty() && sender.slab.stats().local_dma == 0; }));
    EXPECT_EQ(receiver.io.stats().stale_route_drops, 1u);
    EXPECT_FALSE(receiver.io.first_error());
    EXPECT_EQ(receiver.io.stats().failures, 0u);
    EXPECT_FALSE(receiver.receive.frozen());
    EXPECT_EQ(receiver.buffers.held(), 0u);
    EXPECT_EQ(receiver.sources.size(), 2u);
}

TEST_F(ConnectionRuntimeTest, RuntimeOwnsHandshakeImportDataCreditAndOrderedFin) {
    Activate();
    EXPECT_EQ(shard_.stats()->controls_completed, 2u);
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    std::vector<std::byte> input(4 * 4096, std::byte{0x73});
    ASSERT_EQ(sender.connection.Write(input), input.size());
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    // 原用户缓冲在 Write 后即可修改；接收端验证来自注册 Slab 的独立副本。
    std::fill(input.begin(), input.end(), std::byte{0});
    ASSERT_TRUE(Until([&] { return receiver.buffers.held() == 4 && receiver.callback_rx_pending == 4; }));
    EXPECT_GT(receiver.callback_rx_changes, 0u);
    EXPECT_FALSE(receiver.callback_bad_id);
    EXPECT_EQ(sender.send_credits.available(CreditClass::kData), 0u);
    std::array<std::byte, 17> short_read{};
    auto first = receiver.connection.Read(short_read, Now());
    ASSERT_TRUE(first);
    EXPECT_EQ(first->bytes, short_read.size());
    EXPECT_EQ(receiver.buffers.held(), 4u);
    ASSERT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
    std::vector<std::byte> output(input.size() - short_read.size());
    std::size_t offset = 0;
    while (offset < output.size()) {
        auto read = receiver.connection.Read(std::span(output).subspan(offset), Now());
        ASSERT_TRUE(read);
        ASSERT_GT(read->bytes, 0u);
        offset += read->bytes;
    }
    EXPECT_TRUE(std::all_of(output.begin(), output.end(), [](auto byte) { return byte == std::byte{0x73}; }));
    EXPECT_EQ(receiver.receive.pending_refill(), 4u);
    ASSERT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
    ASSERT_TRUE(Until([&] { return sender.send_credits.available(CreditClass::kData) == 4; }));
    EXPECT_EQ(receiver.receive.pending_refill(), 0u);
    EXPECT_FALSE(receiver.io.first_error());

    ASSERT_TRUE(sender.connection.ShutdownWrite());
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    bool eof = false;
    ASSERT_TRUE(Until([&] {
        std::array<std::byte, 1> byte{};
        auto read = receiver.connection.Read(byte, Now());
        if (read) {
            eof = read->eof;
            EXPECT_EQ(read->bytes, 0u);
            EXPECT_TRUE(receiver.scheduler.Wake(receiver.scheduled));
        } else {
            EXPECT_EQ(read.error().code, ConnectionErrorCode::kWouldBlock);
        }
        return eof;
    }));
    EXPECT_TRUE(eof);
    EXPECT_FALSE(sender.io.first_error());
}

TEST_F(ConnectionRuntimeTest, CancelKeepsAcceptedDmaRoutableUntilRuntimeConsumesCompletion) {
    Activate();
    auto& sender = endpoints_[0];
    const std::array<std::byte, 16> input{};
    ASSERT_TRUE(sender.connection.Write(input));
    ASSERT_TRUE(sender.scheduler.Wake(sender.scheduled));
    // 第一次 Tick 在 mailbox 阶段提交 WR，TX poll 阶段已过，因此 DMA 引用仍真实在途。
    ASSERT_TRUE(shard_.Tick());
    ASSERT_EQ(sender.slab.stats().local_dma, 1u);
    ASSERT_FALSE(sender.completions[0].empty());
    const auto completion = sender.completions[0].front();
    auto record = sender.ledger.LookupPosted(completion.user_ctx);
    ASSERT_TRUE(record);
    raw::TxCompletionEvent valid{.kind = raw::TxCompletionKind::kCompleted,
                                 .completion = completion,
                                 .lane = record->ticket.lane,
                                 .record = *record,
                                 .error = {}};
    sender.connection.Cancel();
    // 已取消的流仍须拒绝错误代次，不能把 existing failure 当作忽略非法完成的理由。
    auto malformed = valid;
    ++malformed.record->metadata.operation_generation;
    auto rejected = sender.scheduler.OnTxCompletion(malformed);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, runtime::ProgressErrorCode::kRetained);
    EXPECT_EQ(sender.slab.stats().local_dma, 1u);
    EXPECT_EQ(sender.callback_tx_retired, sender.initial_callback_retired);
    auto closed = sender.connection.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, ConnectionErrorCode::kRetained);
    EXPECT_FALSE(sender.scheduler.Detach(sender.scheduled));
    Deliver();
    ASSERT_TRUE(Until([&] { return sender.slab.stats().local_dma == 0; }));
    EXPECT_EQ(sender.connection.tx_stats().retired, sender.initial_retired + 1);
    EXPECT_EQ(sender.callback_tx_retired, sender.initial_callback_retired + 1);
    EXPECT_EQ(sender.callback_closing_tx, 1u);
    EXPECT_FALSE(sender.io.first_error());
    auto duplicate = sender.scheduler.OnTxCompletion(valid);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, runtime::ProgressErrorCode::kRetained);
    EXPECT_EQ(sender.connection.tx_stats().retired, sender.initial_retired + 1);
    ASSERT_TRUE(Step());
    EXPECT_EQ(sender.callback_tx_retired, sender.initial_callback_retired + 1);
    EXPECT_EQ(sender.callback_closing_tx, 1u);
    EXPECT_FALSE(sender.callback_bad_id);
    ASSERT_TRUE(sender.connection.Close());
}

TEST_F(ConnectionRuntimeTest, StoppedRawReceiveFreezesCreditsAndFailsConnectionWithoutEndingTxDma) {
    Activate();
    auto& endpoint = endpoints_[0];
    const std::array<std::byte, 16> payload{};
    ASSERT_TRUE(endpoint.connection.Write(payload));
    ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
    ASSERT_TRUE(shard_.Tick());
    ASSERT_EQ(endpoint.slab.stats().local_dma, 1u);
    ASSERT_EQ(endpoint.completions[0].size(), 1u);
    const auto delayed = endpoint.completions[0].front();
    endpoint.completions[0].pop_front();
    Deliver();

    // raw 的接收域停用必须同步冻结授信；错误回调与连接失败均不能代替尚未到达的 TX CQE。
    endpoint.buffers.Stop();
    runtime::ConnectionIoBinding::ErrorCallback(&endpoint.io, {runtime::ProgressErrorCode::kProvider, EIO});
    EXPECT_TRUE(endpoint.credits.stats().frozen);
    EXPECT_TRUE(endpoint.receive.frozen());
    EXPECT_EQ(endpoint.slab.stats().local_dma, 1u);
    ASSERT_TRUE(Until([&] { return endpoint.connection.state() == ConnectionState::kFailed; }));
    ASSERT_TRUE(endpoint.connection.failure());
    EXPECT_EQ(endpoint.connection.failure()->code, ConnectionErrorCode::kCredit);
    EXPECT_EQ(endpoint.slab.stats().local_dma, 1u);
    EXPECT_EQ(endpoint.callback_tx_retired, endpoint.initial_callback_retired);
    ASSERT_TRUE(endpoint.io.first_error());
    ASSERT_TRUE(endpoint.io.first_error()->progress);
    EXPECT_EQ(endpoint.io.first_error()->progress->detail, EIO);

    // Failed 状态不变时也必须向宿主报告新终结，宿主才有机会重试保留中的关闭。
    endpoint.completions[0].push_back(delayed);
    ASSERT_TRUE(Until([&] { return endpoint.slab.stats().local_dma == 0; }));
    EXPECT_EQ(endpoint.callback_tx_retired, endpoint.initial_callback_retired + 1);
    EXPECT_EQ(endpoint.callback_failed_tx, 1u);
    EXPECT_FALSE(endpoint.callback_bad_id);
}

TEST_F(ConnectionRuntimeTest, ImportControlKeepsRawAndConnectionsRunning) {
    endpoints_[0].block_import.store(true);
    Prepare();
    ASSERT_TRUE(Until([&] { return endpoints_[0].importing.load() == 1; }));
    const auto raw_before = endpoints_[0].polls;
    const auto other_before = endpoints_[1].polls;
    ASSERT_TRUE(Until([&] { return !shard_.paused(1) && endpoints_[1].importing.load() == 1; }));
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(Step());
    }
    EXPECT_GT(endpoints_[0].polls, raw_before);
    EXPECT_FALSE(shard_.paused(0));
    EXPECT_NE(endpoints_[0].connection.state(), ConnectionState::kRawActive);
    EXPECT_GT(endpoints_[1].polls, other_before);
    // raw 一直推进；只有等待该 Import 的握手须等 owner 收割后才能激活。
    endpoints_[0].block_import.store(false);
    endpoints_[0].block_import.notify_all();
    ASSERT_TRUE(Until([&] {
        return endpoints_[0].connection.state() == ConnectionState::kRawActive &&
               endpoints_[1].connection.state() == ConnectionState::kRawActive;
    }));
    EXPECT_GT(endpoints_[0].polls, raw_before);
}

} // namespace
} // namespace connection
} // namespace kbsocket
