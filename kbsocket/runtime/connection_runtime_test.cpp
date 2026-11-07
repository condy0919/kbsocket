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
#include "kbsocket/runtime/connection_io.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
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
        std::array<urma_jetty_t, 2> jetty{};
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
        runtime::ConnectionIoBinding io;
        runtime::ScheduledConnection scheduled;
        runtime::RxSourceLease source_lease;
        runtime::RawQueueBinding binding;
        std::uint64_t polls = 0;
        std::uint64_t callback_tx_retired = 0, callback_closing_tx = 0, callback_failed_tx = 0;
        std::uint64_t callback_rx_changes = 0;
        std::size_t callback_rx_pending = 0;
        bool callback_bad_id = false;
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
    void SetUp() override {
        self = this;
        override_.emplace(Functions());
        ASSERT_TRUE(imports_.Open({.entries = 4, .leases = 128, .in_flight = 2}));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            e.context.async_fd = -1;
            raw::JettyPoolConfig config;
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
            ASSERT_TRUE(e.imports.Open(imports_, &e.context, 1));
            ASSERT_TRUE(e.sources.Open({&e.buffers, 1, {i + 1, i + 101}, 8}));
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
            // 独立可信测试拓扑注册主动发送 Lane；帧 token 不能代替 CQE 的实际来源。
            auto source = e.sources.Register(
                {.local_id = e.jetty[0].jetty_id.id,
                 .remote_id = endpoints_[1 - i].jetty[1].jetty_id,
                 .token = e.connection.handshake().local().identity.token,
                 .source = {{i + 1, i + 101}, {kWireVersion, 2 - i, i + 101, i + 201}, e.credit_peer}});
            ASSERT_TRUE(source);
            e.source_lease = *source;
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
        return {.handshake = {.imports = &imports_,
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
                .tx_capacity = 8};
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
    void Activate() {
        Prepare();
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].connection.state() == ConnectionState::kRawActive &&
                   endpoints_[1].connection.state() == ConnectionState::kRawActive;
        }));
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
            ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
        }
        Deliver();
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].slab.stats().local_dma == 0 && endpoints_[1].slab.stats().local_dma == 0 &&
                   endpoints_[0].completions[1].empty() && endpoints_[1].completions[1].empty();
        }));
        for (auto& endpoint : endpoints_) {
            ASSERT_TRUE(endpoint.connection.Close());
            ASSERT_TRUE(endpoint.scheduler.Wake(endpoint.scheduled));
        }
        ASSERT_TRUE(Until([&] {
            return endpoints_[0].receive.pending_refill() == 0 && endpoints_[1].receive.pending_refill() == 0;
        }));
        for (auto& endpoint : endpoints_) {
            ASSERT_TRUE(endpoint.scheduler.Detach(endpoint.scheduled));
        }
        imports_.StopAccepting();
        ASSERT_TRUE(Until([&] { return imports_.entries() == 0; }));
        ASSERT_TRUE(shard_.Close());
        shard_open_ = false;
        for (auto& endpoint : endpoints_) {
            ASSERT_TRUE(endpoint.imports.Close());
            ASSERT_TRUE(endpoint.scheduler.Close());
            ASSERT_TRUE(endpoint.sources.Remove(endpoint.source_lease, {1, true, true, true}));
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
                                 .record = *record};
    sender.connection.Cancel();
    // 已取消的流仍须拒绝错误代次，不能把 existing failure 当作忽略非法完成的理由。
    auto malformed = valid;
    ++malformed.record->metadata.operation_generation;
    auto rejected = sender.scheduler.OnTxCompletion(malformed);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, runtime::ProgressErrorCode::kRetained);
    EXPECT_EQ(sender.slab.stats().local_dma, 1u);
    EXPECT_EQ(sender.callback_tx_retired, 0u);
    auto closed = sender.connection.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, ConnectionErrorCode::kRetained);
    EXPECT_FALSE(sender.scheduler.Detach(sender.scheduled));
    Deliver();
    ASSERT_TRUE(Until([&] { return sender.slab.stats().local_dma == 0; }));
    EXPECT_EQ(sender.connection.tx_stats().retired, 1u);
    EXPECT_EQ(sender.callback_tx_retired, 1u);
    EXPECT_EQ(sender.callback_closing_tx, 1u);
    EXPECT_FALSE(sender.io.first_error());
    auto duplicate = sender.scheduler.OnTxCompletion(valid);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, runtime::ProgressErrorCode::kRetained);
    EXPECT_EQ(sender.connection.tx_stats().retired, 1u);
    ASSERT_TRUE(Step());
    EXPECT_EQ(sender.callback_tx_retired, 1u);
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
    EXPECT_EQ(endpoint.callback_tx_retired, 0u);
    ASSERT_TRUE(endpoint.io.first_error());
    ASSERT_TRUE(endpoint.io.first_error()->progress);
    EXPECT_EQ(endpoint.io.first_error()->progress->detail, EIO);

    // Failed 状态不变时也必须向宿主报告新终结，宿主才有机会重试保留中的关闭。
    endpoint.completions[0].push_back(delayed);
    ASSERT_TRUE(Until([&] { return endpoint.slab.stats().local_dma == 0; }));
    EXPECT_EQ(endpoint.callback_tx_retired, 1u);
    EXPECT_EQ(endpoint.callback_failed_tx, 1u);
    EXPECT_FALSE(endpoint.callback_bad_id);
}

TEST_F(ConnectionRuntimeTest, ImportControlPausesRawAndConnectionsOnlyForItsContext) {
    endpoints_[0].block_import.store(true);
    Prepare();
    ASSERT_TRUE(Until([&] { return shard_.paused(0) && endpoints_[0].importing.load() == 1; }));
    const auto raw_before = endpoints_[0].polls;
    const auto connection_before = endpoints_[0].scheduler.stats()->visits;
    const auto other_before = endpoints_[1].polls;
    ASSERT_TRUE(Until([&] { return !shard_.paused(1) && endpoints_[1].importing.load() == 1; }));
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(Step());
    }
    EXPECT_EQ(endpoints_[0].polls, raw_before);
    EXPECT_EQ(endpoints_[0].scheduler.stats()->visits, connection_before);
    EXPECT_GT(endpoints_[1].polls, other_before);
    // 阻塞的 Import 返回并由 owner 收割后，原 context 恢复握手和数据推进。
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
