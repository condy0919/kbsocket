// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <expected>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/control/rndv_arena.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"
#include "kbsocket/control/tx_lane_snapshot.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace connection {
namespace {
class ConnectionRndvTest : public ::testing::Test {
protected:
    // 这里隔离验证 Connection 的准入时序；真实来源登记及 CQE 查表在 Runtime 双端测试覆盖。
    struct Admission : control::RawSourceAdmission {
        bool ready = false;
        bool reject = false;
        unsigned prepares = 0;
        std::expected<void, control::RawSourceAdmissionError>
        Prepare(const control::RawSourceAdmissionRequest& request) noexcept override {
            ++prepares;
            if (reject || !request.peer_lanes_authorized || !request.peer || !request.peer->tx_lanes.count) {
                return std::unexpected(control::RawSourceAdmissionError::kUnauthorized);
            }
            ready = true;
            return {};
        }
        std::expected<void, control::RawSourceAdmissionError> Check() const noexcept override {
            if (!ready) {
                return std::unexpected(control::RawSourceAdmissionError::kNotReady);
            }
            return {};
        }
    };
    struct Receive {
        std::uint64_t id;
        urma_sge_t sge;
    };
    struct Packet {
        std::size_t to;
        std::uint64_t imm;
        std::vector<std::byte> bytes;
    };
    struct Endpoint {
        urma_context_t context{};
        std::array<urma_jfc_t, 2> cq{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetty{};
        std::array<urma_target_seg_t, 3> segments{};
        urma_target_seg_t imported_segment{};
        urma_target_jetty_t target{};
        std::size_t cq_created = 0, jetty_created = 0, registered = 0;
        raw::JettyPool pool;
        raw::RxBufferPool buffers;
        raw::AttemptLedger ledger;
        raw::TxSender sender;
        memory::SlabAllocator slab;
        memory::SlabAllocator target_slab;
        std::size_t writes = 0;
        bool hold_tx = false;
        ReceiveCreditDomain credits;
        SenderCreditAccount send_credits;
        CreditPeerHandle credit_peer;
        ConnectionTable routes;
        EagerReceiveDomain receive;
        Connection connection;
        runtime::ConnectionProgress progress;
        runtime::ScheduledConnection scheduled;
        std::uint32_t completion_kinds = 0;
        Admission admission;
        std::deque<Receive> posted;
        std::array<std::deque<urma_cr_t>, 2> completions;
    };
    static inline ConnectionRndvTest* self;
    std::array<Endpoint, 2> endpoints_;
    std::deque<Packet> packets_;
    memory::MemoryBudget budget_{8 * 1024 * 1024};
    control::EndpointImportCache imports_;
    control::TsegImportCache segments_;
    RndvConnectionConfig rndv_{.tx_capacity = 2,
                               .receive_capacity = 8,
                               .notification_capacity = 16,
                               .ingress_packets = 8,
                               .max_chunks = 8,
                               .post_budget = 8,
                               .chunk_bytes = 4096,
                               .threshold = 8192,
                               .timeout_ns = 10000000000,
                               .progress_budget = 8};
    runtime::ControlExecutor executor_;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    std::uint64_t now_ = 0;
    bool use_scheduler_ = false;
    static Endpoint& Find(urma_context_t* context) {
        return context == &self->endpoints_[0].context ? self->endpoints_[0] : self->endpoints_[1];
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
            seg->seg.ubva.eid = context->eid;
            seg->seg.attr.bs.access = config->flag.bs.access;
            seg->seg.token_id = static_cast<std::uint32_t>(e.registered);
            return seg;
        };
        f.import_jetty = [](auto* context, auto* remote, auto*) {
            auto& target = Find(context).target;
            target.urma_ctx = context;
            target.id = remote->jetty_id;
            target.trans_mode = remote->trans_mode;
            target.tp_type = remote->tp_type;
            target.type = remote->type;
            return &target;
        };
        f.import_seg = [](auto* context, auto* descriptor, auto*, std::uint64_t, urma_import_seg_flag_t) {
            auto& segment = Find(context).imported_segment;
            segment.urma_ctx = context;
            segment.seg = *descriptor;
            return &segment;
        };
        f.unimport_seg = [](auto*) { return URMA_SUCCESS; };
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
                if (wr->opcode == URMA_OPC_WRITE) {
                    // 真正写入 Grant 指向的注册 backing；不能仅伪造成功 CQE 绕过可见性验证。
                    EXPECT_EQ(wr->rw.dst.num_sge, 1u);
                    std::size_t offset = 0;
                    for (std::uint32_t i = 0; i < wr->rw.src.num_sge; ++i) {
                        const auto& source = wr->rw.src.sge[i];
                        std::memcpy(reinterpret_cast<void*>(wr->rw.dst.sge[0].addr + offset),
                                    reinterpret_cast<const void*>(source.addr), source.len);
                        offset += source.len;
                    }
                    EXPECT_EQ(offset, wr->rw.dst.sge[0].len);
                    ++e.writes;
                } else {
                    EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
                    Packet packet{wr->tjetty->id.id >= 110 ? 1u : 0u, wr->send.imm_data, {}};
                    for (std::uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
                        const auto& sge = wr->send.src.sge[i];
                        auto* data = reinterpret_cast<const std::byte*>(sge.addr);
                        packet.bytes.insert(packet.bytes.end(), data, data + sge.len);
                    }
                    self->packets_.push_back(std::move(packet));
                }
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
        hello.capabilities = control::kHandshakeCapabilityRaw | control::kHandshakeCapabilityWriteRndv;
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
        hello.rndv.limits = {.version = control::kRndvVersion,
                             .flags = control::kRndvSuccessfulWriteDone,
                             .max_write_bytes = 4096,
                             .max_transfer_bytes = 32768,
                             .max_grants = 2,
                             .window = 8,
                             .max_bytes = 262144};
        hello.rndv.arena_count = 1;
        const auto arena =
            control::ExportRndvArena(endpoints_[side].target_slab, &endpoints_[side].context, {side + 71, 1});
        EXPECT_TRUE(arena);
        if (arena) {
            hello.rndv.arenas[0] = *arena;
        }
        return hello;
    }
    void SetUp() override {
        self = this;
        override_.emplace(Functions());
        ASSERT_TRUE(imports_.Open({.entries = 4, .leases = 128, .in_flight = 2}));
        ASSERT_TRUE(segments_.Open({.entries = 4, .leases = 128, .in_flight = 2}));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = 2;
        cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        cap.max_write_size = 200u * 1024u * 1024u;
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            e.context.eid.raw[0] = static_cast<std::uint8_t>(i + 1);
            raw::JettyPoolConfig config;
            config.tx_depth = 64;
            config.send_sge = 2;
            config.rx_depth = 8;
            ASSERT_TRUE(e.pool.Open(&e.context, cap, config));
            ASSERT_TRUE(e.buffers.Open(e.pool));
            ASSERT_TRUE(e.buffers.Refill(8));
            ASSERT_TRUE(e.ledger.Open(e.pool, 64));
            ASSERT_TRUE(e.sender.Open(e.ledger, true));
            const std::array contexts{&e.context};
            ASSERT_TRUE(
                e.slab.Open(contexts, {.buffer_count = 32, .buffer_size = 32768, .max_references = 256}, budget_));
            ASSERT_TRUE(
                e.target_slab.Open(contexts, {.buffer_count = 8, .buffer_size = 32768, .max_references = 64}, budget_));
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
                                           .deadline_ns = now_ + 1000000000},
                                          now_, &rndv_));
            if (use_scheduler_) {
                ASSERT_TRUE(e.progress.Open({.capacity = 1, .context = &e.context}));
                const auto attached = e.progress.Attach(e.connection);
                ASSERT_TRUE(attached);
                e.scheduled = *attached;
            }
        }
        for (int n = 0; n < 100; ++n) {
            for (auto& e : endpoints_) {
                ASSERT_TRUE(e.connection.Progress(++now_));
            }
            if (endpoints_[0].connection.state() == ConnectionState::kPreparing &&
                endpoints_[1].connection.state() == ConnectionState::kPreparing) {
                return;
            }
        }
        FAIL() << "HELLO did not complete";
    }
    RawConnectionPreparation Preparation(std::size_t i) {
        auto& e = endpoints_[i];
        return {.handshake = {.imports = &imports_,
                              .local_context = &e.context,
                              .local_context_incarnation = 1,
                              .descriptor_compatible = true,
                              .credits = &e.credits,
                              .credit_peer = e.credit_peer,
                              .reorder = {.window = 8, .max_resident_bytes = 262144},
                              .rndv_peer_authorized = true},
                .receive = &e.receive,
                .sender = &e.sender,
                .slab = &e.slab,
                .sender_credits = &e.send_credits,
                .connection_id = use_scheduler_ ? e.scheduled.id : i + 1,
                .tx_capacity = 8,
                .sources = &e.admission,
                .peer_lanes_authorized = true,
                .rndv = {.target_slab = &e.target_slab, .segments = &segments_, .lane = *e.pool.lane(0)}};
    }
    template <typename Cache>
    void DriveOneCache(Cache& cache) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        do {
            const auto next = cache.NextOperation();
            ASSERT_TRUE(next) << static_cast<int>(next.error().code);
            if (*next) {
                const auto op = **next;
                ASSERT_TRUE(cache.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true}));
            }
            std::array<runtime::ControlTaskCompletion, 4> results;
            const auto count = executor_.Poll(results);
            ASSERT_TRUE(count);
            for (std::size_t j = 0; j < *count; ++j) {
                ASSERT_TRUE(cache.HandleCompletion(results[j]));
            }
            if (!*next && !cache.in_flight()) {
                return;
            }
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        FAIL() << "control task timeout";
    }
    void DriveCache() {
        // 测试串行暂停全部数据访问，两个缓存不能同时操作同一个 context。
        DriveOneCache(imports_);
        DriveOneCache(segments_);
    }
    void Prepare() {
        ASSERT_TRUE(endpoints_[0].connection.PrepareRaw(Preparation(0)));
        ASSERT_TRUE(endpoints_[1].connection.PrepareRaw(Preparation(1)));
        DriveCache();
    }
    void Activate() {
        Prepare();
        for (int n = 0; n < 100; ++n) {
            for (auto& e : endpoints_) {
                ASSERT_TRUE(e.connection.Progress(++now_));
            }
            if (endpoints_[0].connection.state() == ConnectionState::kRawActive &&
                endpoints_[1].connection.state() == ConnectionState::kRawActive) {
                return;
            }
        }
        FAIL() << "READY did not complete";
    }
    void Deliver() {
        while (!packets_.empty()) {
            auto packet = std::move(packets_.front());
            packets_.pop_front();
            auto& e = endpoints_[packet.to];
            ASSERT_FALSE(e.posted.empty()) << "physical credit overrun";
            const auto rx = e.posted.front();
            e.posted.pop_front();
            ASSERT_LE(packet.bytes.size(), rx.sge.len);
            std::memcpy(reinterpret_cast<void*>(rx.sge.addr), packet.bytes.data(), packet.bytes.size());
            urma_cr_t cr{};
            cr.flag.bs.jetty = 1;
            cr.flag.bs.s_r = 1;
            cr.local_id = e.jetty[0].jetty_id.id;
            cr.status = URMA_CR_SUCCESS;
            cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
            cr.user_ctx = rx.id;
            cr.imm_data = packet.imm;
            cr.completion_len = packet.bytes.size();
            e.completions[1].push_back(cr);
        }
    }
    void Poll(bool allow_failure = false) {
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            raw::TxCompletionProcessor processor(e.ledger, true);
            std::array<raw::TxCompletionEvent, 64> tx;
            if (!e.hold_tx) {
                auto count = processor.Poll(tx);
                ASSERT_TRUE(count);
                for (std::size_t j = 0; j < count->count; ++j) {
                    ASSERT_TRUE(tx[j].record);
                    const auto kind = tx[j].record->metadata.connection_id >> 62;
                    e.completion_kinds |= std::uint32_t{1} << kind;
                    if (use_scheduler_) {
                        const auto done = e.progress.OnTxCompletion(tx[j]);
                        if (!allow_failure) {
                            ASSERT_TRUE(done);
                        }
                    } else {
                        const auto done = e.connection.OnTxCompletion(tx[j]);
                        if (!allow_failure) {
                            ASSERT_TRUE(done);
                        }
                    }
                }
            }
            std::array<raw::RxBufferEvent, 64> rx;
            auto received = e.buffers.Poll(rx);
            ASSERT_TRUE(received);
            for (std::size_t j = 0; j < *received; ++j) {
                // 来源由测试传输连线确认，与包内 token 无关，避免拿预期连接反推来源。
                EagerReceiveSource source{{i + 1, i + 101}, {kWireVersion, 2 - i, i + 101, i + 201}, e.credit_peer};
                auto done = e.receive.Dispatch(rx[j], source, Connection::DispatchRx, ++now_);
                if (!allow_failure) {
                    ASSERT_TRUE(done) << static_cast<int>(done.error().code);
                }
            }
            ASSERT_TRUE(e.receive.Refill());
        }
    }
    void Step() {
        DriveCache();
        for (auto& e : endpoints_) {
            if (use_scheduler_) {
                ASSERT_TRUE(e.progress.Wake(e.scheduled));
                ASSERT_TRUE(e.progress.Progress(runtime::ProgressKind::kMailbox, 8, ++now_));
                ASSERT_NE(e.connection.state(), ConnectionState::kFailed);
            } else {
                const auto progress = e.connection.Progress(++now_);
                ASSERT_TRUE(progress) << static_cast<int>(progress.error().code) << ":" << progress.error().detail;
            }
        }
        Deliver();
        Poll();
    }
    void Pump(std::size_t turns = 40) {
        for (std::size_t n = 0; n < turns; ++n) {
            Step();
            if (HasFatalFailure()) {
                return;
            }
        }
    }
    void ExpectRead(std::size_t receiver, std::span<const std::byte> expected) {
        std::vector<std::byte> output(expected.size());
        std::size_t offset = 0;
        for (int n = 0; n < 100 && offset < output.size(); ++n) {
            const auto read = endpoints_[receiver].connection.Read(std::span(output).subspan(offset), ++now_);
            if (read) {
                offset += read->bytes;
            } else {
                ASSERT_EQ(read.error().code, ConnectionErrorCode::kWouldBlock);
            }
            Step();
        }
        ASSERT_EQ(offset, expected.size());
        EXPECT_TRUE(std::equal(output.begin(), output.end(), expected.begin(), expected.end()));
    }
    bool HasPacket(WireControlType type) const {
        return std::any_of(packets_.begin(), packets_.end(), [type](const Packet& packet) {
            const auto decoded = DecodeWireFrame(packet.imm, packet.bytes);
            return decoded && decoded->frame.type == WireFrameType::kControl && decoded->frame.control == type;
        });
    }
    void TearDown() override {
        for (auto& e : endpoints_) {
            e.hold_tx = false;
        }
        // 测试宿主先停止双方发送，再消费真实已排队完成；不是用 Close 伪造 DMA 终结。
        for (auto& e : endpoints_) {
            e.connection.Cancel();
        }
        Deliver();
        Poll(true);
        for (auto& e : endpoints_) {
            for (int n = 0; n < 10; ++n) {
                (void)e.connection.Progress(++now_);
                if (e.connection.Close()) {
                    break;
                }
            }
            EXPECT_TRUE(e.connection.Close());
            if (use_scheduler_) {
                EXPECT_TRUE(e.progress.Detach(e.scheduled));
                EXPECT_TRUE(e.progress.Close());
            }
            EXPECT_TRUE(e.receive.Refill());
            EXPECT_TRUE(e.receive.Close());
            EXPECT_TRUE(e.credits.RetirePeer(e.credit_peer, {true, true, true}));
            EXPECT_TRUE(e.credits.Close());
            EXPECT_TRUE(e.send_credits.Close());
            EXPECT_TRUE(e.routes.Close());
            EXPECT_TRUE(e.sender.Close());
            EXPECT_TRUE(e.ledger.Close());
            EXPECT_TRUE(e.slab.Close());
            EXPECT_TRUE(e.target_slab.Close());
            raw::ReceiveDomainCloser closer;
            ASSERT_TRUE(closer.Begin(e.pool, e.buffers));
            std::array<raw::RxBufferEvent, 64> events;
            auto done = closer.Advance({true, true, true, true}, events);
            ASSERT_TRUE(done);
            EXPECT_EQ(done->stage, raw::ReceiveDomainCloseStage::kClosed);
        }
        auto closed = imports_.Close();
        if (!closed) {
            DriveOneCache(imports_);
        }
        EXPECT_TRUE(imports_.Close());
        if (!segments_.Close()) {
            DriveOneCache(segments_);
        }
        EXPECT_TRUE(segments_.Close());
        EXPECT_TRUE(executor_.Close());
        override_.reset();
        self = nullptr;
    }
};

// 双端都走公开 Connection 接口，通知、授权、真实 WRITE 与应用消费必须组成完整闭环。
TEST_F(ConnectionRndvTest, BidirectionalLargeCopiesDetachCallerMemoryAndDeliverExactBytes) {
    Activate();
    std::array<std::vector<std::byte>, 2> input{std::vector<std::byte>(16391), std::vector<std::byte>(32768)};
    for (std::size_t side = 0; side < 2; ++side) {
        for (std::size_t n = 0; n < input[side].size(); ++n) {
            input[side][n] = std::byte((n * 13 + side * 37) % 251);
        }
        auto temporary = input[side];
        ASSERT_EQ(endpoints_[side].connection.Write(temporary), temporary.size());
        std::fill(temporary.begin(), temporary.end(), std::byte{0xff});
    }
    Pump();
    ExpectRead(1, input[0]);
    ExpectRead(0, input[1]);
    EXPECT_EQ(endpoints_[0].writes, 5u);
    EXPECT_EQ(endpoints_[1].writes, 8u);
    EXPECT_EQ(endpoints_[0].target_slab.stats().remote_grants, 0u);
    EXPECT_EQ(endpoints_[1].target_slab.stats().remote_grants, 0u);
}

TEST_F(ConnectionRndvTest, BidirectionalMixedCopyAndLeaseWritesAdvanceBeyondSequenceWindow) {
    Activate();
    std::array<std::uint32_t, 2> submitted{};
    std::array<std::vector<std::byte>, 2> expected, received;
    for (int turn = 0; turn < 2000; ++turn) {
        for (std::size_t side = 0; side < 2; ++side) {
            auto& endpoint = endpoints_[side];
            if (submitted[side] < 24) {
                const auto index = submitted[side];
                const std::size_t bytes = index % 3 == 0 ? 31 : 16384;
                const std::vector<std::byte> input(bytes, std::byte(side * 61 + index));
                // 交替 copy 和 Lease，跨过八帧窗口；失败重试不能偷偷消费 SN 或留存源 CPU 引用。
                std::expected<std::size_t, ConnectionError> accepted =
                    std::unexpected(ConnectionError{ConnectionErrorCode::kWouldBlock});
                if (index % 2 == 0) {
                    accepted = endpoint.connection.Write(input);
                } else {
                    const auto lease = endpoint.slab.Copy(input);
                    ASSERT_TRUE(lease);
                    accepted = endpoint.connection.WriteLease(*lease, 0, input.size());
                    ASSERT_TRUE(endpoint.slab.ReleaseCpu(*lease));
                }
                if (accepted) {
                    ASSERT_EQ(*accepted, input.size());
                    expected[1 - side].insert(expected[1 - side].end(), input.begin(), input.end());
                    ++submitted[side];
                } else {
                    ASSERT_EQ(accepted.error().code, ConnectionErrorCode::kWouldBlock);
                }
            }
            for (int read_attempt = 0; read_attempt < 4; ++read_attempt) {
                std::array<std::byte, 4093> output;
                const auto read = endpoint.connection.Read(output, ++now_);
                if (!read) {
                    ASSERT_EQ(read.error().code, ConnectionErrorCode::kWouldBlock);
                    break;
                }
                received[side].insert(received[side].end(), output.begin(), output.begin() + read->bytes);
            }
        }
        Step();
        if (submitted[0] == 24 && submitted[1] == 24 && received == expected) {
            break;
        }
    }
    EXPECT_EQ(submitted[0], 24u);
    EXPECT_EQ(submitted[1], 24u);
    for (std::size_t side = 0; side < 2; ++side) {
        const auto& endpoint = endpoints_[side];
        EXPECT_EQ(received[side].size(), expected[side].size())
            << "side=" << side << " rx_pending=" << endpoint.connection.rx_pending()
            << " control=" << endpoint.send_credits.available(CreditClass::kControl)
            << " promised=" << endpoint.send_credits.stats().promised_control
            << " control_limit=" << endpoint.send_credits.stats().limits[1]
            << " control_spent=" << endpoint.send_credits.stats().spent[1]
            << " queued=" << endpoint.connection.tx_stats().queued
            << " accepted=" << endpoint.connection.tx_stats().accepted
            << " retired=" << endpoint.connection.tx_stats().retired;
    }
    EXPECT_TRUE(received[0] == expected[0]);
    EXPECT_TRUE(received[1] == expected[1]);
    Pump();
    for (auto& endpoint : endpoints_) {
        EXPECT_GT(endpoint.writes, 0u);
        EXPECT_EQ(endpoint.target_slab.stats().remote_grants, 0u);
        EXPECT_EQ(endpoint.slab.stats().local_dma, 0u);
    }
}

TEST_F(ConnectionRndvTest, SourceLeaseMayReleaseCpuBeforeRtsImportAndWriteComplete) {
    Activate();
    auto& sender = endpoints_[0];
    std::vector<std::byte> input(17001);
    for (std::size_t index = 0; index < input.size(); ++index) {
        input[index] = std::byte(index % 251);
    }
    const auto lease = sender.slab.Copy(input);
    ASSERT_TRUE(lease);
    ASSERT_EQ(sender.connection.WriteLease(*lease, 7, 16384), 16384u);
    ASSERT_TRUE(sender.slab.ReleaseCpu(*lease));
    // 在 RTS 尚未送达、远端段尚未导入时释放 CPU；保留的 DMA pin 仍须覆盖后续 WRITE。
    EXPECT_GT(sender.slab.stats().local_dma, 0u);
    EXPECT_FALSE(sender.slab.Readable(*lease));
    Pump();
    ExpectRead(1, std::span(input).subspan(7, 16384));
    EXPECT_EQ(sender.writes, 4u);
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
}

TEST_F(ConnectionRndvTest, ReadLeasePinsRndvTargetUntilExactApplicationRelease) {
    Activate();
    auto& receiver = endpoints_[1];
    const auto initial = receiver.target_slab.stats().available;
    const std::vector<std::byte> input(16384, std::byte{0x31});
    ASSERT_EQ(endpoints_[0].connection.Write(input), input.size());
    Pump();
    const auto lease = receiver.connection.ReadLease(input.size(), ++now_);
    ASSERT_TRUE(lease);
    ASSERT_EQ(lease->data.size(), input.size());
    EXPECT_TRUE(std::equal(lease->data.begin(), lease->data.end(), input.begin(), input.end()));
    EXPECT_EQ(receiver.target_slab.stats().available, initial - 1);
    // 错误归还代次不能偷走应用借用；合法归还才推进公共序列窗口并释放目标 backing。
    EXPECT_FALSE(receiver.connection.ReleaseReadLease(lease->generation + 1));
    EXPECT_EQ(receiver.target_slab.stats().available, initial - 1);
    ASSERT_TRUE(receiver.connection.ReleaseReadLease(lease->generation));
    EXPECT_EQ(receiver.target_slab.stats().available, initial);
    EXPECT_FALSE(receiver.connection.ReleaseReadLease(lease->generation));
    Pump();
}

TEST_F(ConnectionRndvTest, EagerRndvAndFinShareOneOrderedStream) {
    Activate();
    auto& endpoint = endpoints_[0];
    auto& sender = endpoint.connection;
    const std::vector<std::byte> prefix(17, std::byte{0x11});
    const std::vector<std::byte> large(16384, std::byte{0x22});
    const std::array tail{std::byte{0x33}, std::byte{0x44}};
    ASSERT_EQ(sender.Write(prefix), prefix.size());
    // 已排队 Eager 尚未取得 SN 时，不能让后到 RTS 抢占公共序列。
    const auto blocked = sender.Write(large);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, ConnectionErrorCode::kWouldBlock);
    ASSERT_TRUE(sender.Progress(++now_));
    ASSERT_EQ(sender.Write(large), large.size());
    const auto overtaking = sender.Write(tail);
    ASSERT_FALSE(overtaking);
    EXPECT_EQ(overtaking.error().code, ConnectionErrorCode::kWouldBlock);
    endpoint.hold_tx = true;
    for (int n = 0; n < 40 && endpoint.writes == 0; ++n) {
        Step();
    }
    ASSERT_EQ(endpoint.writes, 4u);
    // RTS 已占序号后允许后续 Eager/FIN；刻意扣住 WRITE CQE，使它们先于 DONE 到达。
    ASSERT_EQ(sender.Write(tail), tail.size());
    ASSERT_TRUE(sender.ShutdownWrite());
    Pump();
    ExpectRead(1, prefix);
    std::array<std::byte, 1> output;
    const auto hole = endpoints_[1].connection.Read(output, ++now_);
    ASSERT_FALSE(hole);
    EXPECT_EQ(hole.error().code, ConnectionErrorCode::kWouldBlock);
    EXPECT_FALSE(endpoints_[1].connection.readiness().read_eof);
    endpoint.hold_tx = false;
    Poll();
    Pump();
    auto expected = large;
    expected.insert(expected.end(), tail.begin(), tail.end());
    ExpectRead(1, expected);
    const auto eof = endpoints_[1].connection.Read(output, ++now_);
    ASSERT_TRUE(eof);
    EXPECT_EQ(eof->bytes, 0u);
    EXPECT_TRUE(eof->eof);
}

TEST_F(ConnectionRndvTest, RemoteFinDoesNotDisableReverseRndvGrantAndDone) {
    Activate();
    ASSERT_TRUE(endpoints_[0].connection.ShutdownWrite());
    Pump();
    std::array<std::byte, 1> output;
    const auto eof = endpoints_[1].connection.Read(output, ++now_);
    ASSERT_TRUE(eof);
    ASSERT_TRUE(eof->eof);
    // 本地接收方向 EOF 后仍能发起反向 WRITE；返回的 GRANT 不属于已终止的数据方向。
    const std::vector<std::byte> input(16384, std::byte{0x68});
    const auto source = endpoints_[1].slab.Copy(input);
    ASSERT_TRUE(source);
    ASSERT_EQ(endpoints_[1].connection.WriteLease(*source, 0, input.size()), input.size());
    ASSERT_TRUE(endpoints_[1].slab.ReleaseCpu(*source));
    Pump();
    ExpectRead(0, input);
    const auto still_eof = endpoints_[1].connection.Read(output, ++now_);
    ASSERT_TRUE(still_eof);
    EXPECT_TRUE(still_eof->eof);
    EXPECT_EQ(endpoints_[1].writes, 4u);
}

TEST_F(ConnectionRndvTest, PublishedRouteAcceptsEarlyRtsBeforeLocalReady) {
    Prepare();
    std::optional<std::size_t> active;
    for (int n = 0; n < 100 && !active; ++n) {
        for (std::size_t i = 0; i < 2; ++i) {
            ASSERT_TRUE(endpoints_[i].connection.Progress(++now_));
            if (endpoints_[i].connection.state() == ConnectionState::kRawActive &&
                endpoints_[1 - i].connection.state() != ConnectionState::kRawActive) {
                active = i;
                break;
            }
        }
    }
    ASSERT_TRUE(active);
    auto& sender = endpoints_[*active];
    auto& receiver = endpoints_[1 - *active];
    ASSERT_EQ(receiver.connection.state(), ConnectionState::kWaitingReady);
    ASSERT_EQ(receiver.routes.published(), 1u);
    const std::vector<std::byte> input(16384, std::byte{0x27});
    ASSERT_EQ(sender.connection.Write(input), input.size());
    ASSERT_TRUE(sender.connection.Progress(++now_));
    ASSERT_TRUE(HasPacket(WireControlType::kRndvRts));
    // READY 已发布而本地尚未激活时仍须接住 RTS；应用 API 在激活前继续拒绝 Read。
    Deliver();
    Poll();
    EXPECT_EQ(receiver.connection.state(), ConnectionState::kWaitingReady);
    std::array<std::byte, 1> output;
    const auto early = receiver.connection.Read(output, ++now_);
    ASSERT_FALSE(early);
    EXPECT_EQ(early.error().code, ConnectionErrorCode::kNotReady);
    Pump();
    ExpectRead(1 - *active, input);
}

TEST_F(ConnectionRndvTest, WriteCompletionAndDoneNotificationHaveIndependentDmaLifetimes) {
    Activate();
    auto& sender = endpoints_[0];
    const std::vector<std::byte> input(16384, std::byte{0x75});
    ASSERT_EQ(sender.connection.Write(input), input.size());
    sender.hold_tx = true;
    for (int n = 0; n < 40 && sender.writes == 0; ++n) {
        Step();
    }
    ASSERT_EQ(sender.writes, 4u);
    // WRITE 已执行但成功 CQE 尚未消费，绝不能先发布 DONE 或向应用交付目标内存。
    EXPECT_FALSE(HasPacket(WireControlType::kRndvDone));
    EXPECT_GT(sender.slab.stats().local_dma, 0u);
    std::array<std::byte, 1> output;
    const auto pending = endpoints_[1].connection.Read(output, ++now_);
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error().code, ConnectionErrorCode::kWouldBlock);
    sender.hold_tx = false;
    Poll();
    for (int n = 0; n < 10 && !HasPacket(WireControlType::kRndvDone); ++n) {
        ASSERT_TRUE(sender.connection.Progress(++now_));
    }
    ASSERT_TRUE(HasPacket(WireControlType::kRndvDone));
    sender.hold_tx = true;
    Deliver();
    Poll();
    // 对端已接收 DONE 后可以消费，而发送侧通知自身的 SEND DMA 仍由独立 CQE 负责结束。
    ExpectRead(1, input);
    EXPECT_GT(sender.slab.stats().local_dma, 0u);
    sender.hold_tx = false;
    Poll();
    Pump();
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
}

TEST_F(ConnectionRndvTest, CancelAfterPublishedGrantKeepsRouteUntilLateDoneWithoutDelivery) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const std::vector<std::byte> input(16384, std::byte{0x46});
    ASSERT_EQ(sender.connection.Write(input), input.size());
    for (int n = 0; n < 80 && !HasPacket(WireControlType::kRndvDone); ++n) {
        DriveCache();
        ASSERT_TRUE(sender.connection.Progress(++now_));
        ASSERT_TRUE(receiver.connection.Progress(++now_));
        if (HasPacket(WireControlType::kRndvDone)) {
            break;
        }
        Deliver();
        Poll();
    }
    ASSERT_TRUE(HasPacket(WireControlType::kRndvDone));
    ASSERT_GT(receiver.target_slab.stats().remote_grants, 0u);
    receiver.connection.Cancel();
    EXPECT_FALSE(receiver.connection.Close());
    EXPECT_EQ(receiver.routes.published(), 1u);
    EXPECT_GT(receiver.target_slab.stats().remote_grants, 0u);
    // 取消不撤销已发布远端写授权；保留路由只为迟到 DONE 终结，不得恢复应用交付。
    Deliver();
    Poll(true);
    for (int n = 0; n < 20; ++n) {
        (void)receiver.connection.Progress(++now_);
    }
    EXPECT_EQ(receiver.target_slab.stats().remote_grants, 0u);
    std::array<std::byte, 1> output;
    EXPECT_FALSE(receiver.connection.Read(output, ++now_));
    EXPECT_TRUE(receiver.connection.Close());
    EXPECT_EQ(receiver.routes.published(), 0u);
}

TEST_F(ConnectionRndvTest, MissingTargetArenaCannotPublishReady) {
    auto preparation = Preparation(0);
    preparation.rndv.target_slab = nullptr;
    const auto result = endpoints_[0].connection.PrepareRaw(preparation);
    EXPECT_FALSE(result);
    EXPECT_EQ(endpoints_[0].routes.published(), 0u);
    EXPECT_EQ(endpoints_[0].connection.handshake().local_ready(), nullptr);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRndvTest, PreparationMustMatchAdvertisedRegisteredArena) {
    auto preparation = Preparation(0);
    preparation.rndv.target_slab = &endpoints_[0].slab;
    // 同 context 的另一个合法 MR 也不能替换 HELLO 中已认证的 Arena 地址与容量。
    const auto result = endpoints_[0].connection.PrepareRaw(preparation);
    EXPECT_FALSE(result);
    EXPECT_EQ(endpoints_[0].routes.published(), 0u);
    EXPECT_EQ(endpoints_[0].connection.handshake().local_ready(), nullptr);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRndvTest, MissingSegmentCacheCannotPublishReady) {
    auto preparation = Preparation(0);
    preparation.rndv.segments = nullptr;
    EXPECT_FALSE(endpoints_[0].connection.PrepareRaw(preparation));
    EXPECT_EQ(endpoints_[0].routes.published(), 0u);
    EXPECT_EQ(endpoints_[0].connection.handshake().local_ready(), nullptr);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionRndvTest, NonnullUnopenedSegmentCacheCannotPublishReady) {
    control::TsegImportCache unopened;
    auto preparation = Preparation(0);
    preparation.rndv.segments = &unopened;
    // 非空地址不代表缓存可接受导入；不能直到 READY 后首个 GRANT 到达才发现无服务。
    EXPECT_FALSE(endpoints_[0].connection.PrepareRaw(preparation));
    EXPECT_EQ(endpoints_[0].routes.published(), 0u);
    EXPECT_EQ(endpoints_[0].connection.handshake().local_ready(), nullptr);
    EXPECT_TRUE(packets_.empty());
}

class ConnectionRndvIdleTest : public ConnectionRndvTest {
protected:
    void SetUp() override {
        rndv_.timeout_ns = 100000000;
        ConnectionRndvTest::SetUp();
    }
};

TEST_F(ConnectionRndvIdleTest, NewWriteTimeoutStartsAtSubmissionAfterLongIdle) {
    Activate();
    // 空闲时不推进 Connection；新事务不能继承 200ms 前的最后一次 Progress 时刻。
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const std::vector<std::byte> input(16384, std::byte{0x41});
    ASSERT_EQ(endpoints_[0].connection.Write(input), input.size());
    now_ += 200000000;
    Pump();
    ExpectRead(1, input);
}

class ConnectionRndvScheduledTest : public ConnectionRndvTest {
protected:
    void SetUp() override {
        use_scheduler_ = true;
        now_ = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
        ConnectionRndvTest::SetUp();
    }
};

TEST_F(ConnectionRndvScheduledTest, SchedulerRoutesRealEagerWriteAndNotificationOwnerCompletions) {
    Activate();
    const std::array small{std::byte{0x15}};
    ASSERT_EQ(endpoints_[0].connection.Write(small), small.size());
    ASSERT_EQ(endpoints_[1].connection.Write(small), small.size());
    Pump();
    ExpectRead(0, small);
    ExpectRead(1, small);
    const std::vector<std::byte> input(16384, std::byte{0x26});
    std::array<bool, 2> accepted{};
    for (int turn = 0; turn < 100 && !(accepted[0] && accepted[1]); ++turn) {
        for (std::size_t side = 0; side < 2; ++side) {
            if (accepted[side]) {
                continue;
            }
            const auto write = endpoints_[side].connection.Write(input);
            if (write) {
                ASSERT_EQ(*write, input.size());
                accepted[side] = true;
            } else {
                ASSERT_EQ(write.error().code, ConnectionErrorCode::kWouldBlock);
            }
        }
        Step();
    }
    ASSERT_TRUE(accepted[0]);
    ASSERT_TRUE(accepted[1]);
    Pump();
    ExpectRead(0, input);
    ExpectRead(1, input);
    // 三类 owner 来自真实 AttemptLedger/CQE；调度器须把子 owner 还原到精确连接代次。
    for (auto& endpoint : endpoints_) {
        EXPECT_EQ(endpoint.completion_kinds, 7u);
        EXPECT_GT(endpoint.progress.stats()->visits, 0u);
        EXPECT_EQ(endpoint.progress.stats()->failures, 0u);
        EXPECT_EQ(endpoint.slab.stats().local_dma, 0u);
    }
}

class ConnectionRndvSmallNotificationTest : public ConnectionRndvTest {
protected:
    void SetUp() override {
        rndv_.tx_capacity = 1;
        rndv_.notification_capacity = 3;
        ConnectionRndvTest::SetUp();
    }
};

TEST_F(ConnectionRndvSmallNotificationTest, ConcurrentBidirectionalWritesDoNotStarveGrantOrDone) {
    Activate();
    const std::vector<std::byte> left(16384, std::byte{0x12});
    const std::vector<std::byte> right(16384, std::byte{0x34});
    // 最小合法配额必须同时容纳本地 RTS/DONE 和反向 GRANT，不能出现双方互等通知槽。
    ASSERT_EQ(endpoints_[0].connection.Write(left), left.size());
    ASSERT_EQ(endpoints_[1].connection.Write(right), right.size());
    Pump();
    ExpectRead(1, left);
    ExpectRead(0, right);
    for (auto& endpoint : endpoints_) {
        EXPECT_EQ(endpoint.writes, 4u);
        EXPECT_EQ(endpoint.target_slab.stats().remote_grants, 0u);
    }
}

TEST(ConnectionRndvConfigurationTest, NotificationCapacityMustReserveProgressForBothDirections) {
    RndvConnectionConfig config;
    config.tx_capacity = 1;
    config.notification_capacity = 2;
    EXPECT_FALSE(RndvConnection::ValidConfig(config));
    config.notification_capacity = 3;
    EXPECT_TRUE(RndvConnection::ValidConfig(config));
}

TEST(ConnectionRndvConfigurationTest, InvalidCapacityRejectsBeforeTakingControlFd) {
    Connection connection;
    ConnectionTable table;
    ASSERT_TRUE(table.Open({1, 101}, 4));
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    control::ConnectionHandshakeConfig hello;
    hello.local.capabilities = control::kHandshakeCapabilityRaw | control::kHandshakeCapabilityWriteRndv;
    RndvConnectionConfig config;
    config.tx_capacity = 0;
    const auto rejected = connection.Open(sockets[0], table, hello, 0, &config);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, ConnectionErrorCode::kInvalidArgument);
    // 无效预算不能消耗 fd/token，也不能写出宣告 RNDV 能力的半份 HELLO。
    const std::byte marker{0x54};
    ASSERT_EQ(send(sockets[0], &marker, 1, MSG_NOSIGNAL), 1);
    std::byte received{};
    ASSERT_EQ(recv(sockets[1], &received, 1, 0), 1);
    EXPECT_EQ(received, marker);
    EXPECT_TRUE(connection.Close());
    EXPECT_EQ(close(sockets[0]), 0);
    EXPECT_EQ(close(sockets[1]), 0);
    EXPECT_TRUE(table.Close());
}

} // namespace
} // namespace connection
} // namespace kbsocket
