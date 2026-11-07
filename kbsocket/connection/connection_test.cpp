// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/connection.hpp"

#include <sys/socket.h>

#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/control/tx_lane_snapshot.hpp"
#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace connection {
namespace {
class ConnectionTest : public ::testing::Test {
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
        Admission admission;
        std::deque<Receive> posted;
        std::array<std::deque<urma_cr_t>, 2> completions;
    };
    static inline ConnectionTest* self;
    std::array<Endpoint, 2> endpoints_;
    std::deque<Packet> packets_;
    memory::MemoryBudget budget_{1024 * 1024};
    control::EndpointImportCache imports_;
    runtime::ControlExecutor executor_;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    std::uint64_t now_ = 0;
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
            return seg;
        };
        f.import_jetty = [](auto* context, auto* remote, auto*) {
            auto& target = Find(context).target;
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
                Packet packet{wr->tjetty->id.id >= 110 ? 1u : 0u, wr->send.imm_data, {}};
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
    void SetUp() override {
        self = this;
        override_.emplace(Functions());
        ASSERT_TRUE(imports_.Open({.entries = 4, .leases = 128, .in_flight = 2}));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
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
            ASSERT_TRUE(e.connection.Open(
                sockets[i], e.routes,
                {.local = Hello(i), .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000},
                now_));
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
                              .reorder = {.window = 4, .max_resident_bytes = 16384}},
                .receive = &e.receive,
                .sender = &e.sender,
                .slab = &e.slab,
                .sender_credits = &e.send_credits,
                .connection_id = i + 1,
                .tx_capacity = 8,
                .sources = &e.admission,
                .peer_lanes_authorized = true};
    }
    void DriveCache() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        do {
            auto next = imports_.NextOperation();
            ASSERT_TRUE(next);
            if (*next) {
                const auto op = **next;
                ASSERT_TRUE(imports_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true}));
            }
            std::array<runtime::ControlTaskCompletion, 4> results;
            auto count = executor_.Poll(results);
            ASSERT_TRUE(count);
            for (std::size_t j = 0; j < *count; ++j) {
                ASSERT_TRUE(imports_.HandleCompletion(results[j]));
            }
            if (!*next && !imports_.in_flight()) {
                return;
            }
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        FAIL() << "control task timeout";
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
            auto count = processor.Poll(tx);
            ASSERT_TRUE(count);
            for (std::size_t j = 0; j < count->count; ++j) {
                auto done = e.connection.OnTxCompletion(tx[j]);
                if (!allow_failure) {
                    ASSERT_TRUE(done);
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
        for (auto& e : endpoints_) {
            ASSERT_TRUE(e.connection.Progress(++now_));
        }
        Deliver();
        Poll();
    }
    void TearDown() override {
        // 测试宿主先停止双方发送，再消费真实已排队完成；不是用 Close 伪造 DMA 终结。
        for (auto& e : endpoints_) {
            e.connection.Cancel();
        }
        Deliver();
        Poll(true);
        for (auto& e : endpoints_) {
            for (int n = 0; n < 10; ++n) {
                if (e.connection.Close()) {
                    break;
                }
            }
            EXPECT_TRUE(e.connection.Close());
            EXPECT_TRUE(e.receive.Refill());
            EXPECT_TRUE(e.receive.Close());
            EXPECT_TRUE(e.credits.RetirePeer(e.credit_peer, {true, true, true}));
            EXPECT_TRUE(e.credits.Close());
            EXPECT_TRUE(e.send_credits.Close());
            EXPECT_TRUE(e.routes.Close());
            EXPECT_TRUE(e.sender.Close());
            EXPECT_TRUE(e.ledger.Close());
            EXPECT_TRUE(e.slab.Close());
            raw::ReceiveDomainCloser closer;
            ASSERT_TRUE(closer.Begin(e.pool, e.buffers));
            std::array<raw::RxBufferEvent, 64> events;
            auto done = closer.Advance({true, true, true, true}, events);
            ASSERT_TRUE(done);
            EXPECT_EQ(done->stage, raw::ReceiveDomainCloseStage::kClosed);
        }
        auto closed = imports_.Close();
        if (!closed) {
            DriveCache();
        }
        EXPECT_TRUE(imports_.Close());
        EXPECT_TRUE(executor_.Close());
        override_.reset();
        self = nullptr;
    }
};

TEST_F(ConnectionTest, GatesWritesUntilReadyAndRejectsWrongDomainBeforePublication) {
    std::array<std::byte, 1> byte{};
    auto write = endpoints_[0].connection.Write(byte);
    ASSERT_FALSE(write);
    EXPECT_EQ(write.error().code, ConnectionErrorCode::kNotReady);
    auto prep = Preparation(0);
    prep.receive = &endpoints_[1].receive;
    EXPECT_FALSE(endpoints_[0].connection.PrepareRaw(prep));
    EXPECT_EQ(endpoints_[0].routes.published(), 0u);
    Activate();
    EXPECT_TRUE(endpoints_[0].connection.Write(byte));
    Step();
}

TEST_F(ConnectionTest, RequiresAuthorizedSourceAdmissionBeforeAnyReady) {
    auto& e = endpoints_[0];
    auto prep = Preparation(0);
    prep.sources = nullptr;
    ASSERT_FALSE(e.connection.PrepareRaw(prep));
    prep = Preparation(0);
    prep.peer_lanes_authorized = false;
    auto denied = e.connection.PrepareRaw(prep);
    ASSERT_FALSE(denied);
    EXPECT_EQ(denied.error().code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(e.admission.prepares, 0u);
    EXPECT_EQ(e.routes.published(), 0u);
    EXPECT_EQ(e.connection.handshake().local_ready(), nullptr);
    // 授权尚未满足时不改变握手，可在宿主完成验证后显式重试。
    Activate();
    EXPECT_EQ(e.admission.prepares, 1u);
}

TEST_F(ConnectionTest, FailedAtomicAdmissionClosesHandshakeWithoutReadyOrPost) {
    auto& e = endpoints_[0];
    e.admission.reject = true;
    auto prepared = e.connection.PrepareRaw(Preparation(0));
    ASSERT_FALSE(prepared);
    EXPECT_EQ(prepared.error().code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(e.connection.state(), ConnectionState::kFailed);
    EXPECT_EQ(e.routes.published(), 0u);
    EXPECT_EQ(e.connection.handshake().local_ready(), nullptr);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionTest, RechecksSourceBindingsBeforeReadyPublication) {
    Prepare();
    auto& e = endpoints_[0];
    e.admission.ready = false;
    auto progressed = e.connection.Progress(++now_);
    ASSERT_FALSE(progressed);
    EXPECT_EQ(progressed.error().code, ConnectionErrorCode::kSourceAdmission);
    EXPECT_EQ(e.routes.published(), 0u);
    EXPECT_EQ(e.connection.handshake().local_ready(), nullptr);
}

TEST_F(ConnectionTest, ChangedPhysicalLaneCannotPostQueuedData) {
    Activate();
    auto& e = endpoints_[0];
    const std::array payload{std::byte{1}};
    ASSERT_EQ(e.connection.Write(payload), 1u);
    // 模拟池内实际端点身份变化，数据已复制进 Slab 但尚未提交 provider。
    ++e.jetty[1].jetty_id.id;
    auto progressed = e.connection.Progress(++now_);
    ASSERT_FALSE(progressed);
    EXPECT_EQ(progressed.error().code, ConnectionErrorCode::kLaneChanged);
    EXPECT_TRUE(packets_.empty());
    EXPECT_EQ(e.slab.stats().local_dma, 0u);
    --e.jetty[1].jetty_id.id;
}

TEST_F(ConnectionTest, ReordersRealRxAndHoldsCreditUntilFullApplicationConsumption) {
    Activate();
    auto& a = endpoints_[0];
    auto& b = endpoints_[1];
    std::vector<std::byte> input(4 * 4096);
    for (std::size_t i = 0; i < input.size(); ++i) {
        input[i] = std::byte(i / 4096 + 1);
    }
    ASSERT_EQ(a.connection.Write(input), input.size());
    std::fill(input.begin(), input.end(), std::byte{99});
    ASSERT_TRUE(a.connection.Progress(++now_));
    ASSERT_EQ(packets_.size(), 4u);
    // 真正重排线上的 SEND，CQE 与 DMA 来自同一份已注册 RX，不手工伪造应用帧。
    std::swap(packets_[0], packets_[2]);
    std::swap(packets_[1], packets_[2]);
    std::swap(packets_[2], packets_[3]);
    Deliver();
    Poll();
    EXPECT_EQ(b.buffers.held(), 4u);
    EXPECT_EQ(a.send_credits.available(CreditClass::kData), 0u);
    std::array<std::byte, 17> short_read{};
    auto first = b.connection.Read(short_read, ++now_);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->bytes, 17u);
    EXPECT_EQ(b.buffers.held(), 4u);
    EXPECT_EQ(b.receive.pending_refill(), 0u);
    std::vector<std::byte> output(input.size() - 17);
    std::size_t offset = 0;
    while (offset < output.size()) {
        auto r = b.connection.Read(std::span(output).subspan(offset), ++now_);
        ASSERT_TRUE(r);
        ASSERT_GT(r->bytes, 0u);
        offset += r->bytes;
    }
    for (std::size_t i = 0; i < output.size(); ++i) {
        EXPECT_EQ(output[i], std::byte((i + 17) / 4096 + 1));
    }
    EXPECT_EQ(b.buffers.held(), 0u);
    EXPECT_EQ(b.receive.pending_refill(), 4u);
    EXPECT_EQ(b.credits.stats().data.free, 0u);
    ASSERT_TRUE(b.receive.Refill());
    Step();
    EXPECT_EQ(a.send_credits.available(CreditClass::kData), 4u);
}

TEST_F(ConnectionTest, BidirectionalShortWritesResumeWithCreditsAndFinDoesNotStopReverseTraffic) {
    Activate();
    constexpr std::size_t size = 100003;
    std::array<std::vector<std::byte>, 2> input{std::vector<std::byte>(size, std::byte{5}),
                                                std::vector<std::byte>(size, std::byte{9})};
    std::array<std::size_t, 2> written{}, read{};
    std::array<bool, 2> shutdown{}, eof{};
    for (int n = 0; n < 2000 && !(eof[0] && eof[1]); ++n) {
        for (std::size_t i = 0; i < 2; ++i) {
            auto& c = endpoints_[i].connection;
            if (written[i] < size) {
                auto w = c.Write(std::span(input[i]).subspan(written[i]));
                if (w) {
                    written[i] += *w;
                } else {
                    ASSERT_EQ(w.error().code, ConnectionErrorCode::kWouldBlock);
                }
            }
            if (written[i] == size && !shutdown[i]) {
                ASSERT_TRUE(c.ShutdownWrite());
                shutdown[i] = true;
            }
            std::array<std::byte, 777> out{};
            auto r = c.Read(out, ++now_);
            if (r) {
                for (std::size_t j = 0; j < r->bytes; ++j) {
                    ASSERT_EQ(out[j], input[1 - i][0]);
                }
                read[i] += r->bytes;
                eof[i] = r->eof;
            } else {
                ASSERT_EQ(r.error().code, ConnectionErrorCode::kWouldBlock);
            }
        }
        Step();
    }
    EXPECT_EQ(read[0], size);
    EXPECT_EQ(read[1], size);
    EXPECT_TRUE(eof[0]);
    EXPECT_TRUE(eof[1]);
    // 结束后不会因 CREDIT 自身消费 RQE 而形成持续通知风暴。
    for (int n = 0; n < 20; ++n) {
        Step();
    }
    auto accepted = endpoints_[0].connection.tx_stats().accepted + endpoints_[1].connection.tx_stats().accepted;
    for (int n = 0; n < 20; ++n) {
        Step();
    }
    EXPECT_EQ(endpoints_[0].connection.tx_stats().accepted + endpoints_[1].connection.tx_stats().accepted, accepted);
}

TEST_F(ConnectionTest, CloseRetainsAcceptedDmaUntilCompletionAndDoesNotReplay) {
    Activate();
    auto& a = endpoints_[0];
    std::array<std::byte, 16> input{};
    ASSERT_TRUE(a.connection.Write(input));
    ASSERT_TRUE(a.connection.Progress(++now_));
    EXPECT_EQ(a.slab.stats().local_dma, 1u);
    auto closed = a.connection.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, ConnectionErrorCode::kRetained);
    EXPECT_EQ(a.slab.stats().local_dma, 1u);
    Deliver();
    Poll();
    EXPECT_EQ(a.slab.stats().local_dma, 0u);
    EXPECT_TRUE(a.connection.Close());
}

TEST_F(ConnectionTest, StatusEightFailsConnectionAndKeepsPhysicalSqReady) {
    Activate();
    auto& a = endpoints_[0];
    std::array<std::byte, 16> input{};
    ASSERT_TRUE(a.connection.Write(input));
    ASSERT_TRUE(a.connection.Progress(++now_));
    ASSERT_EQ(a.completions[0].size(), 1u);
    a.completions[0][0].status = URMA_CR_REM_ACCESS_ABORT_ERR;
    // 失败目标没有真正收到数据；Mock 丢掉尚未搬入远端 RX 的包，不消费任何 RQE。
    packets_.clear();
    Poll(true);
    EXPECT_EQ(a.connection.state(), ConnectionState::kFailed);
    EXPECT_FALSE(a.connection.Write(input));
    auto lane = a.pool.lane(0);
    ASSERT_TRUE(lane);
    EXPECT_EQ(a.pool.lane_state(*lane), raw::JettyLaneState::kReady);
    EXPECT_EQ(a.slab.stats().local_dma, 0u);
    EXPECT_EQ(a.connection.tx_stats().retired, 1u);
}

TEST_F(ConnectionTest, LastControlCreditIsReservedForQuotaReturnAndIdleDoesNotPingPong) {
    Activate();
    std::deque<Packet> delayed_controls;
    const std::array<std::byte, 1> payload{std::byte{0x37}};
    // 对端尚有数据授信时持续产生窗口更新，刻意延迟控制包交付，让双方只剩最后一个控制额度。
    // 三轮不能提前合并成一个 CREDIT_UPDATE，否则覆盖不到双向配额同时接近耗尽的场景。
    for (int round = 0; round < 3; ++round) {
        for (auto& endpoint : endpoints_) {
            ASSERT_EQ(endpoint.connection.Write(payload), 1u);
            ASSERT_TRUE(endpoint.connection.Progress(++now_));
        }
        ASSERT_EQ(packets_.size(), 2u);
        Deliver();
        Poll();
        for (auto& endpoint : endpoints_) {
            std::array<std::byte, 1> output{};
            const auto read = endpoint.connection.Read(output, ++now_);
            ASSERT_TRUE(read);
            ASSERT_EQ(read->bytes, 1u);
            ASSERT_EQ(output, payload);
            ASSERT_TRUE(endpoint.receive.Refill());
            ASSERT_TRUE(endpoint.connection.Progress(++now_));
        }
        ASSERT_EQ(packets_.size(), 2u);
        while (!packets_.empty()) {
            const auto decoded = DecodeWireFrame(packets_.front().imm, packets_.front().bytes);
            ASSERT_TRUE(decoded);
            ASSERT_EQ(decoded->frame.control, WireControlType::kCreditUpdate);
            delayed_controls.push_back(std::move(packets_.front()));
            packets_.pop_front();
        }
        Poll();
    }
    for (auto& endpoint : endpoints_) {
        EXPECT_EQ(endpoint.send_credits.available(CreditClass::kControl), 1u);
        EXPECT_EQ(endpoint.send_credits.available(CreditClass::kData), 1u);
    }

    // 第四轮仍可交付最后一个 DATA，但它引起的纯窗口更新必须等待，不能耗尽最后的控制额度。
    for (auto& endpoint : endpoints_) {
        ASSERT_EQ(endpoint.connection.Write(payload), 1u);
        ASSERT_TRUE(endpoint.connection.Progress(++now_));
    }
    ASSERT_EQ(packets_.size(), 2u);
    Deliver();
    Poll();
    for (auto& endpoint : endpoints_) {
        std::array<std::byte, 1> output{};
        const auto read = endpoint.connection.Read(output, ++now_);
        ASSERT_TRUE(read);
        ASSERT_EQ(read->bytes, 1u);
        ASSERT_TRUE(endpoint.receive.Refill());
        ASSERT_TRUE(endpoint.connection.Progress(++now_));
        EXPECT_EQ(endpoint.send_credits.available(CreditClass::kControl), 1u);
    }
    EXPECT_TRUE(packets_.empty());

    // 现在交付双方各三条旧控制包：真实 refill 后可归还控制配额，允许使用保留的最后一个额度。
    packets_.swap(delayed_controls);
    Deliver();
    Poll();
    for (auto& endpoint : endpoints_) {
        const auto credit = endpoint.credits.PeerStats(endpoint.credit_peer);
        ASSERT_TRUE(credit);
        ASSERT_EQ(credit->control_granted, 1u);
        ASSERT_TRUE(endpoint.connection.Progress(++now_));
        EXPECT_EQ(endpoint.send_credits.available(CreditClass::kControl), 0u);
    }
    ASSERT_EQ(packets_.size(), 2u);
    for (const auto& packet : packets_) {
        const auto decoded = DecodeWireFrame(packet.imm, packet.bytes);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->frame.max_control_credit_seq, 7u);
    }
    Deliver();
    Poll();
    for (auto& endpoint : endpoints_) {
        EXPECT_EQ(endpoint.send_credits.available(CreditClass::kControl), 3u);
        EXPECT_EQ(endpoint.send_credits.available(CreditClass::kData), 4u);
        ASSERT_EQ(endpoint.connection.Write(payload), 1u);
    }
    Step();
    for (auto& endpoint : endpoints_) {
        std::array<std::byte, 1> output{};
        const auto read = endpoint.connection.Read(output, ++now_);
        ASSERT_TRUE(read);
        EXPECT_EQ(read->bytes, 1u);
        EXPECT_EQ(output, payload);
        ASSERT_TRUE(endpoint.receive.Refill());
    }
    // 恢复后空闲推进必须收敛，不能因为给 CREDIT_UPDATE 返额度而无限互相发送更新。
    for (int i = 0; i < 20; ++i) {
        Step();
    }
    const auto accepted = endpoints_[0].connection.tx_stats().accepted + endpoints_[1].connection.tx_stats().accepted;
    for (int i = 0; i < 20; ++i) {
        Step();
    }
    EXPECT_EQ(endpoints_[0].connection.tx_stats().accepted + endpoints_[1].connection.tx_stats().accepted, accepted);
    EXPECT_TRUE(packets_.empty());
}

TEST_F(ConnectionTest, PublishedRouteAcceptsEarlyDataWhileLocalReadyIsStillPending) {
    Prepare();
    std::optional<std::size_t> active;
    for (int n = 0; n < 100 && !active; ++n) {
        for (std::size_t i = 0; i < endpoints_.size(); ++i) {
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
    const std::array<std::byte, 3> input{std::byte{3}, std::byte{7}, std::byte{0xff}};
    ASSERT_EQ(sender.connection.Write(input), input.size());
    ASSERT_TRUE(sender.connection.Progress(++now_));
    ASSERT_EQ(packets_.size(), 1u);
    // 已发布路由必须接住先到的首包并保留 RX Lease，但尚未 Active 的应用 Read 仍被门禁阻止。
    Deliver();
    Poll();
    EXPECT_EQ(receiver.buffers.held(), 1u);
    EXPECT_EQ(receiver.connection.state(), ConnectionState::kWaitingReady);
    std::array<std::byte, 3> output{};
    const auto early = receiver.connection.Read(output, ++now_);
    ASSERT_FALSE(early);
    EXPECT_EQ(early.error().code, ConnectionErrorCode::kNotReady);
    for (int n = 0; n < 100 && receiver.connection.state() != ConnectionState::kRawActive; ++n) {
        ASSERT_TRUE(receiver.connection.Progress(++now_));
    }
    ASSERT_EQ(receiver.connection.state(), ConnectionState::kRawActive);
    const auto read = receiver.connection.Read(output, ++now_);
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, input.size());
    EXPECT_EQ(output, input);
    EXPECT_EQ(receiver.buffers.held(), 0u);
    EXPECT_EQ(receiver.receive.pending_refill(), 1u);
}

TEST_F(ConnectionTest, ControlEofFailsRawConnectionButKeepsAcceptedDmaUntilTerminalCompletion) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    // 空闲非阻塞控制 fd 的 EAGAIN 不是断连，也不能影响活跃 raw 数据路径。
    ASSERT_TRUE(sender.connection.OnControlReady());
    EXPECT_EQ(sender.connection.state(), ConnectionState::kRawActive);
    const std::array<std::byte, 8> input{};
    ASSERT_EQ(sender.connection.Write(input), input.size());
    ASSERT_TRUE(sender.connection.Progress(++now_));
    ASSERT_EQ(sender.slab.stats().local_dma, 1u);
    ASSERT_EQ(sender.connection.tx_stats().in_flight, 1u);
    ASSERT_EQ(shutdown(receiver.connection.fd(), SHUT_WR), 0);
    const auto closed = sender.connection.OnControlReady();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, ConnectionErrorCode::kControlClosed);
    EXPECT_EQ(sender.connection.state(), ConnectionState::kFailed);
    EXPECT_EQ(sender.slab.stats().local_dma, 1u);
    EXPECT_EQ(sender.connection.tx_stats().in_flight, 1u);
    const auto close_pending = sender.connection.Close();
    ASSERT_FALSE(close_pending);
    EXPECT_EQ(close_pending.error().code, ConnectionErrorCode::kRetained);
    EXPECT_EQ(sender.slab.stats().local_dma, 1u);
    // 控制 EOF 不能制造 TX 终结；仅真实排队 CQE 解除 DMA 和独立 import 引用。
    Deliver();
    Poll();
    EXPECT_EQ(sender.slab.stats().local_dma, 0u);
    EXPECT_EQ(sender.connection.tx_stats().in_flight, 0u);
    EXPECT_TRUE(sender.connection.Close());
}

} // namespace
} // namespace connection
} // namespace kbsocket
