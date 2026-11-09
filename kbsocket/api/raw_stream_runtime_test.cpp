// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/raw_stream_runtime.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace api {
namespace {
std::uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
class RawStreamRuntimeTest : public ::testing::Test {
protected:
    struct Receive {
        std::uint64_t id;
        urma_sge_t sge;
    };
    struct Endpoint {
        urma_context_t context{};
        std::array<urma_jfc_t, 2> cq{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetty{};
        std::array<urma_target_seg_t, 2> segments{};
        urma_target_jetty_t target{};
        std::size_t cq_count = 0, jetty_count = 0, segment_count = 0;
        std::deque<Receive> posted;
        std::array<std::deque<urma_cr_t>, 2> completed;
        bool hold_tx = false;
        std::atomic<bool> block_import{false};
        std::atomic<unsigned> imports{0};
        std::size_t callbacks = 0, commands = 0, withdrawals = 0, exports = 0, puts = 0;
        std::uint64_t last_command_token = 0;
        runtime::PeerWithdrawalState withdrawal_state = runtime::PeerWithdrawalState::kActive;
        alignas(urma_rjetty_t) std::array<std::byte, sizeof(urma_rjetty_t) + 8> descriptor{};
        runtime::RawStackContextResources resources{};
        std::array<kbs_stream_handle, 4> streams{};
        std::size_t stream_count = 0;
        connection::CreditPeerHandle peer;
        connection::SenderCreditAccount sender_credits;
    };
    static inline RawStreamRuntimeTest* active_ = nullptr;
    urma_device_t device_{};
    std::array<urma_device_t*, 1> devices_{&device_};
    std::array<urma_eid_info_t, 2> eids_{};
    std::array<Endpoint, 2> endpoints_;
    RawStreamRuntime runtime_;
    std::array<RawStreamEndpointConfig, 2> configs_;
    runtime::RawStackConfig config_;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    std::array<raw::ReceiveDomainQuiescence, 2> proofs_{{{true, true, true, true}, {true, true, true, true}}};
    bool opened_ = false;
    int fail_export_index_ = -1;
    static Endpoint& Find(urma_context_t* context) {
        return context == &active_->endpoints_[0].context ? active_->endpoints_[0] : active_->endpoints_[1];
    }
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.init = [](auto*) { return URMA_SUCCESS; };
        f.uninit = [] { return URMA_SUCCESS; };
        f.get_devices = [](int* count) {
            *count = 1;
            return active_->devices_.data();
        };
        f.free_devices = [](auto*) {};
        f.get_eids = [](auto*, auto* count) {
            *count = 2;
            return active_->eids_.data();
        };
        f.free_eids = [](auto*) {};
        f.query_device = [](auto*, auto* attr) {
            *attr = {};
            auto& cap = attr->dev_cap;
            cap.trans_mode = URMA_TM_RM;
            cap.rm_tp_cap.bs.ctp = 1;
            cap.max_eid_cnt = 64;
            cap.max_jetty = 8;
            cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
            cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
            return URMA_SUCCESS;
        };
        f.create_context = [](auto*, std::uint32_t eid) { return &active_->endpoints_[eid].context; };
        f.delete_context = [](auto*) { return URMA_SUCCESS; };
        f.create_jfc = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto& cq = e.cq[e.cq_count++];
            cq.urma_ctx = context;
            cq.jfc_cfg = *config;
            return &cq;
        };
        f.create_jfr = [](auto* context, auto* config) {
            auto& e = Find(context);
            e.jfr.urma_ctx = context;
            e.jfr.jfr_cfg = *config;
            return &e.jfr;
        };
        f.create_jetty = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto& jetty = e.jetty[e.jetty_count++];
            jetty.urma_ctx = context;
            jetty.jetty_cfg = *config;
            jetty.jetty_id.id = 100 + (context == &active_->endpoints_[0].context ? 0 : 10) + e.jetty_count;
            jetty.jetty_id.eid = context->eid;
            return &jetty;
        };
        f.register_seg = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto& segment = e.segments[e.segment_count++];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            return &segment;
        };
        f.get_rjetty = [](auto* jetty, auto** descriptor, auto* length) {
            auto& e = Find(jetty->urma_ctx);
            ++e.exports;
            // 模拟 provider 原样导出的扩展；门面必须保留扩展并显式改为 CTP。
            urma_rjetty_t remote{};
            remote.jetty_id = jetty->jetty_id;
            remote.trans_mode = URMA_TM_RM;
            remote.type = URMA_JETTY;
            remote.flag.bs.has_user_info = 1;
            std::memcpy(e.descriptor.data(), &remote, sizeof(remote));
            const std::uint32_t extra = 4;
            std::memcpy(e.descriptor.data() + sizeof(remote), &extra, sizeof(extra));
            e.descriptor.back() = std::byte{0x67};
            *descriptor = reinterpret_cast<urma_rjetty_t*>(e.descriptor.data());
            *length = static_cast<std::uint32_t>(e.descriptor.size());
            return active_->fail_export_index_ >= 0 && &e == &active_->endpoints_[active_->fail_export_index_]
                       ? URMA_FAIL
                       : URMA_SUCCESS;
        };
        f.put_rjetty = [](auto* descriptor) {
            for (auto& e : active_->endpoints_) {
                if (descriptor == reinterpret_cast<urma_rjetty_t*>(e.descriptor.data())) {
                    ++e.puts;
                }
            }
        };
        f.import_jetty = [](auto* context, auto* remote, auto*) {
            auto& e = Find(context);
            ++e.imports;
            while (e.block_import.load()) {
                e.block_import.wait(true);
            }
            e.target.urma_ctx = context;
            e.target.id = remote->jetty_id;
            return &e.target;
        };
        f.unimport_jetty = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        f.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) {
            *bad = nullptr;
            auto& e = Find(jfr->urma_ctx);
            for (; wr; wr = wr->next) {
                e.posted.push_back({wr->user_ctx, wr->src.sge[0]});
            }
            return URMA_SUCCESS;
        };
        f.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) {
            *bad = nullptr;
            auto& e = Find(jetty->urma_ctx);
            for (; wr; wr = wr->next) {
                auto& peer = active_->endpoints_[wr->tjetty->id.id >= 110 ? 1 : 0];
                if (peer.posted.empty()) {
                    *bad = wr;
                    return URMA_ENOMEM;
                }
                auto receive = peer.posted.front();
                peer.posted.pop_front();
                std::size_t bytes = 0;
                for (std::uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
                    const auto& sge = wr->send.src.sge[i];
                    std::memcpy(reinterpret_cast<std::byte*>(receive.sge.addr) + bytes,
                                reinterpret_cast<const void*>(sge.addr), sge.len);
                    bytes += sge.len;
                }
                urma_cr_t tx{};
                tx.status = URMA_CR_SUCCESS;
                tx.flag.bs.jetty = 1;
                tx.local_id = jetty->jetty_id.id;
                tx.user_ctx = wr->user_ctx;
                tx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                e.completed[0].push_back(tx);
                urma_cr_t rx{};
                rx.status = URMA_CR_SUCCESS;
                rx.flag.bs.jetty = 1;
                rx.flag.bs.s_r = 1;
                rx.local_id = peer.jetty[0].jetty_id.id;
                rx.remote_id = jetty->jetty_id;
                rx.user_ctx = receive.id;
                rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                rx.imm_data = wr->send.imm_data;
                rx.completion_len = bytes;
                peer.completed[1].push_back(rx);
            }
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* cq, int capacity, auto* out) {
            auto& e = Find(cq->urma_ctx);
            const auto side = cq == &e.cq[0] ? 0u : 1u;
            if (side == 0 && e.hold_tx) {
                return 0;
            }
            auto& completed = e.completed[side];
            int count = 0;
            while (count < capacity && !completed.empty()) {
                out[count++] = completed.front();
                completed.pop_front();
            }
            return count;
        };
        return f;
    }
    bool Until(auto predicate) {
        const auto deadline = Now() + 3'000'000'000ull;
        while (!predicate() && Now() < deadline) {
            const auto progress = runtime_.Step();
            if (!progress) {
                ADD_FAILURE() << "Step error " << progress.error().code << " detail=" << progress.error().detail;
                return false;
            }
            std::this_thread::yield();
        }
        return predicate();
    }
    std::optional<kbs_stream_status> Status(std::size_t endpoint, std::size_t stream = 0) {
        kbs_stream_status status{};
        if (kbs_stream_inspect(runtime_.table(endpoint), endpoints_[endpoint].streams[stream], &status, nullptr)) {
            return std::nullopt;
        }
        return status;
    }
    void Initialize() {
        active_ = this;
        override_.emplace(Functions());
        std::strcpy(device_.name, "raw");
        device_.type = URMA_TRANSPORT_UB;
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            eids_[i].eid_index = i;
            eids_[i].eid.raw[0] = i + 1;
            e.context.dev = &device_;
            e.context.eid = eids_[i].eid;
            e.context.eid_index = i;
            e.context.async_fd = -1;
            auto& input = configs_[i];
            input.streams = {401 + i, 4, 3};
            input.authorization = 7;
            input.endpoint_generation = 21 + i;
            input.credit_epoch = 301 + i;
            input.endpoint.device = {"raw", static_cast<std::uint32_t>(i)};
            input.endpoint.cookie = &e;
            input.endpoint.on_connection = [](void* cookie, const runtime::ConnectionProgressEvent&) noexcept {
                ++static_cast<Endpoint*>(cookie)->callbacks;
            };
            input.endpoint.on_command = [](void* cookie, const runtime::ConnectionMailboxNotice& notice) noexcept {
                auto& endpoint = *static_cast<Endpoint*>(cookie);
                ++endpoint.commands;
                endpoint.last_command_token = notice.token;
            };
            input.endpoint.on_withdrawal = [](void* cookie, const runtime::PeerCreditProgressEvent& event) noexcept {
                auto& endpoint = *static_cast<Endpoint*>(cookie);
                ++endpoint.withdrawals;
                endpoint.withdrawal_state = event.snapshot.state;
            };
            auto& domain = input.endpoint.domain;
            domain.local_domain = {11, 101 + i};
            domain.context_incarnation = i + 1;
            domain.queues.tx_depth = domain.tx_attempts = 16;
            domain.queues.rx_depth = domain.queues.rx_cq_depth = 16;
            domain.route_capacity = domain.source_capacity = 8;
            domain.peer_capacity = 2;
            domain.control_slots = 8;
            domain.base_credits = 4;
        }
        config_.slab = {32, 4096, 64};
        config_.memory_budget_bytes = 32 * 4096;
        config_.connections_per_context = config_.withdrawals_per_context = 4;
        config_.commands_per_context = 4;
        config_.imports = {4, 64, 2};
        config_.runtime.reactor.budget = 8;
        config_.runtime.control.workers = 2;
    }
    void SetUp() override {
        Initialize();
        const auto opened = runtime_.Open(configs_, config_);
        opened_ = runtime_.stage() != runtime::RawStackStage::kEmpty;
        ASSERT_TRUE(opened) << "code=" << opened.error().code << " detail=" << opened.error().detail;
        for (std::size_t i = 0; i < 2; ++i) {
            const auto resources = runtime_.HostResources(i);
            ASSERT_TRUE(resources);
            auto& e = endpoints_[i];
            e.resources = *resources;
            // 每个 Peer 只准入/授信一次，后续逻辑连接必须复用同一账户。
            const auto peer = e.resources.domain.credits->Admit({11, 101 + i, 301 + i});
            ASSERT_TRUE(peer);
            e.peer = *peer;
            ASSERT_TRUE(e.resources.domain.credits->Grant(*peer, connection::CreditClass::kData, 8));
            ASSERT_TRUE(e.resources.domain.credits->Grant(*peer, connection::CreditClass::kControl, 4));
            ASSERT_TRUE(e.sender_credits.Open({11, 102 - i, 302 - i}));
        }
    }
    void AdoptPair() {
        const auto pair = endpoints_[0].stream_count;
        ASSERT_LT(pair, 4u);
        ASSERT_EQ(endpoints_[1].stream_count, pair);
        int sockets[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        for (std::size_t i = 0; i < 2; ++i) {
            const RawStreamConnectOptions options{.session = 201 + i + pair * 2,
                                                  .expected_peer_authorization = 7,
                                                  .expected_peer_runtime = 11,
                                                  .compatible_peer = true,
                                                  .deadline_ns = Now() + 10'000'000'000ull};
            const auto stream = runtime_.AdoptProtocol(i, &sockets[i], options, Now());
            if (!stream && sockets[i] >= 0) {
                ::close(sockets[i]);
            }
            ASSERT_TRUE(stream) << "code=" << stream.error().code;
            EXPECT_EQ(sockets[i], -1);
            endpoints_[i].streams[endpoints_[i].stream_count++] = *stream;
        }
        ASSERT_TRUE(Until([&] {
            const auto first = Status(0, pair), second = Status(1, pair);
            return first && second && first->state == KBS_STREAM_PREPARING && second->state == KBS_STREAM_PREPARING;
        }));
    }
    RawStreamPreparation Preparation(std::size_t i) {
        return {.credit_peer = endpoints_[i].peer,
                .sender_credits = &endpoints_[i].sender_credits,
                .reorder = {.window = 8, .max_resident_bytes = 32768},
                .tx_capacity = 8,
                .descriptor_compatible = true,
                .peer_lanes_authorized = true};
    }
    void PreparePair(std::size_t pair = 0) {
        for (std::size_t i = 0; i < 2; ++i) {
            const auto result = runtime_.PrepareRaw(endpoints_[i].streams[pair], Preparation(i));
            ASSERT_TRUE(result) << "code=" << result.error().code << " native=" << result.error().native_code;
        }
    }
    void ActivePair(std::size_t pair = 0) {
        ASSERT_TRUE(Until([&] {
            const auto first = Status(0, pair), second = Status(1, pair);
            return first && second && first->state == KBS_STREAM_RAW_ACTIVE && second->state == KBS_STREAM_RAW_ACTIVE;
        }));
    }
    void Activate() {
        AdoptPair();
        PreparePair();
        ActivePair();
    }
    void Transfer(std::size_t pair = 0) {
        const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
        kbs_stream_io io{};
        ASSERT_EQ(kbs_stream_write(runtime_.table(0), endpoints_[0].streams[pair], payload.data(), payload.size(), &io,
                                   nullptr),
                  0);
        ASSERT_EQ(io.bytes, payload.size());
        ASSERT_TRUE(Until([&] {
            const auto receiver = Status(1, pair);
            return receiver && receiver->rx_pending;
        }));
        std::array<std::byte, 3> output{};
        ASSERT_EQ(kbs_stream_read(runtime_.table(1), endpoints_[1].streams[pair], output.data(), output.size(), Now(),
                                  &io, nullptr),
                  0);
        ASSERT_EQ(io.bytes, payload.size());
        EXPECT_EQ(output, payload);
        ASSERT_TRUE(Until([&] {
            const auto sender = Status(0, pair);
            return sender && sender->tx_pending == 0;
        }));
    }
    void ClosePair(std::size_t pair) {
        // Mock 已停发；真实 CQE 先收敛，来源退休仍需独立给出远端及事件静默证明。
        ASSERT_TRUE(Until([&] {
            bool closed = true;
            for (std::size_t i = 0; i < 2; ++i) {
                closed &= kbs_stream_close(runtime_.table(i), endpoints_[i].streams[pair], nullptr) == 0;
            }
            return closed;
        }));
        for (std::size_t i = 0; i < 2; ++i) {
            ASSERT_TRUE(runtime_.RetireSources(endpoints_[i].streams[pair], {i + 1, true, true, true}));
            ASSERT_EQ(kbs_stream_release(runtime_.table(i), endpoints_[i].streams[pair], nullptr), 0);
        }
    }
    void TearDown() override {
        for (auto& e : endpoints_) {
            e.block_import.store(false);
            e.block_import.notify_all();
            e.hold_tx = false;
        }
        if (opened_ && runtime_.stage() != runtime::RawStackStage::kClosed) {
            ASSERT_TRUE(Until([&] { return !runtime_.paused(0) && !runtime_.paused(1); }));
            for (std::size_t i = 0; i < 2; ++i) {
                auto& e = endpoints_[i];
                for (std::size_t j = 0; j < e.stream_count; ++j) {
                    if (Status(i, j)) {
                        (void)kbs_stream_cancel(runtime_.table(i), e.streams[j], nullptr);
                    }
                }
            }
            ASSERT_TRUE(Until([&] {
                bool closed = true;
                for (std::size_t i = 0; i < 2; ++i) {
                    auto& e = endpoints_[i];
                    for (std::size_t j = 0; j < e.stream_count; ++j) {
                        if (Status(i, j)) {
                            closed &= kbs_stream_close(runtime_.table(i), e.streams[j], nullptr) == 0;
                        }
                    }
                }
                return closed;
            }));
            for (int i = 0; i < 4; ++i) {
                ASSERT_TRUE(runtime_.Step());
            }
            for (std::size_t i = 0; i < 2; ++i) {
                auto& e = endpoints_[i];
                for (std::size_t j = 0; j < e.stream_count; ++j) {
                    if (Status(i, j)) {
                        ASSERT_TRUE(runtime_.RetireSources(e.streams[j], {i + 1, true, true, true}));
                        ASSERT_EQ(kbs_stream_release(runtime_.table(i), e.streams[j], nullptr), 0);
                    }
                }
                ASSERT_TRUE(e.sender_credits.Close());
            }
            ASSERT_TRUE(runtime_.BeginClose());
            ASSERT_TRUE(Until([&] {
                const auto snapshot = runtime_.Inspect();
                return snapshot && snapshot->import_entries == 0;
            }));
            const auto closed = runtime_.Close({true, proofs_});
            ASSERT_TRUE(closed) << "code=" << closed.error().code;
            EXPECT_EQ(closed->stage, runtime::RawStackStage::kClosed);
        }
        override_.reset();
        active_ = nullptr;
    }
};

TEST_F(RawStreamRuntimeTest, ExportsActualEndpointsAndBridgesCStreamTrafficAndEvents) {
    AdoptPair();
    for (std::size_t i = 0; i < 2; ++i) {
        auto& remote = endpoints_[1 - i];
        const auto peer = runtime_.Peer(endpoints_[i].streams[0]);
        ASSERT_TRUE(peer);
        EXPECT_EQ(peer->identity.receive_domain, 102 - i);
        EXPECT_EQ(peer->identity.endpoint_generation, 22 - i);
        EXPECT_EQ(peer->identity.authorization, 7u);
        EXPECT_EQ(peer->credit_epoch, 302 - i);
        EXPECT_EQ(peer->jetty_id, remote.jetty[0].jetty_id.id);
        ASSERT_EQ(peer->tx_lanes.count, 1u);
        EXPECT_EQ(peer->tx_lanes.lanes[0].jetty_id, remote.jetty[1].jetty_id.id);
        EXPECT_NE(peer->jetty_id, peer->tx_lanes.lanes[0].jetty_id);
        ASSERT_EQ(peer->descriptor_bytes, remote.descriptor.size());
        EXPECT_EQ(peer->descriptor[peer->descriptor_bytes - 1], std::byte{0x67});
        urma_rjetty_t descriptor{};
        std::memcpy(&descriptor, peer->descriptor.data(), sizeof(descriptor));
        EXPECT_EQ(descriptor.tp_type, URMA_CTP);
        EXPECT_EQ(descriptor.jetty_id.id, peer->jetty_id);
        EXPECT_EQ(remote.exports, 1u);
        EXPECT_EQ(remote.puts, 1u);
        EXPECT_EQ(remote.imports.load(), 0u);
    }
    PreparePair();
    ActivePair();
    Transfer();
    for (std::size_t i = 0; i < 2; ++i) {
        std::array<kbs_stream_event, 4> events{};
        std::size_t count = 0;
        ASSERT_EQ(kbs_stream_poll_events(runtime_.table(i), events.data(), events.size(), &count, nullptr), 0);
        ASSERT_EQ(count, 1u);
        EXPECT_EQ(events[0].stream.generation, endpoints_[i].streams[0].generation);
        EXPECT_EQ(events[0].state, KBS_STREAM_RAW_ACTIVE);
        EXPECT_GT(endpoints_[i].callbacks, 0u);
    }
    EXPECT_EQ(runtime_.table(2), nullptr);
}

TEST_F(RawStreamRuntimeTest, RequiresIndependentAuthorizationAndCorrectSharedCreditIdentities) {
    AdoptPair();
    auto prep = Preparation(0);
    prep.descriptor_compatible = false;
    const auto unauthorized = runtime_.PrepareRaw(endpoints_[0].streams[0], prep);
    ASSERT_FALSE(unauthorized);
    EXPECT_EQ(unauthorized.error().code, KBS_STREAM_PROTOCOL);
    EXPECT_EQ(unauthorized.error().detail, static_cast<int>(control::ConnectionHandshakeErrorCode::kUnauthorized));
    prep.descriptor_compatible = true;
    prep.peer_lanes_authorized = false;
    EXPECT_FALSE(runtime_.PrepareRaw(endpoints_[0].streams[0], prep));
    prep = Preparation(0);
    prep.credit_peer = endpoints_[1].peer;
    EXPECT_FALSE(runtime_.PrepareRaw(endpoints_[0].streams[0], prep));
    prep = Preparation(0);
    prep.sender_credits = &endpoints_[1].sender_credits;
    EXPECT_FALSE(runtime_.PrepareRaw(endpoints_[0].streams[0], prep));
    EXPECT_EQ(endpoints_[0].imports.load(), 0u);
    ASSERT_TRUE(Status(0));
    EXPECT_EQ(Status(0)->state, KBS_STREAM_PREPARING);
    // 拒绝发生在改变握手/导入状态之前，同一流可完成独立认证后重新 Prepare。
    PreparePair();
    ActivePair();
    Transfer();
}

TEST_F(RawStreamRuntimeTest, MultipleLogicalConnectionsSharePeerBudgetsAndSurviveSiblingClose) {
    Activate();
    Transfer();
    // Read 已释放 RX，但真正补投/发布 CREDIT 需要随后推进；先等待这份真实新额度收敛。
    ASSERT_TRUE(Until([&] {
        const auto grant = endpoints_[1].resources.domain.credits->PeerStats(endpoints_[1].peer);
        const auto receiver = Status(1);
        return grant && grant->max_data == 9 && receiver && receiver->tx_pending == 0 &&
               endpoints_[0].sender_credits.stats().limits[0] == grant->max_data;
    }));
    const auto spent = endpoints_[0].sender_credits.stats().spent;
    const auto first_grant = endpoints_[1].resources.domain.credits->PeerStats(endpoints_[1].peer);
    ASSERT_TRUE(first_grant);
    AdoptPair();
    PreparePair(1);
    ActivePair(1);
    const auto second_grant = endpoints_[1].resources.domain.credits->PeerStats(endpoints_[1].peer);
    ASSERT_TRUE(second_grant);
    EXPECT_EQ(second_grant->max_data, first_grant->max_data);
    EXPECT_EQ(second_grant->max_control, first_grant->max_control);
    EXPECT_EQ(endpoints_[0].sender_credits.stats().spent, spent);
    ClosePair(0);
    EXPECT_TRUE(endpoints_[0].sender_credits.ready());
    EXPECT_FALSE(endpoints_[0].sender_credits.stats().stopped);
    Transfer(1);
    EXPECT_GT(endpoints_[0].sender_credits.stats().spent[0], spent[0]);
    EXPECT_FALSE(runtime_.Peer(endpoints_[0].streams[0]));
    EXPECT_FALSE(runtime_.PrepareRaw(endpoints_[0].streams[0], Preparation(0)));
}

TEST_F(RawStreamRuntimeTest, ImportPauseGuardsContextAndSharedPreparationWithoutPartialEffects) {
    AdoptPair();
    endpoints_[0].block_import.store(true);
    ASSERT_TRUE(runtime_.PrepareRaw(endpoints_[0].streams[0], Preparation(0)));
    ASSERT_TRUE(Until([&] { return runtime_.paused(0); }));
    const auto paused = runtime_.HostResources(0);
    ASSERT_FALSE(paused);
    EXPECT_EQ(paused.error().code, KBS_STREAM_BUSY);
    EXPECT_TRUE(runtime_.HostResources(1));
    EXPECT_FALSE(runtime_.Peer(endpoints_[0].streams[0]));
    const auto shared_gate = runtime_.PrepareRaw(endpoints_[1].streams[0], Preparation(1));
    ASSERT_FALSE(shared_gate);
    EXPECT_EQ(shared_gate.error().code, KBS_STREAM_BUSY);
    EXPECT_EQ(endpoints_[1].imports.load(), 0u);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_PREPARING);
    EXPECT_FALSE(runtime_.BeginClose());
    endpoints_[0].block_import.store(false);
    endpoints_[0].block_import.notify_all();
    ASSERT_TRUE(Until([&] { return !runtime_.paused(0); }));
    ASSERT_TRUE(runtime_.PrepareRaw(endpoints_[1].streams[0], Preparation(1)));
    ActivePair();
    Transfer();
}

TEST_F(RawStreamRuntimeTest, ApplicationBuffersShareRegisteredBackingWithoutDisruptingPrivateStreamStaging) {
    EXPECT_EQ(runtime_.buffers(), nullptr);
    EXPECT_FALSE(runtime_.OpenBuffers({0, 2}));
    ASSERT_TRUE(runtime_.OpenBuffers({901, 2}));
    auto* buffers = runtime_.buffers();
    ASSERT_NE(buffers, nullptr);
    const auto duplicate = runtime_.OpenBuffers({902, 2});
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, KBS_STREAM_BUSY);
    Activate();
    const auto before = runtime_.Inspect();
    ASSERT_TRUE(before);
    kbs_buffer_handle handle{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 31, &handle, nullptr), 0);
    kbs_buffer_view writable{};
    ASSERT_EQ(kbs_buffer_writable(buffers, handle, &writable, nullptr), 0);
    ASSERT_EQ(writable.size, 31u);
    std::fill_n(static_cast<std::byte*>(writable.data), writable.size, std::byte{0x5a});
    const auto held = runtime_.Inspect();
    ASSERT_TRUE(held);
    EXPECT_EQ(held->slab.cpu_leases, before->slab.cpu_leases + 1);
    EXPECT_EQ(held->slab.backing_bytes, before->slab.backing_bytes);
    // 公开 Lease 独立占用一槽；普通 stream 仍从同一 Slab 分配私有 staging，不能覆盖应用内容。
    Transfer();
    kbs_buffer_const_view readable{};
    ASSERT_EQ(kbs_buffer_readable(buffers, handle, &readable, nullptr), 0);
    const auto* bytes = static_cast<const std::byte*>(readable.data);
    EXPECT_TRUE(std::all_of(bytes, bytes + readable.size, [](auto byte) { return byte == std::byte{0x5a}; }));
    ASSERT_EQ(kbs_buffer_release(buffers, handle, nullptr), 0);
    EXPECT_EQ(kbs_buffer_readable(buffers, handle, &readable, nullptr), KBS_STREAM_INVALID_HANDLE);
    Transfer();
}

TEST_F(RawStreamRuntimeTest, ApplicationLeaseBlocksPhysicalCloseUntilReturnedAfterStop) {
    ASSERT_TRUE(runtime_.OpenBuffers({903, 2}));
    auto* buffers = runtime_.buffers();
    kbs_buffer_handle handle{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 17, &handle, nullptr), 0);
    kbs_buffer_view writable{};
    ASSERT_EQ(kbs_buffer_writable(buffers, handle, &writable, nullptr), 0);
    static_cast<std::byte*>(writable.data)[0] = std::byte{0x39};
    const auto close = runtime_.Close({true, proofs_});
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error().code, KBS_STREAM_BUSY);
    EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kStopping);
    EXPECT_EQ(runtime_.buffers(), buffers);
    const auto stopped = runtime_.OpenBuffers({904, 2});
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, KBS_STREAM_STOPPED);
    kbs_buffer_handle rejected{};
    EXPECT_EQ(kbs_buffer_acquire(buffers, 1, &rejected, nullptr), KBS_STREAM_STOPPED);
    EXPECT_EQ(kbs_buffer_writable(buffers, handle, &writable, nullptr), KBS_STREAM_STOPPED);
    // CPU 持有使底层 Slab 仍然存活；停止后仍能只读并显式归还，不把 Close 当成隐式归还。
    kbs_buffer_const_view readable{};
    ASSERT_EQ(kbs_buffer_readable(buffers, handle, &readable, nullptr), 0);
    EXPECT_EQ(static_cast<const std::byte*>(readable.data)[0], std::byte{0x39});
    ASSERT_EQ(kbs_buffer_release(buffers, handle, nullptr), 0);
    const auto closed = runtime_.Close({true, proofs_});
    ASSERT_TRUE(closed);
    EXPECT_EQ(closed->stage, runtime::RawStackStage::kClosed);
    EXPECT_EQ(kbs_buffer_readable(buffers, handle, &readable, nullptr), KBS_STREAM_NOT_READY);
}

TEST_F(RawStreamRuntimeTest, SharedBufferOperationsWaitForOtherContextControlPauseWithoutLosingLease) {
    ASSERT_TRUE(runtime_.OpenBuffers({905, 2}));
    auto* buffers = runtime_.buffers();
    kbs_buffer_handle handle{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 17, &handle, nullptr), 0);
    kbs_buffer_view writable{};
    ASSERT_EQ(kbs_buffer_writable(buffers, handle, &writable, nullptr), 0);
    static_cast<std::byte*>(writable.data)[0] = std::byte{0x31};
    AdoptPair();
    endpoints_[1].block_import.store(true);
    ASSERT_TRUE(runtime_.PrepareRaw(endpoints_[1].streams[0], Preparation(1)));
    ASSERT_TRUE(Until([&] { return runtime_.paused(1); }));
    EXPECT_TRUE(runtime_.HostResources(0));
    EXPECT_FALSE(runtime_.paused(0));

    // 池在两个 context 注册；即便 context 0 可用，任何 context 暂停也禁止共享 Lease 操作。
    kbs_buffer_handle rejected{99, 99, 99, 99};
    EXPECT_EQ(kbs_buffer_acquire(buffers, 1, &rejected, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(rejected.generation, 0u);
    EXPECT_EQ(kbs_buffer_writable(buffers, handle, &writable, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(writable.data, nullptr);
    kbs_buffer_const_view readable{};
    EXPECT_EQ(kbs_buffer_readable(buffers, handle, &readable, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(readable.data, nullptr);
    EXPECT_EQ(kbs_buffer_release(buffers, handle, nullptr), KBS_STREAM_BUSY);
    EXPECT_FALSE(runtime_.BeginClose());

    endpoints_[1].block_import.store(false);
    endpoints_[1].block_import.notify_all();
    ASSERT_TRUE(Until([&] { return !runtime_.paused(1); }));
    ASSERT_EQ(kbs_buffer_readable(buffers, handle, &readable, nullptr), 0);
    EXPECT_EQ(static_cast<const std::byte*>(readable.data)[0], std::byte{0x31});
    // 暂停拒绝没有消耗第二个发布槽，也没有停止准入或清除已有 Lease。
    ASSERT_EQ(kbs_buffer_acquire(buffers, 1, &rejected, nullptr), 0);
    ASSERT_EQ(kbs_buffer_release(buffers, rejected, nullptr), 0);
    ASSERT_EQ(kbs_buffer_release(buffers, handle, nullptr), 0);
    ASSERT_TRUE(runtime_.PrepareRaw(endpoints_[0].streams[0], Preparation(0)));
    ActivePair();
    Transfer();
}

TEST_F(RawStreamRuntimeTest, LogicalCloseDoesNotFabricateSourceOrDomainQuiescence) {
    Activate();
    Transfer();
    const auto premature = runtime_.Close({true, proofs_});
    ASSERT_FALSE(premature);
    EXPECT_NE(premature.error().code, 0);
    EXPECT_NE(runtime_.table(0), nullptr);
    for (std::size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(kbs_stream_close(runtime_.table(i), endpoints_[i].streams[0], nullptr), 0);
        EXPECT_EQ(kbs_stream_release(runtime_.table(i), endpoints_[i].streams[0], nullptr), KBS_STREAM_RETAINED);
        EXPECT_FALSE(runtime_.RetireSources(endpoints_[i].streams[0], {i + 1, true, true, false}));
        ASSERT_TRUE(runtime_.RetireSources(endpoints_[i].streams[0], {i + 1, true, true, true}));
        ASSERT_EQ(kbs_stream_release(runtime_.table(i), endpoints_[i].streams[0], nullptr), 0);
    }
    ASSERT_TRUE(Until([&] {
        const auto snapshot = runtime_.Inspect();
        return snapshot && snapshot->import_entries == 0;
    }));
    const auto missing_proof = runtime_.Close({});
    EXPECT_FALSE(missing_proof);
    EXPECT_NE(runtime_.stage(), runtime::RawStackStage::kClosed);
    const auto closed = runtime_.Close({true, proofs_});
    ASSERT_TRUE(closed);
    EXPECT_EQ(closed->stage, runtime::RawStackStage::kClosed);
    EXPECT_EQ(runtime_.table(0), nullptr);
}

TEST_F(RawStreamRuntimeTest, FullTableLeavesNewProtocolFdWithCaller) {
    for (std::size_t i = 0; i < 4; ++i) {
        AdoptPair();
    }
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    const int original = sockets[0];
    const auto rejected = runtime_.AdoptProtocol(0, &sockets[0],
                                                 {.session = 999,
                                                  .expected_peer_authorization = 7,
                                                  .compatible_peer = true,
                                                  .deadline_ns = Now() + 10'000'000'000ull},
                                                 Now());
    EXPECT_FALSE(rejected);
    EXPECT_EQ(sockets[0], original);
    EXPECT_NE(::fcntl(sockets[0], F_GETFD), -1);
    ::close(sockets[0]);
    ::close(sockets[1]);
}

TEST_F(RawStreamRuntimeTest, PreservesOriginalCookiesForMailboxAndPeerWithdrawalCallbacks) {
    // 故意提交本 owner 的无效槽，命令仍应执行一次并以失败结果通知原 cookie。
    std::array<runtime::ConnectionCommandTicket, 2> tickets;
    for (std::size_t i = 0; i < 2; ++i) {
        auto& e = endpoints_[i];
        const auto submitted = e.resources.commands->TrySubmit({.kind = runtime::ConnectionCommandKind::kInspect,
                                                                .connection = {e.resources.connections, 999, 1},
                                                                .token = 77 + i});
        ASSERT_TRUE(submitted);
        tickets[i] = *submitted;
    }
    ASSERT_TRUE(Until([&] { return endpoints_[0].commands == 1 && endpoints_[1].commands == 1; }));
    for (std::size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(endpoints_[i].last_command_token, 77 + i);
        const auto completion = endpoints_[i].resources.commands->TakeCompletion(tickets[i]);
        ASSERT_TRUE(completion);
        EXPECT_FALSE(completion->result);
    }

    // 使用独立协议 fd 完成零流量 Peer 撤回，验证第三种回调也保留独立 context cookie。
    std::array<control::HandshakeIdentity, 2> identities;
    for (std::size_t i = 0; i < 2; ++i) {
        identities[i] = {11, 101 + i, 21 + i, 7, 501 + i, {1, 1}};
        ASSERT_TRUE(endpoints_[i].sender_credits.Update(endpoints_[i].sender_credits.identity(), 8, 4));
    }
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    std::array<runtime::PeerWithdrawal, 2> withdrawals;
    for (std::size_t i = 0; i < 2; ++i) {
        auto& e = endpoints_[i];
        const auto withdrawal = e.resources.withdrawals->BeginWithdrawal(sockets[i],
                                                                         {.local = identities[i],
                                                                          .peer = identities[1 - i],
                                                                          .local_credit_epoch = 301 + i,
                                                                          .peer_credit_epoch = 302 - i,
                                                                          .sender = &e.sender_credits,
                                                                          .receive = e.resources.domain.credits,
                                                                          .receive_peer = e.peer,
                                                                          .peer_authorized = true},
                                                                         Now());
        ASSERT_TRUE(withdrawal);
        withdrawals[i] = *withdrawal;
    }
    ASSERT_TRUE(Until([&] { return endpoints_[0].withdrawals == 1 && endpoints_[1].withdrawals == 1; }));
    for (std::size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(endpoints_[i].withdrawal_state, runtime::PeerWithdrawalState::kComplete);
        ASSERT_TRUE(endpoints_[i].resources.withdrawals->Release(withdrawals[i]));
    }
}

class RawStreamRuntimeOpenFailureTest : public RawStreamRuntimeTest {
protected:
    void SetUp() override {
        Initialize();
    }
};
TEST_F(RawStreamRuntimeOpenFailureTest, RejectedStackConfigurationCanBeCorrectedBeforeAnyProviderOwnership) {
    auto invalid = config_;
    invalid.memory_budget_bytes = 0;
    const auto rejected = runtime_.Open(configs_, invalid);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kEmpty);
    EXPECT_EQ(endpoints_[0].cq_count, 0u);
    EXPECT_EQ(endpoints_[1].exports, 0u);
    // RawStack 参数拒绝没有取得资源，门面不能留下永久 BUSY 的半开标记。
    const auto retry = runtime_.Open(configs_, config_);
    opened_ = runtime_.stage() != runtime::RawStackStage::kEmpty;
    ASSERT_TRUE(retry);
    EXPECT_NE(runtime_.table(0), nullptr);
    EXPECT_NE(runtime_.table(1), nullptr);
    const auto closed = runtime_.Close({true, proofs_});
    ASSERT_TRUE(closed);
    EXPECT_EQ(closed->stage, runtime::RawStackStage::kClosed);
}
TEST_F(RawStreamRuntimeOpenFailureTest, PartialExportFailureKeepsResourcesUntilExplicitProvenClose) {
    // 第二个 context 导出失败；第一个已建表也不能提前暴露，provider 描述符仍需配对 Put。
    fail_export_index_ = 1;
    const auto opened = runtime_.Open(configs_, config_);
    opened_ = runtime_.stage() != runtime::RawStackStage::kEmpty;
    ASSERT_FALSE(opened);
    EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kOpenFailed);
    EXPECT_EQ(runtime_.table(0), nullptr);
    EXPECT_EQ(runtime_.table(1), nullptr);
    EXPECT_FALSE(runtime_.HostResources(0));
    EXPECT_EQ(endpoints_[0].exports, 1u);
    EXPECT_EQ(endpoints_[0].puts, 1u);
    EXPECT_EQ(endpoints_[1].exports, 1u);
    EXPECT_EQ(endpoints_[1].puts, 1u);
    EXPECT_FALSE(runtime_.Close({}));
    const auto closed = runtime_.Close({true, proofs_});
    ASSERT_TRUE(closed);
    EXPECT_EQ(closed->stage, runtime::RawStackStage::kClosed);
}

TEST(RawStreamRuntimeValidationTest, InvalidConfigurationDoesNotEnterProvider) {
    unsigned init_calls = 0;
    static unsigned* calls = nullptr;
    calls = &init_calls;
    raw::UrmaFunctions functions{};
    functions.init = [](auto*) {
        ++*calls;
        return URMA_SUCCESS;
    };
    raw::test_support::ScopedUrmaOverride provider(functions);
    std::array<RawStreamEndpointConfig, 2> configs{};
    for (std::size_t i = 0; i < configs.size(); ++i) {
        configs[i].endpoint.device = {"raw", static_cast<std::uint32_t>(i)};
        configs[i].endpoint.domain.local_domain = {1, i + 1};
        configs[i].endpoint.domain.context_incarnation = i + 1;
        configs[i].streams.table_id = 7;
        configs[i].authorization = 1;
        configs[i].endpoint_generation = 1;
        configs[i].credit_epoch = 1;
    }
    RawStreamRuntime duplicate;
    EXPECT_FALSE(duplicate.Open(configs));
    RawStreamRuntime empty;
    EXPECT_FALSE(empty.Open({}));
    configs[1].streams.table_id = 8;
    configs[1].credit_epoch = 0;
    RawStreamRuntime missing_epoch;
    EXPECT_FALSE(missing_epoch.Open(configs));
    EXPECT_EQ(init_calls, 0u);
    EXPECT_EQ(duplicate.stage(), runtime::RawStackStage::kEmpty);
    EXPECT_EQ(empty.stage(), runtime::RawStackStage::kEmpty);
    EXPECT_EQ(missing_epoch.stage(), runtime::RawStackStage::kEmpty);
}
} // namespace
} // namespace api
} // namespace kbsocket
