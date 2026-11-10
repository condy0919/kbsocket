// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/raw_stream_runtime.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/api/lease_receive.h"
#include "kbsocket/api/lease_send.h"
#include "kbsocket/connection/wire_frame.hpp"
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
        std::array<urma_target_seg_t, 3> segments{};
        urma_target_seg_t imported_segment{};
        urma_target_jetty_t target{};
        std::size_t cq_count = 0, jetty_count = 0, segment_count = 0;
        std::deque<Receive> posted;
        std::array<std::deque<urma_cr_t>, 2> completed;
        bool hold_tx = false;
        bool hold_write_tx = false;
        std::size_t posted_sends = 0;
        std::size_t posted_writes = 0;
        std::vector<std::uint64_t> write_tickets;
        std::atomic<unsigned> segment_imports{0};
        std::uint64_t last_write_source = 0;
        const void* last_received_data = nullptr;
        std::uint64_t last_source_address = 0;
        std::uint64_t last_data_ticket = 0;
        urma_target_seg_t* last_source_segment = nullptr;
        std::uint32_t last_source_sges = 0;
        std::atomic<bool> block_import{false};
        std::atomic<bool> block_segment_import{false};
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
            cap.max_write_size = 200u * 1024u * 1024u;
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
            segment.seg.ubva.eid = context->eid;
            segment.seg.attr.bs.access = config->flag.bs.access;
            segment.seg.token_id = static_cast<std::uint32_t>(e.segment_count);
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
            e.target.trans_mode = remote->trans_mode;
            e.target.tp_type = remote->tp_type;
            e.target.type = remote->type;
            return &e.target;
        };
        f.import_seg = [](auto* context, auto* descriptor, auto*, std::uint64_t, urma_import_seg_flag_t) {
            auto& e = Find(context);
            ++e.segment_imports;
            while (e.block_segment_import.load()) {
                e.block_segment_import.wait(true);
            }
            e.imported_segment.urma_ctx = context;
            e.imported_segment.seg = *descriptor;
            return &e.imported_segment;
        };
        f.unimport_seg = [](auto*) { return URMA_SUCCESS; };
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
                if (wr->opcode == URMA_OPC_WRITE) {
                    // WRITE 必须实际落到已授权目标内存；仅返回 CQE 无法验证 RNDV 的可见性。
                    EXPECT_EQ(wr->rw.dst.num_sge, 1u);
                    std::size_t offset = 0;
                    for (std::uint32_t i = 0; i < wr->rw.src.num_sge; ++i) {
                        const auto& source = wr->rw.src.sge[i];
                        std::memcpy(reinterpret_cast<void*>(wr->rw.dst.sge[0].addr + offset),
                                    reinterpret_cast<const void*>(source.addr), source.len);
                        offset += source.len;
                    }
                    EXPECT_EQ(offset, wr->rw.dst.sge[0].len);
                    e.last_write_source = wr->rw.src.sge[0].addr;
                    ++e.posted_writes;
                    urma_cr_t tx{};
                    tx.status = URMA_CR_SUCCESS;
                    tx.flag.bs.jetty = 1;
                    tx.local_id = jetty->jetty_id.id;
                    tx.user_ctx = wr->user_ctx;
                    // 公共 CR opcode 描述接收操作；发送操作身份取自本地 WR 台账。
                    tx.opcode = URMA_CR_OPC_SEND;
                    e.write_tickets.push_back(wr->user_ctx);
                    e.completed[0].push_back(tx);
                    continue;
                }
                EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
                auto& peer = active_->endpoints_[wr->tjetty->id.id >= 110 ? 1 : 0];
                if (peer.posted.empty()) {
                    *bad = wr;
                    return URMA_ENOMEM;
                }
                ++e.posted_sends;
                e.last_source_address = wr->send.src.sge[0].addr;
                e.last_source_segment = wr->send.src.sge[0].tseg;
                e.last_source_sges = wr->send.src.num_sge;
                auto receive = peer.posted.front();
                peer.posted.pop_front();
                std::size_t bytes = 0;
                for (std::uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
                    const auto& sge = wr->send.src.sge[i];
                    std::memcpy(reinterpret_cast<std::byte*>(receive.sge.addr) + bytes,
                                reinterpret_cast<const void*>(sge.addr), sge.len);
                    bytes += sge.len;
                }
                const auto decoded = connection::DecodeWireFrame(
                    wr->send.imm_data, {reinterpret_cast<const std::byte*>(receive.sge.addr), bytes});
                if (decoded && !decoded->frame.data.empty()) {
                    peer.last_received_data = decoded->frame.data.data();
                    e.last_data_ticket = wr->user_ctx;
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
                if (side == 0 && e.hold_write_tx &&
                    std::find(e.write_tickets.begin(), e.write_tickets.end(), completed.front().user_ctx) !=
                        e.write_tickets.end()) {
                    break;
                }
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
        AdmitPeers();
    }
    void AdmitPeers() {
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
    virtual RawStreamPreparation Preparation(std::size_t i) {
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
        // RAW_ACTIVE 不代表 READY 及其派生 CREDIT 已退休；冻结 TX 的测试从真实静默边界开始。
        ASSERT_TRUE(Until([&] {
            for (std::size_t i = 0; i < 2; ++i) {
                const auto status = Status(i, pair);
                if (!status || status->tx_pending != 0 || !endpoints_[i].completed[0].empty() ||
                    !endpoints_[i].completed[1].empty()) {
                    return false;
                }
            }
            return true;
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
    void SendForLease(std::size_t pair = 0) {
        const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
        kbs_stream_io io{};
        ASSERT_EQ(kbs_stream_write(runtime_.table(0), endpoints_[0].streams[pair], payload.data(), payload.size(), &io,
                                   nullptr),
                  0);
        ASSERT_EQ(io.bytes, payload.size());
        ASSERT_TRUE(Until([&] {
            const auto status = Status(1, pair);
            return status && status->rx_pending;
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
            e.block_segment_import.store(false);
            e.block_segment_import.notify_all();
            e.hold_tx = false;
            e.hold_write_tx = false;
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
                return snapshot && snapshot->import_entries == 0 && snapshot->segment_entries == 0;
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
    const auto credit_identity = endpoints_[0].sender_credits.identity();
    const auto first_grant = endpoints_[1].resources.domain.credits->PeerStats(endpoints_[1].peer);
    ASSERT_TRUE(first_grant);
    AdoptPair();
    PreparePair(1);
    ActivePair(1);
    const auto second_grant = endpoints_[1].resources.domain.credits->PeerStats(endpoints_[1].peer);
    ASSERT_TRUE(second_grant);
    EXPECT_EQ(second_grant->max_data, first_grant->max_data);
    // 兄弟连接复用物理授信；CREDIT 可以合并或被去重，控制消费只要求不回退且不透支。
    EXPECT_GE(second_grant->max_control, first_grant->max_control);
    EXPECT_EQ(endpoints_[0].sender_credits.identity(), credit_identity);
    EXPECT_EQ(endpoints_[0].sender_credits.stats().spent[0], spent[0]);
    EXPECT_GE(endpoints_[0].sender_credits.stats().spent[1], spent[1]);
    EXPECT_LE(endpoints_[0].sender_credits.stats().spent[1], endpoints_[0].sender_credits.stats().limits[1]);
    ClosePair(0);
    EXPECT_TRUE(endpoints_[0].sender_credits.ready());
    EXPECT_FALSE(endpoints_[0].sender_credits.stats().stopped);
    Transfer(1);
    EXPECT_EQ(endpoints_[0].sender_credits.stats().spent[0], spent[0] + 1);
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

TEST_F(RawStreamRuntimeTest, ReceiveLeaseBorrowsRegisteredPrefixAndKeepsPhysicalCreditUntilCompleteConsumption) {
    Activate();
    SendForLease();
    auto& receiver = endpoints_[1];
    auto* table = runtime_.table(1);
    const auto stream = receiver.streams[0];
    const auto held = receiver.resources.domain.rx->held();
    const auto released = receiver.resources.domain.credits->stats().released;
    kbs_receive_view view{};
    ASSERT_EQ(kbs_stream_read_lease(table, stream, 1, Now(), &view, nullptr), 0);
    ASSERT_EQ(view.size, 1u);
    EXPECT_EQ(view.data, receiver.last_received_data);
    EXPECT_EQ(*static_cast<const std::byte*>(view.data), std::byte{0x11});
    EXPECT_NE(view.lease.generation, 0u);
    EXPECT_EQ(view.lease.stream.table_id, stream.table_id);
    EXPECT_EQ(view.eof, 0u);
    EXPECT_EQ(view.reserved, 0u);
    kbs_receive_view busy{};
    EXPECT_EQ(kbs_stream_read_lease(table, stream, 3, Now(), &busy, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(busy.data, nullptr);
    EXPECT_EQ(busy.size, 0u);
    std::array<std::byte, 3> copied{};
    kbs_stream_io io{};
    EXPECT_EQ(kbs_stream_read(table, stream, copied.data(), copied.size(), Now(), &io, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_RAW_ACTIVE);
    EXPECT_EQ(Status(1)->failure.code, 0);
    // 应用持有期间推进 Runtime 也不能释放该帧；归还前缀仍须等剩余字节消费完。
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(runtime_.Step());
    }
    EXPECT_EQ(receiver.resources.domain.rx->held(), held);
    EXPECT_EQ(receiver.resources.domain.credits->stats().released, released);
    ASSERT_EQ(kbs_stream_release_read_lease(table, view.lease, nullptr), 0);
    EXPECT_EQ(receiver.resources.domain.rx->held(), held);
    EXPECT_EQ(receiver.resources.domain.credits->stats().released, released);
    ASSERT_EQ(kbs_stream_read(table, stream, copied.data(), copied.size(), Now(), &io, nullptr), 0);
    EXPECT_EQ(io.bytes, 2u);
    EXPECT_EQ(copied[0], std::byte{0x22});
    EXPECT_EQ(copied[1], std::byte{0x33});
    EXPECT_EQ(receiver.resources.domain.rx->held() + 1, held);
    EXPECT_EQ(receiver.resources.domain.credits->stats().released, released + 1);
    EXPECT_EQ(kbs_stream_release_read_lease(table, view.lease, nullptr), KBS_STREAM_INVALID_HANDLE);
}

TEST_F(RawStreamRuntimeTest, ReceiveLeaseContinuesPartialCopyAndSeparatesWouldBlockFromOrderedEof) {
    Activate();
    auto* table = runtime_.table(1);
    const auto stream = endpoints_[1].streams[0];
    kbs_receive_view view{};
    EXPECT_EQ(kbs_stream_read_lease(table, stream, 3, Now(), &view, nullptr), KBS_STREAM_WOULD_BLOCK);
    EXPECT_EQ(view.data, nullptr);
    EXPECT_EQ(view.lease.generation, 0u);
    EXPECT_EQ(view.eof, 0u);
    SendForLease();
    std::byte prefix{};
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_read(table, stream, &prefix, 1, Now(), &io, nullptr), 0);
    EXPECT_EQ(prefix, std::byte{0x11});
    ASSERT_EQ(kbs_stream_read_lease(table, stream, 99, Now(), &view, nullptr), 0);
    EXPECT_EQ(view.size, 2u);
    EXPECT_EQ(view.data, static_cast<const std::byte*>(endpoints_[1].last_received_data) + 1);
    EXPECT_EQ(static_cast<const std::byte*>(view.data)[0], std::byte{0x22});
    EXPECT_EQ(static_cast<const std::byte*>(view.data)[1], std::byte{0x33});
    ASSERT_EQ(kbs_stream_shutdown_write(runtime_.table(0), endpoints_[0].streams[0], nullptr), 0);
    ASSERT_TRUE(Until([&] { return Status(1)->rx_pending == 2; }));
    kbs_receive_view blocked{};
    EXPECT_EQ(kbs_stream_read_lease(table, stream, 3, Now(), &blocked, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(blocked.eof, 0u);
    ASSERT_EQ(kbs_stream_release_read_lease(table, view.lease, nullptr), 0);
    ASSERT_EQ(kbs_stream_read_lease(table, stream, 3, Now(), &view, nullptr), 0);
    EXPECT_EQ(view.size, 0u);
    EXPECT_EQ(view.data, nullptr);
    EXPECT_EQ(view.lease.generation, 0u);
    EXPECT_EQ(view.eof, 1u);
    ASSERT_EQ(kbs_stream_read_lease(table, stream, 3, Now(), &view, nullptr), 0);
    EXPECT_EQ(view.eof, 1u);
}

TEST_F(RawStreamRuntimeTest, ReceiveLeaseSurvivesCancellationAndClosingUntilExplicitReturn) {
    Activate();
    SendForLease();
    auto* table = runtime_.table(1);
    const auto stream = endpoints_[1].streams[0];
    kbs_receive_view view{};
    ASSERT_EQ(kbs_stream_read_lease(table, stream, 3, Now(), &view, nullptr), 0);
    const auto held = endpoints_[1].resources.domain.rx->held();
    ASSERT_EQ(kbs_stream_cancel(table, stream, nullptr), 0);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_FAILED);
    EXPECT_EQ(kbs_stream_close(table, stream, nullptr), KBS_STREAM_RETAINED);
    EXPECT_EQ(kbs_stream_release(table, stream, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_CLOSING);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(runtime_.Step());
    }
    EXPECT_EQ(endpoints_[1].resources.domain.rx->held(), held);
    EXPECT_EQ(static_cast<const std::byte*>(view.data)[2], std::byte{0x33});
    // Close 没有夺回已借出的指针；归还不受业务 Active/失败门禁拦截。
    ASSERT_EQ(kbs_stream_release_read_lease(table, view.lease, nullptr), 0);
    ASSERT_EQ(kbs_stream_close(table, stream, nullptr), 0);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_STATE_CLOSED);
    EXPECT_EQ(kbs_stream_release_read_lease(table, view.lease, nullptr), KBS_STREAM_INVALID_HANDLE);
}

TEST_F(RawStreamRuntimeTest, ReceiveLeaseRejectsOldGenerationAndCrossStreamTransplantsWithoutConsumingEither) {
    Activate();
    AdoptPair();
    PreparePair(1);
    ActivePair(1);
    SendForLease(0);
    kbs_receive_view first{};
    ASSERT_EQ(kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[0], 1, Now(), &first, nullptr), 0);
    SendForLease(1);
    kbs_receive_view second{};
    ASSERT_EQ(kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[1], 1, Now(), &second, nullptr), 0);
    EXPECT_NE(first.lease.generation, second.lease.generation);
    auto wrong = first.lease;
    wrong.stream = second.lease.stream;
    EXPECT_EQ(kbs_stream_release_read_lease(runtime_.table(1), wrong, nullptr), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(kbs_stream_release_read_lease(runtime_.table(0), first.lease, nullptr), KBS_STREAM_INVALID_HANDLE);
    wrong = first.lease;
    wrong.stream.reserved = 1;
    EXPECT_EQ(kbs_stream_release_read_lease(runtime_.table(1), wrong, nullptr), KBS_STREAM_INVALID_HANDLE);
    wrong = first.lease;
    wrong.generation += (1ull << 32);
    EXPECT_EQ(kbs_stream_release_read_lease(runtime_.table(1), wrong, nullptr), KBS_STREAM_INVALID_HANDLE);
    ASSERT_EQ(kbs_stream_release_read_lease(runtime_.table(1), first.lease, nullptr), 0);
    kbs_receive_view next{};
    ASSERT_EQ(kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[0], 2, Now(), &next, nullptr), 0);
    EXPECT_NE(next.lease.generation, first.lease.generation);
    EXPECT_EQ(kbs_stream_release_read_lease(runtime_.table(1), first.lease, nullptr), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(static_cast<const std::byte*>(next.data)[0], std::byte{0x22});
    ASSERT_EQ(kbs_stream_release_read_lease(runtime_.table(1), next.lease, nullptr), 0);
    ASSERT_EQ(kbs_stream_release_read_lease(runtime_.table(1), second.lease, nullptr), 0);
}

TEST_F(RawStreamRuntimeTest, ReceiveLeaseInvalidArgumentsResetOutputWithoutConsumingData) {
    Activate();
    SendForLease();
    const auto stream = endpoints_[1].streams[0];
    auto* table = runtime_.table(1);
    kbs_receive_view view{};
    view.data = &view;
    view.size = 99;
    view.eof = view.reserved = 1;
    view.lease.generation = 99;
    EXPECT_EQ(kbs_stream_read_lease(table, stream, 0, Now(), &view, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(view.data, nullptr);
    EXPECT_EQ(view.size, 0u);
    EXPECT_EQ(view.eof, 0u);
    EXPECT_EQ(view.reserved, 0u);
    EXPECT_EQ(view.lease.generation, 0u);
    EXPECT_EQ(kbs_stream_read_lease(table, stream, 1, Now(), nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_read_lease(nullptr, stream, 1, Now(), &view, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    auto invalid_stream = stream;
    ++invalid_stream.generation;
    EXPECT_EQ(kbs_stream_read_lease(table, invalid_stream, 1, Now(), &view, nullptr), KBS_STREAM_INVALID_HANDLE);
    ASSERT_EQ(kbs_stream_read_lease(table, stream, 99, Now(), &view, nullptr), 0);
    EXPECT_EQ(view.size, 3u);
    EXPECT_EQ(*static_cast<const std::byte*>(view.data), std::byte{0x11});
    ASSERT_EQ(kbs_stream_release_read_lease(table, view.lease, nullptr), 0);
}

TEST_F(RawStreamRuntimeTest, LeaseWritePostsApplicationAddressAndRetainsSourceUntilActualCqe) {
    ASSERT_TRUE(runtime_.OpenBuffers({906, 2}));
    auto* buffers = runtime_.buffers();
    Activate();
    kbs_buffer_handle buffer{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 17, &buffer, nullptr), 0);
    kbs_buffer_view writable{};
    ASSERT_EQ(kbs_buffer_writable(buffers, buffer, &writable, nullptr), 0);
    for (std::size_t i = 0; i < writable.size; ++i) {
        static_cast<std::byte*>(writable.data)[i] = static_cast<std::byte>(i + 1);
    }
    const auto source_address = reinterpret_cast<std::uintptr_t>(writable.data) + 2;
    const auto before = runtime_.Inspect();
    ASSERT_TRUE(before);
    endpoints_[0].hold_tx = true;
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, buffer, 2, 8, &io, nullptr),
              0);
    EXPECT_EQ(io.bytes, 3u); // 公开表的 max_io_bytes=3，只接受有界前缀。
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.bytes, 17u);
    EXPECT_EQ(status.source_reusable, 0u);
    EXPECT_EQ(kbs_buffer_writable(buffers, buffer, &writable, nullptr), KBS_STREAM_BUSY);
    kbs_buffer_const_view readable{};
    EXPECT_EQ(kbs_buffer_readable(buffers, buffer, &readable, nullptr), KBS_STREAM_BUSY);
    io = {99, 1, 1};
    EXPECT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, buffer, 5, 3, &io, nullptr),
              KBS_STREAM_BUSY);
    EXPECT_EQ(io.bytes, 0u);
    EXPECT_EQ(io.eof, 0u);
    EXPECT_EQ(io.reserved, 0u);
    ASSERT_TRUE(Until([&] { return Status(1)->rx_pending != 0; }));
    EXPECT_EQ(endpoints_[0].last_source_address, source_address);
    ASSERT_NE(endpoints_[0].last_source_segment, nullptr);
    EXPECT_EQ(endpoints_[0].last_source_segment->urma_ctx, &endpoints_[0].context);
    EXPECT_LE(endpoints_[0].last_source_segment->seg.ubva.va, source_address);
    EXPECT_GE(endpoints_[0].last_source_segment->seg.ubva.va + endpoints_[0].last_source_segment->seg.len,
              source_address + 3);
    EXPECT_EQ(endpoints_[0].last_source_sges, 1u);
    std::array<std::byte, 3> wire{};
    ASSERT_EQ(
        kbs_stream_read(runtime_.table(1), endpoints_[1].streams[0], wire.data(), wire.size(), Now(), &io, nullptr), 0);
    EXPECT_EQ(wire, (std::array<std::byte, 3>{std::byte{3}, std::byte{4}, std::byte{5}}));
    // 远端已经读取仍不能替代本地 TX 终态；CPU 句柄一直保留且没有 payload staging 副本。
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 0u);
    ASSERT_FALSE(endpoints_[0].completed[0].empty());
    endpoints_[0].hold_tx = false;
    ASSERT_TRUE(Until([&] { return Status(0)->tx_pending == 0; }));
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    ASSERT_EQ(kbs_buffer_readable(buffers, buffer, &readable, nullptr), 0);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(readable.data) + 2, source_address);
    EXPECT_EQ(static_cast<const std::byte*>(readable.data)[16], std::byte{17});
    ASSERT_EQ(kbs_buffer_release(buffers, buffer, nullptr), 0);
}

TEST_F(RawStreamRuntimeTest, LeaseWriteRangeAndForeignSourceRejectBeforeQueueMutation) {
    ASSERT_TRUE(runtime_.OpenBuffers({907, 2}));
    auto* buffers = runtime_.buffers();
    Activate();
    kbs_buffer_handle buffer{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 17, &buffer, nullptr), 0);
    const auto before = runtime_.Inspect();
    ASSERT_TRUE(before);
    ASSERT_EQ(Status(0)->tx_pending, 0u);
    for (const auto [offset, length] : std::array<std::pair<std::size_t, std::size_t>, 4>{
             {{0, 0}, {17, 1}, {16, 2}, {std::numeric_limits<std::size_t>::max(), 2}}}) {
        kbs_stream_io io{99, 1, 1};
        EXPECT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, buffer, offset, length,
                                         &io, nullptr),
                  KBS_STREAM_INVALID_ARGUMENT);
        EXPECT_EQ(io.bytes, 0u);
        EXPECT_EQ(io.eof, 0u);
        EXPECT_EQ(io.reserved, 0u);
    }
    BufferBinding foreign;
    ASSERT_TRUE(foreign.OpenHost({KBS_BUFFER_ABI_VERSION, 0, 909, 1, 0, 64, 1u << 20}));
    const auto cpu_only = foreign.Acquire(17);
    ASSERT_TRUE(cpu_only);
    kbs_stream_io io{};
    EXPECT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], foreign.table(), *cpu_only, 0, 3, &io,
                                     nullptr),
              KBS_STREAM_INVALID_ARGUMENT);
    const auto after = runtime_.Inspect();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->slab.local_dma, before->slab.local_dma);
    EXPECT_EQ(Status(0)->tx_pending, 0u);
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    ASSERT_TRUE(foreign.Release(*cpu_only));
    ASSERT_TRUE(foreign.Close());
    ASSERT_EQ(kbs_buffer_release(buffers, buffer, nullptr), 0);
}

TEST_F(RawStreamRuntimeTest, CancellingQueuedLeaseReleasesSourceWithoutPosting) {
    ASSERT_TRUE(runtime_.OpenBuffers({910, 1}));
    auto* buffers = runtime_.buffers();
    Activate();
    kbs_buffer_handle buffer{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 3, &buffer, nullptr), 0);
    const auto sends = endpoints_[0].posted_sends;
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, buffer, 0, 3, &io, nullptr),
              0);
    ASSERT_EQ(kbs_stream_cancel(runtime_.table(0), endpoints_[0].streams[0], nullptr), 0);
    EXPECT_EQ(endpoints_[0].posted_sends, sends);
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    EXPECT_EQ(Status(0)->state, KBS_STREAM_FAILED);
    ASSERT_EQ(kbs_buffer_release(buffers, buffer, nullptr), 0);
}

TEST_F(RawStreamRuntimeTest, CancellingPostedLeaseKeepsSourceBusyUntilErrorCompletionRetires) {
    ASSERT_TRUE(runtime_.OpenBuffers({911, 1}));
    auto* buffers = runtime_.buffers();
    Activate();
    kbs_buffer_handle buffer{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 3, &buffer, nullptr), 0);
    endpoints_[0].hold_tx = true;
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, buffer, 0, 3, &io, nullptr),
              0);
    // READY/CREDIT 也有 TX CQE，必须等到本次 DATA 真正被 provider 接受。
    ASSERT_TRUE(Until([&] { return endpoints_[0].last_data_ticket != 0; }));
    ASSERT_EQ(kbs_stream_cancel(runtime_.table(0), endpoints_[0].streams[0], nullptr), 0);
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 0u);
    EXPECT_EQ(Status(0)->state, KBS_STREAM_FAILED);
    // 连接取消不是 DMA 终态；可信 status 8 CQE 到达后才允许重新访问源。
    auto& completions = endpoints_[0].completed[0];
    const auto data_cqe = std::find_if(completions.begin(), completions.end(),
                                       [&](const auto& cqe) { return cqe.user_ctx == endpoints_[0].last_data_ticket; });
    ASSERT_NE(data_cqe, completions.end());
    data_cqe->status = URMA_CR_REM_ACCESS_ABORT_ERR;
    endpoints_[0].hold_tx = false;
    ASSERT_TRUE(Until([&] { return Status(0)->tx_pending == 0; }));
    ASSERT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    EXPECT_EQ(Status(0)->state, KBS_STREAM_FAILED);
    ASSERT_EQ(kbs_buffer_release(buffers, buffer, nullptr), 0);
}

TEST_F(RawStreamRuntimeTest, ReturningCpuHandleWhilePostedKeepsDmaAliveAndInvalidatesQueries) {
    ASSERT_TRUE(runtime_.OpenBuffers({912, 1}));
    auto* buffers = runtime_.buffers();
    Activate();
    kbs_buffer_handle buffer{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 3, &buffer, nullptr), 0);
    endpoints_[0].hold_tx = true;
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, buffer, 0, 3, &io, nullptr),
              0);
    ASSERT_TRUE(Until([&] { return endpoints_[0].last_data_ticket != 0; }));
    const auto before = runtime_.Inspect();
    ASSERT_TRUE(before);
    ASSERT_EQ(kbs_buffer_release(buffers, buffer, nullptr), 0);
    const auto after = runtime_.Inspect();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->slab.local_dma, before->slab.local_dma);
    EXPECT_EQ(after->slab.available, before->slab.available);
    EXPECT_EQ(after->slab.cpu_leases + 1, before->slab.cpu_leases);
    kbs_buffer_status status{99, 1, 1};
    EXPECT_EQ(kbs_buffer_inspect(buffers, buffer, &status, nullptr), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(status.bytes, 0u);
    EXPECT_EQ(status.source_reusable, 0u);
    endpoints_[0].hold_tx = false;
    ASSERT_TRUE(Until([&] { return Status(0)->tx_pending == 0; }));
    EXPECT_EQ(runtime_.Inspect()->slab.local_dma, before->slab.local_dma - 1);
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
    kbs_buffer_status status{99, 1, 1};
    EXPECT_EQ(kbs_buffer_inspect(buffers, handle, &status, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(status.bytes, 0u);
    EXPECT_EQ(status.source_reusable, 0u);
    EXPECT_EQ(status.reserved, 0u);
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

class RawStreamRuntimeRndvTest : public RawStreamRuntimeTest {
protected:
    RawStreamRndvBudget rndv_budget_;

    void ConfigureRndv() {
        config_.enable_rndv = true;
        config_.slab = {64, 32768, 512};
        config_.rndv_target_slab = memory::SlabConfig{8, 32768, 64};
        config_.memory_budget_bytes = (64 + 8) * 32768;
        config_.segments = {4, 64, 2};
        config_.command_io_bytes = 32768;
        config_.command_payload_budget_bytes = 2 * 4 * 32768;
        rndv_budget_ = {.connections = 4,
                        .tx_slots = 8,
                        .receive_entries = 32,
                        .write_chunks = 64,
                        .notification_slots = 20,
                        .ingress_packets = 96,
                        .grant_bytes = 262144};
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = configs_[i];
            e.streams.capacity = 2;
            e.streams.max_io_bytes = 32768;
            e.rndv = RawStreamRndvConfig{.connection = {.tx_capacity = 2,
                                                        .receive_capacity = 8,
                                                        .notification_capacity = 5,
                                                        .ingress_packets = 8,
                                                        .max_chunks = 8,
                                                        .post_budget = 8,
                                                        .chunk_bytes = 4096,
                                                        .threshold = 16384,
                                                        .timeout_ns = 10000000000,
                                                        .progress_budget = 8},
                                         .limits = {.version = control::kRndvVersion,
                                                    .flags = control::kRndvSuccessfulWriteDone,
                                                    .max_write_bytes = 4096,
                                                    .max_transfer_bytes = 32768,
                                                    .max_grants = 2,
                                                    .window = 8,
                                                    .max_bytes = 65536},
                                         .arena = {71 + i, 1}};
        }
    }
    void SetUp() override {
        Initialize();
        ConfigureRndv();
        const auto opened = runtime_.Open(configs_, config_, rndv_budget_);
        opened_ = runtime_.stage() != runtime::RawStackStage::kEmpty;
        ASSERT_TRUE(opened) << "code=" << opened.error().code << " detail=" << opened.error().detail;
        AdmitPeers();
    }
    RawStreamPreparation Preparation(std::size_t i) override {
        auto prep = RawStreamRuntimeTest::Preparation(i);
        prep.reorder.max_resident_bytes = 262144;
        prep.rndv_peer_authorized = true;
        const auto lane = endpoints_[i].resources.domain.pool->lane(0);
        EXPECT_TRUE(lane);
        if (lane) {
            prep.rndv_lane = *lane;
        }
        return prep;
    }
    static std::vector<std::byte> Payload(std::size_t bytes, unsigned seed) {
        std::vector<std::byte> payload(bytes);
        for (std::size_t i = 0; i < bytes; ++i) {
            payload[i] = static_cast<std::byte>((i * 17 + seed) % 251);
        }
        return payload;
    }
    void WritePayload(std::size_t side, std::size_t pair, std::span<const std::byte> payload) {
        std::size_t offset = 0;
        ASSERT_TRUE(Until([&] {
            if (offset == payload.size()) {
                return true;
            }
            kbs_stream_io io{};
            kbs_stream_error error{};
            const auto result = kbs_stream_write(runtime_.table(side), endpoints_[side].streams[pair],
                                                 payload.data() + offset, payload.size() - offset, &io, &error);
            if (result == 0) {
                offset += io.bytes;
            } else {
                if (result != KBS_STREAM_WOULD_BLOCK && result != KBS_STREAM_BUSY) {
                    ADD_FAILURE() << "write side=" << side << " pair=" << pair << " code=" << result
                                  << " origin=" << error.origin << " native=" << error.native_code
                                  << " detail=" << error.detail << " underlying=" << error.underlying;
                    return true;
                }
            }
            return offset == payload.size();
        }));
        ASSERT_EQ(offset, payload.size());
    }
    void ReadPayload(std::size_t side, std::size_t pair, std::span<const std::byte> expected) {
        std::vector<std::byte> output(expected.size());
        std::size_t offset = 0;
        ASSERT_TRUE(Until([&] {
            if (offset == output.size()) {
                return true;
            }
            kbs_stream_io io{};
            kbs_stream_error error{};
            const auto result = kbs_stream_read(runtime_.table(side), endpoints_[side].streams[pair],
                                                output.data() + offset, output.size() - offset, Now(), &io, &error);
            if (result == 0) {
                EXPECT_EQ(io.eof, 0u);
                offset += io.bytes;
            } else {
                if (result != KBS_STREAM_WOULD_BLOCK && result != KBS_STREAM_BUSY) {
                    ADD_FAILURE() << "read side=" << side << " pair=" << pair << " code=" << result
                                  << " origin=" << error.origin << " native=" << error.native_code
                                  << " detail=" << error.detail << " underlying=" << error.underlying;
                    return true;
                }
            }
            return offset == output.size();
        }));
        ASSERT_EQ(offset, output.size());
        EXPECT_TRUE(std::equal(output.begin(), output.end(), expected.begin()));
    }
    void WaitIdle() {
        ASSERT_TRUE(Until([&] {
            const auto snapshot = runtime_.Inspect();
            if (!snapshot || runtime_.paused(0) || runtime_.paused(1)) {
                return false;
            }
            for (const auto& e : endpoints_) {
                if (!e.completed[0].empty() || !e.completed[1].empty() ||
                    e.sender_credits.stats().promised_control != 0) {
                    return false;
                }
            }
            return snapshot->rndv_target.available == snapshot->rndv_target.buffers && snapshot->segment_leases == 0;
        }));
    }
    void NoIdleCreditEcho() {
        // 先收敛已排队的窗口更新，再比较无新业务时的投递数，防止兄弟连接反复通知同一 Peer 授信。
        for (unsigned i = 0; i < 32; ++i) {
            ASSERT_TRUE(runtime_.Step());
        }
        const auto sends = endpoints_[0].posted_sends + endpoints_[1].posted_sends;
        for (unsigned i = 0; i < 32; ++i) {
            ASSERT_TRUE(runtime_.Step());
        }
        EXPECT_EQ(endpoints_[0].posted_sends + endpoints_[1].posted_sends, sends);
    }
};

TEST_F(RawStreamRuntimeRndvTest, AdvertisesRealSharedArenaAndRequiresIndependentRndvAuthorization) {
    AdoptPair();
    for (std::size_t side = 0; side < 2; ++side) {
        const auto peer = runtime_.Peer(endpoints_[side].streams[0]);
        ASSERT_TRUE(peer);
        EXPECT_NE(peer->capabilities & control::kHandshakeCapabilityWriteRndv, 0);
        EXPECT_EQ(peer->rndv.arena_count, 1u);
        EXPECT_EQ(peer->rndv.arenas[0].id, 72 - side);
        EXPECT_EQ(peer->rndv.arenas[0].max_grant_bytes, 32768u);
        EXPECT_EQ(peer->rndv.arenas[0].length, 8u * 32768);
        EXPECT_EQ(endpoints_[side].resources.connections->stats().capacity, 2u);
    }
    // 一般 Lane/描述符授权不能代替对成功 WRITE + DONE 契约及 Arena 的独立认证。
    auto prep = Preparation(0);
    prep.rndv_peer_authorized = false;
    const auto denied = runtime_.PrepareRaw(endpoints_[0].streams[0], prep);
    ASSERT_FALSE(denied);
    EXPECT_EQ(endpoints_[0].imports.load(), 0u);
    EXPECT_EQ(Status(0)->state, KBS_STREAM_PREPARING);
    prep = Preparation(0);
    prep.rndv_lane = Preparation(1).rndv_lane;
    EXPECT_FALSE(runtime_.PrepareRaw(endpoints_[0].streams[0], prep));
    prep = Preparation(0);
    ++prep.rndv_lane.generation;
    EXPECT_FALSE(runtime_.PrepareRaw(endpoints_[0].streams[0], prep));
    EXPECT_EQ(endpoints_[0].imports.load(), 0u);
    PreparePair();
    ActivePair();
    const auto snapshot = runtime_.Inspect();
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->budget_bytes, (64u + 8u) * 32768);
    EXPECT_EQ(snapshot->rndv_target.backing_bytes, 8u * 32768);
    EXPECT_EQ(snapshot->rndv_target.available, 8u);
}

TEST_F(RawStreamRuntimeRndvTest, CopiesBidirectionalMixedDataAndRetiresActualWriteCompletions) {
    Activate();
    auto first = Payload(16384, 13), reverse = Payload(24576, 27);
    const auto expected_first = first, expected_reverse = reverse, tail = Payload(17, 41);
    // 同一有序流交替经过 Eager 与 RNDV；复制写返回后原输入允许修改。
    Transfer();
    WritePayload(0, 0, first);
    std::fill(first.begin(), first.end(), std::byte{0});
    WritePayload(1, 0, reverse);
    std::fill(reverse.begin(), reverse.end(), std::byte{0});
    ReadPayload(1, 0, expected_first);
    ReadPayload(0, 0, expected_reverse);
    WritePayload(0, 0, tail);
    ReadPayload(1, 0, tail);
    WaitIdle();
    EXPECT_EQ(endpoints_[0].posted_writes, 4u);
    EXPECT_EQ(endpoints_[1].posted_writes, 6u);
    EXPECT_EQ(endpoints_[0].segment_imports.load(), 1u);
    EXPECT_EQ(endpoints_[1].segment_imports.load(), 1u);
    EXPECT_EQ(Status(0)->state, KBS_STREAM_RAW_ACTIVE);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_RAW_ACTIVE);
}

TEST_F(RawStreamRuntimeRndvTest, OrderedEofKeepsReverseRndvAvailableThroughThePublicStreamApi) {
    Activate();
    const auto payload = Payload(16384, 47), reverse = Payload(16384, 49);
    WritePayload(0, 0, payload);
    // FIN 可以紧跟尚未完成的 RNDV；它只能关闭本地写方向，不能取消已接受事务或反向 GRANT。
    ASSERT_EQ(kbs_stream_shutdown_write(runtime_.table(0), endpoints_[0].streams[0], nullptr), 0);
    ReadPayload(1, 0, payload);
    bool eof = false;
    ASSERT_TRUE(Until([&] {
        if (eof) {
            return true;
        }
        std::byte byte{};
        kbs_stream_io io{};
        const auto result = kbs_stream_read(runtime_.table(1), endpoints_[1].streams[0], &byte, 1, Now(), &io, nullptr);
        eof = result == 0 && io.eof;
        return eof;
    }));
    WritePayload(1, 0, reverse);
    ReadPayload(0, 0, reverse);
    WaitIdle();
    EXPECT_EQ(endpoints_[1].posted_writes, 4u);
    EXPECT_EQ(Status(1)->state, KBS_STREAM_RAW_ACTIVE);
}

TEST_F(RawStreamRuntimeRndvTest, MailboxCommandsUseAuthenticatedRuntimeHandlesInBothDirections) {
    Activate();
    std::array<runtime::OwnedConnection, 2> owned;
    std::array<runtime::ConnectionCommandTicket, 2> writes;
    const std::array payloads{Payload(16384, 51), Payload(16384, 79)};
    for (std::size_t side = 0; side < 2; ++side) {
        const auto resolved = runtime_.Resolve(endpoints_[side].streams[0]);
        ASSERT_TRUE(resolved);
        owned[side] = *resolved;
        const auto ticket =
            endpoints_[side].resources.commands->TrySubmit({.kind = runtime::ConnectionCommandKind::kWrite,
                                                            .connection = owned[side],
                                                            .token = 101 + side,
                                                            .bytes = 16384},
                                                           payloads[side]);
        ASSERT_TRUE(ticket);
        writes[side] = *ticket;
    }
    // 业务线程只入队，只有 Runtime 的真实单 owner 调度可以执行传输。
    EXPECT_EQ(endpoints_[0].posted_writes + endpoints_[1].posted_writes, 0u);
    ASSERT_TRUE(Until([&] { return endpoints_[0].commands == 1 && endpoints_[1].commands == 1; }));
    for (std::size_t side = 0; side < 2; ++side) {
        const auto result = endpoints_[side].resources.commands->TakeCompletion(writes[side]);
        ASSERT_TRUE(result);
        ASSERT_TRUE(result->result);
        EXPECT_EQ(result->result->bytes, 16384u);
    }
    for (std::size_t side = 0; side < 2; ++side) {
        std::array<std::byte, 16384> output{};
        bool received = false;
        // mailbox 读取不隐藏 WouldBlock；每次结果都领取后才重新提交。
        ASSERT_TRUE(Until([&] {
            if (received) {
                return true;
            }
            if (runtime_.paused(side)) {
                return false;
            }
            const auto ticket =
                endpoints_[side].resources.commands->TrySubmit({.kind = runtime::ConnectionCommandKind::kRead,
                                                                .connection = owned[side],
                                                                .token = 201 + side,
                                                                .bytes = 16384});
            EXPECT_TRUE(ticket);
            if (!ticket) {
                return false;
            }
            std::optional<runtime::ConnectionCommandCompletion> completion;
            EXPECT_TRUE(Until([&] {
                if (completion) {
                    return true;
                }
                const auto result = endpoints_[side].resources.commands->TakeCompletion(*ticket, output);
                if (result) {
                    completion = *result;
                }
                return completion.has_value();
            }));
            if (completion && completion->result) {
                EXPECT_EQ(completion->result->bytes, output.size());
                received = true;
            }
            return received;
        }));
        EXPECT_TRUE(std::equal(output.begin(), output.end(), payloads[1 - side].begin()));
    }
    WaitIdle();
    EXPECT_EQ(endpoints_[0].posted_writes, 4u);
    EXPECT_EQ(endpoints_[1].posted_writes, 4u);
}

TEST_F(RawStreamRuntimeRndvTest, SharedPeerAdmissionSerializesPromisesAcrossConnectionsAndReusesWindows) {
    Activate();
    AdoptPair();
    PreparePair(1);
    ActivePair(1);
    ASSERT_NO_FATAL_FAILURE(NoIdleCreditEcho());
    const auto payload = Payload(16384, 31);
    for (unsigned round = 0; round < 10; ++round) {
        SCOPED_TRACE(round);
        // 两个方向可独立前进；同 Peer 的第二条连接必须等待现有 DONE 承诺转为真实通知。
        for (std::size_t side = 0; side < 2; ++side) {
            WritePayload(side, 0, payload);
            // 在本方向的 Submit 后立即检查；推进另一 context 可能已经退休本方向的事务。
            EXPECT_EQ(endpoints_[side].sender_credits.stats().promised_control, 1u);
            kbs_stream_io io{99, 1, 1};
            EXPECT_EQ(kbs_stream_write(runtime_.table(side), endpoints_[side].streams[1], payload.data(),
                                       payload.size(), &io, nullptr),
                      KBS_STREAM_WOULD_BLOCK);
            EXPECT_EQ(io.bytes, 0u);
        }
        ASSERT_NO_FATAL_FAILURE(ReadPayload(1, 0, payload));
        ASSERT_NO_FATAL_FAILURE(ReadPayload(0, 0, payload));
        for (std::size_t side = 0; side < 2; ++side) {
            WritePayload(side, 1, payload);
        }
        ASSERT_NO_FATAL_FAILURE(ReadPayload(1, 1, payload));
        ASSERT_NO_FATAL_FAILURE(ReadPayload(0, 1, payload));
        WaitIdle();
    }
    NoIdleCreditEcho();
    // 每条连接超过协商窗口仍可前进，且兄弟连接复用同一 Arena 导入缓存。
    EXPECT_EQ(endpoints_[0].posted_writes, 80u);
    EXPECT_EQ(endpoints_[1].posted_writes, 80u);
    EXPECT_EQ(endpoints_[0].segment_imports.load(), 1u);
    EXPECT_EQ(endpoints_[1].segment_imports.load(), 1u);
    ClosePair(0);
    WritePayload(0, 1, payload);
    ReadPayload(1, 1, payload);
    WaitIdle();
}

TEST_F(RawStreamRuntimeRndvTest, LeaseWritePinsOriginalSourceUntilWriteRetirementAfterCpuReturn) {
    Activate();
    ASSERT_TRUE(runtime_.OpenBuffers({920, 2}));
    auto* buffers = runtime_.buffers();
    kbs_buffer_handle source{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, 16384, &source, nullptr), 0);
    kbs_buffer_view view{};
    ASSERT_EQ(kbs_buffer_writable(buffers, source, &view, nullptr), 0);
    const auto payload = Payload(view.size, 61);
    std::memcpy(view.data, payload.data(), payload.size());
    const auto address = reinterpret_cast<std::uintptr_t>(view.data);
    endpoints_[0].hold_write_tx = true;
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, source, 0, view.size, &io,
                                     nullptr),
              0);
    ASSERT_EQ(io.bytes, payload.size());
    ASSERT_TRUE(Until([&] { return endpoints_[0].posted_writes == 4 && !runtime_.paused(0); }));
    EXPECT_EQ(endpoints_[0].last_write_source, address + 3 * 4096);
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers, source, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 0u);
    const auto before = runtime_.Inspect();
    ASSERT_TRUE(before);
    ASSERT_EQ(kbs_buffer_release(buffers, source, nullptr), 0);
    // 归还 CPU 句柄不夺回已经投递的源；实际 WRITE CQE 以前不能复用 backing 或提前发 DONE。
    const auto after = runtime_.Inspect();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->slab.available, before->slab.available);
    EXPECT_EQ(after->slab.local_dma, before->slab.local_dma);
    EXPECT_EQ(kbs_buffer_inspect(buffers, source, &status, nullptr), KBS_STREAM_INVALID_HANDLE);
    endpoints_[0].hold_write_tx = false;
    ReadPayload(1, 0, payload);
    WaitIdle();
    EXPECT_GT(runtime_.Inspect()->slab.available, before->slab.available);
}

TEST_F(RawStreamRuntimeRndvTest, SegmentImportPauseAlsoGuardsHostResolutionAndApplicationIo) {
    Activate();
    const auto payload = Payload(16384, 67);
    endpoints_[0].block_segment_import = true;
    WritePayload(0, 0, payload);
    ASSERT_TRUE(Until([&] { return runtime_.paused(0) && endpoints_[0].segment_imports.load() == 1; }));
    // 段导入属于同一 context 的独占控制窗口；门面和宿主 mailbox 句柄解析不能绕过暂停。
    const auto resolved = runtime_.Resolve(endpoints_[0].streams[0]);
    ASSERT_FALSE(resolved);
    EXPECT_EQ(resolved.error().code, KBS_STREAM_BUSY);
    kbs_stream_io io{};
    EXPECT_EQ(
        kbs_stream_write(runtime_.table(0), endpoints_[0].streams[0], payload.data(), payload.size(), &io, nullptr),
        KBS_STREAM_BUSY);
    const auto snapshot = runtime_.Inspect();
    ASSERT_FALSE(snapshot);
    EXPECT_EQ(snapshot.error().code, KBS_STREAM_BUSY);
    EXPECT_EQ(endpoints_[0].posted_writes, 0u);
    endpoints_[0].block_segment_import = false;
    endpoints_[0].block_segment_import.notify_all();
    ReadPayload(1, 0, payload);
    WaitIdle();
    EXPECT_EQ(endpoints_[0].posted_writes, 4u);
}

TEST_F(RawStreamRuntimeRndvTest, BorrowedReceiveTargetSurvivesCloseUntilExplicitReturn) {
    Activate();
    const auto payload = Payload(16384, 73);
    WritePayload(0, 0, payload);
    kbs_receive_view borrowed{};
    ASSERT_TRUE(Until([&] {
        if (borrowed.size) {
            return true;
        }
        return kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[0], payload.size(), Now(), &borrowed,
                                     nullptr) == 0;
    }));
    ASSERT_EQ(borrowed.size, payload.size());
    EXPECT_EQ(std::memcmp(borrowed.data, payload.data(), payload.size()), 0);
    const auto before = runtime_.Inspect();
    ASSERT_TRUE(before);
    EXPECT_LT(before->rndv_target.available, before->rndv_target.buffers);
    EXPECT_NE(kbs_stream_close(runtime_.table(1), endpoints_[1].streams[0], nullptr), 0);
    // Close 既不隐式归还业务视图，也不能把目标 Arena 注销后留下悬挂地址。
    EXPECT_EQ(std::memcmp(borrowed.data, payload.data(), payload.size()), 0);
    EXPECT_EQ(runtime_.Inspect()->rndv_target.available, before->rndv_target.available);
    ASSERT_EQ(kbs_stream_release_read_lease(runtime_.table(1), borrowed.lease, nullptr), 0);
    ClosePair(0);
    EXPECT_EQ(runtime_.Inspect()->rndv_target.available, runtime_.Inspect()->rndv_target.buffers);
}

TEST_F(RawStreamRuntimeRndvTest, OccupiedConnectionBudgetPersistsThroughCloseUntilSourceRetirementAndRelease) {
    Activate();
    AdoptPair();
    PreparePair(1);
    ActivePair(1);
    const auto old = endpoints_[0].streams[0];
    const auto identity = runtime_.Resolve(old);
    ASSERT_TRUE(identity);
    ASSERT_EQ(kbs_stream_cancel(runtime_.table(0), old, nullptr), 0);
    ASSERT_TRUE(Until([&] { return kbs_stream_close(runtime_.table(0), old, nullptr) == 0; }));
    EXPECT_EQ(endpoints_[0].resources.connections->stats().occupied, 2u);
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    const auto fd = sockets[0];
    const RawStreamConnectOptions options{.session = 999,
                                          .expected_peer_authorization = 7,
                                          .expected_peer_runtime = 11,
                                          .compatible_peer = true,
                                          .deadline_ns = Now() + 10'000'000'000ull};
    EXPECT_FALSE(runtime_.AdoptProtocol(0, &sockets[0], options, Now()));
    EXPECT_EQ(sockets[0], fd);
    EXPECT_NE(::fcntl(fd, F_GETFD), -1);
    // 底层 owner 本身也只有两个槽，借用宿主入口不能绕过同一预算。
    const auto bypass = endpoints_[0].resources.connections->AdoptTcp(sockets[0]);
    ASSERT_FALSE(bypass);
    EXPECT_EQ(bypass.error().code, runtime::ConnectionOwnerErrorCode::kExhausted);
    ::close(sockets[0]);
    ::close(sockets[1]);
    ClosePair(0);
    EXPECT_FALSE(runtime_.Resolve(old));
    AdoptPair();
    PreparePair(2);
    ActivePair(2);
    const auto replacement = runtime_.Resolve(endpoints_[0].streams[2]);
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->slot, identity->slot);
    EXPECT_NE(replacement->generation, identity->generation);
    const auto payload = Payload(16384, 83);
    WritePayload(0, 2, payload);
    ReadPayload(1, 2, payload);
    WaitIdle();
}

class RawStreamRuntimeRndvOpenTest : public RawStreamRuntimeRndvTest {
protected:
    void SetUp() override {
        Initialize();
        ConfigureRndv();
    }
    void OpenConfigured() {
        const auto opened = runtime_.Open(configs_, config_, rndv_budget_);
        opened_ = runtime_.stage() != runtime::RawStackStage::kEmpty;
        ASSERT_TRUE(opened);
        AdmitPeers();
    }
};

TEST_F(RawStreamRuntimeRndvOpenTest, RejectsEachCrossEndpointBudgetBeforeAnyProviderSideEffect) {
    const std::array fields{&RawStreamRndvBudget::connections,        &RawStreamRndvBudget::tx_slots,
                            &RawStreamRndvBudget::receive_entries,    &RawStreamRndvBudget::write_chunks,
                            &RawStreamRndvBudget::notification_slots, &RawStreamRndvBudget::ingress_packets,
                            &RawStreamRndvBudget::grant_bytes};
    for (std::size_t i = 0; i < fields.size(); ++i) {
        SCOPED_TRACE(i);
        auto budget = rndv_budget_;
        --(budget.*fields[i]);
        const auto rejected = runtime_.Open(configs_, config_, budget);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code, KBS_STREAM_INVALID_ARGUMENT);
        EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kEmpty);
        EXPECT_EQ(endpoints_[0].cq_count + endpoints_[1].cq_count, 0u);
        EXPECT_EQ(endpoints_[0].segment_count + endpoints_[1].segment_count, 0u);
    }
    // 完整预算按两个 endpoint 的最坏占用总量填写后，同一门面仍可以成功打开。
    OpenConfigured();
}

TEST_F(RawStreamRuntimeRndvOpenTest, RejectsMissingOrUndersizedBackingBeforeProviderOwnership) {
    std::array<runtime::RawStackConfig, 5> invalid;
    invalid.fill(config_);
    invalid[0].enable_rndv = false;
    invalid[1].rndv_target_slab.reset();
    invalid[2].rndv_target_slab->buffer_count = 7;
    invalid[3].slab.buffer_count = 31;
    invalid[4].rndv_target_slab->buffer_size = 16384;
    for (const auto& config : invalid) {
        const auto rejected = runtime_.Open(configs_, config, rndv_budget_);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code, KBS_STREAM_INVALID_ARGUMENT);
        EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kEmpty);
        EXPECT_EQ(endpoints_[0].segment_count + endpoints_[1].segment_count, 0u);
    }
    auto duplicate = configs_;
    duplicate[1].rndv->arena = duplicate[0].rndv->arena;
    EXPECT_FALSE(runtime_.Open(duplicate, config_, rndv_budget_));
    EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kEmpty);
    // 不允许跨连接字节数乘加溢出后绕回较小值，伪装成满足预算。
    auto overflow = configs_;
    auto huge_budget = rndv_budget_;
    overflow[0].rndv->limits.max_bytes = std::numeric_limits<std::uint64_t>::max();
    huge_budget.grant_bytes = std::numeric_limits<std::uint64_t>::max();
    EXPECT_FALSE(runtime_.Open(overflow, config_, huge_budget));
    EXPECT_EQ(runtime_.stage(), runtime::RawStackStage::kEmpty);
    EXPECT_EQ(endpoints_[0].segment_count + endpoints_[1].segment_count, 0u);
    OpenConfigured();
}

TEST_F(RawStreamRuntimeRndvOpenTest, PeerWithoutRndvKeepsTheEstablishedEagerProtocol) {
    configs_[1].rndv.reset();
    OpenConfigured();
    Activate();
    const auto peer = runtime_.Peer(endpoints_[0].streams[0]);
    ASSERT_TRUE(peer);
    EXPECT_EQ(peer->capabilities & control::kHandshakeCapabilityWriteRndv, 0);
    const auto payload = Payload(16384, 97);
    WritePayload(0, 0, payload);
    ReadPayload(1, 0, payload);
    EXPECT_EQ(endpoints_[0].posted_writes + endpoints_[1].posted_writes, 0u);
    EXPECT_EQ(endpoints_[0].segment_imports.load() + endpoints_[1].segment_imports.load(), 0u);
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
