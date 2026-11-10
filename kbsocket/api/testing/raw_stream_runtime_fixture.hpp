// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_TESTING_RAW_STREAM_RUNTIME_FIXTURE_HPP_
#define KBSOCKET_API_TESTING_RAW_STREAM_RUNTIME_FIXTURE_HPP_

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
#include "kbsocket/api/raw_stream_runtime.hpp"
#include "kbsocket/api/stream.h"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/control/rndv_capabilities.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/runtime/connection_mailbox.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/peer_credit_progress.hpp"
#include "kbsocket/runtime/raw_stack.hpp"
#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace api {
namespace test_support {
inline std::uint64_t Now() {
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
        // 只延迟指定的真实 CQE，不阻挡同 JFC 上的其他完成；旧 hold_* 保持原语义。
        bool defer_write_tx = false, defer_done_tx = false, defer_done_rx = false;
        std::deque<urma_cr_t> withheld_write_tx, withheld_done_tx, withheld_done_rx;
        std::optional<connection::WireControlType> fail_control;
        urma_cr_status_t control_failure = URMA_CR_REM_ACCESS_ABORT_ERR;
        // 接受第一个 WRITE 后返回无法定位 bad_wr 的失败，故意留下未知接受边界。
        bool unknown_write_acceptance = false;
        std::size_t posted_sends = 0;
        std::size_t posted_writes = 0;
        std::size_t write_post_calls = 0, max_writes_per_post = 0, max_write_bytes = 0, total_write_bytes = 0;
        std::size_t posted_rts = 0, posted_grants = 0, posted_done = 0, posted_credits = 0, posted_fins = 0;
        std::size_t modifies = 0, flushes = 0, deletes = 0, unregisters = 0;
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
        f.delete_jetty = [](auto* jetty) {
            ++Find(jetty->urma_ctx).deletes;
            return URMA_SUCCESS;
        };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.unregister_seg = [](auto* segment) {
            ++Find(segment->urma_ctx).unregisters;
            return URMA_SUCCESS;
        };
        f.modify_jetty = [](auto* jetty, auto*) {
            ++Find(jetty->urma_ctx).modifies;
            return URMA_SUCCESS;
        };
        f.flush_jetty = [](auto* jetty, int, auto*) {
            ++Find(jetty->urma_ctx).flushes;
            return 0;
        };
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
            std::size_t writes = 0;
            for (auto* next = wr; next; next = next->next) {
                writes += next->opcode == URMA_OPC_WRITE;
            }
            if (writes) {
                ++e.write_post_calls;
                e.max_writes_per_post = std::max(e.max_writes_per_post, writes);
            }
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
                    e.max_write_bytes = std::max(e.max_write_bytes, offset);
                    e.total_write_bytes += offset;
                    urma_cr_t tx{};
                    tx.status = URMA_CR_SUCCESS;
                    tx.flag.bs.jetty = 1;
                    tx.local_id = jetty->jetty_id.id;
                    tx.user_ctx = wr->user_ctx;
                    // 公共 CR opcode 描述接收操作；发送操作身份取自本地 WR 台账。
                    tx.opcode = URMA_CR_OPC_SEND;
                    e.write_tickets.push_back(wr->user_ctx);
                    (e.defer_write_tx ? e.withheld_write_tx : e.completed[0]).push_back(tx);
                    if (e.unknown_write_acceptance) {
                        e.unknown_write_acceptance = false;
                        return URMA_ENOMEM;
                    }
                    continue;
                }
                EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
                std::array<std::byte, 4096> encoded{};
                std::size_t encoded_bytes = 0;
                for (std::uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
                    const auto& source = wr->send.src.sge[i];
                    if (source.len > encoded.size() - encoded_bytes) {
                        *bad = wr;
                        return URMA_EINVAL;
                    }
                    std::memcpy(encoded.data() + encoded_bytes, reinterpret_cast<const void*>(source.addr), source.len);
                    encoded_bytes += source.len;
                }
                const auto frame = connection::DecodeWireFrame(wr->send.imm_data, {encoded.data(), encoded_bytes});
                const bool control = frame && frame->frame.type == connection::WireFrameType::kControl;
                const bool done = control && frame->frame.control == connection::WireControlType::kRndvDone;
                if (control) {
                    e.posted_rts += frame->frame.control == connection::WireControlType::kRndvRts;
                    e.posted_grants += frame->frame.control == connection::WireControlType::kRndvGrant;
                    e.posted_done += done;
                    e.posted_credits += frame->frame.control == connection::WireControlType::kCreditUpdate;
                    e.posted_fins += frame->frame.control == connection::WireControlType::kFin;
                }
                if (control && e.fail_control && *e.fail_control == frame->frame.control) {
                    // 已接受通知但没有远端 RX；错误 CQE 不能冒充远端 Grant 的终结证明。
                    e.fail_control.reset();
                    ++e.posted_sends;
                    urma_cr_t tx{};
                    tx.status = e.control_failure;
                    tx.flag.bs.jetty = 1;
                    tx.local_id = jetty->jetty_id.id;
                    tx.user_ctx = wr->user_ctx;
                    tx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                    e.completed[0].push_back(tx);
                    continue;
                }
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
                (done && e.defer_done_tx ? e.withheld_done_tx : e.completed[0]).push_back(tx);
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
                (done && peer.defer_done_rx ? peer.withheld_done_rx : peer.completed[1]).push_back(rx);
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
    static void Release(std::deque<urma_cr_t>& held, std::deque<urma_cr_t>& ready) {
        while (!held.empty()) {
            ready.push_back(held.front());
            held.pop_front();
        }
    }
    void ReleaseWriteCompletions(std::size_t side) {
        auto& e = endpoints_[side];
        e.defer_write_tx = false;
        Release(e.withheld_write_tx, e.completed[0]);
    }
    void ReleaseDoneTx(std::size_t side) {
        auto& e = endpoints_[side];
        e.defer_done_tx = false;
        Release(e.withheld_done_tx, e.completed[0]);
    }
    void ReleaseDoneRx(std::size_t side) {
        auto& e = endpoints_[side];
        e.defer_done_rx = false;
        Release(e.withheld_done_rx, e.completed[1]);
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
        for (std::size_t i = 0; i < endpoints_.size(); ++i) {
            ReleaseWriteCompletions(i);
            ReleaseDoneTx(i);
            ReleaseDoneRx(i);
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

class RawStreamRuntimeRndvTest : public RawStreamRuntimeTest {
protected:
    RawStreamRndvBudget rndv_budget_;

    virtual void ConfigureRndv() {
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

} // namespace test_support
} // namespace api
} // namespace kbsocket
#endif // KBSOCKET_API_TESTING_RAW_STREAM_RUNTIME_FIXTURE_HPP_
