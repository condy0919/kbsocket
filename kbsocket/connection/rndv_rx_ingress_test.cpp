// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_rx_ingress.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <new>
#include <optional>
#include <span>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection_table.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/connection/rndv_notification_tx.hpp"
#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/connection/rndv_receiver.hpp"
#include "kbsocket/connection/tx_sequence_window.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/runtime/control_executor.hpp"
#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace {
// 只测 Open 后的数据路径；断言与 fixture 清理由计数区间之外执行。
thread_local bool count_allocations = false;
thread_local std::size_t allocation_count = 0;
thread_local std::size_t deallocation_count = 0;
void CountAllocation() {
    if (count_allocations) {
        ++allocation_count;
    }
}
void CountDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocation_count;
    }
}
} // namespace
void* operator new(std::size_t bytes) {
    CountAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    CountAllocation();
    void* pointer = nullptr;
    if (::posix_memalign(&pointer, static_cast<std::size_t>(alignment), bytes ? bytes : 1) != 0) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) {
    return ::operator new(bytes, alignment);
}
void operator delete(void* pointer) noexcept {
    CountDeallocation(pointer);
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace connection {
namespace {
class RndvIngressTest : public ::testing::Test {
protected:
    struct PostedRx {
        std::uint64_t id = 0;
        std::span<std::byte> memory;
    };
    struct PostedTx {
        std::uint64_t id = 0;
        WireFrameInput frame;
        bool completed = false;
    };
    static inline RndvIngressTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto*, auto*) { return active_->jfc_count_++ ? &active_->rx_ : &active_->tx_; };
        f.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        f.create_jetty = [](auto*, auto*) { return active_->jetty_count_++ ? &active_->jetty_ : &active_->passive_; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.register_seg = [](auto* context, auto* config) {
            auto& seg = active_->segments_[active_->segment_count_++];
            seg.urma_ctx = context;
            seg.seg.ubva.va = config->va;
            seg.seg.ubva.eid = context->eid;
            seg.seg.len = config->len;
            seg.seg.attr.bs.access = config->flag.bs.access;
            seg.seg.token_id = static_cast<std::uint32_t>(active_->segment_count_);
            return &seg;
        };
        f.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        f.import_jetty = [](auto* context, auto* descriptor, auto*) {
            auto& endpoint = active_->endpoint_;
            endpoint.urma_ctx = context;
            endpoint.id = descriptor->jetty_id;
            endpoint.trans_mode = descriptor->trans_mode;
            endpoint.tp_type = descriptor->tp_type;
            endpoint.type = descriptor->type;
            return &endpoint;
        };
        f.unimport_jetty = [](auto*) { return URMA_SUCCESS; };
        f.post_jfr_wr = [](auto*, auto* first, auto** bad) {
            *bad = nullptr;
            for (auto* wr = first; wr; wr = wr->next) {
                const auto& sge = wr->src.sge[0];
                auto& s = *active_;
                if (s.rx_end_ >= s.posted_rx_.size()) {
                    *bad = wr;
                    return URMA_ENOMEM;
                }
                s.posted_rx_[s.rx_end_++] = {wr->user_ctx, {reinterpret_cast<std::byte*>(sge.addr), sge.len}};
            }
            return URMA_SUCCESS;
        };
        f.post_jetty_send_wr = [](auto*, auto* first, auto** bad) {
            *bad = nullptr;
            for (auto* wr = first; wr; wr = wr->next) {
                auto& s = *active_;
                const auto& sge = wr->send.src.sge[0];
                const auto decoded =
                    DecodeWireFrame(wr->send.imm_data, {reinterpret_cast<const std::byte*>(sge.addr), sge.len});
                if (!decoded || s.tx_count_ >= s.posted_tx_.size()) {
                    *bad = wr;
                    return URMA_EINVAL;
                }
                s.posted_tx_[s.tx_count_++] = {wr->user_ctx, decoded->frame, false};
                // 测试远端立即完成授权区间内的全部 WRITE。DONE 仍由用例显式送达，
                // GRANT 本地 SEND CQE 独立推进，不能借远端成功提前释放通知源。
                if (decoded->frame.control == WireControlType::kRndvGrant) {
                    std::fill_n(reinterpret_cast<std::byte*>(decoded->frame.rndv.target_address),
                                decoded->frame.rndv.bytes, std::byte{0x5a});
                }
            }
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* jfc, int, auto* out) {
            auto& pending = jfc == &active_->rx_ ? active_->rx_cqe_ : active_->tx_cqe_;
            if (!pending) {
                return 0;
            }
            *out = *pending;
            pending.reset();
            return 1;
        };
        f.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        return f;
    }
    virtual std::uint32_t TargetBuffers() const noexcept {
        return 4;
    }
    void SetUp() override {
        active_ = this;
        jetty_.jetty_id.id = 19;
        passive_.jetty_id.id = 20;
        jfr_.jfr_id.id = 23;
        context_.eid.raw[15] = 1;
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        cap.max_write_size = 200ULL * 1024 * 1024;
        raw::JettyPoolConfig pool_config;
        pool_config.tx_depth = 64;
        pool_config.rx_depth = pool_config.rx_cq_depth = 32;
        pool_config.tx_cq_depth = 128;
        ASSERT_TRUE(pool_.Open(&context_, cap, pool_config));
        ASSERT_TRUE(ledger_.Open(pool_, 64));
        ASSERT_TRUE(sender_.Open(ledger_, true));
        ASSERT_TRUE(buffers_.Open(pool_, 32));
        ASSERT_TRUE(buffers_.Refill(32));
        ASSERT_TRUE(rx_credits_.Open({33, 32, 16, 2, 1}));
        ASSERT_TRUE(rx_credits_.RecordInitialPosted(CreditClass::kData, 16));
        ASSERT_TRUE(rx_credits_.RecordInitialPosted(CreditClass::kControl, 16));
        const auto peer = rx_credits_.Admit({11, 33, 77});
        ASSERT_TRUE(peer);
        source_ = {{22, 33}, {kWireVersion, 11, 33, 44}, *peer};
        ASSERT_TRUE(rx_credits_.Grant(*peer, CreditClass::kData, 16));
        ASSERT_TRUE(rx_credits_.Grant(*peer, CreditClass::kControl, 16));
        ASSERT_TRUE(routes_.Open({22, 33}, 4));
        const auto reserved = routes_.Reserve();
        ASSERT_TRUE(reserved);
        route_ = *reserved;
        route_binding_ = {source_.peer_context, route_.token, 55};
        ASSERT_TRUE(routes_.Publish(route_, route_binding_, this));
        ASSERT_TRUE(domain_.Open(buffers_, rx_credits_, routes_));
        const std::array contexts{&context_};
        ASSERT_TRUE(slab_.Open(contexts, {.buffer_count = 16, .buffer_size = 4096, .max_references = 64}, budget_));
        ASSERT_TRUE(target_slab_.Open(
            contexts, {.buffer_count = TargetBuffers(), .buffer_size = 4096, .max_references = 64}, budget_));
        ASSERT_TRUE(grants_.Open(Binding(), {77, 88}, target_slab_, &context_,
                                 {.window = 8, .max_active = 4, .max_bytes = 16384}));
        ASSERT_TRUE(window_.Open({11, 33, 44, route_.token.slot, route_.token.generation},
                                 {.window = 8, .max_resident_bytes = 16384, .gap_timeout_ns = 1000000}));
        ASSERT_TRUE(tx_credits_.Open({11, 34, 1}));
        ASSERT_TRUE(tx_credits_.Update({11, 34, 1}, 32, 32));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        ASSERT_TRUE(imports_.Open({.entries = 4, .leases = 8, .in_flight = 2}));
        endpoint_key_ = {.local_context = &context_,
                         .local_context_incarnation = 1,
                         .peer_runtime = 11,
                         .receive_domain = 34,
                         .endpoint_generation = 7,
                         .jetty_id = 40,
                         .authorization_identity = 99};
        endpoint_key_.eid[15] = 9;
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = 40;
        descriptor.jetty_id.eid.raw[15] = 9;
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        const auto acquired = imports_.Acquire({endpoint_key_, std::as_bytes(std::span(&descriptor, 1)), {}});
        ASSERT_TRUE(acquired);
        endpoint_lease_ = *acquired;
        ASSERT_TRUE(DriveImports());
        const auto lane = pool_.lane(0);
        ASSERT_TRUE(lane);
        ASSERT_TRUE(notifications_.Open({.connection_id = 202,
                                         .destination = {11, 34, 45, {56, 67}},
                                         .credit_identity = {11, 34, 1},
                                         .endpoint = endpoint_key_,
                                         .lane = *lane,
                                         .capacity = 4},
                                        sender_, slab_, imports_, endpoint_lease_, tx_credits_));
    }
    void TearDown() override {
        count_allocations = false;
        // 每个通知 SEND 都提供来自真实 ledger 的终结 CQE；取消不等于远端 WRITE 终结。
        (void)ingress_.Cancel();
        for (std::size_t i = 0; i < tx_count_; ++i) {
            if (!posted_tx_[i].completed) {
                CompleteTx(i);
            }
            auto frame = DoneFrame(i);
            std::array<std::byte, 112> payload{};
            const auto encoded = EncodeWireFrame(frame, payload);
            ASSERT_TRUE(encoded);
            const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(payload).first(encoded->payload_bytes));
            ASSERT_TRUE(decoded);
            // mock 远端已在 post 回调中完成写入，取消后到达的成功 DONE 只清理；重复 DONE 可被拒绝。
            (void)receiver_.OnDone(*decoded, route_binding_.context, ++now_);
        }
        mixed_stream_.Fail();
        if (use_mixed_stream_) {
            EXPECT_TRUE(mixed_stream_.Drain());
        }
        EXPECT_TRUE(mixed_stream_.Close());
        window_.Fail();
        std::array<ReorderDelivery, 8> deliveries;
        const auto drained = window_.Drain(deliveries);
        ASSERT_TRUE(drained);
        for (std::size_t i = 0; i < *drained; ++i) {
            EXPECT_TRUE(receiver_.ReturnGrant(deliveries[i]));
        }
        for (unsigned i = 0; i < 8; ++i) {
            (void)ingress_.Progress(++now_);
        }
        EXPECT_TRUE(ingress_.Close());
        EXPECT_TRUE(receiver_.Close());
        EXPECT_TRUE(notifications_.Close());
        EXPECT_TRUE(window_.Close());
        EXPECT_TRUE(grants_.Close());
        EXPECT_TRUE(domain_.Refill());
        EXPECT_TRUE(domain_.Close());
        while (rx_begin_ < rx_end_) {
            const auto event = CompleteRx(posted_rx_[rx_begin_++], 0, 1);
            ASSERT_TRUE(event.lease);
            EXPECT_TRUE(buffers_.Release(*event.lease));
        }
        EXPECT_TRUE(buffers_.Close());
        EXPECT_TRUE(sender_.Close());
        EXPECT_TRUE(ledger_.Close());
        EXPECT_TRUE(pool_.Close());
        EXPECT_TRUE(tx_credits_.Close());
        EXPECT_TRUE(imports_.Release(endpoint_lease_));
        imports_.StopAccepting();
        EXPECT_TRUE(DriveImports());
        EXPECT_TRUE(imports_.Close());
        EXPECT_TRUE(executor_.Close());
        EXPECT_EQ(slab_.stats().local_dma, 0u);
        EXPECT_EQ(target_slab_.stats().remote_grants, 0u);
        EXPECT_TRUE(slab_.Close());
        EXPECT_TRUE(target_slab_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    bool DriveImports() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            std::array<runtime::ControlTaskCompletion, 4> events;
            const auto count = executor_.Poll(events);
            if (!count) {
                return false;
            }
            for (std::size_t i = 0; i < *count; ++i) {
                if (!imports_.HandleCompletion(events[i])) {
                    return false;
                }
            }
            if (imports_.in_flight()) {
                std::this_thread::yield();
                continue;
            }
            const auto next = imports_.NextOperation();
            if (!next) {
                return false;
            }
            if (!*next) {
                return true;
            }
            const auto& op = **next;
            if (!imports_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true})) {
                return false;
            }
        }
        return false;
    }
    GrantBinding Binding() const {
        return {11, 22, 33, 44, route_.token};
    }
    RndvIngressConfig Config() const {
        return {.route = route_binding_,
                .rts_packets = 2,
                .grant_packets = 2,
                .done_packets = 2,
                .grant_jobs = 2,
                .on_grant = OnGrant,
                .grant_owner = const_cast<RndvIngressTest*>(this)};
    }
    void Open(std::uint32_t receiver_capacity = 2, std::optional<RndvIngressConfig> config = {}) {
        ASSERT_TRUE(receiver_.Open({Binding(), route_binding_, receiver_capacity, true}, grants_, window_));
        ASSERT_TRUE(ingress_.Open(config ? *config : Config(), domain_, receiver_, grants_, window_, notifications_));
    }
    WireFrameInput RtsFrame(std::uint32_t seq = 0) const {
        const auto frame = BuildRndvRts({Binding(), 100 + seq, seq, 32}, {22, 33, 44, route_.token});
        EXPECT_TRUE(frame);
        return frame ? *frame : WireFrameInput{};
    }
    WireFrameInput GrantFrame(std::uint32_t seq = 0) const {
        const GrantIdentity identity{{{22, 11, 34, 45, {56, 67}}, 200 + seq, seq, 32}, {78, 89}, 0, 1, 0x10000};
        const auto frame = BuildRndvGrant(identity, {22, 33, 44, route_.token});
        EXPECT_TRUE(frame);
        return frame ? *frame : WireFrameInput{};
    }
    WireFrameInput DoneFrame(std::size_t posted) const {
        auto frame = posted_tx_[posted].frame;
        frame.control = WireControlType::kRndvDone;
        frame.session = route_binding_.context.session;
        frame.token = route_.token;
        return frame;
    }
    raw::RxBufferEvent CompleteRx(const PostedRx& posted, std::uint64_t imm, std::size_t length) {
        urma_cr_t cqe{};
        cqe.flag.bs.s_r = 1;
        cqe.flag.bs.jetty = 1;
        cqe.local_id = passive_.jetty_id.id;
        cqe.user_ctx = posted.id;
        cqe.completion_len = length;
        cqe.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        cqe.imm_data = imm;
        rx_cqe_ = cqe;
        raw::RxBufferEvent event;
        const auto count = buffers_.Poll(std::span(&event, 1));
        EXPECT_TRUE(count);
        EXPECT_EQ(count ? *count : 0, 1u);
        return event;
    }
    std::expected<void, EagerRxError> Dispatch(const WireFrameInput& frame) {
        if (rx_begin_ >= rx_end_) {
            return std::unexpected(EagerRxError{EagerRxErrorCode::kReceiveFailed});
        }
        const auto posted = posted_rx_[rx_begin_++];
        const auto encoded = EncodeWireFrame(frame, posted.memory);
        EXPECT_TRUE(encoded);
        if (!encoded) {
            return std::unexpected(EagerRxError{EagerRxErrorCode::kMalformedFrame});
        }
        return domain_.Dispatch(CompleteRx(posted, encoded->imm_data, encoded->payload_bytes), source_, Handler,
                                ++now_);
    }
    static std::expected<EagerRxDisposition, EagerRxError> Handler(void* owner, EagerReceiveDomain& domain,
                                                                   const WireFrameView& frame, EagerRxPacket packet,
                                                                   std::uint64_t) noexcept {
        auto& self = *static_cast<RndvIngressTest*>(owner);
        self.last_packet_ = packet;
        const auto view = domain.View(packet);
        if (view) {
            self.last_observed_ = view->observed;
        }
        if (self.use_mixed_stream_ && frame.frame.type != WireFrameType::kControl) {
            const auto accepted = self.mixed_stream_.Accept(packet, ++self.now_);
            if (!accepted) {
                return std::unexpected(accepted.error());
            }
            return EagerRxDisposition::kHeld;
        }
        const auto accepted = self.ingress_.Accept(packet);
        if (!accepted) {
            self.accept_error_ = accepted.error().code;
            return std::unexpected(EagerRxError{EagerRxErrorCode::kReorderFailed});
        }
        self.accept_error_.reset();
        return EagerRxDisposition::kHeld;
    }
    static std::expected<void, RndvIngressError> OnGrant(void* owner, const WireFrameView& view,
                                                         const WireReceiveContext& observed, std::uint64_t) noexcept {
        auto& self = *static_cast<RndvIngressTest*>(owner);
        ++self.grant_calls_;
        self.callback_observed_ = observed;
        self.callback_fields_ = view.frame.rndv;
        if (self.grant_error_) {
            return std::unexpected(RndvIngressError{*self.grant_error_});
        }
        ++self.grant_consumed_;
        return {};
    }
    void Pump(std::size_t rounds = 4) {
        for (std::size_t i = 0; i < rounds; ++i) {
            ASSERT_TRUE(ingress_.Progress(++now_));
        }
    }
    void PostOne() {
        const auto posted = notifications_.Progress();
        ASSERT_TRUE(posted);
        ASSERT_EQ(*posted, 1u);
    }
    void CompleteTx(std::size_t packet) {
        ASSERT_LT(packet, tx_count_);
        ASSERT_FALSE(posted_tx_[packet].completed);
        urma_cr_t cqe{};
        cqe.flag.bs.jetty = 1;
        cqe.local_id = jetty_.jetty_id.id;
        cqe.user_ctx = posted_tx_[packet].id;
        tx_cqe_ = cqe;
        std::array<raw::TxCompletionEvent, 1> events;
        const auto polled = processor_.Poll(events);
        ASSERT_TRUE(polled);
        ASSERT_EQ(polled->count, 1u);
        ASSERT_TRUE(notifications_.OnCompletion(events[0]));
        posted_tx_[packet].completed = true;
    }
    void ReturnOne() {
        std::array<ReorderDelivery, 1> deliveries;
        const auto count = window_.Poll(deliveries, ++now_);
        ASSERT_TRUE(count);
        ASSERT_EQ(*count, 1u);
        EXPECT_EQ(deliveries[0].frame.payload.size(), 32u);
        EXPECT_TRUE(std::all_of(deliveries[0].frame.payload.begin(), deliveries[0].frame.payload.end(),
                                [](std::byte value) { return value == std::byte{0x5a}; }));
        ASSERT_TRUE(receiver_.ReturnGrant(deliveries[0]));
    }
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{}, passive_{};
    urma_target_jetty_t endpoint_{};
    std::array<urma_target_seg_t, 8> segments_{};
    std::size_t jfc_count_ = 0, jetty_count_ = 0, segment_count_ = 0;
    raw::JettyPool pool_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_, true};
    raw::RxBufferPool buffers_;
    ReceiveCreditDomain rx_credits_;
    ConnectionTable routes_;
    ConnectionRouteLease route_;
    EagerReceiveSource source_;
    WireSessionBinding route_binding_;
    EagerReceiveDomain domain_;
    memory::MemoryBudget budget_{1024 * 1024};
    memory::SlabAllocator slab_, target_slab_;
    GrantManager grants_;
    ReorderWindow window_;
    RndvReceiver receiver_;
    SenderCreditAccount tx_credits_;
    runtime::ControlExecutor executor_;
    control::EndpointImportCache imports_;
    control::EndpointImportKey endpoint_key_;
    control::ImportLease endpoint_lease_;
    RndvNotificationTx notifications_;
    RndvRxIngress ingress_;
    EagerRxStream mixed_stream_;
    bool use_mixed_stream_ = false;
    std::array<PostedRx, 256> posted_rx_{};
    std::array<PostedTx, 32> posted_tx_{};
    std::size_t rx_begin_ = 0, rx_end_ = 0, tx_count_ = 0;
    std::optional<urma_cr_t> rx_cqe_, tx_cqe_;
    EagerRxPacket last_packet_;
    WireReceiveContext last_observed_, callback_observed_;
    WireRndvFields callback_fields_;
    std::optional<RndvIngressErrorCode> accept_error_, grant_error_;
    std::size_t grant_calls_ = 0, grant_consumed_ = 0;
    std::uint64_t now_ = 0;
};

TEST_F(RndvIngressTest, EmptyIngressPinsReceiverAndNotificationDependenciesUntilClosed) {
    Open();
    EXPECT_FALSE(receiver_.Close());
    EXPECT_FALSE(notifications_.Close());
    RndvRxIngress second;
    EXPECT_FALSE(second.Open(Config(), domain_, receiver_, grants_, window_, notifications_));
    EXPECT_TRUE(ingress_.Close());
    EXPECT_TRUE(receiver_.Close());
    EXPECT_TRUE(notifications_.Close());
}

TEST_F(RndvIngressTest, RtsKeepsPhysicalCreditUntilAdmissionAndRefillKeepsAccountingHonest) {
    Open();
    ASSERT_TRUE(Dispatch(RtsFrame()));
    EXPECT_EQ(domain_.held(), 1u);
    EXPECT_EQ(rx_credits_.stats().held, 1u);
    EXPECT_EQ(rx_credits_.stats().physical_posted, 31u);
    EXPECT_EQ(rx_credits_.stats().data.granted, 15u);
    EXPECT_EQ(rx_credits_.stats().control.granted, 16u);
    Pump();
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(rx_credits_.stats().released, 1u);
    EXPECT_EQ(rx_credits_.stats().physical_posted, 31u);
    EXPECT_EQ(notifications_.stats().queued, 1u);
    EXPECT_EQ(grants_.stats().published, 0u);
    ASSERT_TRUE(domain_.Refill());
    EXPECT_EQ(rx_credits_.stats().physical_posted, 32u);
    EXPECT_EQ(rx_credits_.stats().released, 0u);
    // 补投只恢复可授信额度，不能增加已经发给对端的累计额度。
    EXPECT_EQ(rx_credits_.stats().data.granted, 15u);
    PostOne();
    EXPECT_EQ(grants_.stats().published, 1u);
    EXPECT_EQ(tx_count_, 1u);
    Pump();
    EXPECT_EQ(tx_count_, 1u);
}

TEST_F(RndvIngressTest, ReceiverPressureHoldsSecondRtsAndDoneReleasesItExactlyOnce) {
    Open(1);
    ASSERT_TRUE(Dispatch(RtsFrame(0)));
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    EXPECT_EQ(domain_.held(), 1u);
    PostOne();
    CompleteTx(0);
    Pump();
    ASSERT_EQ(tx_count_, 1u);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    // DONE 不受第二个 RTS 的容量压力阻塞，但应用归还之前不能复用接收事务槽。
    EXPECT_EQ(ingress_.stats().done_packets, 0u);
    EXPECT_EQ(receiver_.stats().in_reorder, 1u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    ReturnOne();
    Pump();
    EXPECT_EQ(ingress_.stats().rts_packets, 0u);
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(notifications_.stats().queued, 1u);
    PostOne();
    CompleteTx(1);
    ASSERT_TRUE(Dispatch(DoneFrame(1)));
    Pump();
    ReturnOne();
    Pump();
    EXPECT_EQ(tx_count_, 2u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().grant_jobs, 0u);
}

TEST_F(RndvIngressTest, GrantCallbackBackpressureRetainsActualPacketAndObservedSource) {
    Open();
    grant_error_ = RndvIngressErrorCode::kWouldBlock;
    ASSERT_TRUE(Dispatch(GrantFrame()));
    EXPECT_EQ(last_observed_, source_.peer_context);
    Pump();
    EXPECT_GT(grant_calls_, 0u);
    EXPECT_EQ(grant_consumed_, 0u);
    EXPECT_EQ(callback_observed_, source_.peer_context);
    EXPECT_EQ(callback_fields_, GrantFrame().rndv);
    EXPECT_EQ(domain_.held(), 1u);
    EXPECT_EQ(rx_credits_.stats().control.granted, 15u);
    grant_error_.reset();
    Pump();
    EXPECT_EQ(grant_consumed_, 1u);
    EXPECT_EQ(domain_.held(), 0u);
    const auto calls = grant_calls_;
    Pump();
    EXPECT_EQ(grant_calls_, calls);
}

TEST_F(RndvIngressTest, GrantPressureDoesNotConsumeDonePartitionOrBlockCompletedWrite) {
    auto config = Config();
    config.grant_packets = 1;
    config.done_packets = 1;
    Open(1, config);
    ASSERT_TRUE(Dispatch(RtsFrame()));
    Pump();
    PostOne();
    grant_error_ = RndvIngressErrorCode::kWouldBlock;
    ASSERT_TRUE(Dispatch(GrantFrame()));
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    EXPECT_EQ(ingress_.stats().grant_packets, 1u);
    EXPECT_EQ(ingress_.stats().done_packets, 0u);
    EXPECT_EQ(receiver_.stats().in_reorder, 1u);
    ReturnOne();
    // 远端 DONE 与应用归还均不能代替尚未收到的本地 GRANT SEND CQE。
    EXPECT_EQ(notifications_.stats().in_flight, 1u);
    EXPECT_EQ(ingress_.stats().grant_jobs, 1u);
    CompleteTx(0);
    grant_error_.reset();
    Pump();
    EXPECT_EQ(ingress_.stats().grant_jobs, 0u);
    EXPECT_EQ(notifications_.stats().active, 0u);
}

TEST_F(RndvIngressTest, BoundedRoundRobinEventuallyReachesDoneBehindBlockedGrant) {
    auto config = Config();
    config.rts_packets = config.grant_packets = config.done_packets = config.grant_jobs = 1;
    Open(1, config);
    ASSERT_TRUE(Dispatch(RtsFrame()));
    Pump();
    PostOne();
    grant_error_ = RndvIngressErrorCode::kWouldBlock;
    ASSERT_TRUE(Dispatch(GrantFrame()));
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    const auto calls = grant_calls_;
    for (unsigned i = 0; i < 4; ++i) {
        const auto step = ingress_.Progress(++now_, 1);
        ASSERT_TRUE(step);
    }
    EXPECT_LE(grant_calls_ - calls, 1u);
    EXPECT_EQ(receiver_.stats().in_reorder, 1u);
    EXPECT_EQ(ingress_.stats().done_packets, 0u);
    ReturnOne();
}

TEST_F(RndvIngressTest, PacketCapacityFailureDoesNotTakeRejectedPacketOrDropPreviouslyHeldCredit) {
    auto config = Config();
    config.grant_packets = 1;
    Open(1, config);
    grant_error_ = RndvIngressErrorCode::kWouldBlock;
    ASSERT_TRUE(Dispatch(GrantFrame(0)));
    EXPECT_FALSE(Dispatch(GrantFrame(1)));
    ASSERT_TRUE(accept_error_);
    EXPECT_EQ(*accept_error_, RndvIngressErrorCode::kCapacity);
    EXPECT_TRUE(ingress_.stats().stopped);
    // 被拒帧由 RX 域释放，原先 Held 帧只由 ingress 取消路径释放一次。
    EXPECT_EQ(domain_.held(), 1u);
    EXPECT_EQ(rx_credits_.stats().released, 1u);
    Pump();
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(rx_credits_.stats().released, 2u);
    EXPECT_EQ(grant_consumed_, 0u);
}

TEST_F(RndvIngressTest, FatalGrantCallbackIsNotRetriedAsCapacityPressure) {
    Open();
    grant_error_ = RndvIngressErrorCode::kWrongIdentity;
    ASSERT_TRUE(Dispatch(GrantFrame()));
    const auto progressed = ingress_.Progress(++now_);
    ASSERT_FALSE(progressed);
    EXPECT_TRUE(ingress_.stats().stopped);
    const auto calls = grant_calls_;
    Pump();
    EXPECT_EQ(grant_calls_, calls);
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(grant_consumed_, 0u);
}

TEST_F(RndvIngressTest, InvalidObservedRouteNeverReachesGrantCallback) {
    Open();
    ++source_.peer_context.session;
    EXPECT_FALSE(Dispatch(GrantFrame()));
    EXPECT_EQ(grant_calls_, 0u);
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(rx_credits_.stats().released, 1u);
    --source_.peer_context.session;
}

TEST_F(RndvIngressTest, CancellationKeepsPublishedTargetUntilLateDoneAndNotificationCqe) {
    Open();
    ASSERT_TRUE(Dispatch(RtsFrame()));
    Pump();
    PostOne();
    ASSERT_EQ(tx_count_, 1u);
    (void)ingress_.Cancel();
    EXPECT_FALSE(ingress_.Close());
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(target_slab_.stats().remote_grants, 1u);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(target_slab_.stats().remote_grants, 0u);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_FALSE(ingress_.Close());
    EXPECT_EQ(notifications_.stats().in_flight, 1u);
    CompleteTx(0);
    Pump();
    EXPECT_TRUE(ingress_.Close());
}

TEST_F(RndvIngressTest, CancellingUnpublishedGrantDoesNotNeedInventedRemoteEvidence) {
    Open();
    ASSERT_TRUE(Dispatch(RtsFrame()));
    Pump();
    ASSERT_EQ(notifications_.stats().queued, 1u);
    ASSERT_EQ(grants_.stats().reserved, 1u);
    (void)ingress_.Cancel();
    Pump();
    EXPECT_EQ(tx_count_, 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(notifications_.stats().active, 0u);
    EXPECT_EQ(target_slab_.stats().remote_grants, 0u);
    EXPECT_TRUE(ingress_.Close());
}

TEST_F(RndvIngressTest, PacketIdentityCannotBeAcceptedTwice) {
    Open();
    ASSERT_TRUE(Dispatch(RtsFrame()));
    const auto duplicate = ingress_.Accept(last_packet_);
    EXPECT_FALSE(duplicate);
    EXPECT_EQ(domain_.held(), 1u);
    Pump();
    EXPECT_LE(receiver_.stats().active, 1u);
    EXPECT_EQ(domain_.held(), 0u);
}

TEST_F(RndvIngressTest, ReceiverAndRouteMismatchDoesNotBindOrCancelHealthyDependencies) {
    ASSERT_TRUE(receiver_.Open({Binding(), route_binding_, 2, true}, grants_, window_));
    auto config = Config();
    ++config.route.context.session;
    EXPECT_FALSE(ingress_.Open(config, domain_, receiver_, grants_, window_, notifications_));
    EXPECT_FALSE(receiver_.stats().stopped);
    EXPECT_TRUE(window_.accepting());
    EXPECT_EQ(notifications_.stats().active, 0u);
    ASSERT_TRUE(ingress_.Open(Config(), domain_, receiver_, grants_, window_, notifications_));
}

TEST_F(RndvIngressTest, ZeroProgressBudgetDoesNotConsumeHeldFrame) {
    Open();
    ASSERT_TRUE(Dispatch(GrantFrame()));
    const auto stepped = ingress_.Progress(++now_, 0);
    EXPECT_FALSE(stepped);
    EXPECT_EQ(grant_calls_, 0u);
    EXPECT_EQ(domain_.held(), 1u);
    Pump();
    EXPECT_EQ(grant_consumed_, 1u);
}

TEST_F(RndvIngressTest, ControlCreditPressurePreservesPreparedGrantWithoutDuplicateAdmission) {
    Open();
    std::array<ControlCreditLease, 31> held{};
    for (auto& lease : held) {
        const auto reserved = tx_credits_.ReserveControl();
        ASSERT_TRUE(reserved);
        lease = *reserved;
    }
    ASSERT_TRUE(Dispatch(RtsFrame()));
    Pump();
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(grants_.stats().reserved, 1u);
    EXPECT_EQ(notifications_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().grant_jobs, 1u);
    // 没有新的物理额度时只等待，不将同一 RTS 重复占位，也不把静止状态报告为进度。
    const auto stalled = ingress_.Progress(++now_);
    ASSERT_TRUE(stalled);
    EXPECT_EQ(*stalled, 0u);
    ASSERT_TRUE(tx_credits_.ReleaseControl(held.back()));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(grants_.stats().reserved, 1u);
    EXPECT_EQ(notifications_.stats().active, 1u);
    PostOne();
    EXPECT_EQ(tx_count_, 1u);
    for (std::size_t i = 0; i + 1 < held.size(); ++i) {
        EXPECT_TRUE(tx_credits_.ReleaseControl(held[i]));
    }
}

TEST_F(RndvIngressTest, JobCapacityKeepsFurtherRtsInRxUntilEarlierNotificationRetires) {
    auto config = Config();
    config.grant_jobs = 1;
    Open(2, config);
    ASSERT_TRUE(Dispatch(RtsFrame(0)));
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    PostOne();
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    ReturnOne();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    CompleteTx(0);
    Pump();
    EXPECT_EQ(ingress_.stats().rts_packets, 0u);
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(notifications_.stats().queued, 1u);
    PostOne();
    EXPECT_EQ(tx_count_, 2u);
}

TEST_F(RndvIngressTest, UnsupportedGrantWithoutSenderHandlerFailsExplicitlyAndReturnsPhysicalCredit) {
    auto config = Config();
    config.on_grant = nullptr;
    config.grant_owner = nullptr;
    Open(2, config);
    EXPECT_FALSE(Dispatch(GrantFrame()));
    ASSERT_TRUE(accept_error_);
    EXPECT_EQ(*accept_error_, RndvIngressErrorCode::kUnsupportedFrame);
    EXPECT_TRUE(ingress_.stats().stopped);
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(rx_credits_.stats().released, 1u);
}

TEST_F(RndvIngressTest, WrongDoneCannotReleasePublishedTargetAndValidLateDoneStillCleansIt) {
    Open();
    ASSERT_TRUE(Dispatch(RtsFrame()));
    Pump();
    PostOne();
    auto wrong = DoneFrame(0);
    ++wrong.rndv.grant_generation;
    ASSERT_TRUE(Dispatch(wrong));
    EXPECT_FALSE(ingress_.Progress(++now_));
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(target_slab_.stats().remote_grants, 1u);
    Pump();
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(target_slab_.stats().remote_grants, 0u);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
}

TEST_F(RndvIngressTest, GrantCallbacksAndBoundedProgressDoNotAllocateOrFreeOnHotPath) {
    Open();
    grant_error_ = RndvIngressErrorCode::kWouldBlock;
    ASSERT_TRUE(Dispatch(GrantFrame()));
    allocation_count = deallocation_count = 0;
    count_allocations = true;
    const auto blocked = ingress_.Progress(++now_);
    grant_error_.reset();
    const auto accepted = ingress_.Progress(++now_);
    const auto idle = ingress_.Progress(++now_);
    count_allocations = false;
    ASSERT_TRUE(blocked);
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(idle);
    EXPECT_EQ(*blocked, 0u);
    EXPECT_GT(*accepted, 0u);
    EXPECT_EQ(*idle, 0u);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(deallocation_count, 0u);
    EXPECT_EQ(grant_consumed_, 1u);
    EXPECT_EQ(domain_.held(), 0u);
}

TEST_F(RndvIngressTest, OutOfOrderRtsCannotOccupyOnlyReceiverSlotBeforeMissingPredecessor) {
    Open(1);
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    ASSERT_TRUE(Dispatch(RtsFrame(0)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    PostOne();
    ASSERT_EQ(tx_count_, 1u);
    EXPECT_EQ(posted_tx_[0].frame.seq, 0u);
    CompleteTx(0);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    ReturnOne();
    Pump();
    PostOne();
    ASSERT_EQ(tx_count_, 2u);
    EXPECT_EQ(posted_tx_[1].frame.seq, 1u);
    CompleteTx(1);
    ASSERT_TRUE(Dispatch(DoneFrame(1)));
    Pump();
    ReturnOne();
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().rts_packets, 0u);
}

class RndvIngressSingleTargetTest : public RndvIngressTest {
protected:
    std::uint32_t TargetBuffers() const noexcept override {
        return 1;
    }
};

TEST_F(RndvIngressSingleTargetTest, OutOfOrderRtsCannotTakeOnlyTargetAwayFromStreamPredecessor) {
    Open(2);
    // 接收协调器可容纳两事务，但真实目标 Slab 只有一个 buffer，不能让高 SN 先占它。
    ASSERT_EQ(target_slab_.stats().available, 1u);
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    ASSERT_TRUE(Dispatch(RtsFrame(0)));
    Pump();
    PostOne();
    ASSERT_EQ(tx_count_, 1u);
    EXPECT_EQ(posted_tx_[0].frame.seq, 0u);
    EXPECT_EQ(target_slab_.stats().available, 0u);
    CompleteTx(0);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    EXPECT_EQ(notifications_.stats().queued, 0u);
    ReturnOne();
    Pump();
    PostOne();
    ASSERT_EQ(tx_count_, 2u);
    EXPECT_EQ(posted_tx_[1].frame.seq, 1u);
    CompleteTx(1);
    ASSERT_TRUE(Dispatch(DoneFrame(1)));
    Pump();
    ReturnOne();
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(target_slab_.stats().available, 1u);
}

TEST_F(RndvIngressTest, RtsWaitsForEarlierEagerFrameAndThenUsesCommonStreamPrefix) {
    Open(1);
    ASSERT_TRUE(mixed_stream_.Open(domain_, window_, route_binding_, &receiver_));
    use_mixed_stream_ = true;
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().rts_packets, 1u);
    const std::array payload{std::byte{0x31}, std::byte{0x32}, std::byte{0x33}};
    WireFrameInput eager;
    eager.token = route_.token;
    eager.seq = 0;
    eager.data = payload;
    ASSERT_TRUE(Dispatch(eager));
    Pump();
    EXPECT_EQ(ingress_.stats().rts_packets, 0u);
    std::array<std::byte, 3> copied{};
    const auto read = mixed_stream_.Read(copied, ++now_);
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, payload.size());
    EXPECT_EQ(copied, payload);
    PostOne();
    ASSERT_EQ(tx_count_, 1u);
    EXPECT_EQ(posted_tx_[0].frame.seq, 1u);
    CompleteTx(0);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    std::array<std::byte, 32> target{};
    const auto second = mixed_stream_.Read(target, ++now_);
    ASSERT_TRUE(second);
    EXPECT_EQ(second->bytes, target.size());
    EXPECT_TRUE(std::all_of(target.begin(), target.end(), [](std::byte value) { return value == std::byte{0x5a}; }));
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
}

TEST_F(RndvIngressTest, MissingPredecessorExpiresHeldRtsWithoutOccupyingReceiverOrTarget) {
    auto config = Config();
    config.gap_timeout_ns = 10;
    Open(1, config);
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    now_ = 100;
    const auto waiting = ingress_.Progress(now_);
    ASSERT_TRUE(waiting);
    EXPECT_EQ(*waiting, 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(grants_.stats().active, 0u);
    const auto before_deadline = ingress_.Progress(now_ + 9);
    ASSERT_TRUE(before_deadline);
    now_ += 10;
    const auto expired = ingress_.Progress(now_);
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, RndvIngressErrorCode::kGapTimeout);
    EXPECT_TRUE(ingress_.stats().stopped);
    Pump();
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(tx_count_, 0u);
}

TEST_F(RndvIngressTest, AlreadyReceivedPredecessorWaitingForApplicationIsNotMissingFrameTimeout) {
    auto config = Config();
    config.rts_packets = 3;
    config.gap_timeout_ns = 10;
    Open(1, config);
    // 三帧逆序到达；SN 0 占唯一接收槽，SN 1/2 都已在物理 RX 域中等待。
    ASSERT_TRUE(Dispatch(RtsFrame(2)));
    ASSERT_TRUE(Dispatch(RtsFrame(1)));
    ASSERT_TRUE(Dispatch(RtsFrame(0)));
    Pump();
    PostOne();
    ASSERT_EQ(posted_tx_[0].frame.seq, 0u);
    CompleteTx(0);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    Pump();
    std::array<ReorderDelivery, 1> application;
    const auto delivered = window_.Poll(application, ++now_);
    ASSERT_TRUE(delivered);
    ASSERT_EQ(*delivered, 1u);
    // 应用真正持有 SN 0 的 Grant；未归还导致容量不足，不是网络丢失前序帧。
    now_ += 100;
    const auto stalled = ingress_.Progress(now_);
    EXPECT_TRUE(stalled);
    EXPECT_FALSE(ingress_.stats().stopped);
    EXPECT_EQ(ingress_.stats().rts_packets, 2u);
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_TRUE(receiver_.ReturnGrant(application[0]));
    Pump();
    PostOne();
    ASSERT_EQ(tx_count_, 2u);
    EXPECT_EQ(posted_tx_[1].frame.seq, 1u);
    CompleteTx(1);
    ASSERT_TRUE(Dispatch(DoneFrame(1)));
    Pump();
    ReturnOne();
    Pump();
    PostOne();
    ASSERT_EQ(tx_count_, 3u);
    EXPECT_EQ(posted_tx_[2].frame.seq, 2u);
    CompleteTx(2);
    ASSERT_TRUE(Dispatch(DoneFrame(2)));
    Pump();
    ReturnOne();
    Pump();
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(ingress_.stats().rts_packets, 0u);
}

TEST_F(RndvIngressTest, AdvancingToDifferentMissingPredecessorStartsNewGapDeadline) {
    auto config = Config();
    config.gap_timeout_ns = 10;
    Open(1, config);
    ASSERT_TRUE(Dispatch(RtsFrame(2)));
    now_ = 100;
    ASSERT_TRUE(ingress_.Progress(now_));
    // 先扫描 SN 2 并把游标移到槽 1；下一步只接收 SN 0，避免暂停逻辑掩盖缺口变更。
    now_ = 108;
    ASSERT_TRUE(ingress_.Progress(now_, 1));
    ASSERT_TRUE(Dispatch(RtsFrame(0)));
    ASSERT_EQ(now_, 109u);
    ASSERT_TRUE(ingress_.Progress(now_, 1));
    now_ = 110;
    ASSERT_TRUE(ingress_.Progress(now_));
    EXPECT_FALSE(ingress_.stats().stopped);
    EXPECT_EQ(window_.stats().observed_prefix, 1u);
    PostOne();
    CompleteTx(0);
    ASSERT_TRUE(Dispatch(DoneFrame(0)));
    ASSERT_TRUE(ingress_.Progress(now_));
    std::array<ReorderDelivery, 1> application;
    const auto delivered = window_.Poll(application, ++now_);
    ASSERT_TRUE(delivered);
    ASSERT_EQ(*delivered, 1u);
    now_ = 119;
    const auto before_new_deadline = ingress_.Progress(now_);
    EXPECT_TRUE(before_new_deadline);
    EXPECT_FALSE(ingress_.stats().stopped);
    // SN 0 的旧截止时间已过，只有新缺口 SN 1 持续完整的 10ns 后才应失败。
    now_ = 120;
    const auto expired = ingress_.Progress(now_);
    EXPECT_FALSE(expired);
    if (!expired) {
        EXPECT_EQ(expired.error().code, RndvIngressErrorCode::kGapTimeout);
    }
    EXPECT_TRUE(ingress_.stats().stopped);
    EXPECT_TRUE(receiver_.ReturnGrant(application[0]));
    Pump();
    EXPECT_EQ(domain_.held(), 0u);
}

} // namespace
} // namespace connection
} // namespace kbsocket
