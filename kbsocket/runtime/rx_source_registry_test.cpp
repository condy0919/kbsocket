// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/rx_source_registry.hpp"

#include <array>
#include <cstdlib>
#include <deque>
#include <new>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/runtime/connection_io.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
} // namespace
void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void operator delete(void* pointer) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    std::free(pointer);
}

namespace kbsocket {
namespace runtime {
namespace {

class RxSourceRegistryTest : public ::testing::Test {
protected:
    struct Posted {
        std::uint64_t id;
        std::span<std::byte> memory;
    };
    static inline RxSourceRegistryTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.create_jfc = [](auto*, auto*) { return active_->jfc_count_++ ? &active_->rx_ : &active_->tx_; };
        functions.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        functions.create_jetty = [](auto*, auto*) {
            return active_->jetty_count_++ ? &active_->lane_ : &active_->passive_;
        };
        functions.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        functions.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        functions.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        functions.register_seg = [](auto*, auto*) { return &active_->segment_; };
        functions.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        functions.post_jfr_wr = [](auto*, auto* first, auto** bad) {
            *bad = nullptr;
            for (auto* wr = first; wr; wr = wr->next) {
                const auto& sge = wr->src.sge[0];
                active_->posted_.push_back({wr->user_ctx, {reinterpret_cast<std::byte*>(sge.addr), sge.len}});
            }
            return URMA_SUCCESS;
        };
        functions.poll_jfc = [](auto*, int, auto* out) {
            if (!active_->completion_) {
                return 0;
            }
            *out = *active_->completion_;
            active_->completion_.reset();
            return 1;
        };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        passive_.jetty_id.id = 19;
        lane_.jetty_id.id = 20;
        jfr_.jfr_id.id = 23;
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 1024;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        raw::JettyPoolConfig config;
        config.tx_depth = 8;
        config.rx_depth = config.rx_cq_depth = 16;
        ASSERT_TRUE(pool_.Open(&context_, cap, config));
        ASSERT_TRUE(buffers_.Open(pool_));
        ASSERT_TRUE(buffers_.Refill(16));
        ASSERT_TRUE(credits_.Open({33, 16, 4, 8, 1}));
        ASSERT_TRUE(credits_.RecordInitialPosted(connection::CreditClass::kData, 12));
        ASSERT_TRUE(credits_.RecordInitialPosted(connection::CreditClass::kControl, 4));
        auto peer = credits_.Admit({22, 33, 77});
        ASSERT_TRUE(peer);
        peers_.push_back(*peer);
        route_.local_id = passive_.jetty_id.id;
        route_.remote_id.eid.raw[0] = 4;
        route_.remote_id.id = 71;
        route_.remote_id.uasid = 3;
        route_.token = {1, 1};
        route_.source = {{11, 33}, {connection::kWireVersion, 22, 33, 44}, *peer};
        ASSERT_TRUE(registry_.Open(Config()));
    }
    void TearDown() override {
        count_allocations = false;
        for (const auto handle : handles_) {
            auto removed = registry_.Remove(handle, Proof());
            if (!removed) {
                EXPECT_EQ(removed.error(), RxSourceError::kInvalidLease);
            }
        }
        EXPECT_EQ(registry_.size(), 0u);
        EXPECT_TRUE(registry_.Close());
        // 每个 mock Posted 都生成真实终结事件后归还，来源拒绝不等于释放 DMA 责任。
        for (const auto lease : leases_) {
            EXPECT_TRUE(buffers_.Release(lease));
        }
        while (!posted_.empty()) {
            auto event = Event(route_);
            ASSERT_TRUE(event.lease);
            EXPECT_TRUE(buffers_.Release(*event.lease));
        }
        EXPECT_EQ(buffers_.held(), 0u);
        EXPECT_TRUE(buffers_.Close());
        EXPECT_TRUE(pool_.Close());
        if (expect_credit_retained_) {
            // 未知来源的物理责任不能通过手工清零或关闭来掩盖。
            EXPECT_FALSE(credits_.Close());
            active_ = nullptr;
            return;
        }
        for (const auto peer : peers_) {
            EXPECT_TRUE(credits_.RetirePeer(peer, {true, true, true}));
        }
        EXPECT_TRUE(credits_.Close());
        active_ = nullptr;
    }
    RxSourceRegistryConfig Config(std::uint32_t capacity = 8) {
        return {&buffers_, 101, {11, 33}, capacity};
    }
    static RxSourceRetireProof Proof() {
        return {101, true, true, true};
    }
    RxSourceLease Register(const RxSourceRoute& route) {
        auto result = registry_.Register(route);
        EXPECT_TRUE(result);
        if (!result) {
            return {};
        }
        handles_.push_back(*result);
        return *result;
    }
    raw::RxBufferEvent Event(const RxSourceRoute& route) {
        EXPECT_FALSE(posted_.empty());
        if (posted_.empty()) {
            return {};
        }
        auto posted = posted_.front();
        posted_.pop_front();
        const std::array data{std::byte{0x56}};
        connection::WireFrameInput input;
        input.token = route.token;
        input.data = data;
        auto encoded = connection::EncodeWireFrame(input, posted.memory);
        EXPECT_TRUE(encoded);
        if (!encoded) {
            return {};
        }
        urma_cr_t cr{};
        cr.user_ctx = posted.id;
        cr.flag.bs.s_r = 1;
        cr.flag.bs.jetty = route.is_jetty;
        cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        cr.local_id = route.local_id;
        cr.remote_id = route.remote_id;
        cr.imm_data = encoded->imm_data;
        cr.completion_len = encoded->payload_bytes;
        completion_ = cr;
        raw::RxBufferEvent event;
        auto polled = buffers_.Poll(std::span(&event, 1));
        EXPECT_TRUE(polled);
        if (polled) {
            EXPECT_EQ(*polled, 1u);
        }
        return event;
    }
    raw::RxBufferEvent HeldEvent(const RxSourceRoute& route) {
        auto event = Event(route);
        if (event.lease) {
            leases_.push_back(*event.lease);
        }
        return event;
    }
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t passive_{}, lane_{};
    urma_target_seg_t segment_{};
    int jfc_count_ = 0, jetty_count_ = 0;
    std::deque<Posted> posted_;
    std::optional<urma_cr_t> completion_;
    raw::JettyPool pool_;
    raw::RxBufferPool buffers_;
    connection::ReceiveCreditDomain credits_;
    RxSourceRegistry registry_;
    RxSourceRoute route_;
    std::vector<RxSourceLease> handles_;
    std::vector<raw::RxBufferLease> leases_;
    std::vector<connection::CreditPeerHandle> peers_;
    bool expect_credit_retained_ = false;
};

TEST_F(RxSourceRegistryTest, MatchesActualEndpointAndToken) {
    Register(route_);
    auto event = HeldEvent(route_);
    auto source = registry_.Resolve(event);
    ASSERT_TRUE(source);
    EXPECT_EQ(source->peer_context, route_.source.peer_context);
    EXPECT_EQ(source->peer.id, route_.source.peer.id);
    EXPECT_EQ(source->local_domain.runtime_instance, 11u);
    EXPECT_EQ(registry_.buffers(), &buffers_);
    EXPECT_EQ(registry_.context_incarnation(), 101u);
}

TEST_F(RxSourceRegistryTest, RejectsSpoofedPhysicalIdentityEvenWithValidToken) {
    Register(route_);
    const auto event = HeldEvent(route_);
    // 保持合法 token/session 的帧不变，只更改 CQE 物理来源，不能借当前连接猜账。
    auto altered = event;
    ++altered.completion.remote_id.id;
    auto result = registry_.Resolve(altered);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), RxSourceError::kUnknownSource);
    altered = event;
    ++altered.completion.remote_id.eid.raw[0];
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    ++altered.completion.remote_id.uasid;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.local_id = lane_.jetty_id.id;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.local_id = 999;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.flag.bs.jetty = 0;
    EXPECT_FALSE(registry_.Resolve(altered));
}

TEST_F(RxSourceRegistryTest, RejectsNonReceiveAndMalformedEvents) {
    Register(route_);
    const auto event = HeldEvent(route_);
    auto altered = event;
    altered.completion.flag.bs.s_r = 0;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.status = URMA_CR_ACK_TIMEOUT_ERR;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.opcode = URMA_CR_OPC_SEND;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    ++altered.completion.completion_len;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.flag.bs.reserved = 1;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.flag.bs.suspend_done = 1;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    altered.completion.imm_data = 15ull << 60;
    auto malformed = registry_.Resolve(altered);
    ASSERT_FALSE(malformed);
    EXPECT_EQ(malformed.error(), RxSourceError::kMalformedFrame);
    altered = event;
    altered.error = raw::RxBufferError{raw::RxBufferErrorCode::kReceiveFailed, 0, std::nullopt};
    EXPECT_FALSE(registry_.Resolve(altered));
}

TEST_F(RxSourceRegistryTest, RejectsForeignMissingOrMismatchedLease) {
    Register(route_);
    const auto event = HeldEvent(route_);
    auto altered = event;
    altered.lease.reset();
    EXPECT_FALSE(registry_.Resolve(altered));
    raw::RxBufferPool other;
    altered = event;
    altered.lease->pool = &other;
    EXPECT_FALSE(registry_.Resolve(altered));
    altered = event;
    ++altered.lease->id;
    EXPECT_FALSE(registry_.Resolve(altered));
}

TEST_F(RxSourceRegistryTest, SharedLaneRoutesDistinctSessions) {
    Register(route_);
    auto second = route_;
    second.token = {2, 1000};
    second.source.peer_context.session = 88;
    Register(second);
    auto first_event = HeldEvent(route_);
    auto second_event = HeldEvent(second);
    auto first_source = registry_.Resolve(first_event);
    auto second_source = registry_.Resolve(second_event);
    ASSERT_TRUE(first_source);
    ASSERT_TRUE(second_source);
    EXPECT_EQ(first_source->peer_context.session, 44u);
    EXPECT_EQ(second_source->peer_context.session, 88u);
    EXPECT_EQ(first_source->peer.id, second_source->peer.id);
    auto unknown = route_;
    ++unknown.token.generation;
    auto missing = registry_.Resolve(HeldEvent(unknown));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), RxSourceError::kUnknownSource);
}

TEST_F(RxSourceRegistryTest, RejectsReinterpretingLaneAsAnotherPeer) {
    Register(route_);
    auto other = credits_.Admit({99, 33, 78});
    ASSERT_TRUE(other);
    peers_.push_back(*other);
    auto conflict = route_;
    conflict.token = {2, 1};
    conflict.source.peer_context.peer_runtime = 99;
    conflict.source.peer = *other;
    auto result = registry_.Register(conflict);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), RxSourceError::kIdentityConflict);
    // 改用域内另一接收 Jetty 也不能绕过同一发送 Lane 的旧身份责任。
    conflict.local_id = lane_.jetty_id.id;
    EXPECT_FALSE(registry_.Register(conflict));
    ++conflict.remote_id.id;
    Register(conflict);
}

TEST_F(RxSourceRegistryTest, RejectsInvalidAdmissionIdentityAndDuplicate) {
    Register(route_);
    auto duplicate = registry_.Register(route_);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error(), RxSourceError::kDuplicate);
    auto bad = route_;
    bad.token.generation = 0;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    ++bad.source.local_domain.runtime_instance;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    ++bad.source.peer_context.receive_domain;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    ++bad.source.peer_context.peer_runtime;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    bad.source.peer_context.session = 0;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    ++bad.source.peer_context.version;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    ++bad.source.peer.id;
    EXPECT_FALSE(registry_.Register(bad));
    bad = route_;
    bad.local_id = 999;
    EXPECT_FALSE(registry_.Register(bad));
}

TEST_F(RxSourceRegistryTest, SupportsJfrCompletionNamespace) {
    auto jfr = route_;
    jfr.local_id = jfr_.jfr_id.id;
    jfr.is_jetty = false;
    Register(jfr);
    auto result = registry_.Resolve(HeldEvent(jfr));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->peer_context.session, 44u);
}

TEST_F(RxSourceRegistryTest, OldSourceOutlivesLogicalRouteWithdrawal) {
    connection::ConnectionTable table;
    ASSERT_TRUE(table.Open({11, 33}, 2));
    auto route = table.Reserve();
    ASSERT_TRUE(route);
    route_.token = route->token;
    ASSERT_TRUE(table.Publish(*route, {route_.source.peer_context, route_.token, 55}, this));
    Register(route_);
    auto event = HeldEvent(route_);
    ASSERT_TRUE(table.Withdraw(*route));
    ASSERT_TRUE(table.Retire(*route));
    // 连接路由失效，但实际来源与 Credit tombstone 必须保留，迟到 RX 仍能扣原账。
    auto source = registry_.Resolve(event);
    ASSERT_TRUE(source);
    EXPECT_EQ(source->peer.id, route_.source.peer.id);
    ASSERT_TRUE(table.Close());
    auto premature = registry_.Remove(handles_[0], {});
    ASSERT_FALSE(premature);
    EXPECT_EQ(premature.error(), RxSourceError::kProofRequired);
    EXPECT_FALSE(registry_.Close());
}

TEST_F(RxSourceRegistryTest, RemovalRequiresCompleteProofAndRejectsStaleHandles) {
    const auto old = Register(route_);
    auto proof = Proof();
    --proof.context_incarnation;
    EXPECT_FALSE(registry_.Remove(old, proof));
    proof = Proof();
    proof.remote_sender_stopped = false;
    EXPECT_FALSE(registry_.Remove(old, proof));
    proof = Proof();
    proof.accepted_traffic_settled = false;
    EXPECT_FALSE(registry_.Remove(old, proof));
    proof = Proof();
    proof.receive_events_drained = false;
    EXPECT_FALSE(registry_.Remove(old, proof));
    ASSERT_TRUE(registry_.Remove(old, Proof()));
    ++route_.token.generation;
    const auto replacement = Register(route_);
    EXPECT_EQ(old.slot, replacement.slot);
    EXPECT_NE(old.id, replacement.id);
    auto stale = registry_.Remove(old, Proof());
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), RxSourceError::kInvalidLease);
}

TEST_F(RxSourceRegistryTest, CapacityAndRepeatedReusePreserveHashChains) {
    for (std::uint32_t i = 0; i < 8; ++i) {
        auto route = route_;
        route.token.slot = i;
        Register(route);
    }
    auto extra = route_;
    extra.token.slot = 8;
    auto exhausted = registry_.Register(extra);
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error(), RxSourceError::kExhausted);
    // 释放交错槽位后重新插入，不能因 bucket 链断裂丢失其余来源。
    for (std::size_t i = 0; i < 8; i += 2) {
        ASSERT_TRUE(registry_.Remove(handles_[i], Proof()));
        auto replacement = route_;
        replacement.token.slot = 100 + i;
        Register(replacement);
    }
    for (std::uint32_t i = 0; i < 8; ++i) {
        auto route = route_;
        route.token.slot = i % 2 ? i : 100 + i;
        EXPECT_TRUE(registry_.Resolve(HeldEvent(route)));
    }
}

TEST_F(RxSourceRegistryTest, ResolveAllocatesNothing) {
    Register(route_);
    const auto event = HeldEvent(route_);
    bool success = true;
    allocations = 0;
    count_allocations = true;
    for (int i = 0; i < 10000; ++i) {
        success &= registry_.Resolve(event).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(success);
    EXPECT_EQ(allocations, 0u);
}

TEST_F(RxSourceRegistryTest, OpenValidatesPoolIncarnationAndCannotReopen) {
    RxSourceRegistry empty;
    EXPECT_FALSE(empty.Resolve({}));
    EXPECT_FALSE(empty.Register(route_));
    EXPECT_FALSE(empty.Open({}));
    auto config = Config();
    config.context_incarnation = 0;
    EXPECT_FALSE(empty.Open(config));
    config = Config();
    config.capacity = 0;
    EXPECT_FALSE(empty.Open(config));
    config.capacity = RxSourceRegistry::kMaxCapacity + 1;
    EXPECT_FALSE(empty.Open(config));
    ASSERT_TRUE(empty.Open(Config()));
    ASSERT_TRUE(empty.Close());
    EXPECT_FALSE(empty.Open(Config()));
}

TEST_F(RxSourceRegistryTest, BridgeAccountsAndRefillsKnownStaleFrame) {
    Register(route_);
    connection::ConnectionTable routes;
    ASSERT_TRUE(routes.Open({11, 33}, 2));
    connection::EagerReceiveDomain receive;
    ASSERT_TRUE(receive.Open(buffers_, credits_, routes));
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.context = &context_}));
    ConnectionIoBinding io;
    ASSERT_TRUE(io.Open(progress, receive, registry_));
    EXPECT_FALSE(io.Open(progress, receive, registry_));
    ASSERT_TRUE(credits_.Grant(route_.source.peer, connection::CreditClass::kData, 1));
    // token 没有活动路由，但来源记录仍合法；物理账必须扣一次，再由实际 refill 恢复。
    auto event = Event(route_);
    ASSERT_TRUE(event.lease);
    ASSERT_TRUE(io.OnRx(event, 100));
    EXPECT_FALSE(receive.frozen());
    EXPECT_EQ(credits_.stats().physical_posted, 15u);
    EXPECT_EQ(receive.pending_refill(), 1u);
    EXPECT_EQ(io.stats().stale_route_drops, 1u);
    EXPECT_FALSE(io.first_error());
    auto refill = progress.Progress(ProgressKind::kRefill, 8, 200);
    ASSERT_TRUE(refill);
    EXPECT_EQ(credits_.stats().physical_posted, 16u);
    EXPECT_EQ(receive.pending_refill(), 0u);
    EXPECT_TRUE(progress.Close());
    EXPECT_TRUE(receive.Close());
    EXPECT_TRUE(routes.Close());
}

TEST_F(RxSourceRegistryTest, BridgeAccountsUnknownSourceAndFreezesDomain) {
    connection::ConnectionTable routes;
    ASSERT_TRUE(routes.Open({11, 33}, 2));
    connection::EagerReceiveDomain receive;
    ASSERT_TRUE(receive.Open(buffers_, credits_, routes));
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.context = &context_}));
    ConnectionIoBinding io;
    ASSERT_TRUE(io.Open(progress, receive, registry_));
    // 真实 CQE 已消费 RQE，但没有可信发送 Lane 记录；回调不能简单 return 丢掉 Lease。
    auto event = Event(route_);
    ASSERT_TRUE(event.lease);
    ConnectionIoBinding::RxCallback(&io, event);
    ASSERT_TRUE(io.first_error());
    EXPECT_EQ(io.first_error()->code, ConnectionIoErrorCode::kSource);
    ASSERT_TRUE(io.first_error()->source);
    EXPECT_EQ(*io.first_error()->source, RxSourceError::kUnknownSource);
    EXPECT_EQ(credits_.stats().physical_posted, 15u);
    EXPECT_EQ(credits_.stats().unattributed, 1u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(io.stats().rx_events, 1u);
    EXPECT_TRUE(receive.frozen());
    expect_credit_retained_ = true;
    EXPECT_TRUE(progress.Close());
    EXPECT_FALSE(receive.Close());
    EXPECT_TRUE(routes.Close());
}

TEST_F(RxSourceRegistryTest, BridgeRejectsUnpreparedAndMismatchedDependencies) {
    ConnectionIoBinding io;
    auto missing = io.OnRx({}, 0);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, ConnectionIoErrorCode::kNotReady);
    EXPECT_FALSE(io.OnTx({}));
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.context = &context_}));
    connection::EagerReceiveDomain receive;
    EXPECT_FALSE(io.Open(progress, receive, registry_));
    connection::ConnectionTable routes;
    ASSERT_TRUE(routes.Open({11, 33}, 2));
    ASSERT_TRUE(receive.Open(buffers_, credits_, routes));
    RxSourceRegistry missing_sources;
    EXPECT_FALSE(io.Open(progress, receive, missing_sources));
    EXPECT_TRUE(receive.Close());
    EXPECT_TRUE(progress.Close());
    EXPECT_TRUE(routes.Close());
}

TEST_F(RxSourceRegistryTest, BridgePropagatesStoppedRxToCreditWithoutReleasingHeldData) {
    connection::ConnectionTable routes;
    ASSERT_TRUE(routes.Open({11, 33}, 2));
    auto route = routes.Reserve();
    ASSERT_TRUE(route);
    route_.token = route->token;
    Register(route_);
    connection::EagerRxPacket held;
    ASSERT_TRUE(routes.Publish(*route, {route_.source.peer_context, route_.token, 55}, &held));
    connection::EagerReceiveDomain receive;
    ASSERT_TRUE(receive.Open(buffers_, credits_, routes));
    ConnectionProgress progress;
    ASSERT_TRUE(progress.Open({.context = &context_}));
    ConnectionIoBinding io;
    ASSERT_TRUE(io.Open(progress, receive, registry_));
    ASSERT_TRUE(credits_.Grant(route_.source.peer, connection::CreditClass::kData, 1));
    const auto event = Event(route_);
    ASSERT_TRUE(event.lease);
    auto keep = [](void* cookie, connection::EagerReceiveDomain&, const connection::WireFrameView&,
                   connection::EagerRxPacket packet,
                   std::uint64_t) noexcept -> std::expected<connection::EagerRxDisposition, connection::EagerRxError> {
        *static_cast<connection::EagerRxPacket*>(cookie) = packet;
        return connection::EagerRxDisposition::kHeld;
    };
    ASSERT_TRUE(receive.Dispatch(event, route_.source, keep, 100));
    ASSERT_EQ(receive.held(), 1u);
    ASSERT_FALSE(credits_.stats().frozen);

    // 模拟 raw 错误路径先停止 RX，再报告错误；Credit 必须立即冻结，Held 内存仍由应用归还。
    buffers_.Stop();
    ConnectionIoBinding::ErrorCallback(&io, {ProgressErrorCode::kRetained, 9});
    EXPECT_TRUE(credits_.stats().frozen);
    EXPECT_EQ(receive.held(), 1u);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(credits_.stats().held, 1u);
    ASSERT_TRUE(io.first_error());
    ASSERT_TRUE(io.first_error()->progress);
    EXPECT_EQ(io.first_error()->progress->code, ProgressErrorCode::kRetained);
    EXPECT_EQ(io.first_error()->progress->detail, 9);
    EXPECT_FALSE(credits_.Grant(route_.source.peer, connection::CreditClass::kData, 1));
    ASSERT_TRUE(receive.Release(held));
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(receive.pending_refill(), 1u);
    EXPECT_FALSE(receive.Close());
    expect_credit_retained_ = true;
    ASSERT_TRUE(routes.Withdraw(*route));
    ASSERT_TRUE(routes.Retire(*route));
    EXPECT_TRUE(progress.Close());
    EXPECT_TRUE(routes.Close());
}

} // namespace
} // namespace runtime
} // namespace kbsocket
