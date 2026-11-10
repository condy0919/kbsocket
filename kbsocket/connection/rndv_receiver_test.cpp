// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_receiver.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <new>
#include <span>
#include <variant>

#include <gtest/gtest.h>

#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

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

class RndvReceiverTest : public ::testing::Test {
protected:
    struct TrackedGrant {
        RndvReceiveHandle request;
        GrantReservation grant;
    };
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.register_seg = [](auto* context, auto* config) {
            auto& segment = active_->segments_[active_->registrations_++];
            segment.urma_ctx = context;
            segment.seg.ubva.eid = context->eid;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            segment.seg.attr.value = config->flag.value;
            segment.seg.token_id = static_cast<std::uint32_t>(active_->registrations_);
            return &segment;
        };
        functions.unregister_seg = [](auto*) {
            ++active_->unregistrations_;
            return URMA_SUCCESS;
        };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        context_.eid.raw[0] = 1;
        contexts_[0] = &context_;
    }
    void TearDown() override {
        count_allocations = false;
        // 测试拥有独立的 mock 远端：清理阶段先停止提交，再提供明确的零在途终结事实。
        // 应用已拿走的 delivery 必须由用例自己归还；这里不能伪造 receipt 抢回借用。
        (void)receiver_.Cancel();
        std::array<ReorderDelivery, 8> deliveries{};
        for (unsigned pass = 0; pass < 2 && reorder_.stats().buffered_frames; ++pass) {
            const auto drained = reorder_.Drain(deliveries);
            ASSERT_TRUE(drained);
            for (std::size_t i = 0; i < *drained; ++i) {
                Return(deliveries[i]);
            }
        }
        for (std::size_t i = 0; i < tracked_count_; ++i) {
            (void)receiver_.ConfirmTerminal(tracked_[i].request, Terminal(tracked_[i].grant.identity));
        }
        EXPECT_TRUE(receiver_.Close());
        EXPECT_TRUE(reorder_.Close());
        EXPECT_TRUE(grants_.Close());
        EXPECT_EQ(slab_.stats().remote_grants, 0u);
        EXPECT_EQ(slab_.stats().cpu_leases, 0u);
        EXPECT_TRUE(slab_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    static GrantBinding Binding() {
        return {11, 22, 33, 44, {55, 66}};
    }
    static WireSessionBinding Route() {
        return {{kWireVersion, 11, 33, 44}, {55, 66}, 99};
    }
    static ReorderSession Session() {
        return {11, 33, 44, 55, 66};
    }
    static RndvReceiverConfig Config(std::uint32_t capacity = 8) {
        return {Binding(), Route(), capacity, true};
    }
    void Resources(std::uint32_t buffers = 16, std::uint32_t max_active = 8, std::uint64_t max_grant_bytes = 1024,
                   std::uint64_t gap_timeout = 1000000) {
        ASSERT_TRUE(
            slab_.Open(contexts_, {.buffer_count = buffers, .buffer_size = 128, .max_references = 32}, budget_));
        ASSERT_TRUE(grants_.Open(Binding(), {77, 88}, slab_, &context_,
                                 {.window = 8, .max_active = max_active, .max_bytes = max_grant_bytes}));
        ASSERT_TRUE(reorder_.Open(Session(), {.window = 8, .max_resident_bytes = 1024, .gap_timeout_ns = gap_timeout}));
    }
    void Open(std::uint32_t capacity = 8) {
        Resources();
        ASSERT_TRUE(receiver_.Open(Config(capacity), grants_, reorder_));
    }
    static GrantTransfer Transfer(std::uint32_t seq = 0, std::uint64_t bytes = 32) {
        return {Binding(), 100 + static_cast<std::uint64_t>(seq), seq, bytes};
    }
    static WireFrameInput RtsFrame(const GrantTransfer& transfer) {
        auto frame = BuildRndvRts(transfer, {22, 33, 44, {55, 66}});
        EXPECT_TRUE(frame);
        return frame ? *frame : WireFrameInput{};
    }
    static WireFrameInput DoneFrame(const GrantIdentity& identity) {
        // 模拟已经通过发送端全 WRITE 成功门禁的网络通知；wire 只带身份与长度，没有 WR 计数。
        auto frame = RtsFrame(identity.transfer);
        frame.control = WireControlType::kRndvDone;
        frame.rndv.arena_id = identity.arena.id;
        frame.rndv.arena_generation = identity.arena.generation;
        frame.rndv.grant_slot = identity.slot;
        frame.rndv.grant_generation = identity.generation;
        frame.rndv.target_address = identity.target_address;
        return frame;
    }
    static WireFrameView Decode(const WireFrameInput& frame) {
        std::array<std::byte, 112> encoded{};
        const auto wire = EncodeWireFrame(frame, encoded);
        EXPECT_TRUE(wire);
        if (!wire) {
            return {};
        }
        const auto view = DecodeWireFrame(wire->imm_data, std::span(encoded).first(wire->payload_bytes));
        EXPECT_TRUE(view);
        return view ? *view : WireFrameView{};
    }
    RndvReceiveHandle Rts(std::uint32_t seq = 0, std::uint64_t bytes = 32, std::uint64_t now = 1) {
        const auto received = receiver_.OnRts(Decode(RtsFrame(Transfer(seq, bytes))), Route().context, now);
        EXPECT_TRUE(received);
        return received ? *received : RndvReceiveHandle{};
    }
    GrantReservation Prepare(RndvReceiveHandle handle) {
        const auto grant = receiver_.PrepareGrant(handle);
        EXPECT_TRUE(grant);
        if (!grant) {
            return {};
        }
        if (tracked_count_ < tracked_.size()) {
            tracked_[tracked_count_++] = {handle, *grant};
        }
        return *grant;
    }
    GrantReservation Published(RndvReceiveHandle handle) {
        const auto grant = Prepare(handle);
        EXPECT_TRUE(grants_.CommitPublication(grant.handle));
        // 注册目标由 mock 远端写入，接收端完成通知前不通过 Borrow 访问目标。
        std::fill_n(reinterpret_cast<std::byte*>(grant.target.addr), grant.target.len, std::byte{0x5a});
        return grant;
    }
    void Done(const GrantReservation& grant, std::uint64_t now = 2) {
        ASSERT_TRUE(receiver_.OnDone(Decode(DoneFrame(grant.identity)), Route().context, now));
    }
    ReorderDelivery PollOne(std::uint64_t now = 3) {
        std::array<ReorderDelivery, 1> output{};
        const auto count = reorder_.Poll(output, now);
        EXPECT_TRUE(count);
        EXPECT_EQ(count ? *count : 0, 1u);
        return output[0];
    }
    memory::BufferLease PushEager(std::uint32_t seq, std::uint64_t now = 1) {
        const std::array payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
        const auto lease = slab_.Copy(payload);
        EXPECT_TRUE(lease);
        if (!lease) {
            return {};
        }
        const auto bytes = slab_.Readable(*lease);
        EXPECT_TRUE(bytes);
        EXPECT_TRUE(reorder_.Push(
            {Session(), seq, ReorderFrameKind::kEager, *lease, bytes ? *bytes : std::span<const std::byte>{}}, now));
        return *lease;
    }
    void Return(const ReorderDelivery& delivery) {
        if (std::holds_alternative<GrantHandle>(delivery.frame.lease)) {
            EXPECT_TRUE(receiver_.ReturnGrant(delivery));
        } else {
            if (const auto* lease = std::get_if<memory::BufferLease>(&delivery.frame.lease)) {
                EXPECT_TRUE(slab_.ReleaseCpu(*lease));
            }
            EXPECT_TRUE(reorder_.Release(delivery.receipt));
        }
    }
    static GrantTerminalEvidence Terminal(const GrantIdentity& identity) {
        return {.identity = identity, .no_future_writes = true, .acceptance_known = true};
    }
    static inline RndvReceiverTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    std::array<urma_context_t*, 1> contexts_{};
    std::array<urma_target_seg_t, 4> segments_{};
    std::size_t registrations_ = 0, unregistrations_ = 0;
    memory::MemoryBudget budget_{static_cast<std::size_t>(::sysconf(_SC_PAGESIZE)) * 4};
    memory::SlabAllocator slab_;
    GrantManager grants_;
    ReorderWindow reorder_;
    RndvReceiver receiver_;
    std::array<TrackedGrant, 64> tracked_{};
    std::size_t tracked_count_ = 0;
};

TEST_F(RndvReceiverTest, MatchesRequiresActualManagerWindowAndCompleteRoute) {
    Open();
    EXPECT_TRUE(receiver_.Matches(reorder_, Route()));
    EXPECT_TRUE(receiver_.Matches(grants_, reorder_, Route()));
    ReorderWindow other_window;
    GrantManager other_manager;
    EXPECT_FALSE(receiver_.Matches(other_window, Route()));
    EXPECT_FALSE(receiver_.Matches(other_manager, reorder_, Route()));
    // 不能仅凭共享 token/窗口接纳不同 epoch 或来源的入口。
    for (unsigned field = 0; field < 7; ++field) {
        auto route = Route();
        switch (field) {
        case 0:
            ++route.context.version;
            break;
        case 1:
            ++route.context.peer_runtime;
            break;
        case 2:
            ++route.context.receive_domain;
            break;
        case 3:
            ++route.context.session;
            break;
        case 4:
            ++route.token.slot;
            break;
        case 5:
            ++route.token.generation;
            break;
        case 6:
            ++route.credit_epoch;
            break;
        }
        EXPECT_FALSE(receiver_.Matches(reorder_, route));
    }
    EXPECT_TRUE(receiver_.Close());
    EXPECT_FALSE(receiver_.Matches(reorder_, Route()));
}

TEST_F(RndvReceiverTest, OpenRequiresTrustedPeerAndMatchingManagerRouteAndWindow) {
    Resources();
    for (unsigned field = 0; field < 8; ++field) {
        auto config = Config();
        switch (field) {
        case 0:
            config.peer_successful_write_done = false;
            break;
        case 1:
            ++config.binding.receiver_runtime;
            break;
        case 2:
            ++config.route.context.peer_runtime;
            break;
        case 3:
            ++config.route.context.receive_domain;
            break;
        case 4:
            ++config.route.context.session;
            break;
        case 5:
            ++config.route.token.generation;
            break;
        case 6:
            config.capacity = 0;
            break;
        case 7:
            config.capacity = 9;
            break;
        }
        EXPECT_FALSE(receiver_.Open(config, grants_, reorder_));
        EXPECT_EQ(receiver_.stats().active, 0u);
        EXPECT_EQ(grants_.stats().active, 0u);
    }
    ASSERT_TRUE(receiver_.Open(Config(), grants_, reorder_));
    EXPECT_FALSE(receiver_.Open(Config(), grants_, reorder_));
    ASSERT_TRUE(receiver_.Close());
    EXPECT_FALSE(receiver_.Open(Config(), grants_, reorder_));
}

TEST_F(RndvReceiverTest, OpenRejectsForeignReorderSessionAndNonemptyGrantManager) {
    Resources();
    ReorderWindow foreign;
    auto session = Session();
    ++session.generation;
    ASSERT_TRUE(foreign.Open(session));
    EXPECT_FALSE(receiver_.Open(Config(), grants_, foreign));
    ASSERT_TRUE(foreign.Close());
    const auto external = grants_.Reserve(Transfer());
    ASSERT_TRUE(external);
    EXPECT_FALSE(receiver_.Open(Config(), grants_, reorder_));
    ASSERT_TRUE(grants_.Cancel(external->handle));
}

TEST_F(RndvReceiverTest, ReceiverExclusivelyBindsBothOwnersAndRollsBackFailedSecondBinding) {
    Open();
    RndvReceiver second;
    EXPECT_FALSE(second.Open(Config(), grants_, reorder_));
    EXPECT_TRUE(grants_.accepting());
    EXPECT_TRUE(reorder_.accepting());
    GrantManager other_grants;
    ReorderWindow other_reorder;
    ASSERT_TRUE(
        other_grants.Open(Binding(), {78, 88}, slab_, &context_, {.window = 8, .max_active = 8, .max_bytes = 1024}));
    ASSERT_TRUE(other_reorder.Open(Session(), {.window = 8}));
    // 第二个所有者不能抢占任一依赖；第二阶段绑定失败须撤销已取得的第一阶段绑定。
    EXPECT_FALSE(second.Open(Config(), other_grants, reorder_));
    EXPECT_FALSE(second.Open(Config(), grants_, other_reorder));
    EXPECT_TRUE(other_grants.accepting());
    EXPECT_TRUE(other_reorder.accepting());
    ASSERT_TRUE(second.Open(Config(), other_grants, other_reorder));
    ASSERT_TRUE(second.Close());
    ASSERT_TRUE(other_grants.Close());
    ASSERT_TRUE(other_reorder.Close());
    Done(Published(Rts()));
    Return(PollOne());
}

TEST_F(RndvReceiverTest, BoundOwnersRejectCloseWithoutStoppingAnOtherwiseUsableReceiver) {
    Open();
    EXPECT_FALSE(grants_.Close());
    EXPECT_FALSE(reorder_.Close());
    EXPECT_TRUE(grants_.accepting());
    EXPECT_TRUE(reorder_.accepting());
    // 空表也不能在协调器还持有绑定时销毁；拒绝不能顺带停止，正常收发仍须可进行。
    Done(Published(Rts()));
    Return(PollOne());
    ASSERT_TRUE(receiver_.Close());
    EXPECT_TRUE(grants_.Close());
    EXPECT_TRUE(reorder_.Close());
}

TEST_F(RndvReceiverTest, RtsChargesWholeMessageAndBlocksLaterEagerAndFinUntilDone) {
    Open();
    const auto request = Rts(0, 96);
    EXPECT_EQ(receiver_.stats().waiting_grant, 1u);
    EXPECT_EQ(reorder_.stats().pending_frames, 1u);
    EXPECT_EQ(reorder_.stats().resident_bytes, 96u);
    EXPECT_EQ(grants_.stats().active, 0u);
    PushEager(1);
    ASSERT_TRUE(reorder_.Push({Session(), 2, ReorderFrameKind::kFin, {}, {}}, 1));
    std::array<ReorderDelivery, 3> deliveries{};
    ASSERT_EQ(reorder_.Poll(deliveries, 1), 0u);
    EXPECT_FALSE(reorder_.eof());
    const auto grant = Published(request);
    Done(grant);
    ASSERT_EQ(reorder_.Poll(deliveries, 3), 3u);
    EXPECT_EQ(deliveries[0].frame.kind, ReorderFrameKind::kValidatedRmaDone);
    ASSERT_TRUE(std::holds_alternative<GrantHandle>(deliveries[0].frame.lease));
    EXPECT_EQ(std::get<GrantHandle>(deliveries[0].frame.lease), grant.handle);
    EXPECT_EQ(deliveries[0].frame.payload.size(), 96u);
    EXPECT_TRUE(std::all_of(deliveries[0].frame.payload.begin(), deliveries[0].frame.payload.end(),
                            [](std::byte value) { return value == std::byte{0x5a}; }));
    EXPECT_EQ(deliveries[1].frame.kind, ReorderFrameKind::kEager);
    EXPECT_EQ(deliveries[2].frame.kind, ReorderFrameKind::kFin);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    // Poll 不归还 CPU 或 Grant 配额，FIN 的 EOF 也须等全部前缀的真实归还。
    Return(deliveries[2]);
    Return(deliveries[1]);
    EXPECT_FALSE(reorder_.eof());
    Return(deliveries[0]);
    EXPECT_TRUE(reorder_.eof());
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 99u);
}

TEST_F(RndvReceiverTest, OutOfOrderDoneAndOutOfOrderReturnRespectSharedPrefix) {
    Open();
    const auto first = Published(Rts(0));
    PushEager(1);
    const auto third = Published(Rts(2));
    Done(third, 2);
    std::array<ReorderDelivery, 3> deliveries{};
    ASSERT_EQ(reorder_.Poll(deliveries, 2), 0u);
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    Done(first, 3);
    ASSERT_EQ(reorder_.Poll(deliveries, 4), 3u);
    Return(deliveries[2]);
    EXPECT_EQ(reorder_.stats().released_prefix, 0u);
    EXPECT_EQ(grants_.stats().window_begin, 0u);
    Return(deliveries[0]);
    EXPECT_EQ(reorder_.stats().released_prefix, 1u);
    Return(deliveries[1]);
    ASSERT_TRUE(receiver_.Progress(5));
    EXPECT_EQ(grants_.stats().window_begin, 3u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 67u);
}

TEST_F(RndvReceiverTest, OutOfOrderRtsReservesItsOriginalSequenceWithoutInventingEarlierRequests) {
    Open();
    // 同 SQ 的 SEND 本身不保证顺序；RTS 2 可先到并完成，但必须等待真实的 0/1 前缀。
    const auto later = Published(Rts(2));
    Done(later, 2);
    EXPECT_EQ(reorder_.stats().rcv_next, 0u);
    EXPECT_EQ(receiver_.stats().active, 1u);
    std::array<ReorderDelivery, 3> deliveries{};
    ASSERT_EQ(reorder_.Poll(deliveries, 2), 0u);
    const auto earlier = Published(Rts(0, 32, 3));
    PushEager(1, 3);
    Done(earlier, 4);
    ASSERT_EQ(reorder_.Poll(deliveries, 5), 3u);
    for (std::size_t i = 0; i < deliveries.size(); ++i) {
        EXPECT_EQ(deliveries[i].frame.sequence, i);
        Return(deliveries[i]);
    }
    EXPECT_EQ(reorder_.stats().released_prefix, 3u);
}

TEST_F(RndvReceiverTest, WrongRtsSourceAndMalformedFrameCannotReserveState) {
    Open();
    const auto frame = Decode(RtsFrame(Transfer()));
    for (unsigned field = 0; field < 4; ++field) {
        auto observed = Route().context;
        switch (field) {
        case 0:
            ++observed.version;
            break;
        case 1:
            ++observed.peer_runtime;
            break;
        case 2:
            ++observed.receive_domain;
            break;
        case 3:
            ++observed.session;
            break;
        }
        EXPECT_FALSE(receiver_.OnRts(frame, observed, 1));
    }
    auto malformed = frame;
    malformed.frame.rndv.bytes = 0;
    EXPECT_FALSE(receiver_.OnRts(malformed, Route().context, 1));
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(reorder_.stats().pending_frames, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    Rts();
}

TEST_F(RndvReceiverTest, DuplicateRtsAndLocalCapacityDoNotLoseAdmittedRequests) {
    Open(1);
    const auto request = Rts();
    EXPECT_FALSE(receiver_.OnRts(Decode(RtsFrame(Transfer())), Route().context, 1));
    EXPECT_FALSE(receiver_.OnRts(Decode(RtsFrame(Transfer(1))), Route().context, 1));
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(reorder_.stats().pending_frames, 1u);
    EXPECT_EQ(reorder_.stats().received_bytes, 32u);
    const auto grant = Prepare(request);
    const auto repeated = receiver_.PrepareGrant(request);
    ASSERT_TRUE(repeated);
    EXPECT_EQ(repeated->identity, grant.identity);
    EXPECT_EQ(repeated->handle, grant.handle);
    EXPECT_EQ(grants_.stats().active, 1u);
}

TEST_F(RndvReceiverTest, PrepareRejectsForeignAndStaleRequestHandles) {
    Open(1);
    const auto request = Rts();
    auto foreign = request;
    RndvReceiver other;
    foreign.owner = &other;
    EXPECT_FALSE(receiver_.PrepareGrant(foreign));
    auto stale = request;
    ++stale.generation;
    EXPECT_FALSE(receiver_.PrepareGrant(stale));
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(receiver_.stats().waiting_grant, 1u);
}

TEST_F(RndvReceiverTest, DoneBeforePublicationCannotEndReservedGrant) {
    Open();
    const auto request = Rts();
    const auto grant = Prepare(request);
    const auto done = Decode(DoneFrame(grant.identity));
    EXPECT_FALSE(receiver_.OnDone(done, Route().context, 2));
    EXPECT_EQ(grants_.stats().reserved, 1u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_EQ(reorder_.stats().pending_frames, 1u);
    ASSERT_TRUE(grants_.CommitPublication(grant.handle));
    ASSERT_TRUE(receiver_.OnDone(done, Route().context, 2));
    Return(PollOne());
}

TEST_F(RndvReceiverTest, EveryDoneIdentityFieldAndObservedSourceAreCheckedBeforeEndingGrant) {
    Open();
    const auto grant = Published(Rts());
    const auto good = DoneFrame(grant.identity);
    // 消息格式仍合法，但任何完整事务/授权字段被替换都不得解除远端访问责任。
    for (unsigned field = 0; field < 14; ++field) {
        auto changed = good;
        switch (field) {
        case 0:
            ++changed.token.generation;
            break;
        case 1:
            ++changed.session;
            break;
        case 2:
            ++changed.rndv.sender_runtime;
            break;
        case 3:
            ++changed.rndv.receiver_runtime;
            break;
        case 4:
            ++changed.rndv.receive_domain;
            break;
        case 5:
            ++changed.rndv.session;
            break;
        case 6:
            ++changed.rndv.token.slot;
            break;
        case 7:
            ++changed.rndv.transfer_id;
            break;
        case 8:
            ++changed.rndv.bytes;
            break;
        case 9:
            ++changed.rndv.arena_id;
            break;
        case 10:
            ++changed.rndv.arena_generation;
            break;
        case 11:
            ++changed.rndv.grant_slot;
            break;
        case 12:
            ++changed.rndv.grant_generation;
            break;
        case 13:
            ++changed.rndv.target_address;
            break;
        }
        EXPECT_FALSE(receiver_.OnDone(Decode(changed), Route().context, 2));
        EXPECT_EQ(grants_.stats().published, 1u);
        EXPECT_EQ(slab_.stats().remote_grants, 1u);
        EXPECT_EQ(reorder_.stats().pending_frames, 1u);
    }
    auto observed = Route().context;
    ++observed.peer_runtime;
    EXPECT_FALSE(receiver_.OnDone(Decode(good), observed, 2));
    Done(grant);
    EXPECT_FALSE(receiver_.OnDone(Decode(good), Route().context, 2));
    Return(PollOne());
    EXPECT_FALSE(receiver_.OnDone(Decode(good), Route().context, 4));
}

TEST_F(RndvReceiverTest, OnlyActualDeliveredGrantCanBeReturned) {
    Open();
    const auto grant = Published(Rts());
    Done(grant);
    // Grant 已借给共同窗口，但尚未 Poll；伪造首个 receipt 不能越过 Buffered 状态。
    ReorderDelivery forged{{Session(),
                            0,
                            ReorderFrameKind::kValidatedRmaDone,
                            grant.handle,
                            {reinterpret_cast<const std::byte*>(grant.target.addr), grant.target.len}},
                           {&reorder_, 0, 1},
                           false};
    EXPECT_FALSE(receiver_.ReturnGrant(forged));
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    const auto delivery = PollOne();
    for (unsigned field = 0; field < 9; ++field) {
        auto changed = delivery;
        switch (field) {
        case 0:
            changed.receipt.owner = nullptr;
            break;
        case 1:
            ++changed.receipt.generation;
            break;
        case 2:
            ++changed.frame.session.session;
            break;
        case 3:
            ++changed.frame.sequence;
            break;
        case 4:
            changed.frame.kind = ReorderFrameKind::kEager;
            break;
        case 5:
            changed.frame.payload = changed.frame.payload.subspan(1);
            break;
        case 6:
            changed.frame.payload = {changed.frame.payload.data() + 1, changed.frame.payload.size()};
            break;
        case 7:
            changed.discarded = true;
            break;
        case 8:
            ++std::get<GrantHandle>(changed.frame.lease).generation;
            break;
        }
        EXPECT_FALSE(receiver_.ReturnGrant(changed));
        EXPECT_EQ(grants_.stats().borrowed, 1u);
        EXPECT_EQ(slab_.stats().cpu_leases, 1u);
        EXPECT_EQ(reorder_.stats().consumed_bytes, 0u);
    }
    ASSERT_TRUE(receiver_.ReturnGrant(delivery));
    EXPECT_FALSE(receiver_.ReturnGrant(delivery));
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(reorder_.stats().released_prefix, 1u);
}

TEST_F(RndvReceiverTest, CancelBeforeGrantAbandonsOnlyMetadataAndDoesNotAdvanceConsumption) {
    Open();
    const auto request = Rts();
    ASSERT_TRUE(receiver_.Cancel());
    EXPECT_TRUE(receiver_.stats().stopped);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(reorder_.stats().pending_frames, 0u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 0u);
    EXPECT_EQ(reorder_.stats().released_prefix, 0u);
    EXPECT_FALSE(receiver_.PrepareGrant(request));
    EXPECT_FALSE(receiver_.OnRts(Decode(RtsFrame(Transfer(1))), Route().context, 2));
    ASSERT_TRUE(receiver_.Close());
}

TEST_F(RndvReceiverTest, CancelUnpublishedGrantReturnsTargetWithoutTerminalProof) {
    Open();
    const auto grant = Prepare(Rts());
    ASSERT_EQ(grants_.stats().reserved, 1u);
    ASSERT_TRUE(receiver_.Cancel());
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_FALSE(receiver_.OnDone(Decode(DoneFrame(grant.identity)), Route().context, 2));
    ASSERT_TRUE(receiver_.Close());
}

TEST_F(RndvReceiverTest, PublisherCancellationBeforeAnySendCanBeCollectedByReceiverCancel) {
    Open();
    const auto request = Rts();
    const auto grant = Prepare(request);
    // 唯一通知发布者在 provider 可能接受之前取消，所以管理器已经安全退休目标。
    // 接收协调器仍持有 RTS 占位，随后 Cancel 必须识别同代退休记录并完成元数据清理。
    ASSERT_TRUE(grants_.Cancel(grant.handle));
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(receiver_.stats().active, 1u);
    ASSERT_TRUE(receiver_.Cancel());
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(reorder_.stats().pending_frames, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_FALSE(receiver_.PrepareGrant(request));
    ASSERT_TRUE(receiver_.Close());
}

TEST_F(RndvReceiverTest, CancelPublishedGrantRetainsTargetUntilValidLateDoneWithoutDelivery) {
    Open();
    const auto grant = Published(Rts());
    ASSERT_TRUE(receiver_.Cancel());
    EXPECT_EQ(grants_.stats().quarantined, 1u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(reorder_.stats().pending_frames, 0u);
    EXPECT_FALSE(receiver_.Close());
    auto wrong = DoneFrame(grant.identity);
    ++wrong.rndv.grant_generation;
    EXPECT_FALSE(receiver_.OnDone(Decode(wrong), Route().context, 2));
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    // 取消不能复活业务。可信迟到 DONE 仅解除远端访问并丢弃，不进入共同交付窗口。
    Done(grant);
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_EQ(reorder_.stats().buffered_frames, 0u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 0u);
    EXPECT_EQ(reorder_.stats().released_prefix, 0u);
    ASSERT_TRUE(receiver_.Close());
}

TEST_F(RndvReceiverTest, CancelBufferedGrantRequiresRealDrainDeliveryBeforeReturn) {
    Open();
    const auto grant = Published(Rts());
    Done(grant);
    ASSERT_TRUE(receiver_.Cancel());
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    EXPECT_FALSE(receiver_.Close());
    std::array<ReorderDelivery, 1> output{};
    ASSERT_EQ(reorder_.Drain(output), 1u);
    ASSERT_TRUE(output[0].discarded);
    ASSERT_TRUE(receiver_.ReturnGrant(output[0]));
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 0u);
    EXPECT_EQ(reorder_.stats().released_prefix, 0u);
    ASSERT_TRUE(receiver_.Close());
}

TEST_F(RndvReceiverTest, CloseCannotReclaimGrantAlreadyHeldByApplication) {
    Open();
    Done(Published(Rts()));
    const auto delivery = PollOne();
    const auto* address = delivery.frame.payload.data();
    EXPECT_FALSE(receiver_.Close());
    EXPECT_FALSE(grants_.Close());
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(*address, std::byte{0x5a});
    ASSERT_TRUE(receiver_.ReturnGrant(delivery));
    ASSERT_TRUE(receiver_.Close());
}

TEST_F(RndvReceiverTest, IndependentTerminalProofNeedsCancelledMatchingGrant) {
    Open();
    const auto request = Rts();
    const auto grant = Published(request);
    EXPECT_FALSE(receiver_.ConfirmTerminal(request, Terminal(grant.identity)));
    ASSERT_TRUE(receiver_.Cancel());
    auto wrong = Terminal(grant.identity);
    ++wrong.identity.transfer.transfer_id;
    EXPECT_FALSE(receiver_.ConfirmTerminal(request, wrong));
    auto uncertain = Terminal(grant.identity);
    uncertain.acceptance_known = false;
    EXPECT_FALSE(receiver_.ConfirmTerminal(request, uncertain));
    EXPECT_EQ(grants_.stats().quarantined, 1u);
    ASSERT_TRUE(receiver_.ConfirmTerminal(request, Terminal(grant.identity)));
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(reorder_.stats().buffered_frames, 0u);
    EXPECT_FALSE(receiver_.ConfirmTerminal(request, Terminal(grant.identity)));
}

TEST_F(RndvReceiverTest, TerminalProofCannotStealBorrowedGrantFromApplication) {
    Open();
    const auto request = Rts();
    const auto grant = Published(request);
    Done(grant);
    const auto delivery = PollOne();
    ASSERT_TRUE(receiver_.Cancel());
    EXPECT_FALSE(receiver_.ConfirmTerminal(request, Terminal(grant.identity)));
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    ASSERT_TRUE(receiver_.ReturnGrant(delivery));
}

TEST_F(RndvReceiverTest, GrantCapacityPressurePreservesRtsUntilEarlierApplicationReturn) {
    Resources(16, 1);
    ASSERT_TRUE(receiver_.Open(Config(), grants_, reorder_));
    const auto first = Published(Rts(0));
    const auto second_request = Rts(1);
    EXPECT_FALSE(receiver_.PrepareGrant(second_request));
    EXPECT_EQ(receiver_.stats().active, 2u);
    EXPECT_EQ(receiver_.stats().waiting_grant, 1u);
    EXPECT_EQ(reorder_.stats().pending_frames, 2u);
    Done(first);
    const auto delivery = PollOne();
    EXPECT_FALSE(receiver_.PrepareGrant(second_request));
    // 远端 DMA 已结束仍不足以归还准入额度，应用真实 Return 后第二个请求才能分配。
    Return(delivery);
    const auto second = Published(second_request);
    Done(second, 4);
    Return(PollOne(5));
    EXPECT_EQ(reorder_.stats().received_bytes, 64u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 64u);
}

TEST_F(RndvReceiverTest, SlabPressureDoesNotLoseRtsOrDuplicateItsSequenceCharge) {
    Resources(1);
    ASSERT_TRUE(receiver_.Open(Config(), grants_, reorder_));
    const auto held = slab_.Acquire(1);
    ASSERT_TRUE(held);
    const auto request = Rts();
    EXPECT_FALSE(receiver_.PrepareGrant(request));
    EXPECT_EQ(receiver_.stats().waiting_grant, 1u);
    EXPECT_EQ(reorder_.stats().received_bytes, 32u);
    EXPECT_EQ(reorder_.stats().pending_frames, 1u);
    ASSERT_TRUE(slab_.ReleaseCpu(*held));
    Done(Published(request));
    Return(PollOne());
    EXPECT_EQ(reorder_.stats().received_bytes, 32u);
}

TEST_F(RndvReceiverTest, ReusedLocalSlotAndGrantWindowRejectOldRequestAndDone) {
    Open(1);
    const auto first_request = Rts();
    const auto first = Published(first_request);
    Done(first);
    Return(PollOne());
    for (std::uint32_t seq = 1; seq < 8; ++seq) {
        PushEager(seq, 4);
        Return(PollOne(4));
    }
    ASSERT_TRUE(receiver_.Progress(4));
    EXPECT_EQ(grants_.stats().window_begin, 8u);
    const auto replacement_request = Rts(8, 32, 5);
    const auto replacement = Published(replacement_request);
    EXPECT_EQ(replacement_request.slot, first_request.slot);
    EXPECT_GT(replacement_request.generation, first_request.generation);
    EXPECT_EQ(replacement.handle.slot, first.handle.slot);
    EXPECT_GT(replacement.handle.generation, first.handle.generation);
    EXPECT_FALSE(receiver_.PrepareGrant(first_request));
    EXPECT_FALSE(receiver_.OnDone(Decode(DoneFrame(first.identity)), Route().context, 6));
    EXPECT_EQ(grants_.stats().published, 1u);
    Done(replacement, 6);
    Return(PollOne(7));
}

TEST_F(RndvReceiverTest, LongEagerPrefixAdvancesGrantWindowInBoundedRetries) {
    Open();
    for (std::uint32_t seq = 0; seq < 20; ++seq) {
        PushEager(seq, 1);
        Return(PollOne(1));
    }
    EXPECT_EQ(grants_.stats().window_begin, 0u);
    const auto request = Rts(20, 32, 2);
    // 每次最多推进原 Grant 的 8 个槽；第一次暂不可准入，RTS 占位及长度账保持不变。
    EXPECT_FALSE(receiver_.PrepareGrant(request));
    EXPECT_EQ(grants_.stats().window_begin, 8u);
    EXPECT_EQ(receiver_.stats().waiting_grant, 1u);
    EXPECT_EQ(reorder_.stats().pending_frames, 1u);
    const auto grant = Published(request);
    EXPECT_EQ(grants_.stats().window_begin, 16u);
    Done(grant, 3);
    Return(PollOne(4));
    ASSERT_TRUE(receiver_.Progress(5));
    EXPECT_EQ(grants_.stats().window_begin, 21u);
}

TEST_F(RndvReceiverTest, SuccessfulReceiverCloseDoesNotFailSharedWindowOrCloseExternalOwners) {
    Open();
    Done(Published(Rts()));
    Return(PollOne());
    ASSERT_TRUE(receiver_.Close());
    EXPECT_TRUE(reorder_.accepting());
    EXPECT_FALSE(reorder_.failure());
    EXPECT_TRUE(grants_.accepting());
    // 协调器已无目标责任，宿主的共同流可以继续处理 Eager；其生命周期不随适配器销毁。
    PushEager(1, 4);
    Return(PollOne(5));
}

TEST_F(RndvReceiverTest, SharedWindowFailureAfterRtsTurnsValidDoneIntoCleanup) {
    Open();
    const auto grant = Published(Rts());
    reorder_.Fail();
    // 其他共同流分支已经失败；接收有效 DONE 不能把旧 pending 重新变成可读数据。
    Done(grant);
    EXPECT_EQ(reorder_.stats().pending_frames, 0u);
    EXPECT_EQ(reorder_.stats().buffered_frames, 0u);
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_EQ(reorder_.stats().consumed_bytes, 0u);
}

TEST_F(RndvReceiverTest, GapTimeoutDuringDonePreservesFailureAndDiscardsValidatedTarget) {
    Resources(16, 8, 1024, 10);
    ASSERT_TRUE(receiver_.Open(Config(), grants_, reorder_));
    const auto grant = Published(Rts(0, 32, 1));
    const auto result = receiver_.OnDone(Decode(DoneFrame(grant.identity)), Route().context, 20);
    // CompleteRndv 推进真实时间后发现缺口超时；错误返回或已完成清理都不能留下可交付 Grant。
    (void)result;
    ASSERT_TRUE(receiver_.Progress(20));
    EXPECT_TRUE(reorder_.failure());
    EXPECT_EQ(reorder_.stats().buffered_frames, 0u);
    EXPECT_EQ(reorder_.stats().pending_frames, 0u);
    EXPECT_EQ(grants_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
}

TEST_F(RndvReceiverTest, WrongTimestampCanRetryWithoutReendingOrReborrowingGrant) {
    Open();
    const auto grant = Published(Rts(0, 32, 10));
    EXPECT_FALSE(receiver_.OnDone(Decode(DoneFrame(grant.identity)), Route().context, 9));
    ASSERT_TRUE(receiver_.Progress(11));
    const auto delivery = PollOne(12);
    EXPECT_EQ(delivery.frame.sequence, 0u);
    EXPECT_EQ(grants_.stats().borrowed, 1u);
    Return(delivery);
    EXPECT_EQ(grants_.stats().active, 0u);
}

TEST_F(RndvReceiverTest, RtsDonePollAndReturnHaveNoHotPathAllocationOrFree) {
    Open();
    const auto rts = Decode(RtsFrame(Transfer()));
    allocation_count = deallocation_count = 0;
    count_allocations = true;
    const auto request = receiver_.OnRts(rts, Route().context, 1);
    const auto grant = request ? receiver_.PrepareGrant(*request)
                               : std::expected<GrantReservation, RndvReceiveError>{
                                     std::unexpected(RndvReceiveError{RndvReceiveErrorCode::kInvalidState})};
    bool committed = false;
    if (grant) {
        committed = grants_.CommitPublication(grant->handle).has_value();
    }
    count_allocations = false;
    ASSERT_TRUE(request);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(committed);
    const auto done = Decode(DoneFrame(grant->identity));
    std::array<ReorderDelivery, 1> deliveries{};
    count_allocations = true;
    const auto completed = receiver_.OnDone(done, Route().context, 2);
    const auto polled = reorder_.Poll(deliveries, 3);
    const auto returned = polled && *polled == 1 ? receiver_.ReturnGrant(deliveries[0])
                                                 : std::expected<void, RndvReceiveError>{std::unexpected(
                                                       RndvReceiveError{RndvReceiveErrorCode::kInvalidState})};
    const auto progressed = receiver_.Progress(4);
    count_allocations = false;
    EXPECT_TRUE(completed);
    EXPECT_EQ(polled, 1u);
    EXPECT_TRUE(returned);
    EXPECT_TRUE(progressed);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(deallocation_count, 0u);
}

TEST_F(RndvReceiverTest, PublishedCancellationAndLateDoneCleanupHaveNoHotPathAllocationOrFree) {
    Open();
    const auto grant = Published(Rts());
    const auto done = Decode(DoneFrame(grant.identity));
    allocation_count = deallocation_count = 0;
    count_allocations = true;
    const auto cancelled = receiver_.Cancel();
    const auto completed = receiver_.OnDone(done, Route().context, 2);
    const auto progressed = receiver_.Progress(3);
    count_allocations = false;
    EXPECT_TRUE(cancelled);
    EXPECT_TRUE(completed);
    EXPECT_TRUE(progressed);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(deallocation_count, 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
}

} // namespace
} // namespace connection
} // namespace kbsocket
