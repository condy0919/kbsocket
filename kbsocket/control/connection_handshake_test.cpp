// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/connection_handshake.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/control/rndv_capabilities.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
// Open 之外才启用计数，避免把 GTest 或控制路径的预分配计入发送热路径。
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
    }
}
void RecordDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocations;
    }
}
} // namespace
void* operator new(std::size_t bytes) {
    RecordAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    RecordAllocation();
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
    RecordDeallocation(pointer);
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
namespace control {
namespace {
using State = ConnectionHandshakeState;
using Code = ConnectionHandshakeErrorCode;

template <class T>
void ExpectError(const std::expected<T, ConnectionHandshakeError>& result, Code code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

HandshakeHello Hello(std::uint64_t runtime, bool raw = true) {
    HandshakeHello hello;
    hello.capabilities = kHandshakeCapabilityTcp;
    hello.identity = {.runtime = runtime,
                      .receive_domain = runtime + 100,
                      .endpoint_generation = 1,
                      .authorization = 7,
                      .session = runtime + 200,
                      .token = {.slot = 9, .generation = 1}};
    if (raw) {
        hello.capabilities |= kHandshakeCapabilityRaw;
        // 两端故意使用不同授信 epoch，避免把反向 Credit 与本地物理 RX 总账混为一谈。
        hello.credit_epoch = runtime + 300;
        hello.eid[0] = std::byte(runtime);
        hello.jetty_id = static_cast<std::uint32_t>(runtime);
        hello.tx_lanes.pool_epoch = 1;
        hello.tx_lanes.count = 1;
        hello.tx_lanes.lanes[0] = {hello.eid, 0, hello.jetty_id + 10, 1};
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = hello.jetty_id;
        std::memcpy(&descriptor.jetty_id.eid, hello.eid.data(), hello.eid.size());
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        hello.descriptor_bytes = sizeof(descriptor);
        std::memcpy(hello.descriptor.data(), &descriptor, sizeof(descriptor));
    }
    return hello;
}

HandshakeHello RndvHello(std::uint64_t runtime) {
    auto hello = Hello(runtime);
    hello.capabilities |= kHandshakeCapabilityWriteRndv;
    hello.rndv.limits = {.version = kRndvVersion,
                         .flags = kRndvSuccessfulWriteDone,
                         .max_write_bytes = 65536,
                         .max_transfer_bytes = 32768,
                         .max_grants = 8,
                         .window = 32,
                         .max_bytes = 262144};
    hello.rndv.arena_count = 1;
    hello.rndv.arenas[0] = {.id = runtime + 10,
                            .generation = runtime + 20,
                            .address = runtime * 0x100000,
                            .length = 0x100000,
                            .max_grant_bytes = 65536,
                            .eid = hello.eid,
                            .uasid = static_cast<std::uint32_t>(runtime + 30),
                            .token_id = static_cast<std::uint32_t>(runtime + 40),
                            .access = kRndvAccessRead | kRndvAccessWrite,
                            .token_policy = kRndvTokenNone};
    return hello;
}

connection::GrantIdentity RndvGrant(const ConnectionHandshake& handshake, std::uint32_t arena_index = 0) {
    const auto& peer = *handshake.peer();
    const auto& arena = peer.rndv.arenas[arena_index];
    return {.transfer = {.binding = {.sender_runtime = handshake.local().identity.runtime,
                                     .receiver_runtime = peer.identity.runtime,
                                     .receive_domain = peer.identity.receive_domain,
                                     .session = peer.identity.session,
                                     .token = peer.identity.token},
                         .transfer_id = 99,
                         .seq = 0,
                         .bytes = 16384},
            .arena = {arena.id, arena.generation},
            .slot = 0,
            .generation = 17,
            .target_address = arena.address + 64};
}

class ConnectionHandshakeTest : public ::testing::Test {
protected:
    void SetUp() override {
        current_ = this;
        raw::UrmaFunctions functions;
        functions.import_jetty = &Import;
        functions.unimport_jetty = &Unimport;
        override_.emplace(functions);
        ASSERT_TRUE(a_table_.Open({1, 101}, 4));
        ASSERT_TRUE(b_table_.Open({2, 102}, 4));
        ASSERT_TRUE(cache_.Open({.entries = 4, .leases = 32, .in_flight = 2}));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(a_.Close());
        EXPECT_TRUE(b_.Close());
        if (a_credit_) {
            EXPECT_TRUE(a_credits_.RetirePeer(*a_credit_, {true, true, true}));
        }
        if (b_credit_) {
            EXPECT_TRUE(b_credits_.RetirePeer(*b_credit_, {true, true, true}));
        }
        EXPECT_TRUE(a_credits_.Close());
        EXPECT_TRUE(b_credits_.Close());
        auto cache_closed = cache_.Close();
        if (!cache_closed) {
            DriveCache();
        }
        EXPECT_TRUE(cache_.Close());
        EXPECT_TRUE(executor_.Close());
        EXPECT_TRUE(a_table_.Close());
        EXPECT_TRUE(b_table_.Close());
        override_.reset();
        current_ = nullptr;
    }
    static urma_target_jetty_t* Import(urma_context_t* context, urma_rjetty_t* remote, urma_token_t*) {
        ++current_->import_calls_;
        if (current_->fail_import_) {
            errno = EACCES;
            return nullptr;
        }
        auto& target = context == &current_->a_context_ ? current_->a_target_ : current_->b_target_;
        target.urma_ctx = context;
        target.id = remote->jetty_id;
        return &target;
    }
    static urma_status_t Unimport(urma_target_jetty_t*) {
        ++current_->unimport_calls_;
        return URMA_SUCCESS;
    }
    void OpenPair(bool raw = true) {
        OpenPairHello(Hello(1, raw), Hello(2, raw));
    }
    void OpenPairHello(const HandshakeHello& left, const HandshakeHello& right) {
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        ConnectionHandshakeConfig config{
            .local = left, .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000};
        ASSERT_TRUE(a_.Open(sockets[0], a_table_, config, 0));
        config.local = right;
        ASSERT_TRUE(b_.Open(sockets[1], b_table_, config, 0));
        for (int i = 0; i < 100 && (!a_.peer() || !b_.peer()); ++i) {
            ASSERT_TRUE(a_.Progress(++now_));
            ASSERT_TRUE(b_.Progress(++now_));
        }
        ASSERT_EQ(a_.state(), State::kPreparing);
        ASSERT_EQ(b_.state(), State::kPreparing);
    }
    void Credits() {
        ASSERT_TRUE(a_credits_.Open(
            {.receive_domain = 101, .capacity = 32, .control_slots = 8, .peer_capacity = 4, .base_credits = 8}));
        ASSERT_TRUE(b_credits_.Open(
            {.receive_domain = 102, .capacity = 32, .control_slots = 8, .peer_capacity = 4, .base_credits = 8}));
        for (auto* domain : {&a_credits_, &b_credits_}) {
            ASSERT_TRUE(domain->RecordInitialPosted(connection::CreditClass::kData, 24));
            ASSERT_TRUE(domain->RecordInitialPosted(connection::CreditClass::kControl, 8));
        }
        auto a = a_credits_.Admit({2, 101, 301});
        auto b = b_credits_.Admit({1, 102, 302});
        ASSERT_TRUE(a);
        ASSERT_TRUE(b);
        a_credit_ = *a;
        b_credit_ = *b;
        ASSERT_TRUE(a_credits_.Grant(*a, connection::CreditClass::kData, 8));
        ASSERT_TRUE(a_credits_.Grant(*a, connection::CreditClass::kControl, 2));
        ASSERT_TRUE(b_credits_.Grant(*b, connection::CreditClass::kData, 8));
        ASSERT_TRUE(b_credits_.Grant(*b, connection::CreditClass::kControl, 2));
    }
    RawHandshakePreparation APreparation() {
        return {.imports = &cache_,
                .local_context = &a_context_,
                .local_context_incarnation = 1,
                .descriptor_compatible = true,
                .credits = &a_credits_,
                .credit_peer = *a_credit_,
                .reorder = {},
                .route_owner = &a_};
    }
    void PreparePair(bool rndv_authorized = false) {
        Credits();
        auto preparation = APreparation();
        preparation.rndv_peer_authorized = rndv_authorized;
        ASSERT_TRUE(a_.PrepareRaw(preparation));
        preparation.local_context = &b_context_;
        preparation.credits = &b_credits_;
        preparation.credit_peer = *b_credit_;
        preparation.route_owner = &b_;
        ASSERT_TRUE(b_.PrepareRaw(preparation));
    }
    void DriveCache() {
        // 控制线程真的运行 Mock import；本测试没有 raw post/poll，因而完整满足 context 暂停屏障。
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        do {
            auto next = cache_.NextOperation();
            ASSERT_TRUE(next);
            if (*next) {
                auto operation = **next;
                ASSERT_TRUE(cache_.StartOperation(operation, executor_,
                                                  {operation.context, operation.context_incarnation, true, true}));
            }
            std::array<runtime::ControlTaskCompletion, 4> completions;
            auto count = executor_.Poll(completions);
            ASSERT_TRUE(count);
            for (std::size_t i = 0; i < *count; ++i) {
                ASSERT_TRUE(cache_.HandleCompletion(completions[i]));
            }
            if (!*next && !cache_.in_flight()) {
                return;
            }
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        FAIL() << "control worker failed to complete";
    }
    void Activate(State target, HandshakeIoBudget budget = {}) {
        for (int i = 0; i < 10000 && (a_.state() != target || b_.state() != target); ++i) {
            ASSERT_TRUE(a_.Progress(++now_, budget));
            ASSERT_TRUE(b_.Progress(++now_, budget));
        }
        ASSERT_EQ(a_.state(), target);
        ASSERT_EQ(b_.state(), target);
    }
    void OpenRemote(HandshakeChannel& remote, bool rndv = false) {
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        ASSERT_TRUE(a_.Open(sockets[0], a_table_,
                            {.local = rndv ? RndvHello(1) : Hello(1),
                             .expected_peer_authorization = 7,
                             .compatible_peer = true,
                             .deadline_ns = 1000000},
                            0));
        ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000000));
        HandshakeMessage hello;
        hello.hello = rndv ? RndvHello(2) : Hello(2);
        ASSERT_TRUE(remote.Queue(hello));
        ASSERT_TRUE(remote.Progress(++now_));
        ASSERT_TRUE(a_.Progress(++now_));
        ASSERT_TRUE(remote.Progress(++now_));
        ASSERT_TRUE(remote.TakeMessage());
        ASSERT_EQ(a_.state(), State::kPreparing);
    }
    HandshakeMessage PeerReady() const {
        HandshakeMessage ready;
        ready.kind = HandshakeMessageKind::kReady;
        const auto peer = Hello(2);
        ready.ready = {.sender = peer.identity,
                       .receiver = a_.local().identity,
                       .transport = HandshakeTransport::kRaw,
                       .credit_epoch = peer.credit_epoch,
                       .max_credit_seq = 8,
                       .max_control_credit_seq = 2,
                       .max_frame_seq = 64,
                       .max_stream_bytes = 4096};
        return ready;
    }

    HandshakeMessage RndvPeerReady() const {
        auto ready = PeerReady();
        ready.ready.rndv = RndvHello(1).rndv.limits;
        ready.ready.max_stream_bytes = ready.ready.rndv.max_bytes;
        return ready;
    }

    inline static ConnectionHandshakeTest* current_ = nullptr;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    urma_context_t a_context_{}, b_context_{};
    urma_target_jetty_t a_target_{}, b_target_{};
    std::atomic<int> import_calls_{0}, unimport_calls_{0};
    bool fail_import_ = false;
    connection::ConnectionTable a_table_, b_table_;
    connection::ReceiveCreditDomain a_credits_, b_credits_;
    std::optional<connection::CreditPeerHandle> a_credit_, b_credit_;
    EndpointImportCache cache_;
    runtime::ControlExecutor executor_;
    ConnectionHandshake a_, b_;
    std::uint64_t now_ = 0;
};

TEST_F(ConnectionHandshakeTest, RawImportAndRoutePrecedeReadyAndFirstFrame) {
    OpenPair();
    PreparePair();
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_TRUE(b_.Progress(++now_));
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(b_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
    DriveCache();
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_EQ(a_.state(), State::kWaitingReady);
    ASSERT_EQ(a_table_.published(), 1u);

    // READY 尚未写出，接收状态已经可用；模拟首个 FIN 提前进入已发布路由，不依赖 Active。
    connection::WireFrameInput frame{.type = connection::WireFrameType::kControl,
                                     .control = connection::WireControlType::kFin,
                                     .token = a_.local().identity.token,
                                     .data = {},
                                     .session = a_.local().identity.session};
    std::array<std::byte, 64> payload{};
    auto encoded = connection::EncodeWireFrame(frame, payload);
    ASSERT_TRUE(encoded);
    auto decoded = connection::DecodeWireFrame(encoded->imm_data, std::span(payload).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    auto route = a_table_.Resolve(*decoded, {1, 101}, {1, 2, 101, 201});
    ASSERT_TRUE(route);
    EXPECT_EQ(route->connection, &a_);
    ASSERT_TRUE(a_.receive_window().Push({.session = {2, 101, 201, frame.token.slot, frame.token.generation},
                                          .kind = connection::ReorderFrameKind::kFin},
                                         ++now_));
    Activate(State::kRawActive);
    EXPECT_EQ(import_calls_, 2);
    // Close 撤路由，但不能丢掉已接收而未交付的窗口责任。
    ExpectError(a_.Close(), Code::kInUse);
    EXPECT_EQ(a_table_.published(), 0u);
    std::array<connection::ReorderDelivery, 1> delivery;
    auto drained = a_.receive_window().Drain(delivery);
    ASSERT_TRUE(drained);
    ASSERT_EQ(*drained, 1u);
    EXPECT_TRUE(delivery[0].discarded);
    ASSERT_TRUE(a_.receive_window().Release(delivery[0].receipt));
    ASSERT_TRUE(a_.Close());
}

TEST_F(ConnectionHandshakeTest, OppositeDirectionEpochAndPhysicalCreditRemainDistinct) {
    OpenPair();
    PreparePair();
    DriveCache();
    Activate(State::kRawActive);
    ASSERT_NE(a_.local().credit_epoch, b_.local().credit_epoch);
    connection::WireFrameInput update{.type = connection::WireFrameType::kControl,
                                      .control = connection::WireControlType::kCreditUpdate,
                                      .token = a_.local().identity.token,
                                      .data = {},
                                      .max_credit_seq = 16,
                                      .max_control_credit_seq = 4,
                                      .session = a_.local().identity.session,
                                      .credit_epoch = b_.local().credit_epoch,
                                      .max_frame_seq = 64,
                                      .max_stream_bytes = 4096};
    std::array<std::byte, 64> payload{};
    auto encoded = connection::EncodeWireFrame(update, payload);
    ASSERT_TRUE(encoded);
    auto decoded = connection::DecodeWireFrame(encoded->imm_data, std::span(payload).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(a_table_.Resolve(*decoded, {1, 101}, {1, 2, 101, 201}));
    auto physical = a_credits_.Consume(*a_credit_, connection::CreditClass::kControl);
    ASSERT_TRUE(physical);
    ASSERT_TRUE(a_credits_.Release(*physical));
    ASSERT_TRUE(a_credits_.RecordRefillAccepted(std::span(&*physical, 1)));
    // 将本地 epoch 错放入对端广告必须拒绝，不能增加任一方向额度。
    decoded->frame.credit_epoch = a_.local().credit_epoch;
    EXPECT_FALSE(a_table_.Resolve(*decoded, {1, 101}, {1, 2, 101, 201}));
}

TEST_F(ConnectionHandshakeTest, UnpreparedSideCannotActivateAndCancelPreservesPeerGrants) {
    OpenPair();
    Credits();
    ASSERT_TRUE(a_.PrepareRaw(APreparation()));
    DriveCache();
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(a_.Progress(++now_));
        ASSERT_TRUE(b_.Progress(++now_));
    }
    EXPECT_EQ(a_.state(), State::kWaitingReady);
    EXPECT_EQ(b_.state(), State::kPreparing);
    ASSERT_NE(b_.peer_ready(), nullptr);
    EXPECT_EQ(b_table_.published(), 0u);
    a_.Cancel();
    EXPECT_EQ(a_table_.published(), 0u);
    const auto before = a_credits_.stats();
    ASSERT_TRUE(a_.Close());
    EXPECT_EQ(a_credits_.stats().data.granted, before.data.granted);
    EXPECT_EQ(a_credits_.stats().active_peers, 1u);
    EXPECT_EQ(unimport_calls_, 0);
}

TEST_F(ConnectionHandshakeTest, InvalidatedImportCannotPublishOrActivate) {
    OpenPair();
    PreparePair();
    DriveCache();
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_EQ(a_table_.published(), 1u);
    ASSERT_TRUE(cache_.Invalidate(a_.import_lease()));
    ExpectError(a_.Progress(++now_), Code::kImport);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(a_.fd(), -1);
}

TEST_F(ConnectionHandshakeTest, EarlyReceiveWindowFailureRetractsRouteBeforeReadyFlush) {
    OpenPair();
    PreparePair();
    DriveCache();
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_EQ(a_.state(), State::kWaitingReady);
    ASSERT_EQ(a_table_.published(), 1u);
    // 路由先于 READY 允许首包早到；非法越窗帧此时必须阻止后续 READY，而不是激活坏窗口。
    const auto token = a_.local().identity.token;
    const auto pushed = a_.receive_window().Push({.session = {2, 101, 201, token.slot, token.generation},
                                                  .sequence = 64,
                                                  .kind = connection::ReorderFrameKind::kFin},
                                                 ++now_);
    ASSERT_FALSE(pushed);
    EXPECT_EQ(pushed.error().code, connection::ReorderErrorCode::kOutOfWindow);
    ASSERT_TRUE(a_.receive_window().failure());
    ExpectError(a_.Progress(++now_), Code::kReorder);
    EXPECT_EQ(a_.state(), State::kFailed);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(a_.fd(), -1);
    EXPECT_EQ(b_.peer_ready(), nullptr);
}

TEST_F(ConnectionHandshakeTest, ImportFailureNeverPublishesAndRequiresCleanReconnect) {
    OpenPair();
    PreparePair();
    fail_import_ = true;
    DriveCache();
    ExpectError(a_.Progress(++now_), Code::kImport);
    ASSERT_TRUE(a_.failure());
    EXPECT_EQ(a_.failure()->underlying_error, EACCES);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(a_.fd(), -1);
    ExpectError(a_.PrepareTcp(), Code::kNotReady);
    ExpectError(a_.TakeEstablishedFd(), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, PendingImportTimeoutAndCancellationDoNotCallProvider) {
    OpenPair();
    PreparePair();
    ExpectError(a_.Progress(1000000), Code::kChannel);
    b_.Cancel();
    ASSERT_TRUE(a_.Close());
    ASSERT_TRUE(b_.Close());
    DriveCache();
    EXPECT_EQ(import_calls_, 0);
    EXPECT_EQ(a_table_.active(), 0u);
}

TEST_F(ConnectionHandshakeTest, PreparationChecksAbiCreditIdentityAndLocalEpoch) {
    OpenPair();
    Credits();
    auto prep = APreparation();
    prep.descriptor_compatible = false;
    ExpectError(a_.PrepareRaw(prep), Code::kInvalidArgument);
    prep = APreparation();
    prep.credit_peer = *b_credit_;
    ExpectError(a_.PrepareRaw(prep), Code::kCredit);
    EXPECT_EQ(cache_.leases(), 0u);
    EXPECT_EQ(a_table_.published(), 0u);
    ASSERT_TRUE(a_.PrepareRaw(APreparation()));
    ExpectError(a_.PrepareRaw(APreparation()), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, FrozenCreditDomainPreventsReadyPublication) {
    OpenPair();
    PreparePair();
    DriveCache();
    a_credits_.Freeze();
    ExpectError(a_.Progress(++now_), Code::kCredit);
    EXPECT_EQ(a_table_.published(), 0u);
}

TEST_F(ConnectionHandshakeTest, NegotiatedTcpDoesNotImportOrPublishRawRoute) {
    OpenPair(false);
    ASSERT_TRUE(a_.PrepareTcp());
    ASSERT_TRUE(b_.PrepareTcp());
    Activate(State::kTcpActive);
    EXPECT_EQ(import_calls_, 0);
    EXPECT_EQ(a_table_.published(), 0u);
    auto left = a_.TakeEstablishedFd();
    auto right = b_.TakeEstablishedFd();
    ASSERT_TRUE(left);
    ASSERT_TRUE(right);
    const char text[] = "first business bytes";
    ASSERT_EQ(send(*left, text, sizeof(text), MSG_NOSIGNAL), static_cast<ssize_t>(sizeof(text)));
    char received[sizeof(text)];
    ASSERT_EQ(recv(*right, received, sizeof(received), 0), static_cast<ssize_t>(sizeof(text)));
    EXPECT_EQ(std::memcmp(text, received, sizeof(text)), 0);
    close(*left);
    close(*right);
}

TEST_F(ConnectionHandshakeTest, ReadyThenBusinessBytesWaitUntilLocalReadyFlushed) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    ASSERT_TRUE(a_.Open(
        sockets[0], a_table_,
        {.local = Hello(1, false), .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000},
        0));
    HandshakeChannel remote;
    ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000000));
    HandshakeMessage hello;
    hello.hello = Hello(2, false);
    ASSERT_TRUE(remote.Queue(hello));
    for (int i = 0; i < 10 && (!a_.peer() || !remote.message_ready()); ++i) {
        ASSERT_TRUE(a_.Progress(++now_));
        ASSERT_TRUE(remote.Progress(++now_));
    }
    auto received_hello = remote.TakeMessage();
    ASSERT_TRUE(received_hello);
    HandshakeMessage ready;
    ready.kind = HandshakeMessageKind::kReady;
    ready.ready = {
        .sender = hello.hello.identity, .receiver = a_.local().identity, .transport = HandshakeTransport::kTcp};
    ASSERT_TRUE(remote.Queue(ready));
    ASSERT_TRUE(remote.Progress(++now_));
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_NE(a_.peer_ready(), nullptr);
    // 对端 READY 后紧跟业务数据，本端尚未 Prepare。小预算让本端 READY 多次部分写出。
    const char text[] = "coalesced application payload";
    ASSERT_EQ(send(remote.fd(), text, sizeof(text), MSG_NOSIGNAL), static_cast<ssize_t>(sizeof(text)));
    ASSERT_TRUE(a_.PrepareTcp());
    for (int i = 0; i < 1000 && a_.state() != State::kTcpActive; ++i) {
        ASSERT_TRUE(a_.Progress(++now_, {.bytes = 1, .syscalls = 1}));
    }
    ASSERT_EQ(a_.state(), State::kTcpActive);
    auto fd = a_.TakeEstablishedFd();
    ASSERT_TRUE(fd);
    char result[sizeof(text)];
    ASSERT_EQ(recv(*fd, result, sizeof(result), 0), static_cast<ssize_t>(sizeof(result)));
    EXPECT_EQ(std::memcmp(text, result, sizeof(text)), 0);
    close(*fd);
}

TEST_F(ConnectionHandshakeTest, WrongPeerAuthorizationFailsBeforeAnyRouteOrImport) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    ASSERT_TRUE(a_.Open(
        sockets[0], a_table_,
        {.local = Hello(1), .expected_peer_authorization = 99, .compatible_peer = true, .deadline_ns = 1000}, 0));
    HandshakeChannel remote;
    ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000));
    HandshakeMessage hello;
    hello.hello = Hello(2);
    ASSERT_TRUE(remote.Queue(hello));
    ASSERT_TRUE(remote.Progress(++now_));
    ExpectError(a_.Progress(++now_), Code::kUnauthorized);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
}

TEST_F(ConnectionHandshakeTest, OnlineUpdateBeforeReadyCannotBypassInitialHandshake) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    ASSERT_TRUE(a_.Open(
        sockets[0], a_table_,
        {.local = Hello(1), .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000}, 0));
    HandshakeChannel remote;
    ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000));
    HandshakeMessage update;
    update.kind = HandshakeMessageKind::kLaneUpdate;
    update.lane_update = {Hello(2).identity, a_.local().identity, 1, Hello(2).tx_lanes};
    ASSERT_TRUE(remote.Queue(update));
    ASSERT_TRUE(remote.Progress(++now_));
    // 结构合法的在线消息也不能替代 HELLO/READY，尚未准入任何来源或导入。
    ExpectError(a_.Progress(++now_), Code::kProtocol);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
}

TEST_F(ConnectionHandshakeTest, InvalidOpenLeavesFdOwnedByCallerAndRetiresReservation) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    auto hello = Hello(1);
    hello.credit_epoch = 0;
    ExpectError(
        a_.Open(sockets[0], a_table_,
                {.local = hello, .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000}, 0),
        Code::kChannel);
    EXPECT_EQ(a_table_.active(), 0u);
    char byte = 'x';
    EXPECT_EQ(send(sockets[0], &byte, 1, MSG_NOSIGNAL), 1);
    close(sockets[0]);
    close(sockets[1]);
}

class MalformedReadyTest : public ConnectionHandshakeTest, public ::testing::WithParamInterface<int> {};

TEST_P(MalformedReadyTest, CompleteIdentityEpochAndTransportMustMatchHello) {
    HandshakeChannel remote;
    OpenRemote(remote);
    ASSERT_EQ(a_.state(), State::kPreparing);
    auto ready = PeerReady();
    // 每个用例只破坏一个仍合法编码的字段，确保失败来自握手关联校验而不是 codec。
    switch (GetParam()) {
    case 0:
        ++ready.ready.receiver.token.generation;
        break;
    case 1:
        ++ready.ready.receiver.session;
        break;
    case 2:
        ++ready.ready.receiver.runtime;
        break;
    case 3:
        ++ready.ready.receiver.receive_domain;
        break;
    case 4:
        ++ready.ready.receiver.authorization;
        break;
    case 5:
        ++ready.ready.credit_epoch;
        break;
    case 6:
        ready.ready.transport = HandshakeTransport::kTcp;
        break;
    case 7:
        ++ready.ready.receiver.endpoint_generation;
        break;
    default:
        FAIL() << "unknown malformed READY case";
    }
    ASSERT_TRUE(remote.Queue(ready));
    ASSERT_TRUE(remote.Progress(++now_));
    ExpectError(a_.Progress(++now_), Code::kProtocol);
    EXPECT_EQ(a_.state(), State::kFailed);
    EXPECT_EQ(a_.peer_ready(), nullptr);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
    EXPECT_EQ(a_.fd(), -1);
}

INSTANTIATE_TEST_SUITE_P(HelloBinding, MalformedReadyTest, ::testing::Range(0, 8));

TEST_F(ConnectionHandshakeTest, ReadyBeforeHelloCannotActivateEvenWithMatchingLocalIdentity) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    ASSERT_TRUE(a_.Open(
        sockets[0], a_table_,
        {.local = Hello(1), .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000}, 0));
    HandshakeChannel remote;
    ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000000));
    ASSERT_TRUE(remote.Queue(PeerReady()));
    ASSERT_TRUE(remote.Progress(++now_));
    ExpectError(a_.Progress(++now_), Code::kProtocol);
    EXPECT_EQ(a_.peer(), nullptr);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
    EXPECT_EQ(a_.fd(), -1);
}

TEST_F(ConnectionHandshakeTest, MatchingRejectReportsPeerReasonAndClosesDirtyStream) {
    HandshakeChannel remote;
    OpenRemote(remote);
    ASSERT_EQ(a_.state(), State::kPreparing);
    HandshakeMessage reject;
    reject.kind = HandshakeMessageKind::kReject;
    reject.reject = {
        .sender_session = Hello(2).identity.session, .receiver_session = a_.local().identity.session, .reason = 23};
    ASSERT_TRUE(remote.Queue(reject));
    ASSERT_TRUE(remote.Progress(++now_));
    const auto rejected = a_.Progress(++now_);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, Code::kPeerRejected);
    EXPECT_EQ(rejected.error().detail, 23);
    EXPECT_EQ(a_.fd(), -1);
    EXPECT_EQ(a_table_.published(), 0u);
    ExpectError(a_.PrepareTcp(), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, NoCommonTransportFailsWithoutImportOrFallback) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    auto local = Hello(1);
    local.capabilities = kHandshakeCapabilityRaw;
    ASSERT_TRUE(a_.Open(
        sockets[0], a_table_,
        {.local = local, .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000}, 0));
    HandshakeChannel remote;
    ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000000));
    HandshakeMessage hello;
    hello.hello = Hello(2, false);
    ASSERT_TRUE(remote.Queue(hello));
    ASSERT_TRUE(remote.Progress(++now_));
    ExpectError(a_.Progress(++now_), Code::kNoCommonTransport);
    EXPECT_EQ(a_.fd(), -1);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
    ExpectError(a_.PrepareTcp(), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, RndvNegotiatesCommonLimitsOnlyAfterBothReadyFramesComplete) {
    auto left = RndvHello(1);
    auto right = RndvHello(2);
    right.rndv.limits.max_write_bytes = 32768;
    right.rndv.limits.max_transfer_bytes = 16384;
    right.rndv.limits.max_grants = 4;
    right.rndv.limits.window = 16;
    right.rndv.limits.max_bytes = 131072;
    OpenPairHello(left, right);
    EXPECT_EQ(a_.rndv(), nullptr);
    EXPECT_EQ(b_.rndv(), nullptr);
    ExpectError(a_.PrepareRndvImport(RndvGrant(a_)), Code::kNotReady);
    PreparePair(true);
    DriveCache();
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_EQ(a_.state(), State::kWaitingReady);
    EXPECT_EQ(a_.rndv(), nullptr);
    // 多轮短写覆盖 READY 尚在本地输出中时不能提前启用 RNDV 的门禁。
    Activate(State::kRawActive, {.bytes = 7, .syscalls = 1});
    ASSERT_NE(a_.rndv(), nullptr);
    ASSERT_NE(b_.rndv(), nullptr);
    EXPECT_EQ(*a_.rndv(), right.rndv.limits);
    EXPECT_EQ(*b_.rndv(), right.rndv.limits);
    ASSERT_NE(a_.local_ready(), nullptr);
    ASSERT_NE(a_.peer_ready(), nullptr);
    EXPECT_EQ(a_.local_ready()->rndv, right.rndv.limits);
    EXPECT_EQ(a_.peer_ready()->rndv, right.rndv.limits);
    EXPECT_TRUE(a_.PrepareRndvImport(RndvGrant(a_)));
    EXPECT_TRUE(b_.PrepareRndvImport(RndvGrant(b_)));
}

TEST_F(ConnectionHandshakeTest, OneSidedRndvOfferKeepsRawEagerWithoutImportPermission) {
    OpenPairHello(RndvHello(1), Hello(2));
    // 不共同声明能力时无需 RNDV 授权，普通 RAW 建链仍可完成。
    PreparePair();
    DriveCache();
    Activate(State::kRawActive);
    EXPECT_EQ(a_.rndv(), nullptr);
    EXPECT_EQ(b_.rndv(), nullptr);
    EXPECT_EQ(a_.peer_ready()->rndv, RndvLimits{});
    ExpectError(a_.PrepareRndvImport({}), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, SelectedTcpNeverEnablesRndvFromOneRawOffer) {
    OpenPairHello(RndvHello(1), Hello(2, false));
    ASSERT_TRUE(a_.PrepareTcp());
    ASSERT_TRUE(b_.PrepareTcp());
    Activate(State::kTcpActive);
    EXPECT_EQ(a_.rndv(), nullptr);
    EXPECT_EQ(b_.rndv(), nullptr);
    EXPECT_EQ(cache_.leases(), 0u);
    EXPECT_EQ(import_calls_, 0);
    ExpectError(a_.PrepareRndvImport({}), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, RndvPeerAuthorizationPrecedesAnyImportOrWindowPublication) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    Credits();
    auto prep = APreparation();
    ExpectError(a_.PrepareRaw(prep), Code::kUnauthorized);
    EXPECT_EQ(a_.state(), State::kPreparing);
    EXPECT_EQ(cache_.leases(), 0u);
    EXPECT_EQ(import_calls_, 0);
    EXPECT_FALSE(a_.receive_window().session());
    EXPECT_EQ(a_table_.published(), 0u);
    // 宿主补齐独立认证/导入许可后可以在原握手重试，不需撤销或重发 HELLO。
    prep.rndv_peer_authorized = true;
    ASSERT_TRUE(a_.PrepareRaw(prep));
    EXPECT_EQ(cache_.leases(), 1u);
}

TEST_F(ConnectionHandshakeTest, RndvPreparationRejectsInsufficientCommonWindowOrByteBudget) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    Credits();
    auto prep = APreparation();
    prep.rndv_peer_authorized = true;
    prep.reorder.window = 31;
    ExpectError(a_.PrepareRaw(prep), Code::kInvalidArgument);
    prep.reorder.window = 64;
    prep.reorder.max_resident_bytes = 262143;
    ExpectError(a_.PrepareRaw(prep), Code::kInvalidArgument);
    EXPECT_EQ(cache_.leases(), 0u);
    EXPECT_FALSE(a_.receive_window().session());
    prep.reorder.max_resident_bytes = 262144;
    ASSERT_TRUE(a_.PrepareRaw(prep));
}

TEST_F(ConnectionHandshakeTest, EarlyRndvReadyDoesNotReplaceLocalPreparationOrOutputFlush) {
    HandshakeChannel remote;
    OpenRemote(remote, true);
    ASSERT_TRUE(remote.Queue(RndvPeerReady()));
    ASSERT_TRUE(remote.Progress(++now_));
    ASSERT_TRUE(a_.Progress(++now_));
    ASSERT_NE(a_.peer_ready(), nullptr);
    EXPECT_EQ(a_.rndv(), nullptr);
    ExpectError(a_.PrepareRndvImport(RndvGrant(a_)), Code::kNotReady);
    Credits();
    auto prep = APreparation();
    prep.rndv_peer_authorized = true;
    ASSERT_TRUE(a_.PrepareRaw(prep));
    DriveCache();
    for (int i = 0; i < 1000 && a_.state() != State::kRawActive; ++i) {
        ASSERT_TRUE(a_.Progress(++now_, {.bytes = 1, .syscalls = 1}));
        if (a_.output_pending()) {
            EXPECT_EQ(a_.rndv(), nullptr);
        }
    }
    ASSERT_EQ(a_.state(), State::kRawActive);
    ASSERT_NE(a_.rndv(), nullptr);
    EXPECT_TRUE(a_.PrepareRndvImport(RndvGrant(a_)));
}

class MalformedRndvReadyTest : public ConnectionHandshakeTest, public ::testing::WithParamInterface<int> {};

TEST_P(MalformedRndvReadyTest, ReadyMustEchoTheExactCommonLimitsAndCoverItsWindow) {
    HandshakeChannel remote;
    OpenRemote(remote, true);
    auto ready = RndvPeerReady();
    // 所有变体都保持可编码；证明拒绝来自双向协商关联，不能借合法 READY 偷改能力。
    switch (GetParam()) {
    case 0:
        ready.ready.rndv = {};
        break;
    case 1:
        ready.ready.rndv.max_write_bytes /= 2;
        break;
    case 2:
        ready.ready.rndv.max_transfer_bytes /= 2;
        break;
    case 3:
        --ready.ready.rndv.max_grants;
        break;
    case 4:
        --ready.ready.rndv.window;
        break;
    case 5:
        --ready.ready.rndv.max_bytes;
        break;
    case 6:
        ready.ready.max_frame_seq = ready.ready.rndv.window - 1;
        break;
    case 7:
        ready.ready.max_stream_bytes = ready.ready.rndv.max_bytes - 1;
        break;
    default:
        FAIL() << "unknown malformed RNDV READY case";
    }
    ASSERT_TRUE(remote.Queue(ready));
    ASSERT_TRUE(remote.Progress(++now_));
    ExpectError(a_.Progress(++now_), Code::kProtocol);
    EXPECT_EQ(a_.state(), State::kFailed);
    EXPECT_EQ(a_.rndv(), nullptr);
    EXPECT_EQ(a_.peer_ready(), nullptr);
    EXPECT_EQ(a_table_.published(), 0u);
    EXPECT_EQ(import_calls_, 0);
}
INSTANTIATE_TEST_SUITE_P(NegotiatedRndv, MalformedRndvReadyTest, ::testing::Range(0, 8));

TEST_F(ConnectionHandshakeTest, ReadyCannotEnableRndvWithoutCommonHelloOffer) {
    HandshakeChannel remote;
    OpenRemote(remote);
    ASSERT_TRUE(remote.Queue(RndvPeerReady()));
    ASSERT_TRUE(remote.Progress(++now_));
    ExpectError(a_.Progress(++now_), Code::kProtocol);
    EXPECT_EQ(a_.rndv(), nullptr);
    EXPECT_EQ(import_calls_, 0);
}

TEST_F(ConnectionHandshakeTest, RndvImportRequestUsesPeerIdentityDirectoryAndBoundLocalContext) {
    auto peer = RndvHello(2);
    peer.rndv.arenas[0].token_policy = kRndvTokenPlainText;
    peer.rndv.arenas[0].token = 0xa1b2c3d4;
    OpenPairHello(RndvHello(1), peer);
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    const auto request = a_.PrepareRndvImport(RndvGrant(a_));
    ASSERT_TRUE(request);
    const auto& arena = peer.rndv.arenas[0];
    const auto& key = request->key;
    EXPECT_EQ(key.local_context, &a_context_);
    EXPECT_EQ(key.local_context_incarnation, 1u);
    EXPECT_EQ(key.peer_runtime, peer.identity.runtime);
    EXPECT_EQ(key.receive_domain, peer.identity.receive_domain);
    EXPECT_EQ(key.authorization_identity, peer.identity.authorization);
    EXPECT_EQ(key.slab_id, arena.id);
    EXPECT_EQ(key.slab_generation, arena.generation);
    EXPECT_EQ(std::memcmp(key.eid.data(), arena.eid.data(), arena.eid.size()), 0);
    // 规范字段逐个重建，不能把网络字节解释为包含 ABI padding 的 urma_seg_t。
    EXPECT_EQ(request->descriptor.ubva.va, arena.address);
    EXPECT_EQ(request->descriptor.ubva.uasid, arena.uasid);
    EXPECT_EQ(std::memcmp(request->descriptor.ubva.eid.raw, arena.eid.data(), arena.eid.size()), 0);
    EXPECT_EQ(request->descriptor.len, arena.length);
    EXPECT_EQ(request->descriptor.token_id, arena.token_id);
    EXPECT_EQ(request->descriptor.attr.bs.access, static_cast<std::uint32_t>(URMA_ACCESS_READ | URMA_ACCESS_WRITE));
    EXPECT_EQ(request->descriptor.attr.bs.token_policy, static_cast<std::uint32_t>(URMA_TOKEN_PLAIN_TEXT));
    EXPECT_EQ(request->descriptor.attr.bs.dsva, 0u);
    EXPECT_EQ(request->descriptor.attr.bs.non_pin, 0u);
    EXPECT_EQ(request->descriptor.attr.bs.user_iova, 0u);
    EXPECT_EQ(request->descriptor.attr.bs.has_user_info, 0u);
    EXPECT_EQ(request->descriptor.attr.bs.reserved, 0u);
    EXPECT_EQ(request->token.token, arena.token);
    // 使用真正缓存的准入规则检验映射结果；不提交慢导入操作，单靠目录不能取得可写 target。
    TsegImportCache segments;
    ASSERT_TRUE(segments.Open({.entries = 1, .leases = 2, .in_flight = 1}));
    const auto segment_lease = segments.Acquire(*request);
    ASSERT_TRUE(segment_lease);
    const auto pending = segments.Get(*segment_lease);
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error().code, TsegImportCacheErrorCode::kPending);
    ASSERT_TRUE(segments.Release(*segment_lease));
    EXPECT_TRUE(segments.Close());
    const auto reverse = b_.PrepareRndvImport(RndvGrant(b_));
    ASSERT_TRUE(reverse);
    EXPECT_EQ(reverse->key.local_context, &b_context_);
    EXPECT_EQ(reverse->key.peer_runtime, 1u);
    EXPECT_EQ(reverse->key.receive_domain, 101u);
    EXPECT_EQ(reverse->descriptor.attr.bs.token_policy, static_cast<std::uint32_t>(URMA_TOKEN_NONE));
    EXPECT_EQ(reverse->token.token, 0u);
    EXPECT_EQ(import_calls_, 2);
}

class InvalidRndvGrantTest : public ConnectionHandshakeTest, public ::testing::WithParamInterface<int> {};

TEST_P(InvalidRndvGrantTest, RejectsWrongDirectionStaleIdentityAndUnauthorizedRange) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    auto grant = RndvGrant(a_);
    const auto original = grant;
    const auto& arena = a_.peer()->rndv.arenas[0];
    // 不合法 Grant 不能产生第二套导入 key，也不能污染同一协商下仍然有效的原授权。
    switch (GetParam()) {
    case 0:
        ++grant.transfer.binding.sender_runtime;
        break;
    case 1:
        ++grant.transfer.binding.receiver_runtime;
        break;
    case 2:
        grant.transfer.binding.receive_domain = a_.local().identity.receive_domain;
        break;
    case 3:
        grant.transfer.binding.session = a_.local().identity.session;
        break;
    case 4:
        ++grant.transfer.binding.token.slot;
        break;
    case 5:
        ++grant.transfer.binding.token.generation;
        break;
    case 6:
        ++grant.arena.id;
        break;
    case 7:
        ++grant.arena.generation;
        break;
    case 8:
        grant.slot = a_.rndv()->window;
        break;
    case 9:
        grant.transfer.seq = 1;
        break;
    case 10:
        grant.generation = 0;
        break;
    case 11:
        grant.transfer.transfer_id = 0;
        break;
    case 12:
        grant.transfer.bytes = 0;
        break;
    case 13:
        grant.transfer.bytes = a_.rndv()->max_transfer_bytes + 1;
        break;
    case 14:
        grant.target_address = arena.address - 1;
        break;
    case 15:
        grant.target_address = arena.address + arena.length - grant.transfer.bytes + 1;
        break;
    case 16:
        grant.target_address = std::numeric_limits<std::uint64_t>::max() - 1;
        break;
    case 17:
        grant.transfer.seq = std::numeric_limits<std::uint32_t>::max();
        break;
    default:
        FAIL() << "unknown invalid RNDV Grant case";
    }
    ExpectError(a_.PrepareRndvImport(grant), Code::kProtocol);
    EXPECT_TRUE(a_.PrepareRndvImport(original));
    EXPECT_EQ(a_.state(), State::kRawActive);
    EXPECT_EQ(cache_.leases(), 2u);
    EXPECT_EQ(import_calls_, 2);
}
INSTANTIATE_TEST_SUITE_P(GrantBindingAndRange, InvalidRndvGrantTest, ::testing::Range(0, 18));

TEST_F(ConnectionHandshakeTest, RndvImportSelectsSecondArenaAndAcceptsExactEndBoundary) {
    auto peer = RndvHello(2);
    peer.rndv.arena_count = 2;
    peer.rndv.arenas[1] = peer.rndv.arenas[0];
    peer.rndv.arenas[1].id += 10;
    peer.rndv.arenas[1].generation += 20;
    peer.rndv.arenas[1].address += peer.rndv.arenas[1].length;
    peer.rndv.arenas[1].token_id += 30;
    OpenPairHello(RndvHello(1), peer);
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    auto grant = RndvGrant(a_, 1);
    const auto& second = peer.rndv.arenas[1];
    grant.target_address = second.address + second.length - grant.transfer.bytes;
    const auto request = a_.PrepareRndvImport(grant);
    ASSERT_TRUE(request);
    EXPECT_EQ(request->key.slab_id, second.id);
    EXPECT_EQ(request->key.slab_generation, second.generation);
    EXPECT_EQ(request->descriptor.ubva.va, second.address);
    EXPECT_EQ(request->descriptor.token_id, second.token_id);
}

TEST_F(ConnectionHandshakeTest, InvalidatedEndpointPreventsNewRndvImportRequestsAfterActivation) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    const auto grant = RndvGrant(a_);
    ASSERT_TRUE(a_.PrepareRndvImport(grant));
    ASSERT_TRUE(cache_.Invalidate(a_.import_lease()));
    ExpectError(a_.PrepareRndvImport(grant), Code::kImport);
}

TEST_F(ConnectionHandshakeTest, FrozenCreditDomainPreventsNewRndvImportRequests) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    const auto grant = RndvGrant(a_);
    // Active 的协商结果不是永久准入许可；Peer 共账冻结后不能创建新的 WRITE 准备请求。
    a_credits_.Freeze();
    ExpectError(a_.PrepareRndvImport(grant), Code::kCredit);
    EXPECT_EQ(import_calls_, 2);
}

TEST_F(ConnectionHandshakeTest, FailedCommonReceiveWindowPreventsNewRndvImportRequests) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    const auto grant = RndvGrant(a_);
    a_.receive_window().Fail();
    ExpectError(a_.PrepareRndvImport(grant), Code::kReorder);
    EXPECT_EQ(cache_.leases(), 2u);
}

TEST_F(ConnectionHandshakeTest, CancellationAndCloseInvalidateRndvImportAccess) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    const auto grant = RndvGrant(a_);
    a_.Cancel();
    EXPECT_EQ(a_.rndv(), nullptr);
    ExpectError(a_.PrepareRndvImport(grant), Code::kNotReady);
    ASSERT_TRUE(a_.Close());
    ExpectError(a_.PrepareRndvImport(grant), Code::kNotReady);
}

TEST_F(ConnectionHandshakeTest, ActiveRndvGrantValidationAndImportRequestMappingDoNotAllocate) {
    OpenPairHello(RndvHello(1), RndvHello(2));
    PreparePair(true);
    DriveCache();
    Activate(State::kRawActive);
    auto grant = RndvGrant(a_);
    auto wrong = grant;
    ++wrong.arena.generation;
    bool valid = true;
    allocations = deallocations = 0;
    // 已建链路径只读固定目录与现有缓存，不应隐式导入、构造动态容器或创建责任票据。
    count_allocations = true;
    for (int i = 0; i < 64; ++i) {
        const auto result = a_.PrepareRndvImport(grant);
        const auto rejected = a_.PrepareRndvImport(wrong);
        valid &= result.has_value() && !rejected.has_value() && a_.rndv() != nullptr;
    }
    count_allocations = false;
    EXPECT_TRUE(valid);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    EXPECT_EQ(cache_.leases(), 2u);
    EXPECT_EQ(import_calls_, 2);
}

} // namespace
} // namespace control
} // namespace kbsocket
