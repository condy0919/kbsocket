// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/connection_handshake.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

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
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        ConnectionHandshakeConfig config{
            .local = Hello(1, raw), .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000};
        ASSERT_TRUE(a_.Open(sockets[0], a_table_, config, 0));
        config.local = Hello(2, raw);
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
                .route_owner = &a_};
    }
    void PreparePair() {
        Credits();
        ASSERT_TRUE(a_.PrepareRaw(APreparation()));
        auto preparation = APreparation();
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
    void Activate(State target) {
        for (int i = 0; i < 100 && (a_.state() != target || b_.state() != target); ++i) {
            ASSERT_TRUE(a_.Progress(++now_));
            ASSERT_TRUE(b_.Progress(++now_));
        }
        ASSERT_EQ(a_.state(), target);
        ASSERT_EQ(b_.state(), target);
    }
    void OpenRemote(HandshakeChannel& remote) {
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        ASSERT_TRUE(a_.Open(
            sockets[0], a_table_,
            {.local = Hello(1), .expected_peer_authorization = 7, .compatible_peer = true, .deadline_ns = 1000000}, 0));
        ASSERT_TRUE(remote.Open(sockets[1], true, 0, 1000000));
        HandshakeMessage hello;
        hello.hello = Hello(2);
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

} // namespace
} // namespace control
} // namespace kbsocket
