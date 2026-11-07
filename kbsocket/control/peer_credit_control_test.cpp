// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/peer_credit_control.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <new>
#include <utility>

#include <gtest/gtest.h>

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
namespace control {
namespace {
using connection::CreditClass;
using connection::CreditErrorCode;
using connection::CreditIdentity;
using connection::RxCreditReceipt;
using Code = PeerCreditControlErrorCode;

class PeerCreditControlTest : public ::testing::Test {
protected:
    struct Endpoint {
        HandshakeIdentity identity;
        std::uint64_t epoch = 0;
        connection::ReceiveCreditDomain receive;
        connection::SenderCreditAccount sender;
        connection::CreditPeerHandle peer;
        PeerCreditControl controller;
    } left_, right_;
    HandshakeChannel remote_;
    std::array<int, 2> fds_{-1, -1};
    std::uint64_t now_ = 1;
    static constexpr std::uint64_t kTimeout = 10000;

    static HandshakeIdentity Identity(std::uint64_t seed) {
        return {seed, seed + 1, seed + 2, seed + 3, seed + 4, {7, 300}};
    }
    void SetUp() override {
        left_.identity = Identity(100);
        right_.identity = Identity(200);
        left_.epoch = 11;
        right_.epoch = 22;
        Init(left_, right_);
        Init(right_, left_);
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds_.data()), 0);
    }
    void Init(Endpoint& local, const Endpoint& peer) {
        ASSERT_TRUE(local.receive.Open({local.identity.receive_domain, 32, 8, 4, 8}));
        // 本测试的账本模型在初始真实 post 接受后记入容量；不模拟 UB 硬件完成时序。
        ASSERT_TRUE(local.receive.RecordInitialPosted(CreditClass::kData, 24));
        ASSERT_TRUE(local.receive.RecordInitialPosted(CreditClass::kControl, 8));
        const auto admitted = local.receive.Admit({peer.identity.runtime, local.identity.receive_domain, local.epoch});
        ASSERT_TRUE(admitted);
        local.peer = *admitted;
        ASSERT_TRUE(local.receive.Grant(local.peer, CreditClass::kData, 8));
        ASSERT_TRUE(local.receive.Grant(local.peer, CreditClass::kControl, 4));
        const CreditIdentity identity{peer.identity.runtime, peer.identity.receive_domain, peer.epoch};
        ASSERT_TRUE(local.sender.Open(identity));
        ASSERT_TRUE(local.sender.Update(identity, 8, 4));
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(left_.controller.Close());
        EXPECT_TRUE(right_.controller.Close());
        EXPECT_TRUE(remote_.Close());
        for (auto fd : fds_) {
            if (fd >= 0) {
                ::close(fd);
            }
        }
        // 协议断言已完成。模型销毁时两个发送者均已退出；不存在未返回的 RX receipt。
        // 这里显式提供宿主静默事实，不把测试中的 EOF/超时当作控制器生成的证明。
        for (auto* endpoint : {&left_, &right_}) {
            EXPECT_TRUE(endpoint->sender.Close());
            const auto peer = endpoint->receive.PeerStats(endpoint->peer);
            if (peer && !peer->retired) {
                EXPECT_TRUE(endpoint->receive.RetirePeer(endpoint->peer, {true, true, true}));
            }
            EXPECT_TRUE(endpoint->receive.Close());
        }
    }
    PeerCreditControlConfig Config(Endpoint& local, const Endpoint& peer) {
        return {.local = local.identity,
                .peer = peer.identity,
                .local_credit_epoch = local.epoch,
                .peer_credit_epoch = peer.epoch,
                .sender = &local.sender,
                .receive = &local.receive,
                .receive_peer = local.peer,
                .timeout_ns = kTimeout,
                .peer_authorized = true};
    }
    void OpenLeft() {
        ASSERT_TRUE(left_.controller.Open(fds_[0], Config(left_, right_), 0));
        fds_[0] = -1;
    }
    void OpenBoth() {
        OpenLeft();
        ASSERT_TRUE(right_.controller.Open(fds_[1], Config(right_, left_), 0));
        fds_[1] = -1;
    }
    void OpenRawPeer() {
        OpenLeft();
        ASSERT_TRUE(remote_.Open(fds_[1], true, 0, std::numeric_limits<std::uint64_t>::max()));
        fds_[1] = -1;
    }
    void Accept(Endpoint& endpoint, CreditClass kind, std::uint32_t count) {
        const auto reservation = endpoint.sender.Reserve(kind, count);
        ASSERT_TRUE(reservation);
        ASSERT_TRUE(endpoint.sender.Resolve(*reservation, count));
    }
    RxCreditReceipt Consume(Endpoint& endpoint, CreditClass kind) {
        const auto receipt = endpoint.receive.Consume(endpoint.peer, kind);
        EXPECT_TRUE(receipt);
        return receipt.value_or(RxCreditReceipt{});
    }
    void ReleaseAndRefill(Endpoint& endpoint, RxCreditReceipt receipt) {
        ASSERT_TRUE(endpoint.receive.Release(receipt));
        ASSERT_TRUE(endpoint.receive.RecordRefillAccepted(std::span(&receipt, 1)));
    }
    void Receive(Endpoint& endpoint, CreditClass kind, std::uint32_t count) {
        for (std::uint32_t i = 0; i < count; ++i) {
            ReleaseAndRefill(endpoint, Consume(endpoint, kind));
        }
    }
    std::expected<PeerCreditControlProgress, PeerCreditControlError> Step() {
        return left_.controller.Progress(now_++);
    }
    void Pump(std::size_t count = 8) {
        for (std::size_t i = 0; i < count; ++i) {
            ASSERT_TRUE(left_.controller.Progress(now_++));
            ASSERT_TRUE(right_.controller.Progress(now_++));
        }
    }
    HandshakeMessage Stop(std::array<std::uint64_t, 2> spent = {}) {
        HandshakeMessage message;
        message.kind = HandshakeMessageKind::kCreditStop;
        message.credit_stop = {right_.identity, left_.identity, right_.epoch, left_.epoch, spent};
        return message;
    }
    HandshakeMessage Ack(std::array<std::uint64_t, 2> spent = {}) {
        auto message = Stop(spent);
        message.kind = HandshakeMessageKind::kCreditStopAck;
        message.credit_stop_ack = message.credit_stop;
        return message;
    }
    void Send(const HandshakeMessage& message) {
        ASSERT_TRUE(remote_.Queue(message));
        const auto progressed = remote_.Progress(now_++, {.receive = false});
        ASSERT_TRUE(progressed);
        ASSERT_TRUE(remote_.output_flushed());
    }
    HandshakeMessage ReadRemote() {
        const auto progressed = remote_.Progress(now_++);
        EXPECT_TRUE(progressed);
        if (!progressed || !progressed->message_ready) {
            ADD_FAILURE() << "expected one complete control frame";
            return {};
        }
        const auto message = remote_.TakeMessage();
        EXPECT_TRUE(message);
        return message.value_or(HandshakeMessage{});
    }
    void ExpectUnretired() {
        const auto peer = left_.receive.PeerStats(left_.peer);
        ASSERT_TRUE(peer);
        EXPECT_TRUE(peer->frozen);
        EXPECT_FALSE(peer->retired);
        EXPECT_EQ(peer->base.granted, 8u);
        EXPECT_EQ(peer->control_granted, 4u);
        EXPECT_FALSE(left_.controller.stats().receive_retired);
        EXPECT_FALSE(left_.controller.stats().complete);
    }
};

TEST_F(PeerCreditControlTest, ZeroTrafficWithdrawsUnusedCreditsOnlyAfterBothStopsAndAcks) {
    OpenBoth();
    EXPECT_FALSE(left_.sender.Reserve(CreditClass::kData, 1));
    EXPECT_FALSE(left_.receive.Grant(left_.peer, CreditClass::kControl, 1));
    EXPECT_FALSE(left_.controller.stats().receive_retired);
    Pump();
    for (auto* endpoint : {&left_, &right_}) {
        const auto stats = endpoint->controller.stats();
        EXPECT_TRUE(stats.local_stop_sent);
        EXPECT_TRUE(stats.peer_stop_received);
        EXPECT_TRUE(stats.receive_retired);
        EXPECT_TRUE(stats.ack_sent);
        EXPECT_TRUE(stats.peer_acknowledged);
        EXPECT_TRUE(stats.complete);
        EXPECT_EQ(endpoint->receive.stats().active_peers, 0u);
        EXPECT_EQ(endpoint->receive.stats().physical_posted, 32u);
        EXPECT_EQ(endpoint->receive.stats().data.granted, 0u);
        EXPECT_EQ(endpoint->receive.stats().control.granted, 0u);
    }
}

TEST_F(PeerCreditControlTest, SimultaneousWithdrawalMatchesSeparateDataAndControlTotals) {
    Accept(left_, CreditClass::kData, 3);
    Accept(left_, CreditClass::kControl, 2);
    Accept(right_, CreditClass::kData, 1);
    Accept(right_, CreditClass::kControl, 3);
    Receive(right_, CreditClass::kData, 3);
    Receive(right_, CreditClass::kControl, 2);
    Receive(left_, CreditClass::kData, 1);
    Receive(left_, CreditClass::kControl, 3);
    OpenBoth();
    // 测试期间没有给控制器任何 TX CQE；协议完成不可被解释成本地 DMA 的终结证据。
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_TRUE(right_.controller.stats().complete);
    EXPECT_EQ(left_.controller.stats().local_spent, (std::array<std::uint64_t, 2>{3, 2}));
    EXPECT_EQ(left_.controller.stats().peer_spent, (std::array<std::uint64_t, 2>{1, 3}));
    EXPECT_EQ(right_.controller.stats().peer_spent, left_.controller.stats().local_spent);
}

TEST_F(PeerCreditControlTest, ReceivedStopWaitsForLateRxThenReleaseAndActualRefill) {
    Accept(right_, CreditClass::kData, 1);
    OpenBoth();
    Pump();
    EXPECT_TRUE(left_.controller.stats().peer_stop_received);
    ExpectUnretired();
    // 对端已声明不再发送，先前真实接受的 RX 仍可能晚于 STOP 到达；Held 和 released 都不够。
    const auto receipt = Consume(left_, CreditClass::kData);
    Pump();
    EXPECT_FALSE(left_.controller.stats().receive_retired);
    ASSERT_TRUE(left_.receive.Release(receipt));
    Pump();
    EXPECT_FALSE(left_.controller.stats().receive_retired);
    ASSERT_TRUE(left_.receive.RecordRefillAccepted(std::span(&receipt, 1)));
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_TRUE(right_.controller.stats().complete);
}

TEST_F(PeerCreditControlTest, PendingPostReservationMustResolveBeforePublishingFinalSpent) {
    const auto reservation = left_.sender.Reserve(CreditClass::kData, 3);
    ASSERT_TRUE(reservation);
    OpenBoth();
    Pump();
    EXPECT_FALSE(left_.controller.stats().local_snapshot);
    EXPECT_FALSE(right_.controller.stats().peer_stop_received);
    // stop 阻止后续 Reserve，但不能丢弃开始撤回前已经进入 provider 的批次；只记可信接受前缀。
    ASSERT_TRUE(left_.sender.Resolve(*reservation, 1));
    Receive(right_, CreditClass::kData, 1);
    Pump();
    EXPECT_EQ(left_.controller.stats().local_spent[0], 1u);
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_TRUE(right_.controller.stats().complete);
}

TEST_F(PeerCreditControlTest, UnknownPostAcceptanceNeverProducesStopSnapshot) {
    const auto reservation = left_.sender.Reserve(CreditClass::kData, 2);
    ASSERT_TRUE(reservation);
    OpenBoth();
    ASSERT_TRUE(left_.sender.Resolve(*reservation, std::nullopt));
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kCredit);
    EXPECT_FALSE(left_.controller.stats().local_snapshot);
    EXPECT_FALSE(left_.controller.stats().local_stop_sent);
    ExpectUnretired();
}

TEST_F(PeerCreditControlTest, UnattributedRxCannotBeReassignedToSatisfyWithdrawal) {
    // unknown receipt 的正常账本不能强制清理；在隔离子进程断言保留，父进程不制造假 refill。
    EXPECT_EXIT(
        {
            const auto unknown = left_.receive.ConsumeUnknown();
            if (!unknown || !left_.receive.Release(*unknown)) {
                _exit(1);
            }
            if (!left_.controller.Open(fds_[0], Config(left_, right_), 0)) {
                _exit(2);
            }
            if (!remote_.Open(fds_[1], true, 0, std::numeric_limits<std::uint64_t>::max()) || !remote_.Queue(Stop()) ||
                !remote_.Progress(1, {.receive = false})) {
                _exit(3);
            }
            const auto result = left_.controller.Progress(2);
            const auto peer = left_.receive.PeerStats(left_.peer);
            if (result || result.error().code != Code::kCredit || !peer || peer->retired ||
                left_.controller.stats().ack_sent || left_.receive.stats().unattributed != 1 ||
                left_.receive.stats().released != 1 || left_.receive.stats().physical_posted != 31) {
                _exit(4);
            }
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(PeerCreditControlTest, MissingRxIsNotReclaimedAtTimeout) {
    Accept(right_, CreditClass::kData, 1);
    OpenBoth();
    Pump();
    const auto result = left_.controller.Progress(kTimeout);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kTimeout);
    ExpectUnretired();
    EXPECT_TRUE(left_.sender.stats().stopped);
}

TEST_F(PeerCreditControlTest, ControlEofRetainsOutstandingGrantsAndStopsNewSends) {
    OpenRawPeer();
    ASSERT_TRUE(remote_.Close());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kChannel);
    ExpectUnretired();
    EXPECT_TRUE(left_.sender.stats().stopped);
}

TEST_F(PeerCreditControlTest, AbortUnbindsControlWithoutReturningUnverifiedCredits) {
    OpenBoth();
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 1u);
    EXPECT_FALSE(left_.sender.Close());
    EXPECT_FALSE(left_.sender.Open({right_.identity.runtime, right_.identity.receive_domain, right_.epoch + 1}));
    const auto retired = left_.receive.RetirePeer(left_.peer, {true, true, true});
    ASSERT_FALSE(retired);
    EXPECT_EQ(retired.error().code, CreditErrorCode::kInUse);
    ASSERT_TRUE(left_.controller.Close());
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 0u);
    ASSERT_TRUE(left_.controller.Close());
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 0u);
    ExpectUnretired();
    EXPECT_TRUE(left_.sender.stats().stopped);
    EXPECT_EQ(left_.controller.fd(), -1);
    EXPECT_FALSE(left_.receive.Grant(left_.peer, CreditClass::kData, 1));
    EXPECT_TRUE(left_.sender.Close());
}

TEST_F(PeerCreditControlTest, BindingSurvivesRetirementUntilControlClose) {
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 0u);
    OpenBoth();
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 1u);
    Pump();
    ASSERT_TRUE(left_.controller.stats().complete);
    // active_peers 清零只证明授权已撤回，控制器仍借用账本，关闭屏障必须独立计数。
    EXPECT_EQ(left_.receive.stats().active_peers, 0u);
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 1u);
    EXPECT_FALSE(left_.receive.Close());
    EXPECT_FALSE(left_.sender.Close());
    ASSERT_TRUE(left_.controller.Close());
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 0u);
    EXPECT_TRUE(left_.receive.Close());
    EXPECT_TRUE(left_.sender.Close());
}

TEST_F(PeerCreditControlTest, AdmissionStopStillAllowsWithdrawalOfExistingPeers) {
    Accept(left_, CreditClass::kData, 1);
    left_.receive.StopAdmission();
    right_.receive.StopAdmission();
    // 停准入后仍能绑定撤回控制器；旧 SEND 的 RX 到达前，不能提前释放已发布授权。
    OpenBoth();
    Pump();
    EXPECT_FALSE(right_.controller.stats().complete);
    EXPECT_EQ(right_.receive.stats().withdrawal_bindings, 1u);
    const auto receipt = Consume(right_, CreditClass::kData);
    ASSERT_TRUE(right_.receive.Release(receipt));
    Pump();
    EXPECT_FALSE(right_.controller.stats().complete);
    ASSERT_TRUE(right_.receive.RecordRefillAccepted(std::span(&receipt, 1)));
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_TRUE(right_.controller.stats().complete);
    EXPECT_TRUE(right_.receive.stats().admission_stopped);
    EXPECT_FALSE(right_.receive.stats().frozen);
    EXPECT_EQ(right_.receive.stats().active_peers, 0u);
    EXPECT_EQ(right_.receive.stats().withdrawal_bindings, 1u);
}

TEST_F(PeerCreditControlTest, IndependentPeerCanStillConsumeRefillAndReceiveGrants) {
    const auto other = left_.receive.Admit({300, left_.identity.receive_domain, 33});
    ASSERT_TRUE(other);
    ASSERT_TRUE(left_.receive.Grant(*other, CreditClass::kData, 2));
    OpenBoth();
    EXPECT_FALSE(left_.receive.stats().frozen);
    const auto receipt = left_.receive.Consume(*other, CreditClass::kData);
    ASSERT_TRUE(receipt);
    ASSERT_TRUE(left_.receive.Release(*receipt));
    ASSERT_TRUE(left_.receive.RecordRefillAccepted(std::span(&*receipt, 1)));
    ASSERT_TRUE(left_.receive.Grant(*other, CreditClass::kControl, 1));
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
    const auto stats = left_.receive.PeerStats(*other);
    ASSERT_TRUE(stats);
    EXPECT_FALSE(stats->frozen);
    EXPECT_FALSE(stats->retired);
    ASSERT_TRUE(left_.receive.RetirePeer(*other, {true, true, true}));
}

TEST_F(PeerCreditControlTest, SenderMayHaveSeenOlderLimitsThanReceiverIssued) {
    ASSERT_TRUE(left_.receive.Grant(left_.peer, CreditClass::kData, 2));
    // 新 CREDIT 未被对端看到不影响撤回：必须比较最终 spent，不能要求 limits 与 max 相等。
    OpenBoth();
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_EQ(left_.receive.stats().data.granted, 0u);
}

TEST_F(PeerCreditControlTest, StopCannotClaimMoreThanIssuedCredit) {
    OpenRawPeer();
    Send(Stop({9, 0}));
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kCredit);
    ExpectUnretired();
}

TEST_F(PeerCreditControlTest, StopCannotHideAlreadyConsumedRx) {
    Receive(left_, CreditClass::kData, 1);
    OpenRawPeer();
    Send(Stop());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kCredit);
    const auto peer = left_.receive.PeerStats(left_.peer);
    ASSERT_TRUE(peer);
    EXPECT_FALSE(peer->retired);
    EXPECT_EQ(peer->base.granted, 7u);
    EXPECT_FALSE(left_.controller.stats().ack_sent);
}

TEST_F(PeerCreditControlTest, AcknowledgementMustEchoExactFinalSpent) {
    Accept(left_, CreditClass::kControl, 1);
    OpenRawPeer();
    ASSERT_TRUE(Step());
    const auto stop = ReadRemote();
    ASSERT_EQ(stop.kind, HandshakeMessageKind::kCreditStop);
    EXPECT_EQ(stop.credit_stop.spent, (std::array<std::uint64_t, 2>{0, 1}));
    Send(Stop());
    ASSERT_TRUE(Step());
    Send(Ack());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kProtocol);
    EXPECT_FALSE(left_.controller.stats().peer_acknowledged);
}

TEST_F(PeerCreditControlTest, AcknowledgementBeforeAnyLocalStopFailsClosed) {
    const auto reservation = left_.sender.Reserve(CreditClass::kData, 1);
    ASSERT_TRUE(reservation);
    OpenRawPeer();
    Send(Ack());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kProtocol);
    ASSERT_TRUE(left_.sender.Resolve(*reservation, 0));
    ExpectUnretired();
}

TEST_F(PeerCreditControlTest, AcknowledgementBeforePeerStopCannotStandInForItsSnapshot) {
    OpenRawPeer();
    ASSERT_TRUE(Step());
    ASSERT_TRUE(left_.controller.stats().local_stop_sent);
    Send(Ack());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kProtocol);
    EXPECT_FALSE(left_.controller.stats().peer_stop_received);
    EXPECT_FALSE(left_.controller.stats().peer_acknowledged);
    ExpectUnretired();
}

TEST_F(PeerCreditControlTest, PeerHalfCloseAfterFinalAckDoesNotPreventLocalAckFlush) {
    OpenRawPeer();
    ASSERT_TRUE(Step());
    ASSERT_EQ(ReadRemote().kind, HandshakeMessageKind::kCreditStop);
    // 填满本地发送方向，使收到远端最终 ACK 时本地 ACK 仍未送出；对端输入方向继续可用。
    std::array<std::byte, 4096> filler{};
    for (std::size_t i = 0; i < 4096; ++i) {
        if (::send(left_.controller.fd(), filler.data(), filler.size(), MSG_NOSIGNAL) < 0) {
            ASSERT_EQ(errno, EAGAIN);
            break;
        }
    }
    for (std::size_t i = 0; i < 4096; ++i) {
        if (::send(left_.controller.fd(), filler.data(), 1, MSG_NOSIGNAL) < 0) {
            ASSERT_EQ(errno, EAGAIN);
            break;
        }
    }
    Send(Stop());
    ASSERT_TRUE(Step());
    EXPECT_FALSE(left_.controller.stats().ack_sent);
    Send(Ack());
    ASSERT_EQ(::shutdown(remote_.fd(), SHUT_WR), 0);
    ASSERT_TRUE(Step());
    ASSERT_TRUE(left_.controller.stats().peer_acknowledged);
    EXPECT_FALSE(left_.controller.stats().complete);
    // 只排走测试填充字节，再允许本端发完 ACK；此时继续 recv 会看到 EOF，不能覆盖已验证的 ACK。
    while (::recv(remote_.fd(), filler.data(), filler.size(), MSG_DONTWAIT) > 0) {
    }
    ASSERT_EQ(errno, EAGAIN);
    ASSERT_TRUE(Step());
    EXPECT_TRUE(left_.controller.stats().ack_sent);
    EXPECT_TRUE(left_.controller.stats().complete);
}

TEST_F(PeerCreditControlTest, DuplicateStopDoesNotReplaceFrozenSnapshot) {
    OpenRawPeer();
    Send(Stop({1, 0}));
    ASSERT_TRUE(Step());
    EXPECT_EQ(left_.controller.stats().peer_spent[0], 1u);
    Send(Stop());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kProtocol);
    EXPECT_EQ(left_.controller.stats().peer_spent[0], 1u);
    ExpectUnretired();
}

class PeerCreditIdentityTest : public PeerCreditControlTest, public ::testing::WithParamInterface<int> {};
TEST_P(PeerCreditIdentityTest, StopRequiresBothCompleteIdentitiesAndCreditEpochs) {
    OpenRawPeer();
    auto message = Stop();
    switch (GetParam()) {
    case 0:
        ++message.credit_stop.sender.session;
        break;
    case 1:
        ++message.credit_stop.receiver.token.generation;
        break;
    case 2:
        ++message.credit_stop.sender.receive_domain;
        break;
    case 3:
        ++message.credit_stop.sender.authorization;
        break;
    case 4:
        ++message.credit_stop.sender_credit_epoch;
        break;
    default:
        ++message.credit_stop.receiver_credit_epoch;
        break;
    }
    Send(message);
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kProtocol);
    ExpectUnretired();
}
INSTANTIATE_TEST_SUITE_P(EachIdentityComponent, PeerCreditIdentityTest, ::testing::Range(0, 6));

TEST_F(PeerCreditControlTest, InvalidOpenKeepsFdAndDoesNotFreezeOrStopLedgers) {
    auto config = Config(left_, right_);
    config.peer_authorized = false;
    const auto unauthorized = left_.controller.Open(fds_[0], config, 0);
    ASSERT_FALSE(unauthorized);
    EXPECT_EQ(unauthorized.error().code, Code::kUnauthorized);
    config.peer_authorized = true;
    ++config.peer_credit_epoch;
    EXPECT_FALSE(left_.controller.Open(fds_[0], config, 0));
    EXPECT_GE(::fcntl(fds_[0], F_GETFD), 0);
    EXPECT_FALSE(left_.sender.stats().stopped);
    EXPECT_FALSE(left_.receive.PeerStats(left_.peer)->frozen);
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 0u);
    OpenBoth();
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
}

TEST_F(PeerCreditControlTest, ChannelOpenFailureRollsBackWithdrawalBindingCount) {
    // channel Open 失败发生在两份账本均已绑定之后，必须撤销计数且保留重试能力。
    const auto opened = left_.controller.Open(-1, Config(left_, right_), 0);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, Code::kChannel);
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 0u);
    EXPECT_FALSE(left_.sender.stats().stopped);
    EXPECT_FALSE(left_.receive.PeerStats(left_.peer)->frozen);
    OpenBoth();
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 1u);
    Pump();
    EXPECT_TRUE(left_.controller.stats().complete);
}

TEST_F(PeerCreditControlTest, ReceiverBindingFailureRollsBackNewSenderBinding) {
    OpenBoth();
    connection::SenderCreditAccount alternate_sender;
    ASSERT_TRUE(alternate_sender.Open(left_.sender.identity()));
    std::array<int, 2> additional_fds;
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, additional_fds.data()), 0);
    PeerCreditControl duplicate;
    auto config = Config(left_, right_);
    config.sender = &alternate_sender;
    const auto opened = duplicate.Open(additional_fds[0], config, 0);
    EXPECT_FALSE(opened);
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 1u);
    EXPECT_GE(::fcntl(additional_fds[0], F_GETFD), 0);
    EXPECT_FALSE(alternate_sender.stats().stopped);
    EXPECT_TRUE(alternate_sender.Close());
    EXPECT_TRUE(duplicate.Close());
    EXPECT_EQ(left_.receive.stats().withdrawal_bindings, 1u);
    ::close(additional_fds[0]);
    ::close(additional_fds[1]);
}

TEST_F(PeerCreditControlTest, ProgressRejectsClockRegressionWithoutRetiringPeer) {
    OpenRawPeer();
    ASSERT_TRUE(left_.controller.Progress(100));
    const auto result = left_.controller.Progress(99);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, Code::kInvalidArgument);
    ExpectUnretired();
}

TEST_F(PeerCreditControlTest, ProgressAndReceiptReconciliationDoNotAllocate) {
    Accept(left_, CreditClass::kData, 1);
    const auto receipt = Consume(right_, CreditClass::kData);
    OpenBoth();
    bool passed = true;
    allocations = 0;
    count_allocations = true;
    for (int i = 0; i < 8; ++i) {
        passed = passed && left_.controller.Progress(now_++).has_value();
        passed = passed && right_.controller.Progress(now_++).has_value();
    }
    passed = passed && right_.receive.Release(receipt).has_value();
    passed = passed && right_.receive.RecordRefillAccepted(std::span(&receipt, 1)).has_value();
    for (int i = 0; i < 8; ++i) {
        passed = passed && left_.controller.Progress(now_++).has_value();
        passed = passed && right_.controller.Progress(now_++).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(passed);
    EXPECT_EQ(allocations, 0u);
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_TRUE(right_.controller.stats().complete);
}

TEST_F(PeerCreditControlTest, DedicatedIpv4TcpConnectionCompletesTheSameProtocol) {
    ::close(std::exchange(fds_[0], -1));
    ::close(std::exchange(fds_[1], -1));
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    ASSERT_EQ(::listen(listener, 1), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length), 0);
    fds_[0] = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(fds_[0], 0);
    ASSERT_EQ(::connect(fds_[0], reinterpret_cast<sockaddr*>(&address), length), 0);
    fds_[1] = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
    ::close(listener);
    ASSERT_GE(fds_[1], 0);
    ASSERT_EQ(::fcntl(fds_[0], F_SETFL, O_NONBLOCK), 0);
    OpenBoth();
    // TCP 与 Unix socketpair 共用有界状态机；循环上限只约束测试，不将空读当完成。
    for (std::size_t i = 0; i < 1000 && !right_.controller.stats().complete; ++i) {
        Pump(1);
    }
    EXPECT_TRUE(left_.controller.stats().complete);
    EXPECT_TRUE(right_.controller.stats().complete);
}

} // namespace
} // namespace control
} // namespace kbsocket
