// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/peer_credit_progress.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <new>
#include <span>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/runtime/context_progress_group.hpp"
#include "kbsocket/runtime/shard_reactor.hpp"

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
using connection::CreditClass;
using connection::CreditIdentity;
using connection::RxCreditReceipt;
using State = PeerWithdrawalState;

std::uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class PeerCreditProgressTest : public ::testing::Test {
protected:
    struct Endpoint {
        control::HandshakeIdentity identity;
        std::uint64_t epoch = 0;
        connection::ReceiveCreditDomain receive;
        connection::SenderCreditAccount sender;
        connection::CreditPeerHandle peer;
        PeerWithdrawal withdrawal;
        int fd = -1;
    };
    struct Pair {
        std::array<Endpoint, 2> endpoints;
        bool initialized = false;
    };
    std::array<Pair, 64> pairs_;
    PeerCreditProgress progress_;
    control::HandshakeChannel remote_;
    std::array<PeerCreditProgressEvent, 128> events_{};
    std::size_t event_count_ = 0;
    int context_ = 0;
    std::uint64_t now_ = 0;
    static constexpr std::uint64_t kTimeout = 10'000'000'000;

    static void OnEvent(void* argument, const PeerCreditProgressEvent& event) noexcept {
        auto& self = *static_cast<PeerCreditProgressTest*>(argument);
        if (self.event_count_ < self.events_.size()) {
            self.events_[self.event_count_] = event;
        }
        ++self.event_count_;
    }
    void SetUp() override {
        now_ = Now();
        ASSERT_TRUE(progress_.Open({128, &context_, 1'000'000}, OnEvent, this));
        InitPair(0);
    }
    void InitPair(std::size_t index) {
        auto& pair = pairs_[index];
        ASSERT_FALSE(pair.initialized);
        for (std::size_t side = 0; side < 2; ++side) {
            const auto seed = 1000 + index * 100 + side * 10;
            auto& endpoint = pair.endpoints[side];
            endpoint.identity = {seed, seed + 1, seed + 2, seed + 3, seed + 4, {7, 300}};
            endpoint.epoch = seed + 5;
        }
        for (std::size_t side = 0; side < 2; ++side) {
            auto& endpoint = pair.endpoints[side];
            auto& peer = pair.endpoints[1 - side];
            ASSERT_TRUE(endpoint.receive.Open({endpoint.identity.receive_domain, 32, 8, 4, 8}));
            // 纯账本模型记录已被 provider 接受的初始 RX；不把本地 TCP 时序当成 UB 证据。
            ASSERT_TRUE(endpoint.receive.RecordInitialPosted(CreditClass::kData, 24));
            ASSERT_TRUE(endpoint.receive.RecordInitialPosted(CreditClass::kControl, 8));
            const auto admitted =
                endpoint.receive.Admit({peer.identity.runtime, endpoint.identity.receive_domain, endpoint.epoch});
            ASSERT_TRUE(admitted);
            endpoint.peer = *admitted;
            ASSERT_TRUE(endpoint.receive.Grant(endpoint.peer, CreditClass::kData, 8));
            ASSERT_TRUE(endpoint.receive.Grant(endpoint.peer, CreditClass::kControl, 4));
            const CreditIdentity identity{peer.identity.runtime, peer.identity.receive_domain, peer.epoch};
            ASSERT_TRUE(endpoint.sender.Open(identity));
            ASSERT_TRUE(endpoint.sender.Update(identity, 8, 4));
        }
        std::array<int, 2> fds;
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds.data()), 0);
        pair.endpoints[0].fd = fds[0];
        pair.endpoints[1].fd = fds[1];
        pair.initialized = true;
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(remote_.Close());
        for (auto& pair : pairs_) {
            if (!pair.initialized) {
                continue;
            }
            for (auto& endpoint : pair.endpoints) {
                if (progress_.Inspect(endpoint.withdrawal)) {
                    EXPECT_TRUE(progress_.Abort(endpoint.withdrawal));
                }
                if (endpoint.fd >= 0) {
                    ::close(endpoint.fd);
                    endpoint.fd = -1;
                }
                EXPECT_TRUE(endpoint.sender.Close());
                const auto peer = endpoint.receive.PeerStats(endpoint.peer);
                if (peer && !peer->retired) {
                    // 断言结束后模型双方均退出；测试宿主显式清理账本，不假装 EOF 撤销了授信。
                    EXPECT_TRUE(endpoint.receive.RetirePeer(endpoint.peer, {true, true, true}));
                }
                EXPECT_TRUE(endpoint.receive.Close());
            }
        }
        EXPECT_TRUE(progress_.Close());
    }
    Endpoint& Left(std::size_t index = 0) {
        return pairs_[index].endpoints[0];
    }
    Endpoint& Right(std::size_t index = 0) {
        return pairs_[index].endpoints[1];
    }
    control::PeerCreditControlConfig Config(std::size_t index, std::size_t side) {
        auto& local = pairs_[index].endpoints[side];
        const auto& peer = pairs_[index].endpoints[1 - side];
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
    void Begin(std::size_t index, std::size_t side) {
        auto& endpoint = pairs_[index].endpoints[side];
        const auto withdrawal = progress_.BeginWithdrawal(endpoint.fd, Config(index, side), now_);
        ASSERT_TRUE(withdrawal);
        endpoint.fd = -1;
        endpoint.withdrawal = *withdrawal;
    }
    void BeginBoth(std::size_t index = 0) {
        Begin(index, 0);
        Begin(index, 1);
    }
    void Pump(std::size_t rounds = 64, std::size_t budget = 8) {
        for (std::size_t i = 0; i < rounds; ++i) {
            ASSERT_TRUE(progress_.CollectNotifications(budget));
            ASSERT_TRUE(progress_.Progress(ProgressKind::kMailbox, budget, now_));
            ASSERT_TRUE(progress_.Progress(ProgressKind::kTimer, budget, now_));
            now_ += 1'000'000;
        }
    }
    PeerWithdrawalSnapshot Snapshot(const Endpoint& endpoint) {
        const auto snapshot = progress_.Inspect(endpoint.withdrawal);
        EXPECT_TRUE(snapshot);
        return snapshot.value_or(PeerWithdrawalSnapshot{});
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
    void Receive(Endpoint& endpoint, CreditClass kind, std::uint32_t count) {
        for (std::uint32_t i = 0; i < count; ++i) {
            const auto receipt = Consume(endpoint, kind);
            ASSERT_TRUE(endpoint.receive.Release(receipt));
            ASSERT_TRUE(endpoint.receive.RecordRefillAccepted(std::span(&receipt, 1)));
        }
    }
    void ExpectFrozenUnretired(const Endpoint& endpoint) {
        const auto peer = endpoint.receive.PeerStats(endpoint.peer);
        ASSERT_TRUE(peer);
        EXPECT_TRUE(peer->frozen);
        EXPECT_FALSE(peer->retired);
        EXPECT_EQ(peer->base.granted, 8u);
        EXPECT_EQ(peer->control_granted, 4u);
        EXPECT_TRUE(endpoint.sender.stats().stopped);
    }
};

TEST_F(PeerCreditProgressTest, RequiresContextCapacityAndNonzeroSweepInterval) {
    PeerCreditProgress unopened;
    EXPECT_FALSE(unopened.Open({0, &context_, 1}));
    EXPECT_FALSE(unopened.Open({1, nullptr, 1}));
    EXPECT_FALSE(unopened.Open({1, &context_, 0}));
    EXPECT_FALSE(unopened.Open({PeerCreditProgress::kMaxPeers + 1, &context_, 1}));
    EXPECT_FALSE(unopened.BeginWithdrawal(Left().fd, Config(0, 0), now_));
    EXPECT_FALSE(unopened.Inspect({}));
    EXPECT_FALSE(unopened.Wake({}));
    EXPECT_FALSE(unopened.Rearm());
    EXPECT_TRUE(unopened.wait_fds().empty());
    EXPECT_EQ(unopened.exclusive_context(), nullptr);
    EXPECT_EQ(unopened.stats(), nullptr);
    EXPECT_TRUE(unopened.Close());
}

TEST_F(PeerCreditProgressTest, BeginStopsAndFreezesWholePeerButCloseAndReleaseNeedTerminalState) {
    Begin(0, 0);
    ExpectFrozenUnretired(Left());
    EXPECT_EQ(Snapshot(Left()).state, State::kActive);
    EXPECT_FALSE(progress_.Release(Left().withdrawal));
    EXPECT_FALSE(progress_.Close());
    EXPECT_FALSE(Left().sender.Close());
    EXPECT_FALSE(Left().receive.RetirePeer(Left().peer, {true, true, true}));
    EXPECT_EQ(progress_.stats()->attached, 1u);
    EXPECT_EQ(progress_.exclusive_context(), &context_);
}

TEST_F(PeerCreditProgressTest, BothSidesCompleteAndRetainBindingUntilExplicitRelease) {
    BeginBoth();
    Pump();
    EXPECT_EQ(event_count_, 2u);
    EXPECT_EQ(progress_.stats()->completed, 2u);
    for (auto* endpoint : {&Left(), &Right()}) {
        const auto snapshot = Snapshot(*endpoint);
        EXPECT_EQ(snapshot.state, State::kComplete);
        EXPECT_TRUE(snapshot.protocol.complete);
        EXPECT_FALSE(snapshot.failure);
        EXPECT_FALSE(snapshot.scheduling_failure);
        EXPECT_EQ(endpoint->receive.stats().active_peers, 0u);
        EXPECT_EQ(endpoint->receive.stats().physical_posted, 32u);
        EXPECT_FALSE(endpoint->sender.Close());
        EXPECT_FALSE(endpoint->receive.Close());
    }
    EXPECT_FALSE(progress_.Close());
    ASSERT_TRUE(progress_.Release(Left().withdrawal));
    ASSERT_TRUE(progress_.Release(Right().withdrawal));
    EXPECT_EQ(progress_.stats()->attached, 0u);
    EXPECT_TRUE(Left().sender.Close());
    EXPECT_TRUE(Right().sender.Close());
}

TEST_F(PeerCreditProgressTest, TerminalCallbacksAreSingleShotAndDoNotKeepFdReady) {
    BeginBoth();
    Pump();
    ASSERT_EQ(event_count_, 2u);
    const auto visits = progress_.stats()->visits;
    Pump();
    EXPECT_EQ(event_count_, 2u);
    EXPECT_EQ(progress_.stats()->visits, visits);
    ASSERT_EQ(progress_.wait_fds().size(), 1u);
    pollfd fd{progress_.wait_fds()[0], POLLIN, 0};
    EXPECT_EQ(::poll(&fd, 1, 0), 0);
    EXPECT_FALSE(progress_.CollectNotifications(8)->runnable);
}

TEST_F(PeerCreditProgressTest, UnauthorizedAndBadFdFailuresLeaveOwnershipAndCreditsUntouched) {
    auto config = Config(0, 0);
    config.peer_authorized = false;
    EXPECT_FALSE(progress_.BeginWithdrawal(Left().fd, config, now_));
    EXPECT_GE(::fcntl(Left().fd, F_GETFD), 0);
    EXPECT_FALSE(Left().sender.stats().stopped);
    EXPECT_FALSE(Left().receive.PeerStats(Left().peer)->frozen);
    EXPECT_FALSE(progress_.BeginWithdrawal(-1, Config(0, 0), now_));
    // 前次失败不得残留控制器绑定；同一个合法 fd 随后仍可成功 Begin。
    BeginBoth();
    Pump();
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
}

TEST_F(PeerCreditProgressTest, EpollRegistrationFailureDoesNotFreezeLedgers) {
    const int fd = ::open("/dev/null", O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    EXPECT_FALSE(progress_.BeginWithdrawal(fd, Config(0, 0), now_));
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_FALSE(Left().sender.stats().stopped);
    EXPECT_FALSE(Left().receive.PeerStats(Left().peer)->frozen);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    ::close(fd);
}

TEST_F(PeerCreditProgressTest, BlockingSocketValidationRollsBackRegistrationWithoutTakingFd) {
    const auto flags = ::fcntl(Left().fd, F_GETFL);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(Left().fd, F_SETFL, flags & ~O_NONBLOCK), 0);
    EXPECT_FALSE(progress_.BeginWithdrawal(Left().fd, Config(0, 0), now_));
    EXPECT_GE(::fcntl(Left().fd, F_GETFD), 0);
    EXPECT_FALSE(Left().sender.stats().stopped);
    EXPECT_FALSE(Left().receive.PeerStats(Left().peer)->frozen);
    ASSERT_EQ(::fcntl(Left().fd, F_SETFL, flags), 0);
    BeginBoth();
    Pump();
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
}

TEST_F(PeerCreditProgressTest, CapacityRejectionLeavesSecondPeerUsable) {
    ASSERT_TRUE(progress_.Close());
    ASSERT_TRUE(progress_.Open({1, &context_, 1'000'000}, OnEvent, this));
    Begin(0, 0);
    EXPECT_FALSE(progress_.BeginWithdrawal(Right().fd, Config(0, 1), now_));
    EXPECT_GE(::fcntl(Right().fd, F_GETFD), 0);
    EXPECT_FALSE(Right().sender.stats().stopped);
    EXPECT_FALSE(Right().receive.PeerStats(Right().peer)->frozen);
    EXPECT_EQ(progress_.stats()->attached, 1u);
}

TEST_F(PeerCreditProgressTest, DuplicateFdAndLedgerCannotMutateAnotherPeer) {
    InitPair(1);
    const auto owned_fd = Left().fd;
    Begin(0, 0);
    EXPECT_FALSE(progress_.BeginWithdrawal(owned_fd, Config(1, 0), now_));
    EXPECT_FALSE(Left(1).sender.stats().stopped);
    EXPECT_FALSE(Left(1).receive.PeerStats(Left(1).peer)->frozen);
    // 不同 fd 仍不允许第二个控制器绑定同一份账本；失败后新 fd 归宿主。
    EXPECT_FALSE(progress_.BeginWithdrawal(Left(1).fd, Config(0, 0), now_));
    EXPECT_GE(::fcntl(Left(1).fd, F_GETFD), 0);
    EXPECT_EQ(progress_.stats()->attached, 1u);
    BeginBoth(1);
    Pump();
    EXPECT_EQ(Snapshot(Left(1)).state, State::kComplete);
    EXPECT_EQ(Snapshot(Left()).state, State::kActive);
}

TEST_F(PeerCreditProgressTest, AbortClosesOwnedFdButDoesNotReclaimUnconfirmedGrants) {
    const auto fd = Left().fd;
    Begin(0, 0);
    ASSERT_TRUE(progress_.Abort(Left().withdrawal));
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    ExpectFrozenUnretired(Left());
    EXPECT_FALSE(progress_.Inspect(Left().withdrawal));
    EXPECT_FALSE(progress_.Wake(Left().withdrawal));
    EXPECT_FALSE(progress_.Abort(Left().withdrawal));
    EXPECT_EQ(progress_.stats()->aborted, 1u);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    // 解除的是调度控制绑定；只有宿主已有的真实终结证明才能另行清退账本。
    EXPECT_TRUE(Left().sender.Close());
}

TEST_F(PeerCreditProgressTest, OldAndForeignHandlesCannotReachReusedSlots) {
    ASSERT_TRUE(progress_.Close());
    ASSERT_TRUE(progress_.Open({1, &context_, 1'000'000}, OnEvent, this));
    Begin(0, 0);
    const auto old = Left().withdrawal;
    ASSERT_TRUE(progress_.Abort(old));
    InitPair(1);
    Begin(1, 0);
    EXPECT_NE(old.id, Left(1).withdrawal.id);
    EXPECT_FALSE(progress_.Inspect(old));
    EXPECT_FALSE(progress_.Release(old));
    EXPECT_FALSE(progress_.Wake(old));
    PeerCreditProgress other;
    ASSERT_TRUE(other.Open({1, &context_, 1'000'000}));
    EXPECT_FALSE(other.Inspect(Left(1).withdrawal));
    EXPECT_FALSE(other.Wake(Left(1).withdrawal));
    EXPECT_EQ(Snapshot(Left(1)).state, State::kActive);
}

TEST_F(PeerCreditProgressTest, OldHandleRemainsInvalidAcrossCloseAndOpen) {
    Begin(0, 0);
    const auto old = Left().withdrawal;
    ASSERT_TRUE(progress_.Abort(old));
    ASSERT_TRUE(progress_.Close());
    ASSERT_TRUE(progress_.Open({128, &context_, 1'000'000}, OnEvent, this));
    InitPair(1);
    Begin(1, 0);
    EXPECT_NE(old.id, Left(1).withdrawal.id);
    EXPECT_FALSE(progress_.Inspect(old));
    EXPECT_FALSE(progress_.Abort(old));
    EXPECT_EQ(progress_.stats()->attached, 1u);
}

TEST_F(PeerCreditProgressTest, PendingPostWaitsForKnownAcceptedPrefixAndTimerReconciles) {
    const auto reservation = Left().sender.Reserve(CreditClass::kData, 3);
    ASSERT_TRUE(reservation);
    BeginBoth();
    Pump();
    EXPECT_FALSE(Snapshot(Left()).protocol.local_snapshot);
    EXPECT_EQ(Snapshot(Left()).state, State::kActive);
    // Stop 前的在途 provider 调用只接受前缀 1；不显式 Wake，定时巡检也必须重新取快照。
    ASSERT_TRUE(Left().sender.Resolve(*reservation, 1));
    Receive(Right(), CreditClass::kData, 1);
    Pump();
    EXPECT_EQ(Snapshot(Left()).protocol.local_spent[0], 1u);
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
    EXPECT_EQ(Snapshot(Right()).state, State::kComplete);
}

TEST_F(PeerCreditProgressTest, LateRxHeldAndReleasedNeedActualRefillBeforeCompletion) {
    Accept(Right(), CreditClass::kData, 1);
    BeginBoth();
    Pump();
    EXPECT_TRUE(Snapshot(Left()).protocol.peer_stop_received);
    ExpectFrozenUnretired(Left());
    const auto receipt = Consume(Left(), CreditClass::kData);
    Pump();
    EXPECT_FALSE(Snapshot(Left()).protocol.receive_retired);
    ASSERT_TRUE(Left().receive.Release(receipt));
    ASSERT_TRUE(progress_.Wake(Left().withdrawal));
    Pump();
    EXPECT_FALSE(Snapshot(Left()).protocol.receive_retired);
    ASSERT_TRUE(Left().receive.RecordRefillAccepted(std::span(&receipt, 1)));
    ASSERT_TRUE(progress_.Wake(Left().withdrawal));
    Pump();
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
    EXPECT_EQ(Snapshot(Right()).state, State::kComplete);
}

TEST_F(PeerCreditProgressTest, DataAndControlSpentStaySeparateAcrossWithdrawal) {
    Accept(Left(), CreditClass::kData, 3);
    Accept(Left(), CreditClass::kControl, 2);
    Accept(Right(), CreditClass::kData, 1);
    Receive(Right(), CreditClass::kData, 3);
    Receive(Right(), CreditClass::kControl, 2);
    Receive(Left(), CreditClass::kData, 1);
    BeginBoth();
    Pump();
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
    EXPECT_EQ(Snapshot(Left()).protocol.local_spent, (std::array<std::uint64_t, 2>{3, 2}));
    EXPECT_EQ(Snapshot(Left()).protocol.peer_spent, (std::array<std::uint64_t, 2>{1, 0}));
    EXPECT_EQ(Snapshot(Right()).protocol.peer_spent, Snapshot(Left()).protocol.local_spent);
}

TEST_F(PeerCreditProgressTest, EofFailsOnlyOnePeerAndDoesNotFakeRetirement) {
    InitPair(1);
    Begin(0, 0);
    ASSERT_EQ(::close(Right().fd), 0);
    Right().fd = -1;
    BeginBoth(1);
    Pump();
    const auto failed = Snapshot(Left());
    EXPECT_EQ(failed.state, State::kFailed);
    ASSERT_TRUE(failed.failure);
    EXPECT_EQ(failed.failure->code, control::PeerCreditControlErrorCode::kChannel);
    ExpectFrozenUnretired(Left());
    EXPECT_EQ(Snapshot(Left(1)).state, State::kComplete);
    EXPECT_EQ(Snapshot(Right(1)).state, State::kComplete);
    EXPECT_EQ(progress_.stats()->failures, 1u);
    EXPECT_EQ(event_count_, 3u);
    EXPECT_FALSE(Left().sender.Close());
    ASSERT_TRUE(progress_.Release(Left().withdrawal));
    EXPECT_TRUE(Left().sender.Close());
}

TEST_F(PeerCreditProgressTest, TimeoutWithoutFdReadinessFailsOnlyExpiredPeer) {
    InitPair(1);
    auto config = Config(0, 0);
    config.timeout_ns = 1'000'000'000;
    const auto handle = progress_.BeginWithdrawal(Left().fd, config, now_);
    ASSERT_TRUE(handle);
    Left().withdrawal = *handle;
    Left().fd = -1;
    BeginBoth(1);
    Pump();
    ASSERT_EQ(Snapshot(Left()).state, State::kActive);
    now_ += 2'000'000'000;
    Pump();
    const auto failed = Snapshot(Left());
    EXPECT_EQ(failed.state, State::kFailed);
    ASSERT_TRUE(failed.failure);
    EXPECT_EQ(failed.failure->code, control::PeerCreditControlErrorCode::kTimeout);
    ExpectFrozenUnretired(Left());
    EXPECT_EQ(Snapshot(Left(1)).state, State::kComplete);
    EXPECT_EQ(Snapshot(Right(1)).state, State::kComplete);
}

TEST_F(PeerCreditProgressTest, UnknownAcceptedPostBecomesPerPeerFailureWithGrantsRetained) {
    const auto reservation = Left().sender.Reserve(CreditClass::kData, 2);
    ASSERT_TRUE(reservation);
    Begin(0, 0);
    ASSERT_TRUE(Left().sender.Resolve(*reservation, std::nullopt));
    Pump();
    const auto snapshot = Snapshot(Left());
    EXPECT_EQ(snapshot.state, State::kFailed);
    ASSERT_TRUE(snapshot.failure);
    EXPECT_EQ(snapshot.failure->code, control::PeerCreditControlErrorCode::kCredit);
    EXPECT_FALSE(snapshot.protocol.local_snapshot);
    ExpectFrozenUnretired(Left());
}

TEST_F(PeerCreditProgressTest, WaitingPeerIsNotContinuouslyRunnableBetweenSweeps) {
    Begin(0, 0);
    Pump();
    const auto snapshot = Snapshot(Left());
    ASSERT_TRUE(snapshot.protocol.local_stop_sent);
    ASSERT_FALSE(snapshot.protocol.peer_stop_received);
    ASSERT_TRUE(progress_.Progress(ProgressKind::kMailbox, 8, now_));
    // 对端静默且没有待发送字节。等待协议、RX 或定时器不能使 Reactor 永久忙轮询。
    const auto visits = progress_.stats()->visits;
    for (int i = 0; i < 8; ++i) {
        const auto collected = progress_.CollectNotifications(8);
        ASSERT_TRUE(collected);
        EXPECT_FALSE(collected->runnable);
        const auto advanced = progress_.Progress(ProgressKind::kMailbox, 8, now_);
        ASSERT_TRUE(advanced);
        EXPECT_EQ(advanced->processed, 0u);
        EXPECT_FALSE(advanced->runnable);
    }
    EXPECT_EQ(progress_.stats()->visits, visits);
}

TEST_F(PeerCreditProgressTest, MailboxBudgetAndWakeCoalescingBoundVisits) {
    InitPair(1);
    BeginBoth();
    BeginBoth(1);
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(progress_.Wake(Left().withdrawal));
    }
    const auto before = progress_.stats()->visits;
    const auto no_budget = progress_.Progress(ProgressKind::kMailbox, 0, now_);
    ASSERT_TRUE(no_budget);
    EXPECT_EQ(progress_.stats()->visits, before);
    const auto result = progress_.Progress(ProgressKind::kMailbox, 1, now_);
    ASSERT_TRUE(result);
    EXPECT_EQ(progress_.stats()->visits, before + 1);
    EXPECT_TRUE(result->runnable);
    Pump(128, 1);
    EXPECT_EQ(progress_.stats()->completed, 4u);
}

TEST_F(PeerCreditProgressTest, MoreThanReactorFdLimitUsesOneStableOuterDescriptor) {
    ASSERT_EQ(progress_.wait_fds().size(), 1u);
    const auto outer = progress_.wait_fds()[0];
    // 96 个动态协议 fd 超过外层固定 80 fd 限额；它们只占用一个稳定 inner epoll 描述符。
    for (std::size_t i = 0; i < 48; ++i) {
        if (i) {
            InitPair(i);
        }
        BeginBoth(i);
        ASSERT_EQ(progress_.wait_fds().size(), 1u);
        EXPECT_EQ(progress_.wait_fds()[0], outer);
    }
    Pump(256, 8);
    EXPECT_EQ(progress_.stats()->completed, 96u);
    EXPECT_EQ(event_count_, 96u);
    for (std::size_t i = 0; i < 48; ++i) {
        ASSERT_EQ(Snapshot(Left(i)).state, State::kComplete);
        ASSERT_EQ(Snapshot(Right(i)).state, State::kComplete);
        ASSERT_TRUE(progress_.Release(Left(i).withdrawal));
        ASSERT_TRUE(progress_.Release(Right(i).withdrawal));
    }
    EXPECT_EQ(progress_.wait_fds()[0], outer);
    EXPECT_EQ(progress_.stats()->attached, 0u);
}

TEST_F(PeerCreditProgressTest, ReleasingDenseMiddleSlotDoesNotSkipOtherPeersDuringSweep) {
    InitPair(1);
    InitPair(2);
    Begin(0, 0);
    Begin(1, 0);
    Begin(2, 0);
    Pump();
    ASSERT_TRUE(progress_.Progress(ProgressKind::kTimer, 1, now_));
    ASSERT_TRUE(progress_.Abort(Left(1).withdrawal));
    // 扫描过程中 dense swap-remove，新的尾部成员仍须能被巡检超时。
    now_ += kTimeout + 1;
    Pump(32, 1);
    EXPECT_EQ(Snapshot(Left()).state, State::kFailed);
    EXPECT_EQ(Snapshot(Left(2)).state, State::kFailed);
    EXPECT_EQ(progress_.stats()->failures, 2u);
}

TEST_F(PeerCreditProgressTest, EarlierTickTimeAfterBeginDoesNotCauseClockRegressionFailure) {
    const auto earlier = now_;
    now_ += 1'000'000;
    BeginBoth();
    for (int i = 0; i < 16; ++i) {
        ASSERT_TRUE(progress_.CollectNotifications(8));
        ASSERT_TRUE(progress_.Progress(ProgressKind::kMailbox, 8, earlier));
    }
    Pump();
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
    EXPECT_EQ(progress_.stats()->failures, 0u);
}

TEST_F(PeerCreditProgressTest, ProgressAndNotificationCollectionAllocateNoMemory) {
    BeginBoth();
    allocations = 0;
    count_allocations = true;
    bool ok = true;
    for (int i = 0; i < 64; ++i) {
        ok = static_cast<bool>(progress_.CollectNotifications(8)) && ok;
        ok = static_cast<bool>(progress_.Progress(ProgressKind::kMailbox, 8, now_)) && ok;
        ok = static_cast<bool>(progress_.Progress(ProgressKind::kTimer, 8, now_)) && ok;
        ok = static_cast<bool>(progress_.Rearm()) && ok;
        now_ += 1'000'000;
    }
    count_allocations = false;
    EXPECT_TRUE(ok);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(progress_.stats()->completed, 2u);
}

TEST_F(PeerCreditProgressTest, LocalIpv4TcpCompletesViaNonblockingReadiness) {
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    ASSERT_EQ(::listen(listener, 1), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length), 0);
    const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    ASSERT_EQ(::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    const int server = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    ASSERT_GE(server, 0);
    ASSERT_EQ(::fcntl(client, F_SETFL, ::fcntl(client, F_GETFL) | O_NONBLOCK), 0);
    ::close(listener);
    ::close(Left().fd);
    ::close(Right().fd);
    Left().fd = client;
    Right().fd = server;
    BeginBoth();
    Pump(128);
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
    EXPECT_EQ(Snapshot(Right()).state, State::kComplete);
    EXPECT_GT(progress_.stats()->fd_events, 0u);
}

TEST_F(PeerCreditProgressTest, SocketBackpressureDoesNotDeclareOutputImmediatelyRunnable) {
    const int send_buffer = 1024;
    ASSERT_EQ(::setsockopt(Left().fd, SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer)), 0);
    std::array<char, 1024> data{};
    // 填满真实非阻塞 socket，保留对端不读；协议仍需等待 EPOLLOUT，不能因 output_pending 忙转。
    int writes = 0;
    while (::send(Left().fd, data.data(), data.size(), MSG_NOSIGNAL) > 0 && ++writes < 1024) {
    }
    ASSERT_LT(writes, 1024);
    ASSERT_EQ(errno, EAGAIN);
    Begin(0, 0);
    Pump();
    ASSERT_TRUE(progress_.Progress(ProgressKind::kMailbox, 8, now_));
    EXPECT_FALSE(Snapshot(Left()).protocol.local_stop_sent);
    EXPECT_EQ(Snapshot(Left()).state, State::kActive);
    const auto visits = progress_.stats()->visits;
    for (int i = 0; i < 8; ++i) {
        const auto collected = progress_.CollectNotifications(8);
        ASSERT_TRUE(collected);
        EXPECT_FALSE(collected->runnable);
        const auto advanced = progress_.Progress(ProgressKind::kMailbox, 8, now_);
        ASSERT_TRUE(advanced);
        EXPECT_FALSE(advanced->runnable);
    }
    EXPECT_EQ(progress_.stats()->visits, visits);
}

TEST_F(PeerCreditProgressTest, QueuedOldDescriptorEventCannotWakeReusedSlot) {
    ASSERT_TRUE(progress_.Close());
    ASSERT_TRUE(progress_.Open({1, &context_, 1'000'000}, OnEvent, this));
    const int alias = ::dup(Left().fd);
    ASSERT_GE(alias, 0);
    Begin(0, 0);
    ASSERT_EQ(::close(Right().fd), 0);
    Right().fd = -1;
    // 不先收 epoll：Progress 读到 EOF 自行 close，但 dup 使旧 open-file description/watch 存活。
    ASSERT_TRUE(progress_.Progress(ProgressKind::kMailbox, 1, now_));
    ASSERT_EQ(Snapshot(Left()).state, State::kFailed);
    ASSERT_TRUE(progress_.Release(Left().withdrawal));
    InitPair(1);
    Begin(1, 0);
    const auto notifications = progress_.CollectNotifications(8);
    ASSERT_TRUE(notifications);
    EXPECT_GE(progress_.stats()->stale_events, 1u);
    EXPECT_EQ(Snapshot(Left(1)).state, State::kActive);
    EXPECT_FALSE(Snapshot(Left(1)).failure);
    ::close(alias);
}

class PausedPeerHarness {
public:
    class Blocker final : public ProgressSource {
    public:
        std::atomic<bool> entered{false}, released{false};
        bool requested = true;
        static ControlTaskResult Run(void* argument, std::uint64_t) noexcept {
            auto& self = *static_cast<Blocker*>(argument);
            self.entered.store(true, std::memory_order_release);
            self.released.wait(false, std::memory_order_acquire);
            return {};
        }
        void Release() {
            released.store(true, std::memory_order_release);
            released.notify_all();
        }
        std::expected<ProgressBatch, ProgressError> Progress(ProgressKind, std::size_t,
                                                             std::uint64_t) noexcept override {
            return ProgressBatch{};
        }
        std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t) noexcept override {
            return ProgressBatch{};
        }
        std::expected<void, ProgressError> Rearm() noexcept override {
            return {};
        }
        std::span<const int> wait_fds() const noexcept override {
            return {};
        }
        std::optional<ControlTask> PendingControl() noexcept override {
            return requested ? std::optional(ControlTask{Run, this, 1}) : std::nullopt;
        }
        void ControlSubmitted(ControlTaskId) noexcept override {
            requested = false;
        }
    } blocker;
    // 析构先终结任务再关闭借用者，ASSERT 提前退出也不能让 worker 访问已析构的参数。
    ~PausedPeerHarness() {
        reactor.StopAccepting();
        blocker.Release();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (executor.outstanding() && std::chrono::steady_clock::now() < deadline) {
            (void)reactor.Tick();
            std::this_thread::yield();
        }
        EXPECT_EQ(executor.outstanding(), 0u);
        EXPECT_TRUE(reactor.Close());
        EXPECT_TRUE(group.Close());
        EXPECT_TRUE(executor.Close());
        for (auto handle : other_handles) {
            if (other.Inspect(handle)) {
                EXPECT_TRUE(other.Abort(handle));
            }
        }
        EXPECT_TRUE(other.Close());
    }
    PeerCreditProgress other;
    std::array<PeerWithdrawal, 2> other_handles{};
    ContextProgressGroup group;
    ControlExecutor executor;
    ShardReactor reactor;
};

TEST_F(PeerCreditProgressTest, ReactorPausesRealWithdrawalWithContextAndResumesPendingReadiness) {
    InitPair(1);
    PausedPeerHarness harness;
    int other_context = 0;
    ASSERT_TRUE(harness.other.Open({2, &other_context, 1'000'000}));
    for (std::size_t side = 0; side < 2; ++side) {
        auto& endpoint = pairs_[1].endpoints[side];
        const auto handle = harness.other.BeginWithdrawal(endpoint.fd, Config(1, side), now_);
        ASSERT_TRUE(handle);
        harness.other_handles[side] = *handle;
        endpoint.fd = -1;
    }
    BeginBoth();
    std::array<ProgressSource*, 2> members{&progress_, &harness.blocker};
    ASSERT_TRUE(harness.group.Open(members));
    ASSERT_TRUE(harness.executor.Open({1, 1}));
    std::array<ProgressSource*, 2> sources{&harness.group, &harness.other};
    ASSERT_TRUE(harness.reactor.Open(sources, harness.executor, {.events = true, .budget = 4}));
    ASSERT_TRUE(harness.reactor.Tick());
    const auto entered_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!harness.blocker.entered.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < entered_deadline) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(harness.blocker.entered.load(std::memory_order_acquire));
    ASSERT_TRUE(harness.reactor.paused(0));
    const auto* paused_stats = progress_.stats();
    const auto visits = paused_stats->visits;
    const auto notifications = paused_stats->fd_events;
    // 同 context 的慢任务持有屏障时，不再读取协议 socket/账本；其他 context 仍能完整撤回。
    for (int i = 0; i < 64; ++i) {
        ASSERT_TRUE(harness.reactor.Tick());
    }
    EXPECT_EQ(paused_stats->visits, visits);
    EXPECT_EQ(paused_stats->fd_events, notifications);
    EXPECT_EQ(harness.other.stats()->completed, 2u);
    EXPECT_FALSE(harness.reactor.Close());
    harness.blocker.Release();
    const auto resumed_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (progress_.stats()->completed < 2 && std::chrono::steady_clock::now() < resumed_deadline) {
        ASSERT_TRUE(harness.reactor.RunOnce());
    }
    EXPECT_FALSE(harness.reactor.paused(0));
    EXPECT_EQ(progress_.stats()->completed, 2u);
    EXPECT_GT(progress_.stats()->visits, visits);
    EXPECT_EQ(Snapshot(Left()).state, State::kComplete);
    EXPECT_EQ(Snapshot(Right()).state, State::kComplete);
}

} // namespace
} // namespace runtime
} // namespace kbsocket
