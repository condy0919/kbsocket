// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/credit_account.hpp"

#include <unistd.h>

#include <array>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

namespace {
thread_local bool count_credit_allocations = false;
thread_local std::size_t credit_allocations = 0;
thread_local int fail_credit_allocation = -1;
void RecordAllocation() {
    if (count_credit_allocations) {
        ++credit_allocations;
    }
    if (fail_credit_allocation == 0) {
        fail_credit_allocation = -1;
        throw std::bad_alloc();
    }
    if (fail_credit_allocation > 0) {
        --fail_credit_allocation;
    }
}
} // namespace
void* operator new(std::size_t size) {
    RecordAllocation();
    if (auto* value = std::malloc(size ? size : 1)) {
        return value;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void operator delete(void* value) noexcept {
    std::free(value);
}
void operator delete[](void* value) noexcept {
    std::free(value);
}
void operator delete(void* value, std::size_t) noexcept {
    std::free(value);
}
void operator delete[](void* value, std::size_t) noexcept {
    std::free(value);
}

namespace kbsocket {
namespace connection {
namespace {
constexpr CreditIdentity kIdentity{1, 10, 1};
constexpr CreditRetireProof kProof{true, true, true};

TEST(SenderCreditAccountTest, CumulativeUpdatesAreIdempotentAndBoundToFullIdentity) {
    SenderCreditAccount sender;
    EXPECT_FALSE(sender.Update(kIdentity, 8, 2));
    EXPECT_FALSE(sender.Open({0, 10, 1}));
    ASSERT_TRUE(sender.Open(kIdentity));
    ASSERT_TRUE(sender.Update(kIdentity, 8, 2));
    ASSERT_TRUE(sender.Update(kIdentity, 8, 2));
    ASSERT_TRUE(sender.Update(kIdentity, 5, 1));
    EXPECT_EQ(sender.available(CreditClass::kData), 8u);
    EXPECT_EQ(sender.available(CreditClass::kControl), 2u);
    for (const auto identity : {CreditIdentity{2, 10, 1}, CreditIdentity{1, 11, 1}, CreditIdentity{1, 10, 2}}) {
        auto wrong = sender.Update(identity, 100, 100);
        ASSERT_FALSE(wrong);
        EXPECT_EQ(wrong.error().code, CreditErrorCode::kWrongIdentity);
    }
    EXPECT_EQ(sender.available(CreditClass::kData), 8u);
    EXPECT_TRUE(sender.Close());
}

TEST(SenderCreditAccountTest, OnlyKnownUnacceptedSuffixReturnsCredits) {
    SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open(kIdentity));
    ASSERT_TRUE(sender.Update(kIdentity, 8, 2));
    auto batch = sender.Reserve(CreditClass::kData, 6);
    ASSERT_TRUE(batch);
    EXPECT_EQ(sender.available(CreditClass::kData), 2u);
    auto overtaking = sender.Reserve(CreditClass::kControl, 1);
    ASSERT_FALSE(overtaking);
    EXPECT_EQ(overtaking.error().code, CreditErrorCode::kInUse);
    EXPECT_FALSE(sender.Resolve(*batch, 7));
    EXPECT_EQ(sender.stats().reserved[0], 6u);

    // post 只接受前两条，未接受的四条归还；成功 TX 完成不会额外增加任何 credits。
    ASSERT_TRUE(sender.Resolve(*batch, 2));
    EXPECT_EQ(sender.stats().spent[0], 2u);
    EXPECT_EQ(sender.available(CreditClass::kData), 6u);
    EXPECT_FALSE(sender.Resolve(*batch, 0));
    auto retry = sender.Reserve(CreditClass::kData, 6);
    ASSERT_TRUE(retry);
    ASSERT_TRUE(sender.Resolve(*retry, 0));
    EXPECT_EQ(sender.available(CreditClass::kData), 6u);
    auto accepted = sender.Reserve(CreditClass::kData, 6);
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(sender.Resolve(*accepted, 6));
    EXPECT_EQ(sender.available(CreditClass::kData), 0u);
    auto blocked = sender.Reserve(CreditClass::kData, 1);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, CreditErrorCode::kExhausted);
    ASSERT_TRUE(sender.Update(kIdentity, 9, 2));
    EXPECT_EQ(sender.available(CreditClass::kData), 1u);
    EXPECT_TRUE(sender.Close());
}

TEST(SenderCreditAccountTest, UnknownAcceptanceConsumesWholeBatchAndStopsBothClasses) {
    SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open(kIdentity));
    ASSERT_TRUE(sender.Update(kIdentity, 8, 2));
    auto batch = sender.Reserve(CreditClass::kData, 5);
    ASSERT_TRUE(batch);
    ASSERT_TRUE(sender.Resolve(*batch, std::nullopt));
    EXPECT_EQ(sender.stats().spent[0], 5u);
    EXPECT_EQ(sender.stats().reserved[0], 0u);
    EXPECT_TRUE(sender.stats().stopped);
    EXPECT_FALSE(sender.Reserve(CreditClass::kControl, 1));
    ASSERT_TRUE(sender.Update(kIdentity, 100, 100));
    EXPECT_EQ(sender.available(CreditClass::kData), 0u);
    EXPECT_TRUE(sender.Close());
}

TEST(SenderCreditAccountTest, ClosePreservesPendingReservationAndRequiresHigherReopenEpoch) {
    SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open(kIdentity));
    ASSERT_TRUE(sender.Update(kIdentity, 2, 1));
    auto old = sender.Reserve(CreditClass::kData, 2);
    ASSERT_TRUE(old);
    auto closed = sender.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, CreditErrorCode::kInUse);
    EXPECT_TRUE(sender.stats().stopped);
    ASSERT_TRUE(sender.Resolve(*old, 0));
    ASSERT_TRUE(sender.Close());
    EXPECT_FALSE(sender.Open(kIdentity));
    EXPECT_FALSE(sender.Open({2, 10, 2}));
    EXPECT_FALSE(sender.Open({1, 11, 2}));
    ASSERT_TRUE(sender.Open({1, 10, 2}));
    ASSERT_TRUE(sender.Update({1, 10, 2}, 2, 1));
    auto current = sender.Reserve(CreditClass::kData, 2);
    ASSERT_TRUE(current);
    EXPECT_NE(current->id, old->id);
    EXPECT_FALSE(sender.Resolve(*old, 0));
    EXPECT_EQ(sender.stats().reserved[0], 2u);
    ASSERT_TRUE(sender.Resolve(*current, 2));
    EXPECT_TRUE(sender.Close());
}

TEST(SenderCreditAccountTest, DataExhaustionDoesNotBorrowControlAndLargeLimitsDoNotWrap) {
    SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open(kIdentity));
    ASSERT_TRUE(sender.Update(kIdentity, 1, 1));
    auto data = sender.Reserve(CreditClass::kData, 1);
    ASSERT_TRUE(data);
    ASSERT_TRUE(sender.Resolve(*data, 1));
    EXPECT_FALSE(sender.Reserve(CreditClass::kData, 1));
    auto control = sender.Reserve(CreditClass::kControl, 1);
    ASSERT_TRUE(control);
    ASSERT_TRUE(sender.Resolve(*control, 1));
    EXPECT_FALSE(sender.Reserve(CreditClass::kControl, 1));
    ASSERT_TRUE(sender.Update(kIdentity, std::numeric_limits<std::uint64_t>::max(), 1));
    EXPECT_EQ(sender.available(CreditClass::kData), std::numeric_limits<std::uint64_t>::max() - 1);
    auto large = sender.Reserve(CreditClass::kData, std::numeric_limits<std::uint32_t>::max());
    ASSERT_TRUE(large);
    ASSERT_TRUE(sender.Resolve(*large, large->count));
    EXPECT_EQ(sender.stats().spent[0], std::uint64_t{1} + std::numeric_limits<std::uint32_t>::max());
    EXPECT_EQ(sender.available(static_cast<CreditClass>(9)), 0u);
    EXPECT_FALSE(sender.Reserve(CreditClass::kData, 0));
    EXPECT_TRUE(sender.Close());
}

class ReceiveCreditDomainTest : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(domain_.Open(Config()));
    }
    void TearDown() override {
        count_credit_allocations = false;
        fail_credit_allocation = -1;
        // 只清理本测试明确持有且已经消费的 receipt；旧代次拒绝可忽略，不重置总账。
        for (const auto receipt : receipts_) {
            if (receipt.id) {
                (void)domain_.Release(receipt);
                (void)domain_.RecordRefillAccepted(std::span(&receipt, 1));
            }
        }
        for (std::size_t i = 0; i < peer_count_; ++i) {
            auto result = domain_.RetirePeer(peers_[i], kProof);
            if (!result) {
                EXPECT_EQ(result.error().code, CreditErrorCode::kWrongIdentity);
            }
        }
        EXPECT_TRUE(domain_.Close());
    }
    static CreditDomainConfig Config(std::uint64_t domain = 10) {
        return {.receive_domain = domain, .capacity = 16, .control_slots = 4, .peer_capacity = 8, .base_credits = 2};
    }
    void PostAll() {
        ASSERT_TRUE(domain_.RecordInitialPosted(CreditClass::kData, 12));
        ASSERT_TRUE(domain_.RecordInitialPosted(CreditClass::kControl, 4));
    }
    std::expected<CreditPeerHandle, CreditError> Admit(std::uint64_t peer, std::uint64_t epoch = 1,
                                                       std::uint64_t domain = 10) {
        auto handle = domain_.Admit({peer, domain, epoch});
        if (handle) {
            peers_[peer_count_++] = *handle;
        }
        return handle;
    }
    std::expected<RxCreditReceipt, CreditError> Consume(CreditPeerHandle peer, CreditClass kind = CreditClass::kData) {
        auto receipt = domain_.Consume(peer, kind);
        if (receipt) {
            receipts_[receipt->slot] = *receipt;
        }
        return receipt;
    }
    void CheckConservation() {
        const auto snapshot = domain_.stats();
        EXPECT_EQ(snapshot.data.free + snapshot.data.granted + snapshot.data.unposted, 12u);
        EXPECT_EQ(snapshot.control.free + snapshot.control.granted + snapshot.control.unposted, 4u);
        EXPECT_EQ(snapshot.physical_posted + snapshot.data.unposted + snapshot.control.unposted + snapshot.unattributed,
                  16u);
        EXPECT_LE(snapshot.held + snapshot.released,
                  snapshot.data.unposted + snapshot.control.unposted + snapshot.unattributed);
        std::uint64_t base_free = 0, data_granted = 0, control_granted = 0;
        for (std::size_t i = 0; i < peer_count_; ++i) {
            const auto peer = domain_.PeerStats(peers_[i]);
            ASSERT_TRUE(peer);
            if (!peer->retired) {
                EXPECT_EQ(peer->base.free + peer->base.granted + peer->base.unposted, 2u);
            }
            base_free += peer->base.free;
            data_granted += peer->base.granted + peer->elastic_granted;
            control_granted += peer->control_granted;
        }
        EXPECT_EQ(snapshot.data.free, base_free + snapshot.elastic_free);
        EXPECT_EQ(snapshot.data.granted, data_granted);
        EXPECT_EQ(snapshot.control.granted, control_granted);
    }
    ReceiveCreditDomain domain_;
    std::array<CreditPeerHandle, 16> peers_{};
    std::array<RxCreditReceipt, 16> receipts_{};
    std::size_t peer_count_ = 0;
};

TEST_F(ReceiveCreditDomainTest, OnlyActualInitialPostMakesUnpostedCapacityGrantable) {
    EXPECT_EQ(domain_.stats().data.unposted, 12u);
    EXPECT_EQ(domain_.stats().physical_posted, 0u);
    EXPECT_FALSE(Admit(1));
    ASSERT_TRUE(domain_.RecordInitialPosted(CreditClass::kData, 1));
    EXPECT_FALSE(Admit(1));
    ASSERT_TRUE(domain_.RecordInitialPosted(CreditClass::kData, 1));
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    EXPECT_FALSE(domain_.Grant(*peer, CreditClass::kData, 3));
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 2));
    EXPECT_FALSE(domain_.RecordInitialPosted(CreditClass::kData, 11));
    EXPECT_FALSE(domain_.RecordInitialPosted(CreditClass::kControl, 5));
    EXPECT_FALSE(domain_.RecordInitialPosted(CreditClass::kControl, 0));
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, BaseAndElasticGrantsAreSharedAcrossPeerConnectionsButNeverOversubscribed) {
    PostAll();
    auto first = Admit(1);
    auto second = Admit(2);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(domain_.stats().elastic_free, 8u);
    ASSERT_TRUE(domain_.Grant(*first, CreditClass::kData, 10));
    EXPECT_FALSE(domain_.Grant(*first, CreditClass::kData, 1));
    ASSERT_TRUE(domain_.Grant(*second, CreditClass::kData, 2));
    EXPECT_EQ(domain_.stats().data.granted, 12u);
    EXPECT_EQ(domain_.stats().data.free, 0u);
    EXPECT_FALSE(domain_.Grant(*second, CreditClass::kData, 1));
    EXPECT_FALSE(Admit(3));
    // 新逻辑 socket 不再次 Admit Peer；每份关系只有一个基础窗口，不能重复承诺。
    EXPECT_FALSE(Admit(1));
    EXPECT_FALSE(Admit(1, 2));
    auto stats = domain_.PeerStats(*first);
    ASSERT_TRUE(stats);
    EXPECT_EQ(stats->base.granted, 2u);
    EXPECT_EQ(stats->elastic_granted, 8u);
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, HeldAndRejectedRefillCannotRestoreCreditButAcceptedPrefixCan) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 3));
    std::array<RxCreditReceipt, 3> receipts;
    for (auto& receipt : receipts) {
        auto consumed = Consume(*peer);
        ASSERT_TRUE(consumed);
        receipt = *consumed;
    }
    EXPECT_EQ(domain_.stats().data.unposted, 3u);
    EXPECT_EQ(domain_.stats().held, 3u);
    EXPECT_FALSE(domain_.RecordRefillAccepted(receipts));
    for (const auto receipt : receipts) {
        ASSERT_TRUE(domain_.Release(receipt));
    }
    EXPECT_EQ(domain_.stats().held, 0u);
    EXPECT_EQ(domain_.stats().released, 3u);
    // 只传已被 provider 接受的两条前缀，未接受后缀保持 Released/U，不能提前授出。
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(receipts).first(2)));
    EXPECT_EQ(domain_.stats().data.unposted, 1u);
    EXPECT_EQ(domain_.stats().released, 1u);
    auto peer_stats = domain_.PeerStats(*peer);
    ASSERT_TRUE(peer_stats);
    EXPECT_EQ(peer_stats->base.free, 2u);
    EXPECT_EQ(peer_stats->pending_rx, 1u);
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(receipts).last(1)));
    EXPECT_EQ(domain_.stats().physical_posted, 16u);
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, DuplicateRefillBatchAndReceiptCannotDoubleRestorePhysicalCapacity) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 2));
    auto old = Consume(*peer);
    ASSERT_TRUE(old);
    ASSERT_TRUE(domain_.Release(*old));
    EXPECT_FALSE(domain_.Release(*old));
    const std::array<RxCreditReceipt, 2> duplicates{*old, *old};
    EXPECT_FALSE(domain_.RecordRefillAccepted(duplicates));
    EXPECT_EQ(domain_.stats().released, 1u);
    EXPECT_EQ(domain_.stats().data.unposted, 1u);
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(&*old, 1)));
    EXPECT_FALSE(domain_.RecordRefillAccepted(std::span(&*old, 1)));
    auto current = Consume(*peer);
    ASSERT_TRUE(current);
    EXPECT_EQ(current->slot, old->slot);
    EXPECT_NE(current->id, old->id);
    EXPECT_FALSE(domain_.Release(*old));
    EXPECT_EQ(domain_.stats().held, 1u);
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, FrozenPeerConsumesLateOldSessionFramesWithoutNewGrants) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 3));
    ASSERT_TRUE(domain_.FreezePeer(*peer));
    EXPECT_FALSE(domain_.Grant(*peer, CreditClass::kData, 1));
    auto late = Consume(*peer);
    ASSERT_TRUE(late);
    EXPECT_EQ(domain_.stats().data.granted, 2u);
    EXPECT_EQ(domain_.stats().data.unposted, 1u);
    EXPECT_FALSE(domain_.RetirePeer(*peer, kProof));
    ASSERT_TRUE(domain_.Release(*late));
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(&*late, 1)));
    ASSERT_TRUE(domain_.RetirePeer(*peer, kProof));
    EXPECT_EQ(domain_.stats().elastic_free, 12u);
    EXPECT_EQ(domain_.stats().tombstones, 1u);
    EXPECT_FALSE(Consume(*peer));
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, EveryRetireProofIsRequiredAndIdentityCannotRegressOrRepeat) {
    PostAll();
    auto old = Admit(1, 5);
    ASSERT_TRUE(old);
    ASSERT_TRUE(domain_.Grant(*old, CreditClass::kData, 4));
    ASSERT_TRUE(domain_.Grant(*old, CreditClass::kControl, 2));
    for (auto proof : {CreditRetireProof{false, true, true}, CreditRetireProof{true, false, true},
                       CreditRetireProof{true, true, false}}) {
        auto retired = domain_.RetirePeer(*old, proof);
        ASSERT_FALSE(retired);
        EXPECT_EQ(retired.error().code, CreditErrorCode::kProofRequired);
        EXPECT_EQ(domain_.stats().data.granted, 4u);
        EXPECT_EQ(domain_.stats().control.granted, 2u);
    }
    ASSERT_TRUE(domain_.RetirePeer(*old, kProof));
    ASSERT_TRUE(domain_.RetirePeer(*old, kProof));
    EXPECT_FALSE(Admit(1, 4));
    EXPECT_FALSE(Admit(1, 5));
    auto current = Admit(1, 6);
    ASSERT_TRUE(current);
    EXPECT_NE(current->slot, old->slot);
    EXPECT_FALSE(domain_.Grant(*old, CreditClass::kData, 1));
    ASSERT_TRUE(domain_.Grant(*current, CreditClass::kData, 1));
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, DataFullStillLeavesAuthorizedControlProgressWithoutUnlimitedPeerSends) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 12));
    std::array<RxCreditReceipt, 12> held;
    for (auto& receipt : held) {
        auto consumed = Consume(*peer);
        ASSERT_TRUE(consumed);
        receipt = *consumed;
    }
    EXPECT_EQ(domain_.stats().data.free, 0u);
    EXPECT_FALSE(domain_.Grant(*peer, CreditClass::kData, 1));
    EXPECT_FALSE(Consume(*peer, CreditClass::kControl));
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kControl, 4));
    EXPECT_FALSE(domain_.Grant(*peer, CreditClass::kControl, 1));
    auto update = Consume(*peer, CreditClass::kControl);
    ASSERT_TRUE(update);
    ASSERT_TRUE(domain_.Release(*update));
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(&*update, 1)));
    auto renewed = domain_.Grant(*peer, CreditClass::kControl, 1);
    ASSERT_TRUE(renewed);
    EXPECT_EQ(*renewed, 5u);
    EXPECT_EQ(domain_.stats().data.unposted, 12u);
    EXPECT_EQ(domain_.stats().held, 12u);
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, TombstonesRemainBoundedAndConsumePeerTableCapacity) {
    PostAll();
    for (std::uint64_t epoch = 1; epoch <= 8; ++epoch) {
        auto peer = Admit(1, epoch);
        ASSERT_TRUE(peer);
        ASSERT_TRUE(domain_.RetirePeer(*peer, kProof));
    }
    auto full = Admit(1, 9);
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, CreditErrorCode::kExhausted);
    EXPECT_EQ(domain_.stats().tombstones, 8u);
    EXPECT_EQ(domain_.stats().elastic_free, 12u);
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, ClosingStopsNewAuthorizationButLeavesReceiptsProgressable) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 1));
    auto receipt = Consume(*peer);
    ASSERT_TRUE(receipt);
    auto close = domain_.Close();
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error().code, CreditErrorCode::kInUse);
    EXPECT_FALSE(domain_.Grant(*peer, CreditClass::kData, 1));
    EXPECT_FALSE(Admit(2));
    ASSERT_TRUE(domain_.Release(*receipt));
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(&*receipt, 1)));
    ASSERT_TRUE(domain_.RetirePeer(*peer, kProof));
    EXPECT_TRUE(domain_.Close());
    // fixture 再次 Close 幂等；已关闭句柄不会错误触及新域。
    peer_count_ = 0;
}

TEST_F(ReceiveCreditDomainTest, FreezeDoesNotInventRxAndStillRecordsTrustedRefillPrefix) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kData, 1));
    auto receipt = Consume(*peer);
    ASSERT_TRUE(receipt);
    ASSERT_TRUE(domain_.Release(*receipt));
    const auto before = domain_.stats();
    // raw post 接受范围未知时只冻结准入；没有新 CQE，绝不能假调用 ConsumeUnknown。
    domain_.Freeze();
    EXPECT_TRUE(domain_.stats().frozen);
    EXPECT_EQ(domain_.stats().unattributed, 0u);
    EXPECT_EQ(domain_.stats().physical_posted, before.physical_posted);
    EXPECT_EQ(domain_.stats().data.unposted, before.data.unposted);
    EXPECT_FALSE(domain_.Grant(*peer, CreditClass::kControl, 1));
    EXPECT_FALSE(Admit(2));
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(&*receipt, 1)));
    EXPECT_EQ(domain_.stats().physical_posted, 16u);
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, NewDomainGenerationRejectsOldPeerAndReceiptDespiteReusedSlots) {
    PostAll();
    auto old_peer = Admit(1);
    ASSERT_TRUE(old_peer);
    ASSERT_TRUE(domain_.Grant(*old_peer, CreditClass::kData, 1));
    auto old_receipt = Consume(*old_peer);
    ASSERT_TRUE(old_receipt);
    ASSERT_TRUE(domain_.Release(*old_receipt));
    ASSERT_TRUE(domain_.RecordRefillAccepted(std::span(&*old_receipt, 1)));
    ASSERT_TRUE(domain_.RetirePeer(*old_peer, kProof));
    ASSERT_TRUE(domain_.Close());
    EXPECT_FALSE(domain_.Open(Config()));
    ASSERT_TRUE(domain_.Open(Config(11)));
    PostAll();
    auto current = Admit(1, 1, 11);
    ASSERT_TRUE(current);
    EXPECT_EQ(current->slot, old_peer->slot);
    EXPECT_NE(current->id, old_peer->id);
    ASSERT_TRUE(domain_.Grant(*current, CreditClass::kData, 1));
    auto receipt = Consume(*current);
    ASSERT_TRUE(receipt);
    EXPECT_EQ(receipt->slot, old_receipt->slot);
    EXPECT_NE(receipt->id, old_receipt->id);
    EXPECT_FALSE(domain_.Grant(*old_peer, CreditClass::kData, 1));
    EXPECT_FALSE(domain_.Release(*old_receipt));
    EXPECT_EQ(domain_.stats().held, 1u);
}

TEST(ReceiveCreditDomainDeathTest, UnknownReceiveConsumesPhysicalCapacityAndRetainsUnattributedPromises) {
    // 未归属 CQE 不能猜中某个 Peer 的 G；该模式保留账本直到进程退出，使用独立子进程验证。
    EXPECT_EXIT(([] {
                    ReceiveCreditDomain domain;
                    CreditDomainConfig config{
                        .receive_domain = 1, .capacity = 8, .control_slots = 2, .peer_capacity = 2, .base_credits = 2};
                    if (!domain.Open(config) || !domain.RecordInitialPosted(CreditClass::kData, 6) ||
                        !domain.RecordInitialPosted(CreditClass::kControl, 2)) {
                        _exit(1);
                    }
                    auto peer = domain.Admit({1, 1, 1});
                    if (!peer || !domain.Grant(*peer, CreditClass::kData, 4)) {
                        _exit(2);
                    }
                    // 合法 Peer 未获 control 授权的真实 RX，失败归属后仍须扣未知物理消耗。
                    auto unauthorized = domain.Consume(*peer, CreditClass::kControl);
                    if (unauthorized || unauthorized.error().code != CreditErrorCode::kExhausted ||
                        domain.stats().physical_posted != 8) {
                        _exit(7);
                    }
                    auto unknown = domain.ConsumeUnknown();
                    if (!unknown || domain.stats().physical_posted != 7 || domain.stats().data.granted != 4 ||
                        domain.stats().unattributed != 1 || !domain.stats().frozen) {
                        _exit(3);
                    }
                    if (domain.Grant(*peer, CreditClass::kData, 1) || !domain.Release(*unknown) ||
                        domain.RecordRefillAccepted(std::span(&*unknown, 1))) {
                        _exit(4);
                    }
                    auto close = domain.Close();
                    auto retire = domain.RetirePeer(*peer, kProof);
                    if (close || close.error().code != CreditErrorCode::kRetained || retire ||
                        retire.error().code != CreditErrorCode::kRetained) {
                        _exit(5);
                    }
                    auto known = domain.Consume(*peer, CreditClass::kData);
                    if (!known || domain.stats().physical_posted != 6 || domain.stats().data.granted != 3) {
                        _exit(6);
                    }
                    _exit(0);
                }()),
                ::testing::ExitedWithCode(0), "");
}

TEST(CreditConfigurationTest, InvalidConfigurationAndAllocationFailureLeaveNoPublishedState) {
    ReceiveCreditDomain domain;
    CreditDomainConfig config{
        .receive_domain = 1, .capacity = 8, .control_slots = 2, .peer_capacity = 2, .base_credits = 2};
    auto invalid = config;
    invalid.control_slots = invalid.capacity;
    EXPECT_FALSE(domain.Open(invalid));
    invalid = config;
    invalid.peer_capacity = ReceiveCreditDomain::kMaxPeers + 1;
    EXPECT_FALSE(domain.Open(invalid));
    invalid = config;
    invalid.base_credits = 7;
    EXPECT_FALSE(domain.Open(invalid));
    // 控制路径的 State/Peer/Receipt 三次分配任一失败都不能消耗域 incarnation 或发布半成品。
    for (int failure = 0; failure < 3; ++failure) {
        fail_credit_allocation = failure;
        auto opened = domain.Open(config);
        fail_credit_allocation = -1;
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, CreditErrorCode::kNoMemory);
        EXPECT_EQ(domain.stats().physical_posted, 0u);
    }
    ASSERT_TRUE(domain.Open(config));
    EXPECT_TRUE(domain.Close());
}

TEST(CreditDuplexTest, FullDataWindowsExchangeControlUpdatesAndResumeBothDirections) {
    std::array<ReceiveCreditDomain, 2> domains;
    std::array<SenderCreditAccount, 2> senders;
    std::array<CreditPeerHandle, 2> peers;
    const std::array<CreditIdentity, 2> identities{CreditIdentity{2, 20, 1}, CreditIdentity{1, 21, 1}};
    std::array<std::array<RxCreditReceipt, 3>, 2> held;
    for (std::size_t i = 0; i < 2; ++i) {
        CreditDomainConfig config{.receive_domain = identities[i].receive_domain,
                                  .capacity = 4,
                                  .control_slots = 1,
                                  .peer_capacity = 1,
                                  .base_credits = 1};
        ASSERT_TRUE(domains[i].Open(config));
        ASSERT_TRUE(domains[i].RecordInitialPosted(CreditClass::kData, 3));
        ASSERT_TRUE(domains[i].RecordInitialPosted(CreditClass::kControl, 1));
        auto peer = domains[i].Admit(identities[i]);
        ASSERT_TRUE(peer);
        peers[i] = *peer;
        ASSERT_TRUE(domains[i].Grant(peers[i], CreditClass::kData, 3));
        ASSERT_TRUE(domains[i].Grant(peers[i], CreditClass::kControl, 1));
        ASSERT_TRUE(senders[i].Open(identities[i]));
        ASSERT_TRUE(senders[i].Update(identities[i], 3, 1));
        auto batch = senders[i].Reserve(CreditClass::kData, 3);
        ASSERT_TRUE(batch);
        ASSERT_TRUE(senders[i].Resolve(*batch, 3));
        for (auto& receipt : held[i]) {
            auto consumed = domains[i].Consume(peers[i], CreditClass::kData);
            ASSERT_TRUE(consumed);
            receipt = *consumed;
        }
    }
    // 此时两端数据都驻留于应用，双方 data 窗口为零；控制槽没有被数据/RTS 借用。
    for (std::size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(senders[i].available(CreditClass::kData), 0u);
        EXPECT_EQ(domains[i].stats().data.unposted, 3u);
        EXPECT_EQ(senders[i].available(CreditClass::kControl), 1u);
    }
    std::array<std::uint64_t, 2> limits;
    for (std::size_t i = 0; i < 2; ++i) {
        ASSERT_TRUE(domains[i].Release(held[i][0]));
        ASSERT_TRUE(domains[i].RecordRefillAccepted(std::span(held[i]).first(1)));
        auto granted = domains[i].Grant(peers[i], CreditClass::kData, 1);
        ASSERT_TRUE(granted);
        limits[i] = *granted;
    }
    for (std::size_t i = 0; i < 2; ++i) {
        // 域 i 的更新经反向 UB control 额度传回，不依赖反向 data 空间。
        const auto reverse = 1 - i;
        auto control = senders[reverse].Reserve(CreditClass::kControl, 1);
        ASSERT_TRUE(control);
        ASSERT_TRUE(senders[reverse].Resolve(*control, 1));
        auto notification = domains[reverse].Consume(peers[reverse], CreditClass::kControl);
        ASSERT_TRUE(notification);
        ASSERT_TRUE(domains[reverse].Release(*notification));
        ASSERT_TRUE(domains[reverse].RecordRefillAccepted(std::span(&*notification, 1)));
        ASSERT_TRUE(senders[i].Update(identities[i], limits[i], 1));
    }
    for (std::size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(senders[i].available(CreditClass::kData), 1u);
        auto resumed = senders[i].Reserve(CreditClass::kData, 1);
        ASSERT_TRUE(resumed);
        ASSERT_TRUE(senders[i].Resolve(*resumed, 1));
        auto received = domains[i].Consume(peers[i], CreditClass::kData);
        ASSERT_TRUE(received);
        held[i][0] = *received;
        for (const auto receipt : held[i]) {
            ASSERT_TRUE(domains[i].Release(receipt));
        }
        ASSERT_TRUE(domains[i].RecordRefillAccepted(held[i]));
        EXPECT_EQ(domains[i].stats().physical_posted, 4u);
        ASSERT_TRUE(domains[i].RetirePeer(peers[i], kProof));
        EXPECT_TRUE(senders[i].Close());
        EXPECT_TRUE(domains[i].Close());
    }
}

TEST_F(ReceiveCreditDomainTest, RepeatedCyclesKeepConservationWithNoDataPathAllocation) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open(kIdentity));
    bool succeeded = true;
    credit_allocations = 0;
    count_credit_allocations = true;
    for (std::uint64_t cycle = 1; cycle <= 1000; ++cycle) {
        const auto granted = domain_.Grant(*peer, CreditClass::kData, 3);
        if (!granted) {
            succeeded = false;
            break;
        }
        succeeded &= *granted == cycle * 3;
        succeeded &= sender.Update(kIdentity, *granted, 0).has_value();
        const auto batch = sender.Reserve(CreditClass::kData, 3);
        if (!batch || !sender.Resolve(*batch, 3)) {
            succeeded = false;
            break;
        }
        std::array<RxCreditReceipt, 3> receipts;
        for (auto& receipt : receipts) {
            const auto consumed = domain_.Consume(*peer, CreditClass::kData);
            if (!consumed) {
                succeeded = false;
                break;
            }
            receipt = *consumed;
            succeeded &= domain_.Release(receipt).has_value();
        }
        if (!succeeded) {
            break;
        }
        succeeded &= domain_.RecordRefillAccepted(receipts).has_value();
        const auto snapshot = domain_.stats();
        succeeded &= snapshot.data.free == 12 && snapshot.data.granted == 0 && snapshot.data.unposted == 0;
        succeeded &= snapshot.physical_posted == 16 && snapshot.held == 0 && snapshot.released == 0;
    }
    count_credit_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(credit_allocations, 0u);
    EXPECT_EQ(sender.stats().spent[0], 3000u);
    EXPECT_TRUE(sender.Close());
    CheckConservation();
}

TEST_F(ReceiveCreditDomainTest, PublishedControlIsSharedMonotonicAndCannotExceedActualGrant) {
    PostAll();
    auto peer = Admit(1);
    ASSERT_TRUE(peer);
    EXPECT_EQ(domain_.PublishedControl(*peer), 0u);
    EXPECT_FALSE(domain_.RecordPublishedControl(*peer, 1));
    ASSERT_TRUE(domain_.Grant(*peer, CreditClass::kControl, 4));
    ASSERT_TRUE(domain_.RecordPublishedControl(*peer, 4));
    // 同一 peer 的另一个逻辑连接看到共同发布上界，重复/迟到通知不产生新的控制预算。
    const auto other_connection = *peer;
    EXPECT_EQ(domain_.PublishedControl(other_connection), 4u);
    ASSERT_TRUE(domain_.RecordPublishedControl(other_connection, 2));
    EXPECT_EQ(domain_.PublishedControl(*peer), 4u);
    EXPECT_FALSE(domain_.RecordPublishedControl(*peer, 5));
    auto forged = *peer;
    ++forged.id;
    EXPECT_FALSE(domain_.PublishedControl(forged));
    EXPECT_FALSE(domain_.RecordPublishedControl(forged, 4));
}

} // namespace
} // namespace connection
} // namespace kbsocket
