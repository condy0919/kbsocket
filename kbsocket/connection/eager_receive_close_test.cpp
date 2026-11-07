// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/control/peer_credit_control.hpp"
#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace connection {
namespace {

class EagerReceiveCloseTest : public ::testing::Test {
protected:
    struct Posted {
        std::uint64_t id;
        std::span<std::byte> payload;
    };
    static inline EagerReceiveCloseTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.create_jfc = [](auto*, auto*) {
            return active_->jfc_created_++ % 2 == 0 ? &active_->tx_ : &active_->rx_;
        };
        functions.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        functions.create_jetty = [](auto*, auto*) {
            return active_->jetty_created_++ % 2 == 0 ? &active_->passive_ : &active_->jetty_;
        };
        functions.delete_jfc = [](auto*) {
            ++active_->jfc_deleted_;
            return URMA_SUCCESS;
        };
        functions.delete_jetty = [](auto*) {
            ++active_->jetty_deleted_;
            return URMA_SUCCESS;
        };
        functions.delete_jfr = [](auto*) {
            ++active_->jfr_delete_calls_;
            if (active_->delete_jfr_status_ == URMA_SUCCESS) {
                active_->posted_.clear();
            }
            return active_->delete_jfr_status_;
        };
        functions.register_seg = [](auto*, auto*) { return &active_->segment_; };
        functions.unregister_seg = [](auto*) {
            ++active_->unregister_calls_;
            return active_->unregister_status_;
        };
        functions.post_jfr_wr = [](auto*, auto* first, auto** bad) {
            auto& self = *active_;
            ++self.post_calls_;
            std::size_t offered = 0;
            *bad = nullptr;
            for (auto* wr = first; wr; wr = wr->next, ++offered) {
                if (self.accepted_prefix_ && offered >= *self.accepted_prefix_) {
                    if (!*bad) {
                        *bad = wr;
                    }
                    continue;
                }
                const auto& sge = wr->src.sge[0];
                self.posted_.push_back({wr->user_ctx, {reinterpret_cast<std::byte*>(sge.addr), sge.len}});
            }
            if (self.unknown_acceptance_) {
                *bad = nullptr;
            }
            return self.post_status_;
        };
        functions.poll_jfc = [](auto*, int, auto* out) {
            auto& self = *active_;
            if (!self.completion_) {
                return 0;
            }
            *out = *self.completion_;
            self.completion_.reset();
            ++self.rx_completions_;
            return 1;
        };
        return functions;
    }

    static raw::ReceiveDomainQuiescence Quiesced() {
        return {
            .peer_stopped = true, .events_acked = true, .queue_users_stopped = true, .completion_queues_healthy = true};
    }
    void OpenBuffers(raw::RxBufferPool& buffers) {
        urma_device_cap_t capabilities{};
        capabilities.trans_mode = URMA_TM_RM;
        capabilities.rm_tp_cap.bs.ctp = 1;
        capabilities.max_jetty = 2;
        capabilities.max_jfs_depth = capabilities.max_jfr_depth = capabilities.max_jfc_depth = 1024;
        capabilities.max_jfs_sge = capabilities.max_jfs_rsge = capabilities.max_jfr_sge = 1;
        raw::JettyPoolConfig config;
        config.tx_depth = 8;
        config.rx_depth = config.rx_cq_depth = 8;
        ASSERT_TRUE(pool_.Open(&context_, capabilities, config));
        ASSERT_TRUE(buffers.Open(pool_, 12));
    }
    void SetUp() override {
        active_ = this;
        passive_.jetty_id.id = 19;
        jetty_.jetty_id.id = 20;
        jfr_.jfr_id.id = 23;
        OpenBuffers(buffers_);
        ASSERT_TRUE(buffers_.Refill(8));
        ASSERT_TRUE(credits_.Open({33, 8, 2, 2, 1}));
        ASSERT_TRUE(credits_.RecordInitialPosted(CreditClass::kData, 6));
        ASSERT_TRUE(credits_.RecordInitialPosted(CreditClass::kControl, 2));
        const auto peer = credits_.Admit({22, 33, 77});
        ASSERT_TRUE(peer);
        source_ = {{11, 33}, {kWireVersion, 22, 33, 44}, *peer};
        ASSERT_TRUE(credits_.Grant(*peer, CreditClass::kData, 6));
        ASSERT_TRUE(credits_.Grant(*peer, CreditClass::kControl, 2));
        ASSERT_TRUE(routes_.Open({11, 33}, 4));
        const auto route = routes_.Reserve();
        ASSERT_TRUE(route);
        route_ = *route;
        ASSERT_TRUE(routes_.Publish(route_, {source_.peer_context, route_.token, 55}, this));
        ASSERT_TRUE(domain_.Open(buffers_, credits_, routes_));
    }
    static std::expected<EagerRxDisposition, EagerRxError>
    Handler(void* cookie, EagerReceiveDomain&, const WireFrameView&, EagerRxPacket packet, std::uint64_t) noexcept {
        auto& self = *static_cast<EagerReceiveCloseTest*>(cookie);
        ++self.handler_calls_;
        if (self.hold_next_) {
            self.held_packet_ = packet;
            self.hold_next_ = false;
            return EagerRxDisposition::kHeld;
        }
        return EagerRxDisposition::kRelease;
    }
    void QueueCompletion(bool control = false) {
        ASSERT_FALSE(posted_.empty());
        const auto posted = posted_.front();
        posted_.pop_front();
        const std::array payload{std::byte{0x42}};
        WireFrameInput frame;
        frame.token = route_.token;
        frame.seq = next_sequence_++;
        if (control) {
            frame.type = WireFrameType::kControl;
            frame.control = WireControlType::kCreditUpdate;
            frame.seq = 0;
            frame.session = source_.peer_context.session;
            frame.credit_epoch = 55;
        } else {
            frame.data = payload;
        }
        const auto encoded = EncodeWireFrame(frame, posted.payload);
        ASSERT_TRUE(encoded);
        urma_cr_t completion{};
        completion.flag.bs.s_r = 1;
        completion.flag.bs.jetty = 1;
        completion.local_id = passive_.jetty_id.id;
        completion.user_ctx = posted.id;
        completion.completion_len = encoded->payload_bytes;
        completion.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        completion.imm_data = encoded->imm_data;
        completion_ = completion;
    }
    raw::RxBufferEvent PollOne() {
        raw::RxBufferEvent event;
        const auto count = buffers_.Poll(std::span(&event, 1));
        EXPECT_TRUE(count);
        if (count) {
            EXPECT_EQ(*count, 1u);
        }
        return event;
    }
    void Receive(bool control = false) {
        QueueCompletion(control);
        ASSERT_TRUE(domain_.Dispatch(PollOne(), source_, Handler, next_sequence_));
    }
    void Begin() {
        domain_.Freeze();
        ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    }
    std::expected<raw::ReceiveDomainCloseProgress, raw::ReceiveDomainCloseError>
    Advance(raw::ReceiveDomainQuiescence proof = Quiesced()) {
        return closer_.Advance(proof, events_);
    }
    void ClosePhysical() {
        if (closer_.stage() == raw::ReceiveDomainCloseStage::kIdle) {
            Begin();
        }
        const auto result = Advance();
        ASSERT_TRUE(result);
        EXPECT_EQ(result->stage, raw::ReceiveDomainCloseStage::kClosed);
    }
    void ExpectEmptyMetadata() {
        const auto stats = credits_.stats();
        EXPECT_EQ(stats.active_peers, 0u);
        EXPECT_EQ(stats.tombstones, 0u);
        EXPECT_EQ(stats.held, 0u);
        EXPECT_EQ(stats.released, 0u);
        EXPECT_EQ(stats.unattributed, 0u);
        EXPECT_EQ(stats.physical_posted, 0u);
        EXPECT_EQ(stats.data.free + stats.data.granted + stats.data.unposted, 0u);
        EXPECT_EQ(stats.control.free + stats.control.granted + stats.control.unposted, 0u);
        EXPECT_EQ(domain_.buffers(), nullptr);
        EXPECT_EQ(domain_.held(), 0u);
        EXPECT_EQ(domain_.pending_refill(), 0u);
        EXPECT_FALSE(credits_.Grant(source_.peer, CreditClass::kData, 1));
    }
    void TearDown() override {
        delete_jfr_status_ = unregister_status_ = URMA_SUCCESS;
        if (held_packet_ && domain_.held()) {
            EXPECT_TRUE(domain_.Release(*held_packet_));
        }
        if (pool_.ready() || closer_.stage() != raw::ReceiveDomainCloseStage::kClosed) {
            ClosePhysical();
        }
        EXPECT_TRUE(domain_.CloseDestroyed(closer_));
        EXPECT_TRUE(credits_.Close());
        EXPECT_TRUE(buffers_.Close());
        EXPECT_TRUE(pool_.Close());
        if (routes_.active()) {
            EXPECT_TRUE(routes_.Retire(route_));
        }
        EXPECT_TRUE(routes_.Close());
        active_ = nullptr;
    }

    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t passive_{}, jetty_{};
    urma_target_seg_t segment_{};
    raw::JettyPool pool_;
    raw::RxBufferPool buffers_;
    ReceiveCreditDomain credits_;
    ConnectionTable routes_;
    EagerReceiveDomain domain_;
    raw::ReceiveDomainCloser closer_;
    EagerReceiveSource source_;
    ConnectionRouteLease route_;
    std::deque<Posted> posted_;
    std::optional<urma_cr_t> completion_;
    std::optional<EagerRxPacket> held_packet_;
    std::optional<std::size_t> accepted_prefix_;
    std::array<raw::RxBufferEvent, 4> events_{};
    urma_status_t post_status_ = URMA_SUCCESS;
    urma_status_t delete_jfr_status_ = URMA_SUCCESS;
    urma_status_t unregister_status_ = URMA_SUCCESS;
    bool hold_next_ = false, unknown_acceptance_ = false;
    std::size_t post_calls_ = 0, handler_calls_ = 0, rx_completions_ = 0;
    std::size_t jfc_created_ = 0, jetty_created_ = 0, jfc_deleted_ = 0, jetty_deleted_ = 0;
    std::size_t jfr_delete_calls_ = 0, unregister_calls_ = 0;
    std::uint32_t next_sequence_ = 0;
};

TEST_F(EagerReceiveCloseTest, FrozenReleasedReceiptNeedsRealQueueClosureAndNeverRefillsOnRetirement) {
    Receive();
    domain_.Freeze();
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(credits_.stats().released, 1u);
    EXPECT_FALSE(domain_.Refill());
    EXPECT_FALSE(domain_.Close());
    EXPECT_FALSE(credits_.RetirePeer(source_.peer, {true, true, true}));
    const auto posts = post_calls_;
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(credits_.stats().released, 1u);
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(post_calls_, posts);
    EXPECT_EQ(rx_completions_, 1u);
    EXPECT_EQ(buffers_.retired_on_delete(), 7u);
    // 幂等关闭不能重复记接收、再次触发 provider 删除或重新授信。
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 1u);
}

TEST_F(EagerReceiveCloseTest, ReleasedUnattributedReceiptRetiresOnlyWithTheWholeDestroyedDomain) {
    QueueCompletion();
    auto event = PollOne();
    event.completion.imm_data = 0xf000000000000000ULL;
    const auto dispatched = domain_.Dispatch(event, source_, Handler, 1);
    ASSERT_FALSE(dispatched);
    EXPECT_EQ(dispatched.error().code, EagerRxErrorCode::kMalformedFrame);
    EXPECT_EQ(credits_.stats().unattributed, 1u);
    EXPECT_EQ(credits_.stats().data.granted, 6u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_FALSE(credits_.Close());
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(post_calls_, 1u);
}

TEST_F(EagerReceiveCloseTest, NoReceivedPacketsCanRetireUnusedGrantsAfterAllResidualRxAreDeleted) {
    EXPECT_EQ(credits_.stats().data.granted, 6u);
    EXPECT_EQ(credits_.stats().control.granted, 2u);
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(buffers_.retired_on_delete(), 8u);
    EXPECT_EQ(rx_completions_, 0u);
    EXPECT_EQ(post_calls_, 1u);
}

TEST_F(EagerReceiveCloseTest, ZeroResidualRxStillRequiresActualDeleteEvidence) {
    for (std::size_t i = 0; i < 8; ++i) {
        Receive(i >= 6);
    }
    EXPECT_EQ(buffers_.posted(), 0u);
    EXPECT_EQ(domain_.pending_refill(), 8u);
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    ClosePhysical();
    EXPECT_EQ(buffers_.retired_on_delete(), 0u);
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(rx_completions_, 8u);
    EXPECT_EQ(post_calls_, 1u);
}

TEST_F(EagerReceiveCloseTest, EveryQuiescenceGatePreventsMetadataRetirement) {
    Receive();
    Begin();
    for (const auto proof :
         {raw::ReceiveDomainQuiescence{false, true, true, true}, raw::ReceiveDomainQuiescence{true, false, true, true},
          raw::ReceiveDomainQuiescence{true, true, false, true},
          raw::ReceiveDomainQuiescence{true, true, true, false}}) {
        const auto result = Advance(proof);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->stage, raw::ReceiveDomainCloseStage::kDraining);
        EXPECT_FALSE(domain_.CloseDestroyed(closer_));
        EXPECT_EQ(domain_.pending_refill(), 1u);
        EXPECT_EQ(credits_.stats().released, 1u);
    }
    EXPECT_EQ(jfr_delete_calls_, 0u);
}

TEST_F(EagerReceiveCloseTest, FailedJfrDeletePreservesReceiptsUntilSuccessfulRetry) {
    Receive();
    Begin();
    delete_jfr_status_ = URMA_EAGAIN;
    ASSERT_FALSE(Advance());
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(credits_.stats().released, 1u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(unregister_calls_, 0u);
    delete_jfr_status_ = URMA_SUCCESS;
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(jfr_delete_calls_, 2u);
    EXPECT_EQ(jetty_deleted_, 2u);
}

TEST_F(EagerReceiveCloseTest, DeletedJfrWithUnregisterFailureIsNotYetCompleteEvidence) {
    Receive();
    Begin();
    unregister_status_ = URMA_EAGAIN;
    ASSERT_FALSE(Advance());
    EXPECT_EQ(closer_.stage(), raw::ReceiveDomainCloseStage::kWaitingForLeases);
    EXPECT_EQ(buffers_.posted(), 0u);
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(credits_.stats().released, 1u);
    unregister_status_ = URMA_SUCCESS;
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 2u);
}

TEST_F(EagerReceiveCloseTest, HeldPacketCanBeReleasedAfterJfrDeletionBeforeMetadataCloses) {
    hold_next_ = true;
    Receive();
    ASSERT_TRUE(held_packet_);
    Begin();
    const auto progress = Advance();
    ASSERT_TRUE(progress);
    EXPECT_EQ(progress->stage, raw::ReceiveDomainCloseStage::kWaitingForLeases);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    EXPECT_TRUE(domain_.View(*held_packet_));
    EXPECT_EQ(unregister_calls_, 0u);
    // 成功 DeleteJfr 只终结 Posted，不得抹去上层仍持有的 CPU/receipt 责任。
    ASSERT_TRUE(domain_.Release(*held_packet_));
    EXPECT_FALSE(domain_.Release(*held_packet_));
    EXPECT_EQ(credits_.stats().held, 0u);
    EXPECT_EQ(credits_.stats().released, 1u);
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    EXPECT_FALSE(domain_.View(*held_packet_));
}

TEST_F(EagerReceiveCloseTest, TrustedPartialRefillLeavesOnlyUnacceptedReceiptsForDestroyedClose) {
    Receive();
    Receive();
    Receive();
    accepted_prefix_ = 1;
    post_status_ = URMA_EAGAIN;
    const auto refilled = domain_.Refill();
    ASSERT_FALSE(refilled);
    EXPECT_EQ(refilled.error().accepted, 1u);
    EXPECT_TRUE(refilled.error().accepted_known);
    EXPECT_EQ(domain_.pending_refill(), 2u);
    EXPECT_EQ(credits_.stats().released, 2u);
    EXPECT_EQ(credits_.stats().physical_posted, 6u);
    const auto posts = post_calls_;
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(post_calls_, posts);
    EXPECT_EQ(rx_completions_, 3u);
    EXPECT_EQ(buffers_.retired_on_delete(), 6u);
}

TEST_F(EagerReceiveCloseTest, UnknownRefillAcceptanceIsSettledByRealDeleteWithoutInventingPostedCredits) {
    Receive();
    post_status_ = URMA_ENOMEM;
    unknown_acceptance_ = true;
    const auto refilled = domain_.Refill();
    ASSERT_FALSE(refilled);
    EXPECT_FALSE(refilled.error().accepted_known);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(credits_.stats().physical_posted, 7u);
    EXPECT_EQ(buffers_.posted(), 8u);
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    const auto posts = post_calls_;
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(post_calls_, posts);
    EXPECT_EQ(buffers_.retired_on_delete(), 8u);
    EXPECT_EQ(rx_completions_, 1u);
}

TEST_F(EagerReceiveCloseTest, ClosePollEventsMustBeDispatchedBeforeDeletionAndRetirement) {
    QueueCompletion();
    Begin();
    const auto polled = Advance();
    ASSERT_TRUE(polled);
    ASSERT_EQ(polled->rx_events, 1u);
    EXPECT_EQ(polled->stage, raw::ReceiveDomainCloseStage::kDraining);
    EXPECT_EQ(jfr_delete_calls_, 0u);
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    // closer 交出的真实 CQE 必须仍通过同一 Eager 域结账，不可只归还 raw Lease 丢掉 receipt。
    ASSERT_TRUE(domain_.Dispatch(events_[0], source_, Handler, 1));
    EXPECT_EQ(domain_.pending_refill(), 1u);
    ClosePhysical();
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_EQ(rx_completions_, 1u);
}

TEST_F(EagerReceiveCloseTest, CompletedCloserForDifferentBuffersCannotCloseThisDomainsMetadata) {
    // 只停止并关闭 raw 层，保留无 receipt 的 Eager 独占绑定；不先 Freeze，确保下方
    // 另一个物理池的 Open 确实由绑定门禁拒绝，而不是被 frozen 前置条件挡住。
    ASSERT_TRUE(closer_.Begin(pool_, buffers_));
    const auto old_progress = Advance();
    ASSERT_TRUE(old_progress);
    ASSERT_EQ(old_progress->stage, raw::ReceiveDomainCloseStage::kClosed);
    raw::RxBufferPool other_buffers;
    raw::ReceiveDomainCloser other_closer;
    OpenBuffers(other_buffers);
    ASSERT_TRUE(other_buffers.Refill(8));
    EagerReceiveDomain other_domain;
    // 即使另一个物理池有同样的 Posted 数，也不能共享仍绑定旧域的信用账。
    const auto shared_credit = other_domain.Open(other_buffers, credits_, routes_);
    ASSERT_FALSE(shared_credit);
    EXPECT_EQ(shared_credit.error().code, EagerRxErrorCode::kInUse);
    ASSERT_TRUE(other_closer.Begin(pool_, other_buffers));
    std::array<raw::RxBufferEvent, 1> events;
    const auto progress = other_closer.Advance(Quiesced(), events);
    ASSERT_TRUE(progress);
    ASSERT_EQ(progress->stage, raw::ReceiveDomainCloseStage::kClosed);
    EXPECT_FALSE(domain_.CloseDestroyed(other_closer));
    EXPECT_EQ(credits_.stats().active_peers, 1u);
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
}

TEST_F(EagerReceiveCloseTest, ReopenedBuffersInvalidateBothOldAndNewProofForOldEagerBinding) {
    const auto old_incarnation = buffers_.incarnation();
    ClosePhysical();
    OpenBuffers(buffers_);
    EXPECT_GT(buffers_.incarnation(), old_incarnation);
    raw::ReceiveDomainCloser next_closer;
    ASSERT_TRUE(next_closer.Begin(pool_, buffers_));
    std::array<raw::RxBufferEvent, 1> events;
    const auto progress = next_closer.Advance(Quiesced(), events);
    ASSERT_TRUE(progress);
    ASSERT_EQ(progress->stage, raw::ReceiveDomainCloseStage::kClosed);
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    EXPECT_FALSE(domain_.CloseDestroyed(next_closer));
    EXPECT_EQ(credits_.stats().active_peers, 1u);
    // 本例没有 receipt；正常关闭元数据仍须分别退休已确认静默的旧 Peer，不能借新代证明清账。
    ASSERT_TRUE(domain_.Close());
    ASSERT_TRUE(credits_.RetirePeer(source_.peer, {true, true, true}));
    ASSERT_TRUE(credits_.Close());
}

TEST_F(EagerReceiveCloseTest, BoundCreditDomainCannotCloseOrReopenAheadOfItsEagerOwner) {
    ClosePhysical();
    ASSERT_TRUE(credits_.RetirePeer(source_.peer, {true, true, true}));
    const auto closed = credits_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, CreditErrorCode::kInUse);
    EXPECT_FALSE(credits_.Open({34, 8, 2, 2, 1}));
    EXPECT_EQ(credits_.receive_domain(), 33u);
    // 正常 owner 关闭才解除绑定；其后信用账可按更大 incarnation 重新建立。
    ASSERT_TRUE(domain_.Close());
    ASSERT_TRUE(credits_.Close());
    ASSERT_TRUE(credits_.Open({34, 8, 2, 2, 1}));
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    EXPECT_EQ(credits_.receive_domain(), 34u);
    EXPECT_EQ(credits_.stats().data.unposted, 6u);
    ASSERT_TRUE(credits_.Close());
}

TEST_F(EagerReceiveCloseTest, CreditBindingIsExclusiveUntilNormalEagerCloseReleasesIt) {
    EagerReceiveDomain other;
    const auto duplicate = other.Open(buffers_, credits_, routes_);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, EagerRxErrorCode::kInUse);
    EXPECT_EQ(other.buffers(), nullptr);
    // 未发生 RX 时正常 Close 只解除本层独占绑定；再次绑定不能创造新的信用或 Posted。
    ASSERT_TRUE(domain_.Close());
    ASSERT_TRUE(other.Open(buffers_, credits_, routes_));
    EXPECT_EQ(credits_.stats().physical_posted, 8u);
    EXPECT_EQ(credits_.stats().data.granted, 6u);
    ASSERT_TRUE(other.Close());
    ASSERT_TRUE(domain_.Open(buffers_, credits_, routes_));
}

TEST_F(EagerReceiveCloseTest, CreditAndRouteIncarnationsMustAgreeBeforeBinding) {
    ConnectionTable wrong_routes;
    ASSERT_TRUE(wrong_routes.Open({11, 34}, 4));
    EagerReceiveDomain wrong_domain;
    const auto opened = wrong_domain.Open(buffers_, credits_, wrong_routes);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EagerRxErrorCode::kInvalidArgument);
    EXPECT_EQ(wrong_domain.buffers(), nullptr);
    EXPECT_EQ(credits_.receive_domain(), 33u);
    EXPECT_EQ(buffers_.posted(), 8u);
    EXPECT_TRUE(wrong_routes.Close());
}

TEST_F(EagerReceiveCloseTest, PeerWithdrawalWaitsForRealRxLeaseAndRefillBeforeIndependentPhysicalClose) {
    ReceiveCreditDomain remote_receive;
    SenderCreditAccount local_sender, remote_sender;
    ASSERT_TRUE(remote_receive.Open({66, 8, 2, 2, 1}));
    ASSERT_TRUE(remote_receive.RecordInitialPosted(CreditClass::kData, 6));
    ASSERT_TRUE(remote_receive.RecordInitialPosted(CreditClass::kControl, 2));
    const auto remote_peer = remote_receive.Admit({11, 66, 88});
    ASSERT_TRUE(remote_peer);
    ASSERT_TRUE(remote_receive.Grant(*remote_peer, CreditClass::kData, 6));
    ASSERT_TRUE(remote_receive.Grant(*remote_peer, CreditClass::kControl, 2));
    ASSERT_TRUE(local_sender.Open({22, 66, 88}));
    ASSERT_TRUE(local_sender.Update({22, 66, 88}, 6, 2));
    ASSERT_TRUE(remote_sender.Open({11, 33, 77}));
    ASSERT_TRUE(remote_sender.Update({11, 33, 77}, 6, 2));
    // 模型远端提交一个 DATA，真实 Mock 接收 CQE 延后消费；撤回帧先到也不能把它当未用授权。
    const auto reservation = remote_sender.Reserve(CreditClass::kData, 1);
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(remote_sender.Resolve(*reservation, 1));
    const control::HandshakeIdentity local{11, 33, 1, 1, 101, {0, 1}};
    const control::HandshakeIdentity remote{22, 66, 2, 1, 102, {0, 1}};
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    control::PeerCreditControl left, right;
    ASSERT_TRUE(left.Open(sockets[0], {local, remote, 77, 88, &local_sender, &credits_, source_.peer, 10000, true}, 0));
    ASSERT_TRUE(
        right.Open(sockets[1], {remote, local, 88, 77, &remote_sender, &remote_receive, *remote_peer, 10000, true}, 0));
    std::uint64_t now = 0;
    const auto pump = [&] {
        for (unsigned i = 0; i < 8; ++i) {
            EXPECT_TRUE(left.Progress(++now));
            EXPECT_TRUE(right.Progress(now));
        }
    };
    pump();
    EXPECT_TRUE(left.stats().peer_stop_received);
    EXPECT_FALSE(left.stats().receive_retired);
    EXPECT_FALSE(left.stats().complete);
    EXPECT_FALSE(domain_.frozen());
    hold_next_ = true;
    Receive();
    ASSERT_TRUE(held_packet_);
    pump();
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_FALSE(left.stats().receive_retired);
    ASSERT_TRUE(domain_.Release(*held_packet_));
    pump();
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_FALSE(left.stats().receive_retired);
    const auto posts = post_calls_;
    const auto refilled = domain_.Refill();
    ASSERT_TRUE(refilled);
    EXPECT_EQ(*refilled, 1u);
    EXPECT_EQ(post_calls_, posts + 1);
    pump();
    ASSERT_TRUE(left.stats().complete);
    ASSERT_TRUE(right.stats().complete);
    EXPECT_EQ(credits_.stats().active_peers, 0u);
    EXPECT_EQ(credits_.stats().physical_posted, 8u);
    EXPECT_EQ(buffers_.posted(), 8u);
    EXPECT_EQ(jfr_delete_calls_, 0u);
    EXPECT_EQ(unregister_calls_, 0u);

    // 授信退休仍未删除 raw 域。M1 另外获得完整静默屏障后才能销毁，且不能越过还在借用
    // 账本的协议控制器直接清空元数据；先 Close 协调器解除绑定，再完成 M5.3.6 核销。
    ClosePhysical();
    EXPECT_FALSE(domain_.CloseDestroyed(closer_));
    ASSERT_TRUE(left.Close());
    ASSERT_TRUE(right.Close());
    ASSERT_TRUE(domain_.CloseDestroyed(closer_));
    ExpectEmptyMetadata();
    EXPECT_TRUE(local_sender.Close());
    EXPECT_TRUE(remote_sender.Close());
    EXPECT_TRUE(remote_receive.Close());
}

TEST_F(EagerReceiveCloseTest, ExternalReceiptCorruptionRejectsWholeRetirementWithoutPartialClearing) {
    EXPECT_EXIT(
        {
            Receive();
            Receive();
            // 白盒注入违规 owner：新账本第一个 receipt 是 slot=0/id=1。先取得真实 refill
            // 接受，再绕过 Eager 的队列核销它，模拟两个元数据层不一致；不能追加伪造 post 清理。
            const auto accepted = buffers_.Refill(1);
            RxCreditReceipt first;
            first.domain = &credits_;
            first.slot = 0;
            first.id = 1;
            if (!accepted || *accepted != 1 || !credits_.RecordRefillAccepted(std::span(&first, 1))) {
                _exit(1);
            }
            Begin();
            const auto physically_closed = Advance();
            if (!physically_closed || physically_closed->stage != raw::ReceiveDomainCloseStage::kClosed) {
                _exit(2);
            }
            const auto failed = domain_.CloseDestroyed(closer_);
            if (failed || failed.error().code != EagerRxErrorCode::kCreditFailed || domain_.pending_refill() != 2 ||
                credits_.stats().released != 1 || credits_.stats().active_peers != 1 ||
                credits_.receive_domain() != 33) {
                _exit(3);
            }
            // 真实 DMA 已关闭，但错误元数据仍保留；错误退出不能靠强制补投/授信掩盖账本损坏。
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace connection
} // namespace kbsocket
