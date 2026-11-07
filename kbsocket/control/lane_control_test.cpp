// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/lane_control.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/control/tx_lane_snapshot.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace control {
namespace {
class LaneControlTest : public ::testing::Test {
protected:
    struct Sources final : RawSourceAdmission {
        bool ready = true, reject = false;
        std::uint32_t updates = 0;
        HandshakeTxLanes current;
        std::expected<void, RawSourceAdmissionError> Prepare(const RawSourceAdmissionRequest&) noexcept override {
            return {};
        }
        std::expected<void, RawSourceAdmissionError> Check() const noexcept override {
            if (!ready) {
                return std::unexpected(RawSourceAdmissionError::kNotReady);
            }
            return {};
        }
        std::expected<void, RawSourceAdmissionError> Update(const HandshakeTxLanes& lanes,
                                                            bool authorized) noexcept override {
            if (reject || !authorized) {
                return std::unexpected(RawSourceAdmissionError::kExhausted);
            }
            ++updates;
            current = lanes;
            return {};
        }
    } sources_;
    static inline LaneControlTest* self;
    urma_context_t context_{};
    urma_device_cap_t cap_{};
    std::array<urma_jfc_t, 4> cqs_{};
    std::array<urma_jfr_t, 2> jfrs_{};
    std::array<urma_jetty_t, 16> jetties_{};
    std::size_t cqs_created_ = 0, jfrs_created_ = 0, jetties_created_ = 0;
    bool fail_create_ = false, authorize_ = true;
    std::uint32_t authorizations_ = 0;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    raw::JettyPool pool_;
    HandshakeHello local_, peer_;
    LaneControl controller_;
    HandshakeChannel remote_;
    int unopened_fd_ = -1;
    std::uint64_t now_ = 1;
    static constexpr std::uint64_t kTimeout = 1000;

    static HandshakeIdentity Identity(std::uint64_t n) {
        return {n, n + 1, n + 2, n + 3, n + 4, {7, 300}};
    }
    void SetUp() override {
        self = this;
        cap_.max_jetty = 128;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 1024;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 1;
        raw::UrmaFunctions functions;
        functions.create_jfc = [](auto* context, auto* config) {
            auto* cq = &self->cqs_[self->cqs_created_++];
            cq->urma_ctx = context;
            cq->jfc_cfg = *config;
            return cq;
        };
        functions.create_jfr = [](auto* context, auto* config) {
            auto* jfr = &self->jfrs_[self->jfrs_created_++];
            jfr->urma_ctx = context;
            jfr->jfr_cfg = *config;
            return jfr;
        };
        functions.create_jetty = [](auto* context, auto* config) -> urma_jetty_t* {
            if (std::exchange(self->fail_create_, false)) {
                errno = ENOMEM;
                return nullptr;
            }
            auto* jetty = &self->jetties_[self->jetties_created_++];
            jetty->urma_ctx = context;
            jetty->jetty_cfg = *config;
            jetty->jetty_id.eid.raw[0] = 11;
            jetty->jetty_id.uasid = 7;
            jetty->jetty_id.id = 100 + self->jetties_created_;
            return jetty;
        };
        functions.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        functions.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        functions.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        functions.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        functions.flush_jetty = [](auto*, auto, auto*) { return 0; };
        override_.emplace(functions);
        raw::JettyPoolConfig pool_config;
        pool_config.jetty_count = 2;
        pool_config.tx_depth = pool_config.rx_depth = 1;
        ASSERT_TRUE(pool_.Open(&context_, cap_, pool_config));
        const auto snapshot = CaptureTxLanes(pool_);
        ASSERT_TRUE(snapshot);
        local_.identity = Identity(1);
        local_.credit_epoch = 1;
        local_.tx_lanes = *snapshot;
        local_.jetty_id = pool_.rx_target_jetty_id()->id;
        std::memcpy(local_.eid.data(), pool_.rx_target_jetty_id()->eid.raw, local_.eid.size());
        peer_ = local_;
        peer_.identity = Identity(100);
        peer_.jetty_id = 500;
        peer_.tx_lanes.pool_epoch = 20;
        for (auto& lane : peer_.tx_lanes.lanes) {
            lane.jetty_id += 1000;
        }
        sources_.current = peer_.tx_lanes;
        std::array<int, 2> fds;
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds.data()), 0);
        unopened_fd_ = fds[0];
        ASSERT_TRUE(remote_.Open(fds[1], true, 0, std::numeric_limits<std::uint64_t>::max()));
    }
    void TearDown() override {
        EXPECT_TRUE(controller_.Close());
        EXPECT_TRUE(remote_.Close());
        if (unopened_fd_ >= 0) {
            ::close(unopened_fd_);
        }
        EXPECT_TRUE(pool_.Close());
        override_.reset();
        self = nullptr;
    }
    void Open(bool allow_update = true) {
        LaneControlConfig config;
        config.local = &local_;
        config.peer = &peer_;
        config.pool = &pool_;
        config.sources = &sources_;
        config.timeout_ns = kTimeout;
        config.authorization_cookie = this;
        if (allow_update) {
            config.authorize_update = [](void* cookie, const auto& identity, const auto&) noexcept {
                auto& fixture = *static_cast<LaneControlTest*>(cookie);
                ++fixture.authorizations_;
                return fixture.authorize_ && identity == fixture.peer_.identity;
            };
        }
        ASSERT_TRUE(controller_.Open(unopened_fd_, config, 0));
        unopened_fd_ = -1;
    }
    void Replace() {
        const auto lane = *pool_.lane(0);
        ASSERT_TRUE(pool_.BeginDrain(lane));
        ASSERT_TRUE(pool_.ObserveFlushDone(lane));
        std::array<urma_cr_t, 1> completions{};
        ASSERT_TRUE(pool_.FlushSend(lane, completions));
        ASSERT_TRUE(pool_.ReplaceDrainedLane(
            lane, {.events_acked = true, .queue_users_stopped = true, .completions_drained = true}));
    }
    HandshakeMessage Ack(std::uint64_t id = 1) {
        HandshakeMessage message;
        message.kind = HandshakeMessageKind::kLaneAck;
        message.lane_ack = {peer_.identity, local_.identity, id};
        return message;
    }
    HandshakeMessage Update(std::uint64_t id = 1) {
        HandshakeMessage message;
        message.kind = HandshakeMessageKind::kLaneUpdate;
        message.lane_update = {peer_.identity, local_.identity, id, peer_.tx_lanes};
        ++message.lane_update.tx_lanes.lanes[0].generation;
        message.lane_update.tx_lanes.lanes[0].jetty_id += 100;
        return message;
    }
    void Send(const HandshakeMessage& message) {
        ASSERT_TRUE(remote_.Queue(message));
        ASSERT_TRUE(remote_.Progress(now_++, {.receive = false}));
        ASSERT_TRUE(remote_.output_flushed());
        controller_.NotifyReadable();
    }
    std::expected<LaneControlProgress, LaneControlError> Step() {
        return controller_.Progress(now_++);
    }
    void BlockOutput() {
        std::array<std::byte, 4096> filler{};
        // 只填本地发送方向；对端仍可发送 UPDATE/ACK，用于真实 socket 背压覆盖。
        for (std::size_t i = 0; i < 4096; ++i) {
            if (::send(controller_.fd(), filler.data(), filler.size(), MSG_NOSIGNAL) < 0) {
                ASSERT_EQ(errno, EAGAIN);
                return;
            }
        }
        FAIL() << "socket send buffer did not fill within finite test budget";
    }
    HandshakeMessage Receive() {
        const auto progress = remote_.Progress(now_++);
        EXPECT_TRUE(progress);
        if (!progress) {
            return {};
        }
        EXPECT_TRUE(progress->message_ready);
        const auto message = remote_.TakeMessage();
        EXPECT_TRUE(message);
        return message.value_or(HandshakeMessage{});
    }
};

TEST_F(LaneControlTest, IdleConnectionDoesNotStartTimeout) {
    Open();
    const auto first = Step();
    ASSERT_TRUE(first);
    EXPECT_TRUE(first->tx_allowed);
    EXPECT_FALSE(controller_.pending());
    const auto idle = controller_.Progress(100 * kTimeout);
    ASSERT_TRUE(idle);
    EXPECT_TRUE(idle->tx_allowed);
    EXPECT_EQ(idle->processed, 0u);
}

TEST_F(LaneControlTest, RebuiltLaneRequiresExactAckAndPreservesInitialHello) {
    Open();
    Replace();
    const auto changed = Step();
    ASSERT_TRUE(changed);
    EXPECT_FALSE(changed->tx_allowed);
    EXPECT_EQ(controller_.local_update_id(), 0u);
    const auto update = Receive();
    ASSERT_EQ(update.kind, HandshakeMessageKind::kLaneUpdate);
    EXPECT_EQ(update.lane_update.update_id, 1u);
    EXPECT_EQ(update.lane_update.sender, local_.identity);
    EXPECT_EQ(update.lane_update.receiver, peer_.identity);
    EXPECT_EQ(update.lane_update.tx_lanes.lanes[0].generation, local_.tx_lanes.lanes[0].generation + 1);
    Send(Ack());
    const auto confirmed = Step();
    ASSERT_TRUE(confirmed);
    EXPECT_TRUE(confirmed->tx_allowed);
    EXPECT_EQ(controller_.local_update_id(), 1u);
    EXPECT_EQ(sources_.updates, 0u);
    EXPECT_FALSE(controller_.pending());
}

TEST_F(LaneControlTest, UnsolicitedAckFailsClosed) {
    Open();
    Send(Ack());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(controller_.fd(), -1);
}

TEST_F(LaneControlTest, WrongAckSequenceFailsWithoutConfirmation) {
    Open();
    Replace();
    ASSERT_TRUE(Step());
    Receive();
    Send(Ack(2));
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(controller_.local_update_id(), 0u);
}

TEST_F(LaneControlTest, AckMustMatchWholeSessionIdentity) {
    Open();
    Replace();
    ASSERT_TRUE(Step());
    Receive();
    auto ack = Ack();
    ++ack.lane_ack.receiver.token.generation;
    Send(ack);
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
}

TEST_F(LaneControlTest, ReplayedAckFailsAfterFirstConfirmation) {
    Open();
    Replace();
    ASSERT_TRUE(Step());
    Receive();
    Send(Ack());
    ASSERT_TRUE(Step());
    Send(Ack());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(controller_.local_update_id(), 1u);
}

TEST_F(LaneControlTest, PeerSourcesAreInstalledBeforeAckCanLeave) {
    Open();
    const auto update = Update();
    Send(update);
    const auto installed = Step();
    ASSERT_TRUE(installed);
    EXPECT_TRUE(installed->tx_allowed);
    EXPECT_EQ(sources_.updates, 1u);
    EXPECT_EQ(authorizations_, 1u);
    EXPECT_TRUE(SameTxLanes(sources_.current, update.lane_update.tx_lanes));
    EXPECT_EQ(controller_.peer_update_id(), 1u);
    EXPECT_TRUE(controller_.output_pending());
    const auto not_sent = remote_.Progress(now_++);
    ASSERT_TRUE(not_sent);
    EXPECT_FALSE(not_sent->message_ready);
    ASSERT_TRUE(Step());
    const auto ack = Receive();
    EXPECT_EQ(ack.kind, HandshakeMessageKind::kLaneAck);
    EXPECT_EQ(ack.lane_ack.sender, local_.identity);
    EXPECT_EQ(ack.lane_ack.receiver, peer_.identity);
    EXPECT_EQ(ack.lane_ack.update_id, 1u);
}

TEST_F(LaneControlTest, ReplayedUpdateDoesNotInstallSourcesAgain) {
    Open();
    Send(Update());
    ASSERT_TRUE(Step());
    ASSERT_TRUE(Step());
    Receive();
    Send(Update());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(sources_.updates, 1u);
}

TEST_F(LaneControlTest, UnauthorizedUpdateNeverInstallsOrAcknowledgesSources) {
    Open();
    authorize_ = false;
    Send(Update());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kUnauthorized);
    EXPECT_EQ(sources_.updates, 0u);
    EXPECT_EQ(controller_.peer_update_id(), 0u);
}

TEST_F(LaneControlTest, UpdateFromAnotherSessionNeverReachesAuthorizer) {
    Open();
    auto update = Update();
    ++update.lane_update.sender.session;
    Send(update);
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(authorizations_, 0u);
    EXPECT_EQ(sources_.updates, 0u);
}

TEST_F(LaneControlTest, PhysicalChangeWithoutGenerationAdvanceFails) {
    Open();
    auto update = Update();
    update.lane_update.tx_lanes.lanes[0].generation = peer_.tx_lanes.lanes[0].generation;
    Send(update);
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(authorizations_, 0u);
    EXPECT_EQ(sources_.updates, 0u);
}

TEST_F(LaneControlTest, ExhaustedSourceAdmissionNeverAcknowledgesUpdate) {
    Open();
    sources_.reject = true;
    Send(Update());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kSource);
    EXPECT_EQ(result.error().detail, static_cast<int>(RawSourceAdmissionError::kExhausted));
    EXPECT_EQ(controller_.peer_update_id(), 0u);
}

TEST_F(LaneControlTest, PartialInputStartsDeadlineButDoesNotPermitPollingFallback) {
    Open();
    std::array<std::byte, kHandshakeMaxFrameBytes> bytes;
    ASSERT_TRUE(EncodeHandshakeMessage(Update(), bytes));
    ASSERT_EQ(::send(remote_.fd(), bytes.data(), 5, MSG_NOSIGNAL), 5);
    ASSERT_TRUE(Step());
    EXPECT_TRUE(controller_.pending());
    const auto result = controller_.Progress(kTimeout + 1);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kTimeout);
}

TEST_F(LaneControlTest, UnacknowledgedLocalChangeHasDeadline) {
    Open();
    Replace();
    ASSERT_TRUE(Step());
    Receive();
    const auto result = controller_.Progress(kTimeout + 1);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kTimeout);
    EXPECT_EQ(controller_.local_update_id(), 0u);
}

TEST_F(LaneControlTest, AckCannotConfirmAnUpdateThatHasNotBeenFullySent) {
    Open();
    BlockOutput();
    Replace();
    Send(Ack());
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kProtocol);
    EXPECT_EQ(controller_.local_update_id(), 0u);
}

TEST_F(LaneControlTest, AckBackpressureExpiresWithoutRemovingInstalledSources) {
    Open();
    BlockOutput();
    Send(Update());
    ASSERT_TRUE(Step());
    EXPECT_EQ(sources_.updates, 1u);
    ASSERT_TRUE(Step());
    EXPECT_TRUE(controller_.output_pending());
    const auto result = controller_.Progress(kTimeout + 2);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kTimeout);
    EXPECT_EQ(controller_.peer_update_id(), 1u);
    EXPECT_EQ(sources_.updates, 1u);
}

TEST_F(LaneControlTest, UpdatesAreExplicitlyOptedIn) {
    Open(false);
    Replace();
    const auto result = Step();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kLane);
}

TEST_F(LaneControlTest, SimultaneousUpdatesAcknowledgeBothDirectionsWithoutDeadlock) {
    Open();
    Replace();
    Send(Update());
    const auto first = Step();
    ASSERT_TRUE(first);
    EXPECT_FALSE(first->tx_allowed);
    EXPECT_EQ(sources_.updates, 1u);
    EXPECT_EQ(Receive().kind, HandshakeMessageKind::kLaneUpdate);
    Send(Ack());
    const auto confirmed = Step();
    ASSERT_TRUE(confirmed);
    EXPECT_TRUE(confirmed->tx_allowed);
    EXPECT_EQ(controller_.local_update_id(), 1u);
    EXPECT_EQ(controller_.peer_update_id(), 1u);
    const auto ack = Receive();
    EXPECT_EQ(ack.kind, HandshakeMessageKind::kLaneAck);
    EXPECT_EQ(ack.lane_ack.update_id, 1u);
    EXPECT_FALSE(controller_.pending());
}

TEST_F(LaneControlTest, AnotherRebuildWhileAwaitingAckRequiresAnotherAnnouncement) {
    Open();
    Replace();
    ASSERT_TRUE(Step());
    const auto first = Receive();
    Replace();
    Send(Ack());
    const auto still_blocked = Step();
    ASSERT_TRUE(still_blocked);
    EXPECT_FALSE(still_blocked->tx_allowed);
    EXPECT_EQ(controller_.local_update_id(), 1u);
    ASSERT_TRUE(Step());
    const auto second = Receive();
    ASSERT_EQ(second.kind, HandshakeMessageKind::kLaneUpdate);
    EXPECT_EQ(second.lane_update.update_id, 2u);
    EXPECT_EQ(second.lane_update.tx_lanes.lanes[0].generation, first.lane_update.tx_lanes.lanes[0].generation + 1);
    Send(Ack(2));
    const auto confirmed = Step();
    ASSERT_TRUE(confirmed);
    EXPECT_TRUE(confirmed->tx_allowed);
    EXPECT_EQ(controller_.local_update_id(), 2u);
}

TEST_F(LaneControlTest, FailedReplacementWaitsBoundedlyForNewPhysicalLane) {
    Open();
    const auto lane = *pool_.lane(0);
    ASSERT_TRUE(pool_.BeginDrain(lane));
    ASSERT_TRUE(pool_.ObserveFlushDone(lane));
    std::array<urma_cr_t, 1> completions{};
    ASSERT_TRUE(pool_.FlushSend(lane, completions));
    fail_create_ = true;
    ASSERT_FALSE(pool_.ReplaceDrainedLane(
        lane, {.events_acked = true, .queue_users_stopped = true, .completions_drained = true}));
    const auto waiting = Step();
    ASSERT_TRUE(waiting);
    EXPECT_FALSE(waiting->tx_allowed);
    EXPECT_FALSE(controller_.output_pending());
    const auto result = controller_.Progress(kTimeout + 1);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LaneControlErrorCode::kTimeout);
}

} // namespace
} // namespace control
} // namespace kbsocket
