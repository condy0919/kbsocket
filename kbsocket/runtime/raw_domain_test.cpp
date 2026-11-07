// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/raw_domain.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection_table.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/control/peer_credit_control.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/runtime_shard.hpp"
#include "kbsocket/runtime/rx_source_registry.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace runtime {
namespace {

class RawDomainTest : public ::testing::Test {
protected:
    enum class Deleted { kJetty, kJfr, kMemory, kJfc };
    struct Posted {
        std::uint64_t id;
        std::span<std::byte> memory;
    };
    static inline RawDomainTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.create_jfc = [](auto*, auto*) -> urma_jfc_t* {
            auto& self = *active_;
            ++self.jfc_created_;
            if (self.fail_jfc_at_ == self.jfc_created_) {
                errno = ENOMEM;
                return nullptr;
            }
            return self.jfc_created_ == 1 ? &self.tx_ : &self.rx_;
        };
        functions.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        functions.create_jetty = [](auto*, auto*) {
            return active_->jetty_created_++ == 0 ? &active_->passive_ : &active_->lane_;
        };
        functions.delete_jetty = [](auto*) {
            ++active_->jetty_delete_calls_;
            if (active_->delete_jetty_status_ == URMA_SUCCESS) {
                active_->deleted_.push_back(Deleted::kJetty);
            }
            return active_->delete_jetty_status_;
        };
        functions.delete_jfr = [](auto*) {
            ++active_->jfr_delete_calls_;
            if (active_->delete_jfr_status_ == URMA_SUCCESS) {
                active_->deleted_.push_back(Deleted::kJfr);
                active_->posted_.clear();
            }
            return active_->delete_jfr_status_;
        };
        functions.delete_jfc = [](auto*) {
            ++active_->jfc_delete_calls_;
            if (active_->delete_jfc_status_ == URMA_SUCCESS) {
                active_->deleted_.push_back(Deleted::kJfc);
            }
            return active_->delete_jfc_status_;
        };
        functions.register_seg = [](auto*, auto*) -> urma_target_seg_t* {
            ++active_->register_calls_;
            if (active_->fail_register_) {
                errno = ENOMEM;
                return nullptr;
            }
            return &active_->segment_;
        };
        functions.unregister_seg = [](auto*) {
            ++active_->unregister_calls_;
            if (active_->unregister_status_ == URMA_SUCCESS) {
                active_->deleted_.push_back(Deleted::kMemory);
            }
            return active_->unregister_status_;
        };
        functions.delete_context = [](auto*) {
            ++active_->context_delete_calls_;
            return URMA_SUCCESS;
        };
        functions.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        functions.flush_jetty = [](auto*, int, auto*) { return 0; };
        functions.post_jfr_wr = [](auto*, auto* first, auto** bad) {
            auto& self = *active_;
            ++self.post_calls_;
            const bool inject = self.fail_post_at_ == 0 || self.fail_post_at_ == self.post_calls_;
            *bad = nullptr;
            std::size_t count = 0;
            for (auto* wr = first; wr; wr = wr->next, ++count) {
                if (inject && self.accepted_prefix_ && count >= *self.accepted_prefix_) {
                    if (!*bad) {
                        *bad = wr;
                    }
                    continue;
                }
                const auto& sge = wr->src.sge[0];
                self.posted_.push_back({wr->user_ctx, {reinterpret_cast<std::byte*>(sge.addr), sge.len}});
            }
            if (inject && self.unknown_acceptance_) {
                *bad = nullptr;
            }
            return inject ? self.post_status_ : URMA_SUCCESS;
        };
        functions.poll_jfc = [](auto* jfc, int count, auto* output) {
            auto& self = *active_;
            if (jfc != &self.rx_) {
                ++self.tx_polls_;
                return 0;
            }
            ++self.rx_polls_;
            if (self.poll_error_) {
                return -EIO;
            }
            int filled = 0;
            while (filled < count && !self.completions_.empty()) {
                output[filled++] = self.completions_.front();
                self.completions_.pop_front();
            }
            return filled;
        };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        context_.async_fd = -1;
        passive_.jetty_id.id = 19;
        lane_.jetty_id.id = 20;
        jfr_.jfr_id.id = 23;
        capabilities_.trans_mode = URMA_TM_RM;
        capabilities_.rm_tp_cap.bs.ctp = 1;
        capabilities_.max_jetty = 2;
        capabilities_.max_jfs_depth = capabilities_.max_jfr_depth = capabilities_.max_jfc_depth = 1024;
        capabilities_.max_jfs_sge = capabilities_.max_jfs_rsge = capabilities_.max_jfr_sge = 1;
        config_.local_domain = {11, 33};
        config_.context_incarnation = 101;
        config_.queues.tx_depth = 8;
        config_.queues.rx_depth = config_.queues.rx_cq_depth = 8;
        config_.tx_attempts = 8;
        config_.route_capacity = config_.source_capacity = config_.peer_capacity = 4;
        config_.control_slots = 2;
        config_.base_credits = 1;
    }
    static RawDomainCloseProof Proof() {
        return {true, {true, true, true, true}};
    }
    void Open() {
        ASSERT_TRUE(domain_.Open(&context_, capabilities_, config_));
        const auto resources = domain_.resources();
        ASSERT_TRUE(resources);
        resources_ = *resources;
    }
    void AddPeerAndRoute() {
        ASSERT_TRUE(resources_);
        auto& resources = *resources_;
        const auto peer = resources.credits->Admit({22, 33, 77});
        ASSERT_TRUE(peer);
        source_ = {{11, 33}, {connection::kWireVersion, 22, 33, 44}, *peer};
        ASSERT_TRUE(resources.credits->Grant(*peer, connection::CreditClass::kData, 6));
        ASSERT_TRUE(resources.credits->Grant(*peer, connection::CreditClass::kControl, 2));
        const auto route = resources.routes->Reserve();
        ASSERT_TRUE(route);
        route_ = *route;
        ASSERT_TRUE(resources.routes->Publish(*route_, {source_.peer_context, route_->token, 55}, this));
    }
    void RetireRoute() {
        if (route_) {
            ASSERT_TRUE(resources_->routes->Retire(*route_));
            route_.reset();
        }
    }
    void QueueCompletion() {
        ASSERT_FALSE(posted_.empty());
        const auto posted = posted_.front();
        posted_.pop_front();
        const std::array data{std::byte{0x42}};
        connection::WireFrameInput frame;
        frame.token = route_ ? route_->token : connection::WireToken{1, 1};
        frame.data = data;
        const auto encoded = connection::EncodeWireFrame(frame, posted.memory);
        ASSERT_TRUE(encoded);
        urma_cr_t completion{};
        completion.flag.bs.s_r = 1;
        completion.flag.bs.jetty = 1;
        completion.local_id = passive_.jetty_id.id;
        completion.user_ctx = posted.id;
        completion.completion_len = encoded->payload_bytes;
        completion.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        completion.imm_data = encoded->imm_data;
        completions_.push_back(completion);
    }
    static std::expected<connection::EagerRxDisposition, connection::EagerRxError>
    Handler(void* cookie, connection::EagerReceiveDomain&, const connection::WireFrameView&,
            connection::EagerRxPacket packet, std::uint64_t) noexcept {
        auto& self = *static_cast<RawDomainTest*>(cookie);
        ++self.handler_calls_;
        if (self.hold_next_) {
            self.held_ = packet;
            return connection::EagerRxDisposition::kHeld;
        }
        return connection::EagerRxDisposition::kRelease;
    }
    void Receive() {
        QueueCompletion();
        raw::RxBufferEvent event;
        const auto count = resources_->rx->Poll(std::span(&event, 1));
        ASSERT_TRUE(count);
        ASSERT_EQ(*count, 1u);
        ASSERT_TRUE(resources_->receive->Dispatch(event, source_, Handler, 1));
    }
    void Close() {
        ASSERT_TRUE(domain_.BeginClose());
        const auto closed = domain_.AdvanceClose(Proof());
        ASSERT_TRUE(closed);
        EXPECT_EQ(closed->stage, RawDomainStage::kClosed);
    }
    void ExpectNoDeletion() {
        EXPECT_TRUE(deleted_.empty());
        EXPECT_EQ(jetty_delete_calls_, 0u);
        EXPECT_EQ(jfr_delete_calls_, 0u);
        EXPECT_EQ(unregister_calls_, 0u);
        EXPECT_EQ(jfc_delete_calls_, 0u);
    }
    void TearDown() override {
        EXPECT_TRUE(withdrawal_.Close());
        if (peer_fd_ >= 0) {
            ::close(peer_fd_);
        }
        EXPECT_TRUE(shard_.Close());
        if (held_ && resources_->receive->held()) {
            EXPECT_TRUE(resources_->receive->Release(*held_));
        }
        RetireRoute();
        if (source_lease_) {
            EXPECT_TRUE(resources_->sources->Remove(*source_lease_, {101, true, true, true}));
        }
        delete_jetty_status_ = delete_jfr_status_ = delete_jfc_status_ = unregister_status_ = URMA_SUCCESS;
        poll_error_ = false;
        const auto stage = domain_.Inspect().stage;
        if (stage != RawDomainStage::kEmpty && stage != RawDomainStage::kClosed && stage != RawDomainStage::kRetained) {
            EXPECT_TRUE(domain_.BeginClose());
            for (unsigned i = 0; i < 4 && domain_.Inspect().stage != RawDomainStage::kClosed; ++i) {
                EXPECT_TRUE(domain_.AdvanceClose(Proof()));
            }
            EXPECT_EQ(domain_.Inspect().stage, RawDomainStage::kClosed);
        }
        EXPECT_TRUE(progress_.Close());
        EXPECT_TRUE(sender_credit_.Close());
        EXPECT_EQ(context_delete_calls_, 0u);
        active_ = nullptr;
    }

    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_device_cap_t capabilities_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t passive_{}, lane_{};
    urma_target_seg_t segment_{};
    urma_target_jetty_t target_{};
    RawDomainConfig config_;
    RawDomain domain_;
    std::optional<RawDomainResources> resources_;
    ConnectionProgress progress_;
    RuntimeShard shard_;
    connection::SenderCreditAccount sender_credit_;
    control::PeerCreditControl withdrawal_;
    int peer_fd_ = -1;
    connection::EagerReceiveSource source_;
    std::optional<connection::ConnectionRouteLease> route_;
    std::optional<RxSourceLease> source_lease_;
    std::optional<connection::EagerRxPacket> held_;
    std::deque<Posted> posted_;
    std::deque<urma_cr_t> completions_;
    std::vector<Deleted> deleted_;
    std::optional<std::size_t> accepted_prefix_;
    urma_status_t post_status_ = URMA_SUCCESS, delete_jetty_status_ = URMA_SUCCESS;
    urma_status_t delete_jfr_status_ = URMA_SUCCESS, delete_jfc_status_ = URMA_SUCCESS;
    urma_status_t unregister_status_ = URMA_SUCCESS;
    bool unknown_acceptance_ = false, fail_register_ = false, poll_error_ = false, hold_next_ = false;
    std::size_t fail_jfc_at_ = 0, fail_post_at_ = 0, jfc_created_ = 0, jetty_created_ = 0, register_calls_ = 0;
    std::size_t jetty_delete_calls_ = 0, jfr_delete_calls_ = 0, jfc_delete_calls_ = 0;
    std::size_t unregister_calls_ = 0, context_delete_calls_ = 0, post_calls_ = 0;
    std::size_t rx_polls_ = 0, tx_polls_ = 0, handler_calls_ = 0;
};

TEST_F(RawDomainTest, RejectsInvalidIdentityBeforeCreatingResources) {
    config_.context_incarnation = 0;
    const auto opened = domain_.Open(&context_, capabilities_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, RawDomainErrorCode::kInvalidArgument);
    EXPECT_EQ(domain_.Inspect().stage, RawDomainStage::kEmpty);
    EXPECT_FALSE(domain_.resources());
    EXPECT_EQ(jfc_created_, 0u);
    EXPECT_EQ(post_calls_, 0u);
}

TEST_F(RawDomainTest, AssemblesOneOwnedDomainWithRealInitialCredit) {
    Open();
    ASSERT_TRUE(resources_);
    const auto& resources = *resources_;
    const auto snapshot = domain_.Inspect();
    EXPECT_EQ(snapshot.stage, RawDomainStage::kReady);
    EXPECT_EQ(snapshot.initial_accepted, 8u);
    EXPECT_TRUE(snapshot.initial_accepted_known);
    EXPECT_EQ(snapshot.posted, 8u);
    EXPECT_EQ(snapshot.credits.physical_posted, 8u);
    EXPECT_EQ(snapshot.credits.data.free, 6u);
    EXPECT_EQ(snapshot.credits.control.free, 2u);
    EXPECT_EQ(resources.rx->pool(), resources.pool);
    EXPECT_EQ(resources.ledger->pool(), resources.pool);
    EXPECT_EQ(resources.sender->pool(), resources.pool);
    EXPECT_EQ(resources.sender->context(), &context_);
    EXPECT_TRUE(resources.sender->defer_drain());
    EXPECT_EQ(resources.receive->buffers(), resources.rx);
    EXPECT_EQ(resources.sources->buffers(), resources.rx);
    EXPECT_EQ(resources.sources->context_incarnation(), 101u);
    EXPECT_EQ(register_calls_, 1u);
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(posted_.size(), 8u);
    EXPECT_FALSE(domain_.Open(&context_, capabilities_, config_));
}

TEST_F(RawDomainTest, InitialPostSpansSeveralProviderBatches) {
    // 一次 post 上限 256；域容量不能误当作一次投递已经全部兑现。
    config_.queues.rx_depth = config_.queues.rx_cq_depth = 512;
    config_.control_slots = 64;
    Open();
    EXPECT_EQ(post_calls_, 2u);
    EXPECT_EQ(domain_.Inspect().initial_accepted, 512u);
    EXPECT_EQ(domain_.Inspect().credits.physical_posted, 512u);
    Close();
    EXPECT_EQ(domain_.Inspect().retired_rx, 512u);
}

TEST_F(RawDomainTest, CloseOrdersQueuesBeforeMemoryAndCompletionQueues) {
    Open();
    Close();
    // 残留 RX 不需要伪造完成；DeleteJfr 是 DMA 屏障，然后才能注销 MR、删除 CQ。
    EXPECT_EQ(deleted_, (std::vector{Deleted::kJetty, Deleted::kJetty, Deleted::kJfr, Deleted::kMemory, Deleted::kJfc,
                                     Deleted::kJfc}));
    const auto closed = domain_.Inspect();
    EXPECT_EQ(closed.retired_rx, 8u);
    EXPECT_EQ(closed.posted, 0u);
    EXPECT_EQ(closed.credits.physical_posted, 0u);
    EXPECT_FALSE(domain_.resources());
    EXPECT_TRUE(domain_.BeginClose());
    EXPECT_TRUE(domain_.AdvanceClose({}));
    EXPECT_FALSE(domain_.Open(&context_, capabilities_, config_));
    EXPECT_EQ(context_delete_calls_, 0u);
}

TEST_F(RawDomainTest, WrongContextBindingDoesNotConsumeSuccessfulBinding) {
    Open();
    urma_context_t wrong_context{};
    ConnectionProgress wrong;
    ASSERT_TRUE(wrong.Open({4, &wrong_context}));
    EXPECT_FALSE(domain_.Bind(wrong));
    EXPECT_FALSE(domain_.Inspect().bound);
    ASSERT_TRUE(wrong.Close());
    ASSERT_TRUE(progress_.Open({4, &context_}));
    const auto bound = domain_.Bind(progress_);
    ASSERT_TRUE(bound);
    EXPECT_TRUE(bound->on_rx);
    EXPECT_TRUE(bound->on_tx);
    EXPECT_TRUE(bound->on_error);
    EXPECT_TRUE(bound->cookie);
    EXPECT_TRUE(bound->external_refill);
    EXPECT_EQ(bound->pool, resources_->pool);
    EXPECT_EQ(bound->rx, resources_->rx);
    EXPECT_EQ(bound->ledger, resources_->ledger);
    EXPECT_FALSE(domain_.Bind(progress_));
}

TEST_F(RawDomainTest, RequiresExplicitProgressDetachBeforeAnyDeletion) {
    Open();
    ASSERT_TRUE(progress_.Open({4, &context_}));
    ASSERT_TRUE(domain_.Bind(progress_));
    ASSERT_TRUE(domain_.BeginClose());
    auto proof = Proof();
    proof.progress_detached = false;
    EXPECT_FALSE(domain_.AdvanceClose(proof));
    EXPECT_TRUE(domain_.Inspect().bound);
    ExpectNoDeletion();
    Close();
    EXPECT_FALSE(domain_.Inspect().bound);
    // 域已删除后，调度器不会再访问遗留的 Eager refill 指针。
    EXPECT_TRUE(progress_.Progress(ProgressKind::kRefill, 8, 1));
}

TEST_F(RawDomainTest, EveryPhysicalCloseProofIsRequired) {
    Open();
    ASSERT_TRUE(domain_.BeginClose());
    // 分别缺失静默、Ack、外部用户和 CQ 健康证据；空 CQ 不能替代任一项。
    for (int missing = 0; missing < 4; ++missing) {
        auto proof = Proof();
        switch (missing) {
        case 0:
            proof.quiescence.peer_stopped = false;
            break;
        case 1:
            proof.quiescence.events_acked = false;
            break;
        case 2:
            proof.quiescence.queue_users_stopped = false;
            break;
        case 3:
            proof.quiescence.completion_queues_healthy = false;
            break;
        }
        const auto advanced = domain_.AdvanceClose(proof);
        ASSERT_TRUE(advanced);
        EXPECT_EQ(advanced->stage, RawDomainStage::kClosing);
        ExpectNoDeletion();
    }
    Close();
}

TEST_F(RawDomainTest, BeginCloseStopsNewAdmissionButKeepsExistingRefill) {
    Open();
    AddPeerAndRoute();
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_TRUE(domain_.Inspect().credits.admission_stopped);
    EXPECT_FALSE(resources_->receive->frozen());
    EXPECT_FALSE(resources_->credits->Admit({23, 33, 78}));
    EXPECT_FALSE(resources_->credits->Grant(source_.peer, connection::CreditClass::kData, 1));
    EXPECT_FALSE(resources_->routes->Reserve());
    EXPECT_FALSE(resources_->ledger->Prepare({.target = &target_}));
    // 旧授信在撤回协商期间仍可能到达；消费、Release、真实补投必须继续结账。
    Receive();
    EXPECT_EQ(resources_->receive->pending_refill(), 1u);
    const auto refilled = resources_->receive->Refill();
    ASSERT_TRUE(refilled);
    EXPECT_EQ(*refilled, 1u);
    EXPECT_EQ(domain_.Inspect().posted, 8u);
    EXPECT_EQ(domain_.Inspect().credits.physical_posted, 8u);
    EXPECT_FALSE(resources_->receive->frozen());
    RetireRoute();
    Close();
}

TEST_F(RawDomainTest, ApplicationHeldLeaseSurvivesJfrDeletion) {
    Open();
    AddPeerAndRoute();
    hold_next_ = true;
    Receive();
    ASSERT_TRUE(held_);
    RetireRoute();
    ASSERT_TRUE(domain_.BeginClose());
    const auto waiting = domain_.AdvanceClose(Proof());
    ASSERT_TRUE(waiting);
    EXPECT_EQ(waiting->close_stage, raw::ReceiveDomainCloseStage::kWaitingForLeases);
    EXPECT_EQ(waiting->held, 1u);
    EXPECT_EQ(waiting->retired_rx, 7u);
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 0u);
    EXPECT_EQ(jfc_delete_calls_, 0u);
    ASSERT_TRUE(resources_->receive->Release(*held_));
    held_.reset();
    Close();
    EXPECT_EQ(unregister_calls_, 1u);
    EXPECT_EQ(domain_.Inspect().credits.held, 0u);
    EXPECT_EQ(domain_.Inspect().credits.released, 0u);
}

TEST_F(RawDomainTest, PreparedAttemptPreventsPhysicalCloseUntilCancelled) {
    Open();
    const auto attempt = resources_->ledger->Prepare({.target = &target_});
    ASSERT_TRUE(attempt);
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    ExpectNoDeletion();
    ASSERT_TRUE(resources_->ledger->Cancel(*attempt));
    Close();
}

TEST_F(RawDomainTest, PostedAttemptRequiresTerminalEvidenceBeforeClose) {
    Open();
    const auto attempt = resources_->ledger->Prepare({.target = &target_});
    ASSERT_TRUE(attempt);
    ASSERT_TRUE(resources_->ledger->Commit(*attempt, 1));
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    ExpectNoDeletion();
    // 模型完成层已确认 WR 终结；Close 本身不能擅自退休账本或重放请求。
    ASSERT_TRUE(resources_->ledger->Complete(*attempt));
    Close();
}

TEST_F(RawDomainTest, EmptyLedgerDoesNotDetachErrorLaneBeforeFlushBoundaryAndDrain) {
    Open();
    ASSERT_TRUE(progress_.Open({4, &context_}));
    ASSERT_TRUE(domain_.Bind(progress_));
    const auto lane = resources_->pool->lane(0);
    ASSERT_TRUE(lane);
    ASSERT_TRUE(resources_->pool->BeginDrain(*lane));
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_EQ(domain_.Inspect().attempts, 0u);
    const auto error_lane = domain_.AdvanceClose(Proof());
    ASSERT_FALSE(error_lane);
    EXPECT_EQ(error_lane.error().code, RawDomainErrorCode::kQueue);
    EXPECT_TRUE(domain_.Inspect().bound);
    ExpectNoDeletion();

    // 空账本不能替代 SQ 排空边界；即使已经消费 FLUSH_DONE，仍须软件 flush 返回 0。
    ASSERT_TRUE(resources_->pool->ObserveFlushDone(*lane));
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_TRUE(domain_.Inspect().bound);
    ExpectNoDeletion();
    urma_cr_t completion{};
    ASSERT_EQ(resources_->pool->FlushSend(*lane, std::span(&completion, 1)), 0);
    Close();
    EXPECT_FALSE(domain_.Inspect().bound);
}

TEST_F(RawDomainTest, RawReservedTicketAlsoPreventsIoDetachWithEmptyLedger) {
    Open();
    ASSERT_TRUE(progress_.Open({4, &context_}));
    ASSERT_TRUE(domain_.Bind(progress_));
    const auto ticket = resources_->pool->Reserve();
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_EQ(domain_.Inspect().attempts, 0u);
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_TRUE(domain_.Inspect().bound);
    ExpectNoDeletion();
    ASSERT_TRUE(resources_->pool->Cancel(*ticket));
    Close();
}

TEST_F(RawDomainTest, LiveLogicalRoutePreventsPhysicalClose) {
    Open();
    AddPeerAndRoute();
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(domain_.Inspect().routes, 1u);
    ExpectNoDeletion();
    RetireRoute();
    Close();
}

TEST_F(RawDomainTest, RetiredLogicalRouteDoesNotImplicitlyRemovePhysicalSource) {
    Open();
    AddPeerAndRoute();
    RxSourceRoute route;
    route.local_id = passive_.jetty_id.id;
    route.remote_id.id = 71;
    route.remote_id.eid.raw[0] = 4;
    route.token = route_->token;
    route.source = source_;
    const auto registered = resources_->sources->Register(route);
    ASSERT_TRUE(registered);
    source_lease_ = *registered;
    RetireRoute();
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(domain_.Inspect().sources, 1u);
    ExpectNoDeletion();
    ASSERT_TRUE(resources_->sources->Remove(*source_lease_, {101, true, true, true}));
    source_lease_.reset();
    Close();
}

TEST_F(RawDomainTest, LivePeerWithdrawalBindingPreventsPhysicalClose) {
    Open();
    AddPeerAndRoute();
    RetireRoute();
    ASSERT_TRUE(sender_credit_.Open({22, 66, 88}));
    ASSERT_TRUE(sender_credit_.Update({22, 66, 88}, 6, 2));
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    peer_fd_ = sockets[1];
    const control::HandshakeIdentity local{11, 33, 1, 1, 101, {0, 1}};
    const control::HandshakeIdentity remote{22, 66, 2, 1, 102, {0, 1}};
    ASSERT_TRUE(withdrawal_.Open(
        sockets[0], {local, remote, 77, 88, &sender_credit_, resources_->credits, source_.peer, 10000, true}, 0));
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_EQ(domain_.Inspect().credits.withdrawal_bindings, 1u);
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    ExpectNoDeletion();
    EXPECT_FALSE(resources_->receive->frozen());
    ASSERT_TRUE(withdrawal_.Close());
    Close();
}

TEST_F(RawDomainTest, PartialInitialPostPublishesNoResourcesAndKeepsRealPrefix) {
    accepted_prefix_ = 3;
    post_status_ = URMA_EAGAIN;
    const auto opened = domain_.Open(&context_, capabilities_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().accepted, 3u);
    EXPECT_EQ(domain_.Inspect().stage, RawDomainStage::kOpenFailed);
    EXPECT_EQ(domain_.Inspect().initial_accepted, 3u);
    EXPECT_EQ(domain_.Inspect().posted, 3u);
    EXPECT_EQ(domain_.Inspect().credits.physical_posted, 3u);
    EXPECT_TRUE(domain_.Inspect().initial_accepted_known);
    EXPECT_FALSE(domain_.resources());
    EXPECT_FALSE(domain_.Open(&context_, capabilities_, config_));
    EXPECT_EQ(post_calls_, 1u);
    ExpectNoDeletion();
    Close();
    EXPECT_EQ(domain_.Inspect().retired_rx, 3u);
}

TEST_F(RawDomainTest, UnknownInitialAcceptanceRetainsWholeOfferedSetUntilJfrDeleted) {
    accepted_prefix_ = 3;
    post_status_ = static_cast<urma_status_t>(EIO);
    unknown_acceptance_ = true;
    const auto opened = domain_.Open(&context_, capabilities_, config_);
    ASSERT_FALSE(opened);
    EXPECT_FALSE(opened.error().accepted);
    EXPECT_FALSE(domain_.Inspect().initial_accepted_known);
    EXPECT_EQ(domain_.Inspect().initial_accepted, 0u);
    EXPECT_EQ(domain_.Inspect().credits.physical_posted, 0u);
    EXPECT_EQ(domain_.Inspect().posted, 8u);
    EXPECT_FALSE(domain_.resources());
    ExpectNoDeletion();
    Close();
    EXPECT_EQ(domain_.Inspect().retired_rx, 8u);
}

TEST_F(RawDomainTest, LaterPartialInitialPostPreservesEarlierAcceptedBatch) {
    config_.queues.rx_depth = config_.queues.rx_cq_depth = 512;
    config_.control_slots = 64;
    fail_post_at_ = 2;
    accepted_prefix_ = 3;
    post_status_ = URMA_EAGAIN;
    ASSERT_FALSE(domain_.Open(&context_, capabilities_, config_));
    EXPECT_EQ(post_calls_, 2u);
    // 前一批 256 和第二批的真实前缀 3 都属于 DMA/物理 Credit 责任，不能仅记录末批。
    EXPECT_EQ(domain_.Inspect().initial_accepted, 259u);
    EXPECT_EQ(domain_.Inspect().posted, 259u);
    EXPECT_EQ(domain_.Inspect().credits.physical_posted, 259u);
    EXPECT_TRUE(domain_.Inspect().initial_accepted_known);
    EXPECT_FALSE(domain_.resources());
    Close();
    EXPECT_EQ(domain_.Inspect().retired_rx, 259u);
}

TEST_F(RawDomainTest, LaterUnknownInitialPostDoesNotEraseKnownPrefixOrPublishSpeculativeCredit) {
    config_.queues.rx_depth = config_.queues.rx_cq_depth = 512;
    config_.control_slots = 64;
    fail_post_at_ = 2;
    accepted_prefix_ = 3;
    post_status_ = static_cast<urma_status_t>(EIO);
    unknown_acceptance_ = true;
    ASSERT_FALSE(domain_.Open(&context_, capabilities_, config_));
    EXPECT_EQ(post_calls_, 2u);
    EXPECT_EQ(domain_.Inspect().initial_accepted, 256u);
    EXPECT_EQ(domain_.Inspect().posted, 512u);
    EXPECT_EQ(domain_.Inspect().credits.physical_posted, 256u);
    EXPECT_FALSE(domain_.Inspect().initial_accepted_known);
    EXPECT_FALSE(domain_.resources());
    Close();
    EXPECT_EQ(domain_.Inspect().retired_rx, 512u);
}

TEST_F(RawDomainTest, RegisterFailureKeepsQueuesForExplicitClose) {
    fail_register_ = true;
    const auto opened = domain_.Open(&context_, capabilities_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(domain_.Inspect().stage, RawDomainStage::kOpenFailed);
    EXPECT_FALSE(domain_.resources());
    EXPECT_EQ(post_calls_, 0u);
    ExpectNoDeletion();
    Close();
    EXPECT_EQ(jetty_delete_calls_, 2u);
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 0u);
}

TEST_F(RawDomainTest, QueueCreationAndRollbackFailurePreserveRetryableResources) {
    // 第二个 CQ 创建失败，已创建的 TX CQ 删除也失败；必须保留两层错误与句柄。
    fail_jfc_at_ = 2;
    delete_jfc_status_ = URMA_EAGAIN;
    const auto opened = domain_.Open(&context_, capabilities_, config_);
    ASSERT_FALSE(opened);
    ASSERT_TRUE(opened.error().queue_error);
    EXPECT_TRUE(opened.error().queue_error->cleanup_error);
    EXPECT_EQ(jfc_delete_calls_, 1u);
    EXPECT_EQ(register_calls_, 0u);
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(domain_.Inspect().stage, RawDomainStage::kClosing);
    delete_jfc_status_ = URMA_SUCCESS;
    Close();
    EXPECT_EQ(deleted_, (std::vector{Deleted::kJfc}));
}

TEST_F(RawDomainTest, JettyDeleteFailureDoesNotUnregisterOrDeleteJfr) {
    Open();
    delete_jetty_status_ = URMA_EAGAIN;
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(jfr_delete_calls_, 0u);
    EXPECT_EQ(unregister_calls_, 0u);
    EXPECT_EQ(jfc_delete_calls_, 0u);
    delete_jetty_status_ = URMA_SUCCESS;
    Close();
    EXPECT_EQ(jetty_delete_calls_, 3u);
}

TEST_F(RawDomainTest, JfrDeleteRetryDoesNotRepeatJettyDeletionOrFreeMemoryEarly) {
    Open();
    delete_jfr_status_ = URMA_EAGAIN;
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(jetty_delete_calls_, 2u);
    EXPECT_EQ(unregister_calls_, 0u);
    EXPECT_EQ(domain_.Inspect().posted, 8u);
    delete_jfr_status_ = URMA_SUCCESS;
    Close();
    EXPECT_EQ(jetty_delete_calls_, 2u);
    EXPECT_EQ(jfr_delete_calls_, 2u);
    EXPECT_EQ(unregister_calls_, 1u);
}

TEST_F(RawDomainTest, MemoryUnregisterRetryKeepsCompletionQueues) {
    Open();
    unregister_status_ = URMA_EAGAIN;
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 1u);
    EXPECT_EQ(jfc_delete_calls_, 0u);
    unregister_status_ = URMA_SUCCESS;
    Close();
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 2u);
    EXPECT_EQ(jfc_delete_calls_, 2u);
}

TEST_F(RawDomainTest, CompletionQueueDeleteRetryDoesNotRepeatDmaTeardown) {
    Open();
    delete_jfc_status_ = URMA_EAGAIN;
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(domain_.Inspect().close_stage, raw::ReceiveDomainCloseStage::kDeletingCompletionQueues);
    EXPECT_EQ(unregister_calls_, 1u);
    delete_jfc_status_ = URMA_SUCCESS;
    Close();
    EXPECT_EQ(jfr_delete_calls_, 1u);
    EXPECT_EQ(unregister_calls_, 1u);
    EXPECT_EQ(jfc_delete_calls_, 3u);
}

TEST_F(RawDomainTest, LateRxIsConsumedWithoutApplicationCallbackBeforeDeletion) {
    Open();
    QueueCompletion();
    ASSERT_TRUE(domain_.BeginClose());
    const auto draining = domain_.AdvanceClose(Proof());
    ASSERT_TRUE(draining);
    EXPECT_EQ(draining->stage, RawDomainStage::kClosing);
    EXPECT_EQ(draining->posted, 7u);
    EXPECT_EQ(draining->held, 0u);
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(draining->credits.unattributed, 1u);
    ExpectNoDeletion();
    // 消费到 CQE 后必须重新确认事件屏障；下一次推进才允许 DeleteJfr。
    Close();
    EXPECT_EQ(domain_.Inspect().retired_rx, 7u);
    EXPECT_EQ(domain_.Inspect().credits.unattributed, 0u);
}

TEST_F(RawDomainTest, FailedCqPollPermanentlyRetainsDmaDependencies) {
    Open();
    poll_error_ = true;
    ASSERT_TRUE(domain_.BeginClose());
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_EQ(domain_.Inspect().stage, RawDomainStage::kRetained);
    poll_error_ = false;
    // 后来空 CQ 不能恢复可靠终结证据，也不能解锁删除。
    EXPECT_FALSE(domain_.AdvanceClose(Proof()));
    EXPECT_FALSE(domain_.BeginClose());
    ExpectNoDeletion();
}

TEST_F(RawDomainTest, RuntimeShardUsesBindingAndDetachesBeforeOwnedClose) {
    Open();
    ASSERT_TRUE(progress_.Open({4, &context_}));
    const auto binding = domain_.Bind(progress_);
    ASSERT_TRUE(binding);
    const RawContextBinding context{.context = &context_, .queues = std::span(&*binding, 1), .connections = &progress_};
    ASSERT_TRUE(shard_.Open(std::span(&context, 1)));
    ASSERT_TRUE(shard_.Tick());
    EXPECT_GT(rx_polls_, 0u);
    EXPECT_GT(tx_polls_, 0u);
    ASSERT_TRUE(domain_.BeginClose());
    auto proof = Proof();
    proof.progress_detached = false;
    EXPECT_FALSE(domain_.AdvanceClose(proof));
    ExpectNoDeletion();
    ASSERT_TRUE(shard_.Close());
    Close();
    EXPECT_TRUE(progress_.Progress(ProgressKind::kRefill, 8, 1));
    EXPECT_EQ(context_delete_calls_, 0u);
}

} // namespace
} // namespace runtime
} // namespace kbsocket
