// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;

class RxBufferPoolTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));
    MOCK_METHOD(urma_status_t, Post, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));

    static UrmaFunctions MockedFunctions() {
        UrmaFunctions f{};
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        f.unregister_seg = [](auto* s) { return active_->Unregister(s); };
        f.post_jfr_wr = [](auto* q, auto* wr, auto** bad) { return active_->Post(q, wr, bad); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        return f;
    }

    void SetUp() override {
        active_ = this;
        jetty_.jetty_id.id = 19;
        jfr_.jfr_id.id = 23;
        EXPECT_CALL(*this, Register(&ctx_, _)).WillRepeatedly([&](auto*, auto* cfg) {
            const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
            EXPECT_EQ(cfg->va % page, 0u);
            EXPECT_EQ(cfg->len % page, 0u);
            EXPECT_GE(cfg->len, 5 * RxBufferPool::kBufferSize);
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            return &segment_;
        });
        EXPECT_CALL(*this, Unregister(&segment_)).WillRepeatedly(Return(URMA_SUCCESS));
        OpenPool(3);
        ASSERT_TRUE(buffers_.Open(pool_, 5));
    }

    void OpenPool(std::uint32_t depth) {
        EXPECT_CALL(*this, CreateJfc(_, _)).WillOnce(Return(&tx_)).WillOnce(Return(&rx_));
        EXPECT_CALL(*this, CreateJfr(_, _)).WillOnce(Return(&jfr_));
        EXPECT_CALL(*this, CreateJetty(_, _)).WillOnce(Return(&jetty_));
        EXPECT_CALL(*this, DeleteJetty(&jetty_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfr(&jfr_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&rx_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(*this, DeleteJfc(&tx_)).WillOnce(Return(URMA_SUCCESS));
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 1;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 1024;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        JettyPoolConfig cfg;
        cfg.tx_depth = 3;
        cfg.rx_depth = depth;
        cfg.rx_cq_depth = depth;
        ASSERT_TRUE(pool_.Open(&ctx_, cap, cfg));
    }

    void TearDown() override {
        EXPECT_EQ(buffers_.posted(), 0u);
        EXPECT_EQ(buffers_.held(), 0u);
        EXPECT_TRUE(buffers_.Close());
        EXPECT_TRUE(pool_.Close());
        active_ = nullptr;
    }

    struct Posted {
        std::uint64_t id;
        std::span<std::byte> memory;
    };
    void Capture(urma_jfr_wr_t* wr) {
        posted_.clear();
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->src.num_sge, 1u);
            EXPECT_EQ(wr->src.sge[0].len, RxBufferPool::kBufferSize);
            EXPECT_EQ(wr->src.sge[0].tseg, &segment_);
            auto* address = reinterpret_cast<std::byte*>(wr->src.sge[0].addr);
            posted_.push_back({wr->user_ctx, {address, wr->src.sge[0].len}});
        }
    }
    void Refill(std::size_t limit = 3) {
        EXPECT_CALL(*this, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
            Capture(wr);
            return URMA_SUCCESS;
        });
        auto result = buffers_.Refill(limit);
        ASSERT_TRUE(result);
        EXPECT_EQ(*result, posted_.size());
    }
    urma_cr_t Completion(std::uint64_t id) {
        urma_cr_t cr{};
        cr.flag.bs.s_r = 1;
        cr.flag.bs.jetty = 1;
        cr.local_id = jetty_.jetty_id.id;
        cr.user_ctx = id;
        cr.completion_len = 17;
        cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        cr.imm_data = 0x123456789abcdef0;
        return cr;
    }
    std::vector<RxBufferEvent> Complete(std::span<const urma_cr_t> crs) {
        std::vector<RxBufferEvent> events(crs.size());
        EXPECT_CALL(*this, Poll(&rx_, static_cast<int>(crs.size()), _)).WillOnce([&](auto*, int, auto* out) {
            std::copy(crs.begin(), crs.end(), out);
            return static_cast<int>(crs.size());
        });
        auto result = buffers_.Poll(events);
        EXPECT_TRUE(result);
        if (result) {
            EXPECT_EQ(*result, crs.size());
        }
        return events;
    }
    void Retire(std::span<const Posted> posted) {
        // 明确提供逐 WR 终结证据再归还，不通过重置对象掩盖未决 DMA。
        for (const auto& wr : posted) {
            const std::array crs{Completion(wr.id)};
            auto events = Complete(crs);
            ASSERT_TRUE(events[0].lease);
            EXPECT_TRUE(buffers_.Release(*events[0].lease));
        }
    }

protected:
    static inline RxBufferPoolTest* active_;
    test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    urma_context_t ctx_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{};
    urma_target_seg_t segment_{};
    JettyPool pool_;
    RxBufferPool buffers_;
    std::vector<Posted> posted_;
};

TEST_F(RxBufferPoolTest, HeldBufferIsNotRepostedAndMetadataSurvives) {
    Refill();
    const auto first_batch = posted_;
    EXPECT_EQ(buffers_.posted(), 3u);
    EXPECT_EQ(*buffers_.Refill(), 0u); // 即使有 spare buffer，也不能超过 JFR 深度。
    first_batch[2].memory[0] = std::byte{42};
    const std::array crs{Completion(first_batch[2].id), Completion(first_batch[0].id)};
    auto events = Complete(crs); // 故意乱序完成，映射必须依据身份而非投递顺序。
    ASSERT_TRUE(events[0].lease);
    ASSERT_TRUE(events[1].lease);
    EXPECT_EQ(events[0].payload.data(), first_batch[2].memory.data());
    EXPECT_EQ(events[0].payload.size(), 17u);
    EXPECT_EQ(events[0].payload[0], std::byte{42});
    EXPECT_EQ(events[0].completion.imm_data, 0x123456789abcdef0u);
    EXPECT_EQ(buffers_.held(), 2u);
    EXPECT_FALSE(pool_.Close());
    EXPECT_FALSE(buffers_.Close());
    // 上层继续持有两块数据时，补入的两块必须来自 spare 区域。
    Refill();
    ASSERT_EQ(posted_.size(), 2u);
    for (const auto& wr : posted_) {
        EXPECT_NE(wr.memory.data(), events[0].payload.data());
        EXPECT_NE(wr.memory.data(), events[1].payload.data());
    }
    Retire(posted_);
    Retire(std::span(first_batch).subspan(1, 1));
    for (const auto& event : events) {
        EXPECT_TRUE(buffers_.Release(*event.lease));
        EXPECT_FALSE(buffers_.Release(*event.lease));
    }
    EXPECT_EQ(buffers_.available(), 5u);
}

TEST_F(RxBufferPoolTest, OldLeaseCannotReleaseReusedSlotOrReopenedPool) {
    Refill(1);
    auto events = Complete(std::array{Completion(posted_[0].id)});
    const auto old = *events[0].lease;
    ASSERT_TRUE(buffers_.Release(old));
    Refill(1);
    EXPECT_NE(posted_[0].id, old.id);
    EXPECT_FALSE(buffers_.Release(old));
    Retire(posted_);
    ASSERT_TRUE(buffers_.Close());
    // 重新 Open 后仍不能复活旧身份，包括 buffer_count 变化的情况。
    ASSERT_TRUE(buffers_.Open(pool_, 7));
    Refill(1);
    events = Complete(std::array{Completion(posted_[0].id)});
    EXPECT_FALSE(buffers_.Release(old));
    auto foreign = *events[0].lease;
    foreign.pool = nullptr;
    EXPECT_FALSE(buffers_.Release(foreign));
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_TRUE(buffers_.Release(*events[0].lease));
}

TEST_F(RxBufferPoolTest, PartialPostRetainsAcceptedPrefixAndRetriesSuffix) {
    EXPECT_CALL(*this, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
        Capture(wr);
        *bad = wr->next;
        return URMA_EAGAIN;
    });
    auto result = buffers_.Refill();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().accepted, 1u);
    EXPECT_EQ(buffers_.posted(), 1u);
    EXPECT_EQ(buffers_.available(), 4u);
    EXPECT_FALSE(buffers_.stopped());
    const auto accepted = posted_[0];
    Refill();
    EXPECT_EQ(posted_.size(), 2u);
    for (const auto& wr : posted_) {
        EXPECT_NE(wr.memory.data(), accepted.memory.data());
    }
    Retire(posted_);
    Retire(std::span(&accepted, 1));
}

TEST_F(RxBufferPoolTest, UnknownPostBoundaryRetainsEntireBatch) {
    EXPECT_CALL(*this, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto**) {
        Capture(wr);
        return URMA_FAIL; // 没有 bad_wr，不能把整批当作未投递而回收。
    });
    auto result = buffers_.Refill();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, RxBufferErrorCode::kProviderContract);
    EXPECT_FALSE(result.error().accepted);
    EXPECT_EQ(buffers_.posted(), 3u);
    EXPECT_TRUE(buffers_.stopped());
    EXPECT_FALSE(buffers_.Refill());
    EXPECT_FALSE(buffers_.Close());
    EXPECT_FALSE(pool_.Close());
    Retire(posted_);
}

TEST_F(RxBufferPoolTest, SuccessWithBadWrIsAlsoAmbiguous) {
    EXPECT_CALL(*this, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
        Capture(wr);
        *bad = wr;
        return URMA_SUCCESS;
    });
    auto result = buffers_.Refill();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, RxBufferErrorCode::kProviderContract);
    EXPECT_EQ(buffers_.posted(), 3u);
    Retire(posted_);
}

TEST_F(RxBufferPoolTest, RejectsForeignAndFakeCompletionsWithoutLosingLaterCqes) {
    Refill();
    auto wrong_direction = Completion(posted_[0].id);
    wrong_direction.flag.bs.s_r = 0;
    auto wrong_queue = Completion(posted_[0].id);
    wrong_queue.local_id = 99;
    auto fake = Completion(posted_[0].id);
    fake.status = URMA_CR_WR_FLUSH_ERR_DONE;
    const std::array crs{wrong_direction, wrong_queue, fake, Completion(posted_[0].id), Completion(posted_[0].id)};
    auto events = Complete(crs);
    EXPECT_FALSE(events[0].lease);
    EXPECT_FALSE(events[1].lease);
    EXPECT_FALSE(events[2].lease);
    EXPECT_EQ(events[2].completion.user_ctx, 0u);
    ASSERT_TRUE(events[3].lease);
    EXPECT_FALSE(events[4].lease); // 同批重复 CQE 不能产生第二个 lease。
    EXPECT_EQ(buffers_.posted(), 2u);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_TRUE(buffers_.stopped());
    EXPECT_TRUE(buffers_.Release(*events[3].lease));
    Retire(std::span(posted_).subspan(1));
}

TEST_F(RxBufferPoolTest, StaleCompletionDoesNotRetireRepostedBuffer) {
    Refill(1);
    const auto old = Completion(posted_[0].id);
    Retire(posted_);
    Refill(1);
    auto events = Complete(std::array{old, Completion(posted_[0].id)});
    // 原槽位已经复用；迟到 CQE 必须拒绝，后面的真实完成仍要交出新 lease。
    EXPECT_FALSE(events[0].lease);
    EXPECT_EQ(events[0].error->code, RxBufferErrorCode::kInvalidLease);
    ASSERT_TRUE(events[1].lease);
    EXPECT_EQ(buffers_.posted(), 0u);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_TRUE(buffers_.Release(*events[1].lease));
}

TEST_F(RxBufferPoolTest, HeldBuffersExhaustCapacityUntilReleased) {
    // 先持有三块，再持有剩余两块：JFR 虽然已空，但不能重投上层未消费的数据。
    std::vector<RxBufferLease> leases;
    for (int round = 0; round < 2; ++round) {
        Refill();
        for (const auto& wr : posted_) {
            auto events = Complete(std::array{Completion(wr.id)});
            ASSERT_TRUE(events[0].lease);
            leases.push_back(*events[0].lease);
        }
    }
    EXPECT_EQ(buffers_.posted(), 0u);
    EXPECT_EQ(buffers_.held(), 5u);
    EXPECT_EQ(buffers_.available(), 0u);
    EXPECT_EQ(*buffers_.Refill(), 0u);
    EXPECT_FALSE(buffers_.Close());
    EXPECT_TRUE(buffers_.Release(leases[0]));
    Refill();
    EXPECT_EQ(posted_.size(), 1u);
    Retire(posted_);
    for (std::size_t i = 1; i < leases.size(); ++i) {
        EXPECT_TRUE(buffers_.Release(leases[i]));
    }
}

TEST_F(RxBufferPoolTest, KnownFatalPostFailureReleasesOnlyRejectedSuffix) {
    EXPECT_CALL(*this, Post(&jfr_, _, _)).WillOnce([&](auto*, auto* wr, auto** bad) {
        Capture(wr);
        *bad = wr->next->next;
        return URMA_EINVAL;
    });
    auto result = buffers_.Refill();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().accepted, 2u);
    EXPECT_EQ(buffers_.posted(), 2u);
    EXPECT_TRUE(buffers_.stopped());
    EXPECT_FALSE(buffers_.Refill());
    Retire(std::span(posted_).first(2));
}

TEST_F(RxBufferPoolTest, ErrorAndOversizedPayloadReturnLeaseButNoData) {
    Refill();
    auto error = Completion(posted_[0].id);
    error.status = URMA_CR_LOC_ACCESS_ERR;
    auto oversized = Completion(posted_[1].id);
    oversized.completion_len = RxBufferPool::kBufferSize + 1;
    auto direct_jfr = Completion(posted_[2].id);
    direct_jfr.flag.bs.jetty = 0;
    direct_jfr.local_id = jfr_.jfr_id.id;
    auto events = Complete(std::array{error, oversized, direct_jfr});
    EXPECT_EQ(events[0].error->code, RxBufferErrorCode::kReceiveFailed);
    EXPECT_TRUE(events[0].payload.empty());
    EXPECT_TRUE(events[1].payload.empty());
    EXPECT_TRUE(events[1].error);
    EXPECT_FALSE(events[2].error);
    for (const auto& event : events) {
        ASSERT_TRUE(event.lease);
        EXPECT_TRUE(buffers_.Release(*event.lease));
    }
}

TEST_F(RxBufferPoolTest, StopAndPollFailurePreserveOutstandingBuffers) {
    Refill();
    buffers_.Stop();
    EXPECT_FALSE(buffers_.Refill());
    EXPECT_CALL(*this, Poll(&rx_, 1, _)).WillOnce(Return(-7));
    std::array<RxBufferEvent, 1> event{};
    auto result = buffers_.Poll(event);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().provider_error, -7);
    EXPECT_EQ(buffers_.posted(), 3u);
    EXPECT_FALSE(buffers_.Close());
    Retire(posted_);
}

TEST_F(RxBufferPoolTest, RegisterFailureRollsBackAndUnregisterFailureRetainsBinding) {
    ASSERT_TRUE(buffers_.Close());
    EXPECT_CALL(*this, Register(&ctx_, _))
        .WillOnce([](auto*, auto*) {
            errno = 0;
            return nullptr;
        })
        .RetiresOnSaturation();
    auto opened = buffers_.Open(pool_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, RxBufferErrorCode::kRegisterFailed);
    EXPECT_EQ(opened.error().provider_error, EIO);
    EXPECT_EQ(buffers_.capacity(), 0u);
    ASSERT_TRUE(buffers_.Open(pool_, 5));
    RxBufferPool other;
    EXPECT_FALSE(other.Open(pool_)); // 一个共享 JFR 只能有一个内存池 owner。
    EXPECT_CALL(*this, Unregister(&segment_)).WillOnce(Return(URMA_EAGAIN)).RetiresOnSaturation();
    auto closed = buffers_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, RxBufferErrorCode::kUnregisterFailed);
    EXPECT_EQ(buffers_.capacity(), 5u);
    EXPECT_FALSE(buffers_.Refill());
    EXPECT_FALSE(pool_.Close());
    EXPECT_TRUE(buffers_.Close()); // 重试成功后才解绑并允许父队列销毁。
}

TEST_F(RxBufferPoolTest, FixedBatchLimitAndPollBudget) {
    ASSERT_TRUE(buffers_.Close());
    ASSERT_TRUE(pool_.Close());
    OpenPool(300);
    ASSERT_TRUE(buffers_.Open(pool_));
    EXPECT_FALSE(buffers_.Refill(0));
    EXPECT_FALSE(buffers_.Refill(257));
    Refill(RxBufferPool::kMaxBatch);
    EXPECT_EQ(posted_.size(), 256u);
    std::array<RxBufferEvent, 100> events{};
    EXPECT_CALL(*this, Poll(&rx_, 64, _)).WillOnce(Return(0));
    EXPECT_EQ(*buffers_.Poll(events), 0u);
    Retire(posted_);
}
} // namespace
} // namespace raw
} // namespace kbsocket
