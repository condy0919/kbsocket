// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/tx_lane_snapshot.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <optional>
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace control {
namespace {
class TxLaneSnapshotTest : public ::testing::Test {
protected:
    static inline TxLaneSnapshotTest* self;
    urma_context_t context_{};
    urma_context_t other_context_{};
    urma_device_cap_t cap_{};
    std::array<urma_jfc_t, 8> cqs_{};
    std::array<urma_jfr_t, 4> jfrs_{};
    std::array<urma_jetty_t, 256> jetties_{};
    std::size_t cqs_created_ = 0, jfrs_created_ = 0, jetties_created_ = 0;
    std::optional<std::uint32_t> next_id_;
    bool fail_create_ = false;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    raw::JettyPool pool_;
    raw::JettyPool other_pool_;

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
            jetty->jetty_id.eid.raw[1] = 12;
            jetty->jetty_id.uasid = 7;
            jetty->jetty_id.id = self->next_id_.value_or(100 + self->jetties_created_);
            self->next_id_.reset();
            return jetty;
        };
        functions.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        functions.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        functions.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        functions.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        functions.flush_jetty = [](auto*, auto, auto*) { return 0; };
        override_.emplace(functions);
    }
    void TearDown() override {
        EXPECT_TRUE(pool_.Close());
        EXPECT_TRUE(other_pool_.Close());
        override_.reset();
        self = nullptr;
    }
    std::expected<void, raw::JettyPoolError> Open(raw::JettyPool& pool, urma_context_t& context,
                                                  std::uint32_t count = 2) {
        raw::JettyPoolConfig config;
        config.jetty_count = count;
        config.tx_depth = 1;
        config.rx_depth = 1;
        return pool.Open(&context, cap_, config);
    }
    HandshakeHello Hello() {
        HandshakeHello hello;
        const auto snapshot = CaptureTxLanes(pool_);
        EXPECT_TRUE(snapshot);
        if (!snapshot) {
            return hello;
        }
        hello.tx_lanes = *snapshot;
        const auto passive = pool_.rx_target_jetty_id();
        EXPECT_TRUE(passive);
        if (passive) {
            hello.jetty_id = passive->id;
            std::memcpy(hello.eid.data(), passive->eid.raw, hello.eid.size());
        }
        return hello;
    }
    void Drain(raw::JettyLane lane) {
        ASSERT_TRUE(pool_.BeginDrain(lane));
        ASSERT_TRUE(pool_.ObserveFlushDone(lane));
        std::array<urma_cr_t, 1> completions{};
        const auto flushed = pool_.FlushSend(lane, completions);
        ASSERT_TRUE(flushed);
        ASSERT_EQ(*flushed, 0);
    }
    static raw::JettyLaneQuiescence Quiesced() {
        return {.events_acked = true, .queue_users_stopped = true, .completions_drained = true};
    }
};

TEST_F(TxLaneSnapshotTest, CapturesAllActiveLanesAndExcludesPassiveTarget) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    ASSERT_EQ(hello.tx_lanes.count, 2u);
    EXPECT_EQ(hello.tx_lanes.pool_epoch, pool_.lane(0)->epoch);
    // 远端来源使用实际主动 Jetty 身份，不能把供导入的被动端点当作发送来源。
    for (std::uint32_t i = 0; i < hello.tx_lanes.count; ++i) {
        const auto& lane = hello.tx_lanes.lanes[i];
        EXPECT_EQ(lane.jetty_id, pool_.lane_jetty(i)->jetty_id.id);
        EXPECT_NE(lane.jetty_id, hello.jetty_id);
        EXPECT_EQ(lane.uasid, 7u);
        EXPECT_EQ(lane.eid[0], std::byte{11});
        EXPECT_EQ(lane.eid[1], std::byte{12});
        EXPECT_EQ(lane.generation, pool_.lane(i)->generation);
    }
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, hello));
}

TEST_F(TxLaneSnapshotTest, RequiresOpenPoolAndBoundsCompleteSnapshot) {
    EXPECT_FALSE(CaptureTxLanes(pool_));
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, {}));
    ASSERT_TRUE(Open(pool_, context_, kHandshakeMaxTxLanes));
    const auto maximum = CaptureTxLanes(pool_);
    ASSERT_TRUE(maximum);
    EXPECT_EQ(maximum->count, kHandshakeMaxTxLanes);
    ASSERT_TRUE(pool_.Close());
    EXPECT_FALSE(CaptureTxLanes(pool_));
    // 不能悄悄截断第 65 条 Lane，否则发送轮转会使用未经远端登记的来源。
    ASSERT_TRUE(Open(pool_, context_, kHandshakeMaxTxLanes + 1));
    const auto overflow = CaptureTxLanes(pool_);
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error().code, HandshakeErrorCode::kInvalidArgument);
}

TEST_F(TxLaneSnapshotTest, RejectsGeometryAndEveryAdvertisedIdentityMismatch) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    auto altered = hello;
    altered.tx_lanes.count = 0;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    --altered.tx_lanes.count;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    ++altered.tx_lanes.count;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    std::swap(altered.tx_lanes.lanes[0], altered.tx_lanes.lanes[1]);
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    ++altered.tx_lanes.pool_epoch;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    ++altered.tx_lanes.lanes[0].generation;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    ++altered.tx_lanes.lanes[0].jetty_id;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    ++altered.tx_lanes.lanes[0].uasid;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    altered.tx_lanes.lanes[0].eid[15] = std::byte{1};
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    ++altered.jetty_id;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    altered.eid[15] = std::byte{1};
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    altered = hello;
    altered.capabilities = kHandshakeCapabilityTcp;
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, altered));
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, hello));
}

TEST_F(TxLaneSnapshotTest, RejectsForeignPoolOrProviderObjectContext) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    ASSERT_TRUE(Open(other_pool_, other_context_));
    EXPECT_FALSE(ValidateLocalTxLanes(other_pool_, hello));
    // 物理数字相同也不能跨 context 借用 provider 对象。
    auto* active = pool_.lane_jetty(0);
    active->urma_ctx = &other_context_;
    EXPECT_FALSE(CaptureTxLanes(pool_));
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
    active->urma_ctx = &context_;
    auto* passive = pool_.rx_target_jetty();
    passive->urma_ctx = &other_context_;
    EXPECT_FALSE(CaptureTxLanes(pool_));
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
    passive->urma_ctx = &context_;
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, hello));
}

TEST_F(TxLaneSnapshotTest, RejectsAmbiguousProviderPhysicalIdentities) {
    ASSERT_TRUE(Open(pool_, context_));
    auto* active = pool_.lane_jetty(0);
    const auto original_id = active->jetty_id.id;
    // provider 若错误地交出重复身份，不能发布不可唯一匹配 RX 来源的快照。
    active->jetty_id.id = pool_.lane_jetty(1)->jetty_id.id;
    EXPECT_FALSE(CaptureTxLanes(pool_));
    active->jetty_id.id = pool_.rx_target_jetty()->jetty_id.id;
    EXPECT_FALSE(CaptureTxLanes(pool_));
    active->jetty_id.id = original_id;
    EXPECT_TRUE(CaptureTxLanes(pool_));
}

TEST_F(TxLaneSnapshotTest, IsolatedLaneKeepsIdentityButReplacementRequiresNewSnapshot) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    const auto lane = pool_.lane(0);
    ASSERT_TRUE(lane);
    ASSERT_TRUE(pool_.MarkFaulted(*lane));
    EXPECT_FALSE(CaptureTxLanes(pool_));
    // 同代故障只禁止池选择该 Lane，健康 Lane 不应仅因此失去已交换的来源绑定。
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, hello));
    Drain(*lane);
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, hello));
    next_id_ = hello.tx_lanes.lanes[0].jetty_id;
    const auto replacement = pool_.ReplaceDrainedLane(*lane, Quiesced());
    ASSERT_TRUE(replacement);
    // 即使 provider 复用同一 EID / UASID / ID，旧 generation 也不能授权新队列发送。
    EXPECT_EQ(pool_.lane_jetty(0)->jetty_id.id, hello.tx_lanes.lanes[0].jetty_id);
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
    const auto updated = Hello();
    EXPECT_EQ(updated.tx_lanes.lanes[0].generation, lane->generation + 1);
    EXPECT_EQ(updated.tx_lanes.lanes[1], hello.tx_lanes.lanes[1]);
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, updated));
}

TEST_F(TxLaneSnapshotTest, FailedReplacementNeverReusesOldSnapshot) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    const auto lane = pool_.lane(0);
    ASSERT_TRUE(lane);
    Drain(*lane);
    // 删除完成但创建失败期间，generation 尚未增加也不能继续使用旧来源。
    fail_create_ = true;
    ASSERT_FALSE(pool_.ReplaceDrainedLane(*lane, Quiesced()));
    ASSERT_EQ(pool_.lane_state(*lane), raw::JettyLaneState::kReplacing);
    EXPECT_FALSE(CaptureTxLanes(pool_));
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
    ASSERT_TRUE(pool_.ReplaceDrainedLane(*lane, Quiesced()));
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, Hello()));
}

TEST_F(TxLaneSnapshotTest, PoolReopenInvalidatesOldEpoch) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    ASSERT_TRUE(pool_.Close());
    ASSERT_TRUE(Open(pool_, context_));
    // 模拟池重建后硬件复用了全部 ID；仍由 pool_epoch 区分旧公告和新对象。
    pool_.rx_target_jetty()->jetty_id.id = hello.jetty_id;
    for (std::uint32_t i = 0; i < hello.tx_lanes.count; ++i) {
        pool_.lane_jetty(i)->jetty_id.id = hello.tx_lanes.lanes[i].jetty_id;
    }
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
    const auto updated = Hello();
    EXPECT_EQ(updated.tx_lanes.pool_epoch, hello.tx_lanes.pool_epoch + 1);
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, updated));
}

TEST_F(TxLaneSnapshotTest, OnlineSnapshotKeepsUnchangedFaultedLaneWhileAnotherIsRebuilt) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto hello = Hello();
    const auto first = pool_.lane(0);
    const auto second = pool_.lane(1);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(pool_.MarkFaulted(*second));
    Drain(*first);
    ASSERT_TRUE(pool_.ReplaceDrainedLane(*first, Quiesced()));
    // 初始 HELLO 的严格 READY 门禁保留；在线公告仍可携带另一条已隔离但身份未变的 Lane。
    EXPECT_FALSE(CaptureTxLanes(pool_));
    const auto online = CaptureTxLanes(pool_, false);
    ASSERT_TRUE(online);
    EXPECT_EQ(online->lanes[1], hello.tx_lanes.lanes[1]);
    EXPECT_TRUE(ValidateTxLaneUpdate(hello.tx_lanes, *online));
    EXPECT_TRUE(ValidateLocalTxLanes(pool_, hello, *online));
    EXPECT_FALSE(ValidateLocalTxLanes(pool_, hello));
}

TEST_F(TxLaneSnapshotTest, UpdateRequiresMonotoneSlotGenerationAndStablePoolGeometry) {
    ASSERT_TRUE(Open(pool_, context_));
    const auto before = Hello().tx_lanes;
    auto after = before;
    EXPECT_FALSE(ValidateTxLaneUpdate(before, after));
    ++after.lanes[0].jetty_id;
    EXPECT_FALSE(ValidateTxLaneUpdate(before, after));
    after = before;
    ++after.lanes[0].generation;
    EXPECT_TRUE(ValidateTxLaneUpdate(before, after)); // 硬件复用 ID 时仍需新的软件代次。
    EXPECT_FALSE(ValidateTxLaneUpdate(after, before));
    ++after.pool_epoch;
    EXPECT_FALSE(ValidateTxLaneUpdate(before, after));
    after = before;
    --after.count;
    EXPECT_FALSE(ValidateTxLaneUpdate(before, after));
}

} // namespace
} // namespace control
} // namespace kbsocket
