// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/rndv_arena.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace control {
namespace {
class RndvArenaTest : public ::testing::Test {
protected:
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions;
        functions.register_seg = +[](urma_context_t* context, urma_seg_cfg_t* config) -> urma_target_seg_t* {
            auto& segment = active_->segments_[active_->registrations_++];
            segment.urma_ctx = context;
            segment.seg.ubva.eid = context->eid;
            segment.seg.ubva.uasid = context->uasid;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            segment.seg.attr.bs.access = config->flag.bs.access;
            segment.seg.token_id = 700 + active_->registrations_;
            return &segment;
        };
        functions.unregister_seg = +[](urma_target_seg_t*) -> urma_status_t {
            ++active_->unregistrations_;
            return URMA_SUCCESS;
        };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        for (std::size_t i = 0; i < contexts_.size(); ++i) {
            contexts_[i].eid.raw[15] = static_cast<std::uint8_t>(30 + i);
            contexts_[i].uasid = static_cast<std::uint32_t>(500 + i);
        }
    }
    void TearDown() override {
        EXPECT_TRUE(slab_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    std::expected<void, memory::MemoryError> Open(std::uint32_t access = URMA_ACCESS_READ | URMA_ACCESS_WRITE) {
        std::array<urma_context_t*, 2> contexts{&contexts_[0], &contexts_[1]};
        return slab_.Open(contexts, {.buffer_count = 4, .buffer_size = 16384, .max_references = 16, .access = access},
                          budget_);
    }
    static inline RndvArenaTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    std::array<urma_context_t, 3> contexts_{};
    std::array<urma_target_seg_t, 8> segments_{};
    memory::MemoryBudget budget_{1u << 20};
    memory::SlabAllocator slab_;
    std::uint32_t registrations_ = 0;
    std::uint32_t unregistrations_ = 0;
};

TEST_F(RndvArenaTest, ExportsCanonicalDirectoryFromRealRegistrationAndHostGeneration) {
    ASSERT_TRUE(Open());
    const connection::GrantArena identity{37, 9001};
    const auto descriptor = ExportRndvArena(slab_, &contexts_[0], identity);
    ASSERT_TRUE(descriptor);
    EXPECT_EQ(descriptor->id, identity.id);
    EXPECT_EQ(descriptor->generation, identity.generation);
    EXPECT_EQ(descriptor->address, segments_[0].seg.ubva.va);
    EXPECT_EQ(descriptor->length, segments_[0].seg.len);
    EXPECT_EQ(descriptor->max_grant_bytes, 16384u);
    EXPECT_EQ(descriptor->eid[15], std::byte{30});
    EXPECT_EQ(descriptor->uasid, 500u);
    EXPECT_EQ(descriptor->token_id, 701u);
    EXPECT_EQ(descriptor->token, 0u);
    EXPECT_EQ(descriptor->access, kRndvAccessRead | kRndvAccessWrite);
    EXPECT_EQ(descriptor->token_policy, kRndvTokenNone);
    // 公布的整个注册区间不代表某个消息已取得访问权；导出不消费 Slab 容量。
    EXPECT_EQ(slab_.stats().available, 4u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_TRUE(ValidateRndvArena(*descriptor));
}

TEST_F(RndvArenaTest, SharedBackingExportsTheChosenContextRatherThanTheFirstRegistration) {
    ASSERT_TRUE(Open());
    const auto first = ExportRndvArena(slab_, &contexts_[0], {37, 9001});
    const auto second = ExportRndvArena(slab_, &contexts_[1], {37, 9001});
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first->address, second->address);
    EXPECT_EQ(first->length, second->length);
    EXPECT_NE(first->eid, second->eid);
    EXPECT_NE(first->uasid, second->uasid);
    EXPECT_NE(first->token_id, second->token_id);
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[2], {37, 9001}));
}

TEST_F(RndvArenaTest, RejectsReadOnlyRegistrationAndInvalidArenaIdentity) {
    ASSERT_TRUE(Open(URMA_ACCESS_READ));
    // READ-only 注册可供别的用途，但不得被协商成 WRITE RNDV 接收目标。
    EXPECT_TRUE(slab_.ExportSegment(&contexts_[0]));
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {0, 9001}));
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 0}));
}

TEST_F(RndvArenaTest, RejectsInactiveHostStoppedAndProviderMismatch) {
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
    ASSERT_TRUE(slab_.OpenHost({.buffer_count = 4, .buffer_size = 16384, .max_references = 16}, budget_));
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
    ASSERT_TRUE(slab_.Close());
    ASSERT_TRUE(Open());
    const auto valid = segments_[0];
    // provider 返回错段时不能通过给它贴 Arena id 来制造可导入目标。
    segments_[0].seg.ubva.va += 4096;
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
    segments_[0] = valid;
    EXPECT_TRUE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
    slab_.StopAccepting();
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
}

TEST_F(RndvArenaTest, ExportDoesNotSubstituteForGrantLifetimeOrPreventOrdinaryClose) {
    ASSERT_TRUE(Open());
    const auto descriptor = ExportRndvArena(slab_, &contexts_[0], {37, 9001});
    ASSERT_TRUE(descriptor);
    auto lease = slab_.Acquire(16384);
    ASSERT_TRUE(lease);
    auto grant = slab_.Reserve(*lease, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(slab_.Commit(*grant));
    ASSERT_TRUE(slab_.ReleaseCpu(*lease));
    // 快照存在与否不改变真实访问责任，实际 Grant 未结束时 backing 必须保留。
    EXPECT_FALSE(slab_.Close());
    EXPECT_EQ(unregistrations_, 0u);
    EXPECT_FALSE(ExportRndvArena(slab_, &contexts_[0], {37, 9001}));
    ASSERT_TRUE(slab_.EndGrant(*grant));
    ASSERT_TRUE(slab_.Close());
    EXPECT_EQ(unregistrations_, 2u);
}
} // namespace
} // namespace control
} // namespace kbsocket
