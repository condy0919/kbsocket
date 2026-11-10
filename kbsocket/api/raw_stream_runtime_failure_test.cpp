// SPDX-License-Identifier: MulanPSL-2.0
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/api/buffer_lease.h"
#include "kbsocket/api/lease_send.h"
#include "kbsocket/api/stream.h"
#include "kbsocket/api/testing/raw_stream_runtime_fixture.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"

namespace kbsocket {
namespace api {
namespace {
using test_support::Now;
using test_support::RawStreamRuntimeRndvTest;

class RawStreamRuntimeFailureTest : public RawStreamRuntimeRndvTest {
protected:
    void SendLease() {
        ASSERT_TRUE(runtime_.OpenBuffers({930, 2}));
        kbs_buffer_handle source{};
        auto* buffers = runtime_.buffers();
        ASSERT_EQ(kbs_buffer_acquire(buffers, 16384, &source, nullptr), 0);
        kbs_buffer_view view{};
        ASSERT_EQ(kbs_buffer_writable(buffers, source, &view, nullptr), 0);
        const auto payload = Payload(view.size, 43);
        std::memcpy(view.data, payload.data(), payload.size());
        kbs_stream_io io{};
        ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, source, 0, view.size,
                                         &io, nullptr),
                  0);
        ASSERT_EQ(io.bytes, 16384u);
        // 应用立即归还 CPU 句柄；后续完成只能通过 Runtime 中真实 DMA 责任回收。
        ASSERT_EQ(kbs_buffer_release(buffers, source, nullptr), 0);
    }
    void Tick(unsigned count = 32) {
        for (unsigned i = 0; i < count; ++i) {
            ASSERT_TRUE(runtime_.Step());
        }
    }
    void ExpectNoDelivery(std::size_t side = 1) {
        const auto status = Status(side);
        ASSERT_TRUE(status);
        // rx_pending 包含未终结的 RNDV 接收责任；只有 Read 的结果表示应用可见性。
        std::array<std::byte, 1> output{};
        kbs_stream_io io{};
        EXPECT_NE(kbs_stream_read(runtime_.table(side), endpoints_[side].streams[0], output.data(), output.size(),
                                  Now(), &io, nullptr),
                  0);
        EXPECT_EQ(io.bytes, 0u);
    }
    void ExpectFailed(std::size_t side, int detail) {
        ASSERT_TRUE(Until([&] {
            const auto status = Status(side);
            return status && status->state == KBS_STREAM_FAILED;
        }));
        const auto status = Status(side);
        ASSERT_TRUE(status);
        EXPECT_EQ(status->failure.code, KBS_STREAM_TRANSPORT);
        EXPECT_EQ(status->failure.underlying, detail);
    }
};

TEST_F(RawStreamRuntimeFailureTest, CancelledReceiverAcceptsLateDoneWithoutRestoringApplicationDelivery) {
    ASSERT_NO_FATAL_FAILURE(Activate());
    endpoints_[0].defer_done_tx = true;
    endpoints_[1].defer_done_rx = true;
    ASSERT_NO_FATAL_FAILURE(SendLease());
    ASSERT_TRUE(Until([&] { return endpoints_[0].posted_done == 1 && !runtime_.paused(0) && !runtime_.paused(1); }));
    ASSERT_EQ(endpoints_[0].withheld_done_tx.size(), 1u);
    ASSERT_EQ(endpoints_[1].withheld_done_rx.size(), 1u);
    ASSERT_GT(runtime_.Inspect()->rndv_target.remote_grants, 0u);
    ASSERT_EQ(kbs_stream_cancel(runtime_.table(1), endpoints_[1].streams[0], nullptr), 0);
    EXPECT_EQ(kbs_stream_close(runtime_.table(1), endpoints_[1].streams[0], nullptr), KBS_STREAM_RETAINED);
    ASSERT_NO_FATAL_FAILURE(ExpectNoDelivery());

    // 已投递 DONE 的迟到 RX 有权终结目标 Grant；取消只禁用应用交付，不撤销已授权 DMA。
    ReleaseDoneRx(1);
    ASSERT_TRUE(Until([&] {
        const auto snapshot = runtime_.Inspect();
        return snapshot && snapshot->rndv_target.remote_grants == 0;
    }));
    ASSERT_NO_FATAL_FAILURE(ExpectNoDelivery());
    ASSERT_TRUE(Until([&] { return kbs_stream_close(runtime_.table(1), endpoints_[1].streams[0], nullptr) == 0; }));
    EXPECT_GT(runtime_.Inspect()->slab.local_dma, 0u);
    EXPECT_EQ(kbs_stream_close(runtime_.table(0), endpoints_[0].streams[0], nullptr), KBS_STREAM_RETAINED);

    // 对端已关闭也不能替代本地通知 CQE；释放真实完成后两个责任独立收敛。
    ReleaseDoneTx(0);
    ASSERT_NO_FATAL_FAILURE(WaitIdle());
    EXPECT_EQ(runtime_.Inspect()->slab.local_dma, 0u);
    EXPECT_EQ(kbs_stream_close(runtime_.table(0), endpoints_[0].streams[0], nullptr), 0);
}

TEST_F(RawStreamRuntimeFailureTest, RemoteReadCanFinishWhileLocalDoneCompletionStillPinsItsSource) {
    ASSERT_NO_FATAL_FAILURE(Activate());
    endpoints_[0].defer_done_tx = true;
    ASSERT_NO_FATAL_FAILURE(SendLease());
    const auto payload = Payload(16384, 43);
    ASSERT_NO_FATAL_FAILURE(ReadPayload(1, 0, payload));
    ASSERT_EQ(endpoints_[0].withheld_done_tx.size(), 1u);
    EXPECT_EQ(runtime_.Inspect()->rndv_target.remote_grants, 0u);
    EXPECT_GT(runtime_.Inspect()->slab.local_dma, 0u);
    EXPECT_EQ(kbs_stream_close(runtime_.table(0), endpoints_[0].streams[0], nullptr), KBS_STREAM_RETAINED);
    ReleaseDoneTx(0);
    ASSERT_NO_FATAL_FAILURE(WaitIdle());
    EXPECT_EQ(runtime_.Inspect()->slab.local_dma, 0u);
}

// 不可终结责任不能借助普通 TearDown 的静默证明消失。所有线程、Runtime、MR 与外部依赖
// 只在子进程内创建，并以 _exit 结束；主测试进程不析构任何仍被 DMA 引用的对象。
class RawStreamRuntimeRetainedDeathTest : public RawStreamRuntimeFailureTest {
protected:
    void SetUp() override {}
    void TearDown() override {}
    [[noreturn]] void ExitChild() {
        const auto* result = ::testing::UnitTest::GetInstance()->current_test_info()->result();
        for (int i = 0; i < result->total_part_count(); ++i) {
            const auto& part = result->GetTestPartResult(i);
            if (part.failed()) {
                std::fprintf(stderr, "%s:%d %s\n", part.file_name(), part.line_number(), part.message());
            }
        }
        ::_exit(HasFailure() ? 1 : 0);
    }
    void Start(bool sibling = false) {
        ASSERT_NO_FATAL_FAILURE(RawStreamRuntimeRndvTest::SetUp());
        ASSERT_NO_FATAL_FAILURE(Activate());
        if (sibling) {
            ASSERT_NO_FATAL_FAILURE(AdoptPair());
            ASSERT_NO_FATAL_FAILURE(PreparePair(1));
            ASSERT_NO_FATAL_FAILURE(ActivePair(1));
        }
    }
    void ExpectRetainedTarget() {
        const auto snapshot = runtime_.Inspect();
        ASSERT_TRUE(snapshot);
        EXPECT_GT(snapshot->rndv_target.remote_grants, 0u);
        ASSERT_NO_FATAL_FAILURE(ExpectNoDelivery());
        EXPECT_EQ(kbs_stream_close(runtime_.table(1), endpoints_[1].streams[0], nullptr), KBS_STREAM_RETAINED);
        for (const auto& endpoint : endpoints_) {
            EXPECT_EQ(endpoint.deletes, 0u);
            EXPECT_EQ(endpoint.unregisters, 0u);
        }
    }
    void WriteError(urma_cr_status_t error) {
        ASSERT_NO_FATAL_FAILURE(Start(error == URMA_CR_REM_ACCESS_ABORT_ERR));
        const auto lane = endpoints_[0].resources.domain.pool->lane(0);
        ASSERT_TRUE(lane);
        endpoints_[0].defer_write_tx = true;
        ASSERT_NO_FATAL_FAILURE(SendLease());
        ASSERT_TRUE(Until([&] { return endpoints_[0].withheld_write_tx.size() == 4 && !runtime_.paused(0); }));
        EXPECT_GT(runtime_.Inspect()->slab.local_dma, 0u);
        ASSERT_NO_FATAL_FAILURE(ExpectNoDelivery());
        for (auto& completion : endpoints_[0].withheld_write_tx) {
            completion.status = error;
        }
        ReleaseWriteCompletions(0);
        ASSERT_NO_FATAL_FAILURE(ExpectFailed(0, error));
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_EQ(endpoints_[0].posted_done, 0u);
        EXPECT_EQ(endpoints_[0].posted_writes, 4u);
        EXPECT_EQ(runtime_.Inspect()->slab.local_dma, 0u);
        EXPECT_EQ(runtime_.Inspect()->segment_leases, 0u);
        const auto state = endpoints_[0].resources.domain.pool->lane_state(*lane);
        ASSERT_TRUE(state);
        if (error == URMA_CR_REM_ACCESS_ABORT_ERR) {
            EXPECT_EQ(*state, raw::JettyLaneState::kReady);
            EXPECT_EQ(endpoints_[0].modifies, 0u);
            // 同 context、同 Lane 的另一逻辑连接仍须经过实际 SEND/RX 完成一次往返。
            ASSERT_NO_FATAL_FAILURE(Transfer(1));
            const std::array<std::byte, 3> reply{std::byte{2}, std::byte{7}, std::byte{9}};
            ASSERT_NO_FATAL_FAILURE(WritePayload(1, 1, reply));
            ASSERT_NO_FATAL_FAILURE(ReadPayload(0, 1, reply));
        } else {
            EXPECT_NE(*state, raw::JettyLaneState::kReady);
            EXPECT_GT(endpoints_[0].modifies, 0u);
            const auto posted = endpoints_[0].posted_sends + endpoints_[0].posted_writes;
            kbs_stream_io io{};
            const std::array<std::byte, 1> byte{};
            EXPECT_NE(
                kbs_stream_write(runtime_.table(0), endpoints_[0].streams[0], byte.data(), byte.size(), &io, nullptr),
                0);
            ASSERT_NO_FATAL_FAILURE(Tick());
            EXPECT_EQ(endpoints_[0].posted_sends + endpoints_[0].posted_writes, posted);
        }
        ASSERT_NO_FATAL_FAILURE(ExpectRetainedTarget());
    }
    void DoneError() {
        ASSERT_NO_FATAL_FAILURE(Start(true));
        endpoints_[0].fail_control = connection::WireControlType::kRndvDone;
        ASSERT_NO_FATAL_FAILURE(SendLease());
        ASSERT_NO_FATAL_FAILURE(ExpectFailed(0, URMA_CR_REM_ACCESS_ABORT_ERR));
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_EQ(endpoints_[0].posted_writes, 4u);
        EXPECT_EQ(endpoints_[0].posted_done, 1u);
        EXPECT_EQ(runtime_.Inspect()->slab.local_dma, 0u);
        ASSERT_NO_FATAL_FAILURE(Transfer(1));
        ASSERT_NO_FATAL_FAILURE(ExpectRetainedTarget());
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_EQ(endpoints_[0].posted_done, 1u);
        EXPECT_EQ(endpoints_[0].posted_writes, 4u);
    }
    void UnknownAcceptance() {
        ASSERT_NO_FATAL_FAILURE(Start());
        endpoints_[0].unknown_write_acceptance = true;
        ASSERT_NO_FATAL_FAILURE(SendLease());
        ASSERT_TRUE(Until([&] {
            const auto status = Status(0);
            return status && status->state == KBS_STREAM_FAILED && !runtime_.paused(0);
        }));
        // provider 只证实了一条真实 WRITE CQE，不能据此结束整个无法定位的接受前缀。
        EXPECT_EQ(endpoints_[0].posted_writes, 1u);
        EXPECT_EQ(endpoints_[0].write_post_calls, 1u);
        EXPECT_EQ(endpoints_[0].posted_done, 0u);
        const auto before = runtime_.Inspect();
        ASSERT_TRUE(before);
        EXPECT_GT(before->slab.local_dma + before->slab.quarantined_buffers, 0u);
        EXPECT_EQ(kbs_stream_close(runtime_.table(0), endpoints_[0].streams[0], nullptr), KBS_STREAM_RETAINED);
        ASSERT_NO_FATAL_FAILURE(ExpectRetainedTarget());
        ASSERT_NO_FATAL_FAILURE(Tick(64));
        EXPECT_EQ(endpoints_[0].write_post_calls, 1u);
        EXPECT_EQ(endpoints_[0].posted_done, 0u);
        EXPECT_GT(runtime_.Inspect()->slab.local_dma + runtime_.Inspect()->slab.quarantined_buffers, 0u);
    }
    void CancelBeforeWriteCompletions() {
        ASSERT_NO_FATAL_FAILURE(Start());
        endpoints_[0].defer_write_tx = true;
        ASSERT_NO_FATAL_FAILURE(SendLease());
        ASSERT_TRUE(Until(
            [&] { return endpoints_[0].withheld_write_tx.size() == 4 && !runtime_.paused(0) && !runtime_.paused(1); }));
        const auto before = runtime_.Inspect();
        ASSERT_TRUE(before);
        ASSERT_GT(before->slab.local_dma, 0u);
        ASSERT_GT(before->segment_leases, 0u);
        ASSERT_EQ(kbs_stream_cancel(runtime_.table(0), endpoints_[0].streams[0], nullptr), 0);
        EXPECT_EQ(kbs_stream_close(runtime_.table(0), endpoints_[0].streams[0], nullptr), KBS_STREAM_RETAINED);
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_EQ(runtime_.Inspect()->slab.local_dma, before->slab.local_dma);
        EXPECT_EQ(runtime_.Inspect()->segment_leases, before->segment_leases);
        EXPECT_EQ(endpoints_[0].posted_done, 0u);

        // 只消费前三条实际完成仍不能释放整个源 Lease 或段引用，更不能把取消转换为 DONE。
        for (unsigned i = 0; i < 3; ++i) {
            endpoints_[0].completed[0].push_back(endpoints_[0].withheld_write_tx.front());
            endpoints_[0].withheld_write_tx.pop_front();
        }
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_GT(runtime_.Inspect()->slab.local_dma, 0u);
        EXPECT_GT(runtime_.Inspect()->segment_leases, 0u);
        EXPECT_EQ(kbs_stream_close(runtime_.table(0), endpoints_[0].streams[0], nullptr), KBS_STREAM_RETAINED);
        ReleaseWriteCompletions(0);
        ASSERT_TRUE(Until([&] {
            const auto snapshot = runtime_.Inspect();
            return snapshot && snapshot->slab.local_dma == 0 && snapshot->segment_leases == 0;
        }));
        EXPECT_EQ(endpoints_[0].posted_done, 0u);
        EXPECT_EQ(endpoints_[0].posted_writes, 4u);
        ASSERT_NO_FATAL_FAILURE(ExpectRetainedTarget());
    }
    void GrantError() {
        ASSERT_NO_FATAL_FAILURE(Start());
        endpoints_[1].fail_control = connection::WireControlType::kRndvGrant;
        ASSERT_NO_FATAL_FAILURE(SendLease());
        ASSERT_NO_FATAL_FAILURE(ExpectFailed(1, URMA_CR_REM_ACCESS_ABORT_ERR));
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_EQ(endpoints_[1].posted_grants, 1u);
        EXPECT_EQ(endpoints_[0].posted_writes, 0u);
        EXPECT_EQ(endpoints_[0].posted_done, 0u);
        // GRANT 已被 provider 接受，通知失败无法证明对端从未获得写授权。
        ASSERT_NO_FATAL_FAILURE(ExpectRetainedTarget());
        ASSERT_NO_FATAL_FAILURE(Tick());
        EXPECT_EQ(endpoints_[1].posted_grants, 1u);
        EXPECT_EQ(endpoints_[0].posted_writes, 0u);
    }
};

TEST_F(RawStreamRuntimeRetainedDeathTest, RemoteAbortFailsOneConnectionButLeavesSharedLaneAndSiblingUsable) {
    ASSERT_EXIT(
        {
            WriteError(URMA_CR_REM_ACCESS_ABORT_ERR);
            ExitChild();
        },
        ::testing::ExitedWithCode(0), "");
}
TEST_F(RawStreamRuntimeRetainedDeathTest, AckTimeoutStopsPostingEvenIfAllKnownWriteCompletionsRetire) {
    ASSERT_EXIT(
        {
            WriteError(URMA_CR_ACK_TIMEOUT_ERR);
            ExitChild();
        },
        ::testing::ExitedWithCode(0), "");
}
TEST_F(RawStreamRuntimeRetainedDeathTest, FailedDoneCannotRecycleRemoteGrantOrReplaySuccessfulWrites) {
    ASSERT_EXIT(
        {
            DoneError();
            ExitChild();
        },
        ::testing::ExitedWithCode(0), "");
}
TEST_F(RawStreamRuntimeRetainedDeathTest, UnknownWriteAcceptanceRetainsSourceAndGrantWithoutReplay) {
    ASSERT_EXIT(
        {
            UnknownAcceptance();
            ExitChild();
        },
        ::testing::ExitedWithCode(0), "");
}
TEST_F(RawStreamRuntimeRetainedDeathTest, CancelledSenderReclaimsSourceOnlyAfterEveryRealWriteCompletion) {
    ASSERT_EXIT(
        {
            CancelBeforeWriteCompletions();
            ExitChild();
        },
        ::testing::ExitedWithCode(0), "");
}
TEST_F(RawStreamRuntimeRetainedDeathTest, FailedGrantKeepsTargetAuthorizationWithoutStartingAnyWrite) {
    ASSERT_EXIT(
        {
            GrantError();
            ExitChild();
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace api
} // namespace kbsocket
