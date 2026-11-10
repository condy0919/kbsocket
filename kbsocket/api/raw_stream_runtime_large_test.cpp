// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/api/buffer_lease.h"
#include "kbsocket/api/lease_receive.h"
#include "kbsocket/api/lease_send.h"
#include "kbsocket/api/raw_stream_runtime.hpp"
#include "kbsocket/api/stream.h"
#include "kbsocket/api/testing/raw_stream_runtime_fixture.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/runtime/raw_stack.hpp"

namespace kbsocket {
namespace api {
namespace {
using test_support::Now;

class RawStreamRuntimeLargeTest : public test_support::RawStreamRuntimeRndvTest {
protected:
    static constexpr std::size_t kMiB = 1024 * 1024;
    static constexpr std::size_t kChunk = 16 * 1024;
    static constexpr std::size_t kPostBudget = 2;
    runtime::RawStackSnapshot baseline_;
    std::size_t steps_ = 0;
    std::size_t source_dma_peak_ = 0;
    std::size_t source_buffers_peak_ = 0;
    std::size_t source_cpu_peak_ = 0;
    std::size_t target_grants_peak_ = 0;
    std::size_t target_buffers_peak_ = 0;
    std::size_t segment_leases_peak_ = 0;

    void ConfigureRndv() override {
        test_support::RawStreamRuntimeRndvTest::ConfigureRndv();
        // 每次公共写最多 1 MiB；16 MiB 逻辑流复用固定池，不为它分配 16 MiB 的每槽 backing。
        config_.slab = {32, kMiB, 512};
        config_.rndv_target_slab = memory::SlabConfig{8, kMiB, 64};
        config_.memory_budget_bytes = 40 * kMiB;
        rndv_budget_.write_chunks = 4 * 2 * 64;
        rndv_budget_.grant_bytes = 8 * kMiB;
        for (auto& endpoint : configs_) {
            endpoint.streams.max_io_bytes = kMiB;
            endpoint.rndv->connection.max_chunks = 64;
            endpoint.rndv->connection.post_budget = kPostBudget;
            endpoint.rndv->connection.chunk_bytes = kChunk;
            endpoint.rndv->connection.progress_budget = 2;
            endpoint.rndv->limits.max_write_bytes = kChunk;
            endpoint.rndv->limits.max_transfer_bytes = kMiB;
            endpoint.rndv->limits.max_bytes = 2 * kMiB;
        }
    }
    RawStreamPreparation Preparation(std::size_t side) override {
        auto preparation = test_support::RawStreamRuntimeRndvTest::Preparation(side);
        preparation.reorder.max_resident_bytes = 4 * kMiB;
        return preparation;
    }
    void Sample() {
        // 记录公共 I/O 和 owner Step 边界的实际快照峰值；不将采样值宣称为指令级最大值或硬件性能。
        const auto snapshot = runtime_.Inspect();
        if (!snapshot) {
            EXPECT_EQ(snapshot.error().code, KBS_STREAM_BUSY);
            return;
        }
        source_dma_peak_ = std::max(source_dma_peak_, snapshot->slab.local_dma);
        source_buffers_peak_ = std::max(source_buffers_peak_, snapshot->slab.buffers - snapshot->slab.available);
        source_cpu_peak_ = std::max(source_cpu_peak_, snapshot->slab.cpu_leases);
        target_grants_peak_ = std::max(target_grants_peak_, snapshot->rndv_target.remote_grants);
        target_buffers_peak_ =
            std::max(target_buffers_peak_, snapshot->rndv_target.buffers - snapshot->rndv_target.available);
        segment_leases_peak_ = std::max(segment_leases_peak_, snapshot->segment_leases);
        EXPECT_LE(snapshot->slab.local_dma, config_.slab.max_references);
        EXPECT_LE(snapshot->rndv_target.remote_grants, 8u);
        EXPECT_LE(snapshot->rndv_target.buffers - snapshot->rndv_target.available, 8u);
        EXPECT_LE(snapshot->segment_leases, rndv_budget_.tx_slots);
        EXPECT_EQ(snapshot->slab.backing_bytes + snapshot->rndv_target.backing_bytes, 40 * kMiB);
    }
    bool Step() {
        const auto result = runtime_.Step();
        ++steps_;
        if (!result) {
            ADD_FAILURE() << "Step code=" << result.error().code << " detail=" << result.error().detail;
            return false;
        }
        Sample();
        std::this_thread::yield();
        return true;
    }
    bool Pump(auto predicate) {
        const auto deadline = Now() + 3'000'000'000ull;
        while (!predicate() && Now() < deadline) {
            if (!Step()) {
                return false;
            }
        }
        return predicate();
    }
    void BeginMeasurement() {
        ASSERT_NO_FATAL_FAILURE(WaitIdle());
        ASSERT_NO_FATAL_FAILURE(NoIdleCreditEcho());
        const auto snapshot = runtime_.Inspect();
        ASSERT_TRUE(snapshot);
        baseline_ = *snapshot;
        Sample();
    }
    void FinishMeasurement() {
        ASSERT_TRUE(Pump([&] {
            const auto snapshot = runtime_.Inspect();
            return snapshot && snapshot->slab.local_dma == 0 && snapshot->segment_leases == 0 &&
                   snapshot->rndv_target.available == snapshot->rndv_target.buffers &&
                   endpoints_[0].sender_credits.stats().promised_control == 0 &&
                   endpoints_[1].sender_credits.stats().promised_control == 0 && endpoints_[0].completed[0].empty() &&
                   endpoints_[0].completed[1].empty() && endpoints_[1].completed[0].empty() &&
                   endpoints_[1].completed[1].empty();
        }));
        const auto snapshot = runtime_.Inspect();
        ASSERT_TRUE(snapshot);
        EXPECT_EQ(snapshot->slab.available, baseline_.slab.available);
        EXPECT_EQ(snapshot->slab.cpu_leases, baseline_.slab.cpu_leases);
        EXPECT_EQ(snapshot->slab.available_references, baseline_.slab.available_references);
        EXPECT_EQ(snapshot->slab.local_dma, 0u);
        EXPECT_EQ(snapshot->slab.remote_grants, 0u);
        EXPECT_EQ(snapshot->slab.quarantined_buffers, 0u);
        EXPECT_EQ(snapshot->rndv_target.cpu_leases, 0u);
        EXPECT_EQ(snapshot->rndv_target.remote_grants, 0u);
        EXPECT_EQ(snapshot->rndv_target.quarantined_buffers, 0u);
        EXPECT_EQ(snapshot->segment_leases, 0u);
        EXPECT_GT(source_dma_peak_, 0u);
        EXPECT_GT(target_grants_peak_, 0u);
        EXPECT_GT(segment_leases_peak_, 0u);
        RecordProperty("source_local_dma_peak", std::to_string(source_dma_peak_));
        RecordProperty("source_buffers_peak", std::to_string(source_buffers_peak_));
        RecordProperty("source_cpu_leases_peak", std::to_string(source_cpu_peak_));
        RecordProperty("target_grants_peak", std::to_string(target_grants_peak_));
        RecordProperty("target_buffers_peak", std::to_string(target_buffers_peak_));
        RecordProperty("segment_leases_peak", std::to_string(segment_leases_peak_));
        RecordProperty("registered_backing_bytes",
                       std::to_string(snapshot->slab.backing_bytes + snapshot->rndv_target.backing_bytes));
        RecordProperty("runtime_steps", std::to_string(steps_));
        for (const auto& endpoint : endpoints_) {
            EXPECT_LE(endpoint.max_writes_per_post, kPostBudget);
            EXPECT_LE(endpoint.max_write_bytes, kChunk);
        }
    }
    void CopyStream(std::size_t bytes, std::size_t side) {
        auto input = Payload(bytes, 37 + static_cast<unsigned>(side));
        const auto expected = input;
        std::vector<std::byte> output(bytes);
        std::size_t sent = 0, received = 0, calls = 0;
        const auto writes_before = endpoints_[side].posted_writes;
        const auto bytes_before = endpoints_[side].total_write_bytes;
        ASSERT_TRUE(Pump([&] {
            if (sent < bytes) {
                kbs_stream_io io{};
                const auto result = kbs_stream_write(runtime_.table(side), endpoints_[side].streams[0],
                                                     input.data() + sent, bytes - sent, &io, nullptr);
                if (result == 0) {
                    EXPECT_EQ(io.bytes, std::min(bytes - sent, kMiB));
                    EXPECT_GT(io.bytes, 0u);
                    // copy Write 返回后立即覆盖应用源，远端必须仍读到提交时的内容。
                    std::fill_n(input.begin() + sent, io.bytes, std::byte{0xfe});
                    sent += io.bytes;
                    ++calls;
                    Sample();
                } else {
                    EXPECT_TRUE(result == KBS_STREAM_BUSY || result == KBS_STREAM_WOULD_BLOCK) << result;
                }
            }
            if (received < bytes) {
                kbs_stream_io io{};
                const auto result = kbs_stream_read(runtime_.table(1 - side), endpoints_[1 - side].streams[0],
                                                    output.data() + received, bytes - received, Now(), &io, nullptr);
                if (result == 0) {
                    EXPECT_EQ(io.eof, 0u);
                    received += io.bytes;
                    Sample();
                } else {
                    EXPECT_TRUE(result == KBS_STREAM_BUSY || result == KBS_STREAM_WOULD_BLOCK) << result;
                }
            }
            return sent == bytes && received == bytes;
        }));
        ASSERT_EQ(sent, bytes);
        ASSERT_EQ(received, bytes);
        EXPECT_EQ(output, expected);
        EXPECT_EQ(calls, (bytes + kMiB - 1) / kMiB);
        EXPECT_EQ(endpoints_[side].posted_writes - writes_before, bytes / kChunk);
        EXPECT_EQ(endpoints_[side].total_write_bytes - bytes_before, bytes);
    }
};

TEST_F(RawStreamRuntimeLargeTest, CopyStreamsCrossPublicShortWritesAndBoundEveryProviderBatch) {
    ASSERT_NO_FATAL_FAILURE(Activate());
    ASSERT_NO_FATAL_FAILURE(BeginMeasurement());
    ASSERT_NO_FATAL_FAILURE(CopyStream(64 * 1024, 0));
    ASSERT_NO_FATAL_FAILURE(CopyStream(kMiB, 1));
    // 16 MiB 的单次应用请求由 C API 返回十六次 1 MiB 短写，每笔继续拆成 64 个 WRITE。
    ASSERT_NO_FATAL_FAILURE(CopyStream(16 * kMiB, 0));
    EXPECT_EQ(endpoints_[0].posted_writes, 1028u);
    EXPECT_EQ(endpoints_[1].posted_writes, 64u);
    EXPECT_GE(endpoints_[0].write_post_calls, 514u);
    EXPECT_EQ(endpoints_[0].max_writes_per_post, kPostBudget);
    RecordProperty("logical_stream_bytes", std::to_string(16 * kMiB));
    RecordProperty("public_max_write_bytes", std::to_string(kMiB));
    RecordProperty("provider_max_write_bytes", std::to_string(endpoints_[0].max_write_bytes));
    RecordProperty("provider_max_writes_per_post", std::to_string(endpoints_[0].max_writes_per_post));
    ASSERT_NO_FATAL_FAILURE(FinishMeasurement());
}

TEST_F(RawStreamRuntimeLargeTest, LeaseSourceRemainsPinnedWhileSiblingDataCreditAndFinProgress) {
    ASSERT_NO_FATAL_FAILURE(Activate());
    ASSERT_NO_FATAL_FAILURE(AdoptPair());
    ASSERT_NO_FATAL_FAILURE(PreparePair(1));
    ASSERT_NO_FATAL_FAILURE(ActivePair(1));
    ASSERT_NO_FATAL_FAILURE(BeginMeasurement());
    ASSERT_TRUE(runtime_.OpenBuffers({930, 2}));
    auto* buffers = runtime_.buffers();
    kbs_buffer_handle source{};
    ASSERT_EQ(kbs_buffer_acquire(buffers, kMiB, &source, nullptr), 0);
    kbs_buffer_view view{};
    ASSERT_EQ(kbs_buffer_writable(buffers, source, &view, nullptr), 0);
    const auto payload = Payload(kMiB, 73);
    std::memcpy(view.data, payload.data(), payload.size());
    const auto address = reinterpret_cast<std::uintptr_t>(view.data);
    endpoints_[0].defer_write_tx = true;
    kbs_stream_io written{};
    ASSERT_EQ(kbs_stream_write_lease(runtime_.table(0), endpoints_[0].streams[0], buffers, source, 0, view.size,
                                     &written, nullptr),
              0);
    EXPECT_EQ(written.bytes, kMiB);
    ASSERT_TRUE(
        Pump([&] { return !runtime_.paused(0) && !runtime_.paused(1) && !endpoints_[0].withheld_write_tx.empty(); }));
    EXPECT_EQ(endpoints_[0].last_write_source, address + (endpoints_[0].posted_writes - 1) * kChunk);
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers, source, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 0u);
    const auto pinned = runtime_.Inspect();
    ASSERT_TRUE(pinned);
    ASSERT_EQ(kbs_buffer_release(buffers, source, nullptr), 0);
    EXPECT_EQ(runtime_.Inspect()->slab.available, pinned->slab.available);
    EXPECT_EQ(runtime_.Inspect()->slab.local_dma, pinned->slab.local_dma);

    // 只扣留 bulk 的真实 WRITE CQE；兄弟流必须完成数据、CREDIT 和 FIN，不能靠伪造退休解锁。
    const std::array small{std::byte{0x21}, std::byte{0x43}, std::byte{0x65}};
    const auto credits_before = endpoints_[1].posted_credits;
    const auto fins_before = endpoints_[0].posted_fins;
    const auto steps_before = steps_;
    kbs_stream_io small_write{};
    ASSERT_EQ(kbs_stream_write(runtime_.table(0), endpoints_[0].streams[1], small.data(), small.size(), &small_write,
                               nullptr),
              0);
    EXPECT_EQ(small_write.bytes, small.size());
    ASSERT_EQ(kbs_stream_shutdown_write(runtime_.table(0), endpoints_[0].streams[1], nullptr), 0);
    std::array<std::byte, 3> output{};
    std::size_t received = 0;
    bool eof = false;
    for (unsigned tick = 0; tick < 128 && !eof; ++tick) {
        ASSERT_TRUE(Step());
        std::array<std::byte, 3> next{};
        kbs_stream_io io{};
        const auto result =
            kbs_stream_read(runtime_.table(1), endpoints_[1].streams[1], next.data(), next.size(), Now(), &io, nullptr);
        if (result == 0) {
            ASSERT_LE(received + io.bytes, output.size());
            std::copy_n(next.begin(), io.bytes, output.begin() + received);
            received += io.bytes;
            eof = io.eof != 0;
        } else {
            ASSERT_TRUE(result == KBS_STREAM_WOULD_BLOCK || result == KBS_STREAM_BUSY) << result;
        }
    }
    ASSERT_TRUE(eof);
    EXPECT_EQ(output, small);
    EXPECT_EQ(received, small.size());
    ASSERT_TRUE(Pump([&] {
        const auto status = Status(0, 1);
        return status && endpoints_[1].posted_credits > credits_before && status->tx_pending == 0;
    }));
    EXPECT_LE(steps_ - steps_before, 128u);
    EXPECT_GT(endpoints_[0].posted_fins, fins_before);
    EXPECT_GT(runtime_.Inspect()->slab.local_dma, 0u);
    EXPECT_EQ(runtime_.Inspect()->rndv_target.remote_grants, 1u);
    EXPECT_FALSE(endpoints_[0].withheld_write_tx.empty());
    kbs_receive_view blocked{};
    EXPECT_EQ(kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[0], kMiB, Now(), &blocked, nullptr),
              KBS_STREAM_WOULD_BLOCK);
    RecordProperty("sibling_data_credit_fin_steps", std::to_string(steps_ - steps_before));
    endpoints_[0].defer_write_tx = false;
    ReleaseWriteCompletions(0);
    ASSERT_NO_FATAL_FAILURE(ReadPayload(1, 0, payload));
    ASSERT_NO_FATAL_FAILURE(FinishMeasurement());
}

TEST_F(RawStreamRuntimeLargeTest, SlowReaderAndHeldTargetLeaseBackpressureLaterGrantWithoutGrowingPools) {
    ASSERT_NO_FATAL_FAILURE(Activate());
    ASSERT_NO_FATAL_FAILURE(BeginMeasurement());
    const auto first = Payload(kMiB, 17);
    const auto second = Payload(kMiB, 31);
    const auto third = Payload(kMiB, 49);
    ASSERT_NO_FATAL_FAILURE(WritePayload(0, 0, first));
    ASSERT_TRUE(Pump([&] {
        const auto snapshot = runtime_.Inspect();
        return snapshot && snapshot->rndv_target.buffers - snapshot->rndv_target.available == 1 &&
               snapshot->rndv_target.remote_grants == 0 && endpoints_[0].posted_done == 1;
    }));
    ASSERT_NO_FATAL_FAILURE(WritePayload(0, 0, second));
    ASSERT_TRUE(Pump([&] {
        const auto snapshot = runtime_.Inspect();
        return snapshot && snapshot->rndv_target.buffers - snapshot->rndv_target.available == 2 &&
               snapshot->rndv_target.remote_grants == 0 && endpoints_[0].posted_done == 2;
    }));
    kbs_receive_view held{};
    ASSERT_EQ(kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[0], kMiB, Now(), &held, nullptr), 0);
    ASSERT_EQ(held.size, kMiB);
    EXPECT_EQ(std::memcmp(held.data, first.data(), first.size()), 0);
    const auto occupied = runtime_.Inspect();
    ASSERT_TRUE(occupied);
    EXPECT_EQ(occupied->rndv_target.buffers - occupied->rndv_target.available, 2u);
    const auto posted = endpoints_[0].posted_writes;
    ASSERT_EQ(posted, 128u);
    ASSERT_NO_FATAL_FAILURE(WritePayload(0, 0, third));
    // 两份未归还目标耗尽本连接 Grant 配额；第三份 RTS 可排队，但不能覆盖 held Lease 或越额 WRITE。
    for (unsigned tick = 0; tick < 64; ++tick) {
        ASSERT_TRUE(Step());
    }
    EXPECT_EQ(endpoints_[0].posted_writes, posted);
    EXPECT_EQ(runtime_.Inspect()->rndv_target.available, occupied->rndv_target.available);
    EXPECT_EQ(std::memcmp(held.data, first.data(), first.size()), 0);
    kbs_receive_view blocked{};
    EXPECT_EQ(kbs_stream_read_lease(runtime_.table(1), endpoints_[1].streams[0], kMiB, Now(), &blocked, nullptr),
              KBS_STREAM_BUSY);
    RecordProperty("grant_backpressure_steps", "64");
    ASSERT_EQ(kbs_stream_release_read_lease(runtime_.table(1), held.lease, nullptr), 0);
    ASSERT_NO_FATAL_FAILURE(ReadPayload(1, 0, second));
    ASSERT_NO_FATAL_FAILURE(ReadPayload(1, 0, third));
    EXPECT_EQ(endpoints_[0].posted_writes, 192u);
    EXPECT_EQ(endpoints_[0].total_write_bytes, 3 * kMiB);
    ASSERT_NO_FATAL_FAILURE(FinishMeasurement());
}

} // namespace
} // namespace api
} // namespace kbsocket
