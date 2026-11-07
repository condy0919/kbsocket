// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/reorder_window.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

namespace {
// 只包围已初始化后的纯状态机调用，排除 GTest 和 Open 的控制路径分配。
thread_local bool count_allocations = false;
thread_local std::size_t allocation_count = 0;
} // namespace
void* operator new(std::size_t size) {
    if (count_allocations) {
        ++allocation_count;
    }
    if (auto* memory = std::malloc(size ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void operator delete(void* memory) noexcept {
    std::free(memory);
}
void operator delete[](void* memory) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t) noexcept {
    std::free(memory);
}

namespace kbsocket {
namespace connection {
namespace {
class ReorderWindowTest : public ::testing::Test {
protected:
    static ReorderSession Session() {
        return {7, 11, 19, 4, 3};
    }
    static ReorderConfig Config(std::uint32_t window = 4, std::size_t bytes = 64) {
        return {.window = window, .max_resident_bytes = bytes, .gap_timeout_ns = 100};
    }
    ReorderFrame Frame(std::uint32_t sequence, std::size_t bytes = 4) {
        ReorderFrame frame;
        frame.session = Session();
        frame.sequence = sequence;
        frame.lease = memory::BufferLease{reinterpret_cast<const memory::SlabAllocator*>(identity_.data()), sequence,
                                          static_cast<std::uint64_t>(sequence) + 1};
        frame.payload = std::span<const std::byte>(payload_).first(bytes);
        return frame;
    }
    static ReorderFrame Fin(std::uint32_t sequence) {
        return {.session = Session(), .sequence = sequence, .kind = ReorderFrameKind::kFin};
    }
    std::expected<std::size_t, ReorderError> Poll(std::span<ReorderDelivery> output, std::uint64_t now = 0) {
        auto result = window_.Poll(output, now);
        if (result) {
            for (std::size_t i = 0; i < *result; ++i) {
                receipts_[receipt_count_++] = output[i].receipt;
            }
        }
        return result;
    }
    void TearDown() override {
        count_allocations = false;
        // 正常测试显式归还；失败断言遗留时只清理本测试已取得的 receipt，不伪造外部完成。
        for (std::size_t i = 0; i < receipt_count_; ++i) {
            (void)window_.Release(receipts_[i]);
        }
        window_.StopAccepting();
        std::array<ReorderDelivery, 64> output;
        for (std::size_t tries = 0; window_.stats().buffered_frames && tries < ReorderWindow::kMaxWindow; ++tries) {
            const auto drained = window_.Drain(output);
            ASSERT_TRUE(drained);
            for (std::size_t i = 0; i < *drained; ++i) {
                EXPECT_TRUE(window_.Release(output[i].receipt));
            }
        }
        EXPECT_TRUE(window_.Close());
    }
    ReorderWindow window_;
    alignas(64) std::array<std::byte, 64> identity_{};
    std::array<std::byte, 64> payload_{};
    std::array<ReorderReceipt, 512> receipts_{};
    std::size_t receipt_count_ = 0;
};

TEST_F(ReorderWindowTest, DeliversTwoZeroThreeOneInOrderWithBoundedBatches) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    for (std::uint32_t sequence : {2u, 0u, 3u, 1u}) {
        auto frame = Frame(sequence);
        if (sequence == 3) {
            // 两种真实 Lease 描述符都原样搬运，不靠整数 ID 猜测其 pool/allocator 归属。
            frame.lease = raw::RxBufferLease{reinterpret_cast<const raw::RxBufferPool*>(identity_.data()), 44};
        }
        ASSERT_TRUE(window_.Push(frame, 0));
    }
    std::array<ReorderDelivery, 2> output;
    auto first = Poll(output);
    ASSERT_TRUE(first);
    ASSERT_EQ(*first, 2u);
    EXPECT_EQ(output[0].frame.sequence, 0u);
    EXPECT_EQ(output[1].frame.sequence, 1u);
    EXPECT_EQ(output[0].frame.payload.data(), payload_.data());
    EXPECT_EQ(window_.stats().rcv_next, 2u);
    EXPECT_EQ(window_.stats().resident_bytes, 16u);
    EXPECT_EQ(window_.stats().max_frame_seq, 4u);
    EXPECT_TRUE(window_.Release(output[0].receipt));
    EXPECT_TRUE(window_.Release(output[1].receipt));
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
    auto second = Poll(output);
    ASSERT_TRUE(second);
    ASSERT_EQ(*second, 2u);
    EXPECT_EQ(output[0].frame.sequence, 2u);
    EXPECT_EQ(output[1].frame.sequence, 3u);
    ASSERT_TRUE(std::holds_alternative<raw::RxBufferLease>(output[1].frame.lease));
    EXPECT_EQ(std::get<raw::RxBufferLease>(output[1].frame.lease).id, 44u);
    EXPECT_TRUE(window_.Release(output[0].receipt));
    EXPECT_TRUE(window_.Release(output[1].receipt));
    EXPECT_EQ(window_.stats().released_prefix, 4u);
    EXPECT_EQ(window_.stats().consumed_bytes, 16u);
}

TEST_F(ReorderWindowTest, DuplicateAndAliasedLeaseCannotOverwriteBufferedFrame) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    const auto original = Frame(2);
    ASSERT_TRUE(window_.Push(original, 1));
    auto duplicate = Frame(2, 7);
    duplicate.lease = std::get<memory::BufferLease>(Frame(8).lease);
    auto result = window_.Push(duplicate, 2);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kDuplicate);
    auto alias = original;
    alias.sequence = 3;
    result = window_.Push(alias, 3);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kLeaseAlreadyHeld);
    EXPECT_EQ(window_.stats().buffered_frames, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 4u);
    EXPECT_FALSE(window_.failure());
}

TEST_F(ReorderWindowTest, AlreadyDeliveredLeaseStillCannotBeInsertedUnderAnotherSequence) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto frame = Frame(0);
    ASSERT_TRUE(window_.Push(frame, 0));
    std::array<ReorderDelivery, 1> output;
    ASSERT_TRUE(Poll(output));
    frame.sequence = 1;
    auto result = window_.Push(frame, 0);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kLeaseAlreadyHeld);
    EXPECT_EQ(window_.stats().delivered_frames, 1u);
    EXPECT_TRUE(window_.Release(output[0].receipt));
    ASSERT_TRUE(window_.Push(Frame(1), 0));
    ASSERT_TRUE(Poll(output));
    EXPECT_TRUE(window_.Release(output[0].receipt));
}

TEST_F(ReorderWindowTest, SlowApplicationRetainsBytesAfterOrderedDelivery) {
    ASSERT_TRUE(window_.Open(Session(), Config(4, 8)));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    ASSERT_TRUE(window_.Push(Frame(1), 0));
    std::array<ReorderDelivery, 2> output;
    ASSERT_TRUE(Poll(output));
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
    EXPECT_EQ(window_.stats().max_stream_bytes, 8u);
    // 对端越过驻留字节授权是协议失败；TX CQE/有序交付都不能替应用归还增加窗口。
    auto too_many = window_.Push(Frame(2, 1), 0);
    ASSERT_FALSE(too_many);
    EXPECT_EQ(too_many.error().code, ReorderErrorCode::kByteLimit);
    ASSERT_TRUE(window_.failure());
    EXPECT_EQ(window_.stats().received_bytes, 8u);
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
}

TEST_F(ReorderWindowTest, OutOfOrderApplicationReleaseAdvancesBytesButKeepsOldestFrameBound) {
    ASSERT_TRUE(window_.Open(Session(), Config(4, 8)));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    ASSERT_TRUE(window_.Push(Frame(1), 0));
    std::array<ReorderDelivery, 2> output;
    ASSERT_TRUE(Poll(output));
    ASSERT_TRUE(window_.Release(output[1].receipt));
    EXPECT_EQ(window_.stats().released_prefix, 0u);
    EXPECT_EQ(window_.stats().max_frame_seq, 4u);
    EXPECT_EQ(window_.stats().consumed_bytes, 4u);
    EXPECT_EQ(window_.stats().max_stream_bytes, 12u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_EQ(window_.stats().released_prefix, 2u);
    EXPECT_EQ(window_.stats().max_frame_seq, 6u);
    EXPECT_EQ(window_.stats().max_stream_bytes, 16u);
}

TEST_F(ReorderWindowTest, FinWaitsForContinuousPrefixAndUnreadApplicationData) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Fin(2), 0));
    ASSERT_TRUE(window_.Push(Frame(1), 0));
    std::array<ReorderDelivery, 3> output;
    auto missing = Poll(output);
    ASSERT_TRUE(missing);
    EXPECT_EQ(*missing, 0u);
    EXPECT_FALSE(window_.eof());
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    auto ready = Poll(output);
    ASSERT_TRUE(ready);
    ASSERT_EQ(*ready, 3u);
    EXPECT_EQ(output[2].frame.kind, ReorderFrameKind::kFin);
    EXPECT_FALSE(window_.accepting());
    ASSERT_TRUE(window_.Release(output[2].receipt));
    ASSERT_TRUE(window_.Release(output[1].receipt));
    EXPECT_FALSE(window_.eof());
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_TRUE(window_.eof());
    auto duplicate = window_.Push(Fin(2), 0);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, ReorderErrorCode::kDuplicate);
    EXPECT_TRUE(window_.eof());
}

TEST_F(ReorderWindowTest, FinCannotHideAlreadyBufferedHigherFrames) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(2), 0));
    auto result = window_.Push(Fin(1), 0);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kProtocol);
    EXPECT_EQ(window_.stats().buffered_frames, 1u);
    EXPECT_FALSE(window_.eof());
}

TEST_F(ReorderWindowTest, FinCannotRewritePreviouslyDeliveredDataSequence) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    std::array<ReorderDelivery, 1> output;
    ASSERT_TRUE(Poll(output));
    ASSERT_TRUE(window_.Release(output[0].receipt));
    auto result = window_.Push(Fin(0), 0);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kProtocol);
    EXPECT_FALSE(window_.eof());
}

TEST_F(ReorderWindowTest, LastWireSequenceIsReservedForFinAndNeverWraps) {
    auto config = Config();
    config.initial_sequence = std::numeric_limits<std::uint32_t>::max() - 1;
    ASSERT_TRUE(window_.Open(Session(), config));
    const auto last = std::numeric_limits<std::uint32_t>::max();
    ASSERT_TRUE(window_.Push(Frame(last - 1), 0));
    ASSERT_TRUE(window_.Push(Fin(last), 0));
    std::array<ReorderDelivery, 2> output;
    auto result = Poll(output);
    ASSERT_TRUE(result);
    ASSERT_EQ(*result, 2u);
    EXPECT_EQ(window_.stats().rcv_next, std::uint64_t{1} << 32);
    EXPECT_EQ(window_.stats().max_frame_seq, std::uint64_t{1} << 32);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    ASSERT_TRUE(window_.Release(output[1].receipt));
    EXPECT_TRUE(window_.eof());
    EXPECT_FALSE(window_.accepting());
    ASSERT_TRUE(window_.Close());

    // 全新窗口也不能把最后一个保留 SN 当普通数据使用。
    config.initial_sequence = last;
    ASSERT_TRUE(window_.Open(Session(), config));
    const auto invalid = window_.Push(Frame(last), 0);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, ReorderErrorCode::kProtocol);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
}

TEST_F(ReorderWindowTest, OldSessionIdentityCannotAffectCurrentOwnership) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    for (unsigned field = 0; field < 5; ++field) {
        auto old = Frame(0);
        switch (field) {
        case 0:
            ++old.session.peer_runtime;
            break;
        case 1:
            ++old.session.receive_domain;
            break;
        case 2:
            ++old.session.session;
            break;
        case 3:
            ++old.session.slot;
            break;
        case 4:
            ++old.session.generation;
            break;
        }
        auto result = window_.Push(old, 0);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, ReorderErrorCode::kWrongSession);
    }
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_EQ(window_.stats().received_bytes, 0u);
    EXPECT_FALSE(window_.failure());
    ASSERT_TRUE(window_.Push(Frame(0), 0));
}

TEST_F(ReorderWindowTest, Window64RejectsExclusiveUpperBoundWithoutTakingInputLease) {
    ASSERT_TRUE(window_.Open(Session(), Config(64)));
    ASSERT_TRUE(window_.Push(Frame(63), 0));
    auto invalid = window_.Push(Frame(64), 0);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, ReorderErrorCode::kOutOfWindow);
    EXPECT_EQ(window_.stats().buffered_frames, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 4u);
    EXPECT_EQ(window_.stats().rcv_next, 0u);
}

TEST_F(ReorderWindowTest, LaterOrDuplicateFramesCannotPostponeTheSameGapTimeout) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(2), 10));
    auto duplicate = window_.Push(Frame(2), 50);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, ReorderErrorCode::kDuplicate);
    ASSERT_TRUE(window_.Push(Frame(3), 100));
    EXPECT_TRUE(window_.CheckTimeout(109));
    auto timeout = window_.CheckTimeout(110);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ReorderErrorCode::kGapTimeout);
    auto late = window_.Push(Frame(0), 111);
    ASSERT_FALSE(late);
    EXPECT_EQ(late.error().code, ReorderErrorCode::kGapTimeout);
    EXPECT_EQ(window_.stats().rcv_next, 0u);
    EXPECT_FALSE(window_.eof());
}

TEST_F(ReorderWindowTest, FailurePreservesReadablePrefixAndBudgetedCleanupDoesNotSkipGap) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    ASSERT_TRUE(window_.Push(Frame(2), 0));
    ASSERT_TRUE(window_.Push(Frame(3), 0));
    window_.Fail();
    std::array<ReorderDelivery, 4> output;
    auto prefix = Poll(output);
    ASSERT_TRUE(prefix);
    ASSERT_EQ(*prefix, 1u);
    EXPECT_EQ(output[0].frame.sequence, 0u);
    EXPECT_FALSE(output[0].discarded);
    EXPECT_EQ(window_.stats().rcv_next, 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    auto stopped = Poll(output);
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, ReorderErrorCode::kFailed);

    std::size_t discarded = 0;
    for (unsigned scan = 0; scan < 4; ++scan) {
        auto drained = window_.Drain(std::span(output).first(1));
        ASSERT_TRUE(drained);
        ASSERT_LE(*drained, 1u);
        if (*drained) {
            EXPECT_TRUE(output[0].discarded);
            EXPECT_GE(output[0].frame.sequence, 2u);
            ASSERT_TRUE(window_.Release(output[0].receipt));
            ++discarded;
        }
    }
    EXPECT_EQ(discarded, 2u);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_EQ(window_.stats().delivered_frames, 0u);
    EXPECT_EQ(window_.stats().consumed_bytes, 4u);
    EXPECT_EQ(window_.stats().rcv_next, 1u);
    EXPECT_FALSE(window_.eof());
}

TEST_F(ReorderWindowTest, ReceiptIdentityRejectsDuplicatesForeignOwnersAndReopenedWindow) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    std::array<ReorderDelivery, 1> output;
    ASSERT_TRUE(Poll(output));
    const auto old = output[0].receipt;
    ReorderWindow foreign;
    auto forged = old;
    forged.owner = &foreign;
    auto result = window_.Release(forged);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kInvalidReceipt);
    EXPECT_EQ(window_.stats().delivered_frames, 1u);
    ASSERT_TRUE(window_.Release(old));
    result = window_.Release(old);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kInvalidReceipt);
    ASSERT_TRUE(window_.Close());
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    ASSERT_TRUE(Poll(output));
    EXPECT_NE(output[0].receipt.generation, old.generation);
    result = window_.Release(old);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ReorderErrorCode::kInvalidReceipt);
    EXPECT_EQ(window_.stats().resident_bytes, 4u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
}

TEST_F(ReorderWindowTest, CloseStopsAdmissionAndWaitsForBothBufferedAndDeliveredLeases) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    ASSERT_TRUE(window_.Push(Frame(2), 0));
    std::array<ReorderDelivery, 1> output;
    ASSERT_TRUE(Poll(output));
    const auto held = output[0].receipt;
    auto close = window_.Close();
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error().code, ReorderErrorCode::kInUse);
    auto push = window_.Push(Frame(1), 0);
    ASSERT_FALSE(push);
    EXPECT_EQ(push.error().code, ReorderErrorCode::kStopped);
    for (unsigned scan = 0; window_.stats().buffered_frames && scan < 4; ++scan) {
        const auto drained = window_.Drain(output);
        ASSERT_TRUE(drained);
        if (*drained) {
            ASSERT_TRUE(window_.Release(output[0].receipt));
        }
    }
    EXPECT_EQ(window_.stats().delivered_frames, 1u);
    EXPECT_FALSE(window_.Close());
    ASSERT_TRUE(window_.Release(held));
    EXPECT_TRUE(window_.Close());
}

TEST_F(ReorderWindowTest, PollCapsEachBatchAndValidatedRmaDoneSharesSequenceSpace) {
    ASSERT_TRUE(window_.Open(Session(), Config(128, 128)));
    for (std::uint32_t sequence = 0; sequence < 100; ++sequence) {
        auto frame = Frame(sequence, 1);
        if (sequence == 20) {
            frame.kind = ReorderFrameKind::kValidatedRmaDone;
        }
        ASSERT_TRUE(window_.Push(frame, 0));
    }
    std::array<ReorderDelivery, 128> output;
    auto first = Poll(output);
    ASSERT_TRUE(first);
    ASSERT_EQ(*first, 64u);
    EXPECT_EQ(output[20].frame.kind, ReorderFrameKind::kValidatedRmaDone);
    for (std::size_t i = 0; i < *first; ++i) {
        ASSERT_TRUE(window_.Release(output[i].receipt));
    }
    auto rest = Poll(output);
    ASSERT_TRUE(rest);
    ASSERT_EQ(*rest, 36u);
    for (std::size_t i = 0; i < *rest; ++i) {
        ASSERT_TRUE(window_.Release(output[i].receipt));
    }
    EXPECT_EQ(window_.stats().rcv_next, 100u);
    EXPECT_EQ(window_.stats().resident_bytes, 0u);
}

TEST_F(ReorderWindowTest, RejectsInvalidInputsWithoutTakingLeaseOwnership) {
    auto config = Config();
    config.window = 0;
    EXPECT_FALSE(window_.Open(Session(), config));
    config.window = ReorderWindow::kMaxWindow + 1;
    EXPECT_FALSE(window_.Open(Session(), config));
    auto bad_session = Session();
    bad_session.generation = 0;
    EXPECT_FALSE(window_.Open(bad_session, Config()));
    ASSERT_TRUE(window_.Open(Session(), Config()));
    EXPECT_FALSE(window_.Open(Session(), Config()));
    auto empty = Frame(0);
    empty.payload = {};
    EXPECT_FALSE(window_.Push(empty, 0));
    auto unowned = Frame(0);
    unowned.lease = std::monostate{};
    EXPECT_FALSE(window_.Push(unowned, 0));
    auto invalid_fin = Fin(0);
    invalid_fin.payload = payload_;
    EXPECT_FALSE(window_.Push(invalid_fin, 0));
    auto bad_kind = Frame(0);
    bad_kind.kind = static_cast<ReorderFrameKind>(200);
    EXPECT_FALSE(window_.Push(bad_kind, 0));
    EXPECT_FALSE(window_.Poll({}, 0));
    std::array<ReorderDelivery, 1> output;
    EXPECT_FALSE(window_.Drain(output));
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_FALSE(window_.failure());
}

TEST_F(ReorderWindowTest, HotAdmissionDeliveryReturnAndTimeoutChecksDoNotAllocate) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    std::array<ReorderDelivery, 1> output;
    bool succeeded = true;
    allocation_count = 0;
    count_allocations = true;
    for (std::uint32_t sequence = 0; sequence < 100; ++sequence) {
        succeeded &= window_.Push(Frame(sequence), sequence).has_value();
        const auto result = window_.Poll(output, sequence);
        succeeded &= result && *result == 1;
        if (result && *result == 1) {
            succeeded &= window_.Release(output[0].receipt).has_value();
        }
        succeeded &= window_.CheckTimeout(sequence).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(window_.stats().resident_bytes, 0u);
    EXPECT_EQ(window_.stats().rcv_next, 100u);
}
} // namespace
} // namespace connection
} // namespace kbsocket
