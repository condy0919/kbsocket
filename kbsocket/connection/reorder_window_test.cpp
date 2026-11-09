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
thread_local std::size_t free_count = 0;
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
    if (count_allocations && memory) {
        ++free_count;
    }
    std::free(memory);
}
void operator delete[](void* memory) noexcept {
    ::operator delete(memory);
}
void operator delete(void* memory, std::size_t) noexcept {
    ::operator delete(memory);
}
void operator delete[](void* memory, std::size_t) noexcept {
    ::operator delete(memory);
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
    ReorderFrame RndvFrame(std::uint32_t sequence, std::size_t bytes = 4) {
        auto frame = Frame(sequence, bytes);
        frame.kind = ReorderFrameKind::kValidatedRmaDone;
        return frame;
    }
    std::expected<ReorderReservation, ReorderError> Reserve(std::uint32_t sequence, std::uint64_t bytes = 4,
                                                            std::uint64_t now = 0) {
        auto result = window_.ReserveRndv(Session(), sequence, bytes, now);
        if (result) {
            reservations_[reservation_count_++] = *result;
        }
        return result;
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
        // 占位本身不持有目标内存；这里只清理元数据，绝不伪造 Grant 或 DMA 终结。
        for (std::size_t i = 0; i < reservation_count_; ++i) {
            (void)window_.AbandonRndv(reservations_[i]);
        }
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
    std::array<ReorderReservation, 512> reservations_{};
    std::size_t reservation_count_ = 0;
};

TEST_F(ReorderWindowTest, ReadableOnlyForContinuousHeadOrConsumedEofWithoutMutation) {
    EXPECT_FALSE(window_.readable());
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Fin(1), 0));
    // 有驻留 FIN 但缺少序号零，不能把 pending 当作 Read 就绪。
    EXPECT_FALSE(window_.readable());
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    const auto before = window_.stats();
    allocation_count = 0;
    count_allocations = true;
    const bool readable = window_.readable();
    count_allocations = false;
    EXPECT_TRUE(readable);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(window_.stats().rcv_next, before.rcv_next);
    EXPECT_EQ(window_.stats().buffered_frames, before.buffered_frames);
    std::array<ReorderDelivery, 1> output;
    ASSERT_EQ(Poll(output), 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_TRUE(window_.readable());
    EXPECT_FALSE(window_.eof());
    ASSERT_EQ(Poll(output), 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_TRUE(window_.eof());
    EXPECT_TRUE(window_.readable());
    window_.Fail();
    EXPECT_FALSE(window_.readable());
}

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

TEST_F(ReorderWindowTest, RndvReservationBlocksLaterEagerAndFinUntilTrustedCompletion) {
    ASSERT_TRUE(window_.Open(Session(), Config(4, 16)));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    auto reservation = Reserve(1, 8);
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(window_.Push(Frame(2), 0));
    ASSERT_TRUE(window_.Push(Fin(3), 0));
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 16u);
    EXPECT_EQ(window_.stats().received_bytes, 16u);
    std::array<ReorderDelivery, 4> output;
    ASSERT_EQ(Poll(output), 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_EQ(window_.stats().rcv_next, 1u);
    EXPECT_FALSE(window_.readable());
    EXPECT_FALSE(window_.eof());
    ASSERT_EQ(Poll(output), 0u);

    // DONE 原位完成原 SN，不能再次花费此前 RTS 已占的完整目标字节额度。
    ASSERT_TRUE(window_.CompleteRndv(*reservation, RndvFrame(1, 8), 1));
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    EXPECT_EQ(window_.stats().resident_bytes, 12u);
    EXPECT_EQ(window_.stats().received_bytes, 16u);
    ASSERT_EQ(Poll(output, 1), 3u);
    EXPECT_EQ(output[0].frame.kind, ReorderFrameKind::kValidatedRmaDone);
    EXPECT_EQ(output[0].frame.payload.size(), 8u);
    EXPECT_EQ(output[1].frame.sequence, 2u);
    EXPECT_EQ(output[2].frame.kind, ReorderFrameKind::kFin);
    ASSERT_TRUE(window_.Release(output[2].receipt));
    ASSERT_TRUE(window_.Release(output[1].receipt));
    EXPECT_FALSE(window_.eof());
    EXPECT_EQ(window_.stats().released_prefix, 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_TRUE(window_.eof());
    EXPECT_EQ(window_.stats().consumed_bytes, 16u);
    EXPECT_EQ(window_.stats().resident_bytes, 0u);
}

TEST_F(ReorderWindowTest, RndvDuplicateCannotChangeReservationLengthOrReacquireCompletedSequence) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reservation = Reserve(0, 8);
    ASSERT_TRUE(reservation);
    auto duplicate = Reserve(0, 9);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, ReorderErrorCode::kDuplicate);
    EXPECT_EQ(window_.stats().received_bytes, 8u);
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    ASSERT_TRUE(window_.CompleteRndv(*reservation, RndvFrame(0, 8), 0));
    auto second_done = window_.CompleteRndv(*reservation, RndvFrame(0, 8), 0);
    ASSERT_FALSE(second_done);
    EXPECT_EQ(second_done.error().code, ReorderErrorCode::kInvalidReservation);
    EXPECT_EQ(window_.stats().buffered_frames, 1u);
    EXPECT_FALSE(Reserve(0, 8));
    std::array<ReorderDelivery, 1> output;
    ASSERT_EQ(Poll(output), 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_FALSE(Reserve(0, 8));
    EXPECT_EQ(window_.stats().released_prefix, 1u);
}

TEST_F(ReorderWindowTest, PendingRndvRejectsEagerCollisionWithoutTakingItsLease) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(Reserve(0, 8));
    auto collision = window_.Push(Frame(0), 0);
    ASSERT_FALSE(collision);
    EXPECT_EQ(collision.error().code, ReorderErrorCode::kProtocol);
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
}

TEST_F(ReorderWindowTest, DirectValidatedDoneCannotBypassReservationIdentity) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reservation = Reserve(0, 8);
    ASSERT_TRUE(reservation);
    auto bypass = window_.Push(RndvFrame(0, 8), 0);
    ASSERT_FALSE(bypass);
    EXPECT_EQ(bypass.error().code, ReorderErrorCode::kDuplicate);
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_FALSE(window_.readable());
    ASSERT_TRUE(window_.CompleteRndv(*reservation, RndvFrame(0, 8), 0));
}

TEST_F(ReorderWindowTest, FinCannotCutOffAlreadyReservedRndvSequence) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(Reserve(2));
    auto fin = window_.Push(Fin(1), 0);
    ASSERT_FALSE(fin);
    EXPECT_EQ(fin.error().code, ReorderErrorCode::kProtocol);
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_FALSE(window_.eof());
}

TEST_F(ReorderWindowTest, RndvCannotReserveAtOrAfterEarlierFin) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Fin(1), 0));
    auto reserved = Reserve(1);
    ASSERT_FALSE(reserved);
    EXPECT_EQ(reserved.error().code, ReorderErrorCode::kProtocol);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    EXPECT_EQ(window_.stats().received_bytes, 0u);
}

TEST_F(ReorderWindowTest, RndvAndEagerShareResidentAndCumulativeByteAdmission) {
    ASSERT_TRUE(window_.Open(Session(), Config(4, 8)));
    auto reservation = Reserve(1, 4);
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(window_.Push(Frame(0, 4), 0));
    std::array<ReorderDelivery, 1> output;
    ASSERT_EQ(Poll(output), 1u);
    // 按序交付 Eager 不等于应用归还，RNDV 额度不能借这一步提前腾出。
    EXPECT_EQ(window_.stats().max_stream_bytes, 8u);
    auto excess = Reserve(2, 1);
    ASSERT_FALSE(excess);
    EXPECT_EQ(excess.error().code, ReorderErrorCode::kByteLimit);
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
    EXPECT_EQ(window_.stats().received_bytes, 8u);
    EXPECT_EQ(window_.stats().pending_frames, 1u);
}

TEST_F(ReorderWindowTest, RndvReserveRejectsOversizedLengthBeforeNarrowingOrCharging) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reserved = Reserve(0, std::numeric_limits<std::uint64_t>::max());
    ASSERT_FALSE(reserved);
    EXPECT_EQ(reserved.error().code, ReorderErrorCode::kByteLimit);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    EXPECT_EQ(window_.stats().resident_bytes, 0u);
    EXPECT_EQ(window_.stats().received_bytes, 0u);
}

TEST_F(ReorderWindowTest, RndvReserveUsesExclusiveFrameWindowAndLastSequenceIsFinOnly) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto outside = Reserve(4);
    ASSERT_FALSE(outside);
    EXPECT_EQ(outside.error().code, ReorderErrorCode::kOutOfWindow);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    ASSERT_TRUE(window_.Close());
    auto config = Config();
    config.initial_sequence = std::numeric_limits<std::uint32_t>::max();
    ASSERT_TRUE(window_.Open(Session(), config));
    auto final = Reserve(config.initial_sequence);
    ASSERT_FALSE(final);
    EXPECT_EQ(final.error().code, ReorderErrorCode::kProtocol);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
}

TEST_F(ReorderWindowTest, PendingHeadStartsGapTimeoutWithoutAnyLaterFrame) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reservation = Reserve(0, 4, 10);
    ASSERT_TRUE(reservation);
    EXPECT_TRUE(window_.CheckTimeout(109));
    auto timeout = window_.CheckTimeout(110);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ReorderErrorCode::kGapTimeout);
    auto late = window_.CompleteRndv(*reservation, RndvFrame(0), 111);
    ASSERT_FALSE(late);
    EXPECT_EQ(late.error().code, ReorderErrorCode::kGapTimeout);
    EXPECT_FALSE(window_.readable());
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 4u);
}

TEST_F(ReorderWindowTest, DuplicateRtsAndLaterReservationsDoNotPostponeMissingCompletion) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(Reserve(0, 4, 10));
    EXPECT_FALSE(Reserve(0, 4, 50));
    ASSERT_TRUE(Reserve(1, 4, 100));
    auto timeout = window_.CheckTimeout(110);
    ASSERT_FALSE(timeout);
    EXPECT_EQ(timeout.error().code, ReorderErrorCode::kGapTimeout);
    EXPECT_EQ(window_.stats().pending_frames, 2u);
}

TEST_F(ReorderWindowTest, RndvWrongSessionAndInvalidCompletionCannotMutatePendingOwnership) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reservation = Reserve(0, 8);
    ASSERT_TRUE(reservation);
    for (unsigned field = 0; field < 5; ++field) {
        auto frame = RndvFrame(0, 8);
        switch (field) {
        case 0:
            ++frame.session.peer_runtime;
            break;
        case 1:
            ++frame.session.receive_domain;
            break;
        case 2:
            ++frame.session.session;
            break;
        case 3:
            ++frame.session.slot;
            break;
        case 4:
            ++frame.session.generation;
            break;
        }
        // 旧会话不能用未来时间推进当前窗口的超时，也不能消耗其字节窗口。
        const auto reserve = window_.ReserveRndv(frame.session, 1, 4, 500);
        ASSERT_FALSE(reserve);
        EXPECT_EQ(reserve.error().code, ReorderErrorCode::kWrongSession);
        const auto complete = window_.CompleteRndv(*reservation, frame, 500);
        ASSERT_FALSE(complete);
        EXPECT_EQ(complete.error().code, ReorderErrorCode::kWrongSession);
    }
    for (unsigned field = 0; field < 5; ++field) {
        auto frame = RndvFrame(0, 8);
        switch (field) {
        case 0:
            frame.kind = ReorderFrameKind::kEager;
            break;
        case 1:
            frame.sequence = 1;
            break;
        case 2:
            frame.payload = frame.payload.first(7);
            break;
        case 3:
            frame.lease = std::monostate{};
            break;
        case 4:
            frame.payload = {};
            break;
        }
        const auto complete = window_.CompleteRndv(*reservation, frame, 0);
        ASSERT_FALSE(complete);
        EXPECT_EQ(complete.error().code, ReorderErrorCode::kInvalidArgument);
    }
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
    EXPECT_EQ(window_.stats().received_bytes, 8u);
    EXPECT_FALSE(window_.failure());
    ASSERT_TRUE(window_.CompleteRndv(*reservation, RndvFrame(0, 8), 0));
}

TEST_F(ReorderWindowTest, RndvCompletionCannotAliasBufferedOrApplicationHeldLease) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    ASSERT_TRUE(window_.Push(Frame(0), 0));
    auto reservation = Reserve(1);
    ASSERT_TRUE(reservation);
    auto aliased = RndvFrame(1);
    aliased.lease = Frame(0).lease;
    auto complete = window_.CompleteRndv(*reservation, aliased, 0);
    ASSERT_FALSE(complete);
    EXPECT_EQ(complete.error().code, ReorderErrorCode::kLeaseAlreadyHeld);
    std::array<ReorderDelivery, 1> output;
    ASSERT_EQ(Poll(output), 1u);
    complete = window_.CompleteRndv(*reservation, aliased, 0);
    ASSERT_FALSE(complete);
    EXPECT_EQ(complete.error().code, ReorderErrorCode::kLeaseAlreadyHeld);
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    ASSERT_TRUE(window_.CompleteRndv(*reservation, RndvFrame(1), 0));
}

TEST_F(ReorderWindowTest, StopAndDrainNeverPublishOrReleaseIncompleteRndvBacking) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reservation = Reserve(0, 8);
    ASSERT_TRUE(reservation);
    ASSERT_TRUE(window_.Push(Frame(1), 0));
    auto active_abandon = window_.AbandonRndv(*reservation);
    ASSERT_FALSE(active_abandon);
    EXPECT_EQ(active_abandon.error().code, ReorderErrorCode::kInUse);
    auto close = window_.Close();
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error().code, ReorderErrorCode::kInUse);
    auto complete = window_.CompleteRndv(*reservation, RndvFrame(0, 8), 0);
    ASSERT_FALSE(complete);
    EXPECT_EQ(complete.error().code, ReorderErrorCode::kStopped);
    std::array<ReorderDelivery, 4> output;
    auto drained = window_.Drain(output);
    ASSERT_TRUE(drained);
    ASSERT_EQ(*drained, 1u);
    EXPECT_EQ(output[0].frame.sequence, 1u);
    EXPECT_TRUE(output[0].discarded);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    EXPECT_EQ(window_.stats().pending_frames, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 8u);
    EXPECT_FALSE(window_.Close());

    // 清理仅解除占位，不将未完成 RNDV 计作已消费，更不据此跳过其 SN。
    ASSERT_TRUE(window_.AbandonRndv(*reservation));
    EXPECT_EQ(window_.stats().resident_bytes, 0u);
    EXPECT_EQ(window_.stats().received_bytes, 12u);
    EXPECT_EQ(window_.stats().consumed_bytes, 0u);
    EXPECT_EQ(window_.stats().rcv_next, 0u);
    EXPECT_EQ(window_.stats().released_prefix, 0u);
    EXPECT_FALSE(window_.AbandonRndv(*reservation));
    EXPECT_TRUE(window_.Close());
}

TEST_F(ReorderWindowTest, FailedWindowAllowsMetadataAbandonButNeverLateDoneDelivery) {
    ASSERT_TRUE(window_.Open(Session(), Config()));
    auto reservation = Reserve(0);
    ASSERT_TRUE(reservation);
    window_.Fail();
    auto complete = window_.CompleteRndv(*reservation, RndvFrame(0), 0);
    ASSERT_FALSE(complete);
    EXPECT_EQ(complete.error().code, ReorderErrorCode::kFailed);
    ASSERT_TRUE(window_.AbandonRndv(*reservation));
    EXPECT_EQ(window_.stats().received_bytes, 4u);
    EXPECT_EQ(window_.stats().consumed_bytes, 0u);
    EXPECT_FALSE(window_.eof());
    EXPECT_TRUE(window_.Close());
}

TEST_F(ReorderWindowTest, RndvReservationRejectsForeignStaleAndReopenedHandles) {
    ASSERT_TRUE(window_.Open(Session(), Config(1)));
    auto reservation = Reserve(0);
    ASSERT_TRUE(reservation);
    auto invalid = *reservation;
    ReorderWindow foreign;
    invalid.owner = &foreign;
    EXPECT_FALSE(window_.CompleteRndv(invalid, RndvFrame(0), 0));
    EXPECT_FALSE(window_.AbandonRndv(invalid));
    invalid = *reservation;
    ++invalid.generation;
    EXPECT_FALSE(window_.CompleteRndv(invalid, RndvFrame(0), 0));
    invalid = *reservation;
    invalid.slot = 1;
    EXPECT_FALSE(window_.CompleteRndv(invalid, RndvFrame(0), 0));
    invalid = *reservation;
    invalid.generation = 0;
    EXPECT_FALSE(window_.CompleteRndv(invalid, RndvFrame(0), 0));
    ASSERT_TRUE(window_.CompleteRndv(*reservation, RndvFrame(0), 0));
    std::array<ReorderDelivery, 1> output;
    ASSERT_EQ(Poll(output), 1u);
    ASSERT_TRUE(window_.Release(output[0].receipt));
    auto reused = Reserve(1);
    ASSERT_TRUE(reused);
    EXPECT_GT(reused->generation, reservation->generation);
    EXPECT_FALSE(window_.CompleteRndv(*reservation, RndvFrame(1), 0));
    window_.StopAccepting();
    ASSERT_TRUE(window_.AbandonRndv(*reused));
    ASSERT_TRUE(window_.Close());
    ASSERT_TRUE(window_.Open(Session(), Config(1)));
    auto reopened = Reserve(0);
    ASSERT_TRUE(reopened);
    EXPECT_GT(reopened->generation, reused->generation);
    EXPECT_FALSE(window_.CompleteRndv(*reservation, RndvFrame(0), 0));
    EXPECT_FALSE(window_.CompleteRndv(*reused, RndvFrame(0), 0));
    ASSERT_TRUE(window_.CompleteRndv(*reopened, RndvFrame(0), 0));
}

TEST_F(ReorderWindowTest, RndvAdmissionCompletionReuseAndAbandonDoNotAllocateOrFree) {
    ASSERT_TRUE(window_.Open(Session(), Config(1)));
    std::array<ReorderDelivery, 1> output;
    bool succeeded = true;
    allocation_count = free_count = 0;
    count_allocations = true;
    for (std::uint32_t sequence = 0; sequence < 100; ++sequence) {
        auto reservation = window_.ReserveRndv(Session(), sequence, 4, sequence);
        succeeded &= reservation.has_value();
        if (!reservation) {
            break;
        }
        succeeded &= window_.CompleteRndv(*reservation, RndvFrame(sequence), sequence).has_value();
        const auto ready = window_.Poll(output, sequence);
        succeeded &= ready && *ready == 1;
        if (ready && *ready == 1) {
            succeeded &= window_.Release(output[0].receipt).has_value();
        }
    }
    auto abandoned = window_.ReserveRndv(Session(), 100, 4, 100);
    succeeded &= abandoned.has_value();
    window_.StopAccepting();
    if (abandoned) {
        succeeded &= window_.AbandonRndv(*abandoned).has_value();
    }
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocation_count, 0u);
    EXPECT_EQ(free_count, 0u);
    EXPECT_EQ(window_.stats().resident_bytes, 0u);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    EXPECT_EQ(window_.stats().rcv_next, 100u);
    EXPECT_EQ(window_.stats().received_bytes, 404u);
    EXPECT_EQ(window_.stats().consumed_bytes, 400u);
}

} // namespace
} // namespace connection
} // namespace kbsocket
