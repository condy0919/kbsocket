// SPDX-License-Identifier: MulanPSL-2.0
#include <array>
#include <expected>
#include <limits>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection_tokens.hpp"
#include "kbsocket/connection/tx_sequence_window.hpp"

namespace kbsocket {
namespace connection {
namespace {
template <class T, class Error>
void ExpectError(const std::expected<T, Error>& result, Error expected) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), expected);
}
constexpr SequenceSession kSession{1, 2, 3};
constexpr std::array<TxFramePlan, 3> kBatch{{{10}, {20}, {30}}};

TEST(TxSequenceWindowTest, RequiresValidSessionAndReceiverAuthorization) {
    TxSequenceWindow window;
    ExpectError(window.Reserve(kBatch), SequenceError::kNotReady);
    ExpectError(window.Open({}), SequenceError::kInvalidArgument);
    ASSERT_TRUE(window.Open(kSession));
    ExpectError(window.Open(kSession), SequenceError::kInUse);
    ExpectError(window.Reserve(kBatch), SequenceError::kWindowFull);
    ExpectError(window.Update({1, 2, 4}, 64, 4096), SequenceError::kWrongSession);
    ExpectError(window.Reserve(kBatch), SequenceError::kWindowFull);
    ASSERT_TRUE(window.Update(kSession, 64, 4096));
    EXPECT_TRUE(window.Reserve(kBatch));
}

TEST(TxSequenceWindowTest, PartialAcceptanceKeepsOriginalSuffixAndBlocksNewData) {
    TxSequenceWindow window;
    ASSERT_TRUE(window.Open(kSession));
    ASSERT_TRUE(window.Update(kSession, 64, 4096));
    auto ticket = window.Reserve(kBatch);
    ASSERT_TRUE(ticket);
    ASSERT_EQ(window.pending().size(), 3u);
    EXPECT_EQ(window.pending()[2].seq, 2u);
    ASSERT_TRUE(window.Resolve(*ticket, 1));
    EXPECT_EQ(window.next_sequence(), 1u);
    EXPECT_EQ(window.accepted_bytes(), 10u);
    ASSERT_EQ(window.pending().size(), 2u);
    EXPECT_EQ(window.pending()[0].seq, 1u);
    EXPECT_EQ(window.pending()[1].seq, 2u);
    ExpectError(window.Reserve(kBatch), SequenceError::kInUse);
    ExpectError(window.Cancel(*ticket), SequenceError::kInUse);
    // 零接受既不跳过 SN，也不允许另一批越过原来的未接受后缀。
    ASSERT_TRUE(window.Resolve(*ticket, 0));
    EXPECT_EQ(window.pending()[0].seq, 1u);
    ASSERT_TRUE(window.Resolve(*ticket, 2));
    EXPECT_TRUE(window.pending().empty());
    EXPECT_EQ(window.next_sequence(), 3u);
    EXPECT_EQ(window.accepted_bytes(), 60u);
    auto next = window.Reserve(kBatch);
    ASSERT_TRUE(next);
    EXPECT_EQ(window.pending()[0].seq, 3u);
    ExpectError(window.Resolve(*ticket, 1), SequenceError::kInvalidTicket);
    EXPECT_TRUE(window.Cancel(*next));
}

TEST(TxSequenceWindowTest, CancelOnlyUnexposedBatchDoesNotLeaveSequenceGap) {
    TxSequenceWindow window;
    ASSERT_TRUE(window.Open(kSession));
    ASSERT_TRUE(window.Update(kSession, 3, 60));
    auto ticket = window.Reserve(kBatch);
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(window.Resolve(*ticket, 0));
    ASSERT_TRUE(window.Cancel(*ticket));
    ExpectError(window.Cancel(*ticket), SequenceError::kInvalidTicket);
    auto replacement = window.Reserve(kBatch);
    ASSERT_TRUE(replacement);
    EXPECT_NE(replacement->generation, ticket->generation);
    EXPECT_EQ(window.pending()[0].seq, 0u);
}

TEST(TxSequenceWindowTest, UnknownAcceptanceRetainsSuffixAndPermanentlyStopsAdmission) {
    TxSequenceWindow window;
    ASSERT_TRUE(window.Open(kSession));
    ASSERT_TRUE(window.Update(kSession, 64, 4096));
    auto ticket = window.Reserve(kBatch);
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(window.Resolve(*ticket, 1));
    ExpectError(window.Resolve(*ticket, std::nullopt), SequenceError::kAcceptanceUnknown);
    EXPECT_TRUE(window.stopped());
    EXPECT_EQ(window.pending().size(), 2u);
    ExpectError(window.Cancel(*ticket), SequenceError::kAcceptanceUnknown);
    ExpectError(window.Resolve(*ticket, 2), SequenceError::kAcceptanceUnknown);
    ASSERT_TRUE(window.Update(kSession, 128, 8192));
    ExpectError(window.Reserve(kBatch), SequenceError::kStopped);
    EXPECT_EQ(window.next_sequence(), 1u);
}

TEST(TxSequenceWindowTest, BothCumulativeWindowsGateAdmissionAndOldUpdatesDoNotAddCredit) {
    TxSequenceWindow window;
    ASSERT_TRUE(window.Open(kSession));
    ASSERT_TRUE(window.Update(kSession, 64, 59));
    ExpectError(window.Reserve(kBatch), SequenceError::kWindowFull);
    ASSERT_TRUE(window.Update(kSession, 2, 60));
    // 独立累计上界合并，旧 frame limit 不撤销已有 64，但也不能重复增加 60 字节额度。
    auto ticket = window.Reserve(kBatch);
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(window.Resolve(*ticket, 3));
    ASSERT_TRUE(window.Update(kSession, 64, 60));
    ExpectError(window.Reserve(kBatch), SequenceError::kWindowFull);
    ASSERT_TRUE(window.Update(kSession, 64, 120));
    EXPECT_TRUE(window.Reserve(kBatch));

    TxSequenceWindow frames;
    ASSERT_TRUE(frames.Open(kSession));
    ASSERT_TRUE(frames.Update(kSession, 2, 4096));
    ExpectError(frames.Reserve(kBatch), SequenceError::kWindowFull);
    EXPECT_TRUE(frames.pending().empty());
}

TEST(TxSequenceWindowTest, ExhaustedDataSequenceReservesLastNumberForFinWithoutWrapping) {
    TxSequenceWindow window;
    constexpr auto last = std::numeric_limits<std::uint32_t>::max();
    ASSERT_TRUE(window.Open(kSession, last - 1));
    ASSERT_TRUE(window.Update(kSession, TxSequenceWindow::kSequenceEnd, 100));
    const std::array<TxFramePlan, 2> data{{{1}, {1}}};
    ExpectError(window.Reserve(data), SequenceError::kExhausted);
    const std::array<TxFramePlan, 2> final{{{1}, {0, true}}};
    auto ticket = window.Reserve(final);
    ASSERT_TRUE(ticket);
    EXPECT_EQ(window.pending()[1].seq, last);
    ASSERT_TRUE(window.Resolve(*ticket, 2));
    EXPECT_EQ(window.next_sequence(), TxSequenceWindow::kSequenceEnd);
    EXPECT_TRUE(window.fin_accepted());
    ExpectError(window.Reserve(data), SequenceError::kStopped);
}

TEST(TxSequenceWindowTest, InvalidPlansLimitsAndForeignTicketsLeaveStateUntouched) {
    TxSequenceWindow window, other;
    ASSERT_TRUE(window.Open(kSession));
    ASSERT_TRUE(other.Open(kSession));
    ASSERT_TRUE(window.Update(kSession, 64, 4096));
    ASSERT_TRUE(other.Update(kSession, 64, 4096));
    ExpectError(window.Update(kSession, TxSequenceWindow::kSequenceEnd + 1, 4096), SequenceError::kInvalidArgument);
    const std::array<TxFramePlan, 2> fin_first{{{0, true}, {1}}};
    const std::array<TxFramePlan, 1> invalid_fin{{{1, true}}}, empty_data{{{0, false}}};
    EXPECT_FALSE(window.Reserve({}));
    EXPECT_FALSE(window.Reserve(fin_first));
    EXPECT_FALSE(window.Reserve(invalid_fin));
    EXPECT_FALSE(window.Reserve(empty_data));
    auto ticket = window.Reserve(kBatch);
    auto foreign = other.Reserve(kBatch);
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(foreign);
    ExpectError(window.Resolve(*foreign, 3), SequenceError::kInvalidTicket);
    ExpectError(window.Resolve(*ticket, 4), SequenceError::kInvalidArgument);
    EXPECT_EQ(window.next_sequence(), 0u);
    EXPECT_EQ(window.pending().size(), 3u);
    window.Fail();
    // 软件故障发生后仍须记录先前 post 的已接受前缀，但不会重新开放连接。
    EXPECT_TRUE(window.Resolve(*ticket, 3));
    EXPECT_EQ(window.next_sequence(), 3u);
    EXPECT_TRUE(window.stopped());
}

TEST(TxSequenceWindowTest, ByteOffsetOverflowCannotReopenWindow) {
    TxSequenceWindow window;
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    ASSERT_TRUE(window.Open(kSession, 0, maximum - 1));
    ASSERT_TRUE(window.Update(kSession, 64, maximum));
    ExpectError(window.Reserve(kBatch), SequenceError::kExhausted);
    EXPECT_TRUE(window.pending().empty());
}

TEST(ConnectionTokensTest, ReuseIncrementsFullGenerationAndRejectsOldOrForeignLease) {
    ConnectionTokens table, other;
    ASSERT_TRUE(table.Open({1, 2}, 1));
    ASSERT_TRUE(other.Open({3, 4}, 1));
    auto first = table.Acquire();
    auto foreign = other.Acquire();
    ASSERT_TRUE(first);
    ASSERT_TRUE(foreign);
    ExpectError(table.Acquire(), TokenError::kExhausted);
    ExpectError(table.Retire(*foreign), TokenError::kInvalidToken);
    ASSERT_TRUE(table.Retire(*first));
    auto second = table.Acquire();
    ASSERT_TRUE(second);
    EXPECT_EQ(second->token.slot, first->token.slot);
    EXPECT_EQ(second->token.generation, 2u);
    EXPECT_FALSE(table.Contains(first->token));
    ExpectError(table.Retire(*first), TokenError::kInvalidToken);
    ASSERT_TRUE(table.Retire(*second));
    ASSERT_TRUE(other.Retire(*foreign));
    EXPECT_TRUE(table.Close());
    ExpectError(table.Open({1, 2}, 1), TokenError::kInUse);
}

TEST(ConnectionTokensTest, Generation255TransitionsToExtendedRatherThanTruncating) {
    ConnectionTokens table;
    ASSERT_TRUE(table.Open({1, 2}, 1, 255));
    auto short_token = table.Acquire();
    ASSERT_TRUE(short_token);
    ASSERT_TRUE(table.Retire(*short_token));
    auto extended = table.Acquire();
    ASSERT_TRUE(extended);
    EXPECT_EQ(extended->token.generation, 256u);
    EXPECT_FALSE(table.Contains(short_token->token));
    ASSERT_TRUE(table.Retire(*extended));
}

TEST(ConnectionTokensTest, FullGenerationExhaustionPermanentlyRetiresSlot) {
    ConnectionTokens table;
    ASSERT_TRUE(table.Open({1, 2}, 1, std::numeric_limits<std::uint32_t>::max()));
    auto token = table.Acquire();
    ASSERT_TRUE(token);
    ASSERT_TRUE(table.Retire(*token));
    ExpectError(table.Acquire(), TokenError::kExhausted);
    EXPECT_TRUE(table.Close());
}

TEST(ConnectionTokensTest, CloseWaitsForRoutesAndStopsNewAdmission) {
    ConnectionTokens table;
    EXPECT_TRUE(table.Close());
    ExpectError(table.Open({}, 1), TokenError::kInvalidArgument);
    ExpectError(table.Open({1, 2}, 0), TokenError::kInvalidArgument);
    ExpectError(table.Open({1, 2}, ConnectionTokens::kMaxSlots + 1), TokenError::kInvalidArgument);
    ASSERT_TRUE(table.Open({1, 2}, 2));
    auto token = table.Acquire();
    ASSERT_TRUE(token);
    ExpectError(table.Close(), TokenError::kInUse);
    ExpectError(table.Acquire(), TokenError::kStopped);
    EXPECT_TRUE(table.Contains(token->token));
    ASSERT_TRUE(table.Retire(*token));
    EXPECT_TRUE(table.Close());
    EXPECT_FALSE(table.Contains(token->token));
}

TEST(ConnectionTokensTest, SlotsAboveShortRangeRemainDistinct) {
    ConnectionTokens table;
    ASSERT_TRUE(table.Open({1, 2}, 65537));
    ConnectionTokenLease last;
    for (std::uint32_t i = 0; i < 65537; ++i) {
        auto token = table.Acquire();
        ASSERT_TRUE(token);
        ASSERT_EQ(token->token.slot, i);
        last = *token;
    }
    EXPECT_EQ(last.token.slot, 65536u);
    EXPECT_EQ(table.active(), 65537u);
    // 测试可重建完整 lease 身份，以便所有槽正常退出路由；没有硬件资源或 payload 所有权。
    for (std::uint32_t i = 0; i < 65537; ++i) {
        ASSERT_TRUE(table.Retire({&table, {i, 1}}));
    }
    EXPECT_TRUE(table.Close());
}
} // namespace
} // namespace connection
} // namespace kbsocket
