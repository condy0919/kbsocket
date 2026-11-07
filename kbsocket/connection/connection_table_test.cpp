// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/connection_table.hpp"

#include <array>
#include <expected>
#include <limits>
#include <span>

#include <gtest/gtest.h>

namespace kbsocket {
namespace connection {
namespace {
template <class T>
void ExpectError(const std::expected<T, ConnectionTableError>& result, ConnectionTableError expected) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), expected);
}

class ConnectionTableTest : public ::testing::Test {
protected:
    static constexpr TokenDomain kLocal{10, 20};
    static constexpr WireReceiveContext kSource{kWireVersion, 30, 20, 40};
    WireSessionBinding Binding(WireToken token) const {
        return {kSource, token, 50};
    }
    std::expected<WireFrameView, WireError> Data(WireToken token) {
        WireFrameInput input;
        input.token = token;
        input.data = payload_;
        return Frame(input);
    }
    std::expected<WireFrameView, WireError> Frame(const WireFrameInput& input) {
        auto encoded = EncodeWireFrame(input, encoded_);
        if (!encoded) {
            return std::unexpected(encoded.error());
        }
        return DecodeWireFrame(encoded->imm_data, std::span(encoded_).first(encoded->payload_bytes));
    }

    ConnectionTable table_;
    int connection_ = 1;
    std::array<std::byte, 4> payload_{};
    std::array<std::byte, 128> encoded_{};
};

TEST_F(ConnectionTableTest, ReservedTokenCannotRouteFirstPacketUntilPublish) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    auto decoded = Data(lease->token);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(table_.active(), 1u);
    EXPECT_EQ(table_.published(), 0u);
    ExpectError(table_.Resolve(*decoded, kLocal, kSource), ConnectionTableError::kNotPublished);
    // 只有握手宿主完成依赖和初始授信后 Publish，随后 READY 才允许通知远端发送首包。
    ASSERT_TRUE(table_.Publish(*lease, Binding(lease->token), &connection_));
    auto routed = table_.Resolve(*decoded, kLocal, kSource);
    ASSERT_TRUE(routed);
    EXPECT_EQ(routed->lease, *lease);
    EXPECT_EQ(routed->connection, &connection_);
    EXPECT_EQ(routed->binding.context, kSource);
    EXPECT_EQ(routed->binding.credit_epoch, 50u);
    EXPECT_EQ(table_.published(), 1u);
    ASSERT_TRUE(table_.Retire(*lease));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, InvalidPublicationKeepsReservationUnpublishedAndCanBeCorrected) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    const auto binding = Binding(lease->token);
    ExpectError(table_.Publish(*lease, binding, nullptr), ConnectionTableError::kInvalidArgument);
    auto wrong = binding;
    wrong.token.generation++;
    ExpectError(table_.Publish(*lease, wrong, &connection_), ConnectionTableError::kInvalidArgument);
    wrong = binding;
    wrong.context.receive_domain++;
    ExpectError(table_.Publish(*lease, wrong, &connection_), ConnectionTableError::kInvalidArgument);
    wrong = binding;
    wrong.context.peer_runtime = 0;
    ExpectError(table_.Publish(*lease, wrong, &connection_), ConnectionTableError::kInvalidArgument);
    wrong = binding;
    wrong.context.session = 0;
    ExpectError(table_.Publish(*lease, wrong, &connection_), ConnectionTableError::kInvalidArgument);
    wrong = binding;
    wrong.credit_epoch = 0;
    ExpectError(table_.Publish(*lease, wrong, &connection_), ConnectionTableError::kInvalidArgument);
    wrong = binding;
    wrong.context.version++;
    ExpectError(table_.Publish(*lease, wrong, &connection_), ConnectionTableError::kVersionMismatch);
    EXPECT_EQ(table_.active(), 1u);
    EXPECT_EQ(table_.published(), 0u);
    ASSERT_TRUE(table_.Publish(*lease, binding, &connection_));
    ExpectError(table_.Publish(*lease, binding, &connection_), ConnectionTableError::kInUse);
    ASSERT_TRUE(table_.Retire(*lease));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, WithdrawKeepsLeasePinnedAndCannotRepublishOldHandshake) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(table_.Publish(*lease, Binding(lease->token), &connection_));
    ASSERT_TRUE(table_.Withdraw(*lease));
    ASSERT_TRUE(table_.Withdraw(*lease));
    EXPECT_EQ(table_.active(), 1u);
    EXPECT_EQ(table_.published(), 0u);
    auto decoded = Data(lease->token);
    ASSERT_TRUE(decoded);
    ExpectError(table_.Resolve(*decoded, kLocal, kSource), ConnectionTableError::kNotPublished);
    ExpectError(table_.Reserve(), ConnectionTableError::kExhausted);
    // 迟到 import 完成不能用撤回后的旧 token 再发布半成品路由。
    ExpectError(table_.Publish(*lease, Binding(lease->token), &connection_), ConnectionTableError::kInUse);
    ASSERT_TRUE(table_.Retire(*lease));
    auto next = table_.Reserve();
    ASSERT_TRUE(next);
    EXPECT_EQ(next->token.slot, lease->token.slot);
    EXPECT_EQ(next->token.generation, lease->token.generation + 1);
    ASSERT_TRUE(table_.Retire(*next));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, ReservationCanBeWithdrawnBeforePublication) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(table_.Withdraw(*lease));
    ExpectError(table_.Publish(*lease, Binding(lease->token), &connection_), ConnectionTableError::kInUse);
    EXPECT_EQ(table_.published(), 0u);
    ASSERT_TRUE(table_.Retire(*lease));
    EXPECT_EQ(table_.active(), 0u);
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, OldLeaseCannotWithdrawOrRetireReusedSlot) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto old = table_.Reserve();
    ASSERT_TRUE(old);
    ASSERT_TRUE(table_.Publish(*old, Binding(old->token), &connection_));
    ASSERT_TRUE(table_.Retire(*old));
    auto current = table_.Reserve();
    ASSERT_TRUE(current);
    int new_connection = 2;
    ASSERT_TRUE(table_.Publish(*current, Binding(current->token), &new_connection));
    ExpectError(table_.Withdraw(*old), ConnectionTableError::kInvalidLease);
    ExpectError(table_.Retire(*old), ConnectionTableError::kInvalidLease);
    auto old_frame = Data(old->token);
    ASSERT_TRUE(old_frame);
    ExpectError(table_.Resolve(*old_frame, kLocal, kSource), ConnectionTableError::kInvalidLease);
    auto new_frame = Data(current->token);
    ASSERT_TRUE(new_frame);
    auto routed = table_.Resolve(*new_frame, kLocal, kSource);
    ASSERT_TRUE(routed);
    EXPECT_EQ(routed->connection, &new_connection);
    EXPECT_EQ(table_.published(), 1u);
    ASSERT_TRUE(table_.Retire(*current));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, ForeignTableLeaseCannotMutateIdenticalNumericSlot) {
    ConnectionTable other;
    ASSERT_TRUE(table_.Open(kLocal, 1));
    ASSERT_TRUE(other.Open({11, 20}, 1));
    auto own = table_.Reserve();
    auto foreign = other.Reserve();
    ASSERT_TRUE(own);
    ASSERT_TRUE(foreign);
    EXPECT_EQ(own->token, foreign->token);
    ExpectError(table_.Publish(*foreign, Binding(own->token), &connection_), ConnectionTableError::kInvalidLease);
    ASSERT_TRUE(table_.Publish(*own, Binding(own->token), &connection_));
    ExpectError(table_.Withdraw(*foreign), ConnectionTableError::kInvalidLease);
    ExpectError(table_.Retire(*foreign), ConnectionTableError::kInvalidLease);
    EXPECT_EQ(table_.published(), 1u);
    ASSERT_TRUE(table_.Retire(*own));
    ASSERT_TRUE(other.Retire(*foreign));
    EXPECT_TRUE(table_.Close());
    EXPECT_TRUE(other.Close());
}

TEST_F(ConnectionTableTest, CompleteSourceAndLocalDomainMustMatchBeyondShortToken) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(table_.Publish(*lease, Binding(lease->token), &connection_));
    auto decoded = Data(lease->token);
    ASSERT_TRUE(decoded);
    // 数值相同的短 token 不能跨 runtime、域 incarnation、对端或 Session 命中。
    ExpectError(table_.Resolve(*decoded, {11, 20}, kSource), ConnectionTableError::kIdentityMismatch);
    ExpectError(table_.Resolve(*decoded, {10, 21}, kSource), ConnectionTableError::kIdentityMismatch);
    auto wrong = kSource;
    wrong.peer_runtime++;
    ExpectError(table_.Resolve(*decoded, kLocal, wrong), ConnectionTableError::kIdentityMismatch);
    wrong = kSource;
    wrong.receive_domain++;
    ExpectError(table_.Resolve(*decoded, kLocal, wrong), ConnectionTableError::kIdentityMismatch);
    wrong = kSource;
    wrong.session++;
    ExpectError(table_.Resolve(*decoded, kLocal, wrong), ConnectionTableError::kIdentityMismatch);
    wrong = kSource;
    wrong.version++;
    ExpectError(table_.Resolve(*decoded, kLocal, wrong), ConnectionTableError::kVersionMismatch);
    EXPECT_TRUE(table_.Resolve(*decoded, kLocal, kSource));
    ASSERT_TRUE(table_.Retire(*lease));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, ControlFramesValidatePayloadSessionAndCreditEpoch) {
    ASSERT_TRUE(table_.Open(kLocal, 1));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    const auto binding = Binding(lease->token);
    ASSERT_TRUE(table_.Publish(*lease, binding, &connection_));
    WireFrameInput credit;
    credit.type = WireFrameType::kControl;
    credit.token = lease->token;
    credit.session = binding.context.session;
    credit.credit_epoch = binding.credit_epoch;
    credit.max_credit_seq = 8;
    credit.max_control_credit_seq = 1;
    credit.max_frame_seq = 64;
    credit.max_stream_bytes = 4096;
    auto valid = Frame(credit);
    ASSERT_TRUE(valid);
    EXPECT_TRUE(table_.Resolve(*valid, kLocal, kSource));
    ++credit.credit_epoch;
    auto old_epoch = Frame(credit);
    ASSERT_TRUE(old_epoch);
    ExpectError(table_.Resolve(*old_epoch, kLocal, kSource), ConnectionTableError::kIdentityMismatch);
    --credit.credit_epoch;
    ++credit.session;
    auto old_session = Frame(credit);
    ASSERT_TRUE(old_session);
    ExpectError(table_.Resolve(*old_session, kLocal, kSource), ConnectionTableError::kIdentityMismatch);
    WireFrameInput fin;
    fin.type = WireFrameType::kControl;
    fin.control = WireControlType::kFin;
    fin.token = lease->token;
    fin.session = binding.context.session + 1;
    auto old_fin = Frame(fin);
    ASSERT_TRUE(old_fin);
    ExpectError(table_.Resolve(*old_fin, kLocal, kSource), ConnectionTableError::kIdentityMismatch);
    ASSERT_TRUE(table_.Retire(*lease));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, CloseStopsAdmissionButKeepsPublishedRoutesForDraining) {
    ASSERT_TRUE(table_.Open(kLocal, 3));
    auto live = table_.Reserve();
    auto preparing = table_.Reserve();
    ASSERT_TRUE(live);
    ASSERT_TRUE(preparing);
    ASSERT_TRUE(table_.Publish(*live, Binding(live->token), &connection_));
    ExpectError(table_.Close(), ConnectionTableError::kInUse);
    ExpectError(table_.Reserve(), ConnectionTableError::kStopped);
    ExpectError(table_.Publish(*preparing, Binding(preparing->token), &connection_), ConnectionTableError::kStopped);
    auto decoded = Data(live->token);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(table_.Resolve(*decoded, kLocal, kSource));
    ASSERT_TRUE(table_.Retire(*preparing));
    ExpectError(table_.Close(), ConnectionTableError::kInUse);
    ASSERT_TRUE(table_.Retire(*live));
    ASSERT_TRUE(table_.Close());
    EXPECT_EQ(table_.published(), 0u);
    ExpectError(table_.Resolve(*decoded, kLocal, kSource), ConnectionTableError::kNotReady);
    ExpectError(table_.Reserve(), ConnectionTableError::kNotReady);
    ExpectError(table_.Open(kLocal, 3), ConnectionTableError::kInUse);
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, Generation255TransitionsToFullExtendedIdentity) {
    ASSERT_TRUE(table_.Open(kLocal, 1, 255));
    auto short_lease = table_.Reserve();
    ASSERT_TRUE(short_lease);
    ASSERT_TRUE(table_.Publish(*short_lease, Binding(short_lease->token), &connection_));
    auto short_frame = Data(short_lease->token);
    ASSERT_TRUE(short_frame);
    EXPECT_FALSE(short_frame->extended_identity);
    EXPECT_TRUE(table_.Resolve(*short_frame, kLocal, kSource));
    ASSERT_TRUE(table_.Retire(*short_lease));
    auto extended_lease = table_.Reserve();
    ASSERT_TRUE(extended_lease);
    EXPECT_EQ(extended_lease->token.generation, 256u);
    ASSERT_TRUE(table_.Publish(*extended_lease, Binding(extended_lease->token), &connection_));
    auto extended_frame = Data(extended_lease->token);
    ASSERT_TRUE(extended_frame);
    EXPECT_TRUE(extended_frame->extended_identity);
    EXPECT_TRUE(table_.Resolve(*extended_frame, kLocal, kSource));
    // 比较完整 generation，既不接受旧 255，也不截断成 8 位后误认新 Session。
    ExpectError(table_.Resolve(*short_frame, kLocal, kSource), ConnectionTableError::kInvalidLease);
    ASSERT_TRUE(table_.Retire(*extended_lease));
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, ExhaustedFullGenerationPermanentlyRemovesSlot) {
    constexpr auto maximum = std::numeric_limits<std::uint32_t>::max();
    ASSERT_TRUE(table_.Open(kLocal, 1, maximum));
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    ASSERT_TRUE(table_.Publish(*lease, Binding(lease->token), &connection_));
    auto decoded = Data(lease->token);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(table_.Resolve(*decoded, kLocal, kSource));
    ASSERT_TRUE(table_.Retire(*lease));
    ExpectError(table_.Reserve(), ConnectionTableError::kExhausted);
    ExpectError(table_.Resolve(*decoded, kLocal, kSource), ConnectionTableError::kInvalidLease);
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, SlotsAboveShortRangeResolveWithoutTruncation) {
    ASSERT_TRUE(table_.Open(kLocal, 65537));
    ConnectionRouteLease last;
    for (std::uint32_t i = 0; i < 65537; ++i) {
        auto lease = table_.Reserve();
        ASSERT_TRUE(lease);
        ASSERT_EQ(lease->token.slot, i);
        last = *lease;
    }
    ASSERT_TRUE(table_.Publish(last, Binding(last.token), &connection_));
    auto decoded = Data(last.token);
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->extended_identity);
    auto routed = table_.Resolve(*decoded, kLocal, kSource);
    ASSERT_TRUE(routed);
    EXPECT_EQ(routed->lease.token.slot, 65536u);
    // 低 16 位同为零的另一个槽仍未发布，不能用截断后身份路由到高位槽。
    auto low = Data({0, 1});
    ASSERT_TRUE(low);
    ExpectError(table_.Resolve(*low, kLocal, kSource), ConnectionTableError::kNotPublished);
    for (std::uint32_t i = 0; i < 65537; ++i) {
        ASSERT_TRUE(table_.Retire({&table_, {i, 1}}));
    }
    EXPECT_TRUE(table_.Close());
}

TEST_F(ConnectionTableTest, InvalidOpenAndOutOfRangeLeasesLeaveTableUsable) {
    EXPECT_TRUE(table_.Close());
    ExpectError(table_.Reserve(), ConnectionTableError::kNotReady);
    ExpectError(table_.Open({}, 1), ConnectionTableError::kInvalidArgument);
    ExpectError(table_.Open(kLocal, 0), ConnectionTableError::kInvalidArgument);
    ExpectError(table_.Open(kLocal, ConnectionTable::kMaxSlots + 1), ConnectionTableError::kInvalidArgument);
    ExpectError(table_.Open(kLocal, 1, 0), ConnectionTableError::kInvalidArgument);
    ASSERT_TRUE(table_.Open(kLocal, 1));
    ExpectError(table_.Open(kLocal, 1), ConnectionTableError::kInUse);
    auto lease = table_.Reserve();
    ASSERT_TRUE(lease);
    ConnectionRouteLease invalid{&table_, {1, 1}};
    ExpectError(table_.Publish(invalid, Binding(invalid.token), &connection_), ConnectionTableError::kInvalidLease);
    ExpectError(table_.Withdraw(invalid), ConnectionTableError::kInvalidLease);
    ExpectError(table_.Retire(invalid), ConnectionTableError::kInvalidLease);
    auto frame = Data(invalid.token);
    ASSERT_TRUE(frame);
    ExpectError(table_.Resolve(*frame, kLocal, kSource), ConnectionTableError::kInvalidLease);
    ASSERT_TRUE(table_.Retire(*lease));
    EXPECT_TRUE(table_.Close());
}

} // namespace
} // namespace connection
} // namespace kbsocket
