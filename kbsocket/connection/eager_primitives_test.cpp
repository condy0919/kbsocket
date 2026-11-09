// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <cstddef>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection_tokens.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/reorder_window.hpp"
#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/connection/tx_sequence_window.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace connection {
namespace {
class EagerPrimitivesTest : public ::testing::Test {
protected:
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions functions{};
        functions.register_seg = [](urma_context_t* context, urma_seg_cfg_t* config) {
            auto& segment = active_->segments_[active_->registrations_++];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            return &segment;
        };
        functions.unregister_seg = [](urma_target_seg_t*) { return URMA_SUCCESS; };
        return functions;
    }
    void SetUp() override {
        active_ = this;
        const std::array<urma_context_t*, 1> contexts{&context_};
        memory::SlabConfig config;
        config.buffer_count = 4;
        ASSERT_TRUE(source_.Open(contexts, config, budget_));
        ASSERT_TRUE(target_.Open(contexts, config, budget_));
    }
    void TearDown() override {
        EXPECT_TRUE(source_.Close());
        EXPECT_TRUE(target_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    static inline EagerPrimitivesTest* active_ = nullptr;
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    std::array<urma_target_seg_t, 2> segments_{};
    std::size_t registrations_ = 0;
    memory::MemoryBudget budget_{1024 * 1024};
    memory::SlabAllocator source_, target_;
};

TEST_F(EagerPrimitivesTest, PartialPostReorderedDeliverySlowReaderAndFinPreserveAllBudgets) {
    constexpr CreditIdentity identity{11, 22, 1};
    constexpr SequenceSession session{11, 22, 33};
    constexpr WireToken token{7, 256}; // 高代际强制走扩展身份，验证实际头部扣除。
    constexpr ReorderSession receive_session{11, 22, 33, 7, 256};
    const WireReceiveContext context{kWireVersion, 11, 22, 33};
    const WireSessionBinding binding{context, token, 1};
    ReceiveCreditDomain credits;
    ASSERT_TRUE(
        credits.Open({.receive_domain = 22, .capacity = 6, .control_slots = 2, .peer_capacity = 1, .base_credits = 2}));
    ASSERT_TRUE(credits.RecordInitialPosted(CreditClass::kData, 4));
    ASSERT_TRUE(credits.RecordInitialPosted(CreditClass::kControl, 2));
    auto peer = credits.Admit(identity);
    ASSERT_TRUE(peer);
    auto data_limit = credits.Grant(*peer, CreditClass::kData, 4);
    auto control_limit = credits.Grant(*peer, CreditClass::kControl, 1);
    ASSERT_TRUE(data_limit);
    ASSERT_TRUE(control_limit);
    SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open(identity));
    ASSERT_TRUE(sender.Update(identity, *data_limit, *control_limit));
    ReorderWindow receiver;
    ASSERT_TRUE(receiver.Open(receive_session, {.window = 4, .max_resident_bytes = 16}));
    TxSequenceWindow sequence;
    ASSERT_TRUE(sequence.Open(session));
    ASSERT_TRUE(sequence.Update(session, receiver.stats().max_frame_seq, receiver.stats().max_stream_bytes));
    const std::array<TxFramePlan, 4> plans{{{4}, {4}, {4}, {4}}};
    auto sequence_ticket = sequence.Reserve(plans);
    auto credit_ticket = sender.Reserve(CreditClass::kData, 4);
    ASSERT_TRUE(sequence_ticket);
    ASSERT_TRUE(credit_ticket);

    // 模拟可信部分 post；同一后缀继续使用 SN=2,3，credits 只取消明确未接受部分。
    ASSERT_TRUE(sequence.Resolve(*sequence_ticket, 2));
    ASSERT_TRUE(sender.Resolve(*credit_ticket, 2));
    EXPECT_EQ(sequence.pending()[0].seq, 2u);
    auto suffix = sender.Reserve(CreditClass::kData, 2);
    ASSERT_TRUE(suffix);
    ASSERT_TRUE(sequence.Resolve(*sequence_ticket, 2));
    ASSERT_TRUE(sender.Resolve(*suffix, 2));
    EXPECT_EQ(sender.available(CreditClass::kData), 0u);
    EXPECT_EQ(sender.available(CreditClass::kControl), 1u);

    std::array<ReorderFrame, 4> frames;
    std::array<std::array<std::byte, 4>, 4> expected;
    for (std::size_t i = 0; i < frames.size(); ++i) {
        expected[i].fill(static_cast<std::byte>(i + 1));
        auto staging = source_.Acquire(kWireMaxPayload);
        ASSERT_TRUE(staging);
        auto writable = source_.Writable(*staging);
        ASSERT_TRUE(writable);
        WireFrameInput input;
        input.token = token;
        input.seq = static_cast<std::uint32_t>(i);
        input.data = expected[i];
        auto encoded = EncodeWireFrame(input, *writable);
        ASSERT_TRUE(encoded);
        auto dma = source_.Reserve(*staging, memory::BufferUse::kLocalDma);
        ASSERT_TRUE(dma);
        auto sge = source_.Sge(*dma, &context_, 0, encoded->payload_bytes);
        ASSERT_TRUE(sge);
        ASSERT_TRUE(source_.Commit(*dma));
        ASSERT_TRUE(source_.ReleaseCpu(*staging));

        // 用 Mock DMA 复制代表传输事实；不以该测试声称真机顺序/可见性已验证。
        const auto* bytes = reinterpret_cast<const std::byte*>(sge->addr);
        auto received = target_.Copy({bytes, encoded->payload_bytes});
        ASSERT_TRUE(received);
        ASSERT_TRUE(source_.CompleteDma(*dma));
        auto read = target_.Readable(*received);
        ASSERT_TRUE(read);
        auto decoded = DecodeWireFrame(encoded->imm_data, *read);
        ASSERT_TRUE(decoded);
        ASSERT_TRUE(ValidateWireIdentity(*decoded, binding, context));
        frames[i] = {receive_session, decoded->frame.seq, ReorderFrameKind::kEager, *received, decoded->frame.data};
    }
    EXPECT_EQ(source_.stats().available, 4u);
    EXPECT_EQ(sender.available(CreditClass::kData), 0u); // 本地完成绝不能重授远端 RQE。

    std::array<RxCreditReceipt, 4> physical;
    std::array<ReorderDelivery, 4> deliveries;
    std::size_t delivered = 0;
    std::uint64_t now = 1;
    for (const auto index : {2u, 0u, 3u, 1u}) {
        auto receipt = credits.Consume(*peer, CreditClass::kData);
        ASSERT_TRUE(receipt);
        physical[index] = *receipt;
        ASSERT_TRUE(receiver.Push(frames[index], now++));
        auto polled = receiver.Poll(std::span(deliveries).subspan(delivered), now++);
        ASSERT_TRUE(polled);
        delivered += *polled;
    }
    ASSERT_EQ(delivered, 4u);
    for (std::size_t i = 0; i < delivered; ++i) {
        EXPECT_EQ(deliveries[i].frame.sequence, i);
        EXPECT_TRUE(
            std::equal(deliveries[i].frame.payload.begin(), deliveries[i].frame.payload.end(), expected[i].begin()));
    }
    EXPECT_EQ(receiver.stats().resident_bytes, 16u);
    EXPECT_EQ(receiver.stats().max_frame_seq, 4u);
    EXPECT_EQ(receiver.stats().max_stream_bytes, 16u);
    EXPECT_EQ(credits.stats().physical_posted, 2u); // 数据满窗后仍保留控制授权容量。

    // 先释放后一个应用帧并补投：物理额度可恢复，发送 SN 仍受最早未消费窗口约束。
    for (const auto index : {3u, 0u, 1u, 2u}) {
        ASSERT_TRUE(target_.ReleaseCpu(std::get<memory::BufferLease>(deliveries[index].frame.lease)));
        ASSERT_TRUE(receiver.Release(deliveries[index].receipt));
        ASSERT_TRUE(credits.Release(physical[index]));
        ASSERT_TRUE(credits.RecordRefillAccepted(std::span(physical).subspan(index, 1)));
        if (index == 3) {
            auto granted = credits.Grant(*peer, CreditClass::kData, 1);
            ASSERT_TRUE(granted);
            ASSERT_TRUE(sender.Update(identity, *granted, *control_limit));
            ASSERT_TRUE(sequence.Update(session, receiver.stats().max_frame_seq, receiver.stats().max_stream_bytes));
            EXPECT_EQ(sender.available(CreditClass::kData), 1u);
            const std::array<TxFramePlan, 1> next{{{4}}};
            const auto blocked = sequence.Reserve(next);
            ASSERT_FALSE(blocked);
            EXPECT_EQ(blocked.error(), SequenceError::kWindowFull);
        }
    }
    ASSERT_TRUE(sequence.Update(session, receiver.stats().max_frame_seq, receiver.stats().max_stream_bytes));
    const std::array<TxFramePlan, 1> final{{{0, true}}};
    auto final_sequence = sequence.Reserve(final);
    auto final_credit = sender.Reserve(CreditClass::kControl, 1);
    ASSERT_TRUE(final_sequence);
    ASSERT_TRUE(final_credit);
    WireFrameInput fin;
    fin.type = WireFrameType::kControl;
    fin.control = WireControlType::kFin;
    fin.token = token;
    fin.seq = sequence.pending()[0].seq;
    fin.session = session.session;
    std::array<std::byte, 64> control;
    auto encoded_fin = EncodeWireFrame(fin, control);
    ASSERT_TRUE(encoded_fin);
    ASSERT_TRUE(sequence.Resolve(*final_sequence, 1));
    ASSERT_TRUE(sender.Resolve(*final_credit, 1));
    auto fin_receipt = credits.Consume(*peer, CreditClass::kControl);
    ASSERT_TRUE(fin_receipt);
    auto decoded_fin = DecodeWireFrame(encoded_fin->imm_data, std::span(control).first(encoded_fin->payload_bytes));
    ASSERT_TRUE(decoded_fin);
    ASSERT_TRUE(ValidateWireIdentity(*decoded_fin, binding, context));
    ASSERT_TRUE(receiver.Push({receive_session, fin.seq, ReorderFrameKind::kFin, {}, {}}, now++));
    auto final_delivery = receiver.Poll(deliveries, now++);
    ASSERT_TRUE(final_delivery);
    ASSERT_EQ(*final_delivery, 1u);
    EXPECT_FALSE(receiver.eof());
    ASSERT_TRUE(receiver.Release(deliveries[0].receipt));
    EXPECT_TRUE(receiver.eof());
    ASSERT_TRUE(credits.Release(*fin_receipt));
    ASSERT_TRUE(credits.RecordRefillAccepted({&*fin_receipt, 1}));
    EXPECT_EQ(credits.stats().physical_posted, 6u);
    ASSERT_TRUE(credits.RetirePeer(*peer, {true, true, true}));
    EXPECT_TRUE(receiver.Close());
    EXPECT_TRUE(sender.Close());
    EXPECT_TRUE(credits.Close());
}

TEST_F(EagerPrimitivesTest, LateOldConnectionConsumesOriginalPeerCreditButCannotEnterReusedSlot) {
    ConnectionTokens tokens;
    ASSERT_TRUE(tokens.Open({100, 200}, 1));
    auto old = tokens.Acquire();
    ASSERT_TRUE(old);
    std::array<std::byte, 4> data{};
    std::array<std::byte, 64> buffer;
    WireFrameInput input;
    input.token = old->token;
    input.data = data;
    auto encoded = EncodeWireFrame(input, buffer);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(tokens.Retire(*old));
    auto current = tokens.Acquire();
    ASSERT_TRUE(current);
    ReceiveCreditDomain credits;
    ASSERT_TRUE(credits.Open(
        {.receive_domain = 200, .capacity = 4, .control_slots = 1, .peer_capacity = 1, .base_credits = 1}));
    ASSERT_TRUE(credits.RecordInitialPosted(CreditClass::kData, 3));
    auto peer = credits.Admit({300, 200, 1});
    ASSERT_TRUE(peer);
    ASSERT_TRUE(credits.Grant(*peer, CreditClass::kData, 1));
    ASSERT_TRUE(credits.FreezePeer(*peer));
    auto receipt = credits.Consume(*peer, CreditClass::kData);
    ASSERT_TRUE(receipt);
    auto lease = target_.Copy(std::span(buffer).first(encoded->payload_bytes));
    ASSERT_TRUE(lease);
    auto payload = target_.Readable(*lease);
    ASSERT_TRUE(payload);
    auto decoded = DecodeWireFrame(encoded->imm_data, *payload);
    ASSERT_TRUE(decoded);
    WireReceiveContext source{kWireVersion, 300, 200, 400};
    WireSessionBinding binding{source, current->token, 1};
    auto valid = ValidateWireIdentity(*decoded, binding, source);
    ASSERT_FALSE(valid);
    EXPECT_EQ(valid.error().code, WireErrorCode::kIdentityMismatch);
    EXPECT_EQ(credits.stats().data.granted, 0u);
    EXPECT_EQ(credits.stats().data.unposted, 1u);
    // 拒绝路由也须归还真实 Lease；只有 accepted refill 能恢复物理槽位。
    ASSERT_TRUE(target_.ReleaseCpu(*lease));
    ASSERT_TRUE(credits.Release(*receipt));
    ASSERT_TRUE(credits.RecordRefillAccepted({&*receipt, 1}));
    EXPECT_EQ(credits.stats().physical_posted, 3u);
    ASSERT_TRUE(credits.RetirePeer(*peer, {true, true, true}));
    EXPECT_TRUE(credits.Close());
    ASSERT_TRUE(tokens.Retire(*current));
    EXPECT_TRUE(tokens.Close());
}

TEST_F(EagerPrimitivesTest, RndvProtocolReservationAndMockWriteShareEagerOrderingAndBudgets) {
    constexpr GrantBinding transfer_binding{11, 44, 22, 33, {7, 256}};
    constexpr RndvDestination forward{44, 22, 33, {7, 256}};
    constexpr RndvDestination reverse{11, 55, 66, {9, 3}};
    constexpr WireReceiveContext observed_forward{kWireVersion, 11, 22, 33};
    constexpr WireSessionBinding forward_route{observed_forward, forward.token, 1};
    constexpr WireReceiveContext observed_reverse{kWireVersion, 44, 55, 66};
    constexpr WireSessionBinding reverse_route{observed_reverse, reverse.token, 2};
    constexpr ReorderSession receive_session{11, 22, 33, 7, 256};
    constexpr SequenceSession send_session{44, 22, 33};
    ReorderWindow receiver;
    ASSERT_TRUE(receiver.Open(receive_session, {.window = 4, .max_resident_bytes = 16}));
    TxSequenceWindow sequence;
    ASSERT_TRUE(sequence.Open(send_session));
    ASSERT_TRUE(sequence.Update(send_session, receiver.stats().max_frame_seq, receiver.stats().max_stream_bytes));

    // RNDV 占一个 SN 与完整 8B 字节额度，RTS/GRANT/DONE 不能各占一个数据序号。
    const std::array<TxFramePlan, 4> plans{{{4}, {8}, {4}, {0, true}}};
    auto ticket = sequence.Reserve(plans);
    ASSERT_TRUE(ticket);
    ASSERT_EQ(sequence.pending().size(), 4u);
    const GrantTransfer transfer{transfer_binding, 99, sequence.pending()[1].seq, sequence.pending()[1].bytes};
    EXPECT_EQ(transfer.seq, 1u);
    ASSERT_TRUE(sequence.Resolve(*ticket, 4)); // Mock 协议准入接受这四个逻辑项。
    EXPECT_EQ(sequence.next_sequence(), 4u);
    EXPECT_EQ(sequence.accepted_bytes(), 16u);

    std::array<std::byte, 128> control{};
    auto rts = BuildRndvRts(transfer, forward);
    ASSERT_TRUE(rts);
    auto encoded = EncodeWireFrame(*rts, control);
    ASSERT_TRUE(encoded);
    auto decoded = DecodeWireFrame(encoded->imm_data, std::span(control).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    auto validated = ValidateRndvRts(*decoded, forward_route, observed_forward, transfer_binding);
    ASSERT_TRUE(validated);
    auto reservation = receiver.ReserveRndv(receive_session, validated->seq, validated->bytes, 0);
    ASSERT_TRUE(reservation);

    auto target = target_.Acquire(transfer.bytes);
    ASSERT_TRUE(target);
    auto grant_reference = target_.Reserve(*target, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(grant_reference);
    auto target_sge = target_.Sge(*grant_reference, &context_, 0, transfer.bytes);
    ASSERT_TRUE(target_sge);
    ASSERT_TRUE(target_.Commit(*grant_reference)); // 在任何可能暴露 GRANT 的操作之前保留远端责任。
    const GrantIdentity grant{transfer, {77, 88}, target->slot, target->generation, target_sge->addr};
    auto grant_frame = BuildRndvGrant(grant, reverse);
    ASSERT_TRUE(grant_frame);
    encoded = EncodeWireFrame(*grant_frame, control);
    ASSERT_TRUE(encoded);
    decoded = DecodeWireFrame(encoded->imm_data, std::span(control).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    auto accepted_grant = ValidateRndvGrant(*decoded, reverse_route, observed_reverse, transfer, grant.arena);
    ASSERT_TRUE(accepted_grant);
    EXPECT_EQ(*accepted_grant, grant);
    EXPECT_NE(decoded->frame.token, decoded->frame.rndv.token);
    EXPECT_EQ(target_.stats().remote_grants, 1u);
    EXPECT_FALSE(target_.Readable(*target));

    const std::array<std::byte, 4> eager_payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44}};
    auto eager_zero = target_.Copy(eager_payload);
    auto eager_two = target_.Copy(eager_payload);
    ASSERT_TRUE(eager_zero);
    ASSERT_TRUE(eager_two);
    auto zero_data = target_.Readable(*eager_zero);
    auto two_data = target_.Readable(*eager_two);
    ASSERT_TRUE(zero_data);
    ASSERT_TRUE(two_data);
    ASSERT_TRUE(receiver.Push({receive_session, 2, ReorderFrameKind::kEager, *eager_two, *two_data}, 1));
    ASSERT_TRUE(receiver.Push({receive_session, 3, ReorderFrameKind::kFin, {}, {}}, 2));
    std::array<ReorderDelivery, 4> output;
    ASSERT_EQ(receiver.Poll(output, 3), 0u);
    ASSERT_TRUE(receiver.Push({receive_session, 0, ReorderFrameKind::kEager, *eager_zero, *zero_data}, 4));
    ASSERT_EQ(receiver.Poll(output, 5), 1u);
    EXPECT_EQ(output[0].frame.sequence, 0u);
    ASSERT_TRUE(target_.ReleaseCpu(*eager_zero));
    ASSERT_TRUE(receiver.Release(output[0].receipt));
    EXPECT_EQ(receiver.stats().rcv_next, 1u);
    EXPECT_EQ(receiver.stats().pending_frames, 1u);
    EXPECT_FALSE(receiver.readable());
    EXPECT_FALSE(receiver.eof());

    const std::array<std::byte, 8> write_payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4},
                                                 std::byte{5}, std::byte{6}, std::byte{7}, std::byte{8}};
    auto source = source_.Copy(write_payload);
    ASSERT_TRUE(source);
    auto source_reference = source_.Reserve(*source, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(source_reference);
    auto source_sge = source_.Sge(*source_reference, &context_, 0, transfer.bytes);
    ASSERT_TRUE(source_sge);
    ASSERT_TRUE(source_.Commit(*source_reference));
    ASSERT_TRUE(source_.ReleaseCpu(*source));

    // Mock 的同步复制是本测试独立掌握的完整传输事实，不能把解析出来的 DONE 当 DMA 证明。
    std::copy_n(reinterpret_cast<const std::byte*>(source_sge->addr), transfer.bytes,
                reinterpret_cast<std::byte*>(target_sge->addr));
    ASSERT_TRUE(source_.CompleteDma(*source_reference));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 4u);
    EXPECT_EQ(target_.stats().remote_grants, 1u); // 本地逐 WR 退休仍不直接解除远端 Grant。

    // 此夹具不装配发送引擎，只模拟遵守成功后通知契约的对端；BuildRndvDone 的门禁另有引擎测试。
    auto done = *grant_frame;
    done.control = WireControlType::kRndvDone;
    done.token = forward.token;
    done.session = forward.session;
    encoded = EncodeWireFrame(done, control);
    ASSERT_TRUE(encoded);
    decoded = DecodeWireFrame(encoded->imm_data, std::span(control).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(ValidateRndvDone(*decoded, forward_route, observed_forward, grant));
    EXPECT_EQ(target_.stats().remote_grants, 1u);
    EXPECT_FALSE(target_.Readable(*target));

    // 由上面的同步 Mock 写入事实确认无后续/在途访问，再独立结束 Grant，才允许完成顺序占位。
    ASSERT_TRUE(target_.EndGrant(*grant_reference));
    auto target_data = target_.Readable(*target);
    ASSERT_TRUE(target_data);
    ASSERT_TRUE(receiver.CompleteRndv(
        *reservation, {receive_session, 1, ReorderFrameKind::kValidatedRmaDone, *target, *target_data}, 6));
    EXPECT_EQ(receiver.stats().received_bytes, 16u);
    EXPECT_EQ(receiver.stats().resident_bytes, 12u);
    ASSERT_EQ(receiver.Poll(output, 7), 3u);
    EXPECT_EQ(output[0].frame.sequence, 1u);
    EXPECT_EQ(output[1].frame.sequence, 2u);
    EXPECT_EQ(output[2].frame.sequence, 3u);
    EXPECT_TRUE(std::equal(output[0].frame.payload.begin(), output[0].frame.payload.end(), write_payload.begin()));
    EXPECT_EQ(target_.stats().remote_grants, 0u);
    EXPECT_EQ(target_.stats().cpu_leases, 2u);

    // 即使后继 Eager 与 FIN 先归还，RNDV 应用借用仍占额度且阻止最终 EOF。
    ASSERT_TRUE(target_.ReleaseCpu(*eager_two));
    ASSERT_TRUE(receiver.Release(output[1].receipt));
    ASSERT_TRUE(receiver.Release(output[2].receipt));
    EXPECT_EQ(receiver.stats().released_prefix, 1u);
    EXPECT_EQ(receiver.stats().resident_bytes, 8u);
    EXPECT_FALSE(receiver.eof());
    ASSERT_TRUE(target_.ReleaseCpu(*target));
    ASSERT_TRUE(receiver.Release(output[0].receipt));
    EXPECT_EQ(receiver.stats().consumed_bytes, 16u);
    EXPECT_EQ(receiver.stats().resident_bytes, 0u);
    EXPECT_EQ(receiver.stats().released_prefix, 4u);
    EXPECT_EQ(target_.stats().available, 4u);
    EXPECT_TRUE(receiver.eof());
    EXPECT_TRUE(receiver.Close());
}
} // namespace
} // namespace connection
} // namespace kbsocket
