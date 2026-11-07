// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <cstddef>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/connection/connection_tokens.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/reorder_window.hpp"
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
} // namespace
} // namespace connection
} // namespace kbsocket
