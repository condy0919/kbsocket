// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/eager_rx.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <deque>
#include <limits>
#include <new>
#include <optional>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/connection/rndv_receiver.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
} // namespace
void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void operator delete(void* pointer) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    std::free(pointer);
}

namespace kbsocket {
namespace connection {
namespace {

class EagerRxTest : public ::testing::Test {
protected:
    struct Posted {
        std::uint64_t id;
        std::span<std::byte> memory;
    };
    static inline EagerRxTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto*, auto*) { return active_->jfc_count_++ == 0 ? &active_->tx_ : &active_->rx_; };
        f.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        f.create_jetty = [](auto*, auto*) {
            return active_->jetty_count_++ == 0 ? &active_->passive_ : &active_->jetty_;
        };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.register_seg = [](auto* context, auto* config) {
            auto& segment = active_->segments_[active_->registrations_++];
            segment.urma_ctx = context;
            segment.seg.ubva.eid = context->eid;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            segment.seg.attr.value = config->flag.value;
            segment.seg.token_id = static_cast<std::uint32_t>(active_->registrations_);
            return &segment;
        };
        f.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        f.post_jfr_wr = [](auto*, auto* first, auto** bad) {
            auto& s = *active_;
            ++s.post_calls_;
            std::size_t index = 0;
            *bad = nullptr;
            for (auto* wr = first; wr; wr = wr->next, ++index) {
                if (s.accepted_prefix_ && index >= *s.accepted_prefix_) {
                    if (!*bad) {
                        *bad = wr;
                    }
                    continue;
                }
                const auto& sge = wr->src.sge[0];
                s.posted_.push_back({wr->user_ctx, {reinterpret_cast<std::byte*>(sge.addr), sge.len}});
            }
            if (s.unknown_acceptance_) {
                *bad = nullptr;
            }
            return s.post_status_;
        };
        f.poll_jfc = [](auto*, int, auto* out) {
            if (!active_->completion_) {
                return 0;
            }
            *out = *active_->completion_;
            active_->completion_.reset();
            return 1;
        };
        return f;
    }
    void SetUp() override {
        active_ = this;
        passive_.jetty_id.id = 19;
        jetty_.jetty_id.id = 20;
        jfr_.jfr_id.id = 23;
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 1024;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        raw::JettyPoolConfig config;
        config.tx_depth = 8;
        config.rx_depth = config.rx_cq_depth = 8;
        ASSERT_TRUE(pool_.Open(&context_, cap, config));
        // 多配四个 buffer，验证 refill 不能把未归还的内存或初始 spare 当作新信用。
        ASSERT_TRUE(buffers_.Open(pool_, 12));
        auto initial = buffers_.Refill(8);
        ASSERT_TRUE(initial);
        ASSERT_EQ(*initial, 8u);
        ASSERT_TRUE(credits_.Open({33, 8, 2, 2, 1}));
        ASSERT_TRUE(credits_.RecordInitialPosted(CreditClass::kData, 6));
        ASSERT_TRUE(credits_.RecordInitialPosted(CreditClass::kControl, 2));
        auto peer = credits_.Admit({22, 33, 77});
        ASSERT_TRUE(peer);
        source_ = {{11, 33}, {kWireVersion, 22, 33, 44}, *peer};
        ASSERT_TRUE(credits_.Grant(*peer, CreditClass::kData, 6));
        ASSERT_TRUE(credits_.Grant(*peer, CreditClass::kControl, 2));
        ASSERT_TRUE(routes_.Open({11, 33}, 4));
        auto route = routes_.Reserve();
        ASSERT_TRUE(route);
        route_ = *route;
        binding_ = {source_.peer_context, route_.token, 55};
        ASSERT_TRUE(routes_.Publish(route_, binding_, this));
        ASSERT_TRUE(window_.Open({22, 33, 44, route_.token.slot, route_.token.generation}, {8, 32, 10000, 0}));
        ASSERT_TRUE(domain_.Open(buffers_, credits_, routes_));
        ASSERT_TRUE(stream_.Open(domain_, window_, binding_));
    }
    void TearDown() override {
        // 测试 owner 先释放应用 Lease；其余 mock WR 逐条提供可信终结 CQE，绝不靠析构假定 DMA 结束。
        stream_.Fail();
        for (int i = 0; i < 8 && stream_.pending(); ++i) {
            EXPECT_TRUE(stream_.Drain());
        }
        EXPECT_EQ(stream_.pending(), 0u);
        EXPECT_TRUE(stream_.Close());
        EXPECT_TRUE(window_.Close());
        if (!domain_.frozen()) {
            accepted_prefix_.reset();
            post_status_ = URMA_SUCCESS;
            EXPECT_TRUE(domain_.Refill());
            EXPECT_TRUE(domain_.Close());
        }
        while (!posted_.empty()) {
            const auto posted = posted_.front();
            posted_.pop_front();
            auto event = Complete(posted, 0, 1);
            ASSERT_TRUE(event.lease);
            EXPECT_TRUE(buffers_.Release(*event.lease));
        }
        EXPECT_EQ(buffers_.held(), 0u);
        EXPECT_EQ(buffers_.posted(), 0u);
        EXPECT_TRUE(buffers_.Close());
        EXPECT_TRUE(pool_.Close());
        active_ = nullptr;
    }
    raw::RxBufferEvent Complete(Posted posted, std::uint64_t imm, std::size_t length) {
        urma_cr_t cr{};
        cr.flag.bs.s_r = 1;
        cr.flag.bs.jetty = 1;
        cr.local_id = passive_.jetty_id.id;
        cr.user_ctx = posted.id;
        cr.completion_len = length;
        cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        cr.imm_data = imm;
        completion_ = cr;
        raw::RxBufferEvent event;
        auto polled = buffers_.Poll(std::span(&event, 1));
        EXPECT_TRUE(polled);
        if (polled) {
            EXPECT_EQ(*polled, 1u);
        }
        return event;
    }
    raw::RxBufferEvent Event(const WireFrameInput& input) {
        EXPECT_FALSE(posted_.empty());
        auto posted = posted_.front();
        posted_.pop_front();
        auto encoded = EncodeWireFrame(input, posted.memory);
        EXPECT_TRUE(encoded);
        if (!encoded) {
            return {};
        }
        return Complete(posted, encoded->imm_data, encoded->payload_bytes);
    }
    raw::RxBufferEvent Data(std::uint32_t seq, const std::string& payload) {
        WireFrameInput frame;
        frame.token = route_.token;
        frame.seq = seq;
        frame.data = {reinterpret_cast<const std::byte*>(payload.data()), payload.size()};
        return Event(frame);
    }
    static std::expected<EagerRxDisposition, EagerRxError> Handler(void* owner, EagerReceiveDomain&,
                                                                   const WireFrameView& view, EagerRxPacket packet,
                                                                   std::uint64_t now) noexcept {
        auto& s = *static_cast<EagerRxTest*>(owner);
        ++s.handler_calls_;
        s.last_packet_ = packet;
        if (view.frame.type == WireFrameType::kControl && view.frame.control != WireControlType::kFin) {
            return EagerRxDisposition::kRelease;
        }
        auto accepted = s.stream_.Accept(packet, now);
        if (!accepted) {
            return std::unexpected(accepted.error());
        }
        return EagerRxDisposition::kHeld;
    }
    std::expected<void, EagerRxError> Dispatch(raw::RxBufferEvent event) {
        return domain_.Dispatch(event, source_, Handler, ++now_);
    }
    static std::expected<EagerRxDisposition, EagerRxError> EagerOnlyHandler(void* owner, EagerReceiveDomain&,
                                                                            const WireFrameView&, EagerRxPacket packet,
                                                                            std::uint64_t now) noexcept {
        auto& self = *static_cast<EagerRxTest*>(owner);
        auto accepted = self.stream_.Accept(packet, now);
        if (!accepted) {
            return std::unexpected(accepted.error());
        }
        return EagerRxDisposition::kHeld;
    }
    std::string Read(std::size_t bytes) {
        std::array<std::byte, 128> output{};
        auto read = stream_.Read(std::span(output).first(bytes), ++now_);
        EXPECT_TRUE(read);
        if (!read) {
            return {};
        }
        return {reinterpret_cast<const char*>(output.data()), read->bytes};
    }

    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t passive_{}, jetty_{};
    std::array<urma_target_seg_t, 4> segments_{};
    std::size_t registrations_ = 0;
    raw::JettyPool pool_;
    raw::RxBufferPool buffers_;
    ReceiveCreditDomain credits_;
    ConnectionTable routes_;
    ReorderWindow window_;
    EagerReceiveDomain domain_;
    EagerRxStream stream_;
    EagerReceiveSource source_;
    WireSessionBinding binding_;
    ConnectionRouteLease route_;
    EagerRxPacket last_packet_;
    std::deque<Posted> posted_;
    std::optional<urma_cr_t> completion_;
    std::optional<std::size_t> accepted_prefix_;
    urma_status_t post_status_ = URMA_SUCCESS;
    bool unknown_acceptance_ = false;
    std::size_t post_calls_ = 0, handler_calls_ = 0;
    int jfc_count_ = 0, jetty_count_ = 0;
    std::uint64_t now_ = 0;
};

TEST_F(EagerRxTest, OutOfOrderAndShortReadHoldRealLeaseUntilFullyConsumed) {
    ASSERT_TRUE(Dispatch(Data(2, "ef")));
    EXPECT_FALSE(stream_.readable());
    EXPECT_EQ(Read(8), "");
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    EXPECT_TRUE(stream_.readable());
    EXPECT_EQ(Read(1), "a");
    EXPECT_TRUE(stream_.readable());
    EXPECT_EQ(buffers_.held(), 2u);
    EXPECT_EQ(credits_.stats().held, 2u);
    EXPECT_EQ(window_.stats().consumed_bytes, 0u);
    EXPECT_EQ(domain_.pending_refill(), 0u);
    // 短读不能释放底层内存，seq=2,0,3,1 最终必须按字节流顺序输出。
    ASSERT_TRUE(Dispatch(Data(3, "gh")));
    ASSERT_TRUE(Dispatch(Data(1, "cd")));
    EXPECT_EQ(Read(7), "bcdefgh");
    EXPECT_FALSE(stream_.readable());
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(credits_.stats().released, 4u);
    EXPECT_EQ(window_.stats().consumed_bytes, 8u);
    EXPECT_EQ(domain_.pending_refill(), 4u);
    auto refill = domain_.Refill();
    ASSERT_TRUE(refill);
    EXPECT_EQ(*refill, 4u);
    EXPECT_EQ(credits_.stats().physical_posted, 8u);
    EXPECT_EQ(credits_.stats().data.free, 4u);
    EXPECT_EQ(credits_.stats().released, 0u);
    EXPECT_FALSE(domain_.Release(last_packet_));
}

TEST_F(EagerRxTest, RndvRtsUsesDataCreditWhileGrantAndDoneUseControlCredit) {
    WireFrameInput frame;
    frame.type = WireFrameType::kControl;
    frame.control = WireControlType::kRndvRts;
    frame.token = route_.token;
    frame.session = binding_.context.session;
    frame.rndv = {.sender_runtime = 22,
                  .receiver_runtime = 11,
                  .receive_domain = 33,
                  .session = 44,
                  .token = route_.token,
                  .transfer_id = 1,
                  .bytes = 16};
    // 此 handler 仅释放控制包；codec 开放不代表业务入口已经启用 RNDV。
    ASSERT_TRUE(Dispatch(Event(frame)));
    EXPECT_EQ(credits_.stats().data.granted, 5u);
    EXPECT_EQ(credits_.stats().control.granted, 2u);
    EXPECT_EQ(window_.stats().received_bytes, 0u);
    frame.rndv.arena_id = 2;
    frame.rndv.arena_generation = 3;
    frame.rndv.grant_generation = 4;
    frame.rndv.target_address = 4096;
    for (auto control : {WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        frame.control = control;
        ASSERT_TRUE(Dispatch(Event(frame)));
    }
    EXPECT_EQ(credits_.stats().data.granted, 5u);
    EXPECT_EQ(credits_.stats().control.granted, 0u);
    EXPECT_EQ(domain_.pending_refill(), 3u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_FALSE(domain_.frozen());
}

TEST_F(EagerRxTest, EagerStreamRejectsRndvWithoutPublishingDataOrLeakingRx) {
    WireFrameInput frame;
    frame.type = WireFrameType::kControl;
    frame.control = WireControlType::kRndvRts;
    frame.token = route_.token;
    frame.session = binding_.context.session;
    frame.rndv = {.sender_runtime = 22,
                  .receiver_runtime = 11,
                  .receive_domain = 33,
                  .session = 44,
                  .token = route_.token,
                  .transfer_id = 1,
                  .bytes = 16};
    // 模拟当前 Connection 仍交给 EagerRxStream 的路径；不静默吞掉或交付不支持的事务。
    auto result = domain_.Dispatch(Event(frame), source_, EagerOnlyHandler, ++now_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kUnsupportedFrame);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_FALSE(stream_.readable());
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(credits_.stats().data.granted, 5u);
    EXPECT_EQ(credits_.stats().control.granted, 2u);
}

TEST_F(EagerRxTest, EagerStreamCannotBindWindowWithPendingRndvReservation) {
    const ReorderSession session{22, 33, 44, route_.token.slot, route_.token.generation};
    ReorderWindow occupied;
    EagerRxStream candidate;
    ASSERT_TRUE(occupied.Open(session, {8, 32, 10000, 0}));
    auto pending = occupied.ReserveRndv(session, 0, 16, ++now_);
    ASSERT_TRUE(pending);
    auto opened = candidate.Open(domain_, occupied, binding_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EagerRxErrorCode::kInvalidArgument);
    // 移除的仅为占位元数据，此测试未创建或发布远端 Grant。
    occupied.StopAccepting();
    ASSERT_TRUE(occupied.AbandonRndv(*pending));
    EXPECT_TRUE(occupied.Close());
}

TEST_F(EagerRxTest, ReadLeaseBorrowsActualRxPrefixAndReturnsCreditOnlyAfterFullReleaseAndRefill) {
    auto event = Data(0, "abcdef");
    const auto* address = event.payload.data();
    ASSERT_TRUE(Dispatch(event));
    const auto posted = credits_.stats().physical_posted;
    const auto post_calls = post_calls_;
    const auto first = stream_.ReadLease(2, ++now_);
    ASSERT_TRUE(first);
    EXPECT_NE(first->generation, 0u);
    EXPECT_EQ(first->data.data(), address);
    EXPECT_EQ(first->data.size(), 2u);
    EXPECT_EQ(first->data[0], std::byte{'a'});
    EXPECT_FALSE(first->eof);
    EXPECT_FALSE(first->would_block);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(credits_.stats().held, 1u);
    EXPECT_EQ(window_.stats().resident_bytes, 6u);
    EXPECT_EQ(window_.stats().consumed_bytes, 0u);
    // 应用仍持有指针时，补投不得借 spare 槽绕过未归还的 receipt。
    const auto early_refill = domain_.Refill();
    ASSERT_TRUE(early_refill);
    EXPECT_EQ(*early_refill, 0u);
    EXPECT_EQ(post_calls_, post_calls);
    EXPECT_EQ(credits_.stats().physical_posted, posted);
    ASSERT_TRUE(stream_.ReleaseLease(first->generation));
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(credits_.stats().held, 1u);
    EXPECT_EQ(domain_.pending_refill(), 0u);
    EXPECT_EQ(window_.stats().consumed_bytes, 0u);
    const auto rest = stream_.ReadLease(100, ++now_);
    ASSERT_TRUE(rest);
    EXPECT_GT(rest->generation, first->generation);
    EXPECT_EQ(rest->data.data(), address + 2);
    EXPECT_EQ(rest->data.size(), 4u);
    EXPECT_EQ(rest->data[0], std::byte{'c'});
    ASSERT_TRUE(stream_.ReleaseLease(rest->generation));
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(window_.stats().consumed_bytes, 6u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(credits_.stats().released, 1u);
    EXPECT_EQ(credits_.stats().data.free, 0u);
    const auto refilled = domain_.Refill();
    ASSERT_TRUE(refilled);
    EXPECT_EQ(*refilled, 1u);
    EXPECT_EQ(credits_.stats().data.free, 1u);
    EXPECT_EQ(credits_.stats().physical_posted, 8u);
}

TEST_F(EagerRxTest, CopyAndBorrowShareOneOffsetWithoutRepeatingOrSkippingBytes) {
    auto event = Data(0, "abcdef");
    const auto* address = event.payload.data();
    ASSERT_TRUE(Dispatch(event));
    ASSERT_TRUE(Dispatch(Data(1, "gh")));
    EXPECT_EQ(Read(2), "ab");
    const auto borrowed = stream_.ReadLease(2, ++now_);
    ASSERT_TRUE(borrowed);
    EXPECT_EQ(borrowed->data.data(), address + 2);
    EXPECT_EQ(borrowed->data[0], std::byte{'c'});
    EXPECT_EQ(borrowed->data[1], std::byte{'d'});
    ASSERT_TRUE(stream_.ReleaseLease(borrowed->generation));
    EXPECT_EQ(Read(4), "efgh");
    EXPECT_EQ(window_.stats().consumed_bytes, 8u);
    EXPECT_EQ(domain_.pending_refill(), 2u);
}

TEST_F(EagerRxTest, BorrowedViewBlocksSecondReadAndInvalidTokensWithoutFailingStream) {
    ASSERT_TRUE(Dispatch(Data(0, "abcd")));
    ASSERT_TRUE(Dispatch(Data(1, "ef")));
    const auto borrowed = stream_.ReadLease(2, ++now_);
    ASSERT_TRUE(borrowed);
    EXPECT_FALSE(stream_.readable());
    EXPECT_FALSE(stream_.eof());
    const auto second = stream_.ReadLease(2, ++now_);
    ASSERT_FALSE(second);
    EXPECT_EQ(second.error().code, EagerRxErrorCode::kInUse);
    std::array<std::byte, 8> output{};
    const auto copied = stream_.Read(output, ++now_);
    ASSERT_FALSE(copied);
    EXPECT_EQ(copied.error().code, EagerRxErrorCode::kInUse);
    for (const auto invalid : {std::uint64_t{0}, borrowed->generation + 1, std::numeric_limits<std::uint64_t>::max()}) {
        const auto released = stream_.ReleaseLease(invalid);
        ASSERT_FALSE(released);
        EXPECT_EQ(released.error().code, EagerRxErrorCode::kInvalidLease);
    }
    EXPECT_FALSE(stream_.failed());
    EXPECT_EQ(buffers_.held(), 2u);
    EXPECT_EQ(borrowed->data[0], std::byte{'a'});
    ASSERT_TRUE(stream_.ReleaseLease(borrowed->generation));
    const auto twice = stream_.ReleaseLease(borrowed->generation);
    ASSERT_FALSE(twice);
    EXPECT_EQ(twice.error().code, EagerRxErrorCode::kInvalidLease);
    EXPECT_FALSE(stream_.failed());
    EXPECT_TRUE(stream_.readable());
    EXPECT_EQ(Read(8), "cdef");
}

TEST_F(EagerRxTest, ReadLeaseWaitsForMissingSequenceAndBorrowsOnlyOneFrame) {
    ASSERT_TRUE(Dispatch(Data(1, "cd")));
    const auto waiting = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(waiting);
    EXPECT_TRUE(waiting->would_block);
    EXPECT_FALSE(waiting->eof);
    EXPECT_EQ(waiting->generation, 0u);
    EXPECT_TRUE(waiting->data.empty());
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    const auto first = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->data.size(), 2u);
    EXPECT_EQ(first->data[0], std::byte{'a'});
    EXPECT_FALSE(stream_.readable());
    ASSERT_TRUE(stream_.ReleaseLease(first->generation));
    EXPECT_TRUE(stream_.readable());
    const auto next = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(next);
    EXPECT_EQ(next->data.size(), 2u);
    EXPECT_EQ(next->data[0], std::byte{'c'});
    ASSERT_TRUE(stream_.ReleaseLease(next->generation));
}

TEST_F(EagerRxTest, HeldDataLeaseKeepsOrderedFinHiddenUntilReleasedAndRead) {
    WireFrameInput fin;
    fin.type = WireFrameType::kControl;
    fin.control = WireControlType::kFin;
    fin.token = route_.token;
    fin.seq = 1;
    fin.session = binding_.context.session;
    ASSERT_TRUE(Dispatch(Event(fin)));
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    const auto data = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(data);
    EXPECT_EQ(data->data.size(), 2u);
    EXPECT_FALSE(data->eof);
    EXPECT_FALSE(stream_.readable());
    EXPECT_FALSE(stream_.eof());
    EXPECT_EQ(buffers_.held(), 2u);
    ASSERT_TRUE(stream_.ReleaseLease(data->generation));
    EXPECT_FALSE(stream_.eof());
    EXPECT_TRUE(stream_.readable());
    const auto end = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(end);
    EXPECT_EQ(end->generation, 0u);
    EXPECT_TRUE(end->data.empty());
    EXPECT_TRUE(end->eof);
    EXPECT_FALSE(end->would_block);
    EXPECT_TRUE(stream_.eof());
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(domain_.pending_refill(), 2u);
    const auto repeated = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(repeated);
    EXPECT_TRUE(repeated->eof);
    EXPECT_EQ(repeated->generation, 0u);
    EXPECT_FALSE(stream_.ReleaseLease(0));
}

TEST_F(EagerRxTest, FailureAndDrainCannotReclaimAnApplicationHeldPrefix) {
    ASSERT_TRUE(Dispatch(Data(0, "abcd")));
    ASSERT_TRUE(Dispatch(Data(6, "z")));
    const auto borrowed = stream_.ReadLease(2, ++now_);
    ASSERT_TRUE(borrowed);
    stream_.Fail();
    // 失败只停止后续交付；原指针与 sparse 后继均留待应用归还后再按预算清退。
    const auto drained = stream_.Drain();
    ASSERT_TRUE(drained);
    EXPECT_EQ(*drained, 0u);
    EXPECT_FALSE(stream_.Close());
    EXPECT_EQ(buffers_.held(), 2u);
    EXPECT_EQ(domain_.pending_refill(), 0u);
    EXPECT_EQ(borrowed->data[0], std::byte{'a'});
    EXPECT_EQ(borrowed->data[1], std::byte{'b'});
    EXPECT_FALSE(stream_.ReadLease(1, ++now_));
    ASSERT_TRUE(stream_.ReleaseLease(borrowed->generation));
    EXPECT_EQ(buffers_.held(), 2u); // 仅归还前缀；余下字节尚由本流清理。
    const auto cleanup = stream_.Drain();
    ASSERT_TRUE(cleanup);
    EXPECT_EQ(*cleanup, 2u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_TRUE(stream_.Close());
}

TEST_F(EagerRxTest, CloseRetainsWholeBorrowedFrameUntilExactLeaseReturns) {
    ASSERT_TRUE(Dispatch(Data(0, "abc")));
    const auto borrowed = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(borrowed);
    const auto closed = stream_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, EagerRxErrorCode::kInUse);
    EXPECT_FALSE(stream_.readable());
    const auto drained = stream_.Drain();
    ASSERT_TRUE(drained);
    EXPECT_EQ(*drained, 0u);
    EXPECT_EQ(borrowed->data[2], std::byte{'c'});
    ASSERT_TRUE(stream_.ReleaseLease(borrowed->generation));
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(credits_.stats().released, 1u);
    EXPECT_TRUE(stream_.Close());
}

TEST_F(EagerRxTest, ReceiveLeaseGenerationNeverRestartsAcrossStreamReopen) {
    ASSERT_TRUE(Dispatch(Data(0, "old")));
    const auto old = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(old);
    ASSERT_TRUE(stream_.ReleaseLease(old->generation));
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(window_.Close());
    ASSERT_TRUE(domain_.Refill());
    ASSERT_TRUE(window_.Open({22, 33, 44, route_.token.slot, route_.token.generation}, {8, 32, 10000, 0}));
    ASSERT_TRUE(stream_.Open(domain_, window_, binding_));
    ASSERT_TRUE(Dispatch(Data(0, "new")));
    const auto current = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(current);
    EXPECT_GT(current->generation, old->generation);
    const auto stale = stream_.ReleaseLease(old->generation);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error().code, EagerRxErrorCode::kInvalidLease);
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(current->data[0], std::byte{'n'});
    ASSERT_TRUE(stream_.ReleaseLease(current->generation));
}

TEST_F(EagerRxTest, ZeroLengthLeaseReadRejectsWithoutDeliveringOrPoisoningCurrentFrame) {
    ASSERT_TRUE(Dispatch(Data(0, "a")));
    const auto result = stream_.ReadLease(0, ++now_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kInvalidArgument);
    EXPECT_FALSE(stream_.failed());
    EXPECT_EQ(window_.stats().buffered_frames, 1u);
    EXPECT_EQ(window_.stats().delivered_frames, 0u);
    EXPECT_EQ(Read(1), "a");
}

TEST_F(EagerRxTest, BorrowPartialReleaseWholeReleaseAndRefillDoNotAllocate) {
    auto event = Data(0, "abcd");
    allocations = 0;
    count_allocations = true;
    const auto dispatched = Dispatch(event);
    const auto first = stream_.ReadLease(2, ++now_);
    const auto first_released = stream_.ReleaseLease(first ? first->generation : 0);
    const auto second = stream_.ReadLease(8, ++now_);
    const auto second_released = stream_.ReleaseLease(second ? second->generation : 0);
    const auto refilled = domain_.Refill();
    count_allocations = false;
    EXPECT_EQ(allocations, 0u);
    ASSERT_TRUE(dispatched);
    ASSERT_TRUE(first);
    ASSERT_TRUE(first_released);
    ASSERT_TRUE(second);
    ASSERT_TRUE(second_released);
    ASSERT_TRUE(refilled);
    EXPECT_EQ(first->data.size(), 2u);
    EXPECT_EQ(second->data.size(), 2u);
    EXPECT_EQ(*refilled, 1u);
}

TEST_F(EagerRxTest, StaleTokenStillDebitsOriginalPhysicalGrantBeforeRouteRejection) {
    auto event = Data(0, "old");
    ASSERT_TRUE(routes_.Retire(route_));
    auto result = Dispatch(event);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kRouteRejected);
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(credits_.stats().data.granted, 5u);
    EXPECT_EQ(credits_.stats().physical_posted, 7u);
    EXPECT_EQ(credits_.stats().released, 1u);
    EXPECT_EQ(buffers_.held(), 0u);
    ASSERT_TRUE(domain_.Refill());
    EXPECT_EQ(credits_.stats().data.free, 1u);
    EXPECT_FALSE(domain_.frozen());
}

TEST_F(EagerRxTest, MalformedWireConsumesUnknownAndFreezesWithoutRegrant) {
    auto event = Data(0, "bad");
    event.completion.imm_data = 0xf000000000000000ULL;
    auto result = Dispatch(event);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kMalformedFrame);
    EXPECT_TRUE(domain_.frozen());
    EXPECT_EQ(credits_.stats().unattributed, 1u);
    EXPECT_EQ(credits_.stats().physical_posted, 7u);
    EXPECT_EQ(credits_.stats().data.granted, 6u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_FALSE(domain_.Refill());
    EXPECT_FALSE(domain_.Close());
}

TEST_F(EagerRxTest, UntrustedSourceDoesNotBorrowExpectedRouteIdentity) {
    auto event = Data(0, "bad");
    source_.peer_context.peer_runtime = 999;
    auto result = Dispatch(event);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kInvalidSource);
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(credits_.stats().unattributed, 1u);
    EXPECT_TRUE(domain_.frozen());
}

TEST_F(EagerRxTest, TrustedPartialRefillRestoresOnlyAcceptedReceiptPrefix) {
    ASSERT_TRUE(Dispatch(Data(0, "a")));
    ASSERT_TRUE(Dispatch(Data(1, "b")));
    ASSERT_TRUE(Dispatch(Data(2, "c")));
    EXPECT_EQ(Read(3), "abc");
    accepted_prefix_ = 1;
    post_status_ = URMA_EAGAIN;
    auto refill = domain_.Refill();
    ASSERT_FALSE(refill);
    EXPECT_TRUE(refill.error().accepted_known);
    EXPECT_EQ(refill.error().accepted, 1u);
    EXPECT_EQ(domain_.pending_refill(), 2u);
    EXPECT_EQ(credits_.stats().released, 2u);
    EXPECT_EQ(credits_.stats().data.free, 1u);
    EXPECT_EQ(credits_.stats().physical_posted, 6u);
    EXPECT_FALSE(domain_.frozen());
    accepted_prefix_.reset();
    post_status_ = URMA_SUCCESS;
    ASSERT_TRUE(domain_.Refill());
    EXPECT_EQ(domain_.pending_refill(), 0u);
    EXPECT_EQ(credits_.stats().physical_posted, 8u);
}

TEST_F(EagerRxTest, UnknownRefillAcceptanceRetainsReceiptAndStopsDomain) {
    ASSERT_TRUE(Dispatch(Data(0, "a")));
    EXPECT_EQ(Read(1), "a");
    // provider 返回错误却不给 bad_wr；真实 DMA 仍可能已接受，不能凭错误码返还信用。
    post_status_ = URMA_ENOMEM;
    unknown_acceptance_ = true;
    auto refill = domain_.Refill();
    ASSERT_FALSE(refill);
    EXPECT_FALSE(refill.error().accepted_known);
    EXPECT_TRUE(domain_.frozen());
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(credits_.stats().released, 1u);
    EXPECT_EQ(credits_.stats().physical_posted, 7u);
    EXPECT_EQ(buffers_.posted(), 8u);
}

TEST_F(EagerRxTest, FinWaitsForOrderedDataAndFullApplicationConsumption) {
    WireFrameInput fin;
    fin.type = WireFrameType::kControl;
    fin.control = WireControlType::kFin;
    fin.token = route_.token;
    fin.seq = 1;
    fin.session = binding_.context.session;
    ASSERT_TRUE(Dispatch(Event(fin)));
    EXPECT_FALSE(stream_.readable());
    EXPECT_FALSE(stream_.eof());
    EXPECT_EQ(Read(4), "");
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    EXPECT_EQ(Read(1), "a");
    EXPECT_TRUE(stream_.readable());
    EXPECT_FALSE(stream_.eof());
    EXPECT_EQ(Read(4), "b");
    EXPECT_TRUE(stream_.eof());
    EXPECT_TRUE(stream_.readable());
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(credits_.stats().released, 2u);
    EXPECT_EQ(credits_.stats().control.granted, 1u);
}

TEST_F(EagerRxTest, FinOnlyBecomesReadableBeforeReadConsumesEof) {
    WireFrameInput fin;
    fin.type = WireFrameType::kControl;
    fin.control = WireControlType::kFin;
    fin.token = route_.token;
    fin.session = binding_.context.session;
    ASSERT_TRUE(Dispatch(Event(fin)));
    // FIN 虽然没有业务字节，仍应给应用一次 Read 机会，按序消费后再释放 RX Lease。
    EXPECT_EQ(stream_.pending(), 1u);
    EXPECT_TRUE(stream_.readable());
    EXPECT_FALSE(stream_.eof());
    EXPECT_EQ(Read(1), "");
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_TRUE(stream_.eof());
    EXPECT_TRUE(stream_.readable());
}

TEST_F(EagerRxTest, CreditControlReleasesWithoutEnteringReorderWindow) {
    WireFrameInput credit;
    credit.type = WireFrameType::kControl;
    credit.control = WireControlType::kCreditUpdate;
    credit.token = route_.token;
    credit.session = binding_.context.session;
    credit.credit_epoch = binding_.credit_epoch;
    ASSERT_TRUE(Dispatch(Event(credit)));
    EXPECT_EQ(handler_calls_, 1u);
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_EQ(credits_.stats().control.granted, 1u);
}

TEST_F(EagerRxTest, SourceBoundCreditRejectsWrongSessionTokenAndEpochBeforeUpdatingSharedAccount) {
    SenderCreditAccount sender;
    const CreditIdentity identity{22, 66, binding_.credit_epoch};
    ASSERT_TRUE(sender.Open(identity));
    ASSERT_TRUE(sender.Update(identity, 1, 2));
    source_.sender_credits = &sender;
    source_.sender_identity = identity;
    source_.token = route_.token;
    const WireFrameInput credit{.type = WireFrameType::kControl,
                                .token = route_.token,
                                .data = {},
                                .max_credit_seq = 100,
                                .max_control_credit_seq = 100,
                                .session = binding_.context.session,
                                .credit_epoch = binding_.credit_epoch};
    // 来源物理归属虽可信，内容仍须逐帧验证；三种身份错配都不更新对向共账或业务窗口。
    for (int mismatch = 0; mismatch < 3; ++mismatch) {
        auto invalid = credit;
        if (mismatch == 0) {
            ++invalid.session;
        } else if (mismatch == 1) {
            ++invalid.token.generation;
        } else {
            ++invalid.credit_epoch;
        }
        const auto dispatched = Dispatch(Event(invalid));
        ASSERT_FALSE(dispatched);
        EXPECT_EQ(dispatched.error().code, EagerRxErrorCode::kCreditFailed);
        EXPECT_EQ(sender.stats().limits, (std::array<std::uint64_t, 2>{1, 2}));
        EXPECT_EQ(handler_calls_, 0u);
        EXPECT_FALSE(domain_.frozen());
        ASSERT_EQ(domain_.Refill(), 1u);
        ASSERT_TRUE(credits_.Grant(source_.peer, CreditClass::kControl, 1));
    }
    EXPECT_TRUE(sender.Close());
    source_.sender_credits = nullptr;
}

TEST_F(EagerRxTest, DelayedCreditCannotUpdateReopenedAccountEpoch) {
    SenderCreditAccount sender;
    CreditIdentity identity{22, 66, binding_.credit_epoch};
    ASSERT_TRUE(sender.Open(identity));
    source_.sender_credits = &sender;
    source_.sender_identity = identity;
    source_.token = route_.token;
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .token = route_.token,
                          .data = {},
                          .max_credit_seq = 100,
                          .max_control_credit_seq = 100,
                          .session = binding_.context.session,
                          .credit_epoch = binding_.credit_epoch};
    const auto delayed = Event(credit);
    ASSERT_TRUE(sender.Close());
    ++identity.epoch;
    ASSERT_TRUE(sender.Open(identity));
    ASSERT_TRUE(sender.Update(identity, 3, 4));
    const auto dispatched = Dispatch(delayed);
    ASSERT_FALSE(dispatched);
    EXPECT_EQ(dispatched.error().code, EagerRxErrorCode::kCreditFailed);
    EXPECT_EQ(sender.stats().limits, (std::array<std::uint64_t, 2>{3, 4}));
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    EXPECT_TRUE(sender.Close());
    source_.sender_credits = nullptr;
}

TEST_F(EagerRxTest, CreditAfterRouteRetirementUpdatesPeerButLeavesReplacementWindowUntouched) {
    SenderCreditAccount sender;
    const CreditIdentity identity{22, 66, binding_.credit_epoch};
    ASSERT_TRUE(sender.Open(identity));
    source_.sender_credits = &sender;
    source_.sender_identity = identity;
    source_.token = route_.token;
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .token = route_.token,
                          .data = {},
                          .max_credit_seq = 12,
                          .max_control_credit_seq = 8,
                          .session = binding_.context.session,
                          .credit_epoch = binding_.credit_epoch,
                          .max_frame_seq = 1234,
                          .max_stream_bytes = 5678};
    const auto delayed = Event(credit);
    ASSERT_TRUE(routes_.Retire(route_));
    const auto replacement = routes_.Reserve();
    ASSERT_TRUE(replacement);
    auto binding = binding_;
    binding.token = replacement->token;
    ++binding.context.session;
    ReorderWindow sibling;
    ASSERT_TRUE(sibling.Open({22, 33, binding.context.session, binding.token.slot, binding.token.generation},
                             {4, 16, 10000, 0}));
    ASSERT_TRUE(routes_.Publish(*replacement, binding, &sibling));
    const auto dispatched = Dispatch(delayed);
    ASSERT_FALSE(dispatched);
    EXPECT_EQ(dispatched.error().code, EagerRxErrorCode::kRouteRejected);
    EXPECT_EQ(sender.stats().limits, (std::array<std::uint64_t, 2>{12, 8}));
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(sibling.stats().max_frame_seq, 4u);
    EXPECT_EQ(sibling.stats().max_stream_bytes, 16u);
    EXPECT_EQ(sibling.stats().pending_frames, 0u);
    EXPECT_TRUE(sibling.Close());
    EXPECT_TRUE(routes_.Retire(*replacement));
    EXPECT_TRUE(sender.Close());
    source_.sender_credits = nullptr;
}

TEST_F(EagerRxTest, DelayedCreditRecordsLimitsWithoutReopeningStoppedAccount) {
    SenderCreditAccount sender;
    const CreditIdentity identity{22, 66, binding_.credit_epoch};
    ASSERT_TRUE(sender.Open(identity));
    source_.sender_credits = &sender;
    source_.sender_identity = identity;
    source_.token = route_.token;
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .token = route_.token,
                          .data = {},
                          .max_credit_seq = 12,
                          .max_control_credit_seq = 8,
                          .session = binding_.context.session,
                          .credit_epoch = binding_.credit_epoch};
    const auto delayed = Event(credit);
    sender.Stop();
    ASSERT_TRUE(Dispatch(delayed));
    EXPECT_EQ(sender.stats().limits, (std::array<std::uint64_t, 2>{12, 8}));
    EXPECT_TRUE(sender.stats().stopped);
    EXPECT_EQ(sender.available(CreditClass::kData), 0u);
    EXPECT_EQ(sender.available(CreditClass::kControl), 0u);
    EXPECT_TRUE(sender.Close());
    source_.sender_credits = nullptr;
}

TEST_F(EagerRxTest, InvalidCompletionNeverUpdatesSourceBoundCredit) {
    SenderCreditAccount sender;
    const CreditIdentity identity{22, 66, binding_.credit_epoch};
    ASSERT_TRUE(sender.Open(identity));
    source_.sender_credits = &sender;
    source_.sender_identity = identity;
    source_.token = route_.token;
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .token = route_.token,
                          .data = {},
                          .max_credit_seq = 12,
                          .max_control_credit_seq = 8,
                          .session = binding_.context.session,
                          .credit_epoch = binding_.credit_epoch};
    auto invalid = Event(credit);
    ++invalid.completion.completion_len;
    EXPECT_FALSE(Dispatch(invalid));
    EXPECT_EQ(sender.stats().limits, (std::array<std::uint64_t, 2>{0, 0}));
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_TRUE(domain_.frozen());
    EXPECT_TRUE(sender.Close());
    source_.sender_credits = nullptr;
}

TEST_F(EagerRxTest, PiggybackCreditUsesRetainedSourceEpochAfterRouteRetirement) {
    SenderCreditAccount sender;
    const CreditIdentity identity{22, 66, binding_.credit_epoch};
    ASSERT_TRUE(sender.Open(identity));
    ASSERT_TRUE(sender.Update(identity, 1, 2));
    source_.sender_credits = &sender;
    source_.sender_identity = identity;
    source_.token = route_.token;
    const std::array data{std::byte{0x12}};
    WireFrameInput frame{
        .type = WireFrameType::kDataWithCredit, .token = route_.token, .data = data, .max_credit_seq = 17};
    const auto delayed = Event(frame);
    ASSERT_TRUE(routes_.Retire(route_));
    const auto dispatched = Dispatch(delayed);
    ASSERT_FALSE(dispatched);
    EXPECT_EQ(dispatched.error().code, EagerRxErrorCode::kRouteRejected);
    EXPECT_EQ(sender.stats().limits, (std::array<std::uint64_t, 2>{17, 2}));
    EXPECT_EQ(handler_calls_, 0u);
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_TRUE(sender.Close());
    source_.sender_credits = nullptr;
}

TEST_F(EagerRxTest, FailureDrainsSparseAndPartiallyReadFramesExactlyOnce) {
    ASSERT_TRUE(Dispatch(Data(0, "abcd")));
    EXPECT_EQ(Read(1), "a");
    ASSERT_TRUE(Dispatch(Data(6, "z")));
    EXPECT_FALSE(stream_.Close());
    EXPECT_EQ(buffers_.held(), 2u);
    stream_.Fail();
    auto drained = stream_.Drain(8);
    ASSERT_TRUE(drained);
    EXPECT_EQ(*drained, 2u);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(domain_.pending_refill(), 2u);
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_FALSE(stream_.eof());
    EXPECT_TRUE(stream_.Close());
}

TEST_F(EagerRxTest, SlowReaderKeepsByteResidenceAndRejectsExcessInput) {
    ASSERT_TRUE(Dispatch(Data(0, std::string(32, 'a'))));
    EXPECT_EQ(Read(1), "a");
    EXPECT_EQ(window_.stats().resident_bytes, 32u);
    auto overflow = Dispatch(Data(1, "x"));
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error().code, EagerRxErrorCode::kReorderFailed);
    // 被拒帧由 dispatcher 归还，原短读 Lease 仍由流负责 drain。
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
    ASSERT_TRUE(stream_.Drain());
    EXPECT_EQ(buffers_.held(), 0u);
}

TEST_F(EagerRxTest, DuplicateEventCannotConsumeAnotherPhysicalGrant) {
    auto event = Data(0, "a");
    ASSERT_TRUE(Dispatch(event));
    const auto physical = credits_.stats().physical_posted;
    EXPECT_FALSE(Dispatch(event));
    EXPECT_EQ(credits_.stats().physical_posted, physical);
    EXPECT_EQ(credits_.stats().unattributed, 0u);
    EXPECT_TRUE(domain_.frozen());
}

TEST_F(EagerRxTest, InitialUnpostedCapacityCannotBecomeRefillCredit) {
    ReceiveCreditDomain incomplete;
    ASSERT_TRUE(incomplete.Open({33, 16, 2, 1, 1}));
    ASSERT_TRUE(incomplete.RecordInitialPosted(CreditClass::kData, 6));
    ASSERT_TRUE(incomplete.RecordInitialPosted(CreditClass::kControl, 2));
    EagerReceiveDomain invalid;
    auto opened = invalid.Open(buffers_, incomplete, routes_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EagerRxErrorCode::kInvalidArgument);
    EXPECT_TRUE(domain_.Matches(credits_, routes_));
    EXPECT_FALSE(domain_.Matches(incomplete, routes_));
}

TEST_F(EagerRxTest, ReceiptSlotReuseRejectsOldPacketWithoutReleasingNewLease) {
    ASSERT_TRUE(Dispatch(Data(0, "a")));
    const auto old = last_packet_;
    EXPECT_EQ(Read(1), "a");
    ASSERT_TRUE(domain_.Refill());
    ASSERT_TRUE(Dispatch(Data(1, "b")));
    EXPECT_EQ(last_packet_.slot, old.slot);
    EXPECT_NE(last_packet_.id, old.id);
    EXPECT_FALSE(domain_.View(old));
    EXPECT_FALSE(domain_.Release(old));
    EXPECT_EQ(buffers_.held(), 1u);
    EXPECT_EQ(credits_.stats().held, 1u);
    EXPECT_EQ(Read(1), "b");
}

TEST_F(EagerRxTest, MissingHandlerStillAccountsAndReleasesRealCompletion) {
    auto result = domain_.Dispatch(Data(0, "a"), source_, nullptr, ++now_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kInvalidArgument);
    EXPECT_EQ(buffers_.held(), 0u);
    EXPECT_EQ(credits_.stats().physical_posted, 7u);
    EXPECT_EQ(domain_.pending_refill(), 1u);
}

TEST_F(EagerRxTest, DispatchShortReadReleaseAndActualRefillDoNotAllocate) {
    auto event = Data(0, "ab");
    std::array<std::byte, 1> output{};
    allocations = 0;
    count_allocations = true;
    const auto dispatched = Dispatch(event);
    const auto first = stream_.Read(output, ++now_);
    const auto second = stream_.Read(output, ++now_);
    const auto refilled = domain_.Refill();
    count_allocations = false;
    EXPECT_EQ(allocations, 0u);
    ASSERT_TRUE(dispatched);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(refilled);
    EXPECT_EQ(first->bytes, 1u);
    EXPECT_EQ(second->bytes, 1u);
    EXPECT_EQ(*refilled, 1u);
}

TEST_F(EagerRxTest, FailureAfterValidatedPrefixPreservesCopiedBytesAndExposesFailure) {
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    ASSERT_TRUE(Dispatch(Data(2, "cd")));
    // 缺口后发生失败，已验证的连续前缀仍只交付一次；后续 gap 不能伪装成正常空读。
    window_.Fail();
    std::array<std::byte, 8> output{};
    auto read = stream_.Read(output, ++now_);
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 2u);
    EXPECT_EQ(output[0], std::byte{'a'});
    EXPECT_EQ(output[1], std::byte{'b'});
    EXPECT_TRUE(stream_.failed());
    EXPECT_FALSE(read->eof);
    EXPECT_FALSE(stream_.Read(output, ++now_));
    EXPECT_EQ(stream_.pending(), 1u);
    ASSERT_TRUE(stream_.Drain());
    EXPECT_EQ(stream_.pending(), 0u);
}

TEST_F(EagerRxTest, PacketViewPreservesObservedSourceAndStreamRejectsMismatchedExpectedPeer) {
    ASSERT_TRUE(Dispatch(Data(0, "a")));
    const auto view = domain_.View(last_packet_);
    ASSERT_TRUE(view);
    EXPECT_EQ(view->observed, source_.peer_context);
    EXPECT_EQ(Read(1), "a");

    // 路由表与真实来源仍匹配，但调用方错配流绑定；不能把 expected 当 observed 接受 DATA。
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(window_.Close());
    ASSERT_TRUE(window_.Open({22, 33, 44, route_.token.slot, route_.token.generation}, {8, 32, 10000, 0}));
    auto wrong = binding_;
    ++wrong.context.peer_runtime;
    ASSERT_TRUE(stream_.Open(domain_, window_, wrong));
    const auto result = Dispatch(Data(0, "b"));
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EagerRxErrorCode::kInvalidSource);
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(domain_.held(), 0u);
}

TEST_F(EagerRxTest, WindowPinsLegacyReaderAndRejectsLateReceiverWithoutPoisoningIt) {
    EagerRxStream other;
    EXPECT_FALSE(other.Open(domain_, window_, binding_));
    EXPECT_FALSE(window_.Close());
    EXPECT_TRUE(window_.accepting());

    // 反向初始化顺序也必须拒绝：旧 Eager 读者仍绑定时不能加入 RNDV 生产者。
    memory::MemoryBudget budget(65536);
    memory::SlabAllocator slab;
    const std::array contexts{&context_};
    ASSERT_TRUE(slab.Open(contexts, {.buffer_count = 8, .buffer_size = 128, .max_references = 16}, budget));
    GrantManager grants;
    const GrantBinding binding{22, 11, 33, 44, route_.token};
    ASSERT_TRUE(grants.Open(binding, {77, 88}, slab, &context_, {.window = 8, .max_active = 8, .max_bytes = 128}));
    RndvReceiver receiver;
    const auto opened = receiver.Open({binding, binding_, 8, true}, grants, window_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, RndvReceiveErrorCode::kInUse);
    EXPECT_TRUE(window_.accepting());
    EXPECT_TRUE(grants.accepting());
    EXPECT_TRUE(receiver.Close());
    // 失败 Open 必须撤销先前取得的管理器绑定，局部资源可立即安全关闭。
    EXPECT_TRUE(grants.Close());
    EXPECT_TRUE(slab.Close());
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    EXPECT_EQ(Read(8), "ab");
}

class MixedRxTest : public EagerRxTest {
protected:
    void SetUp() override {
        EagerRxTest::SetUp();
        ASSERT_TRUE(stream_.Close());
        ASSERT_TRUE(window_.Close());
        ASSERT_TRUE(window_.Open({22, 33, 44, route_.token.slot, route_.token.generation}, {8, 128, 10000, 0}));
        const std::array contexts{&context_};
        ASSERT_TRUE(slab_.Open(contexts, {.buffer_count = 8, .buffer_size = 128, .max_references = 16}, budget_));
        grant_binding_ = {22, 11, 33, 44, route_.token};
        ASSERT_TRUE(
            grants_.Open(grant_binding_, {77, 88}, slab_, &context_, {.window = 8, .max_active = 8, .max_bytes = 128}));
        ASSERT_TRUE(receiver_.Open({grant_binding_, binding_, 8, true}, grants_, window_));
        ASSERT_TRUE(stream_.Open(domain_, window_, binding_, &receiver_));
    }
    void TearDown() override {
        stream_.Fail();
        for (unsigned i = 0; i < 8 && stream_.pending(); ++i) {
            EXPECT_TRUE(stream_.Drain());
        }
        EXPECT_TRUE(stream_.Close());
        (void)receiver_.Cancel();
        // mock 对端在清理时明确承诺无后续 WRITE；不以流关闭替代远端 DMA 终结。
        for (std::size_t i = 0; i < tracked_count_; ++i) {
            (void)receiver_.ConfirmTerminal(
                tracked_[i].first,
                {.identity = tracked_[i].second.identity, .no_future_writes = true, .acceptance_known = true});
        }
        EXPECT_TRUE(receiver_.Close());
        EXPECT_TRUE(grants_.Close());
        EXPECT_EQ(slab_.stats().cpu_leases, 0u);
        EXPECT_EQ(slab_.stats().remote_grants, 0u);
        EXPECT_TRUE(slab_.Close());
        EagerRxTest::TearDown();
    }
    WireFrameInput RtsFrame(std::uint32_t seq, std::size_t bytes) {
        const auto frame = BuildRndvRts({grant_binding_, 100 + static_cast<std::uint64_t>(seq), seq, bytes},
                                        {11, 33, 44, route_.token});
        EXPECT_TRUE(frame);
        return frame ? *frame : WireFrameInput{};
    }
    WireFrameView Decode(const WireFrameInput& frame) {
        const auto wire = EncodeWireFrame(frame, wire_bytes_);
        EXPECT_TRUE(wire);
        if (!wire) {
            return {};
        }
        const auto decoded = DecodeWireFrame(wire->imm_data, std::span(wire_bytes_).first(wire->payload_bytes));
        EXPECT_TRUE(decoded);
        return decoded ? *decoded : WireFrameView{};
    }
    GrantReservation Publish(std::uint32_t seq, const std::string& payload) {
        const auto request = receiver_.OnRts(Decode(RtsFrame(seq, payload.size())), source_.peer_context, ++now_);
        EXPECT_TRUE(request);
        if (!request) {
            return {};
        }
        const auto grant = receiver_.PrepareGrant(*request);
        EXPECT_TRUE(grant);
        if (!grant) {
            return {};
        }
        EXPECT_TRUE(grants_.CommitPublication(grant->handle));
        // 模拟远端已发布授权上的实际 WRITE，数据随后只经 DONE 验证后交付。
        std::copy(payload.begin(), payload.end(), reinterpret_cast<char*>(grant->target.addr));
        tracked_[tracked_count_++] = {*request, *grant};
        return *grant;
    }
    void Done(const GrantReservation& grant) {
        auto frame = RtsFrame(static_cast<std::uint32_t>(grant.identity.transfer.seq), grant.identity.transfer.bytes);
        frame.control = WireControlType::kRndvDone;
        frame.rndv.arena_id = grant.identity.arena.id;
        frame.rndv.arena_generation = grant.identity.arena.generation;
        frame.rndv.grant_slot = grant.identity.slot;
        frame.rndv.grant_generation = grant.identity.generation;
        frame.rndv.target_address = grant.identity.target_address;
        ASSERT_TRUE(receiver_.OnDone(Decode(frame), source_.peer_context, ++now_));
    }
    void Fin(std::uint32_t seq) {
        WireFrameInput frame;
        frame.type = WireFrameType::kControl;
        frame.control = WireControlType::kFin;
        frame.token = route_.token;
        frame.seq = seq;
        frame.session = binding_.context.session;
        ASSERT_TRUE(Dispatch(Event(frame)));
    }
    memory::MemoryBudget budget_{65536};
    memory::SlabAllocator slab_;
    GrantManager grants_;
    RndvReceiver receiver_;
    GrantBinding grant_binding_;
    std::array<std::byte, 112> wire_bytes_{};
    std::array<std::pair<RndvReceiveHandle, GrantReservation>, 8> tracked_{};
    std::size_t tracked_count_ = 0;
};

TEST_F(MixedRxTest, CopiesEagerGrantAndFinInCommonSequenceWithShortReads) {
    const auto grant = Publish(1, "cdef");
    ASSERT_TRUE(Dispatch(Data(2, "gh")));
    Fin(3);
    ASSERT_TRUE(Dispatch(Data(0, "ab")));
    EXPECT_EQ(Read(3), "ab");
    EXPECT_FALSE(stream_.readable());
    Done(grant);
    EXPECT_EQ(stream_.pending(), 3u);

    // 短读不归还 Grant；跨帧读到 FIN 后每份物理/注册责任恰好释放一次。
    EXPECT_EQ(Read(1), "c");
    EXPECT_EQ(receiver_.stats().in_reorder, 1u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(Read(16), "defgh");
    EXPECT_TRUE(stream_.eof());
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(window_.stats().released_prefix, 4u);
    EXPECT_EQ(domain_.held(), 0u);
}

TEST_F(MixedRxTest, LeaseReadsSwitchFromGrantToPhysicalRxThenOrderedFin) {
    Done(Publish(0, "ab"));
    ASSERT_TRUE(Dispatch(Data(1, "cd")));
    Fin(2);
    const auto grant = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(grant);
    EXPECT_EQ(grant->data[0], std::byte{'a'});
    EXPECT_EQ(domain_.held(), 2u);
    ASSERT_TRUE(stream_.ReleaseLease(grant->generation));
    EXPECT_EQ(receiver_.stats().active, 0u);
    const auto eager = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(eager);
    EXPECT_EQ(eager->data[0], std::byte{'c'});
    EXPECT_NE(eager->generation, grant->generation);
    ASSERT_TRUE(stream_.ReleaseLease(eager->generation));
    EXPECT_FALSE(stream_.eof());
    const auto fin = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(fin);
    EXPECT_TRUE(fin->eof);
    EXPECT_EQ(fin->generation, 0u);
    EXPECT_EQ(domain_.held(), 0u);
    EXPECT_EQ(stream_.pending(), 0u);
}

TEST_F(MixedRxTest, CancelRetainsPartiallyCopiedCurrentGrantUntilDrain) {
    Done(Publish(0, "abcd"));
    EXPECT_EQ(Read(1), "a");
    stream_.Fail();
    // current 已由 Poll 移出 Buffered，仍须计入流责任且不能丢弃原完整 delivery。
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_EQ(window_.stats().delivered_frames, 1u);
    EXPECT_EQ(stream_.pending(), 1u);
    EXPECT_FALSE(stream_.Close());
    const auto drained = stream_.Drain(1);
    ASSERT_TRUE(drained);
    EXPECT_EQ(*drained, 1u);
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(slab_.stats().cpu_leases, 0u);
    EXPECT_TRUE(stream_.Close());
}

TEST_F(MixedRxTest, GrantLeasePreventsCloseAndDrainUntilExactReturn) {
    Done(Publish(0, "abcd"));
    ASSERT_TRUE(Dispatch(Data(1, "ef")));
    auto prefix = stream_.ReadLease(2, ++now_);
    ASSERT_TRUE(prefix);
    ASSERT_EQ(prefix->data.size(), 2u);
    EXPECT_EQ(prefix->data[0], std::byte{'a'});
    ASSERT_TRUE(stream_.ReleaseLease(prefix->generation));
    EXPECT_EQ(receiver_.stats().in_reorder, 1u);
    auto tail = stream_.ReadLease(8, ++now_);
    ASSERT_TRUE(tail);
    EXPECT_EQ(tail->data.size(), 2u);
    EXPECT_EQ(tail->data[0], std::byte{'c'});

    // Close 仅停止后续交付并取消准入，不能抢回应用仍持有的注册目标。
    EXPECT_FALSE(stream_.Close());
    const auto drained = stream_.Drain();
    ASSERT_TRUE(drained);
    EXPECT_EQ(*drained, 0u);
    EXPECT_EQ(stream_.pending(), 2u);
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_FALSE(stream_.ReleaseLease(tail->generation + 1));
    EXPECT_EQ(tail->data[1], std::byte{'d'});
    ASSERT_TRUE(stream_.ReleaseLease(tail->generation));
    EXPECT_EQ(receiver_.stats().active, 0u);
    const auto rest = stream_.Drain();
    ASSERT_TRUE(rest);
    EXPECT_EQ(*rest, 1u);
    EXPECT_EQ(stream_.pending(), 0u);
    EXPECT_TRUE(stream_.Close());
}

TEST_F(MixedRxTest, CancelDrainsUnreadGrantAndPhysicalRxWithoutApplicationDelivery) {
    Done(Publish(0, "ab"));
    ASSERT_TRUE(Dispatch(Data(1, "cd")));
    stream_.Fail();
    EXPECT_EQ(stream_.pending(), 2u);
    EXPECT_FALSE(stream_.ReadLease(8, ++now_));
    const auto first = stream_.Drain(1);
    ASSERT_TRUE(first);
    EXPECT_EQ(*first, 1u);
    EXPECT_EQ(stream_.pending(), 1u);
    const auto second = stream_.Drain(1);
    ASSERT_TRUE(second);
    EXPECT_EQ(*second, 1u);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(domain_.held(), 0u);
}

TEST_F(MixedRxTest, ClosingPendingPublishedGrantLeavesTerminalResponsibilityWithReceiver) {
    const auto grant = Publish(0, "ab");
    EXPECT_EQ(receiver_.stats().waiting_done, 1u);
    EXPECT_TRUE(stream_.Close());
    EXPECT_EQ(receiver_.stats().active, 1u);
    EXPECT_EQ(window_.stats().pending_frames, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 1u);
    EXPECT_FALSE(receiver_.Close());
    // 已解绑流后迟到的合法 DONE 仍由 Receiver 接收并清理，不形成新应用交付。
    Done(grant);
    EXPECT_EQ(receiver_.stats().active, 0u);
    EXPECT_EQ(window_.stats().buffered_frames, 0u);
    EXPECT_EQ(slab_.stats().remote_grants, 0u);
}

TEST_F(MixedRxTest, FailedOpenDoesNotTakeAnotherReceiversBindingOrLifetime) {
    EagerRxStream other;
    auto wrong = binding_;
    ++wrong.credit_epoch;
    const auto opened = other.Open(domain_, window_, wrong, &receiver_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EagerRxErrorCode::kInvalidSource);
    EXPECT_TRUE(other.Close());
    EXPECT_FALSE(receiver_.Close());
    // 失败候选既未抢走唯一流绑定，也未取消原流；正常 Grant 仍可交付。
    Done(Publish(0, "ab"));
    EXPECT_EQ(Read(8), "ab");
    EXPECT_EQ(receiver_.stats().active, 0u);
}

TEST_F(MixedRxTest, MixedCopyAndGrantReturnDoNotAllocate) {
    Done(Publish(0, "ab"));
    ASSERT_TRUE(Dispatch(Data(1, "cd")));
    Fin(2);
    std::array<std::byte, 8> output{};
    allocations = 0;
    count_allocations = true;
    const auto result = stream_.Read(output, ++now_);
    count_allocations = false;
    ASSERT_TRUE(result);
    EXPECT_EQ(result->bytes, 4u);
    EXPECT_TRUE(result->eof);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(stream_.pending(), 0u);
}

TEST_F(MixedRxTest, LegacyOpenCannotBypassTheMixedWindowsUniqueReader) {
    EagerRxStream legacy;
    const auto opened = legacy.Open(domain_, window_, binding_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EagerRxErrorCode::kInUse);
    EXPECT_TRUE(legacy.Close());
    EXPECT_FALSE(window_.Close());
    EXPECT_TRUE(window_.accepting());
    Done(Publish(0, "ab"));
    EXPECT_EQ(Read(8), "ab");
    EXPECT_EQ(receiver_.stats().active, 0u);
}

TEST_F(MixedRxTest, ReceiverRejectsCloseAndSecondStreamWhileBoundEvenWithoutGrants) {
    EagerRxStream other;
    const auto closed = receiver_.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, RndvReceiveErrorCode::kInUse);
    EXPECT_TRUE(window_.accepting());
    EXPECT_TRUE(grants_.accepting());
    const auto opened = other.Open(domain_, window_, binding_, &receiver_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, EagerRxErrorCode::kInUse);
    EXPECT_TRUE(receiver_.Matches(grants_, window_, binding_));
    EXPECT_TRUE(stream_.Close());
    EXPECT_FALSE(receiver_.stats().stopped);
    EXPECT_TRUE(grants_.accepting());
    EXPECT_TRUE(receiver_.Close());
}

} // namespace
} // namespace connection
} // namespace kbsocket
