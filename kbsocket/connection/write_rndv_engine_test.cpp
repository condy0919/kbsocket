// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/write_rndv_engine.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>

#include <gtest/gtest.h>

#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
// Open 之外才启用计数，避免把 GTest 或控制路径的预分配计入发送热路径。
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
void RecordAllocation() {
    if (count_allocations) {
        ++allocations;
    }
}
void RecordDeallocation(void* pointer) {
    if (count_allocations && pointer) {
        ++deallocations;
    }
}
} // namespace
void* operator new(std::size_t bytes) {
    RecordAllocation();
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    RecordAllocation();
    void* pointer = nullptr;
    if (::posix_memalign(&pointer, static_cast<std::size_t>(alignment), bytes ? bytes : 1) != 0) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) {
    return ::operator new(bytes, alignment);
}
void operator delete(void* pointer) noexcept {
    RecordDeallocation(pointer);
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace connection {
namespace {
class WriteRndvEngineTest : public ::testing::Test {
protected:
    struct Packet {
        raw::AttemptId id = 0;
        urma_opcode_t opcode = URMA_OPC_WRITE;
        urma_target_jetty_t* target = nullptr;
        std::array<urma_sge_t, 16> sources{};
        std::size_t source_count = 0;
        urma_sge_t remote{};
    };
    enum class PostMode { kAll, kPartial, kUnknown };
    static inline WriteRndvEngineTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto*, auto*) { return active_->jfc_creates_++ ? &active_->rx_ : &active_->tx_; };
        f.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        f.create_jetty = [](auto*, auto*) { return active_->jetty_creates_++ ? &active_->jetty_ : &active_->passive_; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.register_seg = [](auto* context, auto* config) {
            auto& segment = active_->segments_[active_->registrations_++];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            segment.seg.attr.value = config->flag.value;
            return &segment;
        };
        f.unregister_seg = [](auto*) {
            ++active_->unregisters_;
            return URMA_SUCCESS;
        };
        f.modify_jetty = [](auto*, auto*) {
            ++active_->modifies_;
            return URMA_SUCCESS;
        };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.post_jetty_send_wr = [](auto*, auto* first, auto** bad) { return active_->Post(first, bad); };
        f.poll_jfc = [](auto*, int capacity, auto* output) {
            std::size_t count = 0;
            while (count < static_cast<std::size_t>(capacity) && active_->cq_begin_ < active_->cq_end_) {
                output[count++] = active_->cq_[active_->cq_begin_++];
            }
            return static_cast<int>(count);
        };
        return f;
    }
    virtual std::uint64_t MaxWriteSize() const noexcept {
        return 256ULL * 1024 * 1024;
    }
    virtual std::uint32_t SourceReferences() const noexcept {
        return 128;
    }
    void SetUp() override {
        active_ = this;
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
        cap.max_jfs_sge = 2;
        cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        cap.max_write_size = MaxWriteSize();
        raw::JettyPoolConfig config;
        config.tx_depth = 64;
        config.rx_depth = 1;
        config.tx_cq_depth = 128;
        config.send_sge = 2;
        jetty_.jetty_id.id = 19;
        passive_.jetty_id.id = 23;
        ASSERT_TRUE(pool_.Open(&context_, cap, config));
        ASSERT_TRUE(ledger_.Open(pool_, 64));
        ASSERT_TRUE(sender_.Open(ledger_, true));
        auto lane = pool_.lane(0);
        ASSERT_TRUE(lane);
        lane_ = *lane;
        const std::array<urma_context_t*, 1> sources{&context_}, targets{&receiver_context_};
        ASSERT_TRUE(source_.Open(
            sources, {.buffer_count = 16, .buffer_size = 4096, .max_references = SourceReferences()}, budget_));
        ASSERT_TRUE(target_.Open(targets, {.buffer_count = 4, .buffer_size = 4096, .max_references = 8}, budget_));
        ASSERT_TRUE(grants_.Open(Binding(), {77, 88}, target_, &receiver_context_,
                                 {.window = 8, .max_active = 4, .max_bytes = 16384}));
        endpoint_.urma_ctx = &context_;
        endpoint_.id.id = 40;
        remote_.urma_ctx = &context_;
        // 远端 import 使用导出 VA，而不是本地映射地址 mva。
        remote_.mva = 0x12300000000ULL;
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(engine_.Close());
        for (std::size_t i = 0; i < lease_count_; ++i) {
            if (cpu_held_[i]) {
                EXPECT_TRUE(source_.ReleaseCpu(leases_[i]));
            }
        }
        EXPECT_TRUE(grants_.Close());
        EXPECT_EQ(source_.stats().local_dma, 0u);
        EXPECT_EQ(target_.stats().remote_grants, 0u);
        EXPECT_TRUE(sender_.Close());
        EXPECT_TRUE(ledger_.Close());
        EXPECT_TRUE(pool_.Close());
        EXPECT_TRUE(source_.Close());
        EXPECT_TRUE(target_.Close());
        EXPECT_EQ(budget_.reserved_bytes(), 0u);
        active_ = nullptr;
    }
    static GrantBinding Binding() {
        return {.sender_runtime = 11, .receiver_runtime = 22, .receive_domain = 33, .session = 44, .token = {55, 66}};
    }
    GrantReservation Target(std::uint64_t bytes, std::uint32_t seq = 0) {
        auto grant = grants_.Reserve({Binding(), 100 + seq, seq, bytes});
        EXPECT_TRUE(grant);
        if (!grant) {
            return {};
        }
        EXPECT_TRUE(grants_.CommitPublication(grant->handle));
        remote_.seg = grant->target.tseg->seg;
        return *grant;
    }
    memory::BufferLease Source(std::size_t bytes, unsigned seed = 1) {
        auto lease = source_.Acquire(bytes);
        EXPECT_TRUE(lease);
        if (!lease) {
            return {};
        }
        auto payload = source_.Writable(*lease);
        EXPECT_TRUE(payload);
        if (!payload) {
            return {};
        }
        for (std::size_t i = 0; i < bytes; ++i) {
            (*payload)[i] = static_cast<std::byte>((i * 17 + seed) & 255);
        }
        leases_[lease_count_] = *lease;
        cpu_held_[lease_count_++] = true;
        return *lease;
    }
    void ReleaseCpu(memory::BufferLease lease) {
        ASSERT_TRUE(source_.ReleaseCpu(lease));
        for (std::size_t i = 0; i < lease_count_; ++i) {
            if (leases_[i] == lease) {
                cpu_held_[i] = false;
            }
        }
    }
    WriteRndvConfig Config(std::uint64_t chunk = 32, std::uint32_t budget = 8, std::uint32_t maximum = 64) const {
        return {.connection_id = 101,
                .transaction_generation = 202,
                .lane = lane_,
                .chunk_bytes = chunk,
                .max_chunks = maximum,
                .post_budget = budget};
    }
    WriteRndvDestination Destination(const GrantReservation& grant) {
        return {grant.identity, &endpoint_, &remote_};
    }
    void Open(const GrantReservation& grant, std::span<const WriteRndvSource> sources, std::uint64_t chunk = 32,
              std::uint32_t budget = 8, std::uint32_t maximum = 64) {
        ASSERT_TRUE(engine_.Open(Config(chunk, budget, maximum), sender_, source_));
        ASSERT_TRUE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
    }
    void Complete(std::size_t packet, urma_cr_status_t status = URMA_CR_SUCCESS) {
        auto result = engine_.OnCompletion(Completion(packet, status));
        if (status == URMA_CR_SUCCESS) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    void DeliverAndReturn(const GrantReservation& grant) {
        auto done = engine_.DoneEvidence();
        ASSERT_TRUE(done);
        EXPECT_EQ(done->identity, grant.identity);
        ASSERT_TRUE(grants_.CompleteDone(*done));
        auto view = grants_.Borrow(grant.handle);
        ASSERT_TRUE(view);
        ASSERT_TRUE(grants_.Return(grant.handle));
    }
    void EndFailedGrant(const GrantReservation& grant) {
        auto evidence = engine_.TerminalEvidence();
        ASSERT_TRUE(evidence);
        EXPECT_EQ(evidence->identity, grant.identity);
        ASSERT_TRUE(grants_.ConfirmTerminal(*evidence));
    }
    urma_status_t Post(urma_jfs_wr_t* first, urma_jfs_wr_t** bad) {
        for (auto* wr = first; wr; wr = wr->next) {
            ++offered_[post_calls_];
        }
        ++post_calls_;
        std::size_t index = 0;
        for (auto* wr = first; wr; wr = wr->next, ++index) {
            if (mode_ == PostMode::kPartial && index == partial_) {
                *bad = wr;
                return rejection_;
            }
            auto& packet = packets_[packet_count_++];
            packet.id = wr->user_ctx;
            packet.opcode = wr->opcode;
            packet.target = wr->tjetty;
            if (wr->opcode != URMA_OPC_WRITE || wr->rw.src.num_sge > packet.sources.size() || wr->rw.dst.num_sge != 1) {
                ADD_FAILURE() << "invalid WRITE scatter list";
                *bad = wr;
                return URMA_EINVAL;
            }
            packet.source_count = wr->rw.src.num_sge;
            packet.remote = wr->rw.dst.sge[0];
            std::uint64_t offset = 0;
            for (std::size_t i = 0; i < packet.source_count; ++i) {
                packet.sources[i] = wr->rw.src.sge[i];
                // 仅 provider 接受的 WR 模拟 DMA；错误 CQE 不代表目标一定未修改。
                std::memcpy(reinterpret_cast<void*>(packet.remote.addr + offset),
                            reinterpret_cast<const void*>(packet.sources[i].addr), packet.sources[i].len);
                offset += packet.sources[i].len;
            }
            EXPECT_EQ(offset, packet.remote.len);
        }
        *bad = nullptr;
        return mode_ == PostMode::kUnknown ? URMA_ENOMEM : URMA_SUCCESS;
    }
    raw::TxCompletionEvent Completion(std::size_t packet, urma_cr_status_t status = URMA_CR_SUCCESS) {
        auto& completion = cq_[cq_end_++];
        completion = {};
        completion.status = status;
        completion.flag.bs.jetty = 1;
        completion.local_id = jetty_.jetty_id.id;
        completion.user_ctx = packets_[packet].id;
        std::array<raw::TxCompletionEvent, 1> events;
        auto result = processor_.Poll(events);
        EXPECT_TRUE(result);
        if (!result || result->count != 1) {
            ADD_FAILURE() << "missing completion";
            return {};
        }
        return events[0];
    }
    void DrainFaultedLane() {
        ASSERT_TRUE(pool_.BeginDrain(lane_));
        ASSERT_TRUE(pool_.ObserveFlushDone(lane_));
        std::array<raw::TxCompletionEvent, 1> output;
        ASSERT_TRUE(processor_.Flush(lane_, output));
    }
    void DiscardGrant(const GrantReservation& grant, std::uint64_t accepted = 0) {
        // 只在测试已经取得确定终结事实后使用；不是为生产路径补造证据。
        ASSERT_TRUE(grants_.ConfirmTerminal({.identity = grant.identity,
                                             .no_future_writes = true,
                                             .acceptance_known = true,
                                             .accepted_writes = accepted,
                                             .terminal_writes = accepted}));
    }
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{}, receiver_context_{}, foreign_context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{}, passive_{};
    urma_target_jetty_t endpoint_{};
    urma_target_seg_t remote_{};
    std::array<urma_target_seg_t, 8> segments_{};
    raw::JettyPool pool_;
    raw::JettyLane lane_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_, true};
    memory::MemoryBudget budget_{1024 * 1024};
    memory::SlabAllocator source_, target_;
    GrantManager grants_;
    WriteRndvEngine engine_;
    std::array<memory::BufferLease, 16> leases_{};
    std::array<bool, 16> cpu_held_{};
    std::size_t lease_count_ = 0;
    std::array<Packet, 256> packets_{};
    std::array<urma_cr_t, 256> cq_{};
    std::size_t packet_count_ = 0, cq_begin_ = 0, cq_end_ = 0;
    std::array<std::size_t, 256> offered_{};
    std::size_t post_calls_ = 0, jfc_creates_ = 0, jetty_creates_ = 0;
    std::size_t registrations_ = 0, unregisters_ = 0, modifies_ = 0;
    PostMode mode_ = PostMode::kAll;
    urma_status_t rejection_ = URMA_ENOMEM;
    std::size_t partial_ = 0;
};

TEST_F(WriteRndvEngineTest, PinnedSlicesRemainWritableAfterCpuAndOriginalPinsAreReleased) {
    const auto grant = Target(55);
    const auto first = Source(80, 3);
    const auto second = Source(24, 7);
    const auto first_view = source_.Readable(first);
    const auto second_view = source_.Readable(second);
    ASSERT_TRUE(first_view);
    ASSERT_TRUE(second_view);
    std::array<std::byte, 55> expected{};
    std::copy_n(first_view->begin() + 5, 19, expected.begin());
    std::copy_n(first_view->begin() + 33, 21, expected.begin() + 19);
    std::copy_n(second_view->begin() + 3, 15, expected.begin() + 40);
    const auto first_pin = source_.Reserve(first, memory::BufferUse::kLocalDma);
    const auto second_pin = source_.Reserve(second, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(first_pin);
    ASSERT_TRUE(second_pin);
    const std::array sources{WriteRndvSource{first, 5, 19}, WriteRndvSource{first, 33, 21},
                             WriteRndvSource{second, 3, 15}};
    const std::array pins{*first_pin, *first_pin, *second_pin};
    ReleaseCpu(first);
    ReleaseCpu(second);
    ASSERT_TRUE(engine_.Open(Config(24), sender_, source_));
    // GRANT 到达前应用已经归还 CPU；同一 Lease 的两个 slice 可共用原 pin，分块仍各有责任。
    ASSERT_TRUE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins));
    EXPECT_FALSE(source_.Readable(first));
    EXPECT_FALSE(source_.Writable(second));
    ASSERT_TRUE(source_.Cancel(*first_pin));
    ASSERT_TRUE(source_.Cancel(*second_pin));
    EXPECT_GT(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 14u);
    ASSERT_EQ(engine_.Progress(), 3u);
    Complete(2);
    Complete(0);
    EXPECT_FALSE(engine_.DoneEvidence());
    Complete(1);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 16u);
    const auto done = engine_.DoneEvidence();
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    const auto view = grants_.Borrow(grant.handle);
    ASSERT_TRUE(view);
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), view->begin(), view->end()));
    ASSERT_TRUE(grants_.Return(grant.handle));
}

TEST_F(WriteRndvEngineTest, InvalidPinnedSourcesRollbackClonesAndPreserveCallerResponsibilities) {
    const auto grant = Target(32);
    const auto first = Source(48);
    const auto second = Source(48);
    const auto first_pin = source_.Reserve(first, memory::BufferUse::kLocalDma);
    const auto second_pin = source_.Reserve(second, memory::BufferUse::kLocalDma);
    const auto remote_pin = source_.Reserve(second, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(first_pin);
    ASSERT_TRUE(second_pin);
    ASSERT_TRUE(remote_pin);
    ASSERT_TRUE(engine_.Open(Config(16), sender_, source_));
    const std::array sources{WriteRndvSource{first, 2, 16}, WriteRndvSource{second, 3, 16}};
    const std::array pins{*first_pin, *second_pin};
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, {}));
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, std::span(pins).first(1)));
    // 第二个 pin 的区间合法但 Lease 或责任种类错误，必须撤销前一个 slice 已派生的引用。
    for (const auto bad : {memory::BufferReference{}, *first_pin, *remote_pin}) {
        const std::array wrong{*first_pin, bad};
        EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, wrong));
        EXPECT_EQ(source_.stats().local_dma, 2u);
        EXPECT_EQ(source_.stats().remote_grants, 1u);
        EXPECT_EQ(engine_.stats().planned, 0u);
        EXPECT_EQ(post_calls_, 0u);
    }
    auto bad_sources = sources;
    bad_sources[1].offset = std::numeric_limits<std::size_t>::max() - 1;
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), bad_sources, pins));
    bad_sources[1].offset = 33;
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), bad_sources, pins));
    ASSERT_TRUE(source_.Commit(*second_pin));
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins));
    ASSERT_TRUE(source_.CompleteDma(*second_pin));
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins));
    ASSERT_TRUE(source_.Cancel(*remote_pin));
    ASSERT_TRUE(source_.Cancel(*first_pin));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(post_calls_, 0u);
    DiscardGrant(grant);
}

TEST_F(WriteRndvEngineTest, PinnedAdmissionRejectsQuarantineAndContextMismatchBeforePosting) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto pin = source_.Reserve(source, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(pin);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    const std::array pins{*pin};
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    auto destination = Destination(grant);
    endpoint_.urma_ctx = &foreign_context_;
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, destination, sources, pins));
    endpoint_.urma_ctx = &context_;
    ASSERT_TRUE(source_.Quarantine(source));
    EXPECT_FALSE(engine_.StartPinned(grant.identity.transfer, destination, sources, pins));
    EXPECT_EQ(source_.stats().local_dma, 1u);
    EXPECT_EQ(post_calls_, 0u);
    ASSERT_TRUE(source_.Cancel(*pin));
    ReleaseCpu(source);
    DiscardGrant(grant);
}

TEST_F(WriteRndvEngineTest, PinnedStartAndCompletionHaveNoFastpathAllocationOrFree) {
    const auto grant = Target(77);
    const auto source = Source(77);
    const auto pin = source_.Reserve(source, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(pin);
    const std::array sources{WriteRndvSource{source, 0, 77}};
    const std::array pins{*pin};
    ReleaseCpu(source);
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    allocations = deallocations = 0;
    bool succeeded = true;
    count_allocations = true;
    succeeded &= engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins).has_value();
    succeeded &= source_.Cancel(*pin).has_value();
    succeeded &= engine_.Progress().has_value();
    for (std::size_t i = packet_count_; i > 0; --i) {
        succeeded &= engine_.OnCompletion(Completion(i - 1)).has_value();
    }
    const auto done = engine_.DoneEvidence();
    succeeded &= done.has_value();
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    ASSERT_TRUE(grants_.Cancel(grant.handle));
}

TEST_F(WriteRndvEngineTest, UnknownPinnedAcceptanceQuarantinesOwningLeaseAfterOriginalPinCancellation) {
    const auto grant = Target(64);
    const auto source = Source(80);
    const auto pin = source_.Reserve(source, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(pin);
    const std::array sources{WriteRndvSource{source, 8, 64}};
    const std::array pins{*pin};
    ReleaseCpu(source);
    ASSERT_TRUE(engine_.Open(Config(16, 3), sender_, source_));
    ASSERT_TRUE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins));
    ASSERT_TRUE(source_.Cancel(*pin));
    mode_ = PostMode::kUnknown;
    EXPECT_FALSE(engine_.Progress());
    EXPECT_FALSE(engine_.stats().acceptance_known);
    EXPECT_EQ(source_.stats().quarantined_buffers, 1u);
    EXPECT_EQ(source_.stats().local_dma, 3u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(engine_.TerminalEvidence());
    // Mock 明确执行了这三个 WR，真实 CQE 只允许终结它们的本地责任，不补造远端证明。
    for (std::size_t i = 0; i < packet_count_; ++i) {
        ASSERT_TRUE(ledger_.Commit(packets_[i].id, i + 1));
        Complete(i);
    }
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 16u);
    EXPECT_FALSE(engine_.TerminalEvidence());
    ASSERT_TRUE(engine_.Close());
    DiscardGrant(grant, packet_count_);
    DrainFaultedLane();
}

TEST_F(WriteRndvEngineTest, ReorderedCompletionsGateFullDoneAndTargetVisibility) {
    const auto grant = Target(77);
    const auto lease = Source(90);
    auto initial = source_.Readable(lease);
    ASSERT_TRUE(initial);
    std::array<std::byte, 77> expected{};
    std::copy_n(initial->begin() + 5, expected.size(), expected.begin());
    const std::array sources{WriteRndvSource{lease, 5, 77}};
    Open(grant, sources);
    EXPECT_EQ(engine_.stats().planned, 3u);
    EXPECT_EQ(source_.stats().local_dma, 3u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(engine_.TerminalEvidence());
    ReleaseCpu(lease);
    ASSERT_EQ(engine_.Progress(), 3u);
    ASSERT_EQ(packet_count_, 3u);
    EXPECT_EQ(packets_[0].remote.addr, grant.identity.target_address);
    EXPECT_EQ(packets_[1].remote.addr, grant.identity.target_address + 32);
    EXPECT_EQ(packets_[2].remote.addr, grant.identity.target_address + 64);
    EXPECT_EQ(packets_[2].remote.len, 13u);
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(packets_[i].opcode, URMA_OPC_WRITE);
        EXPECT_EQ(packets_[i].target, &endpoint_);
        EXPECT_EQ(packets_[i].remote.tseg, &remote_);
        EXPECT_EQ(packets_[i].source_count, 1u);
    }
    // 最后一块先退休不能代表整个消息成功，也不能提前解除接收端授权。
    Complete(2);
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(grants_.Borrow(grant.handle));
    EXPECT_EQ(source_.stats().available, 15u);
    Complete(0);
    EXPECT_FALSE(engine_.DoneEvidence());
    Complete(1);
    EXPECT_EQ(source_.stats().available, 16u);
    EXPECT_EQ(target_.stats().remote_grants, 1u);
    const auto done = engine_.DoneEvidence();
    ASSERT_TRUE(done);
    EXPECT_EQ(done->accepted_writes, 3u);
    EXPECT_EQ(done->terminal_writes, 3u);
    EXPECT_EQ(done->successful_writes, 3u);
    EXPECT_EQ(done->completed_bytes, 77u);
    EXPECT_TRUE(done->no_future_writes);
    EXPECT_TRUE(done->acceptance_known);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    auto received = grants_.Borrow(grant.handle);
    ASSERT_TRUE(received);
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), received->begin(), received->end()));
    ASSERT_TRUE(grants_.Return(grant.handle));
    // 成功计划只能执行一次，继续 Progress 不得重新投递已经完成的数据。
    const auto calls = post_calls_;
    (void)engine_.Progress();
    EXPECT_EQ(post_calls_, calls);
}

TEST_F(WriteRndvEngineTest, DoneNotificationRequiresEveryTrustedWriteCompletion) {
    const auto grant = Target(77);
    const auto lease = Source(77);
    const auto binding = Binding();
    const RndvDestination destination{binding.receiver_runtime, binding.receive_domain, binding.session, binding.token};
    const std::array sources{WriteRndvSource{lease, 0, 77}};
    Open(grant, sources);
    ReleaseCpu(lease);
    ASSERT_EQ(engine_.Progress(), 3u);
    Complete(2);
    Complete(0);
    // 尾块和首块已退休，仍然不能因为目标内存已有内容而提前编码 DONE。
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(BuildRndvDone(engine_, destination));
    EXPECT_FALSE(grants_.Borrow(grant.handle));
    Complete(1);
    const auto evidence = engine_.DoneEvidence();
    ASSERT_TRUE(evidence);
    // 真正的引擎证据检查和通知构造属于热路径，不能隐式分配或回收内存。
    allocations = deallocations = 0;
    count_allocations = true;
    const auto input = BuildRndvDone(engine_, destination);
    count_allocations = false;
    ASSERT_TRUE(input);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    auto wrong_destination = destination;
    ++wrong_destination.token.generation;
    EXPECT_FALSE(BuildRndvDone(engine_, wrong_destination));
    std::array<std::byte, 112> payload{};
    const auto encoded = EncodeWireFrame(*input, payload);
    ASSERT_TRUE(encoded);
    const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(payload).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    const WireSessionBinding route{.context = {.peer_runtime = binding.sender_runtime,
                                               .receive_domain = binding.receive_domain,
                                               .session = binding.session},
                                   .token = binding.token,
                                   .credit_epoch = 91};
    ASSERT_TRUE(ValidateRndvDone(*decoded, route, route.context, grant.identity));
    // 编解码和身份匹配均不解除目标授权；测试握有独立的真实发送端证据才能终结 Grant。
    EXPECT_FALSE(grants_.Borrow(grant.handle));
    EXPECT_EQ(target_.stats().remote_grants, 1u);
    ASSERT_TRUE(grants_.CompleteDone(*evidence));
    ASSERT_TRUE(grants_.Borrow(grant.handle));
    ASSERT_TRUE(grants_.Return(grant.handle));
}

TEST_F(WriteRndvEngineTest, FailedWriteCleanupEvidenceCannotBuildSuccessNotification) {
    const auto grant = Target(48);
    const auto lease = Source(48);
    const std::array sources{WriteRndvSource{lease, 0, 48}};
    Open(grant, sources, 16);
    ReleaseCpu(lease);
    ASSERT_EQ(engine_.Progress(), 3u);
    Complete(0);
    Complete(1, URMA_CR_REM_ACCESS_ABORT_ERR);
    Complete(2);
    EXPECT_FALSE(engine_.DoneEvidence());
    const auto cleanup = engine_.TerminalEvidence();
    ASSERT_TRUE(cleanup);
    const auto binding = Binding();
    const RndvDestination destination{binding.receiver_runtime, binding.receive_domain, binding.session, binding.token};
    const auto notification = BuildRndvDone(engine_, destination);
    ASSERT_FALSE(notification);
    EXPECT_EQ(notification.error().code, RndvProtocolErrorCode::kInvalidEvidence);
    EXPECT_EQ(target_.stats().remote_grants, 1u);
    ASSERT_TRUE(grants_.ConfirmTerminal(*cleanup));
}

TEST_F(WriteRndvEngineTest, CancelledFullSuccessCleanupCannotBePromotedToDoneNotification) {
    const auto grant = Target(48);
    const auto lease = Source(48);
    const std::array sources{WriteRndvSource{lease, 0, 48}};
    Open(grant, sources, 16);
    ReleaseCpu(lease);
    ASSERT_EQ(engine_.Progress(), 3u);
    ASSERT_TRUE(engine_.Cancel());
    Complete(0);
    Complete(1);
    Complete(2);
    const auto cleanup = engine_.TerminalEvidence();
    ASSERT_TRUE(cleanup);
    ASSERT_EQ(cleanup->successful_writes, cleanup->accepted_writes);
    ASSERT_EQ(cleanup->completed_bytes, grant.identity.transfer.bytes);
    // 成功计数与完整覆盖仍不足以授权 DONE：取消事务只能通过清理入口终结。
    const auto binding = Binding();
    const RndvDestination destination{binding.receiver_runtime, binding.receive_domain, binding.session, binding.token};
    const auto notification = BuildRndvDone(engine_, destination);
    ASSERT_FALSE(notification);
    EXPECT_EQ(notification.error().code, RndvProtocolErrorCode::kInvalidEvidence);
    EXPECT_FALSE(grants_.Borrow(grant.handle));
    ASSERT_TRUE(grants_.ConfirmTerminal(*cleanup));
}

TEST_F(WriteRndvEngineTest, MultiSgeChunkSpansSourceBoundaryAndPreservesConcatenation) {
    const auto grant = Target(12);
    const auto first = Source(8, 1), second = Source(11, 67);
    const std::array sources{WriteRndvSource{first, 2, 5}, WriteRndvSource{second, 3, 7}};
    std::array<std::byte, 12> expected{};
    const auto a = source_.Readable(first), b = source_.Readable(second);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    std::copy_n(a->begin() + 2, 5, expected.begin());
    std::copy_n(b->begin() + 3, 7, expected.begin() + 5);
    Open(grant, sources, 8);
    EXPECT_EQ(engine_.stats().planned, 2u);
    EXPECT_EQ(source_.stats().local_dma, 3u);
    ReleaseCpu(first);
    ReleaseCpu(second);
    ASSERT_EQ(engine_.Progress(), 2u);
    EXPECT_EQ(packets_[0].source_count, 2u);
    EXPECT_EQ(packets_[0].sources[0].len, 5u);
    EXPECT_EQ(packets_[0].sources[1].len, 3u);
    EXPECT_EQ(packets_[1].source_count, 1u);
    EXPECT_EQ(packets_[1].sources[0].len, 4u);
    EXPECT_EQ(packets_[1].remote.addr, grant.identity.target_address + 8);
    Complete(0);
    Complete(1);
    const auto done = engine_.DoneEvidence();
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    const auto view = grants_.Borrow(grant.handle);
    ASSERT_TRUE(view);
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), view->begin(), view->end()));
    ASSERT_TRUE(grants_.Return(grant.handle));
}

TEST_F(WriteRndvEngineTest, SourceScatterLimitEndsChunkWithoutLeavingGap) {
    const auto grant = Target(15);
    const auto first = Source(3), second = Source(5), third = Source(7);
    const std::array sources{WriteRndvSource{first, 0, 3}, WriteRndvSource{second, 0, 5}, WriteRndvSource{third, 0, 7}};
    Open(grant, sources, 64);
    // provider 每 WR 最多两个本地 SGE，不能因 chunk_bytes 较大而越界拼接。
    ASSERT_EQ(engine_.Progress(), 2u);
    EXPECT_EQ(packets_[0].source_count, 2u);
    EXPECT_EQ(packets_[0].remote.len, 8u);
    EXPECT_EQ(packets_[1].remote.addr, grant.identity.target_address + 8);
    EXPECT_EQ(packets_[1].remote.len, 7u);
    Complete(0);
    Complete(1);
    DeliverAndReturn(grant);
}

TEST_F(WriteRndvEngineTest, TrustedPartialAcceptanceContinuesOnlyOriginalSuffix) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 8, 4);
    ReleaseCpu(source);
    mode_ = PostMode::kPartial;
    partial_ = 2;
    (void)engine_.Progress();
    ASSERT_EQ(packet_count_, 2u);
    EXPECT_EQ(engine_.stats().accepted, 2u);
    EXPECT_EQ(engine_.stats().queued, 6u);
    EXPECT_EQ(source_.stats().local_dma, 8u);
    const auto first_id = packets_[0].id, second_id = packets_[1].id;
    Complete(1);
    Complete(0);
    mode_ = PostMode::kAll;
    while (engine_.stats().queued) {
        ASSERT_TRUE(engine_.Progress());
    }
    ASSERT_EQ(packet_count_, 8u);
    EXPECT_EQ(packets_[0].id, first_id);
    EXPECT_EQ(packets_[1].id, second_id);
    for (std::size_t i = 2; i < packet_count_; ++i) {
        EXPECT_EQ(packets_[i].remote.addr, grant.identity.target_address + 8 * i);
        EXPECT_NE(packets_[i].id, first_id);
        EXPECT_NE(packets_[i].id, second_id);
        Complete(i);
    }
    EXPECT_EQ(engine_.stats().accepted, 8u);
    EXPECT_EQ(engine_.stats().successful_bytes, 64u);
    DeliverAndReturn(grant);
}

TEST_F(WriteRndvEngineTest, OneProgressCallPostsAtMostConfiguredBudget) {
    const auto grant = Target(40);
    const auto source = Source(40);
    const std::array sources{WriteRndvSource{source, 0, 40}};
    Open(grant, sources, 8, 2);
    ASSERT_EQ(engine_.Progress(), 2u);
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(engine_.stats().queued, 3u);
    ASSERT_EQ(engine_.Progress(), 2u);
    EXPECT_EQ(post_calls_, 2u);
    ASSERT_EQ(engine_.Progress(), 1u);
    for (std::size_t i = 0; i < packet_count_; ++i) {
        Complete(i);
    }
    DeliverAndReturn(grant);
}

TEST_F(WriteRndvEngineTest, ZeroAcceptanceResourcePressureShrinksBatchWithoutReplay) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 8, 8);
    mode_ = PostMode::kPartial;
    partial_ = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(engine_.Progress());
    }
    EXPECT_EQ(packet_count_, 0u);
    EXPECT_EQ(engine_.stats().accepted, 0u);
    EXPECT_EQ(engine_.stats().queued, 8u);
    EXPECT_EQ(source_.stats().local_dma, 8u);
    EXPECT_EQ(offered_[0], 8u);
    EXPECT_EQ(offered_[1], 4u);
    EXPECT_EQ(offered_[2], 2u);
    EXPECT_EQ(offered_[3], 1u);
    mode_ = PostMode::kAll;
    while (engine_.stats().queued) {
        ASSERT_TRUE(engine_.Progress());
    }
    EXPECT_EQ(packet_count_, 8u);
    for (std::size_t i = 0; i < packet_count_; ++i) {
        Complete(i);
    }
    DeliverAndReturn(grant);
}

TEST_F(WriteRndvEngineTest, CancelBeforePostReleasesOnlyLocalPinsAndProvidesCleanupProof) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 8);
    ReleaseCpu(source);
    ASSERT_TRUE(engine_.Cancel());
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 16u);
    EXPECT_EQ(target_.stats().remote_grants, 1u);
    EXPECT_EQ(post_calls_, 0u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(engine_.Progress());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, CancelAfterPartialPostWaitsForEachAcceptedWrWithoutSuccessDone) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 8);
    ReleaseCpu(source);
    mode_ = PostMode::kPartial;
    partial_ = 2;
    (void)engine_.Progress();
    ASSERT_EQ(packet_count_, 2u);
    ASSERT_TRUE(engine_.Cancel());
    EXPECT_EQ(engine_.stats().queued, 0u);
    EXPECT_EQ(source_.stats().local_dma, 2u);
    EXPECT_FALSE(engine_.TerminalEvidence());
    EXPECT_FALSE(engine_.Close());
    Complete(0);
    EXPECT_EQ(source_.stats().local_dma, 1u);
    EXPECT_FALSE(engine_.TerminalEvidence());
    Complete(1);
    EXPECT_FALSE(engine_.DoneEvidence());
    const auto terminal = engine_.TerminalEvidence();
    ASSERT_TRUE(terminal);
    EXPECT_EQ(terminal->accepted_writes, 2u);
    EXPECT_EQ(terminal->terminal_writes, 2u);
    EXPECT_EQ(terminal->completed_bytes, 16u);
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, CancelAfterAllPostsSuppressesDoneEvenIfEveryCompletionSucceeds) {
    const auto grant = Target(40);
    const auto source = Source(40);
    const std::array sources{WriteRndvSource{source, 0, 40}};
    Open(grant, sources, 8);
    ASSERT_EQ(engine_.Progress(), 5u);
    ASSERT_TRUE(engine_.Cancel());
    for (std::size_t i = 0; i < packet_count_; ++i) {
        Complete(i);
    }
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_EQ(engine_.stats().successful_bytes, 40u);
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, RemoteAbortFailsTransactionWithoutPoisoningSharedSq) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 16, 2);
    ASSERT_EQ(engine_.Progress(), 2u);
    Complete(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    EXPECT_TRUE(engine_.stats().failed);
    EXPECT_EQ(pool_.lane_state(lane_), raw::JettyLaneState::kReady);
    EXPECT_EQ(modifies_, 0u);
    EXPECT_FALSE(engine_.Progress());
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(engine_.stats().queued, 0u);
    EXPECT_FALSE(engine_.DoneEvidence());
    Complete(1);
    EndFailedGrant(grant);
    // 同一物理 SQ 继续允许另一笔独立事务，status 8 不升级成全 SQ 故障。
    auto healthy_grant = Target(16, 1);
    const auto healthy_source = Source(16, 99);
    const std::array healthy_sources{WriteRndvSource{healthy_source, 0, 16}};
    WriteRndvEngine healthy;
    auto config = Config();
    config.connection_id = 102;
    config.transaction_generation = 203;
    ASSERT_TRUE(healthy.Open(config, sender_, source_));
    ASSERT_TRUE(healthy.Start(healthy_grant.identity.transfer, Destination(healthy_grant), healthy_sources));
    ASSERT_EQ(healthy.Progress(), 1u);
    ASSERT_TRUE(healthy.OnCompletion(Completion(2)));
    const auto done = healthy.DoneEvidence();
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    ASSERT_TRUE(grants_.Cancel(healthy_grant.handle));
    ASSERT_TRUE(healthy.Close());
}

TEST_F(WriteRndvEngineTest, AckTimeoutStopsPostingAndRetiresOnlyProvenCompletions) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 16, 2);
    ReleaseCpu(source);
    ASSERT_EQ(engine_.Progress(), 2u);
    Complete(0, URMA_CR_ACK_TIMEOUT_ERR);
    EXPECT_EQ(pool_.lane_state(lane_), raw::JettyLaneState::kFaulted);
    EXPECT_EQ(modifies_, 0u);
    EXPECT_FALSE(engine_.Progress());
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(engine_.TerminalEvidence());
    EXPECT_EQ(source_.stats().local_dma, 1u);
    Complete(1, URMA_CR_WR_FLUSH_ERR);
    EndFailedGrant(grant);
    DrainFaultedLane();
}

TEST_F(WriteRndvEngineTest, UnknownAcceptanceNeverReplaysOrClaimsRemoteTerminalEvidence) {
    const auto grant = Target(64);
    const auto binding = Binding();
    const RndvDestination destination{binding.receiver_runtime, binding.receive_domain, binding.session, binding.token};
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    Open(grant, sources, 16, 3);
    ReleaseCpu(source);
    mode_ = PostMode::kUnknown;
    EXPECT_FALSE(engine_.Progress());
    EXPECT_FALSE(engine_.stats().acceptance_known);
    EXPECT_EQ(packet_count_, 3u);
    EXPECT_EQ(source_.stats().quarantined_buffers, 1u);
    EXPECT_FALSE(engine_.Progress());
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(engine_.TerminalEvidence());
    EXPECT_FALSE(BuildRndvDone(engine_, destination));
    EXPECT_FALSE(engine_.Close());
    // Mock 明确执行了三个 WRITE，仅破坏 bad_wr 契约；依据这项外部事实恢复 Prepared 记录。
    // 生产引擎没有推测接受集合的入口，后续 CQE 只允许解除真实本地 DMA 责任。
    for (std::size_t i = 0; i < packet_count_; ++i) {
        ASSERT_TRUE(ledger_.Commit(packets_[i].id, i + 1));
        Complete(i);
    }
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 16u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EXPECT_FALSE(engine_.TerminalEvidence());
    EXPECT_FALSE(BuildRndvDone(engine_, destination));
    EXPECT_FALSE(engine_.stats().acceptance_known);
    ASSERT_TRUE(engine_.Close());
    // 接收端独立授权凭测试已知的完整终结事实清理，不能从 engine 的未知接受状态合成证明。
    DiscardGrant(grant, packet_count_);
    DrainFaultedLane();
}

TEST_F(WriteRndvEngineTest, DuplicateAndCorruptedCompletionsCannotReleaseSourceReferences) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    Open(grant, sources, 16);
    ASSERT_EQ(engine_.Progress(), 2u);
    const auto valid = Completion(0);
    ASSERT_TRUE(valid.record);
    for (unsigned field = 0; field < 18; ++field) {
        auto bad = valid;
        switch (field) {
        case 0:
            ++bad.record->id;
            break;
        case 1:
            ++bad.record->metadata.connection_id;
            break;
        case 2:
            ++bad.record->metadata.operation_id;
            break;
        case 3:
            ++bad.record->metadata.operation_generation;
            break;
        case 4:
            ++bad.record->metadata.buffer_lease;
            break;
        case 5:
            ++bad.record->metadata.grant_lease;
            break;
        case 6:
            bad.record->metadata.target = nullptr;
            break;
        case 7:
            bad.record->metadata.opcode = URMA_OPC_SEND;
            break;
        case 8:
            ++bad.record->ticket.lane.generation;
            break;
        case 9:
            ++bad.completion.user_ctx;
            break;
        case 10:
            bad.kind = raw::TxCompletionKind::kRejected;
            break;
        case 11:
            bad.record.reset();
            break;
        case 12:
            ++bad.lane->generation;
            break;
        case 13:
            bad.record->metadata.signaled = false;
            break;
        case 14:
            bad.completion.flag.bs.s_r = 1;
            break;
        case 15:
            bad.completion.flag.bs.jetty = 0;
            break;
        case 16:
            bad.completion.status = URMA_CR_WR_FLUSH_ERR_DONE;
            break;
        case 17:
            bad.completion.status = static_cast<urma_cr_status_t>(255);
            break;
        }
        EXPECT_FALSE(engine_.OnCompletion(bad));
        EXPECT_EQ(source_.stats().local_dma, 2u);
        EXPECT_EQ(engine_.stats().retired, 0u);
    }
    ASSERT_TRUE(engine_.OnCompletion(valid));
    EXPECT_FALSE(engine_.OnCompletion(valid));
    EXPECT_EQ(source_.stats().local_dma, 1u);
    Complete(1);
    DeliverAndReturn(grant);
}

TEST_F(WriteRndvEngineTest, FullGrantTransferIdentityMustMatchBeforeAnySourcePin) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    for (unsigned field = 0; field < 9; ++field) {
        auto expected = grant.identity.transfer;
        switch (field) {
        case 0:
            ++expected.binding.sender_runtime;
            break;
        case 1:
            ++expected.binding.receiver_runtime;
            break;
        case 2:
            ++expected.binding.receive_domain;
            break;
        case 3:
            ++expected.binding.session;
            break;
        case 4:
            ++expected.binding.token.slot;
            break;
        case 5:
            ++expected.binding.token.generation;
            break;
        case 6:
            ++expected.transfer_id;
            break;
        case 7:
            ++expected.seq;
            break;
        case 8:
            ++expected.bytes;
            break;
        }
        EXPECT_FALSE(engine_.Start(expected, Destination(grant), sources));
        EXPECT_EQ(source_.stats().local_dma, 0u);
        EXPECT_EQ(post_calls_, 0u);
    }
    ASSERT_TRUE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
    ASSERT_TRUE(engine_.Cancel());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, InvalidDestinationContextEidAndRangeAreRejectedWithoutPins) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    for (unsigned field = 0; field < 8; ++field) {
        auto destination = Destination(grant);
        auto segment = remote_;
        auto jetty = endpoint_;
        destination.segment = &segment;
        destination.jetty = &jetty;
        switch (field) {
        case 0:
            destination.segment = nullptr;
            break;
        case 1:
            destination.jetty = nullptr;
            break;
        case 2:
            segment.urma_ctx = &foreign_context_;
            break;
        case 3:
            jetty.urma_ctx = &foreign_context_;
            break;
        case 4:
            segment.seg.ubva.eid.raw[0] ^= 1;
            break;
        case 5:
            destination.grant.target_address = segment.seg.ubva.va - 1;
            break;
        case 6:
            segment.seg.len = 1;
            break;
        case 7:
            segment.seg.ubva.va = std::numeric_limits<std::uint64_t>::max() - 4;
            segment.seg.len = 8;
            break;
        }
        EXPECT_FALSE(engine_.Start(grant.identity.transfer, destination, sources));
        EXPECT_EQ(source_.stats().local_dma, 0u);
    }
    ASSERT_TRUE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
    ASSERT_TRUE(engine_.Cancel());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, InvalidSourceLengthsAndForeignLeaseRollbackCompletely) {
    const auto grant = Target(32);
    const auto source = Source(40);
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    const std::array good{WriteRndvSource{source, 0, 32}};
    const std::array zero{WriteRndvSource{source, 0, 0}};
    const std::array short_sum{WriteRndvSource{source, 0, 31}};
    const std::array long_sum{WriteRndvSource{source, 0, 33}};
    const std::array bad_range{WriteRndvSource{source, 10, 32}};
    auto stale = source;
    ++stale.generation;
    const std::array stale_source{WriteRndvSource{stale, 0, 32}};
    for (const auto& sources : {zero, short_sum, long_sum, bad_range, stale_source}) {
        EXPECT_FALSE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
        EXPECT_EQ(source_.stats().local_dma, 0u);
    }
    auto foreign = target_.Acquire(32);
    ASSERT_TRUE(foreign);
    const std::array foreign_source{WriteRndvSource{*foreign, 0, 32}};
    EXPECT_FALSE(engine_.Start(grant.identity.transfer, Destination(grant), foreign_source));
    ASSERT_TRUE(target_.ReleaseCpu(*foreign));
    EXPECT_FALSE(engine_.Start(grant.identity.transfer, Destination(grant), {}));
    ASSERT_TRUE(engine_.Start(grant.identity.transfer, Destination(grant), good));
    ASSERT_TRUE(engine_.Cancel());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, MaxChunksBoundsAdmissionAndRollsBackAnyPreparedSourcePins) {
    const auto grant = Target(65);
    const auto source = Source(65);
    const std::array sources{WriteRndvSource{source, 0, 65}};
    ASSERT_TRUE(engine_.Open(Config(16, 4, 4), sender_, source_));
    EXPECT_FALSE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_TRUE(source_.Readable(source));
    EXPECT_EQ(post_calls_, 0u);
    DiscardGrant(grant);
}

TEST_F(WriteRndvEngineTest, StartAndCompleteFastpathDoesNotAllocateOrFree) {
    const auto grant = Target(77);
    const auto source = Source(77);
    const std::array sources{WriteRndvSource{source, 0, 77}};
    ASSERT_TRUE(engine_.Open(Config(32), sender_, source_));
    allocations = deallocations = 0;
    const auto registrations = registrations_;
    bool succeeded = true;
    count_allocations = true;
    succeeded &= engine_.Start(grant.identity.transfer, Destination(grant), sources).has_value();
    succeeded &= !engine_.Start(grant.identity.transfer, Destination(grant), sources).has_value();
    succeeded &= engine_.Progress().has_value();
    for (std::size_t i = packet_count_; i > 0; --i) {
        succeeded &= engine_.OnCompletion(Completion(i - 1)).has_value();
    }
    auto done = engine_.DoneEvidence();
    succeeded &= done.has_value();
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    EXPECT_EQ(registrations_, registrations);
    EXPECT_EQ(unregisters_, 0u);
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    ASSERT_TRUE(grants_.Cancel(grant.handle));
}

TEST_F(WriteRndvEngineTest, RejectsMalformedOpenAndRequiresDeferredDrainSender) {
    EXPECT_FALSE(engine_.Progress());
    EXPECT_FALSE(engine_.DoneEvidence());
    for (unsigned field = 0; field < 11; ++field) {
        auto config = Config();
        switch (field) {
        case 0:
            config.connection_id = 0;
            break;
        case 1:
            config.transaction_generation = 0;
            break;
        case 2:
            config.chunk_bytes = 0;
            break;
        case 3:
            config.max_chunks = 0;
            break;
        case 4:
            config.max_chunks = WriteRndvEngine::kMaxChunks + 1;
            break;
        case 5:
            config.post_budget = 0;
            break;
        case 6:
            config.post_budget = raw::TxSender::kMaxBatch + 1;
            break;
        case 7:
            config.lane.pool = nullptr;
            break;
        case 8:
            ++config.lane.epoch;
            break;
        case 9:
            ++config.lane.generation;
            break;
        case 10:
            ++config.lane.index;
            break;
        }
        EXPECT_FALSE(engine_.Open(config, sender_, source_));
    }
    raw::TxSender synchronous;
    ASSERT_TRUE(synchronous.Open(ledger_, false));
    EXPECT_FALSE(engine_.Open(Config(), synchronous, source_));
    ASSERT_TRUE(synchronous.Close());
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    EXPECT_FALSE(engine_.Open(Config(), sender_, source_));
    ASSERT_TRUE(engine_.Close());
    EXPECT_FALSE(engine_.Open(Config(), sender_, source_));
    EXPECT_EQ(post_calls_, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
}

TEST_F(WriteRndvEngineTest, GrantArenaGenerationAndFinSequenceMustBeValid) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    ASSERT_TRUE(engine_.Open(Config(), sender_, source_));
    for (unsigned field = 0; field < 8; ++field) {
        auto destination = Destination(grant);
        auto transfer = grant.identity.transfer;
        switch (field) {
        case 0:
            destination.grant.arena.id = 0;
            break;
        case 1:
            destination.grant.arena.generation = 0;
            break;
        case 2:
            destination.grant.generation = 0;
            break;
        case 3:
            destination.grant.slot = GrantManager::kMaxWindow;
            break;
        case 4:
            destination.grant.target_address = 0;
            break;
        case 5:
            transfer.seq = std::numeric_limits<std::uint32_t>::max();
            break;
        case 6:
            transfer.binding.token.generation = 0;
            break;
        case 7:
            transfer.transfer_id = 0;
            break;
        }
        destination.grant.transfer = transfer;
        EXPECT_FALSE(engine_.Start(transfer, destination, sources));
        EXPECT_EQ(source_.stats().local_dma, 0u);
    }
    ASSERT_TRUE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
    ASSERT_TRUE(engine_.Cancel());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, RepeatedSourceLeaseSlicesAreValidatedBeforePinning) {
    const auto grant = Target(12);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 2, 5}, WriteRndvSource{source, 20, 7}};
    auto initial = source_.Readable(source);
    ASSERT_TRUE(initial);
    std::array<std::byte, 12> expected{};
    std::copy_n(initial->begin() + 2, 5, expected.begin());
    std::copy_n(initial->begin() + 20, 7, expected.begin() + 5);
    Open(grant, sources, 16);
    ReleaseCpu(source);
    EXPECT_EQ(source_.stats().local_dma, 2u);
    ASSERT_EQ(engine_.Progress(), 1u);
    EXPECT_EQ(packets_[0].source_count, 2u);
    Complete(0);
    EXPECT_EQ(source_.stats().available, 16u);
    const auto done = engine_.DoneEvidence();
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    const auto view = grants_.Borrow(grant.handle);
    ASSERT_TRUE(view);
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), view->begin(), view->end()));
    ASSERT_TRUE(grants_.Return(grant.handle));
}

TEST_F(WriteRndvEngineTest, SourceContextMismatchAndHostOnlyBackingDoNotAuthorizeDma) {
    const auto grant = Target(32);
    for (unsigned host = 0; host < 2; ++host) {
        memory::SlabAllocator other;
        const memory::SlabConfig config{.buffer_count = 1, .buffer_size = 64, .max_references = 4};
        const std::array<urma_context_t*, 1> contexts{&foreign_context_};
        ASSERT_TRUE(host ? other.OpenHost(config, budget_) : other.Open(contexts, config, budget_));
        const auto source = other.Acquire(32);
        ASSERT_TRUE(source);
        const std::array sources{WriteRndvSource{*source, 0, 32}};
        WriteRndvEngine engine;
        ASSERT_TRUE(engine.Open(Config(), sender_, other));
        EXPECT_FALSE(engine.Start(grant.identity.transfer, Destination(grant), sources));
        EXPECT_EQ(other.stats().local_dma, 0u);
        EXPECT_EQ(other.stats().cpu_leases, 1u);
        EXPECT_EQ(other.stats().available_references, 4u);
        ASSERT_TRUE(engine.Close());
        ASSERT_TRUE(other.ReleaseCpu(*source));
        ASSERT_TRUE(other.Close());
    }
    EXPECT_EQ(post_calls_, 0u);
    DiscardGrant(grant);
}

TEST_F(WriteRndvEngineTest, SlabStopBeforePostingCancelsQueuedPinsWithoutProviderCall) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    Open(grant, sources, 16);
    ReleaseCpu(source);
    source_.StopAccepting();
    EXPECT_FALSE(engine_.Progress());
    EXPECT_EQ(post_calls_, 0u);
    EXPECT_TRUE(engine_.stats().failed);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 16u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, KnownZeroHardPostFailureStopsInsteadOfRetrying) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    Open(grant, sources, 16);
    ReleaseCpu(source);
    mode_ = PostMode::kPartial;
    partial_ = 0;
    rejection_ = URMA_EINVAL;
    EXPECT_FALSE(engine_.Progress());
    EXPECT_TRUE(engine_.stats().failed);
    EXPECT_TRUE(engine_.stats().acceptance_known);
    EXPECT_EQ(engine_.stats().accepted, 0u);
    EXPECT_EQ(engine_.stats().queued, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available, 16u);
    EXPECT_FALSE(engine_.Progress());
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_FALSE(engine_.DoneEvidence());
    EndFailedGrant(grant);
}

TEST_F(WriteRndvEngineTest, PartialPostCancellationAndCleanupEvidenceDoNotAllocateOrFree) {
    const auto grant = Target(64);
    const auto source = Source(64);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    ASSERT_TRUE(engine_.Open(Config(16), sender_, source_));
    mode_ = PostMode::kPartial;
    partial_ = 2;
    allocations = deallocations = 0;
    bool succeeded = true;
    count_allocations = true;
    succeeded &= engine_.Start(grant.identity.transfer, Destination(grant), sources).has_value();
    succeeded &= engine_.Progress().has_value();
    succeeded &= engine_.Cancel().has_value();
    succeeded &= !engine_.DoneEvidence().has_value();
    succeeded &= !engine_.TerminalEvidence().has_value();
    succeeded &= !engine_.Progress().has_value();
    for (std::size_t i = 0; i < packet_count_; ++i) {
        succeeded &= engine_.OnCompletion(Completion(i)).has_value();
    }
    const auto terminal = engine_.TerminalEvidence();
    succeeded &= terminal.has_value();
    count_allocations = false;
    EXPECT_TRUE(succeeded);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    ASSERT_TRUE(terminal);
    EXPECT_EQ(terminal->accepted_writes, 2u);
    ASSERT_TRUE(grants_.ConfirmTerminal(*terminal));
}

class SmallWriteProviderTest : public WriteRndvEngineTest {
protected:
    std::uint64_t MaxWriteSize() const noexcept override {
        return 30;
    }
};
TEST_F(SmallWriteProviderTest, ProviderWriteLimitFurtherBoundsEveryChunk) {
    const auto grant = Target(77);
    const auto source = Source(77);
    const std::array sources{WriteRndvSource{source, 0, 77}};
    Open(grant, sources, 64);
    ASSERT_EQ(engine_.Progress(), 3u);
    EXPECT_EQ(packets_[0].remote.len, 30u);
    EXPECT_EQ(packets_[1].remote.len, 30u);
    EXPECT_EQ(packets_[2].remote.len, 17u);
    for (std::size_t i = 0; i < packet_count_; ++i) {
        Complete(i);
    }
    DeliverAndReturn(grant);
}

class SmallWriteReferenceTest : public WriteRndvEngineTest {
protected:
    std::uint32_t SourceReferences() const noexcept override {
        return 2;
    }
};
class PinnedWriteReferenceTest : public WriteRndvEngineTest {
protected:
    std::uint32_t SourceReferences() const noexcept override {
        return 3;
    }
};
TEST_F(PinnedWriteReferenceTest, PinCapacityFailureRollsBackThenRetriesWithoutReplacingOriginalPin) {
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto pin = source_.Reserve(source, memory::BufferUse::kLocalDma);
    const auto pressure = source_.Reserve(source, memory::BufferUse::kLocalDma);
    ASSERT_TRUE(pin);
    ASSERT_TRUE(pressure);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    const std::array pins{*pin};
    ReleaseCpu(source);
    ASSERT_TRUE(engine_.Open(Config(16), sender_, source_));
    // 只剩一个引用槽，第二分块准入失败必须归还第一个派生引用，原始两个 pin 不变。
    const auto started = engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins);
    ASSERT_FALSE(started);
    EXPECT_EQ(started.error().code, WriteRndvErrorCode::kWouldBlock);
    EXPECT_EQ(source_.stats().local_dma, 2u);
    EXPECT_EQ(source_.stats().available_references, 1u);
    EXPECT_EQ(engine_.stats().planned, 0u);
    EXPECT_EQ(post_calls_, 0u);
    ASSERT_TRUE(source_.Cancel(*pressure));
    ASSERT_TRUE(engine_.StartPinned(grant.identity.transfer, Destination(grant), sources, pins));
    ASSERT_TRUE(source_.Cancel(*pin));
    ASSERT_EQ(engine_.Progress(), 2u);
    Complete(0);
    Complete(1);
    DeliverAndReturn(grant);
}
TEST_F(SmallWriteReferenceTest, InsufficientDmaReferenceCapacityRollsBackAllAdmissionPins) {
    const auto grant = Target(48);
    const auto source = Source(48);
    const std::array sources{WriteRndvSource{source, 0, 48}};
    ASSERT_TRUE(engine_.Open(Config(16), sender_, source_));
    EXPECT_FALSE(engine_.Start(grant.identity.transfer, Destination(grant), sources));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(source_.stats().available_references, 2u);
    EXPECT_TRUE(source_.Readable(source));
    EXPECT_EQ(post_calls_, 0u);
    DiscardGrant(grant);
}

} // namespace
} // namespace connection
} // namespace kbsocket
