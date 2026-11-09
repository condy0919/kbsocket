// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/write_rndv_pool.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <thread>

#include <gtest/gtest.h>

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
template <typename T>
void ExpectError(const std::expected<T, WriteRndvPoolError>& result, WriteRndvPoolErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

class WriteRndvPoolTest : public ::testing::Test {
protected:
    void TestBody() override {}
    struct Packet {
        raw::AttemptId id = 0;
        urma_opcode_t opcode = URMA_OPC_WRITE;
        urma_target_jetty_t* target = nullptr;
        std::array<urma_sge_t, 16> sources{};
        std::size_t source_count = 0;
        urma_sge_t remote{};
        std::uint64_t grant_generation = 0;
    };
    enum class PostMode { kAll, kPartial, kUnknown };
    static inline WriteRndvPoolTest* active_ = nullptr;
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
            segment.seg.attr.bs.access = config->flag.bs.access;
            segment.seg.ubva.eid = context->eid;
            segment.seg.token_id = static_cast<std::uint32_t>(active_->registrations_);
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
        f.import_jetty = [](auto* context, auto* descriptor, auto*) {
            active_->endpoint_.urma_ctx = context;
            active_->endpoint_.id = descriptor->jetty_id;
            active_->endpoint_.trans_mode = descriptor->trans_mode;
            active_->endpoint_.tp_type = descriptor->tp_type;
            active_->endpoint_.type = descriptor->type;
            ++active_->jetty_imports_;
            return &active_->endpoint_;
        };
        f.unimport_jetty = [](auto*) {
            ++active_->jetty_unimports_;
            return URMA_SUCCESS;
        };
        f.import_seg = [](auto* context, auto* descriptor, auto*, std::uint64_t, urma_import_seg_flag_t) {
            active_->remote_.urma_ctx = context;
            active_->remote_.seg = *descriptor;
            ++active_->segment_imports_;
            return &active_->remote_;
        };
        f.unimport_seg = [](auto*) {
            ++active_->segment_unimports_;
            return URMA_SUCCESS;
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
        context_.eid.raw[15] = 1;
        receiver_context_.eid.raw[15] = 9;
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
                                 {.window = 32, .max_active = 4, .max_bytes = 16384}));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        ASSERT_TRUE(endpoints_.Open({.entries = 8, .leases = 32, .in_flight = 2}));
        ASSERT_TRUE(segments_cache_.Open({.entries = 8, .leases = 32, .in_flight = 2}));
        endpoint_key_ = {.local_context = &context_,
                         .local_context_incarnation = 1,
                         .peer_runtime = 22,
                         .receive_domain = 33,
                         .endpoint_generation = 7,
                         .jetty_id = 40,
                         .authorization_identity = 99};
        endpoint_key_.eid[15] = 9;
        segment_key_ = {.local_context = &context_,
                        .local_context_incarnation = 1,
                        .peer_runtime = 22,
                        .receive_domain = 33,
                        .slab_id = 77,
                        .slab_generation = 88,
                        .authorization_identity = 99};
        segment_key_.eid[15] = 9;
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = 40;
        descriptor.jetty_id.eid = receiver_context_.eid;
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        auto endpoint = endpoints_.Acquire({endpoint_key_, std::as_bytes(std::span(&descriptor, 1)), {}});
        ASSERT_TRUE(endpoint);
        endpoint_lease_ = *endpoint;
        auto segment = segments_cache_.Acquire({segment_key_, segments_[1].seg, {}});
        ASSERT_TRUE(segment);
        segment_lease_ = *segment;
        ASSERT_TRUE(DriveCaches());
        ASSERT_TRUE(endpoints_.Get(endpoint_lease_));
        ASSERT_TRUE(segments_cache_.Get(segment_lease_));
    }
    void TearDown() override {
        count_allocations = false;
        // Mock 已知每个捕获 WR 确实被接受；失败断言后的兜底只消费这些真实 Posted 记录。
        for (std::size_t i = 0; i < packet_count_; ++i) {
            if (ledger_.LookupPosted(packets_[i].id)) {
                (void)transactions_.OnCompletion(Completion(i, URMA_CR_WR_FLUSH_ERR));
            }
        }
        EXPECT_TRUE(transactions_.Close());
        for (std::size_t i = 0; i < lease_count_; ++i) {
            if (cpu_held_[i]) {
                EXPECT_TRUE(source_.ReleaseCpu(leases_[i]));
            }
        }
        // 全部 Mock WRITE 已终结且池已停止；独立接收端取消尚未交付的 Grant。
        for (std::size_t i = 0; i < grant_count_; ++i) {
            if (!grant_ended_[i]) {
                std::uint64_t accepted = 0;
                for (std::size_t j = 0; j < packet_count_; ++j) {
                    if (packets_[j].grant_generation == reserved_grants_[i].identity.generation) {
                        ++accepted;
                    }
                }
                EXPECT_TRUE(grants_.ConfirmTerminal({.identity = reserved_grants_[i].identity,
                                                     .no_future_writes = true,
                                                     .acceptance_known = true,
                                                     .accepted_writes = accepted,
                                                     .terminal_writes = accepted}));
            }
        }
        EXPECT_TRUE(grants_.Close());
        if (endpoint_owned_) {
            EXPECT_TRUE(endpoints_.Release(endpoint_lease_));
        }
        if (segment_owned_) {
            EXPECT_TRUE(segments_cache_.Release(segment_lease_));
        }
        endpoints_.StopAccepting();
        segments_cache_.StopAccepting();
        EXPECT_TRUE(DriveCaches());
        EXPECT_TRUE(endpoints_.Close());
        EXPECT_TRUE(segments_cache_.Close());
        EXPECT_TRUE(executor_.Close());
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
    bool DriveCaches() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline) {
            std::array<runtime::ControlTaskCompletion, 4> completions;
            const auto count = executor_.Poll(completions);
            if (!count) {
                return false;
            }
            for (std::size_t i = 0; i < *count; ++i) {
                if (endpoints_.OwnsCompletion(completions[i].id)) {
                    if (!endpoints_.HandleCompletion(completions[i])) {
                        return false;
                    }
                } else if (segments_cache_.OwnsCompletion(completions[i].id)) {
                    if (!segments_cache_.HandleCompletion(completions[i])) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            if (endpoints_.in_flight() || segments_cache_.in_flight()) {
                std::this_thread::yield();
                continue;
            }
            const auto endpoint = endpoints_.NextOperation();
            const auto segment = segments_cache_.NextOperation();
            if (!endpoint || !segment) {
                return false;
            }
            // 同 context 的两个缓存始终串行执行，屏障覆盖到完成收割。
            if (*endpoint) {
                const auto& op = **endpoint;
                if (!endpoints_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true})) {
                    return false;
                }
            } else if (*segment) {
                const auto& op = **segment;
                if (!segments_cache_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true})) {
                    return false;
                }
            } else {
                return true;
            }
        }
        return false;
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
        reserved_grants_[grant_count_++] = *grant;
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
    WriteRndvPoolConfig Config(std::uint64_t chunk = 32, std::uint32_t budget = 8, std::uint32_t slots = 2) const {
        WriteRndvPoolConfig config;
        config.connection_id = 101;
        config.binding = Binding();
        config.arena = {77, 88};
        config.endpoint = endpoint_key_;
        config.segment = segment_key_;
        config.lane = lane_;
        config.slots = slots;
        config.window = 32;
        config.chunk_bytes = chunk;
        config.max_chunks = 64;
        config.post_budget = budget;
        return config;
    }
    void Open(std::uint64_t chunk = 32, std::uint32_t budget = 8, std::uint32_t slots = 2) {
        ASSERT_TRUE(transactions_.Open(Config(chunk, budget, slots), sender_, source_, endpoints_, segments_cache_));
    }
    WriteRndvHandle Reserve(const GrantReservation& grant) {
        auto handle = transactions_.Reserve(grant.identity.transfer, grant.identity, endpoint_lease_, segment_lease_);
        EXPECT_TRUE(handle);
        return handle ? *handle : WriteRndvHandle{};
    }
    WriteRndvHandle Begin(const GrantReservation& grant, memory::BufferLease source) {
        const auto handle = Reserve(grant);
        const std::array sources{WriteRndvSource{source, 0, static_cast<std::size_t>(grant.identity.transfer.bytes)}};
        EXPECT_TRUE(transactions_.Start(handle, sources));
        return handle;
    }
    WriteRndvStats Stats(WriteRndvHandle handle) {
        const auto stats = transactions_.stats(handle);
        EXPECT_TRUE(stats);
        return stats ? *stats : WriteRndvStats{};
    }
    void Complete(std::size_t packet, urma_cr_status_t status = URMA_CR_SUCCESS) {
        auto result = transactions_.OnCompletion(Completion(packet, status));
        if (status == URMA_CR_SUCCESS) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    void MarkEnded(const GrantReservation& grant) {
        for (std::size_t i = 0; i < grant_count_; ++i) {
            if (reserved_grants_[i].handle == grant.handle) {
                grant_ended_[i] = true;
            }
        }
    }
    void DeliverAndReturn(WriteRndvHandle handle, const GrantReservation& grant) {
        auto done = transactions_.DoneEvidence(handle);
        ASSERT_TRUE(done);
        EXPECT_EQ(done->identity, grant.identity);
        ASSERT_TRUE(grants_.CompleteDone(*done));
        auto view = grants_.Borrow(grant.handle);
        ASSERT_TRUE(view);
        ASSERT_TRUE(grants_.Return(grant.handle));
        MarkEnded(grant);
    }
    void EndFailedGrant(WriteRndvHandle handle, const GrantReservation& grant) {
        auto evidence = transactions_.TerminalEvidence(handle);
        ASSERT_TRUE(evidence);
        EXPECT_EQ(evidence->identity, grant.identity);
        ASSERT_TRUE(grants_.ConfirmTerminal(*evidence));
        MarkEnded(grant);
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
            const auto record = ledger_.Lookup(packet.id);
            if (record) {
                packet.grant_generation = record->metadata.grant_lease;
            }
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
    WriteRndvPool transactions_;
    runtime::ControlExecutor executor_;
    control::EndpointImportCache endpoints_;
    control::TsegImportCache segments_cache_;
    control::EndpointImportKey endpoint_key_;
    control::TsegImportKey segment_key_;
    control::ImportLease endpoint_lease_;
    control::TsegImportLease segment_lease_;
    bool endpoint_owned_ = true, segment_owned_ = true;
    std::atomic<unsigned> jetty_imports_{0}, jetty_unimports_{0}, segment_imports_{0}, segment_unimports_{0};
    std::array<GrantReservation, 128> reserved_grants_{};
    std::array<bool, 128> grant_ended_{};
    std::size_t grant_count_ = 0;
    std::array<memory::BufferLease, 128> leases_{};
    std::array<bool, 128> cpu_held_{};
    std::size_t lease_count_ = 0;
    std::array<Packet, 1024> packets_{};
    std::array<urma_cr_t, 1024> cq_{};
    std::size_t packet_count_ = 0, cq_begin_ = 0, cq_end_ = 0;
    std::array<std::size_t, 1024> offered_{};
    std::size_t post_calls_ = 0, jfc_creates_ = 0, jetty_creates_ = 0;
    std::size_t registrations_ = 0, unregisters_ = 0, modifies_ = 0;
    PostMode mode_ = PostMode::kAll;
    urma_status_t rejection_ = URMA_ENOMEM;
    std::size_t partial_ = 0;
};

TEST_F(WriteRndvPoolTest, CompleteAllChunksBeforeDoneAndKeepImportsUntilRelease) {
    Open();
    const auto grant = Target(77);
    const auto source = Source(77);
    const auto handle = Begin(grant, source);
    EXPECT_EQ(endpoints_.leases(), 2u);
    EXPECT_EQ(segments_cache_.leases(), 2u);
    ReleaseCpu(source);
    ASSERT_TRUE(endpoints_.Release(endpoint_lease_));
    ASSERT_TRUE(segments_cache_.Release(segment_lease_));
    endpoint_owned_ = segment_owned_ = false;
    EXPECT_EQ(endpoints_.EvictUnused(), 0u);
    EXPECT_EQ(segments_cache_.EvictUnused(), 0u);
    ASSERT_EQ(transactions_.Progress(handle), 3u);
    // 分块乱序退休不能提前暴露目标；独立缓存 pin 覆盖所有 WRITE。
    Complete(2);
    EXPECT_FALSE(transactions_.DoneEvidence(handle));
    EXPECT_FALSE(transactions_.Release(handle));
    Complete(0);
    Complete(1);
    EXPECT_EQ(Stats(handle).successful_bytes, 77u);
    const auto done = transactions_.DoneEvidence(handle);
    ASSERT_TRUE(done);
    ASSERT_TRUE(grants_.CompleteDone(*done));
    const auto view = grants_.Borrow(grant.handle);
    ASSERT_TRUE(view);
    for (std::size_t i = 0; i < view->size(); ++i) {
        EXPECT_EQ((*view)[i], static_cast<std::byte>((i * 17 + 1) & 255));
    }
    ASSERT_TRUE(grants_.Return(grant.handle));
    MarkEnded(grant);
    EXPECT_EQ(jetty_unimports_.load(), 0u);
    EXPECT_EQ(segment_unimports_.load(), 0u);
    ASSERT_TRUE(transactions_.Release(handle));
    EXPECT_EQ(endpoints_.leases(), 0u);
    EXPECT_EQ(segments_cache_.leases(), 0u);
    EXPECT_EQ(endpoints_.EvictUnused(), 1u);
    EXPECT_EQ(segments_cache_.EvictUnused(), 1u);
    ASSERT_TRUE(DriveCaches());
    EXPECT_EQ(jetty_unimports_.load(), 1u);
    EXPECT_EQ(segment_unimports_.load(), 1u);
}

TEST_F(WriteRndvPoolTest, PhysicalSlotReuseRejectsOldHandleAndLateCompletion) {
    Open(32, 8, 1);
    const auto first_grant = Target(32);
    const auto source = Source(32);
    const auto first = Begin(first_grant, source);
    ASSERT_EQ(transactions_.Progress(first), 1u);
    const auto old_event = Completion(0);
    ASSERT_TRUE(transactions_.OnCompletion(old_event));
    DeliverAndReturn(first, first_grant);
    ASSERT_TRUE(transactions_.Release(first));
    const auto second_grant = Target(32, 1);
    const auto second = Begin(second_grant, source);
    EXPECT_EQ(first.slot, second.slot);
    EXPECT_GT(second.generation, first.generation);
    ExpectError(transactions_.Cancel(first), WriteRndvPoolErrorCode::kInvalidHandle);
    ExpectError(transactions_.Release(first), WriteRndvPoolErrorCode::kInvalidHandle);
    EXPECT_FALSE(transactions_.stats(first));
    ASSERT_EQ(transactions_.Progress(second), 1u);
    EXPECT_FALSE(transactions_.OnCompletion(old_event));
    EXPECT_EQ(Stats(second).retired, 0u);
    EXPECT_EQ(source_.stats().local_dma, 1u);
    Complete(1);
    DeliverAndReturn(second, second_grant);
    ASSERT_TRUE(transactions_.Release(second));
}

TEST_F(WriteRndvPoolTest, ActiveCapacityBackpressureDoesNotBurnSequenceOrPinImports) {
    Open(32, 8, 1);
    const auto first_grant = Target(32);
    const auto second_grant = Target(32, 1);
    const auto first = Reserve(first_grant);
    ExpectError(
        transactions_.Reserve(second_grant.identity.transfer, second_grant.identity, endpoint_lease_, segment_lease_),
        WriteRndvPoolErrorCode::kWouldBlock);
    EXPECT_EQ(endpoints_.leases(), 2u);
    EXPECT_EQ(segments_cache_.leases(), 2u);
    ASSERT_TRUE(transactions_.Cancel(first));
    EndFailedGrant(first, first_grant);
    ASSERT_TRUE(transactions_.Release(first));
    const auto second = Reserve(second_grant);
    ASSERT_TRUE(transactions_.Cancel(second));
    EndFailedGrant(second, second_grant);
    ASSERT_TRUE(transactions_.Release(second));
}

TEST_F(WriteRndvPoolTest, CancelReservedTransactionProducesOnlyZeroWriteCleanupEvidence) {
    Open();
    const auto grant = Target(32);
    const auto handle = Reserve(grant);
    EXPECT_FALSE(transactions_.DoneEvidence(handle));
    EXPECT_FALSE(transactions_.TerminalEvidence(handle));
    EXPECT_FALSE(transactions_.Release(handle));
    ASSERT_TRUE(transactions_.Cancel(handle));
    auto evidence = transactions_.TerminalEvidence(handle);
    ASSERT_TRUE(evidence);
    EXPECT_TRUE(evidence->no_future_writes);
    EXPECT_TRUE(evidence->acceptance_known);
    EXPECT_EQ(evidence->accepted_writes, 0u);
    EXPECT_FALSE(transactions_.DoneEvidence(handle));
    EXPECT_EQ(post_calls_, 0u);
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
    EXPECT_EQ(transactions_.stats().retired, 1u);
}

TEST_F(WriteRndvPoolTest, FailedSourceAdmissionKeepsHandleForRetryOrCancellation) {
    Open();
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto handle = Reserve(grant);
    const std::array bad{WriteRndvSource{source, 1, 32}};
    EXPECT_FALSE(transactions_.Start(handle, bad));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(endpoints_.leases(), 2u);
    const std::array valid{WriteRndvSource{source, 0, 32}};
    ASSERT_TRUE(transactions_.Start(handle, valid));
    EXPECT_FALSE(transactions_.Start(handle, valid));
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    Complete(0);
    DeliverAndReturn(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, CancelInFlightKeepsBothPinsUntilTrustedTerminal) {
    Open();
    const auto grant = Target(64);
    const auto source = Source(64);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 2u);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EXPECT_FALSE(transactions_.Release(handle));
    EXPECT_FALSE(transactions_.TerminalEvidence(handle));
    Complete(1);
    EXPECT_EQ(endpoints_.leases(), 2u);
    EXPECT_EQ(segments_cache_.leases(), 2u);
    Complete(0);
    EXPECT_FALSE(transactions_.DoneEvidence(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
    EXPECT_EQ(endpoints_.leases(), 1u);
    EXPECT_EQ(segments_cache_.leases(), 1u);
}

TEST_F(WriteRndvPoolTest, PartialAcceptanceSubmitsOnlyRemainingOriginalRanges) {
    Open(16, 4);
    const auto grant = Target(64);
    const auto source = Source(64);
    const auto handle = Begin(grant, source);
    mode_ = PostMode::kPartial;
    partial_ = 1;
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    EXPECT_EQ(Stats(handle).queued, 3u);
    Complete(0);
    mode_ = PostMode::kAll;
    ASSERT_EQ(transactions_.Progress(handle), 3u);
    EXPECT_EQ(packet_count_, 4u);
    for (std::size_t i = 0; i < packet_count_; ++i) {
        EXPECT_EQ(packets_[i].remote.addr, grant.identity.target_address + i * 16);
    }
    Complete(3);
    Complete(1);
    Complete(2);
    DeliverAndReturn(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, SegmentInvalidationStopsUnsubmittedSuffixButRetainsAcceptedWrite) {
    Open(16, 1);
    const auto grant = Target(48);
    const auto source = Source(48);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    ASSERT_TRUE(segments_cache_.Invalidate(segment_lease_));
    EXPECT_FALSE(transactions_.Progress(handle));
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(Stats(handle).queued, 0u);
    EXPECT_EQ(source_.stats().local_dma, 1u);
    ASSERT_TRUE(segments_cache_.Release(segment_lease_));
    segment_owned_ = false;
    ASSERT_TRUE(DriveCaches());
    EXPECT_EQ(segment_unimports_.load(), 0u);
    Complete(0);
    EXPECT_FALSE(transactions_.DoneEvidence(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
    ASSERT_TRUE(DriveCaches());
    EXPECT_EQ(segment_unimports_.load(), 1u);
}

TEST_F(WriteRndvPoolTest, EndpointInvalidationBeforeFirstPostCancelsSourceReferences) {
    Open();
    const auto grant = Target(64);
    const auto source = Source(64);
    const auto handle = Begin(grant, source);
    ASSERT_TRUE(endpoints_.Invalidate(endpoint_lease_));
    EXPECT_FALSE(transactions_.Progress(handle));
    EXPECT_EQ(post_calls_, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, CacheInvalidationBetweenReserveAndStartCannotExposeSource) {
    Open();
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto handle = Reserve(grant);
    ASSERT_TRUE(segments_cache_.Invalidate(segment_lease_));
    const std::array sources{WriteRndvSource{source, 0, 32}};
    EXPECT_FALSE(transactions_.Start(handle, sources));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(post_calls_, 0u);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, StatusEightFailsOnlyItsTransactionOnSharedLane) {
    Open();
    const auto failed_grant = Target(32);
    const auto healthy_grant = Target(32, 1);
    const auto source = Source(32);
    const auto failed = Begin(failed_grant, source);
    const auto healthy = Begin(healthy_grant, Source(32, 2));
    ASSERT_EQ(transactions_.Progress(failed), 1u);
    Complete(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    EXPECT_EQ(pool_.lane_state(lane_), raw::JettyLaneState::kReady);
    EndFailedGrant(failed, failed_grant);
    ASSERT_TRUE(transactions_.Release(failed));
    ASSERT_EQ(transactions_.Progress(healthy), 1u);
    Complete(1);
    DeliverAndReturn(healthy, healthy_grant);
    ASSERT_TRUE(transactions_.Release(healthy));
}

TEST_F(WriteRndvPoolTest, StatusNineStopsLaneAndKeepsPendingWritePin) {
    Open();
    const auto grant = Target(64);
    const auto source = Source(64);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 2u);
    Complete(0, URMA_CR_ACK_TIMEOUT_ERR);
    EXPECT_NE(pool_.lane_state(lane_), raw::JettyLaneState::kReady);
    EXPECT_FALSE(transactions_.Release(handle));
    EXPECT_EQ(endpoints_.leases(), 2u);
    Complete(1, URMA_CR_WR_FLUSH_ERR);
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
    DrainFaultedLane();
}

TEST_F(WriteRndvPoolTest, RetiredSequenceCannotBeReplayedAndAdvanceIsAtomic) {
    Open();
    const auto first_grant = Target(32);
    const auto second_grant = Target(32, 1);
    const auto first = Reserve(first_grant);
    const auto second = Reserve(second_grant);
    ASSERT_TRUE(transactions_.Cancel(first));
    EndFailedGrant(first, first_grant);
    ASSERT_TRUE(transactions_.Release(first));
    ExpectError(
        transactions_.Reserve(first_grant.identity.transfer, first_grant.identity, endpoint_lease_, segment_lease_),
        WriteRndvPoolErrorCode::kDuplicate);
    EXPECT_FALSE(transactions_.AdvanceWindow(2));
    EXPECT_EQ(transactions_.stats().window_begin, 0u);
    EXPECT_EQ(transactions_.stats().retired, 1u);
    ASSERT_TRUE(transactions_.AdvanceWindow(1));
    EXPECT_EQ(transactions_.stats().window_begin, 1u);
    ExpectError(
        transactions_.Reserve(first_grant.identity.transfer, first_grant.identity, endpoint_lease_, segment_lease_),
        WriteRndvPoolErrorCode::kStaleSequence);
    EXPECT_FALSE(transactions_.AdvanceWindow(0));
    ASSERT_TRUE(transactions_.Cancel(second));
    EndFailedGrant(second, second_grant);
    ASSERT_TRUE(transactions_.Release(second));
    ASSERT_TRUE(transactions_.AdvanceWindow(2));
}

TEST_F(WriteRndvPoolTest, GrantHighwaterSurvivesSequenceTombstoneRetirement) {
    Open();
    const auto grant = Target(32);
    const auto handle = Reserve(grant);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
    ASSERT_TRUE(transactions_.AdvanceWindow(1));
    auto stale_grant = grant.identity;
    stale_grant.transfer.seq = 1;
    ++stale_grant.transfer.transfer_id;
    EXPECT_FALSE(transactions_.Reserve(stale_grant.transfer, stale_grant, endpoint_lease_, segment_lease_));
    EXPECT_EQ(transactions_.stats().active, 0u);
    EXPECT_EQ(endpoints_.leases(), 1u);
}

TEST_F(WriteRndvPoolTest, ActiveGrantSlotAndTargetRangeCannotBeSharedByDifferentSequences) {
    Open();
    const auto grant = Target(32);
    const auto handle = Reserve(grant);
    auto conflict = grant.identity;
    conflict.transfer.seq = 1;
    ++conflict.transfer.transfer_id;
    ++conflict.generation;
    EXPECT_FALSE(transactions_.Reserve(conflict.transfer, conflict, endpoint_lease_, segment_lease_));
    // 即使换成另一个远端槽，也不能覆盖已活跃 Grant 的目标区间。
    ++conflict.slot;
    ++conflict.target_address;
    EXPECT_FALSE(transactions_.Reserve(conflict.transfer, conflict, endpoint_lease_, segment_lease_));
    EXPECT_EQ(transactions_.stats().active, 1u);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, FullTransferAndArenaIdentityCheckedBeforeRetainingImports) {
    Open();
    const auto grant = Target(32);
    for (unsigned field = 0; field < 11; ++field) {
        auto altered = grant.identity;
        switch (field) {
        case 0:
            ++altered.transfer.binding.sender_runtime;
            break;
        case 1:
            ++altered.transfer.binding.receiver_runtime;
            break;
        case 2:
            ++altered.transfer.binding.receive_domain;
            break;
        case 3:
            ++altered.transfer.binding.session;
            break;
        case 4:
            ++altered.transfer.binding.token.slot;
            break;
        case 5:
            ++altered.transfer.binding.token.generation;
            break;
        case 6:
            ++altered.transfer.transfer_id;
            break;
        case 7:
            ++altered.transfer.seq;
            break;
        case 8:
            ++altered.transfer.bytes;
            break;
        case 9:
            ++altered.arena.id;
            break;
        case 10:
            ++altered.arena.generation;
            break;
        }
        EXPECT_FALSE(transactions_.Reserve(grant.identity.transfer, altered, endpoint_lease_, segment_lease_));
        EXPECT_EQ(transactions_.stats().active, 0u);
        EXPECT_EQ(endpoints_.leases(), 1u);
        EXPECT_EQ(segments_cache_.leases(), 1u);
    }
}

TEST_F(WriteRndvPoolTest, OutOfRangeAndReservedFinSequenceDoNotConsumeCapacity) {
    Open();
    const auto grant = Target(32);
    auto altered = grant.identity;
    altered.transfer.seq = std::numeric_limits<std::uint32_t>::max();
    EXPECT_FALSE(transactions_.Reserve(altered.transfer, altered, endpoint_lease_, segment_lease_));
    altered = grant.identity;
    altered.transfer.seq = 32;
    EXPECT_FALSE(transactions_.Reserve(altered.transfer, altered, endpoint_lease_, segment_lease_));
    altered = grant.identity;
    altered.target_address = std::numeric_limits<std::uint64_t>::max() - 3;
    EXPECT_FALSE(transactions_.Reserve(altered.transfer, altered, endpoint_lease_, segment_lease_));
    altered = grant.identity;
    altered.target_address = remote_.seg.ubva.va + remote_.seg.len;
    EXPECT_FALSE(transactions_.Reserve(altered.transfer, altered, endpoint_lease_, segment_lease_));
    EXPECT_EQ(transactions_.stats().active, 0u);
    EXPECT_EQ(endpoints_.leases(), 1u);
}

TEST_F(WriteRndvPoolTest, InvalidAndForeignHandlesCannotCancelAnotherTransaction) {
    Open();
    const auto grant = Target(32);
    const auto handle = Reserve(grant);
    for (unsigned field = 0; field < 3; ++field) {
        auto invalid = handle;
        if (field == 0) {
            invalid.owner = nullptr;
        } else if (field == 1) {
            invalid.slot = 999;
        } else {
            ++invalid.generation;
        }
        EXPECT_FALSE(transactions_.Cancel(invalid));
        EXPECT_FALSE(transactions_.Release(invalid));
        EXPECT_FALSE(transactions_.DoneEvidence(invalid));
    }
    EXPECT_EQ(transactions_.stats().active, 1u);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, WrongCompletionRoutingCannotRetireSourceOrReleasePins) {
    Open();
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    const auto event = Completion(0);
    ASSERT_TRUE(event.record);
    for (unsigned field = 0; field < 4; ++field) {
        auto bad = event;
        if (field == 0) {
            ++bad.record->metadata.connection_id;
        } else if (field == 1) {
            ++bad.record->metadata.operation_generation;
        } else if (field == 2) {
            ++bad.record->metadata.grant_lease;
        } else {
            ++bad.record->id;
        }
        EXPECT_FALSE(transactions_.OnCompletion(bad));
        EXPECT_EQ(source_.stats().local_dma, 1u);
    }
    ASSERT_TRUE(transactions_.OnCompletion(event));
    EXPECT_FALSE(transactions_.OnCompletion(event));
    DeliverAndReturn(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, StopBlocksNewAdmissionButAllowsAdmittedTransactionToFinish) {
    Open();
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto handle = Reserve(grant);
    transactions_.Stop();
    const auto other = Target(32, 1);
    ExpectError(transactions_.Reserve(other.identity.transfer, other.identity, endpoint_lease_, segment_lease_),
                WriteRndvPoolErrorCode::kStopped);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    ASSERT_TRUE(transactions_.Start(handle, sources));
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    Complete(0);
    DeliverAndReturn(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, CloseCancelsQueuedButWaitsForActualAcceptedCompletion) {
    Open(16, 1);
    const auto grant = Target(32);
    const auto source = Source(32);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    EXPECT_FALSE(transactions_.Close());
    EXPECT_EQ(source_.stats().local_dma, 1u);
    EXPECT_EQ(endpoints_.leases(), 2u);
    Complete(0);
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Close());
    EXPECT_EQ(endpoints_.leases(), 1u);
    EXPECT_EQ(segments_cache_.leases(), 1u);
    EXPECT_FALSE(transactions_.Open(Config(), sender_, source_, endpoints_, segments_cache_));
}

TEST_F(WriteRndvPoolTest, ReuseAndCancellationKeepDataPathAllocationFree) {
    Open(32, 8, 1);
    const auto source = Source(32);
    const std::array sources{WriteRndvSource{source, 0, 32}};
    // 预先构造身份和源视图；每次复用真实物理引擎槽，不能以 Close/Open 隐藏分配。
    for (std::uint32_t seq = 0; seq < 16; ++seq) {
        const auto grant = Target(32, seq);
        allocations = deallocations = 0;
        count_allocations = true;
        const auto handle =
            transactions_.Reserve(grant.identity.transfer, grant.identity, endpoint_lease_, segment_lease_);
        bool started = false, progressed = false, completed = false, evidence_ready = false, released = false;
        if (handle) {
            started = transactions_.Start(*handle, sources).has_value();
            if (started && (seq % 2 == 0)) {
                const auto first = packet_count_;
                progressed = transactions_.Progress(*handle).has_value();
                if (progressed) {
                    const auto event = Completion(first);
                    completed = transactions_.OnCompletion(event).has_value();
                }
                evidence_ready = transactions_.DoneEvidence(*handle).has_value();
            } else if (started) {
                progressed = completed = transactions_.Cancel(*handle).has_value();
                evidence_ready = transactions_.TerminalEvidence(*handle).has_value();
            }
            released = transactions_.Release(*handle).has_value();
        }
        const auto advanced = transactions_.AdvanceWindow(seq + 1);
        count_allocations = false;
        ASSERT_TRUE(handle);
        EXPECT_TRUE(started && progressed && completed && evidence_ready && released && advanced.has_value());
        EXPECT_EQ(allocations, 0u);
        EXPECT_EQ(deallocations, 0u);
        // 证据已在本地核验；测试用确定的 Mock 终结事实丢弃目标，不尝试重启旧事务。
        DiscardGrant(grant, seq % 2 == 0 ? 1 : 0);
        MarkEnded(grant);
        ASSERT_TRUE(grants_.AdvanceWindow(seq + 1));
    }
    EXPECT_EQ(transactions_.stats().active, 0u);
    EXPECT_EQ(jetty_imports_.load(), 1u);
    EXPECT_EQ(segment_imports_.load(), 1u);
}

TEST_F(WriteRndvPoolTest, PendingImportBackpressureDoesNotConsumeSequenceOrGeneration) {
    Open();
    const auto grant = Target(32);
    ASSERT_TRUE(endpoints_.Release(endpoint_lease_));
    EXPECT_EQ(endpoints_.EvictUnused(), 1u);
    ASSERT_TRUE(DriveCaches());
    urma_rjetty_t descriptor{};
    descriptor.jetty_id.id = 40;
    descriptor.jetty_id.eid = receiver_context_.eid;
    descriptor.trans_mode = URMA_TM_RM;
    descriptor.tp_type = URMA_CTP;
    descriptor.type = URMA_JETTY;
    const auto pending = endpoints_.Acquire({endpoint_key_, std::as_bytes(std::span(&descriptor, 1)), {}});
    ASSERT_TRUE(pending);
    endpoint_lease_ = *pending;
    const auto before = transactions_.stats();
    const auto result = transactions_.Reserve(grant.identity.transfer, grant.identity, endpoint_lease_, segment_lease_);
    ExpectError(result, WriteRndvPoolErrorCode::kWouldBlock);
    ASSERT_TRUE(result.error().endpoint_error);
    EXPECT_EQ(result.error().endpoint_error->code, control::ImportCacheErrorCode::kPending);
    EXPECT_EQ(transactions_.stats().active, before.active);
    EXPECT_EQ(transactions_.stats().retired, before.retired);
    EXPECT_EQ(endpoints_.leases(), 1u);
    EXPECT_EQ(segments_cache_.leases(), 1u);
    ASSERT_TRUE(DriveCaches());
    const auto handle = Reserve(grant);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, ExactImportKeysRejectForeignCacheTicketsBeforeRetaining) {
    Open();
    const auto grant = Target(32);
    auto wrong_segment_key = segment_key_;
    ++wrong_segment_key.slab_generation;
    const auto other_segment = segments_cache_.Acquire({wrong_segment_key, segments_[1].seg, {}});
    ASSERT_TRUE(other_segment);
    ExpectError(transactions_.Reserve(grant.identity.transfer, grant.identity, endpoint_lease_, *other_segment),
                WriteRndvPoolErrorCode::kWrongIdentity);
    EXPECT_EQ(endpoints_.leases(), 1u);
    EXPECT_EQ(segments_cache_.leases(), 2u);
    ASSERT_TRUE(segments_cache_.Release(*other_segment));
    auto wrong_endpoint_key = endpoint_key_;
    ++wrong_endpoint_key.endpoint_generation;
    urma_rjetty_t descriptor{};
    descriptor.jetty_id.id = 40;
    descriptor.jetty_id.eid = receiver_context_.eid;
    descriptor.trans_mode = URMA_TM_RM;
    descriptor.tp_type = URMA_CTP;
    descriptor.type = URMA_JETTY;
    const auto other_endpoint = endpoints_.Acquire({wrong_endpoint_key, std::as_bytes(std::span(&descriptor, 1)), {}});
    ASSERT_TRUE(other_endpoint);
    ExpectError(transactions_.Reserve(grant.identity.transfer, grant.identity, *other_endpoint, segment_lease_),
                WriteRndvPoolErrorCode::kWrongIdentity);
    EXPECT_EQ(endpoints_.leases(), 2u);
    EXPECT_EQ(segments_cache_.leases(), 1u);
    ASSERT_TRUE(endpoints_.Release(*other_endpoint));
    const auto handle = Reserve(grant);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, ActualImportedObjectsMustMatchIdentityAndWritePermission) {
    Open();
    const auto grant = Target(32);
    const auto original_endpoint = endpoint_;
    const auto original_segment = remote_;
    for (unsigned mutation = 0; mutation < 13; ++mutation) {
        endpoint_ = original_endpoint;
        remote_ = original_segment;
        switch (mutation) {
        case 0:
            endpoint_.urma_ctx = &foreign_context_;
            break;
        case 1:
            ++endpoint_.id.id;
            break;
        case 2:
            ++endpoint_.id.eid.raw[0];
            break;
        case 3:
            endpoint_.trans_mode = URMA_TM_RC;
            break;
        case 4:
            endpoint_.tp_type = URMA_UTP;
            break;
        case 5:
            endpoint_.type = URMA_JFR;
            break;
        case 6:
            remote_.urma_ctx = &foreign_context_;
            break;
        case 7:
            ++remote_.seg.ubva.eid.raw[0];
            break;
        case 8:
            remote_.seg.attr.bs.access = URMA_ACCESS_READ;
            break;
        case 9:
            remote_.seg.attr.bs.access |= URMA_ACCESS_LOCAL_ONLY;
            break;
        case 10:
            remote_.seg.len = 1;
            break;
        case 11:
            remote_.seg.ubva.va = std::numeric_limits<std::uint64_t>::max() - 1;
            break;
        case 12:
            remote_.seg.ubva.va = 0;
            break;
        }
        const auto result =
            transactions_.Reserve(grant.identity.transfer, grant.identity, endpoint_lease_, segment_lease_);
        EXPECT_FALSE(result) << "mutation=" << mutation;
        EXPECT_EQ(transactions_.stats().active, 0u);
        EXPECT_EQ(endpoints_.leases(), 1u);
        EXPECT_EQ(segments_cache_.leases(), 1u);
    }
    endpoint_ = original_endpoint;
    remote_ = original_segment;
    const auto handle = Reserve(grant);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, SecondCacheRetainFailureRollsBackFirstPinWithoutBurningSequence) {
    Open();
    const auto grant = Target(32);
    std::array<control::TsegImportLease, 31> busy;
    for (auto& ticket : busy) {
        const auto retained = segments_cache_.Retain(segment_lease_);
        ASSERT_TRUE(retained);
        ticket = *retained;
    }
    const auto full = transactions_.Reserve(grant.identity.transfer, grant.identity, endpoint_lease_, segment_lease_);
    ExpectError(full, WriteRndvPoolErrorCode::kWouldBlock);
    EXPECT_EQ(endpoints_.leases(), 1u);
    EXPECT_EQ(segments_cache_.leases(), 32u);
    EXPECT_EQ(transactions_.stats().active, 0u);
    for (const auto ticket : busy) {
        ASSERT_TRUE(segments_cache_.Release(ticket));
    }
    const auto handle = Reserve(grant);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

TEST_F(WriteRndvPoolTest, InvalidConfigurationCannotBindDifferentDomainOrUnboundedStorage) {
    for (unsigned mutation = 0; mutation < 15; ++mutation) {
        auto config = Config();
        switch (mutation) {
        case 0:
            config.connection_id = 0;
            break;
        case 1:
            config.slots = 0;
            break;
        case 2:
            config.slots = config.window + 1;
            break;
        case 3:
            config.window = GrantManager::kMaxWindow + 1;
            break;
        case 4:
            config.max_chunks = WriteRndvEngine::kMaxChunks + 1;
            break;
        case 5:
            config.endpoint.local_context = &foreign_context_;
            break;
        case 6:
            ++config.segment.local_context_incarnation;
            break;
        case 7:
            ++config.endpoint.peer_runtime;
            break;
        case 8:
            ++config.segment.receive_domain;
            break;
        case 9:
            ++config.segment.slab_generation;
            break;
        case 10:
            ++config.segment.authorization_identity;
            break;
        case 11:
            ++config.segment.eid[0];
            break;
        case 12:
            config.endpoint.endpoint_generation = 0;
            break;
        case 13:
            config.initial_seq = std::numeric_limits<std::uint32_t>::max();
            break;
        case 14:
            config.binding.token.generation = 0;
            break;
        }
        WriteRndvPool invalid;
        EXPECT_FALSE(invalid.Open(config, sender_, source_, endpoints_, segments_cache_)) << "mutation=" << mutation;
        EXPECT_EQ(endpoints_.leases(), 1u);
        EXPECT_EQ(segments_cache_.leases(), 1u);
    }
    Open();
}

class WriteRndvPoolSmallReferencesTest : public WriteRndvPoolTest {
protected:
    std::uint32_t SourceReferences() const noexcept override {
        return 1;
    }
};
TEST_F(WriteRndvPoolSmallReferencesTest, SourcePinExhaustionRollsBackWithoutLosingReservedHandle) {
    Open();
    const auto grant = Target(64);
    const auto source = Source(64);
    const auto handle = Reserve(grant);
    const std::array sources{WriteRndvSource{source, 0, 64}};
    EXPECT_FALSE(transactions_.Start(handle, sources));
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(transactions_.stats().reserved, 1u);
    EXPECT_EQ(endpoints_.leases(), 2u);
    EXPECT_EQ(segments_cache_.leases(), 2u);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EndFailedGrant(handle, grant);
    ASSERT_TRUE(transactions_.Release(handle));
}

class WriteRndvPoolDeathTest : public WriteRndvPoolTest {};
TEST_F(WriteRndvPoolDeathTest, UnknownAcceptanceRetainsBothImportsAfterKnownLocalDmaRetires) {
    // 子进程保留整个依赖图直到退出；不为未知接受事务伪造接收端授权或强行 Close。
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            Open(16, 3, 1);
            const auto grant = Target(64);
            const auto source = Source(64);
            const auto handle = Begin(grant, source);
            ReleaseCpu(source);
            mode_ = PostMode::kUnknown;
            ExpectError(transactions_.Progress(handle), WriteRndvPoolErrorCode::kAcceptanceUnknown);
            EXPECT_FALSE(Stats(handle).acceptance_known);
            EXPECT_EQ(packet_count_, 3u);
            EXPECT_FALSE(transactions_.Progress(handle));
            EXPECT_EQ(post_calls_, 1u);
            EXPECT_FALSE(transactions_.DoneEvidence(handle));
            EXPECT_FALSE(transactions_.TerminalEvidence(handle));
            EXPECT_FALSE(transactions_.Release(handle));
            EXPECT_FALSE(transactions_.Close());
            // Mock 外部事实只解除真实本地 DMA；池不能把它升格成完整接受集合证明。
            for (std::size_t i = 0; i < packet_count_; ++i) {
                EXPECT_TRUE(ledger_.Commit(packets_[i].id, i + 1));
                Complete(i);
            }
            EXPECT_EQ(source_.stats().local_dma, 0u);
            EXPECT_FALSE(transactions_.DoneEvidence(handle));
            EXPECT_FALSE(transactions_.TerminalEvidence(handle));
            ExpectError(transactions_.Release(handle), WriteRndvPoolErrorCode::kAcceptanceUnknown);
            EXPECT_FALSE(transactions_.Close());
            EXPECT_TRUE(endpoints_.Release(endpoint_lease_));
            EXPECT_TRUE(segments_cache_.Release(segment_lease_));
            EXPECT_EQ(endpoints_.leases(), 1u);
            EXPECT_EQ(segments_cache_.leases(), 1u);
            EXPECT_EQ(endpoints_.EvictUnused(), 0u);
            EXPECT_EQ(segments_cache_.EvictUnused(), 0u);
            EXPECT_EQ(jetty_unimports_.load(), 0u);
            EXPECT_EQ(segment_unimports_.load(), 0u);
            EXPECT_FALSE(endpoints_.Close());
            EXPECT_FALSE(segments_cache_.Close());
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace connection
} // namespace kbsocket
