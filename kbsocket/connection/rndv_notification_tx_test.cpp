// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_notification_tx.hpp"

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

#include "kbsocket/runtime/control_executor.hpp"
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
void ExpectError(const std::expected<T, RndvNotificationError>& result, RndvNotificationErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

class NotificationTxTest : public ::testing::Test {
protected:
    void TestBody() override {}
    struct Packet {
        std::uint64_t imm = 0;
        std::size_t bytes = 0;
        std::array<std::byte, 112> payload{};
        raw::AttemptId id = 0;
        urma_opcode_t opcode = URMA_OPC_WRITE;
        urma_target_jetty_t* target = nullptr;
        std::array<urma_sge_t, 16> sources{};
        std::size_t source_count = 0;
        urma_sge_t remote{};
        std::uint64_t grant_generation = 0;
    };
    enum class PostMode { kAll, kPartial, kUnknown };
    static inline NotificationTxTest* active_ = nullptr;
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
            sources, {.buffer_count = 32, .buffer_size = 4096, .max_references = SourceReferences()}, budget_));
        ASSERT_TRUE(target_.Open(targets, {.buffer_count = 4, .buffer_size = 4096, .max_references = 8}, budget_));
        ASSERT_TRUE(grants_.Open(Binding(), {77, 88}, target_, &receiver_context_,
                                 {.window = 32, .max_active = 4, .max_bytes = 16384}));
        ASSERT_TRUE(return_grants_.Open({22, 11, 34, 45, {56, 67}}, {78, 89}, source_, &context_,
                                        {.window = 32, .max_active = 4, .max_bytes = 16384}));
        ASSERT_TRUE(credits_.Open({22, 33, 1}));
        ASSERT_TRUE(credits_.Update({22, 33, 1}, 64, 64));
        ASSERT_TRUE(sequence_.Open({22, 33, 44}));
        ASSERT_TRUE(sequence_.Update({22, 33, 44}, 32, 16384));
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
                auto completion = Completion(i, URMA_CR_WR_FLUSH_ERR);
                if (packets_[i].opcode == URMA_OPC_WRITE) {
                    (void)transactions_.OnCompletion(completion);
                } else {
                    (void)notifications_.OnCompletion(completion);
                }
            }
        }
        EXPECT_TRUE(notifications_.Close());
        EXPECT_TRUE(transactions_.Close());
        EXPECT_TRUE(return_grants_.Close());
        EXPECT_TRUE(credits_.Close());
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
                    if (packets_[j].opcode == URMA_OPC_WRITE &&
                        packets_[j].grant_generation == reserved_grants_[i].identity.generation) {
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
    WriteRndvPoolConfig WriteConfig(std::uint64_t chunk = 32, std::uint32_t budget = 8, std::uint32_t slots = 2) const {
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
    void OpenWrites(std::uint64_t chunk = 32, std::uint32_t budget = 8, std::uint32_t slots = 2) {
        ASSERT_TRUE(
            transactions_.Open(WriteConfig(chunk, budget, slots), sender_, source_, endpoints_, segments_cache_));
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
            if (wr->opcode == URMA_OPC_SEND_IMM) {
                packet.imm = wr->send.imm_data;
                packet.source_count = wr->send.src.num_sge;
                EXPECT_EQ(packet.source_count, 1u);
                packet.sources[0] = wr->send.src.sge[0];
                packet.bytes = packet.sources[0].len;
                EXPECT_LE(packet.bytes, packet.payload.size());
                std::memcpy(packet.payload.data(), reinterpret_cast<const void*>(packet.sources[0].addr), packet.bytes);
                const auto decoded = DecodeWireFrame(packet.imm, std::span(packet.payload).first(packet.bytes));
                if (decoded && decoded->frame.control == WireControlType::kRndvGrant) {
                    grant_published_at_post_ = return_grants_.stats().published == 1;
                }
                continue;
            }
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
    RndvNotificationConfig Config(std::uint32_t capacity = 8) const {
        return {.connection_id = 202,
                .destination = {22, 33, 44, {55, 66}},
                .credit_identity = {22, 33, 1},
                .endpoint = endpoint_key_,
                .lane = lane_,
                .capacity = capacity};
    }
    void Open(std::uint32_t capacity = 8) {
        ASSERT_TRUE(notifications_.Open(Config(capacity), sender_, source_, endpoints_, endpoint_lease_, credits_));
    }
    static GrantTransfer Transfer(std::uint32_t seq = 0, std::uint64_t bytes = 77) {
        return {Binding(), 100 + seq, seq, bytes};
    }
    GrantReservation ReturnGrant(std::uint32_t seq = 0) {
        const auto grant = return_grants_.Reserve({{22, 11, 34, 45, {56, 67}}, 200 + seq, seq, 77});
        EXPECT_TRUE(grant);
        return grant ? *grant : GrantReservation{};
    }
    void CompleteNotification(std::size_t packet, urma_cr_status_t status = URMA_CR_SUCCESS) {
        const auto result = notifications_.OnCompletion(Completion(packet, status));
        EXPECT_EQ(result.has_value(), status == URMA_CR_SUCCESS);
    }
    void ProveNoRemoteWrites(const GrantReservation& grant) {
        // 测试没有创建远端 WRITE；显式可信零写证明独立于本地通知的 CQE。
        ASSERT_TRUE(return_grants_.ConfirmTerminal(
            {.identity = grant.identity, .no_future_writes = true, .acceptance_known = true}));
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
    GrantManager grants_, return_grants_;
    RndvNotificationTx notifications_;
    SenderCreditAccount credits_;
    TxSequenceWindow sequence_;
    bool grant_published_at_post_ = false;
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

TEST_F(NotificationTxTest, OpenPreallocatesRegisteredSlotsAndOwnsEndpointPin) {
    const auto free = source_.stats().available;
    Open(4);
    EXPECT_EQ(source_.stats().available, free - 4);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(endpoints_.leases(), 2u);
    // 原票据归调用者；释放后通知队列仍独立保护 imported endpoint。
    ASSERT_TRUE(endpoints_.Release(endpoint_lease_));
    endpoint_owned_ = false;
    EXPECT_EQ(endpoints_.EvictUnused(), 0u);
    EXPECT_EQ(jetty_unimports_.load(), 0u);
    ASSERT_TRUE(notifications_.Close());
    EXPECT_EQ(source_.stats().available, free);
    EXPECT_EQ(endpoints_.leases(), 0u);
}

TEST_F(NotificationTxTest, OpenRejectsRouteCreditAndActualImportedIdentityMismatch) {
    auto reject = [&](RndvNotificationConfig config) {
        RndvNotificationTx queue;
        EXPECT_FALSE(queue.Open(config, sender_, source_, endpoints_, endpoint_lease_, credits_));
        EXPECT_EQ(endpoints_.leases(), 1u);
        EXPECT_EQ(source_.stats().local_dma, 0u);
    };
    // 配置与已认证缓存键必须一致，不能只核对 peer runtime。
    auto config = Config();
    ++config.destination.runtime;
    reject(config);
    config = Config();
    ++config.credit_identity.epoch;
    reject(config);
    config = Config();
    ++config.endpoint.endpoint_generation;
    reject(config);
    config = Config();
    config.endpoint.local_context = &foreign_context_;
    reject(config);
    config = Config();
    config.connection_id = 0;
    reject(config);
    config = Config();
    config.capacity = RndvNotificationTx::kMaxCapacity + 1;
    reject(config);
    const auto saved = endpoint_;
    endpoint_.urma_ctx = &foreign_context_;
    reject(Config());
    endpoint_ = saved;
    ++endpoint_.id.id;
    reject(Config());
    endpoint_ = saved;
    endpoint_.tp_type = URMA_RTP;
    reject(Config());
    endpoint_ = saved;
    Open();
}

TEST_F(NotificationTxTest, FailedPreallocationRollsBackBuffersAndEndpointPin) {
    const auto free = source_.stats().available;
    // 33 个槽超过已注册 Slab 容量；不能留下前 32 个槽的 CPU/DMA 引用。
    EXPECT_FALSE(notifications_.Open(Config(33), sender_, source_, endpoints_, endpoint_lease_, credits_));
    EXPECT_EQ(source_.stats().available, free);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(endpoints_.leases(), 1u);
}

TEST_F(NotificationTxTest, RtsAtomicallyReservesDoneSlotControlAndSharedSequence) {
    Open(2);
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    EXPECT_NE(pair->rts, pair->done);
    EXPECT_EQ(notifications_.stats().active, 2u);
    EXPECT_EQ(credits_.stats().promised_control, 1u);
    EXPECT_EQ(credits_.stats().spent[0], 0u);
    ASSERT_EQ(sequence_.pending().size(), 1u);
    EXPECT_EQ(sequence_.pending()[0].seq, 0u);
    EXPECT_EQ(sequence_.pending()[0].bytes, 77u);
    ASSERT_TRUE(notifications_.status(pair->rts));
    EXPECT_TRUE(notifications_.status(pair->rts)->prepared);
    EXPECT_FALSE(notifications_.status(pair->done)->prepared);
    // 预留的未来 DONE 尚未成为共享 post 锁，其他连接可以继续借用物理 data credit。
    auto peer = credits_.Reserve(CreditClass::kData, 1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(credits_.Resolve(*peer, 1));
    EXPECT_FALSE(notifications_.QueueRts(Transfer(1), sequence_));
    EXPECT_EQ(credits_.stats().promised_control, 1u);
    ASSERT_TRUE(notifications_.Cancel(pair->rts));
    ASSERT_TRUE(notifications_.Release(pair->rts));
    ASSERT_TRUE(notifications_.Release(pair->done));
    EXPECT_EQ(notifications_.stats().active, 0u);
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_EQ(sequence_.next_sequence(), 0u);
}

TEST_F(NotificationTxTest, RtsRejectsWrongSequenceSessionAndWholeTransferBudgetAtomically) {
    Open();
    EXPECT_FALSE(notifications_.QueueRts(Transfer(1), sequence_));
    TxSequenceWindow wrong;
    ASSERT_TRUE(wrong.Open({22, 33, 999}));
    ASSERT_TRUE(wrong.Update({22, 33, 999}, 32, 16384));
    EXPECT_FALSE(notifications_.QueueRts(Transfer(), wrong));
    TxSequenceWindow small;
    ASSERT_TRUE(small.Open({22, 33, 44}));
    ASSERT_TRUE(small.Update({22, 33, 44}, 32, 76));
    // RTS frame 很短，但准入必须收走整个远端 WRITE 的 77 字节逻辑预算。
    EXPECT_FALSE(notifications_.QueueRts(Transfer(), small));
    EXPECT_EQ(notifications_.stats().active, 0u);
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_TRUE(small.pending().empty());
    ASSERT_TRUE(small.Update({22, 33, 44}, 32, 77));
    ASSERT_TRUE(notifications_.QueueRts(Transfer(), small));
    // sequence 是队列责任依赖，须在临时对象析构前停止队列并归还预约。
    ASSERT_TRUE(notifications_.Close());
}

TEST_F(NotificationTxTest, RtsNeedsTwoSlotsAndPreservesLastControlCredit) {
    Open(1);
    EXPECT_FALSE(notifications_.QueueRts(Transfer(), sequence_));
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    ASSERT_TRUE(notifications_.Close());
    RndvNotificationTx queue;
    ASSERT_TRUE(credits_.Close());
    ASSERT_TRUE(credits_.Open({22, 33, 2}));
    ASSERT_TRUE(credits_.Update({22, 33, 2}, 64, 1));
    auto config = Config(4);
    config.credit_identity.epoch = 2;
    ASSERT_TRUE(queue.Open(config, sender_, source_, endpoints_, endpoint_lease_, credits_));
    EXPECT_FALSE(queue.QueueRts(Transfer(), sequence_));
    EXPECT_EQ(credits_.available(CreditClass::kControl), 1u);
    EXPECT_EQ(queue.stats().active, 0u);
    ASSERT_TRUE(queue.Close());
}

TEST_F(NotificationTxTest, ZeroAcceptanceRetriesIdenticalRtsWithoutReplayingAcceptedSend) {
    Open();
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    mode_ = PostMode::kPartial;
    partial_ = 0;
    ExpectError(notifications_.Progress(), RndvNotificationErrorCode::kWouldBlock);
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(packet_count_, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(sequence_.next_sequence(), 0u);
    EXPECT_EQ(credits_.stats().spent[0], 0u);
    EXPECT_EQ(credits_.stats().promised_control, 1u);
    ASSERT_EQ(sequence_.pending().size(), 1u);
    EXPECT_EQ(sequence_.pending()[0].seq, 0u);
    mode_ = PostMode::kAll;
    ASSERT_EQ(notifications_.Progress(), 1u);
    ASSERT_EQ(packet_count_, 1u);
    auto frame = DecodeWireFrame(packets_[0].imm, std::span(packets_[0].payload).first(packets_[0].bytes));
    ASSERT_TRUE(frame);
    EXPECT_EQ(frame->frame.control, WireControlType::kRndvRts);
    EXPECT_EQ(frame->frame.seq, 0u);
    EXPECT_EQ(frame->frame.rndv.bytes, 77u);
    EXPECT_EQ(sequence_.next_sequence(), 1u);
    EXPECT_EQ(sequence_.accepted_bytes(), 77u);
    EXPECT_EQ(credits_.stats().spent[0], 1u);
    EXPECT_EQ(source_.stats().local_dma, 1u);
    ASSERT_EQ(notifications_.Progress(), 0u);
    EXPECT_EQ(post_calls_, 2u);
    CompleteNotification(0);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_TRUE(notifications_.status(pair->rts)->retired);
    EXPECT_EQ(credits_.stats().spent[0], 1u);
    ASSERT_TRUE(notifications_.Release(pair->rts));
    ASSERT_TRUE(notifications_.Cancel(pair->done));
    ASSERT_TRUE(notifications_.Release(pair->done));
}

TEST_F(NotificationTxTest, SharedSequenceContinuesAfterOtherFrameAndAcceptedRts) {
    Open();
    const std::array eager{TxFramePlan{3, false}};
    const auto earlier = sequence_.Reserve(eager);
    ASSERT_TRUE(earlier);
    ASSERT_TRUE(sequence_.Resolve(*earlier, 1));
    auto pair = notifications_.QueueRts(Transfer(1), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    EXPECT_EQ(sequence_.next_sequence(), 2u);
    EXPECT_EQ(sequence_.accepted_bytes(), 80u);
    const auto later = sequence_.Reserve(eager);
    ASSERT_TRUE(later);
    ASSERT_EQ(sequence_.pending().size(), 1u);
    EXPECT_EQ(sequence_.pending()[0].seq, 2u);
    ASSERT_TRUE(sequence_.Resolve(*later, 1));
    ASSERT_TRUE(notifications_.Cancel(pair->done));
}

TEST_F(NotificationTxTest, QueueGrantRejectsForgedIdentityAndDuplicateAuthority) {
    Open();
    const auto grant = ReturnGrant();
    auto forged = grant;
    ++forged.identity.target_address;
    EXPECT_FALSE(notifications_.QueueGrant(return_grants_, forged));
    EXPECT_EQ(return_grants_.stats().reserved, 1u);
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    const auto queued = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(queued);
    EXPECT_FALSE(notifications_.QueueGrant(return_grants_, grant));
    EXPECT_EQ(return_grants_.stats().published, 0u);
    EXPECT_EQ(notifications_.stats().active, 1u);
    ASSERT_TRUE(notifications_.Cancel(*queued));
    EXPECT_EQ(return_grants_.stats().active, 0u);
    ASSERT_TRUE(notifications_.Release(*queued));
    EXPECT_FALSE(notifications_.QueueGrant(return_grants_, grant));
}

TEST_F(NotificationTxTest, GrantPublicationPrecedesPostAndLocalSuccessCannotReturnTarget) {
    Open();
    const auto grant = ReturnGrant();
    const auto queued = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(queued);
    ASSERT_EQ(notifications_.Progress(), 1u);
    EXPECT_TRUE(grant_published_at_post_);
    EXPECT_TRUE(notifications_.status(*queued)->grant_published);
    EXPECT_EQ(return_grants_.stats().published, 1u);
    CompleteNotification(0);
    ASSERT_TRUE(notifications_.Release(*queued));
    EXPECT_EQ(return_grants_.stats().published, 1u);
    EXPECT_EQ(source_.stats().remote_grants, 1u);
    EXPECT_FALSE(return_grants_.Borrow(grant.handle));
    ProveNoRemoteWrites(grant);
}

TEST_F(NotificationTxTest, KnownZeroGrantPostCanRetryOrCancelWithNoExposureProof) {
    Open();
    auto first = ReturnGrant();
    auto queued = notifications_.QueueGrant(return_grants_, first);
    ASSERT_TRUE(queued);
    mode_ = PostMode::kPartial;
    partial_ = 0;
    ExpectError(notifications_.Progress(), RndvNotificationErrorCode::kWouldBlock);
    EXPECT_TRUE(notifications_.status(*queued)->grant_published);
    EXPECT_EQ(return_grants_.stats().published, 1u);
    EXPECT_EQ(credits_.stats().promised_control, 1u);
    EXPECT_EQ(credits_.stats().spent[1], 0u);
    ASSERT_TRUE(notifications_.Cancel(*queued));
    ASSERT_TRUE(notifications_.Release(*queued));
    EXPECT_EQ(return_grants_.stats().active, 0u);
    EXPECT_EQ(source_.stats().remote_grants, 0u);
    auto second = ReturnGrant(1);
    queued = notifications_.QueueGrant(return_grants_, second);
    ASSERT_TRUE(queued);
    ExpectError(notifications_.Progress(), RndvNotificationErrorCode::kWouldBlock);
    mode_ = PostMode::kAll;
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    EXPECT_EQ(credits_.stats().spent[1], 1u);
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    ASSERT_TRUE(notifications_.Release(*queued));
    ProveNoRemoteWrites(second);
}

TEST_F(NotificationTxTest, EachProgressPostsOnlyOneControlAndEscrowAllowsPeerTraffic) {
    Open();
    // 三个控制额度中只能承诺两个，最后一个必须留给 CREDIT 控制消息。
    ASSERT_TRUE(credits_.Close());
    ASSERT_TRUE(notifications_.Close());
    ASSERT_TRUE(credits_.Open({22, 33, 2}));
    ASSERT_TRUE(credits_.Update({22, 33, 2}, 64, 3));
    RndvNotificationTx queue;
    auto config = Config();
    config.credit_identity.epoch = 2;
    ASSERT_TRUE(queue.Open(config, sender_, source_, endpoints_, endpoint_lease_, credits_));
    auto a = ReturnGrant(0), b = ReturnGrant(1), c = ReturnGrant(2);
    auto ah = queue.QueueGrant(return_grants_, a), bh = queue.QueueGrant(return_grants_, b);
    ASSERT_TRUE(ah);
    ASSERT_TRUE(bh);
    EXPECT_FALSE(queue.QueueGrant(return_grants_, c));
    EXPECT_EQ(credits_.stats().promised_control, 2u);
    auto peer = credits_.Reserve(CreditClass::kData, 1);
    ASSERT_TRUE(peer);
    ASSERT_TRUE(credits_.Resolve(*peer, 1));
    auto credit = credits_.Reserve(CreditClass::kControl, 1);
    ASSERT_TRUE(credit);
    ASSERT_TRUE(credits_.Resolve(*credit, 1));
    EXPECT_EQ(credits_.available(CreditClass::kControl), 0u);
    ASSERT_EQ(queue.Progress(), 1u);
    EXPECT_EQ(post_calls_, 1u);
    ASSERT_EQ(queue.Progress(), 1u);
    EXPECT_EQ(post_calls_, 2u);
    ASSERT_EQ(queue.Progress(), 0u);
    ASSERT_TRUE(queue.OnCompletion(Completion(0)));
    ASSERT_TRUE(queue.OnCompletion(Completion(1)));
    ASSERT_TRUE(queue.Release(*ah));
    ASSERT_TRUE(queue.Release(*bh));
    ASSERT_TRUE(queue.Close());
    ProveNoRemoteWrites(a);
    ProveNoRemoteWrites(b);
    ASSERT_TRUE(return_grants_.Cancel(c.handle));
}

TEST_F(NotificationTxTest, WritePoolCompletionBuildsExactDoneAndSurvivesPoolSlotRelease) {
    Open();
    OpenWrites(32);
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    ASSERT_TRUE(notifications_.Release(pair->rts));
    // RTS 槽已归还，未来 DONE 的完整绑定不能依赖可复用的 RTS 槽。
    const auto grant = Target(77);
    const auto source = Source(77);
    const auto handle = Begin(grant, source);
    EXPECT_FALSE(notifications_.QueueDone(pair->done, transactions_, handle));
    ASSERT_EQ(transactions_.Progress(handle), 3u);
    Complete(1);
    Complete(2);
    EXPECT_FALSE(notifications_.QueueDone(pair->done, transactions_, handle));
    Complete(3);
    const auto evidence = transactions_.DoneEvidence(handle);
    ASSERT_TRUE(evidence);
    EXPECT_FALSE(notifications_.QueueDone(pair->rts, transactions_, handle));
    notifications_.Stop();
    EXPECT_FALSE(notifications_.QueueRts(Transfer(1), sequence_));
    ASSERT_TRUE(notifications_.QueueDone(pair->done, transactions_, handle));
    EXPECT_FALSE(notifications_.QueueDone(pair->done, transactions_, handle));
    ASSERT_TRUE(transactions_.Release(handle));
    ReleaseCpu(source);
    // DONE 已保存成功事实，不依赖被复用的事务池槽；源内存与远端目标仍各自独立。
    ASSERT_EQ(notifications_.Progress(), 1u);
    ASSERT_EQ(packet_count_, 5u);
    const auto frame = DecodeWireFrame(packets_[4].imm, std::span(packets_[4].payload).first(packets_[4].bytes));
    ASSERT_TRUE(frame);
    EXPECT_EQ(frame->frame.control, WireControlType::kRndvDone);
    EXPECT_EQ(frame->frame.seq, grant.identity.transfer.seq);
    EXPECT_EQ(frame->frame.rndv.transfer_id, grant.identity.transfer.transfer_id);
    EXPECT_EQ(frame->frame.rndv.grant_generation, grant.identity.generation);
    EXPECT_EQ(frame->frame.rndv.target_address, grant.identity.target_address);
    EXPECT_EQ(frame->frame.rndv.bytes, 77u);
    CompleteNotification(4);
    ASSERT_TRUE(notifications_.Release(pair->done));
    EXPECT_EQ(grants_.stats().published, 1u);
    EXPECT_EQ(credits_.stats().spent[0], 1u);
    EXPECT_EQ(credits_.stats().spent[1], 1u);
    ASSERT_TRUE(grants_.CompleteDone(*evidence));
    const auto received = grants_.Borrow(grant.handle);
    ASSERT_TRUE(received);
    for (std::size_t i = 0; i < received->size(); ++i) {
        EXPECT_EQ((*received)[i], static_cast<std::byte>((i * 17 + 1) & 255));
    }
    ASSERT_TRUE(grants_.Return(grant.handle));
    MarkEnded(grant);
}

TEST_F(NotificationTxTest, DoneRejectsSuccessfulWriteForDifferentWholeTransfer) {
    Open();
    OpenWrites();
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    const auto grant = Target(76);
    const auto source = Source(76);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 3u);
    Complete(1);
    Complete(2);
    Complete(3);
    ASSERT_TRUE(transactions_.DoneEvidence(handle));
    EXPECT_FALSE(notifications_.QueueDone(pair->done, transactions_, handle));
    EXPECT_FALSE(notifications_.status(pair->done)->prepared);
    EXPECT_EQ(credits_.stats().promised_control, 1u);
    DeliverAndReturn(handle, grant);
}

TEST_F(NotificationTxTest, DoneRejectsPartialSuccessfulCoverageAndCancelledWrite) {
    Open();
    OpenWrites(32, 1);
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    const auto grant = Target(77);
    const auto source = Source(77);
    const auto handle = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(handle), 1u);
    Complete(1);
    ASSERT_TRUE(transactions_.Cancel(handle));
    EXPECT_FALSE(notifications_.QueueDone(pair->done, transactions_, handle));
    EXPECT_FALSE(notifications_.status(pair->done)->prepared);
    EndFailedGrant(handle, grant);
}

TEST_F(NotificationTxTest, WrongCompletionMetadataCannotRetireSourceAndOldHandlesCannotAliasReuse) {
    Open(1);
    auto grant = ReturnGrant();
    auto handle = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(handle);
    ASSERT_EQ(notifications_.Progress(), 1u);
    const auto event = Completion(0);
    ASSERT_TRUE(event.record);
    auto bad = event;
    ++bad.record->metadata.connection_id;
    ExpectError(notifications_.OnCompletion(bad), RndvNotificationErrorCode::kInvalidCompletion);
    bad = event;
    ++bad.record->metadata.operation_generation;
    EXPECT_FALSE(notifications_.OnCompletion(bad));
    bad = event;
    ++bad.record->metadata.buffer_lease;
    EXPECT_FALSE(notifications_.OnCompletion(bad));
    bad = event;
    bad.record->metadata.opcode = URMA_OPC_WRITE;
    EXPECT_FALSE(notifications_.OnCompletion(bad));
    bad = event;
    bad.record->metadata.target = nullptr;
    EXPECT_FALSE(notifications_.OnCompletion(bad));
    bad = event;
    bad.kind = raw::TxCompletionKind::kRejected;
    EXPECT_FALSE(notifications_.OnCompletion(bad));
    bad = event;
    ++bad.record->ticket.lane.generation;
    EXPECT_FALSE(notifications_.OnCompletion(bad));
    EXPECT_EQ(source_.stats().local_dma, 1u);
    ASSERT_TRUE(notifications_.OnCompletion(event));
    EXPECT_FALSE(notifications_.OnCompletion(event));
    const auto old = *handle;
    ASSERT_TRUE(notifications_.Release(*handle));
    ProveNoRemoteWrites(grant);
    grant = ReturnGrant(1);
    handle = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(handle);
    EXPECT_EQ(old.slot, handle->slot);
    EXPECT_NE(old.generation, handle->generation);
    EXPECT_FALSE(notifications_.status(old));
    EXPECT_FALSE(notifications_.Release(old));
    EXPECT_FALSE(notifications_.OnCompletion(event));
    EXPECT_EQ(notifications_.stats().active, 1u);
    ASSERT_TRUE(notifications_.Cancel(*handle));
}

TEST_F(NotificationTxTest, AcceptedCancellationAndCloseRetainDmaUntilTrueCompletion) {
    Open();
    auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    const auto buffers = source_.stats().available;
    EXPECT_FALSE(notifications_.Release(pair->rts));
    ASSERT_TRUE(notifications_.Cancel(pair->rts));
    EXPECT_TRUE(sequence_.stopped());
    EXPECT_FALSE(notifications_.Close());
    EXPECT_EQ(endpoints_.leases(), 2u);
    EXPECT_EQ(source_.stats().local_dma, 1u);
    EXPECT_EQ(source_.stats().available, buffers);
    EXPECT_EQ(post_calls_, 1u);
    CompleteNotification(0);
    ASSERT_TRUE(notifications_.Close());
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(endpoints_.leases(), 1u);
}

TEST_F(NotificationTxTest, AcceptedGrantCancellationQuarantinesEvenAfterLocalCompletion) {
    Open();
    const auto grant = ReturnGrant();
    auto handle = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(handle);
    ASSERT_EQ(notifications_.Progress(), 1u);
    ASSERT_TRUE(notifications_.Cancel(*handle));
    EXPECT_EQ(return_grants_.stats().quarantined, 1u);
    EXPECT_EQ(source_.stats().remote_grants, 1u);
    CompleteNotification(0);
    ASSERT_TRUE(notifications_.Release(*handle));
    EXPECT_EQ(return_grants_.stats().quarantined, 1u);
    ProveNoRemoteWrites(grant);
}

TEST_F(NotificationTxTest, RemoteAbortFailsOnlyNotificationQueueAndLeavesSharedSqReady) {
    Open();
    auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    EXPECT_TRUE(notifications_.stats().failed);
    EXPECT_EQ(pool_.lane_state(lane_), raw::JettyLaneState::kReady);
    EXPECT_EQ(modifies_, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    // 同一 SQ 的另一个 owner 仍可提交正常 WRITE，不能被远端进程错误连带排空。
    OpenWrites();
    auto grant = Target(32);
    auto source = Source(32);
    auto write = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(write), 1u);
    Complete(1);
    DeliverAndReturn(write, grant);
}

TEST_F(NotificationTxTest, AckTimeoutIsolatedByProcessorAndNotModifiedInsideNotificationFastpath) {
    Open();
    auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0, URMA_CR_ACK_TIMEOUT_ERR);
    EXPECT_EQ(pool_.lane_state(lane_), raw::JettyLaneState::kFaulted);
    EXPECT_EQ(modifies_, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_TRUE(notifications_.stats().failed);
    // 物理队列恢复交给独立控制流程，通知层不擅自 Modify/Flush。
    DrainFaultedLane();
}

TEST_F(NotificationTxTest, HotQueueProgressCompletionAndReleaseAllocateAndFreeNothing) {
    Open();
    allocations = deallocations = 0;
    count_allocations = true;
    auto pair = notifications_.QueueRts(Transfer(), sequence_);
    auto posted = notifications_.Progress();
    auto event = Completion(0);
    auto completed = notifications_.OnCompletion(event);
    bool released = pair && notifications_.Release(pair->rts).has_value() &&
                    notifications_.Cancel(pair->done).has_value() && notifications_.Release(pair->done).has_value();
    count_allocations = false;
    ASSERT_TRUE(pair);
    ASSERT_EQ(posted, 1u);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(released);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
}

TEST_F(NotificationTxTest, CancelUnacceptedDoneAlsoCancelsRtsAndAllAdmissionPromises) {
    Open();
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_TRUE(notifications_.Cancel(pair->done));
    EXPECT_TRUE(notifications_.status(pair->done)->cancelled);
    EXPECT_TRUE(notifications_.status(pair->rts)->cancelled);
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_FALSE(sequence_.stopped());
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    ASSERT_EQ(notifications_.Progress(), 0u);
    EXPECT_EQ(post_calls_, 0u);
    ASSERT_TRUE(notifications_.Release(pair->rts));
    ASSERT_TRUE(notifications_.Release(pair->done));
}

TEST_F(NotificationTxTest, CancelDoneAfterReleasedRtsFailsSharedSequenceAndCannotTouchReusedSlot) {
    Open(4);
    const auto first = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(first);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    ASSERT_TRUE(notifications_.Release(first->rts));
    const auto second = notifications_.QueueRts(Transfer(1), sequence_);
    ASSERT_TRUE(second);
    EXPECT_EQ(first->rts.slot, second->rts.slot);
    EXPECT_NE(first->rts.generation, second->rts.generation);
    // 旧 DONE 持有的是原事务责任；RTS 物理槽复用不能让取消错杀新的 handle。
    ASSERT_TRUE(notifications_.Cancel(first->done));
    EXPECT_TRUE(sequence_.stopped());
    EXPECT_TRUE(notifications_.stats().failed);
    EXPECT_FALSE(notifications_.Progress());
    EXPECT_EQ(post_calls_, 1u);
    ASSERT_TRUE(notifications_.status(second->rts));
    EXPECT_FALSE(notifications_.status(second->rts)->accepted);
    EXPECT_FALSE(notifications_.status(second->rts)->cancelled);
}

TEST_F(NotificationTxTest, FirstRtsBindsOneSharedSequenceObjectAcrossCancellation) {
    Open();
    const auto first = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(first);
    ASSERT_TRUE(notifications_.Cancel(first->rts));
    ASSERT_TRUE(notifications_.Release(first->rts));
    ASSERT_TRUE(notifications_.Release(first->done));
    TxSequenceWindow another;
    ASSERT_TRUE(another.Open({22, 33, 44}));
    ASSERT_TRUE(another.Update({22, 33, 44}, 32, 16384));
    // 相同 session 的另一序号账本也会重复使用 SN，必须绑定到共同的同一对象。
    EXPECT_FALSE(notifications_.QueueRts(Transfer(), another));
    EXPECT_TRUE(another.pending().empty());
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    ASSERT_TRUE(notifications_.QueueRts(Transfer(), sequence_));
}

TEST_F(NotificationTxTest, HotGrantPublicationPostCompletionAndReleaseAllocateAndFreeNothing) {
    Open();
    const auto grant = ReturnGrant();
    allocations = deallocations = 0;
    count_allocations = true;
    auto handle = notifications_.QueueGrant(return_grants_, grant);
    auto posted = notifications_.Progress();
    auto completed = notifications_.OnCompletion(Completion(0));
    bool released = handle && notifications_.Release(*handle).has_value();
    count_allocations = false;
    ASSERT_TRUE(handle);
    ASSERT_EQ(posted, 1u);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(released);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    EXPECT_TRUE(grant_published_at_post_);
    ProveNoRemoteWrites(grant);
}

TEST_F(NotificationTxTest, HotRealWriteToDoneSnapshotPostCompletionAndReleaseAllocateAndFreeNothing) {
    Open();
    OpenWrites();
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    // 本例特意暂不消费 RTS CQE，验证 accepted 足以绑定 DONE，二者退休相互独立。
    const auto grant = Target(77);
    const auto source = Source(77);
    const auto write = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(write), 3u);
    Complete(1);
    Complete(2);
    Complete(3);
    auto evidence = transactions_.DoneEvidence(write);
    ASSERT_TRUE(evidence);
    const std::array fin{TxFramePlan{0, true}};
    const auto fin_ticket = sequence_.Reserve(fin);
    ASSERT_TRUE(fin_ticket);
    ASSERT_TRUE(sequence_.Resolve(*fin_ticket, 1));
    ASSERT_TRUE(sequence_.fin_accepted());
    ASSERT_TRUE(sequence_.stopped());
    allocations = deallocations = 0;
    count_allocations = true;
    auto done = notifications_.QueueDone(pair->done, transactions_, write);
    auto write_released = transactions_.Release(write);
    auto posted = notifications_.Progress();
    auto completed = notifications_.OnCompletion(Completion(4));
    auto released = notifications_.Release(pair->done);
    count_allocations = false;
    ASSERT_TRUE(done);
    ASSERT_TRUE(write_released);
    ASSERT_EQ(posted, 1u);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(released);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
    CompleteNotification(0);
    ASSERT_TRUE(notifications_.Release(pair->rts));
    ASSERT_TRUE(grants_.CompleteDone(*evidence));
    ASSERT_TRUE(grants_.Borrow(grant.handle));
    ASSERT_TRUE(grants_.Return(grant.handle));
    MarkEnded(grant);
}

TEST_F(NotificationTxTest, ExternalSharedSequenceFailureCancelsQueuedRtsWithoutProviderExposure) {
    Open();
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    sequence_.Fail();
    EXPECT_FALSE(notifications_.Progress());
    EXPECT_EQ(post_calls_, 0u);
    EXPECT_EQ(notifications_.stats().accepted, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(credits_.stats().spent[0], 0u);
    EXPECT_EQ(credits_.stats().promised_control, 0u);
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_TRUE(notifications_.status(pair->rts)->cancelled);
    EXPECT_TRUE(notifications_.status(pair->done)->cancelled);
}

TEST_F(NotificationTxTest, RemoteGrantCompletionBeforeNotificationCloseDoesNotCancelReturnedTarget) {
    Open();
    const auto grant = ReturnGrant();
    const auto handle = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(handle);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    // 独立模拟远端唯一 WRITE 的完整 DMA 和已终结证明；不是从 GRANT 的 SEND CQE 推导成功。
    std::array<std::byte, 77> remote_source;
    remote_source.fill(std::byte{0x6a});
    std::memcpy(reinterpret_cast<void*>(grant.target.addr), remote_source.data(), remote_source.size());
    ASSERT_TRUE(return_grants_.CompleteDone({.identity = grant.identity,
                                             .no_future_writes = true,
                                             .acceptance_known = true,
                                             .accepted_writes = 1,
                                             .terminal_writes = 1,
                                             .successful_writes = 1,
                                             .completed_bytes = remote_source.size()}));
    const auto view = return_grants_.Borrow(grant.handle);
    ASSERT_TRUE(view);
    EXPECT_TRUE(std::equal(view->begin(), view->end(), remote_source.begin()));
    ASSERT_TRUE(return_grants_.Return(grant.handle));
    EXPECT_EQ(return_grants_.stats().active, 0u);
    // 本地通知仍保留 terminal 记录；Close 不得再 Cancel 已归还并可能复用的远端 Grant。
    ASSERT_TRUE(notifications_.Close());
    EXPECT_EQ(return_grants_.stats().active, 0u);
    EXPECT_EQ(source_.stats().remote_grants, 0u);
}

TEST_F(NotificationTxTest, SuccessfulRtsAndDoneClosePreservesSharedSequence) {
    Open();
    OpenWrites();
    const auto pair = notifications_.QueueRts(Transfer(), sequence_);
    ASSERT_TRUE(pair);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0);
    const auto grant = Target(77);
    const auto source = Source(77);
    const auto write = Begin(grant, source);
    ASSERT_EQ(transactions_.Progress(write), 3u);
    Complete(1);
    Complete(2);
    Complete(3);
    ASSERT_TRUE(notifications_.QueueDone(pair->done, transactions_, write));
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(4);
    DeliverAndReturn(write, grant);
    // 两个已成功退休的记录没有显式 Release；Close 应视为完成而非取消整个逻辑序列。
    ASSERT_TRUE(notifications_.Close());
    EXPECT_FALSE(sequence_.stopped());
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_EQ(sequence_.next_sequence(), 1u);
}

TEST_F(NotificationTxTest, FailedGrantCompletionQuarantinesTargetWithoutDrainingSharedSq) {
    Open();
    const auto grant = ReturnGrant();
    const auto handle = notifications_.QueueGrant(return_grants_, grant);
    ASSERT_TRUE(handle);
    ASSERT_EQ(notifications_.Progress(), 1u);
    CompleteNotification(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    EXPECT_EQ(return_grants_.stats().quarantined, 1u);
    EXPECT_EQ(source_.stats().remote_grants, 1u);
    EXPECT_EQ(pool_.lane_state(lane_), raw::JettyLaneState::kReady);
    ASSERT_TRUE(notifications_.Release(*handle));
    ProveNoRemoteWrites(grant);
}

class NotificationTxDeathTest : public NotificationTxTest {};
TEST_F(NotificationTxDeathTest, UnknownGrantAcceptancePermanentlyRetainsPublicationAndImport) {
    // 未知接受不能借助单条本地 CQE 升格成完整接受集合，整个依赖图保留至子进程退出。
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            Open();
            auto grant = ReturnGrant();
            auto handle = notifications_.QueueGrant(return_grants_, grant);
            ASSERT_TRUE(handle);
            mode_ = PostMode::kUnknown;
            ExpectError(notifications_.Progress(), RndvNotificationErrorCode::kAcceptanceUnknown);
            EXPECT_EQ(packet_count_, 1u);
            EXPECT_FALSE(notifications_.status(*handle)->acceptance_known);
            EXPECT_EQ(return_grants_.stats().quarantined, 1u);
            EXPECT_FALSE(notifications_.Progress());
            EXPECT_FALSE(notifications_.Release(*handle));
            EXPECT_FALSE(notifications_.Close());
            EXPECT_EQ(post_calls_, 1u);
            EXPECT_TRUE(ledger_.Commit(packets_[0].id, 1));
            // Mock 明确给出唯一捕获 WR 的本地完成；Grant 仍无远端静默证明。
            (void)notifications_.OnCompletion(Completion(0));
            EXPECT_EQ(source_.stats().local_dma, 0u);
            EXPECT_FALSE(notifications_.Release(*handle));
            EXPECT_FALSE(notifications_.Close());
            EXPECT_TRUE(endpoints_.Release(endpoint_lease_));
            EXPECT_EQ(endpoints_.leases(), 1u);
            EXPECT_EQ(endpoints_.EvictUnused(), 0u);
            EXPECT_EQ(jetty_unimports_.load(), 0u);
            EXPECT_EQ(source_.stats().remote_grants, 1u);
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace connection
} // namespace kbsocket
