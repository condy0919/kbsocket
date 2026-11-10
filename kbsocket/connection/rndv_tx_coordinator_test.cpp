// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_tx_coordinator.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <new>
#include <span>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/rndv_protocol.hpp"
#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/runtime/control_executor.hpp"
#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

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
void ExpectError(const std::expected<T, RndvTxError>& result, RndvTxErrorCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}

class RndvTxCoordinatorTest : public ::testing::Test {
protected:
    struct Packet {
        raw::AttemptId id = 0;
        urma_opcode_t opcode = URMA_OPC_WRITE;
        std::uint64_t grant_generation = 0;
        WireControlType control = WireControlType::kRndvRts;
        WireFrameView decoded;
    };
    enum class PostMode { kAll, kPartial, kUnknown };
    static inline RndvTxCoordinatorTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto*, auto*) { return active_->jfc_creates_++ ? &active_->rx_ : &active_->tx_; };
        f.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        f.create_jetty = [](auto*, auto*) { return active_->jetty_creates_++ ? &active_->jetty_ : &active_->passive_; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.register_seg = [](auto* context, auto* config) {
            auto& segment = active_->registered_[active_->registrations_++];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            segment.seg.attr.bs.access = config->flag.bs.access;
            segment.seg.ubva.eid = context->eid;
            segment.seg.token_id = static_cast<std::uint32_t>(active_->registrations_);
            return &segment;
        };
        f.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        f.modify_jetty = [](auto*, auto*) {
            ++active_->modifies_;
            return URMA_SUCCESS;
        };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.post_jetty_send_wr = [](auto*, auto* first, auto** bad) { return active_->Post(first, bad); };
        f.poll_jfc = [](auto*, int capacity, auto* output) {
            if (!capacity || !active_->cqe_ready_) {
                return 0;
            }
            output[0] = active_->cqe_;
            active_->cqe_ready_ = false;
            return 1;
        };
        f.import_jetty = [](auto* context, auto* descriptor, auto*) {
            active_->endpoint_.urma_ctx = context;
            active_->endpoint_.id = descriptor->jetty_id;
            active_->endpoint_.trans_mode = descriptor->trans_mode;
            active_->endpoint_.tp_type = descriptor->tp_type;
            active_->endpoint_.type = descriptor->type;
            return &active_->endpoint_;
        };
        f.unimport_jetty = [](auto*) { return URMA_SUCCESS; };
        f.import_seg = [](auto* context, auto* descriptor, auto*, std::uint64_t, urma_import_seg_flag_t) {
            active_->remote_.urma_ctx = context;
            active_->remote_.seg = *descriptor;
            ++active_->segment_imports_;
            return &active_->remote_;
        };
        f.unimport_seg = [](auto*) { return URMA_SUCCESS; };
        return f;
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
        cap.max_write_size = 200u * 1024u * 1024u;
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
        ASSERT_TRUE(source_.Open(sources, {.buffer_count = 32, .buffer_size = 4096, .max_references = 128}, budget_));
        ASSERT_TRUE(target_.Open(targets, {.buffer_count = 4, .buffer_size = 4096, .max_references = 8}, budget_));
        ASSERT_TRUE(grants_.Open(Binding(), {77, 88}, target_, &receiver_context_,
                                 {.window = 32, .max_active = 4, .max_bytes = 16384}));
        ASSERT_TRUE(credits_.Open({22, 33, 1}));
        ASSERT_TRUE(credits_.Update({22, 33, 1}, 64, 64));
        ASSERT_TRUE(sequence_.Open({22, 33, 44}));
        ASSERT_TRUE(sequence_.Update({22, 33, 44}, 32, 16384));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        ASSERT_TRUE(endpoints_.Open({.entries = 8, .leases = 32, .in_flight = 2}));
        ASSERT_TRUE(segments_.Open({.entries = 8, .leases = 32, .in_flight = 2}));
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
        const auto endpoint = endpoints_.Acquire({endpoint_key_, std::as_bytes(std::span(&descriptor, 1)), {}});
        ASSERT_TRUE(endpoint);
        endpoint_lease_ = *endpoint;
        ASSERT_TRUE(DriveCaches());
        ASSERT_TRUE(endpoints_.Get(endpoint_lease_));
    }
    void TearDown() override {
        count_allocations = false;
        // 捕获列表只包含 provider 确定接受的 WR；逐个真实完成，不能凭 Cancel 释放 DMA。
        for (std::size_t i = 0; i < packet_count_; ++i) {
            if (ledger_.LookupPosted(packets_[i].id)) {
                (void)coordinator_.OnCompletion(Completion(i, URMA_CR_WR_FLUSH_ERR));
            }
        }
        EXPECT_TRUE(coordinator_.Close());
        EXPECT_TRUE(notifications_.Close());
        EXPECT_TRUE(writes_.Close());
        for (std::size_t i = 0; i < lease_count_; ++i) {
            if (cpu_held_[i]) {
                EXPECT_TRUE(source_.ReleaseCpu(leases_[i]));
            }
        }
        // 独立接收端只接受本测试已经逐 WR 终结的证据；不从本地超时推断远端静默。
        for (std::size_t i = 0; i < grant_count_; ++i) {
            if (grant_ended_[i]) {
                continue;
            }
            std::uint64_t count = 0;
            for (std::size_t p = 0; p < packet_count_; ++p) {
                count += packets_[p].opcode == URMA_OPC_WRITE &&
                         packets_[p].grant_generation == reserved_grants_[i].identity.generation;
            }
            EXPECT_TRUE(grants_.ConfirmTerminal({.identity = reserved_grants_[i].identity,
                                                 .no_future_writes = true,
                                                 .acceptance_known = true,
                                                 .accepted_writes = count,
                                                 .terminal_writes = count}));
        }
        EXPECT_TRUE(grants_.Close());
        EXPECT_TRUE(credits_.Close());
        EXPECT_TRUE(endpoints_.Release(endpoint_lease_));
        endpoints_.StopAccepting();
        segments_.StopAccepting();
        EXPECT_TRUE(DriveCaches());
        EXPECT_TRUE(endpoints_.Close());
        EXPECT_TRUE(segments_.Close());
        EXPECT_TRUE(executor_.Close());
        EXPECT_EQ(source_.stats().local_dma, 0u);
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
                } else if (segments_.OwnsCompletion(completions[i].id)) {
                    if (!segments_.HandleCompletion(completions[i])) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            if (endpoints_.in_flight() || segments_.in_flight()) {
                std::this_thread::yield();
                continue;
            }
            const auto endpoint = endpoints_.NextOperation();
            const auto segment = segments_.NextOperation();
            if (!endpoint || !segment) {
                return false;
            }
            // 同 context 慢操作串行推进，屏障始终保持到完成收割。
            if (*endpoint) {
                const auto& op = **endpoint;
                if (!endpoints_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true})) {
                    return false;
                }
            } else if (*segment) {
                const auto& op = **segment;
                if (!segments_.StartOperation(op, executor_, {op.context, op.context_incarnation, true, true})) {
                    return false;
                }
            } else {
                return true;
            }
        }
        return false;
    }
    static GrantBinding Binding() {
        return {11, 22, 33, 44, {55, 66}};
    }
    static WireSessionBinding Route() {
        return {
            .context = {.peer_runtime = 22, .receive_domain = 34, .session = 45}, .token = {56, 67}, .credit_epoch = 1};
    }
    static std::expected<control::TsegImportRequest, RndvTxError> Resolve(void* owner, const GrantIdentity&) noexcept {
        auto& self = *static_cast<RndvTxCoordinatorTest*>(owner);
        ++self.resolver_calls_;
        if (self.resolver_blocked_) {
            return std::unexpected(RndvTxError{RndvTxErrorCode::kWouldBlock});
        }
        auto key = self.segment_key_;
        if (self.resolver_wrong_key_) {
            ++key.authorization_identity;
        }
        return control::TsegImportRequest{key, self.registered_[1].seg, {}};
    }
    RndvTxConfig Config() {
        return {.grant_route = Route(),
                .limits = {.version = control::kRndvVersion,
                           .flags = control::kRndvSuccessfulWriteDone,
                           .max_write_bytes = 32,
                           .max_transfer_bytes = 256,
                           .max_grants = 2,
                           .window = 32,
                           .max_bytes = 512},
                .capacity = 2,
                .timeout_ns = 1000,
                .resolve_import = Resolve,
                .import_owner = this};
    }
    void OpenDependencies(std::uint32_t max_chunks = 8, std::uint32_t post_budget = 8) {
        WriteRndvPoolConfig config;
        config.connection_id = 101;
        config.binding = Binding();
        config.arena = {77, 88};
        config.endpoint = endpoint_key_;
        config.segment = segment_key_;
        config.lane = lane_;
        config.slots = 2;
        config.window = 32;
        config.chunk_bytes = 32;
        config.max_chunks = max_chunks;
        config.post_budget = post_budget;
        ASSERT_TRUE(writes_.Open(config, sender_, source_, endpoints_, segments_));
        ASSERT_TRUE(notifications_.Open({.connection_id = 202,
                                         .destination = {22, 33, 44, {55, 66}},
                                         .credit_identity = {22, 33, 1},
                                         .endpoint = endpoint_key_,
                                         .lane = lane_,
                                         .capacity = 8},
                                        sender_, source_, endpoints_, endpoint_lease_, credits_));
    }
    void Open() {
        OpenDependencies();
        ASSERT_TRUE(coordinator_.Open(Config(), writes_, notifications_, sequence_, source_, endpoints_,
                                      endpoint_lease_, segments_));
    }
    memory::BufferLease Source(std::size_t bytes, unsigned seed = 1) {
        const auto lease = source_.Acquire(bytes);
        EXPECT_TRUE(lease);
        if (!lease) {
            return {};
        }
        const auto view = source_.Writable(*lease);
        EXPECT_TRUE(view);
        if (!view) {
            return {};
        }
        for (std::size_t i = 0; i < bytes; ++i) {
            (*view)[i] = static_cast<std::byte>((i * 17 + seed) & 255);
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
    RndvTxHandle Submit(memory::BufferLease lease, std::size_t bytes = 77) {
        const std::array sources{WriteRndvSource{lease, 0, bytes}};
        const auto result = coordinator_.Submit(sources, now_++);
        EXPECT_TRUE(result);
        return result ? *result : RndvTxHandle{};
    }
    RndvTxStatus Status(RndvTxHandle handle) {
        const auto status = coordinator_.status(handle);
        EXPECT_TRUE(status);
        return status ? *status : RndvTxStatus{};
    }
    GrantReservation Target(RndvTxHandle handle) {
        const auto grant = grants_.Reserve(Status(handle).transfer);
        EXPECT_TRUE(grant);
        if (!grant) {
            return {};
        }
        EXPECT_TRUE(grants_.CommitPublication(grant->handle));
        reserved_grants_[grant_count_++] = *grant;
        return *grant;
    }
    WireFrameView Frame(const GrantIdentity& identity) {
        const auto frame = BuildRndvGrant(identity, {11, 34, 45, Route().token});
        EXPECT_TRUE(frame);
        if (!frame) {
            return {};
        }
        std::array<std::byte, 112> bytes{};
        const auto encoded = EncodeWireFrame(*frame, bytes);
        EXPECT_TRUE(encoded);
        if (!encoded) {
            return {};
        }
        const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(bytes).first(encoded->payload_bytes));
        EXPECT_TRUE(decoded);
        return decoded ? *decoded : WireFrameView{};
    }
    void Grant(const GrantReservation& grant) {
        ASSERT_TRUE(coordinator_.OnGrant(Frame(grant.identity), Route().context, now_++));
    }
    void Progress(std::size_t budget = 8) {
        ASSERT_TRUE(coordinator_.Progress(now_++, budget));
    }
    void StartWrites(const GrantReservation& grant) {
        Grant(grant);
        Progress();
        ASSERT_TRUE(DriveCaches());
        // 每次有界扫描可能分别完成 import/Reserve/Start，宿主可以重复推进而不代填 CQE。
        for (unsigned i = 0; i < 4; ++i) {
            Progress();
        }
    }
    urma_status_t Post(urma_jfs_wr_t* first, urma_jfs_wr_t** bad) {
        ++post_calls_;
        std::size_t accepted = 0;
        for (auto* wr = first; wr; wr = wr->next, ++accepted) {
            if (mode_ == PostMode::kPartial && accepted == partial_) {
                *bad = wr;
                return URMA_ENOMEM;
            }
            auto& packet = packets_[packet_count_++];
            packet.id = wr->user_ctx;
            packet.opcode = wr->opcode;
            const auto record = ledger_.Lookup(packet.id);
            if (record) {
                packet.grant_generation = record->metadata.grant_lease;
            }
            if (wr->opcode == URMA_OPC_SEND_IMM) {
                EXPECT_EQ(wr->send.src.num_sge, 1u);
                const auto& source = wr->send.src.sge[0];
                const auto decoded = DecodeWireFrame(
                    wr->send.imm_data, std::span(reinterpret_cast<const std::byte*>(source.addr), source.len));
                EXPECT_TRUE(decoded);
                if (decoded) {
                    packet.decoded = *decoded;
                    packet.control = decoded->frame.control;
                }
            } else {
                EXPECT_EQ(wr->opcode, URMA_OPC_WRITE);
                EXPECT_EQ(wr->rw.dst.num_sge, 1u);
                std::uint64_t offset = 0;
                for (std::size_t s = 0; s < wr->rw.src.num_sge; ++s) {
                    const auto& source = wr->rw.src.sge[s];
                    std::memcpy(reinterpret_cast<void*>(wr->rw.dst.sge[0].addr + offset),
                                reinterpret_cast<const void*>(source.addr), source.len);
                    offset += source.len;
                }
                EXPECT_EQ(offset, wr->rw.dst.sge[0].len);
            }
        }
        *bad = nullptr;
        return mode_ == PostMode::kUnknown ? URMA_ENOMEM : URMA_SUCCESS;
    }
    raw::TxCompletionEvent Completion(std::size_t index, urma_cr_status_t status = URMA_CR_SUCCESS) {
        cqe_ = {};
        cqe_.status = status;
        cqe_.flag.bs.jetty = 1;
        cqe_.local_id = jetty_.jetty_id.id;
        cqe_.user_ctx = packets_[index].id;
        cqe_ready_ = true;
        std::array<raw::TxCompletionEvent, 1> events;
        const auto result = processor_.Poll(events);
        EXPECT_TRUE(result);
        if (!result || result->count != 1) {
            ADD_FAILURE() << "missing real completion";
            return {};
        }
        return events[0];
    }
    void Complete(std::size_t index) {
        ASSERT_TRUE(coordinator_.OnCompletion(Completion(index)));
    }
    void CompleteWrites() {
        for (std::size_t i = 0; i < packet_count_; ++i) {
            if (packets_[i].opcode == URMA_OPC_WRITE && ledger_.LookupPosted(packets_[i].id)) {
                Complete(i);
            }
        }
    }
    void Finish(RndvTxHandle handle) {
        CompleteWrites();
        for (unsigned i = 0; i < 4; ++i) {
            Progress();
        }
        ASSERT_TRUE(Status(handle).done_queued);
        const auto before = packet_count_;
        ASSERT_TRUE(notifications_.Progress());
        ASSERT_EQ(packet_count_, before + 1);
        EXPECT_EQ(packets_[before].control, WireControlType::kRndvDone);
        Complete(before);
        Progress();
        EXPECT_TRUE(Status(handle).complete);
        EXPECT_TRUE(Status(handle).success);
    }

    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{}, receiver_context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{}, passive_{};
    urma_target_jetty_t endpoint_{};
    urma_target_seg_t remote_{};
    std::array<urma_target_seg_t, 4> registered_{};
    raw::JettyPool pool_;
    raw::JettyLane lane_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_, true};
    memory::MemoryBudget budget_{1024 * 1024};
    memory::SlabAllocator source_, target_;
    GrantManager grants_;
    SenderCreditAccount credits_;
    TxSequenceWindow sequence_;
    RndvNotificationTx notifications_;
    WriteRndvPool writes_;
    RndvTxCoordinator coordinator_;
    runtime::ControlExecutor executor_;
    control::EndpointImportCache endpoints_;
    control::TsegImportCache segments_;
    control::EndpointImportKey endpoint_key_;
    control::TsegImportKey segment_key_;
    control::ImportLease endpoint_lease_;
    std::atomic<unsigned> segment_imports_{0};
    unsigned resolver_calls_ = 0;
    bool resolver_blocked_ = false, resolver_wrong_key_ = false;
    std::array<GrantReservation, 8> reserved_grants_{};
    std::array<bool, 8> grant_ended_{};
    std::size_t grant_count_ = 0;
    std::array<memory::BufferLease, 32> leases_{};
    std::array<bool, 32> cpu_held_{};
    std::size_t lease_count_ = 0;
    std::array<Packet, 256> packets_{};
    std::size_t packet_count_ = 0;
    urma_cr_t cqe_{};
    bool cqe_ready_ = false;
    std::size_t post_calls_ = 0, jfc_creates_ = 0, jetty_creates_ = 0, registrations_ = 0, modifies_ = 0;
    std::uint64_t now_ = 1;
    PostMode mode_ = PostMode::kAll;
    std::size_t partial_ = 0;
};

TEST_F(RndvTxCoordinatorTest, PinsSourceAcrossDelayedGrantAndImportThenWaitsForDoneCompletion) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ReleaseCpu(source);
    EXPECT_FALSE(source_.Readable(source));
    EXPECT_EQ(source_.stats().local_dma, 1u);
    ASSERT_TRUE(notifications_.Progress());
    ASSERT_EQ(packet_count_, 1u);
    Complete(0);
    Progress();
    EXPECT_EQ(segment_imports_.load(), 0u);
    EXPECT_FALSE(Status(handle).complete);

    // GRANT 只复制身份；控制导入尚未推进时，源引用持续存在且没有 WRITE 暴露。
    const auto grant = Target(handle);
    Grant(grant);
    Progress();
    EXPECT_EQ(packet_count_, 1u);
    EXPECT_EQ(source_.stats().local_dma, 1u);
    ASSERT_TRUE(DriveCaches());
    for (unsigned i = 0; i < 4; ++i) {
        Progress();
    }
    ASSERT_EQ(packet_count_, 4u);
    EXPECT_EQ(segment_imports_.load(), 1u);
    EXPECT_FALSE(Status(handle).done_queued);
    CompleteWrites();
    for (unsigned i = 0; i < 4; ++i) {
        Progress();
    }
    EXPECT_TRUE(Status(handle).done_queued);
    EXPECT_TRUE(Status(handle).sources_released);
    EXPECT_FALSE(Status(handle).complete);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    ASSERT_TRUE(notifications_.Progress());
    ASSERT_EQ(packet_count_, 5u);
    EXPECT_EQ(packets_[4].control, WireControlType::kRndvDone);
    EXPECT_FALSE(Status(handle).complete);
    Complete(4);
    Progress();
    ASSERT_TRUE(Status(handle).success);
    const auto evidence = coordinator_.TerminalEvidence(handle);
    ASSERT_TRUE(evidence);
    EXPECT_EQ(evidence->accepted_writes, 3u);
    EXPECT_EQ(evidence->terminal_writes, 3u);
    EXPECT_EQ(evidence->completed_bytes, 77u);
    ASSERT_TRUE(grants_.CompleteDone(*evidence));
    const auto payload = grants_.Borrow(grant.handle);
    ASSERT_TRUE(payload);
    ASSERT_EQ(payload->size(), 77u);
    for (std::size_t i = 0; i < payload->size(); ++i) {
        EXPECT_EQ((*payload)[i], static_cast<std::byte>((i * 17 + 1) & 255));
    }
    ASSERT_TRUE(grants_.Return(grant.handle));
    grant_ended_[0] = true;
    ASSERT_TRUE(coordinator_.Release(handle));
    ExpectError(coordinator_.status(handle), RndvTxErrorCode::kInvalidHandle);
    EXPECT_EQ(coordinator_.stats().active_bytes, 0u);
}

TEST_F(RndvTxCoordinatorTest, AcceptsEarlyGrantButLateRtsFailureSuppressesDone) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ASSERT_TRUE(notifications_.Progress());
    // 对端看见 RTS 可早于本地 RTS CQE；WRITE 可以开始，最后通知必须尊重晚到的失败。
    StartWrites(Target(handle));
    ASSERT_GT(packet_count_, 1u);
    EXPECT_TRUE(Status(handle).grant_received);
    EXPECT_FALSE(coordinator_.OnCompletion(Completion(0, URMA_CR_REM_ACCESS_ABORT_ERR)));
    CompleteWrites();
    (void)coordinator_.Progress(now_++);
    EXPECT_FALSE(Status(handle).done_queued);
    EXPECT_FALSE(Status(handle).success);
    EXPECT_TRUE(coordinator_.stats().stopped);
    EXPECT_EQ(modifies_, 0u);
}

TEST_F(RndvTxCoordinatorTest, EarlyGrantCanCompleteSuccessfullyAfterRtsCqe) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    StartWrites(Target(handle));
    Complete(0);
    Finish(handle);
}

TEST_F(RndvTxCoordinatorTest, CancelBeforeRtsPostReleasesPinsWithoutReservingSequence) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ReleaseCpu(source);
    ASSERT_TRUE(coordinator_.Cancel());
    (void)coordinator_.Progress(now_++);
    EXPECT_EQ(packet_count_, 0u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
    EXPECT_EQ(sequence_.next_sequence(), 0u);
    EXPECT_TRUE(Status(handle).cancelled);
    EXPECT_TRUE(Status(handle).complete);
    EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
    ASSERT_TRUE(coordinator_.Release(handle));
}

TEST_F(RndvTxCoordinatorTest, CancelWithAcceptedWritesRetainsDmaUntilEveryCompletion) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ReleaseCpu(source);
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    StartWrites(Target(handle));
    ASSERT_EQ(packet_count_, 4u);
    (void)coordinator_.Cancel();
    EXPECT_FALSE(coordinator_.Close());
    EXPECT_GT(source_.stats().local_dma, 0u);
    EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
    CompleteWrites();
    (void)coordinator_.Progress(now_++);
    const auto evidence = coordinator_.TerminalEvidence(handle);
    ASSERT_TRUE(evidence);
    EXPECT_TRUE(evidence->no_future_writes);
    EXPECT_EQ(evidence->accepted_writes, 3u);
    EXPECT_EQ(evidence->terminal_writes, 3u);
    EXPECT_TRUE(Status(handle).complete);
    EXPECT_FALSE(Status(handle).done_queued);
    EXPECT_FALSE(Status(handle).success);
    EXPECT_EQ(modifies_, 0u);
}

TEST_F(RndvTxCoordinatorTest, WrongObservedRouteAndGrantIdentityNeverReachResolverOrWrite) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    const auto grant = Target(handle);
    const auto frame = Frame(grant.identity);
    auto observed = Route().context;
    ++observed.session;
    EXPECT_FALSE(coordinator_.OnGrant(frame, observed, now_++));
    EXPECT_FALSE(Status(handle).grant_received);
    auto wrong = grant.identity;
    ++wrong.transfer.transfer_id;
    EXPECT_FALSE(coordinator_.OnGrant(Frame(wrong), Route().context, now_++));
    EXPECT_EQ(resolver_calls_, 0u);
    EXPECT_EQ(packet_count_, 1u);
    EXPECT_FALSE(Status(handle).grant_received);
}

TEST_F(RndvTxCoordinatorTest, ResolverMustReturnExactAuthenticatedSegmentKey) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    Grant(Target(handle));
    resolver_wrong_key_ = true;
    EXPECT_FALSE(coordinator_.Progress(now_++));
    EXPECT_EQ(segment_imports_.load(), 0u);
    EXPECT_EQ(packet_count_, 1u);
    EXPECT_FALSE(Status(handle).done_queued);
}

TEST_F(RndvTxCoordinatorTest, ResolverBackpressureRetainsSourceAndRetriesWithoutDuplicateRts) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ReleaseCpu(source);
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    Grant(Target(handle));
    resolver_blocked_ = true;
    for (unsigned i = 0; i < 3; ++i) {
        Progress();
    }
    EXPECT_EQ(source_.stats().local_dma, 1u);
    EXPECT_EQ(packet_count_, 1u);
    EXPECT_EQ(segment_imports_.load(), 0u);
    resolver_blocked_ = false;
    Progress();
    ASSERT_TRUE(DriveCaches());
    for (unsigned i = 0; i < 4; ++i) {
        Progress();
    }
    Finish(handle);
}

TEST_F(RndvTxCoordinatorTest, RejectsInvalidSourcesAndImpossibleChunkPlanBeforeRtsReservation) {
    OpenDependencies(2);
    ASSERT_TRUE(coordinator_.Open(Config(), writes_, notifications_, sequence_, source_, endpoints_, endpoint_lease_,
                                  segments_));
    const auto source = Source(96);
    const auto references = source_.stats().available_references;
    const std::array too_many_chunks{WriteRndvSource{source, 0, 96}};
    EXPECT_FALSE(coordinator_.Submit(too_many_chunks, now_++));
    const std::array bad_range{WriteRndvSource{source, 95, 2}};
    EXPECT_FALSE(coordinator_.Submit(bad_range, now_++));
    EXPECT_FALSE(coordinator_.Submit({}, now_++));
    EXPECT_EQ(source_.stats().available_references, references);
    EXPECT_EQ(sequence_.next_sequence(), 0u);
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_EQ(notifications_.stats().active, 0u);
    EXPECT_EQ(coordinator_.stats().active, 0u);
}

TEST_F(RndvTxCoordinatorTest, CapacityAndByteLimitsApplyBeforeAnyNewSourceOwnership) {
    OpenDependencies();
    auto config = Config();
    config.limits.max_bytes = config.limits.max_transfer_bytes = 128;
    ASSERT_TRUE(
        coordinator_.Open(config, writes_, notifications_, sequence_, source_, endpoints_, endpoint_lease_, segments_));
    const auto first = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    const auto second_source = Source(77);
    const auto references = source_.stats().available_references;
    const std::array second{WriteRndvSource{second_source, 0, 77}};
    ExpectError(coordinator_.Submit(second, now_++), RndvTxErrorCode::kWouldBlock);
    EXPECT_TRUE(source_.Readable(second_source));
    EXPECT_EQ(source_.stats().available_references, references);
    EXPECT_EQ(coordinator_.stats().active, 1u);
    EXPECT_EQ(coordinator_.stats().active_bytes, 77u);
    EXPECT_FALSE(Status(first).complete);
}

TEST_F(RndvTxCoordinatorTest, PinsBorrowedDependenciesAndRejectsSecondCoordinator) {
    Open();
    const auto endpoint_leases = endpoints_.leases();
    EXPECT_EQ(endpoint_leases, 3u);
    EXPECT_FALSE(notifications_.Close());
    EXPECT_FALSE(writes_.Close());
    EXPECT_FALSE(notifications_.stats().stopped);
    RndvTxCoordinator second;
    EXPECT_FALSE(
        second.Open(Config(), writes_, notifications_, sequence_, source_, endpoints_, endpoint_lease_, segments_));
    EXPECT_EQ(endpoints_.leases(), endpoint_leases);
    ASSERT_TRUE(coordinator_.Close());
    EXPECT_EQ(endpoints_.leases(), 2u);
    ASSERT_TRUE(notifications_.Close());
    ASSERT_TRUE(writes_.Close());
}

TEST_F(RndvTxCoordinatorTest, TimeoutCancelsPendingGrantWithoutFabricatingTerminalProof) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ReleaseCpu(source);
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    ExpectError(coordinator_.Progress(1002), RndvTxErrorCode::kTimeout);
    EXPECT_TRUE(coordinator_.stats().stopped);
    EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
    EXPECT_EQ(packet_count_, 1u);
    EXPECT_EQ(source_.stats().local_dma, 0u);
}

TEST_F(RndvTxCoordinatorTest, StopAllowsExistingTransferToFinishAndRejectsNewSubmit) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    coordinator_.Stop();
    const auto source = Source(77);
    const std::array input{WriteRndvSource{source, 0, 77}};
    ExpectError(coordinator_.Submit(input, now_++), RndvTxErrorCode::kStopped);
    EXPECT_TRUE(source_.Readable(source));
    StartWrites(Target(handle));
    Finish(handle);
}

TEST_F(RndvTxCoordinatorTest, CommonSequenceIncludesEagerAndWindowRequiresExplicitConsumedPrefix) {
    Open();
    // Eager 先占共同 SN；RNDV 的 RTS 必须从 1 开始，不能维护第二套私有序号。
    const std::array eager{TxFramePlan{5, false}};
    const auto ticket = sequence_.Reserve(eager);
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(sequence_.Resolve(*ticket, 1));
    const auto handle = Submit(Source(77));
    EXPECT_EQ(Status(handle).transfer.seq, 1u);
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    EXPECT_FALSE(coordinator_.AdvanceWindow(2));
    StartWrites(Target(handle));
    Finish(handle);
    EXPECT_FALSE(coordinator_.AdvanceWindow(3));
    ASSERT_TRUE(coordinator_.Release(handle));
    ASSERT_TRUE(coordinator_.AdvanceWindow(2));
}

TEST_F(RndvTxCoordinatorTest, RemoteAccessAbortCancelsLogicalTransferWithoutDrainingHealthySq) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    StartWrites(Target(handle));
    ASSERT_EQ(packet_count_, 4u);
    EXPECT_FALSE(coordinator_.OnCompletion(Completion(1, URMA_CR_REM_ACCESS_ABORT_ERR)));
    CompleteWrites();
    (void)coordinator_.Progress(now_++);
    EXPECT_FALSE(Status(handle).success);
    EXPECT_FALSE(Status(handle).done_queued);
    EXPECT_EQ(modifies_, 0u);
    const auto lane = pool_.lane_state(lane_);
    ASSERT_TRUE(lane);
    EXPECT_EQ(*lane, raw::JettyLaneState::kReady);
}

TEST_F(RndvTxCoordinatorTest, CapacityRejectsThirdTransactionWithoutTakingSourceOrSequence) {
    Open();
    const auto first = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    const auto second = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(1);
    EXPECT_NE(first, second);
    const auto source = Source(77);
    const std::array input{WriteRndvSource{source, 0, 77}};
    const auto references = source_.stats().available_references;
    ExpectError(coordinator_.Submit(input, now_++), RndvTxErrorCode::kWouldBlock);
    EXPECT_EQ(coordinator_.stats().active, 2u);
    EXPECT_EQ(sequence_.next_sequence(), 2u);
    EXPECT_TRUE(sequence_.pending().empty());
    EXPECT_EQ(source_.stats().available_references, references);
    EXPECT_TRUE(source_.Readable(source));
}

TEST_F(RndvTxCoordinatorTest, DataPathUsesOnlyPreallocatedStorage) {
    Open();
    const auto source = Source(77);
    const std::array input{WriteRndvSource{source, 0, 77}};
    allocations = deallocations = 0;
    count_allocations = true;
    const auto submitted = coordinator_.Submit(input, now_++);
    const auto posted_rts = notifications_.Progress();
    count_allocations = false;
    ASSERT_TRUE(submitted);
    ASSERT_TRUE(posted_rts);
    ASSERT_EQ(packet_count_, 1u);
    const auto grant = Target(*submitted);
    const auto frame = Frame(grant.identity);
    const auto rts_event = Completion(0);
    count_allocations = true;
    const auto rts_complete = coordinator_.OnCompletion(rts_event);
    const auto accepted = coordinator_.OnGrant(frame, Route().context, now_++);
    const auto prepared = coordinator_.Progress(now_++);
    count_allocations = false;
    ASSERT_TRUE(rts_complete);
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(prepared);
    // provider import 属于慢控制路径，明确排除；事务准备、分块投递、逐 CQE 和 DONE 排队仍计数。
    ASSERT_TRUE(DriveCaches());
    count_allocations = true;
    const auto started = coordinator_.Progress(now_++);
    count_allocations = false;
    ASSERT_TRUE(started);
    ASSERT_EQ(packet_count_, 4u);
    for (std::size_t i = 1; i < 4; ++i) {
        const auto event = Completion(i);
        count_allocations = true;
        const auto completion = coordinator_.OnCompletion(event);
        count_allocations = false;
        ASSERT_TRUE(completion);
    }
    count_allocations = true;
    const auto done_ready = coordinator_.Progress(now_++);
    const auto done_posted = notifications_.Progress();
    count_allocations = false;
    ASSERT_TRUE(done_ready);
    ASSERT_TRUE(done_posted);
    ASSERT_EQ(packet_count_, 5u);
    const auto done_event = Completion(4);
    count_allocations = true;
    const auto done_complete = coordinator_.OnCompletion(done_event);
    const auto cleaned = coordinator_.Progress(now_++);
    const auto status = coordinator_.status(*submitted);
    const auto released = coordinator_.Release(*submitted);
    count_allocations = false;
    ASSERT_TRUE(done_complete);
    ASSERT_TRUE(cleaned);
    ASSERT_TRUE(status);
    EXPECT_TRUE(status->success);
    ASSERT_TRUE(released);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
}

TEST_F(RndvTxCoordinatorTest, ZeroAcceptanceBackpressureRetriesSameWritesAndStillProducesDone) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    Grant(Target(handle));
    Progress();
    ASSERT_TRUE(DriveCaches());
    // ENOMEM 的 bad_wr 指向首项，真实接受集合为空；不能令整个逻辑事务失败。
    mode_ = PostMode::kPartial;
    partial_ = 0;
    for (unsigned i = 0; i < 3; ++i) {
        Progress();
    }
    EXPECT_EQ(packet_count_, 1u);
    EXPECT_FALSE(coordinator_.stats().failed);
    EXPECT_FALSE(Status(handle).cancelled);
    EXPECT_EQ(Status(handle).write.accepted, 0u);
    mode_ = PostMode::kAll;
    // 发送器可在容量压力后缩小下一次批量，允许多个有界 Progress 排完后缀。
    for (unsigned i = 0; i < 3; ++i) {
        Progress();
    }
    ASSERT_EQ(packet_count_, 4u);
    EXPECT_EQ(Status(handle).write.accepted, 3u);
    Finish(handle);
}

TEST_F(RndvTxCoordinatorTest, PartialAcceptanceRetriesOnlyUnacceptedSuffix) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    Complete(0);
    Grant(Target(handle));
    Progress();
    ASSERT_TRUE(DriveCaches());
    // 首个分块已接受，另外两个仍属排队；下一次只能提交后缀，禁止重放首块。
    mode_ = PostMode::kPartial;
    partial_ = 1;
    Progress();
    ASSERT_EQ(packet_count_, 2u);
    EXPECT_EQ(Status(handle).write.accepted, 1u);
    const auto first_write = packets_[1].id;
    mode_ = PostMode::kAll;
    Progress();
    ASSERT_EQ(packet_count_, 4u);
    EXPECT_EQ(Status(handle).write.accepted, 3u);
    EXPECT_NE(packets_[2].id, first_write);
    EXPECT_NE(packets_[3].id, first_write);
    EXPECT_EQ(post_calls_, 3u);
    Finish(handle);
}

TEST_F(RndvTxCoordinatorTest, DuplicateOrForeignCompletionDoesNotCancelHealthyTransaction) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    const auto rts = Completion(0);
    ASSERT_TRUE(coordinator_.OnCompletion(rts));
    ExpectError(coordinator_.OnCompletion(rts), RndvTxErrorCode::kInvalidCompletion);
    ExpectError(coordinator_.OnCompletion({}), RndvTxErrorCode::kInvalidCompletion);
    EXPECT_FALSE(coordinator_.stats().failed);
    StartWrites(Target(handle));
    const auto first_write = Completion(1);
    ASSERT_TRUE(coordinator_.OnCompletion(first_write));
    ExpectError(coordinator_.OnCompletion(first_write), RndvTxErrorCode::kInvalidCompletion);
    EXPECT_FALSE(coordinator_.stats().failed);
    EXPECT_FALSE(Status(handle).cancelled);
    Finish(handle);
}

TEST_F(RndvTxCoordinatorTest, LateRtsFailureCannotBeHiddenByAlreadyAcceptedDone) {
    Open();
    const auto handle = Submit(Source(77));
    ASSERT_TRUE(notifications_.Progress());
    StartWrites(Target(handle));
    CompleteWrites();
    Progress();
    ASSERT_TRUE(Status(handle).done_queued);
    ASSERT_TRUE(notifications_.Progress());
    ASSERT_EQ(packet_count_, 5u);
    EXPECT_EQ(packets_[4].control, WireControlType::kRndvDone);
    // WRITE 和 DONE 均已暴露，但 RTS 的完成还没来；晚到 RTS 失败不能被 DONE 成功覆盖。
    EXPECT_FALSE(Status(handle).complete);
    EXPECT_FALSE(coordinator_.OnCompletion(Completion(0, URMA_CR_REM_ACCESS_ABORT_ERR)));
    Complete(4);
    Progress();
    EXPECT_TRUE(Status(handle).complete);
    EXPECT_TRUE(Status(handle).cancelled);
    EXPECT_FALSE(Status(handle).success);
    EXPECT_TRUE(coordinator_.stats().failed);
    EXPECT_EQ(modifies_, 0u);
}

TEST_F(RndvTxCoordinatorTest, DirectCancelRefreshesRtsAcceptanceBeforeAnyCoordinatorProgress) {
    Open();
    const auto source = Source(77);
    const auto handle = Submit(source);
    ReleaseCpu(source);
    ASSERT_TRUE(notifications_.Progress());
    ASSERT_EQ(packet_count_, 1u);
    // 通知队列已接受 RTS，协调器尚未 Progress；Cancel 仍须同步真实接受事实。
    (void)coordinator_.Cancel();
    EXPECT_TRUE(Status(handle).rts_accepted);
    EXPECT_TRUE(Status(handle).acceptance_known);
    EXPECT_TRUE(Status(handle).sources_released);
    EXPECT_FALSE(Status(handle).complete);
    Complete(0);
    Progress();
    EXPECT_TRUE(Status(handle).complete);
    EXPECT_TRUE(Status(handle).cancelled);
    EXPECT_FALSE(Status(handle).success);
    EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
}

class RndvTxCoordinatorDeathTest : public RndvTxCoordinatorTest {};
TEST_F(RndvTxCoordinatorDeathTest, UnknownWriteAcceptanceRetainsImportsAndNeverEmitsDone) {
    // 不确定接受不能伪造完整终结集合；依赖只能由隔离测试进程整体保留到退出。
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            Open();
            const auto source = Source(77);
            const auto handle = Submit(source);
            ReleaseCpu(source);
            ASSERT_TRUE(notifications_.Progress());
            Complete(0);
            Grant(Target(handle));
            Progress();
            ASSERT_TRUE(DriveCaches());
            mode_ = PostMode::kUnknown;
            EXPECT_FALSE(coordinator_.Progress(now_++));
            ASSERT_EQ(packet_count_, 4u);
            EXPECT_FALSE(Status(handle).acceptance_known);
            EXPECT_FALSE(Status(handle).done_queued);
            EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
            EXPECT_FALSE(coordinator_.Release(handle));
            EXPECT_FALSE(coordinator_.Close());
            EXPECT_GT(source_.stats().local_dma, 0u);
            EXPECT_GT(segments_.leases(), 0u);
            const auto calls = post_calls_;
            (void)coordinator_.Progress(now_++);
            EXPECT_EQ(post_calls_, calls);
            // Mock 确认捕获的三条物理 WR 被接受并生成 CQE，仍不能修复接受集合未知的逻辑证明。
            for (std::size_t i = 1; i < packet_count_; ++i) {
                EXPECT_TRUE(ledger_.Commit(packets_[i].id, 1));
                (void)coordinator_.OnCompletion(Completion(i));
            }
            (void)coordinator_.Progress(now_++);
            EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
            EXPECT_FALSE(coordinator_.Close());
            EXPECT_GT(segments_.leases(), 0u);
            EXPECT_FALSE(Status(handle).done_queued);
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(RndvTxCoordinatorDeathTest, DirectCancelPreservesUnknownRtsAcceptanceWithoutCoordinatorProgress) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            Open();
            const auto available = source_.stats().available;
            const auto source = Source(77);
            const auto handle = Submit(source);
            ReleaseCpu(source);
            mode_ = PostMode::kUnknown;
            EXPECT_FALSE(notifications_.Progress());
            ASSERT_EQ(packet_count_, 1u);
            // 不先 Progress，直接 Cancel 也必须记录 RTS 接受未知；不能被通知 Cancel 的早退跳过。
            EXPECT_FALSE(coordinator_.Cancel());
            EXPECT_FALSE(Status(handle).acceptance_known);
            EXPECT_TRUE(Status(handle).sources_released);
            EXPECT_FALSE(Status(handle).complete);
            EXPECT_FALSE(Status(handle).done_queued);
            EXPECT_FALSE(coordinator_.Close());
            EXPECT_FALSE(coordinator_.TerminalEvidence(handle));
            // RTS DMA 只读取它自己的控制 buffer；未开始 WRITE 的数据源可释放，通知依赖仍保留。
            EXPECT_EQ(source_.stats().available, available);
            EXPECT_EQ(source_.stats().local_dma, 1u);
            EXPECT_EQ(endpoints_.leases(), 3u);
            EXPECT_EQ(post_calls_, 1u);
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace connection
} // namespace kbsocket
