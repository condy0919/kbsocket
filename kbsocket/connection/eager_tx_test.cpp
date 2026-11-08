// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/eager_tx.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <new>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/runtime/control_executor.hpp"
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
class EagerTxTest : public ::testing::Test {
protected:
    struct Packet {
        raw::AttemptId id = 0;
        std::uint64_t imm = 0;
        std::size_t bytes = 0;
        std::array<std::byte, kWireMaxPayload> payload{};
    };
    enum class PostMode { kAll, kPartial, kUnknown };
    static inline EagerTxTest* active_ = nullptr;
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto*, auto*) { return active_->jfc_creates_++ ? &active_->rx_ : &active_->tx_; };
        f.create_jfr = [](auto*, auto*) { return &active_->jfr_; };
        f.create_jetty = [](auto*, auto*) { return active_->jetty_creates_++ ? &active_->jetty_ : &active_->passive_; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.register_seg = [](auto* ctx, auto* cfg) {
            active_->segment_.urma_ctx = ctx;
            active_->segment_.seg.ubva.va = cfg->va;
            active_->segment_.seg.len = cfg->len;
            return &active_->segment_;
        };
        f.unregister_seg = [](auto*) {
            ++active_->unregisters_;
            return URMA_SUCCESS;
        };
        f.import_jetty = [](auto* ctx, auto* descriptor, auto*) {
            active_->target_.urma_ctx = ctx;
            active_->target_.id = descriptor->jetty_id;
            return &active_->target_;
        };
        f.unimport_jetty = [](auto*) {
            ++active_->unimports_;
            return URMA_SUCCESS;
        };
        f.modify_jetty = [](auto*, auto*) {
            ++active_->modifies_;
            return URMA_SUCCESS;
        };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.post_jetty_send_wr = [](auto*, auto* first, auto** bad) { return active_->Post(first, bad); };
        f.poll_jfc = [](auto*, int capacity, auto* output) {
            std::size_t n = 0;
            while (n < static_cast<std::size_t>(capacity) && active_->cq_begin_ < active_->cq_end_) {
                output[n++] = active_->cq_[active_->cq_begin_++];
            }
            return static_cast<int>(n);
        };
        return f;
    }
    void SetUp() override {
        active_ = this;
        urma_device_cap_t cap{};
        cap.trans_mode = URMA_TM_RM;
        cap.rm_tp_cap.bs.ctp = 1;
        cap.max_jetty = 2;
        cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 2048;
        cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
        raw::JettyPoolConfig pool_config;
        pool_config.tx_depth = 512;
        pool_config.rx_depth = 1;
        pool_config.tx_cq_depth = 1024;
        jetty_.jetty_id.id = 19;
        passive_.jetty_id.id = 23;
        ASSERT_TRUE(pool_.Open(&context_, cap, pool_config));
        ASSERT_TRUE(ledger_.Open(pool_, 512));
        ASSERT_TRUE(sender_.Open(ledger_, true));
        std::array<urma_context_t*, 1> contexts{&context_};
        ASSERT_TRUE(slab_.Open(contexts, {.buffer_count = 514, .buffer_size = kWireMaxPayload, .max_references = 514},
                               budget_));
        ASSERT_TRUE(executor_.Open({.workers = 1, .capacity = 4}));
        ASSERT_TRUE(imports_.Open({.entries = 2, .leases = 1024, .in_flight = 1}));
        control::EndpointImportKey key{.local_context = &context_,
                                       .local_context_incarnation = 1,
                                       .peer_runtime = 10,
                                       .receive_domain = 20,
                                       .endpoint_generation = 30,
                                       .jetty_id = 40,
                                       .authorization_identity = 50};
        key.eid[15] = 7;
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = key.jetty_id;
        std::memcpy(&descriptor.jetty_id.eid, key.eid.data(), key.eid.size());
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        const auto pin = imports_.Acquire({key, std::as_bytes(std::span(&descriptor, 1)), {.token = 1}});
        ASSERT_TRUE(pin);
        endpoint_ = *pin;
        ASSERT_TRUE(credits_.Open(identity_));
        ASSERT_TRUE(credits_.Update(identity_, 1000, 1000));
    }
    void TearDown() override {
        count_allocations = false;
        EXPECT_TRUE(stream_.Close());
        EXPECT_TRUE(imports_.Release(endpoint_));
        EXPECT_TRUE(sender_.Close());
        EXPECT_TRUE(ledger_.Close());
        EXPECT_TRUE(pool_.Close());
        EXPECT_TRUE(slab_.Close());
        EXPECT_TRUE(credits_.Close());
        imports_.EvictUnused();
        PumpImports();
        EXPECT_TRUE(imports_.Close());
        EXPECT_TRUE(executor_.Close());
        active_ = nullptr;
    }
    void PumpImports() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            const auto next = imports_.NextOperation();
            ASSERT_TRUE(next);
            if (*next) {
                const auto& op = **next;
                ASSERT_TRUE(imports_.StartOperation(op, executor_,
                                                    {.context = op.context,
                                                     .incarnation = op.context_incarnation,
                                                     .users_paused = true,
                                                     .exclusive_control = true}));
            }
            std::array<runtime::ControlTaskCompletion, 4> completions;
            const auto n = executor_.Poll(completions);
            ASSERT_TRUE(n);
            for (std::size_t i = 0; i < *n; ++i) {
                ASSERT_TRUE(imports_.HandleCompletion(completions[i]));
            }
            if (!imports_.in_flight() && !*next) {
                return;
            }
            std::this_thread::yield();
        }
        FAIL() << "import control operation timed out";
    }
    EagerTxConfig Config(std::uint32_t capacity = 4, std::uint64_t id = 1) {
        return {.local_context = &context_,
                .connection_id = id,
                .destination = {3, 1},
                .session = session_,
                .credit_identity = identity_,
                .capacity = capacity,
                .max_frame_seq = 10000,
                .max_stream_bytes = 10000000};
    }
    void Open(std::uint32_t capacity = 4) {
        ASSERT_TRUE(stream_.Open(Config(capacity), sender_, slab_, imports_, endpoint_, credits_));
        PumpImports();
    }
    urma_status_t Post(urma_jfs_wr_t* first, urma_jfs_wr_t** bad) {
        ++post_calls_;
        std::size_t i = 0;
        for (auto* wr = first; wr; wr = wr->next, ++i) {
            if (mode_ == PostMode::kPartial && i == partial_) {
                *bad = wr;
                return URMA_ENOMEM;
            }
            auto& packet = packets_[packet_count_++];
            packet.id = wr->user_ctx;
            packet.imm = wr->send.imm_data;
            const auto& sg = wr->send.src.sge[0];
            packet.bytes = sg.len;
            std::memcpy(packet.payload.data(), reinterpret_cast<const void*>(sg.addr), sg.len);
        }
        *bad = nullptr;
        return mode_ == PostMode::kUnknown ? URMA_ENOMEM : URMA_SUCCESS;
    }
    void Enqueue(std::size_t packet, urma_cr_status_t status = URMA_CR_SUCCESS) {
        auto& cr = cq_[cq_end_++];
        cr = {};
        cr.status = status;
        cr.flag.bs.jetty = 1;
        cr.local_id = jetty_.jetty_id.id;
        cr.user_ctx = packets_[packet].id;
    }
    raw::TxCompletionEvent PollOne() {
        std::array<raw::TxCompletionEvent, 1> output;
        const auto result = processor_.Poll(output);
        EXPECT_TRUE(result);
        if (!result || result->count != 1) {
            ADD_FAILURE() << "missing completion";
            return {};
        }
        return output[0];
    }
    void Complete(std::size_t packet, EagerTxStream* stream = nullptr) {
        Enqueue(packet);
        EXPECT_TRUE((stream ? stream : &stream_)->OnCompletion(PollOne()));
    }
    raw::test_support::ScopedUrmaOverride override_{Functions()};
    urma_context_t context_{};
    urma_jfc_t tx_{}, rx_{};
    urma_jfr_t jfr_{};
    urma_jetty_t jetty_{}, passive_{};
    urma_target_jetty_t target_{};
    urma_target_seg_t segment_{};
    raw::JettyPool pool_;
    raw::AttemptLedger ledger_;
    raw::TxSender sender_;
    raw::TxCompletionProcessor processor_{ledger_};
    memory::MemoryBudget budget_{3 * 1024 * 1024};
    memory::SlabAllocator slab_;
    control::EndpointImportCache imports_;
    control::ImportLease endpoint_;
    runtime::ControlExecutor executor_;
    SenderCreditAccount credits_;
    EagerTxStream stream_;
    const CreditIdentity identity_{10, 20, 1};
    const SequenceSession session_{10, 20, 30};
    std::array<Packet, 1024> packets_{};
    std::array<urma_cr_t, 1024> cq_{};
    std::size_t jfc_creates_ = 0, jetty_creates_ = 0, post_calls_ = 0, packet_count_ = 0;
    std::size_t cq_begin_ = 0, cq_end_ = 0, unregisters_ = 0, unimports_ = 0, modifies_ = 0;
    PostMode mode_ = PostMode::kAll;
    std::size_t partial_ = 0;
};

TEST_F(EagerTxTest, WritableReportsCopyCapacityWithoutRemoteCreditOrProviderWork) {
    EXPECT_FALSE(stream_.writable());
    ASSERT_TRUE(credits_.Close());
    auto identity = identity_;
    ++identity.epoch;
    ASSERT_TRUE(credits_.Open(identity));
    ASSERT_TRUE(credits_.Update(identity, 0, 4));
    auto config = Config(1);
    config.credit_identity = identity;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, credits_));
    PumpImports();
    EXPECT_EQ(credits_.available(CreditClass::kData), 0u);
    // 本地 copy 准入不依赖 DATA credit，也不消费 Slab 或进行 provider 调用。
    const auto available = slab_.stats().available;
    allocations = 0;
    count_allocations = true;
    const bool writable = stream_.writable();
    count_allocations = false;
    EXPECT_TRUE(writable);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(slab_.stats().available, available);
    EXPECT_EQ(post_calls_, 0u);
    const std::array bytes{std::byte{1}};
    ASSERT_EQ(stream_.Write(bytes), 1u);
    EXPECT_FALSE(stream_.writable());
    const auto blocked = stream_.Progress();
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, EagerTxErrorCode::kWouldBlock);
    EXPECT_EQ(post_calls_, 0u);
    stream_.Fail();
    EXPECT_FALSE(stream_.writable());
}

TEST_F(EagerTxTest, WritableTracksSharedSlabExhaustionRecoveryAndStop) {
    Open();
    std::array<memory::BufferLease, 514> external{};
    std::size_t held = 0;
    while (slab_.stats().available) {
        auto lease = slab_.Acquire(1);
        ASSERT_TRUE(lease);
        external[held++] = *lease;
    }
    EXPECT_FALSE(stream_.writable());
    // 其他使用者归还 Slab 后，本连接没有 TX CQE 也可恢复本地可写提示。
    ASSERT_TRUE(slab_.ReleaseCpu(external[--held]));
    EXPECT_TRUE(stream_.writable());
    slab_.StopAccepting();
    EXPECT_FALSE(stream_.writable());
    while (held) {
        ASSERT_TRUE(slab_.ReleaseCpu(external[--held]));
    }
}

TEST_F(EagerTxTest, WritableTracksByteWindowAndHalfClose) {
    auto config = Config();
    config.max_stream_bytes = 1;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, credits_));
    EXPECT_TRUE(stream_.writable());
    const std::array bytes{std::byte{1}};
    ASSERT_EQ(stream_.Write(bytes), 1u);
    EXPECT_FALSE(stream_.writable());
    ASSERT_TRUE(stream_.ApplyCredit(session_, identity_, 1000, 1000, 10000, 2));
    EXPECT_TRUE(stream_.writable());
    ASSERT_TRUE(stream_.ShutdownWrite());
    EXPECT_FALSE(stream_.writable());
}

TEST_F(EagerTxTest, CopiesAcceptedPrefixAndKeepsDmaPinnedUntilExactTerminal) {
    Open(2);
    std::array<std::byte, 10000> source;
    source.fill(std::byte{0x5a});
    ASSERT_EQ(stream_.Write(source), 8192u);
    source.fill(std::byte{0xee});
    ASSERT_EQ(stream_.Progress(), 2u);
    ASSERT_EQ(packet_count_, 2u);
    EXPECT_EQ(packets_[0].payload[0], std::byte{0x5a});
    EXPECT_EQ(packets_[1].payload[4095], std::byte{0x5a});
    EXPECT_EQ(slab_.stats().cpu_leases, 1u);
    EXPECT_EQ(slab_.stats().local_dma, 2u);
    EXPECT_EQ(imports_.leases(), 4u); // 调用方、流、两个 WR 各一份独立引用。
    EXPECT_EQ(credits_.available(CreditClass::kData), 998u);
    // Close 只能丢弃确定未提交部分，不能借连接关闭回收在途 WR。
    EXPECT_FALSE(stream_.Close());
    EXPECT_EQ(slab_.stats().local_dma, 2u);
    Enqueue(0);
    auto event = PollOne();
    ASSERT_TRUE(event.record);
    auto stale = event;
    ++stale.record->metadata.operation_generation;
    EXPECT_FALSE(stream_.OnCompletion(stale));
    EXPECT_EQ(slab_.stats().local_dma, 2u);
    ASSERT_TRUE(stream_.OnCompletion(event));
    EXPECT_FALSE(stream_.OnCompletion(event));
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    Complete(1);
    EXPECT_EQ(credits_.available(CreditClass::kData), 998u);
    ASSERT_TRUE(stream_.Close());
    EXPECT_EQ(imports_.leases(), 1u);
}

TEST_F(EagerTxTest, PartialPostSuffixKeepsSequenceAndPayloadWithoutReplay) {
    Open(3);
    std::array<std::byte, 9000> bytes;
    bytes.fill(std::byte{0x17});
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    mode_ = PostMode::kPartial;
    partial_ = 1;
    ASSERT_EQ(stream_.Progress(), 1u);
    EXPECT_EQ(stream_.stats().queued, 2u);
    EXPECT_EQ(credits_.stats().spent[0], 1u);
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    mode_ = PostMode::kAll;
    ASSERT_EQ(stream_.Progress(), 2u);
    ASSERT_EQ(packet_count_, 3u);
    for (std::size_t i = 0; i < 3; ++i) {
        const auto frame = DecodeWireFrame(packets_[i].imm, std::span(packets_[i].payload).first(packets_[i].bytes));
        ASSERT_TRUE(frame);
        EXPECT_EQ(frame->frame.seq, i);
        EXPECT_EQ(frame->frame.data.front(), std::byte{0x17});
        Complete(i);
    }
    EXPECT_EQ(stream_.stats().accepted, 3u);
    EXPECT_EQ(credits_.stats().spent[0], 3u);
}

TEST_F(EagerTxTest, ReopenedCreditEpochCannotAdmitAnOldStreamAndDoesNotReleaseItsOutstandingDma) {
    Open(2);
    const std::array<std::byte, 32> bytes{};
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    ASSERT_EQ(stream_.Progress(), 1u);
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    // 共账完成撤回后可重开新 epoch；旧流即使仍保存未提交数据，也不能借用新额度。
    ASSERT_TRUE(credits_.Close());
    auto next = identity_;
    ++next.epoch;
    ASSERT_TRUE(credits_.Open(next));
    ASSERT_TRUE(credits_.Update(next, 10, 10));
    const auto progressed = stream_.Progress();
    ASSERT_FALSE(progressed);
    EXPECT_EQ(progressed.error().code, EagerTxErrorCode::kWrongIdentity);
    EXPECT_TRUE(stream_.stats().failed);
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(stream_.stats().queued, 0u);
    EXPECT_EQ(credits_.stats().spent[0], 0u);
    EXPECT_FALSE(credits_.stats().stopped);
    // 已接受的旧 WR 仍保留 DMA/import 责任，只能用其真实终结完成来归还。
    EXPECT_EQ(stream_.stats().in_flight, 1u);
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    EXPECT_FALSE(stream_.Close());
    Complete(0);
    EXPECT_EQ(slab_.stats().local_dma, 0u);
    EXPECT_TRUE(stream_.Close());
    EXPECT_EQ(credits_.stats().spent[0], 0u);
}

TEST_F(EagerTxTest, StoppingSharedCreditBlocksEveryStreamWhileAcceptedDmaRemainsPinned) {
    Open(2);
    EagerTxStream sibling;
    ASSERT_TRUE(sibling.Open(Config(2, 2), sender_, slab_, imports_, endpoint_, credits_));
    const std::array<std::byte, 32> bytes{};
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    ASSERT_EQ(stream_.Progress(), 1u);
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    ASSERT_EQ(sibling.Write(bytes), bytes.size());
    EXPECT_EQ(imports_.leases(), 4u); // 调用方、两条流和首条已接受 WR。
    credits_.Stop();
    ASSERT_TRUE(credits_.Update(identity_, 2000, 2000));
    // Peer 共账撤回覆盖所有逻辑流；晚到累计 Credit 不能重新开放任一流的 post。
    const auto first = stream_.Progress();
    const auto second = sibling.Progress();
    ASSERT_FALSE(first);
    ASSERT_FALSE(second);
    EXPECT_EQ(first.error().code, EagerTxErrorCode::kCredit);
    EXPECT_EQ(second.error().code, EagerTxErrorCode::kCredit);
    EXPECT_TRUE(stream_.stats().failed);
    EXPECT_TRUE(sibling.stats().failed);
    EXPECT_EQ(stream_.stats().queued, 0u);
    EXPECT_EQ(sibling.stats().queued, 0u);
    EXPECT_EQ(post_calls_, 1u);
    EXPECT_EQ(credits_.stats().spent[0], 1u);
    EXPECT_EQ(modifies_, 0u);
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    EXPECT_EQ(imports_.leases(), 4u);
    EXPECT_FALSE(stream_.Close());
    ASSERT_TRUE(sibling.Close());
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    // 停止只丢弃未提交队列，真实终结继续逐 WR 解除原 DMA/import 引用。
    Complete(0);
    EXPECT_EQ(slab_.stats().local_dma, 0u);
    ASSERT_TRUE(stream_.Close());
    EXPECT_EQ(imports_.leases(), 1u);
}

TEST_F(EagerTxTest, PendingImportAndEarlyCreditsAreValidBeforeRawActive) {
    auto config = Config();
    config.max_frame_seq = config.max_stream_bytes = 0;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, credits_));
    const std::array<std::byte, 10> source{};
    ASSERT_TRUE(stream_.ApplyCredit(session_, identity_, 1000, 1000, 1, 10));
    ASSERT_EQ(stream_.Write(source), 10u);
    EXPECT_FALSE(stream_.Progress());
    ASSERT_EQ(post_calls_, 0u);
    auto wrong = session_;
    ++wrong.session;
    EXPECT_FALSE(stream_.ApplyCredit(wrong, identity_, 1001, 1001, 2, 20));
    EXPECT_EQ(credits_.stats().limits[0], 1000u);
    PumpImports();
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(0);
}

TEST_F(EagerTxTest, UnknownAcceptanceQuarantinesAllAndNeverReplays) {
    Open(3);
    std::array<std::byte, 5000> source{};
    ASSERT_EQ(stream_.Write(source), source.size());
    mode_ = PostMode::kUnknown;
    const auto posted = stream_.Progress();
    ASSERT_FALSE(posted);
    EXPECT_EQ(posted.error().code, EagerTxErrorCode::kAcceptanceUnknown);
    EXPECT_EQ(stream_.stats().in_flight, 2u);
    EXPECT_TRUE(credits_.stats().stopped);
    EXPECT_EQ(credits_.stats().spent[0], 2u);
    EXPECT_EQ(slab_.stats().quarantined_buffers, 2u);
    EXPECT_EQ(imports_.leases(), 4u);
    EXPECT_FALSE(stream_.Close());
    EXPECT_FALSE(stream_.Progress());
    EXPECT_EQ(post_calls_, 1u);
    // Mock 明确执行了整批，只破坏 bad_wr 契约。模拟诊断确认后的账本恢复及真实逐 WR CQE，
    // 未提供这一证据前，生产路径不会自行 Commit/Cancel Prepared 账本。
    const auto record = ledger_.Lookup(packets_[0].id);
    ASSERT_TRUE(record);
    const auto lane = record->ticket.lane;
    for (std::size_t i = 0; i < packet_count_; ++i) {
        ASSERT_TRUE(ledger_.Commit(packets_[i].id, i + 1));
        Complete(i);
    }
    EXPECT_EQ(modifies_, 0u);
    EXPECT_EQ(pool_.lane_state(lane), raw::JettyLaneState::kFaulted);
    ASSERT_TRUE(pool_.BeginDrain(lane));
    ASSERT_TRUE(pool_.ObserveFlushDone(lane));
    std::array<raw::TxCompletionEvent, 1> output;
    ASSERT_TRUE(processor_.Flush(lane, output));
    EXPECT_EQ(slab_.stats().quarantined_buffers, 0u);
    EXPECT_TRUE(stream_.Close());
}

TEST_F(EagerTxTest, RemoteFailureStopsOnlyOneStreamOnSharedSqAndCreditAccount) {
    Open();
    EagerTxStream healthy;
    ASSERT_TRUE(healthy.Open(Config(2, 2), sender_, slab_, imports_, endpoint_, credits_));
    const std::array<std::byte, 16> data{};
    ASSERT_EQ(stream_.Write(data), 16u);
    ASSERT_EQ(healthy.Write(data), 16u);
    ASSERT_EQ(stream_.Progress(), 1u);
    ASSERT_EQ(healthy.Progress(), 1u);
    Enqueue(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    const auto event = PollOne();
    ASSERT_TRUE(event.lane);
    const auto failure = stream_.OnCompletion(event);
    ASSERT_FALSE(failure);
    EXPECT_EQ(failure.error().provider_status, 8);
    EXPECT_TRUE(stream_.stats().failed);
    EXPECT_EQ(pool_.lane_state(*event.lane), raw::JettyLaneState::kReady);
    EXPECT_EQ(modifies_, 0u);
    EXPECT_FALSE(credits_.stats().stopped);
    Complete(1, &healthy);
    ASSERT_EQ(healthy.Write(data), 16u);
    ASSERT_EQ(healthy.Progress(), 1u);
    Complete(2, &healthy);
    EXPECT_EQ(credits_.stats().spent[0], 3u);
    ASSERT_TRUE(healthy.Close());
}

TEST_F(EagerTxTest, FinFollowsQueuedDataAndCreditCanPostAfterHalfCloseWithoutAllocation) {
    Open(2);
    const std::array<std::byte, 8> data{};
    ASSERT_EQ(stream_.Write(data), data.size());
    ASSERT_TRUE(stream_.ShutdownWrite());
    EXPECT_FALSE(stream_.Write(data));
    ASSERT_EQ(stream_.Progress(), 1u);
    ASSERT_EQ(stream_.Progress(), 1u);
    const auto fin = DecodeWireFrame(packets_[1].imm, std::span(packets_[1].payload).first(packets_[1].bytes));
    ASSERT_TRUE(fin);
    EXPECT_EQ(fin->frame.control, WireControlType::kFin);
    EXPECT_EQ(fin->frame.seq, 1u);
    EXPECT_TRUE(stream_.stats().fin_accepted);
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .max_credit_seq = 10,
                          .max_control_credit_seq = 3,
                          .session = session_.session,
                          .credit_epoch = 9,
                          .max_frame_seq = 16,
                          .max_stream_bytes = 1024};
    // 已占满两个数据槽；独立控制槽仍发送，实际 Write/Post/CQE 不触发堆分配。
    allocations = 0;
    count_allocations = true;
    const auto queued = stream_.QueueCredit(credit);
    const auto posted = stream_.Progress();
    Enqueue(2);
    std::array<raw::TxCompletionEvent, 1> events;
    const auto polled = processor_.Poll(events);
    const auto complete =
        polled && polled->count == 1
            ? stream_.OnCompletion(events[0])
            : std::expected<void, EagerTxError>(std::unexpected(EagerTxError{EagerTxErrorCode::kNotReady}));
    count_allocations = false;
    ASSERT_TRUE(queued);
    ASSERT_EQ(posted, 1u);
    ASSERT_TRUE(complete);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(credits_.stats().spent[1], 2u); // FIN 与 CREDIT 分别使用控制额度。
    Complete(0);
    Complete(1);
}

TEST_F(EagerTxTest, ExtendedTokenShrinksPayloadAndPostsAtMost256WrPerCall) {
    auto config = Config(257);
    config.destination = {100000, 400};
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, credits_));
    PumpImports();
    std::array<std::byte, 4088> data{};
    for (std::size_t i = 0; i < 257; ++i) {
        ASSERT_EQ(stream_.Write(data), data.size());
    }
    ASSERT_EQ(stream_.Progress(), 256u);
    ASSERT_EQ(stream_.Progress(), 1u);
    EXPECT_EQ(post_calls_, 2u);
    for (std::size_t i = 0; i < packet_count_; ++i) {
        const auto decoded = DecodeWireFrame(packets_[i].imm, packets_[i].payload);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->frame.token, config.destination);
        EXPECT_EQ(decoded->frame.seq, i);
        EXPECT_EQ(decoded->frame.data.size(), 4088u);
        Complete(i);
    }
}

TEST_F(EagerTxTest, DataDoesNotAllocateOnWritePostOrTerminal) {
    Open();
    std::array<std::byte, 4096> data{};
    allocations = 0;
    count_allocations = true;
    const auto written = stream_.Write(data);
    const auto posted = stream_.Progress();
    Enqueue(0);
    std::array<raw::TxCompletionEvent, 1> events;
    const auto polled = processor_.Poll(events);
    const auto completed =
        polled && polled->count == 1
            ? stream_.OnCompletion(events[0])
            : std::expected<void, EagerTxError>(std::unexpected(EagerTxError{EagerTxErrorCode::kNotReady}));
    count_allocations = false;
    ASSERT_EQ(written, data.size());
    ASSERT_EQ(posted, 1u);
    ASSERT_TRUE(completed);
    EXPECT_EQ(allocations, 0u);
}

TEST_F(EagerTxTest, SmallByteWindowReturnsCopiedPrefixInsteadOfBuildingUnsendableFrame) {
    auto config = Config();
    config.max_stream_bytes = 16;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, credits_));
    PumpImports();
    std::array<std::byte, 100> source{};
    ASSERT_EQ(stream_.Write(source), 16u);
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(0);
    EXPECT_FALSE(stream_.Write(source));
    ASSERT_TRUE(stream_.ApplyCredit(session_, identity_, 1000, 1000, 10000, 32));
    ASSERT_EQ(stream_.Write(source), 16u);
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(1);
}

TEST_F(EagerTxTest, OversizedBatchShrinksAfterTrustedPrePostRejection) {
    Open(3);
    std::array<raw::AttemptId, 510> held{};
    // 其他连接占用共享 SQ，仅余两个槽；大批预留失败不能令本流永久等待三槽同时空闲。
    for (auto& id : held) {
        const auto prepared = ledger_.Prepare({.connection_id = 999, .target = &target_});
        ASSERT_TRUE(prepared);
        id = *prepared;
    }
    std::array<std::byte, 9000> bytes{};
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    EXPECT_FALSE(stream_.Progress());
    EXPECT_EQ(post_calls_, 0u);
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(0);
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(1);
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(2);
    for (const auto id : held) {
        ASSERT_TRUE(ledger_.Cancel(id));
    }
}

TEST_F(EagerTxTest, ReservedControlBufferSurvivesFullSharedSlabAndExhaustedDataCredits) {
    SenderCreditAccount limited;
    ASSERT_TRUE(limited.Open(identity_));
    ASSERT_TRUE(limited.Update(identity_, 0, 2));
    ASSERT_TRUE(stream_.Open(Config(2), sender_, slab_, imports_, endpoint_, limited));
    PumpImports();
    const std::array<std::byte, 8> data{};
    ASSERT_EQ(stream_.Write(data), data.size());
    ASSERT_EQ(stream_.Write(data), data.size());
    EXPECT_FALSE(stream_.Progress());
    // 其他连接借尽 Slab 剩余容量。事前预留的控制 buffer 不依赖此时再 Acquire。
    std::array<memory::BufferLease, 511> others;
    for (auto& lease : others) {
        const auto acquired = slab_.Acquire(64);
        ASSERT_TRUE(acquired);
        lease = *acquired;
    }
    EXPECT_EQ(slab_.stats().available, 0u);
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .max_credit_seq = 3,
                          .max_control_credit_seq = 2,
                          .session = session_.session,
                          .credit_epoch = 1,
                          .max_frame_seq = 16,
                          .max_stream_bytes = 1024};
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(0);
    // 同一控制 buffer 在 CQE 后可再次使用，不产生新 slab 租约。
    ++credit.max_credit_seq;
    ++credit.max_control_credit_seq;
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(1);
    EXPECT_EQ(slab_.stats().available, 0u);
    EXPECT_EQ(limited.stats().spent[0], 0u);
    EXPECT_EQ(limited.stats().spent[1], 2u);
    for (auto lease : others) {
        ASSERT_TRUE(slab_.ReleaseCpu(lease));
    }
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(limited.Close());
}

TEST_F(EagerTxTest, InvalidatedTargetKeepsPostedPinUntilCompletion) {
    Open();
    const std::array<std::byte, 8> data{};
    ASSERT_EQ(stream_.Write(data), data.size());
    ASSERT_EQ(stream_.Progress(), 1u);
    ASSERT_TRUE(imports_.Invalidate(endpoint_));
    EXPECT_FALSE(stream_.Close());
    EXPECT_EQ(imports_.leases(), 3u);
    EXPECT_EQ(unimports_, 0u);
    EXPECT_EQ(slab_.stats().local_dma, 1u);
    Complete(0);
    ASSERT_TRUE(stream_.Close());
    EXPECT_EQ(imports_.leases(), 1u);
    EXPECT_EQ(unimports_, 0u);
}

TEST_F(EagerTxTest, FinalControlCreditOnlyCarriesNewControlAuthorization) {
    SenderCreditAccount limited;
    ASSERT_TRUE(limited.Open(identity_));
    ASSERT_TRUE(limited.Update(identity_, 1, 2));
    auto config = Config();
    config.initial_advertised_control = 10;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, limited));
    PumpImports();
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .max_credit_seq = 3,
                          .max_control_credit_seq = 10,
                          .session = session_.session,
                          .credit_epoch = 1,
                          .max_frame_seq = 16,
                          .max_stream_bytes = 1024};
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(0);
    ++credit.max_credit_seq;
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 0u);
    EXPECT_EQ(limited.available(CreditClass::kControl), 1u);
    // 普通 CREDIT 等待不能阻塞独立 DATA；最后控制槽留给真正增加对方 control 上界的报文。
    const std::array<std::byte, 8> data{};
    ASSERT_EQ(stream_.Write(data), data.size());
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(1);
    ++credit.max_control_credit_seq;
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(2);
    EXPECT_EQ(limited.available(CreditClass::kControl), 0u);
    ASSERT_TRUE(stream_.ShutdownWrite());
    ASSERT_TRUE(stream_.ApplyCredit(session_, identity_, 1, 3, 10000, 10000000));
    EXPECT_FALSE(stream_.Progress());
    EXPECT_FALSE(stream_.stats().fin_accepted);
    ASSERT_TRUE(stream_.ApplyCredit(session_, identity_, 1, 4, 10000, 10000000));
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(3);
    EXPECT_TRUE(stream_.stats().fin_accepted);
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(limited.Close());
}

TEST_F(EagerTxTest, ConnectionsSharePublishedControlFrontierBeforeSpendingLastCredit) {
    SenderCreditAccount limited;
    ReceiveCreditDomain receive;
    ASSERT_TRUE(limited.Open(identity_));
    ASSERT_TRUE(limited.Update(identity_, 0, 2));
    ASSERT_TRUE(receive.Open(
        {.receive_domain = 20, .capacity = 32, .control_slots = 16, .peer_capacity = 1, .base_credits = 1}));
    ASSERT_TRUE(receive.RecordInitialPosted(CreditClass::kData, 16));
    ASSERT_TRUE(receive.RecordInitialPosted(CreditClass::kControl, 16));
    const auto peer = receive.Admit(identity_);
    ASSERT_TRUE(peer);
    ASSERT_EQ(receive.Grant(*peer, CreditClass::kControl, 10), 10u);
    auto config = Config();
    config.advertised_credits = &receive;
    config.advertised_peer = *peer;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, limited));
    EagerTxStream sibling;
    config.connection_id = 2;
    ASSERT_TRUE(sibling.Open(config, sender_, slab_, imports_, endpoint_, limited));
    PumpImports();
    WireFrameInput credit{.type = WireFrameType::kControl,
                          .max_control_credit_seq = 10,
                          .session = session_.session,
                          .credit_epoch = 1,
                          .max_frame_seq = 16,
                          .max_stream_bytes = 1024};
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Complete(0);
    EXPECT_EQ(receive.PublishedControl(*peer), 10u);
    // 第二条连接不能把同一 peer 上其他连接已发布的上界误当作新授权，消耗最后控制额度。
    ASSERT_TRUE(sibling.QueueCredit(credit));
    ASSERT_EQ(sibling.Progress(), 0u);
    EXPECT_EQ(limited.available(CreditClass::kControl), 1u);
    ASSERT_EQ(receive.Grant(*peer, CreditClass::kControl, 1), 11u);
    credit.max_control_credit_seq = 11;
    ASSERT_TRUE(sibling.QueueCredit(credit));
    ASSERT_EQ(sibling.Progress(), 1u);
    Complete(1, &sibling);
    EXPECT_EQ(receive.PublishedControl(*peer), 11u);
    ASSERT_TRUE(sibling.Close());
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(limited.Close());
    ASSERT_TRUE(receive.RetirePeer(
        *peer, {.sender_stopped = true, .accepted_set_settled = true, .unused_grants_revoked = true}));
    ASSERT_TRUE(receive.Close());
}

TEST_F(EagerTxTest, PreparationReferenceExhaustionShrinksBatchWithoutChangingSequence) {
    ASSERT_TRUE(slab_.Close());
    std::array<urma_context_t*, 1> contexts{&context_};
    ASSERT_TRUE(
        slab_.Open(contexts, {.buffer_count = 8, .buffer_size = kWireMaxPayload, .max_references = 1}, budget_));
    Open(3);
    std::array<std::byte, 9000> bytes{};
    ASSERT_EQ(stream_.Write(bytes), bytes.size());
    // 一次只能取得一个 DMA reference；大批准备失败必须全部取消，再以小批推进原 SN 后缀。
    const auto first = stream_.Progress();
    ASSERT_FALSE(first);
    EXPECT_EQ(first.error().code, EagerTxErrorCode::kWouldBlock);
    EXPECT_EQ(post_calls_, 0u);
    EXPECT_EQ(slab_.stats().local_dma, 0u);
    EXPECT_EQ(imports_.leases(), 2u);
    EXPECT_EQ(credits_.stats().spent[0], 0u);
    for (std::size_t i = 0; i < 3; ++i) {
        ASSERT_EQ(stream_.Progress(), 1u);
        const auto frame = DecodeWireFrame(packets_[i].imm, std::span(packets_[i].payload).first(packets_[i].bytes));
        ASSERT_TRUE(frame);
        EXPECT_EQ(frame->frame.seq, i);
        Complete(i);
    }
    EXPECT_EQ(stream_.stats().accepted, 3u);
    EXPECT_EQ(stream_.stats().retired, 3u);
}

TEST_F(EagerTxTest, FailedLastControlReturnStopsSharedAccountAndFreezesPeerWithoutDrainingSq) {
    SenderCreditAccount limited;
    ReceiveCreditDomain receive;
    ASSERT_TRUE(limited.Open(identity_));
    ASSERT_TRUE(limited.Update(identity_, 1, 1));
    ASSERT_TRUE(receive.Open(
        {.receive_domain = 20, .capacity = 32, .control_slots = 16, .peer_capacity = 1, .base_credits = 1}));
    ASSERT_TRUE(receive.RecordInitialPosted(CreditClass::kData, 16));
    ASSERT_TRUE(receive.RecordInitialPosted(CreditClass::kControl, 16));
    const auto peer = receive.Admit(identity_);
    ASSERT_TRUE(peer);
    ASSERT_EQ(receive.Grant(*peer, CreditClass::kControl, 1), 1u);
    auto config = Config();
    config.advertised_credits = &receive;
    config.advertised_peer = *peer;
    ASSERT_TRUE(stream_.Open(config, sender_, slab_, imports_, endpoint_, limited));
    EagerTxStream sibling;
    config.connection_id = 2;
    ASSERT_TRUE(sibling.Open(config, sender_, slab_, imports_, endpoint_, limited));
    PumpImports();
    const std::array<std::byte, 8> data{};
    ASSERT_EQ(sibling.Write(data), data.size());
    const WireFrameInput credit{
        .type = WireFrameType::kControl, .max_control_credit_seq = 1, .session = session_.session, .credit_epoch = 1};
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Enqueue(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    const auto event = PollOne();
    ASSERT_TRUE(event.record);
    // 错代事件即使声称错误也不能冻结共享 account，只有可信终结能触发最后控制额度的失败策略。
    auto stale = event;
    ++stale.record->metadata.operation_generation;
    EXPECT_FALSE(stream_.OnCompletion(stale));
    EXPECT_FALSE(limited.stats().stopped);
    const auto completion = stream_.OnCompletion(event);
    ASSERT_FALSE(completion);
    EXPECT_EQ(completion.error().provider_status, 8);
    EXPECT_TRUE(limited.stats().stopped);
    EXPECT_EQ(limited.stats().spent[1], 1u);
    const auto peer_stats = receive.PeerStats(*peer);
    ASSERT_TRUE(peer_stats);
    EXPECT_TRUE(peer_stats->frozen);
    ASSERT_TRUE(event.lane);
    EXPECT_EQ(pool_.lane_state(*event.lane), raw::JettyLaneState::kReady);
    EXPECT_EQ(modifies_, 0u);
    const auto progress = sibling.Progress();
    ASSERT_FALSE(progress);
    EXPECT_EQ(progress.error().code, EagerTxErrorCode::kCredit);
    EXPECT_TRUE(sibling.stats().failed);
    EXPECT_EQ(sibling.stats().queued, 0u);
    EXPECT_EQ(post_calls_, 1u);
    ASSERT_TRUE(sibling.Close());
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(limited.Close());
    ASSERT_TRUE(receive.RetirePeer(
        *peer, {.sender_stopped = true, .accepted_set_settled = true, .unused_grants_revoked = true}));
    ASSERT_TRUE(receive.Close());
}

TEST_F(EagerTxTest, LastControlCompletionErrorStopsAccountWithoutPublishedDomainBinding) {
    SenderCreditAccount limited;
    ASSERT_TRUE(limited.Open(identity_));
    ASSERT_TRUE(limited.Update(identity_, 1, 1));
    ASSERT_TRUE(stream_.Open(Config(), sender_, slab_, imports_, endpoint_, limited));
    EagerTxStream sibling;
    ASSERT_TRUE(sibling.Open(Config(4, 2), sender_, slab_, imports_, endpoint_, limited));
    PumpImports();
    const WireFrameInput credit{
        .type = WireFrameType::kControl, .max_control_credit_seq = 1, .session = session_.session, .credit_epoch = 1};
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    Enqueue(0);
    auto event = PollOne();
    // 已退休记录仍可能附带完成器错误；无 published-domain 宿主的原语也须显式停止同 peer 流。
    event.error = raw::TxCompletionError{.code = raw::TxCompletionErrorCode::kPoolFailure};
    EXPECT_FALSE(stream_.OnCompletion(event));
    EXPECT_TRUE(limited.stats().stopped);
    const auto progress = sibling.Progress();
    ASSERT_FALSE(progress);
    EXPECT_EQ(progress.error().code, EagerTxErrorCode::kCredit);
    EXPECT_EQ(modifies_, 0u);
    ASSERT_TRUE(sibling.Close());
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(limited.Close());
}

TEST_F(EagerTxTest, OldLastControlFailureCannotStopAReopenedCreditEpoch) {
    SenderCreditAccount limited;
    ASSERT_TRUE(limited.Open(identity_));
    ASSERT_TRUE(limited.Update(identity_, 1, 1));
    ASSERT_TRUE(stream_.Open(Config(), sender_, slab_, imports_, endpoint_, limited));
    PumpImports();
    const WireFrameInput credit{
        .type = WireFrameType::kControl, .max_control_credit_seq = 1, .session = session_.session, .credit_epoch = 1};
    ASSERT_TRUE(stream_.QueueCredit(credit));
    ASSERT_EQ(stream_.Progress(), 1u);
    ASSERT_TRUE(limited.Close());
    auto next = identity_;
    ++next.epoch;
    ASSERT_TRUE(limited.Open(next));
    ASSERT_TRUE(limited.Update(next, 2, 2));
    // 接收责任已结清与本地 TX CQE 消费是两个时间点；旧错误仍须退休 DMA，但不能污染新账。
    Enqueue(0, URMA_CR_REM_ACCESS_ABORT_ERR);
    const auto event = PollOne();
    const auto completed = stream_.OnCompletion(event);
    ASSERT_FALSE(completed);
    EXPECT_EQ(completed.error().provider_status, 8);
    EXPECT_EQ(stream_.stats().in_flight, 0u);
    EXPECT_EQ(slab_.stats().local_dma, 0u);
    EXPECT_FALSE(limited.stats().stopped);
    EXPECT_EQ(limited.available(CreditClass::kControl), 2u);
    EXPECT_EQ(limited.stats().spent[1], 0u);
    EXPECT_EQ(modifies_, 0u);
    ASSERT_TRUE(stream_.Close());
    ASSERT_TRUE(limited.Close());
}
} // namespace
} // namespace connection
} // namespace kbsocket
