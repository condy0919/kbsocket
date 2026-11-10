// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/raw_stack.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <expected>
#include <optional>
#include <span>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace runtime {
namespace {

class RawStackTest : public ::testing::Test {
protected:
    enum class Kind { kJetty, kJfr, kMr, kJfc, kContext, kUninit };
    struct Deletion {
        Kind kind;
        std::size_t context;
    };
    struct Posted {
        std::uint64_t id;
        std::span<std::byte> memory;
    };
    static inline RawStackTest* active_ = nullptr;
    std::size_t Index(const urma_context_t* context) const {
        for (std::size_t i = 0; i < contexts_.size(); ++i) {
            if (context == &contexts_[i]) {
                return i;
            }
        }
        ADD_FAILURE() << "unexpected context";
        return 0;
    }
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.init = [](auto*) {
            ++active_->init_calls_;
            return URMA_SUCCESS;
        };
        f.uninit = [] {
            ++active_->uninit_calls_;
            active_->deletions_.push_back({Kind::kUninit, 0});
            return active_->uninit_status_;
        };
        f.get_devices = [](int* count) {
            *count = 2;
            return active_->device_ptrs_.data();
        };
        f.free_devices = [](auto*) {};
        f.get_eids = [](auto* device, auto* count) {
            *count = 2;
            return active_->eids_[device == &active_->devices_[0] ? 0 : 1].data();
        };
        f.free_eids = [](auto*) {};
        f.query_device = [](auto*, auto* attributes) {
            ++active_->query_calls_;
            *attributes = {};
            auto& cap = attributes->dev_cap;
            cap.trans_mode = URMA_TM_RM;
            cap.rm_tp_cap.bs.ctp = 1;
            cap.max_eid_cnt = 256;
            cap.max_jetty = 8;
            cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 1024;
            cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
            return URMA_SUCCESS;
        };
        f.create_context = [](auto* device, std::uint32_t eid) -> urma_context_t* {
            auto& s = *active_;
            ++s.context_create_calls_;
            if (s.fail_context_create_ == s.context_create_calls_) {
                errno = ENOMEM;
                return nullptr;
            }
            const std::size_t index = (device == &s.devices_[0] ? 0 : 2) + (eid == 39 ? 0 : 1);
            return &s.contexts_[index];
        };
        f.delete_context = [](auto* context) {
            auto& s = *active_;
            const auto index = s.Index(context);
            ++s.context_delete_calls_;
            if (s.fail_context_delete_ && index == *s.fail_context_delete_) {
                return URMA_EAGAIN;
            }
            s.deletions_.push_back({Kind::kContext, index});
            return URMA_SUCCESS;
        };
        f.create_jfc = [](auto* context, auto* config) -> urma_jfc_t* {
            auto& s = *active_;
            const auto index = s.Index(context);
            const auto slot = s.jfc_created_[index]++;
            if (s.fail_jfc_context_ && index == *s.fail_jfc_context_) {
                errno = ENOMEM;
                return nullptr;
            }
            auto& jfc = s.jfcs_[index][slot];
            jfc.urma_ctx = context;
            jfc.jfc_cfg = *config;
            jfc.jfc_id.id = static_cast<std::uint32_t>(100 + index * 2 + slot);
            return &jfc;
        };
        f.delete_jfc = [](auto* jfc) {
            active_->deletions_.push_back({Kind::kJfc, active_->Index(jfc->urma_ctx)});
            return URMA_SUCCESS;
        };
        f.create_jfr = [](auto* context, auto* config) {
            auto& s = *active_;
            const auto index = s.Index(context);
            auto& jfr = s.jfrs_[index];
            jfr.urma_ctx = context;
            jfr.jfr_cfg = *config;
            jfr.jfr_id.id = static_cast<std::uint32_t>(200 + index);
            return &jfr;
        };
        f.delete_jfr = [](auto* jfr) {
            auto& s = *active_;
            const auto index = s.Index(jfr->urma_ctx);
            ++s.jfr_delete_calls_;
            if (s.fail_jfr_delete_ && index == *s.fail_jfr_delete_) {
                return URMA_EAGAIN;
            }
            s.deletions_.push_back({Kind::kJfr, index});
            s.posted_[index].clear();
            return URMA_SUCCESS;
        };
        f.create_jetty = [](auto* context, auto* config) {
            auto& s = *active_;
            const auto index = s.Index(context);
            auto& jetty = s.jetties_[index][s.jetty_created_[index]++];
            jetty.urma_ctx = context;
            jetty.jetty_cfg = *config;
            jetty.jetty_id.id = static_cast<std::uint32_t>(300 + index * 2 + s.jetty_created_[index]);
            return &jetty;
        };
        f.delete_jetty = [](auto* jetty) {
            active_->deletions_.push_back({Kind::kJetty, active_->Index(jetty->urma_ctx)});
            return URMA_SUCCESS;
        };
        f.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.register_seg = [](auto* context, auto* config) -> urma_target_seg_t* {
            auto& s = *active_;
            const auto call = ++s.register_calls_;
            if (s.fail_register_call_ == call) {
                errno = ENOMEM;
                return nullptr;
            }
            auto& segment = s.segments_[call - 1];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            return &segment;
        };
        f.unregister_seg = [](auto* segment) {
            auto& s = *active_;
            ++s.unregister_calls_;
            if (s.fail_unregister_call_ == s.unregister_calls_) {
                return URMA_EAGAIN;
            }
            s.deletions_.push_back({Kind::kMr, s.Index(segment->urma_ctx)});
            return URMA_SUCCESS;
        };
        f.post_jfr_wr = [](auto* jfr, auto* first, auto** bad) {
            auto& s = *active_;
            const auto index = s.Index(jfr->urma_ctx);
            ++s.post_calls_[index];
            *bad = nullptr;
            std::size_t count = 0;
            for (auto* wr = first; wr; wr = wr->next, ++count) {
                if (s.fail_post_context_ && index == *s.fail_post_context_ && count == 3) {
                    *bad = wr;
                    return URMA_EAGAIN;
                }
                const auto& sge = wr->src.sge[0];
                s.posted_[index].push_back({wr->user_ctx, {reinterpret_cast<std::byte*>(sge.addr), sge.len}});
            }
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* jfc, int capacity, auto* output) {
            auto& s = *active_;
            const auto index = s.Index(jfc->urma_ctx);
            ++s.polls_[index];
            if (s.failed_cq_context_ && index == *s.failed_cq_context_) {
                return -EIO;
            }
            if (jfc != &s.jfcs_[index][1]) {
                return 0;
            }
            int count = 0;
            while (count < capacity && !s.completions_[index].empty()) {
                output[count++] = s.completions_[index].front();
                s.completions_[index].pop_front();
            }
            return count;
        };
        f.import_jetty = [](auto* context, auto* descriptor, auto*) -> urma_target_jetty_t* {
            auto& s = *active_;
            const auto index = s.import_calls_.fetch_add(1);
            while (s.block_import_.load()) {
                s.block_import_.wait(true);
            }
            auto& target = s.targets_[index];
            target.urma_ctx = context;
            target.id = descriptor->jetty_id;
            target.type = descriptor->type;
            target.trans_mode = descriptor->trans_mode;
            target.tp_type = descriptor->tp_type;
            return &target;
        };
        f.unimport_jetty = [](auto*) {
            active_->unimport_calls_.fetch_add(1);
            return URMA_SUCCESS;
        };
        f.import_seg = [](auto* context, auto* descriptor, auto*, std::uint64_t,
                          urma_import_seg_flag_t) -> urma_target_seg_t* {
            auto& s = *active_;
            const auto index = s.segment_import_calls_.fetch_add(1);
            while (s.block_segment_import_.load()) {
                s.block_segment_import_.wait(true);
            }
            auto& target = s.imported_segments_[index];
            target.urma_ctx = context;
            target.seg = *descriptor;
            return &target;
        };
        f.unimport_seg = [](auto*) {
            active_->segment_unimport_calls_.fetch_add(1);
            return URMA_SUCCESS;
        };
        return f;
    }
    void SetUp() override {
        active_ = this;
        config_.connections_per_context = config_.withdrawals_per_context = 4;
        config_.commands_per_context = 4;
        config_.imports = {8, 16, 4};
        config_.segments = {8, 16, 4};
        config_.slab = {8, 4096, 16};
        config_.memory_budget_bytes = 32768;
        for (std::size_t device = 0; device < 2; ++device) {
            std::snprintf(devices_[device].name, URMA_MAX_NAME, "raw%zu", device);
            devices_[device].type = URMA_TRANSPORT_UB;
            device_ptrs_[device] = &devices_[device];
            for (std::size_t eid = 0; eid < 2; ++eid) {
                const auto index = device * 2 + eid;
                eids_[device][eid].eid_index = eid == 0 ? 39 : 72;
                eids_[device][eid].eid.raw[15] = static_cast<std::uint8_t>(index + 1);
                auto& context = contexts_[index];
                context.dev = &devices_[device];
                context.async_fd = -1;
                context.eid = eids_[device][eid].eid;
                context.eid_index = eids_[device][eid].eid_index;
                auto& endpoint = endpoints_[index];
                endpoint.device = {devices_[device].name, context.eid_index};
                endpoint.domain.local_domain = {11, 30 + index};
                endpoint.domain.context_incarnation = 101 + index;
                endpoint.domain.queues.tx_depth = 8;
                endpoint.domain.queues.rx_depth = endpoint.domain.queues.rx_cq_depth = 8;
                endpoint.domain.tx_attempts = 8;
                endpoint.domain.route_capacity = endpoint.domain.source_capacity = endpoint.domain.peer_capacity = 4;
                endpoint.domain.control_slots = 2;
                endpoint.domain.base_credits = 1;
                proofs_[index] = {true, true, true, true};
            }
        }
    }
    bool SpinUntil(auto predicate) {
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!predicate() && std::chrono::steady_clock::now() < until) {
            if (const auto ticked = stack_.Tick(); !ticked) {
                ADD_FAILURE() << "Tick failed: " << static_cast<int>(ticked.error().code);
                return false;
            }
            std::this_thread::yield();
        }
        return predicate();
    }
    RawStackCloseProof Proof() {
        return {true, std::span(proofs_).first(endpoint_count_)};
    }
    void Open(std::size_t count = 4) {
        endpoint_count_ = count;
        const auto opened = stack_.Open(std::span(endpoints_).first(count), config_);
        ASSERT_TRUE(opened) << static_cast<int>(opened.error().code);
        for (std::size_t i = 0; i < count; ++i) {
            const auto resources = stack_.resources(i);
            ASSERT_TRUE(resources);
            resources_[i] = *resources;
        }
        const auto shared = stack_.shared();
        ASSERT_TRUE(shared);
        shared_ = *shared;
    }
    void Close() {
        ASSERT_TRUE(stack_.BeginClose());
        for (unsigned attempt = 0; attempt < 8 && stack_.stage() != RawStackStage::kClosed; ++attempt) {
            const auto closed = stack_.AdvanceClose(Proof());
            ASSERT_TRUE(closed) << static_cast<int>(closed.error().code);
        }
        EXPECT_EQ(stack_.stage(), RawStackStage::kClosed);
    }
    std::size_t Deleted(Kind kind) const {
        return std::ranges::count_if(deletions_, [kind](const auto& item) { return item.kind == kind; });
    }
    void ExpectStillRunning() {
        const auto snapshot = stack_.Inspect();
        ASSERT_TRUE(snapshot);
        EXPECT_TRUE(snapshot->progress_attached);
        EXPECT_EQ(Deleted(Kind::kJetty), 0u);
        EXPECT_EQ(Deleted(Kind::kContext), 0u);
        EXPECT_EQ(uninit_calls_, 0u);
        EXPECT_TRUE(stack_.Tick());
    }
    void TearDown() override {
        block_import_.store(false);
        block_import_.notify_all();
        block_segment_import_.store(false);
        block_segment_import_.notify_all();
        if (stack_.stage() != RawStackStage::kEmpty && stack_.stage() != RawStackStage::kClosed &&
            stack_.stage() != RawStackStage::kRetained) {
            // 兜底仅收割本测试已经知道的责任；不伪造生产代码的终结或静默证据。
            (void)SpinUntil([this] {
                for (std::size_t i = 0; i < endpoint_count_; ++i) {
                    if (stack_.paused(i)) {
                        return false;
                    }
                }
                return true;
            });
            for (const auto lease : import_leases_) {
                (void)shared_->imports->Release(lease);
            }
            for (const auto lease : segment_leases_) {
                (void)shared_->segments->Release(lease);
            }
            fail_jfr_delete_.reset();
            fail_context_delete_.reset();
            fail_unregister_call_ = 0;
            ASSERT_TRUE(stack_.BeginClose());
            if (shared_) {
                ASSERT_TRUE(SpinUntil([this] {
                    for (std::size_t i = 0; i < endpoint_count_; ++i) {
                        if (stack_.paused(i)) {
                            return false;
                        }
                    }
                    return shared_->imports->entries() == 0 &&
                           (!shared_->segments || shared_->segments->entries() == 0);
                }));
            }
            Close();
        }
        for (int fd : peer_fds_) {
            ::close(fd);
        }
        active_ = nullptr;
    }
    control::ImportLease AcquireImport(std::size_t context = 0) {
        urma_rjetty_t descriptor{};
        descriptor.jetty_id.id = 71;
        descriptor.jetty_id.eid.raw[15] = 9;
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        control::EndpointImportKey key{.local_context = &contexts_[context],
                                       .local_context_incarnation = 101 + context,
                                       .peer_runtime = 20,
                                       .receive_domain = 21,
                                       .endpoint_generation = 22,
                                       .jetty_id = 71,
                                       .authorization_identity = 23};
        std::memcpy(key.eid.data(), &descriptor.jetty_id.eid, key.eid.size());
        const auto acquired = shared_->imports->Acquire({key, std::as_bytes(std::span(&descriptor, 1)), {}});
        EXPECT_TRUE(acquired);
        if (!acquired) {
            return {};
        }
        import_leases_.push_back(*acquired);
        return *acquired;
    }
    control::TsegImportLease AcquireSegment(std::size_t context = 0) {
        control::TsegImportRequest request;
        request.key = {.local_context = &contexts_[context],
                       .local_context_incarnation = 101 + context,
                       .peer_runtime = 20,
                       .receive_domain = 21,
                       .slab_id = 22,
                       .slab_generation = 1,
                       .authorization_identity = 23};
        request.key.eid[15] = 9;
        std::memcpy(&request.descriptor.ubva.eid, request.key.eid.data(), request.key.eid.size());
        request.descriptor.ubva.va = 0x10000;
        request.descriptor.len = 4096;
        request.descriptor.attr.bs.access = URMA_ACCESS_WRITE;
        const auto acquired = shared_->segments->Acquire(request);
        EXPECT_TRUE(acquired);
        if (!acquired) {
            return {};
        }
        segment_leases_.push_back(*acquired);
        return *acquired;
    }
    OwnedConnection AdoptTcp(std::size_t context = 0) {
        const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
        EXPECT_GE(listener, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        EXPECT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
        EXPECT_EQ(::listen(listener, 1), 0);
        socklen_t length = sizeof(address);
        EXPECT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length), 0);
        const int peer = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
        EXPECT_GE(peer, 0);
        peer_fds_.push_back(peer);
        EXPECT_EQ(::connect(peer, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
        const int fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        EXPECT_GE(fd, 0);
        ::close(listener);
        const auto adopted = resources_[context]->connections->AdoptTcp(fd);
        EXPECT_TRUE(adopted);
        if (!adopted) {
            ::close(fd);
            return {};
        }
        return *adopted;
    }
    connection::EagerReceiveSource Peer(std::size_t index) {
        auto& domain = resources_[index]->domain;
        const auto peer = domain.credits->Admit({22, 30 + index, 77});
        EXPECT_TRUE(peer);
        if (!peer) {
            return {};
        }
        EXPECT_TRUE(domain.credits->Grant(*peer, connection::CreditClass::kData, 6));
        EXPECT_TRUE(domain.credits->Grant(*peer, connection::CreditClass::kControl, 2));
        return {{11, 30 + index}, {connection::kWireVersion, 22, 30 + index, 44}, *peer};
    }

    raw::test_support::ScopedUrmaOverride override_{Functions()};
    std::array<urma_device_t, 2> devices_{};
    std::array<urma_device_t*, 2> device_ptrs_{};
    std::array<std::array<urma_eid_info_t, 2>, 2> eids_{};
    std::array<urma_context_t, 4> contexts_{};
    std::array<std::array<urma_jfc_t, 2>, 4> jfcs_{};
    std::array<urma_jfr_t, 4> jfrs_{};
    std::array<std::array<urma_jetty_t, 2>, 4> jetties_{};
    std::array<urma_target_seg_t, 16> segments_{};
    std::array<urma_target_seg_t, 8> imported_segments_{};
    std::array<urma_target_jetty_t, 8> targets_{};
    std::array<std::deque<Posted>, 4> posted_;
    std::array<std::deque<urma_cr_t>, 4> completions_;
    std::array<std::size_t, 4> jfc_created_{}, jetty_created_{}, polls_{}, post_calls_{};
    std::array<RawEndpointConfig, 4> endpoints_;
    RawStackConfig config_;
    RawStack stack_;
    std::array<std::optional<RawStackContextResources>, 4> resources_;
    std::optional<RawStackSharedResources> shared_;
    std::array<raw::ReceiveDomainQuiescence, 4> proofs_{};
    std::size_t endpoint_count_ = 4;
    std::vector<Deletion> deletions_;
    std::vector<int> peer_fds_;
    std::vector<control::ImportLease> import_leases_;
    std::vector<control::TsegImportLease> segment_leases_;
    std::size_t init_calls_ = 0, uninit_calls_ = 0, query_calls_ = 0, context_create_calls_ = 0;
    std::size_t context_delete_calls_ = 0, register_calls_ = 0, unregister_calls_ = 0, jfr_delete_calls_ = 0;
    std::size_t fail_register_call_ = 0, fail_context_create_ = 0, fail_unregister_call_ = 0;
    std::optional<std::size_t> fail_jfc_context_, fail_post_context_, fail_jfr_delete_, fail_context_delete_;
    std::optional<std::size_t> failed_cq_context_;
    urma_status_t uninit_status_ = URMA_SUCCESS;
    std::atomic<std::size_t> import_calls_{0}, unimport_calls_{0};
    std::atomic<std::size_t> segment_import_calls_{0}, segment_unimport_calls_{0};
    std::atomic<bool> block_import_{false};
    std::atomic<bool> block_segment_import_{false};
};

TEST_F(RawStackTest, OwnsTwoDevicesTwoEidsWithDistinctDomainsAndSharedSlab) {
    Open();
    const auto snapshot = stack_.Inspect();
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->stage, RawStackStage::kReady);
    EXPECT_EQ(snapshot->context_count, 4u);
    EXPECT_TRUE(snapshot->progress_attached);
    EXPECT_FALSE(snapshot->rndv_enabled);
    EXPECT_EQ(shared_->segments, nullptr);
    EXPECT_EQ(shared_->rndv_target, nullptr);
    EXPECT_EQ(snapshot->rndv_target.buffers, 0u);
    EXPECT_EQ(init_calls_, 1u);
    EXPECT_EQ(query_calls_, 2u);
    EXPECT_EQ(context_create_calls_, 4u);
    // 四份 RX MR 加一个共享 backing 在四个 context 注册；预算仅扣一次 Slab backing。
    EXPECT_EQ(register_calls_, 8u);
    EXPECT_EQ(snapshot->budget_bytes, 32768u);
    EXPECT_EQ(snapshot->slab.buffers, 8u);
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(resources_[i]->context, &contexts_[i]);
        EXPECT_EQ(resources_[i]->domain.credits->receive_domain(), 30 + i);
        EXPECT_EQ(snapshot->contexts[i].domain.posted, 8u);
        EXPECT_EQ(snapshot->contexts[i].domain.credits.physical_posted, 8u);
        if (i) {
            EXPECT_NE(resources_[i]->domain.pool, resources_[0]->domain.pool);
            EXPECT_NE(resources_[i]->connections, resources_[0]->connections);
            EXPECT_NE(resources_[i]->commands, resources_[0]->commands);
        }
    }
    ASSERT_TRUE(stack_.Tick());
    for (auto count : polls_) {
        EXPECT_GT(count, 0u);
    }
    EXPECT_FALSE(stack_.resources(4));
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
}

TEST_F(RawStackTest, ClosesAllDmaDependenciesBeforeContextsAndUninit) {
    Open();
    Close();
    EXPECT_EQ(Deleted(Kind::kJetty), 8u);
    EXPECT_EQ(Deleted(Kind::kJfr), 4u);
    EXPECT_EQ(Deleted(Kind::kJfc), 8u);
    EXPECT_EQ(Deleted(Kind::kMr), 8u);
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
    EXPECT_EQ(uninit_calls_, 1u);
    // 全部 context 共享 Slab，任一 context 都不能先于所有共享 MR 的释放。
    const auto first_context =
        std::ranges::find_if(deletions_, [](const auto& item) { return item.kind == Kind::kContext; });
    ASSERT_NE(first_context, deletions_.end());
    EXPECT_EQ(
        std::ranges::count_if(first_context, deletions_.end(), [](const auto& item) { return item.kind == Kind::kMr; }),
        0);
    std::vector<std::size_t> context_order;
    for (const auto& item : deletions_) {
        if (item.kind == Kind::kContext) {
            context_order.push_back(item.context);
        }
    }
    EXPECT_EQ(context_order, (std::vector<std::size_t>{3, 2, 1, 0}));
    EXPECT_EQ(deletions_.back().kind, Kind::kUninit);
    EXPECT_FALSE(stack_.resources(0));
    EXPECT_FALSE(stack_.shared());
    EXPECT_FALSE(stack_.Tick());
    ASSERT_TRUE(stack_.Inspect());
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 0u);
    EXPECT_TRUE(stack_.AdvanceClose({}));
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
}

TEST_F(RawStackTest, RejectsEmptyAndTooManyContextsBeforeProviderInitialization) {
    EXPECT_FALSE(stack_.Open({}, config_));
    std::array<RawEndpointConfig, 5> too_many;
    EXPECT_FALSE(stack_.Open(too_many, config_));
    EXPECT_EQ(stack_.stage(), RawStackStage::kEmpty);
    EXPECT_EQ(init_calls_, 0u);
}

TEST_F(RawStackTest, RejectsDuplicateReceiveDomainIdentityBeforeProviderInitialization) {
    endpoints_[1].domain.local_domain = endpoints_[0].domain.local_domain;
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
    EXPECT_EQ(init_calls_, 0u);
}

TEST_F(RawStackTest, RejectsDuplicateContextIncarnationBeforeProviderInitialization) {
    endpoints_[1].domain.context_incarnation = endpoints_[0].domain.context_incarnation;
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
    EXPECT_EQ(init_calls_, 0u);
}

TEST_F(RawStackTest, RejectsDuplicateEndpointAndMissingEventQueuesBeforeSessionAllocation) {
    // 配置拒绝发生在创建 State/init 前；修正配置后同一对象仍可首次打开。
    endpoints_[1].device = endpoints_[0].device;
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
    EXPECT_EQ(stack_.stage(), RawStackStage::kEmpty);
    EXPECT_EQ(init_calls_, 0u);
    endpoints_[1].device = {devices_[0].name, 72};
    config_.runtime.reactor.events = true;
    for (auto& endpoint : endpoints_) {
        endpoint.domain.queues.events = true;
    }
    endpoints_[2].domain.queues.events = false;
    const auto invalid = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().context_index, 2u);
    EXPECT_EQ(stack_.stage(), RawStackStage::kEmpty);
    EXPECT_EQ(init_calls_, 0u);
    config_.runtime.reactor.events = false;
    for (auto& endpoint : endpoints_) {
        endpoint.domain.queues.events = false;
    }
    Open();
}

TEST_F(RawStackTest, SessionCreationRollbackIsNotRepeatedByExplicitStackClose) {
    // 第三个 context 创建失败，RawRuntime 已回滚前两个；栈仍须显式关闭，但不得再次销毁它们。
    fail_context_create_ = 3;
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(stack_.stage(), RawStackStage::kOpenFailed);
    EXPECT_FALSE(stack_.resources(0));
    EXPECT_FALSE(stack_.shared());
    EXPECT_EQ(context_create_calls_, 3u);
    EXPECT_EQ(context_delete_calls_, 2u);
    EXPECT_EQ(uninit_calls_, 1u);
    EXPECT_EQ(register_calls_, 0u);
    Close();
    EXPECT_EQ(context_delete_calls_, 2u);
    EXPECT_EQ(uninit_calls_, 1u);
}

TEST_F(RawStackTest, BeginCloseStopsAdmissionWithoutDetachingProgress) {
    Open();
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_EQ(stack_.stage(), RawStackStage::kStopping);
    EXPECT_FALSE(shared_->slab->Acquire(1));
    for (auto& resources : resources_) {
        EXPECT_FALSE(resources->connections->stats().accepting);
        EXPECT_FALSE(resources->commands->accepting());
        EXPECT_TRUE(resources->domain.credits->stats().admission_stopped);
        EXPECT_FALSE(resources->domain.receive->frozen());
        EXPECT_FALSE(resources->domain.pool->Reserve());
    }
    ExpectStillRunning();
}

TEST_F(RawStackTest, RequiresMailboxClientProofEvenWhenAllQueuesAreEmpty) {
    Open();
    ASSERT_TRUE(stack_.BeginClose());
    auto proof = Proof();
    proof.clients_stopped = false;
    EXPECT_FALSE(stack_.AdvanceClose(proof));
    ExpectStillRunning();
}

TEST_F(RawStackTest, RequiresOneIndependentProofPerConfiguredContext) {
    Open();
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose({true, std::span(proofs_).first(3)}));
    ExpectStillRunning();
    // 一个域缺少任一门禁时，全栈不得先拆掉其他域的进度或物理对象。
    for (std::size_t missing = 0; missing < 4; ++missing) {
        proofs_[2] = {true, true, true, true};
        switch (missing) {
        case 0:
            proofs_[2].peer_stopped = false;
            break;
        case 1:
            proofs_[2].events_acked = false;
            break;
        case 2:
            proofs_[2].queue_users_stopped = false;
            break;
        case 3:
            proofs_[2].completion_queues_healthy = false;
            break;
        }
        EXPECT_FALSE(stack_.AdvanceClose(Proof()));
        ExpectStillRunning();
    }
    proofs_[2] = {true, true, true, true};
}

TEST_F(RawStackTest, SlabCpuDmaAndGrantEachPreventRuntimeDetach) {
    Open();
    auto& slab = *shared_->slab;
    const auto lease = slab.Acquire(64);
    ASSERT_TRUE(lease);
    const auto dma = slab.Reserve(*lease, memory::BufferUse::kLocalDma);
    const auto grant = slab.Reserve(*lease, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(slab.Commit(*dma));
    ASSERT_TRUE(slab.Commit(*grant));
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(slab.ReleaseCpu(*lease));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ASSERT_TRUE(slab.CompleteDma(*dma));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(slab.EndGrant(*grant));
    Close();
}

TEST_F(RawStackTest, ConnectionSlotMustBeClosedAndReleasedBeforeRuntimeDetach) {
    Open();
    const auto connection = AdoptTcp(2);
    ASSERT_TRUE(connection.owner);
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(resources_[2]->connections->CloseConnection(connection));
    // 连接 Closed 不代表 owner 槽/调度引用已经退出。
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ASSERT_TRUE(resources_[2]->connections->Release(connection));
    Close();
}

TEST_F(RawStackTest, CompletedButUncollectedCommandKeepsMailboxAndRuntimeAlive) {
    Open();
    const auto connection = AdoptTcp();
    const auto ticket =
        resources_[0]->commands->TrySubmit({.kind = ConnectionCommandKind::kInspect, .connection = connection});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ASSERT_TRUE(stack_.Tick());
    ASSERT_TRUE(resources_[0]->connections->CloseConnection(connection));
    ASSERT_TRUE(resources_[0]->connections->Release(connection));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    const auto completion = resources_[0]->commands->TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    EXPECT_TRUE(completion->result);
    Close();
}

TEST_F(RawStackTest, LiveRouteInOneContextBlocksDeletionOfEveryContext) {
    Open();
    const auto route = resources_[1]->domain.routes->Reserve();
    ASSERT_TRUE(route);
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(resources_[1]->domain.routes->Retire(*route));
    Close();
}

TEST_F(RawStackTest, ReservedRawTicketBlocksDetachEvenWithEmptyAttemptLedger) {
    Open();
    const auto ticket = resources_[3]->domain.pool->Reserve();
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(resources_[3]->domain.pool->Cancel(*ticket));
    Close();
}

TEST_F(RawStackTest, EmptyErrorLaneRequiresFlushBoundaryBeforeDetachment) {
    Open();
    auto& pool = *resources_[1]->domain.pool;
    const auto lane = pool.lane(0);
    ASSERT_TRUE(lane);
    ASSERT_TRUE(pool.BeginDrain(*lane));
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    ASSERT_TRUE(pool.ObserveFlushDone(*lane));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    urma_cr_t completion{};
    ASSERT_EQ(pool.FlushSend(*lane, std::span(&completion, 1)), 0);
    Close();
}

TEST_F(RawStackTest, PartialDomainCreationNeverPublishesSuccessfulEarlierContexts) {
    fail_post_context_ = 2;
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(stack_.stage(), RawStackStage::kOpenFailed);
    EXPECT_FALSE(stack_.resources(0));
    EXPECT_FALSE(stack_.resources(2));
    EXPECT_FALSE(stack_.shared());
    const auto snapshot = stack_.Inspect();
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->contexts[2].domain.initial_accepted, 3u);
    EXPECT_EQ(snapshot->contexts[2].domain.posted, 3u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    Close();
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
    EXPECT_EQ(uninit_calls_, 1u);
}

TEST_F(RawStackTest, FailedQueueCreationPreservesEarlierDomainsUntilExplicitClose) {
    fail_jfc_context_ = 1;
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
    EXPECT_FALSE(stack_.resources(0));
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    EXPECT_EQ(Deleted(Kind::kJfr), 0u);
    Close();
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
}

TEST_F(RawStackTest, SlabRegistrationFailureStillAllowsExplicitStackClose) {
    fail_register_call_ = 2;
    EXPECT_FALSE(stack_.Open(endpoints_, config_));
    EXPECT_EQ(stack_.stage(), RawStackStage::kOpenFailed);
    EXPECT_FALSE(stack_.shared());
    Close();
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
    EXPECT_EQ(uninit_calls_, 1u);
}

TEST_F(RawStackTest, DomainDeleteFailureRetainsSharedSlabAndAllContexts) {
    Open();
    fail_jfr_delete_ = 3;
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    EXPECT_EQ(uninit_calls_, 0u);
    ASSERT_TRUE(stack_.Inspect());
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 32768u);
    EXPECT_FALSE(stack_.Tick());
    fail_jfr_delete_.reset();
    Close();
    EXPECT_EQ(jfr_delete_calls_, 5u);
}

TEST_F(RawStackTest, SharedMemoryUnregisterFailureRetriesBeforeContextDeletion) {
    Open();
    // 先注销四份 RX，随后共享 Slab 的第一个逆序注销失败，context 必须全部保留。
    fail_unregister_call_ = 5;
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    EXPECT_EQ(Deleted(Kind::kJfr), 4u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    EXPECT_EQ(stack_.stage(), RawStackStage::kClosingMemory);
    fail_unregister_call_ = 0;
    Close();
    EXPECT_EQ(unregister_calls_, 9u);
}

TEST_F(RawStackTest, ContextDeleteRetryDoesNotRepeatDmaTeardownOrUninitEarly) {
    Open();
    fail_context_delete_ = 2;
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    EXPECT_EQ(stack_.stage(), RawStackStage::kClosingContexts);
    EXPECT_EQ(Deleted(Kind::kMr), 8u);
    EXPECT_EQ(Deleted(Kind::kContext), 1u);
    EXPECT_EQ(uninit_calls_, 0u);
    fail_context_delete_.reset();
    Close();
    EXPECT_EQ(context_delete_calls_, 5u);
    EXPECT_EQ(unregister_calls_, 8u);
    EXPECT_EQ(uninit_calls_, 1u);
}

TEST_F(RawStackTest, UninitErrorIsReportedWithoutRepeatingProviderUninit) {
    Open();
    uninit_status_ = static_cast<urma_status_t>(EIO);
    ASSERT_TRUE(stack_.BeginClose());
    const auto result = stack_.AdvanceClose(Proof());
    ASSERT_FALSE(result);
    ASSERT_TRUE(result.error().session_close);
    EXPECT_EQ(result.error().session_close->code, raw::RuntimeCloseErrorCode::kUninitFailed);
    EXPECT_EQ(uninit_calls_, 1u);
    uninit_status_ = URMA_SUCCESS;
    Close();
    EXPECT_EQ(uninit_calls_, 1u);
}

TEST_F(RawStackTest, ShutdownKeepsRuntimeAliveToUnimportWarmEntries) {
    Open();
    const auto lease = AcquireImport(2);
    ASSERT_TRUE(lease.owner);
    ASSERT_TRUE(SpinUntil([&] { return !stack_.paused(2) && shared_->imports->Get(lease).has_value(); }));
    ASSERT_TRUE(shared_->imports->Release(lease));
    ASSERT_EQ(shared_->imports->entries(), 1u);
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    // 禁止新引用后，仍须通过同一 Runtime 控制执行器 unimport，不能先 StopAccepting reactor。
    ASSERT_TRUE(SpinUntil([&] { return !stack_.paused(2) && shared_->imports->entries() == 0; }));
    EXPECT_EQ(unimport_calls_.load(), 1u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    Close();
}

TEST_F(RawStackTest, InflightImportPauseRejectsGlobalInspectionAndShutdownMutation) {
    Open();
    block_import_.store(true);
    const auto lease = AcquireImport(1);
    ASSERT_TRUE(lease.owner);
    ASSERT_TRUE(SpinUntil([&] { return stack_.paused(1); }));
    EXPECT_FALSE(stack_.Inspect());
    EXPECT_FALSE(stack_.shared());
    EXPECT_FALSE(stack_.resources(1));
    EXPECT_TRUE(stack_.resources(0));
    EXPECT_FALSE(stack_.BeginClose());
    EXPECT_EQ(stack_.stage(), RawStackStage::kReady);
    // 全局关闭检查不能先停止未暂停的域，须保持事务前的准入状态。
    EXPECT_TRUE(resources_[0]->connections->stats().accepting);
    block_import_.store(false);
    block_import_.notify_all();
    ASSERT_TRUE(SpinUntil([&] { return !stack_.paused(1) && shared_->imports->Get(lease).has_value(); }));
    ASSERT_TRUE(shared_->imports->Release(lease));
}

TEST_F(RawStackTest, SegmentAndEndpointImportsShareOneContextPause) {
    config_.enable_rndv = true;
    Open();
    ASSERT_NE(shared_->segments, nullptr);
    block_import_.store(true);
    block_segment_import_.store(true);
    const auto endpoint = AcquireImport(1);
    const auto segment = AcquireSegment(1);
    ASSERT_TRUE(endpoint.owner);
    ASSERT_TRUE(segment.owner);
    ASSERT_TRUE(SpinUntil([&] { return stack_.paused(1) && import_calls_.load() == 1; }));
    // 同 context 的两个慢导入不可并发；暂停还须覆盖 raw poll，其他 context 继续工作。
    const auto before = polls_;
    for (int i = 0; i < 8; ++i) {
        ASSERT_TRUE(stack_.Tick());
    }
    EXPECT_EQ(segment_import_calls_.load(), 0u);
    EXPECT_EQ(polls_[1], before[1]);
    EXPECT_GT(polls_[0], before[0]);
    EXPECT_FALSE(stack_.Inspect());
    EXPECT_FALSE(stack_.shared());
    EXPECT_FALSE(stack_.BeginClose());
    block_import_.store(false);
    block_import_.notify_all();
    ASSERT_TRUE(SpinUntil([&] { return stack_.paused(1) && segment_import_calls_.load() == 1; }));
    const auto segment_pause = polls_[1];
    for (int i = 0; i < 8; ++i) {
        ASSERT_TRUE(stack_.Tick());
    }
    EXPECT_EQ(polls_[1], segment_pause);
    EXPECT_FALSE(stack_.resources(1));
    EXPECT_TRUE(stack_.resources(0));
    block_segment_import_.store(false);
    block_segment_import_.notify_all();
    ASSERT_TRUE(SpinUntil([&] {
        return !stack_.paused(1) && shared_->imports->Get(endpoint).has_value() &&
               shared_->segments->Get(segment).has_value();
    }));
    const auto snapshot = stack_.Inspect();
    ASSERT_TRUE(snapshot);
    EXPECT_TRUE(snapshot->rndv_enabled);
    EXPECT_EQ(snapshot->segment_entries, 1u);
    EXPECT_EQ(snapshot->segment_leases, 1u);
    EXPECT_EQ(snapshot->segment_tasks, 0u);
    EXPECT_EQ(shared_->segments->Get(segment).value()->urma_ctx, &contexts_[1]);
    EXPECT_TRUE(shared_->imports->Release(endpoint));
    EXPECT_TRUE(shared_->segments->Release(segment));
}

TEST_F(RawStackTest, OptionalTargetPoolRequiresRndvBeforeProviderInitialization) {
    config_.rndv_target_slab = memory::SlabConfig{2, 4096, 8};
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, RawStackErrorCode::kInvalidArgument);
    EXPECT_EQ(stack_.stage(), RawStackStage::kEmpty);
    EXPECT_EQ(init_calls_, 0u);
}

TEST_F(RawStackTest, TargetBackingSharesOneBudgetAndRegistersEveryContext) {
    config_.enable_rndv = true;
    config_.rndv_target_slab = memory::SlabConfig{2, 4096, 8};
    config_.memory_budget_bytes += 8192;
    Open();
    ASSERT_NE(shared_->rndv_target, nullptr);
    EXPECT_NE(shared_->rndv_target, shared_->slab);
    const auto snapshot = stack_.Inspect();
    ASSERT_TRUE(snapshot);
    // 源池、目标池各只有一份 backing；四 context 的注册不重复扣预算。
    EXPECT_EQ(snapshot->budget_bytes, 40960u);
    EXPECT_EQ(snapshot->slab.backing_bytes, 32768u);
    EXPECT_EQ(snapshot->rndv_target.backing_bytes, 8192u);
    EXPECT_EQ(snapshot->rndv_target.buffers, 2u);
    EXPECT_EQ(register_calls_, 12u);
    for (std::size_t i = 0; i < contexts_.size(); ++i) {
        EXPECT_EQ(segments_[4 + i].urma_ctx, &contexts_[i]);
        EXPECT_EQ(segments_[4 + i].seg.ubva.va, segments_[4].seg.ubva.va);
    }
    Close();
    EXPECT_EQ(Deleted(Kind::kMr), 12u);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 0u);
    EXPECT_EQ(stack_.Inspect()->rndv_target.backing_bytes, 0u);
    // 目标和源 MR 都必须先于任何 context 删除。
    const auto first_context =
        std::ranges::find_if(deletions_, [](const auto& item) { return item.kind == Kind::kContext; });
    ASSERT_NE(first_context, deletions_.end());
    EXPECT_EQ(
        std::ranges::count_if(first_context, deletions_.end(), [](const auto& item) { return item.kind == Kind::kMr; }),
        0);
}

TEST_F(RawStackTest, TargetAllocationCannotExceedSharedSourceBudget) {
    config_.enable_rndv = true;
    config_.rndv_target_slab = memory::SlabConfig{2, 4096, 8};
    // 源池已占满预算，目标池不能偷偷申请另一份独立预算。
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    ASSERT_TRUE(opened.error().memory);
    EXPECT_EQ(opened.error().memory->code, memory::MemoryErrorCode::kExhausted);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 32768u);
    EXPECT_EQ(register_calls_, 4u);
    EXPECT_FALSE(stack_.shared());
    Close();
    EXPECT_EQ(Deleted(Kind::kMr), 4u);
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 0u);
}

TEST_F(RawStackTest, PartialTargetRegistrationFailureKeepsExplicitClosePath) {
    config_.enable_rndv = true;
    config_.rndv_target_slab = memory::SlabConfig{2, 4096, 8};
    config_.memory_budget_bytes += 8192;
    // 四次源注册后，目标第二 context 注册失败，逆序回滚也首次失败。
    // 整份目标 backing 与预算必须留到显式关闭，不可先删 context。
    fail_register_call_ = 6;
    fail_unregister_call_ = 1;
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    ASSERT_TRUE(opened.error().memory);
    EXPECT_EQ(opened.error().memory->code, memory::MemoryErrorCode::kRegisterFailed);
    EXPECT_EQ(opened.error().memory->cleanup_error, URMA_EAGAIN);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 40960u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    EXPECT_FALSE(stack_.shared());
    fail_unregister_call_ = 0;
    Close();
    EXPECT_EQ(Deleted(Kind::kMr), 5u);
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 0u);
}

TEST_F(RawStackTest, TargetCpuDmaAndRemoteGrantIndependentlyPreventDetach) {
    config_.enable_rndv = true;
    config_.rndv_target_slab = memory::SlabConfig{2, 4096, 8};
    config_.memory_budget_bytes += 8192;
    Open();
    auto& target = *shared_->rndv_target;
    const auto lease = target.Acquire(64);
    ASSERT_TRUE(lease);
    const auto dma = target.Reserve(*lease, memory::BufferUse::kLocalDma);
    const auto grant = target.Reserve(*lease, memory::BufferUse::kRemoteGrant);
    ASSERT_TRUE(dma);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(target.Commit(*dma));
    ASSERT_TRUE(target.Commit(*grant));
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(target.Acquire(64));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    // CPU 归还和本地 CQE 都不能代替远端 Grant 的独立终结。
    ASSERT_TRUE(target.ReleaseCpu(*lease));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ASSERT_TRUE(target.CompleteDma(*dma));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(target.EndGrant(*grant));
    Close();
}

TEST_F(RawStackTest, TargetUnregisterFailureKeepsSourceAndAllContextsUntilRetry) {
    config_.enable_rndv = true;
    config_.rndv_target_slab = memory::SlabConfig{2, 4096, 8};
    config_.memory_budget_bytes += 8192;
    Open();
    // 四份 RX MR 已关闭；目标 MR 的首次注销失败，源池尚不得注销。
    fail_unregister_call_ = 5;
    ASSERT_TRUE(stack_.BeginClose());
    const auto closed = stack_.AdvanceClose(Proof());
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, RawStackErrorCode::kMemory);
    EXPECT_EQ(stack_.stage(), RawStackStage::kClosingMemory);
    EXPECT_EQ(Deleted(Kind::kMr), 4u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 40960u);
    fail_unregister_call_ = 0;
    Close();
    EXPECT_EQ(unregister_calls_, 13u);
    EXPECT_EQ(Deleted(Kind::kMr), 12u);
    EXPECT_EQ(stack_.Inspect()->budget_bytes, 0u);
}

TEST_F(RawStackTest, EndpointCapacityCannotExceedStackCeiling) {
    endpoints_[2].connection_capacity = config_.connections_per_context + 1;
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, RawStackErrorCode::kInvalidArgument);
    EXPECT_EQ(opened.error().context_index, 2u);
    EXPECT_EQ(init_calls_, 0u);
}

TEST_F(RawStackTest, EndpointCapacityBindsOwnerAndSchedulerUntilConnectionRelease) {
    endpoints_[0].connection_capacity = 1;
    Open();
    auto& owner = *resources_[0]->connections;
    EXPECT_EQ(owner.stats().capacity, 1u);
    EXPECT_EQ(owner.scheduler_capacity(), 1u);
    EXPECT_EQ(resources_[1]->connections->stats().capacity, 4u);
    const auto first = AdoptTcp();
    ASSERT_TRUE(first.owner);
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    ASSERT_GE(fd, 0);
    const auto full = owner.AdoptTcp(fd);
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, ConnectionOwnerErrorCode::kExhausted);
    ASSERT_TRUE(owner.CloseConnection(first));
    // Closed 仍占元数据槽；显式 Release 才恢复配额，也不因直接借用 Owner 绕过。
    const auto still_full = owner.AdoptTcp(fd);
    ASSERT_FALSE(still_full);
    EXPECT_EQ(still_full.error().code, ConnectionOwnerErrorCode::kExhausted);
    EXPECT_EQ(::close(fd), 0);
    ASSERT_TRUE(owner.Release(first));
    const auto second = AdoptTcp();
    ASSERT_TRUE(second.owner);
    EXPECT_EQ(second.slot, first.slot);
    EXPECT_NE(second.generation, first.generation);
    ASSERT_TRUE(owner.CloseConnection(second));
    ASSERT_TRUE(owner.Release(second));
}

TEST_F(RawStackTest, SegmentDmaPinAndDeferredUnimportPreventRuntimeDetach) {
    config_.enable_rndv = true;
    Open();
    const auto segment = AcquireSegment(2);
    ASSERT_TRUE(segment.owner);
    ASSERT_TRUE(SpinUntil([&] { return !stack_.paused(2) && shared_->segments->Get(segment).has_value(); }));
    const auto pin = shared_->segments->Retain(segment);
    ASSERT_TRUE(pin);
    segment_leases_.push_back(*pin);
    ASSERT_TRUE(shared_->segments->Release(segment));
    ASSERT_TRUE(stack_.BeginClose());
    // 业务引用已归还但 DMA pin 尚在；失效只是停止提交，不能撤销底层导入或停止 Runtime。
    EXPECT_FALSE(shared_->segments->Get(*pin));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    EXPECT_EQ(segment_unimport_calls_.load(), 0u);
    ASSERT_TRUE(shared_->segments->Release(*pin));
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ASSERT_TRUE(SpinUntil([&] { return !stack_.paused(2) && shared_->segments->entries() == 0; }));
    EXPECT_EQ(segment_unimport_calls_.load(), 1u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    Close();
    const auto snapshot = stack_.Inspect();
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->segment_entries, 0u);
    EXPECT_EQ(snapshot->segment_leases, 0u);
    EXPECT_EQ(snapshot->segment_tasks, 0u);
}

TEST_F(RawStackTest, InvalidOptInSegmentCacheConfigPreservesExplicitClosePath) {
    config_.enable_rndv = true;
    config_.segments.entries = 0;
    const auto opened = stack_.Open(endpoints_, config_);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, RawStackErrorCode::kImport);
    ASSERT_TRUE(opened.error().segment);
    EXPECT_EQ(opened.error().segment->code, control::TsegImportCacheErrorCode::kInvalidArgument);
    EXPECT_FALSE(stack_.shared());
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    Close();
    EXPECT_EQ(Deleted(Kind::kContext), 4u);
}

TEST_F(RawStackTest, PhysicalSourceMustBeRemovedEvenAfterItsLogicalRouteRetires) {
    Open();
    auto& domain = resources_[1]->domain;
    const auto source = Peer(1);
    const auto route = domain.routes->Reserve();
    ASSERT_TRUE(route);
    ASSERT_TRUE(domain.routes->Publish(*route, {source.peer_context, route->token, 55}, this));
    RxSourceRoute physical;
    physical.local_id = jetties_[1][0].jetty_id.id;
    physical.remote_id.id = 71;
    physical.remote_id.eid.raw[0] = 4;
    physical.token = route->token;
    physical.source = source;
    const auto registered = domain.sources->Register(physical);
    ASSERT_TRUE(registered);
    ASSERT_TRUE(domain.routes->Retire(*route));
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(domain.sources->Remove(*registered, {102, true, true, true}));
    Close();
}

TEST_F(RawStackTest, OutstandingPeerWithdrawalBlocksEveryContextUntilExplicitAbort) {
    Open();
    const auto source = Peer(2);
    connection::SenderCreditAccount sender;
    ASSERT_TRUE(sender.Open({22, 66, 88}));
    ASSERT_TRUE(sender.Update({22, 66, 88}, 6, 2));
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
    peer_fds_.push_back(sockets[1]);
    const control::HandshakeIdentity local{11, 32, 1, 1, 103, {0, 1}};
    const control::HandshakeIdentity remote{22, 66, 2, 1, 102, {0, 1}};
    const auto withdrawal = resources_[2]->withdrawals->BeginWithdrawal(
        sockets[0], {local, remote, 77, 88, &sender, resources_[2]->domain.credits, source.peer, 10000, true}, 0);
    ASSERT_TRUE(withdrawal);
    ASSERT_TRUE(stack_.BeginClose());
    EXPECT_FALSE(stack_.AdvanceClose(Proof()));
    ExpectStillRunning();
    ASSERT_TRUE(resources_[2]->withdrawals->Abort(*withdrawal));
    Close();
}

TEST_F(RawStackTest, ApplicationHeldRxSurvivesJfrDeletionUntilExplicitRelease) {
    Open();
    auto& domain = resources_[3]->domain;
    const auto source = Peer(3);
    const auto route = domain.routes->Reserve();
    ASSERT_TRUE(route);
    std::optional<connection::EagerRxPacket> held;
    ASSERT_TRUE(domain.routes->Publish(*route, {source.peer_context, route->token, 55}, &held));
    ASSERT_FALSE(posted_[3].empty());
    const auto posted = posted_[3].front();
    posted_[3].pop_front();
    const std::array data{std::byte{0x42}};
    connection::WireFrameInput frame;
    frame.token = route->token;
    frame.data = data;
    const auto encoded = connection::EncodeWireFrame(frame, posted.memory);
    ASSERT_TRUE(encoded);
    urma_cr_t completion{};
    completion.flag.bs.s_r = 1;
    completion.flag.bs.jetty = 1;
    completion.local_id = jetties_[3][0].jetty_id.id;
    completion.user_ctx = posted.id;
    completion.completion_len = encoded->payload_bytes;
    completion.opcode = URMA_CR_OPC_SEND_WITH_IMM;
    completion.imm_data = encoded->imm_data;
    completions_[3].push_back(completion);
    raw::RxBufferEvent event;
    const auto polled = domain.rx->Poll(std::span(&event, 1));
    ASSERT_TRUE(polled);
    ASSERT_EQ(*polled, 1u);
    ASSERT_TRUE(domain.receive->Dispatch(
        event, source,
        [](void* cookie, connection::EagerReceiveDomain&, const connection::WireFrameView&,
           connection::EagerRxPacket packet,
           std::uint64_t) noexcept -> std::expected<connection::EagerRxDisposition, connection::EagerRxError> {
            *static_cast<std::optional<connection::EagerRxPacket>*>(cookie) = packet;
            return connection::EagerRxDisposition::kHeld;
        },
        1));
    ASSERT_TRUE(held);
    ASSERT_TRUE(domain.routes->Retire(*route));
    ASSERT_TRUE(stack_.BeginClose());
    const auto waiting = stack_.AdvanceClose(Proof());
    ASSERT_TRUE(waiting);
    EXPECT_EQ(waiting->stage, RawStackStage::kClosingDomains);
    EXPECT_FALSE(waiting->progress_attached);
    EXPECT_EQ(waiting->contexts[3].domain.held, 1u);
    EXPECT_EQ(waiting->contexts[3].domain.close_stage, raw::ReceiveDomainCloseStage::kWaitingForLeases);
    EXPECT_EQ(Deleted(Kind::kJfr), 1u);
    EXPECT_EQ(Deleted(Kind::kMr), 0u);
    EXPECT_EQ(Deleted(Kind::kContext), 0u);
    ASSERT_TRUE(domain.receive->Release(*held));
    Close();
}

TEST_F(RawStackTest, FaultedCqRetainsDmaDependenciesWithoutContextDeletion) {
    // 故障 CQ 无终结证据时永久保留；隔离进程，避免占用后续测试的 provider 全局会话。
    ASSERT_EXIT(
        {
            Open();
            failed_cq_context_ = 3;
            EXPECT_TRUE(stack_.BeginClose());
            EXPECT_FALSE(stack_.AdvanceClose(Proof()));
            EXPECT_EQ(stack_.stage(), RawStackStage::kRetained);
            EXPECT_EQ(stack_.Inspect()->contexts[3].domain.stage, RawDomainStage::kRetained);
            EXPECT_EQ(Deleted(Kind::kJfr), 0u);
            EXPECT_EQ(Deleted(Kind::kMr), 0u);
            EXPECT_EQ(Deleted(Kind::kContext), 0u);
            EXPECT_EQ(uninit_calls_, 0u);
            EXPECT_FALSE(stack_.AdvanceClose(Proof()));
            EXPECT_EQ(stack_.stage(), RawStackStage::kRetained);
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

} // namespace
} // namespace runtime
} // namespace kbsocket
