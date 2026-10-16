// SPDX-License-Identifier: MulanPSL-2.0
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <expected>
#include <mutex>
#include <thread>

#include "tools/shared_queue_test/shared_queue_test.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Return;

// 两端运行真实的 Session、队列池和账本，仅 URMA 调用由 gMock 接管。
// mock 中的锁保护两个 owner 之间的数据交付，不给产品的数据路径增加锁。
class SharedQueueHardwareTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));

protected:
    enum class ReceiveCorruption { kNone, kImmediate, kPayload };
    struct PostedBuffer {
        urma_sge_t sge;
        std::uint64_t user_ctx;
    };
    struct Registration {
        urma_target_seg_t segment{};
        std::uint64_t address = 0;
        std::uint64_t length = 0;
        bool live = false;
        bool rx = false;
    };
    struct Endpoint {
        urma_context_t ctx{};
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetties{};
        std::array<urma_target_jetty_t, 2> imports{};
        std::array<Registration, 2> registrations{};
        std::deque<PostedBuffer> buffers;
        std::array<std::deque<urma_cr_t>, 2> completions;
        std::array<std::uint64_t, 4> pairs{};
        std::size_t created_cqs = 0;
        std::size_t created_jfrs = 0;
        std::size_t created_jetties = 0;
        std::size_t registered = 0;
        std::size_t imported = 0;
        std::size_t unimported = 0;
        std::size_t deleted_cqs = 0;
        std::size_t deleted_jfrs = 0;
        std::size_t deleted_jetties = 0;
        std::size_t unregistered = 0;
        std::size_t recv_batches = 0;
        std::size_t send_batches = 0;
        std::size_t reversed_rx_batches = 0;
    };

    static raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions f{};
        f.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        f.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        f.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        f.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        f.import_jetty = [](auto* c, auto* r, auto* t) { return active_->Import(c, r, t); };
        f.post_jetty_send_wr = [](auto* q, auto* w, auto** b) { return active_->PostSend(q, w, b); };
        f.post_jfr_wr = [](auto* q, auto* w, auto** b) { return active_->PostRecv(q, w, b); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        f.unimport_jetty = [](auto* q) { return active_->Unimport(q); };
        f.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        f.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        f.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        f.unregister_seg = [](auto* q) { return active_->Unregister(q); };
        return f;
    }

    Endpoint& Find(urma_context_t* ctx) {
        EXPECT_TRUE(ctx == &endpoints_[0].ctx || ctx == &endpoints_[1].ctx);
        return endpoints_[ctx == &endpoints_[1].ctx];
    }

    bool IsRegistered(const Endpoint& endpoint, const urma_sge_t& sge, bool rx) {
        for (const auto& registration : endpoint.registrations) {
            if (sge.tseg == &registration.segment && registration.live && registration.rx == rx &&
                sge.addr >= registration.address && sge.len <= registration.length &&
                sge.addr - registration.address <= registration.length - sge.len) {
                return true;
            }
        }
        return false;
    }

    void SetUp() override {
        active_ = this;
        cap_.max_msg_size = 4096;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 2;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 256;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 1;
        for (std::size_t side = 0; side < endpoints_.size(); ++side) {
            for (std::size_t lane = 0; lane < 2; ++lane) {
                // 故意让两端的数字 ID 相同而 EID 不同，接收源校验不能只比较数字 ID。
                auto& id = endpoints_[side].jetties[lane].jetty_id;
                id.id = 10 + lane;
                id.eid.raw[15] = side + 1;
            }
        }
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_LT(endpoint.created_cqs, 2u);
            EXPECT_EQ(cfg->depth, endpoint.created_cqs == 0 ? 8u : 6u);
            return &endpoint.cqs.at(endpoint.created_cqs++);
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(++endpoint.created_jfrs, 1u);
            EXPECT_EQ(cfg->jfc, &endpoint.cqs[1]);
            EXPECT_EQ(cfg->depth, 6u);
            return &endpoint.jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(cfg->flag.bs.share_jfr, 1u);
            EXPECT_EQ(cfg->shared.jfr, &endpoint.jfr);
            EXPECT_EQ(cfg->shared.jfc, &endpoint.cqs[1]);
            EXPECT_EQ(cfg->jfs_cfg.jfc, &endpoint.cqs[0]);
            EXPECT_EQ(cfg->jfs_cfg.depth, options_.batch);
            return &endpoint.jetties.at(endpoint.created_jetties++);
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            auto& registration = endpoint.registrations.at(endpoint.registered++);
            const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
            EXPECT_EQ(cfg->va % page, 0u);
            EXPECT_EQ(cfg->len % page, 0u);
            registration.address = cfg->va;
            registration.length = cfg->len;
            registration.live = true;
            registration.rx = cfg->len == options_.batch * options_.jetties * raw::RxBufferPool::kBufferSize;
            return &registration.segment;
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg, auto*) {
            auto& endpoint = Find(ctx);
            auto& imported = endpoint.imports.at(endpoint.imported++);
            EXPECT_EQ(cfg->type, URMA_JETTY);
            EXPECT_EQ(cfg->trans_mode, URMA_TM_RM);
            EXPECT_EQ(cfg->tp_type, URMA_CTP);
            imported.id = cfg->jetty_id;
            return &imported;
        });
        EXPECT_CALL(*this, PostRecv(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr, auto* wr, auto**) {
            std::lock_guard lock(mutex_);
            auto& endpoint = endpoints_[jfr == &endpoints_[1].jfr];
            EXPECT_EQ(jfr, &endpoint.jfr);
            ++endpoint.recv_batches;
            for (; wr; wr = wr->next) {
                EXPECT_EQ(wr->src.num_sge, 1u);
                EXPECT_EQ(wr->src.sge[0].len, raw::RxBufferPool::kBufferSize);
                EXPECT_TRUE(IsRegistered(endpoint, wr->src.sge[0], true));
                endpoint.buffers.push_back({wr->src.sge[0], wr->user_ctx});
            }
            EXPECT_LE(endpoint.buffers.size(), 6u);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostSend(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* wr, auto** bad) {
            return Deliver(jetty, wr, bad);
        });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* cq, int limit, auto* output) {
            std::lock_guard lock(mutex_);
            for (auto& endpoint : endpoints_) {
                for (std::size_t index = 0; index < endpoint.cqs.size(); ++index) {
                    if (cq != &endpoint.cqs[index]) {
                        continue;
                    }
                    auto& queue = endpoint.completions[index];
                    if (corruption_ != ReceiveCorruption::kNone && &endpoint == &endpoints_[0] && index == 1 &&
                        endpoints_[1].send_batches < options_.jetties) {
                        // 故障用例让同一 poll 一次交出整批 lease，验证首个错误不会跳过后续完成。
                        return 0;
                    }
                    const auto count = std::min(static_cast<std::size_t>(limit), queue.size());
                    if (index == 1 && count > 1) {
                        ++endpoint.reversed_rx_batches;
                    }
                    for (std::size_t i = 0; i < count; ++i) {
                        output[i] = queue.front();
                        queue.pop_front();
                    }
                    return static_cast<int>(count);
                }
            }
            ADD_FAILURE() << "poll used an unknown completion queue";
            return -1;
        });
        InstallCleanup();
    }

    urma_status_t Deliver(urma_jetty_t* jetty, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
        std::lock_guard lock(mutex_);
        auto& source = endpoints_[jetty == &endpoints_[1].jetties[0] || jetty == &endpoints_[1].jetties[1]];
        const std::size_t source_lane = jetty == &source.jetties[1];
        EXPECT_EQ(jetty, &source.jetties[source_lane]);
        auto& destination = endpoints_[&source == &endpoints_[0]];
        ++source.send_batches;
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
            EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
            EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
            EXPECT_EQ(wr->send.src.num_sge, 1u);
            EXPECT_TRUE(wr->tjetty == &source.imports[0] || wr->tjetty == &source.imports[1]);
            const std::size_t target = wr->tjetty == &source.imports[1];
            EXPECT_EQ(wr->tjetty->id.id, destination.jetties[target].jetty_id.id);
            EXPECT_EQ(std::memcmp(&wr->tjetty->id.eid, &destination.jetties[target].jetty_id.eid, sizeof(urma_eid_t)),
                      0);
            if (destination.buffers.empty()) {
                ADD_FAILURE() << "peer was notified before enough RX buffers were posted";
                *bad = wr;
                return URMA_EAGAIN;
            }
            const auto buffer = destination.buffers.front();
            destination.buffers.pop_front();
            const auto& sge = wr->send.src.sge[0];
            EXPECT_TRUE(IsRegistered(source, sge, false));
            EXPECT_TRUE(IsRegistered(destination, buffer.sge, true));
            EXPECT_EQ(sge.len, options_.bytes);
            EXPECT_LE(sge.len, buffer.sge.len);
            const auto frame = CheckSharedPayload({reinterpret_cast<const std::byte*>(sge.addr), sge.len});
            EXPECT_TRUE(frame);
            if (frame) {
                EXPECT_EQ(frame->source, source_lane);
                EXPECT_EQ(frame->target, target);
                EXPECT_EQ(target, (frame->sequence + source_lane) % options_.jetties);
                EXPECT_EQ(wr->send.imm_data, SharedImmediate(*frame));
            }
            std::memcpy(reinterpret_cast<void*>(buffer.sge.addr), reinterpret_cast<const void*>(sge.addr), sge.len);
            urma_cr_t tx{};
            tx.flag.bs.jetty = 1;
            tx.local_id = jetty->jetty_id.id;
            tx.user_ctx = wr->user_ctx;
            urma_cr_t rx{};
            rx.flag.bs.jetty = 1;
            rx.flag.bs.s_r = 1;
            rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
            rx.local_id = destination.jetties[target].jetty_id.id;
            rx.remote_id = jetty->jetty_id;
            rx.user_ctx = buffer.user_ctx;
            rx.completion_len = sge.len;
            rx.imm_data = wr->send.imm_data;
            if (corruption_ != ReceiveCorruption::kNone && !corrupted_ && &destination == &endpoints_[0]) {
                if (corruption_ == ReceiveCorruption::kImmediate) {
                    rx.imm_data ^= std::uint64_t{1} << 63;
                } else {
                    reinterpret_cast<std::byte*>(buffer.sge.addr)[kSharedPayloadHeader] ^= std::byte{1};
                }
                corrupted_ = true;
            }
            // 同一批 CQE 倒序，且不同 SQ 的 CQE 混在同一个 CQ，不能按提交顺序或当前 SQ 归属。
            source.completions[0].push_front(tx);
            destination.completions[1].push_front(rx);
            ++source.pairs[source_lane * 2 + target];
        }
        return URMA_SUCCESS;
    }

    void InstallCleanup() {
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly([&](auto* remote) {
            for (auto& endpoint : endpoints_) {
                for (auto& imported : endpoint.imports) {
                    if (remote == &imported) {
                        ++endpoint.unimported;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unimport used an unknown target";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty) {
            for (auto& endpoint : endpoints_) {
                for (auto& local : endpoint.jetties) {
                    if (jetty == &local) {
                        EXPECT_TRUE(endpoint.buffers.empty());
                        EXPECT_TRUE(endpoint.completions[0].empty());
                        EXPECT_TRUE(endpoint.completions[1].empty());
                        ++endpoint.deleted_jetties;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "delete used an unknown jetty";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr) {
            auto& endpoint = endpoints_[jfr == &endpoints_[1].jfr];
            EXPECT_EQ(endpoint.deleted_jetties, endpoint.created_jetties);
            ++endpoint.deleted_jfrs;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([&](auto* cq) {
            auto& endpoint = endpoints_[cq == &endpoints_[1].cqs[0] || cq == &endpoints_[1].cqs[1]];
            EXPECT_EQ(endpoint.deleted_jfrs, endpoint.created_jfrs);
            ++endpoint.deleted_cqs;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([&](auto* segment) {
            for (auto& endpoint : endpoints_) {
                for (auto& registration : endpoint.registrations) {
                    if (segment == &registration.segment) {
                        EXPECT_TRUE(registration.live);
                        if (registration.rx) {
                            EXPECT_TRUE(endpoint.buffers.empty());
                            EXPECT_TRUE(endpoint.completions[1].empty());
                        }
                        registration.live = false;
                        ++endpoint.unregistered;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unregister used an unknown segment";
            return URMA_EINVAL;
        });
    }

    void ExpectClosed(const Endpoint& endpoint) {
        EXPECT_EQ(endpoint.unimported, endpoint.imported);
        EXPECT_EQ(endpoint.deleted_jetties, endpoint.created_jetties);
        EXPECT_EQ(endpoint.deleted_jfrs, endpoint.created_jfrs);
        EXPECT_EQ(endpoint.deleted_cqs, endpoint.created_cqs);
        EXPECT_EQ(endpoint.unregistered, endpoint.registered);
    }

    void RunCorruptReceiveCase(ReceiveCorruption corruption) {
        corruption_ = corruption;
        options_.messages = options_.batch;
        options_.timeout_ms = 1000;
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            if (!sessions_[side].Open(&endpoints_[side].ctx, cap_, options_, side == 0)) {
                std::_Exit(2);
            }
        }
        int fds[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0) {
            std::_Exit(3);
        }
        ControlChannel server(fds[0]), client(fds[1]);
        std::expected<void, ToolError> server_result;
        std::thread worker([&] { server_result = sessions_[0].Run(server); });
        const auto client_result = sessions_[1].Run(client);
        worker.join();
        const auto& stats = sessions_[0].stats();
        const bool immediate = corruption == ReceiveCorruption::kImmediate;
        const bool result_valid = !server_result && !client_result &&
                                  server_result.error().code == (immediate ? EPROTO : EILSEQ) &&
                                  stats.failed_rx.has_value() && stats.failed_rx_frame.has_value() == immediate;
        bool diagnostic_valid = result_valid;
        if (immediate && result_valid) {
            diagnostic_valid =
                stats.failed_rx->imm_data == (SharedImmediate(*stats.failed_rx_frame) ^ (std::uint64_t{1} << 63));
        }
        // 所有 WR 都已有完成，但缺少双方成功结束的确认；Close 必须保留整个依赖链。
        bool resources_retained = true;
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            const auto closed = sessions_[side].Close();
            const auto& endpoint = endpoints_[side];
            resources_retained &= !closed && closed.error().code == EBUSY && endpoint.unimported == 0 &&
                                  endpoint.deleted_jetties == 0 && endpoint.deleted_jfrs == 0 &&
                                  endpoint.deleted_cqs == 0 && endpoint.unregistered == 0 && endpoint.buffers.empty() &&
                                  endpoint.completions[0].empty() && endpoint.completions[1].empty();
        }
        // 一条损坏不影响同批其他消息的校验；退出子进程，模拟工具保留未确认资源至进程结束的契约。
        const auto received = options_.jetties * options_.messages - 1;
        std::_Exit(diagnostic_valid && resources_retained && stats.received == received && !HasFailure() ? 0 : 1);
    }

    static inline SharedQueueHardwareTest* active_;
    SharedQueueOptions options_{.bytes = 64, .messages = 5, .batch = 3, .jetties = 2, .timeout_ms = 2000};
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    std::array<Endpoint, 2> endpoints_{};
    ReceiveCorruption corruption_ = ReceiveCorruption::kNone;
    bool corrupted_ = false;
    raw::test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    std::array<SharedQueueSession, 2> sessions_{};
};

class SharedQueueBidirectionalTest : public SharedQueueHardwareTest, public ::testing::WithParamInterface<int> {};

TEST_P(SharedQueueBidirectionalTest, SharesReceiveQueueAndSwitchesTargetWithReorderedCompletions) {
    options_.messages = GetParam();
    for (std::size_t side = 0; side < sessions_.size(); ++side) {
        ASSERT_TRUE(sessions_[side].Open(&endpoints_[side].ctx, cap_, options_, side == 0));
    }
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel server(fds[0]), client(fds[1]);
    std::expected<void, ToolError> server_result;
    std::thread worker([&] { server_result = sessions_[0].Run(server); });
    const auto client_result = sessions_[1].Run(client);
    worker.join();
    ASSERT_TRUE(server_result) << server_result.error().operation << ": " << server_result.error().code;
    ASSERT_TRUE(client_result) << client_result.error().operation << ": " << client_result.error().code;

    for (std::size_t side = 0; side < sessions_.size(); ++side) {
        const auto& endpoint = endpoints_[side];
        const auto& stats = sessions_[side].stats();
        const auto expected_messages = options_.jetties * options_.messages;
        EXPECT_TRUE(stats.accepted_known);
        EXPECT_EQ(stats.accepted, expected_messages);
        EXPECT_EQ(stats.retired, expected_messages);
        EXPECT_EQ(stats.received, expected_messages);
        EXPECT_EQ(stats.imm_patterns, options_.messages == 5 ? 0x3fu : 0xffu);
        EXPECT_GT(endpoint.reversed_rx_batches, 0u);
        EXPECT_EQ(endpoint.recv_batches, (options_.messages + options_.batch - 1) / options_.batch);
        EXPECT_EQ(endpoint.send_batches, endpoint.recv_batches * options_.jetties);
        // 两个 SQ 都必须访问两个目标，短尾批也必须完整退休；仅总消息数相等不足以发现路由串线。
        for (std::uint32_t source = 0; source < options_.jetties; ++source) {
            for (std::uint32_t target = 0; target < options_.jetties; ++target) {
                std::uint64_t expected = 0;
                for (std::uint32_t seq = 0; seq < options_.messages; ++seq) {
                    expected += (seq + source) % options_.jetties == target;
                }
                EXPECT_EQ(endpoint.pairs[source * 2 + target], expected);
                EXPECT_EQ(stats.tx_pairs[source * kSharedMaxJetties + target], expected);
                EXPECT_EQ(stats.rx_pairs[source * kSharedMaxJetties + target], expected);
            }
        }
        ASSERT_TRUE(sessions_[side].Close());
        ExpectClosed(endpoint);
        // 重复 Close 不应再次销毁已经释放的硬件资源。
        ASSERT_TRUE(sessions_[side].Close());
        ExpectClosed(endpoint);
    }
    std::expected<void, ToolError> server_closed;
    std::thread close_worker([&] { server_closed = ConfirmSharedClosed(server, options_.timeout_ms); });
    const auto client_closed = ConfirmSharedClosed(client, options_.timeout_ms);
    close_worker.join();
    EXPECT_TRUE(server_closed);
    EXPECT_TRUE(client_closed);
}

INSTANTIATE_TEST_SUITE_P(PartialBatchesAndAllImmediatePatterns, SharedQueueBidirectionalTest, ::testing::Values(5, 8));

TEST_F(SharedQueueHardwareTest, PeerMismatchRejectsBeforeImportOrPosting) {
    ASSERT_TRUE(sessions_[0].Open(&endpoints_[0].ctx, cap_, options_, true));
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    SharedQueueHello hello{.options = options_, .server = false};
    ++hello.options.bytes;
    for (std::size_t i = 0; i < 2; ++i) {
        hello.endpoints[i] = endpoints_[1].jetties[i].jetty_id;
    }
    ASSERT_TRUE(peer.Write(EncodeSharedHello(hello), options_.timeout_ms));
    // 不兼容参数必须在导入远端以及投递 RX 前失败，仍然允许完整关闭已创建的本地资源。
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    EXPECT_FALSE(sessions_[0].Run(local));
    ASSERT_TRUE(sessions_[0].Close());
    ExpectClosed(endpoints_[0]);
}

TEST_F(SharedQueueHardwareTest, PartialImportFailureRetainsKnownResourcesForClose) {
    ASSERT_TRUE(sessions_[0].Open(&endpoints_[0].ctx, cap_, options_, true));
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    SharedQueueHello hello{.options = options_, .server = false};
    for (std::size_t i = 0; i < 2; ++i) {
        hello.endpoints[i] = endpoints_[1].jetties[i].jetty_id;
    }
    ASSERT_TRUE(peer.Write(EncodeSharedHello(hello), options_.timeout_ms));
    EXPECT_CALL(*this, Import(&endpoints_[0].ctx, _, _))
        .WillOnce([&](auto*, auto* cfg, auto*) {
            auto& endpoint = endpoints_[0];
            auto& imported = endpoint.imports[endpoint.imported++];
            imported.id = cfg->jetty_id;
            return &imported;
        })
        .WillOnce([](auto*, auto*, auto*) -> urma_target_jetty_t* {
            errno = ENOMEM;
            return nullptr;
        });
    // 第二个导入失败时尚未发布本轮 READY，不得投递数据；第一个目标仍需要且只能反导入一次。
    EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    const auto result = sessions_[0].Run(local);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ENOMEM);
    ASSERT_TRUE(sessions_[0].Close());
    EXPECT_EQ(endpoints_[0].unimported, 1u);
    ExpectClosed(endpoints_[0]);
}

TEST_F(SharedQueueHardwareTest, DeleteFailureKeepsParentQueuesAndCloseCanRetry) {
    ASSERT_TRUE(sessions_[0].Open(&endpoints_[0].ctx, cap_, options_, true));
    // 最后一个 jetty 删除失败时，JFR/JFC 还在被引用；重试从失败处继续，不能提前删除父队列。
    EXPECT_CALL(*this, DeleteJetty(&endpoints_[0].jetties[1])).WillOnce(Return(URMA_EAGAIN)).RetiresOnSaturation();
    EXPECT_FALSE(sessions_[0].Close());
    EXPECT_EQ(endpoints_[0].deleted_jfrs, 0u);
    EXPECT_EQ(endpoints_[0].deleted_cqs, 0u);
    ASSERT_TRUE(sessions_[0].Close());
    ExpectClosed(endpoints_[0]);
}

TEST_F(SharedQueueHardwareTest, ImmediateHighBitCorruptionFailsAndRetainsResourcesUntilProcessExit) {
    // 有意走资源常驻分支，用子进程隔离；_Exit 不执行会破坏保留语义的局部对象析构。
    EXPECT_EXIT(RunCorruptReceiveCase(ReceiveCorruption::kImmediate), ::testing::ExitedWithCode(0), "");
}

TEST_F(SharedQueueHardwareTest, PayloadCorruptionStillValidatesOtherCompletionsInSameBatch) {
    EXPECT_EXIT(RunCorruptReceiveCase(ReceiveCorruption::kPayload), ::testing::ExitedWithCode(0), "");
}
} // namespace
} // namespace tools
} // namespace kbsocket
