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
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "tools/backpressure_test/backpressure_test.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Return;

// 两端运行真实 Session 和收发账本；锁只协调 mock 的跨端 DMA/CQE，不改变单 owner 契约。
class BackpressureHardwareTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(urma_status_t, Query, (urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t*));
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));

protected:
    enum class Failure {
        kNone,
        kMissingBoundary,
        kModify,
        kUnknownAccepted,
        kPayload,
        kProviderEagain,
        kProviderPartial,
        kMissingSuccessfulRx,
        kRnrQuery,
        kRnrState,
        kRnrSuspend,
        kRnrInitialError,
        kRnrContinuationError,
        kRnrBoundary,
        kRnrDuplicate
    };
    struct PostedBuffer {
        urma_sge_t sge;
        std::uint64_t user_ctx;
    };
    struct Work {
        urma_sge_t sge;
        std::uint64_t user_ctx;
        std::uint64_t immediate;
        BackpressureFrame frame;
    };
    struct Registration {
        urma_target_seg_t segment{};
        std::uint64_t address = 0;
        std::uint64_t length = 0;
        bool live = false;
    };
    struct Endpoint {
        urma_context_t ctx{};
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetties{};
        urma_target_jetty_t imported{};
        Registration registration;
        std::deque<PostedBuffer> buffers;
        std::array<std::deque<urma_cr_t>, 2> completions;
        std::array<std::deque<Work>, 2> pending;
        std::array<std::deque<urma_cr_t>, 2> software;
        std::deque<urma_cr_t> deferred_rx;
        std::array<bool, 2> modified{};
        std::array<bool, 2> boundary_polled{};
        std::array<bool, 2> deleted_jetties{};
        std::array<bool, 2> deleted_cqs{};
        std::array<std::size_t, 2> flush_calls{};
        std::vector<std::size_t> send_batches;
        std::vector<std::size_t> recv_batches;
        std::set<std::uint64_t> submitted_frames;
        std::size_t created_cqs = 0;
        std::size_t created_jetties = 0;
        std::size_t registered = 0;
        std::size_t imports = 0;
        std::size_t unimported = 0;
        std::size_t unregistered = 0;
        std::size_t sends_before_rx = 0;
        std::size_t tx_polls = 0;
        std::size_t rnr_polled = 0;
        std::size_t queries = 0;
        bool created_jfr = false;
        bool deleted_jfr = false;
    };

    static raw::UrmaFunctions MockedFunctions() {
        raw::UrmaFunctions functions{};
        functions.create_jfc = [](auto* c, auto* cfg) { return active_->CreateJfc(c, cfg); };
        functions.create_jfr = [](auto* c, auto* cfg) { return active_->CreateJfr(c, cfg); };
        functions.create_jetty = [](auto* c, auto* cfg) { return active_->CreateJetty(c, cfg); };
        functions.register_seg = [](auto* c, auto* cfg) { return active_->Register(c, cfg); };
        functions.import_jetty = [](auto* c, auto* r, auto* t) { return active_->Import(c, r, t); };
        functions.post_jetty_send_wr = [](auto* q, auto* w, auto** b) { return active_->PostSend(q, w, b); };
        functions.post_jfr_wr = [](auto* q, auto* w, auto** b) { return active_->PostRecv(q, w, b); };
        functions.query_jetty = [](auto* q, auto* cfg, auto* attr) { return active_->Query(q, cfg, attr); };
        functions.modify_jetty = [](auto* q, auto* a) { return active_->Modify(q, a); };
        functions.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        functions.flush_jetty = [](auto* q, int n, auto* cr) {
            // 与 liburma 同样限制 flush 预算，防止小深度队列被测试替身默许越界请求。
            if (q == nullptr || cr == nullptr || n <= 0 || static_cast<unsigned>(n) > q->jetty_cfg.jfs_cfg.depth) {
                return -URMA_EINVAL;
            }
            return active_->Flush(q, n, cr);
        };
        functions.unimport_jetty = [](auto* q) { return active_->Unimport(q); };
        functions.delete_jetty = [](auto* q) { return active_->DeleteJetty(q); };
        functions.delete_jfr = [](auto* q) { return active_->DeleteJfr(q); };
        functions.delete_jfc = [](auto* q) { return active_->DeleteJfc(q); };
        functions.unregister_seg = [](auto* q) { return active_->Unregister(q); };
        return functions;
    }

    Endpoint& Find(urma_context_t* ctx) {
        EXPECT_TRUE(ctx == &endpoints_[0].ctx || ctx == &endpoints_[1].ctx);
        return endpoints_[ctx == &endpoints_[1].ctx];
    }

    static bool SameEndpoint(const urma_jetty_id_t& lhs, const urma_jetty_id_t& rhs) {
        return lhs.id == rhs.id && std::memcmp(&lhs.eid, &rhs.eid, sizeof(urma_eid_t)) == 0;
    }

    static bool IsRegistered(const Endpoint& endpoint, const urma_sge_t& sge) {
        const auto& registration = endpoint.registration;
        return registration.live && sge.tseg == &registration.segment && sge.addr >= registration.address &&
               sge.len <= registration.length && sge.addr - registration.address <= registration.length - sge.len;
    }

    urma_cr_t Completion(std::size_t lane, std::uint64_t id, urma_cr_status_t status) const {
        urma_cr_t completion{};
        completion.flag.bs.jetty = 1;
        completion.local_id = endpoints_[1].jetties[lane].jetty_id.id;
        completion.user_ctx = id;
        completion.status = status;
        return completion;
    }

    void SetUp() override {
        active_ = this;
        cap_.max_msg_size = 4096;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 2;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 1024;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 1;
        for (std::size_t side = 0; side < endpoints_.size(); ++side) {
            for (std::size_t lane = 0; lane < 2; ++lane) {
                // 数字 ID 跨端重复，目标查找必须使用 EID 与 ID 的组合。
                endpoints_[side].jetties[lane].jetty_id.id = 10 + lane;
                endpoints_[side].jetties[lane].jetty_id.eid.raw[15] = side + 1;
            }
        }
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            const auto lanes = options_.mode == BackpressureMode::kRxDelay ? 2u : 1u;
            const auto rx_depth = options_.tx_depth + (options_.mode == BackpressureMode::kTxQuota);
            EXPECT_EQ(cfg->depth, endpoint.created_cqs == 0 ? lanes * (options_.tx_depth + 1) : rx_depth);
            return &endpoint.cqs.at(endpoint.created_cqs++);
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_FALSE(endpoint.created_jfr);
            EXPECT_EQ(cfg->jfc, &endpoint.cqs[1]);
            EXPECT_EQ(cfg->depth, options_.tx_depth + (options_.mode == BackpressureMode::kTxQuota));
            endpoint.created_jfr = true;
            return &endpoint.jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(cfg->flag.bs.share_jfr, 1u);
            EXPECT_EQ(cfg->shared.jfr, &endpoint.jfr);
            EXPECT_EQ(cfg->shared.jfc, &endpoint.cqs[1]);
            EXPECT_EQ(cfg->jfs_cfg.jfc, &endpoint.cqs[0]);
            EXPECT_EQ(cfg->jfs_cfg.depth, options_.tx_depth);
            EXPECT_EQ(cfg->jfs_cfg.flag.bs.error_suspend, 0u);
            auto& jetty = endpoint.jetties.at(endpoint.created_jetties++);
            jetty.jetty_cfg = *cfg;
            return &jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(++endpoint.registered, 1u);
            const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
            EXPECT_EQ(cfg->va % page, 0u);
            EXPECT_EQ(cfg->len % page, 0u);
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            auto& registration = endpoint.registration;
            registration.address = cfg->va;
            registration.length = cfg->len;
            registration.live = true;
            return &registration.segment;
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg, auto*) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(ctx, &endpoints_[1].ctx);
            EXPECT_EQ(++endpoint.imports, 1u);
            EXPECT_EQ(cfg->type, URMA_JETTY);
            EXPECT_EQ(cfg->trans_mode, URMA_TM_RM);
            EXPECT_EQ(cfg->tp_type, URMA_CTP);
            EXPECT_TRUE(SameEndpoint(cfg->jetty_id, endpoints_[0].jetties[0].jetty_id));
            endpoint.imported.id = cfg->jetty_id;
            return &endpoint.imported;
        });
        EXPECT_CALL(*this, PostRecv(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr, auto* wr, auto**) {
            std::lock_guard lock(mutex_);
            auto& server = endpoints_[0];
            EXPECT_EQ(jfr, &server.jfr);
            if (options_.mode == BackpressureMode::kRnrContinue) {
                // 必须先从 CQ 取回整批 RNR，才能补投 RX；否则测到的可能只是重试成功。
                EXPECT_EQ(endpoints_[1].rnr_polled, options_.tx_depth);
                EXPECT_EQ(endpoints_[1].queries, 2u);
                EXPECT_TRUE(endpoints_[1].completions[0].empty());
                EXPECT_TRUE(server.recv_batches.empty());
            }
            std::size_t count = 0;
            for (; wr; wr = wr->next) {
                EXPECT_EQ(wr->src.num_sge, 1u);
                EXPECT_EQ(wr->src.sge[0].len, raw::RxBufferPool::kBufferSize);
                EXPECT_TRUE(IsRegistered(server, wr->src.sge[0]));
                server.buffers.push_back({wr->src.sge[0], wr->user_ctx});
                ++count;
            }
            EXPECT_LE(count, raw::TxSender::kMaxBatch);
            server.recv_batches.push_back(count);
            if (options_.mode == BackpressureMode::kRxDelay) {
                EXPECT_EQ(endpoints_[1].sends_before_rx, options_.tx_depth);
                if (!terminal_error_) {
                    auto& pending = endpoints_[1].pending[0];
                    while (!pending.empty() && !server.buffers.empty()) {
                        Deliver(pending.front(), 0, false);
                        pending.pop_front();
                    }
                } else if (late_original_) {
                    // 远端确实收到过错误 TX 的数据，但完成先暂不可见；软件 posted() 会高估空槽。
                    Deliver(late_work_, 0, true);
                } else if (failure_ == Failure::kMissingSuccessfulRx) {
                    // 只有第二条成功；第一条已有错误完成，剩余 WR 留给 ERROR 排空。
                    auto& pending = endpoints_[1].pending[0];
                    EXPECT_FALSE(pending.empty());
                    if (!pending.empty()) {
                        Deliver(pending.front(), 0, false);
                        pending.pop_front();
                    }
                }
            }
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostSend(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* wr, auto** bad) {
            return Submit(jetty, wr, bad);
        });
        EXPECT_CALL(*this, Query(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* cfg, auto* attr) {
            std::lock_guard lock(mutex_);
            auto& client = endpoints_[1];
            EXPECT_EQ(options_.mode, BackpressureMode::kRnrContinue);
            EXPECT_EQ(jetty, &client.jetties[0]);
            ++client.queries;
            if (client.queries == 1) {
                EXPECT_TRUE(client.send_batches.empty());
            } else {
                EXPECT_EQ(client.rnr_polled, options_.tx_depth);
            }
            *cfg = jetty->jetty_cfg;
            // 模拟 provider 对配置的转换，工具必须记录查询值而非原始请求值。
            cfg->jfs_cfg.rnr_retry = 2;
            cfg->jfs_cfg.err_timeout = 5;
            attr->state = URMA_JETTY_STATE_READY;
            if (client.queries == 2) {
                if (failure_ == Failure::kRnrQuery) {
                    return URMA_EINVAL;
                }
                if (failure_ == Failure::kRnrState) {
                    attr->state = URMA_JETTY_STATE_ERROR;
                }
                if (failure_ == Failure::kRnrSuspend) {
                    cfg->jfs_cfg.flag.bs.error_suspend = 1;
                }
            }
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, Modify(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* attr) {
            return EnterError(jetty, attr);
        });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* cq, int limit, auto* out) {
            return PollCompletions(cq, limit, out);
        });
        EXPECT_CALL(*this, Flush(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, int limit, auto* out) {
            std::lock_guard lock(mutex_);
            auto& client = endpoints_[1];
            const auto lane = static_cast<std::size_t>(jetty == &client.jetties[1]);
            EXPECT_EQ(jetty, &client.jetties[lane]);
            EXPECT_TRUE(client.modified[lane]);
            EXPECT_TRUE(client.boundary_polled[lane]);
            ++client.flush_calls[lane];
            auto& queue = client.software[lane];
            const auto count = std::min(queue.size(), static_cast<std::size_t>(limit));
            for (std::size_t i = 0; i < count; ++i) {
                out[i] = queue.front();
                queue.pop_front();
            }
            return static_cast<int>(count);
        });
        InstallCleanup();
    }

    void Deliver(const Work& work, std::size_t lane, bool late) {
        auto& client = endpoints_[1];
        auto& server = endpoints_[0];
        ASSERT_FALSE(server.buffers.empty());
        const auto buffer = server.buffers.front();
        server.buffers.pop_front();
        ASSERT_TRUE(IsRegistered(client, work.sge));
        ASSERT_TRUE(IsRegistered(server, buffer.sge));
        // 延迟交付时再次校验源内容，能发现 TX 完成前覆盖或复用 buffer 的错误。
        const auto frame = CheckBackpressurePayload({reinterpret_cast<const std::byte*>(work.sge.addr), work.sge.len});
        ASSERT_TRUE(frame);
        EXPECT_EQ(frame->index, work.frame.index);
        EXPECT_EQ(frame->round, work.frame.round);
        EXPECT_EQ(frame->cleanup, work.frame.cleanup);
        std::memcpy(reinterpret_cast<void*>(buffer.sge.addr), reinterpret_cast<const void*>(work.sge.addr),
                    work.sge.len);
        urma_cr_t rx{};
        rx.flag.bs.jetty = 1;
        rx.flag.bs.s_r = 1;
        rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        rx.local_id = server.jetties[0].jetty_id.id;
        rx.remote_id = client.jetties[lane].jetty_id;
        rx.user_ctx = buffer.user_ctx;
        rx.completion_len = work.sge.len;
        rx.imm_data = work.immediate;
        if (failure_ == Failure::kMissingSuccessfulRx && !work.frame.cleanup) {
            // 把成功 WR 的 RX 替换成合法但属于错误 WR 的身份；总数相同不能证明成功消息已收到。
            const BackpressureFrame wrong_identity{.round = work.frame.round, .index = 0, .cleanup = false};
            FillBackpressurePayload({reinterpret_cast<std::byte*>(buffer.sge.addr), work.sge.len}, wrong_identity);
            rx.imm_data = BackpressureImmediate(wrong_identity);
        }
        if (failure_ == Failure::kPayload && !corrupted_) {
            reinterpret_cast<std::byte*>(buffer.sge.addr)[kBackpressurePayloadHeader] ^= std::byte{1};
            corrupted_ = true;
        }
        if (late) {
            server.deferred_rx.push_back(rx);
        } else {
            server.completions[1].push_front(rx);
            client.completions[0].push_front(Completion(lane, work.user_ctx, URMA_CR_SUCCESS));
        }
    }

    urma_status_t Submit(urma_jetty_t* jetty, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
        std::lock_guard lock(mutex_);
        auto& client = endpoints_[1];
        auto& server = endpoints_[0];
        const auto lane = static_cast<std::size_t>(jetty == &client.jetties[1]);
        EXPECT_EQ(jetty, &client.jetties[lane]);
        EXPECT_FALSE(client.modified[lane]);
        if (failure_ == Failure::kProviderEagain) {
            *bad = wr;
            return URMA_EAGAIN;
        }
        if (lane == 1 && late_original_) {
            // 只有清理批次已按旧快照开始发送，才交出原始 RX CQE，确定性覆盖过量清理场景。
            server.completions[1].insert(server.completions[1].end(), server.deferred_rx.begin(),
                                         server.deferred_rx.end());
            server.deferred_rx.clear();
        }
        std::size_t count = 0;
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
            EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
            EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
            EXPECT_EQ(wr->send.src.num_sge, 1u);
            EXPECT_EQ(wr->tjetty, &client.imported);
            const auto& sge = wr->send.src.sge[0];
            EXPECT_TRUE(IsRegistered(client, sge));
            EXPECT_EQ(sge.len, options_.bytes);
            const auto frame = CheckBackpressurePayload({reinterpret_cast<const std::byte*>(sge.addr), sge.len});
            if (!frame) {
                ADD_FAILURE() << "invalid payload submitted";
                *bad = wr;
                return URMA_EINVAL;
            }
            EXPECT_EQ(frame->cleanup, lane == 1);
            EXPECT_EQ(wr->send.imm_data, BackpressureImmediate(*frame));
            const auto identity = (static_cast<std::uint64_t>(frame->cleanup) << 63) |
                                  (static_cast<std::uint64_t>(frame->round) << 32) | frame->index;
            EXPECT_TRUE(client.submitted_frames.insert(identity).second) << "accepted data must never be replayed";
            const Work work{sge, wr->user_ctx, wr->send.imm_data, *frame};
            if (options_.mode == BackpressureMode::kRnrContinue && frame->round == 0) {
                EXPECT_EQ(lane, 0u);
                EXPECT_TRUE(server.recv_batches.empty());
                EXPECT_TRUE(server.buffers.empty());
                ++client.sends_before_rx;
                auto completion = Completion(lane, work.user_ctx, URMA_CR_RNR_RETRY_CNT_EXC_ERR);
                if (count == 0 && failure_ == Failure::kRnrInitialError) {
                    completion.status = URMA_CR_ACK_TIMEOUT_ERR;
                }
                if (count == 0 && failure_ == Failure::kRnrBoundary) {
                    // 伪完成不能按普通 RNR 处理，更不能解引用它携带的无效 user_ctx。
                    completion.status = URMA_CR_WR_FLUSH_ERR_DONE;
                    completion.user_ctx = UINT64_MAX;
                }
                client.completions[0].push_back(completion);
                if (count == 0 && failure_ == Failure::kRnrDuplicate) {
                    client.completions[0].push_back(completion);
                }
            } else if (options_.mode == BackpressureMode::kRnrContinue) {
                // 后续消息必须来自同一个物理 SQ，并使用与已失败消息不同的 round。
                EXPECT_EQ(lane, 0u);
                EXPECT_EQ(frame->round, 1u);
                EXPECT_EQ(client.queries, 2u);
                EXPECT_EQ(client.rnr_polled, options_.tx_depth);
                EXPECT_EQ(server.recv_batches.size(), 1u);
                EXPECT_FALSE(server.buffers.empty());
                if (failure_ == Failure::kRnrContinuationError) {
                    client.completions[0].push_back(Completion(lane, work.user_ctx, URMA_CR_RNR_RETRY_CNT_EXC_ERR));
                } else {
                    Deliver(work, lane, false);
                }
            } else if (options_.mode == BackpressureMode::kRxDelay && lane == 0) {
                EXPECT_TRUE(server.recv_batches.empty());
                ++client.sends_before_rx;
                if (count < remote_abort_count_) {
                    // 远端错误可逐条完成，而本地 SQ 仍可用；其余 WR 留给工具主动关闭时排空。
                    client.completions[0].push_back(Completion(lane, work.user_ctx, URMA_CR_REM_ACCESS_ABORT_ERR));
                } else if (terminal_error_ && remote_abort_count_ == 0 && count == 0) {
                    late_work_ = work;
                    const auto status = late_original_ ? URMA_CR_ACK_TIMEOUT_ERR : URMA_CR_RNR_RETRY_CNT_EXC_ERR;
                    client.completions[0].push_back(Completion(lane, work.user_ctx, status));
                } else {
                    client.pending[lane].push_back(work);
                }
            } else if (server.buffers.empty()) {
                EXPECT_EQ(lane, 1u);
                EXPECT_TRUE(late_original_);
                client.completions[0].push_back(Completion(lane, work.user_ctx, URMA_CR_RNR_RETRY_CNT_EXC_ERR));
            } else {
                Deliver(work, lane, false);
            }
            ++count;
            if (failure_ == Failure::kProviderPartial) {
                // provider 已接受首条，bad_wr 指向未接受后缀；工具必须保存这一事实而非从头重放。
                EXPECT_NE(wr->next, nullptr);
                *bad = wr->next;
                client.send_batches.push_back(count);
                return URMA_EAGAIN;
            }
        }
        if (options_.mode == BackpressureMode::kTxQuota) {
            const bool recovery = client.send_batches.size() % 2 == 1;
            EXPECT_EQ(count, recovery ? 1u : options_.tx_depth);
            if (recovery) {
                EXPECT_GT(client.tx_polls, polls_at_initial_send_);
            } else {
                polls_at_initial_send_ = client.tx_polls;
            }
        }
        client.send_batches.push_back(count);
        return failure_ == Failure::kUnknownAccepted ? URMA_EAGAIN : URMA_SUCCESS;
    }

    urma_status_t EnterError(urma_jetty_t* jetty, urma_jetty_attr_t* attr) {
        std::lock_guard lock(mutex_);
        auto& client = endpoints_[1];
        const auto lane = static_cast<std::size_t>(jetty == &client.jetties[1]);
        EXPECT_EQ(jetty, &client.jetties[lane]);
        EXPECT_EQ(attr->mask, JETTY_STATE);
        EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
        EXPECT_FALSE(client.modified[lane]);
        client.modified[lane] = true;
        if (failure_ == Failure::kModify) {
            return URMA_EINVAL;
        }
        auto& pending = client.pending[lane];
        while (!pending.empty()) {
            const bool software = pending.size() == 1;
            auto& output = software ? client.software[lane] : client.completions[0];
            output.push_back(
                Completion(lane, pending.front().user_ctx, software ? URMA_CR_WR_UNHANDLED : URMA_CR_WR_FLUSH_ERR));
            pending.pop_front();
        }
        if (failure_ != Failure::kMissingBoundary) {
            // 边界携带真实 attempt ID，确保其伪造 user_ctx 不会被解释成另一条普通完成。
            client.completions[0].push_back(Completion(lane, late_work_.user_ctx, URMA_CR_WR_FLUSH_ERR_DONE));
        }
        return URMA_SUCCESS;
    }

    int PollCompletions(urma_jfc_t* cq, int limit, urma_cr_t* out) {
        std::lock_guard lock(mutex_);
        for (auto& endpoint : endpoints_) {
            for (std::size_t index = 0; index < endpoint.cqs.size(); ++index) {
                if (cq != &endpoint.cqs[index]) {
                    continue;
                }
                if (index == 0) {
                    ++endpoint.tx_polls;
                }
                auto& queue = endpoint.completions[index];
                const auto count = std::min(queue.size(), static_cast<std::size_t>(limit));
                for (std::size_t i = 0; i < count; ++i) {
                    out[i] = queue.front();
                    queue.pop_front();
                    if (out[i].status == URMA_CR_WR_FLUSH_ERR_DONE) {
                        const auto lane = static_cast<std::size_t>(out[i].local_id == endpoint.jetties[1].jetty_id.id);
                        endpoint.boundary_polled[lane] = true;
                    }
                    if (options_.mode == BackpressureMode::kRnrContinue && index == 0 &&
                        endpoint.send_batches.size() == 1 && out[i].status == URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
                        ++endpoint.rnr_polled;
                    }
                }
                return static_cast<int>(count);
            }
        }
        ADD_FAILURE() << "unknown completion queue";
        return -1;
    }

    void InstallCleanup() {
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly([&](auto* remote) {
            EXPECT_EQ(remote, &endpoints_[1].imported);
            EXPECT_EQ(++endpoints_[1].unimported, 1u);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty) {
            for (auto& endpoint : endpoints_) {
                for (std::size_t lane = 0; lane < endpoint.jetties.size(); ++lane) {
                    if (jetty == &endpoint.jetties[lane]) {
                        EXPECT_TRUE(endpoint.buffers.empty());
                        EXPECT_TRUE(endpoint.completions[0].empty());
                        EXPECT_TRUE(endpoint.completions[1].empty());
                        EXPECT_TRUE(endpoint.pending[lane].empty());
                        EXPECT_TRUE(endpoint.software[lane].empty());
                        EXPECT_TRUE(endpoint.deferred_rx.empty());
                        EXPECT_FALSE(endpoint.deleted_jetties[lane]);
                        endpoint.deleted_jetties[lane] = true;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unknown jetty";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr) {
            auto& endpoint = endpoints_[jfr == &endpoints_[1].jfr];
            EXPECT_EQ(jfr, &endpoint.jfr);
            EXPECT_EQ(static_cast<std::size_t>(
                          std::count(endpoint.deleted_jetties.begin(), endpoint.deleted_jetties.end(), true)),
                      endpoint.created_jetties);
            EXPECT_FALSE(endpoint.deleted_jfr);
            endpoint.deleted_jfr = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([&](auto* cq) {
            for (auto& endpoint : endpoints_) {
                for (std::size_t index = 0; index < endpoint.cqs.size(); ++index) {
                    if (cq == &endpoint.cqs[index]) {
                        EXPECT_TRUE(endpoint.deleted_jfr);
                        EXPECT_FALSE(endpoint.deleted_cqs[index]);
                        endpoint.deleted_cqs[index] = true;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unknown CQ";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([&](auto* segment) {
            for (auto& endpoint : endpoints_) {
                auto& registration = endpoint.registration;
                if (segment == &registration.segment) {
                    EXPECT_TRUE(registration.live);
                    EXPECT_TRUE(endpoint.buffers.empty());
                    EXPECT_TRUE(endpoint.completions[0].empty());
                    EXPECT_TRUE(endpoint.completions[1].empty());
                    EXPECT_TRUE(endpoint.deferred_rx.empty());
                    registration.live = false;
                    ++endpoint.unregistered;
                    return URMA_SUCCESS;
                }
            }
            ADD_FAILURE() << "unknown segment";
            return URMA_EINVAL;
        });
    }

    std::expected<void, ToolError> OpenBoth() {
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            auto result = sessions_[side].Open(&endpoints_[side].ctx, cap_, options_, side == 0);
            if (!result) {
                return result;
            }
        }
        return {};
    }

    std::array<std::expected<void, ToolError>, 2> RunBoth() {
        int fds[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0) {
            const auto error = std::unexpected(ToolError{"socketpair", errno});
            return {error, error};
        }
        ControlChannel server(fds[0]), client(fds[1]);
        std::array<std::expected<void, ToolError>, 2> results;
        std::thread worker([&] { results[0] = sessions_[0].Run(server); });
        results[1] = sessions_[1].Run(client);
        worker.join();
        return results;
    }

    void ExpectClosed(const Endpoint& endpoint) {
        EXPECT_EQ(endpoint.unimported, endpoint.imports);
        EXPECT_EQ(endpoint.unregistered, endpoint.registered);
        EXPECT_EQ(endpoint.deleted_jfr, endpoint.created_jfr);
        EXPECT_EQ(static_cast<std::size_t>(
                      std::count(endpoint.deleted_jetties.begin(), endpoint.deleted_jetties.end(), true)),
                  endpoint.created_jetties);
        EXPECT_EQ(static_cast<std::size_t>(std::count(endpoint.deleted_cqs.begin(), endpoint.deleted_cqs.end(), true)),
                  endpoint.created_cqs);
    }

    void CloseBoth() {
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            ASSERT_TRUE(sessions_[side].Close());
            ExpectClosed(endpoints_[side]);
            ASSERT_TRUE(sessions_[side].Close());
            ExpectClosed(endpoints_[side]);
        }
    }

    void RunFailureCase(Failure failure) {
        failure_ = failure;
        options_.timeout_ms = 100;
        if (failure == Failure::kMissingBoundary || failure == Failure::kModify ||
            failure == Failure::kMissingSuccessfulRx) {
            options_.mode = BackpressureMode::kRxDelay;
            options_.expect = BackpressureExpectation::kError;
            terminal_error_ = true;
        }
        if (!OpenBoth()) {
            std::_Exit(2);
        }
        const auto results = RunBoth();
        bool retained = !results[0] && !results[1];
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            const auto closed = sessions_[side].Close();
            const auto& endpoint = endpoints_[side];
            retained &=
                !closed && closed.error().code == EBUSY && endpoint.registration.live && endpoint.unregistered == 0 &&
                endpoint.unimported == 0 && !endpoint.deleted_jfr &&
                std::none_of(endpoint.deleted_jetties.begin(), endpoint.deleted_jetties.end(),
                             [](bool x) { return x; }) &&
                std::none_of(endpoint.deleted_cqs.begin(), endpoint.deleted_cqs.end(), [](bool x) { return x; });
        }
        const auto& client = sessions_[1].stats();
        if (failure == Failure::kMissingBoundary) {
            // 即使普通硬件完成已退休，缺少边界也不能提前调用软件 flush 或销毁 DMA 内存。
            retained &= results[1].error().code == ETIMEDOUT && client.traffic.flush_done == 0 &&
                        client.traffic.retired == options_.tx_depth - 1 && endpoints_[1].flush_calls[0] == 0;
        } else if (failure == Failure::kModify) {
            retained &= client.traffic.flush_done == 0 && endpoints_[1].flush_calls[0] == 0;
        } else if (failure == Failure::kUnknownAccepted) {
            retained &= !client.accepted_known;
        } else if (failure == Failure::kProviderEagain) {
            retained &= client.accepted_known && client.provider_eagain == 1 && client.traffic.accepted == 0;
        } else if (failure == Failure::kProviderPartial) {
            retained &= client.accepted_known && client.provider_eagain == 1 && client.traffic.accepted == 1 &&
                        client.quota_rejections == 0 && client.recovery_sends == 0 &&
                        endpoints_[1].submitted_frames.size() == 1 && endpoints_[1].send_batches.size() == 1 &&
                        endpoints_[1].send_batches.front() == 1;
        } else if (failure == Failure::kPayload) {
            retained &= results[0].error().code == EILSEQ && corrupted_;
        } else if (failure == Failure::kMissingSuccessfulRx) {
            retained &= results[0].error().code == EPROTO && client.traffic.success == 1 &&
                        sessions_[0].stats().traffic.received == 1;
        }
        // 工具失败时常驻整条依赖链；子进程用 _Exit 避免测试夹具析构破坏此契约。
        std::_Exit(retained && !HasFailure() ? 0 : 1);
    }

    void ConfigureRnrContinue() {
        options_.mode = BackpressureMode::kRnrContinue;
        options_.expect = BackpressureExpectation::kEither;
        // 此实验必须保留 SQ 原状态；进入 ERROR 或 flush 都会破坏要验证的前提。
        EXPECT_CALL(*this, Modify(_, _)).Times(0);
        EXPECT_CALL(*this, Flush(_, _, _)).Times(0);
    }

    void RunRnrFailureCase(Failure failure) {
        ConfigureRnrContinue();
        failure_ = failure;
        options_.timeout_ms = 100;
        if (!OpenBoth()) {
            std::_Exit(2);
        }
        const auto results = RunBoth();
        bool retained = !results[0] && !results[1];
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            const auto closed = sessions_[side].Close();
            const auto& endpoint = endpoints_[side];
            // 失败后没有双端静默证明；不能因本端账本为空就释放仍可能被 peer 使用的资源。
            retained &= !closed && closed.error().code == EBUSY && endpoint.registration.live &&
                        endpoint.unregistered == 0 && endpoint.unimported == 0 && !endpoint.deleted_jfr &&
                        endpoint.created_jetties == 1 && !endpoint.deleted_jetties[0] && !endpoint.deleted_cqs[0] &&
                        !endpoint.deleted_cqs[1] && !endpoint.modified[0] && endpoint.flush_calls[0] == 0;
        }
        const auto& client = sessions_[1].stats();
        retained &= client.accepted_known && client.traffic.accepted == options_.tx_depth &&
                    client.traffic.flush_done == 0 && client.cleanup.accepted == 0;
        if (failure == Failure::kRnrContinuationError) {
            // post 成功不等于实验成功：同一 SQ 后续仍产生 RNR 时必须判为失败。
            retained &= client.traffic.retired == options_.tx_depth &&
                        client.continuation.accepted == options_.tx_depth &&
                        client.continuation.retired == options_.tx_depth && client.continuation.success == 0 &&
                        client.continuation.rnr_retry_count_exceeded_error == options_.tx_depth;
        } else {
            retained &= client.continuation.accepted == 0 && endpoints_[0].recv_batches.empty();
        }
        if (failure == Failure::kRnrQuery) {
            retained &= !results[1] && results[1].error().code == URMA_EINVAL;
        }
        // 与其它 DMA 失败用例相同，依赖链留给进程退出，避免夹具析构掩盖提前释放错误。
        std::_Exit(retained && !HasFailure() ? 0 : 1);
    }

    static inline BackpressureHardwareTest* active_;
    BackpressureOptions options_{.bytes = 64, .tx_depth = 3, .rounds = 3, .rx_delay_us = 1, .timeout_ms = 1000};
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    std::array<Endpoint, 2> endpoints_{};
    bool terminal_error_ = false;
    std::size_t remote_abort_count_ = 0;
    bool late_original_ = false;
    bool corrupted_ = false;
    Work late_work_{};
    Failure failure_ = Failure::kNone;
    std::size_t polls_at_initial_send_ = 0;
    raw::test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    std::array<BackpressureSession, 2> sessions_{};
};

class BackpressureQuotaTest : public BackpressureHardwareTest, public ::testing::WithParamInterface<int> {};

TEST_P(BackpressureQuotaTest, ExhaustsSqQuotaAndRetriesOnlyTheUnacceptedExtraMessage) {
    options_.tx_depth = GetParam();
    options_.bytes = options_.tx_depth == 1 ? 64 : 4096;
    ASSERT_TRUE(OpenBoth());
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }
    const auto& client = sessions_[1].stats();
    const auto& server = sessions_[0].stats();
    const auto count = options_.rounds * (options_.tx_depth + 1);
    // Ledger 比 SQ 多一个槽；每轮先填满 SQ，额外 WR 必须在 provider 之前被拒绝。
    EXPECT_EQ(client.traffic.accepted, count);
    EXPECT_EQ(client.traffic.retired, count);
    EXPECT_EQ(client.traffic.success, count);
    EXPECT_EQ(server.traffic.received, count);
    EXPECT_EQ(client.quota_rejections, options_.rounds);
    EXPECT_EQ(client.recovery_sends, options_.rounds);
    EXPECT_EQ(client.provider_eagain, 0u);
    EXPECT_TRUE(client.accepted_known);
    EXPECT_FALSE(client.terminal_failure);
    EXPECT_EQ(endpoints_[1].send_batches.size(), 2 * options_.rounds);
    EXPECT_EQ(endpoints_[1].submitted_frames.size(), count);
    EXPECT_FALSE(endpoints_[1].modified[0]);
    // 深度 256 需要 257 个 RX buffer，必须拆成合法的 256 + 1 投递批次。
    EXPECT_EQ(endpoints_[0].recv_batches.size(), options_.rounds * (options_.tx_depth == 256 ? 2 : 1));
    if (options_.tx_depth == 256) {
        for (std::size_t i = 0; i < endpoints_[0].recv_batches.size(); i += 2) {
            EXPECT_EQ(endpoints_[0].recv_batches[i], 256u);
            EXPECT_EQ(endpoints_[0].recv_batches[i + 1], 1u);
        }
    }
    CloseBoth();
}

INSTANTIATE_TEST_SUITE_P(QueueDepths, BackpressureQuotaTest, ::testing::Values(1, 32, 256));

class BackpressureRnrContinueTest : public BackpressureHardwareTest, public ::testing::WithParamInterface<int> {};

TEST_P(BackpressureRnrContinueTest, SameSqSendsNewMessagesAfterOriginalAttemptsRetireWithRnr) {
    ConfigureRnrContinue();
    options_.tx_depth = GetParam();
    options_.bytes = options_.tx_depth == 1 ? 64 : 4096;
    ASSERT_TRUE(OpenBoth());
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }
    const auto& client = sessions_[1].stats();
    const auto& server = sessions_[0].stats();
    // 原始批次全部因无 RX 耗尽 RNR 重试；这里只退休 WR，不制造 ERROR/FLUSH 边界。
    EXPECT_EQ(client.traffic.accepted, options_.tx_depth);
    EXPECT_EQ(client.traffic.retired, options_.tx_depth);
    EXPECT_EQ(client.traffic.rnr_retry_count_exceeded_error, options_.tx_depth);
    EXPECT_EQ(client.traffic.success, 0u);
    EXPECT_EQ(client.traffic.flush_done, 0u);
    EXPECT_EQ(server.traffic.received, 0u);
    EXPECT_EQ(endpoints_[1].sends_before_rx, options_.tx_depth);
    EXPECT_EQ(endpoints_[1].rnr_polled, options_.tx_depth);
    // 第二批是 round=1 的新消息；同一 jetty 成功收发，并且两端只创建过一个物理 jetty。
    EXPECT_EQ(client.continuation.accepted, options_.tx_depth);
    EXPECT_EQ(client.continuation.retired, options_.tx_depth);
    EXPECT_EQ(client.continuation.success, options_.tx_depth);
    EXPECT_EQ(client.continuation.flush_done, 0u);
    EXPECT_EQ(server.continuation.received, options_.tx_depth);
    EXPECT_EQ(client.cleanup.accepted, 0u);
    EXPECT_EQ(endpoints_[1].send_batches, (std::vector<std::size_t>{options_.tx_depth, options_.tx_depth}));
    EXPECT_EQ(endpoints_[0].recv_batches, (std::vector<std::size_t>{options_.tx_depth}));
    EXPECT_EQ(endpoints_[1].submitted_frames.size(), 2 * options_.tx_depth);
    EXPECT_EQ(endpoints_[0].created_jetties, 1u);
    EXPECT_EQ(endpoints_[1].created_jetties, 1u);
    // 三个阶段都查询同一个对象；输出采用 provider 返回值，不能拿创建参数冒充硬件读回值。
    EXPECT_EQ(client.jetty_id, endpoints_[1].jetties[0].jetty_id.id);
    EXPECT_EQ(client.state_checks, 3u);
    EXPECT_EQ(endpoints_[1].queries, 3u);
    EXPECT_EQ(client.queried_rnr_retry, 2u);
    EXPECT_EQ(client.queried_err_timeout, 5u);
    EXPECT_EQ(server.state_checks, 0u);
    EXPECT_TRUE(client.accepted_known);
    EXPECT_EQ(client.provider_eagain, 0u);
    CloseBoth();
}

INSTANTIATE_TEST_SUITE_P(QueueDepths, BackpressureRnrContinueTest, ::testing::Values(1, 32, 256));

TEST_F(BackpressureHardwareTest, DelayedReceivePostingCompletesAcceptedSendsWithoutReplay) {
    options_.mode = BackpressureMode::kRxDelay;
    options_.expect = BackpressureExpectation::kSuccess;
    ASSERT_TRUE(OpenBoth());
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }
    const auto& client = sessions_[1].stats();
    EXPECT_EQ(endpoints_[1].sends_before_rx, options_.tx_depth);
    EXPECT_EQ(endpoints_[1].send_batches.size(), 1u);
    EXPECT_EQ(client.traffic.accepted, options_.tx_depth);
    EXPECT_EQ(client.traffic.retired, options_.tx_depth);
    EXPECT_EQ(client.traffic.success, options_.tx_depth);
    EXPECT_EQ(sessions_[0].stats().traffic.received, options_.tx_depth);
    EXPECT_EQ(client.cleanup.accepted, 0u);
    EXPECT_EQ(client.traffic.flush_done, 0u);
    EXPECT_FALSE(client.terminal_failure);
    CloseBoth();
}

TEST_F(BackpressureHardwareTest, RetryExhaustionDrainsOriginalSqAndUsesDistinctCleanupMessages) {
    options_.mode = BackpressureMode::kRxDelay;
    options_.expect = BackpressureExpectation::kError;
    terminal_error_ = true;
    ASSERT_TRUE(OpenBoth());
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }
    const auto& client = sessions_[1].stats();
    // 原始数据全部失败，错误 SQ 退休完毕后另一个 SQ 消耗残留 RX，不重放任何原始消息。
    EXPECT_TRUE(client.terminal_failure);
    EXPECT_EQ(client.traffic.accepted, options_.tx_depth);
    EXPECT_EQ(client.traffic.retired, options_.tx_depth);
    EXPECT_EQ(client.traffic.success, 0u);
    EXPECT_EQ(client.traffic.rnr_retry_count_exceeded_error, 1u);
    EXPECT_EQ(client.traffic.flush_error, options_.tx_depth - 2);
    EXPECT_EQ(client.traffic.unhandled, 1u);
    EXPECT_EQ(client.traffic.flush_done, 1u);
    EXPECT_GT(client.traffic.rejected_sends, 0u);
    EXPECT_EQ(client.cleanup.accepted, options_.tx_depth);
    EXPECT_EQ(client.cleanup.retired, options_.tx_depth);
    EXPECT_EQ(client.cleanup.success, options_.tx_depth);
    EXPECT_EQ(sessions_[0].stats().traffic.received, 0u);
    EXPECT_EQ(sessions_[0].stats().cleanup.received, options_.tx_depth);
    EXPECT_EQ(endpoints_[1].send_batches.size(), 2u);
    EXPECT_GE(endpoints_[1].flush_calls[0], 2u);
    CloseBoth();
}

class BackpressureRemoteAbortTest : public BackpressureHardwareTest, public ::testing::WithParamInterface<int> {};

TEST_P(BackpressureRemoteAbortTest, FailedDelayExperimentExplicitlyClosesDedicatedSqWithoutReplay) {
    options_.mode = BackpressureMode::kRxDelay;
    options_.expect = BackpressureExpectation::kError;
    terminal_error_ = true;
    remote_abort_count_ = GetParam();
    ASSERT_TRUE(OpenBoth());
    // status 8 只退休失败 WR。工具观察到失败后才主动结束独占 SQ 的实验，不能依赖完成层判坏 SQ。
    EXPECT_CALL(*this, Modify(&endpoints_[1].jetties[0], _)).Times(1).WillOnce([&](auto* jetty, auto* attr) {
        EXPECT_EQ(sessions_[1].stats().traffic.remote_access_abort_error, remote_abort_count_);
        return EnterError(jetty, attr);
    });
    EXPECT_CALL(*this, Modify(&endpoints_[1].jetties[1], _)).Times(0);
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }

    const auto& client = sessions_[1].stats();
    const auto remaining = options_.tx_depth - remote_abort_count_;
    // 即使整批已用 status 8 退休，报告仍须等显式关闭的边界；部分完成时还要取回所有残留 WR。
    EXPECT_TRUE(client.terminal_failure);
    EXPECT_EQ(client.traffic.accepted, options_.tx_depth);
    EXPECT_EQ(client.traffic.retired, options_.tx_depth);
    EXPECT_EQ(client.traffic.success, 0u);
    EXPECT_EQ(client.traffic.remote_access_abort_error, remote_abort_count_);
    EXPECT_EQ(client.traffic.flush_error, remaining > 0 ? remaining - 1 : 0);
    EXPECT_EQ(client.traffic.unhandled, remaining > 0 ? 1u : 0u);
    EXPECT_EQ(client.traffic.flush_done, 1u);
    EXPECT_GT(client.traffic.rejected_sends, 0u);
    EXPECT_TRUE(endpoints_[1].boundary_polled[0]);
    EXPECT_EQ(endpoints_[1].flush_calls[0], remaining > 0 ? 2u : 1u);
    EXPECT_EQ(sessions_[0].stats().traffic.flush_done, 1u);
    EXPECT_EQ(sessions_[0].stats().traffic.received, 0u);

    // 清理使用第二条 SQ 和不同的 payload 身份，原始消息不得重放，MR 最后才允许释放。
    EXPECT_EQ(client.cleanup.accepted, options_.tx_depth);
    EXPECT_EQ(client.cleanup.retired, options_.tx_depth);
    EXPECT_EQ(client.cleanup.success, options_.tx_depth);
    EXPECT_EQ(client.cleanup.flush_done, 0u);
    EXPECT_EQ(sessions_[0].stats().cleanup.received, options_.tx_depth);
    EXPECT_EQ(endpoints_[1].send_batches, (std::vector<std::size_t>{options_.tx_depth, options_.tx_depth}));
    EXPECT_EQ(endpoints_[1].submitted_frames.size(), 2 * options_.tx_depth);
    CloseBoth();
}

INSTANTIATE_TEST_SUITE_P(CompletedBeforeClose, BackpressureRemoteAbortTest, ::testing::Values(1, 3));

TEST_F(BackpressureHardwareTest, LateOriginalReceiveCannotTurnCleanupOverpostIntoReplayOrEarlyFree) {
    options_.mode = BackpressureMode::kRxDelay;
    options_.expect = BackpressureExpectation::kError;
    terminal_error_ = true;
    late_original_ = true;
    ASSERT_TRUE(OpenBoth());
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }
    const auto& client = sessions_[1].stats();
    const auto& server = sessions_[0].stats();
    // 清理快照认为还有 D 个 RX，实际只剩 D-1；迟到原始 RX 与清理 RX 必须分别记账。
    EXPECT_EQ(client.traffic.ack_timeout_error, 1u);
    EXPECT_EQ(client.traffic.success, 0u);
    EXPECT_EQ(server.traffic.received, 1u);
    EXPECT_EQ(client.cleanup.accepted, options_.tx_depth);
    EXPECT_EQ(client.cleanup.retired, options_.tx_depth);
    EXPECT_EQ(client.cleanup.success, options_.tx_depth - 1);
    EXPECT_EQ(client.cleanup.rnr_retry_count_exceeded_error, 1u);
    EXPECT_EQ(client.cleanup.flush_done, 1u);
    EXPECT_EQ(server.cleanup.received, options_.tx_depth - 1);
    EXPECT_TRUE(endpoints_[1].modified[0]);
    EXPECT_TRUE(endpoints_[1].modified[1]);
    EXPECT_TRUE(endpoints_[0].deferred_rx.empty());
    EXPECT_EQ(endpoints_[1].submitted_frames.size(), 2 * options_.tx_depth);
    CloseBoth();
}

TEST_F(BackpressureHardwareTest, PeerMismatchFailsBeforeImportOrPostingAndCanClose) {
    ASSERT_TRUE(sessions_[1].Open(&endpoints_[1].ctx, cap_, options_, false));
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    BackpressureHello hello{.options = options_, .server = true};
    ++hello.options.bytes;
    hello.endpoints[0] = endpoints_[0].jetties[0].jetty_id;
    ASSERT_TRUE(peer.Write(EncodeBackpressureHello(hello), options_.timeout_ms));
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    const auto result = sessions_[1].Run(local);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    ASSERT_TRUE(sessions_[1].Close());
    ExpectClosed(endpoints_[1]);
}

TEST_F(BackpressureHardwareTest, DeleteFailurePreservesParentQueuesAndCloseCanRetry) {
    options_.mode = BackpressureMode::kRxDelay;
    ASSERT_TRUE(sessions_[1].Open(&endpoints_[1].ctx, cap_, options_, false));
    // 剩余 jetty 仍引用 JFR/JFC，删除失败不能继续销毁父队列；重试也不能重复释放已删除对象。
    EXPECT_CALL(*this, DeleteJetty(&endpoints_[1].jetties[1])).WillOnce(Return(URMA_EAGAIN)).RetiresOnSaturation();
    const auto first = sessions_[1].Close();
    ASSERT_FALSE(first);
    EXPECT_EQ(first.error().code, URMA_EAGAIN);
    EXPECT_FALSE(endpoints_[1].deleted_jfr);
    EXPECT_FALSE(endpoints_[1].deleted_cqs[0]);
    EXPECT_FALSE(endpoints_[1].deleted_cqs[1]);
    EXPECT_TRUE(endpoints_[1].registration.live);
    ASSERT_TRUE(sessions_[1].Close());
    ExpectClosed(endpoints_[1]);
}

TEST_F(BackpressureHardwareTest, MissingFlushBoundaryTimesOutAndRetainsDmaDependencies) {
    EXPECT_EXIT(RunFailureCase(Failure::kMissingBoundary), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, ModifyFailureKeepsAcceptedAttemptsAndPostedReceiveBuffers) {
    EXPECT_EXIT(RunFailureCase(Failure::kModify), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RemoteAbortExperimentCloseFailureRetainsPendingAttemptsAndReceiveBuffers) {
    // 远端错误本身已退休；工具主动关闭失败后仍不能释放剩余 WR、已投 RX 及其 DMA 依赖。
    remote_abort_count_ = 1;
    EXPECT_EXIT(RunFailureCase(Failure::kModify), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, UnknownAcceptedPrefixFailsWithoutReplayingOrReleasingMemory) {
    EXPECT_EXIT(RunFailureCase(Failure::kUnknownAccepted), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, CorruptPayloadFailsInsteadOfReportingSuccessfulRecovery) {
    EXPECT_EXIT(RunFailureCase(Failure::kPayload), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, ProviderEagainIsReportedSeparatelyFromLocalQuotaExhaustion) {
    EXPECT_EXIT(RunFailureCase(Failure::kProviderEagain), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, ProviderEagainPreservesTrustedAcceptedPrefixWithoutRetryingTheBatch) {
    EXPECT_EXIT(RunFailureCase(Failure::kProviderPartial), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, ErrorReceiveCannotSubstituteForMissingSuccessfulAttemptByMatchingCounts) {
    EXPECT_EXIT(RunFailureCase(Failure::kMissingSuccessfulRx), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinueQueryFailureCannotReportVerifiedRecovery) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrQuery), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinueRejectsSqThatChangedToErrorWithoutSoftwareModification) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrState), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinueRejectsErrorSuspendModeReportedByProvider) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrSuspend), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinueDoesNotAcceptOtherTerminalErrorsAsRnrEvidence) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrInitialError), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinuePostSuccessWithRnrCompletionsIsNotSuccessfulContinuation) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrContinuationError), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinueRejectsFlushBoundaryWithoutInterpretingItsUserContext) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrBoundary), ::testing::ExitedWithCode(0), "");
}

TEST_F(BackpressureHardwareTest, RnrContinueRejectsDuplicateCompletionInsteadOfRetiringTwice) {
    EXPECT_EXIT(RunRnrFailureCase(Failure::kRnrDuplicate), ::testing::ExitedWithCode(0), "");
}
} // namespace
} // namespace tools
} // namespace kbsocket
