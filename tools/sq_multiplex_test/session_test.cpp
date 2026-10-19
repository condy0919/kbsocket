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
#include <tuple>
#include <vector>

#include "tools/sq_multiplex_test/sq_multiplex_test.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;

// 两端运行真实 Session、账本和 RX 池；互斥锁仅串行化 mock 的跨端 DMA/CQE 操作。
class SqMultiplexHardwareTest : public ::testing::Test {
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
        kOtherStatus,
        kPseudoBoundary,
        kDuplicateCompletion,
        kWrongLocalId,
        kPayload,
        kImmediate,
        kQueryState,
        kProviderPartial,
        kUnknownAccepted,
        kMissingCompletion
    };
    struct PostedBuffer {
        urma_sge_t sge;
        std::uint64_t user_ctx;
    };
    struct Queue {
        std::array<urma_jfc_t, 2> cqs{};
        urma_jfr_t jfr{};
        urma_jetty_t jetty{};
        std::deque<PostedBuffer> buffers;
        std::array<std::deque<urma_cr_t>, 2> completions;
        bool deleted_jetty = false;
        bool deleted_jfr = false;
        std::array<bool, 2> deleted_cqs{};
    };
    struct Registration {
        urma_target_seg_t segment{};
        std::uint64_t address = 0;
        std::uint64_t length = 0;
        bool live = false;
    };
    struct Endpoint {
        urma_context_t ctx{};
        std::array<Queue, 2> queues;
        std::array<urma_target_jetty_t, 2> imported{};
        Registration registration;
        std::size_t created_cqs = 0;
        std::size_t created_jfrs = 0;
        std::size_t created_jetties = 0;
        std::size_t registered = 0;
        std::size_t unregistered = 0;
        std::size_t imports = 0;
        std::array<bool, 2> unimported{};
        std::size_t queries = 0;
        std::vector<std::size_t> send_batches;
        std::vector<std::size_t> recv_batches;
        std::set<std::tuple<std::uint32_t, MultiplexPhase, bool, std::uint32_t>> submitted_frames;
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
        functions.flush_jetty = [](auto* q, int n, auto* cr) { return active_->Flush(q, n, cr); };
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

    void SetUp() override {
        active_ = this;
        cap_.max_msg_size = 4096;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 2;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 1024;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 1;
        for (std::size_t side = 0; side < endpoints_.size(); ++side) {
            for (std::size_t pool = 0; pool < 2; ++pool) {
                // 数字 ID 故意跨端重复，目标匹配必须同时核对 EID。
                auto& queue = endpoints_[side].queues[pool];
                queue.jetty.jetty_id.id = 10 + pool;
                queue.jetty.jetty_id.eid.raw[15] = side + 1;
                queue.jfr.jfr_id.id = 20 + pool;
            }
        }
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            const auto index = endpoint.created_cqs++;
            const auto pool = index / 2;
            EXPECT_EQ(cfg->depth,
                      index % 2 == 0 ? (pool == 0 ? 2 * options_.batch + 1 : 2) : (pool == 0 ? options_.batch : 1));
            return &endpoint.queues.at(pool).cqs[index % 2];
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            const auto pool = endpoint.created_jfrs++;
            auto& queue = endpoint.queues.at(pool);
            EXPECT_EQ(cfg->jfc, &queue.cqs[1]);
            EXPECT_EQ(cfg->depth, pool == 0 ? options_.batch : 1);
            return &queue.jfr;
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            const auto pool = endpoint.created_jetties++;
            auto& queue = endpoint.queues.at(pool);
            EXPECT_EQ(cfg->flag.bs.share_jfr, 1u);
            EXPECT_EQ(cfg->shared.jfr, &queue.jfr);
            EXPECT_EQ(cfg->shared.jfc, &queue.cqs[1]);
            EXPECT_EQ(cfg->jfs_cfg.jfc, &queue.cqs[0]);
            EXPECT_EQ(cfg->jfs_cfg.depth, pool == 0 ? 2 * options_.batch : 1);
            EXPECT_EQ(cfg->jfs_cfg.flag.bs.error_suspend, 0u);
            queue.jetty.jetty_cfg = *cfg;
            return &queue.jetty;
        });
        EXPECT_CALL(*this, Register(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(++endpoint.registered, 1u);
            const auto page = static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
            EXPECT_EQ(cfg->va % page, 0u);
            EXPECT_EQ(cfg->len % page, 0u);
            EXPECT_EQ(cfg->flag.bs.access,
                      static_cast<unsigned>(URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC));
            endpoint.registration.address = cfg->va;
            endpoint.registration.length = cfg->len;
            endpoint.registration.live = true;
            return &endpoint.registration.segment;
        });
        EXPECT_CALL(*this, Import(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg, auto*) {
            auto& client = endpoints_[1];
            EXPECT_EQ(ctx, &client.ctx);
            const auto target = client.imports++;
            EXPECT_EQ(cfg->type, URMA_JETTY);
            EXPECT_EQ(cfg->trans_mode, URMA_TM_RM);
            EXPECT_EQ(cfg->tp_type, URMA_CTP);
            EXPECT_TRUE(SameEndpoint(cfg->jetty_id, endpoints_[0].queues.at(target).jetty.jetty_id));
            client.imported.at(target).id = cfg->jetty_id;
            return &client.imported.at(target);
        });
        EXPECT_CALL(*this, PostRecv(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr, auto* wr, auto**) {
            std::lock_guard lock(mutex_);
            auto& server = endpoints_[0];
            // sink 必须有独立 JFR 且从不投 RX，否则无法把 RNR 归因到指定目标。
            EXPECT_EQ(jfr, &server.queues[0].jfr);
            EXPECT_NE(jfr, &server.queues[1].jfr);
            EXPECT_TRUE(server.queues[0].buffers.empty());
            std::size_t count = 0;
            for (; wr; wr = wr->next) {
                EXPECT_EQ(wr->src.num_sge, 1u);
                EXPECT_EQ(wr->src.sge[0].len, raw::RxBufferPool::kBufferSize);
                EXPECT_TRUE(IsRegistered(server, wr->src.sge[0]));
                server.queues[0].buffers.push_back({wr->src.sge[0], wr->user_ctx});
                ++count;
            }
            EXPECT_EQ(count, options_.batch);
            server.recv_batches.push_back(count);
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostSend(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* wr, auto** bad) {
            return Submit(jetty, wr, bad);
        });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* cq, int limit, auto* out) {
            return PollCompletions(cq, limit, out);
        });
        EXPECT_CALL(*this, Query(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* cfg, auto* attr) {
            std::lock_guard lock(mutex_);
            auto& client = endpoints_[1];
            EXPECT_EQ(jetty, &client.queues[0].jetty);
            EXPECT_TRUE(client.queues[0].completions[0].empty());
            EXPECT_EQ(++client.queries, client.send_batches.size() + 1);
            *cfg = jetty->jetty_cfg;
            // 查询值故意不同于请求值，防止工具把创建参数误报成 provider 读回结果。
            cfg->jfs_cfg.rnr_retry = 2;
            cfg->jfs_cfg.err_timeout = 5;
            attr->state = client.queries == 3 && failure_ == Failure::kQueryState ? URMA_JETTY_STATE_ERROR
                                                                                  : URMA_JETTY_STATE_READY;
            return URMA_SUCCESS;
        });
        // 主动 ERROR/flush 会改变硬件的自然 RNR 行为，使本实验失去意义。
        EXPECT_CALL(*this, Modify(_, _)).Times(0);
        EXPECT_CALL(*this, Flush(_, _, _)).Times(0);
        InstallCleanup();
    }

    void Deliver(const urma_sge_t& sge, const MultiplexFrame& frame, std::uint64_t immediate) {
        auto& server = endpoints_[0];
        auto& queue = server.queues[0];
        ASSERT_FALSE(queue.buffers.empty());
        const auto buffer = queue.buffers.front();
        queue.buffers.pop_front();
        ASSERT_TRUE(IsRegistered(server, buffer.sge));
        std::memcpy(reinterpret_cast<void*>(buffer.sge.addr), reinterpret_cast<const void*>(sge.addr), sge.len);
        urma_cr_t rx{};
        rx.flag.bs.jetty = 1;
        rx.flag.bs.s_r = 1;
        rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
        rx.local_id = queue.jetty.jetty_id.id;
        rx.remote_id = endpoints_[1].queues[0].jetty.jetty_id;
        rx.user_ctx = buffer.user_ctx;
        rx.completion_len = sge.len;
        rx.imm_data = immediate;
        if (frame.phase == MultiplexPhase::kMixed && frame.index == 0) {
            if (failure_ == Failure::kPayload) {
                reinterpret_cast<std::byte*>(buffer.sge.addr)[kMultiplexPayloadHeader] ^= std::byte{1};
                corrupted_ = true;
            } else if (failure_ == Failure::kImmediate) {
                rx.imm_data ^= 1;
                corrupted_ = true;
            }
        }
        // RM_CTP 不保证接收顺序：反转 RX CQE，仍必须逐条校验 payload 与 IMM 身份。
        queue.completions[1].push_front(rx);
    }

    urma_status_t Submit(urma_jetty_t* jetty, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
        std::lock_guard lock(mutex_);
        auto& client = endpoints_[1];
        EXPECT_EQ(jetty, &client.queues[0].jetty);
        EXPECT_TRUE(client.queues[0].completions[0].empty());
        EXPECT_TRUE(endpoints_[0].queues[1].buffers.empty());
        std::deque<urma_cr_t> healthy, sink;
        std::size_t count = 0;
        bool mixed = false;
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
            EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
            EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
            EXPECT_EQ(wr->send.src.num_sge, 1u);
            const auto& sge = wr->send.src.sge[0];
            EXPECT_TRUE(IsRegistered(client, sge));
            EXPECT_EQ(sge.len, options_.bytes);
            const auto frame = CheckMultiplexPayload({reinterpret_cast<const std::byte*>(sge.addr), sge.len});
            if (!frame) {
                ADD_FAILURE() << "invalid payload submitted";
                *bad = wr;
                return URMA_EINVAL;
            }
            mixed = frame->phase == MultiplexPhase::kMixed;
            EXPECT_EQ(frame->sink, mixed && count % 2 == 0);
            EXPECT_EQ(frame->index, mixed ? count / 2 : count);
            EXPECT_EQ(wr->tjetty, &client.imported[frame->sink]);
            EXPECT_EQ(wr->send.imm_data, MultiplexImmediate(*frame));
            EXPECT_TRUE(client.submitted_frames.emplace(frame->round, frame->phase, frame->sink, frame->index).second)
                << "accepted data must never be replayed";
            urma_cr_t completion{};
            completion.flag.bs.jetty = 1;
            completion.local_id = jetty->jetty_id.id;
            completion.user_ctx = wr->user_ctx;
            completion.status = frame->sink ? URMA_CR_RNR_RETRY_CNT_EXC_ERR : URMA_CR_SUCCESS;
            if (!frame->sink) {
                Deliver(sge, *frame, wr->send.imm_data);
            }
            if (mixed && count == 0) {
                if (failure_ == Failure::kOtherStatus) {
                    completion.status = URMA_CR_ACK_TIMEOUT_ERR;
                } else if (failure_ == Failure::kPseudoBoundary) {
                    // 伪 CQE 的 user_ctx 不能进入账本；其值刻意不对应任何真实 attempt。
                    completion.status = URMA_CR_WR_FLUSH_ERR_DONE;
                    completion.user_ctx = UINT64_MAX;
                } else if (failure_ == Failure::kWrongLocalId) {
                    ++completion.local_id;
                }
            }
            auto& target = frame->sink ? sink : healthy;
            if (!(mixed && count == 0 && failure_ == Failure::kMissingCompletion)) {
                target.push_front(completion);
            }
            if (mixed && count == 0 && failure_ == Failure::kDuplicateCompletion) {
                target.push_back(completion);
            }
            ++count;
            if (mixed && failure_ == Failure::kProviderPartial) {
                // 首条已经接受，后缀未接受；不能重发整批或遗忘已接受的 sink attempt。
                EXPECT_NE(wr->next, nullptr);
                *bad = wr->next;
                break;
            }
        }
        auto& output = client.queues[0].completions[0];
        const auto append = [&](const auto& completions) {
            output.insert(output.end(), completions.begin(), completions.end());
        };
        if (healthy_first_) {
            append(healthy);
            append(sink);
        } else {
            append(sink);
            append(healthy);
        }
        client.send_batches.push_back(count);
        if (mixed && (failure_ == Failure::kProviderPartial || failure_ == Failure::kUnknownAccepted)) {
            return URMA_EAGAIN;
        }
        EXPECT_EQ(count, mixed ? 2 * options_.batch : options_.batch);
        return URMA_SUCCESS;
    }

    int PollCompletions(urma_jfc_t* cq, int limit, urma_cr_t* out) {
        std::lock_guard lock(mutex_);
        for (auto& endpoint : endpoints_) {
            for (auto& queue : endpoint.queues) {
                for (std::size_t index = 0; index < queue.cqs.size(); ++index) {
                    if (cq != &queue.cqs[index]) {
                        continue;
                    }
                    auto& completions = queue.completions[index];
                    const auto count = std::min(completions.size(), static_cast<std::size_t>(limit));
                    for (std::size_t i = 0; i < count; ++i) {
                        out[i] = completions.front();
                        completions.pop_front();
                    }
                    return static_cast<int>(count);
                }
            }
        }
        ADD_FAILURE() << "unknown completion queue";
        return -1;
    }

    void InstallCleanup() {
        EXPECT_CALL(*this, Unimport(_)).Times(AnyNumber()).WillRepeatedly([&](auto* remote) {
            auto& client = endpoints_[1];
            const auto target = static_cast<std::size_t>(remote == &client.imported[1]);
            EXPECT_EQ(remote, &client.imported[target]);
            EXPECT_FALSE(client.unimported[target]);
            client.unimported[target] = true;
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty) {
            for (auto& endpoint : endpoints_) {
                for (auto& queue : endpoint.queues) {
                    if (jetty == &queue.jetty) {
                        EXPECT_TRUE(queue.buffers.empty());
                        EXPECT_TRUE(queue.completions[0].empty());
                        EXPECT_TRUE(queue.completions[1].empty());
                        EXPECT_FALSE(queue.deleted_jetty);
                        queue.deleted_jetty = true;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unknown jetty";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr) {
            for (auto& endpoint : endpoints_) {
                for (auto& queue : endpoint.queues) {
                    if (jfr == &queue.jfr) {
                        EXPECT_TRUE(queue.deleted_jetty);
                        EXPECT_FALSE(queue.deleted_jfr);
                        queue.deleted_jfr = true;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unknown JFR";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([&](auto* cq) {
            for (auto& endpoint : endpoints_) {
                for (auto& queue : endpoint.queues) {
                    for (std::size_t index = 0; index < queue.cqs.size(); ++index) {
                        if (cq == &queue.cqs[index]) {
                            EXPECT_TRUE(queue.deleted_jfr);
                            EXPECT_FALSE(queue.deleted_cqs[index]);
                            queue.deleted_cqs[index] = true;
                            return URMA_SUCCESS;
                        }
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
                    for (auto& queue : endpoint.queues) {
                        EXPECT_TRUE(queue.buffers.empty());
                        EXPECT_TRUE(queue.completions[0].empty());
                        EXPECT_TRUE(queue.completions[1].empty());
                    }
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
        for (std::size_t side = 0; side < sessions_->size(); ++side) {
            const auto result = (*sessions_)[side].Open(&endpoints_[side].ctx, cap_, options_, side == 0);
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
        std::thread worker([&] { results[0] = (*sessions_)[0].Run(server); });
        results[1] = (*sessions_)[1].Run(client);
        worker.join();
        return results;
    }

    void CloseBoth() {
        for (std::size_t side = 0; side < sessions_->size(); ++side) {
            ASSERT_TRUE((*sessions_)[side].Close());
            ASSERT_TRUE((*sessions_)[side].Close());
            const auto& endpoint = endpoints_[side];
            EXPECT_EQ(endpoint.unregistered, endpoint.registered);
            EXPECT_EQ(
                static_cast<std::size_t>(std::count(endpoint.unimported.begin(), endpoint.unimported.end(), true)),
                endpoint.imports);
            for (std::size_t pool = 0; pool < endpoint.created_jetties; ++pool) {
                const auto& queue = endpoint.queues[pool];
                EXPECT_TRUE(queue.deleted_jetty);
                EXPECT_TRUE(queue.deleted_jfr);
                EXPECT_TRUE(queue.deleted_cqs[0]);
                EXPECT_TRUE(queue.deleted_cqs[1]);
            }
        }
    }

    void RunFailureCase(Failure failure) {
        failure_ = failure;
        options_.timeout_ms = 100;
        if (failure == Failure::kUnknownAccepted) {
            // provider 违反 bad_wr 契约时，TxSender 必须封锁 SQ；这条保护与普通 RNR 的观测路径不同。
            EXPECT_CALL(*this, Modify(&endpoints_[1].queues[0].jetty, _)).WillOnce([&](auto*, auto* attr) {
                EXPECT_EQ(attr->mask, JETTY_STATE);
                EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
                modified_ = true;
                return URMA_SUCCESS;
            });
        }
        if (!OpenBoth()) {
            std::_Exit(2);
        }
        const auto results = RunBoth();
        bool retained = !results[0] && !results[1];
        for (std::size_t side = 0; side < sessions_->size(); ++side) {
            const auto closed = (*sessions_)[side].Close();
            const auto& endpoint = endpoints_[side];
            // 失败没有形成双端静默证明；即使本端已退休部分/全部 WR，也不能提前拆掉依赖链。
            retained &= !closed && closed.error().code == EBUSY && endpoint.registration.live &&
                        endpoint.unregistered == 0 &&
                        std::none_of(endpoint.unimported.begin(), endpoint.unimported.end(), [](bool x) { return x; });
            for (const auto& queue : endpoint.queues) {
                retained &=
                    !queue.deleted_jetty && !queue.deleted_jfr && !queue.deleted_cqs[0] && !queue.deleted_cqs[1];
            }
        }
        const auto& client = (*sessions_)[1].stats();
        retained &= client.healthy[0].success == options_.batch && client.healthy[2].accepted == 0;
        if (failure == Failure::kProviderPartial) {
            retained &= client.accepted_known && client.provider_eagain == 1 && client.sink.accepted == 1 &&
                        client.healthy[1].accepted == 0 && endpoints_[1].send_batches.size() == 2 &&
                        endpoints_[1].send_batches.back() == 1;
        } else if (failure == Failure::kUnknownAccepted) {
            retained &= !client.accepted_known && modified_;
        } else if (failure == Failure::kMissingCompletion) {
            retained &=
                !results[1] && results[1].error().code == ETIMEDOUT && client.sink.retired == options_.batch - 1;
        } else if (failure == Failure::kPayload || failure == Failure::kImmediate) {
            retained &= corrupted_;
        }
        // 常驻 Session 不自动析构；_Exit 让未决 DMA 资源随子进程消失，不在测试中假装安全关闭。
        std::_Exit(retained && !HasFailure() ? 0 : 1);
    }

    static inline SqMultiplexHardwareTest* active_;
    SqMultiplexOptions options_{.bytes = 64, .batch = 3, .rounds = 2, .timeout_ms = 1000};
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    std::array<Endpoint, 2> endpoints_{};
    bool healthy_first_ = true;
    bool corrupted_ = false;
    bool modified_ = false;
    Failure failure_ = Failure::kNone;
    raw::test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    NoDestructor<std::array<SqMultiplexSession, 2>> sessions_;
};

class SqMultiplexOrderingTest : public SqMultiplexHardwareTest,
                                public ::testing::WithParamInterface<std::tuple<int, bool>> {};

TEST_P(SqMultiplexOrderingTest, OnePhysicalSqServesBothTargetsAndContinuesWithoutReplay) {
    options_.batch = std::get<0>(GetParam());
    options_.bytes = options_.batch == 1 ? 64 : 4096;
    healthy_first_ = std::get<1>(GetParam());
    ASSERT_TRUE(OpenBoth());
    const auto results = RunBoth();
    for (const auto& result : results) {
        ASSERT_TRUE(result) << result.error().operation << ": " << result.error().code;
    }
    const auto& client = (*sessions_)[1].stats();
    const auto& server = (*sessions_)[0].stats();
    const auto count = options_.batch * options_.rounds;
    // 三阶段健康流量都成功；只有未投 RX 的目标产生 RNR，同一个 SQ 不重建也不重放。
    for (std::size_t phase = 0; phase < 3; ++phase) {
        EXPECT_EQ(client.healthy[phase].accepted, count);
        EXPECT_EQ(client.healthy[phase].retired, count);
        EXPECT_EQ(client.healthy[phase].success, count);
        EXPECT_EQ(client.healthy[phase].rnr, 0u);
        EXPECT_EQ(client.healthy[phase].other_error, 0u);
        EXPECT_EQ(server.healthy[phase].received, count);
    }
    EXPECT_EQ(client.sink.accepted, count);
    EXPECT_EQ(client.sink.retired, count);
    EXPECT_EQ(client.sink.rnr, count);
    EXPECT_EQ(client.sink.success, 0u);
    EXPECT_EQ(client.sink.other_error, 0u);
    EXPECT_EQ(server.sink.received, 0u);
    EXPECT_TRUE(client.accepted_known);
    EXPECT_EQ(client.provider_eagain, 0u);
    EXPECT_EQ(endpoints_[0].created_jetties, 2u);
    EXPECT_EQ(endpoints_[0].created_jfrs, 2u);
    EXPECT_EQ(endpoints_[1].created_jetties, 1u);
    EXPECT_EQ(endpoints_[1].created_jfrs, 1u);
    EXPECT_EQ(endpoints_[1].submitted_frames.size(), 4 * count);
    EXPECT_EQ(endpoints_[1].send_batches.size(), 3 * options_.rounds);
    EXPECT_EQ(endpoints_[0].recv_batches.size(), 3 * options_.rounds);
    EXPECT_EQ(client.jetty_id, endpoints_[1].queues[0].jetty.jetty_id.id);
    EXPECT_EQ(client.state_checks, 1 + 3 * options_.rounds);
    EXPECT_EQ(endpoints_[1].queries, client.state_checks);
    EXPECT_EQ(client.queried_rnr_retry, 2u);
    EXPECT_EQ(client.queried_err_timeout, 5u);
    ASSERT_EQ(client.rounds.size(), options_.rounds);
    // 两种 CQ 顺序都是合法观测结果；测试只要求计数反映顺序，不把“健康先完成”写成通过条件。
    for (const auto& round : client.rounds) {
        EXPECT_EQ(round.healthy_before_first_rnr, healthy_first_ ? options_.batch : 0u);
        EXPECT_EQ(round.healthy_before_last_rnr, healthy_first_ ? options_.batch : 0u);
        EXPECT_GT(round.first_healthy_ns, 0u);
        EXPECT_GE(round.last_healthy_ns, round.first_healthy_ns);
        EXPECT_GT(round.first_rnr_ns, 0u);
        EXPECT_GE(round.last_rnr_ns, round.first_rnr_ns);
    }
    CloseBoth();
}

INSTANTIATE_TEST_SUITE_P(BatchesAndCompletionOrders, SqMultiplexOrderingTest,
                         ::testing::Combine(::testing::Values(1, 32, 128), ::testing::Bool()));

TEST_F(SqMultiplexHardwareTest, UnexpectedSinkErrorDoesNotMasqueradeAsRnrObservation) {
    EXPECT_EXIT(RunFailureCase(Failure::kOtherStatus), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, PseudoCompletionCannotUseInvalidContextToRetireAnAttempt) {
    EXPECT_EXIT(RunFailureCase(Failure::kPseudoBoundary), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, DuplicateCompletionCannotRetireTheSameAttemptTwice) {
    EXPECT_EXIT(RunFailureCase(Failure::kDuplicateCompletion), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, CompletionFromAnotherSqCannotMatchByAttemptIdAlone) {
    EXPECT_EXIT(RunFailureCase(Failure::kWrongLocalId), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, PayloadCorruptionFailsDespiteSuccessfulTxCompletion) {
    EXPECT_EXIT(RunFailureCase(Failure::kPayload), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, ImmediateCorruptionFailsDespiteValidPayload) {
    EXPECT_EXIT(RunFailureCase(Failure::kImmediate), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, SqLeavingReadyCannotBeReportedAsSuccessfulContinuation) {
    EXPECT_EXIT(RunFailureCase(Failure::kQueryState), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, PartialAcceptancePreservesTheAcceptedPrefixWithoutReplay) {
    EXPECT_EXIT(RunFailureCase(Failure::kProviderPartial), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, UnknownAcceptedPrefixRetainsAllDmaDependencies) {
    EXPECT_EXIT(RunFailureCase(Failure::kUnknownAccepted), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqMultiplexHardwareTest, MissingCompletionTimesOutWithoutReleasingOutstandingMemory) {
    EXPECT_EXIT(RunFailureCase(Failure::kMissingCompletion), ::testing::ExitedWithCode(0), "");
}
} // namespace
} // namespace tools
} // namespace kbsocket
