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

#include "tools/sq_isolation_test/sq_isolation_test.hpp"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Return;

// 两个真实 Session 各由一个 owner 推进；锁只用于 mock 在两个 owner 之间交付 DMA/CQE。
class SqIsolationHardwareTest : public ::testing::Test {
public:
    MOCK_METHOD(urma_jfc_t*, CreateJfc, (urma_context_t*, urma_jfc_cfg_t*));
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_jetty_t*, CreateJetty, (urma_context_t*, urma_jetty_cfg_t*));
    MOCK_METHOD(urma_target_seg_t*, Register, (urma_context_t*, urma_seg_cfg_t*));
    MOCK_METHOD(urma_target_jetty_t*, Import, (urma_context_t*, urma_rjetty_t*, urma_token_t*));
    MOCK_METHOD(urma_status_t, PostSend, (urma_jetty_t*, urma_jfs_wr_t*, urma_jfs_wr_t**));
    MOCK_METHOD(urma_status_t, PostRecv, (urma_jfr_t*, urma_jfr_wr_t*, urma_jfr_wr_t**));
    MOCK_METHOD(urma_status_t, Modify, (urma_jetty_t*, urma_jetty_attr_t*));
    MOCK_METHOD(int, Poll, (urma_jfc_t*, int, urma_cr_t*));
    MOCK_METHOD(int, Flush, (urma_jetty_t*, int, urma_cr_t*));
    MOCK_METHOD(urma_status_t, Unimport, (urma_target_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJetty, (urma_jetty_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
    MOCK_METHOD(urma_status_t, DeleteJfc, (urma_jfc_t*));
    MOCK_METHOD(urma_status_t, Unregister, (urma_target_seg_t*));

protected:
    enum class Failure { kNone, kMissingBoundary, kModify };
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
        bool tx = false;
    };
    struct Endpoint {
        urma_context_t ctx{};
        std::array<urma_jfc_t, 4> cqs{};
        std::array<urma_jfr_t, 2> jfrs{};
        std::array<urma_jetty_t, 3> jetties{};
        std::array<urma_target_jetty_t, 3> imports{};
        std::array<Registration, 2> registrations{};
        std::deque<PostedBuffer> buffers;
        std::array<std::deque<urma_cr_t>, 4> completions;
        std::deque<urma_cr_t> deferred_completions;
        std::deque<urma_cr_t> software_completions;
        std::deque<raw::AttemptId> bad_ids;
        std::array<std::uint64_t, 3> sent{};
        std::array<std::uint64_t, 3> normal_sent{};
        std::array<bool, 3> deleted_jetties{};
        std::array<bool, 2> deleted_jfrs{};
        std::array<bool, 4> deleted_cqs{};
        std::size_t created_cqs = 0;
        std::size_t created_jfrs = 0;
        std::size_t created_jetties = 0;
        std::size_t registered = 0;
        std::size_t imported = 0;
        std::size_t unimported = 0;
        std::size_t unregistered = 0;
        std::size_t recv_batches = 0;
        std::size_t healthy_after_modify = 0;
        std::size_t mixed_tx_batches = 0;
        std::size_t healthy_after_boundary_in_same_batch = 0;
        std::size_t flush_calls = 0;
        bool modified = false;
        bool boundary_polled = false;
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
        f.modify_jetty = [](auto* q, auto* a) { return active_->Modify(q, a); };
        f.poll_jfc = [](auto* q, int n, auto* cr) { return active_->Poll(q, n, cr); };
        f.flush_jetty = [](auto* q, int n, auto* cr) { return active_->Flush(q, n, cr); };
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

    Endpoint& Find(urma_jetty_t* jetty) {
        for (auto& endpoint : endpoints_) {
            for (auto& candidate : endpoint.jetties) {
                if (jetty == &candidate) {
                    return endpoint;
                }
            }
        }
        ADD_FAILURE() << "unknown local jetty";
        return endpoints_[0];
    }

    bool IsRegistered(Endpoint& endpoint, const urma_sge_t& sge, bool rx) {
        for (auto& registration : endpoint.registrations) {
            if (sge.tseg == &registration.segment && registration.live && sge.addr >= registration.address &&
                sge.len <= registration.length && sge.addr - registration.address <= registration.length - sge.len) {
                if (rx) {
                    EXPECT_FALSE(registration.tx);
                    registration.rx = true;
                } else {
                    EXPECT_FALSE(registration.rx);
                    registration.tx = true;
                }
                return true;
            }
        }
        return false;
    }

    static bool SameEndpoint(const urma_jetty_id_t& lhs, const urma_jetty_id_t& rhs) {
        return lhs.id == rhs.id && std::memcmp(&lhs.eid, &rhs.eid, sizeof(urma_eid_t)) == 0;
    }

    static urma_cr_t Completion(Endpoint& endpoint, raw::AttemptId id, urma_cr_status_t status, std::size_t lane = 0) {
        urma_cr_t cr{};
        cr.flag.bs.jetty = 1;
        cr.local_id = endpoint.jetties[lane].jetty_id.id;
        cr.user_ctx = id;
        cr.status = status;
        return cr;
    }

    void SetUp() override {
        active_ = this;
        cap_.max_msg_size = 4096;
        cap_.trans_mode = URMA_TM_RM;
        cap_.rm_tp_cap.bs.ctp = 1;
        cap_.max_jetty = 3;
        cap_.max_jfs_depth = cap_.max_jfr_depth = cap_.max_jfc_depth = 256;
        cap_.max_jfs_sge = cap_.max_jfs_rsge = cap_.max_jfr_sge = 1;
        for (std::size_t side = 0; side < endpoints_.size(); ++side) {
            for (std::size_t lane = 0; lane < 3; ++lane) {
                // 数字 ID 刻意跨端重复；只有完整 EID + jetty ID 才能定位对端。
                auto& id = endpoints_[side].jetties[lane].jetty_id;
                id.id = 10 + lane;
                id.eid.raw[15] = side + 1;
            }
        }
        EXPECT_CALL(*this, CreateJfc(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            if (endpoint.created_cqs == 0) {
                EXPECT_GE(cfg->depth, 2 * (options_.batch + 1));
            }
            return &endpoint.cqs.at(endpoint.created_cqs++);
        });
        EXPECT_CALL(*this, CreateJfr(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            EXPECT_EQ(cfg->jfc, &endpoint.cqs.at(2 * endpoint.created_jfrs + 1));
            return &endpoint.jfrs.at(endpoint.created_jfrs++);
        });
        EXPECT_CALL(*this, CreateJetty(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* ctx, auto* cfg) {
            auto& endpoint = Find(ctx);
            const std::size_t domain = endpoint.created_jetties == 2;
            EXPECT_EQ(cfg->flag.bs.share_jfr, 1u);
            EXPECT_EQ(cfg->shared.jfr, &endpoint.jfrs[domain]);
            EXPECT_EQ(cfg->shared.jfc, &endpoint.cqs[2 * domain + 1]);
            EXPECT_EQ(cfg->jfs_cfg.jfc, &endpoint.cqs[2 * domain]);
            EXPECT_EQ(cfg->jfs_cfg.flag.bs.error_suspend, 0u);
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
            auto& endpoint = endpoints_[jfr == &endpoints_[1].jfrs[0]];
            // 黑洞目标必须拥有独立、从未投递过 buffer 的 JFR，不能耗尽健康流量共享的接收队列。
            EXPECT_EQ(jfr, &endpoint.jfrs[0]);
            ++endpoint.recv_batches;
            for (; wr; wr = wr->next) {
                EXPECT_EQ(wr->src.num_sge, 1u);
                EXPECT_EQ(wr->src.sge[0].len, raw::RxBufferPool::kBufferSize);
                EXPECT_TRUE(IsRegistered(endpoint, wr->src.sge[0], true));
                endpoint.buffers.push_back({wr->src.sge[0], wr->user_ctx});
            }
            return URMA_SUCCESS;
        });
        EXPECT_CALL(*this, PostSend(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* wr, auto** bad) {
            return Deliver(jetty, wr, bad);
        });
        EXPECT_CALL(*this, Modify(_, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, auto* attr) {
            return EnterError(jetty, attr);
        });
        EXPECT_CALL(*this, Poll(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* cq, int limit, auto* out) {
            return PollCompletions(cq, limit, out);
        });
        EXPECT_CALL(*this, Flush(_, _, _)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty, int limit, auto* out) {
            std::lock_guard lock(mutex_);
            auto& endpoint = Find(jetty);
            EXPECT_EQ(jetty, &endpoint.jetties[0]);
            EXPECT_TRUE(endpoint.modified);
            EXPECT_TRUE(endpoint.boundary_polled);
            ++endpoint.flush_calls;
            const auto count = std::min(static_cast<std::size_t>(limit), endpoint.software_completions.size());
            for (std::size_t i = 0; i < count; ++i) {
                out[i] = endpoint.software_completions.front();
                endpoint.software_completions.pop_front();
            }
            return static_cast<int>(count);
        });
        InstallCleanup();
    }

    urma_status_t Deliver(urma_jetty_t* jetty, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
        std::lock_guard lock(mutex_);
        auto& source = Find(jetty);
        auto& destination = endpoints_[&source == &endpoints_[0]];
        const std::size_t lane = jetty == &source.jetties[1];
        EXPECT_EQ(jetty, &source.jetties[lane]);
        // ERROR 的 provider 即使返回 SUCCESS 也不表示完成；工具必须在进入此调用前阻止它。
        EXPECT_FALSE(source.modified && lane == 0);
        for (; wr; wr = wr->next) {
            EXPECT_EQ(wr->flag.bs.complete_enable, 1u);
            EXPECT_EQ(wr->flag.bs.inline_flag, 0u);
            EXPECT_EQ(wr->send.src.num_sge, 1u);
            const auto& sge = wr->send.src.sge[0];
            EXPECT_TRUE(IsRegistered(source, sge, false));
            EXPECT_EQ(sge.len, options_.bytes);
            ++source.sent[lane];
            if (SameEndpoint(wr->tjetty->id, destination.jetties[2].jetty_id)) {
                EXPECT_EQ(wr->opcode, URMA_OPC_SEND);
                EXPECT_EQ(lane, 0u);
                source.bad_ids.push_back(wr->user_ctx);
                continue;
            }
            EXPECT_EQ(wr->opcode, URMA_OPC_SEND_IMM);
            EXPECT_TRUE(SameEndpoint(wr->tjetty->id, destination.jetties[1].jetty_id));
            const auto frame = CheckIsolationPayload({reinterpret_cast<const std::byte*>(sge.addr), sge.len});
            EXPECT_TRUE(frame);
            if (frame) {
                EXPECT_EQ(frame->source, lane);
                EXPECT_EQ(frame->server, &source == &endpoints_[0]);
                EXPECT_EQ(wr->send.imm_data, IsolationImmediate(*frame));
                ++source.normal_sent[static_cast<std::size_t>(frame->phase)];
            }
            if (source.modified) {
                EXPECT_EQ(lane, 1u);
                ++source.healthy_after_modify;
            }
            if (destination.buffers.empty()) {
                ADD_FAILURE() << "healthy peer was notified before enough RX buffers were posted";
                *bad = wr;
                return URMA_EAGAIN;
            }
            const auto buffer = destination.buffers.front();
            destination.buffers.pop_front();
            EXPECT_TRUE(IsRegistered(destination, buffer.sge, true));
            EXPECT_LE(sge.len, buffer.sge.len);
            std::memcpy(reinterpret_cast<void*>(buffer.sge.addr), reinterpret_cast<const void*>(sge.addr), sge.len);
            auto tx = Completion(source, wr->user_ctx, URMA_CR_SUCCESS, lane);
            urma_cr_t rx{};
            rx.flag.bs.jetty = 1;
            rx.flag.bs.s_r = 1;
            rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
            rx.local_id = destination.jetties[1].jetty_id.id;
            rx.remote_id = jetty->jetty_id;
            rx.user_ctx = buffer.user_ctx;
            rx.completion_len = sge.len;
            rx.imm_data = wr->send.imm_data;
            // 倒序交付以排除依赖提交顺序的错误匹配；健康与故障 SQ 的 TX CQE 共用一个队列。
            source.completions[0].push_front(tx);
            if (delay_boundary_ && source.healthy_after_modify == options_.batch + 1) {
                // 第二轮的第一条健康 WR 已提交后才返回剩余故障完成和边界。
                // 边界置于该健康 CQE 前面，验证它不能终结整个共享 CQ 的合法完成。
                auto& queue = source.completions[0];
                queue.insert(queue.begin(), source.deferred_completions.begin(), source.deferred_completions.end());
                source.deferred_completions.clear();
            }
            destination.completions[1].push_front(rx);
        }
        return URMA_SUCCESS;
    }

    urma_status_t EnterError(urma_jetty_t* jetty, urma_jetty_attr_t* attr) {
        std::lock_guard lock(mutex_);
        auto& endpoint = Find(jetty);
        EXPECT_EQ(jetty, &endpoint.jetties[0]);
        EXPECT_EQ(attr->mask, JETTY_STATE);
        EXPECT_EQ(attr->state, URMA_JETTY_STATE_ERROR);
        EXPECT_FALSE(endpoint.modified);
        EXPECT_EQ(endpoint.bad_ids.size(), options_.batch);
        endpoint.modified = true;
        if (failure_ == Failure::kModify) {
            return URMA_EINVAL;
        }
        for (std::size_t i = 0; i < endpoint.bad_ids.size(); ++i) {
            if (i + 1 == endpoint.bad_ids.size()) {
                endpoint.software_completions.push_back(
                    Completion(endpoint, endpoint.bad_ids[i], URMA_CR_WR_UNHANDLED));
            } else {
                const auto status = i % 2 == 0 ? URMA_CR_WR_FLUSH_ERR : URMA_CR_ACK_TIMEOUT_ERR;
                auto& queue = delay_boundary_ && i != 0 ? endpoint.deferred_completions : endpoint.completions[0];
                queue.push_back(Completion(endpoint, endpoint.bad_ids[i], status));
            }
        }
        if (failure_ != Failure::kMissingBoundary) {
            // 伪 CQE 的 user_ctx 故意与有效 attempt 重复，禁止把它当成另一条 WR 完成。
            auto& queue = delay_boundary_ ? endpoint.deferred_completions : endpoint.completions[0];
            queue.push_back(Completion(endpoint, endpoint.bad_ids.front(), URMA_CR_WR_FLUSH_ERR_DONE));
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
                auto& queue = endpoint.completions[index];
                const auto count = std::min(static_cast<std::size_t>(limit), queue.size());
                bool bad = false, healthy = false;
                bool boundary_in_batch = false;
                for (std::size_t i = 0; i < count; ++i) {
                    out[i] = queue.front();
                    queue.pop_front();
                    bad |= out[i].status != URMA_CR_SUCCESS;
                    healthy |= out[i].local_id == endpoint.jetties[1].jetty_id.id && out[i].status == URMA_CR_SUCCESS;
                    endpoint.boundary_polled |= out[i].status == URMA_CR_WR_FLUSH_ERR_DONE;
                    boundary_in_batch |= out[i].status == URMA_CR_WR_FLUSH_ERR_DONE;
                    if (index == 0 && boundary_in_batch && out[i].local_id == endpoint.jetties[1].jetty_id.id &&
                        out[i].status == URMA_CR_SUCCESS) {
                        ++endpoint.healthy_after_boundary_in_same_batch;
                    }
                }
                if (index == 0 && bad && healthy) {
                    ++endpoint.mixed_tx_batches;
                }
                return static_cast<int>(count);
            }
        }
        ADD_FAILURE() << "poll used an unknown completion queue";
        return -1;
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
            ADD_FAILURE() << "unknown imported endpoint";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJetty(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jetty) {
            auto& endpoint = Find(jetty);
            EXPECT_TRUE(endpoint.buffers.empty());
            EXPECT_TRUE(endpoint.software_completions.empty());
            for (const auto& queue : endpoint.completions) {
                EXPECT_TRUE(queue.empty());
            }
            for (std::size_t lane = 0; lane < endpoint.jetties.size(); ++lane) {
                if (jetty == &endpoint.jetties[lane]) {
                    EXPECT_FALSE(endpoint.deleted_jetties[lane]);
                    endpoint.deleted_jetties[lane] = true;
                    return URMA_SUCCESS;
                }
            }
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJfr(_)).Times(AnyNumber()).WillRepeatedly([&](auto* jfr) {
            for (auto& endpoint : endpoints_) {
                for (std::size_t domain = 0; domain < endpoint.jfrs.size(); ++domain) {
                    if (jfr != &endpoint.jfrs[domain]) {
                        continue;
                    }
                    EXPECT_FALSE(endpoint.deleted_jfrs[domain]);
                    if (domain == 0) {
                        EXPECT_TRUE(endpoint.deleted_jetties[0]);
                        EXPECT_TRUE(endpoint.deleted_jetties[1]);
                    } else {
                        EXPECT_TRUE(endpoint.deleted_jetties[2]);
                    }
                    endpoint.deleted_jfrs[domain] = true;
                    return URMA_SUCCESS;
                }
            }
            ADD_FAILURE() << "unknown receive queue";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, DeleteJfc(_)).Times(AnyNumber()).WillRepeatedly([&](auto* cq) {
            for (auto& endpoint : endpoints_) {
                for (std::size_t index = 0; index < endpoint.cqs.size(); ++index) {
                    if (cq == &endpoint.cqs[index]) {
                        EXPECT_TRUE(endpoint.deleted_jfrs[index / 2]);
                        EXPECT_FALSE(endpoint.deleted_cqs[index]);
                        endpoint.deleted_cqs[index] = true;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unknown completion queue";
            return URMA_EINVAL;
        });
        EXPECT_CALL(*this, Unregister(_)).Times(AnyNumber()).WillRepeatedly([&](auto* segment) {
            for (auto& endpoint : endpoints_) {
                for (auto& registration : endpoint.registrations) {
                    if (segment == &registration.segment) {
                        EXPECT_TRUE(registration.live);
                        EXPECT_TRUE(endpoint.buffers.empty());
                        EXPECT_TRUE(endpoint.software_completions.empty());
                        for (const auto& queue : endpoint.completions) {
                            EXPECT_TRUE(queue.empty());
                        }
                        registration.live = false;
                        ++endpoint.unregistered;
                        return URMA_SUCCESS;
                    }
                }
            }
            ADD_FAILURE() << "unknown memory registration";
            return URMA_EINVAL;
        });
    }

    void ExpectClosed(const Endpoint& endpoint) {
        EXPECT_EQ(endpoint.unimported, endpoint.imported);
        EXPECT_EQ(static_cast<std::size_t>(
                      std::count(endpoint.deleted_jetties.begin(), endpoint.deleted_jetties.end(), true)),
                  endpoint.created_jetties);
        EXPECT_EQ(
            static_cast<std::size_t>(std::count(endpoint.deleted_jfrs.begin(), endpoint.deleted_jfrs.end(), true)),
            endpoint.created_jfrs);
        EXPECT_EQ(static_cast<std::size_t>(std::count(endpoint.deleted_cqs.begin(), endpoint.deleted_cqs.end(), true)),
                  endpoint.created_cqs);
        EXPECT_EQ(endpoint.unregistered, endpoint.registered);
    }

    void RunFailureCase(Failure failure) {
        failure_ = failure;
        options_.timeout_ms = 200;
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
        bool retained = !server_result && !client_result;
        for (std::size_t side = 0; side < sessions_.size(); ++side) {
            const auto& endpoint = endpoints_[side];
            const auto& stats = sessions_[side].stats();
            const auto closed = sessions_[side].Close();
            retained &= !closed && closed.error().code == EBUSY && endpoint.unimported == 0 &&
                        endpoint.unregistered == 0 && stats.fault.accepted == options_.batch &&
                        stats.fault.flush_done == 0 && stats.accepted_known;
            for (bool deleted : endpoint.deleted_jetties) {
                retained &= !deleted;
            }
            for (bool deleted : endpoint.deleted_jfrs) {
                retained &= !deleted;
            }
            for (bool deleted : endpoint.deleted_cqs) {
                retained &= !deleted;
            }
            for (const auto& registration : endpoint.registrations) {
                retained &= registration.live;
            }
            if (failure == Failure::kMissingBoundary) {
                // CQ 中普通完成已消耗也不能代替边界；软件 flush 不得抢先回收剩余 WR。
                retained &= endpoint.flush_calls == 0 && stats.fault.retired == options_.batch - 1 &&
                            stats.fault.unhandled == 0 && !endpoint.software_completions.empty();
            } else {
                // modify 失败后既没有排空证据，也可能尚有健康 RX 在硬件中，整条依赖链必须保留。
                retained &= endpoint.flush_calls == 0 && stats.fault.retired == 0;
            }
        }
        if (failure == Failure::kMissingBoundary) {
            retained &= !server_result && server_result.error().code == ETIMEDOUT && !client_result &&
                        client_result.error().code == ETIMEDOUT;
        }
        // 与工具失败退出契约一致：不让局部对象析构试图销毁仍可能用于 DMA 的内存。
        std::_Exit(retained && !HasFailure() ? 0 : 1);
    }

    static inline SqIsolationHardwareTest* active_;
    SqIsolationOptions options_{.bytes = 64, .batch = 3, .rounds = 2, .timeout_ms = 1000};
    urma_device_cap_t cap_{};
    std::mutex mutex_;
    std::array<Endpoint, 2> endpoints_{};
    Failure failure_ = Failure::kNone;
    bool delay_boundary_ = false;
    raw::test_support::ScopedUrmaOverride scope_{MockedFunctions()};
    std::array<SqIsolationSession, 2> sessions_{};
};

class SqIsolationDistributionTest : public SqIsolationHardwareTest, public ::testing::WithParamInterface<int> {};

TEST_P(SqIsolationDistributionTest, FaultedSqDrainsWhileSharedQueuesContinueBidirectionalTraffic) {
    options_.batch = GetParam();
    delay_boundary_ = options_.batch == 3;
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
        const auto& stats = sessions_[side].stats();
        const auto& endpoint = endpoints_[side];
        // 三阶段分别对账，避免只看总数而把基线或排空后的流量错算成故障期间的进展。
        const std::array<std::uint64_t, 3> expected{2 * options_.batch, options_.rounds * options_.batch,
                                                    kIsolationAfterRounds * options_.batch};
        for (std::size_t phase = 0; phase < expected.size(); ++phase) {
            EXPECT_EQ(stats.traffic[phase].accepted, expected[phase]);
            EXPECT_EQ(stats.traffic[phase].retired, expected[phase]);
            EXPECT_EQ(stats.traffic[phase].received, expected[phase]);
            EXPECT_EQ(endpoint.normal_sent[phase], expected[phase]);
        }
        EXPECT_TRUE(stats.accepted_known);
        EXPECT_EQ(stats.fault.accepted, options_.batch);
        EXPECT_EQ(stats.fault.retired, options_.batch);
        EXPECT_EQ(stats.fault.success, 0u);
        EXPECT_EQ(stats.fault.unhandled, 1u);
        EXPECT_EQ(stats.fault.flush_error, options_.batch / 2);
        EXPECT_EQ(stats.fault.ack_timeout_error, (options_.batch - 1) / 2);
        EXPECT_EQ(stats.fault.flush_done, 1u);
        EXPECT_GE(stats.fault.rejected_sends, 3u);
        EXPECT_GT(stats.healthy_ready_checks, 0u);
        EXPECT_FALSE(stats.failed_tx);
        EXPECT_FALSE(stats.failed_rx);
        EXPECT_EQ(endpoint.sent[0], 2 * options_.batch);
        EXPECT_EQ(endpoint.sent[1], (1 + options_.rounds + kIsolationAfterRounds) * options_.batch);
        EXPECT_EQ(endpoint.sent[2], 0u);
        EXPECT_GT(endpoint.mixed_tx_batches, 0u);
        if (delay_boundary_) {
            // 第一轮只退休部分坏 SQ attempt；第二轮必须继续识别原始 ID，而非重置其账本身份。
            EXPECT_GE(stats.healthy_retired_before_drained, 2 * options_.batch);
            EXPECT_EQ(endpoint.healthy_after_boundary_in_same_batch, 1u);
            EXPECT_TRUE(endpoint.deferred_completions.empty());
        }
        EXPECT_EQ(endpoint.flush_calls, 2u);
        EXPECT_TRUE(endpoint.modified);
        EXPECT_TRUE(endpoint.boundary_polled);
        EXPECT_TRUE(endpoint.buffers.empty());
        EXPECT_GE(endpoint.healthy_after_modify, (options_.rounds + kIsolationAfterRounds) * options_.batch);
        // gate probe 不能到达 provider；mock 已断言 ERROR SQ 的 PostSend 永远不会被调用。
        ASSERT_TRUE(sessions_[side].Close());
        ExpectClosed(endpoint);
        ASSERT_TRUE(sessions_[side].Close());
        ExpectClosed(endpoint);
    }
    std::expected<void, ToolError> server_closed;
    std::thread closer([&] { server_closed = ConfirmIsolationClosed(server, options_.timeout_ms); });
    const auto client_closed = ConfirmIsolationClosed(client, options_.timeout_ms);
    closer.join();
    EXPECT_TRUE(server_closed);
    EXPECT_TRUE(client_closed);
}

// batch=1 仅软件回收；batch=3 跨健康轮次延迟故障边界；batch=65 跨过 poll 的 64 条预算。
INSTANTIATE_TEST_SUITE_P(CompletionDistributions, SqIsolationDistributionTest, ::testing::Values(1, 3, 65));

TEST_F(SqIsolationHardwareTest, PeerMismatchRejectsBeforeImportOrPosting) {
    ASSERT_TRUE(sessions_[0].Open(&endpoints_[0].ctx, cap_, options_, true));
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    SqIsolationHello hello{.options = options_, .server = false};
    ++hello.options.rounds;
    for (std::size_t lane = 0; lane < hello.endpoints.size(); ++lane) {
        hello.endpoints[lane] = endpoints_[1].jetties[lane].jetty_id;
    }
    ASSERT_TRUE(peer.Write(EncodeIsolationHello(hello), options_.timeout_ms));
    // 参数不匹配必须在发布 READY 前返回，允许安全关闭已有的本地队列和注册内存。
    EXPECT_CALL(*this, Import(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    EXPECT_CALL(*this, Modify(_, _)).Times(0);
    const auto result = sessions_[0].Run(local);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, EPROTO);
    ASSERT_TRUE(sessions_[0].Close());
    ExpectClosed(endpoints_[0]);
}

TEST_F(SqIsolationHardwareTest, PartialImportFailureReleasesOnlyKnownTargets) {
    ASSERT_TRUE(sessions_[0].Open(&endpoints_[0].ctx, cap_, options_, true));
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);
    ControlChannel local(fds[0]), peer(fds[1]);
    SqIsolationHello hello{.options = options_, .server = false};
    for (std::size_t lane = 0; lane < hello.endpoints.size(); ++lane) {
        hello.endpoints[lane] = endpoints_[1].jetties[lane].jetty_id;
    }
    ASSERT_TRUE(peer.Write(EncodeIsolationHello(hello), options_.timeout_ms));
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
    // 第二个目标导入失败不能遗失第一个句柄，更不能继续提交任意收发 WR。
    EXPECT_CALL(*this, PostRecv(_, _, _)).Times(0);
    EXPECT_CALL(*this, PostSend(_, _, _)).Times(0);
    const auto result = sessions_[0].Run(local);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ENOMEM);
    ASSERT_TRUE(sessions_[0].Close());
    EXPECT_EQ(endpoints_[0].unimported, 1u);
    ExpectClosed(endpoints_[0]);
}

TEST_F(SqIsolationHardwareTest, CloseFailureRetainsParentQueuesAndRetryDoesNotDoubleDelete) {
    ASSERT_TRUE(sessions_[0].Open(&endpoints_[0].ctx, cap_, options_, true));
    // 主池的健康 jetty 删除失败时，仍引用共享 JFR/JFC；重试不能重复销毁已经释放的资源。
    EXPECT_CALL(*this, DeleteJetty(&endpoints_[0].jetties[1])).WillOnce(Return(URMA_EAGAIN)).RetiresOnSaturation();
    const auto first = sessions_[0].Close();
    ASSERT_FALSE(first);
    EXPECT_EQ(first.error().code, URMA_EAGAIN);
    EXPECT_FALSE(endpoints_[0].deleted_jfrs[0]);
    EXPECT_FALSE(endpoints_[0].deleted_cqs[0]);
    EXPECT_FALSE(endpoints_[0].deleted_cqs[1]);
    ASSERT_TRUE(sessions_[0].Close());
    ExpectClosed(endpoints_[0]);
}

TEST_F(SqIsolationHardwareTest, MissingBoundaryTimesOutWithoutFlushingOrReleasingDmaMemory) {
    // 用子进程隔离保留到进程退出的依赖链，避免测试夹具析构违背失败路径的资源契约。
    EXPECT_EXIT(RunFailureCase(Failure::kMissingBoundary), ::testing::ExitedWithCode(0), "");
}

TEST_F(SqIsolationHardwareTest, ModifyFailureRetainsPostedReceiveBuffersAndTransmitAttempts) {
    EXPECT_EXIT(RunFailureCase(Failure::kModify), ::testing::ExitedWithCode(0), "");
}
} // namespace
} // namespace tools
} // namespace kbsocket
