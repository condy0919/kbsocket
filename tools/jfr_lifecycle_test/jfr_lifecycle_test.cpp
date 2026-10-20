// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/jfr_lifecycle_test/jfr_lifecycle_test.hpp"

#include <arpa/inet.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <print>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
constexpr std::uint32_t kMagic = 0x4b424a4c;
constexpr std::uint32_t kVersion = 1;
constexpr std::uint32_t kQuiescent = 0x4a4c444e;
constexpr std::size_t kBytes = 4096;

auto Error(const char* operation, int code = EIO) {
    return std::unexpected(ToolError{operation, code});
}
void Put(std::span<std::byte> wire, std::size_t offset, std::uint32_t value) {
    value = ::htonl(value);
    std::memcpy(wire.data() + offset, &value, sizeof(value));
}
std::uint32_t Get(std::span<const std::byte> wire, std::size_t offset) {
    std::uint32_t value;
    std::memcpy(&value, wire.data() + offset, sizeof(value));
    return ::ntohl(value);
}
std::byte Pattern(std::uint32_t index, std::size_t offset) {
    return static_cast<std::byte>((17 * index + offset) % 251);
}
} // namespace

std::expected<void, ToolError> ValidateJfrLifecycleOptions(const JfrLifecycleOptions& options) noexcept {
    if (!options.rx_depth || options.rx_depth > 256 || options.consumed >= options.rx_depth || !options.timeout_ms ||
        options.timeout_ms > 3600000) {
        return Error("require rx_depth=1..256, consumed<rx_depth, timeout_ms=1..3600000", EINVAL);
    }
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                         const JfrLifecycleOptions& options, bool server) {
    if (ctx_ || started_) {
        return Error("session already opened or experiment completed", EBUSY);
    }
    auto valid = ValidateJfrLifecycleOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || !(cap.trans_mode & URMA_TM_RM) || !cap.rm_tp_cap.bs.ctp || cap.max_jetty < 2 ||
        cap.max_jfs_depth < options.rx_depth || cap.max_jfr_depth < options.rx_depth ||
        cap.max_jfc_depth < 2 * (options.rx_depth + 1) || cap.max_msg_size < kBytes || !cap.max_jfs_sge ||
        !cap.max_jfr_sge || !cap.max_jfs_rsge) {
        return Error("device cannot support requested RM_CTP queues", EINVAL);
    }
    options_ = options;
    server_ = server;
    ctx_ = ctx;
    for (std::size_t i = 0; i < cqs_.size(); ++i) {
        urma_jfc_cfg_t cfg{};
        cfg.depth = i == 0 ? 2 * (options.rx_depth + 1) : options.rx_depth;
        cfg.flag.bs.lock_free = 1;
        cqs_[i] = UrmaApi::CreateJfc(ctx, &cfg);
        if (!cqs_[i]) {
            return Error("create completion queue", errno);
        }
    }
    urma_jfr_cfg_t recv{};
    recv.depth = options.rx_depth;
    recv.trans_mode = URMA_TM_RM;
    recv.max_sge = 1;
    recv.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    recv.jfc = cqs_[1];
    recv.flag.bs.lock_free = 1;
    jfr_ = UrmaApi::CreateJfr(ctx, &recv);
    if (!jfr_) {
        return Error("create shared JFR", errno);
    }
    // 两个真实 Jetty 依赖同一 JFR，关闭实验必须先删除全部依赖者。
    for (auto& jetty : jetties_) {
        urma_jetty_cfg_t cfg{};
        cfg.flag.bs.share_jfr = 1;
        cfg.jfs_cfg.depth = options.rx_depth;
        cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        cfg.jfs_cfg.priority = 4;
        cfg.jfs_cfg.max_sge = 1;
        cfg.jfs_cfg.max_rsge = 1;
        cfg.jfs_cfg.rnr_retry = 6;
        cfg.jfs_cfg.err_timeout = 2;
        cfg.jfs_cfg.jfc = cqs_[0];
        cfg.jfs_cfg.flag.bs.lock_free = 1;
        cfg.shared.jfr = jfr_;
        cfg.shared.jfc = cqs_[1];
        jetty = UrmaApi::CreateJetty(ctx, &cfg);
        if (!jetty) {
            return Error("create dependent jetty", errno);
        }
    }
    const auto page_value = ::sysconf(_SC_PAGESIZE);
    if (page_value <= 0) {
        return Error("read page size");
    }
    const auto page = static_cast<std::size_t>(page_value);
    const auto length = (options.rx_depth * kBytes + page - 1) / page * page;
    storage_.resize(length + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
    memory_ = std::span(storage_).subspan((page - address % page) % page, length);
    urma_seg_cfg_t segment{};
    segment.va = reinterpret_cast<std::uintptr_t>(memory_.data());
    segment.len = memory_.size();
    segment.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    segment_ = UrmaApi::RegisterSeg(ctx, &segment);
    if (!segment_) {
        return Error("register packet memory", errno);
    }
    for (std::uint32_t i = 0; i < options.rx_depth; ++i) {
        sges_[i] = {
            .addr = reinterpret_cast<std::uintptr_t>(memory_.data() + i * kBytes), .len = kBytes, .tseg = segment_};
    }
    ready_ = true;
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::PostReceive() {
    for (std::uint32_t i = 0; i < options_.rx_depth; ++i) {
        rx_wrs_[i] = {};
        rx_wrs_[i].src = {&sges_[i], 1};
        rx_wrs_[i].user_ctx = i + 1;
        rx_wrs_[i].next = i + 1 < options_.rx_depth ? &rx_wrs_[i + 1] : nullptr;
    }
    urma_jfr_wr_t* bad = nullptr;
    const auto result = UrmaApi::PostJfrWr(jfr_, rx_wrs_.data(), &bad);
    if (result != URMA_SUCCESS || bad) {
        // 部分接受或 bad_wr 不可信时，本实验不尝试推断剩余 RX，更不会释放其 MR。
        failed_ = true;
        return Error("post complete RX batch; acceptance unresolved", static_cast<int>(result));
    }
    stats_.posted = options_.rx_depth;
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::Exchange(ControlChannel& channel) {
    std::array<std::byte, 40> local{}, peer{};
    Put(local, 0, kMagic);
    Put(local, 4, kVersion);
    Put(local, 8, server_ ? 1 : 0);
    Put(local, 12, options_.rx_depth);
    Put(local, 16, options_.consumed);
    std::memcpy(local.data() + 20, jetties_[0]->jetty_id.eid.raw, 16);
    Put(local, 36, jetties_[0]->jetty_id.id);
    auto written = channel.Write(local, options_.timeout_ms);
    if (!written) {
        return written;
    }
    auto read = channel.Read(peer, options_.timeout_ms);
    if (!read) {
        return read;
    }
    if (Get(peer, 0) != kMagic || Get(peer, 4) != kVersion || Get(peer, 8) != (server_ ? 0U : 1U) ||
        Get(peer, 12) != options_.rx_depth || Get(peer, 16) != options_.consumed) {
        return Error("peer lifecycle options/protocol mismatch", EPROTO);
    }
    if (!server_) {
        urma_rjetty_t remote{};
        std::memcpy(remote.jetty_id.eid.raw, peer.data() + 20, 16);
        remote.jetty_id.id = Get(peer, 36);
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        target_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!target_) {
            return Error("import lifecycle receiver", errno);
        }
    }
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::Send() {
    for (std::uint32_t i = 0; i < options_.consumed; ++i) {
        auto payload = memory_.subspan(i * kBytes, kBytes);
        Put(payload, 0, i);
        for (std::size_t j = 4; j < kBytes; ++j) {
            payload[j] = Pattern(i, j);
        }
        auto& wr = tx_wrs_[i];
        wr = {};
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.tjetty = target_;
        wr.user_ctx = i + 1;
        wr.send.src = {&sges_[i], 1};
        wr.next = i + 1 < options_.consumed ? &tx_wrs_[i + 1] : nullptr;
    }
    urma_jfs_wr_t* bad = nullptr;
    const auto result = UrmaApi::PostJettySendWr(jetties_[0], tx_wrs_.data(), &bad);
    if (result != URMA_SUCCESS || bad) {
        failed_ = true;
        return Error("post complete TX batch; acceptance unresolved", static_cast<int>(result));
    }
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::Poll(bool receive) {
    auto& retired = receive ? stats_.received : stats_.tx_retired;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<urma_cr_t, 64> completions{};
    while (retired < options_.consumed) {
        const int count = UrmaApi::PollJfc(cqs_[receive ? 1 : 0], completions.size(), completions.data());
        if (count < 0 || static_cast<std::size_t>(count) > completions.size()) {
            return Error("poll lifecycle CQ", count);
        }
        for (int i = 0; i < count; ++i) {
            const auto& cr = completions[i];
            bool local = cr.flag.bs.jetty && cr.local_id == jetties_[0]->jetty_id.id;
            if (receive) {
                local = cr.flag.bs.jetty ? (local || cr.local_id == jetties_[1]->jetty_id.id)
                                         : cr.local_id == jfr_->jfr_id.id;
            }
            // 只成功完成可进入本次关闭观察；伪 CQE 在读取 user_ctx 前就被排除。
            if (cr.status != URMA_CR_SUCCESS || static_cast<bool>(cr.flag.bs.s_r) != receive || !local) {
                return Error("unexpected lifecycle completion status/type/id", static_cast<int>(cr.status));
            }
            const auto bound = receive ? options_.rx_depth : options_.consumed;
            if (!cr.user_ctx || cr.user_ctx > bound || seen_[cr.user_ctx - 1] || retired == options_.consumed) {
                return Error("invalid/duplicate lifecycle completion", EPROTO);
            }
            const auto slot = static_cast<std::size_t>(cr.user_ctx - 1);
            seen_[slot] = true;
            if (receive) {
                if (cr.completion_len != kBytes) {
                    return Error("incorrect lifecycle payload length", EPROTO);
                }
                const auto payload = memory_.subspan(slot * kBytes, kBytes);
                const auto index = Get(payload, 0);
                if (index >= options_.consumed || payload_seen_[index]) {
                    return Error("invalid/duplicate lifecycle payload", EPROTO);
                }
                payload_seen_[index] = true;
                for (std::size_t offset = 4; offset < kBytes; ++offset) {
                    if (payload[offset] != Pattern(index, offset)) {
                        return Error("corrupt lifecycle payload", EPROTO);
                    }
                }
            }
            ++retired;
        }
        if (retired < options_.consumed && std::chrono::steady_clock::now() >= deadline) {
            return Error("wait lifecycle completions", ETIMEDOUT);
        }
        if (!count) {
            std::this_thread::yield();
        }
    }
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::Run(ControlChannel* channel) {
    if (!ready_ || started_ || (!options_.consumed && !server_) || (options_.consumed && !channel)) {
        return Error("invalid lifecycle run state", EINVAL);
    }
    started_ = true;
    if (server_) {
        auto posted = PostReceive();
        if (!posted) {
            return posted;
        }
    }
    if (!options_.consumed) {
        // 单端模式从未公布端点；没有远端能通过本工具向这些 RX 投递。
        return {};
    }
    quiescent_ = false;
    auto exchanged = Exchange(*channel);
    if (!exchanged) {
        return exchanged;
    }
    if (!server_) {
        auto sent = Send();
        if (!sent) {
            return sent;
        }
    }
    auto polled = Poll(server_);
    if (!polled) {
        return polled;
    }
    // 双端都收齐约定 CQE 后才承诺不再投递；TCP EOF 不是 DMA 终结证明。
    auto sent = channel->SendMarker(kQuiescent, options_.timeout_ms);
    if (!sent) {
        return sent;
    }
    auto received = channel->ExpectMarker(kQuiescent, options_.timeout_ms);
    if (!received) {
        return received;
    }
    quiescent_ = true;
    return {};
}

std::expected<void, ToolError> JfrLifecycleSession::Close() {
    if (failed_ || !quiescent_) {
        return Error("lifecycle state unresolved; retain DMA dependency chain", EBUSY);
    }
    ready_ = false;
    auto fail = [this](const char* operation, urma_status_t status) -> std::expected<void, ToolError> {
        // provider 删除失败后对象可能已经部分失效，禁止重新 poll 或重试同一删除。
        failed_ = true;
        return Error(operation, static_cast<int>(status));
    };
    if (target_) {
        const auto status = UrmaApi::UnimportJetty(target_);
        if (status != URMA_SUCCESS) {
            return fail("unimport lifecycle target", status);
        }
        target_ = nullptr;
    }
    for (std::size_t i = jetties_.size(); i > 0; --i) {
        auto& jetty = jetties_[i - 1];
        if (jetty) {
            const auto begin = std::chrono::steady_clock::now();
            const auto status = UrmaApi::DeleteJetty(jetty);
            const auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count();
            std::println("DeleteJetty: index={} status={} elapsed_ns={}", i - 1, static_cast<int>(status), ns);
            std::fflush(stdout);
            if (status != URMA_SUCCESS) {
                return fail("delete dependent jetty", status);
            }
            jetty = nullptr;
        }
    }
    if (jfr_) {
        stats_.residual_at_delete = stats_.posted - stats_.received;
        stats_.delete_jfr_attempted = true;
        std::println("DeleteJfr begin: posted={} consumed={} residual={}", stats_.posted, stats_.received,
                     stats_.residual_at_delete);
        std::fflush(stdout);
        const auto begin = std::chrono::steady_clock::now();
        const auto status = UrmaApi::DeleteJfr(jfr_);
        stats_.delete_jfr_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count();
        stats_.delete_jfr_status = static_cast<int>(status);
        std::println("DeleteJfr end: status={} elapsed_ns={}", stats_.delete_jfr_status, stats_.delete_jfr_ns);
        std::fflush(stdout);
        if (status != URMA_SUCCESS) {
            return fail("delete JFR with residual posted RX", status);
        }
        jfr_ = nullptr;
    }
    // DeleteJfr 成功是本实验采用的 provider 所有权交还边界；不要求其逐 RX 产生可见 CQE。
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return fail("unregister lifecycle memory", status);
        }
        segment_ = nullptr;
        memory_ = {};
        storage_.clear();
    }
    for (auto& cq : cqs_) {
        if (cq) {
            const auto status = UrmaApi::DeleteJfc(cq);
            if (status != URMA_SUCCESS) {
                return fail("delete lifecycle CQ", status);
            }
            cq = nullptr;
        }
    }
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
