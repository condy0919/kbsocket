// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/provider_boundary_test/provider_boundary_test.hpp"

#include <arpa/inet.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr std::uint32_t kHello = 0x4b425042;
constexpr std::uint32_t kResult = 0x4b425052;
constexpr std::uint32_t kVersion = 1;
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
bool IsTerminal(urma_cr_status_t status) noexcept {
    switch (status) {
    case URMA_CR_SUCCESS:
    case URMA_CR_UNSUPPORTED_OPCODE_ERR:
    case URMA_CR_LOC_LEN_ERR:
    case URMA_CR_LOC_OPERATION_ERR:
    case URMA_CR_LOC_ACCESS_ERR:
    case URMA_CR_REM_RESP_LEN_ERR:
    case URMA_CR_REM_UNSUPPORTED_REQ_ERR:
    case URMA_CR_REM_OPERATION_ERR:
    case URMA_CR_REM_ACCESS_ABORT_ERR:
    case URMA_CR_ACK_TIMEOUT_ERR:
    case URMA_CR_RNR_RETRY_CNT_EXC_ERR:
    case URMA_CR_WR_FLUSH_ERR:
    case URMA_CR_LOC_DATA_POISON:
    case URMA_CR_REM_DATA_POISON:
        return true;
    default:
        return false;
    }
}
} // namespace

std::expected<void, ToolError> ValidateProviderBoundaryOptions(const ProviderBoundaryOptions& options) noexcept {
    if (!options.tx_depth || options.tx_depth > 4096 || !options.batch || options.batch > 256 ||
        options.attempts <= options.tx_depth || options.attempts > 16384 || !options.timeout_ms ||
        options.timeout_ms > 3600000) {
        return Error("invalid provider boundary limits", EINVAL);
    }
    return {};
}

void ProviderBoundaryTracker::Initialize(std::size_t max_id, std::uint32_t jetty_id) {
    states_.assign(max_id + 1, 0);
    jetty_id_ = jetty_id;
    error_ = false;
    stats = {};
}

std::expected<std::size_t, ToolError> ProviderBoundaryTracker::RecordPost(std::span<urma_jfs_wr_t> wrs,
                                                                          urma_status_t status,
                                                                          urma_jfs_wr_t* bad_wr) noexcept {
    ++stats.post_calls;
    stats.offered += wrs.size();
    stats.last_post_status = static_cast<int>(status);
    // 保留原始返回值，同时兼容 provider 以正 errno 或负 errno 返回资源不足。
    if (!stats.error_post_attempted) {
        // ERROR 探针的拒绝不能混入 READY SQ 容量不足的观测。
        stats.eagain += status == URMA_EAGAIN || static_cast<int>(status) == -EAGAIN;
        stats.enomem += status == URMA_ENOMEM || static_cast<int>(status) == -ENOMEM;
    }
    std::size_t accepted = wrs.size();
    if (status != URMA_SUCCESS) {
        for (std::size_t i = 0; i < wrs.size(); ++i) {
            if (bad_wr == &wrs[i]) {
                accepted = i;
                break;
            }
        }
    }
    if ((status == URMA_SUCCESS && bad_wr) || (status != URMA_SUCCESS && accepted == wrs.size())) {
        stats.accepted_known = false;
        return Error("provider acceptance unknown: invalid bad_wr", EPROTO);
    }
    for (std::size_t i = 0; i < accepted; ++i) {
        const auto id = wrs[i].user_ctx;
        if (!id || id >= states_.size() || states_[id] != 0) {
            stats.accepted_known = false;
            return Error("accepted WR identity reused or out of range", EPROTO);
        }
        states_[id] = 1;
        ++stats.accepted;
    }
    stats.partial_batches += accepted != 0 && accepted != wrs.size();
    return accepted;
}

std::expected<void, ToolError> ProviderBoundaryTracker::Complete(const urma_cr_t& cr, bool software_flush) noexcept {
    if (cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.local_id != jetty_id_) {
        return Error("completion identity or direction mismatch", EPROTO);
    }
    // 伪完成的 user_ctx 没有意义，必须在任何索引访问之前识别。
    if (!software_flush && cr.status == URMA_CR_WR_FLUSH_ERR_DONE) {
        if (!error_ || stats.flush_done) {
            return Error("unexpected or duplicate FLUSH_ERR_DONE", EPROTO);
        }
        ++stats.flush_done;
        return {};
    }
    if (software_flush ? (!stats.flush_done || cr.status != URMA_CR_WR_UNHANDLED) : !IsTerminal(cr.status)) {
        return Error("unexpected completion status or source", EPROTO);
    }
    if (!software_flush && cr.status == URMA_CR_WR_FLUSH_ERR && !error_) {
        return Error("FLUSH_ERR before explicit ERROR transition", EPROTO);
    }
    const auto id = cr.user_ctx;
    if (!id || id >= states_.size() || states_[id] != 1) {
        return Error("unknown or duplicate terminal WR", EPROTO);
    }
    states_[id] = 2;
    ++stats.retired;
    ++stats.statuses[static_cast<std::size_t>(cr.status)];
    return {};
}

std::expected<void, ToolError> ProviderBoundarySession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                             const ProviderBoundaryOptions& options, bool server) {
    if (ctx_) {
        return Error("session already open", EBUSY);
    }
    auto valid = ValidateProviderBoundaryOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || cap.max_msg_size < 4096) {
        return Error("4096-byte SEND capability required", EINVAL);
    }
    options_ = options;
    server_ = server;
    ctx_ = ctx;
    // 连续 post 不 poll 时 CQ 必须能容纳所有有限尝试及边界，不能用 CQ 溢出来模拟 SQ 背压。
    raw::JettyPoolConfig config;
    config.tx_depth = server ? 1 : options.tx_depth;
    config.rx_depth = 1;
    config.tx_cq_depth = server ? 2 : options.attempts + 2;
    config.rx_cq_depth = 1;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("create raw queues (check attempts+2 against max JFC depth)",
                     opened.error().failure.provider_error ? opened.error().failure.provider_error : EINVAL);
    }
    auto ticket = pool_.Reserve();
    if (!ticket) {
        return Error("reserve endpoint", EIO);
    }
    auto endpoint = pool_.Get(ticket->lane);
    auto cancelled = pool_.Cancel(*ticket);
    if (!endpoint || !cancelled) {
        return Error("obtain raw endpoint", EIO);
    }
    jetty_ = *endpoint;
    tracker_.Initialize(options.attempts + 1, jetty_->jetty_id.id);
    if (server) {
        return {};
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return Error("page size", EINVAL);
    }
    const auto page = static_cast<std::size_t>(page_size);
    const auto length = (4096 + page - 1) / page * page;
    storage_.assign(length + page - 1, std::byte{0x5a});
    const auto base = reinterpret_cast<std::uintptr_t>(storage_.data());
    urma_seg_cfg_t cfg{};
    cfg.va = base + (page - base % page) % page;
    cfg.len = length;
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &cfg);
    if (!segment_) {
        return Error("register immutable TX memory", errno ? errno : EIO);
    }
    sge_ = {.addr = cfg.va, .len = 4096, .tseg = segment_};
    wrs_.resize(options.attempts + 1);
    for (std::size_t i = 0; i < wrs_.size(); ++i) {
        auto& wr = wrs_[i];
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.user_ctx = i + 1;
        wr.send.src.sge = &sge_;
        wr.send.src.num_sge = 1;
    }
    return {};
}

std::expected<void, ToolError> ProviderBoundarySession::ExchangeHello(ControlChannel& channel) {
    std::array<std::uint32_t, 16> local{};
    local[0] = htonl(kHello);
    local[1] = htonl(kVersion);
    local[2] = htonl(server_ ? 1 : 0);
    local[3] = htonl(options_.tx_depth);
    local[4] = htonl(options_.batch);
    local[5] = htonl(options_.attempts);
    local[6] = htonl(options_.error_post ? 1 : 0);
    local[7] = htonl(options_.timeout_ms);
    std::memcpy(local.data() + 8, jetty_->jetty_id.eid.raw, 16);
    local[12] = htonl(jetty_->jetty_id.id);
    // 一旦开始发布端点，控制通道失败不能证明对端没有提交。
    published_ = true;
    quiescent_ = false;
    auto sent = channel.Write(std::as_bytes(std::span(local)), options_.timeout_ms);
    if (!sent) {
        return sent;
    }
    std::array<std::uint32_t, 16> peer{};
    auto read = channel.Read(std::as_writable_bytes(std::span(peer)), options_.timeout_ms);
    if (!read) {
        return read;
    }
    if (peer[0] != local[0] || peer[1] != local[1] || ntohl(peer[2]) != (server_ ? 0u : 1u) ||
        !std::equal(local.begin() + 3, local.begin() + 8, peer.begin() + 3) || peer[13] || peer[14] || peer[15]) {
        return Error("provider boundary hello/options mismatch", EPROTO);
    }
    if (!server_) {
        urma_rjetty_t target{};
        std::memcpy(target.jetty_id.eid.raw, peer.data() + 8, 16);
        target.jetty_id.id = ntohl(peer[12]);
        target.trans_mode = URMA_TM_RM;
        target.type = URMA_JETTY;
        target.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remote_ = UrmaApi::ImportJetty(ctx_, &target, &token);
        if (!remote_) {
            return Error("import empty RM_CTP target", errno ? errno : EIO);
        }
        for (auto& wr : wrs_) {
            wr.tjetty = remote_;
        }
    }
    return {};
}

std::expected<std::size_t, ToolError> ProviderBoundarySession::Post(std::size_t first, std::size_t count) {
    auto wrs = std::span(wrs_).subspan(first, count);
    for (std::size_t i = 0; i < wrs.size(); ++i) {
        wrs[i].next = i + 1 < wrs.size() ? &wrs[i + 1] : nullptr;
    }
    urma_jfs_wr_t* bad = nullptr;
    const auto status = UrmaApi::PostJettySendWr(jetty_, wrs.data(), &bad);
    auto recorded = tracker_.RecordPost(wrs, status, bad);
    if (!recorded) {
        unsafe_ = true;
    }
    return recorded;
}

std::expected<void, ToolError> ProviderBoundarySession::Drain(bool probe) {
    std::array<urma_cr_t, 64> completions{};
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    bool empty_flush = false;
    do {
        auto count = pool_.PollSend(completions);
        if (!count) {
            unsafe_ = true;
            return Error("poll provider completions", count.error().provider_error);
        }
        for (int i = 0; i < *count; ++i) {
            auto consumed = tracker_.Complete(completions[static_cast<std::size_t>(i)], false);
            if (!consumed) {
                unsafe_ = true;
                return consumed;
            }
        }
        if (tracker_.stats.flush_done) {
            // liburma 限制请求数量 <= 本 SQ 深度；边界之后才允许把软件未处理 WR 当作终结证明。
            const int budget = static_cast<int>(std::min(options_.tx_depth, 64u));
            completions.fill({});
            ++tracker_.stats.flush_calls;
            const int flushed = UrmaApi::FlushJetty(jetty_, budget, completions.data());
            if (flushed < 0 || flushed > budget) {
                unsafe_ = true;
                return Error("flush provider SQ", flushed);
            }
            empty_flush = flushed == 0;
            for (int i = 0; i < flushed; ++i) {
                auto consumed = tracker_.Complete(completions[static_cast<std::size_t>(i)], true);
                if (!consumed) {
                    unsafe_ = true;
                    return consumed;
                }
            }
        }
        if (!tracker_.pending() && tracker_.stats.flush_done && empty_flush) {
            return {};
        }
        std::this_thread::yield();
    } while (Clock::now() < deadline);
    if (probe && tracker_.pending() && tracker_.stats.accepted_known) {
        // ERROR post 返回 SUCCESS 不能充当终结证明；这是实验结果，必须保留内存直到进程退出。
        tracker_.stats.error_post_pending = true;
        intentional_retention_ = true;
        return {};
    }
    return Error("timeout waiting for each accepted WR and flush boundary", ETIMEDOUT);
}

std::expected<void, ToolError> ProviderBoundarySession::RunClient() {
    for (std::size_t first = 0; first < options_.attempts;) {
        const auto count = std::min<std::size_t>(options_.batch, options_.attempts - first);
        auto posted = Post(first, count);
        if (!posted) {
            return std::unexpected(posted.error());
        }
        first += count;
        if (tracker_.stats.last_post_status != URMA_SUCCESS) {
            break;
        }
    }
    const int pressure_status = tracker_.stats.last_post_status;
    urma_jetty_attr_t attr{};
    attr.mask = JETTY_STATE;
    attr.state = URMA_JETTY_STATE_ERROR;
    const auto modified = UrmaApi::ModifyJetty(jetty_, &attr);
    if (modified != URMA_SUCCESS) {
        return Error("modify probe SQ ERROR", static_cast<int>(modified));
    }
    tracker_.BeginError();
    auto drained = Drain(false);
    if (!drained) {
        return drained;
    }
    if (pressure_status != URMA_SUCCESS && pressure_status != EAGAIN && pressure_status != -EAGAIN &&
        pressure_status != ENOMEM && pressure_status != -ENOMEM) {
        return Error("unexpected provider rejection during capacity probe", pressure_status);
    }
    if (!options_.error_post) {
        return {};
    }
    urma_jetty_cfg_t cfg{};
    attr = {};
    const auto queried = UrmaApi::QueryJetty(jetty_, &cfg, &attr);
    if (queried != URMA_SUCCESS) {
        return Error("query ERROR SQ before direct post", static_cast<int>(queried));
    }
    tracker_.stats.queried_state = static_cast<int>(attr.state);
    if (attr.state != URMA_JETTY_STATE_ERROR) {
        return Error("queried SQ is not ERROR", EPROTO);
    }
    tracker_.stats.error_post_attempted = true;
    auto probe = Post(options_.attempts, 1);
    tracker_.stats.error_post_status = tracker_.stats.last_post_status;
    if (!probe) {
        return std::unexpected(probe.error());
    }
    return *probe ? Drain(true) : std::expected<void, ToolError>{};
}

std::expected<void, ToolError> ProviderBoundarySession::Run(ControlChannel& channel) {
    if (!jetty_ || started_) {
        return Error("session not ready or already run", EINVAL);
    }
    started_ = true;
    auto hello = ExchangeHello(channel);
    if (!hello) {
        return hello;
    }
    if (server_) {
        std::array<std::uint32_t, 4> report{};
        auto received = channel.Read(std::as_writable_bytes(std::span(report)), 3 * options_.timeout_ms);
        if (!received) {
            return received;
        }
        if (ntohl(report[0]) != kResult || ntohl(report[1]) != kVersion || ntohl(report[2]) > 2 || report[3]) {
            return Error("invalid provider result", EPROTO);
        }
        const auto state = ntohl(report[2]);
        quiescent_ = state == 0;
        intentional_retention_ = state == 1;
        auto ack = channel.SendMarker(kResult, options_.timeout_ms);
        if (!ack) {
            return ack;
        }
        return state == 2 ? Error("peer provider experiment failed", EIO) : std::expected<void, ToolError>{};
    }
    auto result = RunClient();
    quiescent_ = result.has_value() && !unsafe_ && tracker_.stats.accepted_known && !tracker_.pending();
    const std::uint32_t state = !result ? 2u : quiescent_ ? 0u : 1u;
    const std::array<std::uint32_t, 4> report{htonl(kResult), htonl(kVersion), htonl(state), 0};
    auto reported = channel.Write(std::as_bytes(std::span(report)), options_.timeout_ms);
    if (!reported) {
        quiescent_ = false;
        return result ? reported : result;
    }
    auto ack = channel.ExpectMarker(kResult, options_.timeout_ms);
    if (!ack) {
        quiescent_ = false;
        return result ? ack : result;
    }
    return result;
}

std::expected<void, ToolError> ProviderBoundarySession::Close() {
    if (unsafe_ || !tracker_.stats.accepted_known || tracker_.pending() || (published_ && !quiescent_)) {
        return Error("DMA/peer state unresolved; retain provider session", EBUSY);
    }
    if (remote_) {
        const auto status = UrmaApi::UnimportJetty(remote_);
        if (status != URMA_SUCCESS) {
            return Error("unimport target", static_cast<int>(status));
        }
        remote_ = nullptr;
    }
    auto closed = pool_.Close();
    if (!closed) {
        return Error("close raw queues", closed.error().provider_error);
    }
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error("unregister TX memory", static_cast<int>(status));
        }
        segment_ = nullptr;
    }
    storage_.clear();
    wrs_.clear();
    jetty_ = nullptr;
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
