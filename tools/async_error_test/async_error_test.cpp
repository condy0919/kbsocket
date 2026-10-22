// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/async_error_test/async_error_test.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <optional>
#include <print>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr std::size_t kBytes = 64;
constexpr std::size_t kStride = 128;
constexpr std::size_t kGuard = 32;
constexpr std::uint32_t kHello = 0x41454531;
auto Error(const char* text, int code) {
    return std::unexpected(ToolError{text, code});
}
std::byte Pattern(std::uint64_t id, std::size_t offset) {
    return static_cast<std::byte>((id * 37 + offset * 13 + (id >> 8)) & 255);
}
bool SameEndpoint(const urma_jetty_id_t& lhs, const urma_jetty_id_t& rhs) {
    return lhs.id == rhs.id && std::memcmp(lhs.eid.raw, rhs.eid.raw, 16) == 0;
}
bool Terminal(urma_cr_status_t status) {
    return (status >= URMA_CR_SUCCESS && status <= URMA_CR_WR_FLUSH_ERR) || status == URMA_CR_LOC_DATA_POISON ||
           status == URMA_CR_REM_DATA_POISON;
}
} // namespace

const char* AsyncFaultName(AsyncFault fault) noexcept {
    switch (fault) {
    case AsyncFault::kJfcOverflow:
        return "jfc_overflow";
    case AsyncFault::kExternalJfc:
        return "external_jfc";
    case AsyncFault::kExternalJfr:
        return "external_jfr";
    }
    return "unknown";
}

std::expected<void, ToolError> ValidateAsyncErrorOptions(const AsyncErrorOptions& o) noexcept {
    if (o.fault != AsyncFault::kJfcOverflow && o.fault != AsyncFault::kExternalJfc &&
        o.fault != AsyncFault::kExternalJfr) {
        return Error("unknown async fault mode", EINVAL);
    }
    if (o.attempts < 128 || o.attempts > 4096 || o.attempts % 2 || o.cq_depth < 1 || o.cq_depth >= o.attempts ||
        o.healthy_rounds < 2 || o.healthy_rounds > 256 || o.observe_ms < o.healthy_rounds || o.observe_ms > 60000 ||
        o.timeout_ms < 100 || o.timeout_ms > 120000) {
        return Error("invalid async fault limits", EINVAL);
    }
    return {};
}

std::expected<void, ToolError> RecordAsyncErrorEvent(urma_async_event_t& event, const AsyncErrorObjects& objects,
                                                     AsyncErrorEvents& stats) noexcept {
    ++stats.retrieved;
    stats.last_type = static_cast<int>(event.event_type);
    if (stats.last_type >= 0 && static_cast<std::size_t>(stats.last_type) < stats.types.size()) {
        ++stats.types[static_cast<std::size_t>(stats.last_type)];
    }
    const bool context_matches = event.urma_ctx == objects.ctx;
    bool matched = false;
    if (context_matches) {
        if (event.event_type == URMA_EVENT_JFC_ERR) {
            if (event.element.jfc == objects.fault_rx || event.element.jfc == objects.fault_tx) {
                ++stats.fault_jfc;
                stats.fault_rx_jfc += event.element.jfc == objects.fault_rx;
                stats.fault_tx_jfc += event.element.jfc == objects.fault_tx;
                matched = true;
            } else if (event.element.jfc == objects.healthy_rx || event.element.jfc == objects.healthy_tx) {
                ++stats.healthy;
                matched = true;
            }
        } else if (event.event_type == URMA_EVENT_JFR_ERR) {
            if (event.element.jfr == objects.fault_jfr) {
                ++stats.fault_jfr;
                matched = true;
            } else if (event.element.jfr == objects.healthy_jfr) {
                ++stats.healthy;
                matched = true;
            }
        } else if (event.event_type == URMA_EVENT_JETTY_ERR) {
            for (std::size_t i = 0; i < objects.fault_jetties.size(); ++i) {
                if (event.element.jetty == objects.fault_jetties[i]) {
                    ++stats.fault_jetty[i];
                    matched = true;
                }
            }
            if (event.element.jetty == objects.healthy_jetty) {
                ++stats.healthy;
                matched = true;
            }
        }
    }
    stats.other += !matched;
    // union 成员在 Ack 前比较，不通过事件中的指针访问任何资源。
    UrmaApi::AckAsyncEvent(&event);
    ++stats.acknowledged;
    if (!context_matches) {
        return Error("async event context mismatch", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                     std::uint32_t count, std::uint32_t rx_cq_depth, bool server,
                                                     std::uint32_t jets) {
    if (ctx_ || !ctx || !(cap.trans_mode & URMA_TM_RM) || !cap.rm_tp_cap.bs.ctp || (jets != 1 && jets != 2) ||
        count < 1 || count > 8192 || cap.max_jfs_depth < count || cap.max_jfr_depth < count ||
        cap.max_jfc_depth < count + 2 || cap.max_jfc_depth < rx_cq_depth || cap.max_msg_size < kBytes ||
        !cap.max_jfs_sge || !cap.max_jfr_sge) {
        return Error("async queue capability/limits", EINVAL);
    }
    ctx_ = ctx;
    jets_ = jets;
    server_ = server;
    urma_jfc_cfg_t cq{};
    cq.depth = count + 2;
    cq.flag.bs.lock_free = 1;
    tx = UrmaApi::CreateJfc(ctx, &cq);
    if (!tx) {
        return Error("create async TX JFC", errno ? errno : EIO);
    }
    cq.depth = rx_cq_depth;
    rx = UrmaApi::CreateJfc(ctx, &cq);
    if (!rx) {
        return Error("create async RX JFC", errno ? errno : EIO);
    }
    urma_jfr_cfg_t receive{};
    receive.depth = count;
    receive.trans_mode = URMA_TM_RM;
    receive.max_sge = 1;
    receive.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    receive.jfc = rx;
    receive.flag.bs.lock_free = 1;
    jfr = UrmaApi::CreateJfr(ctx, &receive);
    if (!jfr) {
        return Error("create async JFR", errno ? errno : EIO);
    }
    for (std::uint32_t i = 0; i < jets; ++i) {
        urma_jetty_cfg_t config{};
        config.flag.bs.share_jfr = 1;
        config.jfs_cfg.depth = count;
        config.jfs_cfg.trans_mode = URMA_TM_RM;
        config.jfs_cfg.priority = 4;
        config.jfs_cfg.max_sge = 1;
        config.jfs_cfg.max_rsge = 1;
        config.jfs_cfg.rnr_retry = 6;
        config.jfs_cfg.err_timeout = 2;
        config.jfs_cfg.flag.bs.lock_free = 1;
        config.jfs_cfg.jfc = tx;
        config.shared.jfr = jfr;
        config.shared.jfc = rx;
        jetties[i] = UrmaApi::CreateJetty(ctx, &config);
        if (!jetties[i]) {
            return Error("create async jetty", errno ? errno : EIO);
        }
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return Error("get page size", EINVAL);
    }
    const auto page = static_cast<std::size_t>(page_size);
    const auto length = (count * kStride + page - 1) / page * page;
    storage_.assign(length + page - 1, std::byte{0xa5});
    const auto base = reinterpret_cast<std::uintptr_t>(storage_.data());
    const auto aligned = base + (page - base % page) % page;
    memory_ = {reinterpret_cast<std::byte*>(aligned), length};
    urma_seg_cfg_t mr{};
    mr.va = aligned;
    mr.len = length;
    mr.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    segment_ = UrmaApi::RegisterSeg(ctx, &mr);
    if (!segment_) {
        return Error("register async buffers", errno ? errno : EIO);
    }
    states_.assign(count, 0);
    received_ids_.assign(count, false);
    sges_.resize(count);
    sends_.resize(count);
    recvs_.resize(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        auto* payload = memory_.data() + i * kStride + kGuard;
        if (!server) {
            for (std::size_t offset = 0; offset < kBytes; ++offset) {
                payload[offset] = Pattern(i + 1, offset);
            }
        }
        sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(payload), .len = kBytes, .tseg = segment_};
        auto& wr = sends_[i];
        wr.opcode = URMA_OPC_SEND_IMM;
        wr.flag.bs.complete_enable = 1;
        wr.user_ctx = i + 1;
        wr.send.src.sge = &sges_[i];
        wr.send.src.num_sge = 1;
        wr.send.imm_data = i + 1;
        auto& recv = recvs_[i];
        recv.user_ctx = i + 1;
        recv.src.sge = &sges_[i];
        recv.src.num_sge = 1;
        recv.next = i + 1 < count ? &recvs_[i + 1] : nullptr;
    }
    if (server) {
        // RX 按最多 256 条分批，成功前不假定 provider 接受范围；失败保留整个注册区。
        for (std::uint32_t first = 0; first < count; first += 256) {
            const auto n = std::min(256u, count - first);
            recvs_[first + n - 1].next = nullptr;
            urma_jfr_wr_t* bad = nullptr;
            auto status = UrmaApi::PostJfrWr(jfr, &recvs_[first], &bad);
            if (status != URMA_SUCCESS || bad) {
                stats.accepted_known = false;
                return Error("post async RX: retain dependencies", status ? status : EPROTO);
            }
            stats.posted += n;
            std::fill_n(states_.begin() + first, n, 1);
        }
    }
    Query(false);
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::Import(std::span<const urma_jetty_id_t> peers) {
    if (peers.size() != jets_) {
        return Error("async endpoint count mismatch", EPROTO);
    }
    for (std::size_t i = 0; i < peers.size(); ++i) {
        peer_ids_[i] = peers[i];
        if (server_) {
            continue;
        }
        urma_rjetty_t remote{};
        remote.jetty_id = peers[i];
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        peers_[i] = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!peers_[i]) {
            return Error("import async target", errno ? errno : EIO);
        }
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::Post(std::uint32_t first, std::uint32_t count) {
    if (server_ || draining_ || first > sends_.size() || count > sends_.size() - first) {
        return Error("invalid async post range", EINVAL);
    }
    // 每次只投一条，故障实验不依赖链式 WR 的部分接受推断；两个 jetty 交替发送。
    for (std::uint32_t i = first; i < first + count; ++i) {
        if (states_[i] != 0) {
            return Error("async WR replay rejected", EPROTO);
        }
        auto& wr = sends_[i];
        wr.next = nullptr;
        wr.tjetty = peers_[i % jets_];
        urma_jfs_wr_t* bad = nullptr;
        const auto status = UrmaApi::PostJettySendWr(jetties[i % jets_], &wr, &bad);
        stats.post_status = status;
        if ((status == URMA_SUCCESS && bad) || (status != URMA_SUCCESS && bad != &wr)) {
            stats.accepted_known = false;
            return Error("async provider acceptance unknown", EPROTO);
        }
        if (status != URMA_SUCCESS) {
            return Error("async provider rejected WR", status);
        }
        states_[i] = 1;
        ++stats.accepted;
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::Complete(const urma_cr_t& cr, bool require_success, bool flush) {
    if (static_cast<bool>(cr.flag.bs.s_r) != server_ || !cr.flag.bs.jetty) {
        return Error("async CQE direction mismatch", EPROTO);
    }
    std::size_t lane = jets_;
    for (std::size_t i = 0; i < jets_; ++i) {
        if (cr.local_id == jetties[i]->jetty_id.id) {
            lane = i;
        }
    }
    if (lane == jets_) {
        return Error("async CQE unknown local jetty", EPROTO);
    }
    // 边界没有合法 user_ctx；即使资源由硬件主动出错，也仅记录边界，不伪造 WR 退休。
    if (!flush && !server_ && cr.status == URMA_CR_WR_FLUSH_ERR_DONE) {
        if (stats.flush_done[lane]++) {
            return Error("duplicate async flush boundary", EPROTO);
        }
        return {};
    }
    if (flush ? cr.status != URMA_CR_WR_UNHANDLED : !Terminal(cr.status)) {
        return Error("async unexpected CQE status", EPROTO);
    }
    const auto id = cr.user_ctx;
    if (!id || id > states_.size() || states_[id - 1] != 1 || (!server_ && (id - 1) % jets_ != lane)) {
        return Error("async unknown/duplicate completion identity", EPROTO);
    }
    states_[id - 1] = 2;
    ++stats.statuses[static_cast<std::size_t>(cr.status)];
    stats.rx_retired += server_;
    if (!server_) {
        ++stats.retired;
    } else if (cr.status == URMA_CR_SUCCESS) {
        const auto sequence = cr.imm_data;
        if (!sequence || sequence > states_.size() || received_ids_[sequence - 1] || cr.completion_len != kBytes ||
            cr.opcode != URMA_CR_OPC_SEND_WITH_IMM || lane != (sequence - 1) % jets_ ||
            !SameEndpoint(cr.remote_id, peer_ids_[(sequence - 1) % jets_])) {
            return Error("async RX length/opcode/source/IMM", EPROTO);
        }
        received_ids_[sequence - 1] = true;
        auto block = memory_.subspan((id - 1) * kStride, kStride);
        for (std::size_t i = 0; i < block.size(); ++i) {
            const auto expected = i < kGuard || i >= kGuard + kBytes ? std::byte{0xa5} : Pattern(sequence, i - kGuard);
            if (block[i] != expected) {
                return Error("async RX payload/guard mismatch", EILSEQ);
            }
        }
        ++stats.received;
    }
    if (require_success && cr.status != URMA_CR_SUCCESS) {
        return Error("healthy/baseline CQE failed", static_cast<int>(cr.status));
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::Poll(bool require_success) {
    if (poll_failed_) {
        return require_success ? std::expected<void, ToolError>(Error("healthy JFC unavailable", EIO))
                               : std::expected<void, ToolError>{};
    }
    std::array<urma_cr_t, 64> completions{};
    const int count = UrmaApi::PollJfc(server_ ? rx : tx, completions.size(), completions.data());
    if (count < 0 || count > static_cast<int>(completions.size())) {
        stats.poll_status = count;
        poll_failed_ = true;
        if (require_success || count > static_cast<int>(completions.size())) {
            return Error("async poll failed", count);
        }
        return {};
    }
    std::optional<ToolError> failure;
    for (int i = 0; i < count; ++i) {
        auto result = Complete(completions[i], require_success, false);
        if (!result && !failure) {
            failure = result.error();
        }
    }
    if (failure) {
        return std::unexpected(*failure);
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::BeginDrain() {
    draining_ = true;
    std::optional<ToolError> failure;
    for (std::size_t i = 0; i < jets_; ++i) {
        if (stats.flush_done[i]) {
            continue;
        }
        urma_jetty_attr_t attr{};
        attr.mask = JETTY_STATE;
        attr.state = URMA_JETTY_STATE_ERROR;
        const auto status = UrmaApi::ModifyJetty(jetties[i], &attr);
        stats.modify_statuses[i] = status;
        if (status != URMA_SUCCESS) {
            stats.modify_status = status;
            if (!failure) {
                failure = ToolError{"modify fault sender ERROR", status};
            }
        }
    }
    if (failure) {
        return std::unexpected(*failure);
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorQueue::Flush() {
    for (std::size_t i = 0; i < jets_; ++i) {
        if (!draining_ || !stats.flush_done[i]) {
            continue;
        }
        // 某些 provider 在队列已空时拒绝 flush；只对仍有未终结身份的 SQ 调用。
        bool pending = false;
        for (std::size_t j = i; j < states_.size(); j += jets_) {
            pending |= states_[j] == 1;
        }
        if (!pending) {
            continue;
        }
        std::array<urma_cr_t, 64> completions{};
        const int count = UrmaApi::FlushJetty(jetties[i], completions.size(), completions.data());
        if (count < 0 || count > static_cast<int>(completions.size())) {
            stats.flush_status = count;
            return Error("flush fault sender", count);
        }
        std::optional<ToolError> failure;
        for (int j = 0; j < count; ++j) {
            auto result = completions[j].local_id != jetties[i]->jetty_id.id
                              ? std::expected<void, ToolError>(Error("flush returned another SQ", EPROTO))
                              : Complete(completions[j], false, true);
            if (!result && !failure) {
                failure = result.error();
            }
        }
        if (failure) {
            return std::unexpected(*failure);
        }
    }
    return {};
}

void AsyncErrorQueue::Query(bool after) {
    urma_jfr_cfg_t receive_cfg{};
    urma_jfr_attr_t receive_attr{};
    const auto receive_status = UrmaApi::QueryJfr(jfr, &receive_cfg, &receive_attr);
    stats.jfr_query_status = receive_status;
    stats.queried_rx_depth = receive_status == URMA_SUCCESS ? receive_cfg.depth : 0;
    (after ? stats.jfr_state_after : stats.jfr_state_before) =
        receive_status == URMA_SUCCESS ? static_cast<int>(receive_attr.state) : -1;
    for (std::size_t i = 0; i < jets_; ++i) {
        urma_jetty_cfg_t cfg{};
        urma_jetty_attr_t attr{};
        const auto status = UrmaApi::QueryJetty(jetties[i], &cfg, &attr);
        stats.query_status[i] = status;
        (after ? stats.state_after : stats.state_before)[i] =
            status == URMA_SUCCESS ? static_cast<int>(attr.state) : -1;
    }
}

bool AsyncErrorQueue::complete() const noexcept {
    return stats.accepted_known && !poll_failed_ &&
           (server_ ? stats.posted == stats.rx_retired : stats.accepted == stats.retired);
}

std::expected<void, ToolError> AsyncErrorQueue::Close() {
    if (!complete()) {
        return Error("async queue DMA unresolved", EBUSY);
    }
    for (auto*& peer : peers_) {
        if (peer) {
            const auto result = UrmaApi::UnimportJetty(peer);
            if (result != URMA_SUCCESS) {
                return Error("unimport async jetty", result);
            }
            peer = nullptr;
        }
    }
    for (auto*& jetty : jetties) {
        if (jetty) {
            const auto result = UrmaApi::DeleteJetty(jetty);
            if (result != URMA_SUCCESS) {
                return Error("delete async jetty", result);
            }
            jetty = nullptr;
        }
    }
    if (jfr) {
        const auto result = UrmaApi::DeleteJfr(jfr);
        if (result != URMA_SUCCESS) {
            return Error("delete async JFR", result);
        }
        jfr = nullptr;
    }
    for (auto** cq : {&rx, &tx}) {
        if (*cq) {
            const auto result = UrmaApi::DeleteJfc(*cq);
            if (result != URMA_SUCCESS) {
                return Error("delete async JFC", result);
            }
            *cq = nullptr;
        }
    }
    if (segment_) {
        const auto result = UrmaApi::UnregisterSeg(segment_);
        if (result != URMA_SUCCESS) {
            return Error("unregister async memory", result);
        }
        segment_ = nullptr;
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                       const AsyncErrorOptions& options, bool server) {
    if (auto valid = ValidateAsyncErrorOptions(options); !valid) {
        return valid;
    }
    if (!ctx || ctx->async_fd < 0 || objects_.ctx) {
        return Error("async error session requires context event fd", EINVAL);
    }
    options_ = options;
    server_ = server;
    const auto fault_count = options.attempts + 2;
    const auto rx_depth = server && options.fault == AsyncFault::kJfcOverflow ? options.cq_depth : fault_count;
    if (auto opened = fault.Open(ctx, cap, fault_count, rx_depth, server, 2); !opened) {
        return opened;
    }
    const auto healthy_count = options.healthy_rounds + 2;
    if (auto opened = healthy.Open(ctx, cap, healthy_count, healthy_count, server, 1); !opened) {
        return opened;
    }
    objects_ = {ctx,        fault.tx,   fault.rx,    fault.jfr,         fault.jetties,
                healthy.tx, healthy.rx, healthy.jfr, healthy.jetties[0]};
    return {};
}

std::expected<void, ToolError> AsyncErrorSession::Collect(AsyncErrorEvents& events) {
    for (int i = 0; i < 64; ++i) {
        pollfd descriptor{objects_.ctx->async_fd, POLLIN, 0};
        const auto ready = ::poll(&descriptor, 1, 0);
        if (ready < 0) {
            if (errno == EINTR) {
                return {};
            }
            return Error("poll async error fd", errno);
        }
        if (!ready) {
            return {};
        }
        if (!(descriptor.revents & POLLIN) || descriptor.revents & (POLLERR | POLLNVAL | POLLHUP)) {
            return Error("async error fd unavailable", EIO);
        }
        urma_async_event_t event{};
        const auto status = UrmaApi::GetAsyncEvent(objects_.ctx, &event);
        if (status != URMA_SUCCESS) {
            return Error("get async error event", status);
        }
        auto recorded = RecordAsyncErrorEvent(event, objects_, events);
        if (!recorded) {
            return recorded;
        }
    }
    return {};
}

std::expected<void, ToolError> AsyncErrorSession::Exchange(ControlChannel& channel) {
    std::array<std::uint32_t, 24> local{};
    local[0] = htonl(kHello);
    local[1] = htonl(server_);
    local[2] = htonl(static_cast<std::uint32_t>(options_.fault));
    local[3] = htonl(options_.attempts);
    local[4] = htonl(options_.cq_depth);
    local[5] = htonl(options_.healthy_rounds);
    local[6] = htonl(options_.observe_ms);
    const std::array<urma_jetty_t*, 3> jets{fault.jetties[0], fault.jetties[1], healthy.jetties[0]};
    for (std::size_t i = 0; i < jets.size(); ++i) {
        std::memcpy(&local[8 + i * 5], jets[i]->jetty_id.eid.raw, 16);
        local[12 + i * 5] = htonl(jets[i]->jetty_id.id);
    }
    if (auto sent = channel.Write(std::as_bytes(std::span(local)), options_.timeout_ms); !sent) {
        return sent;
    }
    std::array<std::uint32_t, 24> peer{};
    if (auto received = channel.Read(std::as_writable_bytes(std::span(peer)), options_.timeout_ms); !received) {
        return received;
    }
    if (peer[0] != local[0] || ntohl(peer[1]) != !server_ || peer[7] || peer[23] ||
        !std::equal(local.begin() + 2, local.begin() + 7, peer.begin() + 2)) {
        return Error("async error peer version/role/options mismatch", EPROTO);
    }
    std::array<urma_jetty_id_t, 3> ids{};
    for (std::size_t i = 0; i < ids.size(); ++i) {
        std::memcpy(ids[i].eid.raw, &peer[8 + i * 5], 16);
        ids[i].id = ntohl(peer[12 + i * 5]);
    }
    if (auto imported = fault.Import(std::span(ids).first(2)); !imported) {
        return imported;
    }
    return healthy.Import(std::span(ids).last(1));
}

std::expected<void, ToolError> AsyncErrorSession::Baseline(ControlChannel& channel) {
    if (!server_) {
        if (auto posted = fault.Post(0, 2); !posted) {
            return posted;
        }
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    while ((server_ ? fault.stats.received : fault.stats.retired) != 2) {
        if (auto poll = fault.Poll(true); !poll) {
            return poll;
        }
        if (auto events = Collect(stats.baseline); !events) {
            return events;
        }
        if (Clock::now() >= deadline) {
            return Error("async fault baseline timeout", ETIMEDOUT);
        }
    }
    if (auto sent = channel.SendMarker(0x42415345, options_.timeout_ms); !sent) {
        return sent;
    }
    return channel.ExpectMarker(0x42415345, options_.timeout_ms);
}

std::expected<void, ToolError> AsyncErrorSession::HealthyRound(ControlChannel& channel, std::uint32_t round,
                                                               std::uint32_t hold_ms) {
    if (!server_) {
        if (auto posted = healthy.Post(round, 1); !posted) {
            return posted;
        }
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms + hold_ms);
    const auto hold_until = Clock::now() + std::chrono::milliseconds(hold_ms);
    do {
        if (auto poll = healthy.Poll(true); !poll) {
            return poll;
        }
        // 自动溢出阶段有意不 poll 服务端故障 RX JFC；健康队列使用独立 JFC/JFR。
        if (!server_) {
            if (auto poll = fault.Poll(false); !poll) {
                return poll;
            }
        }
        auto& phase = round == 0                             ? stats.baseline
                      : round == options_.healthy_rounds + 1 ? stats.after
                                                             : stats.observation;
        if (auto events = Collect(phase); !events) {
            return events;
        }
        if (Clock::now() >= deadline) {
            return Error("healthy async control traffic timeout", ETIMEDOUT);
        }
        std::this_thread::yield();
    } while ((server_ ? healthy.stats.received : healthy.stats.retired) != round + 1 || Clock::now() < hold_until);
    if (auto sent = channel.SendMarker(0x48454100 + round, options_.timeout_ms); !sent) {
        return sent;
    }
    return channel.ExpectMarker(0x48454100 + round, options_.timeout_ms);
}

std::expected<void, ToolError> AsyncErrorSession::Run(ControlChannel& channel) {
    if (started_ || !objects_.ctx) {
        return Error("async session not ready or already used", EINVAL);
    }
    started_ = true;
    if (auto exchanged = Exchange(channel); !exchanged) {
        return exchanged;
    }
    if (auto baseline = Baseline(channel); !baseline) {
        return baseline;
    }
    if (auto baseline = HealthyRound(channel, 0, 0); !baseline) {
        return baseline;
    }
    if (stats.baseline.retrieved) {
        return Error("async error observed before injection window", EPROTO);
    }
    std::println("INJECTION_WINDOW: mode={} role={} fault_rx_jfc={} fault_jfr={} fault_jetty0={} "
                 "fault_jetty1={} healthy_rx_jfc={} healthy_jetty={} observe_ms={}",
                 AsyncFaultName(options_.fault), server_ ? "receiver" : "sender", fault.rx->jfc_id.id,
                 fault.jfr->jfr_id.id, fault.jetties[0]->jetty_id.id, fault.jetties[1]->jetty_id.id,
                 healthy.rx->jfc_id.id, healthy.jetties[0]->jetty_id.id, options_.observe_ms);
    std::fflush(stdout);
    // 外部模式不执行任意 shell 命令；操作者使用现场确认过的注入工具，目标仅为服务端打印的对象。
    if (!server_ && options_.fault == AsyncFault::kJfcOverflow) {
        auto posted = fault.Post(2, options_.attempts);
        if (!posted && !fault.stats.accepted_known) {
            return posted;
        }
        // 明确拒绝仍可观测已接受前缀，最终不会伪造未发送的 RX 完成。
    }
    for (std::uint32_t round = 1; round <= options_.healthy_rounds; ++round) {
        // 外部注入窗口持续提供故障目标流量，便于观察 JFC 错误与关联 jetty AE 的联动。
        // 一旦 provider 明确拒绝则停止该批次；未接受后缀绝不自动重发。
        if (!server_ && options_.fault != AsyncFault::kJfcOverflow) {
            const auto first = (round - 1) * options_.attempts / options_.healthy_rounds;
            const auto end = round * options_.attempts / options_.healthy_rounds;
            auto posted = fault.Post(2 + first, end - first);
            if (!posted && !fault.stats.accepted_known) {
                return posted;
            }
        }
        const auto hold =
            options_.observe_ms / options_.healthy_rounds + (round <= options_.observe_ms % options_.healthy_rounds);
        if (auto traffic = HealthyRound(channel, round, hold); !traffic) {
            return traffic;
        }
    }
    if (auto events = Collect(stats.observation); !events) {
        return events;
    }
    // 先锁定窗口内故障证据，再发送 after 探针；迟到到 after/cleanup 的事件不追认先前隔离成立。
    stats.target_observed =
        server_ && (options_.fault == AsyncFault::kExternalJfr ? stats.observation.fault_jfr != 0
                                                               : stats.observation.fault_rx_jfc != 0);
    // 接收端锁定观察结果后才允许客户端提交 after WR，不能仅靠各自结束上一轮的时间推断顺序。
    auto after_ready = server_ ? channel.SendMarker(0x41465452, options_.timeout_ms)
                               : channel.ExpectMarker(0x41465452, options_.timeout_ms);
    if (!after_ready) {
        return after_ready;
    }
    if (auto traffic = HealthyRound(channel, options_.healthy_rounds + 1, 0); !traffic) {
        return traffic;
    }
    fault.Query(true);
    healthy.Query(true);
    if (stats.observation.healthy || stats.after.healthy) {
        return Error("async fault affected independent healthy queues", EIO);
    }
    // 观察完成后才开始主动 ERROR/flush，cleanup 事件不得作为注入成功证据。
    if (!server_ && fault.stats.accepted != fault.stats.retired) {
        auto drained = fault.BeginDrain();
        if (!drained) {
            std::println("NOT_OBSERVED: explicit ERROR drain failed status={}", drained.error().code);
        }
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    do {
        if (auto polled = fault.Poll(false); !polled) {
            return polled;
        }
        if (!server_) {
            if (auto flushed = fault.Flush(); !flushed) {
                return flushed;
            }
        }
        if (auto events = Collect(stats.cleanup); !events) {
            return events;
        }
        if (fault.complete()) {
            break;
        }
        std::this_thread::yield();
    } while (Clock::now() < deadline);
    std::array<std::uint32_t, 3> local{htonl(0x52455331), htonl(stats.target_observed),
                                       htonl(fault.complete() && healthy.complete())};
    // 一端可能立即退休完，另一端仍用满 cleanup 窗口。结果等待须覆盖对端剩余观察时间。
    const auto result_timeout = 3 * options_.timeout_ms;
    if (auto sent = channel.Write(std::as_bytes(std::span(local)), result_timeout); !sent) {
        return sent;
    }
    std::array<std::uint32_t, 3> peer{};
    if (auto read = channel.Read(std::as_writable_bytes(std::span(peer)), result_timeout); !read) {
        return read;
    }
    if (peer[0] != local[0] || ntohl(peer[1]) > 1 || ntohl(peer[2]) > 1 || (server_ && peer[1])) {
        return Error("async result mismatch", EPROTO);
    }
    stats.peer_target_observed = ntohl(peer[1]);
    stats.can_close = ntohl(local[2]) && ntohl(peer[2]);
    return {};
}

std::expected<void, ToolError> AsyncErrorSession::Close() {
    if (!stats.can_close) {
        return Error("async DMA evidence incomplete; retain all dependencies", EBUSY);
    }
    if (auto events = Collect(stats.cleanup); !events) {
        return events;
    }
    if (auto closed = fault.Close(); !closed) {
        return closed;
    }
    return healthy.Close();
}
} // namespace tools
} // namespace kbsocket
