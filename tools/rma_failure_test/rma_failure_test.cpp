// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/rma_failure_test/rma_failure_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
constexpr std::byte kGuard{0xa5};
constexpr std::byte kPoison{0xff};
constexpr std::uint32_t kBaseline = 0x46410000;
constexpr std::uint32_t kBefore = 0x46420000;
constexpr std::uint32_t kExited = 0x46430000;
constexpr std::uint32_t kMixed = 0x46440000;
constexpr std::uint32_t kAfter = 0x46450000;
constexpr std::uint32_t kQuiescent = 0x46460000;
auto Error(const char* operation, int code = EIO) {
    return std::unexpected(ToolError{operation, code ? code : EPROTO});
}
RmaOptions WriteOptions(const RmaFailureOptions& options) {
    // 内层协议描述单次 WRITE；本工具用外层 batch 管理并校验整批独立槽位。
    return {.operation = RmaOperation::kWrite,
            .bytes = options.bytes,
            .rounds = options.rounds,
            .timeout_ms = options.timeout_ms,
            .window = 1};
}
std::byte Pattern(RmaFailurePhase phase, std::uint32_t round, std::size_t target, std::size_t slot,
                  std::size_t offset) {
    // 分散编码阶段、轮次和槽号，避免沿用上轮数据仍然通过校验；0xff 专用于未写入目的区。
    const auto generation = static_cast<std::uint32_t>(phase) * 256U + round;
    const auto digit = offset % 2 == 0 ? generation % 251 : generation / 251;
    return static_cast<std::byte>((digit + target * 91 + slot * 17 + offset + offset / 251) % 251);
}
bool IsTerminal(urma_cr_status_t status) {
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

std::expected<void, ToolError> RmaFailureResources::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                         const RmaFailureOptions& options, RmaFailureRole role) {
    if (ctx_ || opened_ || failed_close_) {
        return Error("WRITE failure resources already opened", EBUSY);
    }
    auto valid = ValidateRmaFailureOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx) {
        return Error("WRITE failure context is null", EINVAL);
    }
    auto supported = CheckRmaCapability(WriteOptions(options), cap);
    if (!supported) {
        return supported;
    }
    ctx_ = ctx;
    options_ = options;
    role_ = role;
    hello_.options = WriteOptions(options);
    hello_.server = role != RmaFailureRole::kClient;
    raw::JettyPoolConfig config;
    config.jetty_count = 1;
    config.tx_depth = 2 * options.batch;
    config.rx_depth = 1;
    config.tx_cq_depth = 2 * options.batch + 1;
    config.rx_cq_depth = 1;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("create WRITE failure RM_CTP jetty", opened.error().failure.provider_error);
    }
    auto ticket = pool_.Reserve();
    if (!ticket) {
        return Error("reserve WRITE failure lane");
    }
    auto jetty = pool_.Get(ticket->lane);
    auto canceled = pool_.Cancel(*ticket);
    if (!jetty || !canceled) {
        return Error("get WRITE failure jetty");
    }
    jetty_ = *jetty;
    stats.jetty_id = jetty_->jetty_id.id;
    hello_.endpoint = jetty_->jetty_id;
    slots_ = options.batch * (role == RmaFailureRole::kClient ? 2U : 1U);
    const auto page_value = ::sysconf(_SC_PAGESIZE);
    if (page_value <= 0) {
        return Error("read page size");
    }
    const auto page = static_cast<std::size_t>(page_value);
    const auto length = (RmaSlotStride(hello_.options) * slots_ + page - 1) / page * page;
    storage_.resize(length + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
    registered_ = std::span(storage_).subspan((page - address % page) % page, length);
    std::fill(registered_.begin(), registered_.end(), kGuard);
    for (std::size_t slot = 0; slot < slots_; ++slot) {
        auto payload = Payload(slot);
        std::fill(payload.begin(), payload.end(), kPoison);
    }
    urma_seg_cfg_t cfg{};
    cfg.va = reinterpret_cast<std::uintptr_t>(registered_.data());
    cfg.len = registered_.size();
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &cfg);
    if (!segment_) {
        return Error("register WRITE failure memory", errno ? errno : EIO);
    }
    urma_seg_t* exported = nullptr;
    std::uint32_t size = 0;
    const auto status = UrmaApi::GetSegCtx(segment_, &exported, &size);
    ScopeExit release([&]() noexcept {
        if (exported) {
            UrmaApi::PutSegCtx(exported);
        }
    });
    if (status != URMA_SUCCESS || !exported) {
        return Error("export WRITE failure segment", static_cast<int>(status));
    }
    if (size != sizeof(urma_seg_t) || exported->attr.bs.has_user_info) {
        return Error("WRITE failure requires raw segment descriptor", EOPNOTSUPP);
    }
    hello_.segment = *exported;
    hello_.payload_address = reinterpret_cast<std::uintptr_t>(Payload(0).data());
    auto descriptor = MatchRmaFailureEndpoint(hello_, options_, hello_.server);
    if (!descriptor) {
        return descriptor;
    }
    opened_ = true;
    return {};
}

std::expected<void, ToolError> RmaFailureResources::Import(const std::array<RmaHello, 2>& peers) {
    if (!opened_ || role_ != RmaFailureRole::kClient || imported_ || failed_) {
        return Error("WRITE failure import not ready", EINVAL);
    }
    for (const auto& peer : peers) {
        auto valid = MatchRmaFailureEndpoint(peer, options_, true);
        if (!valid) {
            return valid;
        }
    }
    if (peers[0].endpoint.id == peers[1].endpoint.id &&
        std::memcmp(peers[0].endpoint.eid.raw, peers[1].endpoint.eid.raw, URMA_EID_SIZE) == 0) {
        return Error("WRITE targets must differ", EINVAL);
    }
    // 部分 import 失败也禁止重新覆盖已导入对象；入口统一保留/释放其完整依赖。
    failed_ = true;
    peers_ = peers;
    for (std::size_t i = 0; i < peers.size(); ++i) {
        urma_import_seg_flag_t flags{};
        flags.bs.access = URMA_ACCESS_WRITE;
        errno = 0;
        remote_segments_[i] = UrmaApi::ImportSeg(ctx_, &peers_[i].segment, &peers_[i].token, 0, flags);
        if (!remote_segments_[i]) {
            return Error("import WRITE failure destination", errno ? errno : EIO);
        }
        urma_rjetty_t remote{};
        remote.jetty_id = peers[i].endpoint;
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        targets_[i] = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!targets_[i]) {
            return Error("import WRITE failure target", errno ? errno : EIO);
        }
    }
    imported_ = true;
    failed_ = false;
    return {};
}

std::span<std::byte> RmaFailureResources::Payload(std::size_t slot) {
    return registered_.subspan(slot * RmaSlotStride(hello_.options) + kRmaGuardBytes, options_.bytes);
}

std::expected<void, ToolError> RmaFailureResources::CheckGuards() {
    const auto stride = RmaSlotStride(hello_.options);
    for (std::size_t slot = 0; slot < slots_; ++slot) {
        const auto memory = registered_.subspan(slot * stride, stride);
        for (std::size_t i = 0; i < kRmaGuardBytes; ++i) {
            if (memory[i] != kGuard || memory[kRmaGuardBytes + options_.bytes + i] != kGuard) {
                return Error("WRITE failure guard overwritten", EPROTO);
            }
        }
    }
    for (const auto byte : registered_.subspan(slots_ * stride)) {
        if (byte != kGuard) {
            return Error("WRITE failure registration padding overwritten", EPROTO);
        }
    }
    return {};
}

std::expected<void, ToolError> RmaFailureResources::VerifyTarget(RmaFailurePhase phase, std::uint32_t round) {
    const bool fault = role_ == RmaFailureRole::kFaultTarget;
    const bool baseline = phase == RmaFailurePhase::kHealthyBaseline || phase == RmaFailurePhase::kFaultBaseline;
    if (!opened_ || failed_ || role_ == RmaFailureRole::kClient ||
        fault != (phase == RmaFailurePhase::kFaultBaseline) || round >= options_.rounds || (baseline && round)) {
        return Error("WRITE target verification phase not ready", EINVAL);
    }
    failed_ = true;
    auto guarded = CheckGuards();
    if (!guarded) {
        return guarded;
    }
    const auto count = baseline ? 1U : options_.batch;
    for (std::size_t slot = 0; slot < slots_; ++slot) {
        const auto payload = Payload(slot);
        for (std::size_t i = 0; i < payload.size(); ++i) {
            const auto expected = slot < count ? Pattern(phase, round, fault, slot, i) : kPoison;
            if (payload[i] != expected) {
                return Error("WRITE target payload mismatch", EPROTO);
            }
        }
    }
    Counters(phase, fault).verified += count;
    // 仅在对端声明该批 WR 已成功终结并完成校验后清空；确认发出前目的区就绪。
    for (std::size_t slot = 0; slot < slots_; ++slot) {
        auto payload = Payload(slot);
        std::fill(payload.begin(), payload.end(), kPoison);
    }
    failed_ = false;
    return {};
}

std::expected<void, ToolError> RmaFailureResources::ConfirmRemoteWrites(RmaFailurePhase phase) {
    auto& counters = Counters(phase, phase == RmaFailurePhase::kFaultBaseline);
    const bool baseline = phase == RmaFailurePhase::kHealthyBaseline || phase == RmaFailurePhase::kFaultBaseline;
    const auto count = baseline ? 1U : options_.batch;
    if (role_ != RmaFailureRole::kClient || failed_ || stats.pending ||
        counters.verified + count > counters.statuses[URMA_CR_SUCCESS]) {
        return Error("WRITE remote verification lacks successful local completions", EPROTO);
    }
    counters.verified += count;
    return {};
}

std::expected<void, ToolError> RmaFailureResources::CheckReady() {
    urma_jetty_cfg_t config{};
    urma_jetty_attr_t attr{};
    const auto status = UrmaApi::QueryJetty(jetty_, &config, &attr);
    ++stats.state_checks;
    stats.last_query_status = static_cast<int>(status);
    stats.last_state = status == URMA_SUCCESS ? static_cast<int>(attr.state) : -1;
    if (status != URMA_SUCCESS || attr.state != URMA_JETTY_STATE_READY) {
        return Error("WRITE failure SQ is not confirmed READY",
                     status != URMA_SUCCESS ? static_cast<int>(status) : EIO);
    }
    return {};
}

RmaWriteCounters& RmaFailureResources::Counters(RmaFailurePhase phase, std::size_t target) {
    switch (phase) {
    case RmaFailurePhase::kHealthyBaseline:
        return stats.healthy_baseline;
    case RmaFailurePhase::kFaultBaseline:
        return stats.fault_baseline;
    case RmaFailurePhase::kHealthyBefore:
        return stats.healthy_before;
    case RmaFailurePhase::kMixed:
        return target ? stats.fault_mixed : stats.healthy_mixed;
    case RmaFailurePhase::kHealthyAfter:
        return stats.healthy_after;
    }
    return stats.healthy_baseline;
}

std::expected<void, ToolError> RmaFailureResources::Transfer(RmaFailurePhase phase, std::uint32_t round) {
    const bool mixed = phase == RmaFailurePhase::kMixed;
    const bool after_exit = mixed || phase == RmaFailurePhase::kHealthyAfter;
    const bool baseline = phase == RmaFailurePhase::kHealthyBaseline || phase == RmaFailurePhase::kFaultBaseline;
    if (!opened_ || !imported_ || failed_ || stats.pending || !stats.accepted_known || round >= options_.rounds ||
        (baseline && round) || after_exit != stats.peer_exit_confirmed) {
        return Error("WRITE failure transfer phase not ready", EINVAL);
    }
    // 任何失败都禁止再次投递；本测试不重放 WRITE，也不替硬件恢复/修改 SQ。
    failed_ = true;
    auto ready = CheckReady();
    if (!ready) {
        return ready;
    }
    const std::size_t count = baseline ? 1 : options_.batch * (mixed ? 2U : 1U);
    const auto target_of = [=](std::size_t i) {
        return mixed ? i % 2 : (phase == RmaFailurePhase::kFaultBaseline ? 1U : 0U);
    };
    const auto slot_of = [=](std::size_t i) { return mixed ? i / 2 : i; };
    const auto first_id = next_id_;
    next_id_ += count;
    retired_.fill(false);
    for (std::size_t i = 0; i < count; ++i) {
        const auto target = target_of(i);
        const auto slot = slot_of(i);
        auto source = Payload(target * options_.batch + slot);
        for (std::size_t j = 0; j < source.size(); ++j) {
            source[j] = Pattern(phase, round, target, slot, j);
        }
        sources_[i] = {
            .addr = reinterpret_cast<std::uintptr_t>(source.data()), .len = options_.bytes, .tseg = segment_};
        destinations_[i] = {.addr = peers_[target].payload_address + slot * RmaSlotStride(hello_.options),
                            .len = options_.bytes,
                            .tseg = remote_segments_[target]};
        auto& wr = wrs_[i];
        wr = {};
        wr.opcode = URMA_OPC_WRITE;
        wr.flag.bs.complete_enable = 1;
        wr.rw.src = {&sources_[i], 1};
        wr.rw.dst = {&destinations_[i], 1};
        wr.tjetty = targets_[target];
        wr.user_ctx = first_id + i;
        wr.next = i + 1 < count ? &wrs_[i + 1] : nullptr;
    }
    urma_jfs_wr_t* bad = nullptr;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    stats.pending = true;
    ++stats.post_calls;
    const auto posted = UrmaApi::PostJettySendWr(jetty_, wrs_.data(), &bad);
    stats.last_post_status = static_cast<int>(posted);
    const bool post_failed = posted != URMA_SUCCESS || bad;
    std::size_t accepted = count;
    if (post_failed) {
        bool known = false;
        if (posted != URMA_SUCCESS) {
            for (std::size_t i = 0; i < count; ++i) {
                if (bad == &wrs_[i]) {
                    accepted = i;
                    known = true;
                    break;
                }
            }
        }
        if (!known) {
            stats.accepted_known = false;
            return Error("WRITE failure post acceptance unresolved", static_cast<int>(posted));
        }
        stats.partial_batches += accepted != 0;
    }
    for (std::size_t i = 0; i < accepted; ++i) {
        ++Counters(phase, target_of(i)).accepted;
    }
    auto remaining = accepted;
    stats.pending = remaining != 0;
    std::expected<void, ToolError> result;
    std::array<urma_cr_t, 64> completions{};
    while (remaining) {
        auto polled = pool_.PollSend(completions);
        if (!polled) {
            return Error("poll WRITE failure completion", polled.error().provider_error);
        }
        bool malformed = false;
        for (int i = 0; i < *polled; ++i) {
            const auto& cr = completions[i];
            stats.last_completion_status = static_cast<int>(cr.status);
            // 假 CQE 不含合法 user_ctx；先检查类型，再检查身份，仍处理同批后续可信 CQE。
            if (!IsTerminal(cr.status) || cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.local_id != stats.jetty_id ||
                cr.user_ctx < first_id || cr.user_ctx - first_id >= accepted) {
                malformed = true;
                if (result) {
                    result = Error("unexpected WRITE failure completion type/id", EPROTO);
                }
                continue;
            }
            const auto index = static_cast<std::size_t>(cr.user_ctx - first_id);
            if (retired_[index]) {
                malformed = true;
                if (result) {
                    result = Error("duplicate WRITE failure completion", EPROTO);
                }
                continue;
            }
            retired_[index] = true;
            --remaining;
            const auto target = target_of(index);
            auto& counters = Counters(phase, target);
            ++counters.retired;
            ++counters.statuses[static_cast<std::size_t>(cr.status)];
            const bool expect_success = !mixed || target == 0;
            // 已知芯片限制：任意 status 9 后 jetty 不可继续使用，查询 READY 也不能解除门禁。
            // 先退休这一批已经受理的 WR，再失败退出；不向同一 SQ 投递后续健康探测。
            if (cr.status == URMA_CR_ACK_TIMEOUT_ERR && result) {
                result = Error("WRITE ACK_TIMEOUT makes SQ unusable; stop posting", static_cast<int>(cr.status));
            }
            if ((cr.status == URMA_CR_SUCCESS) != expect_success && result) {
                result = Error(expect_success ? "healthy/baseline WRITE failed"
                                              : "WRITE to exited target unexpectedly succeeded",
                               cr.status == URMA_CR_SUCCESS ? EPROTO : static_cast<int>(cr.status));
            }
        }
        stats.pending = remaining != 0;
        if (malformed) {
            return result;
        }
        if (remaining && std::chrono::steady_clock::now() >= deadline) {
            return Error("wait WRITE failure completion", ETIMEDOUT);
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    // WRITE 的本地 CQE 不证明远端 payload 校验通过；这里只检查 DMA 源未被修改。
    // verified 计数在远端 VerifyTarget 成功并通过控制通道确认后更新。
    for (std::size_t i = 0; i < accepted; ++i) {
        const auto target = target_of(i);
        const auto slot = slot_of(i);
        const auto source = Payload(target * options_.batch + slot);
        for (std::size_t j = 0; j < source.size(); ++j) {
            if (source[j] != Pattern(phase, round, target, slot, j) && result) {
                result = Error("WRITE source payload modified", EPROTO);
            }
        }
    }
    auto guarded = CheckGuards();
    auto queried = CheckReady();
    if (!result) {
        return result;
    }
    if (!guarded) {
        return guarded;
    }
    if (!queried) {
        return queried;
    }
    if (post_failed) {
        return Error("WRITE failure accepted prefix retired; no replay", static_cast<int>(posted));
    }
    failed_ = false;
    return {};
}

std::expected<void, ToolError> RmaFailureResources::CheckQuiescent() {
    if (!opened_ || stats.pending || !stats.accepted_known) {
        return Error("WRITE failure DMA state unresolved", EBUSY);
    }
    std::array<urma_cr_t, 64> tail{};
    auto polled = pool_.PollSend(tail);
    if (!polled || *polled) {
        failed_ = true;
        return Error("unexpected WRITE failure completion before close",
                     polled ? EPROTO : polled.error().provider_error);
    }
    return CheckGuards();
}

std::expected<void, ToolError> RmaFailureResources::Close() {
    if (stats.pending || !stats.accepted_known || failed_close_) {
        return Error("WRITE failure dependencies still required", EBUSY);
    }
    opened_ = false;
    auto failed = [this](const char* operation, int code) -> std::expected<void, ToolError> {
        failed_close_ = true;
        return Error(operation, code);
    };
    for (auto& target : targets_) {
        if (target) {
            const auto status = UrmaApi::UnimportJetty(target);
            if (status != URMA_SUCCESS) {
                return failed("unimport WRITE failure target", status);
            }
            target = nullptr;
        }
    }
    for (auto& segment : remote_segments_) {
        if (segment) {
            const auto status = UrmaApi::UnimportSeg(segment);
            if (status != URMA_SUCCESS) {
                return failed("unimport WRITE failure destination", status);
            }
            segment = nullptr;
        }
    }
    auto closed = pool_.Close();
    if (!closed) {
        return failed("close WRITE failure queues", closed.error().provider_error);
    }
    jetty_ = nullptr;
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return failed("unregister WRITE failure memory", status);
        }
        segment_ = nullptr;
    }
    registered_ = {};
    storage_.clear();
    ctx_ = nullptr;
    return {};
}

std::expected<void, ToolError> RmaFailureSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                       const RmaFailureOptions& options, bool server) {
    if (opened_ || started_) {
        return Error("WRITE failure session already opened", EBUSY);
    }
    options_ = options;
    server_ = server;
    auto opened = resources.Open(ctx, cap, options, server ? RmaFailureRole::kHealthyTarget : RmaFailureRole::kClient);
    if (!opened) {
        return opened;
    }
    opened_ = true;
    quiescent_ = true;
    return {};
}

std::expected<void, ToolError> RmaFailureSession::Run(ControlChannel& channel, RmaFailurePeer* fault_peer) {
    if (!opened_ || started_ || server_ != (fault_peer != nullptr)) {
        return Error("WRITE failure session not ready", EINVAL);
    }
    started_ = true;
    quiescent_ = false;
    RmaFailureHello local;
    local.options = options_;
    local.server = server_;
    local.endpoints[0] = resources.hello();
    if (server_) {
        auto valid = MatchRmaFailureEndpoint(fault_peer->hello(), options_, true);
        if (!valid) {
            return valid;
        }
        local.endpoints[1] = fault_peer->hello();
    }
    auto written = channel.Write(EncodeRmaFailureHello(local), options_.timeout_ms);
    if (!written) {
        return written;
    }
    RmaFailureHelloBytes wire{};
    auto read = channel.Read(wire, options_.timeout_ms);
    if (!read) {
        return read;
    }
    auto peer = DecodeRmaFailureHello(wire);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (peer->server == server_ || peer->options.bytes != options_.bytes || peer->options.batch != options_.batch ||
        peer->options.rounds != options_.rounds) {
        return Error("WRITE failure peer options mismatch", EPROTO);
    }
    if (!server_) {
        auto imported = resources.Import(peer->endpoints);
        if (!imported) {
            return imported;
        }
    }
    const auto transfer = [&](RmaFailurePhase phase, std::uint32_t round,
                              std::uint32_t marker) -> std::expected<void, ToolError> {
        const bool fault_baseline = phase == RmaFailurePhase::kFaultBaseline;
        // 基线还包含 import 的开销；两端都必须等本轮完成与远端校验确认，才能复用槽位。
        const auto wait_ms = marker == kBaseline ? 3 * options_.timeout_ms : options_.timeout_ms;
        if (server_) {
            auto completed = channel.ExpectMarker(marker, wait_ms);
            if (!completed) {
                return completed;
            }
            if (!fault_baseline) {
                auto verified = resources.VerifyTarget(phase, round);
                if (!verified) {
                    return verified;
                }
            }
            // 故障目标由子进程在退出前校验，这里的应答只同步阶段，不计入 verified。
            return channel.SendMarker(marker, options_.timeout_ms);
        }
        auto transferred = resources.Transfer(phase, round);
        if (!transferred) {
            return transferred;
        }
        auto completed = channel.SendMarker(marker, options_.timeout_ms);
        if (!completed) {
            return completed;
        }
        auto verified = channel.ExpectMarker(marker, options_.timeout_ms);
        if (!verified) {
            return verified;
        }
        return fault_baseline ? std::expected<void, ToolError>{} : resources.ConfirmRemoteWrites(phase);
    };
    auto baseline = transfer(RmaFailurePhase::kHealthyBaseline, 0, kBaseline);
    if (!baseline) {
        return baseline;
    }
    baseline = transfer(RmaFailurePhase::kFaultBaseline, 0, kBaseline + 1);
    if (!baseline) {
        return baseline;
    }
    const auto rounds = [&](RmaFailurePhase phase, std::uint32_t marker) -> std::expected<void, ToolError> {
        for (std::uint32_t round = 0; round < options_.rounds; ++round) {
            auto completed = transfer(phase, round, marker + round);
            if (!completed) {
                return completed;
            }
        }
        return {};
    };
    auto before = rounds(RmaFailurePhase::kHealthyBefore, kBefore);
    if (!before) {
        return before;
    }
    if (server_) {
        // 两个目标的基线及故障前流量均已终结，子进程校验写入与保护区后才被杀死。
        auto exited = fault_peer->FinishBaselineAndExit();
        if (!exited) {
            return exited;
        }
        resources.stats.fault_baseline.verified = 1;
        auto sent = channel.SendMarker(kExited, options_.timeout_ms);
        if (!sent) {
            return sent;
        }
    } else {
        // 等待故障目标校验与 SIGKILL/waitpid；仍由外层 watchdog 兜底。
        auto exited = channel.ExpectMarker(kExited, 3 * options_.timeout_ms);
        if (!exited) {
            return exited;
        }
        auto verified = resources.ConfirmRemoteWrites(RmaFailurePhase::kFaultBaseline);
        if (!verified) {
            return verified;
        }
    }
    resources.stats.peer_exit_confirmed = true;
    auto mixed = rounds(RmaFailurePhase::kMixed, kMixed);
    if (!mixed) {
        return mixed;
    }
    auto after = rounds(RmaFailurePhase::kHealthyAfter, kAfter);
    if (!after) {
        return after;
    }
    auto empty = resources.CheckQuiescent();
    if (!empty) {
        return empty;
    }
    auto stopped = channel.SendMarker(kQuiescent, options_.timeout_ms);
    if (!stopped) {
        return stopped;
    }
    auto confirmed = channel.ExpectMarker(kQuiescent, options_.timeout_ms);
    if (!confirmed) {
        return confirmed;
    }
    quiescent_ = true;
    return {};
}

std::expected<void, ToolError> RmaFailureSession::Close() {
    if (!quiescent_) {
        return Error("WRITE failure peer/DMA state unresolved; retain all dependencies", EBUSY);
    }
    auto closed = resources.Close();
    if (closed) {
        opened_ = false;
    }
    return closed;
}
} // namespace tools
} // namespace kbsocket
