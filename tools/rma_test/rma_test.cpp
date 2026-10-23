// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/rma_test/rma_test.hpp"

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
constexpr std::size_t kGuardBytes = kRmaGuardBytes;
constexpr std::byte kGuard{0xa5};
constexpr std::byte kPoison{0xcc};
constexpr std::uint32_t kReady = 0x52410000;
constexpr std::uint32_t kCompleted = 0x52420000;
constexpr std::uint32_t kVerified = 0x52430000;
constexpr std::uint32_t kQuiescent = 0x52444444;

auto Error(const char* operation, int code = EIO) {
    return std::unexpected(ToolError{operation, code ? code : EPROTO});
}
std::byte Pattern(std::uint32_t round, std::uint32_t slot, std::size_t offset) {
    return static_cast<std::byte>((round * 37U + slot * 19U + offset + offset / 251) % 251);
}
// 明确编码完整轮次和槽身份，防止周期性 payload pattern 把历史 WRITE 误判为本轮发布。
std::byte PublicationByte(const RmaOptions& options, std::uint32_t round, std::uint32_t slot, std::size_t offset,
                          bool notification) {
    if (options.write_notify && offset < 16) {
        const std::array<std::uint32_t, 4> identity{0x57504e31U, round, slot, options.bytes};
        return static_cast<std::byte>((identity[offset / 4] >> (8 * (offset % 4))) & 0xffU);
    }
    return Pattern(round, slot, offset + (notification ? options.bytes : 0));
}
// 只有正常硬件终结 CQE 可以退休 WR；未知状态或边界标记不能作为 DMA 已结束的依据。
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

std::expected<void, ToolError> RmaSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                const RmaOptions& options, bool server) {
    if (ctx_ || started_) {
        return Error("RMA session already opened", EBUSY);
    }
    if (!ctx) {
        return Error("RMA context is null", EINVAL);
    }
    auto supported = CheckRmaCapability(options, cap);
    if (!supported) {
        return supported;
    }
    ctx_ = ctx;
    local_.options = options;
    local_.server = server;
    raw::JettyPoolConfig config;
    config.jetty_count = 1;
    const auto wr_count = options.window * (options.mixed_send ? 2U : 1U);
    config.tx_depth = wr_count;
    config.rx_depth = RmaHasSend(options) ? options.window : 1;
    config.tx_cq_depth = wr_count + 1;
    config.rx_cq_depth = *config.rx_depth;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("create RMA RM_CTP jetty", opened.error().failure.provider_error);
    }
    // 工具独占此 SQ，以逐条 identity/退休状态覆盖整批 RMA 与 SEND 的生存期。
    auto ticket = pool_.Reserve();
    if (!ticket) {
        return Error("reserve RMA lane");
    }
    auto jetty = pool_.Get(ticket->lane);
    auto canceled = pool_.Cancel(*ticket);
    if (!jetty || !canceled) {
        return Error("read RMA jetty");
    }
    jetty_ = *jetty;
    local_.endpoint = jetty_->jetty_id;
    const auto page_value = ::sysconf(_SC_PAGESIZE);
    if (page_value <= 0) {
        return Error("read page size");
    }
    const auto page = static_cast<std::size_t>(page_value);
    const auto length = (RmaSlotStride(options) * options.window + page - 1) / page * page;
    storage_.resize(length + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
    registered_ = std::span(storage_).subspan((page - address % page) % page, length);
    urma_seg_cfg_t cfg{};
    cfg.va = reinterpret_cast<std::uintptr_t>(registered_.data());
    cfg.len = registered_.size();
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &cfg);
    if (!segment_) {
        return Error("register RMA memory", errno ? errno : EIO);
    }
    urma_seg_t* exported = nullptr;
    std::uint32_t exported_size = 0;
    const auto status = UrmaApi::GetSegCtx(segment_, &exported, &exported_size);
    ScopeExit release([&]() noexcept {
        if (exported) {
            UrmaApi::PutSegCtx(exported);
        }
    });
    if (status != URMA_SUCCESS || !exported) {
        return Error("export registered segment descriptor", static_cast<int>(status));
    }
    if (exported_size != sizeof(urma_seg_t) || exported->attr.bs.has_user_info) {
        return Error("only raw segment descriptors without provider extensions are supported", EOPNOTSUPP);
    }
    local_.segment = *exported;
    local_.payload_address = reinterpret_cast<std::uintptr_t>(Payload(0).data());
    auto valid = ValidateRmaHello(local_);
    if (!valid) {
        return valid;
    }
    for (std::uint32_t slot = 0; slot < options.window; ++slot) {
        local_sges_[slot] = {
            .addr = reinterpret_cast<std::uintptr_t>(Payload(slot).data()), .len = options.bytes, .tseg = segment_};
        if (RmaHasSend(options)) {
            send_sges_[slot] = {.addr = reinterpret_cast<std::uintptr_t>(SendPayload(slot).data()),
                                .len = kRmaSendBytes,
                                .tseg = segment_};
        }
    }
    if (server && RmaHasSend(options)) {
        auto received = rx_.Open(pool_, options.window);
        if (!received) {
            return Error("open RMA SEND receive buffers", received.error().provider_error);
        }
    }
    ready_ = true;
    return {};
}

std::expected<void, ToolError> RmaSession::Exchange(ControlChannel& channel) {
    const auto local = EncodeRmaHello(local_);
    RmaHelloBytes peer;
    auto written = channel.Write(local, local_.options.timeout_ms);
    if (!written) {
        return written;
    }
    auto read = channel.Read(peer, local_.options.timeout_ms);
    if (!read) {
        return read;
    }
    auto decoded = DecodeRmaHello(peer);
    if (!decoded) {
        return std::unexpected(decoded.error());
    }
    peer_ = *decoded;
    if (peer_.server == local_.server || peer_.options.operation != local_.options.operation ||
        peer_.options.bytes != local_.options.bytes || peer_.options.rounds != local_.options.rounds ||
        peer_.options.window != local_.options.window || peer_.options.mixed_send != local_.options.mixed_send ||
        peer_.options.write_notify != local_.options.write_notify) {
        return Error("peer RMA role/options mismatch", EPROTO);
    }
    if (!local_.server) {
        urma_import_seg_flag_t flags{};
        flags.bs.access = local_.options.operation == RmaOperation::kRead ? URMA_ACCESS_READ : URMA_ACCESS_WRITE;
        // 无本地映射，WR 使用对端明确导出的虚拟地址与 import 后的 tseg。
        errno = 0;
        remote_segment_ = UrmaApi::ImportSeg(ctx_, &peer_.segment, &peer_.token, 0, flags);
        if (!remote_segment_) {
            return Error("import remote RMA segment", errno ? errno : EIO);
        }
        urma_rjetty_t remote{};
        remote.jetty_id = peer_.endpoint;
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t jetty_token{};
        errno = 0;
        target_ = UrmaApi::ImportJetty(ctx_, &remote, &jetty_token);
        if (!target_) {
            return Error("import remote RMA jetty", errno ? errno : EIO);
        }
        for (std::uint32_t slot = 0; slot < local_.options.window; ++slot) {
            remote_sges_[slot] = {.addr = peer_.payload_address + slot * RmaSlotStride(local_.options),
                                  .len = local_.options.bytes,
                                  .tseg = remote_segment_};
        }
    }
    return {};
}

std::span<std::byte> RmaSession::Payload(std::uint32_t slot) {
    return registered_.subspan(slot * RmaSlotStride(local_.options) + kGuardBytes, local_.options.bytes);
}

std::span<std::byte> RmaSession::SendPayload(std::uint32_t slot) {
    return registered_.subspan(slot * RmaSlotStride(local_.options) + 2 * kGuardBytes + local_.options.bytes,
                               kRmaSendBytes);
}

void RmaSession::Prepare(std::uint32_t round) {
    std::fill(registered_.begin(), registered_.end(), kGuard);
    const bool source = (local_.options.operation == RmaOperation::kRead) == local_.server;
    for (std::uint32_t slot = 0; slot < local_.options.window; ++slot) {
        auto payload = Payload(slot);
        for (std::size_t i = 0; i < payload.size(); ++i) {
            payload[i] = source ? PublicationByte(local_.options, round, slot, i, false) : kPoison;
        }
        if (RmaHasSend(local_.options)) {
            auto small = SendPayload(slot);
            for (std::size_t i = 0; i < small.size(); ++i) {
                small[i] = PublicationByte(local_.options, round, slot, i, true);
            }
        }
    }
}

std::expected<void, ToolError> RmaSession::VerifySlot(std::uint32_t round, std::uint32_t slot) {
    // 只检查已收到通知的槽；其余槽尚可能处于 DMA 中，不能提前读取或作跨槽判定。
    const auto payload = Payload(slot);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        if (payload[i] != PublicationByte(local_.options, round, slot, i, false)) {
            return Error("RMA payload mismatch after completion", EPROTO);
        }
    }
    const auto memory = registered_.subspan(slot * RmaSlotStride(local_.options), RmaSlotStride(local_.options));
    for (std::size_t i = 0; i < memory.size(); ++i) {
        if (i >= kGuardBytes && i < kGuardBytes + payload.size()) {
            continue;
        }
        if (RmaHasSend(local_.options) && i >= 2 * kGuardBytes + payload.size() &&
            i < 2 * kGuardBytes + payload.size() + kRmaSendBytes) {
            if (memory[i] != PublicationByte(local_.options, round, slot, i - 2 * kGuardBytes - payload.size(), true)) {
                return Error("RMA SEND source changed before retirement", EPROTO);
            }
        } else if (memory[i] != kGuard) {
            return Error("RMA guard overwritten", EPROTO);
        }
    }
    return {};
}

std::expected<void, ToolError> RmaSession::VerifyPadding() {
    for (const auto value : registered_.subspan(local_.options.window * RmaSlotStride(local_.options))) {
        if (value != kGuard) {
            return Error("RMA registration padding overwritten", EPROTO);
        }
    }
    return {};
}

std::expected<void, ToolError> RmaSession::Verify(std::uint32_t round) {
    for (std::uint32_t slot = 0; slot < local_.options.window; ++slot) {
        auto verified = VerifySlot(round, slot);
        if (!verified) {
            return verified;
        }
        ++stats_.verified;
    }
    return VerifyPadding();
}

std::expected<void, ToolError> RmaSession::Transfer(std::uint32_t round, bool notification) {
    const auto& options = local_.options;
    const std::uint32_t factor = options.mixed_send ? 2 : 1;
    const std::uint32_t count = options.window * factor;
    // WRITE 与通知使用不同 user_ctx 区间，避免延迟、重复 CQE 跨阶段冒充本阶段完成。
    const std::uint64_t first_id =
        static_cast<std::uint64_t>(round) * count * (options.write_notify ? 2U : 1U) + (notification ? count : 0) + 1;
    retired_.fill(false);
    for (std::uint32_t slot = 0; slot < options.window; ++slot) {
        auto& wr = wrs_[slot * factor];
        wr = {};
        if (notification) {
            wr.opcode = URMA_OPC_SEND_IMM;
            wr.send.src = {&send_sges_[slot], 1};
            wr.send.imm_data = static_cast<std::uint64_t>(round) * options.window + slot + 1;
        } else {
            const bool read = options.operation == RmaOperation::kRead;
            wr.opcode = read ? URMA_OPC_READ : URMA_OPC_WRITE;
            wr.rw.src = {read ? &remote_sges_[slot] : &local_sges_[slot], 1};
            wr.rw.dst = {read ? &local_sges_[slot] : &remote_sges_[slot], 1};
        }
        if (options.mixed_send) {
            auto& send = wrs_[slot * factor + 1];
            send = {};
            send.opcode = URMA_OPC_SEND_IMM;
            send.send.src = {&send_sges_[slot], 1};
            send.send.imm_data = static_cast<std::uint64_t>(round) * options.window + slot + 1;
        }
    }
    for (std::uint32_t i = 0; i < count; ++i) {
        auto& wr = wrs_[i];
        wr.flag.bs.complete_enable = 1;
        wr.tjetty = target_;
        wr.user_ctx = first_id + i;
        wr.next = i + 1 < count ? &wrs_[i + 1] : nullptr;
    }
    urma_jfs_wr_t* bad = nullptr;
    const auto begin = std::chrono::steady_clock::now();
    stats_.pending = true;
    const auto posted = UrmaApi::PostJettySendWr(jetty_, wrs_.data(), &bad);
    stats_.last_post_status = static_cast<int>(posted);
    std::uint32_t accepted = count;
    const bool post_failed = posted != URMA_SUCCESS || bad;
    if (post_failed) {
        accepted = 0;
        bool known = false;
        if (posted != URMA_SUCCESS) {
            for (std::uint32_t i = 0; i < count; ++i) {
                if (bad == &wrs_[i]) {
                    accepted = i;
                    known = true;
                    break;
                }
            }
        }
        if (!known) {
            stats_.accepted_known = false;
            return Error("post RMA batch acceptance unresolved", static_cast<int>(posted));
        }
        stats_.partial_batches += accepted != 0;
    }
    stats_.accepted += accepted;
    if (notification) {
        stats_.send_accepted += accepted;
    } else if (options.mixed_send) {
        stats_.send_accepted += accepted / 2;
    }
    stats_.pending = accepted != 0;
    const auto deadline = begin + std::chrono::milliseconds(options.timeout_ms);
    std::uint32_t remaining = accepted;
    std::array<urma_cr_t, 64> completions{};
    while (remaining) {
        auto polled = pool_.PollSend(completions);
        if (!polled) {
            return Error("poll RMA completion", polled.error().provider_error);
        }
        std::expected<void, ToolError> result;
        for (int i = 0; i < *polled; ++i) {
            const auto& cr = completions[i];
            stats_.last_completion_status = static_cast<int>(cr.status);
            // 边界假 CQE 的 user_ctx 无效，必须先排除；异常也不能丢弃同批后续可信 CQE。
            if (!IsTerminal(cr.status) || cr.flag.bs.s_r || !cr.flag.bs.jetty || cr.local_id != jetty_->jetty_id.id ||
                cr.user_ctx < first_id || cr.user_ctx - first_id >= accepted) {
                if (result) {
                    result = Error("unexpected RMA completion type/id", EPROTO);
                }
                continue;
            }
            const auto index = static_cast<std::size_t>(cr.user_ctx - first_id);
            if (retired_[index]) {
                if (result) {
                    result = Error("duplicate RMA completion", EPROTO);
                }
                continue;
            }
            retired_[index] = true;
            --remaining;
            ++stats_.retired;
            stats_.send_retired += notification || (options.mixed_send && index % 2 == 1);
            if (cr.status != URMA_CR_SUCCESS && result) {
                result = Error("RMA completion failed", static_cast<int>(cr.status));
            }
            const auto elapsed = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count());
            if (!stats_.min_cqe_ns || elapsed < stats_.min_cqe_ns) {
                stats_.min_cqe_ns = elapsed;
            }
            stats_.max_cqe_ns = std::max(stats_.max_cqe_ns, elapsed);
        }
        stats_.pending = remaining != 0;
        if (!result) {
            return result;
        }
        if (remaining && std::chrono::steady_clock::now() >= deadline) {
            return Error("wait RMA completion", ETIMEDOUT);
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    if (post_failed) {
        return Error("RMA batch rejected after accepted prefix retired; no replay", static_cast<int>(posted));
    }
    return {};
}

std::expected<void, ToolError> RmaSession::Receive(std::uint32_t round) {
    const auto& options = local_.options;
    received_.fill(false);
    std::uint32_t remaining = options.window;
    const std::uint64_t first_id = static_cast<std::uint64_t>(round) * options.window + 1;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(options.timeout_ms);
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> events{};
    while (remaining) {
        auto polled = rx_.Poll(events);
        if (!polled) {
            return Error("poll RMA SEND receive", polled.error().provider_error);
        }
        std::expected<void, ToolError> result;
        for (std::size_t i = 0; i < *polled; ++i) {
            auto& event = events[i];
            const auto id = event.completion.imm_data;
            bool valid = !event.error && event.lease && event.completion.opcode == URMA_CR_OPC_SEND_WITH_IMM &&
                         event.payload.size() == kRmaSendBytes && id >= first_id && id - first_id < options.window &&
                         event.completion.remote_id.id == peer_.endpoint.id &&
                         std::memcmp(event.completion.remote_id.eid.raw, peer_.endpoint.eid.raw, URMA_EID_SIZE) == 0;
            if (valid) {
                const auto slot = static_cast<std::uint32_t>(id - first_id);
                valid = !received_[slot];
                for (std::size_t j = 0; valid && j < event.payload.size(); ++j) {
                    valid = event.payload[j] == PublicationByte(options, round, slot, j, true);
                }
                if (valid) {
                    received_[slot] = true;
                    --remaining;
                    ++stats_.send_received;
                    if (options.write_notify) {
                        // 通知 CQE 是唯一可见性触发点：此处读取目标，绝不等待 TCP 完成标记后再验证。
                        auto verified = VerifySlot(round, slot);
                        if (verified) {
                            ++stats_.verified;
                            ++stats_.visibility_verified;
                        } else if (result) {
                            result = verified;
                        }
                    }
                }
            }
            if (!valid && result) {
                result = Error("RMA SEND receive identity or payload mismatch", EPROTO);
            }
            if (event.lease) {
                auto released = rx_.Release(*event.lease);
                if (released) {
                    ++stats_.rx_leases_released;
                } else if (result) {
                    result = Error("release RMA SEND receive", released.error().provider_error);
                }
            }
        }
        if (!result) {
            return result;
        }
        if (remaining && std::chrono::steady_clock::now() >= deadline) {
            return Error("wait RMA SEND receive", ETIMEDOUT);
        }
        if (!*polled) {
            std::this_thread::yield();
        }
    }
    return options.write_notify ? VerifyPadding() : std::expected<void, ToolError>{};
}

std::expected<void, ToolError> RmaSession::Barrier(ControlChannel& channel, std::uint32_t marker) {
    auto sent = channel.SendMarker(marker, local_.options.timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(marker, local_.options.timeout_ms);
}

std::expected<void, ToolError> RmaSession::Run(ControlChannel& channel) {
    if (!ready_ || started_) {
        return Error("RMA session not ready or already run", EINVAL);
    }
    started_ = true;
    quiescent_ = false;
    auto exchanged = Exchange(channel);
    if (!exchanged) {
        return exchanged;
    }
    for (std::uint32_t round = 0; round < local_.options.rounds; ++round) {
        Prepare(round);
        if (local_.server && RmaHasSend(local_.options)) {
            auto posted = rx_.Refill(local_.options.window);
            if (!posted || *posted != local_.options.window) {
                return Error("refill RMA SEND receive", posted ? EPROTO : posted.error().provider_error);
            }
        }
        auto ready = Barrier(channel, kReady + round);
        if (!ready) {
            return ready;
        }
        if (!local_.server) {
            auto transferred = Transfer(round);
            if (!transferred) {
                return transferred;
            }
            if (local_.options.write_notify) {
                // 全批 WRITE 获得成功终结后才发布通知；部分接受、未知接受或任何失败均不继续提交。
                auto notified = Transfer(round, true);
                if (!notified) {
                    return notified;
                }
            }
            auto verified = Verify(round);
            if (!verified) {
                return verified;
            }
            auto sent = channel.SendMarker(kCompleted + round, local_.options.timeout_ms);
            if (!sent) {
                return sent;
            }
            auto confirmed = channel.ExpectMarker(kVerified + round, local_.options.timeout_ms);
            if (!confirmed) {
                return confirmed;
            }
        } else {
            // 含 SEND 的模式先推进 RX。WRITE 通知逐槽完成可见性验证，不依赖 TCP 完成标记。
            if (RmaHasSend(local_.options)) {
                auto received = Receive(round);
                if (!received) {
                    return received;
                }
            }
            // WRITE 通知已完成目标读取；此标记只确认发送端停止访问，供验证后复用与关闭。
            auto completed = channel.ExpectMarker(kCompleted + round, local_.options.timeout_ms);
            if (!completed) {
                return completed;
            }
            if (!local_.options.write_notify) {
                auto verified = Verify(round);
                if (!verified) {
                    return verified;
                }
            }
            auto sent = channel.SendMarker(kVerified + round, local_.options.timeout_ms);
            if (!sent) {
                return sent;
            }
        }
    }
    std::array<urma_cr_t, 2> tail{};
    auto extra = pool_.PollSend(tail);
    if (!extra || *extra) {
        return Error("unexpected extra RMA completion before close", extra ? EPROTO : extra.error().provider_error);
    }
    if (local_.server && RmaHasSend(local_.options)) {
        std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> events{};
        auto received = rx_.Poll(events);
        if (!received || *received) {
            // 即使发现额外/陈旧通知也释放当前 poll 返回的租约，其余 DMA 依赖仍保留。
            if (received) {
                for (std::size_t i = 0; i < *received; ++i) {
                    if (events[i].lease && rx_.Release(*events[i].lease)) {
                        ++stats_.rx_leases_released;
                    }
                }
            }
            return Error("unexpected extra RMA SEND receive before close", EPROTO);
        }
    }
    auto stopped = Barrier(channel, kQuiescent);
    if (!stopped) {
        return stopped;
    }
    quiescent_ = true;
    return {};
}

std::expected<void, ToolError> RmaSession::Close() {
    if (failed_close_ || !quiescent_ || stats_.pending) {
        return Error("RMA DMA/peer state unresolved; retain all dependencies", EBUSY);
    }
    ready_ = false;
    auto failed = [this](const char* operation, int status) -> std::expected<void, ToolError> {
        failed_close_ = true;
        return Error(operation, status);
    };
    if (target_) {
        const auto status = UrmaApi::UnimportJetty(target_);
        if (status != URMA_SUCCESS) {
            return failed("unimport RMA target", status);
        }
        target_ = nullptr;
    }
    if (remote_segment_) {
        const auto status = UrmaApi::UnimportSeg(remote_segment_);
        if (status != URMA_SUCCESS) {
            return failed("unimport RMA segment", status);
        }
        remote_segment_ = nullptr;
    }
    auto rx_closed = rx_.Close();
    if (!rx_closed) {
        return failed("close RMA SEND receive buffers", rx_closed.error().provider_error);
    }
    auto closed = pool_.Close();
    if (!closed) {
        return failed("close RMA queues", closed.error().provider_error);
    }
    jetty_ = nullptr;
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return failed("unregister RMA memory", status);
        }
        segment_ = nullptr;
    }
    registered_ = {};
    storage_.clear();
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
