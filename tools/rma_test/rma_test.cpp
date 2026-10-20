// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/rma_test/rma_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <thread>

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
constexpr std::size_t kGuardBytes = 64;
constexpr std::byte kGuard{0xa5};
constexpr std::byte kPoison{0xcc};
constexpr std::uint32_t kReady = 0x52410000;
constexpr std::uint32_t kCompleted = 0x52420000;
constexpr std::uint32_t kVerified = 0x52430000;
constexpr std::uint32_t kQuiescent = 0x52444444;

auto Error(const char* operation, int code = EIO) {
    return std::unexpected(ToolError{operation, code ? code : EPROTO});
}
std::byte Pattern(std::uint32_t round, std::size_t offset) {
    return static_cast<std::byte>((round * 37U + offset + offset / 251) % 251);
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
    config.tx_depth = 1;
    config.rx_depth = 1;
    config.tx_cq_depth = 2;
    config.rx_cq_depth = 1;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("create RMA RM_CTP jetty", opened.error().failure.provider_error);
    }
    // 只读取原始句柄；本工具自己的 pending 状态覆盖唯一 WR 的生存期，不走 SEND 账本。
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
    const auto length = (options.bytes + 2 * kGuardBytes + page - 1) / page * page;
    storage_.resize(length + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
    registered_ = std::span(storage_).subspan((page - address % page) % page, length);
    payload_ = registered_.subspan(kGuardBytes, options.bytes);
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
    local_.payload_address = reinterpret_cast<std::uintptr_t>(payload_.data());
    auto valid = ValidateRmaHello(local_);
    if (!valid) {
        return valid;
    }
    local_sge_ = {.addr = local_.payload_address, .len = options.bytes, .tseg = segment_};
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
        peer_.options.bytes != local_.options.bytes || peer_.options.rounds != local_.options.rounds) {
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
        remote_sge_ = {.addr = peer_.payload_address, .len = local_.options.bytes, .tseg = remote_segment_};
    }
    return {};
}

void RmaSession::Prepare(std::uint32_t round) {
    std::fill(registered_.begin(), registered_.end(), kGuard);
    const bool source = (local_.options.operation == RmaOperation::kRead) == local_.server;
    for (std::size_t i = 0; i < payload_.size(); ++i) {
        payload_[i] = source ? Pattern(round, i) : kPoison;
    }
}

std::expected<void, ToolError> RmaSession::Verify(std::uint32_t round) {
    // READ 的发送源也校验未被改写；WRITE 的发送源同样保持完整，目标两端只在 CQ/屏障后读取。
    for (std::size_t i = 0; i < payload_.size(); ++i) {
        if (payload_[i] != Pattern(round, i)) {
            return Error("RMA payload mismatch after completion", EPROTO);
        }
    }
    for (const auto value : registered_.first(kGuardBytes)) {
        if (value != kGuard) {
            return Error("RMA prefix guard overwritten", EPROTO);
        }
    }
    for (const auto value : registered_.subspan(kGuardBytes + payload_.size())) {
        if (value != kGuard) {
            return Error("RMA suffix guard overwritten", EPROTO);
        }
    }
    ++stats_.verified;
    return {};
}

std::expected<void, ToolError> RmaSession::Transfer(std::uint32_t round) {
    wr_ = {};
    wr_.opcode = local_.options.operation == RmaOperation::kRead ? URMA_OPC_READ : URMA_OPC_WRITE;
    wr_.flag.bs.complete_enable = 1;
    wr_.tjetty = target_;
    wr_.user_ctx = static_cast<std::uint64_t>(round) + 1;
    wr_.rw.src = {local_.options.operation == RmaOperation::kRead ? &remote_sge_ : &local_sge_, 1};
    wr_.rw.dst = {local_.options.operation == RmaOperation::kRead ? &local_sge_ : &remote_sge_, 1};
    urma_jfs_wr_t* bad = nullptr;
    const auto begin = std::chrono::steady_clock::now();
    stats_.pending = true;
    const auto posted = UrmaApi::PostJettySendWr(jetty_, &wr_, &bad);
    stats_.last_post_status = static_cast<int>(posted);
    if (posted != URMA_SUCCESS || bad) {
        if (posted != URMA_SUCCESS && bad == &wr_) {
            stats_.pending = false;
        }
        return Error("post RMA WR rejected or acceptance unresolved", static_cast<int>(posted));
    }
    ++stats_.accepted;
    const auto deadline = begin + std::chrono::milliseconds(local_.options.timeout_ms);
    std::array<urma_cr_t, 2> completions{};
    for (;;) {
        auto count = pool_.PollSend(completions);
        if (!count) {
            return Error("poll RMA completion", count.error().provider_error);
        }
        if (*count) {
            const auto& cr = completions[0];
            stats_.last_completion_status = static_cast<int>(cr.status);
            // 假 CQE/status 错误先于 user_ctx 检查，错误完成不会触发 buffer 复用。
            if (*count != 1 || cr.status != URMA_CR_SUCCESS || cr.flag.bs.s_r || !cr.flag.bs.jetty ||
                cr.local_id != jetty_->jetty_id.id || cr.user_ctx != wr_.user_ctx) {
                return Error("unexpected RMA completion status/type/id/count", static_cast<int>(cr.status));
            }
            stats_.pending = false;
            ++stats_.retired;
            const auto elapsed = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count());
            if (!stats_.min_cqe_ns || elapsed < stats_.min_cqe_ns) {
                stats_.min_cqe_ns = elapsed;
            }
            stats_.max_cqe_ns = std::max(stats_.max_cqe_ns, elapsed);
            return {};
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return Error("wait RMA completion", ETIMEDOUT);
        }
        std::this_thread::yield();
    }
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
        auto ready = Barrier(channel, kReady + round);
        if (!ready) {
            return ready;
        }
        if (!local_.server) {
            auto transferred = Transfer(round);
            if (!transferred) {
                return transferred;
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
            // 服务端不投递 RX，也不等待 RX CQE。客户端的成功 TX CQE 加此控制消息是读取目标数据的前提。
            auto completed = channel.ExpectMarker(kCompleted + round, local_.options.timeout_ms);
            if (!completed) {
                return completed;
            }
            auto verified = Verify(round);
            if (!verified) {
                return verified;
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
    payload_ = {};
    registered_ = {};
    storage_.clear();
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
