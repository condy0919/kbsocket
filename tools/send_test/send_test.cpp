// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/send_test/send_test.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <optional>
#include <thread>

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
auto Error(const char* operation, int code = errno) noexcept {
    return std::unexpected(SendTestError{operation, code});
}
} // namespace

std::expected<void, SendTestError> SendTestSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                         const SendTestOptions& options, bool server) {
    if (ctx_) {
        return Error("session already open", EBUSY);
    }
    auto valid = ValidateOptions(options);
    if (!valid) {
        return valid;
    }
    if (!ctx || options.bytes > cap.max_msg_size) {
        return Error("message exceeds device capability", EINVAL);
    }
    options_ = options;
    server_ = server;
    ctx_ = ctx;
    raw::JettyPoolConfig config;
    config.tx_depth = config.rx_depth = options.batch;
    config.tx_cq_depth = options.batch + 1;
    config.rx_cq_depth = options.batch;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("create jetty pool", opened.error().failure.provider_error);
    }
    // 只借用句柄读取公开端点，不占用实际发送额度。
    auto ticket = pool_.Reserve();
    if (!ticket) {
        return Error("reserve endpoint", EIO);
    }
    auto jetty = pool_.Get(ticket->lane);
    auto cancelled = pool_.Cancel(*ticket);
    if (!jetty || !cancelled) {
        return Error("get endpoint", EIO);
    }
    jetty_ = *jetty;
    if (server_) {
        auto opened_rx = rx_buffers_.Open(pool_);
        if (!opened_rx) {
            return Error("open RX buffer pool", opened_rx.error().provider_error);
        }
        return {};
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return Error("query system page size", EINVAL);
    }
    const auto page = static_cast<std::size_t>(page_size);
    const auto payload_size = static_cast<std::size_t>(options.bytes) * options.batch;
    const auto registration_size = (payload_size + page - 1) / page * page;
    // 保持注册范围完全位于拥有的内存中，不把相邻堆对象所在页暴露给注册操作。
    buffer_.resize(registration_size + page - 1);
    const auto address = reinterpret_cast<std::uintptr_t>(buffer_.data());
    const auto offset = (page - address % page) % page;
    registered_buffer_ = std::span(buffer_).subspan(offset, registration_size);
    urma_seg_cfg_t segment{};
    segment.va = reinterpret_cast<std::uintptr_t>(registered_buffer_.data());
    segment.len = registered_buffer_.size();
    // 与 UMQ 的注册权限一致，避免默认走 LOCAL_ONLY 对应的 e_bit 授权分支。
    segment.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(ctx, &segment);
    if (!segment_) {
        const int saved_errno = errno;
        if (!saved_errno) {
            return Error("register local memory: provider returned nullptr without errno; inspect URMA/UDMA logs", EIO);
        }
        return Error("register local memory", saved_errno);
    }
    for (std::size_t i = 0; i < options.batch; ++i) {
        sges_[i].addr = reinterpret_cast<std::uintptr_t>(registered_buffer_.data() + i * options.bytes);
        sges_[i].len = options.bytes;
        sges_[i].tseg = segment_;
    }
    auto ledger = ledger_.Open(pool_, options.batch);
    if (!ledger) {
        return Error("open ledger", static_cast<int>(ledger.error().code));
    }
    auto sender = sender_.Open(ledger_);
    if (!sender) {
        return Error("open sender", static_cast<int>(sender.error().code));
    }
    return {};
}

std::expected<void, SendTestError> SendTestSession::Run(ControlChannel& channel, bool server) {
    if (!jetty_ || server != server_ || (server_ ? !rx_buffers_.capacity() : !segment_)) {
        return Error("session not open", EINVAL);
    }
    auto local = EncodeHello({jetty_->jetty_id, options_});
    auto written = channel.Write(local, options_.timeout_ms);
    if (!written) {
        return written;
    }
    HelloBytes wire{};
    auto received = channel.Read(wire, options_.timeout_ms);
    if (!received) {
        return received;
    }
    auto peer = DecodeHello(wire);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (peer->options.bytes != options_.bytes || peer->options.messages != options_.messages ||
        peer->options.batch != options_.batch) {
        return Error("peer bytes/messages/batch mismatch", EPROTO);
    }
    if (!server) {
        urma_rjetty_t remote{};
        remote.jetty_id = peer->endpoint;
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remote_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!remote_) {
            return Error("import RM_CTP peer");
        }
    }
    for (std::uint32_t base = 0; base < options_.messages;) {
        const auto count = std::min(options_.batch, options_.messages - base);
        auto result = server ? ReceiveBatch(base, count, channel) : SendBatch(base, count, channel);
        if (!result) {
            return result;
        }
        base += count;
    }
    // 发送端收到最终接收确认后再通知接收端，两端都结束数据路径才销毁对象。
    if (server) {
        return channel.ExpectMarker(0xffffffff, options_.timeout_ms);
    }
    return channel.SendMarker(0xffffffff, options_.timeout_ms);
}

std::expected<void, SendTestError> SendTestSession::ReceiveBatch(std::uint32_t base, std::uint32_t count,
                                                                 ControlChannel& channel) {
    seen_messages_.fill(false);
    auto posted = rx_buffers_.Refill(count);
    if (!posted) {
        return Error("post receive", posted.error().provider_error);
    }
    if (*posted != count) {
        return Error("insufficient RX buffers", ENOBUFS);
    }
    auto ready = channel.SendMarker(base + count, options_.timeout_ms);
    if (!ready) {
        return ready;
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> events{};
    std::uint32_t remaining = count;
    while (remaining) {
        if (Clock::now() >= deadline) {
            return Error("RX completion timeout", ETIMEDOUT);
        }
        auto n = rx_buffers_.Poll(events);
        if (!n) {
            return Error("poll RX", n.error().provider_error);
        }
        if (!*n) {
            std::this_thread::yield();
            continue;
        }
        std::optional<SendTestError> failure;
        for (std::size_t i = 0; i < *n; ++i) {
            const auto& event = events[i];
            if (!event.lease) {
                failure = SendTestError{"unidentified RX completion", EPROTO};
                continue;
            }
            --remaining;
            if (event.error || event.payload.size() != options_.bytes) {
                failure = SendTestError{"invalid RX completion", EPROTO};
            } else {
                auto sequence = CheckPayload(event.payload);
                if (!sequence) {
                    failure = sequence.error();
                } else if (*sequence < base || *sequence >= static_cast<std::uint64_t>(base) + count ||
                           seen_messages_[*sequence - base]) {
                    failure = SendTestError{"duplicate/out-of-range message", EPROTO};
                } else {
                    seen_messages_[*sequence - base] = true;
                }
            }
            // payload 校验结束才归还；某个事件失败也必须处理并归还同批的其他 lease。
            auto released = rx_buffers_.Release(*event.lease);
            if (!released) {
                failure = SendTestError{"release RX buffer", EPROTO};
            }
        }
        if (failure) {
            return std::unexpected(*failure);
        }
    }
    return channel.SendMarker(base + count, options_.timeout_ms);
}

std::expected<void, SendTestError> SendTestSession::SendBatch(std::uint32_t base, std::uint32_t count,
                                                              ControlChannel& channel) {
    auto ready = channel.ExpectMarker(base + count, options_.timeout_ms);
    if (!ready) {
        return ready;
    }
    for (std::uint32_t i = 0; i < count; ++i) {
        FillPayload(registered_buffer_.subspan(static_cast<std::size_t>(i) * options_.bytes, options_.bytes),
                    static_cast<std::uint64_t>(base) + i);
        requests_[i] = {.metadata = {.connection_id = 1,
                                     .operation_id = static_cast<std::uint64_t>(base) + i + 1,
                                     .target = remote_,
                                     .buffer_lease = i + 1},
                        .sges = std::span(&sges_[i], 1)};
    }
    auto submitted = sender_.Send(std::span(requests_).first(count), std::span(ids_).first(count));
    if (!submitted) {
        // 保留部分成功、提交边界不明和记账异常现场，不自动重放。
        unsafe_ = true;
        return Error("post SEND (resources retained)", submitted.error().provider_status);
    }
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<raw::TxCompletionEvent, 64> events{};
    while (ledger_.size()) {
        if (Clock::now() >= deadline) {
            return Error("TX completion timeout", ETIMEDOUT);
        }
        auto batch = processor_.Poll(events);
        if (!batch) {
            return Error("poll TX", static_cast<int>(batch.error().code));
        }
        for (std::size_t i = 0; i < batch->count; ++i) {
            if (events[i].kind != raw::TxCompletionKind::kCompleted || events[i].completion.status != URMA_CR_SUCCESS) {
                unsafe_ = true;
                return Error("TX error/boundary completion", static_cast<int>(events[i].completion.status));
            }
        }
        if (!batch->count) {
            std::this_thread::yield();
        }
    }
    return channel.ExpectMarker(base + count, options_.timeout_ms);
}

std::expected<void, SendTestError> SendTestSession::Close() noexcept {
    if (unsafe_ || ledger_.size()) {
        return Error("DMA state unresolved; retaining session", EBUSY);
    }
    auto sender = sender_.Close();
    if (!sender) {
        return Error("close sender", static_cast<int>(sender.error().code));
    }
    auto ledger = ledger_.Close();
    if (!ledger) {
        return Error("close ledger", static_cast<int>(ledger.error().code));
    }
    if (remote_) {
        auto status = UrmaApi::UnimportJetty(remote_);
        if (status != URMA_SUCCESS) {
            return Error("unimport peer", status);
        }
        remote_ = nullptr;
    }
    auto rx = rx_buffers_.Close();
    if (!rx) {
        return Error("close RX buffers (resources retained)", static_cast<int>(rx.error().code));
    }
    auto pool = pool_.Close();
    if (!pool) {
        return Error("close jetty pool", pool.error().provider_error);
    }
    jetty_ = nullptr;
    if (segment_) {
        auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error("unregister memory", status);
        }
        segment_ = nullptr;
    }
    registered_buffer_ = {};
    std::vector<std::byte>().swap(buffer_);
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
