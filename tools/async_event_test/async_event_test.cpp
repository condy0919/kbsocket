// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/async_event_test/async_event_test.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <optional>
#include <thread>

#include "kbsocket/base/scope_exit.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
using Clock = std::chrono::steady_clock;
constexpr std::uint32_t kMagic = 0x4b424145;
constexpr std::uint32_t kVersion = 2;
constexpr std::uint32_t kReady = 0x41455200;
constexpr std::uint32_t kDone = 0x41454400;
constexpr std::uint32_t kQuiescent = 0x41455154;

auto Error(const char* operation, int code = EIO) {
    return std::unexpected(ToolError{operation, code ? code : EIO});
}
void Put(std::span<std::byte> bytes, std::size_t offset, std::uint32_t value) noexcept {
    value = ::htonl(value);
    std::memcpy(bytes.data() + offset, &value, sizeof(value));
}
std::uint32_t Get(std::span<const std::byte> bytes, std::size_t offset) noexcept {
    std::uint32_t value;
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    return ::ntohl(value);
}
std::byte Pattern(std::uint32_t sequence, std::size_t offset) noexcept {
    return static_cast<std::byte>((sequence * 17 + offset) % 251);
}
std::expected<void, ToolError> Barrier(ControlChannel& channel, std::uint32_t marker, std::uint32_t timeout_ms) {
    auto sent = channel.SendMarker(marker, timeout_ms);
    if (!sent) {
        return sent;
    }
    return channel.ExpectMarker(marker, timeout_ms);
}
void AddEvents(JfrLimitEventStats& total, const JfrLimitEventStats& events) noexcept {
    total.get_calls += events.get_calls;
    total.retrieved += events.retrieved;
    total.acknowledged += events.acknowledged;
    total.limit_events += events.limit_events;
    if (events.get_calls) {
        total.last_type = events.last_type;
        total.last_get_status = events.last_get_status;
        total.last_context_matches = events.last_context_matches;
        total.last_jfr_matches = events.last_jfr_matches;
    }
}
} // namespace

std::expected<void, ToolError> ValidateJfrLimitOptions(const JfrLimitOptions& options) noexcept {
    // 本轮只覆盖 UDMA 的 64 水线档位；小于 64 的请求可能映射为禁用，不能当作等价参数。
    if (options.rx_depth < 128 || options.rx_depth > 256 || options.threshold != 64 || options.bytes < 32 ||
        options.bytes > raw::RxBufferPool::kBufferSize || !options.observe_ms || options.observe_ms > 1000 ||
        options.timeout_ms <= 2 * options.observe_ms || options.timeout_ms > 3600000 || options.crossings < 2 ||
        options.crossings > 1000 ||
        (options.rearm != JfrLimitRearm::kNone && options.rearm != JfrLimitRearm::kRewrite &&
         options.rearm != JfrLimitRearm::kToggle)) {
        return Error("require depth=128..256, threshold=64, bytes=32..4096, observe_ms=1..1000, "
                     "2*observe_ms<timeout_ms<=3600000, crossings=2..1000, rearm=none|rewrite|toggle",
                     EINVAL);
    }
    return {};
}

const char* JfrLimitRearmName(JfrLimitRearm rearm) noexcept {
    switch (rearm) {
    case JfrLimitRearm::kNone:
        return "none";
    case JfrLimitRearm::kRewrite:
        return "rewrite";
    case JfrLimitRearm::kToggle:
        return "toggle";
    }
    return "unknown";
}

const char* JfrLimitPhaseName(JfrLimitPhase phase) noexcept {
    switch (phase) {
    case JfrLimitPhase::kBaseline:
        return "baseline";
    case JfrLimitPhase::kFirstCrossing:
        return "first_crossing";
    case JfrLimitPhase::kRefillControl:
        return "refill_control";
    case JfrLimitPhase::kRepeatCrossing:
        return "repeat_crossing";
    case JfrLimitPhase::kAboveWater:
        return "above_water";
    case JfrLimitPhase::kCleanup:
        return "cleanup";
    case JfrLimitPhase::kCount:
        break;
    }
    return "unknown";
}

std::expected<std::size_t, ToolError> PollJfrLimitEvents(urma_context_t* ctx, urma_jfr_t* jfr,
                                                         JfrLimitEventStats& stats) noexcept {
    if (!ctx || !jfr || ctx->async_fd < 0) {
        return Error("JFR_LIMIT requires an async event fd and JFR", EINVAL);
    }
    std::size_t observed = 0;
    for (int i = 0; i < 64; ++i) {
        pollfd descriptor{ctx->async_fd, POLLIN, 0};
        const auto ready = ::poll(&descriptor, 1, 0);
        if (ready < 0) {
            if (errno == EINTR) {
                return observed;
            }
            return Error("poll JFR async fd", errno);
        }
        if (!ready) {
            return observed;
        }
        if (!(descriptor.revents & POLLIN) || descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            return Error("JFR async fd invalid", EIO);
        }
        urma_async_event_t event{};
        ++stats.get_calls;
        const auto status = UrmaApi::GetAsyncEvent(ctx, &event);
        stats.last_get_status = status;
        if (status != URMA_SUCCESS) {
            return Error("get JFR async event", status);
        }
        ++stats.retrieved;
        stats.last_type = static_cast<int>(event.event_type);
        stats.last_context_matches = event.urma_ctx == ctx;
        stats.last_jfr_matches = event.event_type == URMA_EVENT_JFR_LIMIT && event.element.jfr == jfr;
        // Get 成功交付的事件必须原样确认；先记录身份，不解引用 union 中的资源对象。
        UrmaApi::AckAsyncEvent(&event);
        ++stats.acknowledged;
        if (!stats.last_context_matches || !stats.last_jfr_matches) {
            return Error("unexpected async event type/context/JFR", EPROTO);
        }
        ++stats.limit_events;
        ++observed;
    }
    return observed;
}

JfrLimitHelloBytes EncodeJfrLimitHello(const JfrLimitHello& hello) noexcept {
    JfrLimitHelloBytes bytes{};
    Put(bytes, 0, kMagic);
    Put(bytes, 4, kVersion);
    Put(bytes, 8, hello.server ? 1 : 0);
    Put(bytes, 12, hello.options.rx_depth);
    Put(bytes, 16, hello.options.threshold);
    Put(bytes, 20, hello.options.bytes);
    Put(bytes, 24, hello.options.observe_ms);
    std::memcpy(bytes.data() + 28, hello.endpoint.eid.raw, 16);
    Put(bytes, 44, hello.endpoint.id);
    Put(bytes, 48, hello.options.crossings);
    Put(bytes, 52, static_cast<std::uint32_t>(hello.options.rearm));
    return bytes;
}

std::expected<JfrLimitHello, ToolError> DecodeJfrLimitHello(const JfrLimitHelloBytes& wire) noexcept {
    if (Get(wire, 0) != kMagic || Get(wire, 4) != kVersion || Get(wire, 8) > 1 || !Get(wire, 44)) {
        return Error("invalid async test protocol/endpoint", EPROTO);
    }
    JfrLimitHello hello;
    hello.server = Get(wire, 8) != 0;
    hello.options.rx_depth = Get(wire, 12);
    hello.options.threshold = Get(wire, 16);
    hello.options.bytes = Get(wire, 20);
    hello.options.observe_ms = Get(wire, 24);
    hello.options.crossings = Get(wire, 48);
    hello.options.rearm = static_cast<JfrLimitRearm>(Get(wire, 52));
    std::memcpy(hello.endpoint.eid.raw, wire.data() + 28, 16);
    hello.endpoint.id = Get(wire, 44);
    if (auto valid = ValidateJfrLimitOptions(hello.options); !valid) {
        return std::unexpected(valid.error());
    }
    return hello;
}

std::expected<void, ToolError> ValidateJfrLimitPeer(const JfrLimitHello& local, const JfrLimitHello& peer) noexcept {
    if (local.server == peer.server || local.options.rx_depth != peer.options.rx_depth ||
        local.options.threshold != peer.options.threshold || local.options.bytes != peer.options.bytes ||
        local.options.observe_ms != peer.options.observe_ms || local.options.crossings != peer.options.crossings ||
        local.options.rearm != peer.options.rearm) {
        return Error("peer async role/options mismatch", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                     const JfrLimitOptions& options, bool server) {
    if (ctx_ || started_) {
        return Error("async session already opened", EBUSY);
    }
    if (auto valid = ValidateJfrLimitOptions(options); !valid) {
        return valid;
    }
    if (!ctx || (server && ctx->async_fd < 0) || cap.max_msg_size < options.bytes) {
        return Error("invalid async context or message capability", EINVAL);
    }
    options_ = options;
    // 按轮保存观察结果只在初始化时分配，收发和事件采集过程不扩容。
    stats_.rounds.resize(options.crossings);
    server_ = server;
    ctx_ = ctx;
    raw::JettyPoolConfig config;
    config.jetty_count = 2;
    config.tx_depth = config.rx_depth = options.rx_depth;
    config.tx_cq_depth = 2 * (options.rx_depth + 1);
    config.rx_cq_depth = options.rx_depth;
    auto opened = pool_.Open(ctx, cap, config);
    if (!opened) {
        return Error("open async jetty pool", opened.error().failure.provider_error);
    }
    auto ticket = pool_.Reserve();
    if (!ticket) {
        return Error("reserve async endpoint");
    }
    lane_ = ticket->lane;
    auto jetty = pool_.Get(lane_);
    auto cancelled = pool_.Cancel(*ticket);
    if (!jetty || !cancelled) {
        return Error("read async endpoint");
    }
    jetty_ = *jetty;
    if (server_) {
        auto rx_opened = rx_.Open(pool_);
        if (!rx_opened) {
            return Error("open async RX pool", rx_opened.error().provider_error);
        }
    } else {
        const auto page_value = ::sysconf(_SC_PAGESIZE);
        if (page_value <= 0) {
            return Error("query page size");
        }
        const auto page = static_cast<std::size_t>(page_value);
        const auto length = (options.rx_depth * options.bytes + page - 1) / page * page;
        storage_.resize(length + page - 1);
        const auto address = reinterpret_cast<std::uintptr_t>(storage_.data());
        memory_ = std::span(storage_).subspan((page - address % page) % page, length);
        urma_seg_cfg_t segment{};
        segment.va = reinterpret_cast<std::uintptr_t>(memory_.data());
        segment.len = memory_.size();
        segment.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
        errno = 0;
        segment_ = UrmaApi::RegisterSeg(ctx, &segment);
        if (!segment_) {
            return Error("register async TX memory", errno);
        }
        for (std::uint32_t i = 0; i < options.rx_depth; ++i) {
            sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(memory_.data() + i * options.bytes),
                        .len = options.bytes,
                        .tseg = segment_};
        }
        auto ledger = ledger_.Open(pool_, options.rx_depth);
        if (!ledger) {
            return Error("open async TX ledger", static_cast<int>(ledger.error().code));
        }
        auto sender = sender_.Open(ledger_);
        if (!sender) {
            return Error("open async sender", static_cast<int>(sender.error().code));
        }
    }
    opened_ = true;
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Exchange(ControlChannel& channel) {
    const JfrLimitHello local{options_, server_, jetty_->jetty_id};
    const auto wire = EncodeJfrLimitHello(local);
    auto sent = channel.Write(wire, options_.timeout_ms);
    if (!sent) {
        return sent;
    }
    JfrLimitHelloBytes peer_wire{};
    if (auto received = channel.Read(peer_wire, options_.timeout_ms); !received) {
        return received;
    }
    auto peer = DecodeJfrLimitHello(peer_wire);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    if (auto valid = ValidateJfrLimitPeer(local, *peer); !valid) {
        return valid;
    }
    peer_ = peer->endpoint;
    if (!server_) {
        urma_rjetty_t remote{};
        remote.jetty_id = peer_;
        remote.trans_mode = URMA_TM_RM;
        remote.type = URMA_JETTY;
        remote.tp_type = URMA_CTP;
        urma_token_t token{};
        errno = 0;
        remote_ = UrmaApi::ImportJetty(ctx_, &remote, &token);
        if (!remote_) {
            return Error("import async receiver", errno);
        }
    }
    return {};
}

std::expected<void, ToolError> JfrLimitSession::QueryThreshold(JfrThresholdSnapshot& snapshot) {
    urma_jfr_cfg_t queried{};
    urma_jfr_attr_t attr{};
    const auto status = UrmaApi::QueryJfr(pool_.jfr(), &queried, &attr);
    if (status != URMA_SUCCESS) {
        return Error("query JFR threshold", status);
    }
    snapshot = {.queried = true, .depth = queried.depth, .threshold = attr.rx_threshold};
    return {};
}

std::expected<void, ToolError> JfrLimitSession::ConfigureThreshold(std::uint32_t threshold,
                                                                   JfrThresholdSnapshot* snapshot) {
    urma_jfr_attr_t attr{};
    attr.mask = JFR_RX_THRESHOLD;
    attr.rx_threshold = threshold;
    const auto status = UrmaApi::ModifyJfr(pool_.jfr(), &attr);
    if (status != URMA_SUCCESS) {
        return Error("modify JFR threshold", status);
    }
    stats_.threshold_disabled = false;
    JfrThresholdSnapshot local;
    auto& queried = snapshot ? *snapshot : local;
    if (auto result = QueryThreshold(queried); !result) {
        return result;
    }
    if (threshold) {
        stats_.queried_threshold = queried.threshold;
        stats_.queried_depth = queried.depth;
    }
    // 请求与查询不一致时不能声称已设置被测水线，也不修改 provider 的缓存来伪造一致。
    if (queried.threshold != threshold || queried.depth < options_.rx_depth) {
        return Error("JFR threshold/depth readback mismatch", EPROTO);
    }
    if (!threshold) {
        stats_.threshold_disabled = true;
    }
    return {};
}

std::expected<void, ToolError> JfrLimitSession::PrepareCrossing(JfrLimitRoundStats& round, bool first) {
    if (auto queried = QueryThreshold(round.before_rearm); !queried) {
        return queried;
    }
    // 只修改 RX_THRESHOLD；READY 会重置 provider 的 RX 索引，不能当作水线重新使能。
    if (!first && options_.rearm == JfrLimitRearm::kToggle) {
        if (auto disabled = ConfigureThreshold(0, &round.disabled); !disabled) {
            return disabled;
        }
    }
    if (first || options_.rearm != JfrLimitRearm::kNone) {
        if (auto configured = ConfigureThreshold(options_.threshold, &round.after_rearm); !configured) {
            return configured;
        }
    } else if (auto queried = QueryThreshold(round.after_rearm); !queried) {
        return queried;
    }
    // 水线配置后的事件独立观察，不能误算为随后跨线产生的事件。
    const auto phase = first ? JfrLimitPhase::kBaseline : JfrLimitPhase::kAboveWater;
    if (auto observed = Observe(phase, &round.above_water_events); !observed) {
        return observed;
    }
    if (first && round.above_water_events.limit_events) {
        return Error("JFR_LIMIT observed before crossing the waterline", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Refill() {
    auto refilled = rx_.Refill(options_.rx_depth);
    if (!refilled) {
        return Error("refill async RX", refilled.error().provider_error);
    }
    stats_.posted += *refilled;
    if (rx_.posted() != options_.rx_depth) {
        return Error("incomplete async RX refill", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> JfrLimitSession::CollectEvents(JfrLimitPhase phase, JfrLimitEventStats* round_events) {
    JfrLimitEventStats events;
    auto polled = PollJfrLimitEvents(ctx_, pool_.jfr(), events);
    AddEvents(stats_.events[static_cast<std::size_t>(phase)], events);
    if (round_events) {
        AddEvents(*round_events, events);
    }
    if (!polled) {
        return std::unexpected(polled.error());
    }
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Observe(JfrLimitPhase phase, JfrLimitEventStats* round_events) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.observe_ms);
    do {
        auto polled = CollectEvents(phase, round_events);
        if (!polled) {
            return polled;
        }
        std::this_thread::yield();
    } while (Clock::now() < deadline);
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Transfer(ControlChannel& channel, std::uint32_t sequence,
                                                         std::uint32_t count, JfrLimitPhase phase,
                                                         JfrLimitEventStats* round_events) {
    const auto marker = kReady ^ sequence;
    auto ready =
        server_ ? channel.SendMarker(marker, options_.timeout_ms) : channel.ExpectMarker(marker, options_.timeout_ms);
    if (!ready) {
        return ready;
    }
    seen_.fill(false);
    if (!server_) {
        for (std::uint32_t i = 0; i < count; ++i) {
            auto payload = memory_.subspan(i * options_.bytes, options_.bytes);
            Put(payload, 0, kMagic);
            Put(payload, 4, sequence + i);
            for (std::size_t offset = 8; offset < payload.size(); ++offset) {
                payload[offset] = Pattern(sequence + i, offset);
            }
            requests_[i] = {.metadata = {.connection_id = 1,
                                         .operation_id = sequence + i + 1,
                                         .operation_generation = 1,
                                         .target = remote_,
                                         .opcode = URMA_OPC_SEND,
                                         .buffer_lease = i + 1},
                            .sges = std::span(sges_).subspan(i, 1)};
        }
        auto sent = sender_.Send(std::span(requests_).first(count), std::span(ids_).first(count), lane_);
        if (!sent) {
            stats_.accepted_known = sent.error().accepted.has_value();
            if (sent.error().accepted) {
                stats_.accepted += *sent.error().accepted;
            }
            return Error("submit async SEND batch; retain accepted dependencies", sent.error().provider_status);
        }
        stats_.accepted += *sent;
    }
    std::uint32_t completed = 0;
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.timeout_ms);
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> rx_events{};
    std::array<raw::TxCompletionEvent, raw::TxCompletionProcessor::kMaxPollBatch> tx_events{};
    while (completed < count) {
        if (server_) {
            auto async = CollectEvents(phase, round_events);
            if (!async) {
                return async;
            }
            auto polled = rx_.Poll(rx_events);
            if (!polled) {
                return Error("poll async RX", polled.error().provider_error);
            }
            for (std::size_t i = 0; i < *polled; ++i) {
                auto& event = rx_events[i];
                if (event.error || !event.lease || event.payload.size() != options_.bytes ||
                    event.completion.remote_id.id != peer_.id ||
                    std::memcmp(event.completion.remote_id.eid.raw, peer_.eid.raw, 16) ||
                    Get(event.payload, 0) != kMagic) {
                    return Error("invalid async RX completion/payload", EPROTO);
                }
                const auto identity = Get(event.payload, 4);
                if (identity < sequence || identity - sequence >= count || seen_[identity - sequence]) {
                    return Error("duplicate/foreign async RX identity", EPROTO);
                }
                for (std::size_t offset = 8; offset < event.payload.size(); ++offset) {
                    if (event.payload[offset] != Pattern(identity, offset)) {
                        return Error("corrupt async RX payload", EPROTO);
                    }
                }
                if (auto released = rx_.Release(*event.lease); !released) {
                    return Error("release async RX lease", released.error().provider_error);
                }
                seen_[identity - sequence] = true;
                ++stats_.received;
                ++completed;
            }
        } else {
            auto polled = processor_.Poll(tx_events);
            if (!polled) {
                return Error("poll async TX", static_cast<int>(polled.error().code));
            }
            std::optional<ToolError> first_error;
            const auto remember_error = [&](const char* operation, int code) {
                if (!first_error) {
                    first_error = ToolError{operation, code ? code : EPROTO};
                }
            };
            // 完成层已经消费并退休整批可信 WR；不能因首条错误漏记该 WR 或同批后续成功项。
            // 错误仍保留到批尾返回，不把失败退休当作流量成功，也不继续下一轮发送。
            for (std::size_t i = 0; i < polled->count; ++i) {
                const auto& event = tx_events[i];
                if (event.error || !event.record || event.kind != raw::TxCompletionKind::kCompleted ||
                    event.completion.status != URMA_CR_SUCCESS) {
                    remember_error("invalid async TX completion", event.completion.status);
                }
                if (!event.record || event.kind != raw::TxCompletionKind::kCompleted) {
                    continue;
                }
                if (event.record->metadata.operation_id <= sequence ||
                    event.record->metadata.operation_id - sequence > count) {
                    remember_error("foreign async TX identity", EPROTO);
                    continue;
                }
                const auto index = event.record->metadata.operation_id - sequence - 1;
                if (seen_[index] || event.record->id != ids_[index]) {
                    remember_error("duplicate/foreign async TX completion", EPROTO);
                    continue;
                }
                seen_[index] = true;
                ++stats_.retired;
                ++completed;
            }
            if (first_error) {
                return std::unexpected(*first_error);
            }
        }
        if (completed < count && Clock::now() >= deadline) {
            return Error("wait async test traffic", ETIMEDOUT);
        }
        std::this_thread::yield();
    }
    // 只有双方真实 TX/RX CQE 与 payload 校验均完成，才允许复用本轮槽位和推进下一轮。
    return Barrier(channel, kDone ^ sequence, options_.timeout_ms);
}

std::expected<void, ToolError> JfrLimitSession::ReportCrossing(ControlChannel& channel, std::uint32_t index) {
    std::array<std::byte, 8> result{};
    auto& round = stats_.rounds[index];
    if (server_) {
        Put(result, 0, index);
        Put(result, 4, round.observed ? 1 : 0);
        return channel.Write(result, options_.timeout_ms);
    }
    if (auto received = channel.Read(result, options_.timeout_ms); !received) {
        return received;
    }
    if (Get(result, 0) != index || Get(result, 4) > 1 || (!index && !Get(result, 4))) {
        return Error("peer async round result mismatch", EPROTO);
    }
    round.crossing_completed = true;
    round.observed = Get(result, 4) != 0;
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Run(ControlChannel& channel) {
    if (!opened_ || started_) {
        return Error("invalid async run state", EINVAL);
    }
    started_ = true;
    unsafe_ = true;
    quiescent_ = false;
    if (auto exchanged = Exchange(channel); !exchanged) {
        return exchanged;
    }
    const auto crossing = options_.rx_depth - options_.threshold + 1;
    std::uint32_t sequence = 0;
    for (std::uint32_t index = 0; index < options_.crossings; ++index) {
        auto& round = stats_.rounds[index];
        round.started = true;
        const auto posted_before = stats_.posted;
        const auto received_before = stats_.received;
        const auto accepted_before = stats_.accepted;
        const auto retired_before = stats_.retired;
        // 失败也保存本轮真实进展；不能把尚未推进的轮次报告为未观察到事件。
        ScopeExit record_traffic([&]() noexcept {
            round.posted = stats_.posted - posted_before;
            round.received = stats_.received - received_before;
            round.accepted = stats_.accepted - accepted_before;
            round.retired = stats_.retired - retired_before;
        });
        if (server_) {
            if (auto filled = Refill(); !filled) {
                return filled;
            }
            if (index) {
                if (auto observed = Observe(JfrLimitPhase::kRefillControl, &round.refill_events); !observed) {
                    return observed;
                }
            }
        }
        if (index) {
            // 每次跨线后用新身份流量确认补投仍可接收，再补满并执行指定阈值操作。
            if (auto transfer = Transfer(channel, sequence, 1, JfrLimitPhase::kRefillControl, &round.refill_events);
                !transfer) {
                return transfer;
            }
            ++sequence;
            if (server_) {
                if (auto filled = Refill(); !filled) {
                    return filled;
                }
                if (auto observed = Observe(JfrLimitPhase::kRefillControl, &round.refill_events); !observed) {
                    return observed;
                }
            }
        }
        if (server_) {
            if (auto prepared = PrepareCrossing(round, index == 0); !prepared) {
                return prepared;
            }
        }
        const auto phase = index ? JfrLimitPhase::kRepeatCrossing : JfrLimitPhase::kFirstCrossing;
        if (auto transfer = Transfer(channel, sequence, crossing, phase, &round.crossing_events); !transfer) {
            return transfer;
        }
        sequence += crossing;
        if (server_) {
            if (auto observed = Observe(phase, &round.crossing_events); !observed) {
                return observed;
            }
            round.crossing_completed = true;
            round.observed = round.crossing_events.limit_events != 0;
            if (!index) {
                stats_.first_observed = round.observed;
            } else {
                stats_.repeat_observed |= round.observed;
            }
            // 事件后的阈值只记录实际回读；不能预设硬件会保持或清除已配置的水线。
            if (auto queried = QueryThreshold(round.after_crossing); !queried) {
                return queried;
            }
            if (!index && !round.observed) {
                return Error("JFR_LIMIT not observed after first crossing", ETIMEDOUT);
            }
        }
        // 客户端收到该轮结果后才进入下一轮；事件缺失不会跳过后续观察轮次。
        if (auto reported = ReportCrossing(channel, index); !reported) {
            return reported;
        }
        if (!server_) {
            if (!index) {
                stats_.first_observed = round.observed;
            } else {
                stats_.repeat_observed |= round.observed;
            }
        }
    }
    if (server_) {
        if (auto configured = ConfigureThreshold(0); !configured) {
            return configured;
        }
        if (auto observed = Observe(JfrLimitPhase::kCleanup); !observed) {
            return observed;
        }
    }
    // 不借助删除 JFR 回收 posted RX；关水线后用新身份的 cleanup SEND 消费全部余量。
    if (auto transfer = Transfer(channel, sequence, options_.threshold - 1, JfrLimitPhase::kCleanup); !transfer) {
        return transfer;
    }
    if (server_) {
        if (auto observed = Observe(JfrLimitPhase::kCleanup); !observed) {
            return observed;
        }
        if (rx_.posted() || rx_.held()) {
            return Error("RX ownership unresolved after cleanup", EBUSY);
        }
    }
    if (auto stopped = Barrier(channel, kQuiescent, options_.timeout_ms); !stopped) {
        return stopped;
    }
    quiescent_ = true;
    unsafe_ = false;
    return {};
}

std::expected<void, ToolError> JfrLimitSession::Close() {
    if (!opened_ || unsafe_ || !quiescent_ || ledger_.size() || rx_.posted() || rx_.held()) {
        return Error("async DMA/peer state unresolved; retain dependencies", EBUSY);
    }
    // 一旦关闭中途失败，禁止重试可能已部分失效的 provider 对象；入口保持整个依赖链常驻。
    unsafe_ = true;
    if (server_) {
        if (started_) {
            if (!stats_.threshold_disabled) {
                return Error("JFR threshold still enabled", EBUSY);
            }
            if (auto observed = Observe(JfrLimitPhase::kCleanup); !observed) {
                return observed;
            }
        }
        if (auto closed = rx_.Close(); !closed) {
            return Error("close async RX pool", closed.error().provider_error);
        }
    } else {
        if (auto closed = sender_.Close(); !closed) {
            return Error("close async sender", static_cast<int>(closed.error().code));
        }
        if (auto closed = ledger_.Close(); !closed) {
            return Error("close async ledger", static_cast<int>(closed.error().code));
        }
        if (remote_) {
            const auto status = UrmaApi::UnimportJetty(remote_);
            if (status != URMA_SUCCESS) {
                return Error("unimport async target", status);
            }
            remote_ = nullptr;
        }
    }
    if (auto closed = pool_.Close(); !closed) {
        return Error("close async jetty pool", closed.error().provider_error);
    }
    if (segment_) {
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error("unregister async TX memory", status);
        }
        segment_ = nullptr;
        memory_ = {};
        storage_.clear();
    }
    opened_ = false;
    ctx_ = nullptr;
    unsafe_ = false;
    return {};
}
} // namespace tools
} // namespace kbsocket
