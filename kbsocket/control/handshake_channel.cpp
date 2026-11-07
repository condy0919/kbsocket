// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/handshake_channel.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace kbsocket {
namespace control {
namespace {
constexpr std::uint32_t kMagic = 0x4b425348; // KBSH
constexpr std::size_t kHelloPrefix = 104, kTxLaneBytes = 32, kReadyBytes = 144, kRejectBytes = 20;
constexpr std::size_t kLaneUpdatePrefix = 120, kLaneAckBytes = 104;
constexpr std::size_t kCreditStopBytes = 128;
constexpr std::uint16_t kCapabilities = kHandshakeCapabilityRaw | kHandshakeCapabilityTcp;

class Writer {
public:
    explicit Writer(std::span<std::byte> bytes) : bytes_(bytes) {}
    void Put(std::uint64_t value, std::size_t size) noexcept {
        for (std::size_t i = size; i != 0; --i) {
            bytes_[offset_++] = static_cast<std::byte>((value >> ((i - 1) * 8)) & 255);
        }
    }
    void Copy(std::span<const std::byte> source) noexcept {
        std::copy(source.begin(), source.end(), bytes_.begin() + offset_);
        offset_ += source.size();
    }

private:
    std::span<std::byte> bytes_;
    std::size_t offset_ = 0;
};
class Reader {
public:
    explicit Reader(std::span<const std::byte> bytes) : bytes_(bytes) {}
    std::uint64_t Get(std::size_t size) noexcept {
        std::uint64_t result = 0;
        for (std::size_t i = 0; i < size; ++i) {
            result = (result << 8) | std::to_integer<std::uint8_t>(bytes_[offset_++]);
        }
        return result;
    }
    void Copy(std::span<std::byte> destination) noexcept {
        std::copy_n(bytes_.begin() + offset_, destination.size(), destination.begin());
        offset_ += destination.size();
    }

private:
    std::span<const std::byte> bytes_;
    std::size_t offset_ = 0;
};

bool ValidIdentity(const HandshakeIdentity& id) noexcept {
    return id.runtime != 0 && id.receive_domain != 0 && id.endpoint_generation != 0 && id.authorization != 0 &&
           id.session != 0 && id.token.generation != 0;
}
void PutIdentity(Writer& out, const HandshakeIdentity& id) noexcept {
    out.Put(id.runtime, 8);
    out.Put(id.receive_domain, 8);
    out.Put(id.endpoint_generation, 8);
    out.Put(id.authorization, 8);
    out.Put(id.session, 8);
    out.Put(id.token.slot, 4);
    out.Put(id.token.generation, 4);
}
HandshakeIdentity GetIdentity(Reader& in) noexcept {
    HandshakeIdentity id;
    id.runtime = in.Get(8);
    id.receive_domain = in.Get(8);
    id.endpoint_generation = in.Get(8);
    id.authorization = in.Get(8);
    id.session = in.Get(8);
    id.token.slot = static_cast<std::uint32_t>(in.Get(4));
    id.token.generation = static_cast<std::uint32_t>(in.Get(4));
    return id;
}

std::expected<std::size_t, HandshakeError> BodySize(const HandshakeMessage& message) noexcept {
    switch (message.kind) {
    case HandshakeMessageKind::kHello: {
        const auto& hello = message.hello;
        if (hello.wire_version != connection::kWireVersion) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kVersionMismatch});
        }
        if (!ValidIdentity(hello.identity) || hello.capabilities == 0 || (hello.capabilities & ~kCapabilities) != 0 ||
            hello.descriptor_bytes > kHandshakeMaxDescriptorBytes) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        if ((hello.capabilities & kHandshakeCapabilityRaw) != 0) {
            if (hello.descriptor_bytes == 0 || hello.credit_epoch == 0) {
                return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
            }
            const auto valid = ValidateHandshakeTxLanes(hello.tx_lanes);
            if (!valid) {
                return std::unexpected(valid.error());
            }
            for (std::uint32_t i = 0; i < hello.tx_lanes.count; ++i) {
                const auto& lane = hello.tx_lanes.lanes[i];
                // 被动描述符没有 uasid，不能以另一 uasid 声明绕过端点角色隔离。
                if (lane.eid == hello.eid && lane.jetty_id == hello.jetty_id) {
                    return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
                }
            }
        } else if (hello.descriptor_bytes != 0 || hello.credit_epoch != 0 || hello.jetty_id != 0 ||
                   hello.import_token != 0 || hello.eid != std::array<std::byte, 16>{} || hello.tx_lanes.count != 0 ||
                   hello.tx_lanes.pool_epoch != 0) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        return kHelloPrefix + kTxLaneBytes * hello.tx_lanes.count + hello.descriptor_bytes;
    }
    case HandshakeMessageKind::kReady: {
        const auto& ready = message.ready;
        if (!ValidIdentity(ready.sender) || !ValidIdentity(ready.receiver) ||
            (ready.transport != HandshakeTransport::kRaw && ready.transport != HandshakeTransport::kTcp) ||
            (ready.transport == HandshakeTransport::kRaw && ready.credit_epoch == 0) ||
            ready.max_frame_seq > (std::uint64_t{1} << 32)) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        return kReadyBytes;
    }
    case HandshakeMessageKind::kReject:
        if (message.reject.sender_session == 0 || message.reject.receiver_session == 0 || message.reject.reason == 0) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        return kRejectBytes;
    case HandshakeMessageKind::kLaneUpdate: {
        const auto& update = message.lane_update;
        if (!ValidIdentity(update.sender) || !ValidIdentity(update.receiver) || !update.update_id) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        const auto valid = ValidateHandshakeTxLanes(update.tx_lanes);
        if (!valid) {
            return std::unexpected(valid.error());
        }
        return kLaneUpdatePrefix + kTxLaneBytes * update.tx_lanes.count;
    }
    case HandshakeMessageKind::kLaneAck:
        if (!ValidIdentity(message.lane_ack.sender) || !ValidIdentity(message.lane_ack.receiver) ||
            !message.lane_ack.update_id) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        return kLaneAckBytes;
    case HandshakeMessageKind::kCreditStop:
    case HandshakeMessageKind::kCreditStopAck: {
        const auto& stop =
            message.kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
        if (!ValidIdentity(stop.sender) || !ValidIdentity(stop.receiver) || !stop.sender_credit_epoch ||
            !stop.receiver_credit_epoch) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        return kCreditStopBytes;
    }
    }
    return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
}

std::expected<std::size_t, HandshakeError> ParseHeader(std::span<const std::byte> header) noexcept {
    if (header.size() < kHandshakeHeaderBytes) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
    }
    Reader in(header);
    if (in.Get(4) != kMagic) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
    }
    if (in.Get(2) != kHandshakeVersion) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kVersionMismatch});
    }
    const auto kind = static_cast<HandshakeMessageKind>(in.Get(1));
    const auto reserved = in.Get(1);
    const auto length = in.Get(4);
    const bool valid_size =
        (kind == HandshakeMessageKind::kHello && length >= kHelloPrefix &&
         length <= kHelloPrefix + kTxLaneBytes * kHandshakeMaxTxLanes + kHandshakeMaxDescriptorBytes) ||
        (kind == HandshakeMessageKind::kReady && length == kReadyBytes) ||
        (kind == HandshakeMessageKind::kReject && length == kRejectBytes) ||
        (kind == HandshakeMessageKind::kLaneUpdate && length >= kLaneUpdatePrefix &&
         length <= kLaneUpdatePrefix + kTxLaneBytes * kHandshakeMaxTxLanes) ||
        (kind == HandshakeMessageKind::kLaneAck && length == kLaneAckBytes) ||
        ((kind == HandshakeMessageKind::kCreditStop || kind == HandshakeMessageKind::kCreditStopAck) &&
         length == kCreditStopBytes);
    if (reserved != 0 || !valid_size) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
    }
    return kHandshakeHeaderBytes + length;
}
} // namespace

std::expected<void, HandshakeError> ValidateHandshakeTxLanes(const HandshakeTxLanes& lanes) noexcept {
    if (lanes.pool_epoch == 0 || lanes.count == 0 || lanes.count > kHandshakeMaxTxLanes) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
    }
    for (std::uint32_t i = 0; i < lanes.count; ++i) {
        const auto& lane = lanes.lanes[i];
        if (lane.generation == 0) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        // CQE 来源不能区分相同物理端点的两个软件代次，因此重复键不能只比较 generation。
        for (std::uint32_t j = 0; j < i; ++j) {
            const auto& previous = lanes.lanes[j];
            if (lane.eid == previous.eid && lane.uasid == previous.uasid && lane.jetty_id == previous.jetty_id) {
                return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
            }
        }
    }
    return {};
}

bool SameTxLanes(const HandshakeTxLanes& left, const HandshakeTxLanes& right) noexcept {
    if (left.pool_epoch != right.pool_epoch || left.count != right.count || left.count > kHandshakeMaxTxLanes) {
        return false;
    }
    return std::equal(left.lanes.begin(), left.lanes.begin() + left.count, right.lanes.begin());
}

std::expected<std::size_t, HandshakeError> EncodeHandshakeMessage(const HandshakeMessage& message,
                                                                  std::span<std::byte> output) noexcept {
    const auto body = BodySize(message);
    if (!body) {
        return std::unexpected(body.error());
    }
    const auto size = kHandshakeHeaderBytes + *body;
    if (output.size() < size) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kBufferTooSmall});
    }
    Writer out(output.first(size));
    out.Put(kMagic, 4);
    out.Put(kHandshakeVersion, 2);
    out.Put(static_cast<std::uint8_t>(message.kind), 1);
    out.Put(0, 1);
    out.Put(*body, 4);
    switch (message.kind) {
    case HandshakeMessageKind::kHello: {
        const auto& hello = message.hello;
        out.Put(hello.wire_version, 2);
        out.Put(hello.capabilities, 2);
        PutIdentity(out, hello.identity);
        out.Put(hello.credit_epoch, 8);
        out.Copy(hello.eid);
        out.Put(hello.jetty_id, 4);
        out.Put(hello.import_token, 4);
        out.Put(hello.descriptor_bytes, 4);
        out.Put(hello.tx_lanes.pool_epoch, 8);
        out.Put(hello.tx_lanes.count, 4);
        out.Put(0, 4);
        for (std::uint32_t i = 0; i < hello.tx_lanes.count; ++i) {
            const auto& lane = hello.tx_lanes.lanes[i];
            out.Copy(lane.eid);
            out.Put(lane.uasid, 4);
            out.Put(lane.jetty_id, 4);
            out.Put(lane.generation, 8);
        }
        out.Copy(std::span(hello.descriptor).first(hello.descriptor_bytes));
        break;
    }
    case HandshakeMessageKind::kReady: {
        const auto& ready = message.ready;
        PutIdentity(out, ready.sender);
        PutIdentity(out, ready.receiver);
        out.Put(static_cast<std::uint32_t>(ready.transport), 4);
        out.Put(0, 4);
        out.Put(ready.credit_epoch, 8);
        out.Put(ready.max_credit_seq, 8);
        out.Put(ready.max_control_credit_seq, 8);
        out.Put(ready.max_frame_seq, 8);
        out.Put(ready.max_stream_bytes, 8);
        break;
    }
    case HandshakeMessageKind::kReject:
        out.Put(message.reject.sender_session, 8);
        out.Put(message.reject.receiver_session, 8);
        out.Put(message.reject.reason, 4);
        break;
    case HandshakeMessageKind::kLaneUpdate: {
        const auto& update = message.lane_update;
        PutIdentity(out, update.sender);
        PutIdentity(out, update.receiver);
        out.Put(update.update_id, 8);
        out.Put(update.tx_lanes.pool_epoch, 8);
        out.Put(update.tx_lanes.count, 4);
        out.Put(0, 4);
        for (std::uint32_t i = 0; i < update.tx_lanes.count; ++i) {
            const auto& lane = update.tx_lanes.lanes[i];
            out.Copy(lane.eid);
            out.Put(lane.uasid, 4);
            out.Put(lane.jetty_id, 4);
            out.Put(lane.generation, 8);
        }
        break;
    }
    case HandshakeMessageKind::kLaneAck:
        PutIdentity(out, message.lane_ack.sender);
        PutIdentity(out, message.lane_ack.receiver);
        out.Put(message.lane_ack.update_id, 8);
        break;
    case HandshakeMessageKind::kCreditStop:
    case HandshakeMessageKind::kCreditStopAck: {
        const auto& stop =
            message.kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
        PutIdentity(out, stop.sender);
        PutIdentity(out, stop.receiver);
        out.Put(stop.sender_credit_epoch, 8);
        out.Put(stop.receiver_credit_epoch, 8);
        out.Put(stop.spent[0], 8);
        out.Put(stop.spent[1], 8);
        break;
    }
    }
    return size;
}

std::expected<HandshakeMessage, HandshakeError> DecodeHandshakeMessage(std::span<const std::byte> frame) noexcept {
    const auto size = ParseHeader(frame);
    if (!size) {
        return std::unexpected(size.error());
    }
    if (frame.size() != *size) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
    }
    HandshakeMessage message;
    message.kind = static_cast<HandshakeMessageKind>(std::to_integer<std::uint8_t>(frame[6]));
    Reader in(frame.subspan(kHandshakeHeaderBytes));
    switch (message.kind) {
    case HandshakeMessageKind::kHello: {
        auto& hello = message.hello;
        hello.wire_version = static_cast<std::uint16_t>(in.Get(2));
        hello.capabilities = static_cast<std::uint16_t>(in.Get(2));
        hello.identity = GetIdentity(in);
        hello.credit_epoch = in.Get(8);
        in.Copy(hello.eid);
        hello.jetty_id = static_cast<std::uint32_t>(in.Get(4));
        hello.import_token = static_cast<std::uint32_t>(in.Get(4));
        hello.descriptor_bytes = static_cast<std::uint32_t>(in.Get(4));
        hello.tx_lanes.pool_epoch = in.Get(8);
        hello.tx_lanes.count = static_cast<std::uint32_t>(in.Get(4));
        const auto reserved = in.Get(4);
        // 先验证完整长度与数量，再读取可变列表；不能信任远端声明的 count。
        if (hello.descriptor_bytes > kHandshakeMaxDescriptorBytes || hello.tx_lanes.count > kHandshakeMaxTxLanes ||
            reserved != 0 ||
            frame.size() !=
                kHandshakeHeaderBytes + kHelloPrefix + kTxLaneBytes * hello.tx_lanes.count + hello.descriptor_bytes) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        for (std::uint32_t i = 0; i < hello.tx_lanes.count; ++i) {
            auto& lane = hello.tx_lanes.lanes[i];
            in.Copy(lane.eid);
            lane.uasid = static_cast<std::uint32_t>(in.Get(4));
            lane.jetty_id = static_cast<std::uint32_t>(in.Get(4));
            lane.generation = in.Get(8);
        }
        in.Copy(std::span(hello.descriptor).first(hello.descriptor_bytes));
        break;
    }
    case HandshakeMessageKind::kReady: {
        auto& ready = message.ready;
        ready.sender = GetIdentity(in);
        ready.receiver = GetIdentity(in);
        ready.transport = static_cast<HandshakeTransport>(in.Get(4));
        if (in.Get(4) != 0) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        ready.credit_epoch = in.Get(8);
        ready.max_credit_seq = in.Get(8);
        ready.max_control_credit_seq = in.Get(8);
        ready.max_frame_seq = in.Get(8);
        ready.max_stream_bytes = in.Get(8);
        break;
    }
    case HandshakeMessageKind::kReject:
        message.reject.sender_session = in.Get(8);
        message.reject.receiver_session = in.Get(8);
        message.reject.reason = static_cast<std::uint32_t>(in.Get(4));
        break;
    case HandshakeMessageKind::kLaneUpdate: {
        auto& update = message.lane_update;
        update.sender = GetIdentity(in);
        update.receiver = GetIdentity(in);
        update.update_id = in.Get(8);
        update.tx_lanes.pool_epoch = in.Get(8);
        update.tx_lanes.count = static_cast<std::uint32_t>(in.Get(4));
        const auto reserved = in.Get(4);
        // 在线公告也先核对精确正文长度，再访问有界 Lane 列表。
        if (update.tx_lanes.count > kHandshakeMaxTxLanes || reserved != 0 ||
            frame.size() != kHandshakeHeaderBytes + kLaneUpdatePrefix + kTxLaneBytes * update.tx_lanes.count) {
            return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidFrame});
        }
        for (std::uint32_t i = 0; i < update.tx_lanes.count; ++i) {
            auto& lane = update.tx_lanes.lanes[i];
            in.Copy(lane.eid);
            lane.uasid = static_cast<std::uint32_t>(in.Get(4));
            lane.jetty_id = static_cast<std::uint32_t>(in.Get(4));
            lane.generation = in.Get(8);
        }
        break;
    }
    case HandshakeMessageKind::kLaneAck:
        message.lane_ack.sender = GetIdentity(in);
        message.lane_ack.receiver = GetIdentity(in);
        message.lane_ack.update_id = in.Get(8);
        break;
    case HandshakeMessageKind::kCreditStop:
    case HandshakeMessageKind::kCreditStopAck: {
        auto& stop = message.kind == HandshakeMessageKind::kCreditStop ? message.credit_stop : message.credit_stop_ack;
        stop.sender = GetIdentity(in);
        stop.receiver = GetIdentity(in);
        stop.sender_credit_epoch = in.Get(8);
        stop.receiver_credit_epoch = in.Get(8);
        stop.spent[0] = in.Get(8);
        stop.spent[1] = in.Get(8);
        break;
    }
    }
    const auto valid = BodySize(message);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    return message;
}

HandshakeChannel::~HandshakeChannel() {
    const auto ignored = Close();
    (void)ignored;
}

std::expected<void, HandshakeError> HandshakeChannel::Open(int fd, bool compatible_peer, std::uint64_t now_ns,
                                                           std::uint64_t deadline_ns) noexcept {
    if (opened_once_) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInUse});
    }
    if (!compatible_peer) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kUnauthorized});
    }
    if (fd < 0 || deadline_ns <= now_ns) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidArgument});
    }
    const int flags = fcntl(fd, F_GETFL);
    if (flags < 0) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kSystemError, errno});
    }
    if ((flags & O_NONBLOCK) == 0) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kNotNonblocking});
    }
    int type = 0;
    socklen_t length = sizeof(type);
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &length) < 0) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kSystemError, errno});
    }
    if (type != SOCK_STREAM) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidArgument});
    }
    sockaddr_storage peer{};
    length = sizeof(peer);
    if (getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &length) < 0) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kNotConnected, errno});
    }
    fd_ = fd;
    opened_once_ = true;
    last_now_ns_ = now_ns;
    deadline_ns_ = deadline_ns;
    return {};
}

std::expected<void, HandshakeError> HandshakeChannel::CheckOpen() const noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (fd_ < 0) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kNotReady});
    }
    return {};
}

std::expected<void, HandshakeError> HandshakeChannel::Queue(const HandshakeMessage& message) noexcept {
    const auto open = CheckOpen();
    if (!open) {
        return std::unexpected(open.error());
    }
    if (!output_flushed()) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInUse});
    }
    const auto encoded = EncodeHandshakeMessage(message, tx_);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    tx_size_ = *encoded;
    tx_offset_ = 0;
    return {};
}

HandshakeError HandshakeChannel::Fail(HandshakeError error) noexcept {
    if (!failure_) {
        failure_ = error;
    }
    const auto ignored = Close();
    (void)ignored;
    return *failure_;
}

std::expected<HandshakeProgress, HandshakeError> HandshakeChannel::Progress(std::uint64_t now_ns,
                                                                            HandshakeIoBudget budget) noexcept {
    const auto open = CheckOpen();
    if (!open) {
        return std::unexpected(open.error());
    }
    if (now_ns < last_now_ns_ || budget.bytes == 0 || budget.syscalls == 0 || budget.syscalls > 64) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInvalidArgument});
    }
    last_now_ns_ = now_ns;
    if (now_ns >= deadline_ns_) {
        return std::unexpected(Fail({HandshakeErrorCode::kTimeout}));
    }
    HandshakeProgress result;
    auto remaining = budget.bytes;
    while (remaining != 0 && result.syscalls < budget.syscalls && !output_flushed()) {
        const auto count = std::min(remaining, tx_size_ - tx_offset_);
        ++result.syscalls;
        const auto sent = send(fd_, tx_.data() + tx_offset_, count, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            return std::unexpected(Fail({HandshakeErrorCode::kSystemError, errno}));
        }
        if (sent == 0) {
            return std::unexpected(Fail({HandshakeErrorCode::kPeerClosed}));
        }
        const auto bytes = static_cast<std::size_t>(sent);
        tx_offset_ += bytes;
        result.sent += bytes;
        remaining -= bytes;
    }
    while (budget.receive && remaining != 0 && result.syscalls < budget.syscalls && !message_ready_) {
        const auto count = std::min(remaining, rx_target_ - rx_size_);
        ++result.syscalls;
        const auto received = recv(fd_, rx_.data() + rx_size_, count, MSG_DONTWAIT);
        if (received < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            return std::unexpected(Fail({HandshakeErrorCode::kSystemError, errno}));
        }
        if (received == 0) {
            return std::unexpected(Fail({HandshakeErrorCode::kPeerClosed}));
        }
        const auto bytes = static_cast<std::size_t>(received);
        rx_size_ += bytes;
        result.received += bytes;
        remaining -= bytes;
        if (rx_size_ == kHandshakeHeaderBytes) {
            const auto total = ParseHeader(std::span(rx_).first(rx_size_));
            if (!total) {
                return std::unexpected(Fail(total.error()));
            }
            rx_target_ = *total;
        }
        if (rx_size_ == rx_target_) {
            const auto decoded = DecodeHandshakeMessage(std::span(rx_).first(rx_size_));
            if (!decoded) {
                return std::unexpected(Fail(decoded.error()));
            }
            message_ready_ = true;
        }
    }
    result.message_ready = message_ready_;
    result.output_pending = !output_flushed();
    return result;
}

std::expected<HandshakeMessage, HandshakeError> HandshakeChannel::TakeMessage() noexcept {
    const auto open = CheckOpen();
    if (!open) {
        return std::unexpected(open.error());
    }
    if (!message_ready_) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kNotReady});
    }
    auto result = DecodeHandshakeMessage(std::span(rx_).first(rx_size_));
    if (!result) {
        return std::unexpected(Fail(result.error()));
    }
    rx_size_ = 0;
    rx_target_ = kHandshakeHeaderBytes;
    message_ready_ = false;
    return result;
}

std::expected<int, HandshakeError> HandshakeChannel::ReleaseEstablished(bool established) noexcept {
    const auto open = CheckOpen();
    if (!open) {
        return std::unexpected(open.error());
    }
    if (!established || !output_flushed() || rx_size_ != 0 || message_ready_) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kInUse});
    }
    const int fd = fd_;
    fd_ = -1;
    return fd;
}

void HandshakeChannel::Cancel() noexcept {
    if (fd_ >= 0) {
        Fail({HandshakeErrorCode::kCancelled});
    }
}

std::expected<void, HandshakeError> HandshakeChannel::Close() noexcept {
    if (fd_ < 0) {
        return {};
    }
    const int fd = fd_;
    fd_ = -1;
    // Linux close 即便 EINTR 也已经释放 fd；不能重试，避免关闭其他线程复用的新 fd。
    if (close(fd) < 0) {
        return std::unexpected(HandshakeError{HandshakeErrorCode::kSystemError, errno});
    }
    return {};
}
} // namespace control
} // namespace kbsocket
