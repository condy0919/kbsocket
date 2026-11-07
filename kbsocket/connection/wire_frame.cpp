// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/wire_frame.hpp"

#include <algorithm>
#include <limits>

namespace kbsocket {
namespace connection {
namespace {
constexpr std::uint64_t kSequenceEnd = std::uint64_t{1} << 32;

auto Error(WireErrorCode code) noexcept {
    return std::unexpected(WireError{code});
}
bool Extended(WireToken token) noexcept {
    return token.slot > 65535 || token.generation > 255;
}
bool Data(WireFrameType type) noexcept {
    return type == WireFrameType::kDataEager || type == WireFrameType::kDataWithCredit;
}
bool ValidContext(const WireReceiveContext& context) noexcept {
    return context.peer_runtime && context.receive_domain && context.session;
}
void Put(std::span<std::byte> output, std::uint64_t value) noexcept {
    for (std::size_t i = output.size(); i > 0; --i) {
        output[i - 1] = static_cast<std::byte>(value & 0xff);
        value >>= 8;
    }
}
std::uint64_t Get(std::span<const std::byte> input) noexcept {
    std::uint64_t value = 0;
    for (const auto byte : input) {
        value = (value << 8) | std::to_integer<unsigned>(byte);
    }
    return value;
}

std::expected<std::size_t, WireError> BodySize(const WireFrameInput& input) noexcept {
    if (!input.token.generation) {
        return Error(WireErrorCode::kInvalidIdentity);
    }
    if (Data(input.type)) {
        if (input.data_flags & ~std::uint8_t{7}) {
            return Error(WireErrorCode::kInvalidFlags);
        }
        if (input.control != WireControlType::kCreditUpdate || input.session || input.credit_epoch ||
            input.max_control_credit_seq || input.max_frame_seq || input.max_stream_bytes || input.reset_code ||
            (input.type == WireFrameType::kDataEager && input.max_credit_seq)) {
            return Error(WireErrorCode::kInvalidControl);
        }
        // 最高 SN 仅留给 FIN，不能先被普通数据占用而无法有序结束。
        if (input.seq == std::numeric_limits<std::uint32_t>::max()) {
            return Error(WireErrorCode::kInvalidSequence);
        }
        if (input.data.empty() || input.data.size() > kWireMaxPayload) {
            return Error(WireErrorCode::kInvalidLength);
        }
        return input.data.size() + (input.type == WireFrameType::kDataWithCredit ? 8 : 0);
    }
    if (input.type != WireFrameType::kControl) {
        return Error(WireErrorCode::kUnsupportedType);
    }
    if (input.data_flags) {
        return Error(WireErrorCode::kInvalidFlags);
    }
    if (!input.data.empty()) {
        return Error(WireErrorCode::kInvalidLength);
    }
    if (input.control == WireControlType::kRndvRts || input.control == WireControlType::kRndvGrant ||
        input.control == WireControlType::kRndvDone) {
        return Error(WireErrorCode::kUnsupportedType);
    }
    if (!input.session) {
        return Error(WireErrorCode::kInvalidIdentity);
    }
    switch (input.control) {
    case WireControlType::kCreditUpdate:
        if (input.seq) {
            return Error(WireErrorCode::kInvalidSequence);
        }
        if (!input.credit_epoch || input.max_frame_seq > kSequenceEnd || input.reset_code) {
            return Error(WireErrorCode::kInvalidControl);
        }
        return 48;
    case WireControlType::kFin:
    case WireControlType::kReset:
        if (input.credit_epoch || input.max_credit_seq || input.max_control_credit_seq || input.max_frame_seq ||
            input.max_stream_bytes || (input.control == WireControlType::kFin && input.reset_code)) {
            return Error(WireErrorCode::kInvalidControl);
        }
        if (input.control == WireControlType::kReset && (input.seq || !input.reset_code)) {
            return Error(WireErrorCode::kInvalidControl);
        }
        return input.control == WireControlType::kFin ? 8 : 12;
    default:
        return Error(WireErrorCode::kUnsupportedType);
    }
}
std::expected<WireEncodedFrame, WireError> Layout(const WireFrameInput& input) noexcept {
    const auto body_size = BodySize(input);
    if (!body_size) {
        return std::unexpected(body_size.error());
    }
    const bool extended = Extended(input.token);
    const std::size_t extension_bytes = extended ? 8 : 0;
    const std::size_t total = extension_bytes + *body_size;
    if (total > kWireMaxPayload) {
        return Error(WireErrorCode::kInvalidLength);
    }
    const auto flags = (Data(input.type) ? input.data_flags : static_cast<std::uint8_t>(input.control)) |
                       (extended ? kWireFlagExtended : 0);
    const std::uint64_t imm = (static_cast<std::uint64_t>(input.type) << 60) |
                              (static_cast<std::uint64_t>(flags) << 56) |
                              (extended ? 0
                                        : (static_cast<std::uint64_t>(input.token.generation) << 48) |
                                              (static_cast<std::uint64_t>(input.token.slot) << 32)) |
                              input.seq;
    const std::size_t header_bytes =
        Data(input.type) ? extension_bytes + (input.type == WireFrameType::kDataWithCredit ? 8 : 0) : total;
    return WireEncodedFrame{imm, total, header_bytes};
}

void WriteHeader(const WireFrameInput& input, const WireEncodedFrame& layout, std::span<std::byte> output) noexcept {
    const std::size_t extension_bytes = Extended(input.token) ? 8 : 0;
    if (extension_bytes) {
        Put(output.first(4), input.token.slot);
        Put(output.subspan(4, 4), input.token.generation);
    }
    auto body = output.subspan(extension_bytes, layout.header_bytes - extension_bytes);
    if (Data(input.type)) {
        if (input.type == WireFrameType::kDataWithCredit) {
            Put(body.first(8), input.max_credit_seq);
        }
    } else {
        Put(body.first(8), input.session);
        if (input.control == WireControlType::kCreditUpdate) {
            Put(body.subspan(8, 8), input.credit_epoch);
            Put(body.subspan(16, 8), input.max_credit_seq);
            Put(body.subspan(24, 8), input.max_control_credit_seq);
            Put(body.subspan(32, 8), input.max_frame_seq);
            Put(body.subspan(40, 8), input.max_stream_bytes);
        } else if (input.control == WireControlType::kReset) {
            Put(body.subspan(8, 4), input.reset_code);
        }
    }
}
} // namespace

std::expected<WireEncodedFrame, WireError> EncodeWireHeader(const WireFrameInput& input,
                                                            std::span<std::byte> output) noexcept {
    const auto layout = Layout(input);
    if (!layout) {
        return std::unexpected(layout.error());
    }
    if (output.size() < layout->header_bytes) {
        return Error(WireErrorCode::kBufferTooSmall);
    }
    WriteHeader(input, *layout, output);
    return layout;
}

std::expected<WireEncodedFrame, WireError> EncodeWireFrame(const WireFrameInput& input,
                                                           std::span<std::byte> output) noexcept {
    const auto layout = Layout(input);
    if (!layout) {
        return std::unexpected(layout.error());
    }
    // 完整帧的容量检查必须早于头部写入，失败时仍保持 output 原样。
    if (output.size() < layout->payload_bytes) {
        return Error(WireErrorCode::kBufferTooSmall);
    }
    WriteHeader(input, *layout, output);
    if (Data(input.type)) {
        std::copy(input.data.begin(), input.data.end(), output.subspan(layout->header_bytes).begin());
    }
    return layout;
}

std::expected<WireFrameView, WireError> DecodeWireFrame(std::uint64_t imm_data,
                                                        std::span<const std::byte> payload) noexcept {
    if (payload.size() > kWireMaxPayload) {
        return Error(WireErrorCode::kInvalidLength);
    }
    WireFrameView result;
    auto& frame = result.frame;
    frame.type = static_cast<WireFrameType>(imm_data >> 60);
    const auto flags = static_cast<std::uint8_t>((imm_data >> 56) & 15);
    frame.seq = static_cast<std::uint32_t>(imm_data);
    frame.token = {static_cast<std::uint32_t>((imm_data >> 32) & 65535),
                   static_cast<std::uint32_t>((imm_data >> 48) & 255)};
    result.extended_identity = flags & kWireFlagExtended;
    if (result.extended_identity) {
        if (frame.token.slot || frame.token.generation) {
            return Error(WireErrorCode::kInvalidIdentity);
        }
        if (payload.size() < 8) {
            return Error(WireErrorCode::kInvalidLength);
        }
        frame.token = {static_cast<std::uint32_t>(Get(payload.first(4))),
                       static_cast<std::uint32_t>(Get(payload.subspan(4, 4)))};
        if (!Extended(frame.token)) {
            return Error(WireErrorCode::kInvalidIdentity);
        }
        payload = payload.subspan(8);
    }
    if (Data(frame.type)) {
        frame.data_flags = flags & 7;
        if (frame.type == WireFrameType::kDataWithCredit) {
            if (payload.size() < 8) {
                return Error(WireErrorCode::kInvalidLength);
            }
            frame.max_credit_seq = Get(payload.first(8));
            payload = payload.subspan(8);
        }
        frame.data = payload;
    } else if (frame.type == WireFrameType::kControl) {
        frame.control = static_cast<WireControlType>(flags & 7);
        const auto body_size = frame.control == WireControlType::kCreditUpdate ? 48
                               : frame.control == WireControlType::kFin        ? 8
                               : frame.control == WireControlType::kReset      ? 12
                                                                               : 0;
        if (!body_size) {
            return Error(WireErrorCode::kUnsupportedType);
        }
        if (payload.size() != static_cast<std::size_t>(body_size)) {
            return Error(WireErrorCode::kInvalidLength);
        }
        frame.session = Get(payload.first(8));
        if (frame.control == WireControlType::kCreditUpdate) {
            frame.credit_epoch = Get(payload.subspan(8, 8));
            frame.max_credit_seq = Get(payload.subspan(16, 8));
            frame.max_control_credit_seq = Get(payload.subspan(24, 8));
            frame.max_frame_seq = Get(payload.subspan(32, 8));
            frame.max_stream_bytes = Get(payload.subspan(40, 8));
        } else if (frame.control == WireControlType::kReset) {
            frame.reset_code = static_cast<std::uint32_t>(Get(payload.subspan(8, 4)));
        }
    } else {
        return Error(WireErrorCode::kUnsupportedType);
    }
    if (auto checked = BodySize(frame); !checked) {
        return std::unexpected(checked.error());
    }
    return result;
}

std::expected<void, WireError> ValidateWireIdentity(const WireFrameView& decoded, const WireSessionBinding& binding,
                                                    const WireReceiveContext& observed) noexcept {
    if (binding.context.version != kWireVersion || observed.version != kWireVersion) {
        return Error(WireErrorCode::kVersionMismatch);
    }
    if (!ValidContext(binding.context) || !ValidContext(observed) || !binding.token.generation ||
        !binding.credit_epoch) {
        return Error(WireErrorCode::kInvalidIdentity);
    }
    if (binding.context != observed || decoded.frame.token != binding.token) {
        return Error(WireErrorCode::kIdentityMismatch);
    }
    if (decoded.frame.type == WireFrameType::kControl && decoded.frame.session != binding.context.session) {
        return Error(WireErrorCode::kIdentityMismatch);
    }
    if (decoded.frame.type == WireFrameType::kControl && decoded.frame.control == WireControlType::kCreditUpdate &&
        decoded.frame.credit_epoch != binding.credit_epoch) {
        return Error(WireErrorCode::kIdentityMismatch);
    }
    return {};
}

} // namespace connection
} // namespace kbsocket
