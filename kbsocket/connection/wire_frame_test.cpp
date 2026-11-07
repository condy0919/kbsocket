// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/wire_frame.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <new>

#include <gtest/gtest.h>

namespace {
// 编解码快路径的测试区段不包含 GTest 调用，避免将测试框架分配算入协议开销。
thread_local bool count_allocations = false;
thread_local std::size_t allocation_count = 0;
} // namespace
void* operator new(std::size_t size) {
    if (count_allocations) {
        ++allocation_count;
    }
    if (auto* memory = std::malloc(size ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void operator delete(void* memory) noexcept {
    std::free(memory);
}
void operator delete[](void* memory) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t) noexcept {
    std::free(memory);
}

namespace kbsocket {
namespace connection {
namespace {
constexpr std::array<std::byte, 3> kData{std::byte{0x12}, std::byte{0xab}, std::byte{0x80}};

WireFrameInput Data(WireToken token = {0x1234, 0x56}) {
    WireFrameInput result;
    result.token = token;
    result.seq = 0x789abcde;
    result.data_flags = kWireFlagFirst | kWireFlagLast;
    result.data = kData;
    return result;
}
WireFrameInput Control(WireControlType type, WireToken token = {0x1234, 0x56}) {
    WireFrameInput result;
    result.type = WireFrameType::kControl;
    result.control = type;
    result.token = token;
    result.session = 0x0102030405060708;
    if (type == WireControlType::kCreditUpdate) {
        result.credit_epoch = 0x1112131415161718;
        result.max_credit_seq = 0x2122232425262728;
        result.max_control_credit_seq = 0x3132333435363738;
        result.max_frame_seq = 0x41424344;
        result.max_stream_bytes = 0x5152535455565758;
    } else if (type == WireControlType::kFin) {
        result.seq = 0x789abcde;
    } else if (type == WireControlType::kReset) {
        result.reset_code = 0xa1a2a3a4;
    }
    return result;
}

TEST(WireFrameTest, ShortDataPreservesPayloadAndUsesSpecifiedImmBitLayout) {
    std::array<std::byte, 16> output;
    output.fill(std::byte{0xcc});
    const auto input = Data();
    const auto encoded = EncodeWireFrame(input, output);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->imm_data, 0x03561234789abcdeull);
    EXPECT_EQ(encoded->payload_bytes, kData.size());
    EXPECT_TRUE(std::equal(kData.begin(), kData.end(), output.begin()));
    EXPECT_EQ(output[3], std::byte{0xcc});
    const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    EXPECT_FALSE(decoded->extended_identity);
    EXPECT_EQ(decoded->frame.token, input.token);
    EXPECT_EQ(decoded->frame.seq, input.seq);
    EXPECT_EQ(decoded->frame.data_flags, input.data_flags);
    EXPECT_EQ(decoded->frame.data.data(), output.data());
    EXPECT_EQ(decoded->frame.data.size(), kData.size());
}

TEST(WireFrameTest, ExtendedIdentityUsesBigEndianFullFieldsAndZeroShortIdentity) {
    std::array<std::byte, 16> output{};
    const auto input = Data({0x12345678, 0x90abcdef});
    const auto encoded = EncodeWireFrame(input, output);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->imm_data, 0x0b000000789abcdeull);
    const std::array<std::byte, 8> expected{std::byte{0x12}, std::byte{0x34}, std::byte{0x56}, std::byte{0x78},
                                            std::byte{0x90}, std::byte{0xab}, std::byte{0xcd}, std::byte{0xef}};
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin()));
    const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    EXPECT_TRUE(decoded->extended_identity);
    EXPECT_EQ(decoded->frame.token, input.token);
    EXPECT_EQ(decoded->frame.data.data(), output.data() + 8);
}

TEST(WireFrameTest, IdentityBoundariesNeverTruncateHighGenerationOrSlot) {
    std::array<std::byte, 32> output{};
    for (const auto token :
         {WireToken{0, 1}, WireToken{65535, 255}, WireToken{65536, 1}, WireToken{0, 256},
          WireToken{std::numeric_limits<std::uint32_t>::max(), std::numeric_limits<std::uint32_t>::max()}}) {
        const auto encoded = EncodeWireFrame(Data(token), output);
        ASSERT_TRUE(encoded);
        const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->frame.token, token);
        EXPECT_EQ(decoded->extended_identity, token.slot > 65535 || token.generation > 255);
    }
    auto invalid = EncodeWireFrame(Data({7, 0}), output);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, WireErrorCode::kInvalidIdentity);
    auto no_generation = DecodeWireFrame(0x03001234789abcde, kData);
    ASSERT_FALSE(no_generation);
    EXPECT_EQ(no_generation.error().code, WireErrorCode::kInvalidIdentity);
}

TEST(WireFrameTest, ExtendedDecodeRejectsAmbiguousAndNoncanonicalIdentity) {
    std::array<std::byte, 32> output{};
    auto encoded = EncodeWireFrame(Data({65536, 256}), output);
    ASSERT_TRUE(encoded);
    const auto payload = std::span(output).first(encoded->payload_bytes);
    for (const auto extra : {std::uint64_t{1} << 32, std::uint64_t{1} << 48}) {
        auto ambiguous = DecodeWireFrame(encoded->imm_data | extra, payload);
        ASSERT_FALSE(ambiguous);
        EXPECT_EQ(ambiguous.error().code, WireErrorCode::kInvalidIdentity);
    }
    // 人为将扩展 header 改成 short 可表示的 token，不能保留第二种非规范解释。
    std::fill_n(output.begin(), 8, std::byte{0});
    output[7] = std::byte{1};
    auto short_in_extension = DecodeWireFrame(encoded->imm_data, payload);
    ASSERT_FALSE(short_in_extension);
    EXPECT_EQ(short_in_extension.error().code, WireErrorCode::kInvalidIdentity);
    output[1] = std::byte{1};
    output[7] = std::byte{0};
    auto zero_generation = DecodeWireFrame(encoded->imm_data, payload);
    ASSERT_FALSE(zero_generation);
    EXPECT_EQ(zero_generation.error().code, WireErrorCode::kInvalidIdentity);
}

TEST(WireFrameTest, PiggybackCreditIsOneBigEndianCumulativeLimitBeforeData) {
    std::array<std::byte, 32> output{};
    for (const auto token : {WireToken{1, 1}, WireToken{1, 256}}) {
        auto input = Data(token);
        input.type = WireFrameType::kDataWithCredit;
        input.max_credit_seq = 0x0102030405060708;
        const auto encoded = EncodeWireFrame(input, output);
        ASSERT_TRUE(encoded);
        const auto offset = token.generation > 255 ? 8u : 0u;
        for (std::size_t i = 0; i < 8; ++i) {
            EXPECT_EQ(output[offset + i], static_cast<std::byte>(i + 1));
        }
        const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->frame.max_credit_seq, input.max_credit_seq);
        EXPECT_EQ(decoded->frame.data.data(), output.data() + offset + 8);
        EXPECT_EQ(decoded->frame.data.size(), kData.size());
    }
}

TEST(WireFrameTest, DataCapacityAccountsForEveryInBandHeader) {
    std::array<std::byte, kWireMaxPayload + 1> data{};
    std::array<std::byte, kWireMaxPayload + 16> output{};
    for (const auto token : {WireToken{0, 1}, WireToken{0, 256}}) {
        for (const auto type : {WireFrameType::kDataEager, WireFrameType::kDataWithCredit}) {
            auto input = Data(token);
            input.type = type;
            const std::size_t header =
                (token.generation > 255 ? 8 : 0) + (type == WireFrameType::kDataWithCredit ? 8 : 0);
            input.data = std::span(data).first(kWireMaxPayload - header);
            auto encoded = EncodeWireFrame(input, output);
            ASSERT_TRUE(encoded);
            EXPECT_EQ(encoded->payload_bytes, kWireMaxPayload);
            auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(kWireMaxPayload));
            ASSERT_TRUE(decoded);
            EXPECT_EQ(decoded->frame.data.size(), kWireMaxPayload - header);
            input.data = std::span(data).first(kWireMaxPayload - header + 1);
            auto too_large = EncodeWireFrame(input, output);
            ASSERT_FALSE(too_large);
            EXPECT_EQ(too_large.error().code, WireErrorCode::kInvalidLength);
        }
    }
    auto oversized = DecodeWireFrame(0x0001000000000000, data);
    ASSERT_FALSE(oversized);
    EXPECT_EQ(oversized.error().code, WireErrorCode::kInvalidLength);
}

TEST(WireFrameTest, CreditControlGoldenBytesCarryIndependentLimits) {
    std::array<std::byte, 64> output{};
    const auto input = Control(WireControlType::kCreditUpdate);
    auto encoded = EncodeWireFrame(input, output);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->imm_data, 0x2056123400000000ull);
    EXPECT_EQ(encoded->payload_bytes, 48u);
    const std::array<std::uint8_t, 48> bytes{1,    2,    3,    4,    5,    6,    7,    8,    0x11, 0x12, 0x13, 0x14,
                                             0x15, 0x16, 0x17, 0x18, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                                             0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0,    0,    0,    0,
                                             0x41, 0x42, 0x43, 0x44, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58};
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        EXPECT_EQ(output[i], static_cast<std::byte>(bytes[i]));
    }
    auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->frame.session, input.session);
    EXPECT_EQ(decoded->frame.credit_epoch, input.credit_epoch);
    EXPECT_EQ(decoded->frame.max_credit_seq, input.max_credit_seq);
    EXPECT_EQ(decoded->frame.max_control_credit_seq, input.max_control_credit_seq);
    EXPECT_EQ(decoded->frame.max_frame_seq, input.max_frame_seq);
    EXPECT_EQ(decoded->frame.max_stream_bytes, input.max_stream_bytes);
    EXPECT_TRUE(decoded->frame.data.empty());
}

TEST(WireFrameTest, FinAndResetHaveExactBigEndianBodiesForBothIdentityForms) {
    std::array<std::byte, 32> output{};
    for (const auto token : {WireToken{17, 2}, WireToken{17, 256}}) {
        for (const auto control : {WireControlType::kFin, WireControlType::kReset}) {
            auto input = Control(control, token);
            const auto encoded = EncodeWireFrame(input, output);
            ASSERT_TRUE(encoded);
            const std::size_t offset = token.generation > 255 ? 8 : 0;
            EXPECT_EQ(encoded->payload_bytes, offset + (control == WireControlType::kFin ? 8 : 12));
            for (std::size_t i = 0; i < 8; ++i) {
                EXPECT_EQ(output[offset + i], static_cast<std::byte>(i + 1));
            }
            auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
            ASSERT_TRUE(decoded);
            EXPECT_EQ(decoded->frame.control, control);
            EXPECT_EQ(decoded->frame.token, token);
            EXPECT_EQ(decoded->frame.session, input.session);
            EXPECT_EQ(decoded->frame.seq, input.seq);
            EXPECT_EQ(decoded->frame.reset_code, input.reset_code);
        }
    }
}

TEST(WireFrameTest, EmptyTruncatedAndOversizedControlBodiesAreRejected) {
    std::array<std::byte, 64> output{};
    for (const auto token : {WireToken{0, 1}, WireToken{0, 256}}) {
        for (const auto control : {WireControlType::kCreditUpdate, WireControlType::kFin, WireControlType::kReset}) {
            const auto encoded = EncodeWireFrame(Control(control, token), output);
            ASSERT_TRUE(encoded);
            for (std::size_t length = 0; length < encoded->payload_bytes; ++length) {
                const auto truncated = DecodeWireFrame(encoded->imm_data, std::span(output).first(length));
                ASSERT_FALSE(truncated);
                EXPECT_EQ(truncated.error().code, WireErrorCode::kInvalidLength);
            }
            const auto extra = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes + 1));
            ASSERT_FALSE(extra);
            EXPECT_EQ(extra.error().code, WireErrorCode::kInvalidLength);
        }
    }
    for (const auto imm : {0x0001000000000000ull, 0x1001000000000000ull, 0x1800000000000000ull}) {
        EXPECT_FALSE(DecodeWireFrame(imm, {}));
    }
}

TEST(WireFrameTest, FailedEncodingDoesNotWriteCallerOutput) {
    std::array<std::byte, 64> output;
    output.fill(std::byte{0x7f});
    auto too_small = EncodeWireFrame(Control(WireControlType::kCreditUpdate), std::span(output).first(47));
    ASSERT_FALSE(too_small);
    EXPECT_EQ(too_small.error().code, WireErrorCode::kBufferTooSmall);
    auto invalid = Data();
    invalid.data_flags = kWireFlagExtended;
    auto bad_flags = EncodeWireFrame(invalid, output);
    ASSERT_FALSE(bad_flags);
    EXPECT_EQ(bad_flags.error().code, WireErrorCode::kInvalidFlags);
    invalid = Data();
    invalid.data = {};
    EXPECT_FALSE(EncodeWireFrame(invalid, output));
    EXPECT_TRUE(std::all_of(output.begin(), output.end(), [](auto byte) { return byte == std::byte{0x7f}; }));
}

TEST(WireFrameTest, ShortDataHeaderNeedsNoOutputAndKeepsFull4096ByteLease) {
    std::array<std::byte, kWireMaxPayload> data{};
    auto input = Data();
    input.data = data;
    const auto encoded = EncodeWireHeader(input, {});
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->imm_data, 0x03561234789abcdeull);
    EXPECT_EQ(encoded->payload_bytes, kWireMaxPayload);
    EXPECT_EQ(encoded->header_bytes, 0u);
    // 独立数据 SGE 无需复制即可解码，header-only API 没有提供可容纳数据的输出。
    const auto decoded = DecodeWireFrame(encoded->imm_data, data);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->frame.data.data(), data.data());
    EXPECT_EQ(decoded->frame.data.size(), data.size());
}

TEST(WireFrameTest, HeaderOnlyWritesExactHeaderAndMatchesFullEncoding) {
    std::array<std::byte, kWireMaxPayload> data{};
    std::array<std::byte, 32> header{};
    std::array<std::byte, kWireMaxPayload> complete{};
    for (const auto token : {WireToken{1, 1}, WireToken{1, 256}}) {
        for (const auto type : {WireFrameType::kDataEager, WireFrameType::kDataWithCredit}) {
            auto input = Data(token);
            input.type = type;
            const std::size_t header_size =
                (token.generation > 255 ? 8 : 0) + (type == WireFrameType::kDataWithCredit ? 8 : 0);
            input.data = std::span(data).first(kWireMaxPayload - header_size);
            if (type == WireFrameType::kDataWithCredit) {
                input.max_credit_seq = 0x0102030405060708;
            }
            header.fill(std::byte{0xcc});
            auto encoded_header = EncodeWireHeader(input, header);
            ASSERT_TRUE(encoded_header);
            EXPECT_EQ(encoded_header->header_bytes, header_size);
            EXPECT_EQ(encoded_header->payload_bytes, kWireMaxPayload);
            EXPECT_TRUE(std::all_of(header.begin() + header_size, header.end(),
                                    [](auto byte) { return byte == std::byte{0xcc}; }));
            auto encoded_frame = EncodeWireFrame(input, complete);
            ASSERT_TRUE(encoded_frame);
            EXPECT_EQ(encoded_header->imm_data, encoded_frame->imm_data);
            EXPECT_EQ(encoded_header->payload_bytes, encoded_frame->payload_bytes);
            EXPECT_EQ(encoded_header->header_bytes, encoded_frame->header_bytes);
            EXPECT_TRUE(std::equal(header.begin(), header.begin() + header_size, complete.begin()));
            EXPECT_TRUE(std::equal(input.data.begin(), input.data.end(), complete.begin() + header_size));
        }
    }
}

TEST(WireFrameTest, HeaderOnlyControlIncludesEntireBodyAndFailureNeverWritesOutput) {
    std::array<std::byte, 64> header{};
    std::array<std::byte, 64> full{};
    for (const auto token : {WireToken{0, 1}, WireToken{0, 256}}) {
        for (const auto control : {WireControlType::kCreditUpdate, WireControlType::kFin, WireControlType::kReset}) {
            auto input = Control(control, token);
            auto encoded = EncodeWireHeader(input, header);
            auto complete = EncodeWireFrame(input, full);
            ASSERT_TRUE(encoded);
            ASSERT_TRUE(complete);
            EXPECT_EQ(encoded->payload_bytes, encoded->header_bytes);
            EXPECT_EQ(encoded->imm_data, complete->imm_data);
            EXPECT_EQ(encoded->payload_bytes, complete->payload_bytes);
            EXPECT_TRUE(std::equal(header.begin(), header.begin() + encoded->header_bytes, full.begin()));
            header.fill(std::byte{0xdd});
            auto too_small = EncodeWireHeader(input, std::span(header).first(encoded->header_bytes - 1));
            ASSERT_FALSE(too_small);
            EXPECT_EQ(too_small.error().code, WireErrorCode::kBufferTooSmall);
            EXPECT_TRUE(std::all_of(header.begin(), header.end(), [](auto byte) { return byte == std::byte{0xdd}; }));
        }
    }
    // 即使只输出 8B 头，仍必须检查后续独立 SGE 的总长度；不能绕过 4096B 硬上限。
    std::array<std::byte, kWireMaxPayload> data{};
    auto too_large = Data({1, 256});
    too_large.data = data;
    auto rejected = EncodeWireHeader(too_large, header);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, WireErrorCode::kInvalidLength);
    EXPECT_TRUE(std::all_of(header.begin(), header.end(), [](auto byte) { return byte == std::byte{0xdd}; }));
    // 完整帧 API 不能在发现 data 放不下之前已经写掉头部。
    auto data_frame = Data({1, 256});
    auto full_rejected = EncodeWireFrame(data_frame, std::span(header).first(8));
    ASSERT_FALSE(full_rejected);
    EXPECT_EQ(full_rejected.error().code, WireErrorCode::kBufferTooSmall);
    EXPECT_TRUE(std::all_of(header.begin(), header.end(), [](auto byte) { return byte == std::byte{0xdd}; }));
}

TEST(WireFrameTest, UnknownTypesAndReservedRndvCommandsAreUnsupported) {
    std::array<std::byte, 64> output{};
    for (const auto control : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone,
                               static_cast<WireControlType>(6), static_cast<WireControlType>(7)}) {
        auto encoded = EncodeWireFrame(Control(control), output);
        ASSERT_FALSE(encoded);
        EXPECT_EQ(encoded.error().code, WireErrorCode::kUnsupportedType);
        const auto imm = 0x2001000000000000ull | (static_cast<std::uint64_t>(control) << 56);
        auto decoded = DecodeWireFrame(imm, {});
        ASSERT_FALSE(decoded);
        EXPECT_EQ(decoded.error().code, WireErrorCode::kUnsupportedType);
    }
    auto invalid = Data();
    invalid.type = static_cast<WireFrameType>(15);
    auto encoded = EncodeWireFrame(invalid, output);
    ASSERT_FALSE(encoded);
    EXPECT_EQ(encoded.error().code, WireErrorCode::kUnsupportedType);
    auto decoded = DecodeWireFrame(0xf001000000000000ull, {});
    ASSERT_FALSE(decoded);
    EXPECT_EQ(decoded.error().code, WireErrorCode::kUnsupportedType);
}

TEST(WireFrameTest, SequenceBoundaryReservesLastValueForFinAndControlUsesNoDataSequence) {
    std::array<std::byte, 64> output{};
    auto data = Data();
    data.seq = std::numeric_limits<std::uint32_t>::max() - 1;
    EXPECT_TRUE(EncodeWireFrame(data, output));
    ++data.seq;
    auto exhausted = EncodeWireFrame(data, output);
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error().code, WireErrorCode::kInvalidSequence);
    auto fin = Control(WireControlType::kFin);
    fin.seq = std::numeric_limits<std::uint32_t>::max();
    EXPECT_TRUE(EncodeWireFrame(fin, output));
    auto credit = Control(WireControlType::kCreditUpdate);
    credit.seq = 1;
    EXPECT_FALSE(EncodeWireFrame(credit, output));
    credit.seq = 0;
    credit.max_frame_seq = std::uint64_t{1} << 32;
    EXPECT_TRUE(EncodeWireFrame(credit, output));
    ++credit.max_frame_seq;
    EXPECT_FALSE(EncodeWireFrame(credit, output));
}

TEST(WireFrameTest, UnusedFieldsAndInvalidControlIdentityAreRejected) {
    std::array<std::byte, 64> output{};
    auto data = Data();
    data.max_credit_seq = 1;
    EXPECT_FALSE(EncodeWireFrame(data, output));
    data = Data();
    data.session = 1;
    EXPECT_FALSE(EncodeWireFrame(data, output));
    data = Data();
    data.max_control_credit_seq = 1;
    EXPECT_FALSE(EncodeWireFrame(data, output));
    auto credit = Control(WireControlType::kCreditUpdate);
    credit.credit_epoch = 0;
    EXPECT_FALSE(EncodeWireFrame(credit, output));
    credit = Control(WireControlType::kCreditUpdate);
    credit.session = 0;
    EXPECT_FALSE(EncodeWireFrame(credit, output));
    credit = Control(WireControlType::kCreditUpdate);
    credit.data_flags = kWireFlagFirst;
    EXPECT_FALSE(EncodeWireFrame(credit, output));
    auto reset = Control(WireControlType::kReset);
    reset.reset_code = 0;
    EXPECT_FALSE(EncodeWireFrame(reset, output));
    reset = Control(WireControlType::kReset);
    reset.seq = 1;
    EXPECT_FALSE(EncodeWireFrame(reset, output));
    auto fin = Control(WireControlType::kFin);
    fin.max_control_credit_seq = 1;
    EXPECT_FALSE(EncodeWireFrame(fin, output));
}

TEST(WireFrameTest, BindingRejectsWrongSourceDomainSessionVersionAndConnectionGeneration) {
    WireSessionBinding binding{{kWireVersion, 11, 22, 33}, {7, 256}, 44};
    WireFrameView view{Data(binding.token), true};
    EXPECT_TRUE(ValidateWireIdentity(view, binding, binding.context));
    auto observed = binding.context;
    for (auto* field : {&observed.peer_runtime, &observed.receive_domain, &observed.session}) {
        ++*field;
        auto rejected = ValidateWireIdentity(view, binding, observed);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code, WireErrorCode::kIdentityMismatch);
        --*field;
    }
    observed.version = 2;
    auto wrong_version = ValidateWireIdentity(view, binding, observed);
    ASSERT_FALSE(wrong_version);
    EXPECT_EQ(wrong_version.error().code, WireErrorCode::kVersionMismatch);
    ++view.frame.token.generation;
    EXPECT_FALSE(ValidateWireIdentity(view, binding, binding.context));
    --view.frame.token.generation;
    ++view.frame.token.slot;
    EXPECT_FALSE(ValidateWireIdentity(view, binding, binding.context));
    --view.frame.token.slot;
    auto missing_binding = binding;
    missing_binding.credit_epoch = 0;
    EXPECT_FALSE(ValidateWireIdentity(view, missing_binding, binding.context));
}

TEST(WireFrameTest, ControlBindingChecksBodySessionAndCreditEpoch) {
    const WireSessionBinding binding{{kWireVersion, 11, 22, 33}, {7, 1}, 44};
    WireFrameView view{Control(WireControlType::kCreditUpdate, binding.token), false};
    view.frame.session = binding.context.session;
    view.frame.credit_epoch = binding.credit_epoch;
    ASSERT_TRUE(ValidateWireIdentity(view, binding, binding.context));
    ++view.frame.session;
    EXPECT_FALSE(ValidateWireIdentity(view, binding, binding.context));
    --view.frame.session;
    ++view.frame.credit_epoch;
    EXPECT_FALSE(ValidateWireIdentity(view, binding, binding.context));
    for (const auto control : {WireControlType::kFin, WireControlType::kReset}) {
        view.frame = Control(control, binding.token);
        view.frame.session = binding.context.session;
        EXPECT_TRUE(ValidateWireIdentity(view, binding, binding.context));
        ++view.frame.session;
        EXPECT_FALSE(ValidateWireIdentity(view, binding, binding.context));
    }
}

TEST(WireFrameTest, EncodeDecodeAndIdentityValidationDoNotAllocate) {
    const WireSessionBinding binding{{kWireVersion, 11, 22, 33}, {0x12345, 0x12345}, 44};
    auto input = Control(WireControlType::kCreditUpdate, binding.token);
    input.session = binding.context.session;
    input.credit_epoch = binding.credit_epoch;
    std::array<std::byte, 64> output{};
    bool valid = true;
    allocation_count = 0;
    count_allocations = true;
    for (std::size_t i = 0; i < 100; ++i) {
        auto header = EncodeWireHeader(input, output);
        if (!header) {
            valid = false;
            break;
        }
        auto encoded = EncodeWireFrame(input, output);
        if (!encoded) {
            valid = false;
            break;
        }
        auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
        if (!decoded || !ValidateWireIdentity(*decoded, binding, binding.context)) {
            valid = false;
            break;
        }
    }
    count_allocations = false;
    EXPECT_TRUE(valid);
    EXPECT_EQ(allocation_count, 0u);
}

} // namespace
} // namespace connection
} // namespace kbsocket
