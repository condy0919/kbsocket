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

WireFrameInput RndvControl(WireControlType type, WireToken route = {0x1234, 0x56}) {
    auto result = Control(type, route);
    result.seq = 0x789abcde;
    result.rndv = {.sender_runtime = 0x1112131415161718,
                   .receiver_runtime = 0x2122232425262728,
                   .receive_domain = 0x3132333435363738,
                   .session = 0x4142434445464748,
                   .token = {0x51525354, 0x55565758},
                   .transfer_id = 0x6162636465666768,
                   .bytes = 0x7172737475767778};
    if (type != WireControlType::kRndvRts) {
        result.rndv.arena_id = 0x8182838485868788;
        result.rndv.arena_generation = 0x9192939495969798;
        result.rndv.grant_slot = 0xa1a2a3a4;
        result.rndv.grant_generation = 0xb1b2b3b4b5b6b7b8;
        result.rndv.target_address = 0x0102030405060708;
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

TEST(WireFrameTest, UnknownTypesAndControlCommandsAreUnsupported) {
    std::array<std::byte, 64> output{};
    for (const auto control : {static_cast<WireControlType>(6), static_cast<WireControlType>(7)}) {
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

TEST(WireFrameTest, RndvGoldenBodiesCarryIndependentRouteAndTransferIdentities) {
    // 原事务和反向 GRANT 的目的路由具有不同 session/token，禁止编码时偷偷折叠身份。
    constexpr std::array<std::uint8_t, 64> common{
        1,    2,    3,    4,    5,    6,    7,    8,    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
        0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58,
        0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78};
    constexpr std::array<std::uint8_t, 40> grant{0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x91, 0x92,
                                                 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0xa1, 0xa2, 0xa3, 0xa4,
                                                 0,    0,    0,    0,    0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6,
                                                 0xb7, 0xb8, 1,    2,    3,    4,    5,    6,    7,    8};
    std::array<std::byte, 128> output;
    for (const auto route : {WireToken{0x1234, 0x56}, WireToken{0x12345678, 0x90abcdef}}) {
        for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
            output.fill(std::byte{0xcc});
            const auto input = RndvControl(type, route);
            const auto encoded = EncodeWireFrame(input, output);
            ASSERT_TRUE(encoded);
            const bool extended = route.generation > 255;
            const std::size_t offset = extended ? 8 : 0;
            EXPECT_EQ(encoded->payload_bytes, offset + (type == WireControlType::kRndvRts ? 64 : 104));
            EXPECT_EQ(encoded->header_bytes, encoded->payload_bytes);
            EXPECT_EQ(encoded->imm_data, (extended ? 0x28000000789abcdeull : 0x20561234789abcdeull) |
                                             (static_cast<std::uint64_t>(type) << 56));
            for (std::size_t i = 0; i < common.size(); ++i) {
                EXPECT_EQ(output[offset + i], static_cast<std::byte>(common[i]));
            }
            if (type != WireControlType::kRndvRts) {
                for (std::size_t i = 0; i < grant.size(); ++i) {
                    EXPECT_EQ(output[offset + common.size() + i], static_cast<std::byte>(grant[i]));
                }
            }
            EXPECT_EQ(output[encoded->payload_bytes], std::byte{0xcc});
            const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
            ASSERT_TRUE(decoded);
            EXPECT_EQ(decoded->frame.token, input.token);
            EXPECT_EQ(decoded->frame.session, input.session);
            EXPECT_EQ(decoded->frame.seq, input.seq);
            EXPECT_EQ(decoded->frame.control, input.control);
            EXPECT_EQ(decoded->frame.rndv, input.rndv);
            EXPECT_TRUE(decoded->frame.data.empty());
        }
    }
}

TEST(WireFrameTest, RndvRejectsEveryTruncationAndTrailingByte) {
    std::array<std::byte, 128> output{};
    for (const auto route : {WireToken{1, 1}, WireToken{1, 256}}) {
        for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
            const auto encoded = EncodeWireFrame(RndvControl(type, route), output);
            ASSERT_TRUE(encoded);
            for (std::size_t bytes = 0; bytes < encoded->payload_bytes; ++bytes) {
                const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(bytes));
                ASSERT_FALSE(decoded);
                EXPECT_EQ(decoded.error().code, WireErrorCode::kInvalidLength);
            }
            EXPECT_FALSE(DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes + 1)));
        }
    }
}

TEST(WireFrameTest, RndvGrantAndDoneRejectReservedBytes) {
    std::array<std::byte, 128> output{};
    for (const auto route : {WireToken{1, 1}, WireToken{1, 256}}) {
        for (const auto type : {WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
            const auto encoded = EncodeWireFrame(RndvControl(type, route), output);
            ASSERT_TRUE(encoded);
            const std::size_t offset = route.generation > 255 ? 8 : 0;
            // 不接收第二种带隐含标志的解释；每个保留字节都必须严格为零。
            for (std::size_t byte = 84; byte < 88; ++byte) {
                output[offset + byte] = std::byte{1};
                const auto decoded =
                    DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
                ASSERT_FALSE(decoded);
                EXPECT_EQ(decoded.error().code, WireErrorCode::kInvalidControl);
                output[offset + byte] = std::byte{0};
            }
        }
    }
}

TEST(WireFrameTest, RndvRequiresBothSessionsAndCompleteTransferIdentity) {
    std::array<std::byte, 128> output{};
    for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        auto input = RndvControl(type);
        for (auto* field : {&input.session, &input.rndv.sender_runtime, &input.rndv.receiver_runtime,
                            &input.rndv.receive_domain, &input.rndv.session, &input.rndv.transfer_id}) {
            const auto saved = *field;
            *field = 0;
            const auto rejected = EncodeWireFrame(input, output);
            ASSERT_FALSE(rejected);
            EXPECT_EQ(rejected.error().code, WireErrorCode::kInvalidIdentity);
            *field = saved;
        }
        input.rndv.token.generation = 0;
        EXPECT_FALSE(EncodeWireFrame(input, output));
        input = RndvControl(type);
        input.token.generation = 0;
        EXPECT_FALSE(EncodeWireFrame(input, output));
        input = RndvControl(type);
        input.rndv.bytes = 0;
        const auto empty = EncodeWireFrame(input, output);
        ASSERT_FALSE(empty);
        EXPECT_EQ(empty.error().code, WireErrorCode::kInvalidLength);
    }
}

TEST(WireFrameTest, RndvGrantIdentityRequiresAllGenerationsAndAddress) {
    std::array<std::byte, 128> output{};
    for (const auto type : {WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        auto input = RndvControl(type);
        for (auto* field : {&input.rndv.arena_id, &input.rndv.arena_generation, &input.rndv.grant_generation,
                            &input.rndv.target_address}) {
            const auto saved = *field;
            *field = 0;
            const auto rejected = EncodeWireFrame(input, output);
            ASSERT_FALSE(rejected);
            EXPECT_EQ(rejected.error().code, WireErrorCode::kInvalidIdentity);
            *field = saved;
        }
        // slot 和 SN 的零值合法；64-bit 代次的高位不可被裁成设计早期的 32-bit 字段。
        input.seq = 0;
        input.rndv.token.slot = 0;
        input.rndv.grant_slot = 0;
        input.rndv.arena_generation = std::numeric_limits<std::uint64_t>::max();
        input.rndv.grant_generation = std::numeric_limits<std::uint64_t>::max();
        const auto encoded = EncodeWireFrame(input, output);
        ASSERT_TRUE(encoded);
        const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->frame.rndv, input.rndv);
    }
}

TEST(WireFrameTest, RndvDecodeValidatesIdentitiesAndAddressOverflow) {
    std::array<std::byte, 128> output{};
    for (const auto type : {WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        const auto input = RndvControl(type);
        const auto encoded = EncodeWireFrame(input, output);
        ASSERT_TRUE(encoded);
        const auto original = output;
        // 注入来自网络的零值，确认解码并非只检长度后就交出缺失身份。
        for (const std::size_t offset : {0, 8, 16, 24, 32, 48, 56, 64, 72, 88, 96}) {
            output = original;
            std::fill_n(output.begin() + offset, 8, std::byte{0});
            EXPECT_FALSE(DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes)));
        }
        output = original;
        std::fill_n(output.begin() + 44, 4, std::byte{0});
        EXPECT_FALSE(DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes)));
        output = original;
        std::fill_n(output.begin() + 96, 8, std::byte{0xff});
        const auto overflow = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
        ASSERT_FALSE(overflow);
        EXPECT_EQ(overflow.error().code, WireErrorCode::kInvalidLength);
    }
}

TEST(WireFrameTest, RndvRangeAllowsExactEndAndRejectsOverflow) {
    std::array<std::byte, 128> output{};
    for (const auto type : {WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        auto input = RndvControl(type);
        input.rndv.bytes = 4096;
        input.rndv.target_address = std::numeric_limits<std::uint64_t>::max() - input.rndv.bytes;
        ASSERT_TRUE(EncodeWireFrame(input, output));
        ++input.rndv.target_address;
        const auto overflow = EncodeWireFrame(input, output);
        ASSERT_FALSE(overflow);
        EXPECT_EQ(overflow.error().code, WireErrorCode::kInvalidLength);
    }
}

TEST(WireFrameTest, RndvNeverConsumesReservedFinSequence) {
    std::array<std::byte, 128> output{};
    for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        auto input = RndvControl(type);
        input.seq = std::numeric_limits<std::uint32_t>::max() - 1;
        const auto encoded = EncodeWireFrame(input, output);
        ASSERT_TRUE(encoded);
        ++input.seq;
        const auto rejected = EncodeWireFrame(input, output);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code, WireErrorCode::kInvalidSequence);
        const auto decoded =
            DecodeWireFrame(encoded->imm_data | 0xffffffffull, std::span(output).first(encoded->payload_bytes));
        ASSERT_FALSE(decoded);
        EXPECT_EQ(decoded.error().code, WireErrorCode::kInvalidSequence);
    }
}

TEST(WireFrameTest, RtsCannotSmuggleGrantFieldsAndOtherFramesCannotCarryRndv) {
    std::array<std::byte, 128> output{};
    auto rts = RndvControl(WireControlType::kRndvRts);
    for (auto* field :
         {&rts.rndv.arena_id, &rts.rndv.arena_generation, &rts.rndv.grant_generation, &rts.rndv.target_address}) {
        *field = 1;
        EXPECT_FALSE(EncodeWireFrame(rts, output));
        *field = 0;
    }
    rts.rndv.grant_slot = 1;
    EXPECT_FALSE(EncodeWireFrame(rts, output));
    for (auto input : {Data(), Control(WireControlType::kCreditUpdate), Control(WireControlType::kFin),
                       Control(WireControlType::kReset)}) {
        input.rndv = RndvControl(WireControlType::kRndvRts).rndv;
        const auto rejected = EncodeWireFrame(input, output);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code, WireErrorCode::kInvalidControl);
    }
}

TEST(WireFrameTest, RndvCannotCarryDataCreditOrResetFields) {
    std::array<std::byte, 128> output{};
    for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        auto input = RndvControl(type);
        for (auto* field : {&input.credit_epoch, &input.max_credit_seq, &input.max_control_credit_seq,
                            &input.max_frame_seq, &input.max_stream_bytes}) {
            *field = 1;
            EXPECT_FALSE(EncodeWireFrame(input, output));
            *field = 0;
        }
        input.reset_code = 1;
        EXPECT_FALSE(EncodeWireFrame(input, output));
        input.reset_code = 0;
        input.data = kData;
        EXPECT_FALSE(EncodeWireFrame(input, output));
        input.data = {};
        input.data_flags = kWireFlagUrgent;
        EXPECT_FALSE(EncodeWireFrame(input, output));
    }
}

TEST(WireFrameTest, RndvHeaderMatchesFullFrameAndFailedEncodingNeverWrites) {
    std::array<std::byte, 128> output{}, header{};
    for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        auto input = RndvControl(type, {1, 256});
        const auto encoded = EncodeWireFrame(input, output);
        const auto header_only = EncodeWireHeader(input, header);
        ASSERT_TRUE(encoded);
        ASSERT_TRUE(header_only);
        EXPECT_EQ(header_only->imm_data, encoded->imm_data);
        EXPECT_EQ(header_only->header_bytes, encoded->payload_bytes);
        EXPECT_EQ(header, output);
        output.fill(std::byte{0xdd});
        EXPECT_FALSE(EncodeWireFrame(input, std::span(output).first(encoded->payload_bytes - 1)));
        EXPECT_FALSE(EncodeWireHeader(input, std::span(output).first(encoded->header_bytes - 1)));
        input.rndv.session = 0;
        EXPECT_FALSE(EncodeWireFrame(input, output));
        EXPECT_FALSE(EncodeWireHeader(input, output));
        EXPECT_TRUE(std::all_of(output.begin(), output.end(), [](auto byte) { return byte == std::byte{0xdd}; }));
        output = {};
        header = {};
    }
}

TEST(WireFrameTest, RndvRoutingValidationUsesCurrentDirectionNotOriginalTransaction) {
    const auto input = RndvControl(WireControlType::kRndvGrant);
    const WireSessionBinding route{{kWireVersion, input.rndv.receiver_runtime, 22, input.session}, input.token, 44};
    // GRANT 的原始事务 session/token 与反向路由不同；普通路由校验只证明当前接收来源。
    const WireFrameView decoded{input, false};
    ASSERT_NE(input.session, input.rndv.session);
    ASSERT_NE(input.token, input.rndv.token);
    EXPECT_TRUE(ValidateWireIdentity(decoded, route, route.context));
    auto incorrect = route;
    incorrect.token = input.rndv.token;
    EXPECT_FALSE(ValidateWireIdentity(decoded, incorrect, route.context));
    incorrect = route;
    incorrect.context.session = input.rndv.session;
    EXPECT_FALSE(ValidateWireIdentity(decoded, incorrect, incorrect.context));
}

TEST(WireFrameTest, RndvEncodingDecodingAndRouteValidationDoNotAllocate) {
    std::array<std::byte, 128> output{};
    bool valid = true;
    allocation_count = 0;
    count_allocations = true;
    for (const auto type : {WireControlType::kRndvRts, WireControlType::kRndvGrant, WireControlType::kRndvDone}) {
        const auto input = RndvControl(type, {65536, 256});
        const WireSessionBinding route{{kWireVersion, 11, 22, input.session}, input.token, 44};
        for (std::size_t i = 0; i < 100; ++i) {
            const auto header = EncodeWireHeader(input, output);
            const auto encoded = EncodeWireFrame(input, output);
            if (!header || !encoded) {
                valid = false;
                break;
            }
            const auto decoded = DecodeWireFrame(encoded->imm_data, std::span(output).first(encoded->payload_bytes));
            if (!decoded || !ValidateWireIdentity(*decoded, route, route.context)) {
                valid = false;
                break;
            }
        }
    }
    count_allocations = false;
    EXPECT_TRUE(valid);
    EXPECT_EQ(allocation_count, 0u);
}

} // namespace
} // namespace connection
} // namespace kbsocket
