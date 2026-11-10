// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/rndv_capabilities.hpp"

#include <algorithm>
#include <limits>

namespace kbsocket {
namespace control {
namespace {
auto Error(RndvCapabilityErrorCode code) noexcept {
    return std::unexpected(RndvCapabilityError{code});
}
class Writer {
public:
    explicit Writer(std::span<std::byte> bytes) noexcept : bytes_(bytes) {}
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
    explicit Reader(std::span<const std::byte> bytes) noexcept : bytes_(bytes) {}
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
void PutLimits(Writer& out, const RndvLimits& limits) noexcept {
    out.Put(limits.version, 2);
    out.Put(limits.flags, 2);
    out.Put(limits.max_write_bytes, 8);
    out.Put(limits.max_transfer_bytes, 8);
    out.Put(limits.max_grants, 4);
    out.Put(limits.window, 4);
    out.Put(limits.max_bytes, 8);
    out.Put(0, 4);
}
std::expected<RndvLimits, RndvCapabilityError> GetLimits(Reader& in) noexcept {
    RndvLimits result;
    result.version = static_cast<std::uint16_t>(in.Get(2));
    result.flags = static_cast<std::uint16_t>(in.Get(2));
    result.max_write_bytes = in.Get(8);
    result.max_transfer_bytes = in.Get(8);
    result.max_grants = static_cast<std::uint32_t>(in.Get(4));
    result.window = static_cast<std::uint32_t>(in.Get(4));
    result.max_bytes = in.Get(8);
    if (in.Get(4) != 0) {
        return Error(RndvCapabilityErrorCode::kInvalidFrame);
    }
    if (const auto valid = ValidateRndvLimits(result); !valid) {
        return std::unexpected(valid.error());
    }
    return result;
}
} // namespace

std::expected<void, RndvCapabilityError> ValidateRndvLimits(const RndvLimits& limits) noexcept {
    if (limits.version != kRndvVersion) {
        return Error(RndvCapabilityErrorCode::kVersionMismatch);
    }
    if (limits.flags != kRndvSuccessfulWriteDone || !limits.max_write_bytes ||
        limits.max_write_bytes > kRndvMaxWriteBytes || !limits.max_transfer_bytes ||
        limits.max_transfer_bytes > std::numeric_limits<std::uint32_t>::max() ||
        limits.max_transfer_bytes > limits.max_write_bytes * kRndvMaxChunks ||
        limits.max_transfer_bytes > limits.max_bytes || !limits.max_grants || limits.max_grants > 128 ||
        limits.max_grants > limits.window || limits.window > 4096) {
        return Error(RndvCapabilityErrorCode::kInvalidArgument);
    }
    return {};
}
std::expected<void, RndvCapabilityError> ValidateRndvArena(const RndvArenaDescriptor& arena) noexcept {
    constexpr auto access = kRndvAccessRead | kRndvAccessWrite | kRndvAccessAtomic;
    if (!arena.id || !arena.generation || !arena.address || !arena.length ||
        arena.length > std::numeric_limits<std::uint64_t>::max() - arena.address || !arena.max_grant_bytes ||
        arena.max_grant_bytes > std::numeric_limits<std::uint32_t>::max() || arena.max_grant_bytes > arena.length ||
        arena.eid == std::array<std::byte, 16>{} || arena.uasid >= (1u << 24) ||
        arena.token_id == std::numeric_limits<std::uint32_t>::max() || !(arena.access & kRndvAccessWrite) ||
        (arena.access & ~access) || arena.token_policy > kRndvTokenPlainText ||
        (arena.token_policy == kRndvTokenNone && arena.token)) {
        return Error(RndvCapabilityErrorCode::kInvalidArgument);
    }
    return {};
}
std::expected<void, RndvCapabilityError> ValidateRndvOffer(const RndvOffer& offer) noexcept {
    if (const auto valid = ValidateRndvLimits(offer.limits); !valid) {
        return valid;
    }
    if (!offer.arena_count || offer.arena_count > kRndvMaxArenas) {
        return Error(RndvCapabilityErrorCode::kInvalidArgument);
    }
    std::uint64_t total = 0;
    for (std::uint32_t i = 0; i < offer.arena_count; ++i) {
        const auto& arena = offer.arenas[i];
        if (const auto valid = ValidateRndvArena(arena); !valid) {
            return valid;
        }
        if (arena.max_grant_bytes < offer.limits.max_transfer_bytes) {
            return Error(RndvCapabilityErrorCode::kInvalidArgument);
        }
        // 总 backing 容量仅作上界校验；饱和加法避免两个合法大范围相加发生回绕。
        total += std::min(arena.length, std::numeric_limits<std::uint64_t>::max() - total);
        for (std::uint32_t j = 0; j < i; ++j) {
            const auto& previous = offer.arenas[j];
            if (arena.id == previous.id || (arena.eid == previous.eid && arena.uasid == previous.uasid &&
                                            arena.address < previous.address + previous.length &&
                                            previous.address < arena.address + arena.length)) {
                return Error(RndvCapabilityErrorCode::kInvalidArgument);
            }
        }
    }
    if (offer.limits.max_bytes > total) {
        return Error(RndvCapabilityErrorCode::kInvalidArgument);
    }
    return {};
}
std::expected<std::size_t, RndvCapabilityError> EncodeRndvLimits(const RndvLimits& limits,
                                                                 std::span<std::byte> output) noexcept {
    if (const auto valid = ValidateRndvLimits(limits); !valid) {
        return std::unexpected(valid.error());
    }
    if (output.size() < kRndvLimitsBytes) {
        return Error(RndvCapabilityErrorCode::kBufferTooSmall);
    }
    Writer out(output);
    PutLimits(out, limits);
    return kRndvLimitsBytes;
}
std::expected<RndvLimits, RndvCapabilityError> DecodeRndvLimits(std::span<const std::byte> input) noexcept {
    if (input.size() != kRndvLimitsBytes) {
        return Error(RndvCapabilityErrorCode::kInvalidFrame);
    }
    Reader in(input);
    return GetLimits(in);
}
std::expected<std::size_t, RndvCapabilityError> EncodeRndvOffer(const RndvOffer& offer,
                                                                std::span<std::byte> output) noexcept {
    if (const auto valid = ValidateRndvOffer(offer); !valid) {
        return std::unexpected(valid.error());
    }
    const auto size = kRndvOfferPrefixBytes + kRndvArenaBytes * offer.arena_count;
    if (output.size() < size) {
        return Error(RndvCapabilityErrorCode::kBufferTooSmall);
    }
    Writer out(output);
    PutLimits(out, offer.limits);
    out.Put(offer.arena_count, 4);
    out.Put(0, 4);
    for (std::uint32_t i = 0; i < offer.arena_count; ++i) {
        const auto& arena = offer.arenas[i];
        out.Put(arena.id, 8);
        out.Put(arena.generation, 8);
        out.Put(arena.address, 8);
        out.Put(arena.length, 8);
        out.Put(arena.max_grant_bytes, 8);
        out.Copy(arena.eid);
        out.Put(arena.uasid, 4);
        out.Put(arena.token_id, 4);
        out.Put(arena.token, 4);
        out.Put(arena.access, 1);
        out.Put(arena.token_policy, 1);
        out.Put(0, 2);
    }
    return size;
}
std::expected<RndvOffer, RndvCapabilityError> DecodeRndvOffer(std::span<const std::byte> input) noexcept {
    if (input.size() < kRndvOfferPrefixBytes || input.size() > kRndvMaxOfferBytes) {
        return Error(RndvCapabilityErrorCode::kInvalidFrame);
    }
    Reader in(input);
    const auto limits = GetLimits(in);
    if (!limits) {
        return std::unexpected(limits.error());
    }
    RndvOffer result;
    result.limits = *limits;
    result.arena_count = static_cast<std::uint32_t>(in.Get(4));
    // 必须先证明可变列表的边界，才能读取第一条目录记录。
    if (in.Get(4) || !result.arena_count || result.arena_count > kRndvMaxArenas ||
        input.size() != kRndvOfferPrefixBytes + kRndvArenaBytes * result.arena_count) {
        return Error(RndvCapabilityErrorCode::kInvalidFrame);
    }
    for (std::uint32_t i = 0; i < result.arena_count; ++i) {
        auto& arena = result.arenas[i];
        arena.id = in.Get(8);
        arena.generation = in.Get(8);
        arena.address = in.Get(8);
        arena.length = in.Get(8);
        arena.max_grant_bytes = in.Get(8);
        in.Copy(arena.eid);
        arena.uasid = static_cast<std::uint32_t>(in.Get(4));
        arena.token_id = static_cast<std::uint32_t>(in.Get(4));
        arena.token = static_cast<std::uint32_t>(in.Get(4));
        arena.access = static_cast<std::uint8_t>(in.Get(1));
        arena.token_policy = static_cast<std::uint8_t>(in.Get(1));
        if (in.Get(2)) {
            return Error(RndvCapabilityErrorCode::kInvalidFrame);
        }
    }
    if (const auto valid = ValidateRndvOffer(result); !valid) {
        return std::unexpected(valid.error());
    }
    return result;
}
} // namespace control
} // namespace kbsocket
