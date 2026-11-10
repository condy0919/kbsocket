// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_RNDV_CAPABILITIES_HPP_
#define KBSOCKET_CONTROL_RNDV_CAPABILITIES_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

namespace kbsocket {
namespace control {
inline constexpr std::uint16_t kRndvVersion = 1;
inline constexpr std::uint16_t kRndvSuccessfulWriteDone = 1;
inline constexpr std::uint64_t kRndvMaxWriteBytes = 200u * 1024u * 1024u;
// 当前单个 WRITE 引擎最多追踪 4096 个分块；协商完整事务必须能由单个引擎表达。
inline constexpr std::uint32_t kRndvMaxChunks = 4096;
inline constexpr std::size_t kRndvMaxArenas = 8;
inline constexpr std::size_t kRndvLimitsBytes = 40;
inline constexpr std::size_t kRndvArenaBytes = 72;
inline constexpr std::size_t kRndvOfferPrefixBytes = kRndvLimitsBytes + 8;
inline constexpr std::size_t kRndvMaxOfferBytes = kRndvOfferPrefixBytes + kRndvMaxArenas * kRndvArenaBytes;
inline constexpr std::uint8_t kRndvAccessRead = 2;
inline constexpr std::uint8_t kRndvAccessWrite = 4;
inline constexpr std::uint8_t kRndvAccessAtomic = 8;
inline constexpr std::uint8_t kRndvTokenNone = 0;
inline constexpr std::uint8_t kRndvTokenPlainText = 1;

/// 默认全零表示未声明能力；只有完整成功 WRITE 且禁止后续写入后才允许发送 DONE。
/// 这是已认证对端的协议承诺，不由能力位本身证明身份、完成或远端静默。
struct RndvLimits {
    std::uint16_t version = 0;
    std::uint16_t flags = 0;
    std::uint64_t max_write_bytes = 0;
    std::uint64_t max_transfer_bytes = 0;
    std::uint32_t max_grants = 0;
    std::uint32_t window = 0;
    std::uint64_t max_bytes = 0;
    bool operator==(const RndvLimits&) const = default;
};
/// 规范字段表示固定注册段，不发送 provider 结构体布局、padding 或本地对象指针。
/// 目录不是逐消息写授权；每次 WRITE 仍须校验当前 Grant 的范围、身份和代次。
struct RndvArenaDescriptor {
    std::uint64_t id = 0;
    std::uint64_t generation = 0;
    std::uint64_t address = 0;
    std::uint64_t length = 0;
    std::uint64_t max_grant_bytes = 0;
    std::array<std::byte, 16> eid{};
    std::uint32_t uasid = 0;
    std::uint32_t token_id = 0;
    std::uint32_t token = 0;
    std::uint8_t access = 0;
    std::uint8_t token_policy = 0;
    bool operator==(const RndvArenaDescriptor&) const = default;
};
struct RndvOffer {
    RndvLimits limits;
    std::uint32_t arena_count = 0;
    std::array<RndvArenaDescriptor, kRndvMaxArenas> arenas{};
    bool operator==(const RndvOffer&) const = default;
};
enum class RndvCapabilityErrorCode : std::uint8_t {
    kInvalidArgument,
    kInvalidFrame,
    kVersionMismatch,
    kBufferTooSmall
};
struct RndvCapabilityError {
    RndvCapabilityErrorCode code;
};

/// 校验启用态能力/目录；全零未启用状态由上层省略，不编码为另一种合法后缀。
std::expected<void, RndvCapabilityError> ValidateRndvLimits(const RndvLimits& limits) noexcept;
std::expected<void, RndvCapabilityError> ValidateRndvArena(const RndvArenaDescriptor& arena) noexcept;
std::expected<void, RndvCapabilityError> ValidateRndvOffer(const RndvOffer& offer) noexcept;
/// 编码失败不修改 output；解码要求精确长度、全部保留位为零，所有路径均无分配。
std::expected<std::size_t, RndvCapabilityError> EncodeRndvLimits(const RndvLimits& limits,
                                                                 std::span<std::byte> output) noexcept;
std::expected<RndvLimits, RndvCapabilityError> DecodeRndvLimits(std::span<const std::byte> input) noexcept;
std::expected<std::size_t, RndvCapabilityError> EncodeRndvOffer(const RndvOffer& offer,
                                                                std::span<std::byte> output) noexcept;
std::expected<RndvOffer, RndvCapabilityError> DecodeRndvOffer(std::span<const std::byte> input) noexcept;
} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_RNDV_CAPABILITIES_HPP_
