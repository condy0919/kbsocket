// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_WIRE_FRAME_HPP_
#define KBSOCKET_CONNECTION_WIRE_FRAME_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

namespace kbsocket {
namespace connection {

inline constexpr std::size_t kWireMaxPayload = 4096;
inline constexpr std::uint16_t kWireVersion = 1;
inline constexpr std::uint8_t kWireFlagFirst = 1;
inline constexpr std::uint8_t kWireFlagLast = 2;
inline constexpr std::uint8_t kWireFlagUrgent = 4;
inline constexpr std::uint8_t kWireFlagExtended = 8;

struct WireToken {
    std::uint32_t slot = 0;
    std::uint32_t generation = 0;
    bool operator==(const WireToken&) const = default;
};
enum class WireFrameType : std::uint8_t { kDataEager = 0, kDataWithCredit = 1, kControl = 2 };
enum class WireControlType : std::uint8_t {
    kCreditUpdate = 0,
    kRndvRts = 1,
    kRndvGrant = 2,
    kRndvDone = 3,
    kFin = 4,
    kReset = 5
};
enum class WireErrorCode : std::uint8_t {
    kInvalidArgument,
    kInvalidIdentity,
    kInvalidFlags,
    kInvalidLength,
    kInvalidSequence,
    kInvalidControl,
    kUnsupportedType,
    kBufferTooSmall,
    kIdentityMismatch,
    kVersionMismatch
};
struct WireError {
    WireErrorCode code;
};

/// 原始 WRITE 事务身份；session/token 是目标接收连接身份，与当前帧路由身份独立。
/// GRANT 反向返回时不能把接收回复的 token 写入这里；Arena 描述由可信控制目录另行绑定。
/// 这些值只是协议字段，不构成 DMA 完成或远端 Grant 终结证明。
struct WireRndvFields {
    std::uint64_t sender_runtime = 0;
    std::uint64_t receiver_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t session = 0;
    WireToken token;
    std::uint64_t transfer_id = 0;
    std::uint64_t bytes = 0;
    std::uint64_t arena_id = 0;
    std::uint64_t arena_generation = 0;
    std::uint32_t grant_slot = 0;
    std::uint64_t grant_generation = 0;
    std::uint64_t target_address = 0;
    bool operator==(const WireRndvFields&) const = default;
};

/// 只含值与借用视图；编码不接管数据。data_flags 仅用于两类数据帧，不含扩展身份位。
/// DATA 使用 seq/data，捎带帧另用 max_credit_seq；CREDIT 使用 session/epoch/四个累计上界；
/// FIN 使用 seq/session，RST 使用 session/reset_code；RNDV 使用 seq/session/rndv。
/// 未使用的数值字段必须为零。
struct WireFrameInput {
    WireFrameType type = WireFrameType::kDataEager;
    WireControlType control = WireControlType::kCreditUpdate;
    WireToken token;
    std::uint8_t data_flags = 0;
    std::uint32_t seq = 0;
    std::span<const std::byte> data;
    std::uint64_t max_credit_seq = 0;
    std::uint64_t max_control_credit_seq = 0;
    std::uint64_t session = 0;
    std::uint64_t credit_epoch = 0;
    std::uint64_t max_frame_seq = 0;
    std::uint64_t max_stream_bytes = 0;
    std::uint32_t reset_code = 0;
    WireRndvFields rndv{};
};
struct WireEncodedFrame {
    // URMA imm_data 是数值，不额外 htonll；payload 内所有多字节字段使用大端。
    std::uint64_t imm_data = 0;
    std::size_t payload_bytes = 0;
    // 协议头在 payload 中占用的字节；纯控制帧等于 payload_bytes，short DATA 为零。
    std::size_t header_bytes = 0;
};
struct WireFrameView {
    WireFrameInput frame;
    bool extended_identity = false;
};

/// 完整路由事实来自已认证/校验的接收来源和握手；不能由短 IMM 或扩展 8B 推断。
struct WireReceiveContext {
    std::uint16_t version = kWireVersion;
    std::uint64_t peer_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t session = 0;
    bool operator==(const WireReceiveContext&) const = default;
};
struct WireSessionBinding {
    WireReceiveContext context;
    WireToken token;
    // 捎带帧继承此 epoch；换 epoch 必须建立新 Session，不能沿用旧短 token 的绑定。
    std::uint64_t credit_epoch = 0;
};

/// 固定格式，无分配。输出空间不足/输入非法时不写 output；输出不能与 input.data 重叠。
/// IMM: type[63:60], flags[59:56], gen[55:48], slot[47:32], seq[31:0]。
/// 扩展身份为 BE slot32/gen32，IMM 的 gen/slot 必须为零，禁止两种身份解释。
/// CREDIT body=BE session64/epoch64/max_credit64/max_control64/max_frame64/max_bytes64；
/// FIN body=BE session64；RST body=BE session64/reason32。
/// RNDV 公共 body=BE route_session64/sender_runtime64/receiver_runtime64/receive_domain64/
/// transfer_session64/transfer_token.slot32/gen32/transfer_id64/bytes64，RTS 恰好 64B；SN 来自 IMM。
/// GRANT/DONE 再追加 arena_id64/arena_generation64/grant_slot32/zero32/grant_generation64/
/// target_address64，恰好 104B；不直接发送原生 MR 结构或将任意网络计数视作完成证明。
std::expected<WireEncodedFrame, WireError> EncodeWireFrame(const WireFrameInput& input,
                                                           std::span<std::byte> output) noexcept;
/// 仅写协议头，不读取/复制 input.data；宿主可用独立注册 SGE 拼接 header 和原 Lease。
/// output 只需容纳 header_bytes，short DATA 可传空区间；仍校验协议头+数据总长不超过 4096。
/// 头部输出区间不得覆盖 input.data；纯控制帧写完整控制 body；失败不写 output。
/// 函数不注册或延长任何 SGE/Lease 生命周期。
std::expected<WireEncodedFrame, WireError> EncodeWireHeader(const WireFrameInput& input,
                                                            std::span<std::byte> output) noexcept;
/// payload 必须恰好是 CQE completion_len 的区间；返回 data 借用 payload 生命周期。
std::expected<WireFrameView, WireError> DecodeWireFrame(std::uint64_t imm_data,
                                                        std::span<const std::byte> payload) noexcept;
/// 仅验证当前方向的帧路由；RNDV 的原事务 binding/Arena/Grant 仍须与本地账本核对。
/// 解码成功或路由匹配均不证明 WRITE 已完成，也不授权调用 Grant 终结接口。
std::expected<void, WireError> ValidateWireIdentity(const WireFrameView& decoded, const WireSessionBinding& binding,
                                                    const WireReceiveContext& observed) noexcept;

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_WIRE_FRAME_HPP_
