// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_RNDV_PROTOCOL_HPP_
#define KBSOCKET_CONNECTION_RNDV_PROTOCOL_HPP_

#include <cstdint>
#include <expected>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/connection/write_rndv_pool.hpp"

namespace kbsocket {
namespace connection {

/// 已认证的消息目的路由；GRANT 的回程 session/token 可以不同于原 WRITE 事务。
struct RndvDestination {
    std::uint64_t runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t session = 0;
    WireToken token;
};
enum class RndvProtocolErrorCode : std::uint8_t {
    kInvalidArgument,
    kMalformedFrame,
    kWrongControl,
    kWrongRoute,
    kWrongTransfer,
    kWrongGrant,
    kInvalidEvidence
};
struct RndvProtocolError {
    RndvProtocolErrorCode code;
};

/// 仅构造值对象，不预留 SN、物理授信、Grant 或 SEND 的注册缓冲区，也不提交网络操作。
/// RTS 必须沿已确认的原接收路由发送；发送者先在共同 TxSequenceWindow 预留总长度和一个 SN。
std::expected<WireFrameInput, RndvProtocolError> BuildRndvRts(const GrantTransfer& transfer,
                                                              RndvDestination destination) noexcept;
/// GRANT 仅引用已认证的 Arena 目录；不传输 provider 原生结构或裸本地句柄。
/// 宿主须在任何可能暴露 GRANT 的操作前 CommitPublication，并预留回程控制授信。
std::expected<WireFrameInput, RndvProtocolError> BuildRndvGrant(const GrantIdentity& grant,
                                                                RndvDestination destination) noexcept;
/// 直接读取引擎/事务池的 DoneEvidence，不能用清理证据或网络计数构造成功通知。
/// 即使取消后的全部 WRITE 均成功，也必须由原引擎决定是否仍允许交付 DONE。
/// 仍须预留控制授信并持有 SEND 源责任；构造或发送 DONE 不等于接收端已经消费目标。
std::expected<WireFrameInput, RndvProtocolError> BuildRndvDone(const WriteRndvEngine& engine,
                                                               RndvDestination destination) noexcept;
std::expected<WireFrameInput, RndvProtocolError> BuildRndvDone(const WriteRndvPool& pool, WriteRndvHandle handle,
                                                               RndvDestination destination) noexcept;

/// view 来自 DecodeWireFrame；observed 必须来自可信 RX 来源，不能用预期 route.context 冒充。
/// expected 是本地授权/请求状态。校验失败不改变任何责任账本，RTS 成功后仍须共同 SN/Grant 准入。
std::expected<GrantTransfer, RndvProtocolError> ValidateRndvRts(const WireFrameView& view,
                                                                const WireSessionBinding& route,
                                                                const WireReceiveContext& observed,
                                                                const GrantBinding& expected) noexcept;
std::expected<GrantIdentity, RndvProtocolError>
ValidateRndvGrant(const WireFrameView& view, const WireSessionBinding& route, const WireReceiveContext& observed,
                  const GrantTransfer& expected, GrantArena arena) noexcept;
/// 仅确认 DONE 的来源、事务和授权身份；不生成 GrantTerminalEvidence、不调用 CompleteDone。
/// 网络字节无法独立证明逐 WR 终结和完整覆盖；可信通知状态机还须核验当前发布状态及发送端契约。
std::expected<void, RndvProtocolError> ValidateRndvDone(const WireFrameView& view, const WireSessionBinding& route,
                                                        const WireReceiveContext& observed,
                                                        const GrantIdentity& expected) noexcept;

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_RNDV_PROTOCOL_HPP_
