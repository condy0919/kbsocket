// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_TX_LANE_SNAPSHOT_HPP_
#define KBSOCKET_CONTROL_TX_LANE_SNAPSHOT_HPP_

#include <expected>

#include "kbsocket/control/handshake_channel.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"

namespace kbsocket {
namespace control {

/// 控制路径提取完整的主动发送 Lane 快照；固定被动接收端点不进入来源清单。
/// 仅接纳全部 Lane 为 READY 的池；调用方须在同一 owner 下绑定 HELLO 与后续发送。
std::expected<HandshakeTxLanes, HandshakeError> CaptureTxLanes(const raw::JettyPool& pool) noexcept;

/// 在发布 READY / 提交 TX 前校验 HELLO 仍绑定当前池、被动端点及每个主动 Lane 的物理身份和代次。
/// 已隔离但未替换的 Lane 不使快照失效，发送方仍由池禁止选择该 Lane；替换后须重新交换身份。
/// O(Lane 数量)、零分配、不调用 provider；不将本地快照校验当作远端静默证明。
std::expected<void, HandshakeError> ValidateLocalTxLanes(const raw::JettyPool& pool,
                                                         const HandshakeHello& hello) noexcept;

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_TX_LANE_SNAPSHOT_HPP_
