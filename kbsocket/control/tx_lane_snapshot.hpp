// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_TX_LANE_SNAPSHOT_HPP_
#define KBSOCKET_CONTROL_TX_LANE_SNAPSHOT_HPP_

#include <expected>

#include "kbsocket/control/handshake_channel.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"

namespace kbsocket {
namespace control {

/// 控制路径提取完整的主动发送 Lane 快照；固定被动接收端点不进入来源清单。
/// 初始 HELLO 要求全部 READY；在线公告可保留已隔离但身份未变的 Lane，仍拒绝替换中的空槽。
std::expected<HandshakeTxLanes, HandshakeError> CaptureTxLanes(const raw::JettyPool& pool,
                                                               bool require_ready = true) noexcept;

/// 在发布 READY / 提交 TX 前校验 HELLO 仍绑定当前池、被动端点及每个主动 Lane 的物理身份和代次。
/// 已隔离但未替换的 Lane 不使快照失效，发送方仍由池禁止选择该 Lane；替换后须重新交换身份。
/// O(Lane 数量)、零分配、不调用 provider；不将本地快照校验当作远端静默证明。
std::expected<void, HandshakeError> ValidateLocalTxLanes(const raw::JettyPool& pool,
                                                         const HandshakeHello& hello) noexcept;
std::expected<void, HandshakeError> ValidateLocalTxLanes(const raw::JettyPool& pool, const HandshakeHello& hello,
                                                         const HandshakeTxLanes& snapshot) noexcept;

/// 在线更新仅替换同池、同槽位的 Lane；至少一个代次递增，同代物理身份不得改变。
std::expected<void, HandshakeError> ValidateTxLaneUpdate(const HandshakeTxLanes& before,
                                                         const HandshakeTxLanes& after) noexcept;

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_TX_LANE_SNAPSHOT_HPP_
