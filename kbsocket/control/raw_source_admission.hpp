// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_RAW_SOURCE_ADMISSION_HPP_
#define KBSOCKET_CONTROL_RAW_SOURCE_ADMISSION_HPP_

#include <cstdint>
#include <expected>

#include "kbsocket/connection/connection_tokens.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/control/handshake_channel.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
class RxBufferPool;
}
namespace control {

enum class RawSourceAdmissionError : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kUnauthorized,
    kRegistry,
    kRetained
};
struct RawSourceAdmissionRequest {
    const HandshakeHello* local = nullptr;
    const HandshakeHello* peer = nullptr;
    urma_context_t* context = nullptr;
    raw::RxBufferPool* buffers = nullptr;
    std::uint64_t context_incarnation = 0;
    connection::TokenDomain local_domain;
    connection::CreditPeerHandle credit_peer;
    // 宿主可信控制路径对整份发送 Lane 快照的授权结果；HELLO 自报身份本身不是认证。
    bool peer_lanes_authorized = false;
};

/// READY 发布前原子安装远端主动 Lane 来源；连接关闭不等于远端静默，不能自动撤销来源。
/// 与 Connection 同 owner，所有依赖须活到连接与迟到 RX 的责任分别完成。
class RawSourceAdmission {
public:
    virtual ~RawSourceAdmission() = default;
    virtual std::expected<void, RawSourceAdmissionError> Prepare(const RawSourceAdmissionRequest& request) noexcept = 0;
    virtual std::expected<void, RawSourceAdmissionError> Check() const noexcept = 0;
};

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_RAW_SOURCE_ADMISSION_HPP_
