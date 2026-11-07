// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_LANE_CONTROL_HPP_
#define KBSOCKET_CONTROL_LANE_CONTROL_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>

#include "kbsocket/control/handshake_channel.hpp"
#include "kbsocket/control/raw_source_admission.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"

namespace kbsocket {
namespace control {

using LaneUpdateAuthorizer = bool (*)(void*, const HandshakeIdentity&, const HandshakeTxLanes&) noexcept;
struct LaneControlConfig {
    const HandshakeHello* local = nullptr;
    const HandshakeHello* peer = nullptr;
    const raw::JettyPool* pool = nullptr;
    RawSourceAdmission* sources = nullptr;
    LaneUpdateAuthorizer authorize_update = nullptr;
    void* authorization_cookie = nullptr;
    std::uint64_t timeout_ns = 5'000'000'000;
};
enum class LaneControlErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kProtocol,
    kChannel,
    kSource,
    kLane,
    kTimeout,
    kUnauthorized
};
struct LaneControlError {
    LaneControlErrorCode code;
    int detail = 0;
    int system_error = 0;
};
struct LaneControlProgress {
    std::size_t processed = 0;
    bool tx_allowed = false;
};

/// 双方 READY 后接管专用控制 fd；ACK 前禁止使用新 Lane，旧来源仍保留至独立静默证明。
/// 与 Connection 同 owner，借用 HELLO、池和准入器至 Close；Progress 无分配且不调用 provider。
/// 最多一份本地在途更新及一份待发远端 ACK；不重发业务数据，也不替代已失败连接重连。
class LaneControl {
public:
    std::expected<void, LaneControlError> Open(int fd, const LaneControlConfig& config, std::uint64_t now_ns) noexcept;
    std::expected<LaneControlProgress, LaneControlError> Progress(std::uint64_t now_ns) noexcept;
    void NotifyReadable() noexcept {
        readable_ = true;
    }
    void Cancel() noexcept;
    std::expected<void, LaneControlError> Close() noexcept;
    int fd() const noexcept {
        return channel_.fd();
    }
    bool output_pending() const noexcept;
    bool pending() const noexcept;
    std::uint64_t local_update_id() const noexcept {
        return local_update_id_;
    }
    std::uint64_t peer_update_id() const noexcept {
        return peer_update_id_;
    }

private:
    enum class Output : std::uint8_t { kNone, kUpdate, kAck };
    LaneControlError Fail(LaneControlError error) noexcept;
    std::expected<void, LaneControlError> ObserveLocal(std::uint64_t now_ns) noexcept;
    std::expected<void, LaneControlError> QueueOutput() noexcept;
    std::expected<void, LaneControlError> Handle(const HandshakeMessage&, std::uint64_t now_ns) noexcept;
    void ObserveFlushed() noexcept;
    bool TimedOut(std::uint64_t now_ns) const noexcept;
    HandshakeChannel channel_;
    LaneControlConfig config_;
    urma_context_t* context_ = nullptr;
    HandshakeTxLanes local_confirmed_, local_offered_, peer_current_;
    std::uint64_t local_update_id_ = 0, peer_update_id_ = 0, last_now_ = 0;
    std::optional<std::uint64_t> local_since_, ack_since_, input_since_;
    std::optional<LaneControlError> failure_;
    Output output_ = Output::kNone;
    bool opened_once_ = false, readable_ = true;
    bool local_pending_ = false, local_queued_ = false, local_sent_ = false, peer_ack_pending_ = false;
};

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_LANE_CONTROL_HPP_
