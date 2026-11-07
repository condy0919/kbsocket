// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_HANDSHAKE_RX_SOURCES_HPP_
#define KBSOCKET_RUNTIME_HANDSHAKE_RX_SOURCES_HPP_

#include <array>
#include <cstddef>
#include <expected>

#include "kbsocket/control/raw_source_admission.hpp"
#include "kbsocket/runtime/rx_source_registry.hpp"

namespace kbsocket {
namespace runtime {

/// HELLO 主动 Lane 快照到共享 RX 来源表的有界适配器；Prepare 后不持有 HELLO 指针。
/// 不在析构或 Close 中推断远端已静默，Retire 之前迟到帧仍按原 token/session/Peer 结账。
class HandshakeRxSources final : public control::RawSourceAdmission {
public:
    HandshakeRxSources() = default;
    HandshakeRxSources(const HandshakeRxSources&) = delete;
    HandshakeRxSources& operator=(const HandshakeRxSources&) = delete;
    std::expected<void, control::RawSourceAdmissionError> Open(RxSourceRegistry& registry) noexcept;
    std::expected<void, control::RawSourceAdmissionError>
    Prepare(const control::RawSourceAdmissionRequest& request) noexcept override;
    std::expected<void, control::RawSourceAdmissionError> Check() const noexcept override;
    std::expected<void, control::RawSourceAdmissionError> Retire(const RxSourceRetireProof& proof) noexcept;
    std::expected<void, control::RawSourceAdmissionError> Close() noexcept;
    std::size_t size() const noexcept {
        return count_;
    }

private:
    RxSourceRegistry* registry_ = nullptr;
    raw::RxBufferPool* buffers_ = nullptr;
    urma_context_t* context_ = nullptr;
    connection::CreditPeerHandle credit_peer_;
    connection::CreditIdentity credit_identity_;
    std::array<RxSourceLease, control::kHandshakeMaxTxLanes> leases_{};
    std::uint64_t context_incarnation_ = 0;
    std::size_t count_ = 0;
    bool opened_once_ = false, prepared_once_ = false;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_HANDSHAKE_RX_SOURCES_HPP_
