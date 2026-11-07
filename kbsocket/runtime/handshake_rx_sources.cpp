// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/handshake_rx_sources.hpp"

#include <cstring>
#include <span>

namespace kbsocket {
namespace runtime {
namespace {
using ErrorCode = control::RawSourceAdmissionError;
auto Error(ErrorCode code) noexcept {
    return std::unexpected(code);
}
bool ValidIdentity(const control::HandshakeIdentity& identity) noexcept {
    return identity.runtime && identity.receive_domain && identity.endpoint_generation && identity.authorization &&
           identity.session && identity.token.generation;
}
} // namespace

std::expected<void, ErrorCode> HandshakeRxSources::Open(RxSourceRegistry& registry) noexcept {
    if (registry_ || opened_once_) {
        return Error(ErrorCode::kInUse);
    }
    if (!registry.buffers() || !registry.context_incarnation()) {
        return Error(ErrorCode::kInvalidArgument);
    }
    registry_ = &registry;
    opened_once_ = true;
    return {};
}

std::expected<void, ErrorCode> HandshakeRxSources::Prepare(const control::RawSourceAdmissionRequest& request) noexcept {
    if (!registry_) {
        return Error(ErrorCode::kNotReady);
    }
    if (prepared_once_) {
        return Error(ErrorCode::kInUse);
    }
    if (!request.peer_lanes_authorized) {
        return Error(ErrorCode::kUnauthorized);
    }
    const auto domain = registry_->local_domain();
    if (!request.local || !request.peer || !request.context || !request.buffers ||
        request.buffers != registry_->buffers() || request.buffers->stopped() || !request.buffers->pool() ||
        request.buffers->pool()->context() != request.context ||
        request.context_incarnation != registry_->context_incarnation() ||
        request.local_domain.runtime_instance != domain.runtime_instance ||
        request.local_domain.receive_domain != domain.receive_domain || !request.credit_peer.domain) {
        return Error(ErrorCode::kInvalidArgument);
    }
    const auto& local = *request.local;
    const auto& peer = *request.peer;
    const auto passive = request.buffers->pool()->rx_target_jetty_id();
    const auto credit = request.credit_peer.domain->PeerStats(request.credit_peer);
    const connection::CreditIdentity identity{peer.identity.runtime, domain.receive_domain, local.credit_epoch};
    if (!ValidIdentity(local.identity) || !ValidIdentity(peer.identity) ||
        local.identity.runtime != domain.runtime_instance || local.identity.receive_domain != domain.receive_domain ||
        local.wire_version != connection::kWireVersion || peer.wire_version != connection::kWireVersion ||
        !(local.capabilities & control::kHandshakeCapabilityRaw) ||
        !(peer.capabilities & control::kHandshakeCapabilityRaw) || !local.credit_epoch || !peer.credit_epoch ||
        !control::ValidateHandshakeTxLanes(local.tx_lanes) || !control::ValidateHandshakeTxLanes(peer.tx_lanes) ||
        !passive || passive->id != local.jetty_id ||
        std::memcmp(&passive->eid, local.eid.data(), local.eid.size()) != 0 || !credit || credit->retired ||
        credit->frozen || request.credit_peer.domain->stats().frozen || credit->identity != identity) {
        return Error(ErrorCode::kInvalidArgument);
    }
    std::array<RxSourceRoute, control::kHandshakeMaxTxLanes> routes{};
    for (std::uint32_t i = 0; i < peer.tx_lanes.count; ++i) {
        const auto& lane = peer.tx_lanes.lanes[i];
        // 被动目标不能伪装成主动发送 Lane；解析器已检查，直接 API 调用也须守住边界。
        if (lane.jetty_id == peer.jetty_id && lane.eid == peer.eid) {
            return Error(ErrorCode::kInvalidArgument);
        }
        auto& route = routes[i];
        route.local_id = local.jetty_id;
        std::memcpy(&route.remote_id.eid, lane.eid.data(), lane.eid.size());
        route.remote_id.uasid = lane.uasid;
        route.remote_id.id = lane.jetty_id;
        route.token = local.identity.token;
        route.source = {
            request.local_domain,
            {local.wire_version, peer.identity.runtime, local.identity.receive_domain, local.identity.session},
            request.credit_peer};
    }
    const auto added = registry_->RegisterBatch(std::span(routes).first(peer.tx_lanes.count),
                                                std::span(leases_).first(peer.tx_lanes.count));
    if (!added) {
        return Error(ErrorCode::kRegistry);
    }
    count_ = peer.tx_lanes.count;
    context_ = request.context;
    buffers_ = request.buffers;
    context_incarnation_ = request.context_incarnation;
    credit_peer_ = request.credit_peer;
    credit_identity_ = identity;
    prepared_once_ = true;
    return {};
}

std::expected<void, ErrorCode> HandshakeRxSources::Check() const noexcept {
    if (!registry_ || !count_) {
        return Error(ErrorCode::kNotReady);
    }
    if (registry_->buffers() != buffers_ || buffers_->stopped() || !buffers_->pool() ||
        buffers_->pool()->context() != context_ || registry_->context_incarnation() != context_incarnation_) {
        return Error(ErrorCode::kRegistry);
    }
    const auto credit = credit_peer_.domain->PeerStats(credit_peer_);
    if (!credit || credit->frozen || credit->retired || credit_peer_.domain->stats().frozen ||
        credit->identity != credit_identity_) {
        return Error(ErrorCode::kRegistry);
    }
    for (std::size_t i = 0; i < count_; ++i) {
        if (!registry_->Check(leases_[i])) {
            return Error(ErrorCode::kRegistry);
        }
    }
    return {};
}

std::expected<void, ErrorCode> HandshakeRxSources::Retire(const RxSourceRetireProof& proof) noexcept {
    if (!registry_) {
        return Error(ErrorCode::kNotReady);
    }
    if (count_) {
        const auto removed = registry_->RemoveBatch(std::span(leases_).first(count_), proof);
        if (!removed) {
            return Error(ErrorCode::kRetained);
        }
        count_ = 0;
    }
    return {};
}

std::expected<void, ErrorCode> HandshakeRxSources::Close() noexcept {
    if (count_) {
        return Error(ErrorCode::kRetained);
    }
    registry_ = nullptr;
    return {};
}

} // namespace runtime
} // namespace kbsocket
