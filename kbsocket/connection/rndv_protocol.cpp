// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/rndv_protocol.hpp"

#include <array>
#include <cstddef>

namespace kbsocket {
namespace connection {
namespace {
auto Error(RndvProtocolErrorCode code) noexcept {
    return std::unexpected(RndvProtocolError{code});
}
bool ValidDestination(const RndvDestination& destination) noexcept {
    return destination.runtime && destination.receive_domain && destination.session && destination.token.generation;
}
bool ForwardRoute(const RndvDestination& destination, const GrantBinding& binding) noexcept {
    return destination.runtime == binding.receiver_runtime && destination.receive_domain == binding.receive_domain &&
           destination.session == binding.session && destination.token == binding.token;
}
WireFrameInput TransferFrame(const GrantTransfer& transfer, RndvDestination destination,
                             WireControlType control) noexcept {
    WireFrameInput frame;
    frame.type = WireFrameType::kControl;
    frame.control = control;
    frame.token = destination.token;
    frame.session = destination.session;
    frame.seq = transfer.seq;
    const auto& binding = transfer.binding;
    frame.rndv.sender_runtime = binding.sender_runtime;
    frame.rndv.receiver_runtime = binding.receiver_runtime;
    frame.rndv.receive_domain = binding.receive_domain;
    frame.rndv.session = binding.session;
    frame.rndv.token = binding.token;
    frame.rndv.transfer_id = transfer.transfer_id;
    frame.rndv.bytes = transfer.bytes;
    return frame;
}
WireFrameInput GrantFrame(const GrantIdentity& grant, RndvDestination destination, WireControlType control) noexcept {
    auto frame = TransferFrame(grant.transfer, destination, control);
    frame.rndv.arena_id = grant.arena.id;
    frame.rndv.arena_generation = grant.arena.generation;
    frame.rndv.grant_slot = grant.slot;
    frame.rndv.grant_generation = grant.generation;
    frame.rndv.target_address = grant.target_address;
    return frame;
}
bool ValidFrame(const WireFrameInput& frame) noexcept {
    // 固定栈空间复用 canonical codec 的完整校验，避免两套字段合法性规则漂移。
    std::array<std::byte, 112> buffer{};
    return EncodeWireHeader(frame, buffer).has_value();
}
std::expected<WireFrameInput, RndvProtocolError> CheckedFrame(const WireFrameInput& frame) noexcept {
    if (!ValidFrame(frame)) {
        return Error(RndvProtocolErrorCode::kInvalidArgument);
    }
    return frame;
}
GrantTransfer Transfer(const WireFrameInput& frame) noexcept {
    const auto& fields = frame.rndv;
    return {{fields.sender_runtime, fields.receiver_runtime, fields.receive_domain, fields.session, fields.token},
            fields.transfer_id,
            frame.seq,
            fields.bytes};
}
GrantIdentity Identity(const WireFrameInput& frame) noexcept {
    return {Transfer(frame),
            {frame.rndv.arena_id, frame.rndv.arena_generation},
            frame.rndv.grant_slot,
            frame.rndv.grant_generation,
            frame.rndv.target_address};
}
std::expected<void, RndvProtocolError> CheckReceived(const WireFrameView& view, WireControlType type,
                                                     const WireSessionBinding& route,
                                                     const WireReceiveContext& observed) noexcept {
    if (view.frame.type != WireFrameType::kControl || view.frame.control != type) {
        return Error(RndvProtocolErrorCode::kWrongControl);
    }
    if (!ValidFrame(view.frame)) {
        return Error(RndvProtocolErrorCode::kMalformedFrame);
    }
    if (!ValidateWireIdentity(view, route, observed)) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    return {};
}
bool ForwardSource(const WireSessionBinding& route, const GrantBinding& binding) noexcept {
    return route.context.peer_runtime == binding.sender_runtime &&
           route.context.receive_domain == binding.receive_domain && route.context.session == binding.session &&
           route.token == binding.token;
}
} // namespace

std::expected<WireFrameInput, RndvProtocolError> BuildRndvRts(const GrantTransfer& transfer,
                                                              RndvDestination destination) noexcept {
    if (!ValidDestination(destination) || !ForwardRoute(destination, transfer.binding)) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    return CheckedFrame(TransferFrame(transfer, destination, WireControlType::kRndvRts));
}
std::expected<WireFrameInput, RndvProtocolError> BuildRndvGrant(const GrantIdentity& grant,
                                                                RndvDestination destination) noexcept {
    if (!ValidDestination(destination) || destination.runtime != grant.transfer.binding.sender_runtime) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    if (grant.slot >= GrantManager::kMaxWindow) {
        return Error(RndvProtocolErrorCode::kInvalidArgument);
    }
    return CheckedFrame(GrantFrame(grant, destination, WireControlType::kRndvGrant));
}
namespace {
std::expected<WireFrameInput, RndvProtocolError> DoneFrame(const GrantTerminalEvidence& evidence,
                                                           RndvDestination destination) noexcept {
    if (!evidence.no_future_writes || !evidence.acceptance_known || !evidence.accepted_writes ||
        evidence.accepted_writes != evidence.terminal_writes ||
        evidence.accepted_writes != evidence.successful_writes ||
        evidence.completed_bytes != evidence.identity.transfer.bytes) {
        return Error(RndvProtocolErrorCode::kInvalidEvidence);
    }
    if (!ValidDestination(destination) || !ForwardRoute(destination, evidence.identity.transfer.binding)) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    return CheckedFrame(GrantFrame(evidence.identity, destination, WireControlType::kRndvDone));
}
} // namespace
std::expected<WireFrameInput, RndvProtocolError> BuildRndvDone(const WriteRndvEngine& engine,
                                                               RndvDestination destination) noexcept {
    const auto evidence = engine.DoneEvidence();
    if (!evidence) {
        return Error(RndvProtocolErrorCode::kInvalidEvidence);
    }
    return DoneFrame(*evidence, destination);
}
std::expected<WireFrameInput, RndvProtocolError> BuildRndvDone(const WriteRndvPool& pool, WriteRndvHandle handle,
                                                               RndvDestination destination) noexcept {
    const auto evidence = pool.DoneEvidence(handle);
    if (!evidence) {
        return Error(RndvProtocolErrorCode::kInvalidEvidence);
    }
    return DoneFrame(*evidence, destination);
}
std::expected<GrantTransfer, RndvProtocolError> ValidateRndvRts(const WireFrameView& view,
                                                                const WireSessionBinding& route,
                                                                const WireReceiveContext& observed,
                                                                const GrantBinding& expected) noexcept {
    if (auto checked = CheckReceived(view, WireControlType::kRndvRts, route, observed); !checked) {
        return std::unexpected(checked.error());
    }
    if (!ForwardSource(route, expected)) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    const auto transfer = Transfer(view.frame);
    if (transfer.binding != expected) {
        return Error(RndvProtocolErrorCode::kWrongTransfer);
    }
    return transfer;
}
std::expected<GrantIdentity, RndvProtocolError>
ValidateRndvGrant(const WireFrameView& view, const WireSessionBinding& route, const WireReceiveContext& observed,
                  const GrantTransfer& expected, GrantArena arena) noexcept {
    if (auto checked = CheckReceived(view, WireControlType::kRndvGrant, route, observed); !checked) {
        return std::unexpected(checked.error());
    }
    if (route.context.peer_runtime != expected.binding.receiver_runtime) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    const auto identity = Identity(view.frame);
    if (identity.transfer != expected) {
        return Error(RndvProtocolErrorCode::kWrongTransfer);
    }
    if (identity.arena != arena || identity.slot >= GrantManager::kMaxWindow) {
        return Error(RndvProtocolErrorCode::kWrongGrant);
    }
    return identity;
}
std::expected<void, RndvProtocolError> ValidateRndvDone(const WireFrameView& view, const WireSessionBinding& route,
                                                        const WireReceiveContext& observed,
                                                        const GrantIdentity& expected) noexcept {
    if (auto checked = CheckReceived(view, WireControlType::kRndvDone, route, observed); !checked) {
        return std::unexpected(checked.error());
    }
    if (!ForwardSource(route, expected.transfer.binding)) {
        return Error(RndvProtocolErrorCode::kWrongRoute);
    }
    const auto identity = Identity(view.frame);
    if (identity.transfer != expected.transfer) {
        return Error(RndvProtocolErrorCode::kWrongTransfer);
    }
    if (identity != expected || identity.slot >= GrantManager::kMaxWindow) {
        return Error(RndvProtocolErrorCode::kWrongGrant);
    }
    return {};
}

} // namespace connection
} // namespace kbsocket
