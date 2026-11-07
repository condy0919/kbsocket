// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/tx_lane_snapshot.hpp"

#include <cstring>

namespace kbsocket {
namespace control {
namespace {
HandshakeError Invalid() noexcept {
    return {HandshakeErrorCode::kInvalidArgument};
}
HandshakeError NotReady() noexcept {
    return {HandshakeErrorCode::kNotReady};
}

bool SameEid(const std::array<std::byte, 16>& expected, const urma_eid_t& actual) noexcept {
    return std::memcmp(expected.data(), actual.raw, expected.size()) == 0;
}
} // namespace

std::expected<HandshakeTxLanes, HandshakeError> CaptureTxLanes(const raw::JettyPool& pool) noexcept {
    if (!pool.context() || pool.retained()) {
        return std::unexpected(NotReady());
    }
    if (!pool.capacity() || pool.capacity() > kHandshakeMaxTxLanes) {
        return std::unexpected(Invalid());
    }
    auto* passive = pool.rx_target_jetty();
    if (!passive || passive->urma_ctx != pool.context()) {
        return std::unexpected(NotReady());
    }
    HandshakeTxLanes snapshot;
    snapshot.count = static_cast<std::uint32_t>(pool.capacity());
    for (std::uint32_t index = 0; index < snapshot.count; ++index) {
        const auto lane = pool.lane(index);
        auto* jetty = pool.lane_jetty(index);
        if (!lane || !jetty || jetty->urma_ctx != pool.context()) {
            return std::unexpected(NotReady());
        }
        if (jetty->jetty_id.id == passive->jetty_id.id &&
            std::memcmp(jetty->jetty_id.eid.raw, passive->jetty_id.eid.raw, sizeof(urma_eid_t)) == 0) {
            return std::unexpected(Invalid());
        }
        const auto state = pool.lane_state(*lane);
        if (!state || *state != raw::JettyLaneState::kReady) {
            return std::unexpected(NotReady());
        }
        if (index == 0) {
            snapshot.pool_epoch = lane->epoch;
        } else if (lane->epoch != snapshot.pool_epoch) {
            return std::unexpected(Invalid());
        }
        auto& entry = snapshot.lanes[index];
        std::memcpy(entry.eid.data(), jetty->jetty_id.eid.raw, entry.eid.size());
        entry.uasid = jetty->jetty_id.uasid;
        entry.jetty_id = jetty->jetty_id.id;
        entry.generation = lane->generation;
    }
    const auto valid = ValidateHandshakeTxLanes(snapshot);
    if (!valid) {
        return std::unexpected(valid.error());
    }
    return snapshot;
}

std::expected<void, HandshakeError> ValidateLocalTxLanes(const raw::JettyPool& pool,
                                                         const HandshakeHello& hello) noexcept {
    if (!pool.context() || pool.retained()) {
        return std::unexpected(NotReady());
    }
    const auto& snapshot = hello.tx_lanes;
    if (!(hello.capabilities & kHandshakeCapabilityRaw) || !snapshot.count || snapshot.count > kHandshakeMaxTxLanes ||
        snapshot.count != pool.capacity() || !snapshot.pool_epoch) {
        return std::unexpected(Invalid());
    }
    const auto passive = pool.rx_target_jetty_id();
    auto* target = pool.rx_target_jetty();
    if (!passive || !target || target->urma_ctx != pool.context()) {
        return std::unexpected(NotReady());
    }
    if (hello.jetty_id != passive->id || !SameEid(hello.eid, passive->eid)) {
        return std::unexpected(Invalid());
    }
    for (std::uint32_t index = 0; index < snapshot.count; ++index) {
        const auto lane = pool.lane(index);
        auto* jetty = pool.lane_jetty(index);
        if (!lane || !jetty || jetty->urma_ctx != pool.context()) {
            return std::unexpected(NotReady());
        }
        const auto& entry = snapshot.lanes[index];
        if (snapshot.pool_epoch != lane->epoch || entry.generation != lane->generation ||
            entry.jetty_id != jetty->jetty_id.id || entry.uasid != jetty->jetty_id.uasid ||
            !SameEid(entry.eid, jetty->jetty_id.eid)) {
            return std::unexpected(Invalid());
        }
    }
    return {};
}

} // namespace control
} // namespace kbsocket
