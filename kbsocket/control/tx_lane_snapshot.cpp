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

std::expected<HandshakeTxLanes, HandshakeError> CaptureTxLanes(const raw::JettyPool& pool,
                                                               bool require_ready) noexcept {
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
        if (!state || *state == raw::JettyLaneState::kReplacing ||
            (require_ready && *state != raw::JettyLaneState::kReady)) {
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
    return ValidateLocalTxLanes(pool, hello, hello.tx_lanes);
}

std::expected<void, HandshakeError> ValidateLocalTxLanes(const raw::JettyPool& pool, const HandshakeHello& hello,
                                                         const HandshakeTxLanes& snapshot) noexcept {
    if (!pool.context() || pool.retained()) {
        return std::unexpected(NotReady());
    }
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

std::expected<void, HandshakeError> ValidateTxLaneUpdate(const HandshakeTxLanes& before,
                                                         const HandshakeTxLanes& after) noexcept {
    if (!ValidateHandshakeTxLanes(before) || !ValidateHandshakeTxLanes(after) ||
        before.pool_epoch != after.pool_epoch || before.count != after.count) {
        return std::unexpected(Invalid());
    }
    bool changed = false;
    for (std::uint32_t i = 0; i < before.count; ++i) {
        const auto& old_lane = before.lanes[i];
        const auto& new_lane = after.lanes[i];
        if (new_lane.generation < old_lane.generation ||
            (new_lane.generation == old_lane.generation && new_lane != old_lane)) {
            return std::unexpected(Invalid());
        }
        changed |= new_lane.generation > old_lane.generation;
    }
    if (!changed) {
        return std::unexpected(Invalid());
    }
    return {};
}

} // namespace control
} // namespace kbsocket
