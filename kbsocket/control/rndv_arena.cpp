// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/rndv_arena.hpp"

#include <cstddef>
#include <cstdint>

namespace kbsocket {
namespace control {
std::expected<RndvArenaDescriptor, RndvCapabilityError>
ExportRndvArena(memory::SlabAllocator& slab, urma_context_t* context, connection::GrantArena arena) noexcept {
    if (!arena.id || !arena.generation) {
        return std::unexpected(RndvCapabilityError{RndvCapabilityErrorCode::kInvalidArgument});
    }
    const auto snapshot = slab.ExportSegment(context);
    if (!snapshot) {
        return std::unexpected(RndvCapabilityError{RndvCapabilityErrorCode::kInvalidArgument});
    }
    const auto& segment = snapshot->descriptor;
    RndvArenaDescriptor descriptor;
    descriptor.id = arena.id;
    descriptor.generation = arena.generation;
    descriptor.address = segment.ubva.va;
    descriptor.length = segment.len;
    descriptor.max_grant_bytes = snapshot->buffer_size;
    for (std::size_t i = 0; i < descriptor.eid.size(); ++i) {
        descriptor.eid[i] = static_cast<std::byte>(segment.ubva.eid.raw[i]);
    }
    descriptor.uasid = segment.ubva.uasid;
    descriptor.token_id = segment.token_id;
    descriptor.token = snapshot->token.token;
    descriptor.access = static_cast<std::uint8_t>(segment.attr.bs.access);
    descriptor.token_policy = static_cast<std::uint8_t>(segment.attr.bs.token_policy);
    if (const auto validated = ValidateRndvArena(descriptor); !validated) {
        return std::unexpected(validated.error());
    }
    return descriptor;
}
} // namespace control
} // namespace kbsocket
