// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONTROL_RNDV_ARENA_HPP_
#define KBSOCKET_CONTROL_RNDV_ARENA_HPP_

#include <expected>

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/control/rndv_capabilities.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace control {

/// 将活动 Slab 的真实注册快照转换为规范 Arena 目录；id/generation 由宿主唯一分配。
/// 不分配、不增加引用、不授予逐消息 WRITE 权限；复制目录不能替代 Grant 或延长注册生命周期。
/// 当前 Slab 固定 NONE/零 token，拒绝无法由规范目录表示的 provider 注册属性。
std::expected<RndvArenaDescriptor, RndvCapabilityError>
ExportRndvArena(memory::SlabAllocator& slab, urma_context_t* context, connection::GrantArena arena) noexcept;

} // namespace control
} // namespace kbsocket
#endif // KBSOCKET_CONTROL_RNDV_ARENA_HPP_
