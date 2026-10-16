// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_IMPORT_JETTY_TEST_CONTROL_PLANE_HPP_
#define KBSOCKET_TOOLS_IMPORT_JETTY_TEST_CONTROL_PLANE_HPP_

#include "tools/import_jetty_test/api_timings.hpp"
#include "tools/import_jetty_test/import_test.hpp"

namespace kbsocket {
namespace tools {
// 仅工具加载额外符号，不扩大核心库的必需 ABI。缺少符号时明确跳过对应场景。
struct ControlPlaneApis {
    decltype(&::urma_modify_jfc) modify_jfc = nullptr;
    decltype(&::urma_create_jfs) create_jfs = nullptr;
    decltype(&::urma_delete_jfs) delete_jfs = nullptr;
    decltype(&::urma_query_jfs) query_jfs = nullptr;
    decltype(&::urma_modify_jfs) modify_jfs = nullptr;
    decltype(&::urma_query_jfr) query_jfr = nullptr;
    decltype(&::urma_query_jetty) query_jetty = nullptr;
    decltype(&::urma_create_jetty_grp) create_jetty_grp = nullptr;
    decltype(&::urma_delete_jetty_grp) delete_jetty_grp = nullptr;
    decltype(&::urma_alloc_token_id) alloc_token_id = nullptr;
    decltype(&::urma_free_token_id) free_token_id = nullptr;
    std::expected<void, ToolError> Load(const char* library) noexcept;
};

std::expected<void, ToolError> ValidateSegmentDescriptor(std::span<const std::byte> bytes) noexcept;

/// 原始 100 次 jetty import 之后运行的附加场景，不提前预热其 TP。
/// Close 成功前必须保留 context 和注册内存；失败时留给入口保留至退出。
class ControlPlaneSession {
public:
    ControlPlaneSession(ApiTimings& timings, const ControlPlaneApis& apis) : timings_(timings), apis_(apis) {}
    ControlPlaneSession(const ControlPlaneSession&) = delete;
    ControlPlaneSession& operator=(const ControlPlaneSession&) = delete;
    std::expected<void, ToolError> RunLocal(urma_context_t* ctx, unsigned priority,
                                            std::span<urma_jetty_t* const> jettys);
    std::expected<void, ToolError> RunSegments(ControlChannel& channel, bool server, std::uint32_t timeout);
    std::expected<void, ToolError> Close() noexcept;

private:
    std::expected<void, ToolError> CloseLocal() noexcept;
    std::expected<void, ToolError> UnimportSegments() noexcept;
    ApiTimings& timings_;
    const ControlPlaneApis& apis_;
    urma_context_t* ctx_ = nullptr;
    urma_context_t* temp_ctx_ = nullptr;
    urma_jfce_t* jfce_ = nullptr;
    urma_jfc_t* jfc_ = nullptr;
    urma_jfr_t* jfr_ = nullptr;
    urma_jfs_t* jfs_ = nullptr;
    urma_jetty_grp_t* group_ = nullptr;
    urma_token_id_t* token_ = nullptr;
    void* memory_ = nullptr;
    std::array<urma_target_seg_t*, kImportCount> local_segments_{};
    std::array<urma_target_seg_t*, kImportCount> remote_segments_{};
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_IMPORT_JETTY_TEST_CONTROL_PLANE_HPP_
