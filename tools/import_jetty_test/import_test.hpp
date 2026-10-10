// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_IMPORT_JETTY_TEST_IMPORT_TEST_HPP_
#define KBSOCKET_TOOLS_IMPORT_JETTY_TEST_IMPORT_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "tools/common/control_channel.hpp"
#include "tools/import_jetty_test/api_timings.hpp"

#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
inline constexpr std::size_t kImportCount = 100;
inline constexpr std::size_t kMaxDescriptorBytes = 65536;
using ImportSamples = std::array<std::uint64_t, kImportCount>;

std::expected<void, ToolError> ValidateDescriptor(std::span<const std::byte> bytes) noexcept;
std::expected<unsigned, ToolError> SelectCtpPriority(const urma_device_cap_t& cap, int requested) noexcept;

/// 双端先创建本地队列；客户端保留全部导入对象直到测量结束，不投递任何 WR。
/// Close 失败后保留句柄及下层依赖，入口在进程退出前不自动析构它们。
class ImportSession {
public:
    explicit ImportSession(ApiTimings* timings = nullptr) : timings_(timings) {}
    ImportSession(const ImportSession&) = delete;
    ImportSession& operator=(const ImportSession&) = delete;
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap, unsigned priority);
    std::expected<void, ToolError> Run(ControlChannel& channel, bool server, std::uint32_t timeout_ms);
    std::expected<void, ToolError> Close() noexcept;
    std::span<urma_jetty_t* const> jettys() const noexcept {
        return jettys_;
    }
    const ImportSamples& samples() const noexcept {
        return samples_;
    }
    std::size_t completed() const noexcept {
        return completed_;
    }

private:
    std::expected<void, ToolError> UnimportAll() noexcept;
    ApiTimings* timings_;
    urma_context_t* ctx_ = nullptr;
    urma_jfc_t* send_cq_ = nullptr;
    urma_jfc_t* recv_cq_ = nullptr;
    urma_jfr_t* jfr_ = nullptr;
    std::array<urma_jetty_t*, kImportCount> jettys_{};
    std::array<urma_target_jetty_t*, kImportCount> targets_{};
    ImportSamples samples_{};
    std::size_t completed_ = 0;
    bool ran_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_IMPORT_JETTY_TEST_IMPORT_TEST_HPP_
