// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_IMPORT_JETTY_TEST_IMPORT_STRESS_HPP_
#define KBSOCKET_TOOLS_IMPORT_JETTY_TEST_IMPORT_STRESS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
struct StressOptions {
    bool enabled = false;
    std::uint32_t count = 10000;
    std::uint32_t threads = 8;
};

struct StressSample {
    std::uint64_t start_ns = 0;
    std::uint64_t elapsed_ns = 0;
    int error = 0;
    bool attempted = false;
    bool success = false;
};

/// 所有 worker 共享 context，各自持有描述符副本；测量期间不释放任何导入对象。
/// 资源释放失败时保留句柄，调用方必须保持 context 和远端 jetty 存活。
class ImportStress {
public:
    std::expected<void, ToolError> Run(urma_context_t* ctx, std::span<const std::span<const std::byte>> descriptors,
                                       StressOptions options);
    std::expected<void, ToolError> Close() noexcept;
    void Print(bool all_samples) const;
    std::span<const StressSample> samples() const noexcept {
        return samples_;
    }
    std::size_t completed() const noexcept;

private:
    StressOptions options_{};
    std::vector<urma_target_jetty_t*> targets_;
    std::vector<StressSample> samples_;
    std::uint64_t wall_ns_ = 0;
    bool ran_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_IMPORT_JETTY_TEST_IMPORT_STRESS_HPP_
