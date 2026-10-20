// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_RMA_TEST_RMA_TEST_HPP_
#define KBSOCKET_TOOLS_RMA_TEST_RMA_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"

#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
enum class RmaOperation : std::uint32_t { kRead = 1, kWrite = 2 };
struct RmaOptions {
    RmaOperation operation = RmaOperation::kRead;
    std::uint32_t bytes = 4096;
    std::uint32_t rounds = 8;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateRmaOptions(const RmaOptions& options) noexcept;
std::expected<void, ToolError> CheckRmaCapability(const RmaOptions& options, const urma_device_cap_t& cap) noexcept;

struct RmaHello {
    RmaOptions options;
    bool server = false;
    urma_jetty_id_t endpoint{};
    urma_seg_t segment{};
    urma_token_t token{};
    std::uint64_t payload_address = 0;
};
using RmaHelloBytes = std::array<std::byte, 112>;
RmaHelloBytes EncodeRmaHello(const RmaHello& hello) noexcept;
std::expected<RmaHello, ToolError> DecodeRmaHello(const RmaHelloBytes& bytes) noexcept;
std::expected<void, ToolError> ValidateRmaHello(const RmaHello& hello) noexcept;

struct RmaStats {
    std::uint32_t accepted = 0;
    std::uint32_t retired = 0;
    std::uint32_t verified = 0;
    std::uint64_t min_cqe_ns = 0;
    std::uint64_t max_cqe_ns = 0;
    int last_completion_status = 0;
    int last_post_status = 0;
    bool pending = false;
};

/// 每轮一条真正的 READ/WRITE WR，不按 SEND 的 4KiB 限制拆分，也不重放失败请求。
/// 显式双端静默确认前保留 MR；入口须保证失败后的整个对象与 context 存活至进程退出。
class RmaSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap, const RmaOptions& options,
                                        bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel);
    std::expected<void, ToolError> Close();
    const RmaStats& stats() const noexcept {
        return stats_;
    }

private:
    std::expected<void, ToolError> Exchange(ControlChannel& channel);
    std::expected<void, ToolError> Transfer(std::uint32_t round);
    std::expected<void, ToolError> Barrier(ControlChannel& channel, std::uint32_t marker);
    void Prepare(std::uint32_t round);
    std::expected<void, ToolError> Verify(std::uint32_t round);

    urma_context_t* ctx_ = nullptr;
    raw::JettyPool pool_;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    urma_target_seg_t* remote_segment_ = nullptr;
    urma_target_jetty_t* target_ = nullptr;
    RmaHello local_, peer_;
    RmaStats stats_;
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_, payload_;
    urma_sge_t local_sge_{}, remote_sge_{};
    urma_jfs_wr_t wr_{};
    bool ready_ = false;
    bool started_ = false;
    bool quiescent_ = true;
    bool failed_close_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_RMA_TEST_RMA_TEST_HPP_
