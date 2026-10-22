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
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
enum class RmaOperation : std::uint32_t { kRead = 1, kWrite = 2 };
inline constexpr std::uint32_t kRmaMaxWindow = 128;
inline constexpr std::size_t kRmaGuardBytes = 64;
inline constexpr std::size_t kRmaSendBytes = 64;

struct RmaOptions {
    RmaOperation operation = RmaOperation::kRead;
    std::uint32_t bytes = 4096;
    std::uint32_t rounds = 8;
    std::uint32_t timeout_ms = 10000;
    std::uint32_t window = 1;
    bool mixed_send = false;
};
/// 每个并发槽含独立 RMA payload、前后保护区，混合模式另有 SEND 区及尾部保护区。
constexpr std::uint64_t RmaSlotStride(const RmaOptions& options) noexcept {
    return options.bytes + 2 * kRmaGuardBytes + (options.mixed_send ? kRmaSendBytes + kRmaGuardBytes : 0);
}
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
    std::uint32_t send_accepted = 0;
    std::uint32_t send_retired = 0;
    std::uint32_t send_received = 0;
    std::uint32_t partial_batches = 0;
    bool accepted_known = true;
    std::uint64_t min_cqe_ns = 0;
    std::uint64_t max_cqe_ns = 0;
    int last_completion_status = 0;
    int last_post_status = 0;
    bool pending = false;
};

/// 每轮提交 window 条独立 READ，可在同一 SQ 交错小 SEND；WRITE 保留串行，不拆分或重放请求。
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
    std::expected<void, ToolError> Receive(std::uint32_t round);
    std::span<std::byte> Payload(std::uint32_t slot);
    std::span<std::byte> SendPayload(std::uint32_t slot);
    std::expected<void, ToolError> Barrier(ControlChannel& channel, std::uint32_t marker);
    void Prepare(std::uint32_t round);
    std::expected<void, ToolError> Verify(std::uint32_t round);

    urma_context_t* ctx_ = nullptr;
    raw::JettyPool pool_;
    raw::RxBufferPool rx_;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    urma_target_seg_t* remote_segment_ = nullptr;
    urma_target_jetty_t* target_ = nullptr;
    RmaHello local_, peer_;
    RmaStats stats_;
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_;
    std::array<urma_sge_t, kRmaMaxWindow> local_sges_{}, remote_sges_{}, send_sges_{};
    std::array<urma_jfs_wr_t, kRmaMaxWindow * 2> wrs_{};
    std::array<bool, kRmaMaxWindow * 2> retired_{};
    std::array<bool, kRmaMaxWindow> received_{};
    bool ready_ = false;
    bool started_ = false;
    bool quiescent_ = true;
    bool failed_close_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_RMA_TEST_RMA_TEST_HPP_
