// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_RMA_FAILURE_TEST_RMA_FAILURE_TEST_HPP_
#define KBSOCKET_TOOLS_RMA_FAILURE_TEST_RMA_FAILURE_TEST_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include "tools/common/control_channel.hpp"
#include "tools/rma_test/rma_test.hpp"

#include "kbsocket/transport/raw/jetty_pool.hpp"

namespace kbsocket {
namespace tools {
struct RmaFailureOptions {
    std::uint32_t bytes = 16384;
    std::uint32_t batch = 32;
    std::uint32_t rounds = 8;
    std::uint32_t timeout_ms = 10000;
};
std::expected<void, ToolError> ValidateRmaFailureOptions(const RmaFailureOptions& options) noexcept;
enum class RmaFailureRole { kClient, kHealthyTarget, kFaultTarget };
enum class RmaFailurePhase { kHealthyBaseline, kFaultBaseline, kHealthyBefore, kMixed, kHealthyAfter };
struct RmaWriteCounters {
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::uint64_t verified = 0;
    std::array<std::uint64_t, 17> statuses{};
};
struct RmaFailureStats {
    RmaWriteCounters healthy_baseline, fault_baseline, healthy_before, healthy_mixed, fault_mixed, healthy_after;
    std::uint64_t post_calls = 0;
    std::uint64_t partial_batches = 0;
    std::uint64_t state_checks = 0;
    std::uint32_t jetty_id = 0;
    int last_post_status = 0;
    int last_completion_status = 0;
    int last_query_status = 0;
    int last_state = -1;
    bool accepted_known = true;
    bool pending = false;
    bool peer_exit_confirmed = false;
};

struct RmaFailureHello {
    RmaFailureOptions options;
    bool server = false;
    // 服务端依次发布健康目标、故障目标；客户端只发布本地描述符，第二项在线上必须为零。
    // 内层描述单次 WRITE（window=1），外层 batch 定义槽数并单独校验完整内存范围。
    std::array<RmaHello, 2> endpoints{};
};
using RmaFailureHelloBytes = std::array<std::byte, 256>;
RmaFailureHelloBytes EncodeRmaFailureHello(const RmaFailureHello& hello) noexcept;
std::expected<RmaFailureHello, ToolError> DecodeRmaFailureHello(const RmaFailureHelloBytes& wire) noexcept;
std::expected<void, ToolError> MatchRmaFailureEndpoint(const RmaHello& hello, const RmaFailureOptions& options,
                                                       bool server) noexcept;

/// 由监督者提供退出证明；正常 WRITE 基线经远端校验之后才允许终止故障目标。
class RmaFailurePeer {
public:
    virtual ~RmaFailurePeer() = default;
    virtual const RmaHello& hello() const noexcept = 0;
    virtual std::expected<void, ToolError> FinishBaselineAndExit() = 0;
};

/// 客户端各目标/槽使用独立源区；远端逐轮校验写入与保护区并确认后才允许复用。
/// 所有 WR、SGE、身份及内存于 Open 预分配；失败对象必须由入口保留到进程退出。
class RmaFailureResources {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const RmaFailureOptions& options, RmaFailureRole role);
    std::expected<void, ToolError> Import(const std::array<RmaHello, 2>& peers);
    std::expected<void, ToolError> Transfer(RmaFailurePhase phase, std::uint32_t round = 0);
    std::expected<void, ToolError> VerifyTarget(RmaFailurePhase phase, std::uint32_t round = 0);
    std::expected<void, ToolError> CheckQuiescent();
    std::expected<void, ToolError> Close();
    const RmaHello& hello() const noexcept {
        return hello_;
    }
    RmaFailureStats stats;

private:
    friend class RmaFailureSession;
    std::expected<void, ToolError> ConfirmRemoteWrites(RmaFailurePhase phase);
    std::expected<void, ToolError> CheckReady();
    std::expected<void, ToolError> CheckGuards();
    std::span<std::byte> Payload(std::size_t slot);
    RmaWriteCounters& Counters(RmaFailurePhase phase, std::size_t target);
    RmaFailureOptions options_;
    RmaFailureRole role_ = RmaFailureRole::kClient;
    urma_context_t* ctx_ = nullptr;
    raw::JettyPool pool_;
    urma_jetty_t* jetty_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    std::array<urma_target_seg_t*, 2> remote_segments_{};
    std::array<urma_target_jetty_t*, 2> targets_{};
    std::array<RmaHello, 2> peers_{};
    RmaHello hello_;
    std::vector<std::byte> storage_;
    std::span<std::byte> registered_;
    std::array<urma_jfs_wr_t, 256> wrs_{};
    std::array<urma_sge_t, 256> sources_{}, destinations_{};
    std::array<bool, 256> retired_{};
    std::uint64_t next_id_ = 1;
    std::size_t slots_ = 0;
    bool opened_ = false;
    bool imported_ = false;
    bool failed_ = false;
    bool failed_close_ = false;
};

/// 健康主进程独占 TCP 控制。退出标记只在子进程确实回收后发送；不修改或重建客户端 SQ。
class RmaFailureSession {
public:
    std::expected<void, ToolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                        const RmaFailureOptions& options, bool server);
    std::expected<void, ToolError> Run(ControlChannel& channel, RmaFailurePeer* fault_peer = nullptr);
    std::expected<void, ToolError> Close();
    RmaFailureResources resources;
    bool can_close() const noexcept {
        return quiescent_;
    }

private:
    RmaFailureOptions options_;
    bool server_ = false;
    bool started_ = false;
    bool opened_ = false;
    bool quiescent_ = false;
};
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_RMA_FAILURE_TEST_RMA_FAILURE_TEST_HPP_
