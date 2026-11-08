// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_REORDER_WINDOW_HPP_
#define KBSOCKET_CONNECTION_REORDER_WINDOW_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <variant>

#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"

namespace kbsocket {
namespace connection {
class ReorderWindow;

struct ReorderSession {
    std::uint64_t peer_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t session = 0;
    std::uint32_t slot = 0;
    std::uint32_t generation = 0;
    bool operator==(const ReorderSession&) const = default;
};
enum class ReorderErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kWrongSession,
    kDuplicate,
    kLeaseAlreadyHeld,
    kOutOfWindow,
    kByteLimit,
    kProtocol,
    kStopped,
    kGapTimeout,
    kFailed,
    kInvalidReceipt,
    kSequenceExhausted
};
struct ReorderError {
    ReorderErrorCode code;
};

enum class ReorderFrameKind : std::uint8_t { kEager, kValidatedRmaDone, kFin };
using ReorderLease = std::variant<std::monostate, raw::RxBufferLease, memory::BufferLease>;
struct ReorderFrame {
    ReorderSession session{};
    std::uint32_t sequence = 0;
    ReorderFrameKind kind = ReorderFrameKind::kEager;
    ReorderLease lease{};
    std::span<const std::byte> payload{};
};
struct ReorderReceipt {
    const ReorderWindow* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const ReorderReceipt&) const = default;
};
struct ReorderDelivery {
    ReorderFrame frame{};
    ReorderReceipt receipt{};
    // Drain 交出的是清理责任，不能当作有序业务数据或正常 EOF 交付。
    bool discarded = false;
};
struct ReorderConfig {
    std::uint32_t window = 64;
    std::size_t max_resident_bytes = 256 * 1024;
    std::uint64_t gap_timeout_ns = 1000000000;
    std::uint32_t initial_sequence = 0;
};
struct ReorderStats {
    std::size_t buffered_frames = 0;
    std::size_t delivered_frames = 0;
    std::size_t resident_bytes = 0;
    std::uint64_t received_bytes = 0;
    std::uint64_t consumed_bytes = 0;
    std::uint64_t rcv_next = 0;
    std::uint64_t released_prefix = 0;
    std::uint64_t max_frame_seq = 0;
    std::uint64_t max_stream_bytes = 0;
};

/// 单 owner、固定容量的乱序与应用持有账本；Open 分配，数据路径不分配或调用 provider。
/// Push 成功才接管 Lease；所有错误均由调用方继续持有并归还输入 Lease。
/// Poll/Drain 交出 Lease，但驻留责任保留到 Release(receipt)；宿主须先完成消费并归还真实 Lease。
/// 本类不检查 DMA/Grant 终结，kValidatedRmaDone 必须由上层先验证完整授权与完成证明。
/// context、allocator/RX pool 和 payload backing 须覆盖全部持有及交付责任；描述符复制不增引用。
class ReorderWindow {
public:
    static constexpr std::uint32_t kMaxWindow = 4096;
    static constexpr std::size_t kMaxBatch = 64;
    ReorderWindow() noexcept;
    ~ReorderWindow();
    ReorderWindow(const ReorderWindow&) = delete;
    ReorderWindow& operator=(const ReorderWindow&) = delete;
    std::expected<void, ReorderError> Open(ReorderSession session, const ReorderConfig& config = {}) noexcept;
    /// 停止新准入；仍有 Buffered 或已交付但未归还的 receipt 时拒绝关闭。
    std::expected<void, ReorderError> Close() noexcept;
    std::expected<void, ReorderError> Push(const ReorderFrame& frame, std::uint64_t now_ns) noexcept;
    /// 最多交付 min(output.size(), 64) 个连续帧；失败后仍可取出已经验证的连续前缀。
    std::expected<std::size_t, ReorderError> Poll(std::span<ReorderDelivery> output, std::uint64_t now_ns) noexcept;
    /// 仅在真实 Lease 已归还且应用不再访问后解除驻留；它不调用底层 ReleaseCpu/Release。
    std::expected<void, ReorderError> Release(ReorderReceipt receipt) noexcept;
    /// 仅停止/失败后使用，按预算交出 Buffered 描述符供清理；不抢回已交付给应用的 Lease。
    std::expected<std::size_t, ReorderError> Drain(std::span<ReorderDelivery> output) noexcept;
    std::expected<void, ReorderError> CheckTimeout(std::uint64_t now_ns) noexcept;
    void StopAccepting() noexcept;
    void Fail() noexcept;
    /// 只查看下一个连续帧（包括 FIN）或已确认 EOF，不消费 receipt，也不推进超时。
    bool readable() const noexcept;
    bool eof() const noexcept;
    bool accepting() const noexcept;
    std::optional<ReorderError> failure() const noexcept;
    ReorderStats stats() const noexcept;

private:
    struct State;
    std::expected<void, ReorderError> ObserveTime(std::uint64_t now_ns) noexcept;
    void UpdateGap(std::uint64_t now_ns) noexcept;
    bool Deliver(std::uint32_t index, ReorderDelivery& output, bool discarded) noexcept;
    std::unique_ptr<State> state_;
    // Close/Open 不重置；同一对象生命周期内 receipt 身份绝不回绕复用。
    std::uint64_t next_receipt_ = 0;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_REORDER_WINDOW_HPP_
