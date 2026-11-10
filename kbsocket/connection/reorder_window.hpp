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

#include "kbsocket/connection/grant_manager.hpp"
#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"

namespace kbsocket {
namespace connection {
class ReorderWindow;
class EagerRxStream;

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
    kSequenceExhausted,
    kInvalidReservation
};
struct ReorderError {
    ReorderErrorCode code;
};

enum class ReorderFrameKind : std::uint8_t { kEager, kValidatedRmaDone, kFin };
using ReorderLease = std::variant<std::monostate, raw::RxBufferLease, memory::BufferLease, GrantHandle>;
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
struct ReorderReservation {
    const ReorderWindow* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const ReorderReservation&) const = default;
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
    // 已接收或 RTS 占位的连续 SN 前缀；不代表 DONE、应用交付或消费。
    std::uint64_t observed_prefix = 0;
    std::uint64_t released_prefix = 0;
    std::uint64_t max_frame_seq = 0;
    std::uint64_t max_stream_bytes = 0;
    // RTS 已占用 SN/完整字节额度，但尚未取得可信 DONE 和目标 Lease 的项。
    std::size_t pending_frames = 0;
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
    /// 停止新准入；仍有 Pending、Buffered 或已交付但未归还的 receipt 时拒绝关闭。
    std::expected<void, ReorderError> Close() noexcept;
    std::expected<void, ReorderError> Push(const ReorderFrame& frame, std::uint64_t now_ns) noexcept;
    /// RTS 与 Eager 共用同一 SN/字节窗口；成功只占元数据，不借用或终结远端 Grant。
    /// 宿主须先核验 RTS 来源/授权，负责将票据与对应 Grant 绑定；不能由重复 RTS 再占位。
    std::expected<ReorderReservation, ReorderError> ReserveRndv(ReorderSession session, std::uint32_t sequence,
                                                                std::uint64_t bytes, std::uint64_t now_ns) noexcept;
    /// 仅完整可信 DONE 后调用，原位转换占位；成功才接管 Lease，不再次计入字节额度。
    /// 输入必须为相同 session/SN/长度的 kValidatedRmaDone；本类不校验 Grant 或 DMA 证据。
    std::expected<void, ReorderError> CompleteRndv(ReorderReservation reservation, const ReorderFrame& frame,
                                                   std::uint64_t now_ns) noexcept;
    /// 只在停止/失败后丢弃占位元数据；不产生 Grant 终结证据，不前进消费序号或累计已消费字节。
    /// 宿主仍须独立保留未终结 Grant 的 backing，不能把本接口成功视为可复用目标内存。
    std::expected<void, ReorderError> AbandonRndv(ReorderReservation reservation) noexcept;
    /// 最多交付 min(output.size(), 64) 个连续帧；失败后仍可取出已经验证的连续前缀。
    std::expected<std::size_t, ReorderError> Poll(std::span<ReorderDelivery> output, std::uint64_t now_ns) noexcept;
    /// 只读核验真实已交付 receipt 及完整描述符；用于归还底层 Lease 之前的身份门禁。
    /// 不重新核验底层 Lease 存活，允许真实归还成功后重试尚未完成的窗口 Release。
    std::expected<void, ReorderError> ValidateDelivery(const ReorderDelivery& delivery) const noexcept;
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
    std::optional<ReorderSession> session() const noexcept;

private:
    friend class RndvReceiver;
    friend class EagerRxStream;
    std::expected<void, ReorderError> BindStream(const EagerRxStream& stream, const RndvReceiver* receiver) noexcept;
    std::expected<void, ReorderError> UnbindStream(const EagerRxStream& stream) noexcept;
    std::expected<void, ReorderError> BindReceiver(const RndvReceiver& receiver) noexcept;
    std::expected<void, ReorderError> UnbindReceiver(const RndvReceiver& receiver) noexcept;
    struct State;
    std::expected<void, ReorderError> CheckAdmission(const ReorderFrame& frame, std::uint64_t bytes,
                                                     std::uint64_t now_ns, bool reserving) noexcept;
    std::expected<void, ReorderError> ObserveTime(std::uint64_t now_ns) noexcept;
    void UpdateGap(std::uint64_t now_ns) noexcept;
    void UpdateObservedPrefix() noexcept;
    bool Deliver(std::uint32_t index, ReorderDelivery& output, bool discarded) noexcept;
    std::unique_ptr<State> state_;
    // Close/Open 不重置；同一对象生命周期内 receipt 身份绝不回绕复用。
    std::uint64_t next_receipt_ = 0;
    std::uint64_t next_reservation_ = 0;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_REORDER_WINDOW_HPP_
