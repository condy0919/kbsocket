// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_GRANT_MANAGER_HPP_
#define KBSOCKET_CONNECTION_GRANT_MANAGER_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace connection {
enum class GrantErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kWrongBinding,
    kWindowFull,
    kStaleSequence,
    kDuplicate,
    kExhausted,
    kNoMemory,
    kMemory,
    kInvalidHandle,
    kIdentityMismatch,
    kInvalidState,
    kInvalidEvidence
};
struct GrantError {
    GrantErrorCode code;
    std::optional<memory::MemoryError> memory_error;
};
struct GrantBinding {
    std::uint64_t sender_runtime = 0;
    std::uint64_t receiver_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t session = 0;
    WireToken token;
    bool operator==(const GrantBinding&) const = default;
};
struct GrantArena {
    std::uint64_t id = 0;
    std::uint64_t generation = 0;
    bool operator==(const GrantArena&) const = default;
};
struct GrantTransfer {
    GrantBinding binding;
    std::uint64_t transfer_id = 0;
    std::uint32_t seq = 0;
    std::uint64_t bytes = 0;
    bool operator==(const GrantTransfer&) const = default;
};
struct GrantIdentity {
    GrantTransfer transfer;
    GrantArena arena;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    std::uint64_t target_address = 0;
    bool operator==(const GrantIdentity&) const = default;
};
class GrantManager;
struct GrantHandle {
    const GrantManager* owner = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t generation = 0;
    bool operator==(const GrantHandle&) const = default;
};
struct GrantReservation {
    GrantHandle handle;
    GrantIdentity identity;
    // 只用于构造授权描述；不得将此本地注册句柄当作可直接发送的 wire 对象。
    // 必须先 CommitPublication 才可暴露；取消/终结/归还后不得使用旧 target 或跨复用缓存。
    // 不是本地 WRITE 的提交许可，不能绕过 Grant/本地 DMA 的独立责任账本。
    urma_sge_t target{};
};
struct GrantConfig {
    std::uint32_t window = 128;
    std::uint32_t max_active = 128;
    std::uint64_t max_bytes = 64u << 20;
    std::uint32_t initial_seq = 0;
};
struct GrantStats {
    std::uint64_t window_begin = 0;
    std::uint64_t bytes = 0;
    std::size_t active = 0;
    std::size_t reserved = 0;
    std::size_t published = 0;
    std::size_t ready = 0;
    std::size_t borrowed = 0;
    std::size_t quarantined = 0;
    std::size_t retired = 0;
};
/// 可信宿主边界：来源已认证，发送端逐 WR 账本证明未来/排队提交均停止、接受数明确且全部终结。
/// 成功 DONE 还须由宿主证明所有非重叠分块完整覆盖授权范围；这些计数不能独立证明覆盖。
/// 不能仅从接收到的任意数字、本地 CQE/flush、断链或超时构造证据。
struct GrantTerminalEvidence {
    GrantIdentity identity;
    bool no_future_writes = false;
    bool acceptance_known = false;
    std::uint64_t accepted_writes = 0;
    std::uint64_t terminal_writes = 0;
    std::uint64_t successful_writes = 0;
    // DONE 必须是完整成功覆盖长度；仅清理的证明可用零表示不携带覆盖统计。
    std::uint64_t completed_bytes = 0;
};

/// 单 owner、固定容量的接收端 WRITE Grant 账本；不发送控制帧、不提交 WRITE、不重放。
/// 一个对象只允许一次成功 Open；完整 binding/arena 代次在旧网络消息可能存在时不得复用。
/// Slab/provider/budget 必须覆盖全部 Grant 和应用借用；失败 Close/析构保留未终结责任。
/// max_bytes 是实际授权长度预算；Slab/MemoryBudget 另行约束全部固定 backing，隔离和应用持有均占准入额度。
/// transfer_id 与 seq 组成复合身份，允许不同 SN 使用相同 transfer_id；统一 Eager/RNDV 顺序占位由宿主负责。
class GrantManager {
public:
    static constexpr std::uint32_t kMaxWindow = 4096;
    GrantManager() noexcept;
    ~GrantManager();
    GrantManager(const GrantManager&) = delete;
    GrantManager& operator=(const GrantManager&) = delete;
    std::expected<void, GrantError> Open(GrantBinding binding, GrantArena arena, memory::SlabAllocator& slab,
                                         urma_context_t* context, const GrantConfig& config) noexcept;
    /// 窗口内允许 RTS 乱序；同 SN 即使 transfer_id/长度不同也拒绝，退休 tombstone 在推进前保留。
    std::expected<GrantReservation, GrantError> Reserve(const GrantTransfer& transfer) noexcept;
    /// 必须先于任何可能暴露 GRANT 的操作调用；之后接受未知与取消均不得撤销远端责任。
    std::expected<void, GrantError> CommitPublication(GrantHandle handle) noexcept;
    /// Reserved 可撤销；Published 仅隔离；Ready 可丢弃；Borrowed 必须先 Return。
    std::expected<void, GrantError> Cancel(GrantHandle handle) noexcept;
    /// 全部成功后进入 Ready；取消后晚到的有效 DONE 仅终结并丢弃，不复活交付。
    std::expected<void, GrantError> CompleteDone(const GrantTerminalEvidence& evidence) noexcept;
    /// 终结证明只允许丢弃；即使全部成功也不交付。零接受合法，但必须禁止后续提交。
    std::expected<void, GrantError> ConfirmTerminal(const GrantTerminalEvidence& evidence) noexcept;
    std::expected<std::span<const std::byte>, GrantError> Borrow(GrantHandle handle) noexcept;
    /// 成功归还后全部 span 副本失效；部分清理失败保留 handle 供重试，不允许重新借用。
    std::expected<void, GrantError> Return(GrantHandle handle) noexcept;
    /// consumed_prefix 为独占下界；宿主已确认其前全部逻辑 SN 消费完毕（包括非 RNDV）。
    /// 只允许推进当前窗口内的空槽/退休槽，遇活跃责任原子拒绝；不因到达顺序推进。
    std::expected<void, GrantError> AdvanceWindow(std::uint64_t consumed_prefix) noexcept;
    /// 只停止新准入，不等于连接失败；失败路径须显式 Cancel，以禁止晚到 DONE 恢复交付。
    void Stop() noexcept;
    /// 仅停止本管理器，绝不停止共享 Slab；仍有活跃责任/应用借用则返回 InUse。
    std::expected<void, GrantError> Close() noexcept;
    GrantStats stats() const noexcept;
    bool accepting() const noexcept;

private:
    struct Entry;
    struct State;
    std::expected<Entry*, GrantError> Find(GrantHandle handle) noexcept;
    std::expected<Entry*, GrantError> Find(const GrantIdentity& identity) noexcept;
    std::expected<void, GrantError> Discard(Entry& entry) noexcept;
    std::expected<void, GrantError> EndRemote(Entry& entry) noexcept;
    std::expected<void, GrantError> ValidateEvidence(const GrantTerminalEvidence& evidence,
                                                     bool success) const noexcept;
    void Retire(Entry& entry) noexcept;
    std::unique_ptr<State> state_;
    std::uint64_t next_generation_ = 0;
    bool opened_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_GRANT_MANAGER_HPP_
