// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_SOCKET_FD_TABLE_HPP_
#define KBSOCKET_SOCKET_FD_TABLE_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <span>

#include "kbsocket/api/stream.h"

namespace kbsocket {
namespace socket {
namespace detail {
struct FdTableState;
struct FdTableEntry;
} // namespace detail

struct FdBindingId {
    std::uint64_t table_id = 0;
    std::int32_t fd = -1;
    std::uint32_t generation = 0;
    bool operator==(const FdBindingId&) const = default;
};
/// 只保存完整身份，不授予跨线程直接调用 C stream 或访问 owner 对象的权利。
struct FdTarget {
    std::uint64_t owner_id = 0;
    kbs_stream_handle stream{};
};
struct FdBinding {
    FdBindingId id;
    FdTarget target;
};
struct FdRetireToken {
    FdBindingId id;
    std::uint32_t slot = 0;
    bool operator==(const FdRetireToken&) const = default;
};
struct FdTableConfig {
    // 宿主保证进程内不复用表身份；同一对象只允许成功 Open 一次。
    std::uint64_t table_id = 0;
    std::uint32_t fd_limit = 65536;
    // 当前绑定与未回收旧绑定共同占用容量，不按 fd 数推断旧引用已消失。
    std::uint32_t binding_capacity = 1024;
    std::uint32_t reader_capacity = 64;
    std::uint32_t initial_generation = 1;
    std::uint32_t max_references = std::numeric_limits<std::uint32_t>::max();
    std::size_t memory_budget_bytes = 32u * 1024u * 1024u;
};
enum class FdError : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kBudgetExceeded,
    kStopped,
    kFull,
    kReaderFull,
    kReferenceLimit,
    kGenerationExhausted,
    kNotBound,
    kStaleBinding,
    kInvalidReader,
    kInvalidRetirement,
    kContended,
    kRetained,
    kNotQuiescent
};
struct FdTableStats {
    std::size_t active = 0;
    std::size_t retired = 0;
    std::size_t available = 0;
    std::size_t readers = 0;
    std::size_t references = 0;
    std::size_t storage_bytes = 0;
    bool accepting = false;
};
class FdTable;
/// 一个调用线程独占的 Hazard 槽；可移动交接，不能同时由多个线程使用。
/// Lookup 返回前已清除 Hazard，等待异步操作只能持有 FdLease。
class FdReader {
public:
    FdReader() noexcept = default;
    ~FdReader();
    FdReader(FdReader&& other) noexcept;
    FdReader& operator=(FdReader&& other) noexcept;
    FdReader(const FdReader&) = delete;
    FdReader& operator=(const FdReader&) = delete;
    explicit operator bool() const noexcept;
    void Reset() noexcept;

private:
    friend class FdTable;
    FdReader(detail::FdTableState* state, std::uint32_t slot) noexcept;
    detail::FdTableState* state_ = nullptr;
    std::uint32_t slot_ = 0;
};
/// 固定旧绑定的 CPU 引用；可移动给 Mailbox 操作/回调，直到这些使用者完成才归还。
/// 不保留 Hazard、不证明 DMA/Grant 已终结，也不会自动关闭流或内核 fd。
class FdLease {
public:
    FdLease() noexcept = default;
    ~FdLease();
    FdLease(FdLease&& other) noexcept;
    FdLease& operator=(FdLease&& other) noexcept;
    FdLease(const FdLease&) = delete;
    FdLease& operator=(const FdLease&) = delete;
    explicit operator bool() const noexcept;
    /// 仅有效 Lease 可读；返回引用只活到本 Lease Reset/销毁。
    const FdBinding& binding() const noexcept;
    /// 瞬时准入快照，不能代替 owner 对已排队操作的关闭/身份核验。
    bool current() const noexcept;
    void Reset() noexcept;

private:
    friend class FdTable;
    FdLease(detail::FdTableState* state, detail::FdTableEntry* entry) noexcept;
    detail::FdTableState* state_ = nullptr;
    detail::FdTableEntry* entry_ = nullptr;
};

/// 固定两级 fd 目录、绑定池与 Hazard 注册表。Open 预分配全部存储，后续操作不分配/加锁。
/// Publish/Unbind/ConfirmClosed/Collect/stats 由唯一 owner 串行调用；Lookup 和 RegisterReader
/// 可与它们并发。Open/Close/析构要求外部排除正在进入门面的调用。
/// 不拥有内核 fd、stream 或 owner；这些资源的真实关闭及其依赖仍由宿主管理。
class FdTable {
public:
    static constexpr std::uint32_t kPageSize = 256;
    static constexpr std::uint32_t kMaxFdLimit = 1u << 20;
    static constexpr std::uint32_t kMaxBindings = 1u << 20;
    static constexpr std::uint32_t kMaxReaders = 4096;
    static constexpr std::size_t kMaxCollectBudget = 4096;
    FdTable() noexcept;
    ~FdTable();
    FdTable(const FdTable&) = delete;
    FdTable& operator=(const FdTable&) = delete;
    std::expected<void, FdError> Open(const FdTableConfig& config) noexcept;
    std::expected<FdBindingId, FdError> Publish(int fd, FdTarget target) noexcept;
    /// 解绑立即使新 Lookup 不再命中旧代；已成功取得的引用继续保留原目标。
    std::expected<FdRetireToken, FdError> Unbind(FdBindingId id) noexcept;
    /// 只有宿主确认真实 stream Release 及所有 owner 侧外部责任已结束后才能调用。
    /// 此确认不替代 Hazard/Lease 核验，Collect 仍等待最后 CPU 使用者退出。
    std::expected<void, FdError> ConfirmClosed(FdRetireToken token) noexcept;
    /// 有界扫描 owner 退休队列；输出仅表示绑定元数据可回收，不是硬件静默证明。
    std::expected<std::size_t, FdError> Collect(std::span<FdBinding> output, std::size_t budget) noexcept;
    std::expected<FdReader, FdError> RegisterReader() noexcept;
    std::expected<FdLease, FdError> Lookup(FdReader& reader, int fd) noexcept;
    /// 异步事件必须使用完整旧身份，禁止重新按裸 fd 解释迟到操作。
    std::expected<FdLease, FdError> Lookup(FdReader& reader, FdBindingId expected) noexcept;
    /// 可与 Lookup/注册并发；此前越过最后检查的 Lookup 仍可能返回原绑定引用。
    void StopAccepting() noexcept;
    /// 先停止外部调用者、销毁全部 Reader/Lease、解绑并确认/收集全部退休项。
    /// 无确认就析构将保留 State，使迟归还句柄安全；宿主仍须保留实际流/owner 依赖。
    std::expected<void, FdError> Close(bool clients_stopped = false) noexcept;
    FdTableStats stats() const noexcept;

private:
    std::expected<FdLease, FdError> LookupImpl(FdReader& reader, int fd, std::uint32_t generation) noexcept;
    std::unique_ptr<detail::FdTableState> state_;
    bool opened_once_ = false;
};

} // namespace socket
} // namespace kbsocket
#endif // KBSOCKET_SOCKET_FD_TABLE_HPP_
