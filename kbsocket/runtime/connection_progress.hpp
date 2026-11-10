// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_CONNECTION_PROGRESS_HPP_
#define KBSOCKET_RUNTIME_CONNECTION_PROGRESS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>

#include "kbsocket/connection/connection.hpp"
#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace runtime {
class ConnectionProgress;
struct ScheduledConnection {
    const ConnectionProgress* owner = nullptr;
    std::uint64_t id = 0;
    bool operator==(const ScheduledConnection&) const = default;
    /// 只解码槽下标，不访问 owner；使用者仍须比对完整 owner/id，防止代次复用。
    std::optional<std::uint32_t> slot_index() const noexcept;
};
struct ConnectionProgressConfig {
    std::uint32_t capacity = 4096;
    // raw companion 必须与所属 RawContextBinding.context 一致；nullptr 仅允许 TCP。
    urma_context_t* context = nullptr;
    std::uint64_t sweep_interval_ns = 1'000'000;
};
struct ConnectionProgressEvent {
    ScheduledConnection connection;
    connection::ConnectionState state;
    // NativeTCP 的一次性 readiness；宿主 Read/Write 后显式 WatchTcp 重新订阅。
    std::uint32_t fd_events = 0;
    // RX 驻留变化可能含乱序帧，Read 仍可能 WouldBlock；此通知不绕过按序交付门禁。
    bool rx_changed = false;
    std::size_t rx_pending = 0;
    // 自上次回调以来真实退休的 WR 数；Closing/Failed 也报告，供宿主重试保留中的 Close。
    std::uint64_t tx_retired = 0;
    connection::ConnectionReadiness readiness = {};
    std::optional<connection::ConnectionError> failure = {};
};
using ConnectionProgressCallback = void (*)(void*, const ConnectionProgressEvent&) noexcept;
struct ConnectionProgressStats {
    std::uint64_t visits = 0, fd_events = 0, stale_events = 0, failures = 0;
    std::size_t attached = 0;
};

/// 单 owner 连接进度源，借用 Connection；与同 context 的 raw/import 源组成 ContextProgressGroup。
/// 固定槽、就绪队列和有界巡检；动态 fd 由内部 epoll 汇聚为一个固定 Reactor fd。
/// Attach 在 PrepareRaw 前执行，其 id 必须用作 RawConnectionPreparation.connection_id。
/// id 高两位保留给 RNDV 子 owner；完成分发还须校验完整代次、连接登记身份与本地 context。
/// 应用 Write/Read/Prepare/Shutdown 后 Wake；跨线程操作先经宿主 mailbox 转交 owner。
/// 回调只报告状态/就绪，不得重入或销毁调度器。context 暂停期间宿主不得调用本类或借用连接。
/// 关闭连接后仍保留注册直到 Close 成功，确保迟到 TX 终结继续按完整 id 分发。
class ConnectionProgress final : public ProgressSource {
public:
    static constexpr std::uint32_t kMaxConnections = 65536;
    ConnectionProgress() noexcept;
    ~ConnectionProgress() override;
    std::expected<void, ProgressError> Open(const ConnectionProgressConfig& config = {},
                                            ConnectionProgressCallback callback = nullptr,
                                            void* cookie = nullptr) noexcept;
    std::expected<ScheduledConnection, ProgressError> Attach(connection::Connection& connection) noexcept;
    /// 控制路径检查当前已附加连接是否拥有相同 fd 数值，供先 Attach 空对象再接管 fd 的入口使用。
    /// 单 owner 调用，不替代 fd 所有权协议，也不识别 dup 后不同数值的描述符别名。
    bool HasFd(int fd) const noexcept;
    std::expected<void, ProgressError> Detach(ScheduledConnection connection) noexcept;
    std::expected<void, ProgressError> Wake(ScheduledConnection connection) noexcept;
    std::expected<void, ProgressError> WatchTcp(ScheduledConnection connection, bool read, bool write) noexcept;
    std::expected<void, ProgressError> OnTxCompletion(const raw::TxCompletionEvent& event) noexcept;
    // RX/Credit 和补投影响共享 Peer 共账，启动一轮有预算的巡检，合并重复请求。
    std::expected<void, ProgressError> RequestSweep() noexcept;
    std::expected<void, ProgressError> AddReceiveDomain(connection::EagerReceiveDomain& domain) noexcept;
    /// 控制路径移除同一对象的补投注册，不关闭域或结清 Lease；调用方先停用 raw 回调和相关借用。
    /// 调度器已 Close 时注册表已清空，允许最终解绑；调度器仍开着但未登记该对象则拒绝。
    std::expected<void, ProgressError> RemoveReceiveDomain(connection::EagerReceiveDomain& domain) noexcept;
    std::expected<void, ProgressError> Close() noexcept;
    const ConnectionProgressStats* stats() const noexcept;
    std::uint32_t capacity() const noexcept;

    // now_ns 为本轮调度时间下界；实际 Connection 访问刷新单调时钟，晚到 RX 不造成时间倒退。
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind, std::size_t, std::uint64_t) noexcept override;
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t) noexcept override;
    std::expected<void, ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override;
    const void* exclusive_context() const noexcept override;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::uint32_t generation_ = 0;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_CONNECTION_PROGRESS_HPP_
