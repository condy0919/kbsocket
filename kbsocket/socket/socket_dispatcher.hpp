// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_SOCKET_SOCKET_DISPATCHER_HPP_
#define KBSOCKET_SOCKET_SOCKET_DISPATCHER_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/api/stream.h"
#include "kbsocket/runtime/progress_source.hpp"
#include "kbsocket/socket/fd_command_queue.hpp"
#include "kbsocket/socket/fd_table.hpp"

namespace kbsocket {
namespace socket {

/// 只由 dispatcher 的唯一 owner 调用；对象及其 Runtime 必须活到 dispatcher Close 成功。
/// 绑定期间禁止宿主绕过 dispatcher 对相同 stream 执行 I/O、Watch、Close 或 Release。
class SocketStreamBackend {
public:
    virtual ~SocketStreamBackend() = default;
    virtual std::expected<kbs_stream_io, kbs_stream_error> Write(kbs_stream_handle stream,
                                                                 std::span<const std::byte> input) noexcept = 0;
    virtual std::expected<kbs_stream_io, kbs_stream_error> Read(kbs_stream_handle stream, std::span<std::byte> output,
                                                                std::uint64_t now_ns) noexcept = 0;
    virtual std::expected<void, kbs_stream_error> ShutdownWrite(kbs_stream_handle stream) noexcept = 0;
    virtual std::expected<kbs_stream_status, kbs_stream_error> Inspect(kbs_stream_handle stream) noexcept = 0;
    virtual std::expected<void, kbs_stream_error> CloseStream(kbs_stream_handle stream) noexcept = 0;
    virtual std::expected<void, kbs_stream_error> Release(kbs_stream_handle stream) noexcept = 0;
    // 默认不支持事件，保持只提供 I/O 的 backend 可用；开启事件要求独占消费其整张 stream 表事件。
    virtual bool supports_events() const noexcept {
        return false;
    }
    virtual std::expected<void, kbs_stream_error> Watch(kbs_stream_handle stream, bool readable,
                                                        bool writable) noexcept;
    virtual std::expected<std::size_t, kbs_stream_error> PollEvents(std::span<kbs_stream_event> output) noexcept;
    virtual const void* exclusive_context() const noexcept = 0;
};

/// C stream 的薄转发，既不拥有表也不推进 Runtime。raw 必须传入真实 context 并作为 companion 注册。
class CStreamBackend final : public SocketStreamBackend {
public:
    explicit CStreamBackend(kbs_stream_table* table, const void* context = nullptr) noexcept;
    std::expected<kbs_stream_io, kbs_stream_error> Write(kbs_stream_handle stream,
                                                         std::span<const std::byte> input) noexcept override;
    std::expected<kbs_stream_io, kbs_stream_error> Read(kbs_stream_handle stream, std::span<std::byte> output,
                                                        std::uint64_t now_ns) noexcept override;
    std::expected<void, kbs_stream_error> ShutdownWrite(kbs_stream_handle stream) noexcept override;
    std::expected<kbs_stream_status, kbs_stream_error> Inspect(kbs_stream_handle stream) noexcept override;
    std::expected<void, kbs_stream_error> CloseStream(kbs_stream_handle stream) noexcept override;
    std::expected<void, kbs_stream_error> Release(kbs_stream_handle stream) noexcept override;
    bool supports_events() const noexcept override;
    std::expected<void, kbs_stream_error> Watch(kbs_stream_handle stream, bool readable,
                                                bool writable) noexcept override;
    std::expected<std::size_t, kbs_stream_error> PollEvents(std::span<kbs_stream_event> output) noexcept override;
    const void* exclusive_context() const noexcept override;

private:
    kbs_stream_table* table_;
    const void* context_;
};

struct SocketDispatcherConfig {
    std::uint64_t owner_id = 0;
    FdTableConfig bindings;
    FdCommandQueueConfig commands;
    // 按 stream.slot 直接索引，宿主配置为底层 stream 表容量；记录单独受硬预算约束。
    std::uint32_t stream_capacity = 1024;
    std::size_t record_budget_bytes = 1024u * 1024u;
    // 开启后独占 backend 的整表事件消费；未 Attach 的 stream 提示会计为 unmapped，不转交其他消费者。
    bool events = false;
};
enum class SocketDispatchErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kNoMemory,
    kBudgetExceeded,
    kFd,
    kQueue,
    kStream,
    kSystem,
    kRetained,
    kNotQuiescent
};
struct SocketDispatchError {
    SocketDispatchErrorCode code;
    FdError fd = FdError::kInvalidArgument;
    FdQueueError queue{};
    kbs_stream_error stream{};
    int system_error = 0;
};
enum class SocketRetireStage : std::uint8_t { kActive, kClose, kRelease, kConfirmed };
struct SocketBindingStatus {
    FdBinding binding;
    SocketRetireStage stage = SocketRetireStage::kActive;
    kbs_stream_error last_error{};
    int fd_close_error = 0;
};
/// 原始提示与完整绑定身份；无订阅代次。取消/重 arm 不撤回同一流已经排队的提示。
/// 取出后可能立即发生关闭/复用；应用必须用 binding.id 重新 Lookup，不能只读裸 fd。
struct FdEvent {
    FdBinding binding;
    kbs_stream_event event{};
};
struct SocketDispatcherStats {
    FdTableStats bindings;
    std::size_t commands = 0;
    std::size_t closing = 0;
    std::size_t releasing = 0;
    std::size_t confirmed = 0;
    std::size_t record_bytes = 0;
    std::size_t pending_events = 0;
    std::uint64_t mapped_events = 0, unmapped_events = 0;
    // 最近一次 backend 事件收集错误；成功收集后清零，不推断 DMA/连接已终结。
    kbs_stream_error event_error{};
};

/// 有界跨线程请求桥；不是 POSIX syscall 替换层。Submit 成功只表示入队，实际 I/O 结果由 Take 领取。
/// Attach 在控制路径预留独立 eventfd 作为公开 fd；该 fd 仅保留身份，不能当作流 readiness 或直接读写。
/// 传输 socket 继续归 Runtime 所有。Attach 成功转交 stream 管理权，失败则仍归宿主。
/// Close 命令先解绑/关闭公开 fd，完成代表逻辑关闭；真实流通过 owner 控制入口 RetireStreams 的 Close/Release 收敛。
/// Retained 和真实诊断由 InspectBinding 可见，仍须宿主提供 raw 来源退休等独立证明。
/// 除 RegisterReader/Lookup/Submit/Take 可并发外，其他操作均为单 owner。
/// raw 必须注册到同 context 暂停组，宿主不能单独绕过暂停推进。Open/Close/析构排除所有外部调用。
class SocketDispatcher final : public runtime::ProgressSource {
public:
    SocketDispatcher() noexcept;
    ~SocketDispatcher();
    SocketDispatcher(const SocketDispatcher&) = delete;
    SocketDispatcher& operator=(const SocketDispatcher&) = delete;
    std::expected<void, SocketDispatchError> Open(SocketStreamBackend& backend,
                                                  const SocketDispatcherConfig& config) noexcept;
    std::expected<FdBindingId, SocketDispatchError> Attach(kbs_stream_handle stream) noexcept;
    std::expected<void, SocketDispatchError> Detach(FdBindingId binding) noexcept;
    /// owner 控制路径，不从 reactor 的热 Progress 调用；实际 Close/Release 可能释放连接堆存储。
    /// 每条退休记录每次至多尝试一个阶段，错误保留于 InspectBinding；不重试业务 I/O。
    std::expected<runtime::ProgressBatch, SocketDispatchError> RetireStreams(std::size_t budget) noexcept;
    std::expected<SocketBindingStatus, SocketDispatchError> InspectBinding(FdBindingId binding) const noexcept;
    std::expected<FdReader, FdError> RegisterReader() noexcept;
    std::expected<FdLease, FdError> Lookup(FdReader& reader, int fd) noexcept;
    std::expected<FdLease, FdError> Lookup(FdReader& reader, FdBindingId binding) noexcept;
    // 入队失败保留传入 Lease；成功移交至结果领取，不访问后端或实际流。
    std::expected<FdCommandTicket, SocketDispatchError> TrySubmit(FdLease& lease, const FdCommand& command,
                                                                  std::span<const std::byte> input = {}) noexcept;
    std::expected<FdCommandCompletion, FdQueueError> TakeCompletion(FdCommandTicket ticket,
                                                                    std::span<std::byte> output = {}) noexcept;
    /// 唯一 owner 领取合并后的提示，每次最多 64 条；不推进 Runtime，不调用 Watch。
    /// flags 为待取期间历史并集，快照为最新值，TX_RETIRED 饱和累加；不是持续 readiness 电平。
    /// 宿主每轮必须领取并分发；本地尚有提示时 CollectNotifications 持续报告 runnable。
    std::expected<std::size_t, SocketDispatchError> PollEvents(std::span<FdEvent> output) noexcept;
    bool events_pending() const noexcept;
    void StopAccepting() noexcept;
    // 停准入后仍须 Detach 全部绑定、推进退休并领取全部结果；最后退出/销毁全部 Reader/Lease。
    // Close 失败保留完整 State 和借用依赖要求，绝不主动关闭后端 Runtime。
    std::expected<void, SocketDispatchError> Close(bool clients_stopped = false) noexcept;
    SocketDispatcherStats stats() const noexcept;
    std::expected<runtime::ProgressBatch, runtime::ProgressError>
    Progress(runtime::ProgressKind kind, std::size_t budget, std::uint64_t now_ns) noexcept override;
    std::expected<runtime::ProgressBatch, runtime::ProgressError> CollectNotifications(std::size_t) noexcept override;
    std::expected<void, runtime::ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override;
    const void* exclusive_context() const noexcept override;

private:
    struct State;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
    FdCommandResult Execute(const FdCommandRequest& request, std::uint64_t now_ns) noexcept;
};

} // namespace socket
} // namespace kbsocket
#endif // KBSOCKET_SOCKET_SOCKET_DISPATCHER_HPP_
