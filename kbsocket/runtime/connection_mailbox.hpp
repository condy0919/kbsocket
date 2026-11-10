// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_CONNECTION_MAILBOX_HPP_
#define KBSOCKET_RUNTIME_CONNECTION_MAILBOX_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "kbsocket/runtime/connection_command_queue.hpp"
#include "kbsocket/runtime/connection_owner.hpp"
#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace runtime {

struct ConnectionMailboxConfig {
    std::size_t capacity = 256;
    std::size_t max_io_bytes = ConnectionCommandQueue::kMaxIoBytes;
    // 单个 mailbox 的 payload backing 上限，按 64 B 槽步长对齐，不含固定槽元数据。
    std::size_t payload_budget_bytes = ConnectionCommandQueue::kMaxCapacity * ConnectionCommandQueue::kMaxIoBytes;
};
struct ConnectionMailboxNotice {
    ConnectionCommandTicket ticket;
    OwnedConnection connection;
    std::uint64_t token = 0;
};
using ConnectionMailboxCallback = void (*)(void*, const ConnectionMailboxNotice&) noexcept;

/// typed 业务命令到单 owner 的桥接：Read/Write 仅尝试一次，短操作和 WouldBlock 原样交回。
/// TrySubmit/TakeCompletion 允许客户端并发，成功 Submit 只表示复制入队，不表示数据已经发送。
/// 完成结果占槽直至领取；callback 在 owner 上仅通知 ticket/token，不得阻塞/重入/销毁本源。
/// raw mailbox 必须作为 RuntimeShard 的同 context companion 注册，不能独立绕过暂停屏障。
/// Open/Close 和 Progress 由 owner 调用；先停止并等待客户端退出，执行/领取全部命令，解绑后 Close(true)。
/// 不提供跨线程 Adopt/Prepare/Close/Release：这些控制路径仍由宿主驱动，不在热循环分配/释放对象。
class ConnectionMailbox final : public ProgressSource {
public:
    std::expected<void, CommandQueueError> Open(ConnectionOwner& owner, const ConnectionMailboxConfig& config = {},
                                                ConnectionMailboxCallback callback = nullptr,
                                                void* cookie = nullptr) noexcept;
    std::expected<ConnectionCommandTicket, CommandQueueError> TrySubmit(const ConnectionCommand& command,
                                                                        std::span<const std::byte> input = {}) noexcept;
    std::expected<ConnectionCommandCompletion, CommandQueueError>
    TakeCompletion(ConnectionCommandTicket ticket, std::span<std::byte> output = {}) noexcept;
    void StopAccepting() noexcept;
    std::expected<void, CommandQueueError> Close(bool clients_stopped = false) noexcept;
    std::size_t outstanding() const noexcept {
        return queue_.outstanding();
    }
    std::size_t capacity() const noexcept {
        return queue_.capacity();
    }
    std::size_t max_io_bytes() const noexcept {
        return queue_.max_io_bytes();
    }
    bool accepting() const noexcept {
        return queue_.accepting();
    }
    std::expected<ProgressBatch, ProgressError> Progress(ProgressKind kind, std::size_t budget,
                                                         std::uint64_t now_ns) noexcept override;
    std::expected<ProgressBatch, ProgressError> CollectNotifications(std::size_t) noexcept override {
        return ProgressBatch{};
    }
    std::expected<void, ProgressError> Rearm() noexcept override {
        return {};
    }
    std::span<const int> wait_fds() const noexcept override {
        return owner_ ? std::span(fd_) : std::span<const int>{};
    }
    const void* exclusive_context() const noexcept override {
        return context_;
    }

private:
    ConnectionCommandResult Execute(const ConnectionCommandRequest& request, std::uint64_t now_ns) noexcept;
    ConnectionCommandQueue queue_;
    ConnectionOwner* owner_ = nullptr;
    const void* context_ = nullptr;
    ConnectionMailboxCallback callback_ = nullptr;
    void* cookie_ = nullptr;
    std::array<int, 1> fd_{};
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_CONNECTION_MAILBOX_HPP_
