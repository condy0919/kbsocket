// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_CONNECTION_COMMAND_QUEUE_HPP_
#define KBSOCKET_RUNTIME_CONNECTION_COMMAND_QUEUE_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "kbsocket/runtime/connection_owner.hpp"

namespace kbsocket {
namespace runtime {

enum class ConnectionCommandKind : std::uint8_t { kWrite, kRead, kShutdownWrite, kCancel, kInspect, kWatchTcp };
struct ConnectionCommand {
    ConnectionCommandKind kind = ConnectionCommandKind::kInspect;
    OwnedConnection connection;
    std::uint64_t token = 0;
    std::uint32_t bytes = 0;
    bool readable = false;
    bool writable = false;
};
class ConnectionCommandQueue;
struct ConnectionCommandTicket {
    const ConnectionCommandQueue* queue = nullptr;
    std::uint64_t sequence = 0;
    bool operator==(const ConnectionCommandTicket&) const = default;
};
struct ConnectionCommandReply {
    std::size_t bytes = 0;
    bool eof = false;
    std::optional<OwnedConnectionStatus> status;
};
using ConnectionCommandResult = std::expected<ConnectionCommandReply, ConnectionOwnerError>;
struct ConnectionCommandCompletion {
    ConnectionCommandTicket ticket;
    ConnectionCommand command;
    ConnectionCommandResult result;
};
struct ConnectionCommandRequest {
    ConnectionCommandTicket ticket;
    ConnectionCommand command;
    // 只供唯一 owner 使用至 Complete；Write 为已复制输入，Read 为结果暂存区。
    std::span<std::byte> payload;
};
enum class CommandQueueErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kStopped,
    kFull,
    kSequenceExhausted,
    kInvalidTicket,
    kPending,
    kOutputTooSmall,
    kNotQuiescent,
    kSystem
};
struct CommandQueueError {
    CommandQueueErrorCode code;
    int system_error = 0;
};

/// 有界 MPSC 命令环：提交、执行、完成未领取共同占用槽，消费者领取完成后才允许复用。
/// 生产者只复制命令/Write 数据，不访问 ConnectionOwner；单生产者顺序及队列预留顺序保持。
/// 多 collector 可并发领取不同 ticket；同 ticket 仅一份成功，结果复制期间槽不得被重用。
/// Open/Close 不与客户端并发；Close(true) 必须证明所有 producer/collector 已退出。
class ConnectionCommandQueue {
public:
    static constexpr std::size_t kMaxCapacity = 65536;
    static constexpr std::size_t kMaxIoBytes = 4096;
    ConnectionCommandQueue() noexcept;
    ~ConnectionCommandQueue();
    ConnectionCommandQueue(const ConnectionCommandQueue&) = delete;
    ConnectionCommandQueue& operator=(const ConnectionCommandQueue&) = delete;
    /// 容量向上取至少 2 的二次幂；每个槽内固定保存最多 4096 B I/O 数据。
    std::expected<void, CommandQueueError> Open(std::size_t capacity) noexcept;
    std::expected<ConnectionCommandTicket, CommandQueueError> TrySubmit(const ConnectionCommand& command,
                                                                        std::span<const std::byte> input = {}) noexcept;
    /// 未完成返回 kPending，不重新执行命令；Read 的 output 太小时保留原结果。
    std::expected<ConnectionCommandCompletion, CommandQueueError>
    TakeCompletion(ConnectionCommandTicket ticket, std::span<std::byte> output = {}) noexcept;
    /// 唯一 owner；不等未发布槽，按连续前缀出队，预算不足保留 eventfd readiness。
    std::expected<std::size_t, CommandQueueError> PollCommands(std::span<ConnectionCommandRequest> requests) noexcept;
    /// 唯一 owner；发布后 request.payload 即失效，不能再访问槽或引用其 command。
    std::expected<void, CommandQueueError> Complete(ConnectionCommandTicket ticket,
                                                    const ConnectionCommandResult& result) noexcept;
    /// 可与客户端并发；此前已取得预留资格的 Submit 仍可能成功，停止后须等待这些客户端退出。
    void StopAccepting() noexcept;
    std::expected<void, CommandQueueError> Close(bool clients_stopped = false) noexcept;
    int command_fd() const noexcept;
    std::size_t capacity() const noexcept;
    /// 并发提交/领取期间仅为快照，客户端静止后精确；不能单独证明可以释放队列。
    std::size_t outstanding() const noexcept;
    bool accepting() const noexcept;
    /// 唯一 owner 查询连续头部是否已发布，不把 producer 空洞当 runnable。
    bool runnable() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_CONNECTION_COMMAND_QUEUE_HPP_
