// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_SOCKET_FD_COMMAND_QUEUE_HPP_
#define KBSOCKET_SOCKET_FD_COMMAND_QUEUE_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/api/stream.h"
#include "kbsocket/socket/fd_table.hpp"

namespace kbsocket {
namespace socket {

enum class FdCommandKind : std::uint8_t { kWrite, kRead, kShutdownWrite, kInspect, kClose };
struct FdCommand {
    FdCommandKind kind = FdCommandKind::kInspect;
    std::uint64_t token = 0;
    std::uint32_t bytes = 0;
};
struct FdCommandTicket {
    std::uint64_t queue_id = 0;
    std::uint64_t sequence = 0;
    bool operator==(const FdCommandTicket&) const = default;
};
struct FdCommandReply {
    kbs_stream_io io{};
    kbs_stream_status status{};
};
using FdCommandResult = std::expected<FdCommandReply, kbs_stream_error>;
struct FdCommandCompletion {
    FdCommandTicket ticket;
    FdCommand command;
    FdBinding binding;
    FdCommandResult result;
};
struct FdCommandRequest {
    FdCommandTicket ticket;
    FdCommand command;
    FdBinding binding;
    // 仅供 owner 访问至 Complete；Write 是已复制输入，Read 是结果暂存区。
    std::span<std::byte> payload;
};
struct FdCommandQueueConfig {
    // 宿主保证进程内唯一且不复用；成功 Open 后不能重新 Open 同一队列对象。
    std::uint64_t queue_id = 0;
    std::size_t capacity = 256;
    std::size_t max_io_bytes = 4096;
    // 只覆盖按 64 B 对齐的 payload backing；固定元数据另由 capacity 约束。
    std::size_t payload_budget_bytes = 16u * 1024u * 1024u;
};
enum class FdQueueErrorCode : std::uint8_t {
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
    kSystem,
    kContended
};
struct FdQueueError {
    FdQueueErrorCode code;
    int system_error = 0;
};

/// 有界 MPSC 调度环；应用线程只复制数据并移动 FD 元数据 Lease，不访问实际 stream。
/// TrySubmit 成功仅表示入队。执行结果和 Lease 占槽至 TakeCompletion，不自动重试短 I/O。
/// 多生产者、多个完成领取者可并发；PollCommands/Complete/runnable 仅由唯一 owner 调用。
/// Open/Close/析构须与所有外部调用互斥；Close(true) 还要求 producer/collector 已退出。
class FdCommandQueue {
public:
    static constexpr std::size_t kMaxCapacity = 65536;
    static constexpr std::size_t kMaxPayloadBytes = 1024u * 1024u;
    static constexpr std::size_t kMaxPollBatch = 64;
    FdCommandQueue() noexcept;
    ~FdCommandQueue();
    FdCommandQueue(const FdCommandQueue&) = delete;
    FdCommandQueue& operator=(const FdCommandQueue&) = delete;
    /// Open 一次预分配，容量向上取至少 2 的二次幂；后续路径不分配、不加锁。
    std::expected<void, FdQueueError> Open(const FdCommandQueueConfig& config) noexcept;
    /// 仅成功预留后移动 lease；失败保留原 Lease，不借 current() 推断 owner 侧目标可用。
    std::expected<FdCommandTicket, FdQueueError> TrySubmit(FdLease& lease, const FdCommand& command,
                                                           std::span<const std::byte> input = {}) noexcept;
    /// Read output 太小时保留完整结果及 Lease。一个 ticket 至多被成功领取一次。
    std::expected<FdCommandCompletion, FdQueueError> TakeCompletion(FdCommandTicket ticket,
                                                                    std::span<std::byte> output = {}) noexcept;
    /// 仅出队连续已发布前缀，每次最多 kMaxPollBatch；预算耗尽保留 eventfd readiness。
    std::expected<std::size_t, FdQueueError> PollCommands(std::span<FdCommandRequest> requests) noexcept;
    /// 发布后 request.payload 失效；外部不能再访问请求引用或暂存区。
    std::expected<void, FdQueueError> Complete(FdCommandTicket ticket, const FdCommandResult& result) noexcept;
    /// 已取得预留资格的并发生产者仍可发布；停止后必须等待调用者退出再 Close。
    void StopAccepting() noexcept;
    std::expected<void, FdQueueError> Close(bool clients_stopped = false) noexcept;
    int command_fd() const noexcept;
    std::size_t capacity() const noexcept;
    std::size_t max_io_bytes() const noexcept;
    std::size_t outstanding() const noexcept;
    bool accepting() const noexcept;
    bool runnable() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};

} // namespace socket
} // namespace kbsocket
#endif // KBSOCKET_SOCKET_FD_COMMAND_QUEUE_HPP_
