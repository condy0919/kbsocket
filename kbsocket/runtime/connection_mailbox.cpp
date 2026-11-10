// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_mailbox.hpp"

#include <algorithm>
#include <chrono>

namespace kbsocket {
namespace runtime {
namespace {
auto Error(CommandQueueErrorCode code) noexcept {
    return std::unexpected(CommandQueueError{code});
}
auto ProgressFailure(const CommandQueueError& error) noexcept {
    return std::unexpected(ProgressError{error.code == CommandQueueErrorCode::kSystem ? ProgressErrorCode::kSystem
                                                                                      : ProgressErrorCode::kRetained,
                                         error.system_error ? error.system_error : static_cast<int>(error.code)});
}
ConnectionCommandResult VoidReply(const std::expected<void, ConnectionOwnerError>& result) noexcept {
    if (!result) {
        return std::unexpected(result.error());
    }
    return ConnectionCommandReply{};
}
} // namespace

std::expected<void, CommandQueueError> ConnectionMailbox::Open(ConnectionOwner& owner,
                                                               const ConnectionMailboxConfig& config,
                                                               ConnectionMailboxCallback callback,
                                                               void* cookie) noexcept {
    if (owner_) {
        return Error(CommandQueueErrorCode::kInUse);
    }
    if (!owner.stats().capacity) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
    }
    auto opened = queue_.Open(config.capacity, config.max_io_bytes, config.payload_budget_bytes);
    if (!opened) {
        return opened;
    }
    owner_ = &owner;
    context_ = owner.exclusive_context();
    callback_ = callback;
    cookie_ = cookie;
    fd_[0] = queue_.command_fd();
    return {};
}

std::expected<ConnectionCommandTicket, CommandQueueError>
ConnectionMailbox::TrySubmit(const ConnectionCommand& command, std::span<const std::byte> input) noexcept {
    if (!owner_) {
        return Error(CommandQueueErrorCode::kNotReady);
    }
    // producer 仅检查不可变的 owner 身份，连接是否仍有效由唯一 owner 执行时核验。
    if (command.connection.owner != owner_) {
        return Error(CommandQueueErrorCode::kInvalidArgument);
    }
    return queue_.TrySubmit(command, input);
}
std::expected<ConnectionCommandCompletion, CommandQueueError>
ConnectionMailbox::TakeCompletion(ConnectionCommandTicket ticket, std::span<std::byte> output) noexcept {
    return queue_.TakeCompletion(ticket, output);
}
void ConnectionMailbox::StopAccepting() noexcept {
    queue_.StopAccepting();
}
std::expected<void, CommandQueueError> ConnectionMailbox::Close(bool clients_stopped) noexcept {
    auto closed = queue_.Close(clients_stopped);
    if (!closed) {
        return closed;
    }
    owner_ = nullptr;
    context_ = nullptr;
    callback_ = nullptr;
    cookie_ = nullptr;
    fd_[0] = -1;
    return {};
}

ConnectionCommandResult ConnectionMailbox::Execute(const ConnectionCommandRequest& request,
                                                   std::uint64_t now_ns) noexcept {
    const auto& command = request.command;
    const auto handle = command.connection;
    switch (command.kind) {
    case ConnectionCommandKind::kWrite: {
        const auto written = owner_->Write(handle, request.payload);
        if (!written) {
            return std::unexpected(written.error());
        }
        ConnectionCommandReply reply;
        reply.bytes = *written;
        return reply;
    }
    case ConnectionCommandKind::kRead: {
        // 同一 Tick 的 RX/连接推进可能已观察到更新时刻；批次时间只能作为下界。
        const auto current = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
        const auto read = owner_->Read(handle, request.payload, std::max(now_ns, current));
        if (!read) {
            return std::unexpected(read.error());
        }
        ConnectionCommandReply reply;
        reply.bytes = read->bytes;
        reply.eof = read->eof;
        return reply;
    }
    case ConnectionCommandKind::kShutdownWrite:
        return VoidReply(owner_->ShutdownWrite(handle));
    case ConnectionCommandKind::kCancel:
        return VoidReply(owner_->Cancel(handle));
    case ConnectionCommandKind::kWatchTcp:
        return VoidReply(owner_->WatchTcp(handle, command.readable, command.writable));
    case ConnectionCommandKind::kInspect: {
        const auto status = owner_->Inspect(handle);
        if (!status) {
            return std::unexpected(status.error());
        }
        ConnectionCommandReply reply;
        reply.status = *status;
        return reply;
    }
    }
    return std::unexpected(ConnectionOwnerError{.code = ConnectionOwnerErrorCode::kInvalidArgument});
}

std::expected<ProgressBatch, ProgressError> ConnectionMailbox::Progress(ProgressKind kind, std::size_t budget,
                                                                        std::uint64_t now_ns) noexcept {
    if (!owner_) {
        return std::unexpected(ProgressError{ProgressErrorCode::kNotReady});
    }
    if (kind != ProgressKind::kMailbox || !budget) {
        return ProgressBatch{};
    }
    // 每次额外设定固定批次上限，禁止调用方的大预算形成无界 owner 占用。
    std::array<ConnectionCommandRequest, 16> requests{};
    const auto count = queue_.PollCommands(std::span(requests).first(std::min(budget, requests.size())));
    if (!count) {
        return ProgressFailure(count.error());
    }
    for (std::size_t i = 0; i < *count; ++i) {
        const auto& request = requests[i];
        // Complete 后 collector 可立刻复用槽；通知只使用发布前复制的身份，不触碰旧 payload。
        const ConnectionMailboxNotice notice{request.ticket, request.command.connection, request.command.token};
        const auto result = Execute(request, now_ns);
        const auto completed = queue_.Complete(request.ticket, result);
        if (!completed) {
            return ProgressFailure(completed.error());
        }
        if (callback_) {
            callback_(cookie_, notice);
        }
    }
    return ProgressBatch{*count, queue_.runnable()};
}

} // namespace runtime
} // namespace kbsocket
