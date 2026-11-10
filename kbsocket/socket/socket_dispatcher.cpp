// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/socket/socket_dispatcher.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace socket {
namespace {
auto Error(SocketDispatchErrorCode code, int status = 0) noexcept {
    return std::unexpected(SocketDispatchError{.code = code, .system_error = status});
}
auto FdFailure(FdError error) noexcept {
    return std::unexpected(SocketDispatchError{.code = SocketDispatchErrorCode::kFd, .fd = error});
}
auto QueueFailure(FdQueueError error) noexcept {
    return std::unexpected(SocketDispatchError{.code = SocketDispatchErrorCode::kQueue, .queue = error});
}
auto StreamFailure(kbs_stream_error error) noexcept {
    return std::unexpected(SocketDispatchError{.code = SocketDispatchErrorCode::kStream, .stream = error});
}
kbs_stream_error StreamError(int code, int detail = 0) noexcept {
    return {.code = code,
            .origin = KBS_STREAM_ORIGIN_API,
            .native_code = 0,
            .detail = detail,
            .underlying = 0,
            .reserved = 0};
}
bool SameStream(kbs_stream_handle a, kbs_stream_handle b) noexcept {
    return a.table_id == b.table_id && a.generation == b.generation && a.slot == b.slot && a.reserved == b.reserved;
}
void Signal(int fd) noexcept {
    const std::uint64_t one = 1;
    ssize_t result;
    do {
        result = ::write(fd, &one, sizeof(one));
    } while (result < 0 && errno == EINTR);
    // EAGAIN 表示已经可读。请求/归还一经发布，不能再报告失败诱导业务重放。
}
auto ProgressFailure(FdQueueError error) noexcept {
    return std::unexpected(
        runtime::ProgressError{error.code == FdQueueErrorCode::kSystem ? runtime::ProgressErrorCode::kSystem
                                                                       : runtime::ProgressErrorCode::kRetained,
                               error.system_error ? error.system_error : static_cast<int>(error.code)});
}
} // namespace

CStreamBackend::CStreamBackend(kbs_stream_table* table, const void* context) noexcept
    : table_(table), context_(context) {}
std::expected<kbs_stream_io, kbs_stream_error> CStreamBackend::Write(kbs_stream_handle stream,
                                                                     std::span<const std::byte> input) noexcept {
    kbs_stream_io result{};
    kbs_stream_error error{};
    if (kbs_stream_write(table_, stream, input.data(), input.size(), &result, &error)) {
        return std::unexpected(error);
    }
    return result;
}
std::expected<kbs_stream_io, kbs_stream_error>
CStreamBackend::Read(kbs_stream_handle stream, std::span<std::byte> output, std::uint64_t now_ns) noexcept {
    kbs_stream_io result{};
    kbs_stream_error error{};
    if (kbs_stream_read(table_, stream, output.data(), output.size(), now_ns, &result, &error)) {
        return std::unexpected(error);
    }
    return result;
}
std::expected<void, kbs_stream_error> CStreamBackend::ShutdownWrite(kbs_stream_handle stream) noexcept {
    kbs_stream_error error{};
    if (kbs_stream_shutdown_write(table_, stream, &error)) {
        return std::unexpected(error);
    }
    return {};
}
std::expected<kbs_stream_status, kbs_stream_error> CStreamBackend::Inspect(kbs_stream_handle stream) noexcept {
    kbs_stream_status result{};
    kbs_stream_error error{};
    if (kbs_stream_inspect(table_, stream, &result, &error)) {
        return std::unexpected(error);
    }
    return result;
}
std::expected<void, kbs_stream_error> CStreamBackend::CloseStream(kbs_stream_handle stream) noexcept {
    kbs_stream_error error{};
    if (kbs_stream_close(table_, stream, &error)) {
        return std::unexpected(error);
    }
    return {};
}
std::expected<void, kbs_stream_error> CStreamBackend::Release(kbs_stream_handle stream) noexcept {
    kbs_stream_error error{};
    if (kbs_stream_release(table_, stream, &error)) {
        return std::unexpected(error);
    }
    return {};
}
const void* CStreamBackend::exclusive_context() const noexcept {
    return context_;
}

struct SocketDispatcher::State {
    static constexpr auto kNone = std::numeric_limits<std::uint32_t>::max();
    struct Record {
        std::uint32_t next = kNone;
        bool occupied = false;
        SocketBindingStatus status;
        FdRetireToken retired;
    };
    SocketDispatcherConfig config;
    SocketStreamBackend* backend = nullptr;
    const void* context = nullptr;
    FdTable bindings;
    FdCommandQueue commands;
    std::unique_ptr<Record[]> records;
    std::array<int, 1> fds{-1};
    std::size_t record_bytes = 0, pending = 0, scan_remaining = 0, retired = 0, collect_remaining = 0;
    std::uint32_t head = kNone, tail = kNone;

    ~State() {
        // 仅 Open 回滚或已经确认外部静止的 Close 会销毁 State；不完整析构由门面整体保留。
        (void)commands.Close(true);
        (void)bindings.Close(true);
    }
    void Queue(std::uint32_t index) noexcept {
        records[index].next = kNone;
        if (tail == kNone) {
            head = index;
        } else {
            records[tail].next = index;
        }
        tail = index;
        ++pending;
    }
    Record* Find(const FdBinding& binding) noexcept {
        if (binding.target.owner_id != config.owner_id || binding.target.stream.slot >= config.stream_capacity) {
            return nullptr;
        }
        auto& record = records[binding.target.stream.slot];
        return record.occupied && record.status.binding.id == binding.id &&
                       SameStream(record.status.binding.target.stream, binding.target.stream)
                   ? &record
                   : nullptr;
    }
    std::expected<void, SocketDispatchError> Detach(Record& record) noexcept {
        if (record.status.stage != SocketRetireStage::kActive) {
            return Error(SocketDispatchErrorCode::kStopped);
        }
        const auto unbound = bindings.Unbind(record.status.binding.id);
        if (!unbound) {
            return FdFailure(unbound.error());
        }
        record.retired = *unbound;
        record.status.stage = SocketRetireStage::kClose;
        Queue(record.status.binding.target.stream.slot);
        ++scan_remaining;
        ++retired;
        ++collect_remaining;
        // 不重试 Linux close，即使 EINTR 也可能已经释放了整数 fd；它可能立即被其他线程复用。
        const int status = ::close(record.status.binding.id.fd);
        const int close_error = status < 0 ? errno : 0;
        record.status.fd_close_error = close_error;
        Signal(fds[0]);
        return close_error ? Error(SocketDispatchErrorCode::kSystem, close_error)
                           : std::expected<void, SocketDispatchError>{};
    }
};
SocketDispatcher::SocketDispatcher() noexcept = default;
SocketDispatcher::~SocketDispatcher() {
    StopAccepting();
    // 仅显式 Close(true) 能证明调用者已退出；保留完整关闭记录，不能只留 FD 元数据而丢掉责任。
    if (state_) {
        (void)state_.release();
    }
}
std::expected<void, SocketDispatchError> SocketDispatcher::Open(SocketStreamBackend& backend,
                                                                const SocketDispatcherConfig& config) noexcept {
    if (state_ || opened_once_) {
        return Error(SocketDispatchErrorCode::kInUse);
    }
    if (!config.owner_id || !config.stream_capacity || config.stream_capacity > FdTable::kMaxBindings ||
        !config.record_budget_bytes) {
        return Error(SocketDispatchErrorCode::kInvalidArgument);
    }
    const auto bytes = sizeof(State) + config.stream_capacity * sizeof(State::Record);
    if (bytes > config.record_budget_bytes) {
        return Error(SocketDispatchErrorCode::kBudgetExceeded);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(SocketDispatchErrorCode::kNoMemory);
    }
    state->records.reset(new (std::nothrow) State::Record[config.stream_capacity]);
    if (!state->records) {
        return Error(SocketDispatchErrorCode::kNoMemory);
    }
    const auto bindings = state->bindings.Open(config.bindings);
    if (!bindings) {
        return FdFailure(bindings.error());
    }
    const auto commands = state->commands.Open(config.commands);
    if (!commands) {
        return QueueFailure(commands.error());
    }
    state->backend = &backend;
    state->context = backend.exclusive_context();
    state->config = config;
    state->record_bytes = bytes;
    state->fds[0] = state->commands.command_fd();
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<FdBindingId, SocketDispatchError> SocketDispatcher::Attach(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(SocketDispatchErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (!stream.table_id || !stream.generation || stream.reserved || stream.slot >= state.config.stream_capacity) {
        return Error(SocketDispatchErrorCode::kInvalidArgument);
    }
    if (!state.commands.accepting()) {
        return Error(SocketDispatchErrorCode::kStopped);
    }
    auto& record = state.records[stream.slot];
    if (record.occupied) {
        return Error(SocketDispatchErrorCode::kInUse);
    }
    const auto status = state.backend->Inspect(stream);
    if (!status) {
        return StreamFailure(status.error());
    }
    if (status->state == KBS_STREAM_CLOSING || status->state == KBS_STREAM_STATE_CLOSED) {
        return StreamFailure(StreamError(KBS_STREAM_CLOSED));
    }
    // 控制路径独立预留公开整数，绝不借用或复制 Runtime 拥有的传输 socket。
    const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) {
        return Error(SocketDispatchErrorCode::kSystem, errno);
    }
    const FdTarget target{state.config.owner_id, stream};
    const auto binding = state.bindings.Publish(fd, target);
    if (!binding) {
        (void)::close(fd);
        return FdFailure(binding.error());
    }
    record.occupied = true;
    record.status = {.binding = {*binding, target}};
    return *binding;
}
std::expected<void, SocketDispatchError> SocketDispatcher::Detach(FdBindingId binding) noexcept {
    if (!state_) {
        return Error(SocketDispatchErrorCode::kNotReady);
    }
    // StopAccepting 后 Lookup 被封禁；控制路径以完整身份寻找旧记录，不重新开放应用准入。
    // 扫描仅用于宿主控制路径；热 mailbox 直接按 stream.slot 定位。
    for (std::uint32_t i = 0; i < state_->config.stream_capacity; ++i) {
        auto& record = state_->records[i];
        if (record.occupied && record.status.binding.id == binding) {
            return state_->Detach(record);
        }
    }
    return FdFailure(FdError::kStaleBinding);
}
std::expected<SocketBindingStatus, SocketDispatchError>
SocketDispatcher::InspectBinding(FdBindingId binding) const noexcept {
    if (!state_) {
        return Error(SocketDispatchErrorCode::kNotReady);
    }
    for (std::uint32_t i = 0; i < state_->config.stream_capacity; ++i) {
        const auto& record = state_->records[i];
        if (record.occupied && record.status.binding.id == binding) {
            return record.status;
        }
    }
    return FdFailure(FdError::kStaleBinding);
}
std::expected<FdReader, FdError> SocketDispatcher::RegisterReader() noexcept {
    return state_ ? state_->bindings.RegisterReader() : std::unexpected(FdError::kNotReady);
}
std::expected<FdLease, FdError> SocketDispatcher::Lookup(FdReader& reader, int fd) noexcept {
    return state_ ? state_->bindings.Lookup(reader, fd) : std::unexpected(FdError::kNotReady);
}
std::expected<FdLease, FdError> SocketDispatcher::Lookup(FdReader& reader, FdBindingId binding) noexcept {
    return state_ ? state_->bindings.Lookup(reader, binding) : std::unexpected(FdError::kNotReady);
}
std::expected<FdCommandTicket, SocketDispatchError>
SocketDispatcher::TrySubmit(FdLease& lease, const FdCommand& command, std::span<const std::byte> input) noexcept {
    if (!state_) {
        return Error(SocketDispatchErrorCode::kNotReady);
    }
    // producer 只读不可变配置及自己持有的绑定，不访问 owner 的记录、后端或 Runtime。
    if (!lease || lease.binding().id.table_id != state_->config.bindings.table_id ||
        lease.binding().target.owner_id != state_->config.owner_id) {
        return Error(SocketDispatchErrorCode::kInvalidArgument);
    }
    auto submitted = state_->commands.TrySubmit(lease, command, input);
    if (!submitted) {
        return QueueFailure(submitted.error());
    }
    return *submitted;
}
std::expected<FdCommandCompletion, FdQueueError>
SocketDispatcher::TakeCompletion(FdCommandTicket ticket, std::span<std::byte> output) noexcept {
    if (!state_) {
        return std::unexpected(FdQueueError{FdQueueErrorCode::kNotReady});
    }
    auto result = state_->commands.TakeCompletion(ticket, output);
    if (result) {
        // 完成领取归还 FD Lease 后唤醒 owner 收集退休节点，不能只依赖新的业务请求。
        Signal(state_->fds[0]);
    }
    return result;
}
FdCommandResult SocketDispatcher::Execute(const FdCommandRequest& request, std::uint64_t now_ns) noexcept {
    auto& state = *state_;
    auto* record = state.Find(request.binding);
    if (!record || record->status.stage != SocketRetireStage::kActive) {
        return std::unexpected(StreamError(KBS_STREAM_CLOSED));
    }
    const auto stream = request.binding.target.stream;
    FdCommandReply reply;
    switch (request.command.kind) {
    case FdCommandKind::kWrite: {
        auto result = state.backend->Write(stream, request.payload);
        if (!result) {
            return std::unexpected(result.error());
        }
        reply.io = *result;
        break;
    }
    case FdCommandKind::kRead: {
        // 同一轮中 Runtime 可能已经推进了连接时钟，不能把轮次起点作为更早的 Read 时间。
        const auto current = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
        auto result = state.backend->Read(stream, request.payload, std::max(now_ns, current));
        if (!result) {
            return std::unexpected(result.error());
        }
        reply.io = *result;
        break;
    }
    case FdCommandKind::kShutdownWrite: {
        const auto result = state.backend->ShutdownWrite(stream);
        if (!result) {
            return std::unexpected(result.error());
        }
        break;
    }
    case FdCommandKind::kInspect: {
        const auto result = state.backend->Inspect(stream);
        if (!result) {
            return std::unexpected(result.error());
        }
        reply.status = *result;
        break;
    }
    case FdCommandKind::kClose: {
        const auto result = state.Detach(*record);
        if (!result) {
            return std::unexpected(StreamError(KBS_STREAM_TRANSPORT, result.error().system_error));
        }
        break;
    }
    }
    return reply;
}
std::expected<runtime::ProgressBatch, runtime::ProgressError>
SocketDispatcher::Progress(runtime::ProgressKind kind, std::size_t budget, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return std::unexpected(runtime::ProgressError{runtime::ProgressErrorCode::kNotReady});
    }
    if (!budget) {
        return std::unexpected(runtime::ProgressError{runtime::ProgressErrorCode::kInvalidArgument});
    }
    auto& state = *state_;
    const auto limit = std::min(budget, FdCommandQueue::kMaxPollBatch);
    if (kind == runtime::ProgressKind::kMailbox) {
        std::array<FdCommandRequest, FdCommandQueue::kMaxPollBatch> requests;
        const auto count = state.commands.PollCommands(std::span(requests).first(limit));
        if (!count) {
            return ProgressFailure(count.error());
        }
        for (std::size_t i = 0; i < *count; ++i) {
            auto result = Execute(requests[i], now_ns);
            const auto completed = state.commands.Complete(requests[i].ticket, result);
            if (!completed) {
                // 后端违反结果大小契约仍应恰好终结，不能留下 executing 槽或重复业务 I/O。
                result = std::unexpected(StreamError(KBS_STREAM_PROTOCOL));
                const auto failed = state.commands.Complete(requests[i].ticket, result);
                if (!failed) {
                    return ProgressFailure(failed.error());
                }
            }
        }
        return runtime::ProgressBatch{*count, state.commands.runnable()};
    }
    if (kind != runtime::ProgressKind::kRetire) {
        return runtime::ProgressBatch{};
    }
    // 热路径只回收预分配元数据。真实 Close/Release 可能释放堆对象，交给显式 owner 控制入口。
    if (!state.collect_remaining) {
        state.collect_remaining = state.retired;
    }
    const auto visits = std::min(limit, state.collect_remaining);
    std::array<FdBinding, FdCommandQueue::kMaxPollBatch> output;
    const auto count = state.bindings.Collect(std::span(output).first(limit), std::max(std::size_t{1}, visits));
    if (!count) {
        return std::unexpected(
            runtime::ProgressError{runtime::ProgressErrorCode::kRetained, static_cast<int>(count.error())});
    }
    for (std::size_t i = 0; i < *count; ++i) {
        auto* record = state.Find(output[i]);
        if (!record || record->status.stage != SocketRetireStage::kConfirmed) {
            return std::unexpected(runtime::ProgressError{runtime::ProgressErrorCode::kRetained});
        }
        *record = {};
    }
    state.retired -= *count;
    state.collect_remaining -= visits;
    return runtime::ProgressBatch{*count, state.collect_remaining != 0};
}
std::expected<runtime::ProgressBatch, SocketDispatchError>
SocketDispatcher::RetireStreams(std::size_t budget) noexcept {
    if (!state_) {
        return Error(SocketDispatchErrorCode::kNotReady);
    }
    if (!budget) {
        return Error(SocketDispatchErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    const auto limit = std::min(budget, FdCommandQueue::kMaxPollBatch);
    std::size_t processed = 0;
    if (!state.scan_remaining) {
        state.scan_remaining = state.pending;
    }
    const auto visits = std::min(limit, state.scan_remaining);
    for (std::size_t i = 0; i < visits; ++i) {
        const auto index = state.head;
        auto& record = state.records[index];
        state.head = record.next;
        if (state.head == State::kNone) {
            state.tail = State::kNone;
        }
        --state.pending;
        --state.scan_remaining;
        auto& status = record.status;
        if (status.stage != SocketRetireStage::kClose && status.stage != SocketRetireStage::kRelease) {
            continue;
        }
        const bool closing = status.stage == SocketRetireStage::kClose;
        const auto result = closing ? state.backend->CloseStream(status.binding.target.stream)
                                    : state.backend->Release(status.binding.target.stream);
        if (!result) {
            status.last_error = result.error();
            state.Queue(index);
            continue;
        }
        status.last_error = {};
        ++processed;
        if (closing) {
            status.stage = SocketRetireStage::kRelease;
            state.Queue(index);
        } else {
            const auto confirmed = state.bindings.ConfirmClosed(record.retired);
            if (!confirmed) {
                return FdFailure(confirmed.error());
            }
            status.stage = SocketRetireStage::kConfirmed;
            Signal(state.fds[0]);
        }
    }
    return runtime::ProgressBatch{processed, state.scan_remaining != 0 || (processed != 0 && state.pending != 0)};
}
void SocketDispatcher::StopAccepting() noexcept {
    if (state_) {
        state_->commands.StopAccepting();
        state_->bindings.StopAccepting();
    }
}
std::expected<void, SocketDispatchError> SocketDispatcher::Close(bool clients_stopped) noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (!clients_stopped) {
        return Error(SocketDispatchErrorCode::kNotQuiescent);
    }
    const auto stats = state_->bindings.stats();
    if (stats.active || stats.retired || stats.readers != 0 || state_->commands.outstanding()) {
        return Error(SocketDispatchErrorCode::kRetained);
    }
    const auto bindings = state_->bindings.Close(true);
    if (!bindings) {
        return FdFailure(bindings.error());
    }
    const auto commands = state_->commands.Close(true);
    if (!commands) {
        return QueueFailure(commands.error());
    }
    state_.reset();
    return {};
}
SocketDispatcherStats SocketDispatcher::stats() const noexcept {
    SocketDispatcherStats result;
    if (!state_) {
        return result;
    }
    result.bindings = state_->bindings.stats();
    result.commands = state_->commands.outstanding();
    result.record_bytes = state_->record_bytes;
    for (std::uint32_t i = 0; i < state_->config.stream_capacity; ++i) {
        const auto& record = state_->records[i];
        if (!record.occupied) {
            continue;
        }
        result.closing += record.status.stage == SocketRetireStage::kClose;
        result.releasing += record.status.stage == SocketRetireStage::kRelease;
        result.confirmed += record.status.stage == SocketRetireStage::kConfirmed;
    }
    return result;
}
std::expected<runtime::ProgressBatch, runtime::ProgressError>
SocketDispatcher::CollectNotifications(std::size_t) noexcept {
    return runtime::ProgressBatch{};
}
std::expected<void, runtime::ProgressError> SocketDispatcher::Rearm() noexcept {
    return {};
}
std::span<const int> SocketDispatcher::wait_fds() const noexcept {
    return state_ ? std::span<const int>(state_->fds) : std::span<const int>{};
}
const void* SocketDispatcher::exclusive_context() const noexcept {
    return state_ ? state_->context : nullptr;
}

} // namespace socket
} // namespace kbsocket
