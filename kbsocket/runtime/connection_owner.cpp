// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_owner.hpp"

#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace runtime {
namespace {
using Code = ConnectionOwnerErrorCode;
auto Error(Code code) noexcept {
    return std::unexpected(ConnectionOwnerError{.code = code});
}
auto ConnectionError(const connection::ConnectionError& error) noexcept {
    const auto code = error.code == connection::ConnectionErrorCode::kRetained ? Code::kRetained : Code::kConnection;
    return std::unexpected(ConnectionOwnerError{.code = code, .connection = error});
}
auto ProgressFailure(const ProgressError& error) noexcept {
    return std::unexpected(ConnectionOwnerError{.code = Code::kProgress, .progress = error});
}
auto SourceError(control::RawSourceAdmissionError error) noexcept {
    return std::unexpected(ConnectionOwnerError{.code = Code::kSource, .source = error});
}
} // namespace

struct ConnectionOwner::State {
    struct Slot {
        std::optional<connection::Connection> connection;
        std::optional<HandshakeRxSources> sources;
        RxSourceRegistry* registry = nullptr;
        ScheduledConnection scheduled;
        std::uint64_t generation = 0;
    };
    const ConnectionOwner* owner = nullptr;
    ConnectionProgress* progress = nullptr;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<std::uint32_t[]> free;
    std::size_t free_count = 0;
    std::uint64_t next_generation = 0;
    ConnectionOwnerStats stats;

    Slot* Find(OwnedConnection handle) noexcept {
        if (handle.owner != owner || handle.slot >= stats.capacity || !handle.generation) {
            return nullptr;
        }
        auto& slot = slots[handle.slot];
        return slot.connection && slot.generation == handle.generation ? &slot : nullptr;
    }
    std::expected<Slot*, ConnectionOwnerError> Lookup(OwnedConnection handle) noexcept {
        auto* slot = Find(handle);
        if (!slot) {
            return Error(Code::kInvalidHandle);
        }
        return slot;
    }
    std::expected<Slot*, ConnectionOwnerError> Wake(OwnedConnection handle) noexcept {
        auto slot = Lookup(handle);
        if (!slot) {
            return slot;
        }
        const auto woken = progress->Wake((*slot)->scheduled);
        if (!woken) {
            return ProgressFailure(woken.error());
        }
        return slot;
    }
    void Free(OwnedConnection handle) noexcept {
        auto& slot = slots[handle.slot];
        slot.sources.reset();
        slot.connection.reset();
        slot.registry = nullptr;
        slot.scheduled = {};
        free[free_count++] = handle.slot;
        --stats.occupied;
    }
    std::expected<OwnedConnection, ConnectionOwnerError> Reserve(int fd) noexcept {
        if (!stats.accepting) {
            return Error(Code::kStopped);
        }
        // Attach 空对象时 fd 尚不存在，须先检查整个共享调度器，防止重复接管同一描述符。
        if (progress->HasFd(fd)) {
            return Error(Code::kInUse);
        }
        if (!free_count || next_generation == std::numeric_limits<std::uint64_t>::max()) {
            return Error(Code::kExhausted);
        }
        const auto index = free[--free_count];
        auto& slot = slots[index];
        slot.generation = ++next_generation;
        slot.connection.emplace();
        ++stats.occupied;
        const OwnedConnection handle{owner, index, slot.generation};
        // 注册先于 fd 接管，失败没有外部 fd 或 DMA 责任需要回滚。
        const auto attached = progress->Attach(*slot.connection);
        if (!attached) {
            Free(handle);
            return ProgressFailure(attached.error());
        }
        slot.scheduled = *attached;
        return handle;
    }
    ConnectionOwnerError Rollback(OwnedConnection handle, ConnectionOwnerError original) noexcept {
        auto& slot = slots[handle.slot];
        const auto closed = slot.connection->Close();
        if (!closed) {
            original.connection = closed.error();
            original.retained_handle = handle;
            return original;
        }
        const auto detached = progress->Detach(slot.scheduled);
        if (!detached) {
            original.progress = detached.error();
            original.retained_handle = handle;
            return original;
        }
        Free(handle);
        return original;
    }
};

ConnectionOwner::ConnectionOwner() noexcept = default;
ConnectionOwner::~ConnectionOwner() {
    if (state_ && state_->stats.occupied) {
        // 调度器/路由仍借用槽地址；析构绝不能制造悬空回调或猜测 DMA 已终结。
        (void)state_.release();
    }
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::Open(ConnectionProgress& progress,
                                                                const ConnectionOwnerConfig& config) noexcept {
    if (opened_once_) {
        return Error(Code::kInUse);
    }
    if (!config.capacity || config.capacity > ConnectionProgress::kMaxConnections || !progress.stats()) {
        return Error(Code::kInvalidArgument);
    }
    std::unique_ptr<State> state;
    try {
        state = std::make_unique<State>();
        state->slots = std::make_unique<State::Slot[]>(config.capacity);
        state->free = std::make_unique<std::uint32_t[]>(config.capacity);
    } catch (const std::bad_alloc&) {
        return Error(Code::kNoMemory);
    }
    state->owner = this;
    state->progress = &progress;
    state->free_count = config.capacity;
    state->stats = {config.capacity, 0, 0, true};
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->free[i] = config.capacity - 1 - i;
    }
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<OwnedConnection, ConnectionOwnerError> ConnectionOwner::AdoptTcp(int fd) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto handle = state_->Reserve(fd);
    if (!handle) {
        return handle;
    }
    const auto opened = state_->Find(*handle)->connection->OpenTcp(fd);
    if (!opened) {
        return std::unexpected(state_->Rollback(*handle, ConnectionError(opened.error()).error()));
    }
    ++state_->stats.adopted;
    return handle;
}
std::expected<OwnedConnection, ConnectionOwnerError> ConnectionOwner::ConnectTcp(const sockaddr* address,
                                                                                 socklen_t length, std::uint64_t now_ns,
                                                                                 std::uint64_t deadline_ns) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto handle = state_->Reserve(-1);
    if (!handle) {
        return handle;
    }
    const auto started = state_->Find(*handle)->connection->ConnectTcp(address, length, now_ns, deadline_ns);
    if (!started) {
        return std::unexpected(state_->Rollback(*handle, ConnectionError(started.error()).error()));
    }
    ++state_->stats.adopted;
    return handle;
}
std::expected<OwnedConnection, ConnectionOwnerError>
ConnectionOwner::AdoptProtocol(int fd, connection::ConnectionTable& routes,
                               const control::ConnectionHandshakeConfig& config, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto handle = state_->Reserve(fd);
    if (!handle) {
        return handle;
    }
    const auto opened = state_->Find(*handle)->connection->Open(fd, routes, config, now_ns);
    if (!opened) {
        return std::unexpected(state_->Rollback(*handle, ConnectionError(opened.error()).error()));
    }
    ++state_->stats.adopted;
    return handle;
}
std::expected<void, ConnectionOwnerError>
ConnectionOwner::PrepareRaw(OwnedConnection handle, const connection::RawConnectionPreparation& preparation,
                            RxSourceRegistry& sources) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    auto slot = state_->Lookup(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    if (preparation.connection_id || preparation.sources || !preparation.handshake.local_context ||
        preparation.handshake.local_context != state_->progress->exclusive_context() ||
        ((*slot)->registry && (*slot)->registry != &sources)) {
        return Error(Code::kInvalidArgument);
    }
    auto woken = state_->Wake(handle);
    if (!woken) {
        return std::unexpected(woken.error());
    }
    if (!(*slot)->sources) {
        (*slot)->sources.emplace();
        const auto opened = (*slot)->sources->Open(sources);
        if (!opened) {
            (*slot)->sources.reset();
            return SourceError(opened.error());
        }
        (*slot)->registry = &sources;
    }
    auto bound = preparation;
    bound.connection_id = (*slot)->scheduled.id;
    bound.sources = &*(*slot)->sources;
    const auto prepared = (*slot)->connection->PrepareRaw(bound);
    return prepared ? std::expected<void, ConnectionOwnerError>{} : ConnectionError(prepared.error());
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::PrepareTcp(OwnedConnection handle) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto prepared = (*slot)->connection->PrepareTcp();
    return prepared ? std::expected<void, ConnectionOwnerError>{} : ConnectionError(prepared.error());
}
std::expected<std::size_t, ConnectionOwnerError> ConnectionOwner::Write(OwnedConnection handle,
                                                                        std::span<const std::byte> input) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    // 先保证调度，再接受应用字节；不能在接受前缀后用 Wake 错误覆盖字节计数。
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto written = (*slot)->connection->Write(input);
    return written ? std::expected<std::size_t, ConnectionOwnerError>{*written} : ConnectionError(written.error());
}
std::expected<connection::ConnectionReadResult, ConnectionOwnerError>
ConnectionOwner::Read(OwnedConnection handle, std::span<std::byte> output, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto read = (*slot)->connection->Read(output, now_ns);
    return read ? std::expected<connection::ConnectionReadResult, ConnectionOwnerError>{*read}
                : ConnectionError(read.error());
}
std::expected<std::size_t, ConnectionOwnerError> ConnectionOwner::WriteLease(OwnedConnection handle,
                                                                             memory::BufferLease source,
                                                                             std::size_t offset,
                                                                             std::size_t bytes) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    // 先取得调度责任，再接受源 Lease；Wake 失败不能发生在接受字节之后。
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto written = (*slot)->connection->WriteLease(source, offset, bytes);
    return written ? std::expected<std::size_t, ConnectionOwnerError>{*written} : ConnectionError(written.error());
}
std::expected<connection::EagerReadLeaseResult, ConnectionOwnerError>
ConnectionOwner::ReadLease(OwnedConnection handle, std::size_t max_bytes, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto read = (*slot)->connection->ReadLease(max_bytes, now_ns);
    return read ? std::expected<connection::EagerReadLeaseResult, ConnectionOwnerError>{*read}
                : ConnectionError(read.error());
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::ReleaseReadLease(OwnedConnection handle,
                                                                            std::uint64_t generation) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    // 先保证后续授信/关闭能继续调度；Wake 失败时尚未消费应用的归还责任。
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto released = (*slot)->connection->ReleaseReadLease(generation);
    return released ? std::expected<void, ConnectionOwnerError>{} : ConnectionError(released.error());
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::ShutdownWrite(OwnedConnection handle) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto shut = (*slot)->connection->ShutdownWrite();
    return shut ? std::expected<void, ConnectionOwnerError>{} : ConnectionError(shut.error());
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::WatchTcp(OwnedConnection handle, bool read,
                                                                    bool write) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Lookup(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto watched = state_->progress->WatchTcp((*slot)->scheduled, read, write);
    return watched ? std::expected<void, ConnectionOwnerError>{} : ProgressFailure(watched.error());
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::Cancel(OwnedConnection handle) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    (*slot)->connection->Cancel();
    return {};
}
std::expected<OwnedConnectionStatus, ConnectionOwnerError>
ConnectionOwner::Inspect(OwnedConnection handle) const noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Lookup(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto& connection = *(*slot)->connection;
    return OwnedConnectionStatus{connection.state(),
                                 connection.failure(),
                                 (*slot)->scheduled,
                                 connection.tx_stats(),
                                 connection.rx_pending(),
                                 (*slot)->sources ? (*slot)->sources->size() : 0,
                                 connection.lane_update_pending(),
                                 connection.readiness()};
}
std::expected<const control::HandshakeHello*, ConnectionOwnerError>
ConnectionOwner::Peer(OwnedConnection handle) const noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Lookup(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto* peer = (*slot)->connection->handshake().peer();
    if (!peer) {
        return Error(Code::kNotReady);
    }
    return peer;
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::CloseConnection(OwnedConnection handle) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Wake(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto closed = (*slot)->connection->Close();
    return closed ? std::expected<void, ConnectionOwnerError>{} : ConnectionError(closed.error());
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::RetireSources(OwnedConnection handle,
                                                                         const RxSourceRetireProof& proof) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Lookup(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    if ((*slot)->connection->state() != connection::ConnectionState::kClosed) {
        return Error(Code::kInUse);
    }
    if ((*slot)->sources) {
        const auto retired = (*slot)->sources->Retire(proof);
        if (!retired) {
            return SourceError(retired.error());
        }
    }
    return {};
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::Release(OwnedConnection handle) noexcept {
    if (!state_) {
        return Error(Code::kNotReady);
    }
    const auto slot = state_->Lookup(handle);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    if ((*slot)->connection->state() != connection::ConnectionState::kClosed) {
        return Error(Code::kInUse);
    }
    if ((*slot)->sources) {
        const auto closed = (*slot)->sources->Close();
        if (!closed) {
            return Error(Code::kRetained);
        }
    }
    const auto detached = state_->progress->Detach((*slot)->scheduled);
    if (!detached) {
        return ProgressFailure(detached.error());
    }
    state_->Free(handle);
    return {};
}
void ConnectionOwner::StopAccepting() noexcept {
    if (state_) {
        state_->stats.accepting = false;
    }
}
std::expected<void, ConnectionOwnerError> ConnectionOwner::Close() noexcept {
    StopAccepting();
    if (state_ && state_->stats.occupied) {
        return Error(Code::kInUse);
    }
    state_.reset();
    return {};
}
ConnectionOwnerStats ConnectionOwner::stats() const noexcept {
    return state_ ? state_->stats : ConnectionOwnerStats{};
}
const void* ConnectionOwner::exclusive_context() const noexcept {
    return state_ ? state_->progress->exclusive_context() : nullptr;
}
std::uint32_t ConnectionOwner::scheduler_capacity() const noexcept {
    return state_ ? state_->progress->capacity() : 0;
}

} // namespace runtime
} // namespace kbsocket
