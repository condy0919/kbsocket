// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_binding.hpp"

#include <algorithm>
#include <limits>
#include <new>

#include "kbsocket/api/stream_error.hpp"

// 不把 C++ 对象布局暴露给 C；指针有效期为绑定对象存活期，Close 后仍能拒绝旧调用。
struct kbs_stream_table {
    kbsocket::api::StreamBinding* binding = nullptr;
    std::size_t max_io_bytes = 0;
};

namespace kbsocket {
namespace api {
namespace {
auto Error(int code) noexcept {
    return std::unexpected(StreamError(code));
}
constexpr auto kUnused = std::numeric_limits<std::uint32_t>::max();
std::expected<void, kbs_stream_error> Active(const runtime::OwnedConnectionStatus& status) noexcept {
    using State = connection::ConnectionState;
    if (status.state == State::kClosed || status.state == State::kClosing) {
        return Error(KBS_STREAM_CLOSED);
    }
    if (status.failure) {
        return std::unexpected(MapStreamError(*status.failure));
    }
    if (status.state != State::kRawActive && status.state != State::kTcpActive) {
        return Error(KBS_STREAM_NOT_READY);
    }
    return {};
}
} // namespace
struct StreamBinding::State {
    struct Slot {
        runtime::OwnedConnection connection;
        std::uint64_t generation = 0;
    };
    kbs_stream_table table;
    runtime::ConnectionOwner* owner = nullptr;
    runtime::RawStack* stack = nullptr;
    std::size_t context = 0;
    StreamBindingConfig config;
    std::unique_ptr<Slot[]> slots;
    std::unique_ptr<std::uint32_t[]> free;
    // 按原 owner 槽定位发布，避免 40k 连接时每次 Publish 扫全表。
    std::unique_ptr<std::uint32_t[]> owner_slots;
    std::size_t count = 0;
    std::uint64_t next_generation = 0;
    bool accepting = true, closed = false;

    std::expected<void, kbs_stream_error> Gate() noexcept {
        if (closed) {
            return Error(KBS_STREAM_NOT_READY);
        }
        if (stack) {
            const auto resources = stack->resources(context);
            if (!resources) {
                return std::unexpected(MapStreamError(resources.error()));
            }
            if (resources->connections != owner) {
                return Error(KBS_STREAM_NOT_READY);
            }
        } else if (!owner->stats().capacity) {
            return Error(KBS_STREAM_NOT_READY);
        }
        return {};
    }
    std::expected<Slot*, kbs_stream_error> Find(kbs_stream_handle handle) noexcept {
        if (const auto ready = Gate(); !ready) {
            return std::unexpected(ready.error());
        }
        if (handle.table_id != config.table_id || handle.reserved || !handle.generation ||
            handle.slot >= config.capacity) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        auto& slot = slots[handle.slot];
        if (!slot.generation || slot.generation != handle.generation) {
            return Error(KBS_STREAM_INVALID_HANDLE);
        }
        return &slot;
    }
    std::expected<runtime::OwnedConnectionStatus, kbs_stream_error> Inspect(Slot& slot) noexcept {
        const auto status = owner->Inspect(slot.connection);
        if (!status) {
            return std::unexpected(MapStreamError(status.error()));
        }
        return *status;
    }
};
StreamBinding::StreamBinding() noexcept = default;
StreamBinding::~StreamBinding() = default;
std::expected<void, kbs_stream_error> StreamBinding::Open(runtime::ConnectionOwner& owner, runtime::RawStack* stack,
                                                          std::size_t context,
                                                          const StreamBindingConfig& config) noexcept {
    if (opened_once_) {
        return Error(KBS_STREAM_BUSY);
    }
    const auto capacity = owner.stats().capacity;
    if (!capacity || !config.table_id || !config.capacity || config.capacity > capacity || !config.max_io_bytes ||
        config.max_io_bytes > 1024u * 1024u) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    std::unique_ptr<State> state;
    try {
        state = std::make_unique<State>();
        state->slots = std::make_unique<State::Slot[]>(config.capacity);
        state->free = std::make_unique<std::uint32_t[]>(config.capacity);
        state->owner_slots = std::make_unique<std::uint32_t[]>(capacity);
    } catch (const std::bad_alloc&) {
        return Error(KBS_STREAM_NO_MEMORY);
    }
    state->table = {this, config.max_io_bytes};
    state->owner = &owner;
    state->stack = stack;
    state->context = context;
    state->config = config;
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->free[i] = config.capacity - i - 1;
    }
    std::fill_n(state->owner_slots.get(), capacity, kUnused);
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}
std::expected<void, kbs_stream_error> StreamBinding::OpenTcp(runtime::ConnectionOwner& owner,
                                                             const StreamBindingConfig& config) noexcept {
    if (owner.exclusive_context()) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    return Open(owner, nullptr, 0, config);
}
std::expected<void, kbs_stream_error> StreamBinding::OpenRaw(runtime::RawStack& stack, std::size_t context,
                                                             const StreamBindingConfig& config) noexcept {
    const auto resources = stack.resources(context);
    if (!resources) {
        return std::unexpected(MapStreamError(resources.error()));
    }
    return Open(*resources->connections, &stack, context, config);
}
std::expected<kbs_stream_handle, kbs_stream_error>
StreamBinding::Publish(runtime::OwnedConnection connection) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& s = *state_;
    if (const auto ready = s.Gate(); !ready) {
        return std::unexpected(ready.error());
    }
    if (!s.accepting) {
        return Error(KBS_STREAM_STOPPED);
    }
    // Inspect 先校验 owner/完整代次/范围，再使用 connection.slot 访问索引。
    const auto status = s.owner->Inspect(connection);
    if (!status) {
        return std::unexpected(MapStreamError(status.error()));
    }
    if (s.owner_slots[connection.slot] != kUnused) {
        return Error(KBS_STREAM_BUSY);
    }
    if (s.count == s.config.capacity || s.next_generation == std::numeric_limits<std::uint64_t>::max()) {
        return Error(KBS_STREAM_EXHAUSTED);
    }
    const auto index = s.free[s.config.capacity - ++s.count];
    auto& slot = s.slots[index];
    slot = {connection, ++s.next_generation};
    s.owner_slots[connection.slot] = index;
    return kbs_stream_handle{s.config.table_id, slot.generation, index, 0};
}
kbs_stream_table* StreamBinding::table() noexcept {
    return state_ && !state_->closed ? &state_->table : nullptr;
}
std::size_t StreamBinding::published() const noexcept {
    return state_ ? state_->count : 0;
}
void StreamBinding::StopAccepting() noexcept {
    if (state_) {
        state_->accepting = false;
    }
}
std::expected<void, kbs_stream_error> StreamBinding::Close() noexcept {
    if (!state_ || state_->closed) {
        return {};
    }
    if (const auto ready = state_->Gate(); !ready) {
        return std::unexpected(ready.error());
    }
    StopAccepting();
    if (state_->count) {
        return Error(KBS_STREAM_BUSY);
    }
    state_->closed = true;
    return {};
}
std::expected<std::size_t, kbs_stream_error> StreamBinding::Write(kbs_stream_handle stream,
                                                                  std::span<const std::byte> input) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return std::unexpected(active.error());
    }
    if (status->tx.write_shutdown) {
        return Error(KBS_STREAM_CLOSED);
    }
    const auto written =
        state_->owner->Write((*slot)->connection, input.first(std::min(input.size(), state_->config.max_io_bytes)));
    if (!written) {
        return std::unexpected(MapStreamError(written.error()));
    }
    return *written;
}
std::expected<kbs_stream_io, kbs_stream_error>
StreamBinding::Read(kbs_stream_handle stream, std::span<std::byte> output, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return std::unexpected(active.error());
    }
    if (output.empty()) {
        return kbs_stream_io{};
    }
    const auto read = state_->owner->Read((*slot)->connection,
                                          output.first(std::min(output.size(), state_->config.max_io_bytes)), now_ns);
    if (!read) {
        return std::unexpected(MapStreamError(read.error()));
    }
    return kbs_stream_io{read->bytes, read->eof ? 1u : 0u, 0};
}
std::expected<void, kbs_stream_error> StreamBinding::ShutdownWrite(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return active;
    }
    const auto result = state_->owner->ShutdownWrite((*slot)->connection);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::Cancel(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (status->state == connection::ConnectionState::kClosed ||
        status->state == connection::ConnectionState::kClosing) {
        return Error(KBS_STREAM_CLOSED);
    }
    const auto result = state_->owner->Cancel((*slot)->connection);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::WatchTcp(kbs_stream_handle stream, bool readable,
                                                              bool writable) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    if (const auto active = Active(*status); !active) {
        return active;
    }
    if (status->state != connection::ConnectionState::kTcpActive) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto result = state_->owner->WatchTcp((*slot)->connection, readable, writable);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<kbs_stream_status, kbs_stream_error> StreamBinding::Inspect(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto status = state_->Inspect(**slot);
    if (!status) {
        return std::unexpected(status.error());
    }
    return StreamStatus(*status);
}
std::expected<void, kbs_stream_error> StreamBinding::CloseStream(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto result = state_->owner->CloseConnection((*slot)->connection);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::RetireSources(kbs_stream_handle stream,
                                                                   const runtime::RxSourceRetireProof& proof) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto result = state_->owner->RetireSources((*slot)->connection, proof);
    return result ? std::expected<void, kbs_stream_error>{} : std::unexpected(MapStreamError(result.error()));
}
std::expected<void, kbs_stream_error> StreamBinding::Release(kbs_stream_handle stream) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto slot = state_->Find(stream);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    const auto result = state_->owner->Release((*slot)->connection);
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    auto& s = *state_;
    s.owner_slots[(*slot)->connection.slot] = kUnused;
    **slot = {};
    s.free[s.config.capacity - s.count--] = stream.slot;
    return {};
}
} // namespace api
} // namespace kbsocket

namespace {
int32_t Report(kbs_stream_error result, kbs_stream_error* error) noexcept {
    if (error) {
        *error = result;
    }
    return result.code;
}
int32_t Invalid(kbs_stream_error* error) noexcept {
    return Report(kbsocket::api::StreamError(KBS_STREAM_INVALID_ARGUMENT), error);
}
int32_t Finish(const std::expected<void, kbs_stream_error>& result, kbs_stream_error* error) noexcept {
    return Report(result ? kbs_stream_error{} : result.error(), error);
}
} // namespace
extern "C" {
int32_t kbs_stream_write(kbs_stream_table* table, kbs_stream_handle stream, const void* data, size_t length,
                         kbs_stream_io* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out || (!data && length)) {
        return Invalid(error);
    }
    const auto result =
        table->binding->Write(stream, {static_cast<const std::byte*>(data), std::min(length, table->max_io_bytes)});
    if (!result) {
        return Report(result.error(), error);
    }
    out->bytes = *result;
    return Report({}, error);
}
int32_t kbs_stream_read(kbs_stream_table* table, kbs_stream_handle stream, void* data, size_t length, uint64_t now_ns,
                        kbs_stream_io* out, kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out || (!data && length)) {
        return Invalid(error);
    }
    const auto result =
        table->binding->Read(stream, {static_cast<std::byte*>(data), std::min(length, table->max_io_bytes)}, now_ns);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
int32_t kbs_stream_shutdown_write(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->ShutdownWrite(stream), error) : Invalid(error);
}
int32_t kbs_stream_cancel(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->Cancel(stream), error) : Invalid(error);
}
int32_t kbs_stream_watch_tcp(kbs_stream_table* table, kbs_stream_handle stream, uint32_t readable, uint32_t writable,
                             kbs_stream_error* error) {
    if (!table || readable > 1 || writable > 1) {
        return Invalid(error);
    }
    return Finish(table->binding->WatchTcp(stream, readable != 0, writable != 0), error);
}
int32_t kbs_stream_inspect(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_status* out,
                           kbs_stream_error* error) {
    if (out) {
        *out = {};
    }
    if (!table || !out) {
        return Invalid(error);
    }
    const auto result = table->binding->Inspect(stream);
    if (!result) {
        return Report(result.error(), error);
    }
    *out = *result;
    return Report({}, error);
}
int32_t kbs_stream_close(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->CloseStream(stream), error) : Invalid(error);
}
int32_t kbs_stream_release(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error) {
    return table ? Finish(table->binding->Release(stream), error) : Invalid(error);
}
}
