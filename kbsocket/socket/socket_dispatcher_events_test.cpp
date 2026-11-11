// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/api/stream.h"
#include "kbsocket/runtime/progress_source.hpp"
#include "kbsocket/socket/fd_command_queue.hpp"
#include "kbsocket/socket/fd_table.hpp"
#include "kbsocket/socket/socket_dispatcher.hpp"

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
thread_local std::size_t deallocations = 0;
} // namespace

void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* pointer = std::malloc(bytes ? bytes : 1)) {
        return pointer;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void* operator new(std::size_t bytes, std::align_val_t alignment) {
    if (count_allocations) {
        ++allocations;
    }
    void* pointer = nullptr;
    if (::posix_memalign(&pointer, static_cast<std::size_t>(alignment), bytes ? bytes : 1) != 0) {
        throw std::bad_alloc();
    }
    return pointer;
}
void* operator new[](std::size_t bytes, std::align_val_t alignment) {
    return ::operator new(bytes, alignment);
}
void operator delete(void* pointer) noexcept {
    if (count_allocations && pointer) {
        ++deallocations;
    }
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}

namespace kbsocket {
namespace socket {
namespace {
constexpr std::uint64_t kStreamTable = 7201;
kbs_stream_handle Handle(std::uint32_t slot, std::uint64_t generation = 1) {
    return {.table_id = kStreamTable, .generation = generation, .slot = slot, .reserved = 0};
}
kbs_stream_error Failure(int code, int underlying = 0) {
    return {.code = code,
            .origin = KBS_STREAM_ORIGIN_PROGRESS,
            .native_code = 17,
            .detail = 23,
            .underlying = underlying,
            .reserved = 0};
}
kbs_stream_event Event(std::uint32_t slot, std::uint32_t flags, std::uint64_t generation = 1) {
    kbs_stream_event event{};
    event.stream = Handle(slot, generation);
    event.flags = flags;
    event.state = KBS_STREAM_TCP_ACTIVE;
    return event;
}
SocketDispatcherConfig Config(bool events = true) {
    SocketDispatcherConfig config;
    config.owner_id = 7202;
    config.bindings.table_id = 7203;
    config.bindings.fd_limit = 4096;
    config.bindings.binding_capacity = 128;
    config.bindings.reader_capacity = 8;
    config.commands.queue_id = 7204;
    config.commands.capacity = 8;
    config.commands.max_io_bytes = 64;
    config.commands.payload_budget_bytes = 4096;
    config.stream_capacity = 128;
    config.record_budget_bytes = 1024u * 1024u;
    config.events = events;
    return config;
}
class FakeEventBackend final : public SocketStreamBackend {
public:
    struct Stream {
        std::uint64_t generation = 1;
        bool closed = false, released = false;
        bool readable = false, writable = false;
        std::size_t writes = 0, watches = 0;
        kbs_stream_error watch_error{};
    };
    std::array<Stream, 128> streams{};
    std::array<kbs_stream_event, 256> events{};
    std::size_t event_count = 0, event_offset = 0;
    std::size_t poll_calls = 0, last_capacity = 0;
    bool supported = true, malformed_count = false, wrong_thread = false;
    kbs_stream_error poll_error{};
    std::thread::id owner = std::this_thread::get_id();

    std::expected<Stream*, kbs_stream_error> Find(kbs_stream_handle handle) noexcept {
        wrong_thread |= std::this_thread::get_id() != owner;
        if (handle.table_id != kStreamTable || handle.reserved || handle.slot >= streams.size() ||
            streams[handle.slot].generation != handle.generation || streams[handle.slot].released) {
            return std::unexpected(Failure(KBS_STREAM_INVALID_HANDLE));
        }
        return &streams[handle.slot];
    }
    std::expected<kbs_stream_io, kbs_stream_error> Write(kbs_stream_handle handle,
                                                         std::span<const std::byte> input) noexcept override {
        const auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        ++(*found)->writes;
        return kbs_stream_io{.bytes = input.size(), .eof = 0, .reserved = 0};
    }
    std::expected<kbs_stream_io, kbs_stream_error> Read(kbs_stream_handle, std::span<std::byte>,
                                                        std::uint64_t) noexcept override {
        return std::unexpected(Failure(KBS_STREAM_WOULD_BLOCK));
    }
    std::expected<void, kbs_stream_error> ShutdownWrite(kbs_stream_handle) noexcept override {
        return {};
    }
    std::expected<kbs_stream_status, kbs_stream_error> Inspect(kbs_stream_handle handle) noexcept override {
        const auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        kbs_stream_status status{};
        status.state = (*found)->closed ? KBS_STREAM_STATE_CLOSED : KBS_STREAM_TCP_ACTIVE;
        return status;
    }
    std::expected<void, kbs_stream_error> CloseStream(kbs_stream_handle handle) noexcept override {
        const auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        (*found)->closed = true;
        return {};
    }
    std::expected<void, kbs_stream_error> Release(kbs_stream_handle handle) noexcept override {
        const auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        if (!(*found)->closed) {
            return std::unexpected(Failure(KBS_STREAM_RETAINED));
        }
        (*found)->released = true;
        return {};
    }
    bool supports_events() const noexcept override {
        return supported;
    }
    std::expected<void, kbs_stream_error> Watch(kbs_stream_handle handle, bool readable,
                                                bool writable) noexcept override {
        const auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        auto& stream = **found;
        ++stream.watches;
        if (stream.watch_error.code) {
            return std::unexpected(stream.watch_error);
        }
        stream.readable = readable;
        stream.writable = writable;
        return {};
    }
    std::expected<std::size_t, kbs_stream_error> PollEvents(std::span<kbs_stream_event> output) noexcept override {
        wrong_thread |= std::this_thread::get_id() != owner;
        ++poll_calls;
        last_capacity = output.size();
        if (poll_error.code) {
            return std::unexpected(poll_error);
        }
        if (malformed_count) {
            return output.size() + 1;
        }
        const auto count = std::min(output.size(), event_count - event_offset);
        std::copy_n(events.begin() + event_offset, count, output.begin());
        event_offset += count;
        return count;
    }
    const void* exclusive_context() const noexcept override {
        return nullptr;
    }
    void Push(kbs_stream_event event) {
        ASSERT_LT(event_count, events.size());
        events[event_count++] = event;
    }
};

class SocketDispatcherEventsTest : public testing::Test {
protected:
    FakeEventBackend backend;
    SocketDispatcher dispatcher;
    FdReader reader;
    std::array<FdBindingId, 130> bindings{};
    std::size_t binding_count = 0;
    std::array<FdCommandTicket, 32> tickets{};
    std::size_t ticket_count = 0;
    bool opened = false;

    void Open(bool events = true) {
        ASSERT_TRUE(dispatcher.Open(backend, Config(events)));
        opened = true;
        auto registered = dispatcher.RegisterReader();
        ASSERT_TRUE(registered);
        reader = std::move(*registered);
    }
    std::optional<FdBindingId> Attach(std::uint32_t slot, std::uint64_t generation = 1) {
        auto result = dispatcher.Attach(Handle(slot, generation));
        EXPECT_TRUE(result);
        if (!result) {
            return std::nullopt;
        }
        bindings[binding_count++] = *result;
        return *result;
    }
    std::optional<FdCommandTicket> Submit(FdBindingId binding, FdCommand command) {
        auto lease = dispatcher.Lookup(reader, binding);
        EXPECT_TRUE(lease);
        if (!lease) {
            return std::nullopt;
        }
        auto result = dispatcher.TrySubmit(*lease, command);
        EXPECT_TRUE(result);
        if (!result) {
            return std::nullopt;
        }
        tickets[ticket_count++] = *result;
        return *result;
    }
    void Retire() {
        for (unsigned i = 0; i < 16; ++i) {
            ASSERT_TRUE(dispatcher.RetireStreams(64));
            ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kRetire, 64, 12345));
        }
    }
    void TearDown() override {
        count_allocations = false;
        if (!opened) {
            return;
        }
        // 失败断言也必须回收命令 Lease，再执行真实 Close/Release 和 FD 元数据退休。
        for (unsigned i = 0; i < 8; ++i) {
            (void)dispatcher.Progress(runtime::ProgressKind::kMailbox, 64, 9000);
        }
        for (std::size_t i = 0; i < ticket_count; ++i) {
            (void)dispatcher.TakeCompletion(tickets[i]);
        }
        for (std::size_t i = 0; i < binding_count; ++i) {
            auto status = dispatcher.InspectBinding(bindings[i]);
            if (status && status->stage == SocketRetireStage::kActive) {
                (void)dispatcher.Detach(bindings[i]);
            }
        }
        reader.Reset();
        Retire();
        EXPECT_TRUE(dispatcher.Close(true));
        EXPECT_FALSE(backend.wrong_thread);
    }
};

TEST(SocketDispatcherEventsConfigTest, UnopenedAndUnsupportedEventBridgeRejectWithoutConsumingOpen) {
    SocketDispatcher dispatcher;
    FakeEventBackend backend;
    std::array<FdEvent, 1> output;
    const auto empty = dispatcher.PollEvents(output);
    ASSERT_FALSE(empty);
    EXPECT_EQ(empty.error().code, SocketDispatchErrorCode::kNotReady);
    const auto collect = dispatcher.CollectNotifications(1);
    ASSERT_FALSE(collect);
    EXPECT_EQ(collect.error().code, runtime::ProgressErrorCode::kNotReady);
    EXPECT_FALSE(dispatcher.events_pending());
    backend.supported = false;
    const auto unsupported = dispatcher.Open(backend, Config());
    ASSERT_FALSE(unsupported);
    EXPECT_EQ(unsupported.error().code, SocketDispatchErrorCode::kStream);
    EXPECT_EQ(unsupported.error().stream.code, KBS_STREAM_NOT_SUPPORTED);
    // 错误配置不占用一次 Open 资格；同一个后端仍可按旧的无事件方式打开。
    ASSERT_TRUE(dispatcher.Open(backend, Config(false)));
    EXPECT_TRUE(dispatcher.Close(true));
}

TEST_F(SocketDispatcherEventsTest, DisabledEventsKeepIoCompatibleAndNeverConsumeBackendEvents) {
    backend.supported = false;
    ASSERT_NO_FATAL_FAILURE(Open(false));
    const auto binding = Attach(0);
    ASSERT_TRUE(binding);
    backend.Push(Event(0, KBS_STREAM_EVENT_READABLE));
    const auto collect = dispatcher.CollectNotifications(1);
    ASSERT_TRUE(collect);
    EXPECT_EQ(collect->processed, 0u);
    EXPECT_FALSE(collect->runnable);
    EXPECT_EQ(backend.poll_calls, 0u);
    std::array<FdEvent, 1> output;
    const auto events = dispatcher.PollEvents(output);
    ASSERT_FALSE(events);
    EXPECT_EQ(events.error().code, SocketDispatchErrorCode::kStream);
    EXPECT_EQ(events.error().stream.code, KBS_STREAM_NOT_SUPPORTED);
    const auto watch = Submit(*binding, {.kind = FdCommandKind::kWatch, .readable = true});
    const auto write = Submit(*binding, {.kind = FdCommandKind::kWrite});
    ASSERT_TRUE(watch);
    ASSERT_TRUE(write);
    ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 64, 0));
    const auto watched = dispatcher.TakeCompletion(*watch);
    ASSERT_TRUE(watched);
    ASSERT_FALSE(watched->result);
    EXPECT_EQ(watched->result.error().code, KBS_STREAM_NOT_SUPPORTED);
    const auto written = dispatcher.TakeCompletion(*write);
    ASSERT_TRUE(written);
    EXPECT_TRUE(written->result);
    EXPECT_EQ(backend.streams[0].watches, 0u);
    EXPECT_EQ(backend.streams[0].writes, 1u);
}

TEST_F(SocketDispatcherEventsTest, WatchCommandsExecuteOnceAndCancellationDoesNotWithdrawQueuedHint) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto binding = Attach(0);
    ASSERT_TRUE(binding);
    // 提交只移交命令；owner 执行前不能触碰 backend 的订阅状态。
    const auto watch =
        Submit(*binding, {.kind = FdCommandKind::kWatch, .token = 91, .readable = true, .writable = true});
    ASSERT_TRUE(watch);
    EXPECT_EQ(backend.streams[0].watches, 0u);
    ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 0));
    EXPECT_EQ(backend.streams[0].watches, 1u);
    EXPECT_TRUE(backend.streams[0].readable);
    EXPECT_TRUE(backend.streams[0].writable);
    auto completed = dispatcher.TakeCompletion(*watch);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(completed->result);
    EXPECT_EQ(completed->command.token, 91u);
    backend.Push(Event(0, KBS_STREAM_EVENT_WRITABLE));
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    const auto cancel = Submit(*binding, {.kind = FdCommandKind::kWatch, .token = 92});
    ASSERT_TRUE(cancel);
    ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 0));
    completed = dispatcher.TakeCompletion(*cancel);
    ASSERT_TRUE(completed);
    ASSERT_TRUE(completed->result);
    EXPECT_EQ(completed->command.token, 92u);
    EXPECT_FALSE(backend.streams[0].readable);
    EXPECT_FALSE(backend.streams[0].writable);
    std::array<FdEvent, 1> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    EXPECT_EQ(output[0].event.flags, KBS_STREAM_EVENT_WRITABLE);
    ASSERT_TRUE(dispatcher.Rearm());
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    EXPECT_EQ(backend.streams[0].watches, 2u);
    // 后端失败以一次完成交付原始诊断，不自动重试订阅。
    backend.streams[0].watch_error = Failure(KBS_STREAM_TRANSPORT, 9);
    const auto failed = Submit(*binding, {.kind = FdCommandKind::kWatch, .readable = true});
    ASSERT_TRUE(failed);
    ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 0));
    completed = dispatcher.TakeCompletion(*failed);
    ASSERT_TRUE(completed);
    ASSERT_FALSE(completed->result);
    EXPECT_EQ(completed->result.error().underlying, 9);
    ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 0));
    EXPECT_EQ(backend.streams[0].watches, 3u);
}

TEST_F(SocketDispatcherEventsTest, CoalescesFlagsLatestSnapshotAndSaturatingRetirementCount) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto first = Event(0, KBS_STREAM_EVENT_READABLE | KBS_STREAM_EVENT_TX_RETIRED);
    first.rx_pending = 9;
    first.tx_retired = std::numeric_limits<std::uint64_t>::max() - 2;
    auto latest = Event(0, KBS_STREAM_EVENT_ERROR | KBS_STREAM_EVENT_TX_RETIRED | KBS_STREAM_EVENT_STATE);
    latest.state = KBS_STREAM_FAILED;
    latest.failure = Failure(KBS_STREAM_TRANSPORT, 9);
    latest.rx_pending = 3;
    latest.tx_retired = 4;
    latest.read_eof = 1;
    latest.write_closed = 1;
    backend.Push(first);
    backend.Push(latest);
    ASSERT_TRUE(dispatcher.CollectNotifications(2));
    EXPECT_EQ(dispatcher.stats().pending_events, 1u);
    std::array<FdEvent, 2> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    // 同一绑定只占一条；历史 flags 合并，其他状态来自最后快照，退休数不可回绕。
    const auto& event = output[0].event;
    EXPECT_EQ(event.flags, first.flags | latest.flags | KBS_STREAM_EVENT_COUNTER_SATURATED);
    EXPECT_EQ(event.tx_retired, std::numeric_limits<std::uint64_t>::max());
    EXPECT_EQ(event.state, KBS_STREAM_FAILED);
    EXPECT_EQ(event.failure.underlying, 9);
    EXPECT_EQ(event.rx_pending, 3u);
    EXPECT_EQ(event.read_eof, 1u);
    EXPECT_EQ(event.write_closed, 1u);
    EXPECT_EQ(output[0].binding.id, *binding);
    EXPECT_EQ(output[0].binding.target.owner_id, Config().owner_id);
    EXPECT_EQ(output[0].binding.target.stream.table_id, kStreamTable);
    EXPECT_EQ(output[0].binding.target.stream.generation, 1u);
    EXPECT_EQ(output[0].binding.target.stream.slot, 0u);
    EXPECT_EQ(dispatcher.stats().mapped_events, 2u);
    EXPECT_FALSE(dispatcher.events_pending());
}

TEST_F(SocketDispatcherEventsTest, WorkerSubmitsWatchWhileOnlyOwnerAccessesBackendAndEvents) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto binding = Attach(0);
    ASSERT_TRUE(binding);
    std::optional<FdCommandTicket> ticket;
    bool lease_moved = false;
    // 应用线程只入队；与 owner 分阶段运行，使错误的直接 backend 调用可由线程身份检测。
    std::thread submitter([&] {
        auto lease = dispatcher.Lookup(reader, *binding);
        if (!lease) {
            return;
        }
        const auto submitted = dispatcher.TrySubmit(*lease, {.kind = FdCommandKind::kWatch, .readable = true});
        if (submitted) {
            ticket = *submitted;
            lease_moved = !*lease;
        }
    });
    submitter.join();
    ASSERT_TRUE(ticket);
    tickets[ticket_count++] = *ticket;
    EXPECT_TRUE(lease_moved);
    EXPECT_EQ(backend.streams[0].watches, 0u);
    ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 0));
    backend.Push(Event(0, KBS_STREAM_EVENT_READABLE));
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    std::array<FdEvent, 1> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    EXPECT_EQ(output[0].binding.id, *binding);
    bool taken = false;
    std::thread collector([&] {
        const auto completed = dispatcher.TakeCompletion(*ticket);
        taken = completed && completed->result.has_value();
    });
    collector.join();
    EXPECT_TRUE(taken);
    EXPECT_EQ(backend.streams[0].watches, 1u);
    EXPECT_FALSE(backend.wrong_thread);
}

TEST_F(SocketDispatcherEventsTest, AdvisoryErrorAndPeerShutdownDoNotInventFailureOrEof) {
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(Attach(0));
    auto first = Event(0, KBS_STREAM_EVENT_ERROR);
    first.failure = Failure(KBS_STREAM_TRANSPORT, 8);
    first.rx_pending = 11;
    first.read_eof = 1;
    first.write_closed = 1;
    backend.Push(first);
    backend.Push(Event(0, KBS_STREAM_EVENT_ERROR | KBS_STREAM_EVENT_PEER_SHUTDOWN));
    ASSERT_TRUE(dispatcher.CollectNotifications(2));
    std::array<FdEvent, 1> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    // fd ERR 可没有确定错误，RDHUP 也不是 Read EOF；最新零值不能被旧快照覆盖。
    EXPECT_EQ(output[0].event.flags, KBS_STREAM_EVENT_ERROR | KBS_STREAM_EVENT_PEER_SHUTDOWN);
    EXPECT_EQ(output[0].event.failure.code, 0);
    EXPECT_EQ(output[0].event.failure.underlying, 0);
    EXPECT_EQ(output[0].event.read_eof, 0u);
    EXPECT_EQ(output[0].event.write_closed, 0u);
    EXPECT_EQ(output[0].event.rx_pending, 0u);
    EXPECT_EQ(output[0].event.state, KBS_STREAM_TCP_ACTIVE);
}

TEST_F(SocketDispatcherEventsTest, DetachRevokesPendingHintAndDropsLateEventsAcrossFdReuse) {
    ASSERT_NO_FATAL_FAILURE(Open());
    const auto old = Attach(0);
    ASSERT_TRUE(old);
    backend.Push(Event(0, KBS_STREAM_EVENT_READABLE));
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    ASSERT_TRUE(dispatcher.events_pending());
    ASSERT_TRUE(dispatcher.Detach(*old));
    EXPECT_FALSE(dispatcher.events_pending());
    EXPECT_EQ(dispatcher.stats().pending_events, 0u);
    // 已取消绑定的事件在真实 Release 前也不能交付。
    backend.Push(Event(0, KBS_STREAM_EVENT_CLOSED));
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    EXPECT_FALSE(dispatcher.events_pending());
    ASSERT_NO_FATAL_FAILURE(Retire());
    backend.streams[0] = FakeEventBackend::Stream{.generation = 2};
    const auto current = Attach(0, 2);
    ASSERT_TRUE(current);
    EXPECT_NE(*current, *old);
    // 相同 slot 的旧 generation、不同 stream 表及未绑定 slot 均不能命中新绑定。
    backend.Push(Event(0, KBS_STREAM_EVENT_ERROR));
    auto foreign = Event(0, KBS_STREAM_EVENT_ERROR, 2);
    ++foreign.stream.table_id;
    backend.Push(foreign);
    backend.Push(Event(1, KBS_STREAM_EVENT_ERROR));
    backend.Push(Event(0, KBS_STREAM_EVENT_WRITABLE, 2));
    ASSERT_TRUE(dispatcher.CollectNotifications(4));
    std::array<FdEvent, 2> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    EXPECT_EQ(output[0].binding.id, *current);
    EXPECT_EQ(output[0].event.stream.generation, 2u);
    EXPECT_EQ(output[0].event.flags, KBS_STREAM_EVENT_WRITABLE);
    EXPECT_EQ(dispatcher.stats().unmapped_events, 4u);
    EXPECT_FALSE(dispatcher.Lookup(reader, *old));
}

TEST_F(SocketDispatcherEventsTest, BudgetOnePreservesFifoAndRunnableUntilPendingEventsDrain) {
    ASSERT_NO_FATAL_FAILURE(Open());
    for (std::uint32_t slot = 0; slot < 3; ++slot) {
        ASSERT_TRUE(Attach(slot));
    }
    backend.Push(Event(2, KBS_STREAM_EVENT_READABLE));
    backend.Push(Event(0, KBS_STREAM_EVENT_WRITABLE));
    backend.Push(Event(1, KBS_STREAM_EVENT_RX_CHANGED));
    backend.Push(Event(2, KBS_STREAM_EVENT_PEER_SHUTDOWN));
    for (unsigned i = 0; i < 4; ++i) {
        const auto collected = dispatcher.CollectNotifications(1);
        ASSERT_TRUE(collected);
        EXPECT_EQ(collected->processed, 1u);
        EXPECT_TRUE(collected->runnable);
        EXPECT_EQ(backend.last_capacity, 1u);
    }
    EXPECT_EQ(dispatcher.stats().pending_events, 3u);
    const auto empty_backend = dispatcher.CollectNotifications(1);
    ASSERT_TRUE(empty_backend);
    EXPECT_EQ(empty_backend->processed, 0u);
    EXPECT_TRUE(empty_backend->runnable);
    // 合并已有节点不能将它移到队尾；一次只领取一条，未领取提示持续阻止 reactor 睡眠。
    std::array<FdEvent, 1> output;
    for (const auto slot : {2u, 0u, 1u}) {
        ASSERT_EQ(dispatcher.PollEvents(output), 1u);
        EXPECT_EQ(output[0].event.stream.slot, slot);
        if (slot == 2u) {
            EXPECT_EQ(output[0].event.flags, KBS_STREAM_EVENT_READABLE | KBS_STREAM_EVENT_PEER_SHUTDOWN);
        }
    }
    EXPECT_FALSE(dispatcher.events_pending());
    const auto drained = dispatcher.CollectNotifications(1);
    ASSERT_TRUE(drained);
    EXPECT_FALSE(drained->runnable);
}

TEST_F(SocketDispatcherEventsTest, DetachRemovesMiddleTailAndHeadWithoutLosingRemainingEvents) {
    ASSERT_NO_FATAL_FAILURE(Open());
    std::array<FdBindingId, 4> attached;
    for (std::uint32_t slot = 0; slot < attached.size(); ++slot) {
        const auto binding = Attach(slot);
        ASSERT_TRUE(binding);
        attached[slot] = *binding;
    }
    for (std::uint32_t slot = 0; slot < 3; ++slot) {
        backend.Push(Event(slot, KBS_STREAM_EVENT_READABLE));
    }
    ASSERT_TRUE(dispatcher.CollectNotifications(3));
    // 撤销任意位置的待取节点后仍能继续追加，不能使队尾悬空或丢掉存活绑定的提示。
    ASSERT_TRUE(dispatcher.Detach(attached[1]));
    ASSERT_TRUE(dispatcher.Detach(attached[2]));
    EXPECT_EQ(dispatcher.stats().pending_events, 1u);
    backend.Push(Event(3, KBS_STREAM_EVENT_WRITABLE));
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    ASSERT_TRUE(dispatcher.Detach(attached[0]));
    EXPECT_EQ(dispatcher.stats().pending_events, 1u);
    std::array<FdEvent, 4> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    EXPECT_EQ(output[0].binding.id, attached[3]);
    EXPECT_EQ(output[0].event.flags, KBS_STREAM_EVENT_WRITABLE);
    EXPECT_FALSE(dispatcher.events_pending());
}

TEST_F(SocketDispatcherEventsTest, CollectionAndDeliveryCapEachCallAtSixtyFour) {
    ASSERT_NO_FATAL_FAILURE(Open());
    for (std::uint32_t slot = 0; slot < 66; ++slot) {
        ASSERT_TRUE(Attach(slot));
        backend.Push(Event(slot, KBS_STREAM_EVENT_READABLE));
    }
    const auto first = dispatcher.CollectNotifications(100);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->processed, 64u);
    EXPECT_EQ(backend.last_capacity, 64u);
    const auto second = dispatcher.CollectNotifications(100);
    ASSERT_TRUE(second);
    EXPECT_EQ(second->processed, 2u);
    std::array<FdEvent, 100> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 64u);
    for (std::uint32_t slot = 0; slot < 64; ++slot) {
        EXPECT_EQ(output[slot].event.stream.slot, slot);
    }
    EXPECT_TRUE(dispatcher.events_pending());
    ASSERT_EQ(dispatcher.PollEvents(output), 2u);
    EXPECT_EQ(output[0].event.stream.slot, 64u);
    EXPECT_EQ(output[1].event.stream.slot, 65u);
    EXPECT_FALSE(dispatcher.events_pending());
}

TEST_F(SocketDispatcherEventsTest, BackendFailureAndMalformedCountPreservePendingEventsAndDiagnostics) {
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(Attach(0));
    backend.Push(Event(0, KBS_STREAM_EVENT_READABLE));
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    backend.poll_error = Failure(KBS_STREAM_TRANSPORT, 9);
    auto failed = dispatcher.CollectNotifications(1);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, runtime::ProgressErrorCode::kProvider);
    EXPECT_EQ(dispatcher.stats().event_error.code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(dispatcher.stats().event_error.underlying, 9);
    EXPECT_EQ(dispatcher.stats().pending_events, 1u);
    backend.poll_error = {};
    backend.malformed_count = true;
    failed = dispatcher.CollectNotifications(1);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, runtime::ProgressErrorCode::kProvider);
    EXPECT_EQ(failed.error().detail, KBS_STREAM_PROTOCOL);
    EXPECT_EQ(dispatcher.stats().event_error.code, KBS_STREAM_PROTOCOL);
    EXPECT_EQ(dispatcher.stats().pending_events, 1u);
    // 收集失败不吞掉已经映射的提示，也不能阻止 owner 将其领取。
    std::array<FdEvent, 1> output;
    ASSERT_EQ(dispatcher.PollEvents(output), 1u);
    EXPECT_EQ(output[0].event.flags, KBS_STREAM_EVENT_READABLE);
    backend.malformed_count = false;
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    EXPECT_EQ(dispatcher.stats().event_error.code, 0);
}

TEST_F(SocketDispatcherEventsTest, EmptyBudgetsRejectWithoutConsumingOrRearmingBackend) {
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(Attach(0));
    backend.Push(Event(0, KBS_STREAM_EVENT_READABLE));
    const auto zero = dispatcher.CollectNotifications(0);
    ASSERT_FALSE(zero);
    EXPECT_EQ(zero.error().code, runtime::ProgressErrorCode::kInvalidArgument);
    EXPECT_EQ(backend.poll_calls, 0u);
    const auto empty = dispatcher.PollEvents({});
    ASSERT_FALSE(empty);
    EXPECT_EQ(empty.error().code, SocketDispatchErrorCode::kInvalidArgument);
    ASSERT_TRUE(dispatcher.Rearm());
    EXPECT_EQ(backend.streams[0].watches, 0u);
    ASSERT_TRUE(dispatcher.CollectNotifications(1));
    EXPECT_TRUE(dispatcher.events_pending());
}

TEST_F(SocketDispatcherEventsTest, EventCollectionCoalescingAndDeliveryDoNotAllocateOrFree) {
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(Attach(0));
    backend.Push(Event(0, KBS_STREAM_EVENT_READABLE));
    backend.Push(Event(0, KBS_STREAM_EVENT_WRITABLE));
    std::array<FdEvent, 1> output;
    allocations = 0;
    deallocations = 0;
    // 注入数据与断言都在计数窗口外，只测 owner 热路径，不把 gtest 分配计入。
    count_allocations = true;
    const auto first = dispatcher.CollectNotifications(1);
    const auto second = dispatcher.CollectNotifications(1);
    const auto pending = dispatcher.events_pending();
    const auto polled = dispatcher.PollEvents(output);
    count_allocations = false;
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_TRUE(pending);
    ASSERT_EQ(polled, 1u);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(deallocations, 0u);
}
} // namespace
} // namespace socket
} // namespace kbsocket
