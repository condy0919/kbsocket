// SPDX-License-Identifier: MulanPSL-2.0
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

#include <gtest/gtest.h>

#include "kbsocket/api/stream.h"
#include "kbsocket/api/stream_runtime.h"
#include "kbsocket/runtime/progress_source.hpp"
#include "kbsocket/socket/fd_command_queue.hpp"
#include "kbsocket/socket/fd_table.hpp"
#include "kbsocket/socket/socket_dispatcher.hpp"

namespace kbsocket {
namespace socket {
namespace {
struct NativeFd {
    int value = -1;
    ~NativeFd() {
        if (value >= 0) {
            ::close(value);
        }
    }
};
bool Wait(int fd, short events) {
    pollfd item{fd, events, 0};
    return ::poll(&item, 1, 1000) == 1 && (item.revents & (events | POLLERR | POLLHUP));
}
struct TcpPair {
    NativeFd local, peer;
    bool Open() {
        NativeFd listener;
        listener.value = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (listener.value < 0) {
            return false;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listener.value, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
            ::listen(listener.value, 8) < 0) {
            return false;
        }
        socklen_t length = sizeof(address);
        if (::getsockname(listener.value, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
            return false;
        }
        peer.value = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (peer.value < 0 ||
            (::connect(peer.value, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 &&
             errno != EINPROGRESS) ||
            !Wait(listener.value, POLLIN)) {
            return false;
        }
        local.value = ::accept4(listener.value, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (local.value < 0 || !Wait(peer.value, POLLOUT)) {
            return false;
        }
        int error = 0;
        length = sizeof(error);
        return ::getsockopt(peer.value, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && error == 0;
    }
};

// 测试只通过 dispatcher 消费整表事件。所有 I/O 和 Watch 经命令队列执行，
// Runtime 与收集/领取事件保持同一 owner；清理不能绕过未完成的流退休责任。
struct Harness {
    kbs_runtime* runtime = nullptr;
    kbs_stream_table* streams = nullptr;
    std::optional<CStreamBackend> backend;
    SocketDispatcher dispatcher;
    std::array<kbs_stream_handle, 4> adopted{};
    std::array<FdBindingId, 4> attached{};
    std::array<FdCommandTicket, 32> tickets{};
    std::size_t adopted_count = 0, attached_count = 0, ticket_count = 0;
    bool opened = false;

    ~Harness() {
        if (!runtime) {
            return;
        }
        if (opened) {
            dispatcher.StopAccepting();
            for (std::size_t i = 0; i < attached_count; ++i) {
                const auto status = dispatcher.InspectBinding(attached[i]);
                if (status && status->stage == SocketRetireStage::kActive) {
                    EXPECT_TRUE(dispatcher.Detach(attached[i]));
                }
            }
            std::array<std::byte, 64> output{};
            for (unsigned round = 0; round < 16; ++round) {
                EXPECT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 32, kbs_runtime_now_ns()));
                for (std::size_t i = 0; i < ticket_count; ++i) {
                    (void)dispatcher.TakeCompletion(tickets[i], output);
                }
                EXPECT_TRUE(dispatcher.RetireStreams(32));
                EXPECT_TRUE(dispatcher.Progress(runtime::ProgressKind::kRetire, 32, kbs_runtime_now_ns()));
            }
            const auto closed = dispatcher.Close(true);
            EXPECT_TRUE(closed);
            if (!closed) {
                return;
            }
        }
        for (std::size_t i = 0; i < adopted_count; ++i) {
            kbs_stream_status status{};
            if (kbs_stream_inspect(streams, adopted[i], &status, nullptr) == 0) {
                EXPECT_EQ(kbs_stream_close(streams, adopted[i], nullptr), 0);
                EXPECT_EQ(kbs_stream_release(streams, adopted[i], nullptr), 0);
            }
        }
        EXPECT_EQ(kbs_runtime_close(runtime, nullptr), 0);
        EXPECT_EQ(kbs_runtime_destroy(&runtime, nullptr), 0);
    }
    bool Open() {
        static std::uint64_t instance = 10200;
        kbs_runtime_config config{};
        if (kbs_runtime_config_init(&config, nullptr) != 0) {
            return false;
        }
        config.table_id = ++instance;
        config.stream_capacity = 4;
        config.listener_capacity = 0;
        config.max_io_bytes = 64;
        config.events = 1;
        config.max_wait_ms = 1;
        if (kbs_runtime_create(&config, &runtime, nullptr) != 0 ||
            kbs_runtime_streams(runtime, &streams, nullptr) != 0) {
            return false;
        }
        backend.emplace(streams);
        SocketDispatcherConfig dispatch;
        dispatch.owner_id = ++instance;
        dispatch.bindings.table_id = ++instance;
        dispatch.bindings.fd_limit = 65536;
        dispatch.bindings.binding_capacity = 8;
        dispatch.bindings.reader_capacity = 4;
        dispatch.commands.queue_id = ++instance;
        dispatch.commands.capacity = 8;
        dispatch.commands.max_io_bytes = 64;
        dispatch.commands.payload_budget_bytes = 512;
        dispatch.stream_capacity = 4;
        dispatch.events = true;
        opened = dispatcher.Open(*backend, dispatch).has_value();
        return opened;
    }
    std::optional<FdBindingId> AdoptAndAttach(TcpPair& pair, kbs_stream_handle& stream) {
        if (kbs_runtime_adopt_tcp(runtime, &pair.local.value, &stream, nullptr) != 0) {
            return std::nullopt;
        }
        adopted[adopted_count++] = stream;
        const auto binding = dispatcher.Attach(stream);
        if (!binding) {
            return std::nullopt;
        }
        attached[attached_count++] = *binding;
        return *binding;
    }
    std::optional<FdCommandCompletion> Command(FdReader& reader, FdBindingId binding, FdCommand command,
                                               std::span<std::byte> output = {}) {
        auto lease = dispatcher.Lookup(reader, binding);
        if (!lease || ticket_count == tickets.size()) {
            return std::nullopt;
        }
        const auto ticket = dispatcher.TrySubmit(*lease, command);
        if (!ticket) {
            return std::nullopt;
        }
        tickets[ticket_count++] = *ticket;
        if (!dispatcher.Progress(runtime::ProgressKind::kMailbox, 32, kbs_runtime_now_ns())) {
            return std::nullopt;
        }
        const auto completion = dispatcher.TakeCompletion(*ticket, output);
        return completion ? std::optional{*completion} : std::nullopt;
    }
    bool Watch(FdReader& reader, FdBindingId binding, bool readable, bool writable) {
        const auto completion =
            Command(reader, binding, {.kind = FdCommandKind::kWatch, .readable = readable, .writable = writable});
        return completion && completion->result.has_value();
    }
    bool Step(bool collect = true) {
        kbs_runtime_progress tick{};
        return kbs_runtime_step(runtime, 0, &tick, nullptr) == 0 &&
               (!collect || dispatcher.CollectNotifications(32).has_value());
    }
    bool AwaitQueued(bool collect) {
        const auto deadline = kbs_runtime_now_ns() + 3'000'000'000ull;
        do {
            kbs_runtime_progress tick{};
            if (kbs_runtime_step(runtime, 0, &tick, nullptr) != 0 ||
                (collect && !dispatcher.CollectNotifications(32))) {
                return false;
            }
            if (collect ? dispatcher.events_pending() : tick.pending_events != 0) {
                return true;
            }
        } while (kbs_runtime_now_ns() < deadline);
        return false;
    }
    std::optional<FdEvent> AwaitFlag(std::uint32_t flags) {
        const auto deadline = kbs_runtime_now_ns() + 3'000'000'000ull;
        do {
            if (!Step()) {
                return std::nullopt;
            }
            std::array<FdEvent, 4> events{};
            const auto count = dispatcher.PollEvents(events);
            if (!count) {
                return std::nullopt;
            }
            for (std::size_t i = 0; i < *count; ++i) {
                if ((events[i].event.flags & flags) == flags) {
                    return events[i];
                }
            }
        } while (kbs_runtime_now_ns() < deadline);
        return std::nullopt;
    }
    void DrainInitial() {
        ASSERT_TRUE(Step());
        std::array<FdEvent, 4> events{};
        ASSERT_TRUE(dispatcher.PollEvents(events));
        ASSERT_FALSE(dispatcher.events_pending());
    }
    void ExpectNoReadiness() {
        for (unsigned i = 0; i < 8; ++i) {
            ASSERT_TRUE(Step());
            std::array<FdEvent, 4> events{};
            const auto count = dispatcher.PollEvents(events);
            ASSERT_TRUE(count);
            for (std::size_t j = 0; j < *count; ++j) {
                EXPECT_EQ(events[j].event.flags & (KBS_STREAM_EVENT_READABLE | KBS_STREAM_EVENT_WRITABLE), 0u);
            }
        }
    }
};

void ExpectIdentity(const FdEvent& event, FdBindingId binding, kbs_stream_handle stream) {
    EXPECT_EQ(event.binding.id, binding);
    EXPECT_EQ(event.binding.target.stream.table_id, stream.table_id);
    EXPECT_EQ(event.binding.target.stream.generation, stream.generation);
    EXPECT_EQ(event.binding.target.stream.slot, stream.slot);
    EXPECT_EQ(event.event.stream.table_id, stream.table_id);
    EXPECT_EQ(event.event.stream.generation, stream.generation);
    EXPECT_EQ(event.event.stream.slot, stream.slot);
}

TEST(SocketDispatcherEventsTcpTest, WholeOneShotAndRearmObserveAlreadyReadableSocket) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    kbs_stream_handle stream{};
    const auto binding = harness.AdoptAndAttach(pair, stream);
    ASSERT_TRUE(binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    ASSERT_NO_FATAL_FAILURE(harness.DrainInitial());

    ASSERT_TRUE(harness.Watch(*reader, *binding, true, true));
    auto event = harness.AwaitFlag(KBS_STREAM_EVENT_WRITABLE);
    ASSERT_TRUE(event);
    ExpectIdentity(*event, *binding, stream);
    // 可写消耗整个订阅；之后对端有数据也不能复用之前的读关注。
    const std::byte value{0x4a};
    ASSERT_EQ(::send(pair.peer.value, &value, 1, MSG_NOSIGNAL), 1);
    ASSERT_NO_FATAL_FAILURE(harness.ExpectNoReadiness());
    ASSERT_TRUE(harness.Watch(*reader, *binding, true, false));
    event = harness.AwaitFlag(KBS_STREAM_EVENT_READABLE);
    ASSERT_TRUE(event);
    ExpectIdentity(*event, *binding, stream);
    ASSERT_NO_FATAL_FAILURE(harness.ExpectNoReadiness());

    // 不消费 socket 数据，重新订阅仍应收到提示；PollEvents 本身不能偷偷重新 arm。
    ASSERT_TRUE(harness.Watch(*reader, *binding, true, false));
    event = harness.AwaitFlag(KBS_STREAM_EVENT_READABLE);
    ASSERT_TRUE(event);
    ExpectIdentity(*event, *binding, stream);
    EXPECT_EQ(event->event.read_eof, 0u);
}

TEST(SocketDispatcherEventsTcpTest, CancelPreservesQueuedHintsInBothEventLayers) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    kbs_stream_handle stream{};
    const auto binding = harness.AdoptAndAttach(pair, stream);
    ASSERT_TRUE(binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    ASSERT_NO_FATAL_FAILURE(harness.DrainInitial());

    // 分别停在 C stream 队列和 FD 队列，再执行取消。已排队历史提示都应保留，
    // 但这些提示不携带新的 Watch 命令 token，也不代表重新开启关注。
    for (const bool collected : {false, true}) {
        ASSERT_TRUE(harness.Watch(*reader, *binding, false, true));
        ASSERT_TRUE(harness.AwaitQueued(collected));
        ASSERT_TRUE(harness.Watch(*reader, *binding, false, false));
        ASSERT_TRUE(harness.dispatcher.CollectNotifications(32));
        std::array<FdEvent, 4> events{};
        const auto count = harness.dispatcher.PollEvents(events);
        ASSERT_EQ(count, 1u);
        ExpectIdentity(events[0], *binding, stream);
        EXPECT_NE(events[0].event.flags & KBS_STREAM_EVENT_WRITABLE, 0u);
        ASSERT_NO_FATAL_FAILURE(harness.ExpectNoReadiness());
    }
}

TEST(SocketDispatcherEventsTcpTest, PeerShutdownHintDoesNotInventReadEof) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    kbs_stream_handle stream{};
    const auto binding = harness.AdoptAndAttach(pair, stream);
    ASSERT_TRUE(binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    ASSERT_NO_FATAL_FAILURE(harness.DrainInitial());

    // FIN 前仍有一字节未读，任何原生关闭提示都不能让桥接层提前报告 EOF。
    const std::byte value{0x52};
    ASSERT_EQ(::send(pair.peer.value, &value, 1, MSG_NOSIGNAL), 1);
    ASSERT_EQ(::shutdown(pair.peer.value, SHUT_WR), 0);
    ASSERT_TRUE(harness.Watch(*reader, *binding, true, false));
    auto event = harness.AwaitFlag(KBS_STREAM_EVENT_READABLE);
    ASSERT_TRUE(event);
    ExpectIdentity(*event, *binding, stream);
    EXPECT_EQ(event->event.read_eof, 0u);
    std::array<std::byte, 1> output{};
    auto completion = harness.Command(*reader, *binding, {.kind = FdCommandKind::kRead, .bytes = 1}, output);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, 1u);
    EXPECT_EQ(completion->result->io.eof, 0u);
    EXPECT_EQ(output[0], value);

    ASSERT_TRUE(harness.Watch(*reader, *binding, true, false));
    event = harness.AwaitFlag(KBS_STREAM_EVENT_READABLE);
    ASSERT_TRUE(event);
    EXPECT_EQ(event->event.read_eof, 0u);
    EXPECT_NE(event->event.flags & KBS_STREAM_EVENT_PEER_SHUTDOWN, 0u);
    completion = harness.Command(*reader, *binding, {.kind = FdCommandKind::kRead, .bytes = 1}, output);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, 0u);
    EXPECT_EQ(completion->result->io.eof, 1u);
    // EOF 只来自实际 Read；再次 Watch 时可以按已确认的快照立即报告。
    ASSERT_TRUE(harness.Watch(*reader, *binding, true, false));
    event = harness.AwaitFlag(KBS_STREAM_EVENT_READABLE);
    ASSERT_TRUE(event);
    EXPECT_EQ(event->event.read_eof, 1u);
}

TEST(SocketDispatcherEventsTcpTest, DetachDropsBothQueuedAndLateEventsBeforeFdReuse) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair old_pair, new_pair;
    ASSERT_TRUE(old_pair.Open());
    ASSERT_TRUE(new_pair.Open());
    kbs_stream_handle old_stream{}, new_stream{};
    const auto old_binding = harness.AdoptAndAttach(old_pair, old_stream);
    ASSERT_TRUE(old_binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    ASSERT_NO_FATAL_FAILURE(harness.DrainInitial());
    ASSERT_TRUE(harness.Watch(*reader, *old_binding, false, true));
    ASSERT_TRUE(harness.AwaitQueued(true));

    // FD 队列保留旧 OUT，C 队列再保留迟到的 IN；解绑必须同时隔离两层旧身份。
    ASSERT_TRUE(harness.Watch(*reader, *old_binding, true, false));
    const std::byte value{0x6c};
    ASSERT_EQ(::send(old_pair.peer.value, &value, 1, MSG_NOSIGNAL), 1);
    ASSERT_TRUE(harness.AwaitQueued(false));
    ASSERT_TRUE(harness.dispatcher.Detach(*old_binding));
    EXPECT_FALSE(harness.dispatcher.events_pending());
    const auto new_binding = harness.AdoptAndAttach(new_pair, new_stream);
    ASSERT_TRUE(new_binding);
    // 两对 socket 已提前创建，期间没有新增 fd；Linux 重用刚释放的最小 eventfd 数值。
    ASSERT_EQ(new_binding->fd, old_binding->fd);
    EXPECT_NE(new_binding->generation, old_binding->generation);
    ASSERT_TRUE(harness.dispatcher.CollectNotifications(32));
    std::array<FdEvent, 4> events{};
    const auto count = harness.dispatcher.PollEvents(events);
    ASSERT_EQ(count, 1u);
    ExpectIdentity(events[0], *new_binding, new_stream);
    EXPECT_EQ(events[0].event.flags & (KBS_STREAM_EVENT_READABLE | KBS_STREAM_EVENT_WRITABLE), 0u);
    EXPECT_GE(harness.dispatcher.stats().unmapped_events, 1u);
    EXPECT_FALSE(harness.dispatcher.Lookup(*reader, *old_binding));

    ASSERT_TRUE(harness.Watch(*reader, *new_binding, false, true));
    const auto event = harness.AwaitFlag(KBS_STREAM_EVENT_WRITABLE);
    ASSERT_TRUE(event);
    ExpectIdentity(*event, *new_binding, new_stream);
    EXPECT_NE(event->binding.id, *old_binding);
}

} // namespace
} // namespace socket
} // namespace kbsocket
