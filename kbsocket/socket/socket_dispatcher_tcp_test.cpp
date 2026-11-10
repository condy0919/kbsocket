// SPDX-License-Identifier: MulanPSL-2.0
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <thread>

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
    NativeFd left, right;
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
        left.value = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (left.value < 0 ||
            (::connect(left.value, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 &&
             errno != EINPROGRESS) ||
            !Wait(listener.value, POLLIN)) {
            return false;
        }
        right.value = ::accept4(listener.value, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (right.value < 0 || !Wait(left.value, POLLOUT)) {
            return false;
        }
        int error = 0;
        length = sizeof(error);
        return ::getsockopt(left.value, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && error == 0;
    }
};
void ExpectBytes(int fd, std::span<const std::byte> expected) {
    std::array<std::byte, 64> output{};
    ASSERT_LE(expected.size(), output.size());
    std::size_t received = 0;
    while (received < expected.size()) {
        ASSERT_TRUE(Wait(fd, POLLIN));
        const auto count = ::recv(fd, output.data() + received, expected.size() - received, 0);
        ASSERT_GT(count, 0);
        received += static_cast<std::size_t>(count);
    }
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin()));
}
void ExpectEmpty(int fd) {
    std::byte value{};
    EXPECT_EQ(::recv(fd, &value, 1, MSG_DONTWAIT), -1);
    EXPECT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
}

// Fixture 只在 owner 线程推进真实 Runtime。失败清理也先停客户端、领取命令，再退休绑定；
// 如果 dispatcher 无法关闭，则保留 Runtime 指针，不能为了清理测试而绕过尚存责任。
struct Harness {
    kbs_runtime* runtime = nullptr;
    kbs_stream_table* streams = nullptr;
    std::optional<CStreamBackend> backend;
    SocketDispatcher dispatcher;
    std::array<kbs_stream_handle, 8> adopted{};
    std::array<FdBindingId, 8> attached{};
    std::array<FdCommandTicket, 32> submitted{};
    std::size_t adopted_count = 0, attached_count = 0, submitted_count = 0;
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
            for (unsigned i = 0; i < 16; ++i) {
                EXPECT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 32, kbs_runtime_now_ns()));
                for (std::size_t j = 0; j < submitted_count; ++j) {
                    (void)dispatcher.TakeCompletion(submitted[j], output);
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
        // Attach 失败时真实流仍由宿主拥有；成功绑定的流应已被 dispatcher Release。
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
    bool Open(std::uint32_t max_io_bytes = 64) {
        static std::uint64_t instance = 9400;
        kbs_runtime_config config{};
        if (kbs_runtime_config_init(&config, nullptr) != 0) {
            return false;
        }
        config.table_id = ++instance;
        config.stream_capacity = 4;
        config.listener_capacity = 0;
        config.max_io_bytes = max_io_bytes;
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
        opened = dispatcher.Open(*backend, dispatch).has_value();
        return opened;
    }
    bool Adopt(int& fd, kbs_stream_handle& stream) {
        if (kbs_runtime_adopt_tcp(runtime, &fd, &stream, nullptr) != 0) {
            return false;
        }
        adopted[adopted_count++] = stream;
        return true;
    }
    std::optional<FdBindingId> Attach(kbs_stream_handle stream) {
        const auto binding = dispatcher.Attach(stream);
        if (!binding) {
            return std::nullopt;
        }
        attached[attached_count++] = *binding;
        return *binding;
    }
    void Track(FdCommandTicket ticket) {
        ASSERT_LT(submitted_count, submitted.size());
        submitted[submitted_count++] = ticket;
    }
    std::optional<FdCommandTicket> Submit(FdReader& reader, FdBindingId binding, FdCommand command,
                                          std::span<const std::byte> input = {}) {
        auto lease = dispatcher.Lookup(reader, binding);
        if (!lease) {
            return std::nullopt;
        }
        const auto ticket = dispatcher.TrySubmit(*lease, command, input);
        if (!ticket) {
            return std::nullopt;
        }
        Track(*ticket);
        return *ticket;
    }
    bool Mailbox() {
        kbs_runtime_progress tick{};
        return kbs_runtime_step(runtime, 0, &tick, nullptr) == 0 &&
               dispatcher.Progress(runtime::ProgressKind::kMailbox, 32, kbs_runtime_now_ns()).has_value();
    }
    void Retire(unsigned rounds = 8) {
        for (unsigned i = 0; i < rounds; ++i) {
            // 真实 Close/Release 属于 owner 控制路径；热循环仅收集已确认的 CPU 元数据。
            ASSERT_TRUE(dispatcher.RetireStreams(32));
            ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kRetire, 32, kbs_runtime_now_ns()));
        }
    }
};

TEST(SocketDispatcherTcpTest, ProducerCopiesWriteAndOwnerReportsOnlyAcceptedNativePrefix) {
    Harness harness;
    ASSERT_TRUE(harness.Open(2));
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int transport_fd = pair.left.value;
    kbs_stream_handle stream{};
    ASSERT_TRUE(harness.Adopt(pair.left.value, stream));
    EXPECT_EQ(pair.left.value, -1);
    const auto binding = harness.Attach(stream);
    ASSERT_TRUE(binding);
    EXPECT_NE(binding->fd, transport_fd);
    EXPECT_NE(::fcntl(binding->fd, F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(binding->fd, F_GETFD) & FD_CLOEXEC, 0);
    EXPECT_GE(::fcntl(transport_fd, F_GETFD), 0);

    // 生产者提交后立刻改写源内存，owner 此时尚未执行；这里只访问队列与 FD 引用。
    std::optional<FdCommandTicket> ticket;
    std::jthread producer([&] {
        auto reader = harness.dispatcher.RegisterReader();
        if (!reader) {
            return;
        }
        auto lease = harness.dispatcher.Lookup(*reader, *binding);
        if (!lease) {
            return;
        }
        std::array payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
        const auto queued = harness.dispatcher.TrySubmit(*lease, {.kind = FdCommandKind::kWrite, .bytes = 4}, payload);
        std::fill(payload.begin(), payload.end(), std::byte{99});
        if (queued) {
            ticket = *queued;
        }
    });
    producer.join();
    ASSERT_TRUE(ticket);
    harness.Track(*ticket);
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(pair.right.value));
    ASSERT_TRUE(harness.Mailbox());
    const auto completion = harness.dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->binding.id, *binding);
    EXPECT_EQ(completion->result->io.bytes, 2u);
    const std::array expected{std::byte{1}, std::byte{2}};
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(pair.right.value, expected));
    // 未接受的尾部不能被调度器补发；进度推进也不得重复执行已经完成的命令。
    ASSERT_TRUE(harness.Mailbox());
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(pair.right.value));
}

TEST(SocketDispatcherTcpTest, ReadWouldBlockDataAndBothHalfClosesRemainDistinct) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int transport_fd = pair.left.value;
    kbs_stream_handle stream{};
    ASSERT_TRUE(harness.Adopt(pair.left.value, stream));
    const auto binding = harness.Attach(stream);
    ASSERT_TRUE(binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    auto ticket = harness.Submit(*reader, *binding, {.kind = FdCommandKind::kRead, .bytes = 8});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(harness.Mailbox());
    auto completion = harness.dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_FALSE(completion->result);
    EXPECT_EQ(completion->result.error().code, KBS_STREAM_WOULD_BLOCK);

    // 真实对端写入后一次 owner Read 得到可领取的拷贝；输出不足不丢数据或释放原结果。
    const std::array payload{std::byte{7}, std::byte{8}, std::byte{9}};
    ASSERT_EQ(::send(pair.right.value, payload.data(), payload.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(payload.size()));
    ASSERT_TRUE(Wait(transport_fd, POLLIN));
    ticket = harness.Submit(*reader, *binding, {.kind = FdCommandKind::kRead, .bytes = 8});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(harness.Mailbox());
    std::array<std::byte, 1> too_small{};
    completion = harness.dispatcher.TakeCompletion(*ticket, too_small);
    ASSERT_FALSE(completion);
    EXPECT_EQ(completion.error().code, FdQueueErrorCode::kOutputTooSmall);
    std::array<std::byte, 8> output{};
    completion = harness.dispatcher.TakeCompletion(*ticket, output);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, payload.size());
    EXPECT_EQ(completion->result->io.eof, 0u);
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), output.begin()));

    ticket = harness.Submit(*reader, *binding, {.kind = FdCommandKind::kShutdownWrite});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(harness.Mailbox());
    completion = harness.dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    ASSERT_TRUE(Wait(pair.right.value, POLLIN));
    EXPECT_EQ(::recv(pair.right.value, output.data(), output.size(), 0), 0);
    // 本端写方向关闭不等于读 EOF；必须等对端也发送 FIN，再由实际 Read 确认。
    ticket = harness.Submit(*reader, *binding, {.kind = FdCommandKind::kRead, .bytes = 8});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(harness.Mailbox());
    completion = harness.dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_FALSE(completion->result);
    EXPECT_EQ(completion->result.error().code, KBS_STREAM_WOULD_BLOCK);
    ASSERT_EQ(::shutdown(pair.right.value, SHUT_WR), 0);
    ASSERT_TRUE(Wait(transport_fd, POLLIN));
    ticket = harness.Submit(*reader, *binding, {.kind = FdCommandKind::kRead, .bytes = 8});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(harness.Mailbox());
    completion = harness.dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, 0u);
    EXPECT_EQ(completion->result->io.eof, 1u);
}

TEST(SocketDispatcherTcpTest, DetachedQueuedAndDelayedRequestsNeverReachReplacementStream) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair old_pair, new_pair;
    ASSERT_TRUE(old_pair.Open());
    ASSERT_TRUE(new_pair.Open());
    kbs_stream_handle old_stream{}, new_stream{};
    ASSERT_TRUE(harness.Adopt(old_pair.left.value, old_stream));
    ASSERT_TRUE(harness.Adopt(new_pair.left.value, new_stream));
    const auto old_binding = harness.Attach(old_stream);
    ASSERT_TRUE(old_binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    auto late_lease = harness.dispatcher.Lookup(*reader, *old_binding);
    ASSERT_TRUE(late_lease);
    const std::array old_payload{std::byte{11}, std::byte{12}, std::byte{13}};
    const auto old_ticket =
        harness.Submit(*reader, *old_binding, {.kind = FdCommandKind::kWrite, .bytes = 3}, old_payload);
    ASSERT_TRUE(old_ticket);
    ASSERT_TRUE(harness.dispatcher.Detach(*old_binding));
    const auto new_binding = harness.Attach(new_stream);
    ASSERT_TRUE(new_binding);
    if (new_binding->fd == old_binding->fd) {
        EXPECT_NE(new_binding->generation, old_binding->generation);
    }
    // 已入队和已取得 Lease 的旧调用都不得按裸 fd 重定向，即使内核复用了相同整数。
    const auto late_ticket =
        harness.dispatcher.TrySubmit(*late_lease, {.kind = FdCommandKind::kWrite, .bytes = 3}, old_payload);
    ASSERT_TRUE(late_ticket);
    harness.Track(*late_ticket);
    const std::array new_payload{std::byte{21}, std::byte{22}, std::byte{23}};
    const auto new_ticket =
        harness.Submit(*reader, *new_binding, {.kind = FdCommandKind::kWrite, .bytes = 3}, new_payload);
    ASSERT_TRUE(new_ticket);
    ASSERT_TRUE(harness.Mailbox());
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(old_pair.right.value));
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(new_pair.right.value, new_payload));
    ASSERT_NO_FATAL_FAILURE(harness.Retire());
    const auto old_status = harness.dispatcher.InspectBinding(*old_binding);
    ASSERT_TRUE(old_status);
    EXPECT_EQ(old_status->stage, SocketRetireStage::kConfirmed);
    EXPECT_EQ(harness.dispatcher.stats().confirmed, 1u);
    // 流已真实 Release，但未领取完成中的 FD Lease 仍阻止退休元数据复用。
    kbs_stream_status native_status{};
    EXPECT_EQ(kbs_stream_inspect(harness.streams, old_stream, &native_status, nullptr), KBS_STREAM_INVALID_HANDLE);
    for (const auto ticket : {*old_ticket, *late_ticket}) {
        const auto completion = harness.dispatcher.TakeCompletion(ticket);
        ASSERT_TRUE(completion);
        ASSERT_FALSE(completion->result);
        EXPECT_EQ(completion->binding.id, *old_binding);
        EXPECT_EQ(completion->result.error().code, KBS_STREAM_CLOSED);
    }
    const auto completion = harness.dispatcher.TakeCompletion(*new_ticket);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, new_payload.size());
    ASSERT_NO_FATAL_FAILURE(harness.Retire());
    EXPECT_FALSE(harness.dispatcher.InspectBinding(*old_binding));
    EXPECT_EQ(harness.dispatcher.stats().confirmed, 0u);
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(new_pair.right.value));
}

TEST(SocketDispatcherTcpTest, LogicalClosePrecedesPhysicalReleaseAndRuntimeDestruction) {
    Harness harness;
    ASSERT_TRUE(harness.Open());
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int transport_fd = pair.left.value;
    kbs_stream_handle stream{};
    ASSERT_TRUE(harness.Adopt(pair.left.value, stream));
    const auto binding = harness.Attach(stream);
    ASSERT_TRUE(binding);
    auto reader = harness.dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    const auto ticket = harness.Submit(*reader, *binding, {.kind = FdCommandKind::kClose});
    ASSERT_TRUE(ticket);
    ASSERT_TRUE(harness.Mailbox());
    EXPECT_EQ(::fcntl(binding->fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    // kRetire 热循环只处理已确认的元数据；未调用控制路径时真实 socket 必须仍然存活。
    ASSERT_TRUE(harness.dispatcher.Progress(runtime::ProgressKind::kRetire, 32, kbs_runtime_now_ns()));
    EXPECT_GE(::fcntl(transport_fd, F_GETFD), 0);
    const auto status = harness.dispatcher.InspectBinding(*binding);
    ASSERT_TRUE(status);
    EXPECT_EQ(status->stage, SocketRetireStage::kClose);
    const auto completion = harness.dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_FALSE(harness.dispatcher.Lookup(*reader, *binding));

    // 逻辑 close 完成不能销毁仍持有底层流的 Runtime；真实退休走分阶段 Close/Release。
    EXPECT_EQ(kbs_runtime_close(harness.runtime, nullptr), KBS_STREAM_BUSY);
    auto* original_runtime = harness.runtime;
    EXPECT_EQ(kbs_runtime_destroy(&harness.runtime, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(harness.runtime, original_runtime);
    ASSERT_NO_FATAL_FAILURE(harness.Retire());
    EXPECT_EQ(::fcntl(transport_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_EQ(harness.dispatcher.stats().bindings.active, 0u);
    EXPECT_EQ(harness.dispatcher.stats().bindings.retired, 0u);
    reader->Reset();
    ASSERT_TRUE(harness.dispatcher.Close(true));
    harness.opened = false;
    EXPECT_EQ(kbs_runtime_close(harness.runtime, nullptr), 0);
    EXPECT_EQ(kbs_runtime_destroy(&harness.runtime, nullptr), 0);
    EXPECT_EQ(harness.runtime, nullptr);
}

} // namespace
} // namespace socket
} // namespace kbsocket
