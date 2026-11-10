// SPDX-License-Identifier: MulanPSL-2.0
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
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/api/stream.h"
#include "kbsocket/api/stream_runtime.h"
#include "kbsocket/socket/fd_table.hpp"

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
struct NativeRuntime {
    kbs_runtime* value = nullptr;
    kbs_stream_table* streams = nullptr;
    std::array<kbs_stream_handle, 3> handles{};
    std::size_t count = 0;
    ~NativeRuntime() {
        if (!value) {
            return;
        }
        for (std::size_t i = 0; i < count; ++i) {
            kbs_stream_status status{};
            if (kbs_stream_inspect(streams, handles[i], &status, nullptr) == 0) {
                EXPECT_EQ(kbs_stream_close(streams, handles[i], nullptr), 0);
                EXPECT_EQ(kbs_stream_release(streams, handles[i], nullptr), 0);
            }
        }
        EXPECT_EQ(kbs_runtime_close(value, nullptr), 0);
        EXPECT_EQ(kbs_runtime_destroy(&value, nullptr), 0);
    }
    bool Open() {
        kbs_runtime_config config{};
        if (kbs_runtime_config_init(&config, nullptr) != 0) {
            return false;
        }
        config.table_id = 9101;
        config.stream_capacity = 2;
        config.listener_capacity = 0;
        config.max_wait_ms = 1;
        return kbs_runtime_create(&config, &value, nullptr) == 0 && kbs_runtime_streams(value, &streams, nullptr) == 0;
    }
    bool Adopt(int& fd, kbs_stream_handle& output) {
        if (kbs_runtime_adopt_tcp(value, &fd, &output, nullptr) != 0) {
            return false;
        }
        handles[count++] = output;
        return true;
    }
    bool Close(kbs_stream_handle stream) {
        return kbs_stream_close(streams, stream, nullptr) == 0 && kbs_stream_release(streams, stream, nullptr) == 0;
    }
};
void ExpectBytes(int fd, std::span<const std::byte> expected) {
    std::array<std::byte, 3> output{};
    ASSERT_EQ(expected.size(), output.size());
    std::size_t received = 0;
    while (received != output.size()) {
        ASSERT_TRUE(Wait(fd, POLLIN));
        const auto count = ::recv(fd, output.data() + received, output.size() - received, 0);
        ASSERT_GT(count, 0);
        received += static_cast<std::size_t>(count);
    }
    EXPECT_TRUE(std::equal(output.begin(), output.end(), expected.begin(), expected.end()));
}
void ExpectEmpty(int fd) {
    std::byte value{};
    EXPECT_EQ(::recv(fd, &value, 1, MSG_DONTWAIT), -1);
    EXPECT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
}

TEST(FdTableStreamTest, ReusedFdDispatchesQueuedAndNewRequestsToTheirOriginalNativeStreams) {
    NativeRuntime runtime;
    ASSERT_TRUE(runtime.Open());
    TcpPair old_pair, new_pair, reused_pair;
    ASSERT_TRUE(old_pair.Open());
    ASSERT_TRUE(new_pair.Open());
    kbs_stream_handle old_stream{}, new_stream{}, reused_stream{};
    ASSERT_TRUE(runtime.Adopt(old_pair.left.value, old_stream));
    ASSERT_TRUE(runtime.Adopt(new_pair.left.value, new_stream));
    FdTable table;
    ASSERT_TRUE(table.Open({.table_id = 9102, .fd_limit = 256, .binding_capacity = 2, .reader_capacity = 2}));
    const auto first = table.Publish(73, {21, old_stream});
    ASSERT_TRUE(first);
    std::optional<FdLease> queued;
    // 模拟应用线程把已取得的绑定引用交给 owner；此线程绝不调用单 owner 的 C stream。
    std::jthread producer([&] {
        auto reader = table.RegisterReader();
        if (!reader) {
            return;
        }
        auto lease = table.Lookup(*reader, *first);
        if (lease) {
            queued.emplace(std::move(*lease));
        }
    });
    producer.join();
    ASSERT_TRUE(queued);
    const auto first_retired = table.Unbind(*first);
    ASSERT_TRUE(first_retired);
    const auto second = table.Publish(73, {21, new_stream});
    ASSERT_TRUE(second);
    EXPECT_FALSE(queued->current());
    EXPECT_EQ(queued->binding().id, *first);

    const std::array old_payload{std::byte{1}, std::byte{2}, std::byte{3}};
    const std::array new_payload{std::byte{4}, std::byte{5}, std::byte{6}};
    kbs_stream_io io{};
    // 延后执行的操作只使用已固定的完整 stream 身份，不能按同一整数 fd 重新选中新连接。
    ASSERT_EQ(kbs_stream_write(runtime.streams, queued->binding().target.stream, old_payload.data(), old_payload.size(),
                               &io, nullptr),
              0);
    EXPECT_EQ(io.bytes, old_payload.size());
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(old_pair.right.value, old_payload));
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(new_pair.right.value));
    auto reader = table.RegisterReader();
    ASSERT_TRUE(reader);
    auto current = table.Lookup(*reader, *second);
    ASSERT_TRUE(current);
    ASSERT_EQ(kbs_stream_write(runtime.streams, current->binding().target.stream, new_payload.data(),
                               new_payload.size(), &io, nullptr),
              0);
    EXPECT_EQ(io.bytes, new_payload.size());
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(new_pair.right.value, new_payload));
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(old_pair.right.value));
    current->Reset();

    // 只有旧操作执行结束且真实流 Close/Release 成功后确认退休；回调持有的元数据引用仍阻止 Collect。
    ASSERT_TRUE(runtime.Close(old_stream));
    ASSERT_TRUE(table.ConfirmClosed(*first_retired));
    std::array<FdBinding, 2> collected;
    auto count = table.Collect(collected, 2);
    ASSERT_TRUE(count);
    EXPECT_EQ(*count, 0u);
    queued.reset();
    count = table.Collect(collected, 2);
    ASSERT_TRUE(count);
    ASSERT_EQ(*count, 1u);
    EXPECT_EQ(collected[0].id, *first);

    // StreamBinding 自身也复用旧槽并提升代次；陈旧应用句柄和陈旧 FD 事件都不能命中新的流。
    ASSERT_TRUE(reused_pair.Open());
    ASSERT_TRUE(runtime.Adopt(reused_pair.left.value, reused_stream));
    EXPECT_EQ(reused_stream.slot, old_stream.slot);
    EXPECT_NE(reused_stream.generation, old_stream.generation);
    const auto second_retired = table.Unbind(*second);
    ASSERT_TRUE(second_retired);
    const auto third = table.Publish(73, {21, reused_stream});
    ASSERT_TRUE(third);
    auto stale = table.Lookup(*reader, *first);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), FdError::kStaleBinding);
    stale = table.Lookup(*reader, *second);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), FdError::kStaleBinding);
    EXPECT_EQ(kbs_stream_write(runtime.streams, old_stream, old_payload.data(), old_payload.size(), &io, nullptr),
              KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(io.bytes, 0u);
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(reused_pair.right.value));
    const std::array final_payload{std::byte{7}, std::byte{8}, std::byte{9}};
    current = table.Lookup(*reader, *third);
    ASSERT_TRUE(current);
    ASSERT_EQ(kbs_stream_write(runtime.streams, current->binding().target.stream, final_payload.data(),
                               final_payload.size(), &io, nullptr),
              0);
    ASSERT_NO_FATAL_FAILURE(ExpectBytes(reused_pair.right.value, final_payload));
    ASSERT_NO_FATAL_FAILURE(ExpectEmpty(new_pair.right.value));
    current->Reset();
    ASSERT_TRUE(runtime.Close(new_stream));
    ASSERT_TRUE(table.ConfirmClosed(*second_retired));
    const auto third_retired = table.Unbind(*third);
    ASSERT_TRUE(third_retired);
    ASSERT_TRUE(runtime.Close(reused_stream));
    ASSERT_TRUE(table.ConfirmClosed(*third_retired));
    count = table.Collect(collected, 2);
    ASSERT_TRUE(count);
    EXPECT_EQ(*count, 2u);
    reader->Reset();
    EXPECT_TRUE(table.Close(true));
}

} // namespace
} // namespace socket
} // namespace kbsocket
