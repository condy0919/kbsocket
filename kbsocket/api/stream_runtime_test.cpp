// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_runtime.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <new>
#include <span>
#include <vector>

#include <gtest/gtest.h>

extern "C" int KbsStreamRuntimeCHeaderTest(void);
namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
} // namespace
void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* result = std::malloc(bytes ? bytes : 1)) {
        return result;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void operator delete(void* pointer) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept {
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    std::free(pointer);
}

namespace kbsocket {
namespace api {
namespace {
std::uint64_t Now() {
    return kbs_runtime_now_ns();
}
sockaddr_in Loopback() {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}
const sockaddr* Address(const sockaddr_in& address) {
    return reinterpret_cast<const sockaddr*>(&address);
}
struct Fd {
    int value = -1;
    ~Fd() {
        if (value >= 0) {
            ::close(value);
        }
    }
};
bool Wait(int fd, short events) {
    pollfd item{fd, events, 0};
    return ::poll(&item, 1, 1000) == 1 && (item.revents & (events | POLLHUP | POLLERR));
}
struct TcpPair {
    Fd left, right;
    bool Open() {
        Fd listener;
        listener.value = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (listener.value < 0) {
            return false;
        }
        auto address = Loopback();
        if (::bind(listener.value, Address(address), sizeof(address)) < 0 || ::listen(listener.value, 8) < 0) {
            return false;
        }
        socklen_t length = sizeof(address);
        if (::getsockname(listener.value, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
            return false;
        }
        left.value = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (left.value < 0 || (::connect(left.value, Address(address), sizeof(address)) < 0 && errno != EINPROGRESS) ||
            !Wait(listener.value, POLLIN)) {
            return false;
        }
        right.value = ::accept4(listener.value, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        return right.value >= 0 && Wait(left.value, POLLOUT);
    }
};

struct Runtime {
    kbs_runtime* value = nullptr;
    kbs_stream_table* table = nullptr;
    std::vector<kbs_stream_handle> streams;
    std::vector<kbs_listener_handle> listeners;
    ~Runtime() {
        count_allocations = false;
        if (!value) {
            return;
        }
        for (auto stream : streams) {
            kbs_stream_status status{};
            if (kbs_stream_inspect(table, stream, &status, nullptr) == 0) {
                EXPECT_EQ(kbs_stream_close(table, stream, nullptr), 0);
                EXPECT_EQ(kbs_stream_release(table, stream, nullptr), 0);
            }
        }
        for (auto listener : listeners) {
            const auto result = kbs_runtime_close_listener(value, listener, nullptr);
            EXPECT_TRUE(result == 0 || result == KBS_STREAM_INVALID_HANDLE || result == KBS_STREAM_NOT_READY);
        }
        EXPECT_EQ(kbs_runtime_close(value, nullptr), 0);
        EXPECT_EQ(kbs_runtime_destroy(&value, nullptr), 0);
    }
    static kbs_runtime_config Config(std::uint32_t capacity = 8, std::uint32_t listener_capacity = 2) {
        static std::uint64_t next_id = 1000;
        kbs_runtime_config config{};
        EXPECT_EQ(kbs_runtime_config_init(&config, nullptr), 0);
        config.table_id = next_id++;
        config.stream_capacity = capacity;
        config.listener_capacity = listener_capacity;
        config.max_wait_ms = 1;
        return config;
    }
    bool Open(kbs_runtime_config config = Config()) {
        return kbs_runtime_create(&config, &value, nullptr) == 0 && kbs_runtime_streams(value, &table, nullptr) == 0;
    }
    bool Listen(kbs_listener_handle& listener, sockaddr_in& address) {
        address = Loopback();
        if (kbs_runtime_listen_tcp(value, Address(address), sizeof(address), 8, &listener, nullptr) != 0) {
            return false;
        }
        listeners.push_back(listener);
        socklen_t length = sizeof(address);
        return kbs_runtime_listener_address(value, listener, reinterpret_cast<sockaddr*>(&address), &length, nullptr) ==
                   0 &&
               length == sizeof(address) && address.sin_port != 0;
    }
    bool Step() {
        kbs_runtime_progress progress{};
        return kbs_runtime_step(value, 0, &progress, nullptr) == 0;
    }
    bool Connect(const sockaddr_in& address, kbs_stream_handle& stream) {
        if (kbs_runtime_connect_tcp(value, Address(address), sizeof(address), Now() + 3'000'000'000ull, &stream,
                                    nullptr) != 0) {
            return false;
        }
        streams.push_back(stream);
        return true;
    }
    bool Adopt(int& fd, kbs_stream_handle& stream) {
        if (kbs_runtime_adopt_tcp(value, &fd, &stream, nullptr) != 0) {
            return false;
        }
        streams.push_back(stream);
        return true;
    }
    bool Release(kbs_stream_handle stream) {
        return kbs_stream_close(table, stream, nullptr) == 0 && kbs_stream_release(table, stream, nullptr) == 0;
    }
    bool Active(kbs_stream_handle stream) {
        kbs_stream_status status{};
        return kbs_stream_inspect(table, stream, &status, nullptr) == 0 && status.state == KBS_STREAM_TCP_ACTIVE;
    }
};
template <class Predicate>
bool Until(Runtime& left, Runtime& right, Predicate predicate) {
    const auto deadline = Now() + 3'000'000'000ull;
    do {
        if (!left.Step() || !right.Step()) {
            return false;
        }
        if (predicate()) {
            return true;
        }
    } while (Now() < deadline);
    return false;
}
bool ConnectPair(Runtime& server, Runtime& client, kbs_listener_handle listener, const sockaddr_in& address,
                 kbs_stream_handle& sender, kbs_stream_handle& receiver) {
    if (!client.Connect(address, sender)) {
        return false;
    }
    bool accepted = false;
    return Until(server, client, [&] {
        if (!accepted) {
            const auto result = kbs_runtime_accept_tcp(server.value, listener, &receiver, nullptr);
            if (result == 0) {
                server.streams.push_back(receiver);
                accepted = true;
            } else if (result != KBS_STREAM_WOULD_BLOCK) {
                return false;
            }
        }
        return accepted && client.Active(sender) && server.Active(receiver);
    });
}

TEST(StreamRuntimeCAbiTest, CompilesAndLinksAllRuntimeFunctionsAsC) {
    EXPECT_EQ(KbsStreamRuntimeCHeaderTest(), 0);
}
TEST(StreamRuntimeTest, InvalidConfigurationDoesNotPublishRuntime) {
    const auto defaults = Runtime::Config();
    std::array<kbs_runtime_config, 12> invalid;
    invalid.fill(defaults);
    invalid[0].abi_version = 99;
    invalid[1].struct_size = 0;
    invalid[2].table_id = 0;
    invalid[3].stream_capacity = 0;
    invalid[4].stream_capacity = 65537;
    invalid[5].max_io_bytes = 0;
    invalid[6].max_io_bytes = 1024 * 1024 + 1;
    invalid[7].events = 2;
    invalid[8].progress_budget = 0;
    invalid[9].progress_budget = 65;
    invalid[10].max_wait_ms = 1001;
    invalid[11].listener_capacity = 65537;
    for (const auto& config : invalid) {
        kbs_runtime* result = nullptr;
        kbs_stream_error error{};
        EXPECT_EQ(kbs_runtime_create(&config, &result, &error), KBS_STREAM_INVALID_ARGUMENT);
        EXPECT_EQ(result, nullptr);
        EXPECT_EQ(error.code, KBS_STREAM_INVALID_ARGUMENT);
    }
    Runtime runtime;
    auto config = Runtime::Config(1, 0);
    config.events = 0;
    config.max_wait_ms = 0;
    ASSERT_TRUE(runtime.Open(config));
    kbs_listener_handle listener{};
    const auto address = Loopback();
    EXPECT_EQ(kbs_runtime_listen_tcp(runtime.value, Address(address), sizeof(address), 8, &listener, nullptr),
              KBS_STREAM_EXHAUSTED);
}
TEST(StreamRuntimeTest, CreatesConnectsExchangesPrefixesAndFinEntirelyThroughC) {
    Runtime server, client;
    auto config = Runtime::Config();
    config.max_io_bytes = 3;
    ASSERT_TRUE(server.Open(config));
    ASSERT_TRUE(client.Open());
    kbs_listener_handle listener{};
    sockaddr_in address{};
    ASSERT_TRUE(server.Listen(listener, address));
    kbs_stream_handle sender{}, receiver{};
    ASSERT_TRUE(ConnectPair(server, client, listener, address, sender, receiver));
    const std::array<std::byte, 7> bytes{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4},
                                         std::byte{5}, std::byte{6}, std::byte{7}};
    std::array<std::byte, 7> output{};
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write(client.table, sender, bytes.data(), bytes.size(), &io, nullptr), 0);
    ASSERT_EQ(io.bytes, bytes.size());
    ASSERT_EQ(kbs_stream_shutdown_write(client.table, sender, nullptr), 0);
    ASSERT_EQ(kbs_stream_watch(server.table, receiver, 1, 0, nullptr), 0);
    std::uint32_t observed = 0;
    ASSERT_TRUE(Until(server, client, [&] {
        std::array<kbs_stream_event, 4> events{};
        std::size_t count = 0;
        if (kbs_stream_poll_events(server.table, events.data(), events.size(), &count, nullptr) != 0) {
            return false;
        }
        for (std::size_t i = 0; i < count; ++i) {
            observed |= events[i].flags;
        }
        return (observed & KBS_STREAM_EVENT_READABLE) != 0;
    }));
    std::size_t received = 0;
    bool eof = false;
    ASSERT_TRUE(Until(server, client, [&] {
        std::array<std::byte, 8> piece{};
        const auto result = kbs_stream_read(server.table, receiver, piece.data(), piece.size(), Now(), &io, nullptr);
        if (result == KBS_STREAM_WOULD_BLOCK) {
            return false;
        }
        EXPECT_EQ(result, 0);
        EXPECT_LE(io.bytes, 3u);
        if (received + io.bytes <= output.size()) {
            std::copy_n(piece.data(), io.bytes, output.data() + received);
        }
        received += io.bytes;
        eof = io.eof;
        return eof;
    }));
    EXPECT_EQ(received, bytes.size());
    EXPECT_EQ(output, bytes);
    // 对端 FIN 只关闭一个方向，服务端仍可反向写，且受绑定的复制预算限制。
    ASSERT_EQ(kbs_stream_write(server.table, receiver, bytes.data(), bytes.size(), &io, nullptr), 0);
    EXPECT_EQ(io.bytes, 3u);
    ASSERT_TRUE(Until(server, client, [&] {
        return kbs_stream_read(client.table, sender, output.data(), output.size(), Now(), &io, nullptr) == 0 &&
               io.bytes == 3;
    }));
    EXPECT_TRUE(std::equal(bytes.begin(), bytes.begin() + 3, output.begin()));
}
TEST(StreamRuntimeTest, AdoptTransfersOnlyValidConnectedNonblockingFd) {
    Runtime runtime;
    ASSERT_TRUE(runtime.Open(Runtime::Config(1, 0)));
    TcpPair pair, spare;
    ASSERT_TRUE(pair.Open());
    ASSERT_TRUE(spare.Open());
    const int original = pair.right.value;
    const auto flags = ::fcntl(original, F_GETFL);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(original, F_SETFL, flags & ~O_NONBLOCK), 0);
    kbs_stream_handle stream{};
    EXPECT_EQ(kbs_runtime_adopt_tcp(runtime.value, &pair.right.value, &stream, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(pair.right.value, original);
    EXPECT_GE(::fcntl(original, F_GETFD), 0);
    ASSERT_EQ(::fcntl(original, F_SETFL, flags), 0);
    Fd udp;
    udp.value = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_UDP);
    ASSERT_GE(udp.value, 0);
    const int udp_fd = udp.value;
    EXPECT_EQ(kbs_runtime_adopt_tcp(runtime.value, &udp.value, &stream, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(udp.value, udp_fd);
    ASSERT_TRUE(runtime.Adopt(pair.right.value, stream));
    EXPECT_EQ(pair.right.value, -1);
    const int spare_fd = spare.right.value;
    kbs_stream_handle rejected{99, 99, 99, 99};
    EXPECT_EQ(kbs_runtime_adopt_tcp(runtime.value, &spare.right.value, &rejected, nullptr), KBS_STREAM_EXHAUSTED);
    EXPECT_EQ(spare.right.value, spare_fd);
    EXPECT_EQ(rejected.generation, 0u);
    EXPECT_GE(::fcntl(spare_fd, F_GETFD), 0);
}
TEST(StreamRuntimeTest, CapacityRejectsAcceptBeforeTakingQueuedConnection) {
    Runtime server, client;
    ASSERT_TRUE(server.Open(Runtime::Config(1, 1)));
    ASSERT_TRUE(client.Open());
    kbs_listener_handle listener{};
    sockaddr_in address{};
    ASSERT_TRUE(server.Listen(listener, address));
    kbs_stream_handle first{}, accepted{}, second{};
    ASSERT_TRUE(ConnectPair(server, client, listener, address, first, accepted));
    ASSERT_TRUE(client.Connect(address, second));
    ASSERT_TRUE(Until(server, client, [&] { return client.Active(second); }));
    kbs_stream_handle rejected{99, 99, 99, 99};
    EXPECT_EQ(kbs_runtime_accept_tcp(server.value, listener, &rejected, nullptr), KBS_STREAM_EXHAUSTED);
    EXPECT_EQ(rejected.generation, 0u);
    // 满表不应 accept 后关掉连接；释放一个槽后仍可取得原先排队的第二条连接。
    ASSERT_TRUE(server.Release(accepted));
    ASSERT_TRUE(Until(server, client, [&] {
        if (kbs_runtime_accept_tcp(server.value, listener, &accepted, nullptr) != 0) {
            return false;
        }
        server.streams.push_back(accepted);
        return true;
    }));
    const std::byte byte{0x49};
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write(client.table, second, &byte, 1, &io, nullptr), 0);
    std::byte output{};
    ASSERT_TRUE(Until(server, client, [&] {
        return kbs_stream_read(server.table, accepted, &output, 1, Now(), &io, nullptr) == 0 && io.bytes == 1;
    }));
    EXPECT_EQ(output, byte);
}
TEST(StreamRuntimeTest, ListenerHandlesRejectForeignReservedAndReusedSlots) {
    Runtime runtime, other;
    ASSERT_TRUE(runtime.Open(Runtime::Config(2, 1)));
    ASSERT_TRUE(other.Open(Runtime::Config(2, 1)));
    kbs_listener_handle old{}, current{};
    sockaddr_in address{};
    ASSERT_TRUE(runtime.Listen(old, address));
    EXPECT_EQ(kbs_runtime_close_listener(other.value, old, nullptr), KBS_STREAM_INVALID_HANDLE);
    auto invalid = old;
    invalid.reserved = 1;
    EXPECT_EQ(kbs_runtime_close_listener(runtime.value, invalid, nullptr), KBS_STREAM_INVALID_HANDLE);
    ASSERT_EQ(kbs_runtime_close_listener(runtime.value, old, nullptr), 0);
    ASSERT_TRUE(runtime.Listen(current, address));
    EXPECT_EQ(old.slot, current.slot);
    EXPECT_NE(old.generation, current.generation);
    EXPECT_EQ(kbs_runtime_close_listener(runtime.value, old, nullptr), KBS_STREAM_INVALID_HANDLE);
    kbs_stream_handle stream{};
    EXPECT_EQ(kbs_runtime_accept_tcp(runtime.value, old, &stream, nullptr), KBS_STREAM_INVALID_HANDLE);
    EXPECT_EQ(kbs_runtime_accept_tcp(runtime.value, current, &stream, nullptr), KBS_STREAM_WOULD_BLOCK);
}
TEST(StreamRuntimeTest, ListenerFreeListSurvivesFailedBindAndNonLifoReuse) {
    Runtime runtime;
    ASSERT_TRUE(runtime.Open(Runtime::Config(2, 3)));
    std::array<kbs_listener_handle, 3> original{};
    std::array<sockaddr_in, 3> addresses{};
    ASSERT_TRUE(runtime.Listen(original[0], addresses[0]));
    ASSERT_TRUE(runtime.Listen(original[1], addresses[1]));
    // 相同端口已有监听者，失败发生于创建 socket 之后；回滚不能消耗最后一个槽。
    kbs_listener_handle rejected{99, 99, 99, 99};
    EXPECT_EQ(kbs_runtime_listen_tcp(runtime.value, Address(addresses[0]), sizeof(addresses[0]), 8, &rejected, nullptr),
              KBS_STREAM_TRANSPORT);
    EXPECT_EQ(rejected.generation, 0u);
    ASSERT_TRUE(runtime.Listen(original[2], addresses[2]));
    const auto fresh_address = Loopback();
    EXPECT_EQ(
        kbs_runtime_listen_tcp(runtime.value, Address(fresh_address), sizeof(fresh_address), 8, &rejected, nullptr),
        KBS_STREAM_EXHAUSTED);
    // 先释放中间再释放首槽，尾槽保持活动；复用顺序必须由空闲栈决定，不能覆盖活动监听器。
    ASSERT_EQ(kbs_runtime_close_listener(runtime.value, original[1], nullptr), 0);
    ASSERT_EQ(kbs_runtime_close_listener(runtime.value, original[0], nullptr), 0);
    std::array<kbs_listener_handle, 3> replacement{};
    sockaddr_in actual{};
    ASSERT_TRUE(runtime.Listen(replacement[0], actual));
    ASSERT_TRUE(runtime.Listen(replacement[1], actual));
    EXPECT_EQ(replacement[0].slot, original[0].slot);
    EXPECT_EQ(replacement[1].slot, original[1].slot);
    socklen_t length = sizeof(actual);
    ASSERT_EQ(kbs_runtime_listener_address(runtime.value, original[2], reinterpret_cast<sockaddr*>(&actual), &length,
                                           nullptr),
              0);
    EXPECT_EQ(actual.sin_port, addresses[2].sin_port);
    ASSERT_EQ(kbs_runtime_close_listener(runtime.value, original[2], nullptr), 0);
    ASSERT_TRUE(runtime.Listen(replacement[2], actual));
    EXPECT_EQ(replacement[2].slot, original[2].slot);
    for (std::size_t i = 0; i < original.size(); ++i) {
        EXPECT_NE(replacement[i].generation, original[i].generation);
        EXPECT_EQ(kbs_runtime_close_listener(runtime.value, original[i], nullptr), KBS_STREAM_INVALID_HANDLE);
    }
    EXPECT_EQ(
        kbs_runtime_listen_tcp(runtime.value, Address(fresh_address), sizeof(fresh_address), 8, &rejected, nullptr),
        KBS_STREAM_EXHAUSTED);
}
TEST(StreamRuntimeTest, CloseBusyStopsAdmissionButPreservesEstablishedIoAndProgress) {
    Runtime runtime;
    ASSERT_TRUE(runtime.Open());
    TcpPair pair, spare;
    ASSERT_TRUE(pair.Open());
    ASSERT_TRUE(spare.Open());
    kbs_stream_handle stream{};
    ASSERT_TRUE(runtime.Adopt(pair.right.value, stream));
    kbs_listener_handle listener{};
    sockaddr_in address{};
    ASSERT_TRUE(runtime.Listen(listener, address));
    auto* original = runtime.value;
    EXPECT_EQ(kbs_runtime_destroy(&runtime.value, nullptr), KBS_STREAM_BUSY);
    EXPECT_EQ(runtime.value, original);
    EXPECT_EQ(kbs_runtime_close(runtime.value, nullptr), KBS_STREAM_BUSY);
    kbs_stream_handle rejected{};
    EXPECT_EQ(kbs_runtime_adopt_tcp(runtime.value, &spare.right.value, &rejected, nullptr), KBS_STREAM_STOPPED);
    EXPECT_GE(spare.right.value, 0);
    EXPECT_EQ(kbs_runtime_connect_tcp(runtime.value, Address(address), sizeof(address), Now() + 1'000'000'000ull,
                                      &rejected, nullptr),
              KBS_STREAM_STOPPED);
    EXPECT_EQ(kbs_runtime_accept_tcp(runtime.value, listener, &rejected, nullptr), KBS_STREAM_STOPPED);
    const std::byte byte{0x24};
    kbs_stream_io io{};
    EXPECT_EQ(kbs_stream_write(runtime.table, stream, &byte, 1, &io, nullptr), 0);
    EXPECT_TRUE(runtime.Step());
    ASSERT_TRUE(Wait(pair.left.value, POLLIN));
    std::byte read{};
    EXPECT_EQ(::recv(pair.left.value, &read, 1, 0), 1);
    EXPECT_EQ(read, byte);
    ASSERT_TRUE(runtime.Release(stream));
    ASSERT_EQ(kbs_runtime_close_listener(runtime.value, listener, nullptr), 0);
    auto* borrowed = runtime.table;
    ASSERT_EQ(kbs_runtime_close(runtime.value, nullptr), 0);
    EXPECT_EQ(kbs_runtime_close(runtime.value, nullptr), 0);
    EXPECT_EQ(kbs_stream_write(borrowed, stream, &byte, 1, &io, nullptr), KBS_STREAM_NOT_READY);
    kbs_stream_table* table = borrowed;
    EXPECT_EQ(kbs_runtime_streams(runtime.value, &table, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(table, nullptr);
    kbs_runtime_progress progress{99, 99, 99};
    EXPECT_EQ(kbs_runtime_step(runtime.value, 0, &progress, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(progress.processed, 0u);
    ASSERT_EQ(kbs_runtime_destroy(&runtime.value, nullptr), 0);
    EXPECT_EQ(runtime.value, nullptr);
    EXPECT_EQ(kbs_runtime_destroy(&runtime.value, nullptr), 0);
}
TEST(StreamRuntimeTest, RefusedConnectionReportsFailureWithoutRetryOrLosingHandle) {
    Runtime runtime;
    ASSERT_TRUE(runtime.Open());
    Fd reserved;
    reserved.value = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    ASSERT_GE(reserved.value, 0);
    auto address = Loopback();
    ASSERT_EQ(::bind(reserved.value, Address(address), sizeof(address)), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(::getsockname(reserved.value, reinterpret_cast<sockaddr*>(&address), &length), 0);
    // 保留端口但不 listen，避免测试依赖外部不可达地址或端口竞争。
    kbs_stream_handle stream{};
    kbs_stream_error error{};
    const auto result = kbs_runtime_connect_tcp(runtime.value, Address(address), sizeof(address),
                                                Now() + 3'000'000'000ull, &stream, &error);
    if (result != 0) {
        EXPECT_EQ(result, KBS_STREAM_TRANSPORT);
        EXPECT_EQ(stream.generation, 0u);
        EXPECT_EQ(error.underlying, ECONNREFUSED);
        return;
    }
    runtime.streams.push_back(stream);
    const auto deadline = Now() + 3'000'000'000ull;
    kbs_stream_status status{};
    do {
        ASSERT_TRUE(runtime.Step());
        ASSERT_EQ(kbs_stream_inspect(runtime.table, stream, &status, nullptr), 0);
    } while (status.state != KBS_STREAM_FAILED && Now() < deadline);
    ASSERT_EQ(status.state, KBS_STREAM_FAILED);
    EXPECT_EQ(status.failure.code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(status.failure.underlying, ECONNREFUSED);
    std::array<kbs_stream_event, 4> events{};
    std::size_t count = 0;
    ASSERT_EQ(kbs_stream_poll_events(runtime.table, events.data(), events.size(), &count, nullptr), 0);
    ASSERT_EQ(count, 1u);
    EXPECT_EQ(events[0].stream.generation, stream.generation);
    EXPECT_EQ(events[0].state, KBS_STREAM_FAILED);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_ERROR, 0u);
    ASSERT_TRUE(runtime.Step());
    ASSERT_EQ(kbs_stream_poll_events(runtime.table, events.data(), events.size(), &count, nullptr), 0);
    EXPECT_EQ(count, 0u);
}
TEST(StreamRuntimeTest, InvalidDialDeadlineAndCancellationKeepExplicitLifecycle) {
    Runtime server, client;
    ASSERT_TRUE(server.Open());
    ASSERT_TRUE(client.Open());
    kbs_listener_handle listener{};
    sockaddr_in address{};
    ASSERT_TRUE(server.Listen(listener, address));
    kbs_stream_handle stream{};
    EXPECT_EQ(kbs_runtime_connect_tcp(client.value, Address(address), sizeof(address), Now() - 1, &stream, nullptr),
              KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(stream.generation, 0u);
    ASSERT_TRUE(client.Connect(address, stream));
    ASSERT_EQ(kbs_stream_cancel(client.table, stream, nullptr), 0);
    kbs_stream_status status{};
    ASSERT_EQ(kbs_stream_inspect(client.table, stream, &status, nullptr), 0);
    EXPECT_EQ(status.state, KBS_STREAM_FAILED);
    EXPECT_EQ(status.failure.code, KBS_STREAM_CANCELLED);
    ASSERT_TRUE(client.Step());
    ASSERT_EQ(kbs_stream_inspect(client.table, stream, &status, nullptr), 0);
    EXPECT_EQ(status.state, KBS_STREAM_FAILED);
    EXPECT_EQ(kbs_runtime_close(client.value, nullptr), KBS_STREAM_BUSY);
    ASSERT_TRUE(client.Release(stream));
    EXPECT_EQ(kbs_runtime_close(client.value, nullptr), 0);
}
TEST(StreamRuntimeTest, PendingApplicationEventsPreventRunOnceSleeping) {
    Runtime runtime;
    auto config = Runtime::Config(2, 0);
    config.events = 1;
    config.max_wait_ms = 1000;
    ASSERT_TRUE(runtime.Open(config));
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    kbs_stream_handle stream{};
    ASSERT_TRUE(runtime.Adopt(pair.right.value, stream));
    ASSERT_TRUE(runtime.Step());
    // 不领取 Publish 的初始事件；连接 scheduler 已被非阻塞 Tick 排空，不能再等 kernel fd。
    const auto begin = Now();
    kbs_runtime_progress progress{};
    ASSERT_EQ(kbs_runtime_step(runtime.value, 1, &progress, nullptr), 0);
    const auto elapsed = Now() - begin;
    EXPECT_LT(elapsed, 500'000'000ull);
    EXPECT_EQ(progress.pending_events, 1u);
    EXPECT_EQ(progress.more_work, 1u);
}
TEST(StreamRuntimeTest, RuntimeStepAndCopyEventPathAllocateNothing) {
    Runtime server, client;
    ASSERT_TRUE(server.Open());
    ASSERT_TRUE(client.Open());
    kbs_listener_handle listener{};
    sockaddr_in address{};
    ASSERT_TRUE(server.Listen(listener, address));
    kbs_stream_handle sender{}, receiver{};
    ASSERT_TRUE(ConnectPair(server, client, listener, address, sender, receiver));
    std::array<kbs_stream_event, 8> events{};
    std::size_t count = 0;
    ASSERT_EQ(kbs_stream_poll_events(server.table, events.data(), events.size(), &count, nullptr), 0);
    ASSERT_EQ(kbs_stream_poll_events(client.table, events.data(), events.size(), &count, nullptr), 0);
    kbs_runtime_progress progress{};
    kbs_stream_io io{};
    const std::array<std::byte, 4> payload{std::byte{1}, std::byte{3}, std::byte{5}, std::byte{7}};
    std::array<std::byte, 4> output{};
    bool good = true;
    std::size_t received = 0;
    allocations = 0;
    count_allocations = true;
    good &= kbs_stream_watch(server.table, receiver, 1, 0, nullptr) == 0;
    good &= kbs_stream_write(client.table, sender, payload.data(), payload.size(), &io, nullptr) == 0;
    good &= io.bytes == payload.size();
    const auto deadline = Now() + 3'000'000'000ull;
    do {
        good &= kbs_runtime_step(client.value, 0, &progress, nullptr) == 0;
        good &= kbs_runtime_step(server.value, 0, &progress, nullptr) == 0;
        good &= kbs_stream_poll_events(server.table, events.data(), events.size(), &count, nullptr) == 0;
        const auto result = kbs_stream_read(server.table, receiver, output.data() + received, output.size() - received,
                                            Now(), &io, nullptr);
        received += io.bytes;
        good &= result == 0 || result == KBS_STREAM_WOULD_BLOCK;
    } while (good && received < output.size() && Now() < deadline);
    count_allocations = false;
    EXPECT_TRUE(good);
    EXPECT_EQ(received, output.size());
    EXPECT_EQ(output, payload);
    EXPECT_EQ(allocations, 0u);
}
TEST(StreamRuntimeTest, InvalidCallsDoNotConsumeDescriptorsOrRuntime) {
    Runtime runtime;
    ASSERT_TRUE(runtime.Open());
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int original = pair.right.value;
    EXPECT_EQ(kbs_runtime_adopt_tcp(runtime.value, &pair.right.value, nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(pair.right.value, original);
    kbs_stream_handle stream{};
    EXPECT_EQ(kbs_runtime_adopt_tcp(runtime.value, nullptr, &stream, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_runtime_step(runtime.value, 2, nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    kbs_runtime_progress progress{99, 99, 99};
    EXPECT_EQ(kbs_runtime_step(runtime.value, 2, &progress, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(progress.processed, 0u);
    EXPECT_EQ(kbs_runtime_streams(runtime.value, nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_runtime_destroy(&runtime.value, nullptr), KBS_STREAM_BUSY);
    EXPECT_NE(runtime.value, nullptr);
    ASSERT_TRUE(runtime.Adopt(pair.right.value, stream));
    EXPECT_EQ(pair.right.value, -1);
}
} // namespace
} // namespace api
} // namespace kbsocket
