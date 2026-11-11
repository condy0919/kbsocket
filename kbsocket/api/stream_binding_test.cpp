// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_binding.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <new>
#include <optional>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/api/buffer_binding.hpp"
#include "kbsocket/api/lease_receive.h"
#include "kbsocket/api/lease_send.h"
#include "kbsocket/control/tx_lane_snapshot.hpp"
#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/tcp/native_tcp_backend.hpp"

extern "C" int KbsStreamCHeaderTest(void);
namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
} // namespace
void* operator new(std::size_t bytes) {
    if (count_allocations) {
        ++allocations;
    }
    if (auto* p = std::malloc(bytes ? bytes : 1)) {
        return p;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t bytes) {
    return ::operator new(bytes);
}
void operator delete(void* p) noexcept {
    std::free(p);
}
void operator delete[](void* p) noexcept {
    std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
    std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
    std::free(p);
}

namespace kbsocket {
namespace api {
namespace {
std::uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
bool Wait(int fd, short events) {
    pollfd ready{fd, events, 0};
    return ::poll(&ready, 1, 1000) == 1 && (ready.revents & (events | POLLHUP | POLLERR));
}
struct TcpPair {
    int left = -1, right = -1;
    ~TcpPair() {
        if (left >= 0) {
            ::close(left);
        }
        if (right >= 0) {
            ::close(right);
        }
    }
    bool Open() {
        tcp::NativeTcpListener listener;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (!listener.Open(reinterpret_cast<sockaddr*>(&address), sizeof(address))) {
            return false;
        }
        socklen_t length = sizeof(address);
        if (::getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &length) < 0) {
            return false;
        }
        left = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (left < 0 ||
            (::connect(left, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 && errno != EINPROGRESS)) {
            return false;
        }
        if (!Wait(listener.fd(), POLLIN)) {
            return false;
        }
        auto accepted = listener.Accept();
        if (!accepted) {
            return false;
        }
        right = *accepted;
        if (!Wait(left, POLLOUT)) {
            return false;
        }
        int error = 0;
        length = sizeof(error);
        return ::getsockopt(left, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && error == 0;
    }
};
template <class T>
void ExpectError(const std::expected<T, kbs_stream_error>& result, int code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
class StreamBindingTest : public ::testing::Test {
protected:
    runtime::ConnectionProgress progress_;
    runtime::ConnectionOwner owner_;
    runtime::RuntimeShard runtime_;
    StreamBinding binding_;
    std::array<runtime::OwnedConnection, 12> owned_{};
    std::array<kbs_stream_handle, 12> published_{};
    std::size_t owned_count_ = 0, published_count_ = 0;
    bool runtime_open_ = false;
    std::uint32_t last_events_ = 0;
    std::size_t notifications_ = 0, forward_errors_ = 0;
    static void Observe(void* cookie, const runtime::ConnectionProgressEvent& event) noexcept {
        auto& self = *static_cast<StreamBindingTest*>(cookie);
        if (self.binding_.table() && !self.binding_.RecordProgress(event)) {
            ++self.forward_errors_;
        }
        if (event.fd_events) {
            self.last_events_ = event.fd_events;
            ++self.notifications_;
        }
    }
    void SetUp() override {
        ASSERT_TRUE(progress_.Open({.capacity = 16}, &Observe, this));
        ASSERT_TRUE(owner_.Open(progress_, {.capacity = 12}));
    }
    void Open(std::uint32_t capacity = 8, std::size_t max_io_bytes = 64 * 1024) {
        ASSERT_TRUE(binding_.OpenTcp(owner_, {101, capacity, max_io_bytes}));
    }
    runtime::OwnedConnection Adopt(int& fd) {
        const auto result = owner_.AdoptTcp(fd);
        EXPECT_TRUE(result);
        if (!result) {
            return {};
        }
        fd = -1;
        owned_[owned_count_++] = *result;
        return *result;
    }
    kbs_stream_handle Publish(runtime::OwnedConnection connection) {
        const auto result = binding_.Publish(connection);
        EXPECT_TRUE(result);
        if (!result) {
            return {};
        }
        published_[published_count_++] = *result;
        return *result;
    }
    void TearDown() override {
        count_allocations = false;
        for (std::size_t i = 0; i < published_count_; ++i) {
            if (binding_.Inspect(published_[i])) {
                EXPECT_TRUE(binding_.CloseStream(published_[i]));
                EXPECT_TRUE(binding_.Release(published_[i]));
            }
        }
        EXPECT_TRUE(binding_.Close());
        // 未发布或已因测试主动释放的槽也必须结束；只处理仍属于原代次的连接。
        for (std::size_t i = 0; i < owned_count_; ++i) {
            if (owner_.Inspect(owned_[i])) {
                EXPECT_TRUE(owner_.CloseConnection(owned_[i]));
                EXPECT_TRUE(owner_.Release(owned_[i]));
            }
        }
        EXPECT_TRUE(owner_.Close());
        if (runtime_open_) {
            EXPECT_TRUE(runtime_.Close());
        }
        EXPECT_TRUE(progress_.Close());
    }
};
TEST(StreamCAbiTest, CompilesAsCAndLinksEveryExportedEntry) {
    EXPECT_EQ(KbsStreamCHeaderTest(), 0);
}
TEST_F(StreamBindingTest, ConfigurationChecksDoNotConsumeSingleLifecycle) {
    ExpectError(binding_.OpenTcp(owner_, {0, 2, 16}), KBS_STREAM_INVALID_ARGUMENT);
    ExpectError(binding_.OpenTcp(owner_, {1, 0, 16}), KBS_STREAM_INVALID_ARGUMENT);
    ExpectError(binding_.OpenTcp(owner_, {1, 2, 0}), KBS_STREAM_INVALID_ARGUMENT);
    ExpectError(binding_.OpenTcp(owner_, {1, 2, 1024 * 1024 + 1}), KBS_STREAM_INVALID_ARGUMENT);
    Open();
    ASSERT_NE(binding_.table(), nullptr);
    ExpectError(binding_.OpenTcp(owner_, {2, 2, 16}), KBS_STREAM_BUSY);
}
TEST_F(StreamBindingTest, PublicationRejectsDuplicatesForeignAndInvalidConnections) {
    Open(1);
    TcpPair pair, spare;
    ASSERT_TRUE(pair.Open());
    ASSERT_TRUE(spare.Open());
    const auto owned = Adopt(pair.right);
    const auto stream = Publish(owned);
    ExpectError(binding_.Publish(owned), KBS_STREAM_BUSY);
    auto foreign = owned;
    foreign.owner = nullptr;
    ExpectError(binding_.Publish(foreign), KBS_STREAM_INVALID_HANDLE);
    const auto second = Adopt(spare.right);
    ExpectError(binding_.Publish(second), KBS_STREAM_EXHAUSTED);
    EXPECT_EQ(binding_.published(), 1u);
    EXPECT_TRUE(owner_.Inspect(second));
    EXPECT_TRUE(binding_.Inspect(stream));
}
TEST_F(StreamBindingTest, NativeTcpLeaseWriteExplicitlyRejectsWithoutCopyOrSourceReservation) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    BufferBinding buffers;
    ASSERT_TRUE(buffers.OpenHost({KBS_BUFFER_ABI_VERSION, 0, 701, 1, 0, 64, 1u << 20}));
    const auto buffer = buffers.Acquire(17);
    ASSERT_TRUE(buffer);
    const auto view = buffers.Writable(*buffer);
    ASSERT_TRUE(view);
    std::fill(view->begin(), view->end(), std::byte{0x53});
    kbs_stream_io io{99, 1, 1};
    kbs_stream_error error{};
    EXPECT_EQ(kbs_stream_write_lease(binding_.table(), stream, buffers.table(), *buffer, 1, 7, &io, &error),
              KBS_STREAM_NOT_SUPPORTED);
    EXPECT_EQ(error.code, KBS_STREAM_NOT_SUPPORTED);
    EXPECT_EQ(io.bytes, 0u);
    EXPECT_EQ(io.eof, 0u);
    EXPECT_EQ(io.reserved, 0u);
    kbs_buffer_status status{};
    ASSERT_EQ(kbs_buffer_inspect(buffers.table(), *buffer, &status, nullptr), 0);
    EXPECT_EQ(status.source_reusable, 1u);
    EXPECT_TRUE(buffers.Writable(*buffer));
    std::array<std::byte, 17> wire{};
    EXPECT_EQ(::recv(pair.left, wire.data(), wire.size(), 0), -1);
    EXPECT_EQ(errno, EAGAIN);
    EXPECT_EQ(kbs_stream_write_lease(binding_.table(), stream, nullptr, *buffer, 0, 1, &io, nullptr),
              KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_write_lease(binding_.table(), stream, buffers.table(), *buffer, 0, 1, nullptr, nullptr),
              KBS_STREAM_INVALID_ARGUMENT);
    ASSERT_TRUE(buffers.Release(*buffer));
    ASSERT_TRUE(buffers.Close());
}

TEST_F(StreamBindingTest, NativeTcpReceiveLeaseExplicitlyRejectsWithoutConsumingReadableBytes) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    ASSERT_EQ(::send(pair.left, payload.data(), payload.size(), MSG_NOSIGNAL), 3);
    kbs_receive_view view{};
    view.data = &view;
    view.size = 99;
    view.eof = view.reserved = 1;
    view.lease.generation = 99;
    kbs_stream_error error{};
    EXPECT_EQ(kbs_stream_read_lease(binding_.table(), stream, 3, Now(), &view, &error), KBS_STREAM_NOT_SUPPORTED);
    EXPECT_EQ(error.code, KBS_STREAM_NOT_SUPPORTED);
    EXPECT_EQ(view.data, nullptr);
    EXPECT_EQ(view.size, 0u);
    EXPECT_EQ(view.lease.generation, 0u);
    EXPECT_EQ(view.eof, 0u);
    EXPECT_EQ(view.reserved, 0u);
    std::array<std::byte, 3> output{};
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_read(binding_.table(), stream, output.data(), output.size(), Now(), &io, nullptr), 0);
    EXPECT_EQ(io.bytes, payload.size());
    EXPECT_EQ(output, payload);
    EXPECT_EQ(binding_.Inspect(stream)->state, KBS_STREAM_TCP_ACTIVE);
}

TEST_F(StreamBindingTest, CopyIoHasBoundedPrefixesAndImmediateSourceReuse) {
    Open(2, 3);
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int peer_fd = pair.left;
    const auto stream = Publish(Adopt(pair.right));
    std::array<std::byte, 7> input{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4},
                                   std::byte{5}, std::byte{6}, std::byte{7}};
    const auto original = input;
    kbs_stream_io out{};
    kbs_stream_error error{123, 99, 99, 99, 99, 99};
    ASSERT_EQ(kbs_stream_write(binding_.table(), stream, input.data(), input.size(), &out, &error), 0);
    ASSERT_EQ(out.bytes, 3u);
    EXPECT_EQ(error.code, 0);
    input.fill(std::byte{0xff});
    ASSERT_TRUE(Wait(peer_fd, POLLIN));
    std::array<std::byte, 8> wire{};
    ASSERT_EQ(::recv(peer_fd, wire.data(), wire.size(), 0), 3);
    EXPECT_TRUE(std::equal(original.begin(), original.begin() + 3, wire.begin()));
    // 接收调用容量大于单次预算，先只交付连续前缀，尾部留给下一次读取。
    ASSERT_EQ(::send(peer_fd, original.data(), original.size(), MSG_NOSIGNAL), 7);
    std::array<std::byte, 7> output{};
    std::size_t received = 0;
    const auto deadline = Now() + 3'000'000'000ull;
    while (received < output.size() && Now() < deadline) {
        const auto status = kbs_stream_read(binding_.table(), stream, output.data() + received,
                                            output.size() - received, Now(), &out, &error);
        if (status == KBS_STREAM_WOULD_BLOCK) {
            continue;
        }
        ASSERT_EQ(status, 0);
        ASSERT_GT(out.bytes, 0u);
        EXPECT_LE(out.bytes, 3u);
        EXPECT_EQ(out.eof, 0u);
        received += out.bytes;
    }
    EXPECT_EQ(received, output.size());
    EXPECT_EQ(output, original);
}
TEST_F(StreamBindingTest, WouldBlockEmptyReadAndFinRemainDistinct) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    std::byte byte{};
    kbs_stream_io io{99, 1, 99};
    kbs_stream_error error{};
    EXPECT_EQ(kbs_stream_read(binding_.table(), stream, &byte, 1, Now(), &io, &error), KBS_STREAM_WOULD_BLOCK);
    EXPECT_EQ(io.bytes, 0u);
    EXPECT_EQ(io.eof, 0u);
    EXPECT_EQ(io.reserved, 0u);
    EXPECT_EQ(error.code, KBS_STREAM_WOULD_BLOCK);
    ASSERT_EQ(kbs_stream_read(binding_.table(), stream, nullptr, 0, Now(), &io, &error), 0);
    EXPECT_EQ(io.bytes, 0u);
    EXPECT_EQ(io.eof, 0u);
    EXPECT_EQ(error.code, 0);
    ASSERT_EQ(::shutdown(pair.left, SHUT_WR), 0);
    const auto deadline = Now() + 3'000'000'000ull;
    int status = KBS_STREAM_WOULD_BLOCK;
    while (status == KBS_STREAM_WOULD_BLOCK && Now() < deadline) {
        status = kbs_stream_read(binding_.table(), stream, &byte, 1, Now(), &io, &error);
    }
    ASSERT_EQ(status, 0);
    EXPECT_EQ(io.eof, 1u);
    EXPECT_EQ(io.bytes, 0u);
    // 已经观测 EOF 后空 read 仍不做探测，也不能把它解释为另一份终结证据。
    ASSERT_EQ(kbs_stream_read(binding_.table(), stream, nullptr, 0, Now(), &io, nullptr), 0);
    EXPECT_EQ(io.eof, 0u);
    ASSERT_EQ(kbs_stream_write(binding_.table(), stream, &byte, 1, &io, nullptr), 0);
    EXPECT_EQ(io.bytes, 1u);
}
TEST_F(StreamBindingTest, ShutdownWriteLeavesReadHalfUsable) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_shutdown_write(binding_.table(), stream, nullptr), 0);
    ASSERT_TRUE(Wait(pair.left, POLLIN));
    std::byte byte{0x33};
    EXPECT_EQ(::recv(pair.left, &byte, 1, 0), 0);
    EXPECT_EQ(kbs_stream_write(binding_.table(), stream, &byte, 1, &io, nullptr), KBS_STREAM_CLOSED);
    ASSERT_EQ(::send(pair.left, &byte, 1, MSG_NOSIGNAL), 1);
    const auto deadline = Now() + 3'000'000'000ull;
    int status = KBS_STREAM_WOULD_BLOCK;
    while (status == KBS_STREAM_WOULD_BLOCK && Now() < deadline) {
        status = kbs_stream_read(binding_.table(), stream, &byte, 1, Now(), &io, nullptr);
    }
    EXPECT_EQ(status, 0);
    EXPECT_EQ(io.bytes, 1u);
    EXPECT_EQ(io.eof, 0u);
}
TEST_F(StreamBindingTest, InvalidArgumentsClearOutputsWithoutReadingOrWriting) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    kbs_stream_io io{7, 1, 7};
    kbs_stream_error error{};
    kbs_stream_status status{};
    EXPECT_EQ(kbs_stream_write(binding_.table(), stream, nullptr, 1, &io, &error), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(io.bytes, 0u);
    EXPECT_EQ(error.code, KBS_STREAM_INVALID_ARGUMENT);
    std::byte byte{0x21};
    EXPECT_EQ(kbs_stream_write(binding_.table(), stream, &byte, 1, nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_read(binding_.table(), stream, nullptr, 1, Now(), &io, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_read(binding_.table(), stream, &byte, 1, Now(), nullptr, nullptr),
              KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_inspect(binding_.table(), stream, nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_watch_tcp(binding_.table(), stream, 2, 0, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_watch_tcp(binding_.table(), stream, 0, 2, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(::recv(pair.left, &byte, 1, MSG_DONTWAIT), -1);
    EXPECT_EQ(errno, EAGAIN);
    ASSERT_EQ(kbs_stream_inspect(binding_.table(), stream, &status, &error), 0);
    EXPECT_EQ(status.state, KBS_STREAM_TCP_ACTIVE);
    EXPECT_EQ(error.code, 0);
}
TEST_F(StreamBindingTest, CloseKeepsTokenUntilExplicitReleaseAndRejectsReuse) {
    Open(1);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    const auto old = Publish(Adopt(first.right));
    EXPECT_EQ(kbs_stream_release(binding_.table(), old, nullptr), KBS_STREAM_BUSY);
    ASSERT_EQ(kbs_stream_close(binding_.table(), old, nullptr), 0);
    ASSERT_EQ(kbs_stream_close(binding_.table(), old, nullptr), 0);
    kbs_stream_status status{};
    ASSERT_EQ(kbs_stream_inspect(binding_.table(), old, &status, nullptr), 0);
    EXPECT_EQ(status.state, KBS_STREAM_STATE_CLOSED);
    EXPECT_EQ(binding_.published(), 1u);
    ExpectError(binding_.Close(), KBS_STREAM_BUSY);
    ASSERT_EQ(kbs_stream_release(binding_.table(), old, nullptr), 0);
    EXPECT_EQ(binding_.published(), 0u);
    // 表 Close 的失败停止新发布；验证槽复用使用尚未停止的另一张表。
    ASSERT_TRUE(binding_.Close());
    StreamBinding replacement;
    ASSERT_TRUE(replacement.OpenTcp(owner_, {102, 1, 16}));
    auto new_owned = Adopt(second.right);
    auto current = replacement.Publish(new_owned);
    ASSERT_TRUE(current);
    EXPECT_EQ(kbs_stream_close(replacement.table(), old, nullptr), KBS_STREAM_INVALID_HANDLE);
    EXPECT_TRUE(replacement.Inspect(*current));
    ASSERT_TRUE(replacement.CloseStream(*current));
    ASSERT_TRUE(replacement.Release(*current));
    ASSERT_TRUE(replacement.Close());
}
TEST_F(StreamBindingTest, SlotReuseAndForgedGenerationsCannotOperateNewStream) {
    Open(1);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    const auto old = Publish(Adopt(first.right));
    ASSERT_TRUE(binding_.CloseStream(old));
    ASSERT_TRUE(binding_.Release(old));
    const auto current = Publish(Adopt(second.right));
    EXPECT_EQ(old.slot, current.slot);
    EXPECT_NE(old.generation, current.generation);
    kbs_stream_status output{};
    for (auto forged : {old, kbs_stream_handle{current.table_id + 1, current.generation, current.slot, 0},
                        kbs_stream_handle{current.table_id, current.generation, 99, 0},
                        kbs_stream_handle{current.table_id, current.generation, current.slot, 1}}) {
        EXPECT_EQ(kbs_stream_inspect(binding_.table(), forged, &output, nullptr), KBS_STREAM_INVALID_HANDLE);
        EXPECT_EQ(kbs_stream_cancel(binding_.table(), forged, nullptr), KBS_STREAM_INVALID_HANDLE);
        EXPECT_EQ(kbs_stream_release(binding_.table(), forged, nullptr), KBS_STREAM_INVALID_HANDLE);
    }
    EXPECT_EQ(binding_.Inspect(current)->state, KBS_STREAM_TCP_ACTIVE);
}
TEST_F(StreamBindingTest, OutOfOrderReleasesPreserveOtherSlotsAndOwnerIndex) {
    Open(3);
    std::array<TcpPair, 5> pairs;
    for (auto& pair : pairs) {
        ASSERT_TRUE(pair.Open());
    }
    std::array<kbs_stream_handle, 3> old{};
    for (std::size_t i = 0; i < 3; ++i) {
        old[i] = Publish(Adopt(pairs[i].right));
    }
    // 先释放中间再释放首槽；仍存活的尾槽不得被空闲索引交换所覆盖。
    for (auto index : {1, 0}) {
        ASSERT_TRUE(binding_.CloseStream(old[index]));
        ASSERT_TRUE(binding_.Release(old[index]));
    }
    const auto first = Publish(Adopt(pairs[3].right));
    const auto second = Publish(Adopt(pairs[4].right));
    EXPECT_NE(first.slot, second.slot);
    EXPECT_NE(first.slot, old[2].slot);
    EXPECT_NE(second.slot, old[2].slot);
    EXPECT_EQ(binding_.published(), 3u);
    for (auto stream : {first, second, old[2]}) {
        EXPECT_EQ(binding_.Inspect(stream)->state, KBS_STREAM_TCP_ACTIVE);
    }
    ExpectError(binding_.Inspect(old[0]), KBS_STREAM_INVALID_HANDLE);
    ExpectError(binding_.Inspect(old[1]), KBS_STREAM_INVALID_HANDLE);
}
TEST_F(StreamBindingTest, ClosedTablePointerRejectsOldCallsWhileBindingLives) {
    Open();
    auto* table = binding_.table();
    ASSERT_TRUE(binding_.Close());
    EXPECT_EQ(binding_.table(), nullptr);
    kbs_stream_status status{};
    kbs_stream_io io{};
    EXPECT_EQ(kbs_stream_inspect(table, {}, &status, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(kbs_stream_write(table, {}, nullptr, 0, &io, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(kbs_stream_close(table, {}, nullptr), KBS_STREAM_NOT_READY);
}
TEST_F(StreamBindingTest, StopAcceptingKeepsPublishedIoAndCloseAvailable) {
    Open(2);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    const auto stream = Publish(Adopt(first.right));
    const auto spare = Adopt(second.right);
    binding_.StopAccepting();
    ExpectError(binding_.Publish(spare), KBS_STREAM_STOPPED);
    std::array<std::byte, 1> byte{std::byte{7}};
    EXPECT_EQ(binding_.Write(stream, byte), 1u);
    EXPECT_TRUE(binding_.CloseStream(stream));
    EXPECT_TRUE(binding_.Release(stream));
    EXPECT_TRUE(binding_.Close());
    ExpectError(binding_.OpenTcp(owner_, {102, 1, 16}), KBS_STREAM_BUSY);
}
TEST_F(StreamBindingTest, CancellationPreservesDiagnosticAndRequiresCloseBeforeRelease) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    ASSERT_EQ(kbs_stream_cancel(binding_.table(), stream, nullptr), 0);
    kbs_stream_status status{};
    kbs_stream_error error{};
    ASSERT_EQ(kbs_stream_inspect(binding_.table(), stream, &status, &error), 0);
    EXPECT_EQ(status.state, KBS_STREAM_FAILED);
    EXPECT_EQ(status.failure.code, KBS_STREAM_CANCELLED);
    kbs_stream_io io{};
    EXPECT_EQ(kbs_stream_write(binding_.table(), stream, nullptr, 0, &io, &error), KBS_STREAM_CANCELLED);
    EXPECT_EQ(kbs_stream_release(binding_.table(), stream, nullptr), KBS_STREAM_BUSY);
    ASSERT_EQ(kbs_stream_close(binding_.table(), stream, nullptr), 0);
    EXPECT_TRUE(binding_.RetireSources(stream, {}));
    ASSERT_EQ(kbs_stream_release(binding_.table(), stream, nullptr), 0);
    EXPECT_EQ(kbs_stream_cancel(binding_.table(), stream, nullptr), KBS_STREAM_INVALID_HANDLE);
}
TEST_F(StreamBindingTest, NativeReadinessSubscriptionRunsOnExistingRuntime) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    ASSERT_TRUE(runtime_.Open(
        {}, {.reactor = {.events = true, .budget = 4, .busy_poll_ns = 0, .max_wait_ms = 1}, .control = {}},
        &progress_));
    runtime_open_ = true;
    ASSERT_EQ(kbs_stream_watch_tcp(binding_.table(), stream, 1, 0, nullptr), 0);
    const std::byte byte{0x66};
    ASSERT_EQ(::send(pair.left, &byte, 1, MSG_NOSIGNAL), 1);
    const auto deadline = Now() + 3'000'000'000ull;
    while (!notifications_ && Now() < deadline) {
        ASSERT_TRUE(runtime_.RunOnce());
    }
    ASSERT_GT(notifications_, 0u);
    EXPECT_NE(last_events_ & EPOLLIN, 0u);
    const auto count = notifications_;
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(runtime_.RunOnce());
    }
    EXPECT_EQ(notifications_, count);
}
TEST_F(StreamBindingTest, PublishedFastpathDoesNotAllocate) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int fd = pair.right;
    const auto owned = Adopt(pair.right);
    std::array<std::byte, 1> input{std::byte{0x29}}, output{};
    ASSERT_EQ(::send(pair.left, input.data(), 1, MSG_NOSIGNAL), 1);
    ASSERT_TRUE(Wait(fd, POLLIN));
    // 仅计量已分配表上的发布、数据访问和释放；测试断言及控制面构造不计入快路径。
    allocations = 0;
    count_allocations = true;
    const auto stream = binding_.Publish(owned);
    kbs_stream_io io{};
    kbs_stream_status status{};
    int read = -1, write = -1, inspect = -1, closed = -1, released = -1;
    if (stream) {
        read = kbs_stream_read(binding_.table(), *stream, output.data(), 1, Now(), &io, nullptr);
        write = kbs_stream_write(binding_.table(), *stream, input.data(), 1, &io, nullptr);
        inspect = kbs_stream_inspect(binding_.table(), *stream, &status, nullptr);
        closed = kbs_stream_close(binding_.table(), *stream, nullptr);
        released = kbs_stream_release(binding_.table(), *stream, nullptr);
    }
    count_allocations = false;
    EXPECT_TRUE(stream);
    EXPECT_EQ(read, 0);
    EXPECT_EQ(write, 0);
    EXPECT_EQ(inspect, 0);
    EXPECT_EQ(closed, 0);
    EXPECT_EQ(released, 0);
    EXPECT_EQ(output, input);
    EXPECT_EQ(allocations, 0u);
}

TEST_F(StreamBindingTest, EventArgumentsAndClosedTablePreservePendingState) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    kbs_stream_event event{};
    std::size_t count = 99;
    kbs_stream_error error{};
    EXPECT_EQ(kbs_stream_watch(binding_.table(), stream, 2, 0, &error), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_watch(binding_.table(), stream, 0, 2, &error), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(kbs_stream_poll_events(binding_.table(), nullptr, 1, &count, &error), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(count, 0u);
    count = 99;
    EXPECT_EQ(kbs_stream_poll_events(binding_.table(), &event, 0, &count, &error), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(count, 0u);
    EXPECT_EQ(kbs_stream_poll_events(binding_.table(), &event, 1, nullptr, nullptr), KBS_STREAM_INVALID_ARGUMENT);
    ASSERT_EQ(kbs_stream_poll_events(binding_.table(), &event, 1, &count, &error), 0);
    ASSERT_EQ(count, 1u);
    EXPECT_EQ(event.stream.table_id, stream.table_id);
    EXPECT_EQ(event.stream.generation, stream.generation);
    EXPECT_EQ(event.stream.slot, stream.slot);
    EXPECT_EQ(event.flags, KBS_STREAM_EVENT_STATE);
    EXPECT_EQ(event.state, KBS_STREAM_TCP_ACTIVE);
    EXPECT_EQ(error.code, 0);
    ASSERT_TRUE(binding_.CloseStream(stream));
    ASSERT_TRUE(binding_.Release(stream));
    auto* table = binding_.table();
    ASSERT_TRUE(binding_.Close());
    count = 99;
    EXPECT_EQ(kbs_stream_poll_events(table, &event, 1, &count, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(count, 0u);
    ExpectError(binding_.RecordProgress({{}, connection::ConnectionState::kClosed}), KBS_STREAM_NOT_READY);
}
TEST_F(StreamBindingTest, ProgressEventsCoalesceLatestSnapshotAndSaturatingRetirement) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto owned = Adopt(pair.right);
    const auto stream = Publish(owned);
    const auto scheduled = owner_.Inspect(owned)->scheduled;
    // 高速回调合并到同一节点；进度计数累加，状态及驻留量保持最新快照。
    runtime::ConnectionProgressEvent first{scheduled, connection::ConnectionState::kTcpActive};
    first.rx_changed = true;
    first.rx_pending = 5;
    first.tx_retired = 7;
    ASSERT_EQ(binding_.RecordProgress(first), true);
    first.rx_pending = 2;
    first.tx_retired = 9;
    ASSERT_EQ(binding_.RecordProgress(first), true);
    std::array<kbs_stream_event, 4> events{};
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].stream.generation, stream.generation);
    EXPECT_EQ(events[0].rx_pending, 2u);
    EXPECT_EQ(events[0].tx_retired, 16u);
    EXPECT_EQ(events[0].flags, KBS_STREAM_EVENT_STATE | KBS_STREAM_EVENT_RX_CHANGED | KBS_STREAM_EVENT_TX_RETIRED);
    EXPECT_EQ(binding_.PollEvents(events), 0u);
    first.tx_retired = std::numeric_limits<std::uint64_t>::max();
    ASSERT_EQ(binding_.RecordProgress(first), true);
    first.tx_retired = 1;
    ASSERT_EQ(binding_.RecordProgress(first), true);
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].tx_retired, std::numeric_limits<std::uint64_t>::max());
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_COUNTER_SATURATED, 0u);
}
TEST_F(StreamBindingTest, DirectCancellationAndCloseRefreshQueuedFailure) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    std::array<kbs_stream_event, 1> events{};
    ASSERT_TRUE(binding_.Cancel(stream));
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].state, KBS_STREAM_FAILED);
    EXPECT_EQ(events[0].failure.code, KBS_STREAM_CANCELLED);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_ERROR, 0u);
    ASSERT_TRUE(binding_.CloseStream(stream));
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].state, KBS_STREAM_STATE_CLOSED);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_CLOSED, 0u);
}
TEST_F(StreamBindingTest, StaleSchedulerIdentityCannotReachRepublishedSlot) {
    Open(1);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    const auto old_owned = Adopt(first.right);
    const auto old_stream = Publish(old_owned);
    const auto old_scheduled = owner_.Inspect(old_owned)->scheduled;
    ASSERT_TRUE(binding_.CloseStream(old_stream));
    ASSERT_TRUE(binding_.Release(old_stream));
    const auto new_owned = Adopt(second.right);
    const auto stream = Publish(new_owned);
    const auto scheduled = owner_.Inspect(new_owned)->scheduled;
    EXPECT_EQ(old_stream.slot, stream.slot);
    EXPECT_EQ(old_scheduled.slot_index(), scheduled.slot_index());
    runtime::ConnectionProgressEvent stale{old_scheduled, connection::ConnectionState::kFailed};
    stale.tx_retired = 99;
    EXPECT_EQ(binding_.RecordProgress(stale), false);
    stale.connection = scheduled;
    stale.connection.owner = nullptr;
    EXPECT_EQ(binding_.RecordProgress(stale), false);
    stale.connection = {scheduled.owner, 0};
    EXPECT_EQ(binding_.RecordProgress(stale), false);
    std::array<kbs_stream_event, 2> events{};
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].stream.generation, stream.generation);
    EXPECT_EQ(events[0].state, KBS_STREAM_TCP_ACTIVE);
    EXPECT_EQ(events[0].tx_retired, 0u);
    // 成功 Release 取消尚未取出的提示，不能把旧关闭事件交给复用后的应用句柄。
    ASSERT_TRUE(binding_.CloseStream(stream));
    ASSERT_TRUE(binding_.Release(stream));
    EXPECT_EQ(binding_.PollEvents(events), 0u);
}
TEST_F(StreamBindingTest, PendingMiddleRemovalPreservesFifoAndReleasedTail) {
    Open(3);
    std::array<TcpPair, 3> pairs;
    std::array<kbs_stream_handle, 3> streams{};
    for (std::size_t i = 0; i < pairs.size(); ++i) {
        ASSERT_TRUE(pairs[i].Open());
        streams[i] = Publish(Adopt(pairs[i].right));
    }
    ASSERT_TRUE(binding_.CloseStream(streams[1]));
    ASSERT_TRUE(binding_.Release(streams[1]));
    std::array<kbs_stream_event, 1> events{};
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].stream.slot, streams[0].slot);
    ASSERT_TRUE(binding_.CloseStream(streams[2]));
    ASSERT_TRUE(binding_.Release(streams[2]));
    EXPECT_EQ(binding_.PollEvents(events), 0u);
    ASSERT_TRUE(binding_.ShutdownWrite(streams[0]));
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].stream.slot, streams[0].slot);
    EXPECT_EQ(events[0].write_closed, 1u);
}
TEST_F(StreamBindingTest, NativeOneShotDeliveryRequiresExplicitRearm) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    ASSERT_TRUE(runtime_.Open(
        {}, {.reactor = {.events = true, .budget = 4, .busy_poll_ns = 0, .max_wait_ms = 1}, .control = {}},
        &progress_));
    runtime_open_ = true;
    std::array<kbs_stream_event, 2> events{};
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    ASSERT_EQ(kbs_stream_watch(binding_.table(), stream, 1, 1, nullptr), 0);
    const auto deadline = Now() + 3'000'000'000ull;
    std::uint32_t flags = 0;
    while (!(flags & KBS_STREAM_EVENT_WRITABLE) && Now() < deadline) {
        ASSERT_TRUE(runtime_.RunOnce());
        const auto count = binding_.PollEvents(events);
        ASSERT_TRUE(count);
        for (std::size_t i = 0; i < *count; ++i) {
            flags |= events[i].flags;
        }
    }
    ASSERT_NE(flags & KBS_STREAM_EVENT_WRITABLE, 0u);
    const std::byte byte{0x62};
    ASSERT_EQ(::send(pair.left, &byte, 1, MSG_NOSIGNAL), 1);
    // OUT 已消费整体 oneshot；后到 IN 不得复用此前订阅，Poll 本身也不重新 arm。
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(runtime_.RunOnce());
    }
    ASSERT_EQ(binding_.PollEvents(events), 0u);
    ASSERT_TRUE(binding_.Watch(stream, true, false));
    flags = 0;
    while (!(flags & KBS_STREAM_EVENT_READABLE) && Now() < deadline) {
        ASSERT_TRUE(runtime_.RunOnce());
        const auto count = binding_.PollEvents(events);
        ASSERT_TRUE(count);
        for (std::size_t i = 0; i < *count; ++i) {
            flags |= events[i].flags;
        }
    }
    EXPECT_NE(flags & KBS_STREAM_EVENT_READABLE, 0u);
    EXPECT_EQ(forward_errors_, 0u);
}
TEST_F(StreamBindingTest, PeerShutdownHintsDoNotClaimEofBeforeRead) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto stream = Publish(Adopt(pair.right));
    ASSERT_TRUE(runtime_.Open(
        {}, {.reactor = {.events = true, .budget = 4, .busy_poll_ns = 0, .max_wait_ms = 1}, .control = {}},
        &progress_));
    runtime_open_ = true;
    std::array<kbs_stream_event, 2> events{};
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    ASSERT_TRUE(binding_.Watch(stream, true, false));
    const std::byte byte{0x35};
    ASSERT_EQ(::send(pair.left, &byte, 1, MSG_NOSIGNAL), 1);
    ASSERT_EQ(::shutdown(pair.left, SHUT_WR), 0);
    const auto deadline = Now() + 3'000'000'000ull;
    bool readable = false;
    while (!readable && Now() < deadline) {
        ASSERT_TRUE(runtime_.RunOnce());
        const auto count = binding_.PollEvents(events);
        ASSERT_TRUE(count);
        for (std::size_t i = 0; i < *count; ++i) {
            readable |= (events[i].flags & KBS_STREAM_EVENT_READABLE) != 0;
            EXPECT_EQ(events[i].read_eof, 0u);
        }
    }
    ASSERT_TRUE(readable);
    std::array<std::byte, 1> output{};
    const auto data = binding_.Read(stream, output, Now());
    ASSERT_TRUE(data);
    EXPECT_EQ(data->bytes, 1u);
    EXPECT_EQ(data->eof, 0u);
    const auto eof = binding_.Read(stream, output, Now());
    ASSERT_TRUE(eof);
    EXPECT_EQ(eof->eof, 1u);
    ASSERT_TRUE(binding_.Watch(stream, true, false));
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].read_eof, 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    ASSERT_TRUE(binding_.ShutdownWrite(stream));
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_EQ(events[0].write_closed, 1u);
}
TEST_F(StreamBindingTest, ReadHalfCloseDoesNotInventWritableAndConsumesWholeWatch) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto owned = Adopt(pair.right);
    const auto stream = Publish(owned);
    const auto scheduled = owner_.Inspect(owned)->scheduled;
    std::array<kbs_stream_event, 2> events{};
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    ASSERT_TRUE(binding_.Watch(stream, false, true));
    // 单独 RDHUP 只终结对端发送半部，不能说明本地发送缓冲有空间；底层 oneshot 仍已被消费。
    runtime::ConnectionProgressEvent half_closed{scheduled, connection::ConnectionState::kTcpActive};
    half_closed.fd_events = EPOLLRDHUP;
    ASSERT_EQ(binding_.RecordProgress(half_closed), true);
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_PEER_SHUTDOWN, 0u);
    EXPECT_EQ(events[0].flags & (KBS_STREAM_EVENT_READABLE | KBS_STREAM_EVENT_WRITABLE), 0u);
    EXPECT_EQ(events[0].read_eof, 0u);
    half_closed.fd_events = EPOLLOUT;
    ASSERT_EQ(binding_.RecordProgress(half_closed), true);
    EXPECT_EQ(binding_.PollEvents(events), 0u);
    ASSERT_TRUE(binding_.Watch(stream, true, true));
    half_closed.fd_events = EPOLLERR;
    ASSERT_EQ(binding_.RecordProgress(half_closed), true);
    ASSERT_EQ(binding_.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_ERROR, 0u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_WRITABLE, 0u);
    // 原生错误提示没有经过数据 API 诊断，不能凭 fd 标志猜测 ConnectionError。
    EXPECT_EQ(events[0].failure.code, 0);
}
TEST_F(StreamBindingTest, EventIngressAndPollingAllocateNothing) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto owned = Adopt(pair.right);
    const auto stream = Publish(owned);
    runtime::ConnectionProgressEvent progress{owner_.Inspect(owned)->scheduled,
                                              connection::ConnectionState::kTcpActive};
    progress.tx_retired = 1;
    std::array<kbs_stream_event, 2> events{};
    allocations = 0;
    count_allocations = true;
    bool recorded = true;
    for (std::size_t i = 0; i < 100; ++i) {
        recorded &= binding_.RecordProgress(progress).value_or(false);
    }
    const auto polled = binding_.PollEvents(events);
    const auto watched = binding_.Watch(stream, true, true);
    progress.fd_events = EPOLLOUT;
    recorded &= binding_.RecordProgress(progress).value_or(false);
    const auto second = binding_.PollEvents(events);
    count_allocations = false;
    EXPECT_TRUE(recorded);
    EXPECT_EQ(polled, 1u);
    EXPECT_TRUE(watched);
    EXPECT_EQ(second, 1u);
    EXPECT_EQ(allocations, 0u);
}
TEST(StreamEventQueueTest, PollCapsBatchAndKeepsRemainingHandlesInOrder) {
    runtime::ConnectionProgress progress;
    runtime::ConnectionOwner owner;
    StreamBinding binding;
    constexpr std::size_t kCount = KBS_STREAM_EVENT_BATCH_MAX + 2;
    ASSERT_TRUE(progress.Open({.capacity = kCount}));
    ASSERT_TRUE(owner.Open(progress, {.capacity = kCount}));
    ASSERT_TRUE(binding.OpenTcp(owner, {901, kCount, 16}));
    std::array<TcpPair, kCount> pairs;
    std::array<kbs_stream_handle, kCount> streams{};
    for (std::size_t i = 0; i < kCount; ++i) {
        ASSERT_TRUE(pairs[i].Open());
        const auto owned = owner.AdoptTcp(pairs[i].right);
        ASSERT_TRUE(owned);
        pairs[i].right = -1;
        const auto stream = binding.Publish(*owned);
        ASSERT_TRUE(stream);
        streams[i] = *stream;
    }
    std::array<kbs_stream_event, kCount> events{};
    ASSERT_EQ(binding.PollEvents(events), KBS_STREAM_EVENT_BATCH_MAX);
    for (std::size_t i = 0; i < KBS_STREAM_EVENT_BATCH_MAX; ++i) {
        EXPECT_EQ(events[i].stream.slot, streams[i].slot);
    }
    ASSERT_EQ(binding.PollEvents(events), 2u);
    EXPECT_EQ(events[0].stream.slot, streams[kCount - 2].slot);
    EXPECT_EQ(events[1].stream.slot, streams[kCount - 1].slot);
    EXPECT_EQ(binding.PollEvents(events), 0u);
    for (const auto stream : streams) {
        ASSERT_TRUE(binding.CloseStream(stream));
        ASSERT_TRUE(binding.Release(stream));
    }
    EXPECT_EQ(binding.PollEvents(events), 0u);
    EXPECT_TRUE(binding.Close());
    EXPECT_TRUE(owner.Close());
    EXPECT_TRUE(progress.Close());
}

class RawStreamBindingTest : public ::testing::Test {
protected:
    struct Receive {
        std::uint64_t id;
        urma_sge_t sge;
    };
    struct Endpoint {
        urma_context_t context{};
        std::array<urma_jfc_t, 2> cq{};
        urma_jfr_t jfr{};
        std::array<urma_jetty_t, 2> jetty{};
        std::array<urma_target_seg_t, 2> segments{};
        urma_target_jetty_t target{};
        std::size_t cq_count = 0, jetty_count = 0, segment_count = 0;
        std::deque<Receive> posted;
        std::array<std::deque<urma_cr_t>, 2> completed;
        bool hold_tx = false;
        std::atomic<bool> block_import{false};
        std::atomic<unsigned> imports{0};
        std::size_t forward_errors = 0;
        runtime::RawStackContextResources resources{};
        runtime::OwnedConnection owned;
        kbs_stream_handle stream{};
        StreamBinding binding;
        connection::CreditPeerHandle peer;
        connection::SenderCreditAccount sender_credits;
    };
    static inline RawStreamBindingTest* active_ = nullptr;
    urma_device_t device_{};
    std::array<urma_device_t*, 1> devices_{&device_};
    std::array<urma_eid_info_t, 2> eids_{};
    std::array<Endpoint, 2> endpoints_;
    runtime::RawStack stack_;
    std::optional<runtime::RawStackSharedResources> shared_;
    std::optional<raw::test_support::ScopedUrmaOverride> override_;
    std::array<raw::ReceiveDomainQuiescence, 2> proofs_{{{true, true, true, true}, {true, true, true, true}}};
    bool opened_ = false;
    static Endpoint& Find(urma_context_t* context) {
        return context == &active_->endpoints_[0].context ? active_->endpoints_[0] : active_->endpoints_[1];
    }
    static raw::UrmaFunctions Functions() {
        raw::UrmaFunctions f{};
        f.init = [](auto*) { return URMA_SUCCESS; };
        f.uninit = [] { return URMA_SUCCESS; };
        f.get_devices = [](int* count) {
            *count = 1;
            return active_->devices_.data();
        };
        f.free_devices = [](auto*) {};
        f.get_eids = [](auto*, auto* count) {
            *count = 2;
            return active_->eids_.data();
        };
        f.free_eids = [](auto*) {};
        f.query_device = [](auto*, auto* attr) {
            *attr = {};
            auto& cap = attr->dev_cap;
            cap.trans_mode = URMA_TM_RM;
            cap.rm_tp_cap.bs.ctp = 1;
            cap.max_eid_cnt = 64;
            cap.max_jetty = 8;
            cap.max_jfs_depth = cap.max_jfr_depth = cap.max_jfc_depth = 256;
            cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
            return URMA_SUCCESS;
        };
        f.create_context = [](auto*, std::uint32_t eid) { return &active_->endpoints_[eid].context; };
        f.delete_context = [](auto*) { return URMA_SUCCESS; };
        f.create_jfc = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto& cq = e.cq[e.cq_count++];
            cq.urma_ctx = context;
            cq.jfc_cfg = *config;
            return &cq;
        };
        f.create_jfr = [](auto* context, auto* config) {
            auto& e = Find(context);
            e.jfr.urma_ctx = context;
            e.jfr.jfr_cfg = *config;
            return &e.jfr;
        };
        f.create_jetty = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto& jetty = e.jetty[e.jetty_count++];
            jetty.urma_ctx = context;
            jetty.jetty_cfg = *config;
            jetty.jetty_id.id = 100 + (context == &active_->endpoints_[0].context ? 0 : 10) + e.jetty_count;
            jetty.jetty_id.eid = context->eid;
            return &jetty;
        };
        f.register_seg = [](auto* context, auto* config) {
            auto& e = Find(context);
            auto& segment = e.segments[e.segment_count++];
            segment.urma_ctx = context;
            segment.seg.ubva.va = config->va;
            segment.seg.len = config->len;
            return &segment;
        };
        f.import_jetty = [](auto* context, auto* remote, auto*) {
            auto& e = Find(context);
            ++e.imports;
            while (e.block_import.load()) {
                e.block_import.wait(true);
            }
            e.target.urma_ctx = context;
            e.target.id = remote->jetty_id;
            return &e.target;
        };
        f.unimport_jetty = [](auto*) { return URMA_SUCCESS; };
        f.delete_jetty = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfr = [](auto*) { return URMA_SUCCESS; };
        f.delete_jfc = [](auto*) { return URMA_SUCCESS; };
        f.unregister_seg = [](auto*) { return URMA_SUCCESS; };
        f.modify_jetty = [](auto*, auto*) { return URMA_SUCCESS; };
        f.flush_jetty = [](auto*, int, auto*) { return 0; };
        f.post_jfr_wr = [](auto* jfr, auto* wr, auto** bad) {
            *bad = nullptr;
            auto& e = Find(jfr->urma_ctx);
            for (; wr; wr = wr->next) {
                e.posted.push_back({wr->user_ctx, wr->src.sge[0]});
            }
            return URMA_SUCCESS;
        };
        f.post_jetty_send_wr = [](auto* jetty, auto* wr, auto** bad) {
            *bad = nullptr;
            auto& e = Find(jetty->urma_ctx);
            for (; wr; wr = wr->next) {
                auto& peer = active_->endpoints_[wr->tjetty->id.id >= 110 ? 1 : 0];
                if (peer.posted.empty()) {
                    *bad = wr;
                    return URMA_ENOMEM;
                }
                auto receive = peer.posted.front();
                peer.posted.pop_front();
                std::size_t bytes = 0;
                for (std::uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
                    const auto& sge = wr->send.src.sge[i];
                    std::memcpy(reinterpret_cast<std::byte*>(receive.sge.addr) + bytes,
                                reinterpret_cast<const void*>(sge.addr), sge.len);
                    bytes += sge.len;
                }
                urma_cr_t tx{};
                tx.status = URMA_CR_SUCCESS;
                tx.flag.bs.jetty = 1;
                tx.local_id = jetty->jetty_id.id;
                tx.user_ctx = wr->user_ctx;
                tx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                e.completed[0].push_back(tx);
                urma_cr_t rx{};
                rx.status = URMA_CR_SUCCESS;
                rx.flag.bs.jetty = 1;
                rx.flag.bs.s_r = 1;
                rx.local_id = peer.jetty[0].jetty_id.id;
                rx.remote_id = jetty->jetty_id;
                rx.user_ctx = receive.id;
                rx.opcode = URMA_CR_OPC_SEND_WITH_IMM;
                rx.imm_data = wr->send.imm_data;
                rx.completion_len = bytes;
                peer.completed[1].push_back(rx);
            }
            return URMA_SUCCESS;
        };
        f.poll_jfc = [](auto* cq, int capacity, auto* out) {
            auto& e = Find(cq->urma_ctx);
            const auto side = cq == &e.cq[0] ? 0u : 1u;
            if (side == 0 && e.hold_tx) {
                return 0;
            }
            auto& completed = e.completed[side];
            int count = 0;
            while (count < capacity && !completed.empty()) {
                out[count++] = completed.front();
                completed.pop_front();
            }
            return count;
        };
        return f;
    }
    bool Until(auto predicate) {
        const auto deadline = Now() + 3'000'000'000ull;
        while (!predicate() && Now() < deadline) {
            const auto progress = stack_.Tick();
            if (!progress) {
                ADD_FAILURE() << "Tick error " << static_cast<int>(progress.error().code);
                return false;
            }
            std::this_thread::yield();
        }
        return predicate();
    }
    control::HandshakeHello Hello(std::size_t i) {
        auto& e = endpoints_[i];
        control::HandshakeHello hello;
        hello.capabilities = control::kHandshakeCapabilityRaw;
        hello.identity = {.runtime = 11,
                          .receive_domain = 101 + i,
                          .endpoint_generation = 1,
                          .authorization = 7,
                          .session = 201 + i,
                          .token = {0, 1}};
        hello.credit_epoch = 301 + i;
        hello.jetty_id = e.jetty[0].jetty_id.id;
        std::memcpy(hello.eid.data(), &e.context.eid, hello.eid.size());
        const auto lanes = control::CaptureTxLanes(*e.resources.domain.pool);
        EXPECT_TRUE(lanes);
        if (lanes) {
            hello.tx_lanes = *lanes;
        }
        urma_rjetty_t remote{};
        remote.jetty_id = e.jetty[0].jetty_id;
        remote.trans_mode = URMA_TM_RM;
        remote.tp_type = URMA_CTP;
        remote.type = URMA_JETTY;
        hello.descriptor_bytes = sizeof(remote);
        std::memcpy(hello.descriptor.data(), &remote, sizeof(remote));
        return hello;
    }
    void SetUp() override {
        active_ = this;
        override_.emplace(Functions());
        std::strcpy(device_.name, "raw");
        device_.type = URMA_TRANSPORT_UB;
        std::array<runtime::RawEndpointConfig, 2> endpoints;
        for (std::size_t i = 0; i < 2; ++i) {
            eids_[i].eid_index = i;
            eids_[i].eid.raw[0] = i + 1;
            endpoints_[i].context.dev = &device_;
            endpoints_[i].context.eid = eids_[i].eid;
            endpoints_[i].context.eid_index = i;
            endpoints_[i].context.async_fd = -1;
            endpoints[i].device = {"raw", static_cast<std::uint32_t>(i)};
            endpoints[i].cookie = &endpoints_[i];
            endpoints[i].on_connection = [](void* cookie, const runtime::ConnectionProgressEvent& event) noexcept {
                auto& endpoint = *static_cast<Endpoint*>(cookie);
                if (endpoint.binding.table() && !endpoint.binding.RecordProgress(event)) {
                    ++endpoint.forward_errors;
                }
            };
            auto& domain = endpoints[i].domain;
            domain.local_domain = {11, 101 + i};
            domain.context_incarnation = i + 1;
            domain.queues.tx_depth = domain.tx_attempts = 16;
            domain.queues.rx_depth = domain.queues.rx_cq_depth = 16;
            domain.route_capacity = domain.source_capacity = 8;
            domain.peer_capacity = 2;
            domain.control_slots = 8;
            domain.base_credits = 4;
        }
        runtime::RawStackConfig config;
        config.slab = {32, 4096, 64};
        config.memory_budget_bytes = 32 * 4096;
        config.connections_per_context = config.withdrawals_per_context = 4;
        config.commands_per_context = 4;
        config.imports = {4, 64, 2};
        config.runtime.reactor.budget = 8;
        config.runtime.control.workers = 2;
        const auto opened = stack_.Open(endpoints, config);
        opened_ = stack_.stage() != runtime::RawStackStage::kEmpty;
        ASSERT_TRUE(opened) << "stack=" << static_cast<int>(opened.error().code) << " domain="
                            << (opened.error().domain ? static_cast<int>(opened.error().domain->code) : -1)
                            << " detail=" << (opened.error().domain ? opened.error().domain->detail : -1);

        const auto shared = stack_.shared();
        ASSERT_TRUE(shared);
        shared_ = *shared;
        for (std::size_t i = 0; i < 2; ++i) {
            const auto resource = stack_.resources(i);
            ASSERT_TRUE(resource);
            endpoints_[i].resources = *resource;
            ASSERT_TRUE(endpoints_[i].binding.OpenRaw(stack_, i, {401 + i, 4, 3}));
        }
    }
    void AdoptProtocol() {
        int sockets[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets), 0);
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            auto& domain = e.resources.domain;
            const auto peer = domain.credits->Admit({11, 101 + i, 301 + i});
            ASSERT_TRUE(peer);
            e.peer = *peer;
            ASSERT_TRUE(domain.credits->Grant(*peer, connection::CreditClass::kData, 8));
            ASSERT_TRUE(domain.credits->Grant(*peer, connection::CreditClass::kControl, 4));
            ASSERT_TRUE(e.sender_credits.Open({11, 102 - i, 302 - i}));
            const auto owned = e.resources.connections->AdoptProtocol(sockets[i], *domain.routes,
                                                                      {.local = Hello(i),
                                                                       .expected_peer_authorization = 7,
                                                                       .compatible_peer = true,
                                                                       .deadline_ns = Now() + 10'000'000'000ull},
                                                                      Now());
            ASSERT_TRUE(owned);
            e.owned = *owned;
            const auto stream = e.binding.Publish(*owned);
            ASSERT_TRUE(stream);
            e.stream = *stream;
        }
        ASSERT_TRUE(Until([&] {
            return !stack_.paused(0) && !stack_.paused(1) &&
                   endpoints_[0].resources.connections->Inspect(endpoints_[0].owned)->state ==
                       connection::ConnectionState::kPreparing &&
                   endpoints_[1].resources.connections->Inspect(endpoints_[1].owned)->state ==
                       connection::ConnectionState::kPreparing;
        }));
    }
    void Prepare() {
        for (std::size_t i = 0; i < 2; ++i) {
            auto& e = endpoints_[i];
            auto& domain = e.resources.domain;
            connection::RawConnectionPreparation prep{
                .handshake = {.imports = shared_->imports,
                              .local_context = &e.context,
                              .local_context_incarnation = i + 1,
                              .descriptor_compatible = true,
                              .credits = domain.credits,
                              .credit_peer = e.peer,
                              .reorder = {.window = 8, .max_resident_bytes = 32768}},
                .receive = domain.receive,
                .sender = domain.sender,
                .slab = shared_->slab,
                .sender_credits = &e.sender_credits,
                .tx_capacity = 8,
                .peer_lanes_authorized = true,
                .rndv = {}};
            ASSERT_TRUE(e.resources.connections->PrepareRaw(e.owned, prep, *domain.sources));
        }
    }
    void Activate() {
        AdoptProtocol();
        Prepare();
        ASSERT_TRUE(Until([&] {
            return !stack_.paused(0) && !stack_.paused(1) &&
                   endpoints_[0].binding.Inspect(endpoints_[0].stream)->state == KBS_STREAM_RAW_ACTIVE &&
                   endpoints_[1].binding.Inspect(endpoints_[1].stream)->state == KBS_STREAM_RAW_ACTIVE;
        }));
        // RAW_ACTIVE 尚可带有 READY 派生的 CREDIT；先真实收割 TX/RX、补投和授信通告，
        // 后续暂停或零副本测试的精确资源快照才不包含与被测操作无关的在途控制帧。
        ASSERT_TRUE(Until([&] {
            for (auto& e : endpoints_) {
                const auto status = e.binding.Inspect(e.stream);
                const auto& domain = e.resources.domain;
                const auto peer = domain.credits->PeerStats(e.peer);
                const auto confirmed = domain.credits->ConfirmedCredits(e.peer);
                const auto pending = domain.credits->PublicationPending(e.peer);
                if (!status || status->tx_pending != 0 || !e.completed[0].empty() || !e.completed[1].empty() ||
                    domain.receive->pending_refill() != 0 || !peer || !confirmed || !pending || *pending ||
                    confirmed->max_data < peer->max_data || confirmed->max_control < peer->max_control) {
                    return false;
                }
            }
            return shared_->slab->stats().local_dma == 0;
        }));
    }
    void TearDown() override {
        for (auto& e : endpoints_) {
            e.block_import.store(false);
            e.block_import.notify_all();
            e.hold_tx = false;
        }
        if (opened_ && !shared_) {
            ASSERT_TRUE(stack_.BeginClose());
            ASSERT_TRUE(stack_.AdvanceClose({true, proofs_}));
            override_.reset();
            active_ = nullptr;
            return;
        }
        if (opened_) {
            ASSERT_TRUE(Until([&] { return !stack_.paused(0) && !stack_.paused(1); }));
            for (auto& e : endpoints_) {
                if (e.stream.generation && e.binding.Inspect(e.stream)) {
                    (void)e.binding.Cancel(e.stream);
                }
            }
            ASSERT_TRUE(Until([&] {
                bool closed = true;
                for (auto& e : endpoints_) {
                    if (e.stream.generation && e.binding.Inspect(e.stream)) {
                        closed &= e.binding.CloseStream(e.stream).has_value();
                    }
                }
                return closed && shared_->slab->stats().local_dma == 0;
            }));
            // Mock 不再投递，所有 CQE 由 Runtime 收割后才给出来源与事件静默证明。
            for (int i = 0; i < 4; ++i) {
                ASSERT_TRUE(stack_.Tick());
            }
            for (auto& e : endpoints_) {
                if (e.stream.generation && e.binding.Inspect(e.stream)) {
                    ASSERT_TRUE(e.binding.RetireSources(e.stream, {&e == &endpoints_[0] ? 1u : 2u, true, true, true}));
                    ASSERT_TRUE(e.binding.Release(e.stream));
                }
                ASSERT_TRUE(e.binding.Close());
                ASSERT_TRUE(e.sender_credits.Close());
            }
            ASSERT_TRUE(stack_.BeginClose());
            ASSERT_TRUE(
                Until([&] { return !stack_.paused(0) && !stack_.paused(1) && shared_->imports->entries() == 0; }));
            auto closed = stack_.AdvanceClose({true, proofs_});
            ASSERT_TRUE(closed);
            EXPECT_EQ(closed->stage, runtime::RawStackStage::kClosed);
        }
        override_.reset();
        active_ = nullptr;
    }
};
TEST_F(RawStreamBindingTest, RawOwnerCannotBypassStackGateThroughTcpBinding) {
    StreamBinding unsafe;
    ExpectError(unsafe.OpenTcp(*endpoints_[0].resources.connections, {501, 4, 16}), KBS_STREAM_INVALID_ARGUMENT);
    EXPECT_EQ(unsafe.table(), nullptr);
    ASSERT_TRUE(unsafe.Close());
}
TEST_F(RawStreamBindingTest, SameCAbiDeliversRawPrefixesAndRetainsDmaUntilRealCompletion) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    std::array<std::byte, 5> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5}};
    const auto original = payload;
    kbs_stream_io io{};
    sender.hold_tx = true;
    ASSERT_EQ(kbs_stream_write(sender.binding.table(), sender.stream, payload.data(), payload.size(), &io, nullptr), 0);
    ASSERT_EQ(io.bytes, 3u);
    payload.fill(std::byte{0xff});
    ASSERT_TRUE(Until([&] {
        return !stack_.paused(0) && !stack_.paused(1) && shared_->slab->stats().local_dma > 0 &&
               receiver.binding.Inspect(receiver.stream)->rx_pending > 0;
    }));
    std::array<std::byte, 5> output{};
    ASSERT_EQ(
        kbs_stream_read(receiver.binding.table(), receiver.stream, output.data(), output.size(), Now(), &io, nullptr),
        0);
    EXPECT_EQ(io.bytes, 3u);
    EXPECT_TRUE(std::equal(original.begin(), original.begin() + 3, output.begin()));
    EXPECT_EQ(kbs_stream_watch_tcp(sender.binding.table(), sender.stream, 1, 0, nullptr), KBS_STREAM_NOT_READY);
    // 模拟网卡已接受但 TX CQE 尚未消费，close 不可释放源 Slab 或取消发布身份。
    kbs_stream_error error{};
    EXPECT_EQ(kbs_stream_close(sender.binding.table(), sender.stream, &error), KBS_STREAM_RETAINED);
    EXPECT_EQ(error.code, KBS_STREAM_RETAINED);
    EXPECT_NE(shared_->slab->stats().local_dma, 0u);
    EXPECT_EQ(sender.binding.published(), 1u);
    EXPECT_NE(kbs_stream_release(sender.binding.table(), sender.stream, nullptr), 0);
    EXPECT_EQ(kbs_stream_write(sender.binding.table(), sender.stream, payload.data(), 1, &io, nullptr),
              KBS_STREAM_CLOSED);
    sender.hold_tx = false;
    ASSERT_TRUE(Until([&] { return shared_->slab->stats().local_dma == 0; }));
    ASSERT_EQ(kbs_stream_close(sender.binding.table(), sender.stream, nullptr), 0);
    ASSERT_TRUE(sender.binding.Inspect(sender.stream));
    EXPECT_EQ(kbs_stream_release(sender.binding.table(), sender.stream, nullptr), KBS_STREAM_RETAINED);
    EXPECT_FALSE(sender.binding.RetireSources(sender.stream, {1, true, true, false}));
    ASSERT_TRUE(sender.binding.RetireSources(sender.stream, {1, true, true, true}));
    ASSERT_EQ(kbs_stream_release(sender.binding.table(), sender.stream, nullptr), 0);
    kbs_stream_status status{};
    EXPECT_EQ(kbs_stream_inspect(sender.binding.table(), sender.stream, &status, nullptr), KBS_STREAM_INVALID_HANDLE);
}
TEST_F(RawStreamBindingTest, ImportKeepsBoundEntriesAvailableWhileItsStreamWaitsForHandshake) {
    AdoptProtocol();
    endpoints_[0].block_import.store(true);
    const auto scheduled = endpoints_[0].resources.connections->Inspect(endpoints_[0].owned)->scheduled;
    Prepare();
    ASSERT_TRUE(Until([&] { return endpoints_[0].imports.load() == 1; }));
    auto& e = endpoints_[0];
    kbs_stream_io io{};
    kbs_stream_status status{};
    std::byte byte{};
    // 正在建链的流仍返回其自身的 NotReady；查询、事件、宿主绑定不受慢导入影响。
    EXPECT_FALSE(stack_.paused(0));
    EXPECT_EQ(kbs_stream_write(e.binding.table(), e.stream, &byte, 1, &io, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(kbs_stream_read(e.binding.table(), e.stream, &byte, 1, Now(), &io, nullptr), KBS_STREAM_NOT_READY);
    ASSERT_EQ(kbs_stream_inspect(e.binding.table(), e.stream, &status, nullptr), 0);
    EXPECT_EQ(status.state, KBS_STREAM_PREPARING);
    EXPECT_EQ(kbs_stream_shutdown_write(e.binding.table(), e.stream, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(kbs_stream_watch_tcp(e.binding.table(), e.stream, 1, 0, nullptr), KBS_STREAM_NOT_READY);
    EXPECT_EQ(kbs_stream_watch(e.binding.table(), e.stream, 1, 0, nullptr), KBS_STREAM_NOT_READY);
    std::array<kbs_stream_event, 4> events{};
    std::size_t count = 99;
    EXPECT_EQ(kbs_stream_poll_events(e.binding.table(), events.data(), events.size(), &count, nullptr), 0);
    EXPECT_TRUE(e.binding.RecordProgress({scheduled, connection::ConnectionState::kPreparing}));
    StreamBinding additional;
    ASSERT_TRUE(additional.OpenRaw(stack_, 0, {503, 4, 16}));
    ASSERT_TRUE(additional.Close());
    ASSERT_TRUE(Until([&] { return !stack_.paused(1); }));
    EXPECT_TRUE(endpoints_[1].binding.Inspect(endpoints_[1].stream));
    EXPECT_TRUE(endpoints_[1].binding.PollEvents(events));
    EXPECT_EQ(e.binding.published(), 1u);
    e.block_import.store(false);
    e.block_import.notify_all();
    ASSERT_TRUE(
        Until([&] { return !stack_.paused(0) && e.binding.Inspect(e.stream)->state == KBS_STREAM_RAW_ACTIVE; }));
    EXPECT_EQ(e.binding.Inspect(e.stream)->failure.code, 0);
    const auto pending = e.binding.PollEvents(events);
    ASSERT_TRUE(pending);
    ASSERT_EQ(*pending, 1u);
    EXPECT_EQ(events[0].stream.generation, e.stream.generation);
    EXPECT_EQ(events[0].state, KBS_STREAM_RAW_ACTIVE);
    EXPECT_EQ(e.forward_errors, 0u);
}
TEST_F(RawStreamBindingTest, LeaseWriteContinuesDuringOtherContextImport) {
    Activate();
    BufferBinding buffers;
    ASSERT_TRUE(buffers.OpenSlab(*shared_->slab, {710, 1}));
    const auto buffer = buffers.Acquire(17);
    ASSERT_TRUE(buffer);
    const auto hello = Hello(0);
    // 用独立授权身份创建真实 import 慢任务，保持 context 0 的现有流处于 RawActive。
    control::EndpointImportRequest request{.key = {.local_context = &endpoints_[1].context,
                                                   .local_context_incarnation = 2,
                                                   .peer_runtime = hello.identity.runtime,
                                                   .receive_domain = hello.identity.receive_domain,
                                                   .endpoint_generation = hello.identity.endpoint_generation,
                                                   .jetty_id = hello.jetty_id,
                                                   .authorization_identity = 8},
                                           .descriptor = {hello.descriptor.data(), hello.descriptor_bytes}};
    std::memcpy(request.key.eid.data(), hello.eid.data(), hello.eid.size());
    const auto extra = shared_->imports->Acquire(request);
    ASSERT_TRUE(extra);
    endpoints_[1].block_import.store(true);
    ASSERT_TRUE(Until([&] { return endpoints_[1].imports.load() == 2; }));
    EXPECT_FALSE(stack_.paused(0));
    EXPECT_EQ(endpoints_[0].binding.Inspect(endpoints_[0].stream)->state, KBS_STREAM_RAW_ACTIVE);
    // 另一 context 正在导入时，已有连接仍能从共享 Slab 入队并完成真实收发。
    kbs_buffer_view writable{};
    ASSERT_EQ(kbs_buffer_writable(buffers.table(), *buffer, &writable, nullptr), 0);
    std::memset(writable.data, 0x31, writable.size);
    kbs_stream_io io{};
    ASSERT_EQ(kbs_stream_write_lease(endpoints_[0].binding.table(), endpoints_[0].stream, buffers.table(), *buffer,
                                      0, 3, &io, nullptr), 0);
    EXPECT_EQ(io.bytes, 3u);
    std::array<std::byte, 3> received{};
    std::size_t received_bytes = 0;
    ASSERT_TRUE(Until([&] {
        if (received_bytes == received.size()) {
            return true;
        }
        const auto read = endpoints_[1].binding.Read(
            endpoints_[1].stream, std::span(received).subspan(received_bytes), Now());
        if (read) {
            received_bytes += read->bytes;
        }
        return received_bytes == received.size();
    }));
    EXPECT_EQ(received, (std::array<std::byte, 3>{std::byte{0x31}, std::byte{0x31}, std::byte{0x31}}));
    ASSERT_TRUE(Until([&] {
        kbs_buffer_status status{};
        return kbs_buffer_inspect(buffers.table(), *buffer, &status, nullptr) == 0 && status.source_reusable;
    }));
    endpoints_[1].block_import.store(false);
    endpoints_[1].block_import.notify_all();
    ASSERT_TRUE(Until([&] { return shared_->imports->Get(*extra).has_value(); }));
    ASSERT_TRUE(shared_->imports->Release(*extra));
    ASSERT_TRUE(buffers.Release(*buffer));
    ASSERT_TRUE(buffers.Close());
}

TEST_F(RawStreamBindingTest, ReceiveLeaseBorrowAndReturnAllocateNothingAndRespectReadWatch) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const std::array<std::byte, 3> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    ASSERT_EQ(sender.binding.Write(sender.stream, payload), 3u);
    ASSERT_TRUE(Until([&] { return receiver.binding.Inspect(receiver.stream)->rx_pending != 0; }));
    std::array<kbs_stream_event, 4> events{};
    ASSERT_TRUE(receiver.binding.PollEvents(events));
    kbs_receive_view view{};
    const auto now = Now();
    allocations = 0;
    count_allocations = true;
    const auto borrowed = kbs_stream_read_lease(receiver.binding.table(), receiver.stream, 1, now, &view, nullptr);
    count_allocations = false;
    ASSERT_EQ(borrowed, 0);
    EXPECT_EQ(allocations, 0u);
    ASSERT_EQ(view.size, 1u);
    // 持有中的当前前缀不能再读，不应消耗一次性可读订阅；归还后尾部立即触发该订阅。
    ASSERT_TRUE(receiver.binding.Watch(receiver.stream, true, false));
    const auto held_events = receiver.binding.PollEvents(events);
    ASSERT_TRUE(held_events);
    for (std::size_t i = 0; i < *held_events; ++i) {
        EXPECT_EQ(events[i].flags & KBS_STREAM_EVENT_READABLE, 0u);
    }
    count_allocations = true;
    const auto released = kbs_stream_release_read_lease(receiver.binding.table(), view.lease, nullptr);
    count_allocations = false;
    ASSERT_EQ(released, 0);
    EXPECT_EQ(allocations, 0u);
    ASSERT_EQ(receiver.binding.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    std::array<std::byte, 2> tail{};
    const auto read = receiver.binding.Read(receiver.stream, tail, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 2u);
    EXPECT_EQ(tail, (std::array<std::byte, 2>{std::byte{2}, std::byte{3}}));
}

TEST_F(RawStreamBindingTest, ReceiveLeaseReturnDuringImportImmediatelyReclaimsBuffer) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    const std::array<std::byte, 3> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    ASSERT_EQ(sender.binding.Write(sender.stream, payload), 3u);
    ASSERT_TRUE(Until([&] { return receiver.binding.Inspect(receiver.stream)->rx_pending != 0; }));
    kbs_receive_view view{};
    ASSERT_EQ(kbs_stream_read_lease(receiver.binding.table(), receiver.stream, 3, Now(), &view, nullptr), 0);
    const auto held = receiver.resources.domain.rx->held();
    const auto hello = Hello(0);
    control::EndpointImportRequest request{.key = {.local_context = &receiver.context,
                                                   .local_context_incarnation = 2,
                                                   .peer_runtime = hello.identity.runtime,
                                                   .receive_domain = hello.identity.receive_domain,
                                                   .endpoint_generation = hello.identity.endpoint_generation,
                                                   .jetty_id = hello.jetty_id,
                                                   .authorization_identity = 9},
                                           .descriptor = {hello.descriptor.data(), hello.descriptor_bytes}};
    std::memcpy(request.key.eid.data(), hello.eid.data(), hello.eid.size());
    const auto extra = shared_->imports->Acquire(request);
    ASSERT_TRUE(extra);
    receiver.block_import.store(true);
    ASSERT_TRUE(Until([&] { return receiver.imports.load() == 2; }));
    // worker 阻塞时即可归还 RX lease；随后同一 token 应失效，不能重复归还。
    EXPECT_FALSE(stack_.paused(1));
    EXPECT_EQ(static_cast<const std::byte*>(view.data)[2], std::byte{3});
    ASSERT_EQ(kbs_stream_release_read_lease(receiver.binding.table(), view.lease, nullptr), 0);
    EXPECT_EQ(receiver.resources.domain.rx->held() + 1, held);
    EXPECT_EQ(kbs_stream_release_read_lease(receiver.binding.table(), view.lease, nullptr), KBS_STREAM_INVALID_HANDLE);
    kbs_receive_view output{};
    EXPECT_EQ(kbs_stream_read_lease(receiver.binding.table(), receiver.stream, 3, Now(), &output, nullptr),
              KBS_STREAM_WOULD_BLOCK);
    receiver.block_import.store(false);
    receiver.block_import.notify_all();
    ASSERT_TRUE(Until([&] { return shared_->imports->Get(*extra).has_value(); }));
    ASSERT_TRUE(shared_->imports->Release(*extra));
}

TEST_F(RawStreamBindingTest, ExistingRawDataPartialReadAndFinRearmImmediately) {
    Activate();
    auto& sender = endpoints_[0];
    auto& receiver = endpoints_[1];
    std::array<kbs_stream_event, 4> events{};
    ASSERT_TRUE(sender.binding.PollEvents(events));
    ASSERT_TRUE(receiver.binding.PollEvents(events));
    ASSERT_TRUE(sender.binding.Watch(sender.stream, false, true));
    ASSERT_EQ(sender.binding.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_WRITABLE, 0u);
    std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    ASSERT_EQ(sender.binding.Write(sender.stream, payload), payload.size());
    bool received_event = false;
    ASSERT_TRUE(Until([&] {
        if (!received_event) {
            const auto count = receiver.binding.PollEvents(events);
            received_event = count && *count && (events[0].flags & KBS_STREAM_EVENT_RX_CHANGED);
        }
        return received_event;
    }));
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_RX_CHANGED, 0u);
    EXPECT_EQ(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    // 数据在订阅前已到达；每次显式 Watch 都从实际连续数据快照检查，不依赖再次产生 CQE。
    ASSERT_TRUE(receiver.binding.Watch(receiver.stream, true, false));
    ASSERT_EQ(receiver.binding.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    EXPECT_EQ(receiver.binding.PollEvents(events), 0u);
    std::array<std::byte, 3> output{};
    auto read = receiver.binding.Read(receiver.stream, std::span(output).first(1), Now());
    ASSERT_TRUE(read);
    ASSERT_EQ(read->bytes, 1u);
    ASSERT_TRUE(receiver.binding.Watch(receiver.stream, true, false));
    ASSERT_EQ(receiver.binding.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    read = receiver.binding.Read(receiver.stream, std::span(output).subspan(1), Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 2u);
    EXPECT_EQ(output, payload);
    ASSERT_TRUE(receiver.binding.PollEvents(events));
    ASSERT_TRUE(sender.binding.ShutdownWrite(sender.stream));
    ASSERT_TRUE(Until([&] { return receiver.resources.connections->Inspect(receiver.owned)->readiness.readable; }));
    ASSERT_TRUE(receiver.binding.Watch(receiver.stream, true, false));
    ASSERT_EQ(receiver.binding.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    EXPECT_EQ(events[0].read_eof, 0u);
    read = receiver.binding.Read(receiver.stream, output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 0u);
    EXPECT_EQ(read->eof, 1u);
    ASSERT_TRUE(receiver.binding.Watch(receiver.stream, true, false));
    ASSERT_EQ(receiver.binding.PollEvents(events), 1u);
    EXPECT_EQ(events[0].read_eof, 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_READABLE, 0u);
    EXPECT_EQ(sender.forward_errors, 0u);
    EXPECT_EQ(receiver.forward_errors, 0u);
}
TEST_F(RawStreamBindingTest, RetainedReleaseKeepsQueuedApplicationEvent) {
    Activate();
    auto& sender = endpoints_[0];
    std::array<kbs_stream_event, 4> events{};
    ASSERT_TRUE(sender.binding.PollEvents(events));
    sender.hold_tx = true;
    const std::array<std::byte, 1> payload{std::byte{0x51}};
    ASSERT_EQ(sender.binding.Write(sender.stream, payload), 1u);
    ASSERT_TRUE(Until([&] { return shared_->slab->stats().local_dma > 0; }));
    ExpectError(sender.binding.CloseStream(sender.stream), KBS_STREAM_RETAINED);
    EXPECT_FALSE(sender.binding.Release(sender.stream));
    ASSERT_EQ(sender.binding.PollEvents(events), 1u);
    EXPECT_EQ(events[0].stream.generation, sender.stream.generation);
    EXPECT_EQ(events[0].state, KBS_STREAM_CLOSING);
    EXPECT_EQ(sender.binding.published(), 1u);
    sender.hold_tx = false;
    ASSERT_TRUE(Until([&] { return shared_->slab->stats().local_dma == 0; }));
    ASSERT_EQ(sender.binding.PollEvents(events), 1u);
    EXPECT_NE(events[0].flags & KBS_STREAM_EVENT_TX_RETIRED, 0u);
    EXPECT_GT(events[0].tx_retired, 0u);
}
} // namespace
} // namespace api
} // namespace kbsocket
