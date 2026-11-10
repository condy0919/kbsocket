// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_owner.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <new>

#include <gtest/gtest.h>

#include "kbsocket/runtime/runtime_shard.hpp"
#include "kbsocket/transport/tcp/native_tcp_backend.hpp"

namespace {
thread_local bool count_allocations = false;
thread_local std::size_t allocations = 0;
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
namespace runtime {
namespace {
using Code = ConnectionOwnerErrorCode;
using connection::ConnectionState;
constexpr std::uint64_t kTimeoutNs = 5'000'000'000;
std::uint64_t Now() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
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
control::ConnectionHandshakeConfig Config(std::uint64_t runtime) {
    control::ConnectionHandshakeConfig config;
    config.local.capabilities = control::kHandshakeCapabilityTcp;
    config.local.identity = {.runtime = runtime,
                             .receive_domain = runtime + 100,
                             .endpoint_generation = 1,
                             .authorization = 7,
                             .session = runtime + 200,
                             .token = {0, 1}};
    config.expected_peer_authorization = 7;
    config.compatible_peer = true;
    config.deadline_ns = Now() + kTimeoutNs;
    return config;
}
template <class T>
void ExpectError(const std::expected<T, ConnectionOwnerError>& result, Code code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
class ConnectionOwnerTest : public ::testing::Test {
protected:
    ConnectionProgress progress_;
    ConnectionOwner owner_;
    RuntimeShard shard_;
    std::array<connection::ConnectionTable, 2> routes_;
    std::array<OwnedConnection, 16> handles_{};
    std::size_t handle_count_ = 0;
    bool progress_open_ = false, shard_open_ = false;
    std::uint64_t readiness_ = 0, state_events_ = 0;
    std::uint32_t last_fd_events_ = 0;
    ConnectionState last_state_ = ConnectionState::kIdle;
    ScheduledConnection last_event_;
    static void Observe(void* pointer, const ConnectionProgressEvent& event) noexcept {
        auto& self = *static_cast<ConnectionOwnerTest*>(pointer);
        self.last_event_ = event.connection;
        self.last_state_ = event.state;
        self.readiness_ += event.fd_events != 0;
        self.state_events_ += event.fd_events == 0;
        self.last_fd_events_ = event.fd_events;
    }
    void Open(std::uint32_t capacity = 4, std::uint32_t scheduled_capacity = 8) {
        ASSERT_TRUE(
            progress_.Open({.capacity = scheduled_capacity, .sweep_interval_ns = 60'000'000'000}, &Observe, this));
        progress_open_ = true;
        ASSERT_TRUE(owner_.Open(progress_, {.capacity = capacity}));
        ASSERT_TRUE(routes_[0].Open({1, 101}, 8));
        ASSERT_TRUE(routes_[1].Open({2, 102}, 8));
    }
    void Track(OwnedConnection handle) {
        ASSERT_LT(handle_count_, handles_.size());
        handles_[handle_count_++] = handle;
    }
    void StartShard() {
        ASSERT_TRUE(shard_.Open({}, {.reactor = {.events = true, .budget = 4, .busy_poll_ns = 0, .max_wait_ms = 1}},
                                &progress_));
        shard_open_ = true;
    }
    template <class Predicate>
    bool Until(Predicate predicate) {
        const auto deadline = Now() + kTimeoutNs;
        while (!predicate() && Now() < deadline) {
            if (!shard_.RunOnce()) {
                return false;
            }
        }
        return predicate();
    }
    void TearDown() override {
        count_allocations = false;
        for (std::size_t i = 0; i < handle_count_; ++i) {
            if (owner_.Inspect(handles_[i])) {
                EXPECT_TRUE(owner_.CloseConnection(handles_[i]));
                EXPECT_TRUE(owner_.Release(handles_[i]));
            }
        }
        EXPECT_TRUE(owner_.Close());
        if (shard_open_) {
            EXPECT_TRUE(shard_.Close());
        }
        if (progress_open_) {
            EXPECT_TRUE(progress_.Close());
        }
        for (auto& routes : routes_) {
            EXPECT_TRUE(routes.Close());
        }
    }
};

TEST_F(ConnectionOwnerTest, OpenRequiresReadyProgressAndBoundedCapacity) {
    EXPECT_EQ(owner_.scheduler_capacity(), 0u);
    ExpectError(owner_.AdoptTcp(-1), Code::kNotReady);
    EXPECT_FALSE(owner_.Open(progress_));
    ASSERT_TRUE(progress_.Open({.capacity = 4}));
    progress_open_ = true;
    EXPECT_FALSE(owner_.Open(progress_, {.capacity = 0}));
    EXPECT_FALSE(owner_.Open(progress_, {.capacity = ConnectionProgress::kMaxConnections + 1}));
    ASSERT_TRUE(owner_.Open(progress_, {.capacity = 2}));
    EXPECT_EQ(owner_.scheduler_capacity(), 4u);
    ExpectError(owner_.Open(progress_, {.capacity = 2}), Code::kInUse);
    EXPECT_EQ(owner_.stats().capacity, 2u);
    EXPECT_EQ(owner_.stats().occupied, 0u);
    EXPECT_TRUE(owner_.stats().accepting);
}

TEST_F(ConnectionOwnerTest, DirectConnectUsesRegisteredIdentityAndRuntimeWithoutApplicationWatch) {
    Open(2);
    StartShard();
    tcp::NativeTcpListener listener;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_TRUE(listener.Open(reinterpret_cast<const sockaddr*>(&address), sizeof(address)));
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &length), 0);
    const auto deadline = Now() + kTimeoutNs;
    allocations = 0;
    count_allocations = true;
    const auto client =
        owner_.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), Now(), deadline);
    count_allocations = false;
    ASSERT_TRUE(client);
    Track(*client);
    EXPECT_EQ(allocations, 0u);
    const auto initial = owner_.Inspect(*client);
    ASSERT_TRUE(initial);
    const auto identity = initial->scheduled;
    EXPECT_TRUE(initial->state == ConnectionState::kTcpConnecting || initial->state == ConnectionState::kTcpActive);
    ASSERT_TRUE(Wait(listener.fd(), POLLIN));
    const auto accepted = listener.Accept();
    ASSERT_TRUE(accepted);
    TcpPair peer;
    peer.right = *accepted;
    // 不调用 WatchTcp；连接器自身由原 scheduler 推进并报告同一完整身份。
    ASSERT_TRUE(Until([&] { return last_event_ == identity && last_state_ == ConnectionState::kTcpActive; }));
    EXPECT_EQ(owner_.Inspect(*client)->scheduled, identity);
    const auto notifications = readiness_;
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(shard_.RunOnce());
    }
    EXPECT_EQ(readiness_, notifications);
    std::array<std::byte, 3> payload{std::byte{5}, std::byte{9}, std::byte{4}}, output{};
    ASSERT_EQ(owner_.Write(*client, payload), payload.size());
    ASSERT_TRUE(Wait(peer.right, POLLIN));
    EXPECT_EQ(recv(peer.right, output.data(), output.size(), MSG_DONTWAIT), 3);
    EXPECT_EQ(output, payload);
}

TEST_F(ConnectionOwnerTest, DirectConnectRejectsInvalidAndCapacityWithoutLeakingSlots) {
    Open(1);
    const auto now = Now();
    const auto invalid = owner_.ConnectTcp(nullptr, 0, now, now + kTimeoutNs);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, Code::kConnection);
    EXPECT_EQ(owner_.stats().occupied, 0u);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    tcp::NativeTcpListener listener;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_TRUE(listener.Open(reinterpret_cast<const sockaddr*>(&address), sizeof(address)));
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(listener.fd(), reinterpret_cast<sockaddr*>(&address), &length), 0);
    const auto first =
        owner_.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), now, now + kTimeoutNs);
    ASSERT_TRUE(first);
    Track(*first);
    const auto original = owner_.Inspect(*first)->scheduled;
    ExpectError(owner_.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), now, now + kTimeoutNs),
                Code::kExhausted);
    EXPECT_EQ(owner_.stats().occupied, 1u);
    ASSERT_TRUE(owner_.Cancel(*first));
    ASSERT_TRUE(owner_.CloseConnection(*first));
    ASSERT_TRUE(owner_.Release(*first));
    const auto replacement =
        owner_.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), now, now + kTimeoutNs);
    ASSERT_TRUE(replacement);
    Track(*replacement);
    const auto newer = owner_.Inspect(*replacement)->scheduled;
    EXPECT_EQ(newer.slot_index(), original.slot_index());
    EXPECT_NE(newer.id, original.id);
    EXPECT_FALSE(progress_.Wake(original));
    owner_.StopAccepting();
    ExpectError(owner_.ConnectTcp(reinterpret_cast<const sockaddr*>(&address), sizeof(address), now, now + kTimeoutNs),
                Code::kStopped);
}

TEST_F(ConnectionOwnerTest, InspectIncludesKnownHalfCloseReadiness) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const auto handle = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(handle);
    pair.right = -1;
    Track(*handle);
    const auto opened = owner_.Inspect(*handle);
    ASSERT_TRUE(opened);
    EXPECT_EQ(opened->readiness, connection::ConnectionReadiness{});
    ASSERT_TRUE(owner_.ShutdownWrite(*handle));
    const auto shutdown = owner_.Inspect(*handle);
    ASSERT_TRUE(shutdown);
    EXPECT_TRUE(shutdown->readiness.write_closed);
    EXPECT_FALSE(shutdown->readiness.readable);
    EXPECT_FALSE(shutdown->readiness.writable);
}

TEST_F(ConnectionOwnerTest, FailedTcpAdoptionPreservesFdAndReturnsSlot) {
    Open(1);
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    // 被拒绝的 fd 仍由调用方持有；回滚不能留下占用槽或调度器借用。
    const int flags = ::fcntl(pair.right, F_GETFL);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(pair.right, F_SETFL, flags & ~O_NONBLOCK), 0);
    ExpectError(owner_.AdoptTcp(pair.right), Code::kConnection);
    EXPECT_GE(::fcntl(pair.right, F_GETFD), 0);
    EXPECT_EQ(owner_.stats().occupied, 0u);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    ExpectError(owner_.AdoptTcp(-1), Code::kConnection);
    ASSERT_EQ(::fcntl(pair.right, F_SETFL, flags), 0);
    auto adopted = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    EXPECT_EQ(owner_.stats().adopted, 1u);
}

TEST_F(ConnectionOwnerTest, ExhaustedCapacityDoesNotTakeAnotherFd) {
    Open(1);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    auto adopted = owner_.AdoptTcp(first.right);
    ASSERT_TRUE(adopted);
    first.right = -1;
    Track(*adopted);
    ExpectError(owner_.AdoptTcp(second.right), Code::kExhausted);
    EXPECT_GE(::fcntl(second.right, F_GETFD), 0);
    EXPECT_EQ(owner_.stats().occupied, 1u);
    EXPECT_EQ(progress_.stats()->attached, 1u);
}

TEST_F(ConnectionOwnerTest, SchedulerExhaustionRollsBackWithoutTakingFd) {
    Open(2, 1);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    auto adopted = owner_.AdoptTcp(first.right);
    ASSERT_TRUE(adopted);
    first.right = -1;
    Track(*adopted);
    ExpectError(owner_.AdoptTcp(second.right), Code::kProgress);
    EXPECT_GE(::fcntl(second.right, F_GETFD), 0);
    EXPECT_EQ(owner_.stats().occupied, 1u);
    ASSERT_TRUE(owner_.CloseConnection(*adopted));
    ASSERT_TRUE(owner_.Release(*adopted));
    auto replacement = owner_.AdoptTcp(second.right);
    ASSERT_TRUE(replacement);
    second.right = -1;
    Track(*replacement);
}

TEST_F(ConnectionOwnerTest, DuplicateFdAdoptionPreservesOriginalOwnedConnection) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int owned_fd = pair.right;
    auto adopted = owner_.AdoptTcp(owned_fd);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    // 新槽还是 Idle 时看不到 fd；入口必须在转移所有权前查询整个共享调度器。
    ExpectError(owner_.AdoptTcp(owned_fd), Code::kInUse);
    ExpectError(owner_.AdoptProtocol(owned_fd, routes_[0], Config(1), Now()), Code::kInUse);
    EXPECT_EQ(owner_.stats().occupied, 1u);
    EXPECT_EQ(progress_.stats()->attached, 1u);
    EXPECT_GE(::fcntl(owned_fd, F_GETFD), 0);
    const std::array<std::byte, 1> byte{std::byte{'d'}};
    EXPECT_EQ(owner_.Write(*adopted, byte), 1u);
    ASSERT_TRUE(Wait(pair.left, POLLIN));
    std::byte output{};
    ASSERT_EQ(::recv(pair.left, &output, 1, 0), 1);
    EXPECT_EQ(output, byte[0]);
}

TEST_F(ConnectionOwnerTest, ExistingExternalConnectionPreventsFdOwnershipDuplication) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    connection::Connection external;
    const int fd = pair.right;
    ASSERT_TRUE(external.OpenTcp(fd));
    pair.right = -1;
    const auto scheduled = progress_.Attach(external);
    ASSERT_TRUE(scheduled);
    // 调度器也可以借用其它宿主的连接；池不能只检查自己占用的槽。
    ExpectError(owner_.AdoptTcp(fd), Code::kInUse);
    EXPECT_EQ(owner_.stats().occupied, 0u);
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(external.state(), ConnectionState::kTcpActive);
    ASSERT_TRUE(external.Close());
    ASSERT_TRUE(progress_.Detach(*scheduled));
}

TEST_F(ConnectionOwnerTest, FailedProtocolConfigKeepsFdAndRouteTableReusable) {
    Open(1);
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    auto config = Config(1);
    config.compatible_peer = false;
    ExpectError(owner_.AdoptProtocol(pair.right, routes_[0], config, Now()), Code::kConnection);
    EXPECT_GE(::fcntl(pair.right, F_GETFD), 0);
    EXPECT_EQ(owner_.stats().occupied, 0u);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    config.compatible_peer = true;
    config.local.identity.session = 0;
    // 编码校验失败发生在路由 token 预留之后，也必须完整回滚而不接管 fd。
    ExpectError(owner_.AdoptProtocol(pair.right, routes_[0], config, Now()), Code::kConnection);
    EXPECT_GE(::fcntl(pair.right, F_GETFD), 0);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    config.local.identity.session = 201;
    auto adopted = owner_.AdoptProtocol(pair.right, routes_[0], config, Now());
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    EXPECT_EQ(owner_.Inspect(*adopted)->state, ConnectionState::kNegotiating);
    ExpectError(owner_.Peer(*adopted), Code::kNotReady);
}

TEST_F(ConnectionOwnerTest, InvalidRndvConfigRollsBackWithoutTakingSocketOwnership) {
    Open(1);
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    connection::RndvConnectionConfig rndv;
    rndv.tx_capacity = 0;
    // 可选 RNDV 参数必须转交 Connection 校验；失败不消耗 owner 槽、路由 token 或 TCP fd。
    const auto adopted = owner_.AdoptProtocol(pair.right, routes_[0], Config(1), Now(), &rndv);
    ExpectError(adopted, Code::kConnection);
    EXPECT_EQ(owner_.stats().occupied, 0u);
    EXPECT_EQ(progress_.stats()->attached, 0u);
    EXPECT_GE(::fcntl(pair.right, F_GETFD), 0);
    const auto retry = owner_.AdoptProtocol(pair.right, routes_[0], Config(1), Now());
    ASSERT_TRUE(retry);
    pair.right = -1;
    Track(*retry);
    EXPECT_EQ(owner_.Inspect(*retry)->state, ConnectionState::kNegotiating);
}

TEST_F(ConnectionOwnerTest, CloseAndReleaseInvalidateOldHandleBeforeSlotReuse) {
    Open(1);
    TcpPair first, second;
    ASSERT_TRUE(first.Open());
    ASSERT_TRUE(second.Open());
    auto old = owner_.AdoptTcp(first.right);
    ASSERT_TRUE(old);
    first.right = -1;
    Track(*old);
    const auto old_status = owner_.Inspect(*old);
    ASSERT_TRUE(old_status);
    ExpectError(owner_.Release(*old), Code::kInUse);
    ASSERT_TRUE(owner_.CloseConnection(*old));
    EXPECT_EQ(owner_.Inspect(*old)->state, ConnectionState::kClosed);
    EXPECT_EQ(progress_.stats()->attached, 1u);
    ASSERT_TRUE(owner_.Release(*old));
    auto newer = owner_.AdoptTcp(second.right);
    ASSERT_TRUE(newer);
    second.right = -1;
    Track(*newer);
    EXPECT_EQ(newer->slot, old->slot);
    EXPECT_NE(newer->generation, old->generation);
    const auto new_status = owner_.Inspect(*newer);
    ASSERT_TRUE(new_status);
    EXPECT_NE(new_status->scheduled.id, old_status->scheduled.id);
    // 对象地址可以复用，旧代次不能借此操作新连接或唤醒其调度槽。
    ExpectError(owner_.Inspect(*old), Code::kInvalidHandle);
    ExpectError(owner_.Cancel(*old), Code::kInvalidHandle);
    ExpectError(owner_.Release(*old), Code::kInvalidHandle);
    EXPECT_FALSE(progress_.Wake(old_status->scheduled));
    EXPECT_EQ(new_status->state, ConnectionState::kTcpActive);
}

TEST_F(ConnectionOwnerTest, ForeignOwnerAndForgedHandlesCannotOperateConnection) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    auto adopted = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    ConnectionOwner another;
    ASSERT_TRUE(another.Open(progress_, {.capacity = 1}));
    ExpectError(another.Inspect(*adopted), Code::kInvalidHandle);
    auto forged = *adopted;
    forged.generation++;
    ExpectError(owner_.CloseConnection(forged), Code::kInvalidHandle);
    forged = *adopted;
    forged.slot = 100;
    ExpectError(owner_.WatchTcp(forged, true, false), Code::kInvalidHandle);
    ExpectError(owner_.Inspect({}), Code::kInvalidHandle);
    EXPECT_EQ(owner_.Inspect(*adopted)->state, ConnectionState::kTcpActive);
    EXPECT_TRUE(another.Close());
}

TEST_F(ConnectionOwnerTest, StopAcceptingAndInUseCloseLeaveExistingConnectionUsable) {
    Open();
    TcpPair pair, spare;
    ASSERT_TRUE(pair.Open());
    ASSERT_TRUE(spare.Open());
    auto adopted = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    owner_.StopAccepting();
    EXPECT_FALSE(owner_.stats().accepting);
    ExpectError(owner_.AdoptTcp(spare.right), Code::kStopped);
    EXPECT_GE(::fcntl(spare.right, F_GETFD), 0);
    ExpectError(owner_.Close(), Code::kInUse);
    const std::array<std::byte, 1> byte{std::byte{'x'}};
    EXPECT_EQ(owner_.Write(*adopted, byte), byte.size());
    ASSERT_TRUE(Wait(pair.left, POLLIN));
    std::byte received{};
    ASSERT_EQ(::recv(pair.left, &received, 1, 0), 1);
    EXPECT_EQ(received, byte[0]);
    ASSERT_TRUE(owner_.CloseConnection(*adopted));
    ASSERT_TRUE(owner_.Release(*adopted));
    EXPECT_TRUE(owner_.Close());
    EXPECT_FALSE(owner_.Open(progress_));
}

TEST_F(ConnectionOwnerTest, DirectTcpHasTransparentShortReadsAndBidirectionalFin) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int left_fd = pair.left, right_fd = pair.right;
    auto left = owner_.AdoptTcp(pair.left);
    ASSERT_TRUE(left);
    pair.left = -1;
    Track(*left);
    auto right = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(right);
    pair.right = -1;
    Track(*right);
    std::array<std::byte, 5> source{std::byte{'K'}, std::byte{0}, std::byte{0xff}, std::byte{'B'}, std::byte{7}};
    const auto expected = source;
    std::array<std::byte, 5> output{};
    const auto empty = owner_.Read(*right, output, Now());
    ASSERT_FALSE(empty);
    ASSERT_TRUE(empty.error().connection);
    EXPECT_EQ(empty.error().connection->code, connection::ConnectionErrorCode::kWouldBlock);
    ASSERT_EQ(owner_.Write(*left, source), source.size());
    source.fill(std::byte{0x77});
    // 每次只给两个字节容量，确认入口不要求整消息读取且 write 返回后可复用源内存。
    std::size_t received = 0;
    const auto deadline = Now() + kTimeoutNs;
    while (received < output.size() && Now() < deadline) {
        auto read =
            owner_.Read(*right, std::span(output).subspan(received, std::min<std::size_t>(2, 5 - received)), Now());
        if (read) {
            ASSERT_GT(read->bytes, 0u);
            EXPECT_LE(read->bytes, 2u);
            received += read->bytes;
        } else {
            ASSERT_TRUE(read.error().connection);
            ASSERT_EQ(read.error().connection->code, connection::ConnectionErrorCode::kWouldBlock);
        }
    }
    ASSERT_EQ(received, output.size());
    EXPECT_EQ(output, expected);
    ASSERT_TRUE(owner_.ShutdownWrite(*left));
    ASSERT_TRUE(Wait(right_fd, POLLIN));
    auto eof = owner_.Read(*right, output, Now());
    ASSERT_TRUE(eof);
    EXPECT_TRUE(eof->eof);
    EXPECT_EQ(eof->bytes, 0u);
    ASSERT_EQ(owner_.Write(*right, expected), expected.size());
    ASSERT_TRUE(Wait(left_fd, POLLIN));
    auto reply = owner_.Read(*left, output, Now());
    ASSERT_TRUE(reply);
    EXPECT_EQ(reply->bytes, expected.size());
    EXPECT_EQ(output, expected);
    EXPECT_EQ(owner_.Inspect(*left)->state, ConnectionState::kTcpActive);
}

TEST_F(ConnectionOwnerTest, ReadinessIsOneShotAndApplicationOperationsAutomaticallyWake) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    auto adopted = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    StartShard();
    ASSERT_TRUE(shard_.Tick());
    const auto visits = progress_.stats()->visits;
    const std::array<std::byte, 1> byte{std::byte{'x'}};
    ASSERT_EQ(owner_.Write(*adopted, byte), byte.size());
    // 不手动 Wake；入口负责使已离开 ready 队列的连接再次得到 owner 访问。
    ASSERT_TRUE(shard_.Tick());
    EXPECT_GT(progress_.stats()->visits, visits);
    ASSERT_TRUE(owner_.WatchTcp(*adopted, true, false));
    ASSERT_EQ(::send(pair.left, byte.data(), byte.size(), MSG_NOSIGNAL), 1);
    ASSERT_TRUE(Until([&] { return readiness_ != 0; }));
    EXPECT_NE(last_fd_events_ & EPOLLIN, 0u);
    EXPECT_EQ(last_event_, owner_.Inspect(*adopted)->scheduled);
    const auto notified = readiness_;
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(shard_.RunOnce());
    }
    EXPECT_EQ(readiness_, notified);
    ASSERT_TRUE(owner_.WatchTcp(*adopted, true, false));
    ASSERT_TRUE(Until([&] { return readiness_ > notified; }));
    std::array<std::byte, 1> output{};
    auto read = owner_.Read(*adopted, output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, 1u);
    EXPECT_EQ(output, byte);
}

TEST_F(ConnectionOwnerTest, ProtocolTcpNeedsPrepareAndBothReadyBeforeApplicationIo) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const int right_fd = pair.right;
    auto left = owner_.AdoptProtocol(pair.left, routes_[0], Config(1), Now());
    ASSERT_TRUE(left);
    pair.left = -1;
    Track(*left);
    auto right = owner_.AdoptProtocol(pair.right, routes_[1], Config(2), Now());
    ASSERT_TRUE(right);
    pair.right = -1;
    Track(*right);
    StartShard();
    const std::array<std::byte, 3> payload{std::byte{'K'}, std::byte{'B'}, std::byte{7}};
    ExpectError(owner_.Write(*left, payload), Code::kConnection);
    ExpectError(owner_.PrepareTcp(*left), Code::kConnection);
    ASSERT_TRUE(Until([&] {
        return owner_.Inspect(*left)->state == ConnectionState::kPreparing &&
               owner_.Inspect(*right)->state == ConnectionState::kPreparing;
    }));
    auto peer = owner_.Peer(*left);
    ASSERT_TRUE(peer);
    EXPECT_EQ((*peer)->identity.runtime, 2u);
    ASSERT_TRUE(owner_.PrepareTcp(*left));
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(shard_.Tick());
    }
    EXPECT_EQ(owner_.Inspect(*left)->state, ConnectionState::kWaitingReady);
    EXPECT_EQ(owner_.Inspect(*right)->state, ConnectionState::kPreparing);
    ExpectError(owner_.Write(*left, payload), Code::kConnection);
    ASSERT_TRUE(owner_.PrepareTcp(*right));
    // 不调用 Connection::Progress 或 scheduler.Wake，Prepare 之后只驱动 Runtime。
    ASSERT_TRUE(Until([&] {
        return owner_.Inspect(*left)->state == ConnectionState::kTcpActive &&
               owner_.Inspect(*right)->state == ConnectionState::kTcpActive;
    }));
    ASSERT_EQ(owner_.Write(*left, payload), payload.size());
    ASSERT_TRUE(Wait(right_fd, POLLIN));
    std::array<std::byte, 3> output{};
    const auto read = owner_.Read(*right, output, Now());
    ASSERT_TRUE(read);
    EXPECT_EQ(read->bytes, payload.size());
    EXPECT_EQ(output, payload);
    EXPECT_EQ(routes_[0].published(), 0u);
    EXPECT_EQ(routes_[1].published(), 0u);
}

TEST_F(ConnectionOwnerTest, CancelIsReportedAndDoesNotReleaseTheSlot) {
    Open(1);
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    auto adopted = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    ASSERT_TRUE(owner_.Cancel(*adopted));
    const auto status = owner_.Inspect(*adopted);
    ASSERT_TRUE(status);
    EXPECT_EQ(status->state, ConnectionState::kFailed);
    ASSERT_TRUE(status->failure);
    EXPECT_EQ(status->failure->code, connection::ConnectionErrorCode::kCancelled);
    EXPECT_EQ(owner_.stats().occupied, 1u);
    ExpectError(owner_.Release(*adopted), Code::kInUse);
    ASSERT_TRUE(owner_.CloseConnection(*adopted));
    ASSERT_TRUE(owner_.RetireSources(*adopted, {}));
    ASSERT_TRUE(owner_.Release(*adopted));
    EXPECT_EQ(owner_.stats().occupied, 0u);
}

TEST_F(ConnectionOwnerTest, SourceRetirementRequiresClosedConnectionEvenForTcp) {
    Open();
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    auto adopted = owner_.AdoptTcp(pair.right);
    ASSERT_TRUE(adopted);
    pair.right = -1;
    Track(*adopted);
    ExpectError(owner_.RetireSources(*adopted, {}), Code::kInUse);
    ASSERT_TRUE(owner_.CloseConnection(*adopted));
    EXPECT_TRUE(owner_.RetireSources(*adopted, {}));
    EXPECT_EQ(owner_.Inspect(*adopted)->retained_sources, 0u);
}

TEST_F(ConnectionOwnerTest, HotAdoptionIoAndReleaseUseOnlyPreallocatedStorage) {
    Open(1);
    TcpPair pair;
    ASSERT_TRUE(pair.Open());
    const std::array<std::byte, 1> input{std::byte{'z'}};
    ASSERT_EQ(::send(pair.left, input.data(), input.size(), MSG_NOSIGNAL), 1);
    ASSERT_TRUE(Wait(pair.right, POLLIN));
    std::array<std::byte, 1> output{};
    // 只对已建立的 fd 和预分配的 owner 计数，测试框架及控制面 Open 不在热路径内。
    allocations = 0;
    count_allocations = true;
    const auto adopted = owner_.AdoptTcp(pair.right);
    std::optional<OwnedConnectionStatus> inspected;
    bool read_ok = false, write_ok = false, close_ok = false, release_ok = false;
    if (adopted) {
        pair.right = -1;
        const auto status = owner_.Inspect(*adopted);
        if (status) {
            inspected = *status;
        }
        const auto read = owner_.Read(*adopted, output, Now());
        read_ok = read && read->bytes == 1;
        const auto written = owner_.Write(*adopted, input);
        write_ok = written && *written == 1;
        close_ok = owner_.CloseConnection(*adopted).has_value();
        release_ok = owner_.Release(*adopted).has_value();
    }
    count_allocations = false;
    ASSERT_TRUE(adopted);
    Track(*adopted);
    EXPECT_TRUE(inspected);
    EXPECT_TRUE(read_ok);
    EXPECT_TRUE(write_ok);
    EXPECT_TRUE(close_ok);
    EXPECT_TRUE(release_ok);
    EXPECT_EQ(output, input);
    EXPECT_EQ(allocations, 0u);
}

} // namespace
} // namespace runtime
} // namespace kbsocket
