// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_mailbox.hpp"

#include <netinet/in.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <new>
#include <thread>

#include <gtest/gtest.h>

#include "kbsocket/runtime/context_progress_group.hpp"
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
using QueueCode = CommandQueueErrorCode;
using OwnerCode = ConnectionOwnerErrorCode;
constexpr std::uint64_t kTimeoutNs = 5'000'000'000;
std::uint64_t Now() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
bool Wait(int fd, short events, int timeout_ms = 1000) {
    pollfd ready{fd, events, 0};
    return ::poll(&ready, 1, timeout_ms) == 1 && (ready.revents & (events | POLLHUP | POLLERR));
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
void ExpectQueueError(const std::expected<T, CommandQueueError>& result, QueueCode code) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
}
void ExpectConnectionError(const ConnectionCommandCompletion& completion, connection::ConnectionErrorCode code) {
    ASSERT_FALSE(completion.result);
    EXPECT_EQ(completion.result.error().code, OwnerCode::kConnection);
    ASSERT_TRUE(completion.result.error().connection);
    EXPECT_EQ(completion.result.error().connection->code, code);
}

struct ConcurrentNotices {
    static constexpr std::size_t kCommands = 1024;
    std::array<ConnectionMailboxNotice, kCommands> notices{};
    std::atomic<std::size_t> published = 0;
    static void OnNotice(void* cookie, const ConnectionMailboxNotice& notice) noexcept {
        auto& self = *static_cast<ConcurrentNotices*>(cookie);
        const auto index = self.published.load(std::memory_order_relaxed);
        if (index < self.notices.size()) {
            self.notices[index] = notice;
        }
        self.published.store(index + 1, std::memory_order_release);
    }
};

class ConnectionMailboxTest : public ::testing::Test {
protected:
    ConcurrentNotices concurrent_notices_;
    ConnectionProgress progress_;
    ConnectionOwner owner_;
    ConnectionMailbox mailbox_;
    ContextProgressGroup group_;
    RuntimeShard shard_;
    TcpPair pair_;
    OwnedConnection handle_;
    std::array<OwnedConnection, 8> handles_{};
    std::size_t handle_count_ = 0;
    std::array<ConnectionCommandTicket, 64> tickets_{};
    std::size_t ticket_count_ = 0;
    std::array<ConnectionMailboxNotice, 64> notices_{};
    std::size_t notice_count_ = 0;
    std::uint32_t readiness_ = 0;
    ScheduledConnection ready_connection_;
    int owned_fd_ = -1;
    bool progress_open_ = false;
    bool mailbox_open_ = false;
    bool group_open_ = false;
    bool shard_open_ = false;

    static void Notice(void* cookie, const ConnectionMailboxNotice& notice) noexcept {
        auto& self = *static_cast<ConnectionMailboxTest*>(cookie);
        if (self.notice_count_ < self.notices_.size()) {
            self.notices_[self.notice_count_] = notice;
        }
        ++self.notice_count_;
    }
    static void Ready(void* cookie, const ConnectionProgressEvent& event) noexcept {
        auto& self = *static_cast<ConnectionMailboxTest*>(cookie);
        self.readiness_ |= event.fd_events;
        if (event.fd_events) {
            self.ready_connection_ = event.connection;
        }
    }
    void Open(std::size_t capacity = 8, ConnectionMailboxCallback callback = &Notice, void* cookie = nullptr) {
        ASSERT_TRUE(progress_.Open({.capacity = 8, .sweep_interval_ns = 60'000'000'000}, &Ready, this));
        progress_open_ = true;
        ASSERT_TRUE(owner_.Open(progress_, {.capacity = 4}));
        ASSERT_TRUE(pair_.Open());
        auto adopted = owner_.AdoptTcp(pair_.right);
        ASSERT_TRUE(adopted);
        handle_ = *adopted;
        handles_[handle_count_++] = handle_;
        owned_fd_ = pair_.right;
        pair_.right = -1;
        ASSERT_TRUE(mailbox_.Open(owner_, {.capacity = capacity}, callback, cookie ? cookie : this));
        mailbox_open_ = true;
    }
    ConnectionCommand Command(ConnectionCommandKind kind, std::uint32_t bytes = 0, std::uint64_t token = 17) {
        return {.kind = kind, .connection = handle_, .token = token, .bytes = bytes};
    }
    ConnectionCommandTicket Submit(ConnectionCommand command, std::span<const std::byte> input = {}) {
        auto result = mailbox_.TrySubmit(command, input);
        EXPECT_TRUE(result);
        if (!result) {
            return {};
        }
        EXPECT_LT(ticket_count_, tickets_.size());
        if (ticket_count_ < tickets_.size()) {
            tickets_[ticket_count_++] = *result;
        }
        return *result;
    }
    void Run(std::size_t budget = 8) {
        auto result = mailbox_.Progress(ProgressKind::kMailbox, budget, Now());
        ASSERT_TRUE(result);
    }
    void TearDown() override {
        count_allocations = false;
        if (shard_open_) {
            EXPECT_TRUE(shard_.Close());
        }
        if (group_open_) {
            EXPECT_TRUE(group_.Close());
        }
        if (mailbox_open_) {
            mailbox_.StopAccepting();
            // 即使测试中途断言退出，也先执行/领取已知 ticket，再释放 owner 的借用。
            for (std::size_t i = 0; i < tickets_.size(); ++i) {
                auto result = mailbox_.Progress(ProgressKind::kMailbox, 16, Now());
                if (!result || !result->processed) {
                    break;
                }
            }
            std::array<std::byte, ConnectionCommandQueue::kMaxIoBytes> output{};
            for (std::size_t i = 0; i < ticket_count_; ++i) {
                (void)mailbox_.TakeCompletion(tickets_[i], output);
            }
            EXPECT_TRUE(mailbox_.Close(true));
        }
        for (std::size_t i = 0; i < handle_count_; ++i) {
            if (owner_.Inspect(handles_[i])) {
                EXPECT_TRUE(owner_.CloseConnection(handles_[i]));
                EXPECT_TRUE(owner_.Release(handles_[i]));
            }
        }
        EXPECT_TRUE(owner_.Close());
        if (progress_open_) {
            EXPECT_TRUE(progress_.Close());
        }
    }
};

TEST_F(ConnectionMailboxTest, OpenRequiresOwnerAndProvidesStableWaitFd) {
    EXPECT_FALSE(mailbox_.Open(owner_));
    ExpectQueueError(mailbox_.TrySubmit({}), QueueCode::kNotReady);
    EXPECT_TRUE(mailbox_.wait_fds().empty());
    Open(3);
    EXPECT_EQ(mailbox_.capacity(), 4u);
    EXPECT_EQ(mailbox_.exclusive_context(), nullptr);
    ASSERT_EQ(mailbox_.wait_fds().size(), 1u);
    const int fd = mailbox_.wait_fds()[0];
    EXPECT_GE(fd, 0);
    ExpectQueueError(mailbox_.Open(owner_), QueueCode::kInUse);
    EXPECT_EQ(mailbox_.wait_fds()[0], fd);
}

TEST_F(ConnectionMailboxTest, WriteCopiesInputBeforeReturningAndNotifiesOnlyItsIdentity) {
    Open();
    std::array<std::byte, 5> input{std::byte{'K'}, std::byte{0}, std::byte{0xff}, std::byte{'B'}, std::byte{7}};
    const auto expected = input;
    const auto ticket = Submit(Command(ConnectionCommandKind::kWrite, input.size(), 101), input);
    input.fill(std::byte{0x77});
    EXPECT_FALSE(Wait(pair_.left, POLLIN, 0));
    ExpectQueueError(mailbox_.TakeCompletion(ticket), QueueCode::kPending);
    Run();
    ASSERT_TRUE(Wait(pair_.left, POLLIN));
    std::array<std::byte, 5> output{};
    ASSERT_EQ(::recv(pair_.left, output.data(), output.size(), 0), 5);
    EXPECT_EQ(output, expected);
    const auto done = mailbox_.TakeCompletion(ticket);
    ASSERT_TRUE(done);
    ASSERT_TRUE(done->result);
    EXPECT_EQ(done->result->bytes, expected.size());
    EXPECT_EQ(done->command.kind, ConnectionCommandKind::kWrite);
    EXPECT_EQ(done->command.token, 101u);
    ASSERT_EQ(notice_count_, 1u);
    EXPECT_EQ(notices_[0].ticket, ticket);
    EXPECT_EQ(notices_[0].connection, handle_);
    EXPECT_EQ(notices_[0].token, 101u);
    Run();
    EXPECT_EQ(notice_count_, 1u);
}

TEST_F(ConnectionMailboxTest, ReadRetainsCopiedOutputUntilLargeEnoughCollectorClaimsIt) {
    Open();
    const std::array<std::byte, 5> input{std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'},
                                         std::byte{'o'}};
    ASSERT_EQ(::send(pair_.left, input.data(), input.size(), MSG_NOSIGNAL), 5);
    ASSERT_TRUE(Wait(owned_fd_, POLLIN));
    const auto ticket = Submit(Command(ConnectionCommandKind::kRead, 8));
    Run();
    std::array<std::byte, 4> small{};
    ExpectQueueError(mailbox_.TakeCompletion(ticket, small), QueueCode::kOutputTooSmall);
    EXPECT_EQ(mailbox_.outstanding(), 1u);
    std::array<std::byte, 8> output{};
    auto done = mailbox_.TakeCompletion(ticket, output);
    ASSERT_TRUE(done);
    ASSERT_TRUE(done->result);
    EXPECT_EQ(done->result->bytes, input.size());
    EXPECT_FALSE(done->result->eof);
    EXPECT_TRUE(std::equal(input.begin(), input.end(), output.begin()));
    EXPECT_EQ(mailbox_.outstanding(), 0u);
    ExpectQueueError(mailbox_.TakeCompletion(ticket, output), QueueCode::kInvalidTicket);
}

TEST_F(ConnectionMailboxTest, WouldBlockCompletesOnceWithoutRetryAfterDataArrives) {
    Open();
    const auto empty = Submit(Command(ConnectionCommandKind::kRead, 1));
    Run();
    auto failed = mailbox_.TakeCompletion(empty);
    ASSERT_TRUE(failed);
    ExpectConnectionError(*failed, connection::ConnectionErrorCode::kWouldBlock);
    const std::array<std::byte, 1> byte{std::byte{42}};
    ASSERT_EQ(::send(pair_.left, byte.data(), byte.size(), MSG_NOSIGNAL), 1);
    ASSERT_TRUE(Wait(owned_fd_, POLLIN));
    // 新数据不会把已经完成的 WouldBlock 请求复活；只有新命令可以读取。
    Run();
    EXPECT_EQ(notice_count_, 1u);
    const auto next = Submit(Command(ConnectionCommandKind::kRead, 1));
    Run();
    std::array<std::byte, 1> output{};
    auto done = mailbox_.TakeCompletion(next, output);
    ASSERT_TRUE(done);
    ASSERT_TRUE(done->result);
    EXPECT_EQ(output, byte);
}

TEST_F(ConnectionMailboxTest, ShortReadCompletesWithPrefixWithoutWaitingForRequestedLength) {
    Open();
    const std::array<std::byte, 2> input{std::byte{3}, std::byte{4}};
    ASSERT_EQ(::send(pair_.left, input.data(), input.size(), MSG_NOSIGNAL), 2);
    ASSERT_TRUE(Wait(owned_fd_, POLLIN));
    const auto ticket = Submit(Command(ConnectionCommandKind::kRead, 4096));
    Run();
    // 领取空间按实际完成字节计算，无须为了短读提供请求长度的缓冲区。
    std::array<std::byte, 2> output{};
    auto done = mailbox_.TakeCompletion(ticket, output);
    ASSERT_TRUE(done);
    ASSERT_TRUE(done->result);
    EXPECT_EQ(done->result->bytes, 2u);
    EXPECT_EQ(output, input);
}

TEST_F(ConnectionMailboxTest, ShutdownWritePreservesInboundReadAndEofReporting) {
    Open();
    auto ticket = Submit(Command(ConnectionCommandKind::kShutdownWrite));
    Run();
    auto shutdown = mailbox_.TakeCompletion(ticket);
    ASSERT_TRUE(shutdown);
    ASSERT_TRUE(shutdown->result);
    ASSERT_TRUE(Wait(pair_.left, POLLIN));
    std::array<std::byte, 1> output{};
    EXPECT_EQ(::recv(pair_.left, output.data(), output.size(), 0), 0);
    const std::array<std::byte, 1> input{std::byte{23}};
    ASSERT_EQ(::send(pair_.left, input.data(), input.size(), MSG_NOSIGNAL), 1);
    ASSERT_TRUE(Wait(owned_fd_, POLLIN));
    ticket = Submit(Command(ConnectionCommandKind::kRead, 1));
    Run();
    auto read = mailbox_.TakeCompletion(ticket, output);
    ASSERT_TRUE(read);
    ASSERT_TRUE(read->result);
    EXPECT_EQ(output, input);
    ASSERT_EQ(::shutdown(pair_.left, SHUT_WR), 0);
    ASSERT_TRUE(Wait(owned_fd_, POLLIN));
    ticket = Submit(Command(ConnectionCommandKind::kRead, 1));
    Run();
    auto eof = mailbox_.TakeCompletion(ticket);
    ASSERT_TRUE(eof);
    ASSERT_TRUE(eof->result);
    EXPECT_TRUE(eof->result->eof);
    EXPECT_EQ(eof->result->bytes, 0u);
}

TEST_F(ConnectionMailboxTest, CancelAndInspectAreOrderedAndFailureDoesNotFailProgress) {
    Open();
    const auto cancel = Submit(Command(ConnectionCommandKind::kCancel));
    const std::array<std::byte, 1> input{std::byte{1}};
    const auto write = Submit(Command(ConnectionCommandKind::kWrite, 1), input);
    const auto inspect = Submit(Command(ConnectionCommandKind::kInspect));
    auto processed = mailbox_.Progress(ProgressKind::kMailbox, 8, Now());
    ASSERT_TRUE(processed);
    EXPECT_EQ(processed->processed, 3u);
    auto cancelled = mailbox_.TakeCompletion(cancel);
    ASSERT_TRUE(cancelled);
    EXPECT_TRUE(cancelled->result);
    auto failed = mailbox_.TakeCompletion(write);
    ASSERT_TRUE(failed);
    ExpectConnectionError(*failed, connection::ConnectionErrorCode::kCancelled);
    auto status = mailbox_.TakeCompletion(inspect);
    ASSERT_TRUE(status);
    ASSERT_TRUE(status->result);
    ASSERT_TRUE(status->result->status);
    EXPECT_EQ(status->result->status->state, connection::ConnectionState::kFailed);
    ASSERT_TRUE(status->result->status->failure);
    EXPECT_EQ(status->result->status->failure->code, connection::ConnectionErrorCode::kCancelled);
    EXPECT_EQ(owner_.stats().occupied, 1u);
}

TEST_F(ConnectionMailboxTest, WatchTcpIsExecutedByOwnerAndDoesNotConsumeReadableBytes) {
    Open();
    auto watch = Command(ConnectionCommandKind::kWatchTcp);
    watch.readable = true;
    auto ticket = Submit(watch);
    Run();
    auto done = mailbox_.TakeCompletion(ticket);
    ASSERT_TRUE(done);
    ASSERT_TRUE(done->result);
    const std::array<std::byte, 1> input{std::byte{11}};
    ASSERT_EQ(::send(pair_.left, input.data(), input.size(), MSG_NOSIGNAL), 1);
    const auto deadline = Now() + kTimeoutNs;
    while (!(readiness_ & EPOLLIN) && Now() < deadline) {
        ASSERT_TRUE(progress_.CollectNotifications(8));
        ASSERT_TRUE(progress_.Progress(ProgressKind::kMailbox, 8, Now()));
    }
    EXPECT_NE(readiness_ & EPOLLIN, 0u);
    EXPECT_EQ(ready_connection_, owner_.Inspect(handle_)->scheduled);
    ticket = Submit(Command(ConnectionCommandKind::kRead, 1));
    Run();
    std::array<std::byte, 1> output{};
    auto read = mailbox_.TakeCompletion(ticket, output);
    ASSERT_TRUE(read);
    ASSERT_TRUE(read->result);
    EXPECT_EQ(output, input);
}

TEST_F(ConnectionMailboxTest, StaleAndForeignHandlesCannotOperateOnReplacementConnection) {
    Open();
    const auto stale = handle_;
    const auto stale_ticket = Submit(Command(ConnectionCommandKind::kCancel));
    ASSERT_TRUE(owner_.CloseConnection(handle_));
    ASSERT_TRUE(owner_.Release(handle_));
    TcpPair replacement;
    ASSERT_TRUE(replacement.Open());
    auto adopted = owner_.AdoptTcp(replacement.right);
    ASSERT_TRUE(adopted);
    replacement.right = -1;
    handles_[handle_count_++] = *adopted;
    handle_ = *adopted;
    EXPECT_EQ(handle_.slot, stale.slot);
    EXPECT_NE(handle_.generation, stale.generation);
    const auto current_ticket = Submit(Command(ConnectionCommandKind::kInspect));
    ConnectionOwner foreign;
    auto wrong_owner = Command(ConnectionCommandKind::kInspect);
    wrong_owner.connection.owner = &foreign;
    ExpectQueueError(mailbox_.TrySubmit(wrong_owner), QueueCode::kInvalidArgument);
    Run();
    auto old = mailbox_.TakeCompletion(stale_ticket);
    ASSERT_TRUE(old);
    ASSERT_FALSE(old->result);
    EXPECT_EQ(old->result.error().code, OwnerCode::kInvalidHandle);
    auto current = mailbox_.TakeCompletion(current_ticket);
    ASSERT_TRUE(current);
    ASSERT_TRUE(current->result);
    ASSERT_TRUE(current->result->status);
    EXPECT_EQ(current->result->status->state, connection::ConnectionState::kTcpActive);
}

TEST_F(ConnectionMailboxTest, BudgetAndProgressKindBoundExecutionWithoutLosingWakeup) {
    Open();
    const auto first = Submit(Command(ConnectionCommandKind::kInspect, 0, 1));
    const auto second = Submit(Command(ConnectionCommandKind::kInspect, 0, 2));
    const auto third = Submit(Command(ConnectionCommandKind::kInspect, 0, 3));
    auto other = mailbox_.Progress(ProgressKind::kTimer, 8, Now());
    ASSERT_TRUE(other);
    EXPECT_EQ(other->processed, 0u);
    auto zero = mailbox_.Progress(ProgressKind::kMailbox, 0, Now());
    ASSERT_TRUE(zero);
    EXPECT_EQ(zero->processed, 0u);
    auto partial = mailbox_.Progress(ProgressKind::kMailbox, 2, Now());
    ASSERT_TRUE(partial);
    EXPECT_EQ(partial->processed, 2u);
    EXPECT_TRUE(partial->runnable);
    EXPECT_TRUE(Wait(mailbox_.wait_fds()[0], POLLIN, 0));
    EXPECT_TRUE(mailbox_.TakeCompletion(first));
    EXPECT_TRUE(mailbox_.TakeCompletion(second));
    ExpectQueueError(mailbox_.TakeCompletion(third), QueueCode::kPending);
    auto rest = mailbox_.Progress(ProgressKind::kMailbox, 2, Now());
    ASSERT_TRUE(rest);
    EXPECT_EQ(rest->processed, 1u);
    EXPECT_FALSE(rest->runnable);
    EXPECT_TRUE(mailbox_.TakeCompletion(third));
    EXPECT_EQ(notice_count_, 3u);
}

TEST_F(ConnectionMailboxTest, UncollectedResultsApplyBackpressureUntilClaimed) {
    Open(2);
    const auto first = Submit(Command(ConnectionCommandKind::kInspect));
    const auto second = Submit(Command(ConnectionCommandKind::kInspect));
    Run();
    EXPECT_EQ(mailbox_.outstanding(), 2u);
    ExpectQueueError(mailbox_.TrySubmit(Command(ConnectionCommandKind::kInspect)), QueueCode::kFull);
    ASSERT_TRUE(mailbox_.TakeCompletion(first));
    const auto replacement = Submit(Command(ConnectionCommandKind::kInspect));
    Run();
    EXPECT_TRUE(mailbox_.TakeCompletion(second));
    EXPECT_TRUE(mailbox_.TakeCompletion(replacement));
    EXPECT_EQ(mailbox_.outstanding(), 0u);
    EXPECT_EQ(notice_count_, 3u);
}

TEST_F(ConnectionMailboxTest, StopAndCloseRequireExecutionCollectionAndClientQuiescence) {
    Open();
    const auto ticket = Submit(Command(ConnectionCommandKind::kInspect));
    mailbox_.StopAccepting();
    EXPECT_FALSE(mailbox_.accepting());
    ExpectQueueError(mailbox_.TrySubmit(Command(ConnectionCommandKind::kInspect)), QueueCode::kStopped);
    EXPECT_FALSE(mailbox_.Close(true));
    Run();
    EXPECT_FALSE(mailbox_.Close(true));
    ASSERT_TRUE(mailbox_.TakeCompletion(ticket));
    ExpectQueueError(mailbox_.Close(), QueueCode::kNotQuiescent);
    ASSERT_TRUE(mailbox_.Close(true));
    mailbox_open_ = false;
    EXPECT_TRUE(mailbox_.wait_fds().empty());
    ExpectQueueError(mailbox_.TrySubmit(Command(ConnectionCommandKind::kInspect)), QueueCode::kNotReady);
    EXPECT_FALSE(mailbox_.Open(owner_));
}

TEST_F(ConnectionMailboxTest, SubmitExecuteCollectFastPathAllocatesNothing) {
    Open();
    const std::array<std::byte, 1> input{std::byte{91}};
    ASSERT_EQ(::send(pair_.left, input.data(), input.size(), MSG_NOSIGNAL), 1);
    ASSERT_TRUE(Wait(owned_fd_, POLLIN));
    std::array<std::byte, 1> output{};
    const std::array commands{Command(ConnectionCommandKind::kWrite, 1), Command(ConnectionCommandKind::kRead, 1),
                              Command(ConnectionCommandKind::kInspect)};
    std::array<std::expected<ConnectionCommandTicket, CommandQueueError>, 3> submitted{};
    std::array<std::expected<ConnectionCommandCompletion, CommandQueueError>, 3> completed{};
    // 所有资源和 gtest 断言均在计数区间之外，同时量测 TCP 双向 I/O、状态查询和结果领取。
    allocations = 0;
    count_allocations = true;
    submitted[0] = mailbox_.TrySubmit(commands[0], input);
    submitted[1] = mailbox_.TrySubmit(commands[1]);
    submitted[2] = mailbox_.TrySubmit(commands[2]);
    auto progress = mailbox_.Progress(ProgressKind::kMailbox, 3, Now());
    for (std::size_t i = 0; i < submitted.size(); ++i) {
        if (submitted[i]) {
            completed[i] = mailbox_.TakeCompletion(*submitted[i], output);
        }
    }
    count_allocations = false;
    ASSERT_TRUE(progress);
    for (std::size_t i = 0; i < submitted.size(); ++i) {
        ASSERT_TRUE(submitted[i]);
        ASSERT_TRUE(completed[i]);
        ASSERT_TRUE(completed[i]->result);
    }
    EXPECT_EQ(completed[0]->result->bytes, 1u);
    EXPECT_EQ(completed[1]->result->bytes, 1u);
    EXPECT_EQ(output, input);
    ASSERT_TRUE(completed[2]->result->status);
    EXPECT_EQ(completed[2]->result->status->state, connection::ConnectionState::kTcpActive);
    EXPECT_EQ(allocations, 0u);
    EXPECT_EQ(mailbox_.outstanding(), 0u);
}

TEST_F(ConnectionMailboxTest, ConcurrentCollectorCanRecycleSlotsAndSubmitAfterNotification) {
    auto& notices = concurrent_notices_;
    Open(2, &ConcurrentNotices::OnNotice, &notices);
    std::atomic<bool> finished = false;
    std::atomic<bool> failed = false;
    const auto connection = handle_;
    const auto deadline = Now() + kTimeoutNs;
    // 回调只发布身份且不重入。独立客户端尽快领取并提交下一条，持续复用两个槽。
    std::thread client([&] {
        for (std::size_t i = 0; i < ConcurrentNotices::kCommands; ++i) {
            auto ticket =
                mailbox_.TrySubmit({.kind = ConnectionCommandKind::kInspect, .connection = connection, .token = i + 1});
            if (!ticket) {
                failed = true;
                break;
            }
            while (notices.published.load(std::memory_order_acquire) <= i && Now() < deadline) {
                std::this_thread::yield();
            }
            if (notices.published.load(std::memory_order_acquire) <= i) {
                failed = true;
                break;
            }
            const auto notice = notices.notices[i];
            auto completion = mailbox_.TakeCompletion(*ticket);
            if (notice.ticket != *ticket || notice.connection != connection || notice.token != i + 1 || !completion ||
                !completion->result || completion->command.token != i + 1 || !completion->result->status ||
                completion->result->status->state != connection::ConnectionState::kTcpActive) {
                failed = true;
                break;
            }
        }
        finished.store(true, std::memory_order_release);
    });
    while (!finished.load(std::memory_order_acquire) && Now() < deadline) {
        const auto progress = mailbox_.Progress(ProgressKind::kMailbox, 4, Now());
        if (!progress) {
            failed = true;
            break;
        }
        std::this_thread::yield();
    }
    client.join();
    EXPECT_FALSE(failed.load());
    EXPECT_EQ(notices.published.load(), ConcurrentNotices::kCommands);
    EXPECT_EQ(mailbox_.outstanding(), 0u);
}

TEST_F(ConnectionMailboxTest, TcpRuntimeGroupSchedulesThreadedCommandsAndSocketReadiness) {
    auto& notices = concurrent_notices_;
    Open(4, &ConcurrentNotices::OnNotice, &notices);
    std::array<ProgressSource*, 2> members{&progress_, &mailbox_};
    ASSERT_TRUE(group_.Open(members));
    group_open_ = true;
    ASSERT_TRUE(
        shard_.Open({}, {.reactor = {.events = true, .budget = 2, .busy_poll_ns = 0, .max_wait_ms = 1}}, &group_));
    shard_open_ = true;
    const auto connection = handle_;
    const std::array<std::byte, 3> input{std::byte{7}, std::byte{0}, std::byte{0xfe}};
    std::array<std::byte, 3> output{};
    std::array<ConnectionCommandTicket, 2> submitted{};
    std::atomic<bool> readable = false;
    std::atomic<bool> finished = false;
    std::atomic<bool> failed = false;
    const auto deadline = Now() + kTimeoutNs;
    // 只由 RunOnce 推进 group。客户端先订阅、再发数据，必须等 ConnectionProgress 的真实
    // readiness 后才提交 Read，防止仅 mailbox 成功掩盖连接源未被调度的问题。
    std::thread client([&] {
        auto wait_notice = [&](std::size_t expected) {
            while (notices.published.load(std::memory_order_acquire) < expected && Now() < deadline) {
                std::this_thread::yield();
            }
            return notices.published.load(std::memory_order_acquire) >= expected;
        };
        const auto watch = mailbox_.TrySubmit(
            {.kind = ConnectionCommandKind::kWatchTcp, .connection = connection, .token = 81, .readable = true});
        if (!watch) {
            failed = true;
            finished = true;
            return;
        }
        submitted[0] = *watch;
        if (!wait_notice(1)) {
            failed = true;
            finished = true;
            return;
        }
        const auto watched = mailbox_.TakeCompletion(*watch);
        if (!watched || !watched->result || ::send(pair_.left, input.data(), input.size(), MSG_NOSIGNAL) != 3) {
            failed = true;
            finished = true;
            return;
        }
        while (!readable.load(std::memory_order_acquire) && Now() < deadline) {
            std::this_thread::yield();
        }
        if (!readable.load(std::memory_order_acquire)) {
            failed = true;
            finished = true;
            return;
        }
        const auto read = mailbox_.TrySubmit(
            {.kind = ConnectionCommandKind::kRead, .connection = connection, .token = 82, .bytes = input.size()});
        if (!read) {
            failed = true;
            finished = true;
            return;
        }
        submitted[1] = *read;
        if (!wait_notice(2)) {
            failed = true;
            finished = true;
            return;
        }
        const auto received = mailbox_.TakeCompletion(*read, output);
        if (!received || !received->result || received->result->bytes != input.size()) {
            failed = true;
        }
        finished.store(true, std::memory_order_release);
    });
    while (!finished.load(std::memory_order_acquire) && Now() < deadline) {
        const auto progress = shard_.RunOnce();
        if (!progress) {
            failed = true;
            break;
        }
        if (readiness_ & EPOLLIN) {
            readable.store(true, std::memory_order_release);
        }
    }
    client.join();
    // join 后取得唯一控制权，记录 ticket 以便任何失败路径也能收尾；先解绑 group 再关闭成员。
    for (const auto ticket : submitted) {
        if (ticket.queue) {
            tickets_[ticket_count_++] = ticket;
        }
    }
    EXPECT_FALSE(failed.load());
    EXPECT_EQ(output, input);
    EXPECT_EQ(notices.published.load(), 2u);
    EXPECT_EQ(notices.notices[0].token, 81u);
    EXPECT_EQ(notices.notices[1].token, 82u);
    EXPECT_EQ(ready_connection_, owner_.Inspect(handle_)->scheduled);
    EXPECT_EQ(mailbox_.outstanding(), 0u);
    ASSERT_TRUE(shard_.Close());
    shard_open_ = false;
    ASSERT_TRUE(group_.Close());
    group_open_ = false;
}

} // namespace
} // namespace runtime
} // namespace kbsocket
