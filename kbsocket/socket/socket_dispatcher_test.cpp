// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/socket/socket_dispatcher.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <span>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "kbsocket/api/stream.h"
#include "kbsocket/runtime/progress_source.hpp"
#include "kbsocket/socket/fd_command_queue.hpp"
#include "kbsocket/socket/fd_table.hpp"

namespace kbsocket {
namespace socket {
namespace {
constexpr std::uint64_t kStreamTable = 7101;
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
SocketDispatcherConfig Config() {
    SocketDispatcherConfig config;
    config.owner_id = 7102;
    config.bindings.table_id = 7103;
    config.bindings.fd_limit = 4096;
    config.bindings.binding_capacity = 8;
    config.bindings.reader_capacity = 8;
    config.commands.queue_id = 7104;
    config.commands.capacity = 8;
    config.commands.max_io_bytes = 64;
    config.commands.payload_budget_bytes = 4096;
    config.stream_capacity = 8;
    config.record_budget_bytes = 65536;
    return config;
}
class FakeBackend final : public SocketStreamBackend {
public:
    struct Stream {
        std::uint64_t generation = 1;
        bool closed = false;
        bool released = false;
        bool shutdown = false;
        bool eof = false;
        bool malformed_write_reply = false;
        std::array<std::byte, 64> input{};
        std::array<std::byte, 64> output{};
        std::size_t input_size = 0;
        std::size_t input_offset = 0;
        std::size_t output_size = 0;
        std::size_t write_limit = 64;
        std::size_t writes = 0, reads = 0, shutdowns = 0, inspections = 0, closes = 0, releases = 0;
        std::uint64_t now_ns = 0;
        kbs_stream_error write_error{}, read_error{}, inspect_error{}, close_error{}, release_error{};
    };
    std::array<Stream, 8> streams;
    const void* context = nullptr;
    bool wrong_thread = false;
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
        auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        auto& stream = **found;
        ++stream.writes;
        if (stream.write_error.code) {
            return std::unexpected(stream.write_error);
        }
        if (stream.closed || stream.shutdown) {
            return std::unexpected(Failure(KBS_STREAM_CLOSED));
        }
        const auto bytes = std::min({input.size(), stream.write_limit, stream.output.size()});
        std::copy_n(input.begin(), bytes, stream.output.begin());
        stream.output_size = bytes;
        return kbs_stream_io{.bytes = stream.malformed_write_reply ? input.size() + 1 : bytes, .eof = 0, .reserved = 0};
    }
    std::expected<kbs_stream_io, kbs_stream_error> Read(kbs_stream_handle handle, std::span<std::byte> output,
                                                        std::uint64_t now_ns) noexcept override {
        auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        auto& stream = **found;
        ++stream.reads;
        stream.now_ns = now_ns;
        if (stream.read_error.code) {
            return std::unexpected(stream.read_error);
        }
        const auto bytes = std::min(output.size(), stream.input_size - stream.input_offset);
        if (!bytes && !stream.eof && !output.empty()) {
            return std::unexpected(Failure(KBS_STREAM_WOULD_BLOCK));
        }
        std::copy_n(stream.input.begin() + stream.input_offset, bytes, output.begin());
        stream.input_offset += bytes;
        return kbs_stream_io{
            .bytes = bytes, .eof = static_cast<std::uint32_t>(!bytes && !output.empty() && stream.eof), .reserved = 0};
    }
    std::expected<void, kbs_stream_error> ShutdownWrite(kbs_stream_handle handle) noexcept override {
        auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        ++(*found)->shutdowns;
        (*found)->shutdown = true;
        return {};
    }
    std::expected<kbs_stream_status, kbs_stream_error> Inspect(kbs_stream_handle handle) noexcept override {
        auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        ++(*found)->inspections;
        if ((*found)->inspect_error.code) {
            return std::unexpected((*found)->inspect_error);
        }
        return kbs_stream_status{.state = (*found)->closed ? KBS_STREAM_STATE_CLOSED : KBS_STREAM_TCP_ACTIVE,
                                 .lane_update_pending = 0,
                                 .failure = {},
                                 .tx_pending = 11,
                                 .rx_pending = 13,
                                 .retained_sources = 0};
    }
    std::expected<void, kbs_stream_error> CloseStream(kbs_stream_handle handle) noexcept override {
        auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        ++(*found)->closes;
        if ((*found)->close_error.code) {
            return std::unexpected((*found)->close_error);
        }
        (*found)->closed = true;
        return {};
    }
    std::expected<void, kbs_stream_error> Release(kbs_stream_handle handle) noexcept override {
        auto found = Find(handle);
        if (!found) {
            return std::unexpected(found.error());
        }
        ++(*found)->releases;
        if ((*found)->release_error.code) {
            return std::unexpected((*found)->release_error);
        }
        if (!(*found)->closed) {
            return std::unexpected(Failure(KBS_STREAM_RETAINED));
        }
        (*found)->released = true;
        return {};
    }
    const void* exclusive_context() const noexcept override {
        return context;
    }
};
bool Readable(int fd) {
    pollfd item{fd, POLLIN, 0};
    return ::poll(&item, 1, 0) == 1 && (item.revents & POLLIN);
}
class SocketDispatcherTest : public testing::Test {
protected:
    FakeBackend backend;
    SocketDispatcher dispatcher;
    FdReader reader;
    std::array<FdBindingId, 16> bindings{};
    std::size_t binding_count = 0;
    std::array<FdCommandTicket, 64> tickets{};
    std::size_t ticket_count = 0;

    void SetUp() override {
        ASSERT_TRUE(dispatcher.Open(backend, Config()));
        auto registered = dispatcher.RegisterReader();
        ASSERT_TRUE(registered);
        reader = std::move(*registered);
    }
    void TearDown() override {
        // 即使中途断言失败，也先执行/领取已接纳命令，再按真实关闭步骤释放绑定。
        for (auto& stream : backend.streams) {
            stream.close_error = {};
            stream.release_error = {};
        }
        for (std::size_t i = 0; i < 8; ++i) {
            (void)dispatcher.Progress(runtime::ProgressKind::kMailbox, 64, 9000);
        }
        std::array<std::byte, 64> output{};
        for (std::size_t i = 0; i < ticket_count; ++i) {
            (void)dispatcher.TakeCompletion(tickets[i], output);
        }
        for (std::size_t i = 0; i < binding_count; ++i) {
            auto current = dispatcher.InspectBinding(bindings[i]);
            if (current && current->stage == SocketRetireStage::kActive) {
                (void)dispatcher.Detach(bindings[i]);
            }
        }
        reader.Reset();
        for (std::size_t i = 0; i < 32; ++i) {
            (void)dispatcher.RetireStreams(64);
            (void)dispatcher.Progress(runtime::ProgressKind::kRetire, 64, 9000);
        }
        EXPECT_TRUE(dispatcher.Close(true));
        EXPECT_FALSE(backend.wrong_thread);
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
    std::optional<FdCommandTicket> Submit(FdBindingId binding, FdCommandKind kind,
                                          std::span<const std::byte> input = {}, std::uint32_t bytes = 0) {
        auto lease = dispatcher.Lookup(reader, binding);
        EXPECT_TRUE(lease);
        if (!lease) {
            return std::nullopt;
        }
        auto result = dispatcher.TrySubmit(*lease, {.kind = kind, .token = 99, .bytes = bytes}, input);
        EXPECT_TRUE(result);
        if (!result) {
            return std::nullopt;
        }
        EXPECT_FALSE(*lease);
        tickets[ticket_count++] = *result;
        return *result;
    }
    void Mailbox(std::size_t budget = 64, std::uint64_t now_ns = 12345) {
        ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kMailbox, budget, now_ns));
    }
    void Retire() {
        for (unsigned i = 0; i < 16; ++i) {
            ASSERT_TRUE(dispatcher.RetireStreams(64));
            ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kRetire, 64, 12345));
        }
    }
};

TEST(SocketDispatcherConfigTest, RejectsInvalidConfigurationAndAllowsCorrectedFirstOpen) {
    FakeBackend backend;
    SocketDispatcher dispatcher;
    auto config = Config();
    config.owner_id = 0;
    auto opened = dispatcher.Open(backend, config);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, SocketDispatchErrorCode::kInvalidArgument);
    config = Config();
    config.record_budget_bytes = 1;
    opened = dispatcher.Open(backend, config);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, SocketDispatchErrorCode::kBudgetExceeded);
    config = Config();
    config.stream_capacity = 0;
    EXPECT_FALSE(dispatcher.Open(backend, config));
    config = Config();
    config.commands.queue_id = 0;
    EXPECT_FALSE(dispatcher.Open(backend, config));
    // 队列 Open 失败必须回滚先建好的 FD 表，允许使用相同配置修正后重新打开。
    ASSERT_TRUE(dispatcher.Open(backend, Config()));
    EXPECT_FALSE(dispatcher.Open(backend, Config()));
    EXPECT_TRUE(dispatcher.Close(true));
    EXPECT_FALSE(dispatcher.Open(backend, Config()));
}

TEST(SocketDispatcherConfigTest, UnopenedOperationsFailWithoutCallingBackend) {
    SocketDispatcher dispatcher;
    FdReader reader;
    FdLease lease;
    EXPECT_FALSE(dispatcher.Attach(Handle(0)));
    EXPECT_FALSE(dispatcher.Detach({7103, 1, 1}));
    EXPECT_FALSE(dispatcher.InspectBinding({7103, 1, 1}));
    EXPECT_FALSE(dispatcher.RegisterReader());
    EXPECT_FALSE(dispatcher.Lookup(reader, 1));
    EXPECT_FALSE(dispatcher.TrySubmit(lease, {}));
    EXPECT_FALSE(dispatcher.TakeCompletion({7104, 1}));
    EXPECT_FALSE(dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 1));
    EXPECT_TRUE(dispatcher.wait_fds().empty());
    EXPECT_EQ(dispatcher.exclusive_context(), nullptr);
    EXPECT_TRUE(dispatcher.Close(true));
}

TEST_F(SocketDispatcherTest, ReservesSeparateNonblockingCloexecFdAndClosesItOnDetach) {
    const int unrelated = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    ASSERT_GE(unrelated, 0);
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    EXPECT_NE(binding->fd, unrelated);
    EXPECT_NE(::fcntl(binding->fd, F_GETFL) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(binding->fd, F_GETFD) & FD_CLOEXEC, 0);
    EXPECT_FALSE(Readable(binding->fd));
    ASSERT_TRUE(dispatcher.Detach(*binding));
    EXPECT_EQ(::fcntl(binding->fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_GE(::fcntl(unrelated, F_GETFD), 0);
    EXPECT_EQ(backend.streams[0].closes, 0u);
    EXPECT_EQ(::close(unrelated), 0);
    Retire();
    EXPECT_TRUE(backend.streams[0].released);
}

TEST_F(SocketDispatcherTest, AttachFailureDoesNotTakeOrCloseTheUnderlyingStream) {
    backend.streams[0].inspect_error = Failure(KBS_STREAM_TRANSPORT, 9);
    const auto failed = dispatcher.Attach(Handle(0));
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, SocketDispatchErrorCode::kStream);
    EXPECT_EQ(failed.error().stream.underlying, 9);
    EXPECT_EQ(dispatcher.stats().bindings.active, 0u);
    EXPECT_EQ(backend.streams[0].closes, 0u);
    EXPECT_EQ(backend.streams[0].releases, 0u);
    backend.streams[0].inspect_error = {};
    ASSERT_TRUE(Attach(0));
    EXPECT_FALSE(dispatcher.Attach(Handle(0)));
    EXPECT_FALSE(dispatcher.Attach(Handle(8)));
    EXPECT_FALSE(dispatcher.Attach(Handle(1, 0)));
    EXPECT_EQ(backend.streams[0].closes, 0u);
}

TEST_F(SocketDispatcherTest, SubmitCopiesWriteInputAndReturnsOnlyTheSingleAttemptPrefix) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    std::array input{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    backend.streams[0].write_limit = 2;
    auto ticket = Submit(*binding, FdCommandKind::kWrite, input, input.size());
    ASSERT_TRUE(ticket);
    input.fill(std::byte{9});
    EXPECT_EQ(backend.streams[0].writes, 0u);
    auto pending = dispatcher.TakeCompletion(*ticket);
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error().code, FdQueueErrorCode::kPending);
    Mailbox();
    auto completion = dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, 2u);
    EXPECT_EQ(completion->binding.id, *binding);
    EXPECT_EQ(completion->command.token, 99u);
    EXPECT_EQ(backend.streams[0].output[0], std::byte{1});
    EXPECT_EQ(backend.streams[0].output[1], std::byte{2});
    // 多次推进只处理新命令，短写未接受尾部由应用决定，不得自动补发。
    Mailbox();
    Mailbox();
    EXPECT_EQ(backend.streams[0].writes, 1u);
    EXPECT_FALSE(dispatcher.TakeCompletion(*ticket));
}

TEST_F(SocketDispatcherTest, ReadStagingPreservesDataUntilSufficientOutputIsProvided) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto& stream = backend.streams[0];
    stream.input[0] = std::byte{7};
    stream.input[1] = std::byte{8};
    stream.input[2] = std::byte{9};
    stream.input_size = 3;
    auto ticket = Submit(*binding, FdCommandKind::kRead, {}, 8);
    ASSERT_TRUE(ticket);
    Mailbox(64, 56789);
    std::array<std::byte, 2> small{};
    auto result = dispatcher.TakeCompletion(*ticket, small);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, FdQueueErrorCode::kOutputTooSmall);
    EXPECT_EQ(dispatcher.stats().commands, 1u);
    std::array<std::byte, 8> output{};
    result = dispatcher.TakeCompletion(*ticket, output);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->io.bytes, 3u);
    EXPECT_EQ(result->result->io.eof, 0u);
    EXPECT_EQ(output[0], std::byte{7});
    EXPECT_EQ(output[2], std::byte{9});
    EXPECT_EQ(stream.reads, 1u);
    EXPECT_GE(stream.now_ns, 56789u);
}

TEST_F(SocketDispatcherTest, WouldBlockEofAndZeroLengthRemainDistinct) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto first = Submit(*binding, FdCommandKind::kRead, {}, 8);
    ASSERT_TRUE(first);
    Mailbox();
    auto result = dispatcher.TakeCompletion(*first);
    ASSERT_TRUE(result);
    ASSERT_FALSE(result->result);
    EXPECT_EQ(result->result.error().code, KBS_STREAM_WOULD_BLOCK);
    backend.streams[0].eof = true;
    auto eof = Submit(*binding, FdCommandKind::kRead, {}, 8);
    ASSERT_TRUE(eof);
    auto empty = Submit(*binding, FdCommandKind::kRead);
    ASSERT_TRUE(empty);
    auto write = Submit(*binding, FdCommandKind::kWrite);
    ASSERT_TRUE(write);
    Mailbox();
    result = dispatcher.TakeCompletion(*eof);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->io.bytes, 0u);
    EXPECT_EQ(result->result->io.eof, 1u);
    result = dispatcher.TakeCompletion(*empty);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->io.eof, 0u);
    result = dispatcher.TakeCompletion(*write);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->io.bytes, 0u);
}

TEST_F(SocketDispatcherTest, CompletionPreservesTransportDiagnosticsWithoutReplay) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    backend.streams[0].write_error = Failure(KBS_STREAM_TRANSPORT, 9);
    const std::array input{std::byte{1}};
    auto ticket = Submit(*binding, FdCommandKind::kWrite, input, input.size());
    ASSERT_TRUE(ticket);
    Mailbox();
    auto result = dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(result);
    ASSERT_FALSE(result->result);
    EXPECT_EQ(result->result.error().code, KBS_STREAM_TRANSPORT);
    EXPECT_EQ(result->result.error().origin, KBS_STREAM_ORIGIN_PROGRESS);
    EXPECT_EQ(result->result.error().native_code, 17);
    EXPECT_EQ(result->result.error().detail, 23);
    EXPECT_EQ(result->result.error().underlying, 9);
    Mailbox();
    EXPECT_EQ(backend.streams[0].writes, 1u);
}

TEST_F(SocketDispatcherTest, MalformedBackendReplyBecomesOneProtocolFailureWithoutReplay) {
    auto malformed = Attach(0);
    auto healthy = Attach(1);
    ASSERT_TRUE(malformed);
    ASSERT_TRUE(healthy);
    backend.streams[0].malformed_write_reply = true;
    const std::array input{std::byte{1}, std::byte{2}};
    auto bad = Submit(*malformed, FdCommandKind::kWrite, input, input.size());
    auto good = Submit(*healthy, FdCommandKind::kWrite, input, input.size());
    ASSERT_TRUE(bad);
    ASSERT_TRUE(good);
    // 后端声称接受超过请求大小，不能卡住 executing 槽或重放已经执行的写操作。
    Mailbox();
    auto result = dispatcher.TakeCompletion(*bad);
    ASSERT_TRUE(result);
    ASSERT_FALSE(result->result);
    EXPECT_EQ(result->result.error().code, KBS_STREAM_PROTOCOL);
    result = dispatcher.TakeCompletion(*good);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->io.bytes, input.size());
    Mailbox();
    EXPECT_EQ(backend.streams[0].writes, 1u);
    EXPECT_EQ(backend.streams[1].writes, 1u);
    EXPECT_EQ(dispatcher.stats().commands, 0u);
}

TEST_F(SocketDispatcherTest, ShutdownAndInspectAreExecutedOnlyByOwner) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto shutdown = Submit(*binding, FdCommandKind::kShutdownWrite);
    ASSERT_TRUE(shutdown);
    auto inspect = Submit(*binding, FdCommandKind::kInspect);
    ASSERT_TRUE(inspect);
    EXPECT_EQ(backend.streams[0].shutdowns, 0u);
    Mailbox();
    auto result = dispatcher.TakeCompletion(*shutdown);
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->result);
    EXPECT_EQ(backend.streams[0].shutdowns, 1u);
    result = dispatcher.TakeCompletion(*inspect);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->status.state, KBS_STREAM_TCP_ACTIVE);
    EXPECT_EQ(result->result->status.tx_pending, 11u);
    EXPECT_EQ(result->result->status.rx_pending, 13u);
}

TEST_F(SocketDispatcherTest, LogicalClosePrecedesPhysicalReleaseAndHeldResultPreventsMetadataReclaim) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto close = Submit(*binding, FdCommandKind::kClose);
    ASSERT_TRUE(close);
    Mailbox();
    EXPECT_FALSE(dispatcher.Lookup(reader, *binding));
    EXPECT_EQ(::fcntl(binding->fd, F_GETFD), -1);
    EXPECT_EQ(backend.streams[0].closes, 0u);
    Retire();
    EXPECT_EQ(backend.streams[0].closes, 1u);
    EXPECT_EQ(backend.streams[0].releases, 1u);
    // 即使真实流已经 Release，未领取的关闭结果仍持有旧 FD 元数据 Lease。
    EXPECT_EQ(dispatcher.stats().bindings.retired, 1u);
    auto status = dispatcher.InspectBinding(*binding);
    ASSERT_TRUE(status);
    EXPECT_EQ(status->stage, SocketRetireStage::kConfirmed);
    auto completion = dispatcher.TakeCompletion(*close);
    ASSERT_TRUE(completion);
    EXPECT_TRUE(completion->result);
    Retire();
    EXPECT_EQ(dispatcher.stats().bindings.retired, 0u);
}

TEST_F(SocketDispatcherTest, HotProgressNeverInvokesPhysicalStreamCloseOrRelease) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto close = Submit(*binding, FdCommandKind::kClose);
    ASSERT_TRUE(close);
    Mailbox();
    auto completion = dispatcher.TakeCompletion(*close);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    // 实际 stream Close/Release 可能释放对象，只能由独立 owner 控制入口推进。
    for (unsigned i = 0; i < 8; ++i) {
        ASSERT_TRUE(dispatcher.Progress(runtime::ProgressKind::kRetire, 64, 12345));
        Mailbox();
    }
    EXPECT_EQ(backend.streams[0].closes, 0u);
    EXPECT_EQ(backend.streams[0].releases, 0u);
    auto status = dispatcher.InspectBinding(*binding);
    ASSERT_TRUE(status);
    EXPECT_EQ(status->stage, SocketRetireStage::kClose);
    Retire();
    EXPECT_EQ(backend.streams[0].closes, 1u);
    EXPECT_EQ(backend.streams[0].releases, 1u);
    EXPECT_EQ(dispatcher.stats().bindings.retired, 0u);
}

TEST_F(SocketDispatcherTest, RetainedCloseAndReleaseDoNotBlockIndependentHealthyTraffic) {
    auto fault = Attach(0);
    auto healthy = Attach(1);
    ASSERT_TRUE(fault);
    ASSERT_TRUE(healthy);
    backend.streams[0].close_error = Failure(KBS_STREAM_RETAINED, 9);
    ASSERT_TRUE(dispatcher.Detach(*fault));
    Retire();
    auto state = dispatcher.InspectBinding(*fault);
    ASSERT_TRUE(state);
    EXPECT_EQ(state->stage, SocketRetireStage::kClose);
    EXPECT_EQ(state->last_error.code, KBS_STREAM_RETAINED);
    EXPECT_EQ(state->last_error.underlying, 9);
    EXPECT_EQ(backend.streams[0].releases, 0u);
    const std::array input{std::byte{11}, std::byte{12}};
    auto ticket = Submit(*healthy, FdCommandKind::kWrite, input, input.size());
    ASSERT_TRUE(ticket);
    Mailbox();
    auto result = dispatcher.TakeCompletion(*ticket);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    EXPECT_EQ(result->result->io.bytes, input.size());
    // Close 收敛不代表来源已退休；Release 的第二道门禁必须独立保持。
    backend.streams[0].close_error = {};
    backend.streams[0].release_error = Failure(KBS_STREAM_RETAINED, 42);
    Retire();
    state = dispatcher.InspectBinding(*fault);
    ASSERT_TRUE(state);
    EXPECT_EQ(state->stage, SocketRetireStage::kRelease);
    EXPECT_EQ(state->last_error.underlying, 42);
    const auto close_calls = backend.streams[0].closes;
    Retire();
    EXPECT_EQ(backend.streams[0].closes, close_calls);
    backend.streams[0].release_error = {};
    Retire();
    EXPECT_EQ(dispatcher.stats().bindings.retired, 0u);
    EXPECT_FALSE(dispatcher.InspectBinding(*fault));
    EXPECT_EQ(backend.streams[1].closes, 0u);
}

TEST_F(SocketDispatcherTest, DelayedOldLeaseFailsClosedAfterPublicFdReuse) {
    auto old = Attach(0);
    ASSERT_TRUE(old);
    auto held = dispatcher.Lookup(reader, *old);
    ASSERT_TRUE(held);
    ASSERT_TRUE(dispatcher.Detach(*old));
    auto fresh = Attach(1);
    ASSERT_TRUE(fresh);
    // 两次 Attach 间无额外 fd 分配，内核应复用刚释放的最小整数；两个完整身份仍不同。
    ASSERT_EQ(fresh->fd, old->fd);
    EXPECT_NE(fresh->generation, old->generation);
    EXPECT_FALSE(held->current());
    const std::array input{std::byte{1}};
    auto late = dispatcher.TrySubmit(*held, {.kind = FdCommandKind::kWrite, .bytes = 1}, input);
    ASSERT_TRUE(late);
    tickets[ticket_count++] = *late;
    auto valid = Submit(*fresh, FdCommandKind::kWrite, input, input.size());
    ASSERT_TRUE(valid);
    Mailbox();
    auto result = dispatcher.TakeCompletion(*late);
    ASSERT_TRUE(result);
    ASSERT_FALSE(result->result);
    EXPECT_EQ(result->result.error().code, KBS_STREAM_CLOSED);
    EXPECT_EQ(result->binding.id, *old);
    EXPECT_EQ(backend.streams[0].writes, 0u);
    result = dispatcher.TakeCompletion(*valid);
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->result);
    EXPECT_EQ(backend.streams[1].writes, 1u);
    Retire();
    EXPECT_GE(::fcntl(fresh->fd, F_GETFD), 0);
}

TEST_F(SocketDispatcherTest, StreamSlotCannotBeAttachedUntilOldBindingLeaseIsCollected) {
    auto old = Attach(0);
    ASSERT_TRUE(old);
    auto held = dispatcher.Lookup(reader, *old);
    ASSERT_TRUE(held);
    ASSERT_TRUE(dispatcher.Detach(*old));
    Retire();
    ASSERT_TRUE(backend.streams[0].released);
    backend.streams[0] = {};
    backend.streams[0].generation = 2;
    EXPECT_FALSE(dispatcher.Attach(Handle(0, 2)));
    held->Reset();
    Retire();
    auto fresh = Attach(0, 2);
    ASSERT_TRUE(fresh);
    EXPECT_FALSE(dispatcher.Lookup(reader, *old));
    EXPECT_EQ(dispatcher.stats().bindings.active, 1u);
}

TEST_F(SocketDispatcherTest, BudgetPreservesCommandReadinessAndSubmissionOnlySignalsOwner) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    ASSERT_EQ(dispatcher.wait_fds().size(), 1u);
    const int command_fd = dispatcher.wait_fds()[0];
    auto first = Submit(*binding, FdCommandKind::kInspect);
    auto second = Submit(*binding, FdCommandKind::kInspect);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_TRUE(Readable(command_fd));
    auto progress = dispatcher.Progress(runtime::ProgressKind::kMailbox, 1, 1);
    ASSERT_TRUE(progress);
    EXPECT_EQ(progress->processed, 1u);
    EXPECT_TRUE(progress->runnable);
    EXPECT_TRUE(Readable(command_fd));
    EXPECT_TRUE(dispatcher.TakeCompletion(*first));
    auto pending = dispatcher.TakeCompletion(*second);
    ASSERT_FALSE(pending);
    EXPECT_EQ(pending.error().code, FdQueueErrorCode::kPending);
    Mailbox(1);
    EXPECT_TRUE(dispatcher.TakeCompletion(*second));
    // 空的通知/非 mailbox 阶段不得偷偷执行 I/O；Rearm 不直接访问流后端。
    const auto calls = backend.streams[0].inspections;
    EXPECT_TRUE(dispatcher.CollectNotifications(1));
    EXPECT_TRUE(dispatcher.Rearm());
    EXPECT_TRUE(dispatcher.Progress(runtime::ProgressKind::kRx, 1, 1));
    EXPECT_EQ(backend.streams[0].inspections, calls);
}

TEST_F(SocketDispatcherTest, SmallRetireBudgetKeepsRunnableUntilDistantPendingRecordIsVisited) {
    auto binding = Attach(7);
    ASSERT_TRUE(binding);
    ASSERT_TRUE(dispatcher.Detach(*binding));
    // mailbox 先消费唤醒，退休阶段必须靠 runnable 完成剩余有界扫描，不能等下一条业务请求。
    Mailbox();
    bool runnable = true;
    unsigned iterations = 0;
    while (runnable && iterations++ < 64) {
        auto control = dispatcher.RetireStreams(1);
        ASSERT_TRUE(control);
        auto progress = dispatcher.Progress(runtime::ProgressKind::kRetire, 1, 1);
        ASSERT_TRUE(progress);
        runnable = control->runnable || progress->runnable;
    }
    EXPECT_LT(iterations, 64u);
    EXPECT_TRUE(backend.streams[7].released);
    EXPECT_EQ(dispatcher.stats().bindings.retired, 0u);
}

TEST_F(SocketDispatcherTest, HeldRetirementHeadDoesNotHideLaterCollectableBindingAtBudgetOne) {
    auto held_binding = Attach(0);
    auto ready_binding = Attach(1);
    ASSERT_TRUE(held_binding);
    ASSERT_TRUE(ready_binding);
    auto lease = dispatcher.Lookup(reader, *held_binding);
    ASSERT_TRUE(lease);
    ASSERT_TRUE(dispatcher.Detach(*held_binding));
    ASSERT_TRUE(dispatcher.Detach(*ready_binding));
    Mailbox();
    // 第一个节点有 CPU 引用而第二个可以回收；有界扫描需走完已有退休队列后才决定休眠。
    bool runnable = true;
    unsigned iterations = 0;
    while (runnable && iterations++ < 64) {
        auto control = dispatcher.RetireStreams(1);
        ASSERT_TRUE(control);
        auto progress = dispatcher.Progress(runtime::ProgressKind::kRetire, 1, 1);
        ASSERT_TRUE(progress);
        runnable = control->runnable || progress->runnable;
    }
    EXPECT_LT(iterations, 64u);
    EXPECT_TRUE(backend.streams[0].released);
    EXPECT_TRUE(backend.streams[1].released);
    EXPECT_EQ(dispatcher.stats().bindings.retired, 1u);
    EXPECT_TRUE(dispatcher.InspectBinding(*held_binding));
    EXPECT_FALSE(dispatcher.InspectBinding(*ready_binding));
    lease->Reset();
    Retire();
    EXPECT_EQ(dispatcher.stats().bindings.retired, 0u);
}

TEST_F(SocketDispatcherTest, RetainedPhysicalCloseDoesNotForceAnUnboundedRunnableLoop) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    backend.streams[0].close_error = Failure(KBS_STREAM_RETAINED);
    ASSERT_TRUE(dispatcher.Detach(*binding));
    Mailbox();
    bool runnable = true;
    unsigned iterations = 0;
    while (runnable && iterations++ < 64) {
        auto control = dispatcher.RetireStreams(1);
        ASSERT_TRUE(control);
        auto progress = dispatcher.Progress(runtime::ProgressKind::kRetire, 1, 1);
        ASSERT_TRUE(progress);
        runnable = control->runnable || progress->runnable;
    }
    // 没有新完成或独立终结证据时允许宿主休眠，不应因一直存在 Retained 节点而忙转。
    EXPECT_LT(iterations, 64u);
    EXPECT_GT(backend.streams[0].closes, 0u);
    EXPECT_EQ(backend.streams[0].releases, 0u);
    EXPECT_EQ(dispatcher.stats().bindings.retired, 1u);
}

TEST_F(SocketDispatcherTest, QueueCapacityIncludesUnclaimedResultsAndSubmissionFailureKeepsLease) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    for (unsigned i = 0; i < 8; ++i) {
        ASSERT_TRUE(Submit(*binding, FdCommandKind::kInspect));
    }
    Mailbox();
    auto held = dispatcher.Lookup(reader, *binding);
    ASSERT_TRUE(held);
    auto full = dispatcher.TrySubmit(*held, {});
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, SocketDispatchErrorCode::kQueue);
    EXPECT_EQ(full.error().queue.code, FdQueueErrorCode::kFull);
    EXPECT_TRUE(*held);
    EXPECT_EQ(dispatcher.stats().commands, 8u);
    EXPECT_TRUE(dispatcher.TakeCompletion(tickets[0]));
    auto next = dispatcher.TrySubmit(*held, {});
    ASSERT_TRUE(next);
    EXPECT_FALSE(*held);
    tickets[ticket_count++] = *next;
}

TEST_F(SocketDispatcherTest, RejectsForeignBindingEvenWhenStreamIdentityMatches) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    FdTable foreign;
    auto config = Config().bindings;
    config.table_id = 7200;
    ASSERT_TRUE(foreign.Open(config));
    auto reader2 = foreign.RegisterReader();
    ASSERT_TRUE(reader2);
    auto id = foreign.Publish(3, {Config().owner_id, Handle(0)});
    ASSERT_TRUE(id);
    auto held = foreign.Lookup(*reader2, *id);
    ASSERT_TRUE(held);
    auto result = dispatcher.TrySubmit(*held, {});
    EXPECT_FALSE(result);
    EXPECT_TRUE(*held);
    EXPECT_EQ(dispatcher.stats().commands, 0u);
    held->Reset();
    auto retired = foreign.Unbind(*id);
    ASSERT_TRUE(retired);
    ASSERT_TRUE(foreign.ConfirmClosed(*retired));
    std::array<FdBinding, 1> output{};
    ASSERT_TRUE(foreign.Collect(output, 1));
    reader2->Reset();
    EXPECT_TRUE(foreign.Close(true));
}

TEST_F(SocketDispatcherTest, StopRejectsNewAdmissionButAllowsAcceptedWorkAndOwnerCleanup) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    auto ticket = Submit(*binding, FdCommandKind::kInspect);
    ASSERT_TRUE(ticket);
    auto held = dispatcher.Lookup(reader, *binding);
    ASSERT_TRUE(held);
    dispatcher.StopAccepting();
    EXPECT_FALSE(dispatcher.Attach(Handle(1)));
    EXPECT_FALSE(dispatcher.RegisterReader());
    EXPECT_FALSE(dispatcher.Lookup(reader, *binding));
    EXPECT_FALSE(dispatcher.TrySubmit(*held, {}));
    EXPECT_TRUE(*held);
    Mailbox();
    EXPECT_TRUE(dispatcher.TakeCompletion(*ticket));
    ASSERT_TRUE(dispatcher.Detach(*binding));
    Retire();
    EXPECT_EQ(dispatcher.stats().bindings.retired, 1u);
    held->Reset();
    Retire();
    EXPECT_EQ(dispatcher.stats().bindings.retired, 0u);
}

TEST_F(SocketDispatcherTest, CloseRequiresExplicitQuiescenceAndReturnedReaders) {
    auto closed = dispatcher.Close();
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, SocketDispatchErrorCode::kNotQuiescent);
    closed = dispatcher.Close(true);
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, SocketDispatchErrorCode::kRetained);
    reader.Reset();
    EXPECT_TRUE(dispatcher.Close(true));
}

TEST(SocketDispatcherConfigTest, ContextIdentityRemainsAvailableForPauseGrouping) {
    FakeBackend backend;
    std::uint64_t context = 0;
    backend.context = &context;
    SocketDispatcher dispatcher;
    ASSERT_TRUE(dispatcher.Open(backend, Config()));
    EXPECT_EQ(dispatcher.exclusive_context(), &context);
    EXPECT_TRUE(dispatcher.Close(true));
}

TEST(SocketDispatcherConfigTest, AllConfiguredReaderCapacityIsAvailableToApplication) {
    FakeBackend backend;
    SocketDispatcher dispatcher;
    auto config = Config();
    config.bindings.reader_capacity = 1;
    ASSERT_TRUE(dispatcher.Open(backend, config));
    auto reader = dispatcher.RegisterReader();
    ASSERT_TRUE(reader);
    auto extra = dispatcher.RegisterReader();
    ASSERT_FALSE(extra);
    EXPECT_EQ(extra.error(), FdError::kReaderFull);
    reader->Reset();
    EXPECT_TRUE(dispatcher.Close(true));
}

TEST_F(SocketDispatcherTest, ProducerOnlySubmitsWhileOwnerPerformsBackendCalls) {
    auto binding = Attach(0);
    ASSERT_TRUE(binding);
    std::optional<FdCommandTicket> ticket;
    const std::array input{std::byte{20}, std::byte{21}};
    // 工作线程的唯一跨层行为是 Hazard 查询和复制入队，不执行后端方法。
    std::jthread producer([&] {
        auto registered = dispatcher.RegisterReader();
        if (!registered) {
            return;
        }
        auto lease = dispatcher.Lookup(*registered, *binding);
        if (!lease) {
            return;
        }
        auto submitted = dispatcher.TrySubmit(*lease, {.kind = FdCommandKind::kWrite, .bytes = 2}, input);
        if (submitted) {
            ticket = *submitted;
        }
    });
    producer.join();
    ASSERT_TRUE(ticket);
    tickets[ticket_count++] = *ticket;
    EXPECT_EQ(backend.streams[0].writes, 0u);
    Mailbox();
    std::optional<FdCommandCompletion> completion;
    std::jthread collector([&] {
        auto result = dispatcher.TakeCompletion(*ticket);
        if (result) {
            completion = *result;
        }
    });
    collector.join();
    ASSERT_TRUE(completion);
    ASSERT_TRUE(completion->result);
    EXPECT_EQ(completion->result->io.bytes, 2u);
    EXPECT_EQ(backend.streams[0].writes, 1u);
    EXPECT_FALSE(backend.wrong_thread);
}

} // namespace
} // namespace socket
} // namespace kbsocket
