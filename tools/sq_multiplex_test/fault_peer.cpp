// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/sq_multiplex_test/fault_peer.hpp"

#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>
#include <thread>
#include <utility>

#include "tools/peer_failure_test/process.hpp"

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kBaselineReceived = 0x4d584252;
struct ChildHardware {
    raw::RawRuntime runtime;
    raw::JettyPool pool;
    raw::RxBufferPool rx;
};
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
bool SameEndpoint(const urma_jetty_id_t& lhs, const urma_jetty_id_t& rhs) noexcept {
    return lhs.id == rhs.id && std::memcmp(lhs.eid.raw, rhs.eid.raw, sizeof(lhs.eid.raw)) == 0;
}
std::expected<void, ToolError> RunEndpoint(int fd, const char* library, const char* device, std::uint32_t eid_index,
                                           const SqMultiplexOptions& options) {
    // 故障端点只能由 SIGKILL 回收，失败路径也不在未决 DMA 上执行析构。
    static NoDestructor<ChildHardware> hardware;
    ControlChannel channel(fd);
    auto loaded = raw::UrmaApi::Load(library);
    if (!loaded) {
        return Error("load disposable endpoint URMA", ENOENT);
    }
    auto initialized = hardware->runtime.Initialize({{device, eid_index}});
    if (!initialized) {
        return Error("initialize disposable endpoint", initialized.error().provider_error);
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    auto status = raw::UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (status != URMA_SUCCESS) {
        return Error("query disposable endpoint device", status);
    }
    raw::JettyPoolConfig config;
    config.jetty_count = 1;
    config.tx_depth = config.rx_depth = config.rx_cq_depth = 1;
    config.tx_cq_depth = 2;
    auto opened = hardware->pool.Open(ctx, attributes.dev_cap, config);
    if (!opened) {
        return Error("open disposable endpoint pool", opened.error().failure.provider_error);
    }
    auto ticket = hardware->pool.Reserve();
    if (!ticket) {
        return Error("reserve disposable endpoint", EIO);
    }
    auto jetty = hardware->pool.Get(ticket->lane);
    auto cancelled = hardware->pool.Cancel(*ticket);
    if (!jetty || !cancelled) {
        return Error("read disposable endpoint", EIO);
    }
    const auto endpoint = (*jetty)->jetty_id;
    auto rx = hardware->rx.Open(hardware->pool);
    if (!rx) {
        return Error("open disposable baseline RX", rx.error().provider_error);
    }
    auto posted = hardware->rx.Refill(1);
    if (!posted || *posted != 1) {
        return Error("post disposable baseline RX", posted ? EIO : posted.error().provider_error);
    }
    SqMultiplexHello hello{.options = options};
    hello.endpoints[0] = endpoint;
    auto io = channel.Write(EncodeMultiplexHello(hello), options.timeout_ms);
    if (!io) {
        return io;
    }
    MultiplexHelloBytes incoming{};
    // 父进程尚需初始化健康端点、建立 TCP 和导入 endpoint；不把这段启动等待计入 RX 超时。
    io = channel.Read(incoming, 8 * options.timeout_ms);
    if (!io) {
        return io;
    }
    auto sender = DecodeMultiplexHello(incoming);
    if (!sender || sender->server || sender->options.fault != MultiplexFault::kPeerExit) {
        return Error("invalid disposable endpoint baseline command", EPROTO);
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(options.timeout_ms);
    std::array<raw::RxBufferEvent, raw::RxBufferPool::kPollBatch> events{};
    while (std::chrono::steady_clock::now() < deadline) {
        auto polled = hardware->rx.Poll(events);
        if (!polled) {
            return Error("poll disposable baseline RX", polled.error().provider_error);
        }
        if (*polled != 0) {
            if (*polled != 1) {
                return Error("duplicate disposable baseline RX", EPROTO);
            }
            auto checked = CheckMultiplexBaseline(events[0], endpoint, sender->endpoints[0], options.bytes);
            if (!checked) {
                return checked;
            }
            auto released = hardware->rx.Release(*events[0].lease);
            if (!released) {
                return Error("release disposable baseline RX", static_cast<int>(released.error().code));
            }
            io = channel.SendMarker(kBaselineReceived, options.timeout_ms);
            if (!io) {
                return io;
            }
            // 已校验 baseline，保留 endpoint 直到监督者确认 SIGKILL/waitpid。
            while (true) {
                ::pause();
            }
        }
        std::this_thread::yield();
    }
    return Error("disposable baseline RX timeout", ETIMEDOUT);
}
} // namespace

std::expected<void, ToolError> CheckMultiplexBaseline(const raw::RxBufferEvent& event, const urma_jetty_id_t& receiver,
                                                      const urma_jetty_id_t& sender, std::uint32_t bytes) noexcept {
    const auto& cr = event.completion;
    if (event.error || !event.lease || event.payload.size() != bytes || cr.status != URMA_CR_SUCCESS ||
        cr.opcode != URMA_CR_OPC_SEND_WITH_IMM || !cr.flag.bs.jetty || !cr.flag.bs.s_r || cr.local_id != receiver.id ||
        !SameEndpoint(cr.remote_id, sender)) {
        return Error("invalid disposable baseline RX", EPROTO);
    }
    auto frame = CheckMultiplexPayload(event.payload);
    if (!frame || frame->phase != MultiplexPhase::kBaseline || cr.imm_data != MultiplexImmediate(*frame)) {
        return Error("invalid disposable baseline payload/IMM", EPROTO);
    }
    return {};
}

MultiplexPeerProcess::~MultiplexPeerProcess() {
    if (pid_ > 0) {
        auto result = Stop(false);
        if (!result) {
            std::println(stderr, "FAIL: dispose fault child: {}: {}", result.error().operation, result.error().code);
        }
    }
}

std::expected<void, ToolError> MultiplexPeerProcess::Open(const char* library, const char* device,
                                                          std::uint32_t eid_index, const SqMultiplexOptions& options) {
    if (pid_ > 0 || channel_ || options.fault != MultiplexFault::kPeerExit || !ValidateMultiplexOptions(options)) {
        return Error("invalid disposable endpoint options/lifecycle", EINVAL);
    }
    options_ = options;
    int sockets[2]{};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) {
        return Error("socketpair disposable endpoint", errno);
    }
    const auto parent = ::getpid();
    std::fflush(nullptr);
    const auto child = ::fork();
    if (child < 0) {
        const int saved = errno;
        ::close(sockets[0]);
        ::close(sockets[1]);
        return Error("fork disposable endpoint", saved);
    }
    if (child == 0) {
        ::close(sockets[0]);
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) {
            std::_Exit(1);
        }
        int code = 1;
        try {
            auto result = RunEndpoint(sockets[1], library, device, eid_index, options);
            if (!result) {
                std::println(stderr, "FAIL: disposable endpoint: {}: {}", result.error().operation,
                             result.error().code);
            }
            code = result ? 0 : 1;
        } catch (...) {
            std::println(stderr, "FAIL: disposable endpoint exception");
        }
        std::fflush(nullptr);
        std::_Exit(code);
    }
    ::close(sockets[1]);
    pid_ = child;
    channel_ = std::make_unique<ControlChannel>(sockets[0]);
    MultiplexHelloBytes wire{};
    auto io = channel_->Read(wire, options.timeout_ms);
    if (!io) {
        return io;
    }
    auto hello = DecodeMultiplexHello(wire);
    if (!hello || hello->server || hello->options.fault != options.fault || hello->options.bytes != options.bytes ||
        hello->options.batch != options.batch || hello->options.rounds != options.rounds) {
        return Error("invalid disposable endpoint hello", EPROTO);
    }
    endpoint_ = hello->endpoints[0];
    std::println("Disposable fault endpoint: pid={} jetty_id={}; baseline RX posted=1", pid_, endpoint_.id);
    return {};
}

std::expected<void, ToolError> MultiplexPeerProcess::Stop(bool require_sigkill) {
    // waitpid 可能已回收而语义校验失败；无论结果如何都不再向这个旧 PID 发信号。
    const auto owned = std::exchange(pid_, -1);
    return KillAndReapPeer(owned, options_.timeout_ms, require_sigkill);
}

std::expected<void, ToolError> MultiplexPeerProcess::FinishBaselineAndExit(const urma_jetty_id_t& sender) {
    if (pid_ <= 0 || !channel_) {
        return Error("disposable endpoint not alive", EINVAL);
    }
    SqMultiplexHello hello{.options = options_};
    hello.endpoints[0] = sender;
    auto io = channel_->Write(EncodeMultiplexHello(hello), options_.timeout_ms);
    if (io) {
        io = channel_->ExpectMarker(kBaselineReceived, options_.timeout_ms);
    }
    if (!io) {
        return io;
    }
    const auto child = pid_;
    auto stopped = Stop(true);
    if (!stopped) {
        return stopped;
    }
    std::println("OBSERVED: fault baseline payload validated; SIGKILL/waitpid confirmed; pid={} jetty_id={}", child,
                 endpoint_.id);
    return {};
}
} // namespace tools
} // namespace kbsocket
