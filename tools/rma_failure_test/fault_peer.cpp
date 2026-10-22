// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/rma_failure_test/fault_peer.hpp"

#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <print>
#include <utility>

#include "tools/peer_failure_test/process.hpp"

#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/raw_runtime.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace tools {
namespace {
constexpr std::uint32_t kVerifyBaseline = 0x52464256;
constexpr std::uint32_t kBaselineVerified = 0x52464241;
struct ChildHardware {
    raw::RawRuntime runtime;
    RmaFailureResources resources;
};
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}

std::expected<void, ToolError> RunEndpoint(int fd, const char* library, const char* device, std::uint32_t eid_index,
                                           const RmaFailureOptions& options) {
    // 父进程可能尚未确认远端 DMA 终结，故障子进程的所有路径均由进程退出回收。
    static NoDestructor<ChildHardware> hardware;
    ControlChannel channel(fd);
    if (auto loaded = raw::UrmaApi::Load(library); !loaded) {
        return Error("load disposable READ source URMA", ENOENT);
    }
    auto initialized = hardware->runtime.Initialize({{device, eid_index}});
    if (!initialized) {
        return Error("initialize disposable READ source", initialized.error().provider_error);
    }
    auto* ctx = hardware->runtime.context(0);
    urma_device_attr_t attributes{};
    const auto status = raw::UrmaApi::QueryDevice(ctx->dev, &attributes);
    if (status != URMA_SUCCESS) {
        return Error("query disposable READ source device", status);
    }
    auto opened = hardware->resources.Open(ctx, attributes.dev_cap, options, RmaFailureRole::kFaultSource);
    if (!opened) {
        return opened;
    }
    auto io = channel.Write(EncodeRmaHello(hardware->resources.hello()), options.timeout_ms);
    if (!io) {
        return io;
    }
    // 等待父进程初始化健康源、建立连接及客户端 READ 基线，不将启动时间误判为故障。
    io = channel.ExpectMarker(kVerifyBaseline, (options.rounds + 8) * options.timeout_ms);
    if (!io) {
        return io;
    }
    // 该命令只能在客户端确认基线 READ 的成功 CQE 和目的数据之后发出。
    auto verified = hardware->resources.VerifySource();
    if (!verified) {
        return verified;
    }
    io = channel.SendMarker(kBaselineVerified, options.timeout_ms);
    if (!io) {
        return io;
    }
    while (true) {
        ::pause();
    }
}
} // namespace

RmaFailurePeerProcess::~RmaFailurePeerProcess() {
    if (pid_ > 0) {
        if (auto stopped = Stop(false); !stopped) {
            std::println(stderr, "FAIL: dispose READ source: {}: {}", stopped.error().operation, stopped.error().code);
        }
    }
}

std::expected<void, ToolError> RmaFailurePeerProcess::Open(const char* library, const char* device,
                                                           std::uint32_t eid_index, const RmaFailureOptions& options) {
    if (pid_ > 0 || channel_ || !library || !*library || !device || !*device || !ValidateRmaFailureOptions(options)) {
        return Error("invalid disposable READ source options/lifecycle", EINVAL);
    }
    options_ = options;
    int sockets[2]{};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) {
        return Error("socketpair disposable READ source", errno);
    }
    const auto parent = ::getpid();
    std::fflush(nullptr);
    const auto child = ::fork();
    if (child < 0) {
        const int saved = errno;
        ::close(sockets[0]);
        ::close(sockets[1]);
        return Error("fork disposable READ source", saved);
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
                std::println(stderr, "FAIL: disposable READ source: {}: {}", result.error().operation,
                             result.error().code);
            }
            code = result ? 0 : 1;
        } catch (...) {
            std::println(stderr, "FAIL: disposable READ source exception");
        }
        std::fflush(nullptr);
        std::_Exit(code);
    }
    ::close(sockets[1]);
    pid_ = child;
    channel_ = std::make_unique<ControlChannel>(sockets[0]);
    RmaHelloBytes wire{};
    auto io = channel_->Read(wire, options.timeout_ms);
    if (!io) {
        return io;
    }
    auto decoded = DecodeRmaHello(wire);
    if (!decoded) {
        return std::unexpected(decoded.error());
    }
    auto matched = MatchRmaFailureEndpoint(*decoded, options, true);
    if (!matched) {
        return matched;
    }
    hello_ = *decoded;
    std::println("Disposable READ source: pid={} jetty_id={} bytes_per_read={} slots={}; RX_posted=0", pid_,
                 hello_.endpoint.id, options.bytes, options.batch);
    std::fflush(stdout);
    return {};
}

std::expected<void, ToolError> RmaFailurePeerProcess::Stop(bool require_sigkill) {
    // waitpid 可能已经成功而退出原因不符；不再使用旧 PID 发信号，避免 PID 复用。
    const auto owned = std::exchange(pid_, -1);
    return KillAndReapPeer(owned, options_.timeout_ms, require_sigkill);
}

std::expected<void, ToolError> RmaFailurePeerProcess::FinishBaselineAndExit() {
    if (pid_ <= 0 || !channel_) {
        return Error("disposable READ source not alive", EINVAL);
    }
    auto io = channel_->SendMarker(kVerifyBaseline, options_.timeout_ms);
    if (io) {
        io = channel_->ExpectMarker(kBaselineVerified, options_.timeout_ms);
    }
    if (!io) {
        return io;
    }
    const auto child = pid_;
    if (auto stopped = Stop(true); !stopped) {
        return stopped;
    }
    std::println("OBSERVED: fault source payload/guards unchanged after READ baseline; SIGKILL/waitpid confirmed; "
                 "pid={} jetty_id={}",
                 child, hello_.endpoint.id);
    std::fflush(stdout);
    return {};
}
} // namespace tools
} // namespace kbsocket
