// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/import_test.hpp"

#include <algorithm>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using raw::UrmaApi;
auto Error(const char* operation, int code = errno) noexcept {
    return std::unexpected(ToolError{operation, code});
}
using Blob = std::unique_ptr<void, decltype(&std::free)>;
constexpr std::uint32_t kHello = 0x494d5002;
constexpr std::uint32_t kDone = 0x444f4e45;
constexpr std::uint32_t kAck = 0x41434b31;

// rjetty 扩展由 provider 定义，按 URMA perftest 的做法完整传输；仅支持相同 ABI 的可信双端。
std::expected<void, ToolError> ExchangeHeader(ControlChannel& channel, std::uint32_t timeout, bool control_plane) {
    const std::array<std::uint32_t, 6> fields{kHello,
                                              kImportCount,
                                              sizeof(urma_rjetty_t),
                                              sizeof(void*),
                                              std::endian::native == std::endian::little ? 1u : 2u,
                                              control_plane ? 1u : 0u};
    for (auto value : fields) {
        if (auto result = channel.SendMarker(value, timeout); !result) {
            return result;
        }
    }
    for (auto value : fields) {
        if (auto result = channel.ExpectMarker(value, timeout); !result) {
            return result;
        }
    }
    return {};
}
} // namespace

std::expected<void, ToolError> ValidateDescriptor(std::span<const std::byte> bytes) noexcept {
    if (bytes.size() < sizeof(urma_rjetty_t) || bytes.size() > kMaxDescriptorBytes) {
        return Error("invalid rjetty size", EPROTO);
    }
    urma_rjetty_t r{};
    std::memcpy(&r, bytes.data(), sizeof(r));
    if (r.trans_mode != URMA_TM_RM || r.tp_type != URMA_CTP || r.type != URMA_JETTY || r.flag.bs.has_drv_ext ||
        r.flag.bs.token_policy != URMA_TOKEN_NONE || r.flag.bs.share_tp) {
        return Error("invalid RM_CTP descriptor", EPROTO);
    }
    if (!r.flag.bs.has_user_info) {
        if (bytes.size() != sizeof(r)) {
            return Error("unexpected rjetty extension", EPROTO);
        }
    } else {
        // 公共扩展头以 uint32_t 长度开头；确保 provider 不会读过接收到的缓冲区。
        std::uint32_t length = 0;
        if (bytes.size() < sizeof(r) + sizeof(length)) {
            return Error("truncated rjetty extension", EPROTO);
        }
        std::memcpy(&length, bytes.data() + sizeof(r), sizeof(length));
        if (length != bytes.size() - sizeof(r) - sizeof(length)) {
            return Error("invalid rjetty extension length", EPROTO);
        }
    }
    return {};
}

std::expected<unsigned, ToolError> SelectCtpPriority(const urma_device_cap_t& cap, int requested) noexcept {
    if (!(cap.trans_mode & URMA_TM_RM) || !cap.rm_tp_cap.bs.ctp || requested < -1 || requested > URMA_MAX_PRIORITY) {
        return Error("device/config does not support RM_CTP", EINVAL);
    }
    for (unsigned i = 0; i <= URMA_MAX_PRIORITY; ++i) {
        if (cap.priority_info[i].tp_type.bs.ctp && (requested == -1 || i == static_cast<unsigned>(requested))) {
            return i;
        }
    }
    return Error("no advertised CTP priority; check device configuration", EINVAL);
}

std::expected<void, ToolError> ImportSession::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                   unsigned priority) {
    if (ctx_ || !ctx || priority > URMA_MAX_PRIORITY || cap.max_jetty < kImportCount || cap.max_jfc_depth < 64 ||
        cap.max_jfs_depth < 32 || cap.max_jfr_depth < 32 || cap.max_jfs_sge < 1 || cap.max_jfs_rsge < 1 ||
        cap.max_jfr_sge < 1) {
        return Error("invalid session/capacity", EINVAL);
    }
    ctx_ = ctx;
    urma_jfc_cfg_t cq{};
    cq.depth = 64;
    send_cq_ = MeasureApi(timings_, ControlApi::CreateJfc, [&] { return UrmaApi::CreateJfc(ctx, &cq); });
    if (!send_cq_) {
        return Error("create send JFC");
    }
    recv_cq_ = MeasureApi(timings_, ControlApi::CreateJfc, [&] { return UrmaApi::CreateJfc(ctx, &cq); });
    if (!recv_cq_) {
        return Error("create receive JFC");
    }
    urma_jfr_cfg_t recv{};
    recv.depth = 32;
    recv.trans_mode = URMA_TM_RM;
    recv.max_sge = 1;
    recv.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    recv.jfc = recv_cq_;
    recv.flag.bs.token_policy = URMA_TOKEN_NONE;
    jfr_ = MeasureApi(timings_, ControlApi::CreateJfr, [&] { return UrmaApi::CreateJfr(ctx, &recv); });
    if (!jfr_) {
        return Error("create JFR");
    }
    for (auto& jetty : jettys_) {
        urma_jetty_cfg_t cfg{};
        cfg.flag.bs.share_jfr = 1;
        cfg.jfs_cfg.depth = 32;
        cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        cfg.jfs_cfg.priority = static_cast<std::uint8_t>(priority);
        cfg.jfs_cfg.max_sge = 1;
        cfg.jfs_cfg.max_rsge = 1;
        cfg.jfs_cfg.rnr_retry = 6;
        cfg.jfs_cfg.err_timeout = 2;
        cfg.jfs_cfg.jfc = send_cq_;
        cfg.shared.jfr = jfr_;
        cfg.shared.jfc = recv_cq_;
        jetty = MeasureApi(timings_, ControlApi::CreateJetty, [&] { return UrmaApi::CreateJetty(ctx, &cfg); });
        if (!jetty) {
            return Error("create jetty");
        }
    }
    return {};
}

std::expected<void, ToolError> ImportSession::Run(ControlChannel& channel, bool server, std::uint32_t timeout) {
    if (!jettys_.back() || ran_) {
        return Error("session incomplete or already measured", EINVAL);
    }
    ran_ = true;
    if (auto result = ExchangeHeader(channel, timeout, timings_ != nullptr); !result) {
        return result;
    }
    if (server) {
        for (auto* jetty : jettys_) {
            urma_rjetty_t* descriptor = nullptr;
            std::uint32_t length = 0;
            const auto rc = MeasureApi(timings_, ControlApi::GetRjetty,
                                       [&] { return UrmaApi::GetRjetty(jetty, &descriptor, &length); });
            if (rc != URMA_SUCCESS || !descriptor) {
                return Error("get rjetty", rc);
            }
            ScopeExit release([&]() noexcept {
                MeasureApi(timings_, ControlApi::PutRjetty, [&] { return UrmaApi::PutRjetty(descriptor); });
            });
            // GetRjetty 不一定填 tp_type，显式设置 CTP，保留全部 bonding 扩展。
            descriptor->tp_type = URMA_CTP;
            auto bytes = std::span(reinterpret_cast<const std::byte*>(descriptor), length);
            if (auto result = ValidateDescriptor(bytes); !result) {
                return result;
            }
            if (auto result = channel.SendMarker(length, timeout); !result) {
                return result;
            }
            if (auto result = channel.Write(bytes, timeout); !result) {
                return result;
            }
        }
        // 客户端必须先反导入，服务端才允许释放被引用的本地 jetty。
        if (auto result = channel.ExpectMarker(kDone, timeout); !result) {
            return result;
        }
        return channel.SendMarker(kAck, timeout);
    }

    std::vector<Blob> descriptors;
    descriptors.reserve(kImportCount);
    for (std::size_t i = 0; i < kImportCount; ++i) {
        std::array<std::byte, 4> wire{};
        if (auto result = channel.Read(wire, timeout); !result) {
            return result;
        }
        std::uint32_t length = 0;
        for (auto byte : wire) {
            length = (length << 8) | std::to_integer<unsigned>(byte);
        }
        if (length < sizeof(urma_rjetty_t) || length > kMaxDescriptorBytes) {
            return Error("invalid descriptor frame size", EPROTO);
        }
        Blob blob(std::calloc(1, length), &std::free);
        if (!blob) {
            return Error("allocate descriptor", ENOMEM);
        }
        auto bytes = std::span(static_cast<std::byte*>(blob.get()), length);
        if (auto result = channel.Read(bytes, timeout); !result) {
            return result;
        }
        if (auto result = ValidateDescriptor(bytes); !result) {
            return result;
        }
        descriptors.push_back(std::move(blob));
    }

    // 所有网络交换、分配、预触页在计时前完成；不预热 import，不在循环中输出。
    samples_.fill(0);
    urma_token_t token{};
    using Clock = std::chrono::steady_clock;
    // 只预热时钟入口，避免动态链接/时钟首次访问混入第一个 import；不预热 URMA。
    (void)Clock::now();
    for (std::size_t i = 0; i < kImportCount; ++i) {
        auto* remote = static_cast<urma_rjetty_t*>(descriptors[i].get());
        errno = 0;
        const auto begin = Clock::now();
        auto* target = UrmaApi::ImportJetty(ctx_, remote, &token);
        const auto end = Clock::now();
        const int error = errno;
        targets_[i] = target;
        samples_[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        if (timings_) {
            timings_->Record(ControlApi::ImportJetty, samples_[i], target != nullptr, target ? 0 : error);
        }
        if (!target) {
            return Error("import jetty (see completed count)", error);
        }
        ++completed_;
    }
    if (auto result = UnimportAll(); !result) {
        return result;
    }
    if (auto result = channel.SendMarker(kDone, timeout); !result) {
        return result;
    }
    return channel.ExpectMarker(kAck, timeout);
}

std::expected<void, ToolError> ImportSession::UnimportAll() noexcept {
    for (auto it = targets_.rbegin(); it != targets_.rend(); ++it) {
        if (*it) {
            const auto rc =
                MeasureApi(timings_, ControlApi::UnimportJetty, [&] { return UrmaApi::UnimportJetty(*it); });
            if (rc != URMA_SUCCESS) {
                return Error("unimport jetty", rc);
            }
            *it = nullptr;
        }
    }
    return {};
}

std::expected<void, ToolError> ImportSession::Close() noexcept {
    if (auto result = UnimportAll(); !result) {
        return result;
    }
    for (auto it = jettys_.rbegin(); it != jettys_.rend(); ++it) {
        if (*it) {
            const auto rc = MeasureApi(timings_, ControlApi::DeleteJetty, [&] { return UrmaApi::DeleteJetty(*it); });
            if (rc != URMA_SUCCESS) {
                return Error("delete jetty", rc);
            }
            *it = nullptr;
        }
    }
    if (jfr_) {
        const auto rc = MeasureApi(timings_, ControlApi::DeleteJfr, [&] { return UrmaApi::DeleteJfr(jfr_); });
        if (rc != URMA_SUCCESS) {
            return Error("delete JFR", rc);
        }
        jfr_ = nullptr;
    }
    for (auto** cq : {&recv_cq_, &send_cq_}) {
        if (*cq) {
            const auto rc = MeasureApi(timings_, ControlApi::DeleteJfc, [&] { return UrmaApi::DeleteJfc(*cq); });
            if (rc != URMA_SUCCESS) {
                return Error("delete JFC", rc);
            }
            *cq = nullptr;
        }
    }
    ctx_ = nullptr;
    return {};
}
} // namespace tools
} // namespace kbsocket
