// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/control_plane.hpp"

#include <dlfcn.h>
#include <unistd.h>

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
constexpr std::uint32_t kSegmentsDone = 0x53454744;
} // namespace

std::expected<void, ToolError> ControlPlaneApis::Load(const char* library) noexcept {
    void* handle = dlopen(library, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        return Error("load extra control-plane symbols", ENOENT);
    }
    // 主 UrmaApi 持有同一库直到所有资源关闭，此处仅释放额外的 dlopen 引用。
    ScopeExit release([&]() noexcept { dlclose(handle); });
#define LOAD_OPTIONAL(name) name = reinterpret_cast<decltype(name)>(dlsym(handle, "urma_" #name))
    LOAD_OPTIONAL(modify_jfc);
    LOAD_OPTIONAL(create_jfs);
    LOAD_OPTIONAL(delete_jfs);
    LOAD_OPTIONAL(query_jfs);
    LOAD_OPTIONAL(modify_jfs);
    LOAD_OPTIONAL(query_jfr);
    LOAD_OPTIONAL(query_jetty);
    LOAD_OPTIONAL(create_jetty_grp);
    LOAD_OPTIONAL(delete_jetty_grp);
    LOAD_OPTIONAL(alloc_token_id);
    LOAD_OPTIONAL(free_token_id);
#undef LOAD_OPTIONAL
    return {};
}

std::expected<void, ToolError> ControlPlaneSession::RunLocal(urma_context_t* ctx, unsigned priority,
                                                             std::span<urma_jetty_t* const> jettys) {
    if (ctx_ || !ctx || !ctx->dev) {
        return Error("invalid control-plane context", EINVAL);
    }
    ctx_ = ctx;
    // Query/modify 不改变传输模式；jetty/JFR 只设置接收阈值，不触发 ERROR/flush。
    for (auto* jetty : jettys) {
        if (apis_.query_jetty) {
            urma_jetty_cfg_t cfg{};
            urma_jetty_attr_t attr{};
            timings_.Call(ControlApi::QueryJetty, [&] { return apis_.query_jetty(jetty, &cfg, &attr); });
        } else {
            timings_.Skip(ControlApi::QueryJetty, "symbol missing");
        }
        urma_jetty_attr_t attr{};
        attr.mask = JETTY_RX_THRESHOLD;
        attr.rx_threshold = 1;
        timings_.Call(ControlApi::ModifyJetty, [&] { return UrmaApi::ModifyJetty(jetty, &attr); });
    }

    for (std::size_t i = 0; i < kImportCount; ++i) {
        urma_device_attr_t attr{};
        timings_.Call(ControlApi::QueryDevice, [&] { return UrmaApi::QueryDevice(ctx->dev, &attr); });
        std::uint32_t count = 0;
        auto* eids = timings_.Call(ControlApi::GetEidList, [&] { return UrmaApi::GetEidList(ctx->dev, &count); });
        if (eids) {
            timings_.Call(ControlApi::FreeEidList, [&] { UrmaApi::FreeEidList(eids); });
        }
        temp_ctx_ =
            timings_.Call(ControlApi::CreateContext, [&] { return UrmaApi::CreateContext(ctx->dev, ctx->eid_index); });
        if (!temp_ctx_) {
            return Error("create temporary context");
        }
        // 临时 context 无子对象；独立测量创建/删除，队列测试仍使用稳定主 context。
        const auto context_rc =
            timings_.Call(ControlApi::DeleteContext, [&] { return UrmaApi::DeleteContext(temp_ctx_); });
        if (context_rc != URMA_SUCCESS) {
            return Error("delete temporary context", context_rc);
        }
        temp_ctx_ = nullptr;

        jfce_ = timings_.Call(ControlApi::CreateJfce, [&] { return UrmaApi::CreateJfce(ctx); });
        if (!jfce_) {
            return Error("create JFCE");
        }
        urma_jfc_cfg_t cq{};
        cq.depth = 64;
        cq.jfce = jfce_;
        jfc_ = timings_.Call(ControlApi::CreateJfc, [&] { return UrmaApi::CreateJfc(ctx, &cq); });
        if (!jfc_) {
            return Error("create benchmark JFC");
        }
        if (apis_.modify_jfc) {
            urma_jfc_attr_t attr{};
            attr.mask = JFC_MODERATE_COUNT | JFC_MODERATE_PERIOD;
            attr.moderate_count = 1;
            attr.moderate_period = 0;
            timings_.Call(ControlApi::ModifyJfc, [&] { return apis_.modify_jfc(jfc_, &attr); });
        } else {
            timings_.Skip(ControlApi::ModifyJfc, "symbol missing");
        }
        urma_jfr_cfg_t recv{};
        recv.depth = 32;
        recv.trans_mode = URMA_TM_RM;
        recv.max_sge = 1;
        recv.jfc = jfc_;
        recv.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_ = timings_.Call(ControlApi::CreateJfr, [&] { return UrmaApi::CreateJfr(ctx, &recv); });
        if (!jfr_) {
            return Error("create benchmark JFR");
        }
        if (apis_.query_jfr) {
            urma_jfr_cfg_t queried{};
            urma_jfr_attr_t queried_attr{};
            timings_.Call(ControlApi::QueryJfr, [&] { return apis_.query_jfr(jfr_, &queried, &queried_attr); });
        } else {
            timings_.Skip(ControlApi::QueryJfr, "symbol missing");
        }
        urma_jfr_attr_t recv_attr{};
        recv_attr.mask = JFR_RX_THRESHOLD;
        recv_attr.rx_threshold = 1;
        timings_.Call(ControlApi::ModifyJfr, [&] { return UrmaApi::ModifyJfr(jfr_, &recv_attr); });

        if (apis_.create_jfs && apis_.delete_jfs) {
            urma_jfs_cfg_t send{};
            send.depth = 32;
            send.trans_mode = URMA_TM_RM;
            send.priority = static_cast<std::uint8_t>(priority);
            send.max_sge = send.max_rsge = 1;
            send.jfc = jfc_;
            send.rnr_retry = 6;
            send.err_timeout = 2;
            jfs_ = timings_.Call(ControlApi::CreateJfs, [&] { return apis_.create_jfs(ctx, &send); });
            if (jfs_ && apis_.query_jfs) {
                urma_jfs_cfg_t queried{};
                urma_jfs_attr_t queried_attr{};
                const auto rc =
                    timings_.Call(ControlApi::QueryJfs, [&] { return apis_.query_jfs(jfs_, &queried, &queried_attr); });
                if (rc == URMA_SUCCESS && apis_.modify_jfs) {
                    // 按查询出的当前状态进行同状态 modify，不制造故障事件来污染其他计时。
                    queried_attr.mask = JFS_STATE;
                    timings_.Call(ControlApi::ModifyJfs, [&] { return apis_.modify_jfs(jfs_, &queried_attr); });
                } else {
                    timings_.Skip(ControlApi::ModifyJfs, "query failed or modify symbol missing");
                }
            } else {
                timings_.Skip(ControlApi::QueryJfs, "create failed or query symbol missing");
                timings_.Skip(ControlApi::ModifyJfs, "query unavailable");
            }
        } else {
            for (auto api :
                 {ControlApi::CreateJfs, ControlApi::DeleteJfs, ControlApi::QueryJfs, ControlApi::ModifyJfs}) {
                timings_.Skip(api, "create/delete symbol missing");
            }
        }
        if (apis_.create_jetty_grp && apis_.delete_jetty_grp) {
            urma_jetty_grp_cfg_t group{};
            group.policy = URMA_JETTY_GRP_POLICY_HASH_HINT;
            std::snprintf(group.name, sizeof(group.name), "cp_%ld_%zu", static_cast<long>(getpid()), i);
            group_ = timings_.Call(ControlApi::CreateJettyGrp, [&] { return apis_.create_jetty_grp(ctx, &group); });
        } else {
            timings_.Skip(ControlApi::CreateJettyGrp, "create/delete symbol missing");
            timings_.Skip(ControlApi::DeleteJettyGrp, "create/delete symbol missing");
        }
        if (apis_.alloc_token_id && apis_.free_token_id) {
            token_ = timings_.Call(ControlApi::AllocTokenId, [&] { return apis_.alloc_token_id(ctx); });
        } else {
            timings_.Skip(ControlApi::AllocTokenId, "alloc/free symbol missing");
            timings_.Skip(ControlApi::FreeTokenId, "alloc/free symbol missing");
        }
        if (auto result = CloseLocal(); !result) {
            return result;
        }
    }
    return {};
}

std::expected<void, ToolError> ControlPlaneSession::CloseLocal() noexcept {
    // 任一删除失败即停止，防止释放仍被上层对象引用的队列或 context。
#define CLOSE_RESOURCE(member, api, call)                                                                              \
    if (member) {                                                                                                      \
        const auto rc = timings_.Call(ControlApi::api, [&] { return call; });                                          \
        if (rc != URMA_SUCCESS) {                                                                                      \
            return Error("cleanup " #api, rc);                                                                         \
        }                                                                                                              \
        member = nullptr;                                                                                              \
    }
    CLOSE_RESOURCE(token_, FreeTokenId, apis_.free_token_id(token_));
    CLOSE_RESOURCE(group_, DeleteJettyGrp, apis_.delete_jetty_grp(group_));
    CLOSE_RESOURCE(jfs_, DeleteJfs, apis_.delete_jfs(jfs_));
    CLOSE_RESOURCE(jfr_, DeleteJfr, UrmaApi::DeleteJfr(jfr_));
    CLOSE_RESOURCE(jfc_, DeleteJfc, UrmaApi::DeleteJfc(jfc_));
    CLOSE_RESOURCE(jfce_, DeleteJfce, UrmaApi::DeleteJfce(jfce_));
    CLOSE_RESOURCE(temp_ctx_, DeleteContext, UrmaApi::DeleteContext(temp_ctx_));
#undef CLOSE_RESOURCE
    return {};
}

std::expected<void, ToolError> ValidateSegmentDescriptor(std::span<const std::byte> bytes) noexcept {
    if (bytes.size() < sizeof(urma_seg_t) || bytes.size() > kMaxDescriptorBytes) {
        return Error("invalid segment descriptor size", EPROTO);
    }
    urma_seg_t seg{};
    std::memcpy(&seg, bytes.data(), sizeof(seg));
    if (!seg.len || seg.attr.bs.token_policy != URMA_TOKEN_NONE) {
        return Error("invalid segment descriptor", EPROTO);
    }
    if (seg.attr.bs.has_user_info) {
        std::uint32_t length = 0;
        if (bytes.size() < sizeof(seg) + sizeof(length)) {
            return Error("truncated segment extension", EPROTO);
        }
        std::memcpy(&length, bytes.data() + sizeof(seg), sizeof(length));
        if (length != bytes.size() - sizeof(seg) - sizeof(length)) {
            return Error("invalid segment extension length", EPROTO);
        }
    } else if (bytes.size() != sizeof(seg)) {
        return Error("unexpected segment extension", EPROTO);
    }
    return {};
}

std::expected<void, ToolError> ControlPlaneSession::RunSegments(ControlChannel& channel, bool server,
                                                                std::uint32_t timeout) {
    if (!ctx_ || memory_) {
        return Error("invalid segment session state", EINVAL);
    }
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return Error("get page size", EINVAL);
    }
    const auto page = static_cast<std::size_t>(page_size);
    const int allocation_rc = posix_memalign(&memory_, page, page * kImportCount);
    if (allocation_rc != 0) {
        return Error("allocate segment pages", allocation_rc);
    }
    std::memset(memory_, 0, page * kImportCount);
    for (std::size_t i = 0; i < kImportCount; ++i) {
        urma_seg_cfg_t cfg{};
        cfg.va = reinterpret_cast<std::uintptr_t>(memory_) + i * page;
        cfg.len = page;
        cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
        local_segments_[i] = timings_.Call(ControlApi::RegisterSeg, [&] { return UrmaApi::RegisterSeg(ctx_, &cfg); });
        if (!local_segments_[i]) {
            return Error("register benchmark segment");
        }
    }

    std::vector<Blob> descriptors;
    descriptors.reserve(kImportCount);
    auto send = [&]() -> std::expected<void, ToolError> {
        for (auto* segment : local_segments_) {
            urma_seg_t* desc = nullptr;
            std::uint32_t length = 0;
            const auto rc =
                timings_.Call(ControlApi::GetSegCtx, [&] { return UrmaApi::GetSegCtx(segment, &desc, &length); });
            if (rc != URMA_SUCCESS || !desc) {
                return Error("get segment context", rc);
            }
            ScopeExit release(
                [&]() noexcept { timings_.Call(ControlApi::PutSegCtx, [&] { UrmaApi::PutSegCtx(desc); }); });
            auto bytes = std::span(reinterpret_cast<const std::byte*>(desc), length);
            if (auto result = ValidateSegmentDescriptor(bytes); !result) {
                return result;
            }
            if (auto result = channel.SendMarker(length, timeout); !result) {
                return result;
            }
            if (auto result = channel.Write(bytes, timeout); !result) {
                return result;
            }
        }
        return {};
    };
    auto receive = [&]() -> std::expected<void, ToolError> {
        for (std::size_t i = 0; i < kImportCount; ++i) {
            std::array<std::byte, 4> wire{};
            if (auto result = channel.Read(wire, timeout); !result) {
                return result;
            }
            std::uint32_t length = 0;
            for (auto byte : wire) {
                length = (length << 8) | std::to_integer<unsigned>(byte);
            }
            if (length < sizeof(urma_seg_t) || length > kMaxDescriptorBytes) {
                return Error("invalid segment frame length", EPROTO);
            }
            Blob blob(std::calloc(1, length), &std::free);
            if (!blob) {
                return Error("allocate segment descriptor", ENOMEM);
            }
            auto bytes = std::span(static_cast<std::byte*>(blob.get()), length);
            if (auto result = channel.Read(bytes, timeout); !result) {
                return result;
            }
            if (auto result = ValidateSegmentDescriptor(bytes); !result) {
                return result;
            }
            descriptors.push_back(std::move(blob));
        }
        return {};
    };
    // 半双工交换避免大 bonding 扩展同时写满两端 TCP 缓冲区。
    if (auto result = server ? send() : receive(); !result) {
        return result;
    }
    if (auto result = server ? receive() : send(); !result) {
        return result;
    }
    urma_token_t token{};
    urma_import_seg_flag_t flags{};
    flags.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    for (std::size_t i = 0; i < kImportCount; ++i) {
        auto* desc = static_cast<urma_seg_t*>(descriptors[i].get());
        remote_segments_[i] =
            timings_.Call(ControlApi::ImportSeg, [&] { return UrmaApi::ImportSeg(ctx_, desc, &token, 0, flags); });
        if (!remote_segments_[i]) {
            return Error("import benchmark segment");
        }
    }
    if (auto result = UnimportSegments(); !result) {
        return result;
    }
    if (auto result = channel.SendMarker(kSegmentsDone, timeout); !result) {
        return result;
    }
    // 双端均反导入完成才允许注销本地内存。
    return channel.ExpectMarker(kSegmentsDone, timeout);
}

std::expected<void, ToolError> ControlPlaneSession::UnimportSegments() noexcept {
    for (auto it = remote_segments_.rbegin(); it != remote_segments_.rend(); ++it) {
        if (*it) {
            const auto rc = timings_.Call(ControlApi::UnimportSeg, [&] { return UrmaApi::UnimportSeg(*it); });
            if (rc != URMA_SUCCESS) {
                return Error("unimport benchmark segment", rc);
            }
            *it = nullptr;
        }
    }
    return {};
}
std::expected<void, ToolError> ControlPlaneSession::Close() noexcept {
    if (auto result = UnimportSegments(); !result) {
        return result;
    }
    for (auto it = local_segments_.rbegin(); it != local_segments_.rend(); ++it) {
        if (*it) {
            const auto rc = timings_.Call(ControlApi::UnregisterSeg, [&] { return UrmaApi::UnregisterSeg(*it); });
            if (rc != URMA_SUCCESS) {
                return Error("unregister benchmark segment", rc);
            }
            *it = nullptr;
        }
    }
    std::free(memory_);
    memory_ = nullptr;
    return CloseLocal();
}
} // namespace tools
} // namespace kbsocket
