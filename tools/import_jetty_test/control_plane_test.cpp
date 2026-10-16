// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/control_plane.hpp"

#include <sys/socket.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
TEST(ApiTimingsTest, PreservesResultsAndErrnoAndSeparatesFailures) {
    ApiTimings timings;
    // 成功、状态码失败和空指针失败均保持原始结果，不能把失败延迟混入成功分位数。
    EXPECT_EQ(timings.Call(ControlApi::Init,
                           [] {
                               errno = EBUSY;
                               return 0;
                           }),
              0);
    EXPECT_EQ(errno, EBUSY);
    EXPECT_EQ(timings.Call(ControlApi::Init,
                           [] {
                               errno = EIO;
                               return -7;
                           }),
              -7);
    EXPECT_EQ(errno, EIO);
    EXPECT_EQ(timings.Call(ControlApi::CreateContext,
                           []() -> void* {
                               errno = ENOMEM;
                               return nullptr;
                           }),
              nullptr);
    EXPECT_EQ(errno, ENOMEM);
    timings.Call(ControlApi::FreeDeviceList, [] { errno = EINVAL; });
    EXPECT_EQ(errno, EINVAL);
    EXPECT_TRUE(timings.measurements(ControlApi::Init).samples[0].success);
    EXPECT_FALSE(timings.measurements(ControlApi::Init).samples[1].success);
    EXPECT_EQ(timings.measurements(ControlApi::CreateContext).samples[0].code, ENOMEM);
    EXPECT_TRUE(timings.HasFailures());
}

TEST(ApiTimingsTest, ReportUsesSuccessfulSamplesAndDoesNotInventHundredthCall) {
    ApiTimings timings;
    timings.Record(ControlApi::Init, 10, true, 0);
    timings.Record(ControlApi::Init, 9999, false, -7);
    timings.Record(ControlApi::Init, 30, true, 0);
    FILE* file = tmpfile();
    ASSERT_NE(file, nullptr);
    timings.Print(file, true);
    rewind(file);
    std::string output;
    char buffer[4096];
    while (const auto count = fread(buffer, 1, sizeof(buffer), file)) {
        output.append(buffer, count);
    }
    fclose(file);
    EXPECT_NE(output.find("urma_init,3,2,1,10,1,NA,NA,10,20.000000,10,30,30,30,-7,0,measured"), std::string::npos);
    EXPECT_NE(output.find("urma_init,2,9999,false,-7"), std::string::npos);
    for (unsigned i = 0; i < 513; ++i) {
        timings.Record(ControlApi::QueryDevice, i, true, 0);
    }
    EXPECT_EQ(timings.measurements(ControlApi::QueryDevice).count, 512u);
    EXPECT_EQ(timings.measurements(ControlApi::QueryDevice).dropped, 1u);
}

struct MockControlState {
    bool extension = false;
    int fail_import = 0;
    bool fail_unregister = false;
    std::atomic<unsigned> imports[2]{};
    std::atomic<unsigned> unimports[2]{};
    std::atomic<unsigned> registrations{0};
    std::atomic<unsigned> unregistrations{0};
};
MockControlState* mock;

raw::UrmaFunctions Functions() {
    raw::UrmaFunctions f{};
    f.query_device = +[](urma_device_t*, urma_device_attr_t*) { return URMA_SUCCESS; };
    f.get_eids = +[](urma_device_t*, std::uint32_t* count) {
        *count = 1;
        return new urma_eid_info_t[1]{};
    };
    f.free_eids = +[](urma_eid_info_t* p) { delete[] p; };
    f.create_context = +[](urma_device_t* dev, std::uint32_t eid) {
        auto* p = new urma_context_t{};
        p->dev = dev;
        p->eid_index = eid;
        return p;
    };
    f.delete_context = +[](urma_context_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.create_jfce = +[](urma_context_t*) { return new urma_jfce_t{}; };
    f.delete_jfce = +[](urma_jfce_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.create_jfc = +[](urma_context_t*, urma_jfc_cfg_t*) { return new urma_jfc_t{}; };
    f.delete_jfc = +[](urma_jfc_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.create_jfr = +[](urma_context_t*, urma_jfr_cfg_t*) { return new urma_jfr_t{}; };
    f.delete_jfr = +[](urma_jfr_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.modify_jfr = +[](urma_jfr_t*, urma_jfr_attr_t* attr) {
        EXPECT_EQ(attr->mask, JFR_RX_THRESHOLD);
        return URMA_SUCCESS;
    };
    f.modify_jetty = +[](urma_jetty_t*, urma_jetty_attr_t* attr) {
        EXPECT_EQ(attr->mask, JETTY_RX_THRESHOLD);
        return URMA_SUCCESS;
    };
    f.register_seg = +[](urma_context_t* ctx, urma_seg_cfg_t* cfg) {
        auto* p = new urma_target_seg_t{};
        p->urma_ctx = ctx;
        p->seg.ubva.va = cfg->va;
        p->seg.len = cfg->len;
        ++mock->registrations;
        return p;
    };
    f.unregister_seg = +[](urma_target_seg_t* p) {
        if (mock->fail_unregister) {
            return URMA_FAIL;
        }
        ++mock->unregistrations;
        delete p;
        return URMA_SUCCESS;
    };
    f.get_seg_ctx = +[](urma_target_seg_t* p, urma_seg_t** out, std::uint32_t* size) {
        *size = sizeof(urma_seg_t) + (mock->extension ? 12 : 0);
        *out = static_cast<urma_seg_t*>(std::calloc(1, *size));
        **out = p->seg;
        if (mock->extension) {
            (*out)->attr.bs.has_user_info = 1;
            const std::uint32_t extension[] = {8, 0xcafebabe, 0x12345678};
            std::memcpy(*out + 1, extension, sizeof(extension));
        }
        return URMA_SUCCESS;
    };
    f.put_seg_ctx = +[](urma_seg_t* p) { std::free(p); };
    f.import_seg = +[](urma_context_t* ctx, urma_seg_t* seg, urma_token_t*, std::uint64_t,
                       urma_import_seg_flag_t) -> urma_target_seg_t* {
        const auto index = ctx->eid_index;
        const auto count = ++mock->imports[index];
        EXPECT_EQ(mock->unimports[index], 0u);
        if (mock->extension) {
            // 验证 bonding 的整个描述跨双向控制通道传输，没有截断为基础结构。
            std::uint32_t extension[3]{};
            std::memcpy(extension, seg + 1, sizeof(extension));
            EXPECT_EQ(extension[0], 8u);
            EXPECT_EQ(extension[1], 0xcafebabeu);
            EXPECT_EQ(extension[2], 0x12345678u);
        }
        if (index == 1 && static_cast<int>(count) == mock->fail_import) {
            errno = EIO;
            return nullptr;
        }
        auto* p = new urma_target_seg_t{};
        p->urma_ctx = ctx;
        p->seg = *seg;
        return p;
    };
    f.unimport_seg = +[](urma_target_seg_t* p) {
        ++mock->unimports[p->urma_ctx->eid_index];
        delete p;
        return URMA_SUCCESS;
    };
    return f;
}
ControlPlaneApis Extra() {
    ControlPlaneApis f;
    f.modify_jfc = +[](urma_jfc_t*, urma_jfc_attr_t*) { return URMA_SUCCESS; };
    f.create_jfs = +[](urma_context_t*, urma_jfs_cfg_t* cfg) {
        EXPECT_EQ(cfg->trans_mode, URMA_TM_RM);
        EXPECT_EQ(cfg->priority, 3);
        return new urma_jfs_t{};
    };
    f.delete_jfs = +[](urma_jfs_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.query_jfs = +[](urma_jfs_t*, urma_jfs_cfg_t*, urma_jfs_attr_t*) { return URMA_SUCCESS; };
    f.modify_jfs = +[](urma_jfs_t*, urma_jfs_attr_t*) { return URMA_SUCCESS; };
    f.query_jfr = +[](urma_jfr_t*, urma_jfr_cfg_t*, urma_jfr_attr_t*) { return URMA_SUCCESS; };
    f.query_jetty = +[](urma_jetty_t*, urma_jetty_cfg_t*, urma_jetty_attr_t*) { return URMA_SUCCESS; };
    f.create_jetty_grp = +[](urma_context_t*, urma_jetty_grp_cfg_t*) { return new urma_jetty_grp_t{}; };
    f.delete_jetty_grp = +[](urma_jetty_grp_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.alloc_token_id = +[](urma_context_t*) { return new urma_token_id_t{}; };
    f.free_token_id = +[](urma_token_id_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    return f;
}

void RunPair(bool extension, int fail_import = 0, bool fail_cleanup = false) {
    MockControlState state;
    state.extension = extension;
    state.fail_import = fail_import;
    mock = &state;
    raw::test_support::ScopedUrmaOverride override(Functions());
    auto extra = Extra();
    ApiTimings server_timings;
    ApiTimings client_timings;
    ControlPlaneSession server(server_timings, extra);
    ControlPlaneSession client(client_timings, extra);
    urma_device_t device{};
    urma_context_t server_ctx{};
    server_ctx.dev = &device;
    urma_context_t client_ctx{};
    client_ctx.dev = &device;
    client_ctx.eid_index = 1;
    ASSERT_TRUE(server.RunLocal(&server_ctx, 3, {}));
    ASSERT_TRUE(client.RunLocal(&client_ctx, 3, {}));
    for (auto api :
         {ControlApi::CreateContext, ControlApi::DeleteContext, ControlApi::CreateJfc, ControlApi::DeleteJfc,
          ControlApi::CreateJfr, ControlApi::DeleteJfr, ControlApi::CreateJfs, ControlApi::DeleteJfs,
          ControlApi::AllocTokenId, ControlApi::FreeTokenId, ControlApi::CreateJettyGrp, ControlApi::DeleteJettyGrp}) {
        EXPECT_EQ(client_timings.measurements(api).count, 100u);
    }
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    std::expected<void, ToolError> server_result;
    std::thread peer([&] {
        ControlChannel channel(sockets[0]);
        server_result = server.RunSegments(channel, true, 5000);
    });
    {
        ControlChannel channel(sockets[1]);
        const auto result = client.RunSegments(channel, false, 5000);
        EXPECT_EQ(result.has_value(), fail_import == 0);
        if (fail_cleanup) {
            // 注销失败后保持注册缓冲区存活；下一次 Close 重试不能重复释放成功项。
            state.fail_unregister = true;
            EXPECT_FALSE(client.Close());
            EXPECT_EQ(state.unregistrations.load(), 0u);
            state.fail_unregister = false;
        }
        EXPECT_TRUE(client.Close());
    }
    peer.join();
    EXPECT_EQ(server_result.has_value(), fail_import == 0);
    EXPECT_TRUE(server.Close());
    EXPECT_EQ(state.registrations.load(), 200u);
    EXPECT_EQ(state.unregistrations.load(), 200u);
    EXPECT_EQ(state.unimports[0].load(), 100u);
    EXPECT_EQ(state.unimports[1].load(), fail_import ? static_cast<unsigned>(fail_import - 1) : 100u);
    EXPECT_EQ(client_timings.measurements(ControlApi::ImportSeg).count,
              fail_import ? static_cast<std::size_t>(fail_import) : 100u);
    EXPECT_EQ(client_timings.measurements(ControlApi::UnregisterSeg).count, fail_cleanup ? 101u : 100u);
}

TEST(ControlPlaneTest, RawLifecycleAndBidirectionalSegments) {
    RunPair(false);
}
TEST(ControlPlaneTest, BondingSegmentExtensions) {
    RunPair(true);
}
TEST(ControlPlaneTest, ImportFailureCleansSuccessfulPrefix) {
    RunPair(true, 37);
}
TEST(ControlPlaneTest, UnregisterFailureIsRecordedAndRetryable) {
    RunPair(false, 0, true);
}

TEST(ControlPlaneTest, MissingOptionalSymbolsAreSkipped) {
    MockControlState state;
    mock = &state;
    raw::test_support::ScopedUrmaOverride override(Functions());
    ApiTimings timings;
    ControlPlaneApis extra;
    ControlPlaneSession session(timings, extra);
    urma_device_t device{};
    urma_context_t ctx{};
    ctx.dev = &device;
    ASSERT_TRUE(session.RunLocal(&ctx, 3, {}));
    EXPECT_TRUE(session.Close());
    EXPECT_EQ(timings.measurements(ControlApi::CreateJfs).count, 0u);
    EXPECT_STREQ(timings.measurements(ControlApi::CreateJfs).skipped, "create/delete symbol missing");
    EXPECT_FALSE(timings.HasFailures());
}

TEST(ControlPlaneTest, TruncatedSegmentExtensionIsRejected) {
    std::vector<std::byte> bytes(sizeof(urma_seg_t) + 4);
    urma_seg_t seg{};
    seg.len = 4096;
    seg.attr.bs.has_user_info = 1;
    std::memcpy(bytes.data(), &seg, sizeof(seg));
    std::uint32_t length = 8;
    std::memcpy(bytes.data() + sizeof(seg), &length, sizeof(length));
    EXPECT_FALSE(ValidateSegmentDescriptor(bytes));
    EXPECT_FALSE(ValidateSegmentDescriptor(std::span(bytes).first(sizeof(seg) - 1)));
}
} // namespace
} // namespace tools
} // namespace kbsocket
