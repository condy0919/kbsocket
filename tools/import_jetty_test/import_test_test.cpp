// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/import_test.hpp"

#include <sys/socket.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
struct MockState {
    bool extension = false;
    int fail_import = 0;
    int fail_create = 0;
    int imports = 0;
    int unimports = 0;
    int created = 0;
    int deleted = 0;
    int exports = 0;
    int released = 0;
    bool fail_unimport = false;
};
MockState state;

urma_device_cap_t Capabilities() {
    urma_device_cap_t cap{};
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.priority_info[3].tp_type.bs.ctp = 1;
    cap.max_jetty = 200;
    cap.max_jfc_depth = 4096;
    cap.max_jfs_depth = cap.max_jfr_depth = 128;
    cap.max_jfs_sge = cap.max_jfs_rsge = cap.max_jfr_sge = 1;
    return cap;
}

raw::UrmaFunctions Functions() {
    raw::UrmaFunctions f{};
    f.create_jfc = +[](urma_context_t*, urma_jfc_cfg_t*) { return new urma_jfc_t{}; };
    f.delete_jfc = +[](urma_jfc_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.create_jfr = +[](urma_context_t*, urma_jfr_cfg_t* cfg) {
        auto* p = new urma_jfr_t{};
        p->jfr_cfg = *cfg;
        return p;
    };
    f.delete_jfr = +[](urma_jfr_t* p) {
        delete p;
        return URMA_SUCCESS;
    };
    f.create_jetty = +[](urma_context_t* ctx, urma_jetty_cfg_t* cfg) {
        EXPECT_EQ(cfg->jfs_cfg.trans_mode, URMA_TM_RM);
        EXPECT_EQ(cfg->jfs_cfg.priority, 3);
        if (state.created + 1 == state.fail_create) {
            errno = ENOMEM;
            return static_cast<urma_jetty_t*>(nullptr);
        }
        auto* p = new urma_jetty_t{};
        p->urma_ctx = ctx;
        p->jetty_cfg = *cfg;
        p->jetty_id.id = ++state.created;
        return p;
    };
    f.delete_jetty = +[](urma_jetty_t* p) {
        ++state.deleted;
        delete p;
        return URMA_SUCCESS;
    };
    f.get_rjetty = +[](urma_jetty_t* jetty, urma_rjetty_t** out, std::uint32_t* length) {
        *length = sizeof(urma_rjetty_t) + (state.extension ? 12 : 0);
        auto* p = static_cast<urma_rjetty_t*>(std::calloc(1, *length));
        p->jetty_id = jetty->jetty_id;
        p->trans_mode = URMA_TM_RM;
        p->type = URMA_JETTY;
        // 模拟 GetRjetty 不填 CTP 字段，验证导出端会显式修正。
        if (state.extension) {
            p->flag.bs.has_user_info = 1;
            const std::uint32_t ext[] = {8, 0x12345678, 0xabcdef01};
            std::memcpy(p + 1, ext, sizeof(ext));
        }
        *out = p;
        ++state.exports;
        return URMA_SUCCESS;
    };
    f.put_rjetty = +[](urma_rjetty_t* p) {
        ++state.released;
        std::free(p);
    };
    f.import_jetty = +[](urma_context_t*, urma_rjetty_t* r, urma_token_t*) -> urma_target_jetty_t* {
        ++state.imports;
        EXPECT_EQ(state.unimports, 0); // 保证第 100 次调用仍能复用前 99 次保留的 TP 引用。
        EXPECT_EQ(r->jetty_id.id, static_cast<unsigned>(state.imports));
        EXPECT_EQ(r->trans_mode, URMA_TM_RM);
        EXPECT_EQ(r->tp_type, URMA_CTP);
        if (state.extension) {
            // 扩展内容必须原样跨控制通道传输，不能只交换 EID 和 jetty id。
            std::uint32_t ext[3]{};
            std::memcpy(ext, r + 1, sizeof(ext));
            EXPECT_EQ(ext[0], 8u);
            EXPECT_EQ(ext[1], 0x12345678u);
            EXPECT_EQ(ext[2], 0xabcdef01u);
        }
        if (state.imports == state.fail_import) {
            errno = EIO;
            return nullptr;
        }
        auto* p = new urma_target_jetty_t{};
        p->id = r->jetty_id;
        return p;
    };
    f.unimport_jetty = +[](urma_target_jetty_t* p) {
        if (state.fail_unimport) {
            return URMA_FAIL;
        }
        EXPECT_EQ(state.imports, state.fail_import ? state.fail_import : 100);
        ++state.unimports;
        delete p;
        return URMA_SUCCESS;
    };
    // 所有 WR 投递入口故意留空；意外投递将失败，不能让数据面参与测量。
    return f;
}

class ImportTest : public testing::Test {
protected:
    void SetUp() override {
        state = {};
    }
    void RunPair(bool extension, int fail_import = 0, bool fail_cleanup = false, bool profile = false) {
        state.extension = extension;
        state.fail_import = fail_import;
        state.fail_unimport = fail_cleanup;
        raw::test_support::ScopedUrmaOverride override(Functions());
        urma_context_t context{};
        ApiTimings server_timings;
        ApiTimings client_timings;
        ImportSession server(profile ? &server_timings : nullptr);
        ImportSession client(profile ? &client_timings : nullptr);
        ASSERT_TRUE(server.Open(&context, Capabilities(), 3));
        ASSERT_TRUE(client.Open(&context, Capabilities(), 3));
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
        std::expected<void, ToolError> server_result;
        std::thread peer([&] {
            ControlChannel channel(sockets[0]);
            server_result = server.Run(channel, true, 5000);
        });
        {
            ControlChannel channel(sockets[1]);
            auto result = client.Run(channel, false, 5000);
            EXPECT_EQ(result.has_value(), fail_import == 0 && !fail_cleanup);
            EXPECT_EQ(client.completed(), fail_import ? static_cast<unsigned>(fail_import - 1) : 100u);
            if (fail_cleanup) {
                // 清理失败必须保留依赖，修复故障后 Close 可以重试。
                EXPECT_FALSE(client.Close());
                EXPECT_EQ(state.deleted, 0);
                state.fail_unimport = false;
            }
            EXPECT_TRUE(client.Close());
        }
        peer.join();
        EXPECT_EQ(server_result.has_value(), fail_import == 0 && !fail_cleanup);
        EXPECT_TRUE(server.Close());
        EXPECT_EQ(state.imports, fail_import ? fail_import : 100);
        EXPECT_EQ(state.unimports, fail_import ? fail_import - 1 : 100);
        EXPECT_EQ(state.created, 200);
        EXPECT_EQ(state.deleted, 200);
        EXPECT_EQ(state.exports, 100);
        EXPECT_EQ(state.released, 100);
        if (profile) {
            // 扩展计时仍保持原导入场景；释放、导出也必须进入各自 API 的统计。
            EXPECT_EQ(client_timings.measurements(ControlApi::ImportJetty).count, 100u);
            EXPECT_EQ(client_timings.measurements(ControlApi::UnimportJetty).count, 100u);
            EXPECT_EQ(client_timings.measurements(ControlApi::DeleteJetty).count, 100u);
            EXPECT_EQ(server_timings.measurements(ControlApi::GetRjetty).count, 100u);
            EXPECT_EQ(server_timings.measurements(ControlApi::PutRjetty).count, 100u);
            EXPECT_FALSE(client_timings.HasFailures());
        }
    }
};

TEST_F(ImportTest, RawDescriptorsAndAllImportsRetained) {
    RunPair(false);
}
TEST_F(ImportTest, BondingExtensionSurvivesExchange) {
    RunPair(true);
}
TEST_F(ImportTest, FailedImportCleansSuccessfulPrefix) {
    RunPair(true, 37);
}
TEST_F(ImportTest, FailedUnimportRetainsDependenciesForRetry) {
    RunPair(false, 0, true);
}

TEST_F(ImportTest, ProfileIncludesOriginalImportAndCleanup) {
    RunPair(true, 0, false, true);
}

TEST_F(ImportTest, DifferentControlPlaneModesFailBeforeImport) {
    raw::test_support::ScopedUrmaOverride override(Functions());
    urma_context_t context{};
    ApiTimings timings;
    ImportSession server;
    ImportSession client(&timings);
    ASSERT_TRUE(server.Open(&context, Capabilities(), 3));
    ASSERT_TRUE(client.Open(&context, Capabilities(), 3));
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    std::expected<void, ToolError> server_result;
    std::thread peer([&] {
        ControlChannel channel(sockets[0]);
        server_result = server.Run(channel, true, 5000);
    });
    {
        ControlChannel channel(sockets[1]);
        EXPECT_FALSE(client.Run(channel, false, 5000));
    }
    peer.join();
    EXPECT_FALSE(server_result);
    EXPECT_EQ(state.imports, 0);
    EXPECT_TRUE(client.Close());
    EXPECT_TRUE(server.Close());
}

TEST_F(ImportTest, PartialQueueCreationCanBeClosed) {
    // 初始化中途耗尽资源时，已创建的队列仍由会话持有并可逆序释放。
    state.fail_create = 37;
    raw::test_support::ScopedUrmaOverride override(Functions());
    urma_context_t context{};
    ImportSession session;
    auto result = session.Open(&context, Capabilities(), 3);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ENOMEM);
    EXPECT_TRUE(session.Close());
    EXPECT_TRUE(session.Close());
    EXPECT_EQ(state.created, 36);
    EXPECT_EQ(state.deleted, 36);
    EXPECT_EQ(state.imports, 0);
}

TEST(ImportDescriptorTest, RejectsTruncationAndUnexpectedMode) {
    // 网络长度不可直接信任，避免把截断的 provider 扩展交给 import。
    std::vector<std::byte> bytes(sizeof(urma_rjetty_t) + 4);
    urma_rjetty_t r{};
    r.trans_mode = URMA_TM_RM;
    r.tp_type = URMA_CTP;
    r.type = URMA_JETTY;
    std::memcpy(bytes.data(), &r, sizeof(r));
    EXPECT_TRUE(ValidateDescriptor(std::span(bytes).first(sizeof(r))));
    EXPECT_FALSE(ValidateDescriptor(std::span(bytes).first(sizeof(r) - 1)));
    EXPECT_FALSE(ValidateDescriptor(bytes));
    r.flag.bs.has_user_info = 1;
    std::memcpy(bytes.data(), &r, sizeof(r));
    std::uint32_t length = 100;
    std::memcpy(bytes.data() + sizeof(r), &length, sizeof(length));
    EXPECT_FALSE(ValidateDescriptor(bytes));
    r.trans_mode = URMA_TM_RC;
    std::memcpy(bytes.data(), &r, sizeof(r));
    EXPECT_FALSE(ValidateDescriptor(bytes));
}

TEST(ImportPriorityTest, RequiresAdvertisedCtpPriority) {
    auto cap = Capabilities();
    EXPECT_EQ(*SelectCtpPriority(cap, -1), 3u);
    EXPECT_EQ(*SelectCtpPriority(cap, 3), 3u);
    EXPECT_FALSE(SelectCtpPriority(cap, 0));
    cap.rm_tp_cap.bs.ctp = 0;
    EXPECT_FALSE(SelectCtpPriority(cap, 3));
}
} // namespace
} // namespace tools
} // namespace kbsocket
