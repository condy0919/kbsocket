// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/raw_runtime.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"
#include "kbsocket/transport/raw/urma_context.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;

class MockRuntime {
public:
    MOCK_METHOD(urma_status_t, Init, (urma_init_attr_t*));
    MOCK_METHOD(urma_status_t, Uninit, ());
    MOCK_METHOD(urma_device_t**, Devices, (int*));
    MOCK_METHOD(void, FreeDevices, (urma_device_t**));
    MOCK_METHOD(urma_status_t, Query, (urma_device_t*, urma_device_attr_t*));
    MOCK_METHOD(urma_eid_info_t*, Eids, (urma_device_t*, uint32_t*));
    MOCK_METHOD(void, FreeEids, (urma_eid_info_t*));
    MOCK_METHOD(urma_context_t*, Create, (urma_device_t*, uint32_t));
    MOCK_METHOD(urma_status_t, Delete, (urma_context_t*));
};

class RawRuntimeTest : public ::testing::Test {
public:
    void SetUp() override {
        active_ = &mock_;
        for (int i = 0; i < 4; ++i) {
            std::snprintf(devices_[i].name, URMA_MAX_NAME, "raw%d", i);
            devices_[i].type = URMA_TRANSPORT_UB;
            pointers_[i] = &devices_[i];
            eids_[i].eid.raw[0] = i + 1;
            eids_[i].eid_index = i * 3 + 2;
            contexts_[i].eid = eids_[i].eid;
            contexts_[i].eid_index = eids_[i].eid_index;
            configs_.push_back({
                .device_name = devices_[i].name,
                .eid_index = eids_[i].eid_index,
            });
        }
    }

    void TearDown() override {
        active_ = nullptr;
    }

    UrmaFunctions Functions() {
        return {
            .init = [](auto* a) { return active_->Init(a); },
            .uninit = [] { return active_->Uninit(); },
            .get_devices = [](int* n) { return active_->Devices(n); },
            .free_devices = [](auto* p) { active_->FreeDevices(p); },
            .get_eids = [](auto* d, auto* n) { return active_->Eids(d, n); },
            .free_eids = [](auto* p) { active_->FreeEids(p); },
            .query_device = [](auto* d, auto* a) { return active_->Query(d, a); },
            .create_context = [](auto* d, uint32_t n) { return active_->Create(d, n); },
            .delete_context = [](auto* c) { return active_->Delete(c); },
        };
    }

    void ExpectDiscovery() {
        EXPECT_CALL(mock_, Init(_)).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, Devices(_)).WillOnce([this](int* n) {
            *n = 4;
            return pointers_;
        });
        EXPECT_CALL(mock_, FreeDevices(pointers_));
        for (int i = 0; i < 4; ++i) {
            EXPECT_CALL(mock_, Query(&devices_[i], _)).WillOnce([](auto*, auto* attr) {
                *attr = {};
                attr->dev_cap.trans_mode = URMA_TM_RM;
                attr->dev_cap.rm_tp_cap.bs.ctp = 1;
                attr->dev_cap.max_eid_cnt = 256;
                return URMA_SUCCESS;
            });
            EXPECT_CALL(mock_, Eids(&devices_[i], _)).WillOnce([this, i](auto*, auto* n) {
                *n = 1;
                return &eids_[i];
            });
            EXPECT_CALL(mock_, FreeEids(&eids_[i]));
        }
    }

    void ExpectCreateAll() {
        for (int i = 0; i < 4; ++i) {
            EXPECT_CALL(mock_, Create(&devices_[i], eids_[i].eid_index)).WillOnce(Return(&contexts_[i]));
        }
    }

    void ExpectCloseAll() {
        ::testing::InSequence order;
        for (int i = 3; i >= 0; --i) {
            EXPECT_CALL(mock_, Delete(&contexts_[i])).WillOnce(Return(URMA_SUCCESS));
        }
        EXPECT_CALL(mock_, Uninit()).WillOnce(Return(URMA_SUCCESS));
    }

    LocalEndpoint Endpoint(int i) {
        return {
            .device = &devices_[i],
            .eid = eids_[i].eid,
            .eid_index = eids_[i].eid_index,
        };
    }

protected:
    static inline MockRuntime* active_ = nullptr;
    ::testing::StrictMock<MockRuntime> mock_;
    urma_device_t devices_[4]{};
    urma_device_t* pointers_[4]{};
    urma_eid_info_t eids_[4]{};
    urma_context_t contexts_[4]{};
    std::vector<DeviceConfig> configs_;
};

TEST_F(RawRuntimeTest, ProcessRuntimeSupportsExplicitCloseAndReinitialize) {
    test_support::ScopedUrmaOverride scope(Functions());
    auto& runtime = GetRawRuntime();
    EXPECT_EQ(&runtime, &GetRawRuntime());

    // 验证常驻实例的复用与生命周期：NoDestructor 仅屏蔽进程退出时的自动析构；
    // 在业务受控停止所有借用者后，仍支持显式 Close 并再次 Initialize 开启全新会话。
    // 执行两次循环以验证会话清理干净且完全可重入。
    for (int i = 0; i < 2; ++i) {
        ExpectDiscovery();
        ExpectCreateAll();
        ExpectCloseAll();
        ASSERT_TRUE(runtime.Initialize(configs_));
        EXPECT_EQ(runtime.context(0), &contexts_[0]);
        ASSERT_TRUE(runtime.Close());
        EXPECT_FALSE(runtime.ready());
    }
}

TEST_F(RawRuntimeTest, ProcessRuntimeDoesNotCleanUpAtExit) {
    // 重新执行测试进程，避免继承前序测试已构造的单例，确保退出回调的注册顺序有效。
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    // 子进程隔离常驻会话和退出回调；std::exit 才会执行静态析构，不能用 _Exit 代替。
    EXPECT_EXIT(
        {
            test_support::ScopedUrmaOverride scope(Functions());
            ExpectDiscovery();
            ExpectCreateAll();
            // 常驻单例核心契约：进程退出时严禁自动调用 Delete 或 Uninit，避免与未停止的 worker 产生竞态。
            EXPECT_CALL(mock_, Delete(_)).Times(0);
            EXPECT_CALL(mock_, Uninit()).Times(0);
            // 在首次取得常驻实例前注册，使误加的静态析构先运行，再检查禁止清理的期望。
            // exit 不展开当前栈，fixture/mock 和函数表替换在检查期间仍然存活。
            if (std::atexit([] {
                    const bool verified = ::testing::Mock::VerifyAndClearExpectations(active_);
                    std::quick_exit(verified && !::testing::Test::HasFailure() ? 0 : 1);
                }) != 0) {
                std::quick_exit(2);
            }
            if (!GetRawRuntime().Initialize(configs_)) {
                std::quick_exit(3);
            }
            std::exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(RawRuntimeTest, FourContextsPublishTogetherAndCloseInReverseOrder) {
    test_support::ScopedUrmaOverride scope(Functions());
    ExpectDiscovery();
    ExpectCreateAll();
    ExpectCloseAll();
    RawRuntime runtime;
    // 验证多设备端点原子发布与边界保护。
    ASSERT_TRUE(runtime.Initialize(configs_));
    EXPECT_TRUE(runtime.ready());
    ASSERT_EQ(runtime.device_count(), 4u);
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(runtime.context(i), &contexts_[i]);
    }
    EXPECT_EQ(runtime.context(4), nullptr);

    // 验证进程级排他性：同一进程不能启动第二个活跃会话，防止重新枚举使已有的设备指针悬挂失效。
    RawRuntime other;
    const auto busy = other.Initialize(configs_);
    ASSERT_FALSE(busy);
    EXPECT_EQ(busy.error().code, RuntimeErrorCode::kInUse);

    // 验证关闭状态与幂等性：关闭后句柄置空，重复 Close 安全成功。
    ASSERT_TRUE(runtime.Close());
    EXPECT_EQ(runtime.context(0), nullptr);
    EXPECT_TRUE(runtime.Close());
}

TEST_F(RawRuntimeTest, ThirdCreateFailureRollsBackAndAllowsRetry) {
    test_support::ScopedUrmaOverride scope(Functions());
    ExpectDiscovery();
    {
        ::testing::InSequence order;
        EXPECT_CALL(mock_, Create(&devices_[0], 2)).WillOnce(Return(&contexts_[0]));
        EXPECT_CALL(mock_, Create(&devices_[1], 5)).WillOnce(Return(&contexts_[1]));
        // 故障注入：前两个 context 创建成功，第三个创建时底层返回内存不足（ENOMEM）。
        EXPECT_CALL(mock_, Create(&devices_[2], 8)).WillOnce([](auto*, auto) -> urma_context_t* {
            errno = ENOMEM;
            return nullptr;
        });
        // 严格时序与回滚预期：第三个创建失败时，第四个绝不能尝试创建；
        // 已创建的前两个 context 必须严格逆序删除；随后调用 Uninit 终止底层会话。
        EXPECT_CALL(mock_, Delete(&contexts_[1])).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, Delete(&contexts_[0])).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, Uninit()).WillOnce(Return(URMA_SUCCESS));
    }
    RawRuntime runtime;
    auto failed = runtime.Initialize(configs_);
    ASSERT_FALSE(failed);
    // 断言错误精确定位到第 3 个设备（index=2），底层错误码为 ENOMEM，且回滚清理顺利完成。
    EXPECT_EQ(failed.error().device_index, 2u);
    ASSERT_TRUE(failed.error().context_error);
    EXPECT_EQ(failed.error().context_error->provider_error, ENOMEM);
    EXPECT_FALSE(failed.error().cleanup_error);
    EXPECT_FALSE(runtime.ready());
    EXPECT_EQ(runtime.context(0), nullptr);

    // 验证回滚可恢复性：彻底回滚后状态干净，同一对象能够重新初始化完整会话。
    ExpectDiscovery();
    ExpectCreateAll();
    ExpectCloseAll();
    ASSERT_TRUE(runtime.Initialize(configs_));
    EXPECT_TRUE(runtime.Close());
}

TEST_F(RawRuntimeTest, RollbackFailureRetainsHandlesAndBothErrors) {
    test_support::ScopedUrmaOverride scope(Functions());
    ExpectDiscovery();
    EXPECT_CALL(mock_, Create(&devices_[0], 2)).WillOnce(Return(&contexts_[0]));
    // 阶段一故障注入：模拟设备创建后硬件实际返回的 EID 与目录不符（端点漂移），
    // 触发内部校验失败（kEndpointChanged），从而引发回滚流程。
    contexts_[1].eid.raw[0] = 99;
    EXPECT_CALL(mock_, Create(&devices_[1], 5)).WillOnce(Return(&contexts_[1]));
    // 阶段二故障注入：回滚删除 context[1] 时底层驱动返回 EAGAIN，模拟级联故障。
    EXPECT_CALL(mock_, Delete(&contexts_[1])).WillOnce(Return(URMA_EAGAIN));

    RawRuntime runtime;
    auto failed = runtime.Initialize(configs_);
    ASSERT_FALSE(failed);
    // 验证错误根因保留机制：同时保留启动阶段的业务错误与回滚阶段的清理错误，杜绝后者覆盖前者。
    ASSERT_TRUE(failed.error().context_error);
    EXPECT_EQ(failed.error().context_error->code, ContextErrorCode::kEndpointChanged);
    ASSERT_TRUE(failed.error().cleanup_error);
    EXPECT_EQ(failed.error().cleanup_error->device_index, 1u);
    EXPECT_EQ(failed.error().cleanup_error->provider_error, URMA_EAGAIN);
    EXPECT_EQ(runtime.context(0), nullptr);

    // 验证句柄保留与防重入：由于回滚清理未彻底完成，runtime 必须继续锁定占有权，禁止重复初始化以防句柄泄漏。
    auto blocked = runtime.Initialize(configs_);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error().code, RuntimeErrorCode::kInUse);

    // 阶段三恢复重试：显式调用 Close() 重试，从未关闭的 context[1] 开始继续逆序清理，全部成功后再 Uninit。
    {
        ::testing::InSequence order;
        EXPECT_CALL(mock_, Delete(&contexts_[1])).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, Delete(&contexts_[0])).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, Uninit()).WillOnce(Return(URMA_SUCCESS));
    }
    EXPECT_TRUE(runtime.Close());
}

TEST_F(RawRuntimeTest, CloseFailureDoesNotDeleteSuccessfulContextTwice) {
    test_support::ScopedUrmaOverride scope(Functions());
    ExpectDiscovery();
    ExpectCreateAll();
    RawRuntime runtime;
    ASSERT_TRUE(runtime.Initialize(configs_));

    // 故障注入：逆序关闭时 context[3] 成功删除，但在删除 context[2] 时底层驱动报错。
    {
        ::testing::InSequence order;
        EXPECT_CALL(mock_, Delete(&contexts_[3])).WillOnce(Return(URMA_SUCCESS));
        EXPECT_CALL(mock_, Delete(&contexts_[2])).WillOnce(Return(URMA_EAGAIN));
    }
    auto failed = runtime.Close();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().device_index, 2u);
    EXPECT_FALSE(runtime.ready());

    // 验证防二次释放（Double Free）：重试 Close() 时，已删除成功的 context[3] 绝不能被再次调用，
    // 严格从此前失败的 context[2] 继续往前删除，直到最终结束会话。
    {
        ::testing::InSequence order;
        for (int i = 2; i >= 0; --i) {
            EXPECT_CALL(mock_, Delete(&contexts_[i])).WillOnce(Return(URMA_SUCCESS));
        }
        EXPECT_CALL(mock_, Uninit()).WillOnce(Return(URMA_SUCCESS));
    }
    EXPECT_TRUE(runtime.Close());
}

TEST_F(RawRuntimeTest, MissingLastEidCreatesNoContexts) {
    test_support::ScopedUrmaOverride scope(Functions());
    ExpectDiscovery();
    // 全量端点预检失败时，直接 Uninit 终止会话，不得尝试为任何设备创建 context。
    EXPECT_CALL(mock_, Uninit()).WillOnce(Return(URMA_SUCCESS));
    configs_[3].eid_index = 100;
    RawRuntime runtime;

    // 核心验证：两阶段初始化的原子性。第四个设备的 EID 不存在时，必须在阶段一的端点解析中
    // 提前拦截并失败（kNotFound），严禁对前三个合法设备发起任何 Create 调用。
    auto result = runtime.Initialize(configs_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().device_index, 3u);
    ASSERT_TRUE(result.error().catalog_error);
    EXPECT_EQ(result.error().catalog_error->code, CatalogErrorCode::kNotFound);
}

TEST_F(RawRuntimeTest, InitializationFailureDoesNotUninitAndReleasesClaim) {
    test_support::ScopedUrmaOverride scope(Functions());
    // 故障注入：底层首个调用 UrmaApi::Init 即失败。
    EXPECT_CALL(mock_, Init(_)).WillOnce(Return(URMA_FAIL));
    RawRuntime runtime;
    auto failed = runtime.Initialize(configs_);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, RuntimeErrorCode::kInitFailed);

    // 契约验证：未成功 Init 的会话严禁调用 Uninit（防驱动崩溃）；且失败后必须释放全局占用标记，
    // 允许同一对象修正环境后重新尝试初始化。
    ExpectDiscovery();
    ExpectCreateAll();
    ExpectCloseAll();
    ASSERT_TRUE(runtime.Initialize(configs_));
}

TEST_F(RawRuntimeTest, UninitFailureEndsSessionWithoutRepeatingUninit) {
    test_support::ScopedUrmaOverride scope(Functions());
    ExpectDiscovery();
    ExpectCreateAll();
    RawRuntime runtime;
    ASSERT_TRUE(runtime.Initialize(configs_));
    {
        ::testing::InSequence sequence;
        for (std::size_t i = 4; i > 0; --i) {
            EXPECT_CALL(mock_, Delete(&contexts_[i - 1])).WillOnce(Return(URMA_SUCCESS));
        }
        // 故障注入：所有 context 均成功释放，但在最后的 UrmaApi::Uninit 时底层驱动报错。
        // 根据 URMA 规范，即使 provider 卸载报错，设备列表亦已不可逆销毁，会话已事实结束。
        EXPECT_CALL(mock_, Uninit()).WillOnce(Return(URMA_FAIL));
    }
    auto result = runtime.Close();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, RuntimeCloseErrorCode::kUninitFailed);
    EXPECT_FALSE(runtime.ready());

    // 契约验证：会话已终止，后续重复 Close() 幂等成功，绝不二次调用底层 Uninit。
    EXPECT_TRUE(runtime.Close());

    // 契约验证：全局独占标记必须已被释放，允许开启全新会话且不复用已失效的旧设备指针。
    ExpectDiscovery();
    ExpectCreateAll();
    ExpectCloseAll();
    ASSERT_TRUE(runtime.Initialize(configs_));
}

TEST_F(RawRuntimeTest, InvalidConfigDoesNotTouchProvider) {
    test_support::ScopedUrmaOverride scope(Functions());
    RawRuntime runtime;
    // 零副作用防御：空配置、重复设备名、含 '\0' 畸形名称在进入任何底层 provider API 之前即被前置拦截。
    EXPECT_FALSE(runtime.Initialize({}));
    EXPECT_FALSE(runtime.Initialize({{"raw0", 2}, {"raw0", 5}}));
    EXPECT_FALSE(runtime.Initialize({{std::string("raw\0x", 5), 2}}));
}

TEST_F(RawRuntimeTest, ContextKeepsHandleOnCloseFailure) {
    test_support::ScopedUrmaOverride scope(Functions());
    EXPECT_CALL(mock_, Create(&devices_[0], 2)).WillOnce(Return(&contexts_[0]));
    UrmaContext context;
    ASSERT_TRUE(context.Open(Endpoint(0)));

    // 故障注入：底层 DeleteContext 释放失败。
    EXPECT_CALL(mock_, Delete(&contexts_[0])).WillOnce(Return(URMA_EAGAIN));
    EXPECT_FALSE(context.Close());
    // 契约验证：释放失败时必须保留底层句柄所有权，不得静默置空；允许调用方重试。
    EXPECT_TRUE(context.has_handle());
    EXPECT_EQ(context.get(), &contexts_[0]);

    // 重试释放成功后所有权清除，析构函数不会重复调用 Delete。
    EXPECT_CALL(mock_, Delete(&contexts_[0])).WillOnce(Return(URMA_SUCCESS));
    EXPECT_TRUE(context.Close());
    EXPECT_FALSE(context.has_handle());
    EXPECT_TRUE(context.Close());
}

TEST_F(RawRuntimeTest, ContextValidationFailureRetainsOwnership) {
    test_support::ScopedUrmaOverride scope(Functions());
    // 故障注入：底层已成功分配 context，但实际生效的 EID 索引与请求不一致（端点漂移）。
    contexts_[0].eid_index = 200;
    EXPECT_CALL(mock_, Create(&devices_[0], 2)).WillOnce(Return(&contexts_[0]));
    // 契约验证：析构时必须自动释放底层句柄，防止硬件资源泄漏。
    EXPECT_CALL(mock_, Delete(&contexts_[0])).WillOnce(Return(URMA_SUCCESS));

    UrmaContext context;
    auto result = context.Open(Endpoint(0));
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ContextErrorCode::kEndpointChanged);
    // 校验失败但仍持有资源以备清理：has_handle() 为 true，但业务必须依 expected 判失败。
    EXPECT_TRUE(context.has_handle());
    EXPECT_EQ(context.get(), &contexts_[0]);
}
} // namespace
} // namespace raw
} // namespace kbsocket
