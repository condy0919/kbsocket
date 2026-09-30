// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/urma_api.hpp"

#include <dlfcn.h>

#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace {
class MockLoader {
public:
    MOCK_METHOD(void*, Open, (const char*, int));
    MOCK_METHOD(void*, Symbol, (void*, const char*));
    MOCK_METHOD(int, Close, (void*));
    MOCK_METHOD(char*, Error, ());
};

MockLoader* loader = nullptr;
} // namespace

// 使用 ld --wrap 以解决 CI 环境没有 liburma.so 的问题。
extern "C" {
void* __real_dlopen(const char*, int);  // NOLINT
void* __real_dlsym(void*, const char*); // NOLINT
int __real_dlclose(void*);              // NOLINT
char* __real_dlerror();                 // NOLINT

// NOLINTNEXTLINE
void* __wrap_dlopen(const char* path, int flags) {
    return loader ? loader->Open(path, flags) : __real_dlopen(path, flags);
}

// NOLINTNEXTLINE
void* __wrap_dlsym(void* handle, const char* name) {
    return loader ? loader->Symbol(handle, name) : __real_dlsym(handle, name);
}

// NOLINTNEXTLINE
int __wrap_dlclose(void* handle) {
    return loader ? loader->Close(handle) : __real_dlclose(handle);
}

// NOLINTNEXTLINE
char* __wrap_dlerror() {
    return loader ? loader->Error() : __real_dlerror();
}
}

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::StrictMock;
using ::testing::Test;

// 未实际调用的符号只需提供非空地址；会调用的 Init 单独绑定准确签名。
void SymbolAddress() {}

class UrmaApiLoadingTest : public Test {
public:
    void SetUp() override {
        loader = &mock_;
    }

    void TearDown() override {
        // ASSERT 提前返回时也先卸载，再解除 mock_，避免污染后续测试。
        EXPECT_TRUE(UrmaApi::Unload());
        loader = nullptr;
    }

    void ExpectSuccessfulLoad() {
        EXPECT_CALL(mock_, Open(StrEq("liburma.so"), RTLD_NOW | RTLD_GLOBAL)).WillOnce(Return(handle_));
        EXPECT_CALL(mock_, Error()).Times(0);

        // UrmaApi::Init() 总是会成功。
        auto init = +[](urma_init_attr_t*) -> urma_status_t { return URMA_SUCCESS; };
        EXPECT_CALL(mock_, Symbol(handle_, _))
            .Times(49)
            .WillRepeatedly(Return(reinterpret_cast<void*>(&SymbolAddress)));
        // gmock 优先匹配后声明的 EXPECT_CALL，精确匹配应放在通配匹配之后。
        EXPECT_CALL(mock_, Symbol(handle_, StrEq("urma_init"))).WillOnce(Return(reinterpret_cast<void*>(init)));
    }

protected:
    StrictMock<MockLoader> mock_;
    int handle_storage_ = 0;
    void* handle_ = &handle_storage_;
};

class MockUrma {
public:
    MOCK_METHOD(urma_jfr_t*, CreateJfr, (urma_context_t*, urma_jfr_cfg_t*));
    MOCK_METHOD(urma_status_t, DeleteJfr, (urma_jfr_t*));
};

class UrmaApiOverrideTest : public Test {
public:
    void SetUp() override {
        active_mock_ = &mock_;
    }
    void TearDown() override {
        active_mock_ = nullptr;
    }

    UrmaFunctions MakeJfrMockFunctions() {
        return {
            .create_jfr = [](urma_context_t* ctx, urma_jfr_cfg_t* cfg) { return active_mock_->CreateJfr(ctx, cfg); },
            .delete_jfr = [](urma_jfr_t* jfr) { return active_mock_->DeleteJfr(jfr); },
        };
    }

protected:
    static inline MockUrma* active_mock_ = nullptr;
    StrictMock<MockUrma> mock_;
};

// Loading

TEST_F(UrmaApiLoadingTest, MissingLibraryAndInvalidPathAreExplicit) {
    {
        const auto result = UrmaApi::Load(nullptr);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, LoadErrorCode::kInvalidArgument);
        EXPECT_STREQ(result.error().message, "empty library path");
    }
    {
        const auto result = UrmaApi::Load("");
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, LoadErrorCode::kInvalidArgument);
        EXPECT_STREQ(result.error().message, "empty library path");
    }

    char diagnostic[] = "library not found";
    EXPECT_CALL(mock_, Open(_, _)).WillOnce(Return(nullptr));
    EXPECT_CALL(mock_, Error()).WillOnce(Return(diagnostic));

    auto status = UrmaApi::Load();
    ASSERT_FALSE(status);
    EXPECT_EQ(status.error().code, LoadErrorCode::kOpenFailed);
    EXPECT_STREQ(status.error().message, "library not found");
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
    EXPECT_EQ(errno, ENOSYS);
}

TEST_F(UrmaApiLoadingTest, OpenFailureReturnsDiagnostic) {
    EXPECT_CALL(mock_, Open(_, _)).WillOnce(Return(nullptr));
    EXPECT_CALL(mock_, Error()).WillOnce(Return(nullptr));

    // dlopen() 失败，同时 dlerror() 却返回的 nullptr. 什么时候会出现这种情况呢？
    const auto result = UrmaApi::Load();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LoadErrorCode::kOpenFailed);
    EXPECT_STREQ(result.error().message, "dlopen failed");
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
    EXPECT_EQ(errno, ENOSYS);
}

TEST_F(UrmaApiLoadingTest, MissingSymbolReadsDiagnosticAndStopsLoading) {
    char diagnostic[] = "undefined symbol: urma_init";

    ::testing::InSequence sequence;
    EXPECT_CALL(mock_, Open(_, _)).WillOnce(Return(handle_));
    EXPECT_CALL(mock_, Symbol(handle_, StrEq("urma_init"))).WillOnce(Return(nullptr));
    EXPECT_CALL(mock_, Error()).WillOnce(Return(diagnostic));
    EXPECT_CALL(mock_, Close(handle_)).WillOnce(Return(0));

    const auto result = UrmaApi::Load();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, LoadErrorCode::kSymbolMissing);
    EXPECT_STREQ(result.error().message, "urma_init: undefined symbol: urma_init");
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
    EXPECT_EQ(errno, ENOSYS);
}

TEST_F(UrmaApiLoadingTest, MissingLastSymbolRollsBackAndAllowsRetry) {
    EXPECT_CALL(mock_, Open(_, _)).Times(2).WillRepeatedly(Return(handle_));
    EXPECT_CALL(mock_, Error()).WillOnce(Return(nullptr));
    EXPECT_CALL(mock_, Symbol(handle_, _)).Times(96).WillRepeatedly(Return(reinterpret_cast<void*>(&SymbolAddress)));
    auto perf = +[](char*, uint32_t*) -> urma_status_t { return URMA_SUCCESS; };
    EXPECT_CALL(mock_, Symbol(handle_, StrEq("urma_get_perf_info")))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(reinterpret_cast<void*>(perf)));
    auto init = +[](urma_init_attr_t*) -> urma_status_t { return URMA_SUCCESS; };
    EXPECT_CALL(mock_, Symbol(handle_, StrEq("urma_init")))
        .Times(2)
        .WillRepeatedly(Return(reinterpret_cast<void*>(init)));
    EXPECT_CALL(mock_, Close(handle_)).Times(2).WillRepeatedly(Return(0));

    // 第 1 次在处理第 50 个符号 urma_get_perf_info 时 dlsym 会返回 nullptr, 所以失败。
    const auto status = UrmaApi::Load();
    ASSERT_FALSE(status);
    EXPECT_EQ(status.error().code, LoadErrorCode::kSymbolMissing);
    EXPECT_STREQ(status.error().message, "urma_get_perf_info: null symbol address");
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
    EXPECT_EQ(errno, ENOSYS);

    // 第 2 次第 50 个符号 urma_get_perf_info 时也返回了一个有效地址，符号全部解析成功。
    ASSERT_TRUE(UrmaApi::Load());
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_SUCCESS);
    EXPECT_EQ(UrmaApi::GetPerfInfo(nullptr, nullptr), URMA_SUCCESS);

    // 第 3 次调用，由于之前已成功打开, handle_ != nullptr, 直接失败。
    const auto duplicate = UrmaApi::Load();
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, LoadErrorCode::kAlreadyLoaded);
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_SUCCESS);
}

TEST_F(UrmaApiLoadingTest, GlobalApiRestoresLoadedFunctionsAfterOverride) {
    ExpectSuccessfulLoad();
    EXPECT_CALL(mock_, Close(handle_)).WillOnce(Return(0));

    ASSERT_TRUE(UrmaApi::Load());
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_SUCCESS);
    {
        const auto result = UrmaApi::Load();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, LoadErrorCode::kAlreadyLoaded);
    }

    {
        UrmaFunctions functions;
        functions.init = [](urma_init_attr_t*) -> urma_status_t { return URMA_FAIL; };
        test_support::ScopedUrmaOverride scope(functions);
        EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
        {
            const auto result = UrmaApi::Unload();
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error().code, LoadErrorCode::kInUse);
        }
    }

    // 退出替换作用域后恢复真实函数表。
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_SUCCESS);
    EXPECT_TRUE(UrmaApi::Unload());

    // 显式卸载后不再持有 init 函数指针。
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
    EXPECT_EQ(errno, ENOSYS);
}

TEST_F(UrmaApiLoadingTest, ExplicitUnloadStillClosesLibraryAfterCallsStop) {
    ExpectSuccessfulLoad();
    EXPECT_CALL(mock_, Close(handle_)).WillOnce(Return(0));
    ASSERT_TRUE(UrmaApi::Load());
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_SUCCESS);
    EXPECT_TRUE(UrmaApi::Unload());
    EXPECT_TRUE(UrmaApi::Unload());
    EXPECT_EQ(UrmaApi::Init(nullptr), URMA_FAIL);
    EXPECT_EQ(errno, ENOSYS);
}

// Overriding

TEST_F(UrmaApiOverrideTest, BusinessCallsReachMockOnOwnerThread) {
    urma_context_t ctx{};
    urma_jfr_cfg_t cfg{};
    urma_jfr_t jfr{};
    EXPECT_CALL(mock_, CreateJfr(&ctx, &cfg)).WillOnce(Return(&jfr));
    EXPECT_CALL(mock_, DeleteJfr(&jfr)).WillOnce(Return(URMA_SUCCESS));
    test_support::ScopedUrmaOverride scope(MakeJfrMockFunctions());
    // override urma functions 先于线程发生，恢复晚于 join.
    std::thread owner([&] {
        auto* created = UrmaApi::CreateJfr(&ctx, &cfg);
        EXPECT_EQ(created, &jfr);
        EXPECT_EQ(UrmaApi::DeleteJfr(created), URMA_SUCCESS);
    });
    owner.join();
}

TEST_F(UrmaApiOverrideTest, NestedOverrideRestoresOuterTableAndRejectsUnload) {
    urma_jfr_t jfr{};
    EXPECT_CALL(mock_, CreateJfr(nullptr, nullptr)).WillOnce(Return(&jfr));
    {
        test_support::ScopedUrmaOverride outer(MakeJfrMockFunctions());
        {
            const auto result = UrmaApi::Load();
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error().code, LoadErrorCode::kInUse);
        }
        {
            const auto result = UrmaApi::Unload();
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error().code, LoadErrorCode::kInUse);
        }

        {
            UrmaFunctions failure;
            failure.create_jfr = [](urma_context_t*, urma_jfr_cfg_t*) -> urma_jfr_t* {
                errno = ENOMEM;
                return nullptr;
            };
            test_support::ScopedUrmaOverride inner(failure);
            EXPECT_EQ(UrmaApi::CreateJfr(nullptr, nullptr), nullptr);
            EXPECT_EQ(errno, ENOMEM);
        }
        EXPECT_EQ(UrmaApi::CreateJfr(nullptr, nullptr), &jfr);
    }
    EXPECT_EQ(UrmaApi::CreateJfr(nullptr, nullptr), nullptr);
    EXPECT_EQ(errno, ENOSYS);
}

TEST_F(UrmaApiOverrideTest, RestoresTableDuringExceptionUnwind) {
    try {
        test_support::ScopedUrmaOverride scope(MakeJfrMockFunctions());
        throw 1;
    } catch (int e) {
        EXPECT_EQ(e, 1);
    }
    EXPECT_EQ(UrmaApi::CreateJfr(nullptr, nullptr), nullptr);
    EXPECT_EQ(errno, ENOSYS);
    EXPECT_TRUE(UrmaApi::Unload());
}

TEST(UrmaApiUnavailableTest, MissingFunctionsReturnErrorsInsteadOfCallingNull) {
    test_support::ScopedUrmaOverride scope(UrmaFunctions{});
    EXPECT_EQ(UrmaApi::CreateJfr(nullptr, nullptr), nullptr);
    EXPECT_EQ(errno, ENOSYS);
    EXPECT_EQ(UrmaApi::DeleteJfr(nullptr), URMA_FAIL);
    EXPECT_EQ(UrmaApi::PollJfc(nullptr, 0, nullptr), -1);
    errno = 0;
    UrmaApi::FreeEidList(nullptr);
    EXPECT_EQ(errno, ENOSYS);
}

} // namespace
} // namespace raw
} // namespace kbsocket
