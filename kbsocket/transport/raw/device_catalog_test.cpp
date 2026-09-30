// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/device_catalog.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace raw {
namespace {
using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

class MockDiscovery {
public:
    MOCK_METHOD(urma_device_t**, GetDevices, (int*));
    MOCK_METHOD(void, FreeDevices, (urma_device_t**));
    MOCK_METHOD(urma_status_t, QueryDevice, (urma_device_t*, urma_device_attr_t*));
    MOCK_METHOD(urma_eid_info_t*, GetEids, (urma_device_t*, std::uint32_t*));
    MOCK_METHOD(void, FreeEids, (urma_eid_info_t*));
};

template <typename T>
void ExpectError(const std::expected<T, CatalogError>& result, CatalogErrorCode code, int provider_error = 0) {
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, code);
    EXPECT_EQ(result.error().provider_error, provider_error);
}

class DeviceCatalogTest : public ::testing::Test {
public:
    void SetUp() override {
        active_mock_ = &mock_;
        std::strcpy(devices_[0].name, "raw0");
        std::strcpy(devices_[1].name, "raw1");
        std::strcpy(devices_[2].name, "bonding_dev0");
        for (int i = 0; i < 3; ++i) {
            devices_[i].type = URMA_TRANSPORT_UB;
            pointers_[i] = &devices_[i];
        }
        attributes_.dev_cap.max_eid_cnt = 256;
        attributes_.dev_cap.trans_mode = URMA_TM_RM;
        attributes_.dev_cap.rm_tp_cap.bs.ctp = 1;
        // 刻意使用乱序、稀疏的 index，防止把列表下标当作 index。
        eids_[0].eid.raw[0] = 1;
        eids_[0].eid_index = 7;
        eids_[1].eid.raw[0] = 2;
        eids_[1].eid_index = 2;
    }

    void TearDown() override {
        active_mock_ = nullptr;
    }

    UrmaFunctions MakeDiscoveryMockFunctions() {
        return {
            .get_devices = [](int* count) { return active_mock_->GetDevices(count); },
            .free_devices = [](urma_device_t** list) { active_mock_->FreeDevices(list); },
            .get_eids = [](urma_device_t* device,
                           std::uint32_t* count) { return active_mock_->GetEids(device, count); },
            .free_eids = [](urma_eid_info_t* list) { active_mock_->FreeEids(list); },
            .query_device =
                [](urma_device_t* device, urma_device_attr_t* attributes) {
                    return active_mock_->QueryDevice(device, attributes);
                },
        };
    }

    void ExpectEnumeration(int count = 3) {
        EXPECT_CALL(mock_, GetDevices(_)).WillOnce([this, count](int* out) {
            *out = count;
            return pointers_;
        });
        EXPECT_CALL(mock_, FreeDevices(pointers_));
    }

    void ExpectAttributes(int device = 0) {
        EXPECT_CALL(mock_, QueryDevice(&devices_[device], _)).WillOnce([this](auto*, auto* out) {
            *out = attributes_;
            return URMA_SUCCESS;
        });
    }

    void ExpectEids(int device = 0, std::uint32_t count = 2) {
        EXPECT_CALL(mock_, GetEids(&devices_[device], _)).WillOnce([this, count](auto*, auto* out) {
            *out = count;
            return eids_;
        });
        EXPECT_CALL(mock_, FreeEids(eids_));
    }

protected:
    static inline MockDiscovery* active_mock_ = nullptr;
    StrictMock<MockDiscovery> mock_;
    urma_device_t devices_[3]{};
    urma_device_t* pointers_[3]{};
    urma_device_attr_t attributes_{};
    urma_eid_info_t eids_[2]{};
};

TEST_F(DeviceCatalogTest, CachesOwnedSnapshotAndPreservesSparseIndices) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    ExpectEnumeration();
    ExpectAttributes();
    ExpectEids();
    const auto original_eid = eids_[0].eid;
    DeviceCatalog catalog;
    ASSERT_TRUE(catalog.Initialize({"raw0"}));
    ASSERT_EQ(catalog.devices().size(), 1u);
    EXPECT_EQ(catalog.devices()[0].eids[0].eid_index, 2u);
    EXPECT_EQ(catalog.devices()[0].attributes.dev_cap.max_eid_cnt, 256u);
    // 模拟 provider 查询缓冲失效； Catalog 必须持有自己的值拷贝。
    eids_[0] = {};
    // 初始化阶段的查询和释放必须已完成；清空预期后，StrictMock 会拒绝任何新的 provider 调用。
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(&mock_));
    for (int i = 0; i < 100; ++i) {
        auto endpoint = catalog.Find("raw0", 7);
        ASSERT_TRUE(endpoint);
        EXPECT_EQ(endpoint->device, &devices_[0]);
        EXPECT_EQ(endpoint->eid_index, 7u);
        auto by_eid = catalog.Find(original_eid);
        ASSERT_TRUE(by_eid);
        EXPECT_EQ(by_eid->eid_index, 7u);
    }
    // index 1 虽是有效数组下标，却不是已配置的 eid_index，不能误命中。
    ExpectError(catalog.Find("raw0", 1), CatalogErrorCode::kNotFound);
    ExpectError(catalog.Find("missing", 7), CatalogErrorCode::kNotFound);
    ExpectError(catalog.Find(urma_eid_t{}), CatalogErrorCode::kNotFound);
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kAlreadyInitialized);
}

TEST_F(DeviceCatalogTest, RejectsInvalidConfigurationBeforeProviderCalls) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    // 不设置 provider 预期：空配置、重复名称、超长名称及内嵌空字符都应在枚举前被拒绝。
    const std::vector<std::vector<std::string>> cases{
        {}, {""}, {"raw0", "raw0"}, {std::string(URMA_MAX_NAME, 'x')}, {std::string("raw\0x", 5)},
    };
    for (const auto& names : cases) {
        ExpectError(catalog.Initialize(names), CatalogErrorCode::kInvalidArgument);
    }
    // 失败不能把 Catalog 标记为已初始化，两种查询入口都应拒绝访问。
    ExpectError(catalog.Find("raw0", 2), CatalogErrorCode::kNotInitialized);
    ExpectError(catalog.Find(eids_[0].eid), CatalogErrorCode::kNotInitialized);
}

TEST_F(DeviceCatalogTest, RejectsBondingAndNonUbBeforeCapabilityQueries) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    ExpectEnumeration();
    // bonding 与非 UB 设备均可仅凭枚举信息拒绝，不应继续查询能力或 EID。
    ExpectError(catalog.Initialize({"bonding_dev0"}), CatalogErrorCode::kUnsupportedDevice);
    devices_[0].type = URMA_TRANSPORT_IB;
    ExpectEnumeration();
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kUnsupportedDevice);
}

TEST_F(DeviceCatalogTest, ValidatesRmAndCtpBeforeEidQueries) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    // 分别缺少 RM 和 CTP；两项能力必须同时具备，且失败时不得查询 EID。
    for (bool rm : {false, true}) {
        attributes_.dev_cap.trans_mode = rm ? URMA_TM_RM : 0;
        attributes_.dev_cap.rm_tp_cap.bs.ctp = !rm;
        ExpectEnumeration();
        ExpectAttributes();
        ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kUnsupportedTransport);
    }
}

TEST_F(DeviceCatalogTest, FailureDoesNotPublishPartialSnapshotAndAllowsRetry) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    ExpectEnumeration();
    ExpectAttributes();
    ExpectEids();
    // 第 1 次已读完 raw0，但后续设备不存在：必须释放已获取的列表，且不发布 raw0。
    ExpectError(catalog.Initialize({"raw0", "missing"}), CatalogErrorCode::kNotFound);
    EXPECT_TRUE(catalog.devices().empty());
    ExpectError(catalog.Find("raw0", 7), CatalogErrorCode::kNotInitialized);

    // 第 2 次修正白名单，在同一个 Catalog 对象上重新查询并成功初始化。
    ExpectEnumeration();
    ExpectAttributes();
    ExpectEids();
    ASSERT_TRUE(catalog.Initialize({"raw0"}));
}

TEST_F(DeviceCatalogTest, FindsUniqueEidAcrossDevices) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    ExpectEnumeration();
    ExpectAttributes(0);
    ExpectEids(0);

    // 同一 CLAN 内 EID 唯一；第二个设备必须返回另一组 EID。
    urma_eid_info_t second_eids[1]{};
    second_eids[0].eid.raw[0] = 3;
    second_eids[0].eid_index = 9;
    ExpectAttributes(1);
    EXPECT_CALL(mock_, GetEids(&devices_[1], _)).WillOnce([&](auto*, auto* count) {
        *count = 1;
        return second_eids;
    });
    EXPECT_CALL(mock_, FreeEids(second_eids));
    DeviceCatalog catalog;
    ASSERT_TRUE(catalog.Initialize({"raw0", "raw1"}));

    // 查询第二个设备的 EID，验证会跳过无匹配的第一个设备，并返回真实 index。
    auto endpoint = catalog.Find(second_eids[0].eid);
    ASSERT_TRUE(endpoint);
    EXPECT_EQ(endpoint->device, &devices_[1]);
    EXPECT_EQ(endpoint->eid_index, 9u);
    auto first = catalog.Find(eids_[0].eid);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->device, &devices_[0]);
    EXPECT_EQ(first->eid_index, 7u);
}

TEST_F(DeviceCatalogTest, PreservesProviderErrorsAndReleasesAcquiredLists) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    // 第 1 次枚举失败：保留 errno，未获得任何列表，因此不应调用释放接口。
    EXPECT_CALL(mock_, GetDevices(_)).WillOnce([](int*) -> urma_device_t** {
        errno = ENODEV;
        return nullptr;
    });
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kQueryFailed, ENODEV);

    ExpectEnumeration();
    // 第 2 次能力查询失败：保留 URMA 状态码，并释放此前获取的设备列表。
    EXPECT_CALL(mock_, QueryDevice(&devices_[0], _)).WillOnce(Return(URMA_FAIL));
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kQueryFailed, URMA_FAIL);

    ExpectEnumeration();
    ExpectAttributes();
    // 第 3 次 EID 查询失败：保留 EIO，不猜测为无 EID；只释放已获取的设备列表。
    EXPECT_CALL(mock_, GetEids(&devices_[0], _)).WillOnce([](auto*, auto*) -> urma_eid_info_t* {
        errno = EIO;
        return nullptr;
    });
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kQueryFailed, EIO);
    EXPECT_TRUE(catalog.devices().empty());
}

TEST_F(DeviceCatalogTest, ZeroEidCapacityFailsBeforeEidQuery) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    // cap 已经明确报告零容量，不应再调用 GetEids。
    attributes_.dev_cap.max_eid_cnt = 0;
    ExpectEnumeration();
    ExpectAttributes();
    DeviceCatalog catalog;
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kNoEids);
}

TEST_F(DeviceCatalogTest, ExplicitEmptyEidListIsReleased) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    ExpectEnumeration();
    ExpectAttributes();
    // 返回非空列表但数量为零：可明确报告无 EID，仍必须释放返回的列表。
    ExpectEids(0, 0);
    DeviceCatalog catalog;
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kNoEids);
}

TEST_F(DeviceCatalogTest, RejectsInvalidEnumerationAndDuplicateNames) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    // 负数是非法 provider 数据；零数量则是合法空列表，无法找到指定设备。
    ExpectEnumeration(-1);
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kInvalidProviderData);
    ExpectEnumeration(0);
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kNotFound);
    // 即使 raw0 已匹配，列表中的空设备指针也不能被忽略。
    pointers_[1] = nullptr;
    ExpectEnumeration();
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kInvalidProviderData);
    pointers_[1] = &devices_[1];
    // 固定长度名称没有终止符时必须拒绝，避免按 C 字符串读取越界。
    std::memset(devices_[1].name, 'x', URMA_MAX_NAME);
    ExpectEnumeration();
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kInvalidProviderData);
    // 两个设备同名时不得随意选择其中一个。
    std::strcpy(devices_[1].name, "raw0");
    ExpectEnumeration();
    ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kAmbiguous);
}

TEST_F(DeviceCatalogTest, RejectsInvalidEidsAndAlwaysFreesQueryResults) {
    test_support::ScopedUrmaOverride scope(MakeDiscoveryMockFunctions());
    DeviceCatalog catalog;
    const auto original = eids_[1];
    // 每轮只制造一种错误：重复 index、全零 EID、越界 index、超容量数量。
    // 复用 Catalog 验证失败后可重试；每轮的释放预期保证错误分支不泄漏查询结果。
    for (int invalid_case = 0; invalid_case < 4; ++invalid_case) {
        eids_[1] = original;
        if (invalid_case == 0) {
            eids_[1].eid_index = eids_[0].eid_index;
        }
        if (invalid_case == 1) {
            eids_[1].eid = {};
        }
        if (invalid_case == 2) {
            eids_[1].eid_index = 256;
        }
        ExpectEnumeration();
        ExpectAttributes();
        // 数量超出能力值时必须在读取列表元素前拒绝。
        ExpectEids(0, invalid_case == 3 ? 257 : 2);
        ExpectError(catalog.Initialize({"raw0"}), CatalogErrorCode::kInvalidProviderData);
        EXPECT_TRUE(catalog.devices().empty());
    }
}
} // namespace
} // namespace raw
} // namespace kbsocket
