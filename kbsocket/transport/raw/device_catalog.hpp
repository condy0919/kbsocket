// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_DEVICE_CATALOG_HPP_
#define KBSOCKET_TRANSPORT_RAW_DEVICE_CATALOG_HPP_

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <vector>

#include <urma_api.h>

namespace kbsocket {
namespace raw {
enum class CatalogErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kAlreadyInitialized,
    kNotInitialized,
    kNotFound,
    kAmbiguous,
    kUnsupportedDevice,
    kUnsupportedTransport,
    kQueryFailed,
    kNoEids,
    kInvalidProviderData,
    kNoMemory,
};

struct CatalogError {
    CatalogErrorCode code;
    // 枚举/EID 查询失败时为 errno，QueryDevice 失败时为 URMA 状态码。
    int provider_error = 0;
};

struct DeviceRecord {
    std::string name;
    urma_device_t* device = nullptr;
    urma_device_attr_t attributes{};
    // 按实际 eid_index 排序；数组下标不代表 eid_index。
    std::vector<urma_eid_info_t> eids;
};

struct LocalEndpoint {
    urma_device_t* device = nullptr;
    urma_eid_t eid{};
    std::uint32_t eid_index = 0;
};

/// `DeviceCatalog` 为单次 URMA 会话内的只读快照；初始化成功后查询不分配、不调用 provider API. 调用
/// 方需要先 `UrmaApi::Load()` 再 `UrmaApi::Init()`, 再初始化并发布 Catalog. `Initialize()` 不能与
/// `Find()` 并发使用。`DeviceRecord::device` 与 `LocalEndpoint::device` 都是指向的 urma 全局列表中
/// 的对象，生命周期归属于 URMA, 使用期间不能调用 `UrmaApi::Uninit()`.
class DeviceCatalog {
public:
    DeviceCatalog() = default;

    DeviceCatalog(const DeviceCatalog&) = delete;
    DeviceCatalog& operator=(const DeviceCatalog&) = delete;

    /// 非空且无重复的 raw 设备白名单；失败不发布部分目录，允许重试。
    std::expected<void, CatalogError> Initialize(const std::vector<std::string>& device_names) noexcept;

    std::expected<LocalEndpoint, CatalogError> Find(std::string_view device_name,
                                                    std::uint32_t eid_index) const noexcept;

    /// 多个匹配项返回歧义，调用方应改用设备名和 index 定位。
    std::expected<LocalEndpoint, CatalogError> Find(const urma_eid_t& eid) const noexcept;

    const std::vector<DeviceRecord>& devices() const noexcept {
        return devices_;
    }

private:
    std::vector<DeviceRecord> devices_;
    bool initialized_ = false;
};

} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_DEVICE_CATALOG_HPP_
