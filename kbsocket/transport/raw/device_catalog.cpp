// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/device_catalog.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <utility>

#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
namespace {
using DeviceList = std::unique_ptr<urma_device_t*, decltype(&UrmaApi::FreeDeviceList)>;
using EidList = std::unique_ptr<urma_eid_info_t, decltype(&UrmaApi::FreeEidList)>;

bool SameEid(const urma_eid_t& lhs, const urma_eid_t& rhs) noexcept {
    return std::memcmp(lhs.raw, rhs.raw, sizeof(lhs.raw)) == 0;
}

LocalEndpoint MakeEndpoint(const DeviceRecord& device, const urma_eid_info_t& eid) noexcept {
    return {
        .device = device.device,
        .eid = eid.eid,
        .eid_index = eid.eid_index,
    };
}

// 设备名必须是无内嵌空字符的非空字符串，长度小于 URMA_MAX_NAME 且不重复。
std::expected<void, CatalogError> ValidateNames(const std::vector<std::string>& names) noexcept {
    if (names.empty()) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kInvalidArgument,
        });
    }

    for (std::size_t i = 0; i < names.size(); ++i) {
        const auto& name = names[i];
        if (name.empty() || name.size() >= URMA_MAX_NAME || name.find('\0') != std::string::npos) {
            return std::unexpected(CatalogError{
                .code = CatalogErrorCode::kInvalidArgument,
            });
        }

        for (std::size_t j = 0; j < i; ++j) {
            if (name == names[j]) {
                return std::unexpected(CatalogError{
                    .code = CatalogErrorCode::kInvalidArgument,
                });
            }
        }
    }

    return {};
}

std::expected<urma_device_t*, CatalogError> SelectDevice(std::span<urma_device_t* const> devices,
                                                         std::string_view name) noexcept {
    urma_device_t* selected = nullptr;
    for (auto* device : devices) {
        if (!device || !std::memchr(device->name, '\0', URMA_MAX_NAME)) {
            return std::unexpected(CatalogError{
                .code = CatalogErrorCode::kInvalidProviderData,
            });
        }

        if (name == device->name) {
            if (selected) {
                return std::unexpected(CatalogError{
                    .code = CatalogErrorCode::kAmbiguous,
                });
            }
            selected = device;
        }
    }

    if (!selected) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNotFound,
        });
    }

    // 对应当前 URMA 的 bonding 命名契约；不通过私有 provider ops 判断。
    if (selected->type != URMA_TRANSPORT_UB || name.starts_with("bonding_dev")) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kUnsupportedDevice,
        });
    }
    return selected;
}

// 内存分配失败统一由 Initialize 转换为 CatalogError；局部查询结果由 RAII 释放。
std::expected<DeviceRecord, CatalogError> ReadDevice(urma_device_t* device, const std::string& name) {
    DeviceRecord record = {.name = name, .device = device};
    const auto status = UrmaApi::QueryDevice(device, &record.attributes);
    if (status != URMA_SUCCESS) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kQueryFailed,
            .provider_error = static_cast<int>(status),
        });
    }

    // 仅支持 RM_CTP
    const auto& cap = record.attributes.dev_cap;
    if (!(cap.trans_mode & URMA_TM_RM) || !cap.rm_tp_cap.bs.ctp) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kUnsupportedTransport,
        });
    }

    if (cap.max_eid_cnt == 0) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNoEids,
        });
    }

    std::uint32_t count = 0;
    errno = 0;
    EidList eids(UrmaApi::GetEidList(device, &count), &UrmaApi::FreeEidList);
    const int provider_error = errno;
    // 当前 URMA 也以 nullptr/EIO 表示没有 EID，保留原始错误，不猜测失败原因。
    if (!eids) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kQueryFailed,
            .provider_error = provider_error,
        });
    }
    if (count == 0) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNoEids,
        });
    }
    if (count > cap.max_eid_cnt) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kInvalidProviderData,
        });
    }

    const urma_eid_t zero{};
    for (std::uint32_t i = 0; i < count; ++i) {
        if (SameEid(eids.get()[i].eid, zero) || eids.get()[i].eid_index >= cap.max_eid_cnt) {
            return std::unexpected(CatalogError{
                .code = CatalogErrorCode::kInvalidProviderData,
            });
        }
    }

    record.eids.assign(eids.get(), eids.get() + count);
    std::ranges::sort(record.eids, {}, &urma_eid_info_t::eid_index);
    for (std::size_t i = 1; i < record.eids.size(); ++i) {
        if (record.eids[i - 1].eid_index == record.eids[i].eid_index) {
            return std::unexpected(CatalogError{
                .code = CatalogErrorCode::kInvalidProviderData,
            });
        }
    }
    return record;
}
} // namespace

std::expected<void, CatalogError> DeviceCatalog::Initialize(const std::vector<std::string>& names) noexcept {
    if (initialized_) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kAlreadyInitialized,
        });
    }

    if (auto result = ValidateNames(names); !result) {
        return result;
    }

    try {
        int count = 0;
        errno = 0;
        DeviceList list(UrmaApi::GetDeviceList(&count), &UrmaApi::FreeDeviceList);
        const int provider_error = errno;
        if (!list) {
            return std::unexpected(CatalogError{
                .code = CatalogErrorCode::kQueryFailed,
                .provider_error = provider_error,
            });
        }
        if (count < 0) {
            return std::unexpected(CatalogError{
                .code = CatalogErrorCode::kInvalidProviderData,
            });
        }

        const std::span<urma_device_t* const> devices(list.get(), static_cast<std::size_t>(count));
        std::vector<DeviceRecord> pending;
        pending.reserve(names.size());
        for (const auto& name : names) {
            auto selected = SelectDevice(devices, name);
            if (!selected) {
                return std::unexpected(selected.error());
            }

            auto record = ReadDevice(*selected, name);
            if (!record) {
                return std::unexpected(record.error());
            }
            pending.push_back(std::move(*record));
        }
        devices_.swap(pending);
        initialized_ = true;
        return {};
    } catch (const std::bad_alloc&) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNoMemory,
        });
    } catch (const std::length_error&) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNoMemory,
        });
    }
}

std::expected<LocalEndpoint, CatalogError> DeviceCatalog::Find(std::string_view name,
                                                               std::uint32_t index) const noexcept {
    if (!initialized_) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNotInitialized,
        });
    }

    for (const auto& device : devices_) {
        if (device.name != name) {
            continue;
        }

        auto it = std::ranges::lower_bound(device.eids, index, {}, &urma_eid_info_t::eid_index);
        if (it != device.eids.end() && it->eid_index == index) {
            return MakeEndpoint(device, *it);
        }
        break;
    }

    return std::unexpected(CatalogError{
        .code = CatalogErrorCode::kNotFound,
    });
}

std::expected<LocalEndpoint, CatalogError> DeviceCatalog::Find(const urma_eid_t& value) const noexcept {
    if (!initialized_) {
        return std::unexpected(CatalogError{
            .code = CatalogErrorCode::kNotInitialized,
        });
    }

    // 同一 CLAN 内 EID 唯一，首次匹配即可确定本地端点。
    for (const auto& device : devices_) {
        for (const auto& eid : device.eids) {
            if (SameEid(eid.eid, value)) {
                return MakeEndpoint(device, eid);
            }
        }
    }
    return std::unexpected(CatalogError{
        .code = CatalogErrorCode::kNotFound,
    });
}
} // namespace raw
} // namespace kbsocket
