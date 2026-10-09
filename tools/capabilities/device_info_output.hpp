// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TOOLS_CAPABILITIES_DEVICE_INFO_OUTPUT_HPP_
#define KBSOCKET_TOOLS_CAPABILITIES_DEVICE_INFO_OUTPUT_HPP_

#include "kbsocket/transport/raw/device_catalog.hpp"

/// 输出 provider 上报的能力；数值零保持原样，不推断为无限制或不支持。
void PrintDeviceInfo(const kbsocket::raw::DeviceRecord& device);

#endif // KBSOCKET_TOOLS_CAPABILITIES_DEVICE_INFO_OUTPUT_HPP_
