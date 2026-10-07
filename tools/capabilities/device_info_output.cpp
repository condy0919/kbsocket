#include "tools/capabilities/device_info_output.hpp"

#include <algorithm>
#include <cstdint>
#include <format>
#include <iterator>
#include <print>
#include <string>

namespace {
std::string Mtu(urma_mtu_t value) {
    if (value >= URMA_MTU_256 && value <= URMA_MTU_8192) {
        return std::to_string(128u << static_cast<unsigned>(value));
    }
    return std::format("unknown({})", static_cast<unsigned>(value));
}

std::string Speed(urma_speed_t value) {
    constexpr const char* names[] = {"10Mbps", "100Mbps", "1Gbps",  "2.5Gbps", "5Gbps",   "10Gbps",  "14Gbps",
                                     "25Gbps", "40Gbps",  "50Gbps", "100Gbps", "200Gbps", "400Gbps", "800Gbps"};
    const auto index = static_cast<unsigned>(value);
    return index < std::size(names) ? names[index] : std::format("unknown({})", index);
}

std::string State(urma_port_state_t value) {
    constexpr const char* names[] = {"NOP", "DOWN", "INIT", "ARMED", "ACTIVE", "ACTIVE_DEFER"};
    const auto index = static_cast<unsigned>(value);
    return index < std::size(names) ? names[index] : std::format("unknown({})", index);
}

std::string Width(urma_link_width_t value) {
    switch (value) {
    case URMA_LINK_X1:
        return "X1";
    case URMA_LINK_X2:
        return "X2";
    case URMA_LINK_X4:
        return "X4";
    case URMA_LINK_X8:
        return "X8";
    case URMA_LINK_X16:
        return "X16";
    case URMA_LINK_X32:
        return "X32";
    default:
        return std::format("unknown({})", static_cast<unsigned>(value));
    }
}

void PrintTp(const char* mode, urma_tp_type_cap_t cap) {
    std::println("    {}: rtp={} ctp={} utp={} raw=0x{:x}", mode, static_cast<bool>(cap.bs.rtp),
                 static_cast<bool>(cap.bs.ctp), static_cast<bool>(cap.bs.utp), cap.value);
}
} // namespace

void PrintDeviceInfo(const kbsocket::raw::DeviceRecord& device) {
    const auto& attr = device.attributes;
    const auto& cap = attr.dev_cap;
    std::println("device={} (provider-reported limits, not available resources)", device.name);
    std::print("  guid=");
    for (auto byte : attr.guid.raw) {
        std::print("{:02x}", static_cast<unsigned>(byte));
    }
    std::println();
    std::println("  transport: rm={} rc={} um={} raw=0x{:x} rm_ctp={}", static_cast<bool>(cap.trans_mode & URMA_TM_RM),
                 static_cast<bool>(cap.trans_mode & URMA_TM_RC), static_cast<bool>(cap.trans_mode & URMA_TM_UM),
                 cap.trans_mode, static_cast<bool>((cap.trans_mode & URMA_TM_RM) && cap.rm_tp_cap.bs.ctp));
    PrintTp("rm", cap.rm_tp_cap);
    PrintTp("rc", cap.rc_tp_cap);
    PrintTp("um", cap.um_tp_cap);
    std::println("  max_resources: jetty={} jfs={} jfr={} jfc={} ceq_cnt={}", cap.max_jetty, cap.max_jfs, cap.max_jfr,
                 cap.max_jfc, cap.ceq_cnt);
    std::println("  max_depth: jfs={} jfr={} jfc={}", cap.max_jfs_depth, cap.max_jfr_depth, cap.max_jfc_depth);
    std::println("  max_bytes: message={} read={} write={} inline={}", cap.max_msg_size, cap.max_read_size,
                 cap.max_write_size, cap.max_jfs_inline_len);
    std::println("  max_sge: jfs={} jfs_remote={} jfr={}", cap.max_jfs_sge, cap.max_jfs_rsge, cap.max_jfr_sge);
    std::println("  eids: max={} enumerated={}", cap.max_eid_cnt, device.eids.size());
    std::print("  page_size_cap=0x{:x} page_sizes_bytes=[", cap.page_size_cap);
    bool first = true;
    // 页大小能力是位图，每个置位的位值对应一个支持的页大小。
    for (unsigned bit = 0; bit < 64; ++bit) {
        const auto size = std::uint64_t{1} << bit;
        if (cap.page_size_cap & size) {
            std::print("{}{}", first ? "" : ", ", size);
            first = false;
        }
    }
    std::println("]");
    std::println("  ports: count={}", attr.port_cnt);
    // provider 数据不能使工具越界访问固定大小的端口数组。
    for (std::size_t i = 0; i < std::min<std::size_t>(attr.port_cnt, std::size(attr.port_attr)); ++i) {
        const auto& port = attr.port_attr[i];
        std::println("    port_index={} state={} speed={} width={} max_mtu_bytes={} active_mtu_bytes={}", i,
                     State(port.state), Speed(port.active_speed), Width(port.active_width), Mtu(port.max_mtu),
                     Mtu(port.active_mtu));
    }
    if (attr.port_cnt > std::size(attr.port_attr)) {
        std::println("    warning: port count exceeds attribute array; truncated");
    }
    std::println("  atomic: raw=0x{:x} (supported, max_bytes)", cap.atomic_feat.value);
    std::println("    cas: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.cas), cap.max_cas_size);
    std::println("    swap: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.swap), cap.max_swap_size);
    std::println("    fetch_and_add: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.fetch_and_add),
                 cap.max_fetch_and_add_size);
    std::println("    fetch_and_sub: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.fetch_and_sub),
                 cap.max_fetch_and_sub_size);
    std::println("    fetch_and_and: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.fetch_and_and),
                 cap.max_fetch_and_and_size);
    std::println("    fetch_and_or: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.fetch_and_or),
                 cap.max_fetch_and_or_size);
    std::println("    fetch_and_xor: supported={} max_bytes={}", static_cast<bool>(cap.atomic_feat.bs.fetch_and_xor),
                 cap.max_fetch_and_xor_size);
    std::println("  rm_order: ot={} oi={} ol={} no={} raw=0x{:x}", static_cast<bool>(cap.rm_order_cap.bs.ot),
                 static_cast<bool>(cap.rm_order_cap.bs.oi), static_cast<bool>(cap.rm_order_cap.bs.ol),
                 static_cast<bool>(cap.rm_order_cap.bs.no), cap.rm_order_cap.value);
    std::println("  features: oor={} outorder_comp={} raw=0x{:x}", static_cast<bool>(cap.feature.bs.oor),
                 static_cast<bool>(cap.feature.bs.outorder_comp), cap.feature.value);
}
