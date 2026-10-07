#include "tools/capabilities/device_info_output.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {
using ::testing::HasSubstr;
TEST(DeviceInfoOutputTest, DecodesCapabilitiesAndPreservesLargeLimits) {
    kbsocket::raw::DeviceRecord device{};
    device.name = "raw0";
    auto& attr = device.attributes;
    auto& cap = attr.dev_cap;
    // 大消息上限不能截断到 32 位；三个 SGE 限制必须独立显示。
    cap.max_msg_size = 1ULL << 33;
    cap.max_read_size = 4096;
    cap.max_write_size = 8192;
    cap.max_jfs_sge = 3;
    cap.max_jfs_rsge = 4;
    cap.max_jfr_sge = 5;
    cap.trans_mode = URMA_TM_RM;
    cap.rm_tp_cap.bs.ctp = 1;
    cap.page_size_cap = 4096 | (1ULL << 21);
    cap.atomic_feat.bs.cas = 1;
    cap.max_cas_size = 8;
    cap.feature.bs.outorder_comp = 1;
    attr.port_cnt = 1;
    attr.port_attr[0] = {.max_mtu = URMA_MTU_8192,
                         .state = URMA_PORT_ACTIVE,
                         .active_width = URMA_LINK_X4,
                         .active_speed = URMA_SP_400G,
                         .active_mtu = URMA_MTU_4096};
    ::testing::internal::CaptureStdout();
    PrintDeviceInfo(device);
    const auto output = ::testing::internal::GetCapturedStdout();
    EXPECT_THAT(output, HasSubstr("rm_ctp=true"));
    EXPECT_THAT(output, HasSubstr("message=8589934592 read=4096 write=8192"));
    EXPECT_THAT(output, HasSubstr("jfs=3 jfs_remote=4 jfr=5"));
    EXPECT_THAT(output, HasSubstr("page_sizes_bytes=[4096, 2097152]"));
    EXPECT_THAT(output, HasSubstr("state=ACTIVE speed=400Gbps width=X4 max_mtu_bytes=8192 active_mtu_bytes=4096"));
    EXPECT_THAT(output, HasSubstr("cas: supported=true max_bytes=8"));
    EXPECT_THAT(output, HasSubstr("outorder_comp=true"));
}
TEST(DeviceInfoOutputTest, PreservesUnknownValuesAndBoundsPortArray) {
    kbsocket::raw::DeviceRecord device{};
    // 畸形端口数量必须截断；未知枚举保留数值，能力零值不能伪装成无限制。
    device.attributes.port_cnt = 255;
    device.attributes.port_attr[0].active_mtu = static_cast<urma_mtu_t>(99);
    ::testing::internal::CaptureStdout();
    PrintDeviceInfo(device);
    const auto output = ::testing::internal::GetCapturedStdout();
    EXPECT_THAT(output, HasSubstr("message=0 read=0 write=0"));
    EXPECT_THAT(output, HasSubstr("active_mtu_bytes=unknown(99)"));
    EXPECT_THAT(output, HasSubstr("warning: port count exceeds attribute array; truncated"));
}
} // namespace
