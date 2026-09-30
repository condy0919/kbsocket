// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_TESTING_SCOPED_URMA_OVERRIDE_HPP_
#define KBSOCKET_TRANSPORT_RAW_TESTING_SCOPED_URMA_OVERRIDE_HPP_

#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
namespace test_support {
/// 测试期间替换整个进程的 URMA 函数表，退出作用域时恢复，支持按栈顺序嵌套。同一进程中的测试不能并行
/// 修改函数表。未提供的函数保持为空，不回退到真实库。替换期间禁止 `UrmaApi::Load()` 和
/// `UrmaApi::Unload()`.
class ScopedUrmaOverride {
public:
    explicit ScopedUrmaOverride(const UrmaFunctions& functions) : saved_functions_(UrmaApi::functions_) {
        UrmaApi::functions_ = functions;
        ++UrmaApi::override_depth_;
    }

    ScopedUrmaOverride(const ScopedUrmaOverride&) = delete;
    ScopedUrmaOverride& operator=(const ScopedUrmaOverride&) = delete;

    ~ScopedUrmaOverride() {
        UrmaApi::functions_ = saved_functions_;
        --UrmaApi::override_depth_;
    }

private:
    UrmaFunctions saved_functions_;
};

} // namespace test_support
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_TESTING_SCOPED_URMA_OVERRIDE_HPP_
