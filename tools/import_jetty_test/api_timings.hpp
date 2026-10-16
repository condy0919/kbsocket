// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TOOLS_IMPORT_JETTY_TEST_API_TIMINGS_HPP_
#define KBSOCKET_TOOLS_IMPORT_JETTY_TEST_API_TIMINGS_HPP_

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <type_traits>
#include <utility>

namespace kbsocket {
namespace tools {
// 固定容量在测量前分配，记录动作位于计时区间之外，不改变被测 API 的返回值和 errno。
// clang-format off
#define KBSOCKET_CONTROL_APIS(X) \
    X(Init, init) \
    X(Uninit, uninit) \
    X(GetDeviceList, get_device_list) \
    X(FreeDeviceList, free_device_list) \
    X(GetEidList, get_eid_list) \
    X(FreeEidList, free_eid_list) \
    X(QueryDevice, query_device) \
    X(CreateContext, create_context) \
    X(DeleteContext, delete_context) \
    X(CreateJfce, create_jfce) \
    X(DeleteJfce, delete_jfce) \
    X(ModifyJfc, modify_jfc) \
    X(CreateJfc, create_jfc) \
    X(DeleteJfc, delete_jfc) \
    X(CreateJfr, create_jfr) \
    X(ModifyJfr, modify_jfr) \
    X(QueryJfr, query_jfr) \
    X(DeleteJfr, delete_jfr) \
    X(CreateJfs, create_jfs) \
    X(ModifyJfs, modify_jfs) \
    X(QueryJfs, query_jfs) \
    X(DeleteJfs, delete_jfs) \
    X(CreateJetty, create_jetty) \
    X(ModifyJetty, modify_jetty) \
    X(QueryJetty, query_jetty) \
    X(DeleteJetty, delete_jetty) \
    X(GetRjetty, get_rjetty) \
    X(PutRjetty, put_rjetty) \
    X(ImportJetty, import_jetty) \
    X(UnimportJetty, unimport_jetty) \
    X(CreateJettyGrp, create_jetty_grp) \
    X(DeleteJettyGrp, delete_jetty_grp) \
    X(AllocTokenId, alloc_token_id) \
    X(FreeTokenId, free_token_id) \
    X(RegisterSeg, register_seg) \
    X(UnregisterSeg, unregister_seg) \
    X(GetSegCtx, get_seg_ctx) \
    X(PutSegCtx, put_seg_ctx) \
    X(ImportSeg, import_seg) \
    X(UnimportSeg, unimport_seg)
// clang-format on

enum class ControlApi {
#define KBSOCKET_API_ENUM(camel, snake) camel,
    KBSOCKET_CONTROL_APIS(KBSOCKET_API_ENUM)
#undef KBSOCKET_API_ENUM
        Count,
};
inline constexpr std::array kControlApiNames{
#define KBSOCKET_API_NAME(camel, snake) "urma_" #snake,
    KBSOCKET_CONTROL_APIS(KBSOCKET_API_NAME)
#undef KBSOCKET_API_NAME
};
#undef KBSOCKET_CONTROL_APIS

struct ApiSample {
    std::uint64_t ns = 0;
    bool success = false;
    int code = 0;
};
struct ApiMeasurements {
    std::array<ApiSample, 512> samples{};
    std::size_t count = 0;
    std::size_t dropped = 0;
    const char* skipped = "not reached in this role/run";
};
class ApiTimings {
public:
    void Record(ControlApi api, std::uint64_t ns, bool success, int code) noexcept;
    void Skip(ControlApi api, const char* reason) noexcept;
    const ApiMeasurements& measurements(ControlApi api) const noexcept;
    bool HasFailures() const noexcept;
    void Print(FILE* output, bool all_samples) const;

    template <typename F>
    auto Call(ControlApi api, F&& function) -> std::invoke_result_t<F> {
        using Result = std::invoke_result_t<F>;
        using Clock = std::chrono::steady_clock;
        errno = 0;
        const auto begin = Clock::now();
        if constexpr (std::is_void_v<Result>) {
            std::forward<F>(function)();
            const int saved_errno = errno;
            const auto end = Clock::now();
            Record(api, std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count(), true, 0);
            errno = saved_errno;
        } else {
            auto result = std::forward<F>(function)();
            const int saved_errno = errno;
            const auto end = Clock::now();
            bool success;
            int code;
            if constexpr (std::is_pointer_v<Result>) {
                success = result != nullptr;
                code = success ? 0 : saved_errno;
            } else {
                // 本统计器仅用于状态码 API，不用于返回数量的 poll/wait 等数据面接口。
                success = result == 0;
                code = static_cast<int>(result);
            }
            Record(api, std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count(), success, code);
            errno = saved_errno;
            return result;
        }
    }

private:
    std::array<ApiMeasurements, static_cast<std::size_t>(ControlApi::Count)> rows_{};
};

template <typename F>
auto MeasureApi(ApiTimings* timings, ControlApi api, F&& function) -> std::invoke_result_t<F> {
    if (timings) {
        return timings->Call(api, std::forward<F>(function));
    }
    return std::forward<F>(function)();
}
} // namespace tools
} // namespace kbsocket
#endif // KBSOCKET_TOOLS_IMPORT_JETTY_TEST_API_TIMINGS_HPP_
