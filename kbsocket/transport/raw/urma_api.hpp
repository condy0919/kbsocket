// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_URMA_API_HPP_
#define KBSOCKET_TRANSPORT_RAW_URMA_API_HPP_

#include <cerrno>
#include <cstdint>
#include <expected>
#include <type_traits>
#include <utility>

#include <urma_api.h>
#include <urma_perf.h>

namespace kbsocket {
namespace raw {
/// 全部为必需符号，不包含独立 libtpsa 提供的 UVS 接口。
struct UrmaFunctions {
    // 初始化与设备
    decltype(&::urma_init) init = nullptr;
    decltype(&::urma_uninit) uninit = nullptr;
    decltype(&::urma_get_device_list) get_devices = nullptr;
    decltype(&::urma_free_device_list) free_devices = nullptr;
    decltype(&::urma_get_eid_list) get_eids = nullptr;
    decltype(&::urma_free_eid_list) free_eids = nullptr;
    decltype(&::urma_query_device) query_device = nullptr;

    // 上下文
    decltype(&::urma_create_context) create_context = nullptr;
    decltype(&::urma_delete_context) delete_context = nullptr;

    // 完成队列
    decltype(&::urma_create_jfc) create_jfc = nullptr;
    decltype(&::urma_delete_jfc) delete_jfc = nullptr;
    decltype(&::urma_rearm_jfc) rearm_jfc = nullptr;
    decltype(&::urma_poll_jfc) poll_jfc = nullptr;
    decltype(&::urma_wait_jfc) wait_jfc = nullptr;
    decltype(&::urma_ack_jfc) ack_jfc = nullptr;

    // 完成事件通道
    decltype(&::urma_create_jfce) create_jfce = nullptr;
    decltype(&::urma_delete_jfce) delete_jfce = nullptr;

    // 接收队列
    decltype(&::urma_create_jfr) create_jfr = nullptr;
    decltype(&::urma_delete_jfr) delete_jfr = nullptr;
    decltype(&::urma_modify_jfr) modify_jfr = nullptr;

    // Jetty 与收发
    decltype(&::urma_create_jetty) create_jetty = nullptr;
    decltype(&::urma_delete_jetty) delete_jetty = nullptr;
    decltype(&::urma_modify_jetty) modify_jetty = nullptr;
    decltype(&::urma_bind_jetty) bind_jetty = nullptr;
    decltype(&::urma_unbind_jetty) unbind_jetty = nullptr;
    decltype(&::urma_import_jetty) import_jetty = nullptr;
    decltype(&::urma_unimport_jetty) unimport_jetty = nullptr;
    decltype(&::urma_flush_jetty) flush_jetty = nullptr;
    decltype(&::urma_post_jetty_send_wr) post_jetty_send_wr = nullptr;
    decltype(&::urma_post_jetty_recv_wr) post_jetty_recv_wr = nullptr;
    decltype(&::urma_post_jfr_wr) post_jfr_wr = nullptr;
    decltype(&::urma_get_rjetty) get_rjetty = nullptr;
    decltype(&::urma_put_rjetty) put_rjetty = nullptr;

    // 内存段
    decltype(&::urma_register_seg) register_seg = nullptr;
    decltype(&::urma_unregister_seg) unregister_seg = nullptr;
    decltype(&::urma_import_seg) import_seg = nullptr;
    decltype(&::urma_unimport_seg) unimport_seg = nullptr;
    decltype(&::urma_get_seg_ctx) get_seg_ctx = nullptr;
    decltype(&::urma_put_seg_ctx) put_seg_ctx = nullptr;

    // 异步事件
    decltype(&::urma_get_async_event) get_async_event = nullptr;
    decltype(&::urma_ack_async_event) ack_async_event = nullptr;

    // 日志
    decltype(&::urma_log_set_level) log_set_level = nullptr;
    decltype(&::urma_register_log_func) register_log_func = nullptr;
    decltype(&::urma_register_loc_log_func) register_loc_log_func = nullptr;
    decltype(&::urma_unregister_log_func) unregister_log_func = nullptr;

    // 设备控制
    decltype(&::urma_user_ctl) user_ctl = nullptr;

    // EID 转换
    decltype(&::urma_str_to_eid) str_to_eid = nullptr;

    // 性能统计
    decltype(&::urma_start_perf) start_perf = nullptr;
    decltype(&::urma_stop_perf) stop_perf = nullptr;
    decltype(&::urma_get_perf_info) get_perf_info = nullptr;
};

// 函数表不能在退出期引入析构或清空动作，需要显式 `UrmaApi::Unload()`.
static_assert(std::is_trivially_destructible_v<UrmaFunctions>);

enum class LoadErrorCode : std::uint8_t {
    kAlreadyLoaded = 1,
    kInvalidArgument,
    kOpenFailed,
    kSymbolMissing,
    kInUse,
};

/// 错误文本复制到本地，避免 dlerror 缓冲失效；过长信息截断。
struct LoadError {
    LoadErrorCode code;
    char message[512];
};

namespace test_support {
class ScopedUrmaOverride;
} // namespace test_support

/// `UrmaApi` 为进程级调用入口，加载后函数表只读，数据路径不加锁、不分配内存。它不会自动析构，当不确
/// 定上层 worker 停止使用时，不应在静态析构/atexit 中清理、调用 `Unload()`。需要保证
/// `Load()`/`Unload()`必须在所有 `URMA` 调用停止时执行。
class UrmaApi {
public:
    static std::expected<void, LoadError> Load(const char* path = "liburma.so") noexcept;

    static std::expected<void, LoadError> Unload() noexcept;

    static urma_status_t Init(urma_init_attr_t* conf) {
        return Invoke<true>(functions_.init, conf);
    }

    static urma_status_t Uninit() {
        return Invoke<true>(functions_.uninit);
    }

    static urma_device_t** GetDeviceList(int* num_devices) {
        return Invoke(functions_.get_devices, num_devices);
    }

    static void FreeDeviceList(urma_device_t** device_list) {
        return Invoke(functions_.free_devices, device_list);
    }

    static urma_eid_info_t* GetEidList(urma_device_t* dev, uint32_t* cnt) {
        return Invoke(functions_.get_eids, dev, cnt);
    }

    static void FreeEidList(urma_eid_info_t* eid_list) {
        return Invoke(functions_.free_eids, eid_list);
    }

    static urma_status_t QueryDevice(urma_device_t* dev, urma_device_attr_t* dev_attr) {
        return Invoke<true>(functions_.query_device, dev, dev_attr);
    }

    static urma_context_t* CreateContext(urma_device_t* dev, uint32_t eid_index) {
        return Invoke(functions_.create_context, dev, eid_index);
    }

    static urma_status_t DeleteContext(urma_context_t* ctx) {
        return Invoke<true>(functions_.delete_context, ctx);
    }

    static urma_jfc_t* CreateJfc(urma_context_t* ctx, urma_jfc_cfg_t* jfc_cfg) {
        return Invoke(functions_.create_jfc, ctx, jfc_cfg);
    }

    static urma_status_t DeleteJfc(urma_jfc_t* jfc) {
        return Invoke<true>(functions_.delete_jfc, jfc);
    }

    static urma_status_t RearmJfc(urma_jfc_t* jfc, bool solicited_only) {
        return Invoke<true>(functions_.rearm_jfc, jfc, solicited_only);
    }

    static int PollJfc(urma_jfc_t* jfc, int cr_cnt, urma_cr_t* cr) {
        return Invoke(functions_.poll_jfc, jfc, cr_cnt, cr);
    }

    static int WaitJfc(urma_jfce_t* jfce, uint32_t jfc_cnt, int time_out, urma_jfc_t* jfc[]) {
        return Invoke(functions_.wait_jfc, jfce, jfc_cnt, time_out, jfc);
    }

    static void AckJfc(urma_jfc_t* jfc[], uint32_t nevents[], uint32_t jfc_cnt) {
        return Invoke(functions_.ack_jfc, jfc, nevents, jfc_cnt);
    }

    static urma_jfce_t* CreateJfce(urma_context_t* ctx) {
        return Invoke(functions_.create_jfce, ctx);
    }

    static urma_status_t DeleteJfce(urma_jfce_t* jfce) {
        return Invoke<true>(functions_.delete_jfce, jfce);
    }

    static urma_jfr_t* CreateJfr(urma_context_t* ctx, urma_jfr_cfg_t* jfr_cfg) {
        return Invoke(functions_.create_jfr, ctx, jfr_cfg);
    }

    static urma_status_t DeleteJfr(urma_jfr_t* jfr) {
        return Invoke<true>(functions_.delete_jfr, jfr);
    }

    static urma_status_t ModifyJfr(urma_jfr_t* jfr, urma_jfr_attr_t* attr) {
        return Invoke<true>(functions_.modify_jfr, jfr, attr);
    }

    static urma_jetty_t* CreateJetty(urma_context_t* ctx, urma_jetty_cfg_t* jetty_cfg) {
        return Invoke(functions_.create_jetty, ctx, jetty_cfg);
    }

    static urma_status_t DeleteJetty(urma_jetty_t* jetty) {
        return Invoke<true>(functions_.delete_jetty, jetty);
    }

    static urma_status_t ModifyJetty(urma_jetty_t* jetty, urma_jetty_attr_t* attr) {
        return Invoke<true>(functions_.modify_jetty, jetty, attr);
    }

    static urma_status_t BindJetty(urma_jetty_t* jetty, urma_target_jetty_t* tjetty) {
        return Invoke<true>(functions_.bind_jetty, jetty, tjetty);
    }

    static urma_status_t UnbindJetty(urma_jetty_t* jetty) {
        return Invoke<true>(functions_.unbind_jetty, jetty);
    }

    static urma_target_jetty_t* ImportJetty(urma_context_t* ctx, urma_rjetty_t* rjetty, urma_token_t* token_value) {
        return Invoke(functions_.import_jetty, ctx, rjetty, token_value);
    }

    static urma_status_t UnimportJetty(urma_target_jetty_t* tjetty) {
        return Invoke<true>(functions_.unimport_jetty, tjetty);
    }

    static int FlushJetty(urma_jetty_t* jetty, int cr_cnt, urma_cr_t* cr) {
        return Invoke(functions_.flush_jetty, jetty, cr_cnt, cr);
    }

    static urma_status_t PostJettySendWr(urma_jetty_t* jetty, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad_wr) {
        return Invoke<true>(functions_.post_jetty_send_wr, jetty, wr, bad_wr);
    }

    static urma_status_t PostJettyRecvWr(urma_jetty_t* jetty, urma_jfr_wr_t* wr, urma_jfr_wr_t** bad_wr) {
        return Invoke<true>(functions_.post_jetty_recv_wr, jetty, wr, bad_wr);
    }

    static urma_status_t PostJfrWr(urma_jfr_t* jfr, urma_jfr_wr_t* wr, urma_jfr_wr_t** bad_wr) {
        return Invoke<true>(functions_.post_jfr_wr, jfr, wr, bad_wr);
    }

    static urma_status_t GetRjetty(urma_jetty_t* jetty, urma_rjetty_t** rjetty, uint32_t* length) {
        return Invoke<true>(functions_.get_rjetty, jetty, rjetty, length);
    }

    static void PutRjetty(urma_rjetty_t* rjetty) {
        return Invoke(functions_.put_rjetty, rjetty);
    }

    static urma_target_seg_t* RegisterSeg(urma_context_t* ctx, urma_seg_cfg_t* seg_cfg) {
        return Invoke(functions_.register_seg, ctx, seg_cfg);
    }

    static urma_status_t UnregisterSeg(urma_target_seg_t* target_seg) {
        return Invoke<true>(functions_.unregister_seg, target_seg);
    }

    static urma_target_seg_t* ImportSeg(urma_context_t* ctx, urma_seg_t* seg, urma_token_t* token_value, uint64_t addr,
                                        urma_import_seg_flag_t flag) {
        return Invoke(functions_.import_seg, ctx, seg, token_value, addr, flag);
    }

    static urma_status_t UnimportSeg(urma_target_seg_t* tseg) {
        return Invoke<true>(functions_.unimport_seg, tseg);
    }

    static urma_status_t GetSegCtx(urma_target_seg_t* tseg, urma_seg_t** seg, uint32_t* size) {
        return Invoke<true>(functions_.get_seg_ctx, tseg, seg, size);
    }

    static void PutSegCtx(urma_seg_t* seg) {
        return Invoke(functions_.put_seg_ctx, seg);
    }

    static urma_status_t GetAsyncEvent(urma_context_t* ctx, urma_async_event_t* event) {
        return Invoke<true>(functions_.get_async_event, ctx, event);
    }

    static void AckAsyncEvent(urma_async_event_t* event) {
        return Invoke(functions_.ack_async_event, event);
    }

    static void LogSetLevel(urma_vlog_level_t level) {
        return Invoke(functions_.log_set_level, level);
    }

    static urma_status_t RegisterLogFunc(urma_log_cb_t func) {
        return Invoke<true>(functions_.register_log_func, func);
    }

    static urma_status_t RegisterLocLogFunc(urma_loc_log_cb func) {
        return Invoke<true>(functions_.register_loc_log_func, func);
    }

    static urma_status_t UnregisterLogFunc() {
        return Invoke<true>(functions_.unregister_log_func);
    }

    static urma_status_t UserCtl(urma_context_t* ctx, urma_user_ctl_in_t* in, urma_user_ctl_out_t* out) {
        return Invoke<true>(functions_.user_ctl, ctx, in, out);
    }

    static int StrToEid(const char* buf, urma_eid_t* eid) {
        return Invoke(functions_.str_to_eid, buf, eid);
    }

    static urma_status_t StartPerf() {
        return Invoke<true>(functions_.start_perf);
    }

    static urma_status_t StopPerf() {
        return Invoke<true>(functions_.stop_perf);
    }

    static urma_status_t GetPerfInfo(char* perf_buf, uint32_t* length) {
        return Invoke<true>(functions_.get_perf_info, perf_buf, length);
    }

private:
    friend class test_support::ScopedUrmaOverride;

    static void* handle_;
    static UrmaFunctions functions_;
    static unsigned override_depth_;

    /// 未加载或测试未提供该符号时显式失败，不解引用空函数指针。
    /// `urma_status_t` 是 `int` 的别名，状态码与数量返回值必须显式区分。
    template <bool StatusResult = false, typename Function, typename... Args>
    static auto Invoke(const Function& function, Args&&... args) -> std::invoke_result_t<Function, Args...> {
        using Result = std::invoke_result_t<Function, Args...>;
        if (function) {
            return function(std::forward<Args>(args)...);
        }

        errno = ENOSYS;
        if constexpr (std::is_void_v<Result>) {
            return;
        } else if constexpr (std::is_pointer_v<Result>) {
            return nullptr;
        } else if constexpr (StatusResult) {
            return URMA_FAIL;
        } else {
            return -1;
        }
    }
};
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_URMA_API_HPP_
