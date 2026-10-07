// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_RAW_RUNTIME_HPP_
#define KBSOCKET_TRANSPORT_RAW_RAW_RUNTIME_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kbsocket/transport/raw/device_catalog.hpp"
#include "kbsocket/transport/raw/urma_context.hpp"

namespace kbsocket {
namespace raw {
/// 单个 Raw 设备的配置项，指定设备名称及其绑定的本地 EID 索引。
struct DeviceConfig {
    std::string device_name;
    std::uint32_t eid_index = 0;
};

/// 运行时初始化错误类型。
enum class RuntimeErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kInUse,
    kNoMemory,
    kInitFailed,
    kCatalogFailed,
    kContextFailed,
};

/// 运行时关闭错误类型。
enum class RuntimeCloseErrorCode : std::uint8_t {
    kContextFailed = 1,
    kUninitFailed,
};

/// 运行时关闭失败的详细错误信息。
struct RuntimeCloseError {
    RuntimeCloseErrorCode code;
    std::size_t device_index = 0;
    // 底层 URMA 状态码（如 context 关闭失败或 urma_uninit 失败）。
    int provider_error = 0;
};

/// 运行时初始化失败的详细错误信息。
struct RuntimeError {
    RuntimeErrorCode code;
    std::size_t device_index = 0;
    // 底层 URMA 状态码（如 urma_init 失败时）。
    int provider_error = 0;
    std::optional<CatalogError> catalog_error;
    std::optional<ContextError> context_error;
    // 初始化失败触发回滚时，若清理过程中亦发生错误，在此记录回滚错误，避免覆盖最初的失败根因。
    std::optional<RuntimeCloseError> cleanup_error;
};

/// 单个 URMA 会话的核心所有者，管理设备的目录枚举与 context 创建（每个配置设备绑定一个 EID/context）。
///
/// 生命周期与使用契约：
/// - 宿主须在初始化前完成 `UrmaApi::Load()`；本类负责驱动 `UrmaApi::Init()` 与 `UrmaApi::Uninit()`；
///   只有在 `Close()` 成功后，宿主才可安全调用 `UrmaApi::Unload()`。
/// - 进程级独占：同一进程内同时仅允许一个活跃的 `RawRuntime`，外部不得并发调用 Init/Uninit 或枚举设备。
/// - 并发模型：所有生命周期操作（`Initialize()`/`Close()`）须由控制线程串行执行；初始化完成后支持多
///   线程并发读取 context。
/// - 资源清理：调用 `Close()` 前，必须确保已停止所有借用者，并释放全部关联队列、内存注册及在途传输；
///   本类不提供 worker 停机或设备热迁移协议。进程级常驻单例建议使用 `GetRawRuntime()`。
class RawRuntime {
public:
    RawRuntime() = default;

    RawRuntime(const RawRuntime&) = delete;
    RawRuntime& operator=(const RawRuntime&) = delete;

    ~RawRuntime();

    /// 初始化 URMA 会话、枚举设备目录并为配置的设备依次打开 context。
    /// 任一环节失败均会自动执行回滚，并在返回的错误中保留原始失败原因及回滚状态。
    std::expected<void, RuntimeError> Initialize(const std::vector<DeviceConfig>& devices) noexcept;

    /// 逆序关闭所有 context 并终止 URMA 会话（Uninit）。
    /// - 若某一 context 关闭失败，中断后续释放并保留剩余所有权，供调用方排查原因后重试；
    /// - 若 context 全部成功关闭但 `UrmaApi::Uninit()` 报错，底层会话已不可逆终止，返回错误但不会重复 Uninit。
    std::expected<void, RuntimeCloseError> Close() noexcept;

    /// 查询运行时是否已完成初始化并处于就绪状态。
    bool ready() const noexcept {
        return ready_;
    }

    /// 获取成功打开的 context 数量；未就绪时返回 0。
    std::size_t device_count() const noexcept {
        return ready_ ? contexts_.size() : 0;
    }

    /// 获取指定索引的底层 context 句柄；未就绪或索引越界时返回 nullptr。
    urma_context_t* context(std::size_t index) const noexcept {
        return ready_ && index < contexts_.size() ? contexts_[index]->get() : nullptr;
    }

private:
    std::expected<void, RuntimeError> Rollback(RuntimeError error) noexcept;

    std::unique_ptr<DeviceCatalog> catalog_;
    std::vector<std::unique_ptr<UrmaContext>> contexts_;
    bool claimed_ = false;
    bool initialized_ = false;
    bool ready_ = false;
};

/// 获取常驻内存、不自动析构的进程级 `RawRuntime` 单例。
/// - 本函数不会隐式调用 `UrmaApi::Load()` 或 `Initialize()`，需由控制线程显式初始化。
/// - 使用 `NoDestructor` 包装：在无法严格保证所有 worker 线程已退出的场景下，进程退出时不会自动
///   触发 Close/Uninit/Unload，避免静态析构顺序错乱导致崩溃。
/// - 若显式调用 `Close()`，仍须保证所有借用者已停止；承载该实例的动态库（DSO）不得提前卸载。
RawRuntime& GetRawRuntime() noexcept;
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_RAW_RUNTIME_HPP_
