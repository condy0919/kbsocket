// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_URMA_CONTEXT_HPP_
#define KBSOCKET_TRANSPORT_RAW_URMA_CONTEXT_HPP_

#include <expected>
#include <optional>

#include "kbsocket/transport/raw/device_catalog.hpp"

namespace kbsocket {
namespace raw {
enum class ContextErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kAlreadyOpen,
    kCreateFailed,
    kEndpointChanged,
    kDeleteFailed,
};

/// 上下文操作错误信息，记录失败类型及底层原始错误码。
struct ContextError {
    ContextErrorCode code;
    // 底层错误码：kCreateFailed（urma_create_context 失败）时为系统 errno；
    // kDeleteFailed（urma_delete_context 失败）时为 urma_status_t 状态码；
    // 纯内部校验失败（如参数非法、已打开、EID 变更）时保持为 0。
    int provider_error = 0;
    // EID 校验失败后的自动删除如果也失败，则额外记录其 urma_status_t，不覆盖原始错误。
    std::optional<int> cleanup_error;
};

/// `UrmaContext` 独占底层的 `urma_context_t*` 句柄，不可复制、不可移动。释放 context 之前，所有依赖
/// 该 context 创建的下级资源（队列、内存注册等）必须已先完成释放。显式调用 `Close()` 失败时仍保留底
/// 层句柄所有权，允许调用方排查后重试。析构时尝试执行 `Close()`；若释放失败，将记录错误日志以避免异
/// 常逃逸，底层硬件资源亦不会被静默重置。
class UrmaContext {
public:
    UrmaContext() = default;

    UrmaContext(const UrmaContext&) = delete;
    UrmaContext& operator=(const UrmaContext&) = delete;

    UrmaContext(UrmaContext&& rhs) noexcept = delete;
    UrmaContext& operator=(UrmaContext&& rhs) noexcept = delete;

    ~UrmaContext();

    /// 为指定设备和 EID 索引创建 context，并校验实际生效的 EID 与索引；成功后可通过 `get()` 借用句柄。
    /// 已持有 context 时拒绝重复打开；校验失败立即尝试删除，并始终返回校验错误。
    /// 如果删除也失败时保留所有权，get() 返回 nullptr；cleanup_error 记录删除错误，须 Close 后再 Open。
    std::expected<void, ContextError> Open(const LocalEndpoint& endpoint) noexcept;

    /// 销毁持有的 context 句柄；未持有句柄时直接成功（幂等）。
    /// 若底层删除失败，仍保留句柄所有权，调用方处理失败原因后可重试。
    /// 调用前必须确保已停止所有并发访问并释放关联资源，不得依赖删除失败来检测并发冲突。
    std::expected<void, ContextError> Close() noexcept;

    /// 仅借用已通过端点校验的 context；未创建或校验失败时返回 nullptr。
    urma_context_t* get() const noexcept {
        return verified_ ? ctx_ : nullptr;
    }

    /// 查询当前是否持有底层 context 句柄（无论是否通过校验）。
    bool has_handle() const noexcept {
        return ctx_ != nullptr;
    }

private:
    urma_context_t* ctx_ = nullptr;
    bool verified_ = false;
};

} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_URMA_CONTEXT_HPP_
