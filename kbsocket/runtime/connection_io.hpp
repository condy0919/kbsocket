// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_CONNECTION_IO_HPP_
#define KBSOCKET_RUNTIME_CONNECTION_IO_HPP_

#include <cstdint>
#include <expected>
#include <optional>

#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/rx_source_registry.hpp"

namespace kbsocket {
namespace runtime {

enum class ConnectionIoErrorCode : std::uint8_t { kNotReady, kSource, kReceive, kProgress };
struct ConnectionIoError {
    ConnectionIoErrorCode code;
    std::optional<RxSourceError> source;
    std::optional<connection::EagerRxError> receive;
    std::optional<ProgressError> progress;
};
struct ConnectionIoStats {
    std::uint64_t rx_events = 0, tx_events = 0, stale_route_drops = 0, failures = 0;
};

/// 将唯一 raw poll owner 的完成回调接入连接调度器；不拥有 raw/Credit/Connection 或来源表。
/// Open 在控制路径注册外部补投域；RawQueueBinding 必须接入 TX/RX/Error 三种回调，
/// 使用本对象为 cookie，并置 external_refill=true。
/// 整个 context 的 raw、连接和 import 源必须共用暂停屏障；回调与公开方法只由同一 owner 调用。
/// 来源表未命中也要消费真实 RX Lease，不能在回调中直接丢弃事件或绕过物理 Credit 结账。
/// 借用依赖必须活过最后一次回调；本对象不终结在途 DMA，也不因记录错误自动删除共享队列。
class ConnectionIoBinding {
public:
    std::expected<void, ProgressError> Open(ConnectionProgress& progress, connection::EagerReceiveDomain& receive,
                                            RxSourceRegistry& sources) noexcept;
    /// 宿主已解绑全部 raw 回调且停止相关外部访问后，在 owner 控制路径移除补投注册和借用。
    /// false 不能由空 CQ/空账本代替；Close 不归还 Lease、不撤回授信、不关闭 raw 资源。
    /// 已解绑时幂等；成功后可重新 Open，新绑定重新开始诊断计数。
    std::expected<void, ProgressError> Close(bool callbacks_stopped) noexcept;
    std::expected<void, ConnectionIoError> OnRx(const raw::RxBufferEvent& event, std::uint64_t now_ns) noexcept;
    std::expected<void, ConnectionIoError> OnTx(const raw::TxCompletionEvent& event) noexcept;
    /// raw 域停止/隔离必须同步冻结物理 Credit，不能仅停止底层 refill 而继续给对端授信。
    void OnError(const ProgressError& error) noexcept;
    static void RxCallback(void* cookie, const raw::RxBufferEvent& event) noexcept;
    static void TxCallback(void* cookie, const raw::TxCompletionEvent& event) noexcept;
    static void ErrorCallback(void* cookie, const ProgressError& error) noexcept;
    const ConnectionIoStats& stats() const noexcept {
        return stats_;
    }
    const std::optional<ConnectionIoError>& first_error() const noexcept {
        return first_error_;
    }

private:
    std::unexpected<ConnectionIoError> Record(ConnectionIoError error) noexcept;
    ConnectionProgress* progress_ = nullptr;
    connection::EagerReceiveDomain* receive_ = nullptr;
    RxSourceRegistry* sources_ = nullptr;
    ConnectionIoStats stats_;
    std::optional<ConnectionIoError> first_error_;
};

} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_CONNECTION_IO_HPP_
