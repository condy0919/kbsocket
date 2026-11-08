// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_LISTENER_EVENTS_HPP_
#define KBSOCKET_API_LISTENER_EVENTS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/api/stream_runtime.h"
#include "kbsocket/runtime/progress_source.hpp"

namespace kbsocket {
namespace api {
// Runtime 内部的有界监听通知源；只借用 socket，fd 在 Detach 成功前必须存活且不得更换。
// Open 预分配，所有后续事件操作不分配；唯一 owner，不在回调或其他线程重入。
class ListenerEvents final : public runtime::ProgressSource {
public:
    ListenerEvents() noexcept;
    ~ListenerEvents() override;
    std::expected<void, runtime::ProgressError> Open(std::uint64_t runtime_id, std::uint32_t capacity) noexcept;
    std::expected<void, runtime::ProgressError> Attach(kbs_listener_handle handle, int fd) noexcept;
    std::expected<void, runtime::ProgressError> Detach(kbs_listener_handle handle) noexcept;
    std::expected<void, runtime::ProgressError> Watch(kbs_listener_handle handle, bool enabled) noexcept;
    std::expected<std::size_t, runtime::ProgressError> Poll(std::span<kbs_listener_event> output) noexcept;
    bool pending() const noexcept;
    // 永久停止事件源，清除应用提示；保留注册直到 Detach，Reactor 不再等待其 fd。
    void Stop() noexcept;
    // 必须先从 Reactor 解绑，且全部 borrowed listener 已 Detach。
    std::expected<void, runtime::ProgressError> Close() noexcept;
    std::expected<runtime::ProgressBatch, runtime::ProgressError>
    Progress(runtime::ProgressKind kind, std::size_t budget, std::uint64_t now_ns) noexcept override;
    std::expected<runtime::ProgressBatch, runtime::ProgressError>
    CollectNotifications(std::size_t budget) noexcept override;
    std::expected<void, runtime::ProgressError> Rearm() noexcept override;
    std::span<const int> wait_fds() const noexcept override;
    bool WaitFdEnabled(int fd) const noexcept override;

private:
    struct State;
    std::unique_ptr<State> state_;
};
} // namespace api
} // namespace kbsocket
#endif // KBSOCKET_API_LISTENER_EVENTS_HPP_
