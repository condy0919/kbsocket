// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/connection_io.hpp"

#include <cerrno>
#include <ctime>

namespace kbsocket {
namespace runtime {

std::expected<void, ProgressError> ConnectionIoBinding::Open(ConnectionProgress& progress,
                                                             connection::EagerReceiveDomain& receive,
                                                             RxSourceRegistry& sources) noexcept {
    if (progress_) {
        return std::unexpected(ProgressError{ProgressErrorCode::kInUse});
    }
    if (!receive.buffers() || receive.buffers() != sources.buffers() || !sources.context_incarnation()) {
        return std::unexpected(ProgressError{ProgressErrorCode::kInvalidArgument});
    }
    if (auto added = progress.AddReceiveDomain(receive); !added) {
        return added;
    }
    progress_ = &progress;
    receive_ = &receive;
    sources_ = &sources;
    stats_ = {};
    first_error_.reset();
    return {};
}

std::expected<void, ProgressError> ConnectionIoBinding::Close(bool callbacks_stopped) noexcept {
    if (!progress_) {
        return {};
    }
    if (!callbacks_stopped) {
        return std::unexpected(ProgressError{ProgressErrorCode::kInUse});
    }
    if (const auto removed = progress_->RemoveReceiveDomain(*receive_); !removed) {
        return removed;
    }
    progress_ = nullptr;
    receive_ = nullptr;
    sources_ = nullptr;
    return {};
}

std::unexpected<ConnectionIoError> ConnectionIoBinding::Record(ConnectionIoError error) noexcept {
    ++stats_.failures;
    if (!first_error_) {
        first_error_ = error;
    }
    return std::unexpected(error);
}

std::expected<void, ConnectionIoError> ConnectionIoBinding::OnRx(const raw::RxBufferEvent& event,
                                                                 std::uint64_t now_ns) noexcept {
    if (!progress_) {
        return Record({.code = ConnectionIoErrorCode::kNotReady, .source = {}, .receive = {}, .progress = {}});
    }
    ++stats_.rx_events;
    const auto source = sources_->Resolve(event);
    // Resolve 只确认身份，不消费 Lease。任何拒绝仍须走真实 RQE 的未知来源结账并冻结接收域。
    const auto received = receive_->Dispatch(event, source ? *source : connection::EagerReceiveSource{},
                                             &connection::Connection::DispatchRx, now_ns);
    const auto swept = progress_->RequestSweep();
    if (!source) {
        return Record({.code = ConnectionIoErrorCode::kSource,
                       .source = source.error(),
                       .receive = received ? std::nullopt : std::optional(received.error()),
                       .progress = swept ? std::nullopt : std::optional(swept.error())});
    }
    if (!received) {
        if (received.error().code == connection::EagerRxErrorCode::kRouteRejected) {
            // 已撤回的逻辑 token 仍扣旧 Peer 的物理账并释放；这不证明共享接收域故障。
            ++stats_.stale_route_drops;
        } else {
            return Record({.code = ConnectionIoErrorCode::kReceive,
                           .source = {},
                           .receive = received.error(),
                           .progress = swept ? std::nullopt : std::optional(swept.error())});
        }
    }
    if (!swept) {
        return Record(
            {.code = ConnectionIoErrorCode::kProgress, .source = {}, .receive = {}, .progress = swept.error()});
    }
    return {};
}

std::expected<void, ConnectionIoError> ConnectionIoBinding::OnTx(const raw::TxCompletionEvent& event) noexcept {
    if (!progress_) {
        return Record({.code = ConnectionIoErrorCode::kNotReady, .source = {}, .receive = {}, .progress = {}});
    }
    ++stats_.tx_events;
    if (auto delivered = progress_->OnTxCompletion(event); !delivered) {
        return Record(
            {.code = ConnectionIoErrorCode::kProgress, .source = {}, .receive = {}, .progress = delivered.error()});
    }
    return {};
}

void ConnectionIoBinding::RxCallback(void* cookie, const raw::RxBufferEvent& event) noexcept {
    auto& binding = *static_cast<ConnectionIoBinding*>(cookie);
    timespec now{};
    if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        (void)binding.Record({.code = ConnectionIoErrorCode::kProgress,
                              .source = {},
                              .receive = {},
                              .progress = ProgressError{ProgressErrorCode::kSystem, errno}});
        // 时钟失败仍不可丢掉已取得的 Lease；后续接收失败保留在 first_error 中供 owner 收口。
    }
    (void)binding.OnRx(event, static_cast<std::uint64_t>(now.tv_sec) * 1'000'000'000ull + now.tv_nsec);
}
void ConnectionIoBinding::TxCallback(void* cookie, const raw::TxCompletionEvent& event) noexcept {
    (void)static_cast<ConnectionIoBinding*>(cookie)->OnTx(event);
}

void ConnectionIoBinding::OnError(const ProgressError& error) noexcept {
    (void)Record({.code = progress_ ? ConnectionIoErrorCode::kProgress : ConnectionIoErrorCode::kNotReady,
                  .source = {},
                  .receive = {},
                  .progress = error});
    if (!progress_) {
        return;
    }
    if (receive_->frozen()) {
        // frozen() 同时观察真实 RX Stop/队列依赖隔离；显式 Freeze 将故障传播到 Credit 总账。
        // 它不释放应用持有的 Lease，也不把故障 CQ 当作 WR 已终结的证明。
        receive_->Freeze();
    }
    if (auto swept = progress_->RequestSweep(); !swept) {
        (void)Record(
            {.code = ConnectionIoErrorCode::kProgress, .source = {}, .receive = {}, .progress = swept.error()});
    }
}
void ConnectionIoBinding::ErrorCallback(void* cookie, const ProgressError& error) noexcept {
    static_cast<ConnectionIoBinding*>(cookie)->OnError(error);
}

} // namespace runtime
} // namespace kbsocket
