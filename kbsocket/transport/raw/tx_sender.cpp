// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/tx_sender.hpp"

#include <algorithm>
#include <limits>

namespace kbsocket {
namespace raw {
namespace {
auto Error(TxSendErrorCode code) noexcept {
    return std::unexpected(TxSendError{
        .code = code,
    });
}
} // namespace

std::expected<void, TxSendError> TxSender::Open(AttemptLedger& ledger) noexcept {
    if (ledger_) {
        return Error(TxSendErrorCode::kInUse);
    }
    if (!ledger.pool() || !ledger.pool()->ready()) {
        return Error(TxSendErrorCode::kInvalidArgument);
    }
    ledger_ = &ledger;
    pool_ = ledger.pool();
    blocked_ = false;
    return {};
}

std::expected<void, TxSendError> TxSender::Close() noexcept {
    ledger_ = nullptr;
    pool_ = nullptr;
    return {};
}

std::optional<AttemptLedgerError> TxSender::Cancel(std::span<AttemptId> ids) noexcept {
    std::optional<AttemptLedgerError> first_error;
    for (auto& id : ids) {
        if (!id) {
            continue;
        }
        auto result = ledger_->Cancel(id);
        if (result) {
            id = 0;
        } else if (!first_error) {
            first_error = result.error();
        }
    }
    return first_error;
}

std::expected<std::size_t, TxSendError> TxSender::Send(std::span<const TxSendRequest> requests,
                                                       std::span<AttemptId> ids,
                                                       std::optional<JettyLane> lane) noexcept {
    if (!ledger_ || ledger_->pool() != pool_ || !pool_->ready() || blocked_) {
        return Error(TxSendErrorCode::kNotReady);
    }
    if (requests.empty() || requests.size() > kMaxBatch || ids.size() < requests.size()) {
        return Error(TxSendErrorCode::kInvalidArgument);
    }

    ids = ids.first(requests.size());
    std::ranges::fill(ids, 0);
    for (const auto& request : requests) {
        if (!request.metadata.target || request.metadata.opcode != URMA_OPC_SEND || !request.metadata.signaled ||
            request.sges.empty() || request.sges.size() > std::numeric_limits<std::uint32_t>::max()) {
            return Error(TxSendErrorCode::kInvalidArgument);
        }
        for (const auto& sge : request.sges) {
            if (!sge.tseg || !sge.addr || !sge.len) {
                return Error(TxSendErrorCode::kInvalidArgument);
            }
        }
    }
    if (requests.size() > std::numeric_limits<std::uint64_t>::max() - next_sequence_) {
        return Error(TxSendErrorCode::kExhausted);
    }

    // 所有失败清理都保留未能撤销的 id；不能静默遗失其资源归属。
    auto rollback = [&](TxSendError error) -> std::expected<std::size_t, TxSendError> {
        if (auto cleanup = Cancel(ids)) {
            error.code = TxSendErrorCode::kAccountingFailure;
            error.ledger_error = cleanup;
            blocked_ = true;
        }
        return std::unexpected(error);
    };
    for (std::size_t i = 0; i < requests.size(); ++i) {
        auto id = ledger_->Prepare(requests[i].metadata, lane);
        if (!id) {
            return rollback(TxSendError{
                .code = TxSendErrorCode::kLedgerFailure,
                .ledger_error = id.error(),
            });
        }
        ids[i] = *id;
        if (!lane) {
            auto record = ledger_->Lookup(*id);
            if (!record) {
                return rollback(TxSendError{
                    .code = TxSendErrorCode::kLedgerFailure,
                    .ledger_error = record.error(),
                });
            }
            lane = record->ticket.lane;
        }
        auto& wr = wrs_[i];
        wr = {};
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.tjetty = requests[i].metadata.target;
        wr.user_ctx = *id;
        wr.send.src.sge = requests[i].sges.data();
        wr.send.src.num_sge = static_cast<std::uint32_t>(requests[i].sges.size());
        wr.next = i + 1 < requests.size() ? &wrs_[i + 1] : nullptr;
    }
    auto jetty = pool_->Get(*lane);
    if (!jetty) {
        return rollback(TxSendError{
            .code = TxSendErrorCode::kPoolFailure,
            .pool_error = jetty.error(),
        });
    }
    urma_jfs_wr_t* bad_wr = nullptr;
    const auto status = UrmaApi::PostJettySendWr(*jetty, wrs_.data(), &bad_wr);
    std::size_t accepted = requests.size();
    if (status != URMA_SUCCESS) {
        // 仅比较指针，不对可能属于其他对象的 bad_wr 做减法或解引用。
        for (std::size_t i = 0; i < requests.size(); ++i) {
            if (bad_wr == &wrs_[i]) {
                accepted = i;
                break;
            }
        }
    }
    if ((status != URMA_SUCCESS && accepted == requests.size()) || (status == URMA_SUCCESS && bad_wr)) {
        blocked_ = true;
        auto fault = pool_->MarkFaulted(*lane);
        return std::unexpected(TxSendError{
            .code = TxSendErrorCode::kProviderContract,
            .accepted = std::nullopt,
            .provider_status = status,
            .pool_error = fault ? std::nullopt : std::optional(fault.error()),
        });
    }
    // 所有已接受 WR 先尝试登记；单条记账失败也不能撤销其硬件已接受的请求。
    std::optional<AttemptLedgerError> accounting_error;
    for (std::size_t i = 0; i < accepted; ++i) {
        auto result = ledger_->Commit(ids[i], ++next_sequence_);
        if (!result && !accounting_error) {
            accounting_error = result.error();
        }
    }
    if (auto cleanup = Cancel(ids.subspan(accepted)); cleanup && !accounting_error) {
        accounting_error = cleanup;
    }
    if (accounting_error) {
        blocked_ = true;
        auto fault = pool_->MarkFaulted(*lane);
        return std::unexpected(TxSendError{
            .code = TxSendErrorCode::kAccountingFailure,
            .accepted = accepted,
            .provider_status = status,
            .ledger_error = accounting_error,
            .pool_error = fault ? std::nullopt : std::optional(fault.error()),
        });
    }
    if (status != URMA_SUCCESS) {
        return std::unexpected(TxSendError{
            .code = TxSendErrorCode::kPostFailed,
            .accepted = accepted,
            .provider_status = status,
        });
    }
    return accepted;
}
} // namespace raw
} // namespace kbsocket
