// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/attempt_ledger.hpp"

#include <limits>
#include <new>
#include <stdexcept>

namespace kbsocket {
namespace raw {
namespace {
auto Error(AttemptLedgerErrorCode code) noexcept {
    return std::unexpected(AttemptLedgerError{
        .code = code,
        .pool_failure = {},
    });
}
auto PoolError(JettyPoolFailure failure) noexcept {
    return std::unexpected(AttemptLedgerError{
        .code = AttemptLedgerErrorCode::kPoolFailure,
        .pool_failure = failure,
    });
}
} // namespace

std::expected<void, AttemptLedgerError> AttemptLedger::Open(JettyPool& pool, std::uint32_t capacity) noexcept {
    if (pool_) {
        return Error(AttemptLedgerErrorCode::kInUse);
    }
    if (!capacity || !pool.ready()) {
        return Error(AttemptLedgerErrorCode::kInvalidArgument);
    }

    try {
        entries_.resize(capacity);
    } catch (const std::bad_alloc&) {
        return Error(AttemptLedgerErrorCode::kNoMemory);
    } catch (const std::length_error&) {
        return Error(AttemptLedgerErrorCode::kNoMemory);
    }
    pool_ = &pool;
    return {};
}

std::expected<void, AttemptLedgerError> AttemptLedger::Close() noexcept {
    if (used_) {
        return Error(AttemptLedgerErrorCode::kInUse);
    }
    entries_.clear();
    pool_ = nullptr;
    return {};
}

std::expected<AttemptId, AttemptLedgerError> AttemptLedger::Prepare(const AttemptMetadata& metadata,
                                                                    std::optional<JettyLane> lane) noexcept {
    if (!pool_) {
        return Error(AttemptLedgerErrorCode::kNotReady);
    }
    if (!metadata.target || !metadata.signaled) {
        return Error(AttemptLedgerErrorCode::kInvalidArgument);
    }
    if (used_ == entries_.size()) {
        return Error(AttemptLedgerErrorCode::kExhausted);
    }

    // id 直接映射固定槽位，跳过未退休的槽；最坏扫描 capacity 次，无哈希表或动态分配。
    // id 不回绕，Close/Open 也不重置，避免迟到完成命中新记录。
    std::size_t index;
    do {
        if (next_id_ == std::numeric_limits<AttemptId>::max()) {
            return Error(AttemptLedgerErrorCode::kExhausted);
        }
        index = ++next_id_ % entries_.size();
    } while (entries_[index].state != State::kFree);

    auto ticket = lane ? pool_->ReserveOn(*lane) : pool_->Reserve();
    if (!ticket) {
        return PoolError(ticket.error());
    }

    entries_[index] = Entry{
        .record =
            AttemptRecord{
                .id = next_id_,
                .ticket = *ticket,
                .metadata = metadata,
                .submission_sequence = 0,
            },
        .state = State::kPrepared,
    };
    ++used_;
    return next_id_;
}

std::expected<std::size_t, AttemptLedgerError> AttemptLedger::Find(AttemptId id) const noexcept {
    if (!pool_) {
        return Error(AttemptLedgerErrorCode::kNotReady);
    }
    const auto index = id % entries_.size();
    if (!id || entries_[index].state == State::kFree || entries_[index].record.id != id) {
        return Error(AttemptLedgerErrorCode::kInvalidId);
    }
    return index;
}

std::expected<AttemptRecord, AttemptLedgerError> AttemptLedger::Lookup(AttemptId id) const noexcept {
    auto index = Find(id);
    if (!index) {
        return std::unexpected(index.error());
    }
    return entries_[*index].record;
}

std::expected<void, AttemptLedgerError> AttemptLedger::Commit(AttemptId id,
                                                              std::uint64_t submission_sequence) noexcept {
    auto index = Find(id);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& entry = entries_[*index];
    if (entry.state != State::kPrepared) {
        return Error(AttemptLedgerErrorCode::kInvalidState);
    }
    auto result = pool_->Commit(entry.record.ticket);
    if (!result) {
        return PoolError(result.error());
    }
    entry.record.submission_sequence = submission_sequence;
    entry.state = State::kPosted;
    ++posted_;
    return {};
}

std::expected<AttemptRecord, AttemptLedgerError> AttemptLedger::Retire(AttemptId id, State state) noexcept {
    auto index = Find(id);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& entry = entries_[*index];
    if (entry.state != state) {
        return Error(AttemptLedgerErrorCode::kInvalidState);
    }
    auto result = state == State::kPosted ? pool_->Complete(entry.record.ticket) : pool_->Cancel(entry.record.ticket);
    if (!result) {
        return PoolError(result.error());
    }
    auto record = entry.record;
    entry = {};
    --used_;
    posted_ -= state == State::kPosted;
    return record;
}

std::expected<AttemptRecord, AttemptLedgerError> AttemptLedger::Cancel(AttemptId id) noexcept {
    return Retire(id, State::kPrepared);
}
std::expected<AttemptRecord, AttemptLedgerError> AttemptLedger::Complete(AttemptId id) noexcept {
    return Retire(id, State::kPosted);
}
} // namespace raw
} // namespace kbsocket
