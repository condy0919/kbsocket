// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/tx_sequence_window.hpp"

#include <algorithm>
#include <limits>

namespace kbsocket {
namespace connection {
std::expected<void, SequenceError> TxSequenceWindow::Open(SequenceSession session, std::uint32_t initial_seq,
                                                          std::uint64_t initial_bytes) noexcept {
    if (open_) {
        return std::unexpected(SequenceError::kInUse);
    }
    if (!session.peer_runtime || !session.receive_domain || !session.session) {
        return std::unexpected(SequenceError::kInvalidArgument);
    }
    session_ = session;
    next_seq_ = initial_seq;
    accepted_bytes_ = initial_bytes;
    frame_limit_ = initial_seq;
    byte_limit_ = initial_bytes;
    open_ = true;
    return {};
}
std::expected<void, SequenceError> TxSequenceWindow::Update(SequenceSession session, std::uint64_t max_frame_seq,
                                                            std::uint64_t max_stream_bytes) noexcept {
    if (!open_) {
        return std::unexpected(SequenceError::kNotReady);
    }
    if (session != session_) {
        return std::unexpected(SequenceError::kWrongSession);
    }
    if (max_frame_seq > kSequenceEnd) {
        return std::unexpected(SequenceError::kInvalidArgument);
    }
    // 两项都是独立、不可撤销的累计上界；旧更新不能收回或重复授予窗口。
    frame_limit_ = std::max(frame_limit_, max_frame_seq);
    byte_limit_ = std::max(byte_limit_, max_stream_bytes);
    return {};
}
std::expected<SequenceTicket, SequenceError> TxSequenceWindow::Reserve(std::span<const TxFramePlan> plans) noexcept {
    if (!open_) {
        return std::unexpected(SequenceError::kNotReady);
    }
    if (stopped_) {
        return std::unexpected(SequenceError::kStopped);
    }
    if (begin_ != end_) {
        return std::unexpected(SequenceError::kInUse);
    }
    if (plans.empty() || plans.size() > kMaxBatch) {
        return std::unexpected(SequenceError::kInvalidArgument);
    }
    if (generation_ == std::numeric_limits<std::uint64_t>::max()) {
        return std::unexpected(SequenceError::kExhausted);
    }
    auto seq = next_seq_;
    auto bytes = accepted_bytes_;
    for (std::size_t i = 0; i < plans.size(); ++i) {
        const auto& plan = plans[i];
        if ((plan.fin && (plan.bytes || i + 1 != plans.size())) || (!plan.fin && !plan.bytes)) {
            return std::unexpected(SequenceError::kInvalidArgument);
        }
        // UINT32_MAX 留给 FIN；任何数据类型（包括 RNDV 占位）都不得消耗终结序号。
        if (seq >= kSequenceEnd || (!plan.fin && seq == kSequenceEnd - 1) ||
            plan.bytes > std::numeric_limits<std::uint64_t>::max() - bytes) {
            return std::unexpected(SequenceError::kExhausted);
        }
        bytes += plan.bytes;
        if (seq >= frame_limit_ || bytes > byte_limit_) {
            return std::unexpected(SequenceError::kWindowFull);
        }
        frames_[i] = {static_cast<std::uint32_t>(seq++), plan.bytes, plan.fin};
    }
    begin_ = 0;
    end_ = plans.size();
    ++generation_;
    return SequenceTicket{this, generation_};
}
bool TxSequenceWindow::Matches(SequenceTicket ticket) const noexcept {
    return ticket.owner == this && ticket.generation == generation_ && begin_ != end_;
}
bool TxSequenceWindow::OwnsReservation(SequenceTicket ticket) const noexcept {
    return Matches(ticket);
}
std::span<const TxSequencedFrame> TxSequenceWindow::pending() const noexcept {
    return std::span(frames_).subspan(begin_, end_ - begin_);
}
std::expected<void, SequenceError> TxSequenceWindow::Resolve(SequenceTicket ticket,
                                                             std::optional<std::size_t> accepted_prefix) noexcept {
    if (!Matches(ticket)) {
        return std::unexpected(SequenceError::kInvalidTicket);
    }
    if (unknown_) {
        return std::unexpected(SequenceError::kAcceptanceUnknown);
    }
    if (!accepted_prefix) {
        unknown_ = stopped_ = failed_ = true;
        return std::unexpected(SequenceError::kAcceptanceUnknown);
    }
    if (*accepted_prefix > end_ - begin_) {
        return std::unexpected(SequenceError::kInvalidArgument);
    }
    // 即使显式 Fail 也须为之前已发生的可信接受结账，绝不能据此把它当作未提交。
    for (std::size_t i = 0; i < *accepted_prefix; ++i) {
        const auto& frame = frames_[begin_++];
        ++next_seq_;
        accepted_bytes_ += frame.bytes;
        if (frame.fin) {
            fin_ = stopped_ = true;
        }
    }
    return {};
}
std::expected<void, SequenceError> TxSequenceWindow::Cancel(SequenceTicket ticket) noexcept {
    if (!Matches(ticket)) {
        return std::unexpected(SequenceError::kInvalidTicket);
    }
    if (unknown_) {
        return std::unexpected(SequenceError::kAcceptanceUnknown);
    }
    if (begin_) {
        return std::unexpected(SequenceError::kInUse);
    }
    end_ = 0;
    return {};
}
} // namespace connection
} // namespace kbsocket
