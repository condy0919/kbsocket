// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/connection/reorder_window.hpp"

#include <algorithm>
#include <limits>
#include <new>

namespace kbsocket {
namespace connection {
namespace {
constexpr std::uint64_t kSequenceEnd = std::uint64_t{1} << 32;
auto Error(ReorderErrorCode code) noexcept {
    return std::unexpected(ReorderError{code});
}
bool ValidLease(const ReorderLease& lease) noexcept {
    if (const auto* raw = std::get_if<raw::RxBufferLease>(&lease)) {
        return raw->pool && raw->id;
    }
    if (const auto* memory = std::get_if<memory::BufferLease>(&lease)) {
        return memory->allocator && memory->generation;
    }
    return false;
}
bool SameLease(const ReorderLease& left, const ReorderLease& right) noexcept {
    if (left.index() != right.index()) {
        return false;
    }
    if (const auto* raw = std::get_if<raw::RxBufferLease>(&left)) {
        const auto& other = std::get<raw::RxBufferLease>(right);
        return raw->pool == other.pool && raw->id == other.id;
    }
    if (const auto* memory = std::get_if<memory::BufferLease>(&left)) {
        return *memory == std::get<memory::BufferLease>(right);
    }
    return false;
}
} // namespace

struct ReorderWindow::State {
    enum class Status : std::uint8_t { kEmpty, kBuffered, kDelivered, kConsumed };
    struct Entry {
        ReorderFrame frame;
        std::uint64_t receipt = 0;
        Status status = Status::kEmpty;
        bool discarded = false;
    };
    ReorderSession session;
    ReorderConfig config;
    std::unique_ptr<Entry[]> entries;
    ReorderStats stats;
    std::optional<ReorderError> failure;
    std::optional<std::uint32_t> fin;
    std::optional<std::uint64_t> gap_since;
    std::uint64_t last_now = 0;
    std::uint32_t drain_cursor = 0;
    bool stopping = false;
};

ReorderWindow::ReorderWindow() noexcept = default;
ReorderWindow::~ReorderWindow() {
    if (!Close()) {
        // 未归还时保留描述符账本，不替宿主释放仍被借用的真实 Lease/backing。
        (void)state_.release();
    }
}
std::expected<void, ReorderError> ReorderWindow::Open(ReorderSession session, const ReorderConfig& config) noexcept {
    if (state_) {
        return Error(ReorderErrorCode::kInUse);
    }
    if (!session.peer_runtime || !session.receive_domain || !session.session || !session.generation || !config.window ||
        config.window > kMaxWindow || !config.max_resident_bytes || !config.gap_timeout_ns) {
        return Error(ReorderErrorCode::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(ReorderErrorCode::kNoMemory);
    }
    state->entries.reset(new (std::nothrow) State::Entry[config.window]);
    if (!state->entries) {
        return Error(ReorderErrorCode::kNoMemory);
    }
    state->session = session;
    state->config = config;
    state->stats.rcv_next = state->stats.released_prefix = config.initial_sequence;
    state_ = std::move(state);
    return {};
}
std::expected<void, ReorderError> ReorderWindow::Close() noexcept {
    if (!state_) {
        return {};
    }
    state_->stopping = true;
    if (state_->stats.buffered_frames || state_->stats.delivered_frames) {
        return Error(ReorderErrorCode::kInUse);
    }
    state_.reset();
    return {};
}
void ReorderWindow::StopAccepting() noexcept {
    if (state_) {
        state_->stopping = true;
    }
}
void ReorderWindow::Fail() noexcept {
    if (state_ && !state_->failure) {
        state_->failure = ReorderError{ReorderErrorCode::kFailed};
    }
}
bool ReorderWindow::accepting() const noexcept {
    return state_ && !state_->failure && !state_->stopping && state_->stats.rcv_next < kSequenceEnd &&
           (!state_->fin || state_->stats.rcv_next <= *state_->fin);
}
bool ReorderWindow::eof() const noexcept {
    return state_ && !state_->failure && state_->fin && state_->stats.released_prefix > *state_->fin;
}
std::optional<ReorderError> ReorderWindow::failure() const noexcept {
    return state_ ? state_->failure : std::nullopt;
}
ReorderStats ReorderWindow::stats() const noexcept {
    if (!state_) {
        return {};
    }
    auto result = state_->stats;
    // 交给应用尚未归还的槽仍占窗口，不能仅凭 rcv_next 前进就再次广告该槽。
    result.max_frame_seq = std::min(kSequenceEnd, result.released_prefix + state_->config.window);
    const auto capacity = static_cast<std::uint64_t>(state_->config.max_resident_bytes);
    result.max_stream_bytes = capacity > std::numeric_limits<std::uint64_t>::max() - result.consumed_bytes
                                  ? std::numeric_limits<std::uint64_t>::max()
                                  : result.consumed_bytes + capacity;
    return result;
}
std::expected<void, ReorderError> ReorderWindow::ObserveTime(std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(ReorderErrorCode::kNotReady);
    }
    if (now_ns < state_->last_now) {
        return Error(ReorderErrorCode::kInvalidArgument);
    }
    state_->last_now = now_ns;
    if (!state_->failure && state_->gap_since && now_ns - *state_->gap_since >= state_->config.gap_timeout_ns) {
        state_->failure = ReorderError{ReorderErrorCode::kGapTimeout};
    }
    return {};
}
std::expected<void, ReorderError> ReorderWindow::CheckTimeout(std::uint64_t now_ns) noexcept {
    if (const auto checked = ObserveTime(now_ns); !checked) {
        return checked;
    }
    if (state_->failure) {
        return std::unexpected(*state_->failure);
    }
    return {};
}
void ReorderWindow::UpdateGap(std::uint64_t now_ns) noexcept {
    const auto sequence = state_->stats.rcv_next;
    const auto& head = state_->entries[sequence % state_->config.window];
    if (!state_->stats.buffered_frames ||
        (head.status == State::Status::kBuffered && head.frame.sequence == sequence)) {
        state_->gap_since.reset();
    } else if (!state_->gap_since) {
        state_->gap_since = now_ns;
    }
}
std::expected<void, ReorderError> ReorderWindow::Push(const ReorderFrame& frame, std::uint64_t now_ns) noexcept {
    if (!state_) {
        return Error(ReorderErrorCode::kNotReady);
    }
    // 旧 Session 不影响当前流的故障状态，也不能占用其窗口和驻留预算。
    if (frame.session != state_->session) {
        return Error(ReorderErrorCode::kWrongSession);
    }
    if (const auto observed = ObserveTime(now_ns); !observed) {
        return observed;
    }
    if (state_->failure) {
        return std::unexpected(*state_->failure);
    }
    if (state_->stopping) {
        return Error(ReorderErrorCode::kStopped);
    }
    const bool fin = frame.kind == ReorderFrameKind::kFin;
    if (frame.kind != ReorderFrameKind::kEager && frame.kind != ReorderFrameKind::kValidatedRmaDone && !fin) {
        return Error(ReorderErrorCode::kInvalidArgument);
    }
    if ((!fin && (frame.payload.empty() || !frame.payload.data() || !ValidLease(frame.lease))) ||
        (fin && (!frame.payload.empty() || (frame.lease.index() != 0 && !ValidLease(frame.lease))))) {
        return Error(ReorderErrorCode::kInvalidArgument);
    }
    if (!fin && frame.sequence == std::numeric_limits<std::uint32_t>::max()) {
        state_->failure = ReorderError{ReorderErrorCode::kProtocol};
        return std::unexpected(*state_->failure);
    }
    if (state_->fin && ((fin && frame.sequence != *state_->fin) || frame.sequence > *state_->fin ||
                        (!fin && frame.sequence == *state_->fin))) {
        state_->failure = ReorderError{ReorderErrorCode::kProtocol};
        return std::unexpected(*state_->failure);
    }
    if (frame.sequence < state_->stats.rcv_next) {
        if (fin && !state_->fin) {
            state_->failure = ReorderError{ReorderErrorCode::kProtocol};
            return std::unexpected(*state_->failure);
        }
        return Error(ReorderErrorCode::kDuplicate);
    }
    if (frame.sequence >= stats().max_frame_seq) {
        state_->failure = ReorderError{ReorderErrorCode::kOutOfWindow};
        return std::unexpected(*state_->failure);
    }
    auto& entry = state_->entries[frame.sequence % state_->config.window];
    if (entry.status != State::Status::kEmpty) {
        if (entry.frame.sequence == frame.sequence && entry.frame.kind == frame.kind) {
            return Error(ReorderErrorCode::kDuplicate);
        }
        state_->failure = ReorderError{ReorderErrorCode::kProtocol};
        return std::unexpected(*state_->failure);
    }
    // 有界身份检查覆盖已交给应用的 Lease，避免同一物理所有权挂进两个 SN 后被双重归还。
    for (std::uint32_t i = 0; i < state_->config.window; ++i) {
        const auto& held = state_->entries[i];
        if (held.status != State::Status::kBuffered && held.status != State::Status::kDelivered) {
            continue;
        }
        if (fin && held.frame.sequence > frame.sequence) {
            state_->failure = ReorderError{ReorderErrorCode::kProtocol};
            return std::unexpected(*state_->failure);
        }
        if (SameLease(held.frame.lease, frame.lease)) {
            return Error(ReorderErrorCode::kLeaseAlreadyHeld);
        }
    }
    const auto available_bytes = state_->config.max_resident_bytes - state_->stats.resident_bytes;
    if (frame.payload.size() > available_bytes ||
        frame.payload.size() > stats().max_stream_bytes - state_->stats.received_bytes) {
        state_->failure = ReorderError{ReorderErrorCode::kByteLimit};
        return std::unexpected(*state_->failure);
    }
    entry.frame = frame;
    entry.status = State::Status::kBuffered;
    ++state_->stats.buffered_frames;
    state_->stats.resident_bytes += frame.payload.size();
    state_->stats.received_bytes += frame.payload.size();
    if (fin) {
        state_->fin = frame.sequence;
    }
    UpdateGap(now_ns);
    return {};
}
bool ReorderWindow::Deliver(std::uint32_t index, ReorderDelivery& output, bool discarded) noexcept {
    if (next_receipt_ == std::numeric_limits<std::uint64_t>::max()) {
        if (!state_->failure) {
            state_->failure = ReorderError{ReorderErrorCode::kSequenceExhausted};
        }
        return false;
    }
    auto& entry = state_->entries[index];
    entry.receipt = ++next_receipt_;
    entry.status = State::Status::kDelivered;
    entry.discarded = discarded;
    --state_->stats.buffered_frames;
    ++state_->stats.delivered_frames;
    output = {entry.frame, ReorderReceipt{this, index, entry.receipt}, discarded};
    return true;
}
std::expected<std::size_t, ReorderError> ReorderWindow::Poll(std::span<ReorderDelivery> output,
                                                             std::uint64_t now_ns) noexcept {
    if (const auto observed = ObserveTime(now_ns); !observed) {
        return std::unexpected(observed.error());
    }
    if (output.empty()) {
        return Error(ReorderErrorCode::kInvalidArgument);
    }
    std::size_t count = 0;
    const auto limit = std::min(output.size(), kMaxBatch);
    while (count < limit && state_->stats.rcv_next < kSequenceEnd) {
        const auto index = static_cast<std::uint32_t>(state_->stats.rcv_next % state_->config.window);
        auto& entry = state_->entries[index];
        if (entry.status != State::Status::kBuffered || entry.frame.sequence != state_->stats.rcv_next) {
            break;
        }
        if (!Deliver(index, output[count], false)) {
            break;
        }
        ++count;
        ++state_->stats.rcv_next;
        // 当前 gap 已经被连续消费，后续缺口重新以新的 rcv_next 计时。
        state_->gap_since.reset();
        if (entry.frame.kind == ReorderFrameKind::kFin) {
            break;
        }
    }
    UpdateGap(now_ns);
    if (!count && state_->failure) {
        return std::unexpected(*state_->failure);
    }
    return count;
}
std::expected<void, ReorderError> ReorderWindow::Release(ReorderReceipt receipt) noexcept {
    if (!state_) {
        return Error(ReorderErrorCode::kNotReady);
    }
    if (receipt.owner != this || !receipt.generation || receipt.slot >= state_->config.window) {
        return Error(ReorderErrorCode::kInvalidReceipt);
    }
    auto& entry = state_->entries[receipt.slot];
    if (entry.status != State::Status::kDelivered || entry.receipt != receipt.generation) {
        return Error(ReorderErrorCode::kInvalidReceipt);
    }
    state_->stats.resident_bytes -= entry.frame.payload.size();
    if (!entry.discarded) {
        state_->stats.consumed_bytes += entry.frame.payload.size();
    }
    --state_->stats.delivered_frames;
    entry.frame.lease = std::monostate{};
    entry.frame.payload = {};
    entry.status = State::Status::kConsumed;
    // 乱序归还不越过仍由应用持有的前缀，也不会把 Drain 丢弃的帧当作连续业务进度。
    while (state_->stats.released_prefix < state_->stats.rcv_next) {
        auto& first = state_->entries[state_->stats.released_prefix % state_->config.window];
        if (first.status != State::Status::kConsumed || first.frame.sequence != state_->stats.released_prefix) {
            break;
        }
        first = {};
        ++state_->stats.released_prefix;
    }
    return {};
}
std::expected<std::size_t, ReorderError> ReorderWindow::Drain(std::span<ReorderDelivery> output) noexcept {
    if (!state_) {
        return Error(ReorderErrorCode::kNotReady);
    }
    if (output.empty()) {
        return Error(ReorderErrorCode::kInvalidArgument);
    }
    if (!state_->stopping && !state_->failure) {
        return Error(ReorderErrorCode::kInUse);
    }
    std::size_t count = 0;
    const auto limit = std::min(output.size(), kMaxBatch);
    // 扫描也受预算约束；遇到已交付项不等待，下次从游标继续，避免空洞拖出无界循环。
    for (std::size_t scanned = 0; scanned < limit; ++scanned) {
        const auto index = state_->drain_cursor;
        state_->drain_cursor = (index + 1) % state_->config.window;
        if (state_->entries[index].status != State::Status::kBuffered) {
            continue;
        }
        if (!Deliver(index, output[count], true)) {
            if (!count) {
                return std::unexpected(*state_->failure);
            }
            break;
        }
        ++count;
    }
    return count;
}
} // namespace connection
} // namespace kbsocket
