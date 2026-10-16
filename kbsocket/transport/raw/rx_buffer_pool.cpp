// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <new>
#include <stdexcept>

#include "kbsocket/base/log.hpp"

namespace kbsocket {
namespace raw {
namespace {
auto Error(RxBufferErrorCode code, int status = 0, std::optional<std::size_t> accepted = std::nullopt) noexcept {
    return std::unexpected(RxBufferError{code, status, accepted});
}

// 排除 fake CQE、软件 TX flush 及未知状态；它们不能证明某个 RX buffer 已停止 DMA。
bool IsTerminal(urma_cr_status_t status) noexcept {
    switch (status) {
    case URMA_CR_SUCCESS:
    case URMA_CR_UNSUPPORTED_OPCODE_ERR:
    case URMA_CR_LOC_LEN_ERR:
    case URMA_CR_LOC_OPERATION_ERR:
    case URMA_CR_LOC_ACCESS_ERR:
    case URMA_CR_REM_RESP_LEN_ERR:
    case URMA_CR_REM_UNSUPPORTED_REQ_ERR:
    case URMA_CR_REM_OPERATION_ERR:
    case URMA_CR_REM_ACCESS_ABORT_ERR:
    case URMA_CR_ACK_TIMEOUT_ERR:
    case URMA_CR_RNR_RETRY_CNT_EXC_ERR:
    case URMA_CR_WR_FLUSH_ERR:
    case URMA_CR_LOC_DATA_POISON:
    case URMA_CR_REM_DATA_POISON:
        return true;
    default:
        return false;
    }
}
} // namespace

RxBufferPool::~RxBufferPool() {
    if (auto result = Close(); !result) {
        // 保留 slab 和注册句柄，避免析构造成 DMA use-after-free；上层应常驻整个资源依赖链。
        KBSOCKET_LOG_ERROR("RX buffer cleanup failed: code={} status={}; retaining DMA memory",
                           static_cast<unsigned>(result.error().code), result.error().provider_error);
    }
}

std::expected<void, RxBufferError> RxBufferPool::Open(JettyPool& pool, std::uint32_t buffer_count) noexcept {
    if (pool_ || pool.rx_owner_) {
        return Error(RxBufferErrorCode::kInUse);
    }
    if (!pool.ready()) {
        return Error(RxBufferErrorCode::kNotReady);
    }
    const auto count = buffer_count ? buffer_count : pool.rx_depth_;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (!count || page_size <= 0 || count > std::numeric_limits<std::size_t>::max() / kBufferSize) {
        return Error(RxBufferErrorCode::kInvalidArgument);
    }
    const auto page = static_cast<std::size_t>(page_size);
    const auto bytes = static_cast<std::size_t>(count) * kBufferSize;
    if (bytes > std::numeric_limits<std::size_t>::max() - (page - 1)) {
        return Error(RxBufferErrorCode::kInvalidArgument);
    }
    const auto registration_size = (bytes + page - 1) / page * page;
    try {
        entries_.resize(count);
    } catch (const std::bad_alloc&) {
        return Error(RxBufferErrorCode::kNoMemory);
    } catch (const std::length_error&) {
        return Error(RxBufferErrorCode::kNoMemory);
    }
    void* allocation = nullptr;
    const int allocated = ::posix_memalign(&allocation, page, registration_size);
    if (allocated) {
        entries_.clear();
        return Error(RxBufferErrorCode::kNoMemory, allocated);
    }
    memory_ = static_cast<std::byte*>(allocation);
    urma_seg_cfg_t cfg{};
    cfg.va = reinterpret_cast<std::uintptr_t>(memory_);
    cfg.len = registration_size;
    // 与 UMQ/硬件 SEND 工具一致，避免 LOCAL_ONLY 的 e_bit 授权路径。
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    errno = 0;
    segment_ = UrmaApi::RegisterSeg(pool.ctx_, &cfg);
    if (!segment_) {
        const int saved_errno = errno ? errno : EIO;
        std::free(memory_);
        memory_ = nullptr;
        entries_.clear();
        return Error(RxBufferErrorCode::kRegisterFailed, saved_errno);
    }
    for (std::uint32_t i = 0; i < count; ++i) {
        entries_[i] = {.next = i + 1};
    }
    free_head_ = 0;
    base_id_ = next_id_;
    stopped_ = false;
    pool_ = &pool;
    pool.rx_owner_ = this;
    return {};
}

std::expected<void, RxBufferError> RxBufferPool::Close() noexcept {
    if (posted_ || held_) {
        return Error(RxBufferErrorCode::kInUse);
    }
    if (segment_) {
        // 一旦开始关闭，即使注销失败也不再允许重新投递。
        stopped_ = true;
        const auto status = UrmaApi::UnregisterSeg(segment_);
        if (status != URMA_SUCCESS) {
            return Error(RxBufferErrorCode::kUnregisterFailed, status);
        }
        segment_ = nullptr;
    }
    std::free(memory_);
    memory_ = nullptr;
    entries_.clear();
    if (pool_) {
        pool_->rx_owner_ = nullptr;
        pool_ = nullptr;
    }
    return {};
}

void RxBufferPool::Recycle(std::uint32_t index) noexcept {
    auto& entry = entries_[index];
    entry.state = State::kFree;
    entry.next = free_head_;
    free_head_ = index;
}

std::expected<std::size_t, RxBufferError> RxBufferPool::Refill(std::size_t limit) noexcept {
    if (!pool_) {
        return Error(RxBufferErrorCode::kNotReady);
    }
    if (stopped_) {
        return Error(RxBufferErrorCode::kStopped);
    }
    if (!limit || limit > kMaxBatch) {
        return Error(RxBufferErrorCode::kInvalidArgument);
    }
    const auto count = std::min({limit, available(), pool_->rx_depth_ - posted_});
    if (count > (std::numeric_limits<std::uint64_t>::max() - next_id_) / capacity()) {
        return Error(RxBufferErrorCode::kExhausted);
    }
    for (std::size_t i = 0; i < count; ++i) {
        const auto index = free_head_;
        auto& entry = entries_[index];
        free_head_ = entry.next;
        entry.id = next_id_ + index;
        next_id_ += capacity();
        entry.state = State::kPosted;
        indices_[i] = index;
        sges_[i] = {.addr = reinterpret_cast<std::uintptr_t>(memory_ + index * kBufferSize),
                    .len = kBufferSize,
                    .tseg = segment_};
        wrs_[i] = {};
        wrs_[i].src = {&sges_[i], 1};
        wrs_[i].user_ctx = entry.id;
        wrs_[i].next = i + 1 < count ? &wrs_[i + 1] : nullptr;
    }
    if (!count) {
        return 0;
    }
    posted_ += count;
    urma_jfr_wr_t* bad = nullptr;
    const auto status = UrmaApi::PostJfrWr(pool_->jfr(), wrs_.data(), &bad);
    if (status == URMA_SUCCESS && !bad) {
        return count;
    }
    if (status != URMA_SUCCESS) {
        for (std::size_t i = 0; i < count; ++i) {
            if (bad == &wrs_[i]) {
                for (std::size_t j = count; j > i; --j) {
                    Recycle(indices_[j - 1]);
                }
                posted_ -= count - i;
                // EAGAIN 可在后续进度后重试，其他错误交由上层终止接收路径。
                stopped_ = status != URMA_EAGAIN;
                return Error(RxBufferErrorCode::kPostFailed, status, i);
            }
        }
    }
    stopped_ = true;
    return Error(RxBufferErrorCode::kProviderContract, status);
}

std::expected<std::size_t, RxBufferError> RxBufferPool::Poll(std::span<RxBufferEvent> events) noexcept {
    if (!pool_) {
        return Error(RxBufferErrorCode::kNotReady);
    }
    if (events.empty()) {
        return Error(RxBufferErrorCode::kInvalidArgument);
    }
    const auto budget = std::min(events.size(), completions_.size());
    std::fill_n(completions_.begin(), budget, urma_cr_t{});
    auto count = pool_->PollRecv(std::span(completions_).first(budget));
    if (!count) {
        stopped_ = true;
        return Error(RxBufferErrorCode::kPollFailed, count.error().provider_error);
    }
    for (int i = 0; i < *count; ++i) {
        events[i] = Process(completions_[i]);
    }
    return static_cast<std::size_t>(*count);
}

RxBufferEvent RxBufferPool::Process(const urma_cr_t& cr) noexcept {
    RxBufferEvent event{.completion = cr};
    if (cr.status == URMA_CR_WR_FLUSH_ERR_DONE || cr.status == URMA_CR_WR_SUSPEND_DONE) {
        event.completion.user_ctx = 0;
    }
    const bool local =
        cr.flag.bs.jetty ? pool_->FindLane(cr.local_id).has_value() : cr.local_id == pool_->jfr()->jfr_id.id;
    if (!cr.flag.bs.s_r || !local || !IsTerminal(cr.status) || cr.user_ctx < base_id_) {
        stopped_ = true;
        event.error = RxBufferError{.code = RxBufferErrorCode::kUnexpectedCompletion};
        return event;
    }
    const auto index = static_cast<std::uint32_t>((cr.user_ctx - base_id_) % capacity());
    auto& entry = entries_[index];
    if (entry.id != cr.user_ctx || entry.state != State::kPosted) {
        stopped_ = true;
        event.error = RxBufferError{.code = RxBufferErrorCode::kInvalidLease};
        return event;
    }
    entry.state = State::kHeld;
    --posted_;
    ++held_;
    event.lease = RxBufferLease{this, entry.id};
    if (cr.status != URMA_CR_SUCCESS) {
        stopped_ = true;
        event.error =
            RxBufferError{.code = RxBufferErrorCode::kReceiveFailed, .provider_error = static_cast<int>(cr.status)};
    } else if (cr.completion_len > kBufferSize ||
               (cr.opcode != URMA_CR_OPC_SEND && cr.opcode != URMA_CR_OPC_SEND_WITH_IMM)) {
        stopped_ = true;
        event.error = RxBufferError{.code = RxBufferErrorCode::kUnexpectedCompletion};
    } else {
        event.payload = {memory_ + index * kBufferSize, cr.completion_len};
    }
    return event;
}

std::expected<void, RxBufferError> RxBufferPool::Release(RxBufferLease lease) noexcept {
    if (lease.pool != this || !pool_ || lease.id < base_id_) {
        return Error(RxBufferErrorCode::kInvalidLease);
    }
    const auto index = static_cast<std::uint32_t>((lease.id - base_id_) % capacity());
    auto& entry = entries_[index];
    if (entry.id != lease.id || entry.state != State::kHeld) {
        return Error(RxBufferErrorCode::kInvalidLease);
    }
    --held_;
    Recycle(index);
    return {};
}
} // namespace raw
} // namespace kbsocket
