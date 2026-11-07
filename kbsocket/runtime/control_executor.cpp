// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/runtime/control_executor.hpp"

#include <pthread.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <limits>
#include <new>
#include <utility>

namespace kbsocket {
namespace runtime {
namespace {
auto Error(ControlExecutorErrorCode code, int status = 0) noexcept {
    return std::unexpected(ControlExecutorError{.code = code, .system_error = status});
}

void Signal(int fd) noexcept {
    const std::uint64_t value = 1;
    ssize_t written;
    do {
        written = ::write(fd, &value, sizeof(value));
    } while (written < 0 && errno == EINTR);
    // EAGAIN 表示计数器已满且本来就可读。FD 由 state 独占，其他错误仅可能来自外部破坏其生命周期。
}
} // namespace

struct ControlExecutor::State {
    enum class Phase : std::uint8_t { kFree, kPending, kRunning, kCompleted };

    struct alignas(64) Slot {
        std::atomic<Phase> phase{Phase::kFree};
        ControlTask task;
        ControlTaskResult result;
        std::uint64_t sequence = 0;
        // 只有 owner 访问；worker 不根据取消跳过已接受任务，也不丢弃结果。
        bool cancelled = false;
    };

    struct alignas(64) Worker {
        State* state = nullptr;
        std::uint32_t index = 0;
        std::atomic<std::uint64_t> wake_sequence{0};
        pthread_t thread{};
        bool started = false;
    };

    static void* Run(void* argument) noexcept {
        auto& worker = *static_cast<Worker*>(argument);
        auto& state = *worker.state;
        auto observed = worker.wake_sequence.load(std::memory_order_acquire);
        for (;;) {
            // 每个槽位只有一个固定 worker 消费，不需要多消费者 CAS 或共享工作队列锁。
            for (std::uint32_t i = worker.index; i < state.capacity; i += state.worker_count) {
                auto& slot = state.slots[i];
                if (slot.phase.load(std::memory_order_acquire) != Phase::kPending) {
                    continue;
                }
                slot.phase.store(Phase::kRunning, std::memory_order_relaxed);
                slot.result = slot.task.function(slot.task.argument, slot.task.token);
                slot.phase.store(Phase::kCompleted, std::memory_order_release);
                Signal(state.fd);
            }
            if (state.stop.load(std::memory_order_acquire)) {
                return nullptr;
            }
            const auto current = worker.wake_sequence.load(std::memory_order_acquire);
            if (current != observed) {
                observed = current;
                continue;
            }
            // 发布任务发生在扫描与 wait 之间时，序号变化让 wait 立即返回，避免丢失提交唤醒。
            worker.wake_sequence.wait(observed, std::memory_order_acquire);
            observed = worker.wake_sequence.load(std::memory_order_acquire);
        }
    }

    int StopWorkers() noexcept {
        stop.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            if (worker.started) {
                worker.wake_sequence.fetch_add(1, std::memory_order_release);
                worker.wake_sequence.notify_one();
            }
        }
        int first_error = 0;
        for (auto& worker : workers) {
            if (!worker.started) {
                continue;
            }
            const int result = ::pthread_join(worker.thread, nullptr);
            if (!result) {
                worker.started = false;
            } else if (!first_error) {
                first_error = result;
            }
        }
        return first_error;
    }

    ~State() {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    std::unique_ptr<Slot[]> slots;
    std::array<Worker, kMaxWorkers> workers;
    std::atomic<bool> stop{false};
    std::uint32_t capacity = 0;
    std::uint32_t worker_count = 0;
    std::uint32_t next_submit = 0;
    std::uint32_t next_poll = 0;
    std::size_t outstanding = 0;
    bool accepting = true;
    int fd = -1;
};

ControlExecutor::ControlExecutor() noexcept = default;

ControlExecutor::~ControlExecutor() {
    if (!Close()) {
        // 忙任务或无法 join 的线程仍可能访问 state；完整保留而非 detach 后销毁槽位/FD。
        (void)state_.release();
    }
}

std::expected<void, ControlExecutorError> ControlExecutor::Open(const ControlExecutorConfig& config) noexcept {
    if (state_) {
        return Error(ControlExecutorErrorCode::kInUse);
    }
    if (!config.workers || config.workers > kMaxWorkers || !config.capacity || config.capacity > kMaxCapacity ||
        config.workers > config.capacity) {
        return Error(ControlExecutorErrorCode::kInvalidArgument);
    }
    std::unique_ptr<State> state;
    try {
        state = std::make_unique<State>();
        state->slots = std::make_unique<State::Slot[]>(config.capacity);
    } catch (const std::bad_alloc&) {
        return Error(ControlExecutorErrorCode::kNoMemory);
    }
    state->capacity = config.capacity;
    state->worker_count = config.workers;
    state->fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (state->fd < 0) {
        return Error(ControlExecutorErrorCode::kSystemError, errno);
    }
    for (std::uint32_t i = 0; i < config.workers; ++i) {
        auto& worker = state->workers[i];
        worker.state = state.get();
        worker.index = i;
        const int status = ::pthread_create(&worker.thread, nullptr, &State::Run, &worker);
        if (status) {
            // Open 尚未发布，worker 没有任务；先唤醒并 join 已创建线程，再释放共享槽位和 FD。
            if (state->StopWorkers()) {
                (void)state.release();
            }
            return Error(ControlExecutorErrorCode::kThreadError, status);
        }
        worker.started = true;
    }
    state_ = std::move(state);
    return {};
}

std::expected<ControlTaskId, ControlExecutorError> ControlExecutor::TrySubmit(const ControlTask& task) noexcept {
    if (!state_) {
        return Error(ControlExecutorErrorCode::kNotReady);
    }
    auto& state = *state_;
    if (!state.accepting) {
        return Error(ControlExecutorErrorCode::kStopped);
    }
    if (!task.function) {
        return Error(ControlExecutorErrorCode::kInvalidArgument);
    }
    if (next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(ControlExecutorErrorCode::kSequenceExhausted);
    }
    if (state.outstanding == state.capacity) {
        return Error(ControlExecutorErrorCode::kResourceExhausted);
    }
    for (std::uint32_t scanned = 0; scanned < state.capacity; ++scanned) {
        const auto index = state.next_submit;
        state.next_submit = (index + 1) % state.capacity;
        auto& slot = state.slots[index];
        if (slot.phase.load(std::memory_order_acquire) != State::Phase::kFree) {
            continue;
        }
        slot.task = task;
        slot.sequence = ++next_sequence_;
        slot.cancelled = task.cancelled;
        const ControlTaskId id{.executor = this, .slot = index, .sequence = slot.sequence};
        ++state.outstanding;
        slot.phase.store(State::Phase::kPending, std::memory_order_release);
        auto& worker = state.workers[index % state.worker_count];
        worker.wake_sequence.fetch_add(1, std::memory_order_release);
        worker.wake_sequence.notify_one();
        return id;
    }
    return Error(ControlExecutorErrorCode::kResourceExhausted);
}

std::expected<void, ControlExecutorError> ControlExecutor::Cancel(ControlTaskId id) noexcept {
    if (!state_) {
        return Error(ControlExecutorErrorCode::kNotReady);
    }
    if (id.executor != this || !id.sequence || id.slot >= state_->capacity) {
        return Error(ControlExecutorErrorCode::kInvalidTask);
    }
    auto& slot = state_->slots[id.slot];
    if (slot.phase.load(std::memory_order_acquire) == State::Phase::kFree || slot.sequence != id.sequence) {
        return Error(ControlExecutorErrorCode::kInvalidTask);
    }
    slot.cancelled = true;
    return {};
}

std::expected<std::size_t, ControlExecutorError>
ControlExecutor::Poll(std::span<ControlTaskCompletion> completions) noexcept {
    if (!state_) {
        return Error(ControlExecutorErrorCode::kNotReady);
    }
    if (completions.empty()) {
        return Error(ControlExecutorErrorCode::kInvalidArgument);
    }
    auto& state = *state_;
    std::uint64_t notification;
    const auto read = ::read(state.fd, &notification, sizeof(notification));
    if (read < 0 && errno != EAGAIN && errno != EINTR) {
        return Error(ControlExecutorErrorCode::kSystemError, errno);
    }
    std::size_t count = 0;
    for (std::uint32_t scanned = 0; scanned < state.capacity && count < completions.size(); ++scanned) {
        const auto index = state.next_poll;
        state.next_poll = (index + 1) % state.capacity;
        auto& slot = state.slots[index];
        if (slot.phase.load(std::memory_order_acquire) != State::Phase::kCompleted) {
            continue;
        }
        completions[count++] = {.id = {.executor = this, .slot = index, .sequence = slot.sequence},
                                .argument = slot.task.argument,
                                .token = slot.task.token,
                                .cancelled = slot.cancelled,
                                .result = slot.result};
        --state.outstanding;
        slot.phase.store(State::Phase::kFree, std::memory_order_release);
    }
    // 不在扫描之后 drain：并发发布的新完成必须保留其通知；预算未收割的完成重新置可读。
    for (std::uint32_t i = 0; i < state.capacity; ++i) {
        if (state.slots[i].phase.load(std::memory_order_acquire) == State::Phase::kCompleted) {
            Signal(state.fd);
            break;
        }
    }
    return count;
}

void ControlExecutor::StopAccepting() noexcept {
    if (state_) {
        state_->accepting = false;
    }
}

std::expected<void, ControlExecutorError> ControlExecutor::Close() noexcept {
    if (!state_) {
        return {};
    }
    StopAccepting();
    if (state_->outstanding) {
        return Error(ControlExecutorErrorCode::kInUse);
    }
    if (const int status = state_->StopWorkers()) {
        return Error(ControlExecutorErrorCode::kThreadError, status);
    }
    state_.reset();
    return {};
}

int ControlExecutor::completion_fd() const noexcept {
    return state_ ? state_->fd : -1;
}

std::size_t ControlExecutor::outstanding() const noexcept {
    return state_ ? state_->outstanding : 0;
}

bool ControlExecutor::accepting() const noexcept {
    return state_ && state_->accepting;
}

} // namespace runtime
} // namespace kbsocket
