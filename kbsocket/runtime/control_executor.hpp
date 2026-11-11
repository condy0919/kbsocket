// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_RUNTIME_CONTROL_EXECUTOR_HPP_
#define KBSOCKET_RUNTIME_CONTROL_EXECUTOR_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

namespace kbsocket {
namespace runtime {

class ControlExecutor;

struct ControlTaskError {
    /// 工作函数自己的错误域；执行器只原样交回，不解释 provider 或业务错误。
    int code = 0;
};

using ControlTaskResult = std::expected<void, ControlTaskError>;
using ControlTaskFunction = ControlTaskResult (*)(void*, std::uint64_t) noexcept;

struct ControlTask {
    ControlTaskFunction function = nullptr;
    void* argument = nullptr;
    /// 由调用方携带的操作/实例身份，不用于复用执行器槽位。
    std::uint64_t token = 0;
    /// 已接受任务始终执行；取消仅决定 owner 如何处理返回的资源，不能撤销硬件操作。
    bool cancelled = false;
    /// 默认暂停整个源；仅独立参数快照、与 owner 活动资源无共享写入的任务可关闭屏障。
    bool pause_progress = true;
};

struct ControlTaskId {
    const ControlExecutor* executor = nullptr;
    std::uint32_t slot = 0;
    /// 同一执行器对象的 Close/Open 也不复用序号，拒绝迟到的取消请求。
    std::uint64_t sequence = 0;
    bool operator==(const ControlTaskId&) const = default;
};

struct ControlTaskCompletion {
    ControlTaskId id;
    void* argument = nullptr;
    std::uint64_t token = 0;
    bool cancelled = false;
    ControlTaskResult result;
};

struct ControlExecutorConfig {
    std::uint32_t workers = 1;
    /// 排队、执行中、已完成但未 Poll 的任务共同占用容量。
    std::uint32_t capacity = 64;
};

enum class ControlExecutorErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kResourceExhausted,
    kSequenceExhausted,
    kInvalidTask,
    kNoMemory,
    kSystemError,
    kThreadError,
};

struct ControlExecutorError {
    ControlExecutorErrorCode code;
    /// 系统调用 errno 或 pthread 返回值；不替代工作函数的 ControlTaskError。
    int system_error = 0;
};

/// 单 owner 的有界慢控制任务执行器；工作线程不得修改 owner 的活动队列或账本。
/// Open 分配全部内部槽位并创建固定线程；TrySubmit/Cancel/Poll 零分配、无互斥锁、不等待工作函数。
/// 所有公开方法由同一 owner 调用，不并发或重入。不同任务可并行，无提交顺序完成保证。
/// function 必须 noexcept，argument 及其依赖由调用方一直持有到完成被 Poll 收割，取消亦不例外。
/// 结果中的新建/半成品资源交回 owner 处理；代次失效或取消只能清理，不能继续发布。
/// 析构前必须 StopAccepting、收割所有完成并 Close；无法终结的工作须连同全部依赖常驻。
/// 违反契约且仍有任务时，析构保留整个内部状态/线程/FD，绝不 join 忙任务或释放其正在访问的内存；
/// 这种保留不接管调用方的 argument，也不能代替其资源生命周期责任。
class ControlExecutor {
public:
    static constexpr std::uint32_t kMaxWorkers = 4;
    static constexpr std::uint32_t kMaxCapacity = 4096;

    ControlExecutor() noexcept;
    ~ControlExecutor();
    ControlExecutor(const ControlExecutor&) = delete;
    ControlExecutor& operator=(const ControlExecutor&) = delete;

    std::expected<void, ControlExecutorError> Open(const ControlExecutorConfig& config = {}) noexcept;
    std::expected<ControlTaskId, ControlExecutorError> TrySubmit(const ControlTask& task) noexcept;
    std::expected<void, ControlExecutorError> Cancel(ControlTaskId id) noexcept;

    /// 一次最多输出 completions.size() 条，最多扫描固定 capacity 两次，不等待 worker。
    /// 先清通知再扫描；输出预算不足时重新置可读，未收割完成不会因事件合并而失去唤醒。
    std::expected<std::size_t, ControlExecutorError> Poll(std::span<ControlTaskCompletion> completions) noexcept;

    void StopAccepting() noexcept;
    /// 隐含 StopAccepting。存在未收割任务时立即 kInUse，只有全部收割后才停止并 join 空闲线程。
    std::expected<void, ControlExecutorError> Close() noexcept;

    /// 借用的 nonblocking eventfd，仅供 Reactor 注册就绪；禁止调用方 read/write/close，由 Poll 消费。
    int completion_fd() const noexcept;
    std::size_t outstanding() const noexcept;
    bool accepting() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    std::uint64_t next_sequence_ = 0;
};

} // namespace runtime
} // namespace kbsocket

#endif // KBSOCKET_RUNTIME_CONTROL_EXECUTOR_HPP_
