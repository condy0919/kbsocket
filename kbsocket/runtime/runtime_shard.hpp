// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_RUNTIME_SHARD_HPP_
#define KBSOCKET_RUNTIME_RUNTIME_SHARD_HPP_

#include <array>
#include <cstddef>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/runtime/raw_context_progress.hpp"
#include "kbsocket/runtime/shard_reactor.hpp"

namespace kbsocket {
namespace runtime {
struct RawContextBinding {
    urma_context_t* context = nullptr;
    std::span<const RawQueueBinding> queues;
    // 同 context 的连接/导入与 raw 轮询构成一个暂停单元；不能再作为独立 hooks 注册。
    ProgressSource* connections = nullptr;
    ProgressSource* control = nullptr;
};
struct RuntimeShardConfig {
    ReactorConfig reactor;
    ControlExecutorConfig control;
};

/// 将最多四个 raw context 及其连接/控制源绑定至一个 owner，拥有反应堆/控制线程，借用 URMA/DMA。
/// 纯 TCP 可用空 contexts 加独立 hooks；raw 模式下同 context 的全部访问必须加入其暂停单元。
/// 宿主在控制路径 Open 后由同一 owner 推进；回调、raw 句柄与控制任务不得并发或重入。
/// Open 不创建连接、不授信；完成只归还本地配额，RX lease 由调用方消费并归还。
/// 析构前须 Close 成功；若仍有控制任务，只保留内部状态，宿主仍须保留借用的 raw/context/回调依赖。
class RuntimeShard {
public:
    static constexpr std::size_t kMaxContexts = 4;
    RuntimeShard() noexcept;
    ~RuntimeShard();
    RuntimeShard(const RuntimeShard&) = delete;
    RuntimeShard& operator=(const RuntimeShard&) = delete;
    // hooks 为独立 TCP/mailbox/timer/retire 进度源；不得访问任一 raw context 或重复其成员/fd。
    std::expected<void, ProgressError> Open(std::span<const RawContextBinding> contexts,
                                            const RuntimeShardConfig& config = {},
                                            ProgressSource* hooks = nullptr) noexcept;
    std::expected<ProgressBatch, ProgressError> Tick() noexcept;
    std::expected<ProgressBatch, ProgressError> RunOnce() noexcept;
    std::expected<void, ProgressError> Notify() noexcept;
    void StopAccepting() noexcept;
    // 先停止新控制任务；有已提交任务即返回 kInUse，继续 Tick 收割后重试。不会销毁借用的 raw 资源。
    std::expected<void, ProgressError> Close() noexcept;
    std::expected<void, ProgressError> RequestReplacement(std::size_t context, raw::JettyLane lane,
                                                          const raw::JettyLaneQuiescence& proof) noexcept;
    bool paused(std::size_t context) const noexcept;
    const RawDomainProgressStats* domain_stats(std::size_t context, std::size_t domain) const noexcept;
    const ReactorStats* stats() const noexcept;

private:
    struct State;
    // Open 控制路径一次分配；未终结时析构保留完整内部进度源和任务参数，不制造工作线程 UAF。
    std::unique_ptr<State> state_;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_RUNTIME_SHARD_HPP_
