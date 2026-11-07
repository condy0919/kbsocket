// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_RUNTIME_RAW_DOMAIN_HPP_
#define KBSOCKET_RUNTIME_RAW_DOMAIN_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>

#include "kbsocket/connection/connection_table.hpp"
#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/runtime/connection_io.hpp"
#include "kbsocket/runtime/raw_context_progress.hpp"
#include "kbsocket/runtime/rx_source_registry.hpp"
#include "kbsocket/transport/raw/attempt_ledger.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/receive_domain_closer.hpp"
#include "kbsocket/transport/raw/rx_buffer_pool.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace runtime {

struct RawDomainConfig {
    connection::TokenDomain local_domain;
    std::uint64_t context_incarnation = 0;
    raw::JettyPoolConfig queues;
    std::uint32_t tx_attempts = 256, route_capacity = 4096, source_capacity = 4096;
    std::uint32_t control_slots = 32, peer_capacity = 1024, base_credits = 8;
    std::uint32_t watermark_threshold = 0;
};
enum class RawDomainStage : std::uint8_t { kEmpty, kReady, kOpenFailed, kClosing, kClosed, kRetained };
enum class RawDomainErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kQueue,
    kReceive,
    kAttempt,
    kSender,
    kCredit,
    kRoute,
    kEager,
    kSource,
    kProgress,
    kClose,
    kRetained
};
struct RawDomainError {
    RawDomainErrorCode code;
    int detail = 0, provider_error = 0;
    std::optional<std::size_t> accepted = {};
    std::optional<raw::JettyPoolError> queue_error = {};
    std::optional<raw::ReceiveDomainCloseError> close_error = {};
};
struct RawDomainResources {
    raw::JettyPool* pool;
    raw::RxBufferPool* rx;
    raw::AttemptLedger* ledger;
    raw::TxSender* sender;
    connection::ReceiveCreditDomain* credits;
    connection::ConnectionTable* routes;
    connection::EagerReceiveDomain* receive;
    RxSourceRegistry* sources;
};
struct RawDomainSnapshot {
    RawDomainStage stage = RawDomainStage::kEmpty;
    raw::ReceiveDomainCloseStage close_stage = raw::ReceiveDomainCloseStage::kIdle;
    std::size_t posted = 0, held = 0, attempts = 0, routes = 0, sources = 0;
    std::size_t initial_accepted = 0, retired_rx = 0;
    bool initial_accepted_known = true, bound = false;
    connection::CreditDomainStats credits;
    std::optional<RawDomainError> open_error;
};
struct RawDomainCloseProof {
    // 绑定本域的 Runtime/raw poll owner 已成功解绑，相关控制任务已收割，回调不会再到达。
    bool progress_detached = false;
    // M1 的四个独立宿主屏障；Peer 授信 ACK 不自动构成整个域的静默或 DMA/事件终结。
    // queue_users_stopped 包括连接槽及外部 Slab/import/Grant 等访问责任；旧资源视图/绑定禁止再使用。
    // Held 的最后归还仍可经本域 receive 执行，不授予 post/poll 或重新登记来源的权限。
    raw::ReceiveDomainQuiescence quiescence;
};

/// 单 owner、单次生命周期的 raw 接收域资源所有者；借用 context，拥有固定地址的整份依赖。
/// Open/Bind/BeginClose/AdvanceClose 仅在控制路径执行；不能在 Tick 或业务回调内删除/注销。
/// Open 成功才发布资源视图；失败后保留已创建资源与真实初始 RX 接受证据，显式关闭后才能析构。
/// context、provider 及外部借用者必须覆盖未完成的生命周期；未关闭析构保留整份状态。
/// 不创建 context、Slab、import 或逻辑连接，不代办认证、Peer 撤回或故障 CQ 在线回收。
class RawDomain {
public:
    RawDomain() noexcept;
    ~RawDomain();
    RawDomain(const RawDomain&) = delete;
    RawDomain& operator=(const RawDomain&) = delete;
    std::expected<void, RawDomainError> Open(urma_context_t* context, const urma_device_cap_t& capabilities,
                                             const RawDomainConfig& config) noexcept;
    /// 注册 Eager 补投并生成完整 TX/RX/Error 回调绑定；宿主把返回值交给相同 context 的 RuntimeShard。
    /// progress 可先 Close，但对象须存活且不得重开，直到本域 AdvanceClose 成功解除绑定。
    std::expected<RawQueueBinding, RawDomainError> Bind(ConnectionProgress& progress) noexcept;
    /// 仅成功 Open 的域可借用；关闭期间只允许终结已有责任，禁止绕过本所有者 Close/Open 成员。
    std::expected<RawDomainResources, RawDomainError> resources() noexcept;
    RawDomainSnapshot Inspect() const noexcept;
    /// 停止新路由/授信及 TX 准入；保持既有 RX 消费、归还和实际补投，供 Peer 撤回收敛。
    std::expected<void, RawDomainError> BeginClose() noexcept;
    /// Runtime 解绑、routes/sources 退出、TX 账本及排空边界结清、撤回控制器解绑后，才进入 M1。
    /// 迟到 RX 只作为未知来源消费并归还，不交付业务；本次有 RX 时不删除，须重新确认事件屏障。
    /// M1 Closed 后清退 Eager/Credit 元数据。删除/注销失败保留阶段可重试；故障 CQ 永久保留。
    std::expected<RawDomainSnapshot, RawDomainError> AdvanceClose(const RawDomainCloseProof& proof) noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
};
} // namespace runtime
} // namespace kbsocket
#endif // KBSOCKET_RUNTIME_RAW_DOMAIN_HPP_
