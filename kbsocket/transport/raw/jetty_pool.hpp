// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_JETTY_POOL_HPP_
#define KBSOCKET_TRANSPORT_RAW_JETTY_POOL_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <vector>

#include <gflags/gflags.h>

#include "kbsocket/transport/raw/jetty_pool_error.hpp"
#include "kbsocket/transport/raw/urma_api.hpp" // IWYU pragma: keep

// 命令行参数声明：库内部仅在 Open() 控制路径读取默认值，不主动解析 argv。
DECLARE_uint32(kbsocket_tx_depth);
DECLARE_uint32(kbsocket_rx_depth);
DECLARE_uint32(kbsocket_link_priority);

namespace kbsocket {
namespace raw {
/// Jetty 池硬件队列参数配置。
struct JettyPoolConfig {
    /// 池中创建的物理 Jetty 数量（每个 Jetty 包含独立的发送队列 JFS，共享公共接收队列 JFR）。
    std::uint32_t jetty_count = 1;
    /// 单个 Jetty JFS 的最大发送队列深度；未显式指定时在 `Open()` 中读取 `--kbsocket_tx_depth`。
    std::optional<std::uint32_t> tx_depth;
    /// 共享 JFR 的最大接收队列深度；未显式指定时在 `Open()` 中读取 `--kbsocket_rx_depth`。
    std::optional<std::uint32_t> rx_depth;
    /// 发送完成队列（Send JFC）深度；必须满足 `jetty_count * (tx_depth + 1) <= tx_cq_depth`。
    /// 公式中每个 jetty 额外预留一个 `FLUSH_ERR_DONE` 边界事件 CQE，不占用用户 WR 额度。
    std::uint32_t tx_cq_depth = 256;
    /// 接收完成队列（Recv JFC）深度；必须满足 `rx_depth <= rx_cq_depth`。
    std::uint32_t rx_cq_depth = 256;
    /// 单个发送 WR 支持的最大 SGE 数量。
    std::uint8_t send_sge = 1;
    /// 单个发送 WR 支持的最大内联（inline）数据字节数；为 0 表示遵循 URMA 约定使用设备默认值，并非禁用。
    std::uint32_t inline_bytes = 0;
};

class JettyPool;

/// 物理发送队列（SQ）的稳定通道身份。
/// - 共享通道模型：同一物理 Jetty 通道在生命周期内可同时服务多个逻辑连接与远端目标。
/// - 纪元校验（epoch）：仅在所属池的当前生命周期内有效，池对象重新 `Open()` 后历史通道引用立即失效。
struct JettyLane {
    const JettyPool* pool = nullptr;
    std::uint32_t index = 0;
    std::uint64_t epoch = 0;
};

/// 故障通道只单向排空，不恢复为可发送状态。
enum class JettyLaneState : std::uint8_t {
    kReady,
    kFaulted,    // 已隔离，尚未确认 modify ERROR 成功。
    kError,      // 已切 ERROR，等待硬件 FLUSH_ERR_DONE。
    kFlushReady, // 已消费硬件边界，允许分批软件 flush。
    kDrained,    // 软件队列和本 SQ 票据均已排空，仍不可发送。
};

/// 单个发送 WR 的预留额度票据。
/// - 配额凭证：代表已在目标物理 SQ 上成功预扣的一个 WR 投递额度。
/// - 序号防错（sequence）：全局单调递增序号，严格防止重复完成或迟到完成误释放已被回收复用的额度条目（防 ABA 错乱）。
/// - 复制语义：复制票据不会增加底层配额，亦不转移 DMA 内存所有权。
struct JettyTicket {
    JettyLane lane;
    std::uint32_t index = 0;
    std::uint64_t sequence = 0;
};

/// 基于 RM_CTP 传输模式的高性能共享发送队列（SQ）池。
///
/// 架构拓扑与共享模型：
/// - 内部独占两个完成队列（Send JFC、Recv JFC）、一个共享接收队列（JFR）以及固定数量的 Jetty（包含各自 JFS）。
/// - 区别于连接独占 Jetty 模型，RM_CTP 模式下多个连接可并发复用同一组物理 SQ，每个 WR 在投递时自行指定目标远端 jetty。
/// - 遵循单线程 Owner 推进模型：通道选择、票据流转与完成轮询零互斥锁，Fastpath 零动态内存分配。
///
/// 票据状态机生命周期：
/// - 预留（Reserve / ReserveOn）：向通道申请额度，进入 Reserved 状态；
/// - 提交（Commit）：硬件驱动成功接受 WR 后确认为 Posted 状态；
/// - 撤销（Cancel）：硬件拒绝或放弃投递时撤销 Reserved 额度；
/// - 终结（Complete）：确认对应 WR 已终结且不再访问 DMA 内存后退休 Posted 额度，槽位返回自由链表。
///
/// 生命周期与硬件依赖：
/// - 仅借用外部传入的 `urma_context_t*`，且 `cap` 必须源自同一物理设备；底层 context 及注册内存必须覆盖池的存活期。
/// - 调用 `Close()` 前必须停止提交并退休全部在途票据；任一票据未退休时拒绝关闭，并保持轮询能力以推进排空。
/// - 若进程退出时仍有工作线程访问，整个池及依赖资源必须常驻，不得触发自动析构。
class JettyPool {
public:
    JettyPool() = default;

    JettyPool(const JettyPool&) = delete;
    JettyPool& operator=(const JettyPool&) = delete;

    ~JettyPool();

    /// 创建并初始化全部底层硬件队列资源（JFC、JFR、Jetty）。
    /// 严格校验参数与硬件能力边界（确保 CQ 不溢出）；创建失败将触发自动回滚，并在异常时报告 cleanup_error。
    std::expected<void, JettyPoolError> Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                             const JettyPoolConfig& config = {}) noexcept;

    /// 逆序销毁池内全部硬件队列资源。
    /// 若仍有票据或尚未完成 ERROR 排空，返回 `kInUse` 并保持轮询可用；删除失败保留句柄供重试。
    std::expected<void, JettyPoolFailure> Close() noexcept;

    /// 轮询发送完成队列（Send JFC），由调用方传入预分配的完成条目缓冲区（零动态分配）。
    std::expected<int, JettyPoolFailure> PollSend(std::span<urma_cr_t> completions) noexcept;

    /// 轮询接收完成队列（Recv JFC），由调用方传入预分配的完成条目缓冲区（零动态分配）。
    std::expected<int, JettyPoolFailure> PollRecv(std::span<urma_cr_t> completions) noexcept;

    /// 获取共享接收队列（JFR）句柄，供调用方投递 Recv WR；未初始化或未就绪时返回 nullptr。
    urma_jfr_t* jfr() const noexcept {
        return ready_ ? jfr_ : nullptr;
    }

    /// 轮转（Round-Robin）选择未故障且有空闲额度的 SQ，为一个 WR 预留额度票据。
    std::expected<JettyTicket, JettyPoolFailure> Reserve() noexcept;

    /// 在指定的物理 SQ 通道上为一个 WR 预留额度；同一批批处理 WR 若需绑定同一 SQ 可用此接口。
    std::expected<JettyTicket, JettyPoolFailure> ReserveOn(JettyLane lane) noexcept;

    /// 获取通道对应的物理 `urma_jetty_t*` 句柄以执行投递；句柄不具独占权，投递前必须已预留足额票据。
    /// 不得跨故障状态转换缓存句柄再投递；ERROR jetty 的 provider post 仍可能返回 SUCCESS。
    std::expected<urma_jetty_t*, JettyPoolFailure> Get(JettyLane lane) noexcept;

    /// 确认票据：在底层驱动接受 WR 后调用，票据由 Reserved 状态转换为 Posted 状态。
    /// 预留时已完成配额扣减，Commit 不会重复扣减额度。批量投递部分成功时，仅对成功前缀调用 Commit。
    std::expected<void, JettyPoolFailure> Commit(JettyTicket ticket) noexcept;

    /// 撤销票据：仅用于确定未被底层驱动接受的 WR（处于 Reserved 状态），将其配额归还；严禁撤销已提交工作。
    std::expected<void, JettyPoolFailure> Cancel(JettyTicket ticket) noexcept;

    /// 终结票据：在上层确认该已提交 WR（处于 Posted 状态）彻底终结且不再访问 DMA
    /// 内存（连接断开本身不构成此保证）后退休票据并归还配额。 严禁依据 CQE 数量推算非逐条 signaled
    /// 的完成数；FLUSH_ERR_DONE 属于边界事件，不对应用户票据。
    std::expected<void, JettyPoolFailure> Complete(JettyTicket ticket) noexcept;

    /// 仅执行本地隔离，不修改硬件状态；排空须继续调用 BeginDrain。
    /// 禁止新的预留与 Get；已存在票据仍可 Commit、Cancel 或 Complete。
    std::expected<void, JettyPoolFailure> MarkFaulted(JettyLane lane) noexcept;

    /// 先隔离 SQ，再修改硬件为 ERROR；失败仍禁止发送，可重试。
    /// 成功后幂等，不重复 modify；必须继续 poll 等待 FLUSH_ERR_DONE。
    std::expected<void, JettyPoolFailure> BeginDrain(JettyLane lane) noexcept;

    /// 仅在已切 ERROR 后，由完成层确认消费了该 SQ 的 FLUSH_ERR_DONE 时调用。
    /// 边界不对应 WR；不读取 user_ctx，也不归还票据。
    std::expected<void, JettyPoolFailure> ObserveFlushDone(JettyLane lane) noexcept;

    /// 仅在边界之后调用软件 flush；返回的 WR_UNHANDLED 仍须逐条核验并退休。
    /// 返回 0 且该 SQ 无残留票据才进入 kDrained；残留票据返回 kInUse，禁止推测释放。
    std::expected<int, JettyPoolFailure> FlushSend(JettyLane lane, std::span<urma_cr_t> completions) noexcept;

    std::expected<JettyLaneState, JettyPoolFailure> lane_state(JettyLane lane) const noexcept;

    /// 用 TX CQE 的本地 jetty id 查找当前通道，包括已隔离但仍需排空的 SQ。
    /// 不授予发送权限；未知 id 返回 kInvalidLane。
    std::expected<JettyLane, JettyPoolFailure> FindLane(std::uint32_t local_id) const noexcept;

    /// 查询池是否已就绪。
    bool ready() const noexcept {
        return ready_;
    }

    /// 获取池的总容量（配置的物理 Jetty 数量）。
    std::size_t capacity() const noexcept {
        return slots_.size();
    }

    /// 查询当前至少能预留一个 WR 的可用物理 SQ 数量（非完全空闲的 Jetty 数量）。
    std::size_t available() const noexcept;

private:
    enum class TicketState : std::uint8_t {
        kFree,
        kReserved,
        kPosted,
    };

    struct Entry {
        std::uint64_t sequence = 0;
        std::uint32_t next = 0;
        TicketState state = TicketState::kFree;
    };

    struct Slot {
        urma_jetty_t* jetty = nullptr;
        std::vector<Entry> entries;
        std::uint32_t free_head = 0;
        std::uint32_t used = 0;
        JettyLaneState state = JettyLaneState::kReady;
    };

    std::expected<Slot*, JettyPoolFailure> Find(JettyLane lane) noexcept;
    std::expected<Entry*, JettyPoolFailure> Find(JettyTicket ticket) noexcept;
    std::expected<void, JettyPoolFailure> Retire(JettyTicket ticket, TicketState state) noexcept;

    std::expected<void, JettyPoolError> Rollback(JettyPoolResource resource, int error,
                                                 std::uint32_t index = 0) noexcept;

    std::expected<int, JettyPoolFailure> Poll(urma_jfc_t* jfc, JettyPoolResource resource,
                                              std::span<urma_cr_t> completions) noexcept;

    urma_jfc_t* send_jfc_ = nullptr;
    urma_jfc_t* recv_jfc_ = nullptr;
    urma_jfr_t* jfr_ = nullptr;
    std::vector<Slot> slots_;
    std::uint64_t epoch_ = 0;
    std::uint64_t next_sequence_ = 0;
    std::uint32_t next_lane_ = 0;
    std::uint32_t tx_depth_ = 0;
    bool ready_ = false;
};
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_JETTY_POOL_HPP_
