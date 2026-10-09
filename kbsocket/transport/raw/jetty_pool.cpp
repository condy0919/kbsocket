// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/jetty_pool.hpp"

#include <cerrno>
#include <limits>
#include <new>
#include <stdexcept>

#include "kbsocket/base/log.hpp"

// 默认运行参数定义：仅在控制路径读取，不影响数据热路径性能。
DEFINE_uint32(kbsocket_tx_depth, 128, "Maximum outstanding WRs per kbsocket jetty");
DEFINE_uint32(kbsocket_rx_depth, 128, "Depth of the shared kbsocket JFR");
DEFINE_uint32(kbsocket_link_priority, 4, "JFS link priority for kbsocket jetties (0-15)");

namespace kbsocket {
namespace raw {
namespace {
// 构造队列或槽位故障信息的内部辅助函数。
JettyPoolFailure Failure(JettyPoolErrorCode code, std::uint32_t index = 0, int status = 0) {
    return {
        .code = code,
        .resource = JettyPoolResource::kJetty,
        .provider_error = status,
        .jetty_index = index,
    };
}
} // namespace

JettyPool::~JettyPool() {
    // 析构时尝试清理全部硬件资源；若底层删除失败，仅记录错误日志并不抛出异常，
    // 句柄以泄漏方式保留，交由操作系统在进程退出时统一回收。
    if (auto result = Close(); !result) {
        KBSOCKET_LOG_ERROR("jetty pool cleanup failed: resource={} status={}; retaining dependent resources",
                           static_cast<unsigned>(result.error().resource), result.error().provider_error);
    }
}

// 初始化失败时的原子回滚流程：调用 Close() 逆序释放已分配的部分硬件资源；
// 若回滚清理亦发生错误，将其保存在 cleanup_error 中，确保原始创建失败根因不被冲掉。
std::expected<void, JettyPoolError> JettyPool::Rollback(JettyPoolResource resource, int error,
                                                        std::uint32_t index) noexcept {
    JettyPoolError result{
        .failure =
            {
                .code = JettyPoolErrorCode::kCreateFailed,
                .resource = resource,
                .provider_error = error,
                .jetty_index = index,
            },
        .cleanup_error = {},
    };
    if (auto cleanup = Close(); !cleanup) {
        result.cleanup_error = cleanup.error();
    }
    return std::unexpected(result);
}

std::expected<void, JettyPoolError> JettyPool::Open(urma_context_t* ctx, const urma_device_cap_t& cap,
                                                    const JettyPoolConfig& c) noexcept {
    // 仅在控制路径读取 flags：显式配置优先；已创建的池不受运行期 flag 修改影响。
    const auto tx_depth = c.tx_depth.value_or(FLAGS_kbsocket_tx_depth);
    const auto rx_depth = c.rx_depth.value_or(FLAGS_kbsocket_rx_depth);
    const auto link_priority = FLAGS_kbsocket_link_priority;

    // 防止重复打开：已持有硬件队列资源或槽位时直接拒绝。
    if (send_jfc_ || recv_jfc_ || jfr_ || !slots_.empty()) {
        return std::unexpected(JettyPoolError{
            .failure =
                {
                    .code = JettyPoolErrorCode::kInUse,
                },
            .cleanup_error = {},
        });
    }

    const auto invalid_argument = std::unexpected(JettyPoolError{
        .failure =
            {
                .code = JettyPoolErrorCode::kInvalidArgument,
            },
        .cleanup_error = {},
    });

    // 强校验优先级：在转换为 uint8_t 之前检查完整 flag 值，防止数值截断导致溢出值伪装成合法优先级。
    if (link_priority > URMA_MAX_PRIORITY) {
        return invalid_argument;
    }

    // 传输模式校验：跨机仅支持 raw URMA 的 RM_CTP 模式（URMA_TM_RM 且 ctp == 1）。
    if (!ctx || !(cap.trans_mode & URMA_TM_RM) || !cap.rm_tp_cap.bs.ctp) {
        return invalid_argument;
    }
    if (!c.jetty_count || c.jetty_count > cap.max_jetty) {
        return invalid_argument;
    }

    // 硬件队列与完成队列深度上限校验。
    if (!tx_depth || tx_depth > cap.max_jfs_depth || !rx_depth || rx_depth > cap.max_jfr_depth) {
        return invalid_argument;
    }
    if (!c.tx_cq_depth || c.tx_cq_depth > cap.max_jfc_depth || !c.rx_cq_depth || c.rx_cq_depth > cap.max_jfc_depth) {
        return invalid_argument;
    }

    // 核心防 CQ 溢出公式校验：
    // 要求 `jetty_count * (tx_depth + 1) <= tx_cq_depth`。
    // 保证池内全部 Jetty 并发打满发送队列时 CQ 亦绝不会溢出丢 CQE；
    // 公式中每个 jetty 额外预留 1 个条目用于接收驱动产生的 `FLUSH_ERR_DONE` 边界事件，不占用用户 WR 额度。
    const auto required_tx_cq_depth =
        static_cast<std::uint64_t>(c.jetty_count) * (static_cast<std::uint64_t>(tx_depth) + 1);
    if (required_tx_cq_depth > c.tx_cq_depth || rx_depth > c.rx_cq_depth) {
        return invalid_argument;
    }

    if (!c.send_sge || c.send_sge > cap.max_jfs_sge || cap.max_jfs_rsge < 1 || cap.max_jfr_sge < 1) {
        return invalid_argument;
    }
    if (c.inline_bytes > cap.max_jfs_inline_len) {
        return invalid_argument;
    }

    // 纪元计数器防回绕溢出检查。
    if (epoch_ == std::numeric_limits<std::uint64_t>::max()) {
        return std::unexpected(JettyPoolError{
            .failure = Failure(JettyPoolErrorCode::kExhausted),
            .cleanup_error = {},
        });
    }

    // 预分配内存容器与自由链表：
    // 预先分配 slots_ 及每个 slot 内的 entries 数组，初始化单向自由链表（next 指针串联）。
    // 彻底规避在硬件资源创建成功后因 vector 动态扩容抛出 bad_alloc 而导致复杂的回滚状态。
    try {
        slots_.resize(c.jetty_count);
        for (auto& slot : slots_) {
            slot.entries.resize(tx_depth);
            for (std::uint32_t i = 0; i < tx_depth; ++i) {
                slot.entries[i].next = i + 1;
            }
        }
    } catch (const std::bad_alloc&) {
        slots_.clear();
        return std::unexpected(JettyPoolError{
            .failure = Failure(JettyPoolErrorCode::kNoMemory),
            .cleanup_error = {},
        });
    } catch (const std::length_error&) {
        slots_.clear();
        return std::unexpected(JettyPoolError{
            .failure = Failure(JettyPoolErrorCode::kNoMemory),
            .cleanup_error = {},
        });
    }

    // 硬件拓扑创建：
    // 阶段 1：创建发送完成队列（Send JFC）。配置 lock_free=1 实现无锁推进，不创建 JFCE（纯用户态轮询）。
    urma_jfc_cfg_t cq{};
    cq.depth = c.tx_cq_depth;
    cq.flag.bs.lock_free = 1;
    errno = 0;
    send_jfc_ = UrmaApi::CreateJfc(ctx, &cq);
    if (!send_jfc_) {
        return Rollback(JettyPoolResource::kSendJfc, errno);
    }

    // 阶段 2：创建接收完成队列（Recv JFC）。同样配置 lock_free=1。
    cq = {};
    cq.depth = c.rx_cq_depth;
    cq.flag.bs.lock_free = 1;
    errno = 0;
    recv_jfc_ = UrmaApi::CreateJfc(ctx, &cq);
    if (!recv_jfc_) {
        return Rollback(JettyPoolResource::kRecvJfc, errno);
    }

    // 阶段 3：创建共享接收队列（JFR）。绑定 Recv JFC 与 RM 传输模式，配置典型的 RNR 定时器。
    urma_jfr_cfg_t recv{
        .depth = rx_depth,
        .trans_mode = URMA_TM_RM,
        .max_sge = 1,
        .min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER, // 2us * 2^12 = 8.192ms RNR
        .jfc = recv_jfc_,
        .user_ctx = 0,
    };
    recv.flag.bs.lock_free = 1;
    errno = 0;
    jfr_ = UrmaApi::CreateJfr(ctx, &recv);
    if (!jfr_) {
        return Rollback(JettyPoolResource::kJfr, errno);
    }

    // 阶段 4：批量创建 Jetty。各 Jetty 共享公共 JFR 与 Recv JFC，各自发送队列 JFS 绑定 Send JFC。
    urma_jetty_cfg_t jetty{
        .jfs_cfg =
            {
                .depth = tx_depth,
                .trans_mode = URMA_TM_RM,
                .priority = static_cast<std::uint8_t>(link_priority),
                .max_sge = c.send_sge,
                .max_rsge = 1,
                .max_inline_data = c.inline_bytes,
                .rnr_retry = 6,
                .err_timeout = 2, // 4.096us * 2^2 = 16.384us
                .jfc = send_jfc_,
                .user_ctx = 0,
            },
        .shared =
            {
                .jfr = jfr_,
                .jfc = recv_jfc_,
            },
        .user_ctx = 0,
    };
    jetty.flag.bs.share_jfr = 1;
    jetty.jfs_cfg.flag.bs.lock_free = 1;
    for (std::uint32_t i = 0; i < c.jetty_count; ++i) {
        auto cfg = jetty;
        errno = 0;
        slots_[i].jetty = UrmaApi::CreateJetty(ctx, &cfg);
        if (!slots_[i].jetty) {
            return Rollback(JettyPoolResource::kJetty, errno, i);
        }
    }

    // 更新生命周期纪元，重置轮转游标并激活就绪状态。
    ++epoch_;
    next_lane_ = 0;
    tx_depth_ = tx_depth;
    ready_ = true;
    return {};
}

std::expected<void, JettyPoolFailure> JettyPool::Close() noexcept {
    // 票据必须全部退休；已经切 ERROR 的队列还须消费边界并完成软件排空，避免丢下 fake CQE。
    // 拒绝关闭时刻意保持 ready_ 为 true，确保调用方仍可继续调用 PollSend/PollRecv 与 Complete 推进排空。
    for (std::uint32_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i].used || slots_[i].state == JettyLaneState::kError ||
            slots_[i].state == JettyLaneState::kFlushReady) {
            return std::unexpected(Failure(JettyPoolErrorCode::kInUse, i));
        }
    }

    // 全部发送票据已退休，置为未就绪以阻断新的提交与轮询。
    ready_ = false;

    // 逆序销毁各个 Jetty 硬件句柄；若任一删除失败，保留剩余句柄并中断返回，允许外部排查后重试。
    for (std::size_t i = slots_.size(); i > 0; --i) {
        auto& slot = slots_[i - 1];
        if (!slot.jetty) {
            continue;
        }
        auto status = UrmaApi::DeleteJetty(slot.jetty);
        if (status != URMA_SUCCESS) {
            return std::unexpected(Failure(JettyPoolErrorCode::kDeleteFailed, i - 1, status));
        }
        slot.jetty = nullptr;
    }
    slots_.clear();

    // 严格按依赖关系反向销毁父级队列：JFR -> Recv JFC -> Send JFC。
    // 使用 lambda 确保：成功删除后立即将指针置空以防重试时重复释放；若子资源删除失败，绝不提前销毁其引用的父 JFC。
    auto drop = [](auto*& ptr, auto deleter, JettyPoolResource resource) -> std::expected<void, JettyPoolFailure> {
        if (!ptr) {
            return {};
        }
        const auto status = deleter(ptr);
        if (status != URMA_SUCCESS) {
            return std::unexpected(JettyPoolFailure{
                .code = JettyPoolErrorCode::kDeleteFailed,
                .resource = resource,
                .provider_error = status,
            });
        }
        ptr = nullptr;
        return {};
    };
    if (auto r = drop(jfr_, UrmaApi::DeleteJfr, JettyPoolResource::kJfr); !r) {
        return r;
    }
    if (auto r = drop(recv_jfc_, UrmaApi::DeleteJfc, JettyPoolResource::kRecvJfc); !r) {
        return r;
    }
    return drop(send_jfc_, UrmaApi::DeleteJfc, JettyPoolResource::kSendJfc);
}

std::expected<int, JettyPoolFailure> JettyPool::Poll(urma_jfc_t* jfc, JettyPoolResource resource,
                                                     std::span<urma_cr_t> completions) noexcept {
    if (!ready_) {
        return std::unexpected(JettyPoolFailure{
            .code = JettyPoolErrorCode::kNotReady,
            .resource = resource,
        });
    }
    if (completions.empty() || completions.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return std::unexpected(JettyPoolFailure{
            .code = JettyPoolErrorCode::kInvalidArgument,
            .resource = resource,
        });
    }

    // 调用底层驱动轮询硬件完成队列；lock_free 模式下在用户态直接消费 CQE，无全局互斥锁。
    const int count = UrmaApi::PollJfc(jfc, static_cast<int>(completions.size()), completions.data());
    if (count < 0 || static_cast<std::size_t>(count) > completions.size()) {
        return std::unexpected(JettyPoolFailure{
            .code = JettyPoolErrorCode::kPollFailed,
            .resource = resource,
            .provider_error = count,
        });
    }
    return count;
}

std::expected<int, JettyPoolFailure> JettyPool::PollSend(std::span<urma_cr_t> completions) noexcept {
    return Poll(send_jfc_, JettyPoolResource::kSendJfc, completions);
}

std::expected<int, JettyPoolFailure> JettyPool::PollRecv(std::span<urma_cr_t> completions) noexcept {
    return Poll(recv_jfc_, JettyPoolResource::kRecvJfc, completions);
}

std::expected<JettyPool::Slot*, JettyPoolFailure> JettyPool::Find(JettyLane lane) noexcept {
    // 校验池就绪状态、所属指针、epoch 纪元匹配（防跨生命周期误用）及槽位索引边界。
    if (!ready_ || lane.pool != this || lane.epoch != epoch_ || lane.index >= slots_.size()) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidLane, lane.index));
    }
    return &slots_[lane.index];
}

std::expected<JettyPool::Entry*, JettyPoolFailure> JettyPool::Find(JettyTicket ticket) noexcept {
    auto slot = Find(ticket.lane);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    if (ticket.index >= (*slot)->entries.size()) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidTicket, ticket.lane.index));
    }
    auto& entry = (*slot)->entries[ticket.index];
    // 核心安全校验：条目必须处于使用中（非 kFree），且 sequence 必须完全吻合。
    // 严格防止迟到完成、重复完成或已回收条目被错误修改（防 ABA 错乱）。
    if (entry.state == TicketState::kFree || entry.sequence != ticket.sequence) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidTicket, ticket.lane.index));
    }
    return &entry;
}

std::expected<JettyLane, JettyPoolFailure> JettyPool::FindLane(std::uint32_t local_id) const noexcept {
    if (!ready_) {
        return std::unexpected(Failure(JettyPoolErrorCode::kNotReady));
    }
    for (std::uint32_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i].jetty && slots_[i].jetty->jetty_id.id == local_id) {
            return JettyLane{this, i, epoch_};
        }
    }
    return std::unexpected(Failure(JettyPoolErrorCode::kInvalidLane));
}

std::size_t JettyPool::available() const noexcept {
    if (!ready_) {
        return 0;
    }

    // 统计当前未故障且尚有剩余可预留额度（used < tx_depth_）的物理 SQ 数量。
    std::size_t count = 0;
    for (const auto& slot : slots_) {
        count += slot.state == JettyLaneState::kReady && slot.used < tx_depth_;
    }
    return count;
}

std::expected<JettyTicket, JettyPoolFailure> JettyPool::Reserve() noexcept {
    if (!ready_) {
        return std::unexpected(Failure(JettyPoolErrorCode::kNotReady));
    }

    // Round-Robin 轮转查找未故障且有空闲额度的 SQ。
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        const auto index = next_lane_;
        next_lane_ = (next_lane_ + 1) % slots_.size();
        if (slots_[index].state == JettyLaneState::kReady && slots_[index].used < tx_depth_) {
            return ReserveOn(JettyLane{
                .pool = this,
                .index = index,
                .epoch = epoch_,
            });
        }
    }
    return std::unexpected(Failure(JettyPoolErrorCode::kExhausted));
}

std::expected<JettyTicket, JettyPoolFailure> JettyPool::ReserveOn(JettyLane lane) noexcept {
    auto result = Find(lane);
    if (!result) {
        return std::unexpected(result.error());
    }

    auto& slot = **result;
    if (slot.state != JettyLaneState::kReady) {
        return std::unexpected(Failure(JettyPoolErrorCode::kFaulted, lane.index));
    }
    // 额度满或序号计数器达到上限时拒绝预留。
    if (slot.used == tx_depth_ || next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        return std::unexpected(Failure(JettyPoolErrorCode::kExhausted, lane.index));
    }

    // 从单向自由链表头部弹出可用 Entry，分配单调递增 sequence 并标记为 Reserved 状态。
    const auto index = slot.free_head;
    auto& entry = slot.entries[index];
    slot.free_head = entry.next;
    entry.sequence = ++next_sequence_;
    entry.state = TicketState::kReserved;
    ++slot.used;
    return JettyTicket{
        .lane = lane,
        .index = index,
        .sequence = entry.sequence,
    };
}

std::expected<urma_jetty_t*, JettyPoolFailure> JettyPool::Get(JettyLane lane) noexcept {
    auto slot = Find(lane);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    // 故障 SQ 严禁获取硬件句柄投递新请求。
    if ((*slot)->state != JettyLaneState::kReady) {
        return std::unexpected(Failure(JettyPoolErrorCode::kFaulted, lane.index));
    }

    return (*slot)->jetty;
}

std::expected<void, JettyPoolFailure> JettyPool::Commit(JettyTicket ticket) noexcept {
    auto entry = Find(ticket);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->state != TicketState::kReserved) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidTicket, ticket.lane.index));
    }

    // 票据状态机转换：Reserved -> Posted。
    // 关键契约：即使通道后续被 MarkFaulted，底层硬件此前已接受的 WR 仍必须 Commit，绝不能被当作未提交 Cancel 释放。
    (*entry)->state = TicketState::kPosted;
    return {};
}

std::expected<void, JettyPoolFailure> JettyPool::Retire(JettyTicket ticket, TicketState state) noexcept {
    auto entry = Find(ticket);
    if (!entry) {
        return std::unexpected(entry.error());
    }
    if ((*entry)->state != state) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidTicket, ticket.lane.index));
    }

    // 票据状态机终结：将 Entry 放回自由链表头部，重置为 kFree 并递减 used 计数。
    auto& slot = slots_[ticket.lane.index];
    (*entry)->state = TicketState::kFree;
    (*entry)->next = slot.free_head;
    slot.free_head = ticket.index;
    --slot.used;
    return {};
}

std::expected<void, JettyPoolFailure> JettyPool::Cancel(JettyTicket ticket) noexcept {
    // 撤销未被硬件接受的 WR：要求票据处于 Reserved 状态。
    return Retire(ticket, TicketState::kReserved);
}

std::expected<void, JettyPoolFailure> JettyPool::Complete(JettyTicket ticket) noexcept {
    // 终结已成功提交且已产生完成事件的 WR：要求票据处于 Posted 状态。
    return Retire(ticket, TicketState::kPosted);
}

std::expected<void, JettyPoolFailure> JettyPool::MarkFaulted(JettyLane lane) noexcept {
    auto slot = Find(lane);
    if (!slot) {
        return std::unexpected(slot.error());
    }
    // 标记故障隔离：阻断后续新的 Reserve / Get，但保留通道以供已有在途票据继续 Cancel 或 Complete。
    if ((*slot)->state == JettyLaneState::kReady) {
        (*slot)->state = JettyLaneState::kFaulted;
    }
    return {};
}

std::expected<JettyLaneState, JettyPoolFailure> JettyPool::lane_state(JettyLane lane) const noexcept {
    if (!ready_ || lane.pool != this || lane.epoch != epoch_ || lane.index >= slots_.size()) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidLane, lane.index));
    }
    return slots_[lane.index].state;
}

std::expected<void, JettyPoolFailure> JettyPool::BeginDrain(JettyLane lane) noexcept {
    auto isolated = MarkFaulted(lane);
    if (!isolated) {
        return isolated;
    }
    auto& slot = slots_[lane.index];
    if (slot.state != JettyLaneState::kFaulted) {
        return {};
    }
    urma_jetty_attr_t attr{};
    attr.mask = JETTY_STATE;
    attr.state = URMA_JETTY_STATE_ERROR;
    const auto status = UrmaApi::ModifyJetty(slot.jetty, &attr);
    if (status != URMA_SUCCESS) {
        return std::unexpected(Failure(JettyPoolErrorCode::kModifyFailed, lane.index, status));
    }
    slot.state = JettyLaneState::kError;
    return {};
}

std::expected<void, JettyPoolFailure> JettyPool::ObserveFlushDone(JettyLane lane) noexcept {
    auto found = Find(lane);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (slot.state != JettyLaneState::kError) {
        // 意外边界不能替代本地状态转换，但也不能继续向该 SQ 发送。
        if (slot.state == JettyLaneState::kReady) {
            slot.state = JettyLaneState::kFaulted;
        }
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidState, lane.index));
    }
    slot.state = JettyLaneState::kFlushReady;
    return {};
}

std::expected<int, JettyPoolFailure> JettyPool::FlushSend(JettyLane lane, std::span<urma_cr_t> completions) noexcept {
    auto found = Find(lane);
    if (!found) {
        return std::unexpected(found.error());
    }
    auto& slot = **found;
    if (completions.empty() || completions.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidArgument, lane.index));
    }
    if (slot.state == JettyLaneState::kDrained) {
        return 0;
    }
    if (slot.state != JettyLaneState::kFlushReady) {
        return std::unexpected(Failure(JettyPoolErrorCode::kInvalidState, lane.index));
    }
    const int count = UrmaApi::FlushJetty(slot.jetty, static_cast<int>(completions.size()), completions.data());
    if (count < 0 || static_cast<std::size_t>(count) > completions.size()) {
        return std::unexpected(Failure(JettyPoolErrorCode::kFlushFailed, lane.index, count));
    }
    if (count == 0) {
        // 软件队列为空不等于账本为空；缺失或不可信的完成必须保留资源并报告。
        if (slot.used) {
            return std::unexpected(Failure(JettyPoolErrorCode::kInUse, lane.index));
        }
        slot.state = JettyLaneState::kDrained;
    }
    return count;
}
} // namespace raw
} // namespace kbsocket
