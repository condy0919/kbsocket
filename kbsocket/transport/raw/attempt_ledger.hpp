// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_ATTEMPT_LEDGER_HPP_
#define KBSOCKET_TRANSPORT_RAW_ATTEMPT_LEDGER_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <vector>

#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp" // IWYU pragma: keep

namespace kbsocket {
namespace raw {
/// 可写入 WR user_ctx 的身份；同一账本对象存活期间不复用，0 无效。
using AttemptId = std::uint64_t;

/// 标识由上层分配并保证其代次安全；不是 fd 或可随意复用的数组下标。
/// 租约标识引用上层持有的资源，不是拥有型指针；0 表示不涉及该类租约。
struct AttemptMetadata {
    std::uint64_t connection_id = 0;
    std::uint64_t operation_id = 0;
    std::uint64_t operation_generation = 0;
    urma_target_jetty_t* target = nullptr;
    urma_opcode_t opcode = URMA_OPC_SEND;
    std::uint64_t buffer_lease = 0;
    std::uint64_t grant_lease = 0;
    bool signaled = true;
};

struct AttemptRecord {
    AttemptId id = 0;
    JettyTicket ticket;
    AttemptMetadata metadata;
    /// 由提交层提供的实际提交位置，不能用预留票据序号代替提交顺序。
    std::uint64_t submission_sequence = 0;
};

enum class AttemptLedgerErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kNotReady,
    kInUse,
    kNoMemory,
    kExhausted,
    kInvalidId,
    kInvalidState,
    kPoolFailure,
};

struct AttemptLedgerError {
    AttemptLedgerErrorCode code;
    std::optional<JettyPoolFailure> pool_failure;
};

/// 单 owner、固定容量发送账本；Open 分配内存，数据路径不分配且不可重入。
/// Prepare 同时预留账本与 SQ 额度；Commit 后才计入在途集合。
/// 仅支持全 signal 基线，不解析 CQE、不执行 post，也不推导累计完成或 flush 边界。
/// 上层须将记录关联的目标、buffer/grant 保持到 Cancel/Complete 返回，并分别处理其协议生命周期。
/// 连接关闭不能作为 Complete 的依据；必须确认对应 WR 已终结且不再访问 DMA 内存。
/// pool 必须覆盖账本生命周期；使用本账本的票据只能经本账本流转。
/// 析构不会取消或退休在途 WR；存在未退休记录时，上层必须让账本及全部依赖常驻。
class AttemptLedger {
public:
    AttemptLedger() = default;

    AttemptLedger(const AttemptLedger&) = delete;
    AttemptLedger& operator=(const AttemptLedger&) = delete;

    std::expected<void, AttemptLedgerError> Open(JettyPool& pool, std::uint32_t capacity) noexcept;

    std::expected<void, AttemptLedgerError> Close() noexcept;

    /// 失败不接管资源引用；返回的 id 可作为 WR user_ctx，CQ 路由必须先选中所属账本。
    std::expected<AttemptId, AttemptLedgerError> Prepare(const AttemptMetadata& metadata,
                                                         std::optional<JettyLane> lane = {}) noexcept;

    /// post 接受后调用。批量部分成功仅确认前缀；必须在 poll 前完成登记。
    std::expected<void, AttemptLedgerError> Commit(AttemptId id, std::uint64_t submission_sequence) noexcept;

    /// 返回副本供提交层读取目标和 ticket；副本不转移资源生命周期责任。
    std::expected<AttemptRecord, AttemptLedgerError> Lookup(AttemptId id) const noexcept;

    /// 仅取消未被 provider 接受的记录，返回记录供上层处理未提交资源。
    std::expected<AttemptRecord, AttemptLedgerError> Cancel(AttemptId id) noexcept;

    /// 仅退休已提交且已由完成层确认终结的 WR，返回记录供上层分发结果和处理资源。
    std::expected<AttemptRecord, AttemptLedgerError> Complete(AttemptId id) noexcept;

    std::size_t size() const noexcept {
        return used_;
    }
    std::size_t posted() const noexcept {
        return posted_;
    }

private:
    enum class State : std::uint8_t {
        kFree,
        kPrepared,
        kPosted,
    };

    struct Entry {
        AttemptRecord record;
        State state = State::kFree;
    };

    std::expected<std::size_t, AttemptLedgerError> Find(AttemptId id) const noexcept;
    std::expected<AttemptRecord, AttemptLedgerError> Retire(AttemptId id, State state) noexcept;

    JettyPool* pool_ = nullptr;
    std::vector<Entry> entries_;
    AttemptId next_id_ = 0;
    std::size_t used_ = 0;
    std::size_t posted_ = 0;
};
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_ATTEMPT_LEDGER_HPP_
