// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_EAGER_TX_HPP_
#define KBSOCKET_CONNECTION_EAGER_TX_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/tx_sequence_window.hpp"
#include "kbsocket/connection/wire_frame.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace connection {

enum class EagerTxErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kWouldBlock,
    kStopped,
    kWrongIdentity,
    kInvalidCompletion,
    kMemory,
    kImport,
    kSequence,
    kCredit,
    kPost,
    kAcceptanceUnknown,
    kAccounting,
    kCompletion,
    kRetained
};
struct EagerTxError {
    EagerTxErrorCode code;
    int provider_status = 0;
};
struct EagerTxConfig {
    urma_context_t* local_context = nullptr;
    std::uint64_t connection_id = 0;
    WireToken destination;
    SequenceSession session;
    CreditIdentity credit_identity;
    std::uint32_t capacity = 256;
    std::uint64_t max_frame_seq = 0;
    std::uint64_t max_stream_bytes = 0;
    std::uint64_t initial_advertised_control = 0;
    // 宿主按 PeerRuntime 共享已发布的控制上界；独立原语测试可省略并使用上述本地基线。
    ReceiveCreditDomain* advertised_credits = nullptr;
    CreditPeerHandle advertised_peer;
};
struct EagerTxStats {
    std::uint64_t copied_bytes = 0;
    std::uint64_t accepted = 0;
    std::uint64_t retired = 0;
    std::size_t queued = 0;
    std::size_t in_flight = 0;
    bool failed = false;
    bool write_shutdown = false;
    bool fin_accepted = false;
};

/// 单 owner 的有界 Eager 发送流。Write 成功的字节已复制到注册 Slab，可立即复用用户缓冲。
/// TxSender、Slab、导入缓存和 PeerRuntime 共账必须存活至 Close 成功；宿主仅在 RawActive 后推进。
/// 逐 WR 保存独立 import/DMA 引用；终结失败、Close 或析构不能代替可信 CQE 释放依赖。
class EagerTxStream {
public:
    EagerTxStream() noexcept;
    ~EagerTxStream();
    EagerTxStream(const EagerTxStream&) = delete;
    EagerTxStream& operator=(const EagerTxStream&) = delete;
    /// 可绑定尚未完成的 import，以接收 READY 前的 CREDIT；本对象只允许 Open 一次。
    /// sender 必须开启 defer_drain；Open 预留一个注册控制 buffer，避免 DATA 耗尽 Slab 后授信死锁。
    std::expected<void, EagerTxError> Open(const EagerTxConfig& config, raw::TxSender& sender,
                                           memory::SlabAllocator& slab, control::EndpointImportCache& imports,
                                           control::ImportLease endpoint, SenderCreditAccount& credits) noexcept;
    std::expected<std::size_t, EagerTxError> Write(std::span<const std::byte> bytes) noexcept;
    /// FIN 排在全部已接受 write 的数据之后；半关闭仍允许发送 CREDIT_UPDATE。
    std::expected<void, EagerTxError> ShutdownWrite() noexcept;
    /// 无分配合并累计额度；session 必须是接收方 session，token 由绑定强制填入。
    std::expected<void, EagerTxError> QueueCredit(const WireFrameInput& credit) noexcept;
    /// 每次最多一个 256 WR post。返回实际可信接受数；资源暂缺返回 kWouldBlock。
    std::expected<std::size_t, EagerTxError> Progress() noexcept;
    /// 仅接收 TxCompletionProcessor 的逐 WR 终结；拒绝事件不解除任何 DMA/import 引用。
    std::expected<void, EagerTxError> OnCompletion(const raw::TxCompletionEvent& event) noexcept;
    std::expected<void, EagerTxError> ApplyCredit(SequenceSession session, CreditIdentity identity,
                                                  std::uint64_t max_data, std::uint64_t max_control,
                                                  std::uint64_t max_frames, std::uint64_t max_bytes) noexcept;
    /// 丢弃确定未提交的数据；保留所有已提交或接受未知的 DMA，不改变其他连接/SQ/共账状态。
    void Fail() noexcept;
    std::expected<void, EagerTxError> Close() noexcept;
    EagerTxStats stats() const noexcept;

private:
    struct State;
    std::expected<std::size_t, EagerTxError> Post(bool control) noexcept;
    std::expected<void, EagerTxError> Drop(std::uint32_t index) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_EAGER_TX_HPP_
