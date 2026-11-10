// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_RNDV_CONNECTION_HPP_
#define KBSOCKET_CONNECTION_RNDV_CONNECTION_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/connection/credit_account.hpp"
#include "kbsocket/connection/eager_rx.hpp"
#include "kbsocket/connection/eager_tx.hpp"
#include "kbsocket/connection/rndv_receiver.hpp"
#include "kbsocket/control/connection_handshake.hpp"
#include "kbsocket/control/endpoint_import_cache.hpp"
#include "kbsocket/control/tseg_import_cache.hpp"
#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/tx_completion_processor.hpp"
#include "kbsocket/transport/raw/tx_sender.hpp"

namespace kbsocket {
namespace connection {
/// 显式启用后才允许 Connection 公告 RNDV；预算在 PrepareRaw 控制路径落实。
/// 当前一份本地与远端 Arena，超出能力拒绝准备，不按空闲连接自动创建默认池。
struct RndvConnectionConfig {
    std::uint32_t tx_capacity = 4;
    std::uint32_t receive_capacity = 128;
    std::uint32_t notification_capacity = 16;
    std::uint32_t ingress_packets = 16;
    std::uint32_t max_chunks = 64;
    std::uint32_t post_budget = 32;
    std::uint64_t chunk_bytes = 200u * 1024u * 1024u;
    std::size_t threshold = 16384;
    std::uint64_t timeout_ns = 10000000000;
    std::size_t progress_budget = 8;
};
struct RndvConnectionPreparation {
    memory::SlabAllocator* target_slab = nullptr;
    control::TsegImportCache* segments = nullptr;
    raw::JettyLane lane;
};
enum class RndvConnectionError : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kWouldBlock,
    kMemory,
    kProtocol,
    kTx,
    kRx,
    kInvalidCompletion,
    kRetained
};
/// Connection 内部装配器：拥有固定 RNDV 元数据，借用已握手身份与共享 raw 资源。
/// Open/Close 是控制路径；Progress/Write/Accept/OnCompletion 不分配。
class RndvConnection {
public:
    static constexpr std::uint64_t kOwnerMask = std::uint64_t{3} << 62;
    static constexpr std::uint64_t kWriteOwner = std::uint64_t{1} << 62;
    static constexpr std::uint64_t kNotificationOwner = std::uint64_t{2} << 62;
    RndvConnection() noexcept;
    ~RndvConnection();
    RndvConnection(const RndvConnection&) = delete;
    RndvConnection& operator=(const RndvConnection&) = delete;
    static bool ValidConfig(const RndvConnectionConfig& config) noexcept;
    std::expected<void, RndvConnectionError>
    Open(const RndvConnectionConfig& config, const RndvConnectionPreparation& prep,
         control::ConnectionHandshake& handshake, EagerTxStream& eager, EagerReceiveDomain& receive,
         raw::TxSender& sender, memory::SlabAllocator& source, SenderCreditAccount& credits,
         control::EndpointImportCache& endpoints, std::uint64_t connection_id) noexcept;
    RndvReceiver* receiver() noexcept;
    bool Owns(std::uint64_t owner) const noexcept;
    bool pending_sequence() const noexcept;
    std::expected<std::size_t, RndvConnectionError> Write(std::span<const std::byte> input,
                                                          std::uint64_t now_ns) noexcept;
    std::expected<std::size_t, RndvConnectionError> WriteLease(memory::BufferLease source, std::size_t offset,
                                                               std::size_t bytes, std::uint64_t now_ns) noexcept;
    std::expected<void, RndvConnectionError> Accept(EagerRxPacket packet) noexcept;
    std::expected<void, RndvConnectionError> OnCompletion(const raw::TxCompletionEvent& event) noexcept;
    /// active=false 只清理取消后的事务与迟到 DONE，不提交新通知或 WRITE。
    std::expected<std::size_t, RndvConnectionError> Progress(std::uint64_t now_ns, bool active) noexcept;
    std::expected<void, RndvConnectionError> Consumed(std::uint64_t prefix) noexcept;
    void Cancel() noexcept;
    /// 宿主先关闭混合 RX stream，归还全部应用借用；失败保留 route/window 和依赖。
    std::expected<void, RndvConnectionError> Close() noexcept;
    std::size_t pending_rx() const noexcept;
    std::uint64_t retired() const noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_RNDV_CONNECTION_HPP_
