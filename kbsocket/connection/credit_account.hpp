// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_CREDIT_ACCOUNT_HPP_
#define KBSOCKET_CONNECTION_CREDIT_ACCOUNT_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>

namespace kbsocket {
namespace control {
class PeerCreditControl;
}
namespace connection {

enum class CreditErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kExhausted,
    kStopped,
    kWrongIdentity,
    kInvalidReceipt,
    kInvalidReservation,
    kSequenceExhausted,
    kProofRequired,
    kRetained
};
struct CreditError {
    CreditErrorCode code;
};
enum class CreditClass : std::uint8_t { kData, kControl };

/// 接收域代次已包含 incarnation；逻辑连接关闭不改变 PeerRuntime 共账身份。
struct CreditIdentity {
    std::uint64_t peer_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t epoch = 0;
    bool operator==(const CreditIdentity&) const = default;
};

class SenderCreditAccount;
struct CreditReservation {
    const SenderCreditAccount* account = nullptr;
    std::uint64_t id = 0;
    CreditClass kind = CreditClass::kData;
    std::uint32_t count = 0;
};
struct SenderCreditStats {
    std::array<std::uint64_t, 2> limits{};
    std::array<std::uint64_t, 2> spent{};
    std::array<std::uint64_t, 2> reserved{};
    bool stopped = false;
    // Resolve(nullopt) 后本 epoch 永久未知；不能用保守 spent 总数生成撤回完成证明。
    bool accepted_known = true;
};

/// 单 owner 的远端物理 RQE 授信账；没有通过 TX CQE 返还远端额度的接口。
/// 一个待决 post 批次阻止后续 Reserve，明确未接受的后缀结账后才可继续。
/// 对象生命周期内绑定同一 PeerRuntime/ReceiveDomain，重新 Open 的 epoch 必须严格增加。
class SenderCreditAccount {
public:
    SenderCreditAccount() noexcept = default;
    SenderCreditAccount(const SenderCreditAccount&) = delete;
    SenderCreditAccount& operator=(const SenderCreditAccount&) = delete;
    std::expected<void, CreditError> Open(CreditIdentity identity) noexcept;
    std::expected<void, CreditError> Update(CreditIdentity identity, std::uint64_t max_data,
                                            std::uint64_t max_control) noexcept;
    std::expected<CreditReservation, CreditError> Reserve(CreditClass kind, std::uint32_t count) noexcept;
    /// nullopt 表示接受范围未知：全部保留为 spent 并停止准入，绝不猜测可返还后缀。
    /// 已知前缀只收走其 credits；0 仅可用于可信的全批未接受。
    std::expected<void, CreditError> Resolve(CreditReservation reservation,
                                             std::optional<std::uint32_t> accepted_prefix) noexcept;
    void Stop() noexcept {
        stopped_ = true;
    }
    std::expected<void, CreditError> Close() noexcept;
    std::uint64_t available(CreditClass kind) const noexcept;
    SenderCreditStats stats() const noexcept;
    CreditIdentity identity() const noexcept {
        return identity_;
    }
    bool ready() const noexcept {
        return ready_;
    }

private:
    friend class control::PeerCreditControl;
    std::expected<void, CreditError> BindWithdrawal(const control::PeerCreditControl& owner) noexcept;
    std::expected<void, CreditError> UnbindWithdrawal(const control::PeerCreditControl& owner) noexcept;
    std::expected<SenderCreditStats, CreditError>
    StoppedSnapshot(const control::PeerCreditControl& owner) const noexcept;
    CreditIdentity identity_{};
    std::array<std::uint64_t, 2> limits_{};
    std::array<std::uint64_t, 2> spent_{};
    CreditReservation pending_{};
    const control::PeerCreditControl* withdrawal_owner_ = nullptr;
    std::uint64_t next_reservation_ = 0;
    bool ready_ = false, stopped_ = false, accepted_known_ = true;
};

class ReceiveCreditDomain;
class EagerReceiveDomain;
struct CreditPeerHandle {
    const ReceiveCreditDomain* domain = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t id = 0;
};
struct RxCreditReceipt {
    const ReceiveCreditDomain* domain = nullptr;
    std::uint32_t slot = 0;
    std::uint64_t id = 0;
    bool operator==(const RxCreditReceipt&) const = default;
};
struct CreditDomainConfig {
    std::uint64_t receive_domain = 0;
    std::uint32_t capacity = 8192;
    std::uint32_t control_slots = 512;
    std::uint32_t peer_capacity = 1024;
    std::uint32_t base_credits = 8;
};
struct CreditCounts {
    std::uint64_t free = 0, granted = 0, unposted = 0;
};
struct CreditDomainStats {
    CreditCounts data, control;
    std::uint64_t elastic_free = 0;
    std::uint64_t physical_posted = 0;
    std::uint64_t held = 0, released = 0, unattributed = 0;
    std::uint32_t active_peers = 0, tombstones = 0;
    bool frozen = false;
};
struct CreditPeerStats {
    CreditIdentity identity;
    CreditCounts base;
    std::uint64_t elastic_granted = 0, control_granted = 0;
    std::uint64_t max_data = 0, max_control = 0, pending_rx = 0;
    bool frozen = false, retired = false;
};
struct CreditRetireProof {
    bool sender_stopped = false;
    bool accepted_set_settled = false;
    bool unused_grants_revoked = false;
};

/// 单 owner、固定容量的接收域 F/G/U 总账。只记录可信事实，不调用 post/Release/网络协议。
/// 初始未投递额度为 U；Consume 返回的 receipt 在实际 Lease 归还后 Release，只有可信
/// refill accepted 前缀才 RecordRefillAccepted。descriptor 复制不增加物理容量。
/// data/control 是共享 JFR 上的软件预算，不是硬件独立槽；RTS 必须走 data。
/// peer tombstone 占固定表项直到域 Close，同一域不复用已退休身份。
class ReceiveCreditDomain {
public:
    static constexpr std::uint32_t kMaxCapacity = 1u << 20;
    static constexpr std::uint32_t kMaxPeers = 65536;
    static constexpr std::size_t kMaxRefillBatch = 256;
    ReceiveCreditDomain() noexcept;
    ~ReceiveCreditDomain();
    ReceiveCreditDomain(const ReceiveCreditDomain&) = delete;
    ReceiveCreditDomain& operator=(const ReceiveCreditDomain&) = delete;
    /// 同一对象再次打开必须使用更大的 receive_domain incarnation，避免旧身份重入。
    std::expected<void, CreditError> Open(const CreditDomainConfig& config) noexcept;
    /// 只关闭授信元数据，不终结真实 Posted RQE/DMA；M1 域关闭仍由宿主推进。
    /// 冻结 receipt 的整域清退仅由 EagerReceiveDomain 验证真实物理域关闭后执行。
    std::expected<void, CreditError> Close() noexcept;
    std::expected<void, CreditError> RecordInitialPosted(CreditClass kind, std::uint32_t accepted) noexcept;
    std::expected<CreditPeerHandle, CreditError> Admit(CreditIdentity identity) noexcept;
    /// 发布前 F->G；返回可重复发送的累计上界，发布失败不能擅自回收已经可能暴露的授权。
    std::expected<std::uint64_t, CreditError> Grant(CreditPeerHandle peer, CreditClass kind,
                                                    std::uint32_t count) noexcept;
    /// Peer 共账上已经通过 READY 或可信 accepted CREDIT 暴露的控制授信；不能按逻辑连接重复计新。
    std::expected<std::uint64_t, CreditError> PublishedControl(CreditPeerHandle peer) const noexcept;
    std::expected<void, CreditError> RecordPublishedControl(CreditPeerHandle peer, std::uint64_t limit) noexcept;
    /// 以可信 CQE 的完整 Peer 授信归属结账；冻结/旧连接的合法迟到帧仍扣原 epoch。
    /// 若真实 RX CQE 已消耗 RQE 而本调用失败，必须改走 ConsumeUnknown 并隔离，不能只丢帧。
    /// 本接口不是收包前的额度探测；非法/重复调用本身不代表又一次真实 RQE 消耗。
    std::expected<RxCreditReceipt, CreditError> Consume(CreditPeerHandle peer, CreditClass kind) noexcept;
    /// 未知 CQE 扣物理 posted 并冻结域；不猜测应扣哪份 G，未归属责任禁止重新授信或关闭。
    std::expected<RxCreditReceipt, CreditError> ConsumeUnknown() noexcept;
    std::expected<void, CreditError> Release(RxCreditReceipt receipt) noexcept;
    std::expected<void, CreditError> RecordRefillAccepted(std::span<const RxCreditReceipt> receipts) noexcept;
    /// 仅冻结新准入/授信，不假造 RQE 消耗。refill 接受未知等外部故障通过此处停用域，
    /// 仍可记录既有可信 accepted 前缀；未知 raw DMA 责任继续由宿主保留，不能猜测记账。
    void Freeze() noexcept;
    std::expected<void, CreditError> FreezePeer(CreditPeerHandle peer) noexcept;
    std::expected<void, CreditError> RetirePeer(CreditPeerHandle peer, CreditRetireProof proof) noexcept;
    CreditDomainStats stats() const noexcept;
    /// 当前打开的域 incarnation；关闭后为 0，重开必须严格递增。
    std::uint64_t receive_domain() const noexcept;
    std::expected<CreditPeerStats, CreditError> PeerStats(CreditPeerHandle peer) const noexcept;

private:
    friend class EagerReceiveDomain;
    friend class control::PeerCreditControl;
    std::expected<void, CreditError> BindWithdrawal(const control::PeerCreditControl& owner,
                                                    CreditPeerHandle peer) noexcept;
    std::expected<void, CreditError> UnbindWithdrawal(const control::PeerCreditControl& owner,
                                                      CreditPeerHandle peer) noexcept;
    std::expected<void, CreditError> RetireWithdrawn(const control::PeerCreditControl& owner, CreditPeerHandle peer,
                                                     std::array<std::uint64_t, 2> spent) noexcept;
    void FinishRetire(std::uint32_t peer) noexcept;
    // 仅接收桥接器在核对 M1 Closed 及绑定代次后使用；不提供可手工置真的删除证明。
    std::expected<void, CreditError> BindReceive(const EagerReceiveDomain& owner) noexcept;
    std::expected<void, CreditError> UnbindReceive(const EagerReceiveDomain& owner) noexcept;
    std::expected<void, CreditError> ValidateReleasedForClose(RxCreditReceipt receipt) const noexcept;
    std::expected<void, CreditError> CloseDestroyed(const EagerReceiveDomain& owner, std::uint64_t receive_domain,
                                                    std::size_t released_receipts) noexcept;
    struct State;
    std::expected<std::uint32_t, CreditError> Find(CreditPeerHandle peer) const noexcept;
    std::expected<std::uint32_t, CreditError> Find(RxCreditReceipt receipt) const noexcept;
    std::expected<RxCreditReceipt, CreditError> NewReceipt(std::uint32_t peer, std::uint8_t bucket) noexcept;
    std::unique_ptr<State> state_;
    std::uint64_t last_domain_ = 0, next_peer_ = 0, next_receipt_ = 0;
};

} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_CREDIT_ACCOUNT_HPP_
