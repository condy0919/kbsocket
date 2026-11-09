// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_TX_SEQUENCE_WINDOW_HPP_
#define KBSOCKET_CONNECTION_TX_SEQUENCE_WINDOW_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

namespace kbsocket {
namespace connection {
enum class SequenceError {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kStopped,
    kWrongSession,
    kWindowFull,
    kExhausted,
    kInvalidTicket,
    kAcceptanceUnknown
};
struct SequenceSession {
    std::uint64_t peer_runtime = 0;
    std::uint64_t receive_domain = 0;
    std::uint64_t session = 0;
    bool operator==(const SequenceSession&) const = default;
};
struct TxFramePlan {
    std::uint64_t bytes = 0;
    bool fin = false;
};
struct TxSequencedFrame {
    std::uint32_t seq = 0;
    std::uint64_t bytes = 0;
    bool fin = false;
};
class TxSequenceWindow;
struct SequenceTicket {
    const TxSequenceWindow* owner = nullptr;
    std::uint64_t generation = 0;
};

/// 单 owner 发送顺序账本；只记录序号/字节准入，不拥有 payload，也不提交或重放 WR。
/// 上界是接收端累计授权：TX CQE 不增加 max_frame_seq 或 max_stream_bytes。
/// 同一对象只绑定一个 Session。未知接受永久停止；外层仍须保留全部 DMA/Grant 依赖。
class TxSequenceWindow {
public:
    static constexpr std::size_t kMaxBatch = 256;
    static constexpr std::uint64_t kSequenceEnd = std::uint64_t{1} << 32;
    TxSequenceWindow() = default;
    TxSequenceWindow(const TxSequenceWindow&) = delete;
    TxSequenceWindow& operator=(const TxSequenceWindow&) = delete;
    std::expected<void, SequenceError> Open(SequenceSession session, std::uint32_t initial_seq = 0,
                                            std::uint64_t initial_bytes = 0) noexcept;
    std::expected<void, SequenceError> Update(SequenceSession session, std::uint64_t max_frame_seq,
                                              std::uint64_t max_stream_bytes) noexcept;
    std::expected<SequenceTicket, SequenceError> Reserve(std::span<const TxFramePlan> frames) noexcept;
    /// 每次仅提交 pending() 所示后缀；明确接受前缀推进，未接受后缀保留原 SN 并阻挡新 Reserve。
    std::expected<void, SequenceError> Resolve(SequenceTicket ticket,
                                               std::optional<std::size_t> accepted_prefix) noexcept;
    /// 仅整批从未被接受时可撤销；已接受前缀的后缀必须继续同 SN 或令连接显式失败。
    std::expected<void, SequenceError> Cancel(SequenceTicket ticket) noexcept;
    void Fail() noexcept {
        stopped_ = true;
    }
    std::span<const TxSequencedFrame> pending() const noexcept;
    SequenceSession session() const noexcept {
        return session_;
    }
    std::uint64_t next_sequence() const noexcept {
        return next_seq_;
    }
    std::uint64_t accepted_bytes() const noexcept {
        return accepted_bytes_;
    }
    bool stopped() const noexcept {
        return stopped_;
    }
    bool fin_accepted() const noexcept {
        return fin_;
    }

private:
    bool Matches(SequenceTicket ticket) const noexcept;
    SequenceSession session_;
    std::array<TxSequencedFrame, kMaxBatch> frames_{};
    std::size_t begin_ = 0, end_ = 0;
    std::uint64_t next_seq_ = 0, accepted_bytes_ = 0, frame_limit_ = 0, byte_limit_ = 0, generation_ = 0;
    bool open_ = false, stopped_ = false, unknown_ = false, fin_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_TX_SEQUENCE_WINDOW_HPP_
