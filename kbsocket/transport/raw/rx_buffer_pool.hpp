// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_TRANSPORT_RAW_RX_BUFFER_POOL_HPP_
#define KBSOCKET_TRANSPORT_RAW_RX_BUFFER_POOL_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <vector>

#include "kbsocket/transport/raw/jetty_pool.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
class RxBufferPool;

enum class RxBufferErrorCode : std::uint8_t {
    kInvalidArgument,
    kNotReady,
    kInUse,
    kNoMemory,
    kRegisterFailed,
    kUnregisterFailed,
    kStopped,
    kExhausted,
    kPostFailed,
    kProviderContract,
    kPollFailed,
    kUnexpectedCompletion,
    kInvalidLease,
    kReceiveFailed,
};

struct RxBufferError {
    RxBufferErrorCode code;
    int provider_error = 0;
    // 仅投递失败使用：有效 bad_wr 给出已接受前缀；nullopt 表示不能确定边界。
    std::optional<std::size_t> accepted;
};

/// 复制 lease 不增加引用计数；只能由 owner 归还一次，归还后所有副本及 payload span 失效。
struct RxBufferLease {
    const RxBufferPool* pool = nullptr;
    std::uint64_t id = 0;
};

struct RxBufferEvent {
    // 保留 SEND_WITH_IMM 的 IMM、远端身份及长度，供连接层解码；本层不定义线格式。
    urma_cr_t completion{};
    std::optional<RxBufferLease> lease;
    std::span<const std::byte> payload;
    std::optional<RxBufferError> error;
};

/// 单 owner 的共享 JFR 接收内存池：Free -> Posted -> Held -> Free。
/// Open 预分配并注册固定 4 KiB 槽位，数据路径无分配、无锁；不负责连接重排或消息重组。
/// 独占所属池的 JFR 与 RX CQ，绑定期间禁止绕过本类直接 post/poll RX。
/// JettyPool、context 和本对象必须覆盖 DMA 及全部 lease 的生命周期。
class RxBufferPool {
public:
    static constexpr std::size_t kBufferSize = 4096;
    static constexpr std::size_t kMaxBatch = 256;
    static constexpr std::size_t kPollBatch = 64;

    RxBufferPool() = default;
    RxBufferPool(const RxBufferPool&) = delete;
    RxBufferPool& operator=(const RxBufferPool&) = delete;
    ~RxBufferPool();

    /// buffer_count 为 0 时使用 JFR 深度；可多配 buffer 吸收上层持有，但在途 WR 不超过 rx_depth。
    std::expected<void, RxBufferError> Open(JettyPool& pool, std::uint32_t buffer_count = 0) noexcept;
    /// 仅在无在途 WR、无上层 lease 时注销内存并解绑；注销失败保留所有资源供重试。
    /// 不假定空 CQ 或对端断开代表 DMA 已停止；退出时仍有未决 WR 应常驻整个依赖链。
    std::expected<void, RxBufferError> Close() noexcept;
    /// 最多补入 limit 个 WR，受空闲 buffer 和 JFR 剩余额度限制；返回 0 表示当前无需/无法补充。
    /// 部分投递失败只回收确定未接受的后缀；边界不明则停止投递并保留整批。
    std::expected<std::size_t, RxBufferError> Refill(std::size_t limit = kMaxBatch) noexcept;
    /// 每个已消费 CQE 对应一个事件；一条异常不丢弃同批后续 CQE。
    /// 可信终结 CQE 均交出 lease（包括接收错误），调用方必须归还；仅成功 SEND 提供 payload。
    std::expected<std::size_t, RxBufferError> Poll(std::span<RxBufferEvent> events) noexcept;
    std::expected<void, RxBufferError> Release(RxBufferLease lease) noexcept;
    /// 仅停止新投递，仍可 poll/release；不会制造 RX 完成或触发未经验证的硬件排空协议。
    void Stop() noexcept {
        stopped_ = true;
    }

    std::size_t capacity() const noexcept {
        return entries_.size();
    }
    std::size_t posted() const noexcept {
        return posted_;
    }
    std::size_t held() const noexcept {
        return held_;
    }
    std::size_t available() const noexcept {
        return capacity() - posted_ - held_;
    }
    bool stopped() const noexcept {
        return stopped_;
    }

private:
    enum class State : std::uint8_t { kFree, kPosted, kHeld };
    struct Entry {
        std::uint64_t id = 0;
        std::uint32_t next = 0;
        State state = State::kFree;
    };
    void Recycle(std::uint32_t index) noexcept;
    RxBufferEvent Process(const urma_cr_t& cr) noexcept;

    JettyPool* pool_ = nullptr;
    urma_target_seg_t* segment_ = nullptr;
    // 显式管理 DMA slab：失败析构不得让普通容器的析构释放尚在使用的内存。
    std::byte* memory_ = nullptr;
    std::vector<Entry> entries_;
    std::array<urma_sge_t, kMaxBatch> sges_{};
    std::array<urma_jfr_wr_t, kMaxBatch> wrs_{};
    std::array<std::uint32_t, kMaxBatch> indices_{};
    std::array<urma_cr_t, kPollBatch> completions_{};
    std::uint32_t free_head_ = 0;
    std::size_t posted_ = 0;
    std::size_t held_ = 0;
    // id = 本次 block 起点 + 槽位；block 每次递增 capacity，Close/Open 不重置高水位，防 ABA。
    std::uint64_t next_id_ = 1;
    std::uint64_t base_id_ = 1;
    bool stopped_ = false;
};
} // namespace raw
} // namespace kbsocket
#endif // KBSOCKET_TRANSPORT_RAW_RX_BUFFER_POOL_HPP_
