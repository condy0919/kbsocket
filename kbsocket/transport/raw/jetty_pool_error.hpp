// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_TRANSPORT_RAW_JETTY_POOL_ERROR_HPP_
#define KBSOCKET_TRANSPORT_RAW_JETTY_POOL_ERROR_HPP_

#include <cstdint>
#include <optional>

namespace kbsocket {
namespace raw {
/// 标识 JettyPool 操作失败涉及的队列资源。
enum class JettyPoolResource : std::uint8_t {
    kPool,
    kSendJfc,
    kRecvJfc,
    kJfr,
    kJetty,
};

enum class JettyPoolErrorCode : std::uint8_t {
    kInvalidArgument = 1,
    kInUse,
    kCreateFailed,
    kDeleteFailed,
    kNotReady,
    kPollFailed,
    kNoMemory,
    kExhausted,
    kInvalidLane,
    kInvalidTicket,
    kFaulted,
};

struct JettyPoolFailure {
    JettyPoolErrorCode code;
    JettyPoolResource resource = JettyPoolResource::kPool;
    // 创建失败为 errno，删除失败为 urma_status_t；轮询失败保留 PollJfc 原始返回值。
    int provider_error = 0;
    // 仅在失败涉及具体 jetty 槽位时有意义。
    std::uint32_t jetty_index = 0;
};

/// 同时保留初始化错误和回滚错误，避免清理失败覆盖原始原因。
struct JettyPoolError {
    JettyPoolFailure failure;
    std::optional<JettyPoolFailure> cleanup_error;
};
} // namespace raw
} // namespace kbsocket

#endif // KBSOCKET_TRANSPORT_RAW_JETTY_POOL_ERROR_HPP_
