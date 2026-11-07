// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_CONNECTION_CONNECTION_TOKENS_HPP_
#define KBSOCKET_CONNECTION_CONNECTION_TOKENS_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>

#include "kbsocket/connection/wire_frame.hpp"

namespace kbsocket {
namespace connection {
enum class TokenError { kInvalidArgument, kNotReady, kInUse, kNoMemory, kExhausted, kInvalidToken, kStopped };
struct TokenDomain {
    std::uint64_t runtime_instance = 0;
    std::uint64_t receive_domain = 0;
};
class ConnectionTokens;
struct ConnectionTokenLease {
    const ConnectionTokens* owner = nullptr;
    WireToken token;
};
/// 接收域 incarnation 内唯一 token 分配器；不能为同一域重建表以重置 generation。
/// 仅管理路由身份，Retire 不撤销 Credit/Grant，也不证明旧帧消失。tombstone 由对应账本保留。
/// 单 owner；必须活到域的全部路由使用者退出。Open 一次，Close 后不可重新 Open。
class ConnectionTokens {
public:
    static constexpr std::uint32_t kMaxSlots = 1u << 20;
    ConnectionTokens() = default;
    ConnectionTokens(const ConnectionTokens&) = delete;
    ConnectionTokens& operator=(const ConnectionTokens&) = delete;
    std::expected<void, TokenError> Open(TokenDomain domain, std::uint32_t slots,
                                         std::uint32_t initial_generation = 1) noexcept;
    std::expected<ConnectionTokenLease, TokenError> Acquire() noexcept;
    std::expected<void, TokenError> Retire(ConnectionTokenLease lease) noexcept;
    bool Contains(WireToken token) const noexcept;
    void StopAccepting() noexcept {
        stopped_ = true;
    }
    std::expected<void, TokenError> Close() noexcept;
    TokenDomain domain() const noexcept {
        return domain_;
    }
    std::size_t active() const noexcept {
        return active_;
    }

private:
    struct Entry {
        std::uint32_t generation = 0, next = 0;
        bool active = false;
    };
    std::unique_ptr<Entry[]> entries_;
    TokenDomain domain_;
    std::uint32_t count_ = 0, free_ = 0;
    std::size_t active_ = 0;
    bool opened_once_ = false, stopped_ = false;
};
} // namespace connection
} // namespace kbsocket
#endif // KBSOCKET_CONNECTION_CONNECTION_TOKENS_HPP_
