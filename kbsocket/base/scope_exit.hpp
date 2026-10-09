// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_BASE_SCOPE_EXIT_HPP_
#define KBSOCKET_BASE_SCOPE_EXIT_HPP_

#include <type_traits>
#include <utility>

namespace kbsocket {
/// 离开作用域时执行清理，包括提前返回与异常展开；自身不分配内存。
/// 回调必须声明 noexcept，且构造、移动和析构均不得抛异常。
/// 移动后只有目标负责清理；Release 仅取消执行，不立即调用回调。
/// 必须绑定到具名局部变量，引用捕获的对象必须覆盖 guard 的生命周期。
template <typename F>
class [[nodiscard]] ScopeExit {
    static_assert(std::is_nothrow_invocable_v<F&>);
    static_assert(std::is_nothrow_move_constructible_v<F>);
    static_assert(std::is_nothrow_destructible_v<F>);

public:
    template <typename G>
        requires std::is_nothrow_constructible_v<F, G&&>
    explicit ScopeExit(G&& callback) noexcept : callback_(std::forward<G>(callback)) {}

    ScopeExit(ScopeExit&& other) noexcept
        : callback_(std::move(other.callback_)), active_(std::exchange(other.active_, false)) {}

    ScopeExit& operator=(ScopeExit&&) noexcept = delete;

    ~ScopeExit() noexcept {
        if (active_) {
            callback_();
        }
    }

    void Release() noexcept {
        active_ = false;
    }

private:
    [[no_unique_address]] F callback_;
    bool active_ = true;
};

template <typename F>
ScopeExit(F&&) -> ScopeExit<std::decay_t<F>>;
} // namespace kbsocket

#endif // KBSOCKET_BASE_SCOPE_EXIT_HPP_
