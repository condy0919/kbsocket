// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_BASE_NO_DESTRUCTOR_HPP_
#define KBSOCKET_BASE_NO_DESTRUCTOR_HPP_

#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

namespace kbsocket {
/// `NoDestructor` 在内部存储中构造 `T`，但不自动析构 `T`；用于进程生命周期对象。不分配堆内存，也不
/// 提供单例或访问同步；通常声明为函数局部 static。构造异常由调用方处理；资源清理若有需要，应通过
/// `T` 的显式接口完成。`NoDestructor` 存储失效后不能继续访问对象，不适用于可卸载 DSO 中的永久引用。
template <typename T>
class NoDestructor {
    static_assert(std::is_object_v<T> && !std::is_array_v<T>);
    static_assert(!std::is_const_v<T> && !std::is_volatile_v<T>);

public:
    template <typename... Args>
        requires std::is_constructible_v<T, Args...>
    explicit NoDestructor(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) {
        ::new (static_cast<void*>(storage_)) T(std::forward<Args>(args)...);
    }

    NoDestructor(const NoDestructor&) = delete;
    NoDestructor& operator=(const NoDestructor&) = delete;
    NoDestructor(NoDestructor&&) = delete;
    NoDestructor& operator=(NoDestructor&&) = delete;

    // 保持平凡析构，不注册 T 的退出期清理。
    ~NoDestructor() = default;

    T* get() noexcept {
        return std::launder(reinterpret_cast<T*>(storage_));
    }
    const T* get() const noexcept {
        return std::launder(reinterpret_cast<const T*>(storage_));
    }

    T& operator*() noexcept {
        return *get();
    }
    const T& operator*() const noexcept {
        return *get();
    }

    T* operator->() noexcept {
        return get();
    }
    const T* operator->() const noexcept {
        return get();
    }

private:
    alignas(T) std::byte storage_[sizeof(T)];
};
} // namespace kbsocket

#endif // KBSOCKET_BASE_NO_DESTRUCTOR_HPP_
