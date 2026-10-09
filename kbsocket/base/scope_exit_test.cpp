// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/base/scope_exit.hpp"

#include <array>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

namespace kbsocket {
namespace {
TEST(ScopeExitTest, RunsOnNormalExitAndEarlyReturn) {
    int calls = 0;
    auto run = [&](bool early) {
        ScopeExit cleanup([&]() noexcept { ++calls; });
        EXPECT_EQ(calls, early ? 1 : 0);
        if (early) {
            return;
        }
    };
    run(false);
    EXPECT_EQ(calls, 1);
    run(true);
    EXPECT_EQ(calls, 2);
}

TEST(ScopeExitTest, UnwindingPreservesReverseCleanupOrder) {
    std::array<int, 2> order{};
    std::size_t index = 0;
    // 模拟先加载库再初始化会话，异常展开也必须先结束会话再卸载库。
    try {
        ScopeExit library([&]() noexcept { order[index++] = 1; });
        ScopeExit session([&]() noexcept { order[index++] = 2; });
        throw 7;
    } catch (int value) {
        EXPECT_EQ(value, 7);
    }
    EXPECT_EQ(index, 2u);
    EXPECT_EQ(order, (std::array{2, 1}));
}

TEST(ScopeExitTest, ReleaseIsIdempotentAndDoesNotInvokeCallback) {
    int calls = 0;
    {
        auto callback = [&]() noexcept { ++calls; };
        ScopeExit cleanup(callback);
        cleanup.Release();
        cleanup.Release();
        EXPECT_EQ(calls, 0);
    }
    EXPECT_EQ(calls, 0);
}

struct MoveOnlyCleanup {
    int* calls;
    explicit MoveOnlyCleanup(int& value) noexcept : calls(&value) {}
    MoveOnlyCleanup(const MoveOnlyCleanup&) = delete;
    MoveOnlyCleanup(MoveOnlyCleanup&&) noexcept = default;
    void operator()() noexcept {
        ++*calls;
    }
};

TEST(ScopeExitTest, MoveTransfersCleanupExactlyOnce) {
    using Guard = ScopeExit<MoveOnlyCleanup>;
    static_assert(!std::is_copy_constructible_v<Guard>);
    static_assert(!std::is_move_assignable_v<Guard>);
    static_assert(std::is_nothrow_move_constructible_v<Guard>);
    int calls = 0;
    {
        ScopeExit original{MoveOnlyCleanup(calls)};
        {
            ScopeExit moved(std::move(original));
        }
        EXPECT_EQ(calls, 1);
    }
    EXPECT_EQ(calls, 1);
    // 已取消的 guard 移动后不能重新激活。
    {
        ScopeExit original{MoveOnlyCleanup(calls)};
        original.Release();
        ScopeExit moved(std::move(original));
    }
    EXPECT_EQ(calls, 1);
}
} // namespace
} // namespace kbsocket
