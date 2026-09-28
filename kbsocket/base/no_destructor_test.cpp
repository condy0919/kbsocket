// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/base/no_destructor.hpp"

#include <cstdint>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

namespace kbsocket {
namespace {
struct Tracked {
    int* destructions;
    int value;

    Tracked(int& count, int initial) noexcept : destructions(&count), value(initial) {}

    ~Tracked() {
        ++*destructions;
    }
};

static_assert(std::is_trivially_destructible_v<NoDestructor<Tracked>>);
static_assert(!std::is_copy_constructible_v<NoDestructor<Tracked>>);
static_assert(!std::is_move_constructible_v<NoDestructor<Tracked>>);
static_assert(!std::is_copy_assignable_v<NoDestructor<Tracked>>);
static_assert(!std::is_move_assignable_v<NoDestructor<Tracked>>);
static_assert(std::is_nothrow_constructible_v<NoDestructor<Tracked>, int&, int>);
static_assert(std::is_same_v<decltype(std::declval<const NoDestructor<Tracked>&>().get()), const Tracked*>);

TEST(NoDestructorTest, ConstructsAndAccessesWithoutDestroyingObject) {
    int destructions = 0;
    {
        NoDestructor<Tracked> value(destructions, 42);
        EXPECT_EQ(value->value, 42);
        value->value = 7;
        const auto& immutable = value;
        EXPECT_EQ((*immutable).value, 7);
        EXPECT_EQ(immutable.get(), value.get());
    }
    EXPECT_EQ(destructions, 0);
}

struct alignas(256) Aligned {
    int value = 9;
};

TEST(NoDestructorTest, PreservesExtendedAlignmentAndDefaultInitialization) {
    NoDestructor<Aligned> value;
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value.get()) % alignof(Aligned), 0u);
    EXPECT_EQ(value->value, 9);
    NoDestructor<int> zero;
    EXPECT_EQ(*zero, 0);
}

struct MoveOnly {
    int value;
    explicit MoveOnly(int initial) noexcept : value(initial) {}
    MoveOnly(const MoveOnly&) = delete;
    MoveOnly(MoveOnly&& source) noexcept : value(std::exchange(source.value, -1)) {}
};

TEST(NoDestructorTest, ForwardsMoveOnlyArguments) {
    MoveOnly source(23);
    NoDestructor<MoveOnly> value(std::move(source));
    EXPECT_EQ(source.value, -1);
    EXPECT_EQ(value->value, 23);
}

} // namespace
} // namespace kbsocket
