// SPDX-License-Identifier: MulanPSL-2.0
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kbsocket/socket/fd_table.hpp"

namespace kbsocket {
namespace socket {
namespace {
FdTarget Target(std::uint32_t generation) {
    return {1000u + generation, {2000u + generation, 3ull * generation, generation % 31, 0}};
}
bool Matches(const FdBinding& binding) {
    const auto expected = Target(binding.id.generation);
    return binding.id.table_id == 901 && binding.id.fd == 73 && binding.id.generation != 0 &&
           binding.target.owner_id == expected.owner_id && binding.target.stream.table_id == expected.stream.table_id &&
           binding.target.stream.generation == expected.stream.generation &&
           binding.target.stream.slot == expected.stream.slot && binding.target.stream.reserved == 0;
}
bool Until(auto predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return predicate();
}

TEST(FdTableConcurrencyTest, UnbindAndReuseWaitForOperationReferenceAfterReaderHasExited) {
    FdTable table;
    ASSERT_TRUE(table.Open({.table_id = 901, .fd_limit = 256, .binding_capacity = 2, .reader_capacity = 1}));
    const auto first = table.Publish(73, Target(1));
    ASSERT_TRUE(first);
    std::atomic<bool> ready{false}, release{false};
    std::atomic<unsigned> failures{0};
    std::jthread operation([&](std::stop_token stop) {
        auto reader = table.RegisterReader();
        if (!reader) {
            ++failures;
            return;
        }
        auto lease = table.Lookup(*reader, *first);
        if (!lease) {
            ++failures;
            return;
        }
        // Lookup 已把短暂 Hazard 交给长期 CPU 引用；线程不再需要占用 Reader 槽。
        reader->Reset();
        ready.store(true, std::memory_order_release);
        while (!release.load(std::memory_order_acquire) && !stop.stop_requested()) {
            if (!Matches(lease->binding()) || lease->binding().id != *first) {
                ++failures;
                break;
            }
            std::this_thread::yield();
        }
    });
    ASSERT_TRUE(Until([&] { return ready.load(std::memory_order_acquire); }));
    EXPECT_EQ(table.stats().readers, 0u);
    EXPECT_EQ(table.stats().references, 1u);
    const auto retired = table.Unbind(*first);
    ASSERT_TRUE(retired);
    // 此测试目标只有数值身份，没有外部 owner 资源；确认不应绕过另一线程的 CPU 引用。
    ASSERT_TRUE(table.ConfirmClosed(*retired));
    const auto second = table.Publish(73, Target(2));
    ASSERT_TRUE(second);
    EXPECT_NE(*second, *first);
    std::array<FdBinding, 2> collected;
    auto count = table.Collect(collected, 2);
    ASSERT_TRUE(count);
    EXPECT_EQ(*count, 0u);
    auto reader = table.RegisterReader();
    ASSERT_TRUE(reader);
    auto stale = table.Lookup(*reader, *first);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), FdError::kStaleBinding);
    auto current = table.Lookup(*reader, *second);
    ASSERT_TRUE(current);
    EXPECT_TRUE(Matches(current->binding()));

    release.store(true, std::memory_order_release);
    operation.join();
    EXPECT_EQ(failures.load(), 0u);
    count = table.Collect(collected, 2);
    ASSERT_TRUE(count);
    ASSERT_EQ(*count, 1u);
    EXPECT_EQ(collected[0].id, *first);
    const auto second_retired = table.Unbind(*second);
    ASSERT_TRUE(second_retired);
    ASSERT_TRUE(table.ConfirmClosed(*second_retired));
    current->Reset();
    reader->Reset();
    count = table.Collect(collected, 2);
    ASSERT_TRUE(count);
    EXPECT_EQ(*count, 1u);
    EXPECT_EQ(collected[0].id, *second);
    EXPECT_TRUE(table.Close(true));
}

TEST(FdTableConcurrencyTest, ReadersKeepImmutableIdentitiesDuringRepeatedOwnerRetirementAndReuse) {
    constexpr unsigned kReaders = 4;
    constexpr std::uint32_t kGenerations = 4000;
    constexpr std::size_t kCapacity = 64;
    FdTable table;
    ASSERT_TRUE(
        table.Open({.table_id = 901, .fd_limit = 256, .binding_capacity = kCapacity, .reader_capacity = kReaders}));
    auto published = table.Publish(73, Target(1));
    ASSERT_TRUE(published);
    std::barrier start(kReaders + 1);
    std::atomic<unsigned> failures{0};
    std::atomic<std::uint64_t> successful{0}, rejected_stale{0};
    std::atomic<bool> stop{false};
    std::vector<std::jthread> readers;
    for (unsigned i = 0; i < kReaders; ++i) {
        readers.emplace_back([&] {
            auto reader = table.RegisterReader();
            if (!reader) {
                ++failures;
            }
            start.arrive_and_wait();
            if (!reader) {
                return;
            }
            while (!stop.load(std::memory_order_acquire)) {
                auto lease = table.Lookup(*reader, 73);
                if (!lease) {
                    if (lease.error() != FdError::kNotBound && lease.error() != FdError::kContended &&
                        lease.error() != FdError::kStopped) {
                        ++failures;
                    }
                    continue;
                }
                const auto identity = lease->binding().id;
                if (!Matches(lease->binding())) {
                    ++failures;
                }
                ++successful;
                std::this_thread::yield();
                // owner 可以解绑并复用整数 fd，但不能重写仍被本线程引用的绑定节点。
                if (lease->binding().id != identity || !Matches(lease->binding())) {
                    ++failures;
                }
                auto exact = table.Lookup(*reader, identity);
                if (exact) {
                    if (exact->binding().id != identity || !Matches(exact->binding())) {
                        ++failures;
                    }
                } else if (exact.error() == FdError::kStaleBinding || exact.error() == FdError::kNotBound) {
                    ++rejected_stale;
                } else if (exact.error() != FdError::kContended && exact.error() != FdError::kStopped) {
                    ++failures;
                }
            }
        });
    }
    start.arrive_and_wait();
    EXPECT_TRUE(Until([&] { return successful.load() >= kReaders; }));
    std::array<FdBinding, kCapacity> collected;
    std::vector<bool> seen(kGenerations + 1, false);
    std::uint32_t published_count = 1;
    std::size_t collected_count = 0;
    const auto collect = [&] {
        const auto count = table.Collect(collected, kCapacity);
        EXPECT_TRUE(count);
        if (!count) {
            return;
        }
        for (std::size_t i = 0; i < *count; ++i) {
            const auto& binding = collected[i];
            EXPECT_TRUE(Matches(binding));
            if (!binding.id.generation || binding.id.generation > kGenerations) {
                ADD_FAILURE() << "invalid collected generation=" << binding.id.generation;
                continue;
            }
            EXPECT_FALSE(seen[binding.id.generation]);
            seen[binding.id.generation] = true;
            ++collected_count;
        }
    };
    // 仅 owner 修改发布/退休链表；目标都是纯数值 stub，确认不会删除其他线程可能访问的资源。
    for (std::uint32_t generation = 1; generation <= kGenerations; ++generation) {
        const auto retired = table.Unbind(*published);
        EXPECT_TRUE(retired);
        if (!retired) {
            break;
        }
        EXPECT_TRUE(table.ConfirmClosed(*retired));
        collect();
        if (generation != kGenerations) {
            published = table.Publish(73, Target(generation + 1));
            EXPECT_TRUE(published);
            if (!published) {
                break;
            }
            ++published_count;
        }
        std::this_thread::yield();
    }
    table.StopAccepting();
    stop.store(true, std::memory_order_release);
    readers.clear();
    for (unsigned pass = 0; pass < 2 && table.stats().retired; ++pass) {
        collect();
    }
    EXPECT_EQ(failures.load(), 0u);
    EXPECT_EQ(published_count, kGenerations);
    EXPECT_EQ(collected_count, kGenerations);
    EXPECT_EQ(table.stats().active, 0u);
    EXPECT_EQ(table.stats().retired, 0u);
    EXPECT_EQ(table.stats().references, 0u);
    EXPECT_EQ(table.stats().readers, 0u);
    EXPECT_EQ(table.stats().available, kCapacity);
    EXPECT_TRUE(table.Close(true));
    RecordProperty("generations", kGenerations);
    RecordProperty("successful_lookups", std::to_string(successful.load()));
    RecordProperty("stale_lookups", std::to_string(rejected_stale.load()));
}

TEST(FdTableConcurrencyTest, ConcurrentReaderRegistrationIsBoundedAndSlotsCanBeReusedAfterExit) {
    constexpr unsigned kReaders = 8;
    FdTable table;
    ASSERT_TRUE(table.Open({.table_id = 902, .fd_limit = 256, .binding_capacity = 1, .reader_capacity = kReaders}));
    std::barrier start(kReaders + 1);
    std::atomic<unsigned> ready{0}, failures{0};
    std::atomic<bool> release{false};
    std::vector<std::jthread> workers;
    for (unsigned i = 0; i < kReaders; ++i) {
        workers.emplace_back([&](std::stop_token stop) {
            start.arrive_and_wait();
            auto reader = table.RegisterReader();
            if (!reader) {
                ++failures;
            }
            ready.fetch_add(1, std::memory_order_release);
            while (!release.load(std::memory_order_acquire) && !stop.stop_requested()) {
                std::this_thread::yield();
            }
        });
    }
    start.arrive_and_wait();
    ASSERT_TRUE(Until([&] { return ready.load(std::memory_order_acquire) == kReaders; }));
    EXPECT_EQ(failures.load(), 0u);
    EXPECT_EQ(table.stats().readers, kReaders);
    auto extra = table.RegisterReader();
    ASSERT_FALSE(extra);
    EXPECT_EQ(extra.error(), FdError::kReaderFull);
    release.store(true, std::memory_order_release);
    workers.clear();
    EXPECT_EQ(table.stats().readers, 0u);
    auto reused = table.RegisterReader();
    ASSERT_TRUE(reused);
    reused->Reset();
    EXPECT_TRUE(table.Close(true));
}

} // namespace
} // namespace socket
} // namespace kbsocket
