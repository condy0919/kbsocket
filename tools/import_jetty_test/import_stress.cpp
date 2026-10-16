// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/import_stress.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <numeric>
#include <print>
#include <thread>

#include "tools/import_jetty_test/import_test.hpp"

#include "kbsocket/base/scope_exit.hpp"

namespace kbsocket {
namespace tools {
namespace {
using Clock = std::chrono::steady_clock;
using Blob = std::unique_ptr<void, decltype(&std::free)>;
auto Error(const char* operation, int code) noexcept {
    return std::unexpected(ToolError{operation, code});
}
std::uint64_t Nanoseconds(Clock::duration duration) noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
}
} // namespace

std::expected<void, ToolError>
ImportStress::Run(urma_context_t* ctx, std::span<const std::span<const std::byte>> descriptors, StressOptions options) {
    if (ran_ || !ctx || descriptors.empty() || options.count == 0 || options.count > 10000000 || options.threads == 0 ||
        options.threads > 256 || options.threads > options.count) {
        return Error("invalid import stress configuration", EINVAL);
    }
    for (auto bytes : descriptors) {
        if (auto result = ValidateDescriptor(bytes); !result) {
            return result;
        }
    }
    options_ = options;
    targets_.resize(options.count, nullptr);
    samples_.resize(options.count);
    // provider 接口不是 const，不能让并发调用共享可写描述符（包括 bonding 扩展）。
    std::vector<std::vector<Blob>> copies(options.threads);
    for (auto& worker : copies) {
        worker.reserve(descriptors.size());
        for (auto bytes : descriptors) {
            Blob copy(std::malloc(bytes.size()), &std::free);
            if (!copy) {
                return Error("allocate stress descriptor", ENOMEM);
            }
            std::memcpy(copy.get(), bytes.data(), bytes.size());
            worker.push_back(std::move(copy));
        }
    }
    std::atomic<int> gate{0};
    std::atomic<unsigned> ready{0};
    std::atomic<bool> failed{false};
    Clock::time_point begin;
    std::vector<std::jthread> workers;
    workers.reserve(options.threads);
    // 部分线程创建失败时，先打开取消门，再由 jthread 析构等待，避免永久阻塞。
    ScopeExit cancel([&]() noexcept {
        gate.store(-1);
        gate.notify_all();
    });
    ran_ = true;
    for (unsigned worker = 0; worker < options.threads; ++worker) {
        workers.emplace_back([&, worker] {
            urma_token_t token{};
            (void)Clock::now();
            ready.fetch_add(1);
            ready.notify_one();
            gate.wait(0);
            if (gate.load() != 1) {
                return;
            }
            // 固定分片免去逐调用分配序号的全局原子争用；序号不是全局开始顺序。
            for (std::size_t i = worker; i < options.count; i += options.threads) {
                if (failed.load(std::memory_order_relaxed)) {
                    break;
                }
                auto* remote = static_cast<urma_rjetty_t*>(copies[worker][i % descriptors.size()].get());
                errno = 0;
                const auto start = Clock::now();
                auto* target = raw::UrmaApi::ImportJetty(ctx, remote, &token);
                const auto end = Clock::now();
                const int error = errno;
                targets_[i] = target;
                samples_[i] = {Nanoseconds(start - begin), Nanoseconds(end - start), target ? 0 : error, true,
                               target != nullptr};
                if (!target) {
                    failed.store(true, std::memory_order_relaxed);
                }
            }
        });
    }
    for (unsigned value = ready.load(); value != options.threads; value = ready.load()) {
        ready.wait(value);
    }
    begin = Clock::now();
    gate.store(1);
    gate.notify_all();
    for (auto& worker : workers) {
        worker.join();
    }
    for (const auto& sample : samples_) {
        if (sample.attempted) {
            wall_ns_ = std::max(wall_ns_, sample.start_ns + sample.elapsed_ns);
        }
    }
    if (failed.load()) {
        const auto it = std::find_if(samples_.begin(), samples_.end(),
                                     [](const auto& sample) { return sample.attempted && !sample.success; });
        return Error("stress import failed; scheduling stopped, in-flight calls joined", it->error);
    }
    return {};
}

std::size_t ImportStress::completed() const noexcept {
    return std::count_if(samples_.begin(), samples_.end(), [](const auto& sample) { return sample.success; });
}

std::expected<void, ToolError> ImportStress::Close() noexcept {
    for (auto it = targets_.rbegin(); it != targets_.rend(); ++it) {
        if (*it) {
            const auto rc = raw::UrmaApi::UnimportJetty(*it);
            if (rc != URMA_SUCCESS) {
                return Error("unimport stress jetty", rc);
            }
            *it = nullptr;
        }
    }
    return {};
}

void ImportStress::Print(bool all_samples) const {
    if (!ran_) {
        std::println("IMPORT_STRESS not_started=1");
        return;
    }
    std::vector<std::uint64_t> successes;
    std::size_t attempted = 0;
    for (const auto& sample : samples_) {
        attempted += sample.attempted;
        if (sample.success) {
            successes.push_back(sample.elapsed_ns);
        }
    }
    std::println("IMPORT_STRESS threads={} requested={} attempted={} success={} failed={} unattempted={} "
                 "wall_ns={} success_per_sec={:.3f}",
                 options_.threads, options_.count, attempted, successes.size(), attempted - successes.size(),
                 options_.count - attempted, wall_ns_, wall_ns_ ? successes.size() * 1e9 / wall_ns_ : 0.0);
    if (!successes.empty()) {
        std::sort(successes.begin(), successes.end());
        const auto percentile = [&](std::size_t numerator, std::size_t denominator) {
            return successes[(successes.size() * numerator + denominator - 1) / denominator - 1] / 1000.0;
        };
        std::println("successful calls: min={:.3f} mean={:.3f} p50={:.3f} p95={:.3f} p99={:.3f} "
                     "p99.9={:.3f} max={:.3f} us",
                     successes.front() / 1000.0,
                     std::accumulate(successes.begin(), successes.end(), 0.0) / successes.size() / 1000.0,
                     percentile(50, 100), percentile(95, 100), percentile(99, 100), percentile(999, 1000),
                     successes.back() / 1000.0);
    }
    if (all_samples) {
        std::println("slot,worker,worker_call,start_ns,elapsed_ns,success,errno");
        for (std::size_t i = 0; i < samples_.size(); ++i) {
            const auto& sample = samples_[i];
            if (sample.attempted) {
                std::println("{},{},{},{},{},{},{}", i + 1, i % options_.threads, i / options_.threads + 1,
                             sample.start_ns, sample.elapsed_ns, sample.success ? 1 : 0, sample.error);
            }
        }
    }
}
} // namespace tools
} // namespace kbsocket
