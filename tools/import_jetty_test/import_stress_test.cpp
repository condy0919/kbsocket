// SPDX-License-Identifier: MulanPSL-2.0
#include "tools/import_jetty_test/import_stress.hpp"

#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <cstring>
#include <set>

#include <gtest/gtest.h>

#include "kbsocket/transport/raw/testing/scoped_urma_override.hpp"

namespace kbsocket {
namespace tools {
namespace {
struct ConcurrentState {
    std::atomic<unsigned> imports{0};
    std::atomic<unsigned> unimports{0};
    std::atomic<unsigned> active{0};
    std::atomic<unsigned> peak{0};
    std::array<urma_rjetty_t*, 4> first_descriptors{};
    std::barrier<> first_wave{4};
    bool fail_import = false;
    bool fail_cleanup = false;
};
ConcurrentState* concurrent = nullptr;

raw::UrmaFunctions ConcurrentFunctions() {
    raw::UrmaFunctions functions{};
    functions.import_jetty = +[](urma_context_t*, urma_rjetty_t* descriptor, urma_token_t*) -> urma_target_jetty_t* {
        const unsigned index = concurrent->imports.fetch_add(1);
        const auto active = concurrent->active.fetch_add(1) + 1;
        auto peak = concurrent->peak.load();
        while (peak < active && !concurrent->peak.compare_exchange_weak(peak, active)) {
        }
        EXPECT_EQ(concurrent->unimports.load(), 0u);
        EXPECT_EQ(descriptor->tp_type, URMA_CTP);
        // bonding 扩展必须完整复制，每个并发 worker 使用独立的可写内存。
        std::array<std::uint32_t, 3> extension{};
        std::memcpy(extension.data(), descriptor + 1, sizeof(extension));
        EXPECT_EQ(extension[0], 8u);
        EXPECT_EQ(extension[1], 0x12345678u);
        EXPECT_EQ(extension[2], 0xabcdef01u);
        if (index < 4) {
            concurrent->first_descriptors[index] = descriptor;
            concurrent->first_wave.arrive_and_wait();
        }
        concurrent->active.fetch_sub(1);
        if (concurrent->fail_import && index == 1) {
            errno = ENOSPC;
            return nullptr;
        }
        return new urma_target_jetty_t{};
    };
    functions.unimport_jetty = +[](urma_target_jetty_t* target) {
        EXPECT_EQ(concurrent->active.load(), 0u);
        if (concurrent->fail_cleanup) {
            return URMA_FAIL;
        }
        concurrent->unimports.fetch_add(1);
        delete target;
        return URMA_SUCCESS;
    };
    return functions;
}

class ImportStressTest : public testing::Test {
protected:
    void SetUp() override {
        concurrent = &state_;
        urma_rjetty_t descriptor{};
        descriptor.trans_mode = URMA_TM_RM;
        descriptor.tp_type = URMA_CTP;
        descriptor.type = URMA_JETTY;
        descriptor.flag.bs.has_user_info = 1;
        std::memcpy(bytes_.data(), &descriptor, sizeof(descriptor));
        const std::array<std::uint32_t, 3> extension{8, 0x12345678, 0xabcdef01};
        std::memcpy(bytes_.data() + sizeof(descriptor), extension.data(), sizeof(extension));
    }
    std::array<std::span<const std::byte>, 1> Descriptors() {
        return {std::span<const std::byte>(bytes_)};
    }
    ConcurrentState state_;
    urma_context_t context_{};
    std::array<std::byte, sizeof(urma_rjetty_t) + 12> bytes_{};
};

TEST_F(ImportStressTest, ConcurrentImportsRetainAllTargetsUntilClose) {
    raw::test_support::ScopedUrmaOverride override(ConcurrentFunctions());
    ImportStress stress;
    ASSERT_TRUE(stress.Run(&context_, Descriptors(), {true, 1000, 4}));
    EXPECT_EQ(state_.peak.load(), 4u);
    EXPECT_EQ(state_.imports.load(), 1000u);
    EXPECT_EQ(stress.completed(), 1000u);
    EXPECT_EQ(state_.unimports.load(), 0u);
    const std::set<urma_rjetty_t*> unique(state_.first_descriptors.begin(), state_.first_descriptors.end());
    EXPECT_EQ(unique.size(), 4u);
    for (const auto& sample : stress.samples()) {
        EXPECT_TRUE(sample.attempted);
        EXPECT_TRUE(sample.success);
        EXPECT_EQ(sample.error, 0);
    }
    testing::internal::CaptureStdout();
    stress.Print(true);
    const auto report = testing::internal::GetCapturedStdout();
    EXPECT_NE(report.find("requested=1000 attempted=1000 success=1000 failed=0 unattempted=0"), std::string::npos);
    EXPECT_NE(report.find("p99.9="), std::string::npos);
    EXPECT_NE(report.find("slot,worker,worker_call,start_ns,elapsed_ns,success,errno"), std::string::npos);
    EXPECT_TRUE(stress.Close());
    EXPECT_TRUE(stress.Close());
    EXPECT_EQ(state_.unimports.load(), 1000u);
    EXPECT_FALSE(stress.Run(&context_, Descriptors(), {true, 1000, 4}));
}

TEST_F(ImportStressTest, FailedCallJoinsInflightImportsAndKeepsTheirTargets) {
    state_.fail_import = true;
    raw::test_support::ScopedUrmaOverride override(ConcurrentFunctions());
    ImportStress stress;
    auto result = stress.Run(&context_, Descriptors(), {true, 1000, 4});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ENOSPC);
    EXPECT_EQ(state_.active.load(), 0u);
    EXPECT_EQ(state_.unimports.load(), 0u);
    EXPECT_EQ(stress.completed() + 1, state_.imports.load());
    unsigned failures = 0;
    for (const auto& sample : stress.samples()) {
        if (sample.attempted && !sample.success) {
            ++failures;
            EXPECT_EQ(sample.error, ENOSPC);
        }
    }
    EXPECT_EQ(failures, 1u);
    // 清理失败不能丢掉 target；重试成功后才允许上层删除 context。
    state_.fail_cleanup = true;
    EXPECT_FALSE(stress.Close());
    EXPECT_EQ(state_.unimports.load(), 0u);
    state_.fail_cleanup = false;
    EXPECT_TRUE(stress.Close());
    EXPECT_EQ(state_.unimports.load(), stress.completed());
}

TEST_F(ImportStressTest, RejectsInvalidCountsAndDescriptorsBeforeImport) {
    raw::test_support::ScopedUrmaOverride override(ConcurrentFunctions());
    ImportStress stress;
    EXPECT_FALSE(stress.Run(&context_, Descriptors(), {true, 0, 4}));
    EXPECT_FALSE(stress.Run(&context_, Descriptors(), {true, 1000, 0}));
    EXPECT_FALSE(stress.Run(&context_, Descriptors(), {true, 2, 4}));
    EXPECT_FALSE(stress.Run(&context_, Descriptors(), {true, 1000, 257}));
    auto descriptors = Descriptors();
    descriptors[0] = descriptors[0].first(1);
    EXPECT_FALSE(stress.Run(&context_, descriptors, {true, 1000, 4}));
    EXPECT_EQ(state_.imports.load(), 0u);
    EXPECT_TRUE(stress.Close());
}
} // namespace
} // namespace tools
} // namespace kbsocket
