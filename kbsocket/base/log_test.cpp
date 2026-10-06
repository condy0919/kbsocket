// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/base/log.hpp"

#include <spdlog/spdlog.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace kbsocket {
namespace {
using ::testing::HasSubstr;
using ::testing::Not;

class LogTest : public ::testing::Test {
public:
    void SetUp() override {
        FLAGS_kbsocket_log_level = "info";
    }

    // Logger 是进程级单例；每个用例结束时统一关闭，避免状态泄漏到后续用例。`ShutdownLog()` 幂等，用
    // 例内已主动关闭时这里同样应成功。
    void TearDown() override {
        EXPECT_TRUE(ShutdownLog());
    }

private:
    gflags::FlagSaver flag_saver_;
};

TEST_F(LogTest, CommandLineFlagControlsInitialization) {
    // 模拟宿主解析 argv；库只读取结果，不能替宿主消费位置参数。
    char program[] = "log_test";
    char flag[] = "--kbsocket_log_level=debug";
    char positional[] = "remaining";
    char* arguments[] = {program, flag, positional, nullptr};
    char** argv = arguments;
    int argc = 3;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    EXPECT_EQ(argc, 2);
    EXPECT_STREQ(argv[1], "remaining");
    ASSERT_TRUE(InitLog());
    EXPECT_NE(internal::EnabledLogger(LogLevel::kDebug), nullptr);
    EXPECT_EQ(internal::EnabledLogger(LogLevel::kTrace), nullptr);
}

TEST_F(LogTest, AcceptsAllFlagLevels) {
    const char* names[] = {"trace", "debug", "info", "warn", "error", "critical", "off"};
    const LogLevel levels[] = {LogLevel::kTrace, LogLevel::kDebug,    LogLevel::kInfo, LogLevel::kWarn,
                               LogLevel::kError, LogLevel::kCritical, LogLevel::kOff};
    // 每个阈值都应屏蔽较低级别；off 不应输出任何级别。
    for (int i = 0; i < 7; ++i) {
        SCOPED_TRACE(names[i]);
        FLAGS_kbsocket_log_level = names[i];
        ASSERT_TRUE(InitLog());
        for (int j = 0; j < 7; ++j) {
            EXPECT_EQ(internal::EnabledLogger(levels[j]) != nullptr, j >= i && j < 6);
        }
        ASSERT_TRUE(ShutdownLog());
    }
}

TEST_F(LogTest, InvalidFlagAllowsRetryAndExplicitLevelTakesPrecedence) {
    for (const auto& value :
         {std::string(""), std::string("INFO"), std::string("verbose"), std::string("info\0debug", 10)}) {
        FLAGS_kbsocket_log_level = value;
        const auto result = InitLog();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, LogErrorCode::kInvalidArgument);
        EXPECT_THAT(result.error().message, HasSubstr("--kbsocket_log_level"));
    }
    // 显式配置优先，即使 flag 非法也不影响；运行中修改 flag 不会隐式改变 logger。
    ASSERT_TRUE(InitLog({.level = LogLevel::kWarn}));
    FLAGS_kbsocket_log_level = "trace";
    EXPECT_EQ(internal::EnabledLogger(LogLevel::kInfo), nullptr);
    ASSERT_TRUE(SetLogLevel(LogLevel::kDebug));
    EXPECT_NE(internal::EnabledLogger(LogLevel::kDebug), nullptr);
    ASSERT_TRUE(ShutdownLog());
    // 关闭后重新初始化才读取新的 flag。
    ASSERT_TRUE(InitLog());
    EXPECT_NE(internal::EnabledLogger(LogLevel::kTrace), nullptr);
}

TEST_F(LogTest, DisabledLogsDoNotEvaluateArguments) {
    int calls = 0;
    // 初始化之前不触发参数表达式；初始化之后按运行时级别过滤。
    KBSOCKET_LOG_INFO("{}", ++calls);
    ASSERT_TRUE(InitLog({.level = LogLevel::kWarn}));
    KBSOCKET_LOG_DEBUG("{}", ++calls);
    KBSOCKET_LOG_INFO("{}", ++calls);

    // kOff 应屏蔽所有级别（含最高的 critical）；关闭后的调用同样不得求值参数。
    ASSERT_TRUE(SetLogLevel(LogLevel::kOff));
    KBSOCKET_LOG_CRITICAL("{}", ++calls);
    ASSERT_TRUE(ShutdownLog());
    KBSOCKET_LOG_ERROR("{}", ++calls);
    EXPECT_EQ(calls, 0);
}

TEST_F(LogTest, FormatsMessagesAndPreservesSourceLocation) {
    // 默认配置输出到 stderr，须在 InitLog 之前开始捕获，以确保 sink 写入被截获。
    ::testing::internal::CaptureStderr();
    const auto initialized = InitLog();
    EXPECT_TRUE(initialized);

    // 默认级别为 info, debug 应被过滤；运行时下调级别后，debug 立即可见。
    KBSOCKET_LOG_INFO("device={} eid_index={:02x}", "raw0", 7);
    KBSOCKET_LOG_DEBUG("hidden");
    EXPECT_TRUE(SetLogLevel(LogLevel::kDebug));
    KBSOCKET_LOG_DEBUG("visible");
    // 先关闭以刷新全部输出，再读取捕获内容。
    EXPECT_TRUE(ShutdownLog());

    const auto output = ::testing::internal::GetCapturedStderr();
    // 依次验证：fmt 格式化参数生效、宏透传的源码位置、级别标签、级别过滤。
    EXPECT_THAT(output, HasSubstr("device=raw0 eid_index=07"));
    EXPECT_THAT(output, HasSubstr("log_test.cpp:"));
    EXPECT_THAT(output, HasSubstr("[info]"));
    EXPECT_THAT(output, Not(HasSubstr("hidden")));
    EXPECT_THAT(output, HasSubstr("visible"));
}

TEST_F(LogTest, RejectsInvalidInitializationAndAllowsRetry) {
    // 非法配置不能发布半初始化 logger，修正配置后应能成功。
    // 越界枚举值：模拟调用方强转出非法级别。
    auto invalid = InitLog({.level = static_cast<LogLevel>(99)});
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, LogErrorCode::kInvalidArgument);

    // 路径中含 '\0' 会在传给 C 接口时被截断成另一个路径，必须在参数校验阶段拒绝。
    auto bad_path = InitLog({.file_path = std::string("a\0b", 3)});
    ASSERT_FALSE(bad_path);
    EXPECT_EQ(bad_path.error().code, LogErrorCode::kInvalidArgument);

    // 参数合法但后端打开失败（/dev/null 不是目录）：应归类为后端错误并携带可读信息。
    auto failed = InitLog({.file_path = "/dev/null/kbsocket.log"});
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, LogErrorCode::kBackendFailure);
    EXPECT_NE(failed.error().message[0], '\0');

    // 前面的失败均未留下残留状态，因此此处可以成功；随后的重复初始化应被拒绝。
    ASSERT_TRUE(InitLog());
    auto duplicate = InitLog();
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().code, LogErrorCode::kAlreadyInitialized);
}

TEST_F(LogTest, ShutdownIsIdempotentAndDoesNotChangeHostLogger) {
    // 记录宿主的 spdlog 默认 logger，验证本库的整个生命周期不会替换或销毁它。
    auto host = spdlog::default_logger();
    ASSERT_TRUE(InitLog());
    ASSERT_TRUE(ShutdownLog());
    EXPECT_TRUE(ShutdownLog());
    EXPECT_EQ(spdlog::default_logger(), host);

    // 关闭后控制接口应显式报告未初始化，而不是静默成功。
    auto flush = FlushLog();
    ASSERT_FALSE(flush);
    EXPECT_EQ(flush.error().code, LogErrorCode::kNotInitialized);

    auto level = SetLogLevel(LogLevel::kInfo);
    ASSERT_FALSE(level);
    EXPECT_EQ(level.error().code, LogErrorCode::kNotInitialized);

    // 显式关闭后可以再次初始化，不依赖 spdlog 全局 registry 的名字注册。
    EXPECT_TRUE(InitLog());
}

TEST_F(LogTest, BackendWriteFailureDoesNotEscapeIntoBusinessCode) {
    // Linux 的 /dev/full 接受打开但拒绝写入；大消息越过 stdio 缓冲，触发真实后端错误。
    ASSERT_TRUE(InitLog({.file_path = "/dev/full"}));
    // 失败计数是进程级累计值，只断言增量，不依赖其他用例的执行情况。
    const auto before = LogFailureCount();
    KBSOCKET_LOG_ERROR("{}", std::string(16384, 'x'));
    EXPECT_EQ(LogFailureCount(), before + 1);
    // 关闭仍须释放 logger，可能发生的刷新失败通过 expected 报告。
    // 写失败后缓冲区是否仍有残留取决于 stdio 实现，因此刷新成功或失败都可接受。
    const auto stopped = ShutdownLog();
    if (!stopped) {
        EXPECT_EQ(stopped.error().code, LogErrorCode::kBackendFailure);
    }
    // 再次关闭成功，说明即使上一次刷新失败，logger 也已被释放。
    EXPECT_TRUE(ShutdownLog());
}

TEST_F(LogTest, ConcurrentFileLoggingAppendsAndFlushes) {
    // 预先写入一行已有内容，用于验证打开文件时为追加而非截断。
    const auto path = std::filesystem::path(::testing::TempDir()) / "kbsocket_log_test.log";
    {
        std::ofstream initial(path);
        initial << "existing\n";
    }
    ASSERT_TRUE(InitLog({.file_path = path.string()}));
    // 工作线程必须先 join 再关闭，验证同步 sink 的并发输出及追加语义。
    std::vector<std::thread> workers;
    workers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([i] {
            for (int j = 0; j < 20; ++j) {
                KBSOCKET_LOG_INFO("worker={} item={}", i, j);
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    ASSERT_TRUE(FlushLog());
    ASSERT_TRUE(ShutdownLog());
    // 首行必须仍是原有内容（追加语义）。
    std::ifstream input(path);
    std::string line;
    ASSERT_TRUE(std::getline(input, line));
    EXPECT_EQ(line, "existing");

    // 后续每行都应是一条完整日志（无交错撕裂），总数为 4 线程 × 20 条，不丢不重。
    int count = 0;
    while (std::getline(input, line)) {
        EXPECT_THAT(line, HasSubstr("worker="));
        ++count;
    }
    EXPECT_EQ(count, 80);
    std::filesystem::remove(path);
}
} // namespace
} // namespace kbsocket
