// SPDX-License-Identifier: MulanPSL-2.0

#ifndef KBSOCKET_BASE_LOG_HPP_
#define KBSOCKET_BASE_LOG_HPP_

#include <spdlog/logger.h>

#include <cstdint>
#include <expected>
#include <string>
#include <utility>

namespace kbsocket {
enum class LogLevel : std::uint8_t {
    kTrace,
    kDebug,
    kInfo,
    kWarn,
    kError,
    kCritical,
    kOff,
};

enum class LogErrorCode : std::uint8_t {
    kAlreadyInitialized,
    kNotInitialized,
    kInvalidArgument,
    kBackendFailure,
};

struct LogError {
    LogErrorCode code;
    char message[256];
};

struct LogOptions {
    LogLevel level = LogLevel::kInfo;
    // 为空时输出到 stderr；非空时以追加模式写入该文件。
    std::string file_path;
};

/// kbsocket 专用的同步 Logger：不修改 spdlog 全局注册表与默认配置，进程退出时也不自动析构。初始化与
/// 关闭须由控制线程执行，期间不得有任何并发日志调用（包括 `FlushLog()`/`SetLogLevel()`）。初始化完
/// 成后，日志写入可多线程并发；但其内部存在加锁与内存分配，不适用于收发、poll 等热路径。
std::expected<void, LogError> InitLog(const LogOptions& options = {}) noexcept;
std::expected<void, LogError> SetLogLevel(LogLevel level) noexcept;
std::expected<void, LogError> FlushLog() noexcept;

/// 幂等：未初始化或重复关闭时直接成功。即使刷新失败也会释放 Logger，并返回刷新错误；不影响宿主程序
/// 自身的其他 logger。
std::expected<void, LogError> ShutdownLog() noexcept;

/// 返回自进程启动以来，写日志时被捕获的后端/格式化异常次数；因级别过滤或未初始化而跳过的调用不计入。
std::uint64_t LogFailureCount() noexcept;

namespace internal {
spdlog::logger* EnabledLogger(LogLevel level) noexcept;
spdlog::level::level_enum BackendLevel(LogLevel level) noexcept;
void RecordFailure() noexcept;

template <typename... Args>
void Write(spdlog::logger* logger, LogLevel level, spdlog::source_loc location, spdlog::format_string_t<Args...> format,
           Args&&... args) noexcept {
    // 日志失败不得影响业务流程，后端异常在此吞掉并计数；但参数表达式在进入本函数前求值，
    // 其异常安全需由调用方保证。
    try {
        logger->log(location, BackendLevel(level), format, std::forward<Args>(args)...);
    } catch (...) {
        RecordFailure();
    }
}
} // namespace internal
} // namespace kbsocket

// 先做级别过滤再求值参数：未初始化、已关闭或级别被过滤时，既不求值参数也不格式化。
#define KBSOCKET_LOG(level, ...)                                                                                       \
    do {                                                                                                               \
        const auto kbsocket_log_level_value = (level);                                                                 \
        if (auto* logger = ::kbsocket::internal::EnabledLogger(kbsocket_log_level_value)) {                            \
            ::kbsocket::internal::Write(logger, kbsocket_log_level_value, {__FILE__, __LINE__, __func__},              \
                                        __VA_ARGS__);                                                                  \
        }                                                                                                              \
    } while (false)

#define KBSOCKET_LOG_TRACE(...) KBSOCKET_LOG(::kbsocket::LogLevel::kTrace, __VA_ARGS__)
#define KBSOCKET_LOG_DEBUG(...) KBSOCKET_LOG(::kbsocket::LogLevel::kDebug, __VA_ARGS__)
#define KBSOCKET_LOG_INFO(...) KBSOCKET_LOG(::kbsocket::LogLevel::kInfo, __VA_ARGS__)
#define KBSOCKET_LOG_WARN(...) KBSOCKET_LOG(::kbsocket::LogLevel::kWarn, __VA_ARGS__)
#define KBSOCKET_LOG_ERROR(...) KBSOCKET_LOG(::kbsocket::LogLevel::kError, __VA_ARGS__)
#define KBSOCKET_LOG_CRITICAL(...) KBSOCKET_LOG(::kbsocket::LogLevel::kCritical, __VA_ARGS__)

#endif // KBSOCKET_BASE_LOG_HPP_
