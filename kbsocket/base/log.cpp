// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/base/log.hpp"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_sinks.h>

#include <atomic>
#include <cstdio>
#include <exception>
#include <memory>

#include "kbsocket/base/no_destructor.hpp"

namespace kbsocket {
namespace {
std::unique_ptr<spdlog::logger>& Logger() noexcept {
    static NoDestructor<std::unique_ptr<spdlog::logger>> logger;
    return *logger;
}
constinit std::atomic<std::uint64_t> failures{0};

bool ValidLevel(LogLevel level) noexcept {
    return level >= LogLevel::kTrace && level <= LogLevel::kOff;
}

LogError BackendError(const char* message) noexcept {
    LogError error{.code = LogErrorCode::kBackendFailure};
    std::snprintf(error.message, sizeof(error.message), "%s", message);
    return error;
}
} // namespace

std::expected<void, LogError> InitLog(const LogOptions& options) noexcept {
    if (Logger()) {
        return std::unexpected(LogError{
            .code = LogErrorCode::kAlreadyInitialized,
            .message = "logger already initialized",
        });
    }

    if (!ValidLevel(options.level) || options.file_path.find('\0') != std::string::npos) {
        return std::unexpected(LogError{
            .code = LogErrorCode::kInvalidArgument,
            .message = "invalid log options",
        });
    }

    try {
        spdlog::sink_ptr sink;
        if (options.file_path.empty()) {
            sink = std::make_shared<spdlog::sinks::stderr_sink_mt>();
        } else {
            sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(options.file_path, false);
        }
        auto pending = std::make_unique<spdlog::logger>("kbsocket", std::move(sink));
        pending->set_level(internal::BackendLevel(options.level));
        pending->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [%s:%#] %v");
        // 替换 spdlog 默认的打印到 stderr 的行为，改为抛出异常交由上层统一处理。
        pending->set_error_handler([](const std::string& message) { throw spdlog::spdlog_ex(message); });
        Logger() = std::move(pending);
        return {};
    } catch (const std::exception& error) {
        return std::unexpected(BackendError(error.what()));
    } catch (...) {
        return std::unexpected(BackendError("unknown logging failure"));
    }
}

std::expected<void, LogError> SetLogLevel(LogLevel level) noexcept {
    if (!ValidLevel(level)) {
        return std::unexpected(LogError{
            .code = LogErrorCode::kInvalidArgument,
            .message = "invalid log level",
        });
    }
    if (!Logger()) {
        return std::unexpected(LogError{
            .code = LogErrorCode::kNotInitialized,
            .message = "logger not initialized",
        });
    }
    Logger()->set_level(internal::BackendLevel(level));
    return {};
}

std::expected<void, LogError> FlushLog() noexcept {
    if (!Logger()) {
        return std::unexpected(LogError{
            .code = LogErrorCode::kNotInitialized,
            .message = "logger not initialized",
        });
    }
    try {
        Logger()->flush();
        return {};
    } catch (const std::exception& error) {
        return std::unexpected(BackendError(error.what()));
    } catch (...) {
        return std::unexpected(BackendError("unknown logging failure"));
    }
}

std::expected<void, LogError> ShutdownLog() noexcept {
    if (!Logger()) {
        return {};
    }
    auto result = FlushLog();
    Logger().reset();
    return result;
}

std::uint64_t LogFailureCount() noexcept {
    return failures.load(std::memory_order_relaxed);
}

namespace internal {
spdlog::level::level_enum BackendLevel(LogLevel level) noexcept {
    switch (level) {
    case LogLevel::kTrace:
        return spdlog::level::trace;
    case LogLevel::kDebug:
        return spdlog::level::debug;
    case LogLevel::kInfo:
        return spdlog::level::info;
    case LogLevel::kWarn:
        return spdlog::level::warn;
    case LogLevel::kError:
        return spdlog::level::err;
    case LogLevel::kCritical:
        return spdlog::level::critical;
    case LogLevel::kOff:
        return spdlog::level::off;
    }
    return spdlog::level::off;
}

spdlog::logger* EnabledLogger(LogLevel level) noexcept {
    auto* logger = Logger().get();
    return logger && ValidLevel(level) && level != LogLevel::kOff && logger->should_log(BackendLevel(level)) ? logger
                                                                                                             : nullptr;
}

void RecordFailure() noexcept {
    failures.fetch_add(1, std::memory_order_relaxed);
}
} // namespace internal
} // namespace kbsocket
