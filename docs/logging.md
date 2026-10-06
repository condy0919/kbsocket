# 日志接口

依赖 `//kbsocket/base:log` 并包含 `kbsocket/base/log.hpp`。后端使用 MODULE.bazel 中的 spdlog 1.17.0，其 BCR 模块依赖外部 fmt 12.1.0；`.bazelrc` 统一开启 fmt 后端并关闭 header-only 模式。无需手工添加 `SPDLOG_FMT_EXTERNAL`，Bazel 依赖会传播对应定义。

```cpp
#include "kbsocket/base/log.hpp"

// 控制线程初始化；空 file_path 默认输出到 stderr。
auto initialized = kbsocket::InitLog({.level = kbsocket::LogLevel::kInfo,
                                     .file_path = "kbsocket.log"});
if (!initialized) {
    // initialized.error() 提供错误码和固定长度诊断文本。
    return std::unexpected(initialized.error());
}
KBSOCKET_LOG_INFO("device={} eid_index={}", "raw0", 7);
KBSOCKET_LOG_ERROR("create context failed: {}", "provider error");
// 所有日志调用停止后由控制线程关闭。
return kbsocket::ShutdownLog();
```

支持 TRACE、DEBUG、INFO、WARN、ERROR、CRITICAL 六个宏，以及 `KBSOCKET_LOG(level, ...)`。CRITICAL 仅表示日志级别，不终止进程。输出带时间、级别、文件名和行号。`SetLogLevel` 调整运行时级别，`kOff` 禁用全部日志；未初始化、关闭及被级别过滤的日志均不求值参数。参数本身的求值异常不由日志封装捕获。

`InitLog`、`SetLogLevel`、`FlushLog`、`ShutdownLog` 返回 `std::expected<void, LogError>`。初始化失败不发布 logger，可修正配置后重试；重复初始化报错，重复关闭成功。日志写入为尽力而为：捕获的格式化或后端异常累计到 `LogFailureCount()`，不向业务抛出；控制接口失败直接通过 expected 返回，不计入该计数。

首版使用同步、线程安全 sink：stderr 或追加文件，不自动轮转、不创建后台线程。`FlushLog` 刷新用户态缓冲，不代表 fsync 持久化。普通日志和级别调整可并发；InitLog/ShutdownLog 必须在所有日志访问停止时执行，不能与写日志、刷新或调整级别并发。

日志器独立于 spdlog 全局 registry，不替换宿主默认 logger，也不调用全局 shutdown。使用 NoDestructor 保持未显式关闭的 logger 存活，避免静态析构先于仍运行的 worker；不保证依赖库析构或 DSO 卸载后的安全调用。需要可靠刷新时必须协调退出并显式 ShutdownLog。

该接口用于初始化、配置和低频异常诊断，不保证零分配或无锁。收发/poll 热路径继续使用 USDT 与计数器，不以同步文本日志替代热路径可观测机制。

验证：`bazel test //kbsocket/base:log_test`。测试覆盖禁用日志不求值、格式和源码位置、失败重试、宿主 logger 隔离、多线程文件追加、后端写入失败。
