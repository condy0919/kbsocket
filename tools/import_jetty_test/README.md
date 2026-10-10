# 双节点 RM_CTP import 耗时测试

工具支持裸 UB 设备与 bonding 设备，使用同一个可执行文件分别启动服务端、客户端。两端各创建 100 个 RM jetty，发送队列选择设备报告支持 CTP 的 priority。客户端导入服务端的 100 个不同 jetty，记录第 1 次和第 100 次 `urma_import_jetty()` 的耗时，以及 100 次的分布。

## 编译与运行

```sh
USE_BAZEL_VERSION=9.2.0 bazel build //tools/import_jetty_test:import_jetty_test
USE_BAZEL_VERSION=9.2.0 bazel test //tools/import_jetty_test:import_test_test --test_output=errors
```

两端需安装匹配的 URMA 驱动、liburma 和 provider，使用相同 URMA ABI/版本及字节序。TCP 地址用于元数据交换，与 UB EID 独立。设备名、EID 索引和库路径按各节点实际配置填写。

节点 A，服务端：

```sh
./bazel-bin/tools/import_jetty_test/import_jetty_test \
  --server --address=0.0.0.0 --port=18516 \
  --device=RAW_DEVICE_A --eid_index=0 \
  --library=/opt/urma/lib/liburma.so
```

节点 B，客户端；将 `192.0.2.10` 替换为 A 的 TCP 地址：

```sh
./bazel-bin/tools/import_jetty_test/import_jetty_test \
  --address=192.0.2.10 --port=18516 \
  --device=RAW_DEVICE_B --eid_index=0 \
  --library=/opt/urma/lib/liburma.so --all_samples
```

测 bonding 时，将**两端**的 `--device` 换成各自的 bonding 设备，例如 `bonding_dev_0`，其余用法相同。工具通过公共 URMA 接口直接枚举设备，不经过核心库只支持裸设备的 RawRuntime。当前测试场景是 raw/raw 或 bonding/bonding，不承诺混合 provider 互通。

默认 `--priority=-1` 自动选择设备报告的第一个 CTP priority；也可以两端分别指定 `--priority=N`。设备必须报告 RM_CTP 能力及该 priority 支持 CTP，否则工具明确失败，避免在错误的传输类型上得出结果。设备/provider 未正确报告这些能力时应先检查安装版本和配置。

`--timeout_ms=120000` 是每个 TCP 操作的等待期限，包括服务端等待客户端完成测量和反导入的时间。可增大至 3600000；它不能中断内核中阻塞的 `urma_import_jetty()`。进程返回 0 表示完整测量和资源清理成功；失败输出已成功导入的数量，不输出不完整的延迟分布。

## 测量范围

1. 两端先创建 context、2 个 JFC、1 个共享 JFR 和 100 个本地 jetty。
2. 服务端逐个调用 `urma_get_rjetty()`，完整传输返回的变长描述，包括 bonding 的物理端点扩展。客户端收齐并校验全部描述后才开始测量。
3. 客户端连续调用 100 次 `urma_import_jetty()`；每次仅在 API 调用前后读取 `std::chrono::steady_clock`，记录纳秒数。不做 import 预热，不在计时循环内分配工具缓冲区、打印或读写 TCP。
4. 前面的导入对象一直保留到第 100 次测量完成，保证复用引用持续存在。随后客户端反导入全部目标，通知服务端，双方才销毁本地资源。

客户端输出 `IMPORT #1`、`IMPORT #100`，以及 min/mean/p50/p95/p99/max，单位为微秒；首尾两次另给出纳秒原值。百分位采用 nearest-rank。`--all_samples` 在测量完成后追加 100 行 `import_index,elapsed_ns` CSV。

不注册 payload 内存、不 bind、不投递应用 SEND/RECV/READ/WRITE WR。TCP 元数据和内核/provider 为导入执行的控制通信仍然存在；bonding provider 自身的健康探测任务也可能产生流量。这里的“不发包”指应用不发送数据，不是关闭设备控制面和 provider 后台通信。

第 1 次是**这个进程的首次 import**，不能保证系统级 CTP 缓存为空：其他进程、内核缓存、provider 后台活动以及既有设备状态都可能影响它。程序不会清空系统缓存。第 100 次是否复用 TP 仍由 provider 和驱动决定。bonding 的一次 import 可能包含多条物理路径的导入，其耗时不能与裸设备的一条物理路径直接等同。

计时包含两次时钟读取之间的函数包装及系统调度影响，没有扣除测量开销。可用 `taskset -c N` 固定客户端 CPU，并分别重复多轮比较；一次运行中的首尾样本不足以代表稳定分布。用户态 URMA 日志默认设为 ERROR，内核日志级别和 provider 配置由运行环境决定。

## 无硬件验证

测试使用 socketpair 控制通道和 URMA 函数表 mock，覆盖普通描述、bonding 变长扩展完整传输、恰好 100 次导入且中途不反导入、第 37 次导入失败后的清理、反导入失败保留依赖后重试、队列创建中途失败后的清理，以及格式与 priority 校验。mock 结果用于验证程序行为，不作为设备耗时数据。
