# 双节点 RM_CTP import 与控制面耗时测试

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

`--timeout_ms=120000` 是每个 TCP 操作的等待期限，包括服务端等待客户端完成测量和反导入的时间。可增大至 3600000；它不能中断内核中阻塞的 `urma_import_jetty()`。进程返回 0 表示完整测量和资源清理成功；默认串行模式失败时输出已成功导入的数量，不输出不完整的延迟分布；并发模式会输出已尝试调用的成功/失败计数及成功样本分布。

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


## RM_CTP 控制面全生命周期模式

两端同时增加 `--control_plane`，原来的 import 测试保持不变，随后执行附加控制面场景。两端必须使用本次构建的相同协议版本，模式不一致会在握手时拒绝。

```sh
# 节点 A
./bazel-bin/tools/import_jetty_test/import_jetty_test \
  --server --address=0.0.0.0 --device=RAW_DEVICE_A \
  --control_plane --timeout_ms=600000 --all_samples > server.csv

# 节点 B：替换为节点 A 的 TCP 地址
./bazel-bin/tools/import_jetty_test/import_jetty_test \
  --address=192.0.2.10 --device=RAW_DEVICE_B \
  --control_plane --timeout_ms=600000 --all_samples > client.csv
```

bonding/bonding 使用对应 bonding 设备名；`--library`、`--eid_index`、`--priority` 用法不变。输出同时包含原来的可读摘要和 CSV 区块，并非只有一个 CSV 表；`api,...` 开始的是汇总，`sample_api,...` 开始的是明细。

本模式覆盖以下 **40 个 API**，不是 URMA 全部公共接口：

| 类别        | API（省略 `urma_` 前缀）                                                              | 正常完整运行每端调用次数                       |
|-------------|---------------------------------------------------------------------------------------|------------------------------------------------|
| 全局与设备  | init / uninit / get_device_list / free_device_list                                    | 各 1 次                                        |
| 设备查询    | query_device / get_eid_list / free_eid_list                                           | 101 / 100 / 100 次                             |
| Context     | create_context / delete_context                                                       | 各 101 次：主 context + 100 次独立创建/销毁    |
| 完成事件    | create_jfce / delete_jfce                                                             | 各 100 次                                      |
| JFC         | create_jfc / modify_jfc / delete_jfc                                                  | 102 / 100 / 102 次：原测试 2 个 + 附加 100 个  |
| JFR         | create_jfr / modify_jfr / query_jfr / delete_jfr                                      | 101 / 100 / 100 / 101 次                       |
| JFS         | create_jfs / modify_jfs / query_jfs / delete_jfs                                      | 各 100 次，取决于 provider 支持                |
| Jetty       | create_jetty / modify_jetty / query_jetty / delete_jetty                              | 各 100 次，使用原有 100 个 jetty               |
| Jetty 描述  | get_rjetty / put_rjetty                                                               | 服务端各 100 次；客户端不调用                  |
| Jetty 导入  | import_jetty / unimport_jetty                                                         | 客户端各 100 次；服务端不调用                  |
| Jetty group | create_jetty_grp / delete_jetty_grp                                                   | 各 100 次，空 HASH_HINT group                  |
| Token       | alloc_token_id / free_token_id                                                        | 各 100 次，独立分配/释放，不用于后面的内存注册 |
| 内存段      | register_seg / unregister_seg / get_seg_ctx / put_seg_ctx / import_seg / unimport_seg | 两端分别各 100 次                              |

附加 context、JFCE、JFC、JFR、JFS、group、token 采用逐轮创建/释放，避免同时占用数百组队列；Jetty 仍是先创建 100 个再逐个导入。内存段使用本机页大小，每端注册 100 个不同页，先触页再计时；所有本地段及远端导入对象保持到该批次测量结束。两端半双工交换完整 segment 描述，保留 bonding 扩展，并等待双方全部反导入完成后才注销内存。注册权限为 READ/WRITE/ATOMIC，实际不投递数据 WR。

同一 API 的统计包含实际发生的初始化和附加调用。例如最初两个 JFC 不绑定 JFCE，后续 100 个 JFC 绑定 JFCE；因此首个样本与第 100 个样本可能来自不同配置，需结合上表解释，不能一律当成同参数冷/热对比。

modify 测试参数是固定场景：JFC 的 moderate_count=1、moderate_period=0；JFR/Jetty 的 rx_threshold=1；JFS 在 query 成功后按查询出的当前状态做同状态 modify。这里测得的是这些特定参数的调用耗时，不代表所有状态转换的耗时。某些 provider 会拒绝同状态 modify，此时按失败样本记录，不强行切 ERROR 来产生 flush/异步事件。

汇总列说明：

- `attempts/success/failed`：调用次数及结果计数；`first_ns/first_ok` 是第一次**调用尝试**的延迟和结果。
- `call100_ns/call100_ok`：第 100 次调用尝试的延迟和结果；不足 100 次为 `NA`。
- `min_ok_ns/mean_ok_ns/p50_ok_ns/p95_ok_ns/p99_ok_ns/max_ok_ns`：**仅成功调用**参与，nearest-rank 百分位；无成功样本为 `NA`。全部单位为纳秒。
- `last_error`：最近一次失败的 URMA 状态码，或空指针返回时的 errno。若 provider 没有设置 errno，失败也可能显示 code=0，应以 success/failed 判断。
- `note`：没有调用时显示角色限制、未执行到该阶段或缺少可选符号等原因。缺少 create/delete 配对符号时整个资源场景跳过，避免创建无法释放的对象。
- `dropped`：超出每 API 512 条记录容量的样本数，正常测试为 0；非零表示记录不完整，进程返回失败。

汇总在资源释放及 `urma_uninit()` **之后**输出，失败退出也会输出已收集到的结果。基础资源创建、传输和清理失败会中止后续依赖操作；独立 query/modify 或可选 JFS/group/token 创建失败会被记录，其他可执行场景继续。任意已执行 API 失败时最终退出码为 1；仅因可选符号缺失而跳过的场景不算调用失败，应检查 `note` 判断实际覆盖率。

当前范围不包含 RC bind/unbind、显式 TP 管理与 `_ex`、异步建链、分阶段 alloc/active/deactive/free、批量接口、独立 JFR 导入/advise、私有 user_ctl、地址解析、日志配置和需要实际事件的 wait/ack。它们需要不同的状态/输入或传输场景，不能用空参数错误返回冒充有效控制面性能。本模式也不覆盖参数空间的所有组合。

无硬件测试额外覆盖逐 API 返回值/errno 保留、失败样本排除、样本不足时的 NA、双向 segment 导入、扩展完整性、部分导入失败、注销失败重试和可选符号缺失；mock 计时不代表硬件延迟。


## 并发 import 压力模式

两端加 `--stress`。客户端多个线程共享一个 context，循环导入服务端预建的 100 个 jetty；**测量期间只有 import，所有成功返回的 target 一直保留到全部线程结束**。之后串行 unimport，再通知服务端释放资源。创建队列、TCP 元数据交换、线程创建、工具缓冲区分配和最终释放均不计入 import 延迟及压力窗口吞吐。

```sh
# 节点 A：每轮测试重新启动服务端
./bazel-bin/tools/import_jetty_test/import_jetty_test \
  --server --address=0.0.0.0 --device=RAW_DEVICE_A \
  --stress --timeout_ms=600000

# 节点 B：8 个并发线程，总共尝试导入 40000 个 target
./bazel-bin/tools/import_jetty_test/import_jetty_test \
  --address=192.0.2.10 --device=RAW_DEVICE_B \
  --stress --import_threads=8 --import_count=40000 \
  --timeout_ms=600000 --all_samples > import-8.log
```

`--library`、`--eid_index`、`--priority` 和 bonding 设备选择方式不变；两端都要使用新构建的程序。`--import_count` 和 `--import_threads` 只决定客户端负载，服务端无须配置相同数值。`--stress` 与 `--control_plane` 互斥。默认 10000 次、8 个线程，线程范围 1–256，总数范围 1–10000000，线程数不能大于总数；上限是参数保护，不表示设备具备相应容量。

建议保持总数 40000，分别用 1、2、4、8、16 个线程，每档重新启动两端程序，重复多轮。若使用 taskset，应分配足够多的 CPU（例如 `taskset -c 0-7`），不要把 8 个 worker 全绑到同一个核后解释为设备并发能力。所有成功对象持续保留，因此并发争用和对象数量增长会同时影响结果；再固定线程数、改变总数，可观察资源规模的影响。这是闭环最大速率负载，没有指定到达速率或建连请求排队模型。

输出包括：

- `requested/attempted/success/failed/unattempted`，以及从打开线程启动门到最后一个调用返回的 `wall_ns`、成功 import/s。吞吐包含压力窗口内的线程调度与循环开销，排除最终 join 和 unimport。
- 仅成功 API 调用的 min/mean/p50/p95/p99/p99.9/max，单位 us。每次延迟只包围 `urma_import_jetty()`，包含线程在调用期间被抢占的时间。
- `--all_samples` 输出 `slot,worker,worker_call,start_ns,elapsed_ns,success,errno`。slot 是预分配记录槽，不是全局开始/完成顺序；worker 从 0 开始，worker_call 从 1 开始，start_ns 相对于压力窗口起点。可按 start_ns 分桶观察随保留对象增长的延迟变化。

不预热 import；每个线程首次调用可能同时参与 TP 首次建立，因此初始并发波次与后续复用调用应分开解读。不要把并发模式某个槽位称为全局“第 1/100 次”。原来的默认串行模式仍保留这两个指标。程序不清除驱动缓存，进程首次 import 不保证硬件冷启动。

第一次 import 失败后停止各 worker 的后续调度，等待已在途调用返回，保存失败 errno 和成功样本，清理全部成功 target，客户端返回失败；失败不会混入成功延迟分位数。服务端正常完成清理协议不代表客户端所有 import 成功，必须检查客户端退出码及报告。资源耗尽时不在窗口内 unimport 来恢复可用容量，否则测量模型就改变了。反导入失败会保留依赖，退出前重试清理。

这里的 40000 指导入对象数量，**不是 40000 个不同远端 jetty、EID 或 TP**。同一远端 jetty 被重复导入，符合共享 jetty 池的导入压力模型，但驱动仍可能复用底层 TP。bonding 每个 worker 使用完整描述符的独立副本，避免并发共享可写 provider 扩展；单次 bonding import 可能展开成多条物理路径。无应用数据 WR，不能由此推断收发满载时的 import 延迟。

新增 mock 测试验证 4 个 import 确实重叠执行、1000 个 target 保留至窗口结束、线程间描述符隔离、bonding 扩展、失败后的在途调用回收与清理重试，以及双端 stress 控制协议。mock 延迟不代表真实设备性能。

## 与 kbsocket 热点的关系

`--control_plane` 是资源生命周期测试，不代表每次建连都会调用其中的全部 API。当前 [JettyPool](../../kbsocket/transport/raw/jetty_pool.cpp) 在 Open 中创建 JFC/JFR/jetty，Close 中销毁；[RxBufferPool](../../kbsocket/transport/raw/rx_buffer_pool.cpp) 在 Open/Close 中注册/注销本地 slab，正常收发时复用。

当前正式连接层尚未实现（见 [raw 路径说明](../../docs/raw-transport-flow.md)）；`import_jetty` 的具体调用目前在双端工具中。核心 TxSender 接收已经导入的 target。将来是否每个逻辑连接都 import 一次，取决于连接层是否缓存并共享同一远端端点的 target，不能由本地 jetty 池直接推出。

当前实际收发热点为 `urma_post_jetty_send_wr`、`urma_post_jfr_wr`、`urma_poll_jfc`，分别用于批量发送、补充接收 buffer、轮询 TX/RX 完成；故障排空路径还有 `urma_modify_jetty` 和 `urma_flush_jetty`。这些不属于本工具无数据 WR 的 import 窗口。当前 raw TxSender 仅支持普通 SEND，没有使用 `urma_import_seg`；单边访问方案可按远端内存段导入并复用，不必每次操作或每个连接重新导入。本地 `register_seg` 与远端 `import_seg` 不同，普通 SEND/RECV 的本地非 inline DMA buffer 仍需提前注册。
