# 共享 JFR 的异步水线事件观察

`async_event_test` 观测 `URMA_EVENT_JFR_LIMIT`：真实 SEND 消耗共享 JFR 中的接收 WR 后，是否通过 context 的 `async_fd` 得到低水线事件，以及补投后能否继续正常收发。支持多轮跨线，分别比较不重配、重写相同阈值和先禁用再恢复阈值。它不触发 JFC/JFR error，不验证 PORT_DOWN，也不把错误 CQE 当作 async event。

每端创建两个 jetty、一个共享 JFR、TX/RX JFC，复用生产 `JettyPool`、`RxBufferPool`、`TxSender` 和 `TxCompletionProcessor`。客户端使用第一个 jetty 发送，服务端验证 payload 并归还 RX lease，但只在实验规定的位置补投。TCP 仅交换端点与阶段同步，不传递 payload。

## 构建与运行

```bash
USE_BAZEL_VERSION=9.3.0 bazel build //tools/async_event_test:async_event_test
USE_BAZEL_VERSION=9.3.0 bazel test //tools/async_event_test:async_event_test_test --test_output=errors
```

两端使用相同版本的二进制，设备/EID 替换为本机实际配置。当前协议为 v2，握手核验轮数和重配方式，拒绝 v1；必须同时更新两端。以下默认 `--crossings=2 --rearm=none`，先启动服务端：

```bash
./bazel-bin/tools/async_event_test/async_event_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=39 \
  --rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100
```

客户端：

```bash
./bazel-bin/tools/async_event_test/async_event_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=42 \
  --rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100
```

可两端改为 `--rx_depth=256` 再跑一组，水线仍为 64。参数限制为 `rx_depth=128..256`、`threshold=64`、`bytes=32..4096`、`observe_ms=1..1000`、`crossings=2..1000`。`timeout_ms` 默认 10000，必须大于 `2*observe_ms`；每个 TCP 操作和流量阶段分别有截止时间。两端需匹配 depth、threshold、bytes、observe_ms、crossings 和 rearm；可使用不同的本地 timeout。

## 多轮重新使能对照

已有首次事件测试通过后，使用 8 轮检查同一个 JFR 的后续通知。以下示例为重写相同阈值：

```bash
# 服务端
./bazel-bin/tools/async_event_test/async_event_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=39 \
  --rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100 \
  --crossings=8 --rearm=rewrite

# 客户端
./bazel-bin/tools/async_event_test/async_event_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=42 \
  --rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100 \
  --crossings=8 --rearm=rewrite
```

每组重新启动双端进程，分别替换 `--rearm`；首次始终在投满 RX 后配置 64，模式只影响后续轮次：

| rearm | 后续每轮补满 RX 后的操作 | 目的 |
| --- | --- | --- |
| `none` | 只查询，不调用 ModifyJfr | 延续已有实验，观察仅补投后的重复通知 |
| `rewrite` | 只带 RX_THRESHOLD mask，再写入 64 并查询 | 验证相同阈值重写后的通知行为 |
| `toggle` | 写入 0 并查询，再写入 64 并查询 | 验证显式禁用、恢复后的通知行为 |

三个模式均不修改 JFR 状态。模式名表示尝试的配置操作，不能仅凭 `rearm=rewrite/toggle` 或回读 64 就认定硬件事件已重新使能。未观察到某个后续轮的事件时继续剩余轮次，不自动切换模式或重试该轮流量。

本地 UDMA 驱动源码将水线映射为 0、64、512、4096 四档，低于 64 的值可能映射为禁用；修改时要求水线严格小于实际 JFR 深度。因此本工具不使用 `threshold=8` 等任意小值。现场驱动可能与本地源码不同，工具通过 `ModifyJfr` 后的 `QueryJfr` 同时记录请求值和查询值；阈值不符或查询深度小于请求深度时直接 FAIL，不修改 provider 缓存，也不假定只要 Modify 成功就已设置正确。

## 实验阶段

以下以 depth=128、threshold=64 为例；所有轮次始终使用同一个 JFR：

| 阶段 | 真实流量与 RX 数量 | 异步事件观察 |
| --- | --- | --- |
| `baseline` | 先投满 128 个 RX，再设置并查询水线 64；对端尚未开始发送 | 在 observe_ms 内不应取得 JFR_LIMIT；出现则 FAIL |
| `first_crossing` | 对端发送 65 条，不补投，剩余 63 个 RX | 收流量期间及随后的 observe_ms 内必须至少取得一个匹配的 JFR_LIMIT，并 Ack |
| `refill_control` | 每个后续轮先补回 128，观察；再发送 1 条验证正常收发，补回 128，再观察 | 单独记录这两个满水位窗口及正常流量期间的事件，不把已在该阶段取得的事件计入下一次跨线 |
| `above_water` | RX 已补满，查询并按 rearm 模式配置、回读后观察，尚未开始本轮跨线流量 | 重配期间已产生、在此取得的事件独立记录，不充当跨线通知 |
| `repeat_crossing` | 再发送 65 条，剩余 63 个 RX；逐轮重复上述补投/配置过程 | 每轮独立输出 OBSERVED/NOT_OBSERVED；查询跨线后的实际阈值，不要求仍为 64 |
| `cleanup` | 设置水线为 0 并查询确认；先采集已有事件，再发送 63 条新身份的 cleanup 消息消费余量，最后再次采集事件 | 记录并 Ack 可能迟到的合法事件；不再补投，确保所有 RX 的 DMA 所有权已归还 |

设轮数为 C、depth 为 D、threshold 为 T，总 SEND 数为 `C*(D-T+1)+(C-1)+(T-1)`：每轮跨线流量，加轮间恢复流量和最终 cleanup。服务端应 `posted=received=expected`，客户端应 `accepted=retired=expected、accepted_known=true`。默认 C=2 时 depth=128/256 分别为 194/450；C=8 时分别为 590/1614，三种 rearm 模式数量相同。每条 TX 有可信终结 CQE，服务端逐条检查长度、远端身份、消息序号和全部 payload；接收槽位编号不被当成消息序号。

每次 Get 都先通过 `poll(ctx->async_fd)` 检查可读，避免在没有事件时调用可能阻塞的 Get。成功 Get 的原事件恰好 Ack 一次，并检查 context、类型 `JFR_LIMIT`、`element.jfr` 的归属。程序不解引用事件中的资源指针；遇到其他类型或错误归属时记录并确认已取得的原事件，然后 FAIL。失败 Get 不 Ack。

`Async phase=...` 是所有轮次按阶段累积的 `get_calls/retrieved/acked/jfr_limit`，同时记录最后事件类型、Get 状态以及 context/JFR 是否匹配。`Async crossing=N phase=...` 单独展示该轮的补投、满水位与跨线窗口；全局计数和逐轮计数是同一批事件的不同视图，不能相加。无事件时 `last_type=-1`，归属字段为 false，仅表示没有可判断的事件；正常取得事件的阶段应满足 `retrieved=acked=jfr_limit`。

`Query crossing=N point=...` 分别记录 before_rearm、disabled（仅 toggle）、after_rearm、after_crossing 的实际回读。`queried=false` 表示该步骤未执行或未成功取得数据，不能把对应的 0 当成禁用成功。只在主动配置后的回读中要求阈值与请求相等；首次事件后、下一轮重配前的查询允许报告不同阈值，保留硬件实际行为。

每轮还打印本地流量差值；第一轮 posted 包含最初投满 RX，后续轮包含补投及一条恢复流量，最终 cleanup 不计入任何轮。只有已开始的轮次才打印，失败中断而未完成观察的轮标为 INCOMPLETE，不能把未来未执行轮次计为 NOT_OBSERVED。服务端逐轮通过控制面发送带索引的观察结果；客户端打印的是对端确认的结果，不伪造本地 Query/Get/Ack。

首次跨线没有取得事件时，输出 NOT_OBSERVED 并 FAIL；不能把数据流量成功代替事件路径验证。任意后续轮没有事件仍可完成实验并 PASS，但该轮必须保留 NOT_OBSERVED；三个模式均遵循这一观测规则。验收“每轮都观察到通知”需另看汇总 `requested=completed=observed=C`，不能只看 PASS 或 `repeat_any=true`。后者仅表示至少一个后续轮有事件。阶段基于软件采集时间，即使重配后设置了独立满水位窗口，仍不能证明延迟到跨线窗口才取得的事件就是该次跨线产生，也不能推导所有重触发时序。

## 已有硬件结果

用户提供的 depth=128/256 两组双端日志均通过，端点为服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:42`，共同参数 `threshold=64、bytes=4096、observe_ms=100`。查询的深度/水线均与请求一致；首次各取得并 Ack 一个 context/JFR 匹配的 JFR_LIMIT（type=3），其他阶段均未取得事件。服务端 posted=received、客户端 accepted=retired 分别为 194/450，水线已禁用，双方正常关闭。

这些历史日志来自两次跨线、没有阈值重写的实验；首次事件与补投恢复已验证，第二次均为 NOT_OBSERVED。

随后 `depth=128、threshold=64、crossings=8、rearm=rewrite、observe_ms=100、bytes=4096` 的 A6 双端通过，服务端 `udmac0d1e4:39`，客户端 `udmac0d1e4:51`。每轮 before_rearm/after_rearm/after_crossing 的 threshold 均为 `0/64/0`，各取得并 Ack 一个匹配的 JFR_LIMIT；其他阶段事件为 0。双端 `requested=completed=observed=8`，服务端 posted=received=590，客户端 accepted=retired=590，最终禁用水线并正常关闭。

这验证了本组补满后直接重写 64 可恢复通知，无需在轮间先显式写 0。重写的是相同请求值，但操作前实际查询值已经是 0；日志不能分辨清零由硬件、驱动还是 provider 内部执行。新版 none/toggle 的 8 轮及 depth=256 多轮仍待反馈，不能继承 A6 的结果。详细计数与解释见[硬件观测记录](../../docs/hardware-observations.md)。

## 完成、Ack 与资源生命周期

`AckAsyncEvent` 不是 CQE 退休，也不意味着 buffer 可以复用或 DMA 已结束。它不等于重新 arm JFR 水线。`none` 保留只补投、不重写阈值的路径；`rewrite/toggle` 的配置与事件结果独立记录。所有模式都没有用 `ModifyJfr(READY)` 代替 rearm，因为本地 UDMA 的 READY 路径可能重置 RQ 索引。

关闭前先禁用水线、采集当前事件，再通过独立 cleanup 流量消费剩余 posted RX；双方收齐 TX/RX 完成并确认不再发送后才开始 Close。服务端在关闭前再次采集并 Ack 已可见事件，然后关闭 RX 池、jetty/JFR/JFC。晚到事件的最大延迟没有保证，不能把一次空 fd 视作永久静默；provider 删除若等待未完成 Ack，会受到整个 worker 的 watchdog 约束。

默认 `--watchdog_ms=0` 自动取 `max(60000, 4*timeout_ms+(6*crossings+8)*observe_ms)` 毫秒，并在 ready 行显示有效预算；轮数增多时不再固定 60 秒。预算最多一小时，自动计算超过上限时会拒绝参数，需减少观察预算或显式指定 watchdog；显式值必须大于 timeout_ms 且不超过一小时，可主动设短以限制整次实验。该预算不保证每个操作都能各自耗尽 timeout。工具复用 `peer_failure_test` 的监督进程，在 URMA Load 之前 fork，覆盖初始化、收发、事件采集与同步 provider 关闭调用。超时退出码为 124，不是安全关闭成功。任意协议、查询、post、CQE、payload 或事件错误都 FAIL；未知接受范围不重放，不能证明 DMA/peer 状态时由 `NoDestructor` 保留所有依赖到 worker 退出。最终 PASS 还要求两端互相确认资源关闭。

所有数据 buffer、WR 和事件数组在 Open 或对象创建时预分配；正常流量阶段不扩容。本地 gMock 验证 API 边界、协议和资源约束，实际水线行为必须用双端硬件日志确认。保留完整命令、stdout/stderr、退出码及服务器使用的 URMA/provider/驱动版本。
