# 共享 JFR 的异步水线事件观察

`async_event_test` 第一版只验证 `URMA_EVENT_JFR_LIMIT`：真实 SEND 消耗共享 JFR 中的接收 WR 后，是否通过 context 的 `async_fd` 得到低水线事件，以及补投后能否继续正常收发。它不触发 JFC/JFR error，不验证 PORT_DOWN，也不把错误 CQE 当作 async event。

每端创建两个 jetty、一个共享 JFR、TX/RX JFC，复用生产 `JettyPool`、`RxBufferPool`、`TxSender` 和 `TxCompletionProcessor`。客户端使用第一个 jetty 发送，服务端验证 payload 并归还 RX lease，但只在实验规定的位置补投。TCP 仅交换端点与阶段同步，不传递 payload。

## 构建与运行

```bash
USE_BAZEL_VERSION=9.3.0 bazel build //tools/async_event_test:async_event_test
USE_BAZEL_VERSION=9.3.0 bazel test //tools/async_event_test:async_event_test_test --test_output=errors
```

两端使用相同版本的二进制，设备/EID 替换为本机实际配置。先启动服务端：

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

第一组通过后，可两端改为 `--rx_depth=256` 再跑一组，水线仍为 64。第一版限制 `rx_depth=128..256`、`threshold=64`、`bytes=32..4096`、`observe_ms=1..1000`。`timeout_ms` 默认 10000，必须大于 `2*observe_ms`；每个 TCP 操作和流量阶段分别有截止时间。两端需匹配 depth、threshold、bytes 和 observe_ms；可使用不同的本地 timeout。

本地 UDMA 驱动源码将水线映射为 0、64、512、4096 四档，低于 64 的值可能映射为禁用；修改时要求水线严格小于实际 JFR 深度。因此本工具不使用 `threshold=8` 等任意小值。现场驱动可能与本地源码不同，工具通过 `ModifyJfr` 后的 `QueryJfr` 同时记录请求值和查询值；阈值不符或查询深度小于请求深度时直接 FAIL，不修改 provider 缓存，也不假定只要 Modify 成功就已设置正确。

## 实验阶段

以下以 depth=128、threshold=64 为例；所有阶段始终使用同一个 JFR：

| 阶段 | 真实流量与 RX 数量 | 异步事件观察 |
| --- | --- | --- |
| `baseline` | 先投满 128 个 RX，再设置并查询水线 64；对端尚未开始发送 | 在 observe_ms 内不应取得 JFR_LIMIT；出现则 FAIL |
| `first_crossing` | 对端发送 65 条，不补投，剩余 63 个 RX | 收流量期间及随后的 observe_ms 内必须至少取得一个匹配的 JFR_LIMIT，并 Ack |
| `refill_control` | 补回 128，观察；再发送 1 条验证正常收发，补回 128，再观察 | 单独记录这两个满水位窗口及正常流量期间的事件，不把可能迟到的事件计入下一次跨线 |
| `repeat_crossing` | 再发送 65 条，不重新设置水线，剩余 63 个 RX | 单独输出 OBSERVED/NOT_OBSERVED；不预设硬件的重复触发机制 |
| `cleanup` | 设置水线为 0 并查询确认；先采集已有事件，再发送 63 条新身份的 cleanup 消息消费余量，最后再次采集事件 | 记录并 Ack 可能迟到的合法事件；不再补投，确保所有 RX 的 DMA 所有权已归还 |

默认组最终服务端应为 `posted=received=194`，客户端应为 `accepted=retired=194、accepted_known=true`。depth=256 时两次跨线各发送 193 条，正常恢复 1 条，cleanup 63 条，总量为 450。每条 TX 有可信终结 CQE，服务端逐条检查长度、远端身份、消息序号和全部 payload；接收槽位编号不被当成消息序号。

每次 Get 都先通过 `poll(ctx->async_fd)` 检查可读，避免在没有事件时调用可能阻塞的 Get。成功 Get 的原事件恰好 Ack 一次，并检查 context、类型 `JFR_LIMIT`、`element.jfr` 的归属。程序不解引用事件中的资源指针；遇到其他类型或错误归属时记录并确认已取得的原事件，然后 FAIL。失败 Get 不 Ack。

`Async phase=...` 按阶段打印 `get_calls/retrieved/acked/jfr_limit`、最后事件类型、Get 状态以及 context/JFR 是否匹配。无事件时 `last_type=-1`，归属字段为 false，仅表示没有可判断的事件；正常取得事件的阶段应满足 `retrieved=acked=jfr_limit`。服务端记录本地事件，客户端打印的是服务端确认的观测结果。

首次跨线没有取得事件时，输出 NOT_OBSERVED 并 FAIL；不能把数据流量成功代替事件路径验证。重复跨线没有事件仍可完成实验并 PASS，但该项必须保留 NOT_OBSERVED。第二次窗口出现事件，也只证明这个窗口实际取得了事件；阶段基于软件采集时间，不能推导硬件事件产生时间或所有重触发时序。

## 已有硬件结果

用户提供的 depth=128/256 两组双端日志均通过，端点为服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:42`，共同参数 `threshold=64、bytes=4096、observe_ms=100`。查询的深度/水线均与请求一致；首次各取得并 Ack 一个 context/JFR 匹配的 JFR_LIMIT（type=3），其他阶段均未取得事件。服务端 posted=received、客户端 accepted=retired 分别为 194/450，水线已禁用，双方正常关闭。

首次事件与补投恢复已验证；两组第二次跨线都为 NOT_OBSERVED。这不妨碍本轮 PASS，但尚不能依赖 Ack/补投自动恢复水线通知，也不能将未测试的显式重新使能计为通过。详细计数与解释见[硬件观测记录](../../docs/hardware-observations.md)。

## 完成、Ack 与资源生命周期

`AckAsyncEvent` 不是 CQE 退休，也不意味着 buffer 可以复用或 DMA 已结束。它不等于重新 arm JFR 水线。本实验在两次跨线之间只补投，不重写阈值；没有用 `ModifyJfr(READY)` 代替 rearm，因为本地 UDMA 的 READY 路径可能重置 RQ 索引。需要研究显式 `0→64` 对重复触发的影响时，应另设实验，不能与本轮结果混记。

关闭前先禁用水线、采集当前事件，再通过独立 cleanup 流量消费剩余 posted RX；双方收齐 TX/RX 完成并确认不再发送后才开始 Close。服务端在关闭前再次采集并 Ack 已可见事件，然后关闭 RX 池、jetty/JFR/JFC。晚到事件的最大延迟没有保证，不能把一次空 fd 视作永久静默；provider 删除若等待未完成 Ack，会受到整个 worker 的 watchdog 约束。

默认 `--watchdog_ms=60000`，复用 `peer_failure_test` 的监督进程，在 URMA Load 之前 fork，覆盖初始化、收发、事件采集与同步 provider 关闭调用。超时退出码为 124，不是安全关闭成功。任意协议、查询、post、CQE、payload 或事件错误都 FAIL；未知接受范围不重放，不能证明 DMA/peer 状态时由 `NoDestructor` 保留所有依赖到 worker 退出。最终 PASS 还要求两端互相确认资源关闭。

所有数据 buffer、WR 和事件数组在 Open 或对象创建时预分配；正常流量阶段不扩容。本地 gMock 验证 API 边界、协议和资源约束，实际水线行为必须用双端硬件日志确认。保留完整命令、stdout/stderr、退出码及服务器使用的 URMA/provider/驱动版本。
