# 持续 SEND、多 EID/context 与 JFC 通知观察

这个工具在一个 `RawRuntime` 中按 1–4 个 `(device_name, eid_index)` 端点创建独立 context，允许同一裸设备选择多个不同 EID，由单 owner 公平轮询推进各 context 的 request/echo 流量。每个 context 有一个 SQ、一个共享 JFR、独立 TX/RX JFC；事件模式下，两只 JFC 绑定同一个 JFCE。端点列表按位置配对，不做 bonding、自动选路或故障迁移。

与逐批 TCP 同步的 SEND 工具不同，流量期间没有 TCP 屏障。客户端最多保持 `window` 条请求；只有本地 TX CQE 与对应回包都到达才复用 TX 槽并发送下一条。服务端在回包之前补投 RX；客户端在发送下一条之前补投 RX。这个测试专用窗口协议避免正常持续流量耗尽 RX，不是正式 kbsocket 连接协议或性能基准。

```bash
bazel build //tools/streaming_test:streaming_test
bazel test //tools/streaming_test:streaming_test_test --test_output=errors
```

## 单设备持续收发

服务端：

```bash
bazel-bin/tools/streaming_test/streaming_test \
  --server --address=0.0.0.0 --port=8894 \
  --devices=udmac0d1e4:38 --window=32 --messages=100001 --bytes=4096
```

客户端：

```bash
bazel-bin/tools/streaming_test/streaming_test \
  --address=141.62.33.25 --port=8894 \
  --devices=udmac0d1e4:60 --window=32 --messages=100001 --bytes=4096
```

再运行 `window=1/messages=1001`、`window=256/messages=100001`。非整倍数总量覆盖最后一轮 RX 停止补投；成功时每端、每 context `accepted=retired=received=rx_posted=messages`，没有遗留 posted RX。payload、IMM、源/目标身份及消息去重均通过后才关闭。

## JFC 事件通知

在两端相同命令上增加 `--events`，分别测试：

| window | messages | arm_delay_us | 观察 |
| ---: | ---: | ---: | --- |
| 1 | 1001 | 0 | 低并发，增加进入 fd 等待分支的机会 |
| 32 | 100001 | 0 | 连续负载与通知合并 |
| 32 | 10001 | 100 | 放大 arm 到 recheck 的软件间隔 |

每轮先通过 `WaitJfc(timeout=0) → AckJfc` 消费已有通知，再 poll CQ 并推进流量。无进展时执行 `rearm → recheck`；recheck 仍无进展才调用 `poll(JFCE fd)`，fd 就绪后再次消费通知。等待超时直接失败，不靠定时重新 poll 掩盖丢通知。`WaitJfc` 每个返回项 ack 一次，ack 的单位是通知而非 CQE。[URMA API 文档](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)

`--arm_delay_us` 主动延迟 rearm 到 recheck 之间的时间，供 CQE 在这个窗口到达。延迟较大时，recheck 常常已经消费 CQE，循环直接继续推进，无须进入 fd 等待分支；已有通知仍会在下一轮由 `WaitJfc(timeout=0)` 消费并 ack。这种情况下，`recheck_progress` 非零、通知存在而 `fd_wakes=0` 是有效结果，不表示丢通知。

检查每个 context 的 `tx_events/rx_events/acks/fd_wakes/recheck_progress` 和最终 `waits/empty_wakes`：

- `OBSERVED: context N JFC notifications` 表示该 context 取得并 ack 了通知，即 `acks>0`。
- `OBSERVED: context N JFCE fd readiness` 表示该 context 的 fd 在 `poll` 中返回过 `POLLIN`，即 `fd_wakes>0`；fd 可能在调用前已经就绪，不能据此断言线程实际睡眠过。
- 两项分别报告 `OBSERVED/NOT_OBSERVED`，总计也分别判断通知与 fd 就绪。`waits` 仅统计 `poll` 调用次数，不能代替 `fd_wakes`。多 context 需逐项检查，总计 OBSERVED 不代表每个 context 都观察到。
- 服务端持续接收并 echo 时可能一直有进展，无须进入 fd 等待分支。`NOT_OBSERVED` 只表示对应项目未取得证据，不能只看最终流量 PASS，也不应把它当作流量失败。`window=1` 或互换客户端/服务端能增加覆盖机会，不能保证每端、每 context 都进入该分支。
- 延迟组重点检查 `recheck_progress>0`，观察 arm 后 recheck 发现完成的路径，不要求同时观察到 fd 就绪。需要验证 fd 路径的组则应同时检查每个 context 的通知与 fd 就绪。
- `acks` 应等于本端 `tx_events + rx_events`，不要求等于消息数。通知合并、CQE 比通知先被消费都不构成错误。
- 单次成功只覆盖本次到达时序，不证明所有可能竞态或硬件延迟上界。事件模式仍保持 `non_blocking` 默认值。

## 同设备多 EID 与多裸设备

`--devices` 支持 1–4 个逗号分隔的 `name:eid_index`，只拒绝完全重复的设备名/EID 索引组合。
例如同一设备的两个 EID：

```bash
--devices=udmac0d1e4:11,udmac0d1e4:14 --window=32 --messages=100001
```

两端都提供两个本端实际可用的 EID，按列表位置配对；双方的设备名和 EID 索引无需相同。
同一设备的目录和 EID 列表只缓存一次，每个配置端点分别创建 context。
日志中的 `Context N name:eid_index` 标明计数属于哪个端点。
工具的上限是四个 context，`RawRuntime` 本身没有四端点限制。

两端各提供四个实际存在、允许使用的裸设备与 EID 索引：

```bash
# 两端均使用相同 window/messages/events；设备名与索引分别替换。
--devices=udma0:38,udma1:39,udma2:40,udma3:41 --window=32 --messages=100001
```

先每个端点独立跑通，再一起跑轮询模式与事件模式。工具同时推进各会话，每 context 独立计数和校验；它验证多 context 共存与持续工作。同设备四个 EID 的测试不能替代四个物理设备测试，也不验证跨设备故障隔离或四个 CPU owner 的并行性能。

## 生命周期与退出

所有 buffer/状态在 Open 预分配；数据路径不扩容。失败或接受范围未知时，不重放、不释放 MR/context；对象由 `NoDestructor` 保留到进程退出。正常结束先互相确认停止，再关闭导入对象、Jetty/JFR、MR、JFC/JFCE、context，最后交换关闭确认。

`--timeout_ms` 是无进展和控制操作期限；默认 `--process_timeout_sec=300` 还通过进程 SIGALRM 覆盖 provider 同步调用卡住的情况。SIGALRM 退出不是安全关闭成功，不运行资源析构；保留完整双端日志及 shell 退出码。长时间压测可显式提高总期限。
