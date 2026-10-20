# 持续 SEND、多裸设备与 JFC 通知观察

这个工具在一个 `RawRuntime` 中创建 1–4 个裸设备 context，由单 owner 公平轮询推进各设备的 request/echo 流量。每个 context 有一个 SQ、一个共享 JFR、独立 TX/RX JFC；事件模式下，两只 JFC 绑定同一个 JFCE。设备列表按位置配对，不做 bonding、自动选路或故障迁移。

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

再运行 `window=1/messages=1001`、`window=256/messages=100001`。非整倍数总量覆盖最后一轮 RX 停止补投；成功时每端、每设备 `accepted=retired=received=rx_posted=messages`，没有遗留 posted RX。payload、IMM、源/目标身份及消息去重均通过后才关闭。

## JFC 事件通知

在两端相同命令上增加 `--events`，分别测试：

| window | messages | arm_delay_us | 观察 |
| ---: | ---: | ---: | --- |
| 1 | 1001 | 0 | 低并发，尽量进入真正的通知等待 |
| 32 | 100001 | 0 | 连续负载与通知合并 |
| 32 | 10001 | 100 | 放大 arm 到 recheck 的软件间隔 |

循环执行 `poll → rearm → recheck → poll(JFCE fd) → WaitJfc(timeout=0) → AckJfc`。recheck 已看到 CQE 时继续推进；只有空 CQ 才睡眠。等待超时直接失败，不靠定时重新 poll 掩盖丢通知。`WaitJfc` 每个返回项 ack 一次，ack 的单位是通知而非 CQE。[URMA API 文档](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)

检查每个设备 `tx_events/rx_events/acks/fd_wakes/recheck_progress` 和最终 `waits/empty_wakes`：

- 每设备的 `OBSERVED: device N JFC notification path` 表示该设备的 fd 实际唤醒且消费了通知；多设备需逐项检查，总计 OBSERVED 不代表四个设备都观察到。
- `NOT_OBSERVED` 表示流量仍可能全部成功，但没有充分走到事件等待路径；应重跑 window=1。不能只看最后的流量 PASS。
- `acks` 应等于本端 `tx_events + rx_events`，不要求等于消息数。通知合并、CQE 比通知先被消费都不构成错误。
- 单次成功只覆盖本次到达时序，不证明所有可能竞态或硬件延迟上界。事件模式仍保持 `non_blocking` 默认值。

## 四裸设备并行

两端各提供四个实际存在、允许使用的裸设备与 EID 索引：

```bash
# 两端均使用相同 window/messages/events；设备名与索引分别替换。
--devices=udma0:38,udma1:39,udma2:40,udma3:41 --window=32 --messages=100001
```

先每个设备独立跑通，再四个一起跑轮询模式与事件模式。工具始终同时推进四个会话，而不是依次跑完。每设备独立计数和校验；它验证多 context 共存与持续工作，不验证跨设备故障隔离，也不是四个 CPU owner 的并行性能测试。

## 生命周期与退出

所有 buffer/状态在 Open 预分配；数据路径不扩容。失败或接受范围未知时，不重放、不释放 MR/context；对象由 `NoDestructor` 保留到进程退出。正常结束先互相确认停止，再关闭导入对象、Jetty/JFR、MR、JFC/JFCE、context，最后交换关闭确认。

`--timeout_ms` 是无进展和控制操作期限；默认 `--process_timeout_sec=300` 还通过进程 SIGALRM 覆盖 provider 同步调用卡住的情况。SIGALRM 退出不是安全关闭成功，不运行资源析构；保留完整双端日志及 shell 退出码。长时间压测可显式提高总期限。
