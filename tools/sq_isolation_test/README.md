# 同池单 SQ 故障隔离硬件测试

`sq_isolation_test` 验证一个 SQ 切 ERROR 并排空时，同一个 JettyPool 中的另一条 SQ 仍能收发，两个 SQ 共用的 JFR、TX JFC 和 RX JFC 不被提前关闭或错误记账。每端由单 owner 推进一个裸设备/context；`--server` 只决定 TCP 监听角色，**双方都会主动置错一条 SQ，并发送和接收健康流量**。

运行前应先通过 [基础 SEND](../send_test/README.md)、[单 SQ 排空](../tx_drain_test/README.md) 和 [共享队列双向收发](../shared_queue_test/README.md)。本工具使用独立协议与默认端口 18518，不能与其他工具配对。

## 资源与测试流程

每端有两个池：

- 主池：SQ 0 是故障候选，SQ 1 是健康 SQ；两者共享一个 JFR、一个 TX JFC 和一个 RX JFC。
- sink 池：额外创建一个 jetty 及独立 JFR/JFC，**始终不投递 RX WR**，仅作为对端故障 SQ 的有效目标。

正常流量全部发往对端主池的 jetty 1。sink 的独立空 JFR 避免故障消息消耗健康流量的接收 buffer，也避免未知的故障消息到达数导致主池留下无法正常回收的 RX WR。sink 与主池位于同一个 context。

流程分三个阶段：

1. `before`：两条 SQ 各发送 `batch` 条 SEND_WITH_IMM，确认故障候选和健康 SQ 的基线均正常。
2. `during`：SQ 0 向对端 sink 提交 `batch` 条消息，调用 `BeginDrain()` 切 ERROR；SQ 1 双向运行 `rounds` 轮，每轮 `batch` 条消息。单 owner 交替推进共享 TX/RX CQ，在故障 SQ 收到 `FLUSH_ERR_DONE` 后调用软件 flush，直到其状态为 Drained。
3. `after`：双方确认故障 SQ 排空后，SQ 1 继续运行固定两轮。最后双方确认不再发送，关闭各自 session、runtime、动态库，再交换关闭确认，才打印 PASS。

每轮只投递正常流量实际需要的 RX WR，完成后释放 lease，下轮补投。故障 SQ 的状态迁移不会回收或重建共享 JFR。TX 逐条核对 AttemptId、lane 和终结状态；RX 校验 phase、source、round、index、方向、长度、正文、完整 64 位 IMM，以及 CQE 的实际源/目标端点。

payload 前 32 字节是工具专用的网络字节序头，`--bytes` 包含这部分。IMM 由测试身份字段生成，并携带固定高位以检查截断；这里不定义正式的连接或分片协议。

`during` 表示**启动故障后推进健康流量的阶段**，不表示硬件一定同时保留两条 SQ 的在途 WR。故障 SQ 可能很快排空，因此 `healthy_retired_before_drained` 可以为 0；该计数记录实际观察，不能为得到非零结果而故意延迟处理故障边界。

## 编译与运行

在 kbsocket 根目录执行：

```sh
bazel build //tools/sq_isolation_test:sq_isolation_test
```

先在机器 A 启动：

```sh
./bazel-bin/tools/sq_isolation_test/sq_isolation_test \
  --server --address=0.0.0.0 --port=18518 \
  --device=RAW_DEVICE_A --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --bytes=4096 --batch=32 --rounds=8 --timeout_ms=60000
```

再在机器 B 启动，替换 TCP IPv4 地址：

```sh
./bazel-bin/tools/sq_isolation_test/sq_isolation_test \
  --address=192.0.2.10 --port=18518 \
  --device=RAW_DEVICE_B --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --bytes=4096 --batch=32 --rounds=8 --timeout_ms=60000
```

两端 `bytes/batch/rounds` 必须一致；设备、EID 索引、库路径和超时各自配置。每次测试重新启动两个进程。

| 参数 | 范围与默认值 |
|------|--------------|
| `bytes` | 32–4096，默认 4096，还须满足设备 max_msg_size |
| `batch` | 1–256，默认 32 |
| `rounds` | 1–1000000，默认 8；排空后另外固定运行两轮 |
| `timeout_ms` | 1–3600000，默认 10000；控制操作、数据轮次和故障排空均有截止时间 |
| `port` | 1–65535，默认 18518 |

主池的每 SQ TX 深度为 `batch`，TX CQ 深度为 `2 * (batch + 1)`，共享 RX 深度和 RX CQ 深度为 `2 * batch`。额外 sink 池还会消耗一套 jetty/JFR/JFC 资源；如果创建失败，应结合设备能力和 provider 日志检查资源限制。

超时无法中断 provider 内部同步阻塞的调用。`--urma_debug` 打开 URMA debug 日志，常规 provider 日志也会转发到 stderr。

## 建议的三组硬件测试

| 组合 | batch | rounds | bytes | 目的 |
|------|-------|--------|-------|------|
| 最小批次 | 1 | 8 | 32 | 最小测试头、状态迁移及错误边界 |
| 常规批次 | 32 | 8 | 4096 | 共享队列正常收发、RX buffer 复用和完整正文校验 |
| 最大批次 | 256 | 8 | 4096 | 跨多次 CQ poll/flush 的排空与健康流量推进 |

最大批次要求主池至少支持 514 个 TX CQ 槽和 512 个 RX CQ 槽；sink 池的资源也需同时满足。若设备不支持，不应将资源创建失败解释为隔离失败。可额外运行 `bytes=33` 或增加 `rounds`，分别检查非对齐正文和长时间健康流量。

## 输出与验收

使用 `batch=32 --rounds=8` 时，两端的正常流量计数应为：

```text
Traffic before: accepted=64 retired=64 received=64
Traffic during: accepted=256 retired=256 received=256
Traffic after: accepted=64 retired=64 received=64
```

`before` 包含两个 SQ，其余两个阶段只有健康 SQ。因此各阶段的期望计数分别为 `2 * batch`、`rounds * batch` 和 `2 * batch`。故障批次独立打印，例如：

```text
Fault SQ: accepted=32 retired=32 success=0 flush_err=20 unhandled=12 loc_access_err=0 remote_access_abort_err=0 ack_timeout_err=0 rnr_retry_cnt_exc_err=0 other_error=0 last_other_status=0 flush_done=1 rejected_sends=3
Isolation: healthy_ready_checks=... healthy_retired_before_drained=...
PASS: fault SQ drained once and rejected new sends; healthy SQ and shared RX remained usable; both endpoints closed resources
```

故障完成分布只是格式示例。FLUSH_ERR、WR_UNHANDLED、ACK_TIMEOUT、RNR 等数量取决于硬件推进时机，不要求其中某类必须出现。验收要求：

- 故障批次 `accepted=retired=batch`，每条 attempt 恰好退休一次，各类终结计数之和等于 retired。
- `flush_done=1`，伪 CQE 的 user_ctx 不参与记账。置错后、收到硬件边界后和彻底排空后，发送均被拒绝，`rejected_sends=3`。
- 健康 SQ 始终保持 Ready，三个阶段均按预期完成，RX 正文和端点归属校验全部通过。
- 两端均打印最终 PASS，且退出码均为 0。统计行出现在资源关闭前，只有统计一致还不够。

`LOC_ACCESS_ERR`、`REM_ACCESS_ABORT_ERR`、`ACK_TIMEOUT_ERR`、`RNR_RETRY_CNT_EXC_ERR` 单独计数，其余错误记入 `other_error`。`last_other_status` 只在 `other_error>0` 时有意义。故障 SQ 的普通错误可作为终结证据；健康流量的任何错误、未知/重复完成、错误归属、重复边界或超时都会导致 FAIL。

退出码 2 表示参数错误，1 表示初始化、协议、收发、校验、排空或清理失败。异常完成会打印首条 TX/RX CQE 的状态及归属；RX 还包含 opcode、完整远端 EID/ID 和原始 IMM。接受边界不可信时，accepted 打印为 unknown。无法证明 DMA 和对端访问终结时，保留完整资源依赖到进程退出，不重放、不强制释放在途内存。

## 无硬件验证与边界

```sh
bazel test //tools/sq_isolation_test:protocol_test //tools/sq_isolation_test:session_test
```

本地测试使用真实 JettyPool、Ledger、Sender、CompletionProcessor 和 RxBufferPool，底层 URMA 调用用 gMock 替换，TCP 控制使用 socketpair；不需要安装 liburma.so。真实硬件结果以双方完整日志、最终 PASS 和退出码为准。

本工具验证的是**主动将单个物理 SQ 置错后的共享资源隔离**。它不证明远端掉线、设备故障或共享 JFR 自身故障时仍有相同隔离范围。同一 SQ 上的多个逻辑连接仍会共同受故障影响；也不覆盖连接重连、分片重组、四裸设备并行、RDMA READ/WRITE 或性能测试。
