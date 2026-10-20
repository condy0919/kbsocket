# UB 基础硬件验证矩阵

[文档入口](README.md) · [已有实测记录](hardware-observations.md)

本轮把尚缺的基础硬件观测整理为五个独立工具。ACK_TIMEOUT 根因排查不在本轮范围内；新增工具会保留 status 9 的原始统计，但不会因一次复测成功宣称问题已解决。

这里的“实现完成”指实验代码、判据与本地 gMock 验证完成。**新增实验尚无真实硬件结果**；只有现场运行后的双端日志才能加入硬件观测记录。此前同 SQ 多目标 `batch=32、rounds=8` 的成功日志已经单独登记。

## 尚需确认什么

| 观察项 | 工具 | 必须区分的结果 |
| --- | --- | --- |
| 残留 posted RX 的共享 JFR 删除 | [jfr_lifecycle_test](../tools/jfr_lifecycle_test/README.md) | 真正带残留调用 DeleteJfr、返回码/耗时、成功后再注销 MR；不强求每条残留都有 CQE |
| 真实 provider 容量限制与部分接受 | [provider_boundary_test](../tools/provider_boundary_test/README.md) | 原始 post EAGAIN/ENOMEM、合法 bad_wr 接受前缀；不把软件 quota 当硬件背压 |
| 非零 WR_UNHANDLED 和 ERROR 后直接 post | 同上 | 硬件 FLUSH_ERR、无 user_ctx 的边界、软件 WR_UNHANDLED 分别统计；post SUCCESS 不等于终结 |
| 真实进程退出与 async 事件 | [peer_failure_test](../tools/peer_failure_test/README.md) | SIGKILL/waitpid 证据、提交返回、逐条 CQE、SQ 查询、Get/Ack 事件；无事件不代表无故障 |
| 提交后发生退出的时间窗口 | 同上 `inflight_exit` | post 在 kill 前，分别报告退出请求前已退休和确认后软件未退休；不能据未 poll 断言硬件仍在途 |
| 真实链路/端口故障 | 同上 `external` | 现场手工注入与无故障对照；工具不操作网口，也不能自己证明故障注入有效 |
| 持续发送、RX 补投和 buffer 复用 | [streaming_test](../tools/streaming_test/README.md) | 无逐批 TCP 屏障；每条消息身份/payload/IMM、TX 退休和 RX 补投全部核对 |
| JFC rearm/recheck/wait/ack | 同上 `--events` | 实际通知等待、每设备事件数/ack数、arm后发现完成的次数；不能用忙 poll 掩盖通知丢失 |
| 四裸设备多 context 同时工作 | 同上 `--devices=...` | 同进程同时推进四套资源，按设备独立计数；不等同故障迁移或跨 CPU 性能测试 |
| 单 WR 大于 4 KiB 的 READ/WRITE | [rma_test](../tools/rma_test/README.md) | MR 导出/导入、能力范围、实际传输长度、完成后数据与越界 guard 校验 |

## 统一准备

两端使用同一版本二进制，分别选择自己的设备/EID。下面沿用最近日志的服务端 `udmac0d1e4:38`、客户端 `udmac0d1e4:60` 和控制 IP `141.62.33.25`，实际运行前应替换。先启动服务端，看到 ready 再启动客户端；每组重新启动两个进程。

```bash
USE_BAZEL_VERSION=9.2.0 bazel build --lockfile_mode=off \
  //tools/jfr_lifecycle_test:jfr_lifecycle_test \
  //tools/provider_boundary_test:provider_boundary_test \
  //tools/peer_failure_test:peer_failure_test \
  //tools/streaming_test:streaming_test \
  //tools/rma_test:rma_test

USE_BAZEL_VERSION=9.2.0 bazel test --lockfile_mode=off \
  //tools/jfr_lifecycle_test:session_test \
  //tools/provider_boundary_test:provider_boundary_test_test \
  //tools/peer_failure_test:peer_failure_unit_test \
  //tools/streaming_test:streaming_test_test \
  //tools/rma_test:protocol_test \
  //tools/rma_test:session_test --test_output=errors
```

建议先执行正常收发、JFR 关闭、provider 边界，再执行进程退出和物理故障；物理故障可能影响后续所有实验，应最后做。测试期间记录完整 stdout/stderr 和退出码，不只摘录 PASS。

## 第一批：正常数据路径与事件

持续收发的首组命令：

```bash
# 服务端
bazel-bin/tools/streaming_test/streaming_test \
  --server --address=0.0.0.0 --port=8894 --devices=udmac0d1e4:38 \
  --window=32 --messages=100001 --bytes=4096

# 客户端
bazel-bin/tools/streaming_test/streaming_test \
  --address=141.62.33.25 --port=8894 --devices=udmac0d1e4:60 \
  --window=32 --messages=100001 --bytes=4096
```

两端同步替换下列参数，按行运行：

| 组 | window | messages | 附加参数 |
| --- | ---: | ---: | --- |
| S1 | 1 | 1001 | 无 |
| S2 | 32 | 100001 | 无 |
| S3 | 256 | 100001 | 无 |
| E1 | 1 | 1001 | `--events` |
| E2 | 32 | 100001 | `--events` |
| E3 | 32 | 10001 | `--events --arm_delay_us=100` |
| M1 | 32 | 100001 | 两端分别使用四个真实裸设备的 `--devices` 列表 |
| M2 | 32 | 100001 | 四设备 + `--events` |

多设备先逐个运行 S2，再运行 M1/M2；列表按位置配对。每端、每设备应有 `accepted=retired=received=rx_posted=messages`。事件组另外要求对应设备出现 `OBSERVED: device N JFC notification path`；只有流量 PASS 但通知 NOT_OBSERVED 时，不能登记该设备的事件路径已验证。

READ/WRITE 按 [rma_test README](../tools/rma_test/README.md) 分别运行 `--operation=read` 和 `--operation=write`，每种操作 `--bytes=4096/16384/1048576/8388608 --rounds=8`。这里是**一条 WR 的实际长度**，不是 SEND 拆成若干 4 KiB。能力不足的组记录 SKIP；实际 post、CQE 或数据错误仍为 FAIL。READ 源 MR 在远端确认完成前不复用，WRITE 在发送端完成后由远端校验可见数据。

## 第二批：关闭与 provider 边界

按对应 README 的命令运行以下矩阵，两端共用参数保持一致：

| 组 | 工具与参数 | 重点 |
| --- | --- | --- |
| J1/J2/J3 | jfr_lifecycle：`--rx_depth=1/32/256 --consumed=0` | 单端即可，全部 RX 残留 |
| J4 | jfr_lifecycle：`--rx_depth=32 --consumed=7` | 双端先正常收发，关闭时剩 25 个 RX |
| J5 | jfr_lifecycle：`--rx_depth=256 --consumed=255` | 最后一个 posted RX 的回收 |
| P1 | provider_boundary：`--tx_depth=1 --batch=2 --attempts=64` | 最小 SQ 与 flush 请求上限 |
| P2 | provider_boundary：`--tx_depth=32 --batch=33 --attempts=1024` | 尝试跨越真实接受边界 |
| P3 | provider_boundary：`--tx_depth=256 --batch=255 --attempts=4096` | 连续 post 后 ERROR，尝试非零 UNHANDLED |
| P4 | P2 再加 `--error_post` | 排空后向 ERROR SQ 直接 post 一次 |

P3 的 `attempts+2` 必须在 `max_jfc_depth` 内，必要时减小 attempts。工具不 poll 时也可能由硬件迅速处理请求，所以不能保证每轮出现容量拒绝或非零 UNHANDLED；分别看三个 `OBSERVED/NOT_OBSERVED` 指标。

P4 若观察到 ERROR post 返回 SUCCESS 却始终没有终结，将明确打印 retained 并保留资源到进程退出。这是有价值的硬件结果，但不是安全关闭 PASS。禁止把这个实验用法复制到生产发送路径。

## 第三批：进程与真实故障

按 [peer_failure_test README](../tools/peer_failure_test/README.md) 运行：

此工具的服务端就绪提示是 `Disposable URMA endpoint: pid=...`；看到该行后启动客户端，不必等待字面上的 `ready`。
该 PID 属于专用 URMA 子进程，TCP 控制连接由另一监督进程持有。

| 组 | fault | batch | 重点 |
| --- | --- | ---: | --- |
| F1/F2/F3 | `process_exit` | 1/32/256 | 先证实进程退出，再向旧 import endpoint 发新 WR |
| F4 | `inflight_exit` | 256 | 在 post 后请求退出，记录退出确认前后软件观察状态 |
| F5 | `external` | 32 | 不注入故障的对照，确认预投 RX 与正常流量 |
| F6 | `external` | 32 | 在 INJECT_NOW 窗口内手工注入实际 UB 链路故障 |

F5/F6 使用 `--fault_delay_ms=10000`，控制 TCP 尽量走不受影响的链路。F6 必须记录现场注入动作和时间；无故障计数或无 async 不能自动解释为设备已经恢复。若无法注入物理故障，明确记为未执行，不能用 F1 代替它。

进程退出工具使用监督进程保留控制连接，并以 waitpid 提供退出证据。TCP EOF 既不证明远端 provider 已回收，也不证明本地在途 DMA 已终结。工具对缺失 CQE、伪 CQE、未知接受范围保留资源，整个进程有 watchdog 限制。

## 结果记录与判定

每一组至少记录：日期、双端完整命令、源码提交与未提交修改、二进制哈希、设备/EID、网卡型号/固件、内核、liburma/provider 版本、能力输出、计数、状态码、最终结果及 shell 退出码。`uname -a`、`sha256sum <binary>` 和现场软件包版本信息应与日志放在一起。

| 输出 | 含义 |
| --- | --- |
| PASS | 该工具定义的流量/协议/资源检查成立；不自动覆盖同一输出中 NOT_OBSERVED 的项目 |
| OBSERVED | 明确观测到某项行为；可能是 ERROR post 无终结等异常行为，不能统称业务成功 |
| NOT_OBSERVED | 本次未触发或未取得证据，不是通过或证伪 |
| SKIP | 明确能力/参数门禁不满足，未执行对应硬件操作 |
| FAIL / watchdog 超时 | 工具检查失败、观测不完整或调用未返回；保留原始日志，不据不完整计数推断数据丢失 |

现有 `backpressure_test --mode=rx_delay` 仍可扫描延迟与深度，但软件配置 `rx_delay_us=0` 不代表实际零延迟。没有硬件重试计数/时间戳时，不能通过这个工具精确反推出 RNR 重试窗口。此前 `rnr_continue_success` 与同 SQ mixed 成功仍是有效的独立证据。

socket 的 EAGAIN→EPOLLOUT/NotifyWritable、正式连接协议、分片重组和 brpc worker 退出顺序属于后续软件实现/集成验证。四设备故障迁移、READ 权限拒绝后的隔离、多主机同 SQ 故障影响范围等也不由以上正常路径或两机实验自动保证；若未来启用相应策略，需要针对策略增加验证。这份矩阵覆盖当前 Raw SEND 基础及下一阶段 READ/WRITE 正向路径，不承诺穷举所有硬件故障。
