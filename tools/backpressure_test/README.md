# 背压、延迟 RX 与 RNR 后续发测试

`backpressure_test` 将本地 TX 配额不足与远端暂时没有 RX WR 分开验证。客户端发送
`SEND_WITH_IMM`，服务端接收；TCP 只交换端点、阶段和终结计数，所有测试数据均走裸设备上的
RM_CTP。每个消息包含 32 字节测试头，校验完整正文、消息身份和 64 位 IMM。
`rnr_continue` 进一步验证同一条 SQ 在 RNR 重试耗尽后，是否仍能发送新的消息。

用户已反馈 `tx_quota`、`rx_delay` 测试通过，以及 `rnr_continue_success`。其中 `rx_delay`
观察到的是原始 WR 的 RNR 终结与另一条 SQ 的清理成功；同 SQ 续发由 `rnr_continue` 独立验证。
已有日志、反馈范围及尚未验证的分支见 [硬件观测记录](../../docs/hardware-observations.md)。

## 测试语义

### `tx_quota`：本地配额耗尽与恢复

每轮服务端先准备 RX。客户端在软件配额为 `D=tx_depth` 的 SQ 上提交 `D` 个 WR，故意暂不 poll
TX，随后尝试第 `D+1` 个 WR。Ledger 容量大于 `D`，因此这次拒绝必须来自 SQ 配额，且
`accepted=0`，不能进入 provider。即使硬件已经完成发送，软件在消费 CQE 前也不能释放配额。

客户端随后 poll TX、终结之前的尝试，再提交刚才未被接受的消息。每轮使用独立的额外 TX
buffer，不会为构造探测消息覆盖尚未终结的 DMA 内存。服务端校验全部 `D+1` 个消息恰好收到一次。
多轮执行验证配额能重复释放和重新使用；这也是今后实现 socket 可写通知所需的基础。

通过时客户端满足：

- `accepted=retired=success=(tx_depth+1)*rounds`；服务端 `received` 等于该数量。
- `quota_rejections=rounds`，`recovery_sends=rounds`。
- 原始消息没有错误 CQE，双方完成硬件资源关闭后输出 `PASS: outcome=quota_recovery`。

这里测试的是 `JettyPool` 配额门禁，不要求触发 provider 的 `URMA_EAGAIN`。软件配额可能已在
调用 provider 前阻止过量提交，因此 `provider_eagain=0` 是正常结果。本工具不提供 socket
`errno=EAGAIN` 映射或 `NotifyWritable()`。

如果 provider 实际返回 `URMA_EAGAIN`，工具单独增加 `provider_eagain`，保留可确认的已接受
前缀计数，并让本次测试失败、保留依赖资源。它不会将这次结果记作本地配额拒绝，也不会在
后台 poll 后补交未接受后缀，因为这会改变“先完整提交 D 条、再验证配额耗尽”的测试条件。

### `rx_delay`：发送后延迟准备 RX

客户端先提交一批 `tx_depth` 个 WR。服务端确认提交阶段后等待 `rx_delay_us`，然后才向共享
JFR 投递同样数量的 RX buffer。`rounds` 在此模式下不生效，每次进程只测试一个原始批次。

远端没有 RX WR 时，post send 仍可能成功，随后以成功或重试耗尽等 CQE 终结。延迟多少微秒能
成功取决于硬件、provider、重试配置和调度；即使 `rx_delay_us=0`，控制同步及线程调度也会产生
实际等待。本工具不承诺任何延迟值必然成功，也不要求某个固定错误码。

原始 SQ 如果出现错误，本实验决定关闭本轮失败的专用单连接 SQ，执行 ERROR 门禁、处理
`FLUSH_ERR_DONE` 并软件 flush，确保所有已接受的 WR 都终结一次。RNR、ACK_TIMEOUT 等仍由生产
完成层发起排空；若 status 8 已退休但 SQ 仍 READY，则由工具显式调用 `BeginDrain`，保留本实验的
清理与控制报告协议。这不表示 status 8 证明物理 SQ 已损坏，也不要求生产共享 SQ 采取同一策略。
**不会重放已接受的原始消息，也不会把错误结果描述成发送恢复。**

服务端延迟投递的 RX WR 可能仍有一部分没有被消费。双方先交换原始 TX 终结计数和成功消息
bitmap。服务端观察已到达的 RX，再请求缺少的数量。客户端使用独立的健康 SQ 发送带有 `cleanup` 标记的新
测试消息，专门消费剩余 RX 槽。原始消息与清理消息分别计数、分别校验身份；清理消息不承载
原始消息的数据语义。错误 TX 不能直接推断为远端一定没有收到数据，因此可能有迟到的原始
RX 与清理流量竞争 RX 槽；清理 SQ 若因此发生 RNR，同样需要完成错误排空。全部 RX 槽归还后，
服务端再根据两组成功 bitmap 逐条确认对应的 RX 身份，防止错误 TX 的接收抵消另一条成功消息的缺失。

只有双方全部已接受 TX 都终结、服务端 `posted=0/held=0`、对端静默且硬件资源正常关闭，才可
打印以下结果之一：

- `PASS: outcome=rx_delay_success`：原始批次全部成功，原始 payload 已完整校验。
- `PASS: outcome=rx_delay_terminal_failure`：原始批次观察到错误，错误排空及资源收尾通过；
  **不代表原始数据全部送达或恢复成功**。

`--expect=either` 接受上述任意一种已完整收尾的结果。`--expect=success` 或 `--expect=error`
用于在明确了解当前硬件行为后固定回归预期；预期不匹配仍会先关闭双方资源，再输出 FAIL 并
返回非零退出码。无法证明 DMA 终结或对端静默时返回 FAIL，保留完整依赖链直到进程退出，
不会强行释放仍可能被硬件访问的内存。

### `rnr_continue`：同一条 SQ 在 RNR 后发送新消息

`rx_delay` 在遇到错误 CQE 后会按实验策略关闭原始专用 SQ；若生产完成层未主动排空（status 8），
工具补充调用 `BeginDrain`，再用另一条健康 SQ 发送清理流量，
因此不能据此判断硬件是否会因 RNR 自动停用 SQ。`rnr_continue` 专门隔离这个变量：

1. 两端各创建一条 jetty。客户端检查创建配置为 `error_suspend=0`，并查询实际状态为 READY。
2. 服务端暂不投递任何 RX。客户端提交 `tx_depth` 个原始 WR，要求每个 WR 都以
   `URMA_CR_RNR_RETRY_CNT_EXC_ERR`（状态码 10）终结；不修改 SQ、不调用 flush，也不重建 jetty。
3. 客户端确认原始 WR 全部终结，并再次查询同一条 jetty 仍为 READY 后，才通知服务端投递 RX。
4. 服务端投递 `tx_depth` 个 RX 后，客户端在同一条 SQ 上发送第二批新消息。新批次使用独立的
   轮次身份，只有原始 WR 已全部终结后才复用 TX 内存；它不是对原始消息的重放。
5. 第二批 TX 必须全部成功，服务端逐条校验 payload、IMM 和消息身份。客户端第三次查询状态，
   双方关闭全部资源后，才输出 `PASS: outcome=rnr_continue_success`。

工具只在这个实验分支直接处理原始 TX CQE，避开 `TxCompletionProcessor` 当前对 RNR 主动排空
的策略。生产完成层对 RNR/ACK_TIMEOUT 的处理本轮未改；合法 status 8 现只退休并携原始状态和
record 上报，不主动排空 SQ，由上层决定对应连接与操作的失败处理。初始错误不是 RNR、出现排空边界、队列状态变化、
第二批 post 失败或 CQE 错误，都使实验失败；超时和无法确认 DMA 终结的情况保留依赖资源。

该模式只接受 `--expect=either`；实验本身已经固定要求“先 RNR、后成功”，没有“只要报错就通过”
的分支。`rounds` 与 `rx_delay_us` 不参与实验；服务端补投 RX 的时机由原始 WR 的终结报告决定，
无需猜测 RNR 重试所需的延迟。若硬件采用无限重试，初始批次会超时失败。

## 编译与本地单元测试

在仓库根目录执行：

```sh
bazel build //tools/backpressure_test:backpressure_test
bazel test //tools/backpressure_test:protocol_test //tools/backpressure_test:session_test --test_output=errors
```

单元测试通过 gMock 覆盖真实 URMA 调用边界，不需要安装 `liburma.so`。硬件工具在运行时使用
`dlopen` 加载 URMA；可用 `--library=/path/to/liburma.so` 指定实际路径。

## 第一批：三组本地 TX 配额测试

先运行服务端，再运行客户端。以下沿用已有测试中的设备与 EID 索引；请替换成实际可访问的
裸设备、本地 EID 索引和服务端 IPv4 地址。两端的 `mode/bytes/tx_depth/rounds` 必须一致。

后续硬件测试从 `tx_depth=64` 起，推荐使用 `64/128/256`。当前 UDMA 实测在请求
`tx_depth=1` 时，未 poll TX 的情况下仍可连续接受 64 个单 SGE SEND WR；因此不再把
小于 64 的请求深度列为硬件复测组。这不意味着 URMA 通用最低深度为 64，也不改变
`JettyPool` 按请求值执行的软件配额。本节验证软件配额耗尽，provider 实际容量及部分接受
边界由 [provider_boundary_test](../provider_boundary_test/README.md) 验证。

服务端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=65 \
  --mode=tx_quota --bytes=32 --tx_depth=64 --rounds=8
```

客户端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=61 \
  --mode=tx_quota --bytes=32 --tx_depth=64 --rounds=8
```

依次重启两端，使用以下三组参数：

| 组别     | `--bytes` | `--tx_depth` | `--rounds` | 客户端 accepted/retired/success、服务端 received |
|----------|----------:|-------------:|-----------:|-------------------------------------------------:|
| 基础配额 |        32 |           64 |          8 |                                              520 |
| 常用批次 |      4096 |          128 |          8 |                                             1032 |
| 最大批次 |      4096 |          256 |          8 |                                             2056 |

三组客户端都应显示 `quota_rejections=8 recovery_sends=8`，两端最终均应输出
`PASS: outcome=quota_recovery`。

## 第二批：延迟 RX 投递

服务端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=65 \
  --mode=rx_delay --bytes=4096 --tx_depth=64 \
  --rx_delay_us=1000 --expect=either --timeout_ms=10000
```

客户端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=61 \
  --mode=rx_delay --bytes=4096 --tx_depth=64 \
  --rx_delay_us=1000 --expect=either --timeout_ms=10000
```

每组重启两端，将 `rx_delay_us` 分别设为 `0`、`1000`、`100000`。两端
`mode/expect/bytes/tx_depth/rx_delay_us` 必须一致；`timeout_ms` 可各自设置，但必须严格大于
配置的延迟。观察不同延迟下的结果，不预设某组必然出现 RNR 或恢复成功。

客户端 `Original traffic` 打印原始 SQ 的 `accepted/retired/success`、常见错误分类及排空边界；
`Cleanup traffic` 单独打印清理 SQ 的统计。服务端只掌握对端上报的终结汇总，打印
`peer_accepted/peer_retired/peer_success/peer_flush_done` 和本地 `received`，具体错误分类以
客户端日志为准。错误完成与远端接收可能并存，不能要求 `peer_success=received`；可要求原始
和清理消息各不重复，且两者接收总数等于本次投递的 `tx_depth` 个 RX 槽。

## 第三批：同一条 SQ 的 RNR 后续发

先用 64 个 WR 确认状态转换，再扩大到 128 和 256 个 WR。两端的 `mode/bytes/tx_depth` 必须一致。
以下命令先运行服务端，再运行客户端：

服务端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=65 \
  --mode=rnr_continue --bytes=4096 --tx_depth=64 --timeout_ms=10000
```

客户端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=61 \
  --mode=rnr_continue --bytes=4096 --tx_depth=64 --timeout_ms=10000
```

每组重启两端，依次使用 `--tx_depth=64`、`128`、`256`。通过时，令 `D=tx_depth`：

- 客户端 `Original traffic`：`accepted=retired=rnr_retry_cnt_exc_err=D`，`success=0`，
  `flush_done=0`，其他错误计数为 0。
- 客户端 `Continuation traffic`：`accepted=retired=success=D`，所有错误及 `flush_done` 为 0。
- 服务端：原始 `received=0`，续发 `received=D`。
- 客户端 `Same SQ`：打印同一个 `jetty_id`，`state_checks=3` 表示三次查询均确认同一条 jetty
  为 READY。创建配置必须为 `error_suspend=0`，但当前本地内核实现查询时会将 JFS flags 填零，
  因此不能把查询返回的零独立当作硬件已采用 error continue 的证明。行为依据是同一条 SQ 在
  RNR 后出现成功 TX CQE，且对端确实收到并校验了新消息。
- `queried_rnr_retry` 和 `queried_err_timeout` 是 provider 查询返回值，当前本地 UDMA 实现从
  硬件 JFS context 读取这两个字段；尤其 `err_timeout` 可能是硬件档位，不能直接按请求配置的
  编码推导微秒数。
- 两端最终均输出 `PASS: outcome=rnr_continue_success`，且退出码为 0。

PASS 证明本次硬件、provider 和配置下，RNR 是原始 WR 的终结结果，同一条 SQ 随后仍可发送
新消息。它不表示原始消息已送达，也不自动放宽生产侧遇到其他错误时的故障策略。
若第二阶段 post 返回成功但 CQE 仍为 RNR，客户端会在 `Continuation traffic` 中显示该错误，
实验不会通过。若未看到 PASS，请保留两端完整日志及退出码，不能只凭 `accepted=retired` 判断。

## 参数与限制

- `--mode=tx_quota|rx_delay|rnr_continue`，默认 `tx_quota`。
- `--expect=either|success|error`，默认 `either`，仅 `rx_delay` 支持后两项。
- `--tx_depth=1..256`，默认 32；`--bytes=32..4096`，默认 4096，包含测试头。
- `--rounds=1..1000000`，默认 8，仅 `tx_quota` 使用。
- `--rx_delay_us` 为 `uint32`，默认 1000；在 `rx_delay` 模式下须小于 `timeout_ms*1000`。
- `--timeout_ms=1..3600000`，默认 10000；`--port=1..65535`，默认 18519。
- `--urma_debug` 可打开 provider 调试日志。

工具需要设备支持 RM_CTP 以及相应的队列深度和已注册内存。`tx_quota` 使用一条 jetty，RX
深度为 `tx_depth+1`；`rx_delay` 使用两条共享 JFR/TX JFC/RX JFC 的 jetty，RX 深度为
`tx_depth`。`rnr_continue` 使用一条 jetty，RX 深度为 `tx_depth`。单 jetty 的 TX CQ 深度为
`tx_depth+1`，`rx_delay` 为 `2*(tx_depth+1)`，保留错误排空的伪 CQE
容量。运行结束应检查两端最后的 PASS/FAIL 与进程退出码；中间的 accepted/retired 相等只说明
该阶段账本归零，不能单独证明 RX 槽、对端或硬件资源已完成收尾。本工具使用的是测试协议，
不定义 kbsocket 正式连接协议或消息分片格式。
