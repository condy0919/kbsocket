# 背压与延迟 RX 投递测试

`backpressure_test` 将本地 TX 配额不足与远端暂时没有 RX WR 分开验证。客户端发送
`SEND_WITH_IMM`，服务端接收；TCP 只交换端点、阶段和终结计数，所有测试数据均走裸设备上的
RM_CTP。每个消息包含 32 字节测试头，校验完整正文、消息身份和 64 位 IMM。

## 测试语义

### `tx_quota`：本地配额耗尽与恢复

每轮服务端先准备 RX。客户端在深度为 `D=tx_depth` 的 SQ 上提交 `D` 个 WR，故意暂不 poll
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

原始 SQ 如果出现错误，按已有协议执行 ERROR 门禁、处理 `FLUSH_ERR_DONE` 并软件 flush，确保
所有已接受的 WR 都终结一次。**不会重放已接受的原始消息，也不会把错误结果描述成发送恢复。**

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

服务端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=65 \
  --mode=tx_quota --bytes=32 --tx_depth=1 --rounds=8
```

客户端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=61 \
  --mode=tx_quota --bytes=32 --tx_depth=1 --rounds=8
```

依次重启两端，使用以下三组参数：

| 组别 | `--bytes` | `--tx_depth` | `--rounds` | 客户端 accepted/retired/success、服务端 received |
| --- | ---: | ---: | ---: | ---: |
| 最小配额 | 32 | 1 | 8 | 16 |
| 常用批次 | 4096 | 32 | 8 | 264 |
| 最大批次 | 4096 | 256 | 8 | 2056 |

三组客户端都应显示 `quota_rejections=8 recovery_sends=8`，两端最终均应输出
`PASS: outcome=quota_recovery`。

## 第二批：延迟 RX 投递

服务端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=65 \
  --mode=rx_delay --bytes=4096 --tx_depth=32 \
  --rx_delay_us=1000 --expect=either --timeout_ms=10000
```

客户端：

```sh
bazel-bin/tools/backpressure_test/backpressure_test \
  --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=61 \
  --mode=rx_delay --bytes=4096 --tx_depth=32 \
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

## 参数与限制

- `--mode=tx_quota|rx_delay`，默认 `tx_quota`。
- `--expect=either|success|error`，默认 `either`，仅 `rx_delay` 支持后两项。
- `--tx_depth=1..256`，默认 32；`--bytes=32..4096`，默认 4096，包含测试头。
- `--rounds=1..1000000`，默认 8，仅 `tx_quota` 使用。
- `--rx_delay_us` 为 `uint32`，默认 1000；在 `rx_delay` 模式下须小于 `timeout_ms*1000`。
- `--timeout_ms=1..3600000`，默认 10000；`--port=1..65535`，默认 18519。
- `--urma_debug` 可打开 provider 调试日志。

工具需要设备支持 RM_CTP 以及相应的队列深度和已注册内存。`tx_quota` 使用一条 jetty，RX
深度为 `tx_depth+1`；`rx_delay` 使用两条共享 JFR/TX JFC/RX JFC 的 jetty，RX 深度为
`tx_depth`。TX CQ 深度分别为 `tx_depth+1` 与 `2*(tx_depth+1)`，保留错误排空的伪 CQE
容量。运行结束应检查两端最后的 PASS/FAIL 与进程退出码；中间的 accepted/retired 相等只说明
该阶段账本归零，不能单独证明 RX 槽、对端或硬件资源已完成收尾。本工具使用的是测试协议，
不定义 kbsocket 正式连接协议或消息分片格式。
