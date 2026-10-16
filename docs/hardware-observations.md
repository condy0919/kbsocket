# UB 裸设备硬件观测记录

[文档入口](README.md)

整理日期：2026-10-16。本文记录当前开发过程中用户反馈的真实硬件结果，以及为解释这些结果做的源码对照。测试命令仍以各工具 README 为准。

最重要的新证据是：**在本次裸设备 RM_CTP、`error_suspend=0` 配置下，WR 返回 RNR 重试耗尽后，同一条 SQ 仍能成功发送新消息。WR 失败、逻辑连接失败、SQ 进入 ERROR 和物理端口故障必须分别判断。**

## 证据与环境范围

- 硬件结果来自用户运行反馈和贴出的日志；本地 gMock 测试不计作硬件证据。
- 已贴出的近期日志使用裸设备 `udmac0d1e4`，服务端 `eid_index=65`、客户端 `eid_index=61`。这些值是该环境的选择，不是设备型号或通用配置要求。
- 工具使用 RM_CTP、单 context；每个 JettyPool 内的多个 jetty 共用 JFR，TX/RX 各有一个 JFC。同池隔离工具另有独立的 sink pool。四个裸设备同时运行尚无测试记录。
- 未完整记录网卡型号、固件、内核、UDMA、liburma 版本、每轮实际命令及运行二进制的提交号。以下结论仅覆盖已反馈的环境和场景。
- `rnr_continue` 实现在提交 `aacb1c0` 中；用户反馈了 `rnr_continue_success`，但尚未贴出该次完整双端日志和参数。不能把 README 中建议的 `tx_depth=1/32/256` 三组都记作已通过，也不能补写实际查询值。

本文使用三个证据层次：**日志数值**是用户贴出的数据；**通过反馈**是用户确认 PASS，具体行为由工具的通过条件解释；**源码对照**说明软件如何处理结果，不表示已经在服务器上验证了对应分支。

## 已完成的硬件验证

| 工具 / 场景 | 已记录结果 | 支持的结论 |
| --- | --- | --- |
| [普通 SEND](../tools/send_test/README.md) | 4096 字节，消息数/batch 为 `1/1`、`1000/32`、`1024/256`，三组通过；接入 RxBufferPool 后同样三组通过 | 正常提交、TX 退休、接收 buffer 复用、payload 校验及正常关闭可完成 |
| [共享队列 SEND_WITH_IMM](../tools/shared_queue_test/README.md) | 单 SQ、双 SQ、四 SQ 三组通过；双 SQ 日志两端均 `accepted=retired=received=2002`、`imm_mask=0xff` | 单 context 多 jetty、共享 JFR/JFC、逐 WR 选择远端 jetty及双向收发可工作 |
| [单 SQ TX 排空](../tools/tx_drain_test/README.md) | `accepted=retired=256`，ACK timeout 253、RNR 3、`flush_done=1`，软件拒绝后续发送，双方资源关闭 | 错误完成与显式 ERROR 排空能够共同完成账本回收 |
| [同池 SQ 故障隔离](../tools/sq_isolation_test/README.md) | 三组通过；最大批次出现非零 FLUSH_ERR，健康 SQ 和共享 RX 持续工作 | 对一条 SQ 主动置 ERROR 后，可单独排空该 SQ，并继续使用另一条 SQ |
| [本地 TX 配额](../tools/backpressure_test/README.md) | 用户确认 `tx_quota` 测试通过，未附独立详细计数 | SQ 软件配额耗尽后拒绝额外请求，退休完成后可提交此前未被接受的请求 |
| 延迟投递 RX | `rx_delay_us=0` 和 `10000` 均反馈 `rx_delay_terminal_failure`；0 的详细日志见下文 | 缺 RX 可以表现为 post 已接受、随后异步 RNR；错误排空及独立清理可完成 |
| 同 SQ 的 RNR 后续发 | 用户反馈 `rnr_continue_success` | 首批 RNR 终结后，补投远端 RX，同一 jetty 可成功发送新批次；没有重建或切换 SQ |

共享队列双 SQ 日志中，每端 `messages_per_jetty=1001`；源 jetty 到目标 jetty 的 TX/RX 计数均为 `0→0:501`、`0→1:500`、`1→0:500`、`1→1:501`。这证明不同源/目标组合被实际使用，不是仅创建了多个空闲 jetty。

`accepted` 表示 provider 接受；`retired` 表示已获得逐 WR 终结证据并回收账本，可以包含成功和错误。两者相等不能单独证明发送成功，成功还需检查 `success` 和远端接收校验。

## ERROR 排空与同池隔离

同池隔离测试中，`bytes=4096、batch=256、rounds=8` 的双端日志为：

| 指标 | 服务端 | 客户端 |
| --- | ---: | ---: |
| 故障前健康流量 accepted/retired/received | 512/512/512 | 512/512/512 |
| 故障期间健康流量 accepted/retired/received | 2048/2048/2048 | 2048/2048/2048 |
| 故障后健康流量 accepted/retired/received | 512/512/512 | 512/512/512 |
| 故障 SQ accepted/retired | 256/256 | 256/256 |
| 故障 SQ success | 0 | 0 |
| FLUSH_ERR | 253 | 254 |
| ACK_TIMEOUT_ERR | 1 | 1 |
| RNR_RETRY_CNT_EXC_ERR | 2 | 1 |
| WR_UNHANDLED | 0 | 0 |
| FLUSH_ERR_DONE | 1 | 1 |
| 软件门禁拒绝后续发送 | 3 | 3 |
| healthy_retired_before_drained | 53 | 256 |

两端最终均报告故障 SQ 上每个 attempt 仅退休一次、拒绝新发送、健康 SQ/共享 RX 可用，并正常关闭资源。

这些日志表明：普通错误 CQE 与显式置 ERROR 后的 FLUSH_ERR 可以出现在同一轮排空中；FLUSH_ERR 不需要覆盖全部已接受 WR。`healthy_retired_before_drained` 表示软件观察到故障 SQ 进入 Drained 前已有健康完成被退休，不是硬件同时执行的时间线测量。

需保留以下边界：

- FLUSH_ERR_DONE 的 `user_ctx` 无效、不能用它查账本，是当前实现遵守的接口/provider 契约。日志只记录了边界数量，没有提供可用于验证该字段的原始 CQE 转储。
- `WR_UNHANDLED` 在已贴出的硬件日志中始终为 0。非零软件 flush 仍只有 gMock 覆盖，不能记作已在硬件上测到。
- `rejected_sends` 验证的是 kbsocket 的软件门禁，没有绕过门禁直接 post 到 ERROR jetty。因而这些日志不能证明 provider 会直接拒绝 ERROR jetty 上的 post。
- 本测试验证一条 SQ 主动置错后的隔离，没有验证同一 SQ 内不同逻辑连接的错误隔离，也没有模拟所有设备或对端进程同时失效。

开发时曾在 `tx_depth=32` 的测试中出现 `urma_flush_jetty: Invalid parameter`、返回 `-22`。请求数沿用 64 条 poll 缓冲，超过了该 SQ 深度；提交 `d16b648` 将 flush 请求限制为 `min(输出容量, 64, tx_depth)` 后，用户确认三组测试全部通过。即使本 SQ 账本已空，确认软件队列为空时也必须使用合法的 flush 请求数。

## 延迟 RX：post 成功与发送成功不同

`rx_delay_us=0、bytes=4096、tx_depth=32` 的详细日志为：

| 流量 | accepted | retired | success | RNR | FLUSH_ERR_DONE | 服务端 received |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 原始消息 | 32 | 32 | 0 | 32 | 1 | 0 |
| 独立清理消息 | 32 | 32 | 32 | 0 | 0 | 32 |

原始 SQ 的 `rejected_sends=1`；两批的其他错误计数均为 0；`quota_rejections=0、recovery_sends=0、provider_eagain=0`。双方最终输出 `rx_delay_terminal_failure` 并关闭资源。用户另反馈 `rx_delay_us=10000` 也进入同一结果分支，未提供对应详细计数。

对这组结果的解释：

1. 原始 WR 已被接受，随后通过 CQE 异步报告 RNR；本次没有观察到 provider 同步返回 `URMA_EAGAIN`。
2. 当前生产完成层收到错误后主动执行 `BeginDrain → ModifyJetty(ERROR)`，所以日志中出现 FLUSH_ERR_DONE。不能把这个边界归因于“硬件收到 RNR 就自动把 SQ 置 ERROR”。
3. 清理流量使用另一条健康 SQ 和独立消息身份，消费服务端后来投递的 RX。它没有恢复或重放原始失败消息，也不能证明原 SQ 仍可用。
4. `rx_delay_us=0` 只取消显式 sleep。客户端提交、TCP 通知、服务端调度和实际 post RX 之间仍有时间间隔；客户端也在收到 RX-ready 通知后才开始消费原始 TX CQE。当前没有测量这一间隔或实际 RNR 重试窗口。

因此，`rx_delay_terminal_failure` 的 PASS 表示错误路径及资源收尾通过，不能解释成原始 32 条消息发送成功。延迟 RX 后原始流量全部成功的 `rx_delay_success` 分支尚无真实硬件通过反馈。

## RNR 后，同一条 SQ 能否继续发送

用户随后反馈新增模式输出 `rnr_continue_success`。与上面的清理测试不同，该模式严格要求：

1. 服务端不投递 RX；客户端提交首批 WR，并逐条获得 RNR 终结 CQE。
2. 不调用 modify ERROR、flush，不销毁或重建 jetty，也不切换至另一条 SQ。
3. 首批全部终结、查询原 jetty 仍为 READY 后，才通知服务端投递 RX。
4. 客户端使用同一个 jetty、同一个远端目标发送具有新轮次身份的第二批消息；旧消息不重放。
5. 第二批 post 和 TX CQE 全部成功，服务端验证 payload、IMM 和消息身份；提交前、首批 RNR 后、第二批成功后三次查询均为 READY，双方关闭资源后才打印 PASS。

这些是当前[工具实现](../tools/backpressure_test/backpressure_test.cpp)的通过条件，不是本记录另行测得的 CQE 数值。由于尚无完整日志，具体批次深度、消息大小、jetty ID、`queried_rnr_retry` 和 `queried_err_timeout` 均未登记。

**此次通过支持“RNR 终结的是首批 WR，同一条物理 SQ 仍可继续发送新请求”。** 它不表示原始数据已送达，不证明任何错误之后都能继续使用 SQ，也不覆盖已经主动置为 ERROR 的 SQ。

测试仅在工具分支直接消费原始 CQE，并通过 AttemptLedger 逐条退休。当前生产 `TxCompletionProcessor` 仍对普通错误执行隔离和 ERROR 排空；本次记录没有修改该策略。

## 接口与查询值的解释边界

本节来自接口文档和本地源码分析。服务器实际加载的库版本、配置仍需对应确认。下列外部源码链接采用工作区中 `kbsocket`、`kernel` 并列的目录布局。

URMA 对 RNR 的公共定义是远端 JFR 无 buffer、重试次数耗尽；`error_suspend=0` 表示 error continue。[官方 API 文档](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)

本地内核 [uburma_cmd.c](../../kernel/drivers/ub/urma/uburma/uburma_cmd.c) 的 jetty query 路径将 `arg.out.jfs_flag` 固定填为 0。因此查询返回 `error_suspend=0` 不能独立证明硬件模式；工具同时检查创建配置，最终用同一 SQ 的成功续发及远端校验提供行为证据。

本地 UDMA 的 query 实现从硬件 context 读取 state、RNR retry 和 timeout，但 `err_timeout` 返回值可能是硬件档位，不能直接套用创建参数的时间编码。当前没有用户提供的查询数值，不推导实际重试时间。

## 对 kbsocket 设计的约束

- 将逐 WR 结果、逻辑连接状态、物理 SQ 状态和设备/端口健康分开记录。单条 RNR 不足以把共享 SQ 或整个裸设备判为失效。
- SQ 仍能发送新消息，不代表失败的字节流可以无条件继续。原请求错误必须显式交给上层处理，不能跳过缺失数据或自动重放已接受的请求。
- 继续保留软件发送门禁与逐 WR 退休规则。只有明确的 ERROR 排空契约才解释 FLUSH_ERR_DONE，不能从普通 RNR 或计数相等推导队列已经停机。
- 是否缩小生产层遇到 RNR 时的隔离范围，需要单独修改状态机及测试；本文只记录证据，不宣称该改动已经完成。

## 尚未覆盖与后续记录

当前还没有真实硬件证据覆盖：

- 延迟 RX 后原始批次全部成功，以及 RNR 的实际重试次数和时间窗口。
- 非零 `WR_UNHANDLED` 软件 flush；绕过软件门禁向 ERROR SQ post 的具体返回行为。
- provider 同步 `URMA_EAGAIN` 与部分接受边界；socket 的 EAGAIN 到 EPOLLOUT/NotifyWritable 闭环。
- 同一共享 SQ 上一个 peer 发生 RNR、其他 peer 同时持续成功的连接级隔离。
- 四个裸设备并行、持续无批间屏障收发、对端异常退出、链路/端口真实故障及异步事件。
- 尚有未消费 WR 的共享 JFR 关闭，以及 brpc worker 仍访问资源时的进程退出。

后续记录至少保留两端完整命令、工具提交号、硬件/软件版本、关键创建参数及查询值、完整计数、最终 PASS/FAIL 和退出码。新结果与本记录冲突时，应先核对配置和版本，再修订适用范围。
