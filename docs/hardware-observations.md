# UB 裸设备硬件观测记录

[文档入口](README.md)

整理日期：2026-10-20。本文记录当前开发过程中用户反馈的真实硬件结果，以及为解释这些结果做的源码对照。测试命令仍以各工具 README 为准。

当前已记录正常 SEND/READ/WRITE、共享队列、RNR 后续发、单 SQ 故障排空、provider 提交边界、残留 RX 的 JFR 关闭、持续多 EID 收发/JFC 通知和对端进程退出。**WR 失败、逻辑连接失败、SQ 进入 ERROR 和物理端口故障必须分别判断。** 在本次裸设备 RM_CTP、`error_suspend=0` 配置下，RNR 后同一 SQ 可继续发送；对端退出后的 SEND 可以异步报错而本地 SQ 仍为 READY；已主动置 ERROR 的 SQ 也可能接受 post，必须保留软件发送门禁。

同 SQ 多目标实验先出现过 ACK_TIMEOUT；修正完成收集后，用户再次运行 `batch=32、rounds=8`，健康目标的 before/mixed/after 全部成功，空 JFR 目标的 256 条 WR 全部 RNR，同一 SQ 在 25 次查询中保持 READY。**这支持本次配置下同 SQ 多目标 RNR 隔离；旧 ACK_TIMEOUT 的原因仍未查明，不能由复测成功宣称已修复。**

## 证据与环境范围

- 硬件结果来自用户运行反馈和贴出的日志；本地 gMock 测试不计作硬件证据。
- 已贴出的日志使用裸设备 `udmac0d1e4`；各轮 EID 不同，具体选择在相应记录中列出。这些值是该环境的选择，不是设备型号或通用配置要求。
- 工具使用 RM_CTP；每个 JettyPool 内的多个 jetty 共用 JFR，TX/RX 各有一个 JFC。同池隔离工具另有独立的 sink pool。持续收发已测试每端同一裸设备的两个 EID/context；目标部署的两个裸设备、每设备两个 EID，共四个 context 尚未测试。四个物理裸设备并行由用户暂缓，不作为当前部署的必测项。
- 未完整记录网卡型号、固件、内核、UDMA、liburma 版本、每轮实际命令及运行二进制的提交号。以下结论仅覆盖已反馈的环境和场景。
- `rnr_continue` 实现在提交 `aacb1c0` 中；用户反馈了 `rnr_continue_success`，但尚未贴出该次完整双端日志和参数。不能把当时 README 中建议的 `tx_depth=1/32/256` 三组都记作已通过，也不能补写实际查询值；后续推荐参数调整为 64/128/256，不改变历史记录。

本文区分四种证据：**日志数值**是用户贴出的数据；**通过反馈**是用户确认 PASS，具体行为由工具的通过条件解释；**现场能力说明**是用户提供的当前设备/provider 支持范围，不补写成工具计数或 PASS；**源码对照**说明软件如何处理结果，不表示已经在服务器上验证了对应分支。

## 用户提供的现场能力说明

用户在已有进程退出与 provider 实测之后补充了以下范围：

- 真实链路/端口故障后，WR 通常以 ACK_TIMEOUT 终结。这是现场故障行为说明，不是 `peer_failure_test --fault=external` 的新日志，也不能据此解释此前没有确认链路故障的 mixed/provider 边界实验中 status 9 的根因。
- URMA async event 目前只有部分支持；当前环境明确无法观察到 PORT_DOWN async。因此不再将 PORT_DOWN 当作硬件故障验证必须补齐的事件。
- 已确认支持 JFS error、JFR error、JFC error，以及 JFR 水线相关事件。当前 JFS/相关 jetty 的错误 AE 需先触发其绑定的 JFC error，再联动产生，不能把它记作可独立注入的 JFS 错误事件。

上述说明没有改变下文 `process_exit/inflight_exit` 实际得到 async=0 的历史结果。事件支持与某次故障触发事件是两回事；JFC 的错误 async 事件也不同于 streaming 已验证的正常完成通知。受支持事件的实际 Get/Ack 工具日志可在对应注入条件下单独登记，不要求任意 CQE 错误或端口故障都伴随 async。

## 已记录的硬件验证

| 工具 / 场景 | 已记录结果 | 支持的结论 |
| --- | --- | --- |
| [普通 SEND](../tools/send_test/README.md) | 4096 字节，消息数/batch 为 `1/1`、`1000/32`、`1024/256`，三组通过；接入 RxBufferPool 后同样三组通过 | 正常提交、TX 退休、接收 buffer 复用、payload 校验及正常关闭可完成 |
| [共享队列 SEND_WITH_IMM](../tools/shared_queue_test/README.md) | 单 SQ、双 SQ、四 SQ 三组通过；双 SQ 日志两端均 `accepted=retired=received=2002`、`imm_mask=0xff` | 单 context 多 jetty、共享 JFR/JFC、逐 WR 选择远端 jetty及双向收发可工作 |
| [单 SQ TX 排空](../tools/tx_drain_test/README.md) | `accepted=retired=256`，ACK timeout 253、RNR 3、`flush_done=1`，软件拒绝后续发送，双方资源关闭 | 错误完成与显式 ERROR 排空能够共同完成账本回收 |
| [同池 SQ 故障隔离](../tools/sq_isolation_test/README.md) | 三组通过；最大批次出现非零 FLUSH_ERR，健康 SQ 和共享 RX 持续工作 | 对一条 SQ 主动置 ERROR 后，可单独排空该 SQ，并继续使用另一条 SQ |
| [本地 TX 配额](../tools/backpressure_test/README.md) | 用户确认 `tx_quota` 测试通过，未附独立详细计数 | SQ 软件配额耗尽后拒绝额外请求，退休完成后可提交此前未被接受的请求 |
| 延迟投递 RX | `rx_delay_us=0` 和 `10000` 均反馈 `rx_delay_terminal_failure`；0 的详细日志见下文 | 缺 RX 可以表现为 post 已接受、随后异步 RNR；错误排空及独立清理可完成 |
| 同 SQ 的 RNR 后续发 | 用户反馈 `rnr_continue_success` | 首批 RNR 终结后，补投远端 RX，同一 jetty 可成功发送新批次；没有重建或切换 SQ |
| [同 SQ 多目标](../tools/sq_multiplex_test/README.md) | 首次 mixed 曾出现 ACK_TIMEOUT；复测 `batch=32、rounds=8` 双端 PASS，健康 768 条全部成功、sink 256 条全部 RNR | 本次同 SQ 可交替服务健康/缺 RX 的不同 JFR，失败 WR 未阻止健康流量；旧 ACK_TIMEOUT 原因未明 |
| [Provider 提交边界](../tools/provider_boundary_test/README.md) | `tx_depth=64/128/256` 分别接受并退休 `128/256/512` 条，三组均容量拒绝和部分接受；额外 ERROR post 组也通过 | 真实容量拒绝为 `ENOMEM`；ERROR SQ 仍可能接受 post；已观察到非零 `WR_UNHANDLED` |
| [残留 RX 的 JFR 关闭](../tools/jfr_lifecycle_test/README.md) | `rx_depth/consumed` 为 `1/0、32/0、256/0、32/7、256/255` 五组通过 | 对端静默、依赖 jetty 已删除后，残留 posted RX 不妨碍本次 DeleteJfr；之后释放 MR |
| [持续收发与 JFC 通知](../tools/streaming_test/README.md) | 同设备两个 EID/context，每 context 100001 条、4096 字节的持续 request/echo 通过；后续 `window=1、arm_delay_us=0` 两端六行 OBSERVED | 持续 buffer 复用、各 context JFC 通知和 JFCE fd readiness 已覆盖；四 context 部署尚未覆盖 |
| [单 WR READ/WRITE](../tools/rma_test/README.md) | READ/WRITE 各 `4 KiB、16 KiB、1 MiB、8 MiB`，每组 8 轮 PASS，未投递 RX | 单 WR 可传输大于 4 KiB 的 RMA 数据；本地/远端 payload 与 guards 校验及关闭通过 |
| [对端退出后提交](../tools/peer_failure_test/README.md) | `process_exit` 的 batch 32、256 均 post 成功，全部以 status 8 退休；本地查询前后 READY，无 async | 已确认对端退出后，本地仍可接受 SEND；故障通过逐 WR CQE 报告，不必伴随本地 SQ ERROR/async |
| 提交后对端退出 | `inflight_exit、batch=256` 全部 SUCCESS，退出确认时软件尚有 224 条未退休 | 提交、退出协调和完成收集通过；未证明硬件在途 WR 被退出中断 |

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
- 本节同池隔离日志的 `WR_UNHANDLED` 为 0；后续 provider 提交边界实验已实际取得非零结果，见下一节。
- `rejected_sends` 验证的是 kbsocket 的软件门禁；后续绕过门禁的独立实验反而观察到 ERROR jetty 的 post 返回成功，不能依赖 provider 拒绝提交。
- 本测试验证一条 SQ 主动置错后的隔离，没有验证同一 SQ 内不同逻辑连接的错误隔离，也没有模拟所有设备或对端进程同时失效。

开发时曾在 `tx_depth=32` 的测试中出现 `urma_flush_jetty: Invalid parameter`、返回 `-22`。请求数沿用 64 条 poll 缓冲，超过了该 SQ 深度；提交 `d16b648` 将 flush 请求限制为 `min(输出容量, 64, tx_depth)` 后，用户确认三组测试全部通过。即使本 SQ 账本已空，确认软件队列为空时也必须使用合法的 flush 请求数。

## Provider 容量拒绝与部分接受

服务端 `udmac0d1e4:23` 的目标 JFR 不投 RX，客户端 `udmac0d1e4:1` 连续直接 post，不经过软件 quota，也不在提交期间 poll。每条 WR 是单 SGE、非 inline 的 4 KiB SEND；请求 `tx_depth=1、attempts=128`，两次客户端结果为：

| batch | offered | accepted / retired | calls | last_status | enomem | partial_batches | ACK_TIMEOUT | RNR |
|------:|--------:|-------------------:|------:|------------:|-------:|----------------:|------------:|----:|
|     2 |      66 |            64 / 64 |    33 |          12 |      1 |               0 |          61 |   3 |
|     3 |      66 |            64 / 64 |    22 |          12 |      1 |               1 |          57 |   7 |

两次均出现 `JFS overflow`，`flush_done=1、flush_calls=1、unhandled=0`，最终 PASS 确认所有接受项恰好退休一次且两端关闭资源。batch=3 时，前 21 次调用接受 63 条；第 22 次调用接受第 64 条，在第 65 条处返回 `ENOMEM`，第 66 条属于未接受后缀。失败返回不撤销此前已经接受的前缀，不能整批重发。

这支持当前环境、配置和 WR 形式下，请求 `tx_depth=1` 时有效接受容量为 64。后续 README 和验证矩阵从 `tx_depth=64` 开始，省去更小深度的重复硬件测试；该选择不定义 URMA 通用最小深度，也不要求软件配额下限为 64。

用户随后运行新参数，前三组不启用 `error_post`，均双端 PASS：

| tx_depth / batch / attempts | offered | accepted / retired | calls | ACK_TIMEOUT | RNR | FLUSH_ERR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 / 3 / 1024 | 129 | 128 / 128 | 43 | 127 | 1 | 0 |
| 128 / 127 / 2048 | 381 | 256 / 256 | 3 | 251 | 5 | 0 |
| 256 / 255 / 4096 | 765 | 512 / 512 | 3 | 371 | 8 | 133 |

三组均为 `last_status=12、enomem=1、partial_batches=1、flush_done=1、flush_calls=1、unhandled=0`，最后一批都接受两条后拒绝后缀。当前 WR 形式下实际容量分别为请求深度的两倍，不能把软件配额或 CQ 容量按这一倍率自动放大。本地 [UDMA SQ 创建与 WR 空间计算](../../umdk/src/urma/hw/udma/udma_u_jfs.c)按通用 WR 预留空间、再结合实际 SEND 占用及页对齐，能解释这些数据和此前的 `1→64`；服务器 provider 版本尚未核对，该解释不是 URMA 通用容量契约。

第四组为 `64/3/1024 --error_post`：先将原始 128 条全部退休并完成 ERROR 排空，查询到 `state=3` 后，再向同一 SQ 直接提交一条额外 WR。结果为：

```text
offered=130 accepted=129 retired=129 calls=44
last_status=0 enomem=1 partial_batches=1
ack_timeout_err=121 rnr=7 unhandled=1
flush_done=1 flush_calls=3
direct_ERROR_post status=0 queried_state=3 terminal_pending=false
```

本组双端 PASS，确认 ERROR SQ 直接 post 仍可返回 `URMA_SUCCESS`，额外 WR 最终也获得终结证据；`last_status=0` 是最后一次 probe 的结果，不覆盖之前的 `ENOMEM` 计数。本轮 `FlushJetty` 实际返回了一条 `WR_UNHANDLED`，补齐非零软件 flush 观测。不过现有计数按整轮汇总，不能仅凭 `unhandled=1` 严格认定它属于那条额外 probe；需要逐 probe 状态和完成来源才能建立对应关系。

这些实验均未观察到同步 `EAGAIN`。ACK_TIMEOUT 的原因仍未查明；错误退休数量等于接受数量只证明排空完整，不代表发送成功。ERROR 后 post 能被接受也不表示 SQ 恢复可用，生产侧仍必须阻止这种提交。

## 延迟 RX：post 成功与发送成功不同

`rx_delay_us=0、bytes=4096、tx_depth=32` 的详细日志为：

| 流量         | accepted | retired | success | RNR | FLUSH_ERR_DONE | 服务端 received |
|--------------|---------:|--------:|--------:|----:|---------------:|----------------:|
| 原始消息     |       32 |      32 |       0 |  32 |              1 |               0 |
| 独立清理消息 |       32 |      32 |      32 |   0 |              0 |              32 |

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

## 同 SQ 多目标：接收完成后仍出现 ACK_TIMEOUT

用户在 `sq_multiplex_test` 的旧版完成收集逻辑下运行 `bytes=4096、batch=32、rounds=8`，mixed 阶段一次提交 64 个 WR，按 sink/healthy 交替排列。服务端两个目标使用独立 JFR，仅 healthy 预先投递 RX。首轮结果如下，ACK_TIMEOUT 计数由日志中的 `other_error=1、last_other_status=9` 解释而来：

| 流量 | 客户端 accepted | retired | SUCCESS | RNR | ACK_TIMEOUT | 服务端 received |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Healthy before | 32 | 32 | 32 | 0 | 0 | 32 |
| Healthy mixed | 32 | 4 | 3 | 0 | 1 | 32 |
| Sink mixed | 32 | 4 | 0 | 3 | 1 | 无 RX 投递 |
| Healthy after | 0 | 0 | 0 | 0 | 0 | 0 |

客户端首先报告 `empty sink WR did not complete with RNR: 9`。旧逻辑消费完当前 poll 返回的 CQE 后退出，因此本轮仅观察到 8 条终结 CQE，剩余 56 条（healthy/sink 各 28 条）结果未知，不能记作成功、失败或丢失 CQE。服务端已经收到并通过消息身份、payload、路由和 IMM 校验的 healthy 消息为 32 条；随后控制读取失败，错误码为 `104`（ECONNRESET）。双方 Close 均以 `16`（EBUSY）保留资源，最终没有 PASS。

本轮客户端额外打印：

- `jetty_id=55646、state_checks=2、queried_rnr_retry=0、queried_err_timeout=0、accepted_known=true、provider_eagain=0`。
- healthy mixed 的 CQE 观察耗时为 `24450..129601842 ns`，sink mixed 为 `24050..129601322 ns`。
- 旧版只对符合预期的 CQE 更新轮次时序：`first_healthy_ns=24450、last_healthy_ns=25590`，`first_rnr_ns=24050、last_rnr_ns=25290`；`healthy_before_first_rnr=0、healthy_before_last_rnr=2`。其中 healthy 的最后时间不包含后来的 ACK_TIMEOUT。

`state_checks=2` 仅对应初始检查和 before 完成后的检查；错误发生后未查询 SQ，不能从该值断言此时仍为 READY 或已经进入 ERROR。旧版随后打印的第 1 至 7 轮全零记录来自预分配统计项，并不表示执行了这些轮次。

这些日志明确区分了远端已接收与本地确认成功：至少一条已经送达 healthy 的 WR 在发送侧得到 ACK_TIMEOUT。不能因此重放已接受的数据。两类目标接近同时出现约 129.6 ms 的完成延迟，与下节本地驱动的 128 ms 超时档位相符，但这只是源码对照；是否涉及 ACK 路径、同 SQ 完成追踪或驱动/固件问题，当前证据无法确定，也不能单凭 CQE 时序认定硬件队头阻塞。

针对旧版提前结束观测的问题，工具代码已调整为：对合法但非预期的终结状态继续收集本批完成，分类统计 ACK_TIMEOUT 等错误，收齐本批后再次查询 SQ，并在控制面交换阶段失败与 TX 静默状态。超时或完整性异常时不新增查询，最后查询值可能仍来自出错前。完整性异常会停止后续 poll；healthy ACK_TIMEOUT 或 sink 非 RNR 仍判 FAIL，失败后不进入 after。随后复测结果见下一节；该次通过不抹去本次失败记录。

## 同 SQ 多目标复测：RNR 与健康完成共存

用户随后贴出完整双端 PASS 日志，参数仍为 `bytes=4096、batch=32、rounds=8`，每轮 mixed 为同一次 post 的 64 条交替 WR：

| 流量 | accepted | retired | SUCCESS | RNR | 服务端 received |
| --- | ---: | ---: | ---: | ---: | ---: |
| Healthy before | 256 | 256 | 256 | 0 | 256 |
| Healthy mixed | 256 | 256 | 256 | 0 | 256 |
| Healthy after | 256 | 256 | 256 | 0 | 256 |
| Sink mixed | 256 | 256 | 0 | 256 | 无 RX 投递 |

全部其他错误计数为 0；`jetty_id=55647、state_checks=25、requested_rnr_retry=6、requested_err_timeout=2、queried_rnr_retry=0、queried_err_timeout=0、last_queried_state=1、accepted_known=true、provider_eagain=0`。每轮 `healthy_retired=healthy_success=sink_retired=sink_rnr=32`，健康完成在最后一条 RNR 之前已观察到 31 条。两端都完成正常资源关闭。

软件从 post 前到 CQE 消费的累计区间为：healthy before `18.220..41.530 μs`、mixed `17.190..39.980 μs`、after `18.020..33.040 μs`，sink `16.900..39.760 μs`。这些是软件观察耗时，不是硬件网络延迟或精确重试时间。

本轮支持三个有限结论：

- 同一 RM_CTP SQ 可逐 WR 指向两个独立 JFR；缺 RX 目标的 RNR 没有使本轮健康目标 WR 失败。
- 首轮到末轮均未主动 modify ERROR、flush 或重建 SQ，检查点查询仍为 READY，后续健康流量继续成功。
- RNR 终结逐 WR，不能仅据此断言整条 SQ 或物理端口失效。

不能由这些日志推出严格完成顺序、普遍无队头阻塞、所有错误隔离或跨主机/端口故障隔离。两个目标仍位于同一远端主机/EID；查询是检查点采样，不能证明期间没有瞬时状态变化。本次未复现 ACK_TIMEOUT，而前次工具修复只改善完成收集及失败同步，不是 ACK 路径的硬件修复。

## 单 WR READ/WRITE：大于 4 KiB 且无需投递 RX

`rma_test` 使用服务端 `udmac0d1e4:14`、客户端 `udmac0d1e4:25`，READ 和 WRITE 分别测试 `4096、16384、1048576、8388608` 字节，每组 8 轮。八组均通过 payload/guards 校验并关闭资源：

- 客户端每组 `accepted=retired=verified=8、pending=false、post_status=completion_status=0`。
- 服务端不发起 RMA WR，`accepted=retired=0、verified=8` 是正常结果。
- 双端均 `RX_posted=0`；JettyPool 仍创建 JFR 对象，不代表完全不需要该对象。
- 每轮是单个 WR、单 SGE，前一轮完成后才进入下一轮；不覆盖多个并发 RMA WR 或大包 SEND。

这确认本次 RM_CTP READ/WRITE 单 WR 能传输到 8 MiB，无需按 SEND 的 4 KiB RX buffer 限制软件切片。设备报告 `max_read_size=max_write_size=268435456`（256 MiB），但不能将报告上限记为已测试大小。

READ 在本地成功 CQE 后立即校验目的内存；WRITE 则在发送端成功 CQE 后，经 TCP 完成消息通知服务端再校验。因此本次没有测定远端内存对 CPU 可见的精确时间点，也未验证生产协议中 WRITE 加数据面通知的发布顺序。软件测得 8 MiB CQE 消费耗时约 183–190 μs；这些串行小样本不是吞吐或延迟基准。

其中一次服务端误用 `8188608`，客户端使用 `8388608`，双方在参数交换时以 `peer RMA role/options mismatch: 71` 拒绝，未接受 WR；后续更正后通过。这是参数不匹配，不能记为硬件 RMA 失败。该轮 Close 保守保留 DMA 依赖，也不能由此推导硬件资源无法关闭。

## 持续双 context 收发与事件通知

`streaming_test` 已在每端同一裸设备的两个 EID 上持续运行；每个 EID 有独立 context。详细双端日志使用服务端 `udmac0d1e4:14,udmac0d1e4:18`、客户端 `udmac0d1e4:1,udmac0d1e4:21`，参数为 `messages=100001、bytes=4096、window=32、events=true、arm_delay_us=1000`。

每端两个 context 均 `accepted=retired=received=rx_posted=100001`，payload 校验通过且两端正常关闭。事件结果为：

| 端 / context | TX events | RX events | acks | recheck_progress | fd_wakes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 服务端 0 | 3099 | 1837 | 4936 | 3076 | 7 |
| 服务端 1 | 3077 | 1824 | 4901 | 3061 | 4 |
| 客户端 0 | 768 | 771 | 1539 | 723 | 0 |
| 客户端 1 | 761 | 756 | 1517 | 745 | 0 |

客户端本轮 `waits=0`：rearm 后延迟并复查，发现任一 context 有进展就继续推进，不进入 fd 等待。JFC 通知仍会被收取和 ack，所以“有 events/acks、没有 fd_wakes”并不矛盾。服务端本轮 `waits=fd_wakes=11`。旧版把通知和 fd readiness 合并成一个 OBSERVED/NOT_OBSERVED 的输出已经拆开，旧版 NOT_OBSERVED 不能抹去实际事件计数。

用户后续用 `arm_delay_us=0、window=1` 运行，确认客户端、服务端均出现六行 OBSERVED，即每个 context 的 JFC 通知、JFCE fd readiness 及两项汇总全部观测到；该次未提供完整计数，不补写消息数。fd readiness 证明观察到 `POLLIN`，不证明线程实际睡眠过，也不证明所有通知竞态都已覆盖。

当前已覆盖持续 buffer 复用、同设备多 EID/context、通知获取与 ack、rearm 后复查，以及 fd readiness。目标部署的两个物理裸设备、每设备两个 EID 的四 context 同时运行仍待测试；不能从同设备双 context 的结果推导跨设备并行已通过。

本轮优先实现严格的 `--two_devices_two_eids` 拓扑检查，现场按照[矩阵 D1–D3](hardware-validation-matrix.md)分别运行无 events、`window=1` 的事件观察和 `window=32、arm_delay_us=1000` 的 recheck 观察。三组均为每 context 100001 条、4096 字节，具体两端命令见 [streaming_test README](../tools/streaming_test/README.md)。新增模式与本地单测不构成四 context 的硬件通过证据，后续日志另行追加。

## 带残留 RX 的共享 JFR 关闭

`jfr_lifecycle_test` 的五组已全部通过，接收端结果如下：

| rx_depth | consumed | residual_rx | DeleteJfr status |
| ---: | ---: | ---: | ---: |
| 1 | 0 | 1 | 0 |
| 32 | 0 | 32 | 0 |
| 256 | 0 | 256 | 0 |
| 32 | 7 | 25 | 0 |
| 256 | 255 | 1 | 0 |

前三组接收端从未发布 endpoint，RX 均未消费；后两组双端约定流量完成、发送端分别退休 7/255 条后确认静默，再开始删除。接收端先删除两个依赖 jetty，再调用 `DeleteJfr`，成功后才释放 MR 和 buffer backing。实际残留 RX 没有用额外 SEND 或伪造完成消耗掉。

本次 `DeleteJfr` 的少量样本约为 1.14–1.16 ms，不能据此推导固定时延或深度无关的复杂度。实验支持：在本次 provider 和受控静默顺序下，尚有 posted RX 不妨碍删除共享 JFR。它没有验证对端仍可能发送时的删除，也未记录逐残留 RX 的 CQE 行为。

这是整个接收资源域的关闭，不是单连接关闭；共享 JFR 仍服务其他连接时不应随连接退出删除。生产 `RxBufferPool::Close()` 的安全前置条件没有随本实验改变，brpc worker 仍访问对象时的进程退出也不在覆盖范围内。

## 对端进程退出：错误 CQE 不必伴随本地 SQ ERROR

`peer_failure_test` 的服务端以独立 URMA 子进程承载 endpoint，监督进程保留 TCP 控制。`process_exit` 先完成一条基线 SEND，再由监督进程 SIGKILL 子进程并 `waitpid` 确认，客户端收到确认后才提交新批次。服务端使用 `udmac0d1e4:23`，客户端使用 `udmac0d1e4:1`：

| batch | post_status | accepted / retired | 终结状态 | 首个 / 最后 CQE 消费耗时 |
| ---: | ---: | ---: | --- | --- |
| 32 | 0 | 32 / 32 | status 8 × 32 | 38.920405 / 41.353731 ms |
| 256 | 0 | 256 / 256 | status 8 × 256 | 42.751658 / 86.617724 ms |

status 8 为 `URMA_CR_REM_ACCESS_ABORT_ERR`，与旧 endpoint 已失效的场景相符，但状态本身不能定位具体资源或校验环节。两轮本地 SQ 在 post 前及观察后均查询为 READY；async fd 可用但事件计数为 0，观察包含 TX 轮询及完成后的默认 1 秒额外窗口。客户端全部接受项恰好退休一次并正常关闭；服务端 PASS 表示监督协议完成且子进程被回收，不表示被杀子进程执行了正常 Close。

该结果确认 post 成功不能判断远端存活，远端故障也不必令本地 SQ 进入 ERROR 或发出可见 async 事件。时间从客户端发送逻辑开始到软件消费 CQE，不能当成从 SIGKILL 起计算的故障检测时延。没有继续向健康远端发送，所以不能仅由 READY 查询证明后续正常发送能力。

`inflight_exit、batch=256` 则先在远端投递 256 个 RX，客户端提交 256 条 WR，然后请求监督进程退出 endpoint。实际日志为：

```text
post_status=0 accepted=256
retired_before_exit_request=32，均为 SUCCESS
software_unretired_at_confirmation=224
retired=256，最终均为 SUCCESS
first_cqe_ns=56680 last_cqe_ns=1065049588
```

本轮本地查询前后仍为 READY，无 async 事件，客户端正常关闭且服务端完成 SIGKILL/回收。它证明提交、退出协调和完成收集流程可完成，但 **224 是软件未退休数，不是硬件在途数**：等待 TCP 退出确认期间，CQE 可能已生成并留在队列中。约 1.065 秒的最后消费时间也包含该控制等待，不能视为硬件处理时延。当前没有证据证明退出中断了仍由硬件执行的 WR，SUCCESS CQE 也不代表对端应用已消费消息。

## 待验证：status 8 与同 SQ 健康目标共存

用户希望缩小故障隔离范围，先观察 `URMA_CR_REM_ACCESS_ABORT_ERR`（status 8）是否影响同一 SQ 上的健康目标。现有证据只覆盖同 SQ 的 RNR 隔离，以及单个已退出目标产生 status 8 时本地查询仍为 READY；尚未组合验证健康流量。

新增 [sq_multiplex_test 的 peer_exit 场景](../tools/sq_multiplex_test/README.md)先用同一客户端 SQ 向故障 endpoint 发送一条 baseline；客户端取得 SUCCESS CQE、查询 READY 并完成结果握手后，服务端才让子进程消费并校验 RX payload/IMM，再 SIGKILL 其专用 URMA 进程并 waitpid 确认。第二次握手确认退出后，健康 endpoint 由存活进程继续服务，客户端开始每轮 healthy before/mixed/after；before 也发生在 SIGKILL 之后。

mixed 将故障/健康目标交替投递到同一 SQ，预期故障目标为 status 8、健康三个阶段均成功，并校验远端 payload。工具要求各检查点 SQ 为 READY，不主动 modify ERROR、不 flush、不重建 SQ，不重放已接受数据；原有 `--fault=rnr` 仍为默认模式。新增 Q1/Q2 分别用 `batch=32/128、rounds=8、bytes=4096`，见[硬件验证矩阵](hardware-validation-matrix.md)，尚无这两组的现场通过日志。

这是一项待硬件实测的验证方案，不是已经确认的隔离能力，也不直接修改生产错误处理策略。必须根据后续双方日志登记实际状态、计数、SQ 查询与关闭结果；故障端成功 baseline 也不能省略，否则仅凭错误码无法区分失效目标与原本就不可访问的目标。

## 接口与查询值的解释边界

本节来自接口文档和本地源码分析。服务器实际加载的库版本、配置仍需对应确认。下列外部源码链接采用工作区中 `kbsocket`、`kernel` 并列的目录布局。

URMA 对 RNR 的公共定义是远端 JFR 无 buffer、重试次数耗尽；`error_suspend=0` 表示 error continue。[官方 API 文档](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)

本地内核 [uburma_cmd.c](../../kernel/drivers/ub/urma/uburma/uburma_cmd.c) 的 jetty query 路径将 `arg.out.jfs_flag` 固定填为 0。因此查询返回 `error_suspend=0` 不能独立证明硬件模式；工具同时检查创建配置，最终用同一 SQ 的成功续发及远端校验提供行为证据。

本地 UDMA [udma_dfx.c](../../kernel/drivers/ub/urma/hw/udma/udma_dfx.c) 从硬件 context 读取 state、`rnr_retry_num` 和 `ta_timeout`。同 SQ 多目标日志的两个查询值均为 0，不能据此认定查询未实现或参数没有配置：

- 当前 JettyPool 创建请求为 `rnr_retry=6、err_timeout=2`。[udma_jetty.h](../../kernel/drivers/ub/urma/hw/udma/udma_jetty.h) 使用 `err_timeout / 8` 转换为硬件档位，所以请求 2 对应查询档位 0。[udma_jetty.c](../../kernel/drivers/ub/urma/hw/udma/udma_jetty.c) 的档位时间表为 128/1000/8000/64000 ms。日志中的约 129.6 ms 与档位 0 的 128 ms 相符；实际服务器版本未核实，不能把这一对应关系当作已测得的硬件计时保证，也不能对查询值 0 套用 `4.096us * 2^0`。
- 同一驱动仅在设备具备私有 `UDMA_CAP_FEATURE_RNR_RETRY` 能力时写入请求的 RNR 重试值。因此查询 0 可能来自能力限制，仍需核对现场版本和能力，不能从它推算实际 RNR 重试次数或窗口。

status 9 是 ACK_TIMEOUT，status 10 才是 RNR 重试耗尽：[urma_opcode.h](../../umdk/src/urma/lib/urma/core/include/urma_opcode.h)。provider 分别映射硬件 transaction ACK timeout 与 RNR 状态，二者不能互换解释。

## 对 kbsocket 设计的约束

- 将逐 WR 结果、逻辑连接状态、物理 SQ 状态和设备/端口健康分开记录。单条 RNR 不足以把共享 SQ 或整个裸设备判为失效。
- SQ 仍能发送新消息，不代表失败的字节流可以无条件继续。原请求错误必须显式交给上层处理，不能跳过缺失数据或自动重放已接受的请求。
- 继续保留软件发送门禁与逐 WR 退休规则；已实测 ERROR post 仍可成功返回。只有明确的 ERROR 排空契约才解释 FLUSH_ERR_DONE，不能从普通 RNR 或计数相等推导队列已经停机。
- 对端退出不能代替逐 WR 终结证据，async 缺失也不能否定 CQE 已报告的远端故障；共享 JFR 删除要先满足整个资源域静默和依赖释放顺序。
- 是否缩小生产层遇到 RNR 时的隔离范围，需要单独修改状态机及测试；本文只记录证据，不宣称该改动已经完成。

## 尚未覆盖与后续记录

已完成的范围见上文；剩余问题不能以工具最终 PASS 自动勾选：

- ACK_TIMEOUT 的根因仍未查明；同 SQ 多目标复测成功不抹去首次失败，provider 边界实验也继续出现该状态。
- 延迟 RX 后原始批次全部成功，以及 RNR 的实际重试次数和时间窗口。
- provider 同步 `URMA_EAGAIN`；已观察到的是 `ENOMEM` 容量拒绝及部分接受，尚不能证明 socket 的 EAGAIN 到 EPOLLOUT/NotifyWritable 闭环。
- ERROR probe 的逐 WR 终结状态及来源；整轮已取得非零 `WR_UNHANDLED`，但汇总日志未将它与 probe 绑定。
- 优先补同一共享 SQ 的 status 8 故障隔离：新 peer_exit 场景尚未实测，已有 RNR mixed 和单目标 process_exit 不能代替健康目标的实际成功完成；跨远端主机/链路的隔离也不能据此推定。
- 优先补两个物理裸设备、每设备两个 EID 的四 context 持续收发、补投与通知，使用严格拓扑模式分别执行矩阵 D1–D3；四个物理裸设备并行按用户部署范围暂缓。
- 真正中断硬件在途 WR 的对端退出；`inflight_exit` 已验证提交后退出的控制时序，但本次全部 SUCCESS，硬件在途未确认。
- 链路/端口真实故障的工具日志：用户说明通常为 ACK_TIMEOUT，现有 external 场景尚无日志，不把当前无法观察的 PORT_DOWN async 列作通过条件。
- 对用户确认支持的 JFS/JFR/JFC error 和 JFR 水线事件，补独立 Get/Ack 工具记录时应遵循其注入条件；当前 JFS/相关 jetty AE 需通过绑定 JFC error 联动。既有进程退出测试未观测到 async，不要求为该故障强行触发事件。
- 多 WR 并发 RMA、RMA 故障、WRITE 加数据面通知的可见性/lease 协议，以及大于 8 MiB 的传输。
- 对端仍活跃时的共享 JFR 删除，以及 brpc worker 仍访问资源时的进程退出；已通过的是受控静默后的残留 RX 删除。

后续记录至少保留两端完整命令、工具提交号、硬件/软件版本、关键创建参数及查询值、完整计数、最终 PASS/FAIL 和退出码。新结果与本记录冲突时，应先核对配置和版本，再修订适用范围。

已测范围与待测项集中在 [硬件验证矩阵](hardware-validation-matrix.md)。工具实现与本地 mock 通过不计为硬件通过；每项按 OBSERVED/NOT_OBSERVED/SKIP/FAIL 独立登记。socket 的 EAGAIN→EPOLLOUT 与 brpc worker 退出属于后续软件集成，不是再增加一个硬件观察就能证明的契约。
