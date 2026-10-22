# UB 裸设备硬件观测记录

[文档入口](README.md)

整理日期：2026-10-22。本文记录当前开发过程中用户反馈的真实硬件结果，以及为解释这些结果做的源码对照。测试命令仍以各工具 README 为准。

当前已记录单/多 SGE SEND、串行 READ/WRITE、并发 READ 与同 SQ READ/SEND 混合、READ 故障后的同 SQ 失败、共享队列、RNR 后续发、单 SQ 故障排空、provider 提交边界、残留 RX 的 JFR 关闭、分阶段及持续收包期间的水线重配、RX JFC_ERR 与关联 jetty 错误、持续多 EID 收发/JFC 通知和对端进程退出。**WR 失败、逻辑连接失败、SQ 进入 ERROR 和物理端口故障必须分别判断。** 在本次裸设备 RM_CTP、`error_suspend=0` 配置下，RNR 后同一 SQ 可继续发送；对端退出后的 SEND 可以异步报错而本地 SQ 仍为 READY；peer_exit 的原始 CQE 路径和正式完成层集成均已实测确认，同 SQ 上失效目标的 status 8 没有使健康目标的 before/mixed/after 流量失败。已主动置 ERROR 的 SQ 也可能接受 post，必须保留软件发送门禁。

同 SQ 多目标实验先出现过 ACK_TIMEOUT；修正完成收集后，用户再次运行 `batch=32、rounds=8`，健康目标的 before/mixed/after 全部成功，空 JFR 目标的 256 条 WR 全部 RNR，同一 SQ 在 25 次查询中保持 READY。**这支持本次配置下同 SQ 多目标 RNR 隔离；旧 ACK_TIMEOUT 的原因仍未查明，不能由复测成功宣称已修复。** 后续四 context 的 D2 首次运行也出现 status 9 和另一端事件等待超时；第二次完整通过并补齐 fd readiness，但首次失败仍需独立定位。

用户随后确认当前芯片存在 status 9（ACK_TIMEOUT）后 jetty 不可复用的缺陷，即使查询仍为 READY 也必须停止新提交；大并发 READ 容易触发，用户说明大并发 WRITE 不出现这一现象。旧 READ 故障实验已出现后续健康 READ 失败，工具因此改为单边 WRITE；新版 WRITE 的硬件结果仍待记录。该现场说明不自动定位历史 SEND/provider 各次 status 9 的触发原因。

## 证据与环境范围

- 硬件结果来自用户运行反馈和贴出的日志；本地 gMock 测试不计作硬件证据。
- 已贴出的日志使用裸设备 `udmac0d1e4`，后续四 context 测试还使用 `udmac1d1e4`；各轮 EID 不同，具体选择在相应记录中列出。这些值是该环境的选择，不是设备型号或通用配置要求。
- 工具使用 RM_CTP；每个 JettyPool 内的多个 jetty 共用 JFR，TX/RX 各有一个 JFC。同池隔离工具另有独立的 sink pool。持续收发已测试同设备双 EID/context，以及目标部署的两个裸设备、每设备两个 EID，共四个 context；后者已覆盖收发、补投、通知、recheck 和双端各路 fd readiness，D2 首次失败的根因仍未查明。四个物理裸设备并行由用户暂缓，不作为当前部署的必测项。
- 未完整记录网卡型号、固件、内核、UDMA、liburma 版本、每轮实际命令及运行二进制的提交号。以下结论仅覆盖已反馈的环境和场景。
- `rnr_continue` 实现在提交 `aacb1c0` 中；用户反馈了 `rnr_continue_success`，但尚未贴出该次完整双端日志和参数。不能把当时 README 中建议的 `tx_depth=1/32/256` 三组都记作已通过，也不能补写实际查询值；后续推荐参数调整为 64/128/256，不改变历史记录。

本文区分四种证据：**日志数值**是用户贴出的数据；**通过反馈**是用户确认 PASS，具体行为由工具的通过条件解释；**现场能力说明**是用户提供的当前设备/provider 支持范围，不补写成工具计数或 PASS；**源码对照**说明软件如何处理结果，不表示已经在服务器上验证了对应分支。

## 用户提供的现场能力说明

用户在已有进程退出与 provider 实测之后补充了以下范围：

- 当前芯片已知缺陷：jetty 出现 status 9 后，后续不可继续使用；READY 查询不代表请求处理仍正常。软件须停止新提交并进入故障排空路径。用户说明大并发 READ 易触发这一错误，而大并发 WRITE 不出现这一现象；这不是新版 WRITE 故障测试的通过记录，也不保证任意故障 WRITE 都不会返回 status 9。
- 真实链路/端口故障后，WR 通常以 ACK_TIMEOUT 终结。这是现场故障行为说明，不是 `peer_failure_test --fault=external` 的新日志，也不能据此解释此前没有确认链路故障的 mixed/provider 边界实验中 status 9 的具体触发原因。
- URMA async event 目前只有部分支持；当前环境明确无法观察到 PORT_DOWN async。因此不再将 PORT_DOWN 当作硬件故障验证必须补齐的事件。
- 已确认支持 JFS error、JFR error、JFC error，以及 JFR 水线相关事件。当前 JFS/相关 jetty 的错误 AE 需先触发其绑定的 JFC error，再联动产生，不能把它记作可独立注入的 JFS 错误事件。

上述说明没有改变下文 `process_exit/inflight_exit` 实际得到 async=0 的历史结果。事件支持与某次故障触发事件是两回事；JFC 的错误 async 事件也不同于 streaming 已验证的正常完成通知。受支持事件的实际 Get/Ack 工具日志可在对应注入条件下单独登记，不要求任意 CQE 错误或端口故障都伴随 async。

## 已记录的硬件验证

| 工具 / 场景 | 已记录结果 | 支持的结论 |
| --- | --- | --- |
| [普通 SEND](../tools/send_test/README.md) | 4096 字节，消息数/batch 为 `1/1`、`1000/32`、`1024/256`，三组通过；接入 RxBufferPool 后同样三组通过 | 正常提交、TX 退休、接收 buffer 复用、payload 校验及正常关闭可完成 |
| [多 SGE SEND_WITH_IMM](../tools/send_test/README.md) | G2/G3/G4 的 2/3/8 个独立注册段、4096 字节、1001 条均双端通过；源段 guard 校验 2002/3003/8008 次 | header/payload 无预拼接发送、接收连续 payload/IMM 与正常关闭通过；64 字节 G1 未反馈 |
| [共享队列 SEND_WITH_IMM](../tools/shared_queue_test/README.md) | 单 SQ、双 SQ、四 SQ 三组通过；双 SQ 日志两端均 `accepted=retired=received=2002`、`imm_mask=0xff` | 单 context 多 jetty、共享 JFR/JFC、逐 WR 选择远端 jetty及双向收发可工作 |
| [单 SQ TX 排空](../tools/tx_drain_test/README.md) | `accepted=retired=256`，ACK timeout 253、RNR 3、`flush_done=1`，软件拒绝后续发送，双方资源关闭 | 错误完成与显式 ERROR 排空能够共同完成账本回收 |
| [同池 SQ 故障隔离](../tools/sq_isolation_test/README.md) | 三组通过；最大批次出现非零 FLUSH_ERR，健康 SQ 和共享 RX 持续工作 | 对一条 SQ 主动置 ERROR 后，可单独排空该 SQ，并继续使用另一条 SQ |
| [本地 TX 配额](../tools/backpressure_test/README.md) | 用户确认 `tx_quota` 测试通过，未附独立详细计数 | SQ 软件配额耗尽后拒绝额外请求，退休完成后可提交此前未被接受的请求 |
| 延迟投递 RX | `rx_delay_us=0` 和 `10000` 均反馈 `rx_delay_terminal_failure`；0 的详细日志见下文 | 缺 RX 可以表现为 post 已接受、随后异步 RNR；错误排空及独立清理可完成 |
| 同 SQ 的 RNR 后续发 | 用户反馈 `rnr_continue_success` | 首批 RNR 终结后，补投远端 RX，同一 jetty 可成功发送新批次；没有重建或切换 SQ |
| [同 SQ 多目标](../tools/sq_multiplex_test/README.md) | 首次 mixed 曾出现 ACK_TIMEOUT；复测 `batch=32、rounds=8` 双端 PASS，健康 768 条全部成功、sink 256 条全部 RNR | 本次同 SQ 可交替服务健康/缺 RX 的不同 JFR，失败 WR 未阻止健康流量；旧 ACK_TIMEOUT 原因未明 |
| 同 SQ 的 status 8 隔离 | peer_exit 的 Q1/Q2（`batch=32/128、rounds=8`）双端 PASS；每个健康阶段分别 256/1024 条 SUCCESS，失效目标分别 256/1024 条 status 8 | 本次已退出 endpoint 与健康 endpoint 共用客户端 SQ，健康收发在 mixed 前、期间及之后均成功；各 26 次状态检查为 READY，无主动 ERROR/flush/重建/重放 |
| status 8 的生产完成层集成 | peer_exit 的 Q3/Q4（`batch=32/128、rounds=8`）双端 PASS；completion_path=processor，processor_events=processor_retired=1025/4097 | 真实完成经 TxCompletionProcessor 逐 WR 退休并归还额度，软件 lane 与硬件 SQ 保持 READY；健康三阶段均成功，跨完成批次及后续轮次继续可用 |
| [Provider 提交边界](../tools/provider_boundary_test/README.md) | `tx_depth=64/128/256` 分别接受并退休 `128/256/512` 条，三组均容量拒绝和部分接受；额外 ERROR post 组也通过 | 真实容量拒绝为 `ENOMEM`；ERROR SQ 仍可能接受 post；已观察到非零 `WR_UNHANDLED` |
| [残留 RX 的 JFR 关闭](../tools/jfr_lifecycle_test/README.md) | `rx_depth/consumed` 为 `1/0、32/0、256/0、32/7、256/255` 五组通过 | 对端静默、依赖 jetty 已删除后，残留 posted RX 不妨碍本次 DeleteJfr；之后释放 MR |
| [JFR 水线 async 事件](../tools/async_event_test/README.md) | depth=128/256 的首次事件通过，不重配的第二次未观察到；A6 的 depth=128、rewrite 8 轮各 Get/Ack 1 个匹配事件，590 条流量及关闭通过 | 当前配置下跨线后阈值回读为 0，补满后直接重写 64 可恢复下一次通知；仅 Ack/补投不应当作重新使能 |
| 持续收包期间的水线重配 | A8 两次高位重配各取得 8 个跨线事件、583 条流量；A9 低位重配取得首次跨线 1 个及 active_rearm 7 个事件、807 条流量，均双端通过 | 高位配置保留到后续跨线；低位配置在当前阶段触发并清零，补满不自动使能；不证明硬件操作精确重叠 |
| [RX JFC_ERR 与关联 jetty](../tools/async_error_test/README.md) | X1 服务端取得并 Ack JFC_ERR/JETTY_ERR 各 1 个，jetty0 为 ERROR、jetty1/JFR 为 READY；独立健康组 18 条成功，双端 PASS + RETAINED | 指定 RX JFC 故障与健康队列隔离已观测；缺少 RX 终结证据，未验证故障资源正常关闭 |
| [持续收发与 JFC 通知](../tools/streaming_test/README.md) | 同设备双 context 已通过；两设备各双 EID 的四 context 在 D1、D3 及额外的 `window=32、events/延迟 0` 组均双端 PASS；D2 首次 status 9/事件等待超时，第二次双端 PASS、各十行 OBSERVED | 四路持续收发、补投、通知、recheck 与双端各路 fd readiness 均有通过证据；D2 首次失败仍未定位，不能据复测通过宣称问题已修复 |
| [单 WR READ/WRITE](../tools/rma_test/README.md) | READ/WRITE 各 `4 KiB、16 KiB、1 MiB、8 MiB`，每组 8 轮 PASS，未投递 RX | 单 WR 可传输大于 4 KiB 的 RMA 数据；本地/远端 payload 与 guards 校验及关闭通过 |
| 并发 READ 与同 SQ READ/SEND 混合 | 16 KiB/1 MiB、window=32 的纯 READ/混合四组，以及 16 KiB、window=128 的混合组，均 8 轮双端通过 | 独立目的区间、每批最多 256 条混合 WR、逐 WR 退休及跨轮复用通过；不证明跨 opcode 完成顺序或故障隔离 |
| READ 故障后的同 SQ 行为 | 旧版 rma_failure_test 的 RF1–RF3 mixed 健康目标出现 status 9；RF0 最小组在故障 READ status 9 之后，新提交的 healthy after 也为 status 9，仍查询 READY | READ 的旧目标故障没有隔离到单个目标；与用户确认的 status 9 后 jetty 不可复用缺陷相符，不能靠 READY 判断恢复 |
| 单边 WRITE 故障隔离 | rma_failure_test 已改为 WRITE，WF0–WF3 待真机 | 以远端校验及逐 WR 终结重新验证，不继承历史 READ 或 SEND 结果；任何 status 9 停止新提交 |
| [对端退出后提交](../tools/peer_failure_test/README.md) | `process_exit` 的 batch 32、256 均 post 成功，全部以 status 8 退休；本地查询前后 READY，无 async | 已确认对端退出后，本地仍可接受 SEND；故障通过逐 WR CQE 报告，不必伴随本地 SQ ERROR/async |
| 提交后对端退出 | `inflight_exit、batch=256` 全部 SUCCESS，退出确认时软件尚有 224 条未退休 | 提交、退出协调和完成收集通过；未证明硬件在途 WR 被退出中断 |

共享队列双 SQ 日志中，每端 `messages_per_jetty=1001`；源 jetty 到目标 jetty 的 TX/RX 计数均为 `0→0:501`、`0→1:500`、`1→0:500`、`1→1:501`。这证明不同源/目标组合被实际使用，不是仅创建了多个空闲 jetty。

`accepted` 表示 provider 接受；账本工具的 `retired` 表示已获得逐 WR 终结证据并回收账本，可以包含成功和错误。两者相等不能单独证明发送成功，成功还需检查 `success` 和远端接收校验。`streaming_test` 是正常流量工具，其 `retired` 只累计通过校验的 SUCCESS 完成；错误 CQE 触发退出且不加入该计数，详见下文 D2 失败记录。

## 多 SGE SEND：独立源段组成一条消息

G2/G3/G4 的完整双端日志均使用服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:51`，共同参数为 `--bytes=4096 --messages=1001 --send_imm --timeout_ms=60000`。设备报告 `max_msg_size=65536、max_jfs_sge=13`，实际测试如下：

| 组 | SGE 数 / header_bytes / batch | 客户端 accepted / retired | 服务端 received | guarded_segments |
| --- | --- | --- | ---: | ---: |
| G2 | 2 / 32 / 64 | 1001 / 1001 | 1001 | 2002 |
| G3 | 3 / 33 / 128 | 1001 / 1001 | 1001 | 3003 |
| G4 | 8 / 32 / 256 | 1001 / 1001 | 1001 | 8008 |

各组 `accepted_known=true`，发送端打印 TX 与远端 RX 均确认，接收端通过 payload 校验，双方释放资源。工具将每个 SGE 放在独立注册的源内存中，按消息内全局偏移直接填充，不先复制到连续 TX buffer；接收端仍为一个连续 RX buffer，并检查来源、序号、opcode、IMM 与完整 payload。guarded_segments 为 `messages*sges`，是源段保护区校验次数，接收端没有这些源段所以为 0。

这支持使用分离的 fragment header/payload 作为一条 SEND_WITH_IMM 的两个 SGE。`bytes=4096` 包含 header；32 字节 header 时业务 payload 为 4064 字节。33 字节 header 组覆盖非整齐分段长度。能力报告的 64 KiB/13 SGE 不是实测上限，本轮只验证到 4096 字节和 8 SGE；G1 的 64 字节小消息尚无日志。该实验不验证跨消息保序，也不证明硬件或 provider 内部完全没有复制。

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

测试仅在工具分支直接消费原始 CQE，并通过 AttemptLedger 逐条退休。当前生产 `TxCompletionProcessor` 对 RNR 仍执行隔离和 ERROR 排空；后续只为 status 8 增加不主动排空的例外，没有将本节 RNR 实验的处理策略移入生产。

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

## 并发 READ 与同 SQ READ/SEND 混合

新版 rma_test 的五组双端日志使用服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:51`，共同参数为 `--operation=read --rounds=8`：

| 单条 READ 大小 | window | mixed_send | READ 总数 / verified | SEND 总数 | 客户端 accepted / retired |
| ---: | ---: | --- | ---: | ---: | --- |
| 16384 | 32 | false | 256 | 0 | 256 / 256 |
| 16384 | 32 | true | 256 | 256 | 512 / 512 |
| 1048576 | 32 | true | 256 | 256 | 512 / 512 |
| 1048576 | 32 | false | 256 | 0 | 256 / 256 |
| 16384 | 128 | true | 1024 | 1024 | 2048 / 2048 |

各组均 `pending=false、post_status=completion_status=0、accepted_known=true、partial_batches=0`，独立 payload 和 guard 校验通过，双方正常关闭。混合组客户端 `send_accepted=send_retired` 与表中 SEND 总数一致，服务端 `send_received` 相等。服务端不发起 READ，accepted/retired 为 0 正常；其 verified 是源数据及保护区校验，不是服务端主动 READ 的数量。

工具在同一 SQ 中交错提交 READ 与 64 字节 SEND_WITH_IMM。window=128 覆盖每批 128 条 READ 加 128 条 SEND；1 MiB、window=32 的每轮 READ 总量为 32 MiB。每条 READ 使用独立目的区间，跨轮更新数据并在完成与双方确认后复用。这些日志确认正常并发、混合推进和回收，不能将同一 WR 链的提交顺序当作不同 opcode 的完成顺序，也不能把收到 SEND 当作前序 READ 已完成的通知。

客户端的 `min_cqe_ns/max_cqe_ns` 从整批提交前到软件消费 CQE 计时；混合模式包括两种 opcode。1 MiB、window=32 的纯 READ/混合最大值分别为 725029/726038 ns，样本与计时方式不足以证明 SEND 对 READ 没有性能影响。真实在途并行度、并发 WRITE、故障 READ 的状态及同 SQ 隔离仍未由这些正向结果验证。

既有矩阵 R9/R10 的建议参数是 window=8；实际反馈为 window=32，因此保留 R9/R10 未运行的记录，将四个 window=32 组合另列为 R12–R15，不追认原参数已执行。

## READ 故障：status 9 后健康目标也失败

用户以旧版 `rma_failure_test` 执行 READ 故障实验，服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:52`，均使用 `--timeout_ms=10000`。工具先通过同一客户端 SQ 对健康源和故障子进程源各执行一条基线 READ，再完成 healthy before；服务端核验故障源后 SIGKILL/waitpid，客户端收到确认才开始 mixed。以下是历史 READ 结果，当前同名工具已改为 WRITE。

| 组 | bytes / batch / rounds | Healthy before 成功数 | 首轮 Healthy mixed | 首轮 Fault mixed | Healthy after |
| --- | --- | ---: | --- | --- | --- |
| RF1 | 16384 / 32 / 8 | 256 | accepted=retired=32，SUCCESS=1、status9=31 | accepted=retired=32，status9=32 | 未提交 |
| RF2 | 16384 / 128 / 8 | 1024 | accepted=retired=128，SUCCESS=1、status9=127 | accepted=retired=128，status9=128 | 未提交 |
| RF3 | 1048576 / 32 / 8 | 256 | accepted=retired=32，SUCCESS=1、status9=31 | accepted=retired=32，status9=32 | 未提交 |
| RF0 | 16384 / 1 / 1 | 1 | accepted=retired=verified=1，SUCCESS=1 | accepted=retired=1，status9=1 | 新提交 1 条，retired=1、status9=1、verified=0 |

四组两个 baseline 都各成功并校验 1 条，`peer_exit_confirmed=true、accepted_known=true、pending=false`，最后查询为 READY。RF1–RF3 首轮 mixed 健康访问失败，工具收集该轮所有已接受 WR 后停止；`Healthy mixed verified=1` 只表示其中一条成功，不代表该阶段通过。直方图没有给出成功项的 WR 身份，不能据此断言成功的是特定槽位。

RF0 的 mixed 两条 WR 都退休之后，工具又提交了一条新的健康 after READ，结果仍为 status 9。它提供了比“同批多条超时”更明确的证据：故障目标 READ 报 status 9 后，同一 jetty 的下一次健康访问仍失败，最后 `state_checks=10、post_calls=5、last_state=1`。READY 只是状态查询值，不能作为该 jetty 仍可提供正常服务的证明。

这些轮次客户端都以 `healthy/baseline READ failed: 9` 失败退出，服务端随后收到 `control receive EOF: 104`。104 属于控制连接结束后的错误，不是新的 RMA CQE 状态，也不替代客户端 status 9 的原因。双方保留未证实可释放的依赖到 worker 退出，不能将 RETAINED 解读为正常 URMA 资源关闭已经验证。

另一次 `bytes=16384、batch=1、rounds=1` 运行在服务端以 `waitpid timeout: 110` 失败，双方 `peer_exit_confirmed=false`，mixed 和 after 都没有提交；客户端 baseline 和 before 各成功 1 条，随后控制 EOF 104。这次只说明故障进程退出确认未在 10 秒预算内完成，不能与已经进入 mixed 的 READ 错误合并，也不能据此认定错误目标访问曾执行。

用户在上述日志后明确说明：**当前芯片存在 status 9 后 jetty 不可继续使用的缺陷；大并发 READ 容易触发，大并发 WRITE 不出现这一现象。** 这是用户提供的芯片行为知识；RF0 后续健康访问失败与其一致，但日志本身不提供芯片内部触发机制、受影响版本范围或永久恢复方式。此前正常并发 READ 的通过记录保留，不能据其排除偶发缺陷；历史 SEND/provider 的 status 9 也不因状态码相同就自动完成触发原因归属。

软件侧据此必须在任一 WR 返回 status 9 后停止该 jetty 的新提交，并在逐 WR 终结和 DMA 安全约束下排空；不能只失败一个逻辑连接后继续利用同一 SQ，也不能通过 READY 查询解除门禁。生产 `TxCompletionProcessor` 已对 status 9 走主动排空路径，status 8 的同 SQ 隔离例外不适用于 status 9。

当前 [rma_failure_test](../tools/rma_failure_test/README.md) 改为单边 WRITE。客户端提供本地源段，健康主进程与故障子进程提供独立远端目的 MR；基线写入先经各远端校验，故障子进程退出确认后再混合提交。每轮数据带阶段/轮次区别，健康目的 payload 和 guard 经服务端校验并返回 TCP ACK 后才复用，所有端点仍无 RX 投递。旧目标合法非成功完成按原始状态记录；任何目标的 status 9 都使实验停止后续提交，即使同批健康项成功且 SQ 查询 READY。WF0–WF3 尚无真机结果，不能继承本节 READ 或 SEND 的通过/失败计数。

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

本节保留同设备双 context 的历史证据；随后两个裸设备、每设备两个 EID 的实测见下一节，不能把本节六行 OBSERVED 直接计作四 context 的事件覆盖。

## 两个裸设备、每设备两个 EID：四 context 持续收发

用户补充三组完整双端日志，均启用严格拓扑检查 `--two_devices_two_eids`，使用 `window=32、messages=100001、bytes=4096`。context 按以下顺序逐路配对：

| context | device_group | 裸设备 | 服务端 eid_index | 客户端 eid_index |
| ---: | ---: | --- | ---: | ---: |
| 0 | 0 | udmac0d1e4 | 39 | 42 |
| 1 | 0 | udmac0d1e4 | 72 | 75 |
| 2 | 1 | udmac1d1e4 | 39 | 42 |
| 3 | 1 | udmac1d1e4 | 72 | 75 |

第一组没有启用 events，对应[矩阵 D1](hardware-validation-matrix.md)；第二组启用 events、`arm_delay_us=0`，是额外的 `window=32` 观测，**不等同于计划中的 D2（`window=1`）**；第三组启用 events、`arm_delay_us=1000`，对应 D3。三组中每端每个 context 都满足：

```text
accepted = retired = received = rx_posted = 100001
rx_reposts = 99969 = 100001 - 32
```

两端每组最终均 PASS，全部 payload 校验成功并关闭资源。每路初始 32 个 RX 后继续补投 99969 个，确认四路实际持续收发并复用 buffer，而非仅创建四组空闲资源；无 events 组所有事件计数为 0 符合配置。

两组 events 结果如下。方括号按 context 0、1、2、3 排列；`rearms` 是每个 context 的计数，TX/RX 两个 JFC 分别计一次。

| arm_delay_us / 端 | 每 context rearms | 各 context acks | 各 context recheck_progress | 各 context fd_wakes | 汇总 waits / events |
| --- | ---: | --- | --- | --- | --- |
| 0 / 服务端 | 2 | [2, 2, 2, 2] | [0, 0, 0, 0] | [1, 0, 0, 0] | 1 / 8 |
| 0 / 客户端 | 31670 | [7393, 7310, 7130, 7172] | [573, 515, 271, 174] | [3529, 3568, 3573, 3632] | 14302 / 29005 |
| 1000 / 服务端 | 2 | [2, 2, 2, 2] | [1, 1, 1, 1] | [0, 0, 0, 0] | 0 / 8 |
| 1000 / 客户端 | 16334 | [6265, 6261, 6261, 6257] | [2065, 2004, 2001, 2154] | [0, 3, 1, 0] | 4 / 25044 |

各 context 均观察到 TX 和 RX 通知，且 `acks=tx_events+rx_events`；服务端两组每路均是 TX/RX 各 1 个通知、ack 2 个。通知对应 JFC 的唤醒机制，不是一条消息对应一个通知，计数远小于消息数不是丢完成的证据。

源码中每轮先收通知并推进所有 context，只要任一路有进展就继续轮询；所有路都无进展时才 arm 各路 TX/RX JFC，复查后仍无进展才等待 fd。服务端 `rearms=2` 表示每路仅执行一轮 TX/RX arm，后续流量持续由轮询推进。延迟 0 组唯一一次 fd 等待只观察到 context 0，不妨碍另外三路后续直接取得通知并 ack；延迟 1000 组每路 recheck 均取得进展，整轮没有进入 fd 等待。因此这些服务端 `NOT_OBSERVED: ... fd readiness` 不表示通知失败，但不能仅凭这三组日志将相应 fd 路径记作已验证。

本次 D1 通过，D3 覆盖了所测 arm/延迟/recheck 时序，且四路在两端都有通知及 ack；客户端延迟 0 组四路 fd readiness 全部观察到。后续 D2 的首次失败和第二次通过见下一节，第二次补齐了服务端其余三路 fd readiness。具体命令见 [streaming_test README](../tools/streaming_test/README.md)。这些结果证明四 context 的共同推进与 buffer 复用，不证明多线程同时调用安全、跨设备故障隔离或所有通知竞态均已覆盖。

## 四 context D2：首次失败与复测通过

用户随后提供两次 `window=1、events=true、arm_delay_us=0` 的 D2 双端日志。第一次客户端报告 `invalid streaming TX completion: 9`，即取得 status 9（ACK_TIMEOUT）CQE；服务端报告事件等待超时，错误码 110。停止时计数如下：

| 端 / context | device_group | accepted | retired | received | rx_posted |
| --- | ---: | ---: | ---: | ---: | ---: |
| 服务端 0 | 0 | 44723 | 44723 | 44723 | 44724 |
| 服务端 1 | 0 | 43729 | 43729 | 43729 | 43730 |
| 服务端 2 / 3（各） | 1 | 100001 | 100001 | 100001 | 100001 |
| 客户端 0 | 0 | 44723 | 44722 | 44722 | 44723 |
| 客户端 1 | 0 | 43729 | 43728 | 43728 | 43729 |
| 客户端 2 / 3（各） | 1 | 100001 | 100001 | 100001 | 100001 |

这次确实发生了运行失败，现有证据的解释范围为：

- 未完成的 context 0/1 同属 `device_group=0`、裸设备 `udmac0d1e4`；另一设备的两路均完成全部流量。这是值得继续追踪的相关性，不能据此认定设备故障，也不能将一条汇总错误消息解释为两路均收到 ACK_TIMEOUT。
- 服务端已接收并校验两路累计 44723/43729 条请求，并观察到相同数量 echo TX 的 SUCCESS；客户端两路的请求 TX 成功退休与 echo 接收计数都少 1。`Advance` 先处理 TX，再处理 RX，遇到 TX 错误即退出；由错误 CQE 不能断言对应 payload 未送达，也不能由客户端 `received` 少 1 断言回包未到硬件或未在 RX CQ 中排队。
- [StreamSession::PollTx](../tools/streaming_test/streaming_test.cpp) 先检查 CQE 状态和身份，再增加 `retired`。status 9 CQE 已由 poll 取出，但未计入成功退休数；因此 `accepted-retired=1` 是软件计数差，不能直接当作仍在 DMA 的 WR 数。异常后的保守资源保留也不构成硬件仍在执行的证明。
- 日志没有记录该错误 CQE 的 context、local_id/user_ctx 或双端关键事件时间戳，无法确定哪一路首先出错、ACK_TIMEOUT 与服务端超时的先后关系，也不能定位为通知丢失。缺少事件等待末尾的 CQ 状态等证据时，不能仅根据超时把问题归因于通知路径。
- `RunStreams` 的停滞 deadline 由任一路进展重置；context 2/3 继续收发时，context 0/1 即使已停滞也不会立即触发全局超时。因此最终汇总不能还原各路停止推进或首次故障的时间。

第二次运行两端都 PASS，并各输出十行 OBSERVED。每端四个 context 均 `accepted=retired=received=rx_posted=100001、rx_reposts=100000`，payload 全部校验成功并正常关闭资源。事件计数如下，数组仍按 context 0～3 排列：

| 端 | 各 context acks | 各 context fd_wakes | 汇总 events | 汇总 waits | 汇总 fd_wakes |
| --- | --- | --- | ---: | ---: | ---: |
| 服务端 | [515, 530, 820, 1949] | [97, 62, 109, 1264] | 3814 | 1458 | 1532 |
| 客户端 | [196705, 197239, 195401, 197075] | [126226, 145042, 99483, 129905] | 786420 | 388673 | 500656 |

一次 `poll` 可以同时返回多个 context 的 fd 就绪，工具逐 fd 增加 `fd_wakes`，所以汇总 `fd_wakes > waits` 是正常计数关系。这里的 acks 是通过 `AckJfc` 确认的 JFC 通知事件数，不能当作传输协议 ACK 的数量。这次已补齐双端四路正常通知、ack 和 fd readiness 的通过证据；**D2 首次失败的原因仍未查明，第二次通过不代表已修复或排除了偶发 ACK_TIMEOUT/停滞。** 它与此前同 SQ 多目标实验的 status 9 也不能仅凭相同状态码合并为同一根因。

后续工具已补充[失败诊断](../tools/streaming_test/README.md)：错误 CQE 的 context/身份、各路最近推进时间、失败和最近等待时间，以及超时后每个 context 的一次通知与 TX/RX CQ 探查。探查只记录，不退休、补投或恢复运行；结果是失败后的分时采样，不能单凭晚到 CQE 认定原等待丢通知。上述旧日志尚不含这些字段，需要新版真机日志继续定位；本地诊断实现与单测不构成 ACK_TIMEOUT 已修复的证据。

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

该结果确认 post 成功不能判断远端存活，远端故障也不必令本地 SQ 进入 ERROR 或发出可见 async 事件。时间从客户端发送逻辑开始到软件消费 CQE，不能当成从 SIGKILL 起计算的故障检测时延。这两轮没有继续向健康远端发送，所以不能仅由其 READY 查询证明后续正常发送能力；后续组合实测见下文同 SQ status 8 隔离记录。

`inflight_exit、batch=256` 则先在远端投递 256 个 RX，客户端提交 256 条 WR，然后请求监督进程退出 endpoint。实际日志为：

```text
post_status=0 accepted=256
retired_before_exit_request=32，均为 SUCCESS
software_unretired_at_confirmation=224
retired=256，最终均为 SUCCESS
first_cqe_ns=56680 last_cqe_ns=1065049588
```

本轮本地查询前后仍为 READY，无 async 事件，客户端正常关闭且服务端完成 SIGKILL/回收。它证明提交、退出协调和完成收集流程可完成，但 **224 是软件未退休数，不是硬件在途数**：等待 TCP 退出确认期间，CQE 可能已生成并留在队列中。约 1.065 秒的最后消费时间也包含该控制等待，不能视为硬件处理时延。当前没有证据证明退出中断了仍由硬件执行的 WR，SUCCESS CQE 也不代表对端应用已消费消息。

## status 8 与同 SQ 健康目标共存

用户贴出 [sq_multiplex_test 的 peer_exit 场景](../tools/sq_multiplex_test/README.md)两组完整双端 PASS 日志。Q1/Q2 分别使用 `batch=32/128、rounds=8、bytes=4096`；服务端为 `udmac0d1e4:39`，客户端为 `udmac0d1e4:61`。两组都先确认故障 endpoint 曾正常收发，再验证 `URMA_CR_REM_ACCESS_ABORT_ERR`（status 8）是否影响同一客户端 SQ 上的健康目标。

baseline 使用后续实验的同一客户端 SQ，客户端 `accepted=retired=success=1`，故障 endpoint 收到并校验一条消息。工具先确认客户端 SUCCESS CQE 和 SQ READY，再由服务端确认故障子进程的 RX payload/IMM 校验，执行 SIGKILL 并 waitpid。双方退出握手完成后，健康 endpoint 继续存活，客户端开始每轮 healthy before/mixed/after；before 也发生在 SIGKILL 之后，不是退出前的健康流量。

| 组 / batch | 客户端 jetty ID | 每个 healthy 阶段 accepted / retired / SUCCESS | 服务端每个 healthy 阶段 received | Sink mixed accepted / retired / status 8 |
| --- | ---: | --- | ---: | --- |
| Q1 / 32 | 27048 | 256 / 256 / 256 | 256 | 256 / 256 / 256 |
| Q2 / 128 | 27049 | 1024 / 1024 / 1024 | 1024 | 1024 / 1024 / 1024 |

表中 healthy 计数分别适用于 before、mixed、after 三个阶段；baseline 单独统计，不包含在这些计数中。每轮 healthy/sink 各为 32 或 128 条，所有健康 WR 都成功，所有失效目标 WR 都以 status 8 终结，其他错误计数均为 0。两组分别使用不同 jetty ID 是重启实验后的新资源；每组内部始终使用同一个 SQ，并未在故障后换 SQ。

两组均为 `state_checks=26、last_queried_state=1、accepted_known=true、provider_eagain=0`。各查询检查点为 READY；工具没有主动 modify ERROR、flush、重建或重放。健康 payload 校验通过，每个已接受 WR 恰好退休一次，双方存活端点显式关闭，故障子进程已由 waitpid 回收。故障子进程是被 SIGKILL 退出，不是执行了正常 Close。

软件观察到的 mixed CQE 耗时如下，单位为 μs：

| batch | Healthy mixed 最小～最大 | Sink mixed 最小～最大 |
| ---: | --- | --- |
| 32 | 17.750～39.600 | 17.400～39.310 |
| 128 | 60.459～118.640 | 60.129～118.460 |

batch=128 时 healthy before 为 `32.280～63.670 μs`，after 为 `32.450～61.000 μs`。计时从批次 `TxSender::Send` 调用之前到软件 CQE 记账完成，包含 Send 内部账本准备、WR 构造、provider 调用，以及网络/硬件和 poll/退休开销；不包含调用前的 payload 填充和 `TxSendRequest` 构造。mixed 一次提交的 WR 总数为普通阶段的两倍，不能由 mixed 数值较大就认定硬件队头阻塞，也不能据这些样本证明完全没有队头阻塞。RNR 相关时间字段为 0 是本轮未出现 RNR、指标不适用，不代表零延迟。

这两组支持：在本次设备/provider、RM_CTP error-continue 场景下，已退出 endpoint 的 status 8 可以与同 SQ 上健康目标的成功完成共存，健康目标在 mixed 之后仍可使用。两个远端 endpoint 位于同一主机；结果不自动扩展到物理链路故障、ACK_TIMEOUT、所有错误类型或不同主机间的故障隔离。它也不表示失效目标可以继续提供正常通信。

上述实测时的工具版本直接消费 CQE，验证结果独立于生产策略。随后[生产 TxCompletionProcessor](../kbsocket/transport/raw/tx_completion_processor.cpp)已调整为：合法 status 8 完成退休对应 WR，并携原始状态和 `AttemptRecord` 上报，不因该状态调用 `BeginDrain`；连接与操作的失败决策仍由上层负责，完成层不自动关闭连接或重试。status 8 不仅可能由进程退出产生，不能将本次故障注入方式作为该状态的唯一定义。RNR、ACK_TIMEOUT 等其他错误仍沿用主动排空策略。

### 正式 TxCompletionProcessor 集成

随后用户提供新版 `sq_multiplex_test --fault=peer_exit` 的 Q3/Q4 双端 PASS 日志。两组仍为 `batch=32/128、rounds=8、bytes=4096`，服务端 `udmac0d1e4:39`，客户端改为 `udmac0d1e4:42`。该版本从 baseline 到最后一个 after 均使用正式 `TxCompletionProcessor`；日志明确为 `completion_path=processor`，与前述 Q1/Q2 原始完成路径分别登记。

| 指标 | Q3 / batch=32 | Q4 / batch=128 |
| --- | ---: | ---: |
| 故障子进程 PID / jetty_id | 500266 / 23434 | 501610 / 23436 |
| 客户端 SQ jetty_id | 49496 | 49497 |
| baseline accepted / retired / SUCCESS / received | 1 / 1 / 1 / 1 | 1 / 1 / 1 / 1 |
| 每个 healthy 阶段 accepted / retired / SUCCESS | 256 / 256 / 256 | 1024 / 1024 / 1024 |
| 服务端每个 healthy 阶段 received | 256 | 1024 |
| Sink mixed accepted / retired / status 8 | 256 / 256 / 256 | 1024 / 1024 / 1024 |
| processor_events / processor_retired | 1025 / 1025 | 4097 / 4097 |
| last_lane_state / last_queried_state | READY / 1 | READY / 1 |
| state_checks | 26 | 26 |
| accepted_known / provider_eagain | true / 0 | true / 0 |
| 双端结果 | PASS | PASS |

三个 healthy 阶段的其他错误计数全部为 0；sink 全部为 status 8，没有 RNR、ACK_TIMEOUT 或其他错误。每个 mixed round 都恰好退休 batch 条健康 SUCCESS 与 batch 条旧目标 status 8。故障 baseline payload 经服务端确认后才 SIGKILL/waitpid，双方 `Peer exit: confirmed=true`；存活客户端和服务端最终完成资源关闭。

生产退休总数为 `1+4*batch*rounds`：baseline 的 1 条，加上三阶段健康流量与 mixed 中的故障目标流量。总数及逐目标身份检查支持全部已接受 WR 经正式完成层恰好退休一次。mixed 每次使用 `2*batch` 软件额度，后续 after 和下一轮仍能完整发送，验证了错误与成功完成均归还额度。Q4 的 mixed 一次提交 256 条 WR，跨多个最多 64 条的完成批次，仍保持同一 SQ 可用。

这补齐了此前 status 8 不触发整 SQ `BeginDrain` 的生产集成证据：合法远端访问中止完成被逐 WR 退休并上报，软件 lane 保持 READY，硬件状态检查通过，健康目标能够继续成功发送。结论限于本轮配置与故障方式，尚不证明上层连接错误分发/回收、跨远端主机或链路隔离，也不改变生产 RNR、ACK_TIMEOUT 的主动排空策略。

Q3 的 healthy/sink mixed 软件 CQE 耗时分别为 `20.610～41.130 / 20.420～41.040 μs`；Q4 为 `75.010～151.570 / 74.730～151.470 μs`。这些值包含软件完成处理与批次消费，mixed WR 数量也是普通阶段的两倍，不能据此认定硬件队头阻塞或量化生产完成层相对旧工具的性能开销。

## JFR 水线 async 事件

用户反馈 [async_event_test](../tools/async_event_test/README.md) 的两组完整双端日志。服务端为 `udmac0d1e4:39`，客户端为 `udmac0d1e4:42`，控制地址 `141.62.33.25:8889`；共同参数为 `threshold=64、bytes=4096、observe_ms=100`。每端创建两个 jetty，共享一个 JFR；本轮由客户端向服务端发送。

| 指标 | rx_depth=128 | rx_depth=256 |
| --- | ---: | ---: |
| 服务端 jetty_id / jfr_id | 23430 / 9618 | 23432 / 9619 |
| 客户端 jetty_id / jfr_id | 49492 / 21081 | 49494 / 21082 |
| 查询 depth / threshold | 128 / 64 | 256 / 64 |
| 首次跨线消费数量 / 剩余 RX | 65 / 63 | 193 / 63 |
| first_crossing 的 retrieved / acked / jfr_limit | 1 / 1 / 1 | 1 / 1 / 1 |
| 首次事件 last_type | 3 | 3 |
| 首次事件 context / JFR 匹配 | true / true | true / true |
| repeat_crossing 的 retrieved / acked / jfr_limit | 0 / 0 / 0 | 0 / 0 / 0 |
| 服务端 posted / received | 194 / 194 | 450 / 450 |
| 客户端 accepted / retired | 194 / 194 | 450 / 450 |
| threshold_disabled / accepted_known | true / true | true / true |
| 双端结果 | PASS | PASS |

两组的 baseline、refill_control、cleanup 阶段也均为 `get_calls=retrieved=acked=jfr_limit=0`。没有取得事件时，`last_type=-1` 和归属字段 false 表示无事件可供判断，不是错误对象事件。服务端没有发送，所以 accepted/retired 为 0；客户端没有投递 RX，所以 posted/received 为 0，符合本工具的单向流量分工。

首次事件通过 context 的 `async_fd`、`GetAsyncEvent` 和 `AckAsyncEvent` 取得并确认，类型 3 对应 JFR_LIMIT，且原事件的 context/JFR 归属正确。补投后的一条正常恢复流量、第二次跨线流量和关闭前的独立 cleanup 流量均通过 payload 与完成检查。最后禁用水线并消费剩余 63 个 RX，双方完成资源关闭。因此首次水线事件、事件确认及补投恢复可登记通过。

两次跨线之间没有重新配置水线，只有 Ack 与 RX 补投；第二次窗口及后续 cleanup 均未取得更多事件。**这两组未验证水线通知会自动重复触发，不能依赖仅 Ack/补投就必有下一次通知。** 当时的证据仅与需要显式重新使能的行为相容，没有证明哪种重新配置方式有效。后续 A6 单独验证补满后重写 64 的行为，并区分配置期间与跨线窗口取得的事件，不覆盖原始“不重写阈值”的结果。

源码对照：本地 [urma_cmd.c](../../umdk/src/urma/lib/urma/core/urma_cmd.c) 对 JFR_LIMIT 的 Ack 仅累计已确认事件数并唤醒资源销毁等待者，没有重新配置硬件；[udma_jfr.c](../../kernel/drivers/ub/urma/hw/udma/udma_jfr.c) 对显式设置 `JFR_RX_THRESHOLD` 会下发修改命令，即使仍写相同阈值，也未见跳过逻辑。仅凭源码不足以证明重新写 64 就会恢复通知，后续 A6 提供了独立实测证据。查询返回的 threshold 是配置值，不单独作为下一次事件已使能的证明；前述两轮实测版本在首次配置与关闭时查询，没有在首次事件后再次查询。上述是本地源码行为，尚未核对现场版本。

后续工具已增加 `--crossings` 和 `--rearm=none|rewrite|toggle`，每轮记录配置前后及跨线后的实际 Query，单独统计补投、满水位与跨线窗口取得的事件；协议为 v2。运行方法与计数见 [async_event_test README](../tools/async_event_test/README.md)，验证矩阵按 A5–A7 单独登记。

### 重写 64 的 8 轮结果

用户提供 A6 的完整双端日志：`rx_depth=128、threshold=64、bytes=4096、observe_ms=100、crossings=8、rearm=rewrite`，watchdog 为 60000 ms。服务端 `udmac0d1e4:39`，jetty=23442、JFR=9626；客户端 `udmac0d1e4:51`，jetty=49500、JFR=21086。

| 指标 | 实测结果 |
| --- | --- |
| requested / completed / observed | 双端均为 8 / 8 / 8 |
| 每轮 before_rearm 查询 | depth=128、threshold=0 |
| 每轮 after_rearm 查询 | depth=128、threshold=64 |
| 每轮 after_crossing 查询 | depth=128、threshold=0 |
| 每轮跨线事件 | Get/retrieved/Ack/JFR_LIMIT 均为 1，type=3，context/JFR 匹配 |
| 首次 / 后续跨线累计事件 | 1 / 7 |
| baseline / refill_control / above_water / cleanup 事件 | 均为 0 |
| 服务端 posted / received | 590 / 590 |
| 客户端 accepted / retired / accepted_known | 590 / 590 / true |
| 最终 threshold_disabled / 双端结果 | true / PASS |

客户端第一轮接受并退休 65 条，随后 7 轮各 66 条（1 条恢复流量加 65 条跨线流量），最后另有 63 条 cleanup，所以总量为 `65+7*66+63=590`。服务端最初投递 128 个 RX，后续每轮补投 66 个，总量为 `128+7*66=590`；每轮日志不包含最终 cleanup，不能把各轮 received/retired 直接相加当作总量。

这组观察到稳定的 `配置64 → 跨线取得并Ack事件 → 查询0 → 补满 → 再配置64` 循环，8 次都取得匹配事件；没有把满水位/配置期间取得的事件充当跨线事件。**当前设备与软件组合下，补满后直接重写 64 足以恢复后续水线通知，对外表现与一次性触发后禁用、显式重新使能一致。** 本实验没有在轮间主动写入 0，`disabled` 的 queried=false 表示未执行该步骤；最终 cleanup 才主动禁用水线。

`rewrite` 重写的是同一个请求值 64，但每轮操作前实际回读已经为 0，因此这不是验证“当前值仍为 64 时再写 64”的效果。日志不能进一步定位清零发生于硬件、驱动还是 provider 内部，也不能给出精确清零时刻或任意时序下的事件数量保证。新版 none 的 8 轮对照、toggle 模式及 depth=256 的多轮结果仍未登记；不能因本组通过推定这些场景也通过。

水线事件也不代替 RX CQE、buffer 所有权归还或 DMA 静默证明。正常 RX 补投应由完成与可用槽位驱动，不能等到水线事件才推进。

### 持续收包时在高位或低位重配

用户随后反馈 A8/A9 的完整双端日志，服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:52`，均为 `--rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100 --crossings=8 --rearm=rewrite --traffic=concurrent --send_interval_us=100`。第一组省略 rearm_at，默认 above；第二组显式 above，属于同配置复测；第三组为 below。

| 指标 | A8：above，两次一致 | A9：below |
| --- | ---: | ---: |
| 服务端 posted / received / rx_leases_released | 583 / 583 / 583 | 807 / 807 / 807 |
| 客户端 accepted / retired | 583 / 583 | 807 / 807 |
| first_crossing 事件 | 1 | 1 |
| active_rearm 累计事件 | 0 | 7 |
| repeat_crossing 累计事件 | 7 | 0 |
| 事件总数 / Ack 总数 | 8 / 8 | 8 / 8 |
| requested / completed / observed | 8 / 8 / 8 | 8 / 8 / 1 |
| 最终 threshold_disabled / 双端结果 | true / PASS | true / PASS |

后续七轮窗口各发送 32 条，全部为配置前消费 1 个 RX CQE、配置后消费 31 个，Modify 均成功。其余 baseline/refill_control/above_water/cleanup/active_before_rearm 阶段没有事件。各取得的 JFR_LIMIT 均 type=3，context/JFR 匹配且 Get/Ack 数量一致。

above 每轮先补满到 128，最多接收 32 条后仍高于 64；重配前的软件 posted 为 127，重配后回读 threshold=64，after_active 仍为 64，active 窗口没有事件。后续再消费到余量 63 时各取得 1 个事件，after_crossing 回读 0。窗口的 NOT_OBSERVED 与未降至水线以下相符，后续七轮跨线均成功通知。

below 每轮从已经确认的余量 63 开始，首次 RX 完成后软件 posted 为 62，再重写 64；七轮均在 active_rearm 窗口取得并 Ack 1 个事件，after_rearm/after_active 已回读 0。随后补满时故意不再重写 threshold，下一次跨线没有事件、查询仍为 0。**低水位持续收包时重新使能，会在当前阶段产生通知并清零；之后仅补投不恢复下一次通知。** 本次不能区分 Modify 本身立即触发，还是紧随其后的收包触发；不能据此建立没有新收包时的即时通知契约。

A9 的 observed=1、repeat_any=false 仅统计独立 crossing 阶段；另外七个事件记录于 active_rearm。因此不是只取得了一个事件，也不是七次丢通知。两组均有重配前后软件 RX 完成进展，但保留 `hardware_overlap=UNPROVEN`：软件 CQE 消费时间不能证明 DMA 与 Modify 精确重叠，software_posted 也不是硬件瞬时余量查询。A8/A9 的行为观测、收发/lease 校验和资源关闭均已完成，depth=256 尚未追认。

## RX JFC_ERR：关联 jetty 与独立健康队列

X1 的双端日志为 `--fault=jfc_overflow --attempts=512 --cq_depth=64 --healthy_rounds=16 --observe_ms=2000`，服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:51`。工具在服务端故障 RX JFC 上暂停 poll，另有独立 JFR/JFC 的健康组继续流量；故障组两个 jetty 共用故障 RX JFC/JFR。

| 指标 | 服务端 | 客户端 |
| --- | --- | --- |
| requested_rx_cq / reported_rx_cq | 64 / 64 | 64 / 514 |
| 故障组 accepted / retired | 0 / 0 | 514 / 514 |
| 故障组 posted / received / rx_retired | 514 / 66 / 66 | 0 / 0 / 0 |
| 故障组终结状态 | SUCCESS × 66 | SUCCESS × 292、status 8 × 222 |
| jetty0/1 状态 before → after | READY/READY → ERROR/READY | READY/READY → READY/READY |
| JFR 状态 before → after | READY → READY | READY → READY |
| 健康组成功流量 | posted=received=rx_retired=18 | accepted=retired=18 |
| observation 阶段 async | JFC_ERR × 1、JETTY_ERR × 1，retrieved=acked=2 | 0 |
| 最终结果 | PASS + RETAINED | PASS + RETAINED |

指定服务端 RX JFC（ID 29164）的 JFC_ERR 为 type=0；关联 jetty0（ID 30789）的 JETTY_ERR 为 type=4，不能写成单独 JFS 对象的 JFS_ERR。jetty1 为 30790，未取得其错误事件且查询仍 READY。baseline/after/cleanup 无其他事件，健康组没有 AE。状态查询先于主动 cleanup，服务端未主动 modify ERROR；这次 ERROR 不是工具人为排空造成的。

健康组的 18 条为 baseline 1 条、窗口内 16 条、锁定故障证据后的 after 探针 1 条，payload 全部通过。由此确认同 context 下独立队列组在本次 RX JFC 故障前、中、后仍可使用；不能因故障组 jetty1/JFR 查询仍 READY，就认为依赖同一失效 JFC 的资源也可安全继续。

服务端 66 条包含两条故障 baseline，所以窗口之后实际取得并校验 64 条 RX 完成。客户端 292 条 SUCCESS 比接收端已校验的 66 条多 226；它说明 RX JFC 出错时，发送 SUCCESS 不能证明接收应用已经取得 CQE 或处理数据，不足以判定各个未取得完成的 buffer 是否发生了 DMA。客户端另外 222 条 status 8 发生在远端进程仍存活时，进一步确认该状态不能专指进程退出，更不能仅据此将本地 SQ 判为故障。

服务端 `posted-rx_retired=448` 是缺少逐 WR 终结证据的数量，可能包含未消费 RX 和无法取得完成的 RX，不是已证实丢包数。双方依工具安全条件保留全部 DMA 依赖到进程退出；没有手工删除/注销/卸载。**PASS 只确认目标 AE、Ack 与健康隔离，RETAINED 不证明正常关闭或排空成功，也不证明 DeleteJetty/DeleteJfr 必然失败。** 已通过的正常静默 JFR 删除不能代替本错误场景的回收契约。

reported_rx_cq 是 URMA 对象公开配置值，客户端没有接收故障流量而使用不同 CQ 配置；它不是该端实际可用 CQ 容量的测量。一次成功触发不保证任意 provider 下停止 poll 都必然得到同种 AE。外部 JFC/JFR 注入、TX JFC 故障以及故障资源正常回收仍需独立验证。

## 接口与查询值的解释边界

本节来自接口文档和本地源码分析。服务器实际加载的库版本、配置仍需对应确认。下列外部源码链接采用工作区中 `kbsocket`、`kernel` 并列的目录布局。

URMA 对 RNR 的公共定义是远端 JFR 无 buffer、重试次数耗尽；`error_suspend=0` 表示 error continue。[官方 API 文档](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)

本地内核 [uburma_cmd.c](../../kernel/drivers/ub/urma/uburma/uburma_cmd.c) 的 jetty query 路径将 `arg.out.jfs_flag` 固定填为 0。因此查询返回 `error_suspend=0` 不能独立证明硬件模式；工具同时检查创建配置，最终用同一 SQ 的成功续发及远端校验提供行为证据。

本地 UDMA [udma_dfx.c](../../kernel/drivers/ub/urma/hw/udma/udma_dfx.c) 从硬件 context 读取 state、`rnr_retry_num` 和 `ta_timeout`。同 SQ 多目标日志的两个查询值均为 0，不能据此认定查询未实现或参数没有配置：

- 当前 JettyPool 创建请求为 `rnr_retry=6、err_timeout=2`。[udma_jetty.h](../../kernel/drivers/ub/urma/hw/udma/udma_jetty.h) 使用 `err_timeout / 8` 转换为硬件档位，所以请求 2 对应查询档位 0。[udma_jetty.c](../../kernel/drivers/ub/urma/hw/udma/udma_jetty.c) 的档位时间表为 128/1000/8000/64000 ms。日志中的约 129.6 ms 与档位 0 的 128 ms 相符；实际服务器版本未核实，不能把这一对应关系当作已测得的硬件计时保证，也不能对查询值 0 套用 `4.096us * 2^0`。
- 同一驱动仅在设备具备私有 `UDMA_CAP_FEATURE_RNR_RETRY` 能力时写入请求的 RNR 重试值。因此查询 0 可能来自能力限制，仍需核对现场版本和能力，不能从它推算实际 RNR 重试次数或窗口。

status 9 是 ACK_TIMEOUT，status 10 才是 RNR 重试耗尽：[urma_opcode.h](../../umdk/src/urma/lib/urma/core/include/urma_opcode.h)。provider 分别映射硬件 transaction ACK timeout 与 RNR 状态，二者不能互换解释。

## 对 kbsocket 设计的约束

- 将逐 WR 结果、逻辑连接状态、物理 SQ 状态和设备/端口健康分开记录。已实测 RNR 和失效 endpoint 的 status 8 可与同 SQ 健康发送共存，不能仅由这两类单条 WR 错误推定共享 SQ 或整个裸设备失效。
- SQ 仍能发送新消息，不代表失败的字节流可以无条件继续。原请求错误必须显式交给上层处理，不能跳过缺失数据或自动重放已接受的请求。
- 继续保留软件发送门禁与逐 WR 退休规则；已实测 ERROR post 仍可成功返回。当前芯片 status 9 后 jetty 不可复用，必须停止该 jetty 新提交，即使查询 READY；生产完成层已有主动排空策略。只有明确的 ERROR 排空契约才解释 FLUSH_ERR_DONE，不能从普通 RNR 或计数相等推导队列已经停机。
- 对端退出不能代替逐 WR 终结证据，async 缺失也不能否定 CQE 已报告的远端故障；共享 JFR 删除要先满足整个资源域静默和依赖释放顺序。JFC_ERR 后应按完成队列的依赖域停止相关新操作，不能只处理报告 JETTY_ERR 的一个对象；独立健康队列的可用性应另行确认。
- 水线事件处理宜先补投恢复余量，再重新配置 threshold；低水位时配置成功也可能已触发通知并回读为 0。不能把 Ack、补投、Modify 成功或一个查询值单独当作下一次通知必然使能的保证。
- status 8 同 SQ 隔离已有 Q1/Q2 原始路径和 Q3/Q4 生产集成的通过证据。生产 `TxCompletionProcessor` 只退休并上报这类合法终结完成，不主动排空整 SQ；原始状态和 record 仍交给上层决定对应连接与操作如何失败。此调整不自动实现连接管理，也不使已经隔离的 SQ 恢复 READY；RNR/ACK_TIMEOUT 的排空策略未改，当前集成通过也不能替代上层连接状态机验证。

## 尚未覆盖与后续记录

已完成的范围见上文；剩余问题不能以工具最终 PASS 自动勾选：

- status 9 后 jetty 不可复用的芯片缺陷已由用户确认，软件规避不再等待重复定位；历史 SEND mixed、provider 边界和四 context D2 首次 status 9 的具体触发原因仍须分别判断，不能仅凭状态码认定这些实验同源，也不能用复测 PASS 宣称芯片缺陷已消失。
- 延迟 RX 后原始批次全部成功，以及 RNR 的实际重试次数和时间窗口。
- provider 同步 `URMA_EAGAIN`；已观察到的是 `ENOMEM` 容量拒绝及部分接受，尚不能证明 socket 的 EAGAIN 到 EPOLLOUT/NotifyWritable 闭环。
- ERROR probe 的逐 WR 终结状态及来源；整轮已取得非零 `WR_UNHANDLED`，但汇总日志未将它与 probe 绑定。
- 同一共享 SQ 的跨远端主机/链路及其他错误类型隔离；同一远端主机上已退出 endpoint 的 status 8 与健康流量共存已由 Q1/Q2 验证，不再列为未测，但不能外推到 ACK_TIMEOUT 等故障。
- 两设备各双 EID 的四 context 已有 D1、D3 和 D2 第二次运行的通过证据，双端各路 fd readiness 已覆盖；后续重点是 D2 首次失败的逐 context CQE/事件时序诊断，而非把原覆盖缺口继续列作未测。四个物理裸设备并行按用户部署范围暂缓。
- 真正中断硬件在途 WR 的对端退出；`inflight_exit` 已验证提交后退出的控制时序，但本次全部 SUCCESS，硬件在途未确认。
- 链路/端口真实故障的工具日志：用户说明通常为 ACK_TIMEOUT，现有 external 场景尚无日志，不把当前无法观察的 PORT_DOWN async 列作通过条件。
- JFR 水线 A6 和并发 A8/A9 已有 depth=128 记录；A9 的七个后续事件位于 active_rearm 而非独立 crossing 阶段。不重写阈值的历史第二次跨线未观察到事件，新版 none/toggle 的 8 轮及 depth=256 多轮仍待记录。X1 已取得 RX JFC_ERR 与关联 JETTY_ERR；外部 JFR 错误、独立 JFS/TX JFC 场景仍待记录，不能将现场能力说明当作实际注入通过。
- 单边 WRITE 故障隔离、WRITE 加数据面通知的可见性/lease 协议，以及大于 8 MiB 的传输。多 WR READ 和同 SQ READ/SEND 正向混合的五组通过保留；旧版 rma_failure_test 的 READ 故障 RF0–RF3 已记录失败，当前改为 WRITE 的 WF0–WF3 待真机。
- JFC 错误后完成不全时的正常资源回收；X1 为 PASS + RETAINED，没有尝试故障对象的正常删除。多 SGE 的 G2/G3/G4 已通过，G1 小消息仍无日志。
- 对端仍活跃时的共享 JFR 删除，以及 brpc worker 仍访问资源时的进程退出；已通过的是受控静默后的残留 RX 删除。

后续记录至少保留两端完整命令、工具提交号、硬件/软件版本、关键创建参数及查询值、完整计数、最终 PASS/FAIL 和退出码。新结果与本记录冲突时，应先核对配置和版本，再修订适用范围。

已测范围与待测项集中在 [硬件验证矩阵](hardware-validation-matrix.md)。工具实现与本地 mock 通过不计为硬件通过；每项按 OBSERVED/NOT_OBSERVED/SKIP/FAIL 独立登记。socket 的 EAGAIN→EPOLLOUT 与 brpc worker 退出属于后续软件集成，不是再增加一个硬件观察就能证明的契约。
