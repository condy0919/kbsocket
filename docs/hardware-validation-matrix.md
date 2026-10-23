# UB 基础硬件验证矩阵

[文档入口](README.md) · [已有实测记录](hardware-observations.md)

更新日期：2026-10-22。本矩阵区分已反馈的硬件结果、尚未观察到的行为和可选复测参数；详细计数、历史失败及源码对照放在[硬件观测记录](hardware-observations.md)中。结论仅覆盖现场使用的裸设备、RM_CTP 和对应 WR 配置：此前主要为 `udmac0d1e4`，四 context 组还使用 `udmac1d1e4`；网卡/固件/provider 完整版本尚未登记。

“已通过”表示该场景的工具判据成立，不要求故障实验中的 WR 成功发送；“部分覆盖”表示流程通过但目标硬件时序未得到证明；“未观察到”不等于硬件不支持或永远不会发生。本地 gMock 通过不计作硬件证据，计划参数也不自动继承其他参数的通过结论。

## 当前硬件覆盖

| 行为 | 状态 | 已有证据与适用范围 |
| --- | --- | --- |
| 普通 SEND、接收 buffer 回收与补投 | 已通过 | send_test 接入 RxBufferPool 后三组通过；streaming_test 进一步覆盖持续复用 |
| 多 SGE SEND_WITH_IMM | 已通过 G2/G3/G4 | 4096 字节、2/3/8 个独立注册段、1001 条，accepted=retired=received=1001；来源/IMM/payload/源段 guard 与关闭通过，G1 未反馈 |
| 共享 JFR/JFC、双向 SEND_WITH_IMM、逐 WR 选择远端 jetty | 已通过 | shared_queue_test 三组通过；双 SQ 每端 accepted/retired/received 均为 2002，IMM 与源/目标配对检查通过 |
| 单 SQ ERROR 排空及同池隔离 | 已通过 | tx_drain_test、sq_isolation_test 通过；故障 SQ 拒绝新发送，健康 SQ 和共享 RX 继续工作 |
| 缺 RX 的 RNR、同 SQ 续发与多目标收发 | 已观察到 | rx_delay 出现异步 RNR 并完成错误收尾；rnr_continue 反馈成功；同 SQ mixed 中健康目标 256 条成功、空 JFR 目标 256 条 RNR，前后各有 256 条健康消息成功；旧 ACK_TIMEOUT 原因未解 |
| status 8 与同 SQ 健康目标隔离 | 已通过 | peer_exit 的 batch=32/128、rounds=8：旧目标分别 256/1024 条 status 8；健康 before/mixed/after 各 256/1024 条 SUCCESS，远端 payload 校验通过；每轮实验同一 SQ 的 26 次状态检查通过，双方正常收尾 |
| 软件 TX 配额耗尽与恢复 | 已通过反馈 | tx_quota 通过；这是 kbsocket 软件额度，不能替代真实 provider 容量拒绝 |
| 单 WR READ/WRITE 大于 4 KiB | 已通过 | 两种操作各覆盖 4 KiB、16 KiB、1 MiB、8 MiB，每组 8 轮，无 RX 投递；这些历史组为串行单 WR，后续并发组另列 |
| 并发 READ 与同 SQ READ/SEND 混合 | 五组历史通过，受已知芯片缺陷约束 | 16 KiB/1 MiB、window=32 的纯 READ/混合四组及 16 KiB、window=128 混合组均 8 轮通过；用户随后说明大并发 READ 易触发 status 9，出现后 jetty 不可继续使用，历史通过不能排除该缺陷 |
| WRITE 成功后 SEND_WITH_IMM 通知的远端可见性 | 当前优先；WN1–WN4 工具已实现，待真机 | 等整批 WRITE 成功 CQE 后在同一 SQ 发送逐槽通知；服务端消费通知时立即校验目标 payload/guards，早于 TCP 完成确认；历史 WRITE 和 READ/SEND 结果不代替本项 |
| READ 故障后的同 SQ 行为 | 已观测失败 | RF1–RF3 mixed 中健康 READ 也出现 status 9；RF0 最小组在故障 READ status 9 后，新提交的健康 after READ 仍为 status 9，查询始终 READY |
| 单边 WRITE 故障隔离 | WF0/WF3 已通过；WF2 复测通过，首次基线失败保留 | WF2 复测每轮 256 条 mixed，旧目标共 1024 条 status 8，健康 before/mixed/after 各 1024 条成功并经远端校验，同 SQ 保持 READY，双端正常关闭；首次健康 baseline 的 status 9 原因未明，新 jetty 通过不代表原 jetty 恢复；WF1 待真机 |
| 持续发送、同设备双 EID/context | 已通过 | 每端两个 context 各完成 100001 条 4096 字节 request/echo，payload 与收发/补投计数通过 |
| JFC 通知、ack、JFCE fd 就绪及 arm/recheck | 已覆盖所测时序，存在失败记录 | 四 context 的 D2 第二次双端十行 OBSERVED，补齐各路通知/ack 与 fd 就绪；D3 有各路 recheck 进展。D2 首次客户端 status 9、服务端等待超时，原因未明，不能因复测通过认定问题已消失 |
| 共享 JFR 带残留 posted RX 删除 | 已通过 | J1–J5 均成功；先停止对端发送并删除依赖 jetty，再 DeleteJfr，成功后释放 MR |
| provider 容量拒绝与批次部分接受 | 已观察到 | P1–P3 请求深度 64/128/256，实际接受 128/256/512；均 ENOMEM、合法 bad_wr 前缀、完整退休及关闭 |
| ERROR SQ 直接 post 与非零 WR_UNHANDLED | 已观察到 | P4 查询 ERROR 后 post 返回 SUCCESS，额外 WR 最终终结；整轮软件 flush 回收 1 条 WR_UNHANDLED，未单独打印该条 WR 的阶段归属 |
| 目标进程退出后新投递 | 已通过 | process_exit 的 batch=32/256，SIGKILL/waitpid 确认后 post 仍成功，全部 CQE 为 status 8；前后查询均 READY，客户端资源关闭 |
| 进程退出相关 async 事件 | 未观察到 | 上述故障 TX 轮询期间及其后默认 1 秒无事件；async fd 可用不代表必定产生通知 |
| JFR 水线 async 事件与再次跨线 | 首次事件与 rewrite 多轮已通过 | depth=128/256 的首次事件已通过，不重配的第二次未观察到；A6 在 depth=128 下重写 64，8 轮各 Get/Ack 1 个匹配事件，跨线后阈值均回读为 0，590 条收发及双端关闭通过 |
| 持续收包时重配 JFR 水线 | A8/A9 行为已观测 | above 两次各取得 8 个跨线事件、583 条流量；below 共 8 个事件，首次跨线 1 个、active_rearm 7 个，随后补满不重写无新跨线通知，807 条流量；硬件精确重叠未证明 |
| RX JFC_ERR、关联 jetty 与健康队列 | X1 观测通过，资源保留 | 指定 RX JFC_ERR 与 jetty0 JETTY_ERR 各取得并 Ack 1 个；jetty0 ERROR、jetty1/JFR READY，独立健康 18 条成功；双端 PASS + RETAINED，未验证故障资源正常关闭 |
| TX JFC_ERR、关联 jetty 与健康队列 | X4 观测通过，资源保留 | 客户端指定 TX JFC_ERR（type 0）与 jetty0 JETTY_ERR（type 4）各 Get/Ack 1 个；故障 TX accepted=514、retired=66，服务端 RX=314；独立健康 18 条全部成功，双端 PASS + RETAINED，故障资源正常关闭未验证 |
| status 8 经生产完成层后的隔离 | 已通过 | Q3/Q4 使用 TxCompletionProcessor，processor_events=processor_retired=1025/4097，软件 lane 与硬件 SQ 均 READY；旧目标 256/1024 条 status 8，健康三阶段各 256/1024 条 SUCCESS，双方正常收尾 |
| 提交后退出、硬件在途窗口 | 部分覆盖 | inflight_exit 的 256 条全部 SUCCESS；退出请求前已退休 32，确认时软件未退休 224，不能证明这 224 条仍由硬件处理 |
| 两个裸设备、每设备两个 EID，共四个 context | 各路径已有通过证据，首次 D2 失败待查 | window=32 的 D1/D2a/D3 均通过；window=1 的 D2 第二次双端通过，各 context 四项流量计数 100001、补投 100000，通知与 fd 全覆盖；首次 D2 在设备组 0 停滞并失败 |
| 四个物理裸设备同时工作 | 暂缓 | 用户明确暂不测试；与四个 context 区分，不作为当前部署验证要求 |
| 真实链路/端口故障及 external 对照 | 低优先级，可选补测 | 故障构造成本较高，按用户要求降低优先级；用户说明此类故障 WR 通常以 ACK_TIMEOUT 终结，尚无 F5/F6 工具日志，不要求出现 PORT_DOWN async |

以下保留运行方法，并在各组旁登记实际覆盖。status 9 后 jetty 不可复用已由用户明确为芯片缺陷；历史各次 ACK_TIMEOUT 的触发原因仍须分别判断。

## 现场能力说明与当前优先级

用户补充了当前设备/provider 的现场能力边界，以下属于**用户提供的现场能力说明**，不是本轮工具取得的新事件日志：

- 当前芯片存在已知缺陷：jetty 一旦出现 status 9（ACK_TIMEOUT），后续就不可继续使用，即使查询仍为 READY。软件必须停止该 jetty 的新提交并按故障路径排空，不能仅失败一个 WR 后继续复用。用户说明大并发 READ 容易触发，而大并发 WRITE 不出现这一现象；该说明与下文 WF0/WF3 和 WF2 复测的实际通过日志分开记录，不推导任意 WRITE 都不会出现 status 9。
- 真实链路/端口故障后，WR 通常以 ACK_TIMEOUT 终结；这不解释历史同 SQ SEND mixed 或 provider 边界实验中 status 9 的具体触发原因。
- URMA async event 目前仅部分支持；当前环境明确无法观察到 PORT_DOWN async，不再将其列为故障测试的待补通过条件。
- 已确认支持 JFS error、JFR error、JFC error 及 JFR 水线相关事件。其中当前 JFS/相关 jetty 的错误 AE 需先触发其绑定的 JFC error，再联动产生，不能按独立 JFS 故障注入理解。

同 SQ `peer_exit` 的 Q1/Q2，以及两设备四 context 的 D1–D3 都已有双端通过记录；D2 在首次失败后第二次通过，补齐各路 fd 就绪。用户随后反馈 streaming 错误暂未复现，保留原始失败与诊断，不要求持续追测到再次失败，也不据此认定根因已修复。

JFR 水线的 depth=128/256 两组已有首次事件通过日志；后续 A6 又验证 depth=128 下直接重写 64 可恢复通知，8 轮全部观察到事件，且每次跨线后阈值均回读为 0。不重配的历史第二次跨线仍保留 NOT_OBSERVED。新版 `sq_multiplex_test --fault=peer_exit` 的正式 `TxCompletionProcessor` 集成也已由 Q3/Q4 双端日志验证，与早期原始 CQE 路径的 Q1/Q2 分开登记。后续 A8/A9 已完成高/低位持续收包重配观测；X1 RX JFC 与 X4 TX JFC 均已取得指定 JFC_ERR 和关联 JETTY_ERR，独立健康组成功，但双方资源处置均为 RETAINED。按用户最新优先级，故障 CQ 处理及 CQE 不全时的 DMA 终结/资源回收暂缓，保留缺口；当前优先验证 WRITE 成功后经 SEND_WITH_IMM 通知的远端可见性，WN1–WN4 工具已实现、待真机；X2/X3 外部 JFC/JFR 错误待具备现场可靠注入工具后再测。多 SGE 及并发 READ/混合 SEND 正向组已有日志；READ 故障实验已出现 status 9 后同 SQ 健康 READ 失败，结合用户确认的芯片缺陷保留为失败记录。改用单边 WRITE 后，WF0/WF3 及 WF2 复测已验证进程退出产生的 status 8 不妨碍同 SQ 健康目标写入，WF2 每轮 256 条 mixed 的覆盖已补齐；其首次健康 baseline WRITE 的 status 9 原因仍未明，不能由新 jetty 上的复测通过推导原 jetty 恢复或芯片缺陷修复；WF1 尚未反馈。水线其他深度/模式按需补测。真实链路/端口故障构造成本较高，按用户要求降为低优先级可选项；不要求任意故障都产生 async，也不以没有 PORT_DOWN 否定错误 CQE。现有进程退出实验的 async=0 记录保持不变。

## 统一准备

两端使用同一版本二进制，分别选择自己的设备/EID。下面示例使用服务端 `udmac0d1e4:38`、客户端 `udmac0d1e4:60` 和控制 IP `141.62.33.25`，实际运行前应替换；实测使用的 EID 以各轮日志为准。先启动服务端，看到 ready 再启动客户端；每组重新启动两个进程。

```bash
USE_BAZEL_VERSION=9.3.0 bazel build --lockfile_mode=off \
  //tools/async_error_test:async_error_test \
  //tools/send_test:send_test \
  //tools/async_event_test:async_event_test \
  //tools/jfr_lifecycle_test:jfr_lifecycle_test \
  //tools/provider_boundary_test:provider_boundary_test \
  //tools/peer_failure_test:peer_failure_test \
  //tools/sq_multiplex_test:sq_multiplex_test \
  //tools/streaming_test:streaming_test \
  //tools/rma_test:rma_test \
  //tools/rma_failure_test:rma_failure_test

USE_BAZEL_VERSION=9.3.0 bazel test --lockfile_mode=off \
  //tools/async_error_test:all \
  //tools/send_test:all \
  //tools/async_event_test:all \
  //tools/jfr_lifecycle_test:session_test \
  //tools/provider_boundary_test:provider_boundary_test_test \
  //tools/peer_failure_test:peer_failure_unit_test \
  //tools/sq_multiplex_test:fault_process_test \
  //tools/sq_multiplex_test:protocol_test \
  //tools/sq_multiplex_test:session_test \
  //tools/streaming_test:streaming_test_test \
  //tools/rma_test:protocol_test \
  //tools/rma_test:session_test \
  //tools/rma_failure_test:all --test_output=errors
```

已有正常收发、JFR 关闭与首次水线事件、provider 边界、进程退出、同 SQ status 8 隔离（含生产完成层集成）和两设备四 context 持续收发记录；后续按下文剩余缺口安排，不要求先重跑已通过的全部组。D2 暂未复现，常规复测保留诊断即可。物理故障为低优先级可选项，执行时放在普通测试之后。测试期间记录完整 stdout/stderr 和退出码，不只摘录 PASS。

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

两端同步替换参数。S/E 单 context 基线保留为可选复测；已有通过证据包括 C2–C4 和下述四 context 组，不要求为了补齐表格而重跑所有基线。

| 组 | window | messages | 附加参数 / 拓扑 | 当前结果 |
| --- | ---: | ---: | --- | --- |
| S1/S2/S3 | 1/32/256 | 1001/100001/100001 | 单 context，无 events | 可选复测，未逐组登记该精确参数的日志 |
| E1/E2 | 1/32 | 1001/100001 | 单 context，`--events` | 可选复测；不与双 context 的 C4 混记 |
| E3 | 32 | 10001 | 单 context，`--events --arm_delay_us=100` | 可选复测；实际延迟 recheck 的证据见 C3 |
| C1 | 32 | 100001 | 同一裸设备的两个不同 EID，无 events | 未单独登记无 events 的双端日志 |
| C2 | 32 | 100001 | 同设备双 EID，`--events --arm_delay_us=0` | 已贴客户端 PASS，两个 context 均有通知/ack 与 fd 就绪；该次服务端完整计数未登记 |
| C3 | 32 | 100001 | 同设备双 EID，`--events --arm_delay_us=1000` | 双端 PASS；每个 context 均有 recheck 进展及通知/ack，服务端有 fd 就绪、客户端未进入 fd 等待 |
| C4 | 1 | 未完整记录 | 同设备双 EID，`--events --arm_delay_us=0` | 用户确认两端各 6 行 OBSERVED，补齐双方逐 context 的通知与 fd 就绪观测；不补写计数 |
| D1 | 32 | 100001 | 两个物理裸设备，每设备两个 EID；`--two_devices_two_eids`，无 events | 双端通过；各 context 四项流量计数均 100001，RX 补投 99969 次，payload 与关闭通过 |
| D2 | 1 | 100001 | D1 拓扑，`--events --arm_delay_us=0` | 首次失败：客户端 status 9，服务端 fd 等待超时；第二次双端通过，各 context 四项流量计数 100001、补投 100000，每端十行 OBSERVED；首次原因未明 |
| D2a | 32 | 100001 | D1 拓扑，`--events --arm_delay_us=0`，本次额外观测 | 双端流量通过，四 context 均取得通知并 ack；客户端全部 fd 可见，服务端仅 context 0 的 fd 可见 |
| D3 | 32 | 100001 | D1 拓扑，`--events --arm_delay_us=1000` | 双端通过，四 context 均取得通知/ack 与 recheck 进展；服务端每路 recheck=1，不要求 fd 就绪 |
| M1/M2 | 32 | 100001 | 四个物理裸设备；M2 加 `--events` | 暂缓 |

`--devices` 接受 1–4 个逗号分隔的 `device:eid_index` 端点，同一设备名可重复，但设备名与 EID 索引的组合不能重复。两端列表长度必须一致，并按位置配对；各端的实际设备名和 EID 索引可以不同。例如 C1 可使用：

```bash
# 服务端：两个 EID 均属于同一裸设备；请替换为本机实际可用索引。
bazel-bin/tools/streaming_test/streaming_test \
  --server --address=0.0.0.0 --port=8894 \
  --devices=udmac0d1e4:11,udmac0d1e4:14 \
  --window=32 --messages=100001 --bytes=4096

# 客户端：同样选择本机的两个有效 EID，与服务端列表按位置配对。
bazel-bin/tools/streaming_test/streaming_test \
  --address=141.62.33.25 --port=8894 \
  --devices=udmac0d1e4:25,udmac0d1e4:26 \
  --window=32 --messages=100001 --bytes=4096
```

复测或补测 D 组时，使用 [streaming_test 的两设备四 context 命令](../tools/streaming_test/README.md)，两端都指定 `--two_devices_two_eids` 和 `--bytes=4096`。每端应列出两个不同物理裸设备名，并为每个设备选择两个不同 EID，按位置与对端配对；工具检查恰好两个设备名、每个各两个不同 EID，现场仍须确认所选名称确实对应两个物理裸设备。日志以 `Context N device:eid_index` 标明原始配置顺序，每端、每 context 应有 `accepted=retired=received=rx_posted=100001`，四 context 合计为每端 400004 条 request/echo 记录；不能用汇总计数替代每 context 检查。

本次 D1/D2a/D3 的端点为服务端 `udmac0d1e4:39,udmac0d1e4:72,udmac1d1e4:39,udmac1d1e4:72`，客户端 `udmac0d1e4:42,udmac0d1e4:75,udmac1d1e4:42,udmac1d1e4:75`，按 `A0,A1,B0,B1` 配对。三组均为 window=32，每端各路 `rx_reposts=99969=100001-32`；payload 全部校验通过，双方正常关闭。D2a 服务端的 `fd_wakes=[1,0,0,0]`，客户端为 `[3529,3568,3573,3632]`。D3 服务端各路 `recheck_progress=1`，客户端为 `[2065,2004,2001,2154]`；服务端 waits=0，客户端 waits=4。全部 context 均满足 `acks=tx_events+rx_events`。详细事件计数及其含义见[硬件观测记录](hardware-observations.md)。

随后相同设备/EID 的 D2 连跑两次：首次 context 0/1（均属 `udmac0d1e4`）未完成，context 2/3 在两端都已完成 100001 条。客户端前两路 `accepted/retired/received` 分别为 `44723/44722/44722`、`43729/43728/43728`；服务端这两路的接收及 echo 接受/成功退休均为 `44723`、`43729`，且各保留一个 posted RX。客户端报 `invalid streaming TX completion: 9`（ACK_TIMEOUT），服务端报 fd 等待超时 110；两端均保留未决资源到退出。工具只对通过验证的 SUCCESS CQE 增加 retired，错误 CQE 可能已被取出而未计数；日志未记录错误 CQE 所属 context、WR 身份及两端时间线，不能将差额直接解释为硬件在途，也不能据此判定两个 context 都发生 ACK_TIMEOUT、设备整体损坏或通知丢失。

D2 第二次双端 PASS，每路 `accepted=retired=received=rx_posted=100001、rx_reposts=100000`。服务端 `fd_wakes=[97,62,109,1264]、events=3814、waits=1458`，客户端 `fd_wakes=[126226,145042,99483,129905]、events=786420、waits=388673`；双方每路都有 TX/RX 通知及对应 ack。一次 poll 可以同时报告多个 fd，因此 fd_wakes 总和分别为 1532、500656，大于 waits 合理。这轮补齐四路 fd 观测，但不解释或修复首次失败；成功运行中的 `empty_wakes=0` 也不能证明首次没有通知问题。

事件覆盖分开记录：`OBSERVED: context N JFC notifications` 表示取得并 ack 通知（`acks>0`）；`OBSERVED: context N JFCE fd readiness` 表示 fd 在 `poll` 中返回过 `POLLIN`（`fd_wakes>0`），不证明线程实际睡眠过。总计也分别报告这两项；`waits>0` 不能代替 fd 就绪证据，总计 OBSERVED 不能代替逐 context 的结果。

E1/E2/C2/C4/D2/M2 若要登记完整 fd 通知路径，应在每端、每 context 同时观察到上述两项；否则只记录流量检查通过和实际覆盖的部分。服务端持续 echo 时可能一直有进展，不进入 fd 等待分支；`window=1` 或互换两端角色可以增加机会，但不保证覆盖。

E3/C3/D3 重点覆盖 arm/recheck：检查每个 context 的 `recheck_progress>0`。`arm_delay_us` 延迟 rearm 到 recheck，可能使 CQE 已被 recheck 消费，无须等待 fd；已有通知仍由循环中的 `WaitJfc(timeout=0)` 消费并 ack。因此不要求 `fd_wakes>0`。C3 的双端日志已观察到这一路径，客户端 `waits=0、fd_wakes=0` 与流量通过并不矛盾；单次运行不证明所有竞态均已验证。

两设备四 context 的 D1/D3 已有独立真机证据，D2 第二次也取得每端十行 OBSERVED，完成四路 fd 观测；当前保留首次失败待查。D3 不要求 fd 就绪，也不把 D2 的 fd 结果代替 D3 的 recheck 结果。当前四路实验验证单 owner 推进、各路身份和 buffer 复用，不验证多 owner 并行性能或跨设备故障隔离。M1/M2 的四物理设备验证按用户要求暂缓。

READ/WRITE 按 [rma_test README](../tools/rma_test/README.md) 分别运行 `--operation=read` 和 `--operation=write`，每种操作 `--bytes=4096/16384/1048576/8388608 --rounds=8`。这里是**一条 WR 的实际长度**，不是 SEND 拆成若干 4 KiB。能力不足的组记录 SKIP；实际 post、CQE 或数据错误仍为 FAIL。READ 源 MR 在远端确认完成前不复用，WRITE 在发送端完成后由远端校验可见数据。

| 每 WR 字节数 | READ | WRITE |
| ---: | --- | --- |
| 4096 | 已通过，8 轮 | 已通过，8 轮 |
| 16384 | 已通过，8 轮 | 已通过，8 轮 |
| 1048576 | 已通过，8 轮 | 已通过，8 轮 |
| 8388608 | 已通过，8 轮 | 已通过，8 轮 |

各组客户端 `accepted=retired=verified=8、pending=false`，post/CQE 状态均成功，payload 与 guard 校验通过。双方没有投递 RX；这是串行单 WR 正向路径。设备上报 READ/WRITE 上限 256 MiB，本次只验证到 8 MiB；WRITE 的远端校验发生在 TX 完成后的 TCP 通知之后，不能代替正式 UB 数据面通知的可见性验证。曾出现的 `8188608/8388608` 两端参数不一致是控制协议拒绝，不能记作 RMA 硬件错误。

## 当前优先：WRITE 后的数据面通知可见性

使用 [rma_test 的 WRITE 通知模式](../tools/rma_test/README.md)，双端增加 `--operation=write --write_notify`，并同步采用下表参数。四组均为**工具已实现，待真机反馈**；不继承串行 WRITE、WRITE 故障隔离或 READ/SEND 混合的历史通过结论。

| 组 | 双端参数 | 当前结果 |
| --- | --- | --- |
| WN1 | `--bytes=16384 --window=1 --rounds=8` | 待真机；单槽通知与跨轮复用 |
| WN2 | `--bytes=16384 --window=32 --rounds=8` | 待真机；32 个独立目的槽 |
| WN3 | `--bytes=16384 --window=128 --rounds=8` | 待真机；跨多个 CQ 消费批次 |
| WN4 | `--bytes=1048576 --window=32 --rounds=8` | 待真机；每轮 32 MiB 目的数据 |

每轮先准备目的区 poison 和通知 RX，然后提交一批 WRITE。客户端取得该批全部成功 CQE 后，才在同一 SQ 向同一目标提交逐槽 64 字节 SEND_WITH_IMM；服务端按通知的完整轮次/槽身份和远端身份，在处理通知 CQE 时立即检查对应目的 payload 与 guards，不能等 TCP `completed` 后才检查。目的 payload 与通知头均包含完整轮次、槽和长度，避免把历史内容误判为本轮发布；随后 TCP 完成/验证确认只用于授权复用和关闭。部分接受、未知接受、错误 CQE、错误通知身份或数据不符均使实验失败，不重放、不发布未成功 WRITE 的通知。

令 `N=rounds*window`，成功记录应包含客户端 `accepted=retired=2*N、send_accepted=send_retired=N`，服务端 `send_received=visibility_verified=verified=rx_leases_released=N`，且双方正常关闭资源。客户端 `verified` 校验的是本地源数据，不能替代服务端 `visibility_verified`。日志须保留通知验证发生在 TCP 完成确认前的模式说明；计时是软件 CQE 消费时间，不是硬件精确可见时间。

本项只验证“先等 WRITE 全部成功，再通知”的发布方式，不覆盖 WRITE/SEND 链式无等待提交、跨 SQ 顺序、WRITE_WITH_IMM、故障 CQ 回收或生产 WRITE 路径集成。通用 RMA 描述符协议已升级 v3，双端应同时更新二进制。

## JFR 水线异步事件

按 [async_event_test README](../tools/async_event_test/README.md) 运行双端测试，两端选择自己的裸设备/EID。该工具监听 `ctx->async_fd`，与 streaming 的 JFCE 完成通知分开验证。

| 组 | 两端共同参数 | 判定与当前覆盖 |
| --- | --- | --- |
| A1 | `--rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100` | 已通过：查询 depth/threshold=128/64；首次 JFR_LIMIT 取得并 Ack 1 次，context/JFR 匹配；服务端 posted=received=194，客户端 accepted=retired=194，双端 PASS |
| A2 | 同 A1，工具内第二次跨线 | NOT_OBSERVED：补投到 128 后再消费到 63，不重新配置水线，本次未取得重复事件 |
| A3 | `--rx_depth=256 --threshold=64 --bytes=4096 --observe_ms=100` | 已通过：查询 depth/threshold=256/64；首次 JFR_LIMIT 取得并 Ack 1 次，context/JFR 匹配；服务端 posted=received=450，客户端 accepted=retired=450，双端 PASS |
| A4 | 同 A3，工具内第二次跨线 | NOT_OBSERVED：补投到 256 后再消费到 63，不重新配置水线，本次未取得重复事件 |

两组端点均为服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:42`。首次事件 `last_type=3`，baseline/refill_control/repeat_crossing/cleanup 均未取得事件；最终 `threshold_disabled=true`。首次事件路径与补投恢复已验证，重复跨线的 NOT_OBSERVED 不使整组失败，也不能推定事件永久不会再次触发或 Ack 自动 rearm。后续显式重写的结果见 A6，与这两组不重配记录区分，详见[硬件观测记录](hardware-observations.md)。

多轮对照最初使用协议 v2；当前新增并发窗口后升级为 v3，两端需同步更新。默认仍为分阶段的 `--traffic=staged`。以下各组两端共同指定 `--rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100 --crossings=8`，具体命令见工具 README：

| 组 | rearm | 判定与当前覆盖 |
| --- | --- | --- |
| A5 | `none` | 待真机：只补投、不重配的 8 轮对照；不将历史两轮结果外推至所有轮次 |
| A6 | `rewrite` | 已通过：requested=completed=observed=8；每轮 before_rearm=0、after_rearm=64、after_crossing=0，各 Get/Ack 1 个匹配 JFR_LIMIT；服务端 posted=received=590，客户端 accepted=retired=590，双端 PASS |
| A7 | `toggle` | 待真机：每次后续跨线前补满，再执行 0→64 并分别查询；区分配置期间与跨线窗口取得的事件 |

三组各应完成 590 条 SEND，服务端 posted=received=590，客户端 accepted=retired=590。每轮独立报告 OBSERVED/NOT_OBSERVED，首次事件必须出现；后续缺失仍继续观测，不自动切换策略。只有 `requested=completed=observed=8` 才能登记该组所有跨线窗口都取得了事件，单独 PASS 或 repeat_any=true 不足以证明这一点。可将 depth 改为 256 补测，此时总量为 1614，不能继承 depth=128 的结果。

A6 的服务端为 `udmac0d1e4:39`（jetty=23442，JFR=9626），客户端为 `udmac0d1e4:51`（jetty=49500，JFR=21086）。首次/后续跨线累计取得并 Ack 1/7 个事件，baseline/refill_control/above_water/cleanup 全部为 0；最终水线禁用、双方关闭。直接重写 64 足以恢复本组后续通知，没有在轮间显式写 0；该结果与触发后禁用、重新配置恢复的行为一致，但日志不能定位清零发生于硬件还是驱动/provider 内部。A5、A7 和 depth=256 的多轮结果仍未登记。

工具记录请求与查询的水线/深度、各阶段事件和流量计数。原始类型或对象归属异常、首次事件缺失、CQE/payload 错误及关闭失败均不能记为通过。结束时禁用水线，以独立收尾流量消费残留 RX，确认数据与控制面静默后释放资源；Ack async 本身不是 WR 终结或 DMA 静默证明。进程监督覆盖可能阻塞的 provider 调用。

此工具只观察水线；JFC/JFR 错误使用下文独立的 async_error_test。当前本地 UDMA 源码的水线档位为 0/64/512/4096；工具回读配置，不能假设任意小阈值都有效，也不能把本地源码直接视为服务器实际版本。

## 多 SGE、并发 RMA、水线重配与错误事件

以下组独立登记实际参数；完整双端命令见各工具 README。G2/G3/G4、R11–R15 和 X1 使用服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:51`；A8/A9 和 X4 使用服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:52`。尚未反馈的参数不继承其他组的 PASS。

| 组 | 工具与两端共同参数 | 要观察的行为 / 当前结果 |
| --- | --- | --- |
| G1 | [send_test](../tools/send_test/README.md) `--bytes=64 --sges=2 --header_bytes=32 --send_imm --batch=64 --messages=1001` | 两个独立注册段的小包；待真机，可按需补齐 |
| G2 | 同上，`--bytes=4096 --sges=2 --header_bytes=32 --send_imm --batch=64 --messages=1001` | 双端通过；accepted=retired=received=1001，guarded_segments=2002，header/payload 无预拼接 |
| G3/G4 | 同上，`--bytes=4096 --sges=3 --header_bytes=33 --batch=128` / `--sges=8 --header_bytes=32 --batch=256`，均 `--messages=1001 --send_imm` | 均双端通过；各 1001 条发送/退休/接收，guarded_segments=3003/8008；非整齐分段与更多 SGE 校验通过 |
| A8 | [async_event_test](../tools/async_event_test/README.md) `--rx_depth=128 --threshold=64 --bytes=4096 --observe_ms=100 --crossings=8 --rearm=rewrite --traffic=concurrent --rearm_at=above --send_interval_us=100` | 两次双端通过，一次使用默认 above；各 583 条收发/lease 归还，8 个 crossing 事件，active 窗口无事件，after_active=64、after_crossing=0 |
| A9 | 同 A8，改 `--rearm_at=below` | 双端通过；807 条收发/lease 归还，首次 crossing 1 个事件、active_rearm 7 个事件；after_rearm=after_active=0，补满不重写后的 crossing 无事件，observed=1 仅统计 crossing |
| R9 | [rma_test](../tools/rma_test/README.md) `--operation=read --bytes=1048576 --window=8 --rounds=8` | 原建议参数未执行；可选补测，不由 window=32 的结果追认 |
| R10 | 同 R9，加 `--mixed_send` | 原建议参数未执行；可选补测，64 条 READ 加 64 条 SEND |
| R11 | `--operation=read --bytes=16384 --window=128 --rounds=8 --mixed_send` | 双端通过；1024 条 READ 校验、1024 条 SEND 发送/退休/远端接收，客户端总 accepted=retired=2048，单批 256 条混合 WR |
| R12 | `--operation=read --bytes=16384 --window=32 --rounds=8` | 双端通过；accepted=retired=verified=256 |
| R13 | 同 R12，加 `--mixed_send` | 双端通过；256 条 READ 校验及 256 条 SEND，客户端总 accepted=retired=512 |
| R14 | `--operation=read --bytes=1048576 --window=32 --rounds=8 --mixed_send` | 双端通过；256 条 READ 校验及 256 条 SEND，客户端总 accepted=retired=512 |
| R15 | 同 R14，去掉 `--mixed_send` | 双端通过；accepted=retired=verified=256 |
| X1 | [async_error_test](../tools/async_error_test/README.md) `--fault=jfc_overflow --attempts=512 --cq_depth=64 --healthy_rounds=16 --observe_ms=2000` | 指定 RX JFC_ERR、关联 jetty0 JETTY_ERR 各 Get/Ack 1 个；独立健康 18 条成功，双端 PASS + RETAINED，资源关闭未验证 |
| X2/X3 | 同上，改 `--fault=external_jfc` / `--fault=external_jfr`，`--observe_ms=30000` | 现场可靠注入作用于服务端指定对象；窗口内仍有流量及独立健康对照；待具备注入工具后再测 |
| X4 | 同上，`--fault=tx_jfc_overflow --attempts=512 --cq_depth=64 --healthy_rounds=16 --observe_ms=2000` | 客户端指定 TX JFC_ERR（type 0）、jetty0 JETTY_ERR（type 4）各 Get/Ack 1 个；accepted=514、retired=66，服务端 RX=314；健康 18 条双端成功，PASS + RETAINED，资源关闭未验证 |

G2/G3/G4 均为 4096 字节总 wire 长度，其中包含 32/33 字节 header；不是 4096 字节业务 payload 再额外加 header。设备报告 max_msg_size=65536、max_jfs_sge=13，仅表示能力报告，已测的是 4096 字节和 2/3/8 SGE。

A8 的七个后续 active 窗口各在高位接收 32 条，没有跨越 64，配置保持到后续 crossing 才触发并回读 0。A9 在已确认余量 63 的基础上，首次接收后软件 posted=62，再重写 64；七个窗口各取得事件且阈值回读 0。之后补满不再重写，所以 crossing 的 NOT_OBSERVED 与先前已触发禁用相符。A9 实际取得并 Ack 的事件总数为 8，汇总 observed=1 不包含 active_rearm，不能解读为七次丢事件。日志不能区分低位 Modify 本身触发还是后续收包触发。

A8/A9 各后续轮均配置前消费 1 个 RX CQE、配置后消费 31 个，Modify 成功，所有 RX lease 归还；仍保留 `hardware_overlap=UNPROVEN`，不能由软件消费时间推断 DMA 与配置精确重叠。SEND 协议 v2、水线协议 v3、RMA 协议 v2 均需双端更新。深度 256 的 A8/A9 总量分别为 1607/1831，目前未运行。

X1 服务端故障组 posted=514、received=rx_retired=66，客户端 accepted=retired=514，其中 SUCCESS=292、status8=222。服务端 jetty0 从 READY 变为 ERROR，jetty1/JFR 保持 READY；客户端两条 SQ 均 READY，无 AE。status 8 出现在远端进程仍存活时，不能专指进程退出。服务端 448 个 RX 缺少逐 WR 终结证据，不能计作丢包，也不能由客户端 SUCCESS 推定接收应用已取得 CQE。

X1 的 PASS 只证明预定 AE 与独立健康隔离，RETAINED 表示双方未手工删除/注销/卸载，依赖保留至进程退出，不证明排空或关闭成功。后续应单独验证故障对象正常回收；现有正常静默的 JFR 删除不能替代错误 CQ 场景。X2/X3 仍需现场已经确认的注入方式，工具不伪造事件或提供未经验证的私有驱动指令；预定错误未出现时退出 77。JFC_ERR 区分故障 RX/TX 对象，不能用非预定对象或主动 cleanup 阶段的事件代替注入证据。

X4 已由服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:52` 的双端日志验证。客户端故障 TX/RX CQ 的 requested/reported 均为 64/514，服务端均为 516/514；reported 是 URMA 公共配置回读，不是独立测得的硬件容量。窗口内客户端故障 CQ poll 次数为 0，服务端为 2565186，符合只暂停发送完成消费、接收侧持续推进的实验设置。客户端 observation 取得并 Ack 指定 TX JFC 60365 的 JFC_ERR（type 0）和 jetty0 25229 的 JETTY_ERR（type 4）各 1 个；baseline/after/cleanup 无其他 AE。服务端 AE 为 0，其 OBSERVED 来自控制通道交换的客户端结果，不表示服务端也取得了事件。

客户端故障组 accepted=514、retired=66，已取得的 66 条终结均为 SUCCESS；其中包含 2 条基线，不能将 66 当作故障 TX CQ 容量。服务端 posted=514、received=rx_retired=314，均完成唯一身份、payload 与 guard 校验。客户端仍有 448 条已接受 WR 缺少本地终结证据；远端成功接收数比本地退休数多 248，不能把全部未退休 WR 认定为发送失败，也不能用远端接收替代本地 DMA 终结。服务端另有 200 个已投 RX 槽缺少终结证据，不等于已证明对应消息从未到达硬件。两端独立健康组均完成前 1、窗口内 16、后 1 条 SUCCESS，未取得健康对象 AE。

主动 cleanup 前的查询显示客户端 jetty0/jetty1 从 READY/READY 变为 ERROR/READY，JFR 保持 READY；服务端两条 jetty 和 JFR 均保持 READY。jetty1 与 jetty0 共享故障 TX JFC，其 READY 和未取得 AE 不能证明可继续使用，故障 CQ 的依赖域仍需停止新提交。该状态快照早于主动 ERROR：客户端恢复 TX CQ 消费后仍有未退休 WR，随后两条 ModifyJetty(ERROR) 返回 0；未再次查询最终状态。flush_done0/1 均为 0，未取得排空边界，不能把默认的 flush_status=0 解释为实际 FlushJetty 成功。双方最终 PASS + RETAINED 仅确认目标错误观测和独立健康队列隔离，不确认故障资源正常回收；依赖保留至进程退出。其他参数仍需各自观测，两端须使用支持 AEE2 协议的同版本程序。

R11–R15 使用逐 WR signal、独立区间和 guard；全部 pending=false、accepted_known=true、partial_batches=0，payload/guard 与正常关闭通过。READ 完成和双方轮间确认后才复用内存；混合 SEND 不作为前序 READ 完成或 WRITE 发布的证明。软件计时不能据此得出没有性能干扰。并发 WRITE、RMA 故障、权限撤销、WRITE 数据面发布和 selective signaling 均未由这些正向结果覆盖。

## RMA 故障：历史 READ 结果与当前 WRITE 验证

旧版 [rma_failure_test](../tools/rma_failure_test/README.md) 固定执行 READ。服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:52` 的以下日志均为失败记录，不再记作待真机，也不能改记为 WRITE 结果：

| 组 | bytes / batch / rounds | READ 观测结果 |
| --- | --- | --- |
| RF0 | 16384 / 1 / 1 | 两个 baseline 和 healthy before 各 1 条成功；mixed 健康 1 条成功、故障 1 条 status 9；随后新提交的 healthy after 1 条也为 status 9，SQ 仍 READY |
| RF1 | 16384 / 32 / 8 | before 256 条成功；首轮 mixed 健康 1 条成功、31 条 status 9，故障 32 条 status 9；停止后续阶段 |
| RF2 | 16384 / 128 / 8 | before 1024 条成功；首轮 mixed 健康 1 条成功、127 条 status 9，故障 128 条 status 9；停止后续阶段 |
| RF3 | 1048576 / 32 / 8 | before 256 条成功；首轮 mixed 健康 1 条成功、31 条 status 9，故障 32 条 status 9；停止后续阶段 |

这四组都先验证两个 READ baseline，确认 SIGKILL/waitpid 后才访问旧目标，客户端已接受项完整退休、最后查询 READY。服务端 EOF 104 是客户端失败后关闭控制连接的后续现象；另一次 RF0 的 waitpid 超时 110 发生在退出确认之前，没有提交 mixed WR，应单独保留为退出阶段失败。详见[观测记录](hardware-observations.md)。

用户随后明确：当前芯片 status 9 后 jetty 不可继续使用，大并发 READ 容易触发，而大并发 WRITE 不出现这一现象。RF0 的 after 失败提供了“当前批次退休后，下一次健康 READ 仍失败”的具体日志，READY 查询不能作为继续使用的依据。该缺陷不允许通过反复健康探针、重放或修改返回状态来规避。

当前工具改为固定单边 WRITE：主进程和故障子进程分别持有健康/故障目的 MR，先校验两端基线，再 SIGKILL/waitpid；客户端收到退出确认后，在同 SQ 交替 WRITE 旧目标和健康目标，随后继续健康 WRITE。所有端点 RX_posted=0。每轮用新数据，健康目的区在本地 TX 完成后由服务端校验并返回 TCP ACK，确认后才复用槽位；这些 ACK 是测试同步，不是 WRITE 加数据面通知协议。

| 组 | 两端共同参数 | 当前结果 / 要观察的行为 |
| --- | --- | --- |
| WF0 | `--bytes=16384 --batch=1 --rounds=1` | 双端通过；健康 before/mixed/after 各 1 条成功并经远端校验，旧目标 1 条 status 8，SQ READY，资源正常关闭 |
| WF1 | `--bytes=16384 --batch=32 --rounds=8` | 待真机；每轮 32 条故障 WRITE 与 32 条健康 WRITE |
| WF2 | `--bytes=16384 --batch=128 --rounds=8` | 同命令复测双端通过：健康 before/mixed/after 各 1024 条成功并经远端校验，旧目标 1024 条 status 8，每轮 256 条 mixed，SQ READY，资源正常关闭；首次单 WR 基线 status 9、隔离未到达及 RETAINED 记录保留，原因未明 |
| WF3 | `--bytes=1048576 --batch=32 --rounds=8` | 双端通过；健康 before/mixed/after 各 256 条成功并经远端校验，旧目标 256 条 status 8，SQ READY，资源正常关闭 |

工具不接受 `--operation` 或 `--mixed_send`；batch 是每个目标每轮的 WR 数，不是 SQ 请求深度。两端均须更新为 WRITE 版本，从 [README](../tools/rma_failure_test/README.md) 启动独立进程，并保持 bytes/batch/rounds 一致。

WF0/WF3 使用服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:52`，两端均指定 `--timeout_ms=10000`。每组健康与故障 baseline 各 1 条成功并完成远端校验，服务端随后确认 SIGKILL/waitpid。客户端被测 SQ 分别为 19159/19160，状态检查 10/52 次，post 调用 5/26 次，最后状态均为 READY（1），`accepted_known=true、pending=false、partial_batches=0、peer_exit_confirmed=true`。故障 mixed 的 `accepted=retired=1/256`，全部为 status 8；健康三个阶段的 `accepted=retired=verified=1/256`，服务端对应 verified 数量一致。双端 PASS 包含主进程资源正常关闭；故障子进程由 SIGKILL/waitpid 回收。

这两组直接验证了所测 WRITE 配置中，已退出目标的 status 8 不妨碍同 SQ 健康目标的混合与后续写入。结论不外推到 READ、其他故障类型、跨主机目标或任意 WRITE 并发度，也不证明没有性能干扰或 WRITE 数据面发布顺序。

WF2 首次使用同一组设备/EID 和 `--timeout_ms=10000` 运行时，客户端 SQ=22394 在**首条健康 baseline WRITE** 就取得 status 9：`post_calls=1、last_post_status=0、accepted=retired=1、verified=0、state_checks=2、last_state=READY（1）`。故障 baseline、healthy before/mixed/after 和 fault mixed 均为 0，`peer_exit_confirmed=false`；本次没有进入计划中的 256 条混合 WR。服务端各 verified 均为 0，EOF 104 是客户端失败关闭控制连接的后续现象，不能据此认定远端硬件故障或故障子进程已完成 SIGKILL/waitpid。

该日志确认 WRITE 也可能返回 ACK_TIMEOUT，并确认 status 9 后停止新提交的门禁生效。它没有证明“大并发 WRITE 触发芯片缺陷”，也没有证明 batch=128 是错误原因：当时只提交了 1 条 WR，初始超时原因未明。客户端 `pending=false、accepted_known=true` 只说明该已接受 WR 已退休；双方输出 RETAINED，未完成正常资源关闭。首次 WF2 登记为**基线失败、隔离阶段未到达**，不撤销 WF0/WF3 在各自配置下的通过结果。

WF2 随后以相同命令复测双端通过，客户端改用新 SQ=25228；服务端健康 jetty=33408，故障子进程 PID=488332、jetty=33407。健康与故障 baseline 各 1 条成功且经远端校验，SIGKILL/waitpid 确认后，健康 before/mixed/after 各 `accepted=retired=verified=1024`，服务端各 verified=1024；故障 mixed 的 `accepted=retired=1024`，全部为 status 8。8 轮各包含 128 条健康 WRITE 与 128 条旧目标 WRITE，已实际覆盖每轮 256 条混合 WR。客户端 26 次 post、52 次状态检查，最后为 READY（1），`accepted_known=true、pending=false、partial_batches=0、peer_exit_confirmed=true`；双方资源正常关闭，全程未在被测 SQ 上修改、重建或重放请求。

WF2 当前登记为**复测通过，首次基线 ACK_TIMEOUT 原因未明**。两次运行使用不同 jetty，不能据此认为先前的 SQ=22394 已恢复，或 status 9 的芯片缺陷已经修复；同命令复测通过也不能证明其他环境状态完全相同。

故障 WRITE 的实际 post 返回值与 CQE 状态均保留，不预设一定是 status 8；除 status 9 外的合法非成功终结可作为旧目标错误结果。**任何目标出现 status 9 都禁止继续提交后续轮次**，即使健康 WR 成功且查询仍为 READY，也在收集当前已接受批次后判失败。健康 WRITE 必须成功且远端 payload/guard 正确，未知接受范围或未决 DMA 保留资源，不重放请求。WF0/WF3 及 WF2 复测没有出现 status 9，不改变该错误后的软件处理要求；WF1 待真机，不继承它们、SEND Q1–Q4 或正常 READ R11–R15 的通过结论。

## 第二批：关闭与 provider 边界

按对应 README 的命令运行以下矩阵，两端共用参数保持一致：

| 组 | 工具与参数 | 当前结果 |
| --- | --- | --- |
| J1/J2/J3 | jfr_lifecycle：`--rx_depth=1/32/256 --consumed=0` | 均通过；残留 RX 分别为 1/32/256，DeleteJfr 返回 0 |
| J4 | jfr_lifecycle：`--rx_depth=32 --consumed=7` | 双端通过；接收 7 条、残留 25，DeleteJfr 返回 0 |
| J5 | jfr_lifecycle：`--rx_depth=256 --consumed=255` | 双端通过；接收 255 条、残留 1，DeleteJfr 返回 0 |
| P1 | provider_boundary：`--tx_depth=64 --batch=3 --attempts=1024` | 通过；接受/退休 128，ENOMEM、部分接受；UNHANDLED=0 |
| P2 | provider_boundary：`--tx_depth=128 --batch=127 --attempts=2048` | 通过；接受/退休 256，ENOMEM、部分接受；UNHANDLED=0 |
| P3 | provider_boundary：`--tx_depth=256 --batch=255 --attempts=4096` | 通过；接受/退休 512，ENOMEM、部分接受；FLUSH_ERR=133，UNHANDLED=0 |
| P4 | P1 再加 `--error_post` | 通过；ERROR post 返回 0，接受/退休总计 129，整轮 UNHANDLED=1 |

J1–J5 均先删除两个依赖 jetty，再删除带残留 RX 的共享 JFR，成功后注销 MR；接收端 DeleteJfr 耗时约 1.14–1.15 ms。双端组在约定流量结束并确认对端静默后操作。结果支持整个接收域的正常关闭，不授权对端仍发送时删除 JFR，也不改变单连接关闭时共享 JFR 继续服务其他连接的原则。

历史 `tx_depth=1` 的单 SGE、非 inline SEND 连续接受 64 条；P1–P3 进一步观察到请求 64/128/256 时实际接受 128/256/512。请求深度与当前 WR 形式的实际容量不同，不能把“最小 64”或“深度乘二”定义为通用接口契约。后续深度测试从 64 起，省去更小请求深度的重复组。[背压测试](../tools/backpressure_test/README.md) 的推荐组也使用 64/128/256；它验证软件配额，已有旧参数的通过反馈不自动证明这三组新参数全部通过。

所有 P 组的 `attempts+2` 都必须在 `max_jfc_depth` 内，必要时减小 attempts，但仍须大于 tx_depth。provider 的实际容量可能超过请求深度；达到 attempts 上限仍无拒绝时，可在 CQ 能力范围内增大 attempts。硬件可能在切 ERROR 之前处理完全部请求，因而不能保证非零 UNHANDLED；分别看三个 `OBSERVED/NOT_OBSERVED` 指标。

P4 先排空原始 128 条 WR，再查询到 ERROR（state=3），额外 post 一条返回 SUCCESS，最终总计 129 条恰好退休一次并正常关闭。整轮为 `ACK_TIMEOUT=121、RNR=7、WR_UNHANDLED=1、flush_done=1、flush_calls=3`；其中 WR_UNHANDLED 确实来自软件 flush，但聚合计数尚不能将它严格归属到 ERROR 后新增的 probe。当前已覆盖非零软件 flush 和 ERROR post 返回行为，未覆盖的是该 probe 独立终结状态的归属。

ERROR post 的成功返回不表示 SQ 恢复可发送，生产必须保留软件门禁。若其他运行出现返回 SUCCESS 却始终没有终结，工具会打印 retained 并保留资源到进程退出，该分支不能记作安全关闭 PASS。当前 P4 没有进入保留分支。

## 第三批：进程与真实故障

按 [peer_failure_test README](../tools/peer_failure_test/README.md) 运行：

此工具的服务端就绪提示是 `Disposable URMA endpoint: pid=...`；看到该行后启动客户端，不必等待字面上的 `ready`。
该 PID 属于专用 URMA 子进程，TCP 控制连接由另一监督进程持有。

| 组 | fault | batch | 当前结果 |
| --- | --- | ---: | --- |
| F1 | `process_exit` | 1 | 未单独反馈；不由 F2/F3 推定该组已运行 |
| F2/F3 | `process_exit` | 32/256 | 双端通过；确认退出后新投递，全部 status 8，前后 READY，无 async |
| F4 | `inflight_exit` | 256 | 流程通过、在途窗口未证实；退出请求前退休 32，确认时软件未退休 224，最终 256 条全部 SUCCESS |
| F5 | `external` | 32 | 未测试；不注入故障的对照，检查预投 RX 与正常流量 |
| F6 | `external` | 32 | 未测试；在 INJECT_NOW 窗口内手工注入实际 UB 链路故障 |

F2/F3 均先通过一条 baseline SEND，再由监督进程 SIGKILL + waitpid 确认 URMA 子进程退出，随后客户端向旧 endpoint 提交新批次。post 返回 0，accepted=retired=32/256，所有 CQE 都是 `URMA_CR_REM_ACCESS_ABORT_ERR`（status 8）。本地 SQ 在前后查询时均为 READY，工具未主动 modify ERROR；不能仅凭 READY 或缺少 async 判断远端仍存活，也不能据此认定整条本地 SQ 已失效。客户端显式关闭成功；服务端 PASS 表示监督协议及子进程回收完成，不表示被 SIGKILL 的子进程执行了正常 Close。

F4 的 `software_unretired_at_confirmation=224` 可能包含已经在 CQ 中排队的完成，不能作为硬件在途数量。`first_cqe_ns=56680、last_cqe_ns=1065049588` 从发送逻辑开始到软件消费 CQE 计时，后者包含等待 TCP 退出确认的间隔，不表示硬件耗时 1.065 秒。256 条 SUCCESS 不证明对端应用消费了全部消息，也不证明 SIGKILL 对真正的硬件在途 WR 没有影响。

上述三组均在 TX 轮询期间及 TX 观察后的默认 1 秒内没有 async 事件。`fd_available=true` 只表示事件 fd 可用；`watchdog_ms=96000` 是整个实验保护时限，不是 async 观察窗口。当前工具日志尚无真实事件的 Get/Ack 通过证据；前文用户确认支持的事件类型属于另一类现场说明，不能据此把本轮 async 计数改成已观测。

F5/F6 使用 `--fault_delay_ms=10000`，控制 TCP 尽量走不受影响的链路。F6 必须记录现场注入动作和时间；根据用户的现场能力说明，WR 通常为 ACK_TIMEOUT，PORT_DOWN async 当前无法观察。工具仍保留实际状态，不把 ACK_TIMEOUT 以外的结果隐去，也不要求 async 非零。无故障计数或无 async 不能自动解释为设备已经恢复。若无法注入物理故障，明确记为未执行，不能用 F1 代替它。

进程退出工具使用监督进程保留控制连接，并以 waitpid 提供退出证据。TCP EOF 既不证明远端 provider 已回收，也不证明本地在途 DMA 已终结。工具对缺失 CQE、伪 CQE、未知接受范围保留资源，整个进程有 watchdog 限制。

## 同 SQ 的 status 8 故障隔离

按 [sq_multiplex_test README](../tools/sq_multiplex_test/README.md) 运行 `--fault=peer_exit`。默认 `--fault=rnr` 保留现有缺 RX 场景；两种模式分开登记。2026-10-22 反馈的 Q1/Q2 双端日志均通过：服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:61`。这些历史记录来自协议 v3；当前工具已升级为 v4，以保证客户端经过正式完成处理器，两端必须同步更新。实验流程如下：

1. 客户端先用被测 SQ 对专用故障 endpoint 发送一条 baseline，收到 SUCCESS CQE 并查询 SQ READY 后，完成第一阶段结果握手；故障子进程此时仍存活。
2. 服务端再让故障子进程消费 baseline RX、校验源端点/payload/IMM，成功后 SIGKILL 并 waitpid 确认；第二阶段结果握手确认退出。健康 endpoint 不受该进程回收影响。
3. 确认退出后才开始每轮 healthy before/mixed/after；其中 mixed 在同一条 SQ 上交替向旧故障 endpoint 和健康 endpoint 提交 WR，并分别记录终结状态。故障目标预期为 status 8，健康目标三个阶段均应成功，并由服务端校验 payload。
4. 全过程不主动 modify ERROR、不 flush、不重建 SQ，也不重放失败消息；同 SQ 各检查点仍为 READY，逐 WR 终结收齐、健康收发与资源收尾都满足后才确认隔离通过。

| 组 | 两端共同参数 | 当前结果 |
| --- | --- | --- |
| Q1 | `--fault=peer_exit --batch=32 --rounds=8 --bytes=4096` | 双端通过；mixed 每次 64 个 WR，healthy 三阶段各 accepted=retired=success=received=256，旧目标 accepted=retired=status8=256；客户端 SQ ID 27048 |
| Q2 | `--fault=peer_exit --batch=128 --rounds=8 --bytes=4096` | 双端通过；mixed 每次 256 个 WR，healthy 三阶段各 accepted=retired=success=received=1024，旧目标 accepted=retired=status8=1024；客户端 SQ ID 27049 |

两组客户端 baseline 均单独统计 `accepted=retired=success=1`，服务端记录 baseline received=1 并确认 SIGKILL/waitpid；双方打印 `Peer exit: confirmed=true`。客户端各有 `state_checks=26、last_queried_state=1、accepted_known=true、provider_eagain=0`，其余错误计数全部为零；每个 mixed round 的健康成功数和故障 status 8 数均等于 batch。表中 received 来自服务端，其余流量计数来自客户端，两轮不同 SQ ID 属于分别启动的实验。

两端最终均 PASS：存活服务端与客户端完成资源关闭，故障子进程由 SIGKILL/waitpid 回收。before 是故障目标退出后、mixed 前的健康发送，不是 SIGKILL 前的流量。任一目标出现非预期状态时工具仍收集当前已接受 WR 的终结证据，再报告 FAIL；判据没有放宽为允许健康目标出错或旧目标返回 RNR/ACK_TIMEOUT。

这两组已经用实际健康流量确认：在本次 RM_CTP/error-continue 配置下，旧远端上的 status 8 没有导致同一 SQ 的健康目标失败或阻止后续发送。证据包含成功 CQE、远端 payload 校验和状态采样，不再仅依赖 F2/F3 的 READY 查询。结论限定于同一远端主机上故障进程与存活进程的端点；不外推至跨远端主机、物理链路 ACK_TIMEOUT 或任意错误类型，也不证明没有性能干扰。mixed 的 WR 总数为 before/after 的两倍，软件 CQE 消费时间不能直接作为硬件阻塞证据。

上述 Q1/Q2 实测版本直接消费合法终结 CQE，保持 SQ 运行以观察隔离。生产 `TxCompletionProcessor` 随后已为合法 status 8 增加例外：只退休对应 WR，保留原始状态与 `AttemptRecord` 上报，不因该状态调用 `BeginDrain`；连接与操作的失败决策由上层负责，本层不自动关闭连接或重试。status 8 是远端访问中止错误类别，进程退出只是本次注入方式。RNR、ACK_TIMEOUT 等其他错误仍主动排空，硬件观测与生产错误处理分别验证。

新版 `peer_exit` 已接入正式 `TxCompletionProcessor`，不再由工具重复退休；`rnr` 仍使用原始观测路径。用户随后反馈 Q3/Q4 的完整双端 PASS 日志，服务端仍为 `udmac0d1e4:39`，客户端改为 `udmac0d1e4:42`。两组均打印 `completion_path=processor`，新增完成层判据与原有逐目标流量、payload、退出确认及双端关闭判据全部满足：

| 组 | batch / rounds / bytes | 客户端 SQ ID | 每个 healthy 阶段 SUCCESS / 服务端 received | 旧目标 status 8 | processor_events / processor_retired |
| --- | --- | ---: | --- | ---: | --- |
| Q3 | 32 / 8 / 4096 | 49496 | 256 / 256 | 256 | 1025 / 1025 |
| Q4 | 128 / 8 / 4096 | 49497 | 1024 / 1024 | 1024 | 4097 / 4097 |

两组 baseline 均成功 1 条，各有 `state_checks=26、last_queried_state=1、last_lane_state=READY、accepted_known=true、provider_eagain=0`。生产退休总数满足 `1+4*batch*rounds`；每个目标 accepted=retired，所有非预期错误计数为 0。Q4 的 mixed 每次 256 个 WR，跨多个最多 64 条的完成批次；连续 8 轮及 after 成功验证额度归还后可用同一 SQ 继续发送。Q3/Q4 是生产完成层集成的独立实测证据，不是对 Q1/Q2 的追认；它们不证明跨主机/链路故障、其他错误类型隔离、无性能干扰或上层连接回收已验证。

## 剩余硬件缺口

| 项目 | 当前缺口 / 后续范围 |
| --- | --- |
| 受支持 async 类型的工具记录 | A8/A9 高/低位重配、X1 RX JFC_ERR、X4 TX JFC_ERR 及两组关联 JETTY_ERR 已有日志；X2/X3 外部 JFC/JFR error 待具备可靠注入工具后再测；独立 JFS 场景未测；A5/A7 及 depth=256 多轮未自动继承结果 |
| 单边 WRITE 故障隔离补测 | WF0/WF3 及 WF2 复测已通过旧目标 status 8 与同 SQ 健康 WRITE 隔离，WF2 每轮 256 条 mixed 已覆盖；首次 WF2 单 WR 基线 ACK_TIMEOUT 原因待查，WF1 尚未反馈；历史 READ RF0–RF3 失败保留，任何 status 9 禁止继续提交 |
| WRITE 后 SEND_WITH_IMM 通知可见性，当前优先 | WN1–WN4 工具已实现、待真机；远端须在通知 CQE 处理时检查目的数据，早于 TCP 完成确认；既有 WRITE 与 READ/SEND 记录不代替本项 |
| 故障 CQ 处理及资源回收，暂缓 | 按用户要求暂不实现；X1 RX JFC、X4 TX JFC 观察均通过但双方 RETAINED，未尝试故障对象正常删除；X4 客户端 448 条 TX、服务端 200 个 RX 槽缺少终结证据；DMA 终结及删除契约仍是保留缺口，同故障 JFC 下其他 jetty 的 READY 不代表可复用 |
| 四 context D2 首次失败，保留观察 | 第二次已补齐双端四路通知/ack/fd；首次客户端 status 9、服务端 fd 超时仍未定位。工具已增加诊断，用户随后反馈暂未复现；不要求无限追测，不代表根因已修复；四物理设备组暂缓 |
| 物理故障，低优先级可选 | 按用户要求降低优先级；F5/F6 工具日志尚未取得，用户说明通常 ACK_TIMEOUT，不将无法观察的 PORT_DOWN async 列为通过条件 |
| 在途中断 | F4 已完成提交后退出流程，但未证实 SIGKILL 命中硬件仍在处理 WR 的窗口；延迟软件 poll 不能补齐这项证据 |
| ERROR probe 的终结归属 | P4 已有非零 WR_UNHANDLED 和 ERROR post SUCCESS；需逐 probe 状态/来源才能把两者严格对应，无须再把非零 flush 本身列为未测 |
| ACK_TIMEOUT | 用户已确认 status 9 后 jetty 不可复用的芯片缺陷，软件必须停止该 jetty 新提交；历史 SEND mixed、provider 边界及 D2 首次失败的具体触发原因仍未分别定位，不能自动认定同源 |
| RNR 时间窗口与 provider EAGAIN | 已有 RNR 和 ENOMEM 容量拒绝；尚无精确重试窗口、rx_delay 原始批次全部恢复成功或同步 EAGAIN 的硬件证据 |

多 SGE 的 G2/G3/G4 与并发 READ/混合 SEND 的 R11–R15 已通过；G1 小消息和原建议 window=8 的 R9/R10 未执行，可按需补齐。WRITE 数据面通知可见性的 WN1–WN4 已列为当前优先实验，仍待真机；权限错误后的隔离也超出已验证范围，按启用范围再补对应验证。

## 结果记录与判定

每一组至少记录：日期、双端完整命令、源码提交与未提交修改、二进制哈希、设备/EID、网卡型号/固件、内核、liburma/provider 版本、能力输出、计数、状态码、最终结果及 shell 退出码。`uname -a`、`sha256sum <binary>` 和现场软件包版本信息应与日志放在一起。

| 输出                 | 含义                                                                           |
|----------------------|--------------------------------------------------------------------------------|
| PASS                 | 该工具定义的流量/协议/资源检查成立；不自动覆盖同一输出中 NOT_OBSERVED 的项目   |
| OBSERVED             | 明确观测到某项行为；可能是 ERROR post 无终结等异常行为，不能统称业务成功       |
| NOT_OBSERVED         | 本次未触发或未取得证据，不是通过或证伪                                         |
| SKIP                 | 明确能力/参数门禁不满足，未执行对应硬件操作                                    |
| FAIL / watchdog 超时 | 工具检查失败、观测不完整或调用未返回；保留原始日志，不据不完整计数推断数据丢失 |

现有 `backpressure_test --mode=rx_delay` 仍可扫描延迟与深度，但软件配置 `rx_delay_us=0` 不代表实际零延迟。没有硬件重试计数/时间戳时，不能通过这个工具精确反推出 RNR 重试窗口。此前 `rnr_continue_success` 与同 SQ mixed 成功仍是有效的独立证据。

socket 的 EAGAIN→EPOLLOUT/NotifyWritable、正式连接协议、分片重组和 brpc worker 退出顺序属于后续软件实现/集成验证。多设备故障迁移、READ 权限拒绝后的隔离、多主机同 SQ 故障影响范围等也不由以上正常路径或两机实验自动保证；若未来启用相应策略，需要针对策略增加验证。这份矩阵记录当前 Raw SEND、READ/WRITE 正向路径及已执行的故障观察，不承诺穷举所有硬件故障。
