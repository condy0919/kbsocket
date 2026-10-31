# kbsocket 实施 roadmap

更新：2026-10-29。目标是将现有 raw 组件推进为**可持续收发、连接内有序、内存有界、故障显式失败**的通信库，再完成规模与性能验收。本文件管理阶段与交付状态；协议和资源规则以 [设计 v1](design/kbsocket-design-v1.md) 为准，硬件依据见 [观测记录](hardware-observations.md)和 [验证矩阵](hardware-validation-matrix.md)。

当前状态：M0 契约与设计修订完成；下一项是 M1。M1–M7、L1/L2 是待交付计划，已有 raw 原语与测试工具是基础，不代表这些阶段已经实现。没有 UB 真机期间使用 gMock/可控 backend 验证状态机，NativeTCP/SHM 使用真实双进程验证；未运行的真机回归保持未执行，不把 mock 结果标为硬件 PASS。

## 阶段与依赖

| 阶段 | 交付物 | 验收重点 | 状态 |
| --- | --- | --- | --- |
| M0 | 硬件契约、身份/所有权、错误作用域、关闭顺序及数据路径选择 | 每项有依据，已实现与待实现可区分 | 文档完成 |
| M1 | 支持残留 RX 的接收域关闭状态机 | DeleteJfr 成功才终结残留 DMA；Held lease、失败重试与正常关闭完整 | 待实现 |
| M2 | 故障 jetty 替换、逐 lane generation 与错误策略 | status 9 停用，旧完成不影响新 lane，健康共享资源继续推进 | 待实现 |
| M3 | 单 owner RuntimeShard / ShardReactor、最小控制执行器 | 有界公平 progress、poll/event 共用逻辑、async Get/Ack 与补投 | 待实现 |
| M4 | 注册 TX staging、wire/session、薄保序、基础 credits | 普通 write 源复用安全、连续前缀、慢读反压、partial post 不丢账 | 待实现 |
| M5 | 最小连接、显式 C API 与 NativeTCP | connect/accept、双向 read/write、readiness、FIN/错误/取消/close 闭环 | 待实现 |
| M6 | 多 shard、缓存、弹性流控、socket/epoll 与框架适配 | 40k 常驻与 40k/s 分别验收，资源预算与冷暖建链可解释 | 待实现 |
| M7 | WRITE RNDV、内存扩展与逐项性能优化 | 完成后通知、grant 安全终结；优化不破坏正确性 | 待实现 |
| L1→L2 | SHM-copy，再做共享 lease/NUMA 优化 | 授权、双进程生命周期、反压及相对 NativeTCP 的收益 | M5 后支线 |

主线按 M1→M2→M3→M4→M5 推进。M1/M2 共用生命周期与身份规则，可独立拆分开发和测试；M3 统一驱动它们。M4 的基础流控属于最小闭环，不能推迟到 M6。M7 的 WRITE 正确性部分在 M4/M5 的 lease、session 和进度基础具备后可以与 M6 并行，不必等 40k 性能验收；复杂优化再逐项启用。普通大写入在 RNDV 上线前采用有界分片 copy eager，不要求为整次 write 预留无上限缓存。

## 当前实现与目标之间的边界

以下以本次核对的已跟踪 raw 源码为准，工作区中的未完成实验不视为已交付接口。

| 组件/能力 | 已有基础 | 需要补齐 |
| --- | --- | --- |
| RawRuntime / UrmaContext | 按 `(device, eid)` 创建 context；同设备多个 EID | 正式 shard 归属、context async owner 和整体关闭协调 |
| JettyPool | 共享 JFR、TX/RX JFC、多 jetty；软件配额；ERROR/flush 排空 | 残留 RX 关闭协调、单 lane 替换及独立 generation |
| AttemptLedger / TxSender | 逐 WR 身份、接受前缀、全 signal SEND/IMM、配额 | 实际 TX lease 所有权、连接操作归属；后续 RMA 提交与通知 |
| TxCompletionProcessor | 正常退休、错误/flush；status 8 不无条件排空 SQ，已有真机回归 | RNR 仍会 BeginDrain；真实 WR 身份验证须先于改变 lane 状态；重建的迟到事件门禁 |
| RxBufferPool | 4 KiB MR slab、Posted/Held/Free、归还和 refill | 当前 Close 要求 posted/held 均为零，尚无 DeleteJfr 终结残留 Posted 的生产流程 |
| 通知/async | UrmaApi 封装及 streaming/async 工具，有硬件证据 | 正式 reactor 的公平预算、对象路由、通知引用和关闭协同 |
| Connection / SocketAdapter | 设计及工具控制协议可供参考 | 正式 wire、连接状态机、普通 socket 内存语义、统一 API 和兼容层 |
| RMA / grant | READ/WRITE、故障隔离和通知观测工具 | 生产 RNDV；工具未实现正式跨连接 grant 生命周期 |

源码入口：[JettyPool](../kbsocket/transport/raw/jetty_pool.hpp)、[TX 完成器](../kbsocket/transport/raw/tx_completion_processor.cpp)、[RX 池](../kbsocket/transport/raw/rx_buffer_pool.hpp)。组件行为说明见 [Raw 传输流程](raw-transport-flow.md)。

## M0：契约与设计修订

M0 的交付是可执行的实现约束与验收清单，不通过修改能力常量或扩大 PASS 范围代替设计。此次已将硬件日志、人工确认、源码事实和设计假设分开，并修正设计的旧阶段划分。

### 已固定的硬件输入

| 输入 | M0 决策与范围 |
| --- | --- |
| RM_CTP SEND 最多 4096 字节，包含 payload 内协议头 | RM_TP 的查询值 `max_msg_size=65536` 不用于扩大 RM_CTP 帧；READ/WRITE 单次上限 200 MiB，不作为默认 chunk |
| 同 SQ、同目标的 SEND 默认不保证顺序 | 连接逻辑 SN、有限重排窗口和连续前缀交付；不默认启用高成本 fence order |
| status 8 已测不破坏同 SQ 健康 SEND/WRITE | 按失败 WR/连接处理；它可能来自失效远端或远端队列错误，不能一律解释为进程退出 |
| RNR 后同 SQ 可续发，混合健康目标成功 | 目标策略不因普通 RNR 无条件排空整 SQ；已失败的已接受 WR 不重放。retry/timer 效果仍未知，不用它代替严格 credits |
| status 9 后原 jetty 不可用，即使 query 为 READY | 软件立即关闭该 lane 准入；正常 JFC 可继续排空；删除旧 jetty 并创建新的对象。READ 高并发有已知风险，WRITE 也保留统一 status 9 防护 |
| DeleteJfr 成功后残留 RX 不再访问内存 | 作为残留 Posted 的 DMA 终结依据；不伪造成功 CQE，不撤销 Held lease、CPU 引用或独立 RMA grant |
| rearm 时已有 CQE 仍触发通知 | 使用 JFC rearm→recheck→必要时等待；JFCE readiness、JFC 通知和 CQE 分账，取得的通知仍须 Ack |
| JFR_LIMIT 触发后 threshold 清零 | refill 与 Ack 不自动重新使能；优先补到高于水线后 rewrite，水线事件不直接产生 credits |
| WRITE 的完成可见性与 READ 一致 | 全部相关 WRITE 逐 WR 成功后，再 SEND/IMM 发布完成；不再作为待确认硬件能力。通知身份及实现回归仍需完成 |
| 两裸设备×两 EID 持续收发/补投/通知已有实测 | 可用于四 context 功能设计；不等同于四物理设备、四端口带宽或多 owner 性能验证 |

以上契约仅针对当前目标环境。人工依据见 [2026-10-23 硬件契约](hardware-observations.md#2026-10-23-人工确认的硬件契约)及 [2026-10-28 WRITE 可见性](hardware-observations.md#2026-10-28-人工确认的-write-可见性)；具体运行覆盖和历史失败保留在验证矩阵。相同命令后来 PASS 不抹去曾出现的 ACK_TIMEOUT，也不证明其诱因已修复。

### 身份、所有权与完成点

按 [设计 §4.2](design/kbsocket-design-v1.md#42-身份必须分层)区分 runtime/context、pool epoch、lane generation、远端 endpoint/接收域、session、attempt、operation、lease 和 grant。身份可聚合存储，但不能混用语义：更换本地发送 lane 不等于更新远端 endpoint，不等于恢复旧 session。没有可信 user_ctx 的 flush 边界不能仅靠软件 generation 防止硬件 ID 重用；必须建立旧对象与事件终结屏障，无法证明则保留隔离。

| 所有者 | 必须保持的责任 |
| --- | --- |
| 队列 owner | 唯一推进 post/poll/refill/flush；操作入账在先，发布/回调交接在后；真实完成先验证身份再改变 lane 状态 |
| AttemptLedger / 操作记录 | 已接受 WR 直到合法终结均可定位；明确未接受后缀可重试，接受未知的范围保留依赖且不重放 |
| RX/TX lease 管理者 | 持有真实 storage/MR 生命周期；raw 账本里保存租约标识不等于已拥有内存 |
| ReceiveDomain | Posted/Held/可授信容量分别计账；删除 JFR 仅结束该域残留 RX 的设备访问 |
| Connection | 维护逻辑顺序、连续可读前缀、FIN 和明确错误；物理完成不能直接当作对端应用执行结果 |
| grant 管理者 | 远端授权与本地 lane 独立终结；超时、flush、重建不能自行允许旧暴露区间复用 |

普通 `write()` 返回已接受字节前，copy 路径完成源数据复制；显式借用 API 必须保持 lease 至 `source_reusable`。WRITE 成功、通知 SEND 的本地完成、远端接收验证、应用释放是不同完成点。READ/WRITE 完成可见性已确认，不消除后几项生命周期责任。

### M0 完成清单

- [x] 证据类型与模式/大小能力分开：[设计 §2](design/kbsocket-design-v1.md#2-约束证据与验收目标)。
- [x] 真实 WR、边界事件、lane 替换与错误作用域：[设计 §11](design/kbsocket-design-v1.md#11-故障健康缓存与重新选择传输)。
- [x] DeleteJfr 终结与 Held/引用/失败重试：[设计 §12.3](design/kbsocket-design-v1.md#123-shutdown-是依赖图不是强行清零)。
- [x] 软件保序、WRITE 优先、grant 与通知：[设计 §6](design/kbsocket-design-v1.md#6-连接内顺序保留薄适配不构建重放协议)、[设计 §8](design/kbsocket-design-v1.md#8-buffer注册内存池inline-与-rndv)。
- [x] 配额/实际接受/credits 分离，通知和 async 生命周期：[设计 §9](design/kbsocket-design-v1.md#9-分层流控基础预授信弹性窗口与-rndv-准入)、[设计 §12](design/kbsocket-design-v1.md#12-busy-poll事件模式与关闭)。
- [x] 明确现有实现缺口、后续依赖及测试门禁：本文件及设计 §15–16。

## M1：接收域安全关闭

改动围绕 JettyPool、RxBufferPool 和域级关闭协调器展开，不让两个组件互相等待对方先 Close。

1. 停止新提交/补投，保留 TX 完成、事件确认与 lease 归还的进度。
2. 正常排空 TX，按已确认依赖顺序删除所有关联 jetty，再删除 JFR；成功才将残留 Posted 记为“DMA 已终结”，不计作收到数据。
3. 在 provider 对象仍有效的阶段处理需要解析其地址的 CQE/通知。删除后遗留记录的处理也必须遵守该限制；不能先释放 queue 再 poll 可能引用它的 CQE。
4. 等待 Held lease、相关 CQ/async 引用和其他访问责任终结，再注销 MR、释放 backing，关闭不再被依赖的资源。
5. 删除/注销失败保留依赖；重试从已完成阶段继续，不能重复回收或强制清零。

验收覆盖全部 RX 未消费、部分消费、最后一个残留 RX、Held 迟归还、晚到 CQE、DeleteJetty/DeleteJfr/注销失败重试、空闲关闭及正常路径回归。断言调用顺序、无提前释放、每条记录恰好终结一次。当前真机删除契约可直接用于 mock，不等待对端再发清理消息；恢复硬件后补生产路径回归。

## M2：故障 lane 替换与错误作用域

保留已有状态机含义，增加替换流程：拒绝新提交→正常 JFC 退休/ERROR 与必要 flush→满足销毁和事件条件→删除旧 jetty→创建新 jetty→发布新 generation→恢复本地准入。复用健康 JFR/JFC；创建失败保持该槽不可提交并支持显式重试。

- 修正真实 WR 先按 local_id 改变 lane 状态、后验证账本身份的次序。普通完成、旧 generation、重复完成、假边界分别测试。
- pool epoch 与 lane generation 分开；不通过重开整个 pool 使健康 lane 的票据失效。硬件 ID 复用时旧边界无法区分的情况必须保持隔离，不能凭空补出事件代次。
- 单独修正 RNR 的完成策略，与 status 8 一样不无条件排空健康 SQ；按原状态将错误交给所属操作/连接，不重放。status 9 维持整 lane 停用，READY 查询不重新开放门禁。
- 如果被替换对象也是远端发布目标，使用新的 endpoint generation、import 路由与新连接协商；不能仅换本地指针后继续旧 session。

验收覆盖一个 lane 故障时同池其他 lane 收发、同 SQ 目标级错误、延迟/重复完成、错误事件与关闭竞争、重建失败/重试、generation 回绕拒绝及 endpoint 重新发布。故障 JFC 的全域恢复不包含在此阶段。

M2 先交付可显式驱动的控制原语与状态机；可能阻塞的创建/删除调用不进入收发 fastpath，正式运行时由 M3 的控制执行机制接管。

## M3：统一 owner 进度器

建立 RuntimeShard/ShardReactor，先单 owner/context，再验证同 owner 管理四 context。将 TX、RX/refill、flush、mailbox、timer、retire、listener/control 纳入有界轮转；达到预算后保留 runnable 状态，不 drain-to-empty 占住线程，不扫描全部空闲连接。

同时提供最小有界控制执行器，处理可能阻塞的创建、导入、注册和销毁，结果交回 owner 后才发布。owner 先冻结目标对象的相关操作并持有依赖；控制执行期间不得并发操作同一资源，关闭/取消后返回的半成品仍按生命周期回收。M5 的非阻塞建链依赖此机制，不能等到规模阶段才补。

polling 与 event 复用同一核心。JFC rearm/recheck、Wait/Ack 与 context async Get/Ack 分开管理；共享 context 指定唯一 async 消费者，转交已复制的对象事件语义。JFR_LIMIT 按实际 RX 账本驱动 refill/rewrite，不依赖 PORT_DOWN；JFC_ERR 至少关闭受影响依赖域准入并保留资源，完整恢复延期。

验收覆盖 rearm 前已有 CQE、recheck 有进展但通知未 Ack、空唤醒、通知合并、refill 暂时失败后无新 CQE、热 TX/错误风暴、mailbox 与睡眠竞争、关闭时 timer/async 仍可推进。不同进度类不能饥饿；跨线程请求经有界队列交给 owner。

## M4：有界有序数据路径

在预注册固定容量上实现 TX staging/lease、正式帧编解码、session/demux、逻辑 SN 与 bounded reorder、连续字节队列和 FIN。接收 pool 不承担连接协议。按 4096 字节总 SEND 上限分片，头部与多 SGE 总长度一起计费；初版全 signal。

基础 credits 从实际成功 post 的 RX 与接收字节预算产生，明确未授/已授未消费/待补投守恒。应用持有、乱序、待发送数据与控制保留都有上限；错误/关闭时旧授信不直接重发。M6 才优化长期基础窗口和弹性池的规模策略，M4 先保证不超卖、不循环等待。

验收包括源 buffer 在 write 返回后立即改写、乱序/重复/越窗/旧 session、部分 post 的后缀继续提交、接受未知、FIN 前有缺口、慢读与 lease 迟归还、双向满窗口仍能归还 credits、注册池耗尽。mock 驱动确定性重排；内存和 pending 描述符始终有界，数据路径不分配系统堆内存。

## M5：最小连接与统一 API

实现 TCP 控制协商、endpoint/import 准备、RX 路由发布后双方 READY，再开放业务数据；建立 connect/accept/read/write/close 的显式 C API 和 NativeTCP backend。先提供可测试的句柄和 readiness 接口，完整 fd/epoll 拦截放入 M6。API 在阶段开始时冻结类型、返回值、取消和借用语义，不把工具 TCP 屏障协议直接作为生产协议。

验收包含双向持续流、短读写、EAGAIN/可写恢复、首包与 READY 竞争、FIN/半关闭/EOF、错误/取消恰好一次完成、失败新连接协商 TCP、旧连接不透明迁移。无需真机即可跑真实 NativeTCP 双进程和可控 raw backend；双方使用相同连接/字节流测试集。

## M6：规模、兼容与集成

增加多 shard 归属、import cache/single-flight、控制执行器的规模准入策略、peer runtime 基础/弹性 credits、健康探测和新连接准入。接入带 binding generation 与读者保护的 fd 表、socket/epoll LT/ET/ONESHOT、brpc 适配及可选 P2300 风格封装；核心不反向依赖 brpc。

分别验收 40k 常驻连接和 40k/s 建链，成功口径含双方 READY 与首个有效数据交换；分报热 peer、冷 import、冷 runtime。覆盖 fd ABA、native listener 公平性、关闭/重连风暴，以及 3000 peer 总账不会随 socket/shard/域数重复授信。CPU、MR/内存、FD、控制并发、retired 资源全部报告；无真机的模型规模测试不冒充 UB 性能。

## M7：WRITE RNDV 与性能优化

先交付 WRITE 正确性：接收端有界准入并授权目标 grant，发送端按有限 chunk 提交，逐条收集成功，再发送携带 session/transfer/grant 代次的 SEND/IMM 通知，接收端验证完整性并按连续前缀发布可读。WRITE 可见性与 READ 一致是已确认输入，WN1–WN4 是实现回归而非重新证明该契约。

grant 终结必须单独定义：通知发送成功不等于应用释放；取消、远端退出、status 9、通知失败及本地重建都不能自动撤销远端访问权。无法证明终结的暴露内存保留隔离并计入上限，达到上限停止新准入。READ 不作默认路径，亦不作失败后的自动替代。

正确性闭环后，分别评估注册内存扩缩容、多档 slab、多 SGE/inline、批量提交、selective signaling、CQ/等待策略和 multi-rail。每项有独立对照和关闭开关；selective signaling 的跨 opcode/目标累计语义未确认前保持全 signal。验收覆盖通知乱序、chunk 部分失败、迟到 WRITE、旧 grant 复用、错误后安全关闭，以及小包/控制不被大包饿死。

## SHM 与可观测性支线

L1 在 M5 连接/lease 接口稳定后实现 runtime-pair 授权、SHM-copy、独立 descriptor/payload 预算和可靠通知。L2 再增加显式共享 lease、NUMA 与通知优化。使用真实双进程覆盖 crash、旧 mapping、ring wrap、慢读、归还与关闭，按 [设计 §18](design/kbsocket-design-v1.md#18-linux-616-同机-tcp-核验与-shm-transport-spec)对照 NativeTCP；不依赖 UB 真机，也不降低授权与生命周期门禁。

从 M1 起为状态、lane/endpoint generation、accepted/retired、Posted/Held、refill debt、隔离内存和失败阶段保留有界 counters/诊断。M4 起贯通连接/操作身份；完整 tracing、尾采样和自动采集按设计 §19 逐步接入，不作为基础收发依赖，不在 fastpath 无条件格式化日志。

## 延期与后续真机回归

- **故障 JFC 完整排空/恢复**：目前只有注入观测，保留停止准入与依赖隔离；不因另一 jetty query READY 就继续使用已坏 CQ。
- **真实链路/端口故障**：低优先级；PORT_DOWN 明确不可观测，不作为关闭或恢复条件。JFR 故障注入工具目前不可用，不阻塞正常路径。
- **硬件 HOL、有效 RNR retry/timer、inline/完整 SGE 上限、selective signaling 与物理 multi-rail 收益**：只有采用对应优化时才要求确认；不扩展现有 PASS 范围。
- **恢复硬件条件后**：依次回归 M1 残留 RX 关闭、M2 status 8/RNR/status 9 和替换、M3 四 context 事件进度、M4/M5 有序流与反压、M7 WRITE 通知及 grant 生命周期。历史偶发 ACK_TIMEOUT 保留为问题记录，不以多次成功宣称根因消失。

M0 本轮只修订文档与实现约束；下一步直接从 M1 的关闭协调接口、可重试阶段和资源保留测试开始。
