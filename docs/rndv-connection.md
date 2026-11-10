# M6.3.9：Connection 与 Runtime 的 RNDV 装配

[`Connection`](../kbsocket/connection/connection.hpp) 现可显式启用 WRITE RNDV，将发送事务、接收入口和共用通知队列接到真实 Eager RX/TX 分发。默认连接仍使用 Eager；`RawStreamRuntime` 便捷入口尚不自动创建或公告目标 Arena。本阶段覆盖单连接固定 Lane、本地与远端各一个 Arena，不代表 UB 真机或规模验收通过。

## 准备与能力门禁

宿主在 `Connection::Open`（或 `ConnectionOwner::AdoptProtocol`）传入 `RndvConnectionConfig`，并提供已授权的 HELLO RNDV 能力与真实 Arena 目录。配置被复制；不传配置而公告 RNDV，在接管 fd 前拒绝。只有一端声明能力时仍按协商结果使用 Eager。

`PrepareRaw` 的 `rndv` 字段提供目标 Slab、共享 TsegImportCache 及固定发送 Lane。宿主必须先完成注册，并用 `ExportRndvArena` 构造本地目录；准备阶段逐字段核对实际注册快照。每端仅允许一个 Arena，双方目录都须容纳协商最大事务。本端目标 Slab 与共享源 Slab 可以不同，生命周期须覆盖所有引用、Grant 和连接关闭。

准备过程按已协商限额创建 GrantManager、接收器、WRITE 池、通知队列、发送协调器及 RX 暂存，再将混合读取流绑定到共同重排窗口。endpoint 尚 Pending 时可以预分配，实际通知/WRITE 仍要求 Ready。准备成功后才允许下一次握手进度发布路由与 READY；`negotiated_rndv()` 仅供准备，`rndv()` 仍要求双 READY。

数量与字节上限均有界，不为默认空闲连接创建 RNDV 池：

- 发送活动槽不超过协商 Grant 上限；接收槽与各类入口槽至少覆盖协商 Grant 数量。
- 通知槽至少为 `2 * tx_capacity + 1`，为每个本地 RTS/DONE 对和反向 GRANT 留出空间。
- 新 RTS 准入时，控制共账至少还有三个可用额度：预留 DONE 后，仍留出反向 GRANT 和最终 CREDIT 的进度空间。当前 wire 不携带对端控制补投低水线，发送准入还要求 Peer 共账没有其他未 Activate 的控制承诺，以防多份未来 DONE 占住额度但未触发对端补投；这是保守的并发限制，不影响已提交 WRITE/DONE 的独立退休。
- chunk 数、每次 post 数、单次进度预算、源引用余量和实际注册 backing 分别受限。单 WRITE 不超过协商值及 200 MiB 上限；完整事务还须能由本地 chunk 预算表达。
- 当前共同序号从零开始，目标授权窗口使用协商的 RNDV window；共同接收窗口可更大。

元数据在 Open/PrepareRaw 控制路径分配，Write/Progress/接收和完成路径不分配。Slab/MemoryBudget 约束实际 backing，跨连接完整元数据总预算仍待后续接入。

## 混合流、背压与半关闭

达到 `threshold` 的 `Write` / `WriteLease` 使用 RNDV；默认阈值为 16 KiB。copy Write 复制到一个源 Slab buffer，可按 buffer 或协商事务大小短写；Lease Write 保留源引用，可按事务上限短写。返回的字节数才是本次接受范围。已接受源 Lease 可以立即归还 CPU 所有权，引用仍由排队 pin、实际 WRITE 和通知各自维护。

Eager 在实际 post 时才取序号，因此尚有 Eager 排队或共同序号预约时，大消息返回 WouldBlock。RTS 接受前也不允许后续业务绕过其预约；CREDIT 仍能独立推进。Eager 只解释自己持有票据的预约后缀，不能误用 RTS 的 SN。RTS、Eager 和 FIN 共用一个逻辑序号空间，DONE 到达顺序不改变应用交付顺序。

半关闭只停止新写入：已接受 FIN 不取消此前的 RNDV；远端 EOF 不停止本端发送方向或反向 GRANT 处理。读取沿用 `Read` / `ReadLease` / `ReleaseReadLease`，只有有效 DONE 后的连续前缀可读。目标 Lease 归还前不能重用相应 Grant。

发送端 WRITE 窗口只能由远端累计 CREDIT 的已消费前缀推进。前缀由 `max_frame_seq - 初始 READY.max_frame_seq` 得到，包含 Eager SN；不能用本地 WRITE/DONE CQE 或协商 RNDV window 替代。若本地真实责任尚未终结，推迟窗口推进。

## Runtime 进度与身份

`RawStackConfig::enable_rndv` 默认关闭。开启后共享资源视图提供 TsegImportCache，每 context 的 endpoint/segment 控制进度进入同一个嵌套调度组。慢导入开始前暂停整个 context，直到控制完成被收割才恢复 post/poll/事件访问；资源视图也拒绝暂停期间访问。关闭检查段缓存引用、任务和 unimport，再释放内存与 context。

`ConnectionProgress` 基础 owner id 保留高两位，低 62 位仍包含原 slot 和代次。高位 `01` 对应 WRITE，`10` 对应通知，`11` 无效。查找仍为直接 slot 查找；还需核对连接登记的完整 owner、原代次及实际 context。旧完成、伪造 owner 或重复完成不能退还新事务引用。

Connection 进度推进 Eager、接收入口、发送协调器和通知队列；段导入由 Runtime 控制源推进。真实 CQE 仍经 TxCompletionProcessor，保留 status 8 的逻辑连接隔离及 status 9 的 Lane 停用规则，不重放旧 WR。业务写入的超时起点包含距最近调度的真实空闲时间，避免把长时间空闲前的进度时间当作新事务开始时间。

## 失败与关闭

失败停止 Eager/RNDV 新业务，但已发布目标 Grant 不能仅凭失败、EOF 或本地 flush 释放。Connection 在准备时保留握手路由；取消及 Closing 状态只允许迟到的 DONE 进入 RNDV 入口，继续真实 CQE 与有界清理进度，不恢复应用交付。未终结 Grant/未知接受使 Close 返回 Retained。

关闭先归还混合读取中的数据和应用借用，再关闭 ingress、接收器、发送协调器、通知队列及 WRITE/Grant 账本，最后解除路由保留、关闭 Eager 与握手。ingress 尚持有 GRANT 通知时，发送完成分发器必须继续存活。成功 DONE 丢失、对端永不返回或缺少独立终结证据时，保留依赖而不猜测远端静默。

## 验证与后续

[Connection RNDV 回归](../kbsocket/connection/connection_rndv_test.cpp) 使用真实软件握手、Slab、两类导入缓存、收发账本及完成处理器，Mock provider 执行实际目标内存 WRITE 和通知交付。覆盖双向 copy/Lease、跨窗口混合流、通知乱序、早到 RTS、独立 WRITE/DONE 责任、取消后迟到 DONE、非法准备及通知预算。Runtime 回归覆盖子 owner 防伪、段导入暂停、DMA pin 与异步关闭。

具体测试计数见 [roadmap](roadmap.md)。下一步是将显式 Arena/预算与准入策略接入 `RawStreamRuntime` 便捷入口，补应用 mailbox/Runtime 全链路 RNDV 和跨连接资源总预算。多 Arena 选择、故障后迁移及 UB WN1–WN4 回归未在本阶段交付。
