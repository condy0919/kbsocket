# M5.2：Eager Connection 与 NativeTCP 字节流

本次交付单 owner 的 C++ `Connection` copy-stream 接口，以及 `EagerTxStream`、`EagerReceiveDomain`、`EagerRxStream` 和 `NativeTcpBackend`。它们复用 M5.1 的 HELLO/READY、路由和导入缓存，实际调用 raw post/poll/Lease 原语；Mock 回环搬运真实注册 SGE 的字节并生成 CQE。UB 真机回归未执行。

## 建链与接口边界

- `Connection::Open` 接管明确支持本协议的已连接非阻塞控制 fd；`Progress` 推进 HELLO，进入 Preparing 后宿主提供已创建的 raw 资源，或调用 `PrepareTcp`。
- `PrepareRaw` 将完整 Connection 作为 route owner；先初始化 RX/TX，并通过 `sources` 原子安装已授权的远端主动 Lane 来源，再由握手协调器发布路由、发送 READY。HELLO 的本地 Lane 快照必须与实际 TxSender 池一致；`peer_lanes_authorized` 由宿主认证策略提供。TX 可绑定 pending import，首包与 Credit 可在本端等待 READY 时入账；应用读写和实际 post 仍等待双方 READY。
- `Write(span)` 返回已复制到注册 Slab 的前缀，调用方可立即复用该前缀。大 write 按 4096 B RM_CTP 限制有界分片，扩展身份头计入上限。驻留/远端窗口不足时短写或 WouldBlock，不分配整次 write 的缓存。
- `Read(span, now)` 复制连续字节并返回短读/EOF；无数据返回 WouldBlock。读到一帧中途继续持有原 RX Lease，完整消费后才归还物理责任和乱序窗口 receipt。已复制前缀之后发生错误，仍保留前缀返回，并锁存失败供下一次调用观察。
- `ShutdownWrite` 把 FIN 排在已接受 write 之后；发送半关闭仍可发送 Credit，接收方向继续工作。FIN 通过相同 SN 重排，前面全部字节消费后才 EOF。
- `Cancel`/不可恢复错误显式失败，不切 TCP、不重放旧 WR。`Close` 撤路由、有界清理未提交数据与未读 RX；已接受或接受未知的 DMA 依赖保留到可信终结。Retained 要继续交付完成后重试，不能销毁对象及其借用依赖。

`NativeTcpBackend` 提供数值地址的非阻塞 connect/listen/accept、有限字节/syscall 预算读写、EAGAIN、半关闭、EOF/reset 和取消。普通服务走 `Connection::OpenTcp`，不发送私有 HELLO；协议 TCP 则在双方 READY 后交接恰好读完握手字节的 fd。没有 DNS、透明切换或额外业务 framing。

## 单 owner 接线

宿主在控制路径创建 context、JettyPool、Slab、RX pool、总账和导入缓存。初始 RX 必须全部实际 post 并 `RecordInitialPosted`，之后才打开 `EagerReceiveDomain`。`RawConnectionPreparation` 中的接收域、路由、Credit 与 context 必须一致；发送提交器使用 `TxSender::Open(ledger, true)`，异常 post 仅封禁 SQ，阻塞的 `BeginDrain` 继续由 M3 控制执行器推进。

每轮按有界预算推进：

1. 原唯一完成 owner poll TX，将逐 WR `TxCompletionEvent` 按完整连接/操作身份交给 `Connection::OnTxCompletion`。每个 WR 独立持有 ImportLease 和 DMA 引用，CQE 验证失败不能释放；TX 成功也不返还远端 Credit。
2. poll RX 后，把实际接收路径已经校验的 `EagerReceiveSource` 与事件交给 `EagerReceiveDomain::Dispatch(..., Connection::DispatchRx, now)`。来源不能从 IMM/token 或预期连接反推。
3. `EagerReceiveDomain::Refill` 将真实归还的 Lease 补投，只把可信 accepted 前缀记录为可授信容量；剩余后缀保留。接受未知冻结并保留，不猜测回收。
4. 调用各 Connection 的 `Progress`，合并累计物理授信及应用消费窗口，再推进至多一个 TX 批次。部分 post 只继续明确未接受的原 SN 后缀，不重发已接受前缀。

使用 `RawContextProgress` 时，相关 `RawQueueBinding.external_refill=true`，禁止原自动补投绕过物理账本。CQ/通知/async 仍由原 owner 消费；M5.3.1 已提供 [连接 Runtime](connection-runtime.md) 的组内补投/连接调度与真实来源桥接。控制 fd readiness 交给 `OnControlReady`，随后 `Progress` 有界处理 raw Active 控制帧。M5.3.3 的 `LaneControl` 仅接受匹配身份和序号的 UPDATE/ACK；EOF、错误及其他帧显式失败。这些消息及断链都不构成 DMA 终结或远端撤权证明。

旧 token 即使路由被拒绝，真实 RX 也先扣原 Peer Credit、再归还/补投。无法归属的真实 RX 扣 unknown 责任并冻结域。重复 CQE 不重复扣一份物理 RQE。

## 已销毁接收域的账务关闭

每个 Credit 接收域只允许绑定一个 EagerReceiveDomain，重复绑定被拒绝。普通 Eager `Close` 成功后解除绑定，之后才可单独关闭 Credit；未完成关闭的异常析构保留本层元数据及绑定，宿主仍须保留全部外部依赖。

`EagerReceiveDomain::Freeze` 停止补投和新授信，现有应用 Lease 仍可 Release；已归还的 receipt 留在 pending-refill 队列，不能借实际关闭伪造一次 refill。普通 `Close` 仍要求 Held、pending refill 和 unknown 责任已结清。

M5.3.6 的 `CloseDestroyed(const raw::ReceiveDomainCloser&)` 提供独立控制路径：先由宿主取得 M1 要求的远端静默、事件确认、队列用户停止和 CQ 健康证明，停用对应 Runtime/raw 进度绑定，推进真实接收域关闭。只有 M1 已达到 `kClosed`，且匹配 Eager 捕获的 RxBufferPool successful-Open incarnation 和 Credit receive-domain incarnation，才能清理冻结账务。等待 Held、DeleteJfr 或 MR 注销失败时都保留原账并等待重试；已知残留 RX 为 0 也不能跳过实际关闭。

该入口先原子核验全部 queued receipt 的身份及 released 状态、全局 released 数量和零 Held，并确认没有仍绑定的 Peer 授信撤回协调器，再永久清退 Eager 与接收授信元数据。成功不补投、不产生可发放额度，也不猜测 unknown 的 Peer/类别。它不会释放仍 Held 的数据，不绕过故障 CQ 隔离，不自动撤路由、退休来源或关闭借用的连接/导入资源；宿主必须先停止这些访问。完整 Runtime 资源装配仍待后续接入。

## Peer 授信撤回

M5.3.7 的 [`PeerCreditControl`](peer-credit-control.md) 在独立已认证控制 fd 上停止整个 Peer 共账并交换最终接受计数，不能使用某条 Connection 的 Close 或 LaneControl ACK 代替。成功 Open 会 Stop SenderCreditAccount、FreezePeer 接收授信；同 Peer 的 Connection 会因账本停止/冻结而显式失败，宿主须推进这些连接 Close，归还缓冲 RX 并继续处理真实 TX 完成。这一路径不承诺把未提交业务数据发完，也不重放。

撤回期间整个 EagerReceiveDomain 必须继续处理真实迟到 RX、Release 和实际 refill；不要先 Freeze 整个域。接收端 DATA/CONTROL 消费量分别等于对端最终 spent 且全部 receipt 补投结账后，协调器才退休旧授信并 ACK。完成后仍需独立等待本地 DMA/import 引用、来源与事件责任收敛；失败保留冻结账本，Close 只解除控制器绑定，不恢复授信，也不产生 M1 销毁证明。新 epoch 的 Sender 额度不能被旧 Eager TX 流复用。

## 控制流的前进保证

只预留控制 WR 记录仍不够：数据可能占满注册 Slab，也可能把双向控制额度全部消耗，导致 Credit 自己无法发送。本实现同时约束内存、额度和共享发布状态：

- 每个 TX stream 在 Open 时预留一块注册控制缓冲；数据不能占用，可信完成后才复用。
- 每个 Peer 使用一致的 `control_credit_target`，至少 4，初始实际授信达到该配额；补投后剩余控制授信不高于一半时补至目标。基础数据额度也只在实际 refill 后重新授出；弹性额度的跨 Peer 调度仍由宿主处理。
- 最后一份发送控制额度保留给推进控制授信上界的 CREDIT_UPDATE，普通窗口更新和 FIN 不得消耗。至少两份的补额度批次避免空闲端点无限一对一回执。
- “已发布的控制授信上界”记在 `ReceiveCreditDomain` 的 Peer 共账，双方 READY 完成及可信 accepted CREDIT 才推进。不同逻辑连接重复同一个累计值，不得各自把它当成新额度而花掉最后的保留份额。
- 连接窗口和 Peer 物理账是两个门禁；Read 释放窗口不等于 RX 已补投，TX CQE 不恢复任何远端额度。
- 如果实际花掉最后控制额度的返还 WR 终结失败，共账的前进保证已经丢失；停止该 Peer 发送账并冻结对应接收授信，让受影响连接显式失败，不擅自退款或重放。普通 DATA 的 status 8 仍只失败所属连接，不修改共享 SQ。

这些控制预算要求属于 M5.2 连接层策略，不改变独立 M4 Credit 原语允许更小容量的测试契约。

## 验证与后续范围

测试覆盖双向短读写、真实 RX 乱序、调用方复用源缓冲、慢读持有、部分/未知 post、独立 DMA/import 引用、共享 SQ status 8、FIN、首包/READY 竞争、控制 EOF、双端延迟 Credit、共享 Peer 重复累计更新及热路径零分配。NativeTCP 包含真实 AF_INET 和 fork 双进程 echo；这些结果不替代 UB 真机或 40k 性能验证。

M5.3.1–3 已完成借用资源的 runtime 进度接线、HELLO 主动 Lane 交换和原子来源登记、动态控制 fd、有界公平连接调度及在线 UPDATE/ACK，见 [连接 Runtime](connection-runtime.md)。宿主须提供 `authorize_lane_update` 回调以显式启用在线更新；健康连接发现本地 Lane 换代后暂停投递，匹配 ACK 到达前不会使用新来源。未启用或更新失败时仍显式失败，已有失败连接不会被 ACK 恢复。旧来源继续保留供迟到 RX 记账。

M5.3.4 的 `ConnectionOwner` 已统一拥有固定容量的连接对象及来源适配器，提供带代次句柄的建链、读写、准备及关闭入口，自动完成调度注册与 Wake。连接 Close、来源退休和槽释放分别执行，迟到 TX/RX 责任收敛前不复用槽；详细接线见 [连接 Runtime](connection-runtime.md#连接对象所有权与入口)。

M5.3.5 已提供固定容量的 `ConnectionMailbox`，支持跨线程复制读写、半关闭、取消及状态命令；完成未领取持续占槽，短操作不自动重试，raw 命令遵守整个 context 的暂停屏障。

raw 资源自动装配、公平额度调度、显式 C ABI/零拷贝接口和完整关闭接线继续推进。Peer 授信撤回已提供独立协调器，尚未自动管理专用 fd 或接入 Runtime 调度；当前代码仍不根据 Connection Close、FIN 或控制断链撤销共账授信和旧来源绑定。工作区已有 API 草稿未作为稳定 ABI 发布。RNDV/Grant、主备选路、故障 CQ 回收和真实规模测试仍按原 roadmap 分期推进。
