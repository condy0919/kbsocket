# M5.3.9：raw 接收域资源所有权

[`RawDomain`](../kbsocket/runtime/raw_domain.hpp) 在控制路径创建并拥有一个 raw 接收域，统一管理队列、RX 注册内存、TX 账本、接收授信、路由与来源表，并把真实物理关闭和 Eager 元数据清退接起来。`RuntimeShard` 仍只借用这些对象推进。M5.3.10 的 [RawStack](raw-stack.md) 可进一步拥有 context/会话、共享 Slab/import、逻辑连接和撤回调度器；独立使用 RawDomain 时这些依赖仍由宿主拥有，Peer 发送账本与 provider 加载责任继续由宿主管理。

## 创建与接线

`Open(context, capabilities, config)` 要求能力来自同一物理设备，且 `local_domain` 与 `context_incarnation` 在宿主生命周期内唯一。内部 State 的地址固定，成员不能单独重新 Open 或 Close。一个 RawDomain 仅承载一次生命周期；成功关闭后重建须使用新对象与新身份。

创建顺序为路由表、JettyPool、接收 Credit、AttemptLedger/延迟排空的 TxSender、RxBufferPool、来源表。初始 RX 分批实际 post，按可信接受数记入 DATA/CONTROL 总账，全部成功后才打开 EagerReceiveDomain 并发布 `resources()`。数据/控制共用物理 JFR，分类是授信账务配额，不是给物理槽位指定报文类型。

| 入口 | 所有权与作用 |
| :--- | :--- |
| `Open` | 预分配并创建一个接收域，初始投递失败也保留关闭所需的稳定对象 |
| `resources` | 返回本域成员的借用视图，供 HELLO、连接准备、来源登记、授信与 Lease 归还使用；Open 失败或已关闭时拒绝 |
| `Bind(progress)` | 登记 Eager 补投源，生成 TX/RX/Error 回调及 `external_refill=true` 的 RawQueueBinding；调度器必须属于同一 context |
| `Inspect` | 返回创建/关闭阶段、真实 Posted/Held、TX/路由/来源责任、初始接受是否确定及原始创建错误 |
| `BeginClose` | 永久停止新路由、接收授信和 TX 准入；已有 RX 消费、归还、补投仍可继续 |
| `AdvanceClose` | 在独立证明和计数门禁满足后推进 M1，再清退 Eager/Credit 和其余元数据 |

宿主先创建 context，打开 RawDomain 和同 context 的 ConnectionProgress，再将 Bind 返回值放进 `RawContextBinding.queues`，把连接、导入、mailbox、Peer 撤回源放入同一个 context 暂停组。Slab/导入/ConnectionOwner 仍按 [连接 Runtime](connection-runtime.md) 装配。Bind 只生成和登记桥接，不自动打开 RuntimeShard；后续装配失败也必须沿本域显式关闭路径处理。

所有接口服从单 owner/context 暂停门禁，不能在另一个控制任务访问该 context 时 Inspect、操作借用视图或关闭。Open/Bind/BeginClose/AdvanceClose 仅在控制路径运行，不能放进 Tick 回调或业务 mailbox；数据推进继续使用原有预分配组件。

## 创建失败

参数预检失败时尚未分配 State，`stage=kEmpty`，可修正参数重试。一旦分配 State，失败会记录 `kOpenFailed`，同一对象不能重开；宿主通过 BeginClose/AdvanceClose 处理已创建资源。底层队列创建失败自身的回滚错误保存在 `queue_error.cleanup_error`，不能只处理首个错误。

已知初始 post 部分接受只记录真实前缀，不将错误解释为全部未接受。接受范围未知时，RX 池保守保留整份可能已接受集合，`initial_accepted_known=false`；`initial_accepted` 仅是此前可信前缀，不能当作全部 DMA 责任。此时不发布端点/授信或正常资源视图，不重投可疑 WR；保留至真实 DeleteJfr 屏障，再注销 MR。

尚未成功打开 RX 的创建失败也需宿主确认事件与外部使用者退出，随后重试底层关闭。创建错误后的清理不是后台任务；调用者不能在 Open 返回错误后立即销毁借用 context/provider。

## 关闭顺序与独立门禁

1. 宿主停止新连接与业务命令，调用 BeginClose。它使用 `ReceiveCreditDomain::StopAdmission` 拒绝 Admit/Grant，但不提前 Freeze 整个 Eager 接收域。继续消费迟到 RX、归还 Lease 和真实补投，允许已启动的 [Peer 授信撤回](peer-credit-control.md) 收敛。
2. 关闭逻辑连接并处理真实 TX 终结；按各自证明退休来源，Release 连接槽。完成或失败的撤回控制器仍须 Release/Abort 解除账本绑定。协议失败本身不授权删除。
3. 在原 owner 仍能推进时，检查 TX 账本为零、`JettyPool::CheckTxDrained()` 成功。ERROR Lane 即使没有票据，也须先消费 flush 边界并完成软件排空。完成全部控制任务收割，然后成功关闭相关 Runtime/raw 进度绑定；保持 ConnectionProgress 对象存活供最终解除 refill 注册，期间允许先 Close 但禁止重开。
4. 传给 AdvanceClose 的 `progress_detached` 必须代表实际解绑、任务收割及不再发生回调。它还无副作用地检查 routes/sources/ledger/withdrawal_bindings 为零和完整 TX 排空，然后调用 `ConnectionIoBinding::Close(true)` 移除补投注册。空计数不能替代宿主线程/事件屏障。
5. 独立提供 M1 的 `peer_stopped`、`events_acked`、`queue_users_stopped`、`completion_queues_healthy`。只要进入删除阶段，这些证明在每次重试时仍须有效。确认远端停止后才停止 RX 补投；按关联 Jetty → JFR → 残留 Posted 清退 → 等待 Held → 注销 MR → JFC/JFCE 的顺序关闭。
6. 只有 M1 实际 Closed 后才调用 EagerReceiveDomain::CloseDestroyed，核验本代 RX 与 receipt 后清退接收 Credit，再关闭来源、路由、Sender 和账本元数据。context/provider 最后由宿主释放。

Peer ACK 只证明对应授信关系结束，不自动生成整个域的远端静默、TX DMA、通知/async 或独立 RMA Grant 的终结证明。撤回控制器仍绑定时禁止物理删除，即使 active_peers 已归零；反之，外部真实物理关闭证明允许终结无法正常撤回的旧授信，不能只靠 active_peers 数量判断。

`queue_users_stopped` 还必须覆盖 ConnectionOwner 槽与外部 Slab/import/Grant 的访问责任；空 routes/sources 不能代替它。关闭期间旧视图仅可用于明确允许的责任终结，例如最后归还 Held；已关闭后全部借用视图及缓存的 RawQueueBinding 失效，不能再次注册或使用。

M1 排空期间收到的迟到 RX 不再交给业务，按未知来源记账并归还 Lease；尚未打开 Eager 的失败创建则直接归还 raw Lease。收到 RX 的那次调用不执行删除，宿主须重新确认事件屏障。未能可靠释放 Lease 或 CQ poll 故障会保留整域，不猜测退休责任。

## 保留与重试

DeleteJetty/DeleteJfr/UnregisterSeg/删除完成队列失败均保留当前阶段，下一次调用只继续未完成部分。Held 可跨越 JFR 删除，但 MR 与 backing 仍存活，最后一个 Lease 归还后才能继续。冻结、已释放未补投及未知来源 receipt 由物理关闭后的清退路径终结，不为了清账重新 post 或 Grant。

故障 CQ 或不可信终结证据进入 `kRetained`，不提供强制清零、在线回收或恢复。未成功 Closed 就析构时保留整份 State，避免分别析构成员破坏 DMA 依赖；这不是正常关闭，也不接管外部 context/provider、Slab、导入或回调借用者。宿主必须维持所有相关外部依赖。

本次 Mock 验证覆盖初始批次/部分和未知接受、创建及回滚失败、Runtime 接线、TX/来源/路由/撤回门禁、迟到 RX、Held 和删除/注销重试。M5.3.10 已在 RawStack 汇总多 context 和共享资源的关闭门禁；应用 C ABI 留给后续阶段。UB 真机与 40k 验收未执行。
