# M6.3.8：有界 RNDV 发送事务协调

[`RndvTxCoordinator`](../kbsocket/connection/rndv_tx_coordinator.hpp) 将源区间准入、RTS、GRANT 验证、远端段导入、分块 WRITE 和 DONE 通知串成一个单 owner 事务。它组合 [通知发送器](rndv-notification-tx.md)、[WRITE 事务池](write-rndv-pool.md)、共同 `TxSequenceWindow` 与导入缓存，绑定一条连接、一个远端 Arena 和一条完整 Lane。

这是可独立装配的发送路径。M6.3.9 的 [Connection/Runtime 装配](rndv-connection.md) 已接通协商资源、应用发送及关闭；`Connection::Open` 或 `ConnectionOwner::AdoptProtocol` 须显式传入 `RndvConnectionConfig`，当前限定单 Arena、固定 Lane。默认连接和 `RawStreamRuntime` 便捷入口仍使用 Eager，多 Arena 选择尚未接通。当前实现和 Mock 证据不代表 UB 真机验证通过。

## 准入与源内存

控制路径 `Open` 预分配固定事务槽，并核对 WRITE 池与通知队列的 endpoint、Lane、发送方向、发送器、Slab、缓存和共同序号窗口。两者使用不同的 `connection_id` 分发实际完成；发送方向的 GrantBinding 与反向 GRANT 通知的路由分别保存，不能混用。

`Submit(sources, now_ns)` 最多接受 16 个源 slice，每项包含 Lease、偏移及长度。源必须属于绑定的 Slab，仍可取得 CPU 只读视图，完整拼接长度不得超过协商上限；同一 Lease 的多个 slice 合法。协调器先检查全部源范围，再按实际 chunk 大小和 SGE 限额预计算计划，超出 `max_chunks` 时在发布 RTS 前拒绝。

成功准入为每个 slice 取得独立 Reserved 本地 DMA pin，再通过 `QueueRts` 原子预留 RTS、未来 DONE、未来控制额度和共同序号。`Submit` 成功后应用即可 `ReleaseCpu`，不必等待 GRANT；此前取得的 CPU 视图必须停止使用。排队 pin 保证源数据在导入等待或背压期间不会复用，也禁止重新取得 CPU 可读写视图。正常准入失败完整回滚这些 pin，不消费共同序号；内部责任核销失败则返回 Retained 并保留依赖。

默认活动容量为 4，上限为 128，同时受 WRITE 池活动槽和协商 `max_grants` 限制。准入还检查共同 SN 窗口、活动事务总字节和 Slab 引用表余量：

```text
available_references >= source_count + source_reference_budget + 1
source_reference_budget = max_chunks × min(16, max_send_sge)
chunk_bytes_limit = min(config.chunk_bytes, provider.max_write_size, 200 MiB)
```

这里保守保留一个最坏事务的分块引用和一个通知提交引用，避免排队源占满引用表后所有事务都无法推进。它不等于为所有活动事务预付完整 WRITE 引用；分块准入仍可背压。宿主还须为通知 buffer、控制额度、缓存票据及其他共用组件提供整体预算。完成但尚未显式 `Release` 的事务继续占活动槽和字节预算。

## 从 RTS 到真实 DONE 完成

1. 宿主推进 `RndvNotificationTx::Progress`，将 RTS 提交为实际 SEND_WITH_IMM。可信接受才推进共同发送 SN；通知队列独立等待真实 CQE。
2. `OnGrant` 使用已准入的原始 `GrantTransfer`、配置的反向路由、真实 `WireReceiveContext` 和固定 Arena 校验 GRANT，并检查其槽位与协商窗口对应。重复、未知事务或错身份授权拒绝。RTS 已明确接受后允许 GRANT 早于本地 RTS CQE 到达，既不要求网络与 CQE 同序，也不把 GRANT 当作本地完成。
3. 协调器调用可信 `resolve_import` 回调。宿主可将它适配到已认证且已完成双 READY 的 `ConnectionHandshake::PrepareRndvImport`，按 [能力与 Arena 目录](rndv-negotiation.md) 生成请求。回调不得调用慢 provider、保存输入引用或重入协调器；WouldBlock 必须可无副作用重试。协调器再次核对完整段缓存键和目标范围，不能接受任意网络字段构造的导入请求。
4. `TsegImportCache::Acquire` 按需取得段票据；尚未 Ready 时保留事务并返回 owner。宿主通过既有控制执行器和共享 context 暂停域执行真正的 import。协调器不自行暂停 context，也不在数据路径直接调用 provider。
5. 段 Ready 后，WRITE 池 `Reserve` 独立保留 endpoint 与 segment 引用，`StartPinned` 从原始排队 pin 派生每个分块的 DMA 责任。只有派生成功后协调器才取消原始 pin；即使 CPU 已归还，也没有允许源 backing 被复用的空窗。
6. `Progress` 每次最多扫描给定预算的槽，每槽至多执行一次有界 WRITE post。真实 WRITE CQE 经 `TxCompletionProcessor` 和 `OnCompletion` 交给事务池；全部分块成功后，协调器取得真实 `DoneEvidence` 并调用 `QueueDone`。通知队列再次从真实 WRITE 池验证证据，成功排队后保存不可变快照。
7. DONE 排队后可以结束 WRITE 本地槽及其导入引用，但事务成功还必须等待 RTS 和 DONE 的真实 SEND 完成，并结束独立通知责任。`status.complete && status.success` 只说明本地发送事务成功收束；它不证明远端应用已读取或归还目标。

`OnGrant` 只复制授权值，不保存 RX span。宿主可直接使用 `DispatchGrant` 作为 `RndvRxIngress` 的 `on_grant` 回调；该适配器在协议错误时同时取消本逻辑发送流。直接调用 `OnGrant` 的宿主负责处理其错误，不能忽略校验失败继续沿该流发送。

## 宿主进度与生命周期

协调器不接管整个 raw owner 循环。宿主继续串行推进以下责任，并遵守 context 暂停屏障：

| 进度源 | 宿主责任 |
| --- | --- |
| 通知队列 | 显式调用通知 `Progress`，提交 RTS、GRANT 和 DONE；协调器的 `Progress` 不代调 |
| 导入缓存 | 调度 endpoint/segment 控制任务，在相同 context 的暂停域内执行并收割结果 |
| 物理 TX CQE | 从 `TxCompletionProcessor` 取得可信逐 WR 事件，再调用协调器 `OnCompletion`，按 WRITE/通知 owner 分发一次 |
| RX 分发 | 真实来源交给 [接收 ingress](rndv-rx-ingress.md)，由 `DispatchGrant` 交付 GRANT；双向 RTS/DONE 继续走接收侧 |
| 事务推进 | 周期性传入单调 `now_ns`，调用协调器 `Progress` 并处理超时、背压、完成与 Release |

通知队列可以与反向接收 ingress 共用，GRANT job 不排斥发送协调器绑定；共用通知 CQE 不应再重复交给通知队列。Open 独占借用 WRITE 池及本地发送协调器绑定，阻止第二个发送协调器接管同一资源。绑定期间池或通知队列的 `Close` 在停止/取消之前返回 InUse，避免外部关闭破坏仍在使用的事务。该借用检查不提供跨线程同步；所有者、Slab、缓存、发送器、序号窗口和 provider 仍须由宿主保留至相应 Close 成功。

`status.sources_released` 表示协调器和 WRITE 池的源责任已经结束，不替应用归还仍持有的 CPU Lease。`Release(handle)` 只回收已本地完成的事务记录，槽代次阻止旧句柄访问新事务。它不释放远端 Grant，不推进远端消费窗口。

## 取消、故障与窗口推进

`Stop` 仅禁止新的 Submit，已准入事务仍可完成。`Cancel`、超时或实际提交/完成错误使整个逻辑发送流失败并停止共同 SN；确定未提交的排队/分块引用可以取消，已经接受或接受未知的 DMA 继续保留。协调器不因一个逻辑流失败而排空健康共享 SQ，也不在新 Lane 上重放旧事务；status 8/9 的物理分类仍由 TX 完成层执行。

源 pin、WRITE 导入引用、通知源 DMA/控制额度和远端 Grant 分别核销。RTS 已被远端接收却尚无 GRANT、GRANT 早到、DONE 已排队和本地通知尚未退休，都是可能存在的独立状态。取消不自动发送远端授权撤销，也不能用本地控制断开、超时、空 CQ、flush 边界或原始 pin 取消来推断远端访问已经结束。

`TerminalEvidence` 只供可信宿主故障清理：已知 GRANT 且唯一 owner 停止、明确零 WRITE，或者已知接受的 WRITE 全部真实终结后才可取得。没有收到 GRANT、未知 WRITE 接受或底层回滚责任不完整时不提供证明。即使能证明 WRITE 已终结，也不能据此伪造成功 DONE；通知接受未知仍会独立阻止本地事务 Close。

`AdvanceWindow(consumed_prefix)` 必须由宿主依据远端连续消费事实显式调用，且覆盖共同流中的 Eager SN。WRITE 完成、DONE SEND 完成和本地 Release 都不能作为该前缀。窗口不能越过尚未本地完成的事务或已分配序号上界，WRITE 池保留 SN tombstone 与 Grant 代次高水位，防止释放后重放旧授权。

Close 先停止新提交；有未完成事务时走取消并继续保留未终结责任，成功后才解除借用并归还协调器独立的 endpoint pin。析构失败保留内部状态，不能替宿主延长外部依赖生命。当前取消没有跨网络无损恢复或数据重放协议。

## 验证范围与下一步

验证基于 Mock URMA 和真实软件 Slab、缓存、通知队列、发送/完成账本及事务池；以源码回归和 [roadmap](roadmap.md) 的验证记录为准。本阶段不增加真机通过结论。M6.3.9 已将双方协商、收发协调器、调度、错误传播和关闭接入显式启用的 Connection/Runtime；`RawStackConfig::enable_rndv` 提供共享段缓存及生命周期门禁。多 Arena 选择、固定 Lane 以外的策略和便捷入口启用仍留待后续。
