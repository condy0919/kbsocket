# M6.3.2：可复用 WRITE 事务槽与导入引用

[`WriteRndvPool`](../kbsocket/connection/write_rndv_pool.hpp) 将已有分块 WRITE 引擎、Jetty 导入缓存和远端段缓存组合成单 owner 的有界事务池。一个池绑定完整连接身份、目标 Arena、两份精确导入键及固定 Lane；不发送 RNDV 控制帧，也不自动迁移到新 Lane。

## 两阶段准入

Open 在控制路径预分配活动引擎槽、逻辑 SN 窗口和远端 Grant 代次记录。默认 4 个活动槽，每槽最多 64 个分块；上限为 128 个槽，且全部槽的分块数乘积不得超过 4096。每个分块包含多 SGE 和独立源 DMA 责任，宿主必须按实际元数据预算配置，不能为 40k 条空闲连接各自自动创建默认池。跨连接共享总预算与规模验收仍待 Runtime 集成阶段完成。

`Reserve(expected_transfer, grant, endpoint_lease, segment_lease)` 校验已准入的本地请求、完整 Grant 身份、SN 窗口以及两种缓存的完整键和实际句柄，分别 Retain 独立引用，然后返回事务句柄。原缓存票据仍由调用者持有。缓存尚未 Ready 时返回 WouldBlock，不占逻辑 SN；该池不执行或调度导入，宿主继续通过共享 context 暂停域推进缓存。

`Start(handle, sources)` 才构造分块计划并预留源引用。应用必须持有源 CPU Lease 至 Start 成功；之后可以归还 CPU Lease，由引擎保留每个分块的 DMA 责任。若 Start 因源范围或引用容量失败，事务句柄和导入引用仍在，可以重试或显式取消；回滚未完成时保留责任，不能释放槽位冒充未准入。

Reserve/Start/Progress、完成处理、取消、Release 和窗口推进均不分配或释放内部存储。公共 `WriteRndvEngine` 仍是一次成功 Start 的原语；只有池能够在责任结束后调用其私有复用入口，保留预分配分块数组。

## 提交、失效和独立责任

每次 `Progress(handle)` 至多执行一次有界 post。准备新的 WRITE 前，两种缓存都须重新 Get 并通过身份检查；缓存失效使未提交后缀取消，已经接受的分块仍可通过 OnCompletion 正常终结。全部 WR 已提交后，不会仅因缓存失效而丢弃已取得的真实完成事实。

池持有的缓存票据与源引用、远端 Grant 是三份独立责任。应用归还原导入票据、缓存失效、连接取消或超时，都不能提前解除池的 pin。只有接受集合已知、未来提交停止且本地责任全部结束，Release 才逐份归还缓存引用并释放物理槽。缓存仍可保留零引用暖条目，最终 unimport 由控制执行器完成。

status 8 使相应事务失败，不扩散为整个共享 SQ 失效；status 9 的 Lane 隔离继续由 TxCompletionProcessor 执行。池不重放已经接受的分块，不重试未知接受，也不在替换后的 Lane 上恢复旧事务。

接受未知的事务永久保留本池槽和两份缓存 pin。即使外部以后得到足以解除部分或全部本地源引用的事实，也不能仅凭计数归零把它转为可复用槽或生成远端 Grant 终结证据。

## 复用与旧消息防护

活动引擎槽与逻辑 SN 窗口分开。每次准入使用严格递增事务代次；OnCompletion 先匹配连接和事务代次，再由引擎检查 attempt ID、完整 Lane 和逐 WR 元数据。Release 后的旧句柄、旧完成不能作用于复用槽中的新事务。

Release 保留 SN tombstone。只有宿主确认此前全部逻辑 SN 已消费，才可调用 AdvanceWindow 推进连续前缀；活跃责任使推进原子拒绝。窗口下界单调，不允许 SN 回绕或使用保留的 FIN 序号。这个前缀必须涵盖 Eager 等其他消息，不能直接使用 RNDV CQE 到达顺序。

每个远端 Grant 槽保留已准入代次的高水位，不随 SN 窗口推进清除。同代次的 Grant 不能换一个 SN 重新启动；同一远端槽不能同时用于两个活动事务，活动目标区间也不能重叠。这些检查只覆盖当前池；宿主仍须认证远端 Arena/会话，保证跨池的独占写权，并避免跨池生命周期复用连接 ID。

## 证据、取消与关闭

DoneEvidence 只在全分块成功且未取消/失败时返回；TerminalEvidence 只用于安全丢弃。尚未 Start 的事务在显式 Cancel 且本地预留全部清理后，可以产生已知零 WRITE 的清理证据。两类证据均是本地快照，不是已发布通知。

成功证据保留至显式 Release。宿主须先保存后续通知所需证据，再释放本地槽；Release 不调用接收端 EndGrant，也不证明远端应用已消费。缓存引用清理按份记录，失败时可以用原句柄重试，已归还的引用不会再次释放。

若已收到可信终结但源责任清理失败，Release 只重试引擎已经保存的终结事实，不补造 CQE 或改变接受集合。责任仍无法核销时继续保留；接受未知在任何清理重试之前即被拒绝。

Stop 仅禁止新的 Reserve；已准入事务可以继续 Start/Progress 或取消。Close 是控制路径操作，会停止准入并取消全部事务；未终结或接受未知的事务使 Close 失败，析构保留池内部状态，宿主仍须保留全部外部依赖。需要成功通知时应先取得证据并正常 Release，而非用 Close 替代成功收尾。

## 验证与范围

Mock 测试使用真实 Slab、发送/完成账本、两种导入缓存和 ControlExecutor，验证槽位复用、身份拒绝、旧完成、缓存失效、部分接受、取消及零分配路径。验证结果记录在 [roadmap](roadmap.md)。

本阶段尚未开放 RTS/GRANT/DONE wire codec、统一 Eager/RNDV SN 准入、RawDomain/RawStack 自动装配及应用 RNDV 入口。UB 真机、端到端 WRITE 通知和规模性能回归均未执行。
