# M6.2：发送端分块 WRITE 与完成证据

[`WriteRndvEngine`](../kbsocket/connection/write_rndv_engine.hpp) 接通现有 `TxSender`、`TxCompletionProcessor` 和注册 Slab，将一份已验证 Grant 对应的源区间拆成有界 WRITE。引擎记录每个分块的接受与终结，全部成功后才提供供 DONE 使用的证据。

当前是单 owner、单事务原语。它不发送 RTS/GRANT/DONE，不执行导入、分配流序号或推进接收重排，也不代替应用 Runtime。后续还需事务槽复用、段导入缓存、wire codec 和 Connection 接线。

## 准入、来源与分块

控制路径 `Open` 预分配固定容量记录并绑定发送器、Slab、完整 Lane 及事务身份；数据路径 `Start` 验证请求、构造计划和取得源引用。一个对象仅允许一次成功 Start，失败且完整回滚后可重试。`connection_id` 与 `transaction_generation` 由宿主分配且不得在旧完成可能存在时复用。

`Start` 的预期 `GrantTransfer` 来自已准入的本地请求，与远端返回的 Grant 必须完整匹配。不能把任意收到的 Grant 同时当作“预期请求”。宿主还须核验实际来源、Arena incarnation、导入 Jetty 和段之间的绑定，并保证该 Grant 只有这一份发送事务可以写入；单个引擎的防重复启动不能替代跨引擎的排他所有权。

最多接受 16 个源 Slice，每个包含 Slab Lease、偏移和长度。拼接后总长必须准确等于 Grant 长度。引擎提前检查全部源区间、注册 context、远端段覆盖范围及地址溢出，再预计算连续且不重叠的目标区间。每个 WRITE：

- 只使用一个远端 SGE；
- 本地 SGE 数不超过池能力和实现上限；超过时提前结束当前分块；
- 字节数不超过配置 chunk 大小、池的 `max_write_size` 和已确认的 200 MiB 契约；
- 尾块仅覆盖剩余字节，不靠对齐填充扩展授权范围。

全部分块必须能装入固定 `max_chunks`，其源责任也必须能装入 Slab 的引用表。默认预分配 256 个分块，硬上限 4096，宿主应按并发事务数和消息大小预算配置元数据。容量不足时不提交部分初始化的事务，不临时扩容，也不分配新的 payload 缓冲区。Start 失败只回滚本地源责任；如果远端已发布 Grant，仍须由协调层凭“未开始任何 WRITE”的可信事实完成授权取消，不能让源初始化失败悄悄回收目标。

每个分块的每个本地 SGE 都在 Start 时取得独立 DMA 引用和真实 SGE。源 CPU 所有权继续归应用，成功 Start 后可以提前归还 CPU Lease；既有 CPU 视图须停止使用。后续 Progress 使用已经保留的引用重新核验 SGE，检查 Slab 的停止/隔离状态，不依赖应用仍持有 CPU Lease。一个分块结束只解除它自己的引用，同一 Lease 的其他分块仍阻止内存复用。

## 提交、背压与故障

每次 `Progress` 最多调用一次 TxSender，数量受 `post_budget` 和 256 WR 上限约束，便于宿主在事务、小消息和控制流量之间轮转。当前事务固定使用准入时的完整 Lane，不自动迁移或重放到替换后的 Lane。

可信部分接受只推进已接受前缀，尚未接受的后缀保留原目标偏移及源引用；再次 Progress 只提交这部分后缀。额度不足或 provider 容量拒绝导致零接受时缩小后续批次，避免持续用超过 SQ 容量的批次原地失败。每次仍交还 owner，不在一次调用内循环重试。

提交结果未知或底层记账失败时，全部可能暴露的源引用提交并隔离，停止后续提交；不能根据返回码猜测它们未被硬件接受。尚未参与该次提交的确定未提交项可以撤销。引擎不提供自动修复 Prepared 账本的接口。

`OnCompletion` 只消费可信完成处理器的逐 WR 结果，验证 attempt ID、完整 Lane 和事务元数据后才解除对应源责任。status 8 使当前事务失败，不由引擎封禁整个共享 SQ；status 9 的 Lane 隔离仍由既有完成处理器执行。失败事务停止未提交后缀，但继续接收其他已提交分块的真实终结。

`Cancel` 也只撤销确定未提交项。已接受和可能已接受的 WRITE 继续保留引用。取消、空 CQ、flush 边界或 Lane 状态变化均不产生分块终结。

## 两类证据

`DoneEvidence` 仅在完整计划的所有分块均已明确接受、成功终结且未取消/失败时返回 [GrantTerminalEvidence](rndv-grants.md)。由于计划在 Start 时构造，既有逐 WR 结果能证明整个目标区间的完整覆盖；最后一个分块先完成不能提前解锁 DONE。

`TerminalEvidence` 只供接收端安全丢弃：未来提交已停止、接受集合明确且全部已接受 WRITE 已终结时才可返回。它允许取消、错误或零接受，但不能代替成功 DONE。宿主经完整身份与来源校验后，分别将两类结果用于 `GrantManager::CompleteDone` 和 `ConfirmTerminal`。

返回的是本地证据快照，不是已发送通知，也不消费控制 Credit。宿主仍负责后续控制帧的接受、引用及取消语义；引擎 Cancel 不能撤回已经交给外部或发布的通知。引擎从不直接调用接收端的 `EndGrant`。

接受未知后，本引擎永久拒绝生成这两类证据。即使外部凭独立诊断事实恢复 raw 账本，并收到足以解除所有本地引用的逐 WR CQE，也不能把本地源可回收自动提升为远端 Grant 已可回收。

## 生命周期与验证边界

源 Slab、发送器、账本、池、context/provider 和借用的已导入 Jetty/segment 必须由宿主持有至引擎 Close 成功。Close 可以取消未提交项，并重试已有可信终结记录的源引用清理；仍有未终结引用时保留内部状态，不能提前 unimport 或重建依赖。宿主还须遵守相关 context 的单 owner 与控制暂停屏障。

Mock 验证使用真实软件发送/完成账本，检查 WRITE 描述符和多 SGE 拼接，向实际 GrantManager 交付成功/清理证据，并覆盖乱序完成、部分接受、取消、status 8/9、未知接受隔离、源 CPU 提前归还及无分配路径。完整结果见 [roadmap](roadmap.md)。UB 真机和端到端 WRITE 通知回归仍未执行。
