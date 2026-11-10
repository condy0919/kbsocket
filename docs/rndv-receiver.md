# M6.3.5：接收端 DONE、Grant 借用与共同重排

[`RndvReceiver`](../kbsocket/connection/rndv_receiver.hpp) 将已校验的 RTS、接收端 Grant 和 [共同重排窗口](connection-primitives.md) 绑定。它使用预分配的事务槽，处理 DONE 后借出真实目标，并在应用归还时先结束 Grant 借用，再释放重排 receipt。本文组件不直接读取 JFC、不发布 GRANT，也未接入 Connection/Runtime 自动 RNDV。

## 成功通知的信任边界

DONE 的现有线格式只有完整事务、Grant 身份与长度，没有发送端 WR 计数。接收端不会补造 accepted/terminal/successful 数量。Open 要求宿主明确设置 `peer_successful_write_done`：对端已认证，并遵循全部 WRITE 成功、完整覆盖目标且停止后续写入后才发送 DONE 的协议。发送端已有 `WriteRndvPool::DoneEvidence` 和 `RndvNotificationTx::QueueDone` 提供该门禁；一个配置布尔值本身不完成认证或能力协商。

`OnDone` 将实际 RX 来源与固定会话路由、当前事务和 Grant 的全部身份比较。来源参数必须来自可信登记，不能照抄期望值。通过这些检查且 Grant 已发布后，才经 `GrantManager` 的私有通知入口解除远端访问责任。这个入口仅开放给接收协调器，不将普通 codec 校验自动升级为内存终结证明。已有 `GrantTerminalEvidence` 仍用于独立可信逐 WR 事实的验证与故障清理。

取消后收到合法迟到 DONE，只结束保留的责任并丢弃。错误来源、旧代次、长度/地址/Arena 不匹配及未发布授权的 DONE 均不能解除当前授权。终结以后，应用引用继续阻止目标复用。

## 准入与发布

Open 原子绑定同一连接的 GrantManager 和 ReorderWindow，核对完整身份及初始释放前缀；绑定期间拒绝第二个协调器接管及直接关闭依赖。Grant 管理器的事务准入、完成、借用和归还由协调器独占；通知发送器仍是唯一的 GRANT 发布者。两者及底层 Slab 必须覆盖所有未完成责任。

1. `OnRts` 先校验线格式和来源，再取得固定事务槽与 `ReserveRndv` 占位，按全部业务长度收走共同 SN/字节预算。
2. `PrepareGrant` 从真实注册 Slab 准入目标；暂时资源不足时保留 RTS 和共同占位，可以重试。不会撤销活跃流中的占位再复用 SN。
3. 宿主将返回的同一 GrantReservation 交给 `RndvNotificationTx::QueueGrant`；发布者在任何可能的 SEND 前提交远端责任。重复 Prepare 只返回原授权，不授予第二个发布权。
4. 正常 DONE 完成目标后，协调器调用真实 `Borrow`，以 GrantHandle 与只读 span 完成原重排占位，不复制 payload、不再次计入业务字节。

物理 RX 及 Credit 扣账、RTS/DONE 包的 Lease 归还与实际补投仍由接收域处理。OnRts 尚未准入时，调用者继续持有输入责任；不能因为返回背压就丢弃已接收请求，也不能重放已接受的请求。能力、Arena 目录与完整连接装配将在后续阶段协商这些容量限制。

## 有序借用与归还

`ReorderLease` 新增明确的 `GrantHandle` 分支，只允许用于 `kValidatedRmaDone`。窗口检查该 Grant 确实处于 Borrowed 状态，payload 必须恰好是对应目标的只读范围；不会构造假 BufferLease，也不允许用 Grant 借用伪装 Eager 或 FIN。

宿主继续从同一个 ReorderWindow Poll：未完成 RTS 阻止后继 Eager/FIN，乱序 DONE 可以就绪但不能越过前面的缺口。每个 Grant delivery 必须交给 `RndvReceiver::ReturnGrant`：

- 先校验 receipt 确实由当前窗口 Poll/Drain 交出，并逐项比较完整 frame、Grant 代次、span 和 discarded 状态。
- 再执行真实 Grant Return；合法归还开始前，应用必须结束对旧 span 所有副本的访问。
- 最后释放重排 receipt。分阶段保存进度，失败重试不会重复归还目标；目标可能已经归还，错误返回不恢复借用，只允许重试剩余清理。

Grant tombstone 只跟随共同 `released_prefix` 推进，包含真正归还的 Eager/FIN。Poll、DONE 到达或单个乱序归还都不代替连续释放前缀。每次推进最多一个 Grant 窗口，落后较远时由有界 Progress 继续追赶。

## 取消与关闭

Cancel 使整个逻辑接收流失败并停止新准入。未发布目标可撤销，已发布目标隔离等待合法迟到 DONE 或独立可信终结证明；清除 RTS 占位只释放元数据，不证明远端停止访问。已借用并进入窗口的目标必须经过真实 Poll/Drain 和 ReturnGrant，不能伪造 receipt、抢回应用视图或借 Close 提前释放。

Close 没有活动 RNDV 责任时仅结束协调器；存在活动责任时先取消并尝试清理，未终结 Grant 或未归还借用导致保留。它不代替外部 GrantManager、ReorderWindow、注册内存或 Runtime 的关闭。析构保留内部状态也不延长外部依赖的 C++ 生命周期。

远端 WRITE/DONE 与归还可能早于本地 GRANT SEND CQE。通知发送器的取消现在只处理尚未完成的发布责任，不取消 Ready/Borrowed 目标；旧前缀已经退休时也不触碰复用后的新授权。通知源 DMA 仍单独等待真实 CQE。

## 当前范围

测试使用真实 Slab/Grant/重排账本与 Mock URMA 注册，覆盖来源和身份校验、资源背压、乱序混合交付、取消、迟到通知、借用保留、槽复用及热路径零分配。结果记录在 [roadmap](roadmap.md)，不代表 UB 真机验证。

现有 EagerRxStream 仍只处理其物理 RX 包归还，不能把它直接接到混合窗口上就认为完成了应用流集成。M6.3.6 已提供 [RNDV 能力/Arena 协商](rndv-negotiation.md)；后续仍需接入通知与事务调度、混合流短读及零拷贝归还、Connection/Runtime 分发和关闭流程。
