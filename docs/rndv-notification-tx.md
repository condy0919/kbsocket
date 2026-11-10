# M6.3.4：RNDV 通知发送与未来控制额度

[`RndvNotificationTx`](../kbsocket/connection/rndv_notification_tx.hpp) 将 [RTS/GRANT/DONE 线格式](rndv-protocol.md) 提交为实际 SEND_WITH_IMM，并跟踪各 WR 的源 DMA、物理授信和退休。它是单 owner、固定 Lane 的有界组件；M6.3.9 的 [Connection/Runtime 装配](rndv-connection.md) 已绑定接收协调、能力协商与发送事务。当前通过 `RndvConnectionConfig` 显式启用，限定单 Arena、固定 Lane；默认连接及 `RawStreamRuntime` 便捷入口仍使用 Eager。

## 资源与准入

Open 预分配通知槽和每槽注册源 buffer，独立持有 endpoint import 引用。通知使用全 signal 单 WR，每次 Progress 最多提交一个，接受后绝不重放或迁移。配置中的 `connection_id` 是完成分发 owner 身份，必须与共用账本的 Eager/WRITE owner 区分；宿主继续负责 context 暂停屏障、固定 Lane 有效性及依赖生命周期。

`QueueRts` 原子取得 RTS 与未来 DONE 两个槽、DONE 控制额度和共同 `TxSequenceWindow` 中的一个总长度 SN。首次成功准入绑定唯一的序号窗口对象，事务必须匹配同一发送方向、下一 SN 和完整身份。RTS 提交时才取得 data 物理额度；可信接受后推进共同 SN，GRANT/DONE 不额外消费逻辑 SN 或业务字节。

未来 DONE 不能长时间占用 `SenderCreditAccount::Reserve` 的即时 post 预约，否则同 Peer 的其他连接与 CREDIT 发送都会被阻塞。新增 `ControlCreditLease` 直接从 Peer 共账预留一个未来控制额度，最多 256 份，始终留下最后一个 CREDIT 逃生额度：

| 操作 | 责任变化 |
| --- | --- |
| `ReserveControl` | 可用 control 减一，增加未来承诺；其他普通 post 仍可进行 |
| `ActivateControl` | 将原承诺转为一个即时 post 预约，不重复扣账 |
| `Resolve(..., 0)` | 恢复原承诺，允许可信零接受后的重试 |
| `Resolve(..., 1)` | 消费承诺，累计 control spent 增一 |
| `Resolve(..., nullopt)` | 保守消费并永久标记接受未知、停止共账新发送 |
| `ReleaseControl` | 仅归还未激活的承诺，不改变 spent |

普通 Reserve 看不到已承诺额度；Stop 后可以释放承诺和结清已经发生的 post，但不能再激活。Peer 撤回与 Close 必须等所有未来承诺结束，不能遗漏稍后仍可能发送的 DONE。票据代次跨账户重开保持单调。

## 三类通知的责任

`QueueGrant` 校验真实 `GrantManager` 中尚未发布的完整授权，取得通知槽与控制票据。它独占此 Grant 的发布权；任何可能的 provider 提交之前先 `CommitPublication`，不能绕过该队列发送同一授权。准备失败或从未暴露的已知零接受可安全撤销；已接受或接受未知则取消只能隔离 Grant。

`QueueDone` 只填充原 RTS 绑定的未来槽，重新从真实 `WriteRndvPool` 读取成功证据，核对完整事务。全部 WRITE 成功前、取消后、接受未知或身份不符均不能生成成功通知。成功排队保存不可变快照，宿主可以随后释放 WRITE 本地槽；通知仍独立保留源 buffer、endpoint pin 和控制责任。

通知返回已知零接受时可在原槽重试，并保留原 SN；一旦接受，等待 `TxCompletionProcessor` 的可信终结事件。完成同时核对账本、attempt、槽代次、Lane、目标和源 Lease 身份。迟到或重复完成不能释放新槽。status 8 使相关通知队列失败；共享 SQ 的故障分类仍由 TX processor 执行，不能因单个远端失败自动排空健康连接。队列失败后禁止新提交，宿主须继续消费真实完成，并通过 Cancel/Close 结清其余预留和发布责任。

**本地 GRANT SEND 完成不等于远端 WRITE 结束。** 通知源 DMA 退休后可以释放本地通知槽，已发布目标继续归 `GrantManager` 所有。Cancel、Close、超时、空 CQ 或 notification CQE 都不代替远端访问终结证明。

## 停止与关闭

Stop 阻止新 RTS/GRANT，已准入事务仍可 QueueDone 和推进。Cancel 停止未提交通知，保留已接受 DMA 至真实 CQE；取消 RTS 同时取消尚未提交的配对 DONE，反向取消未来 DONE 也撤销尚未发送的 RTS。RTS 一旦接受，取消后续 DONE 必须使共同序号流失败，即使 RTS 槽已经 Release，也不能留下可继续发送的永久缺口。每个本地槽通过显式 Release 回收，槽代次防止旧句柄访问新事务。

Close 直接释放已知成功终结的本地通知，对其他条目尝试取消并结束本地责任；存在在途或接受未知时返回保留。已成功发送的 GRANT 继续由外部 GrantManager 管理，Close 不把它重新取消或推断为可回收。接受未知永久保留通知状态、源内存及 endpoint pin，不能靠后续猜测解除。外部 sender、Slab、cache、credits、序号窗口及 GrantManager 必须覆盖其责任生命周期；析构保留内部状态不替宿主延长外部对象生命。

通知队列允许一个发送协调器与一个反向接收 ingress 同时借用。发送绑定核对相同 sender、Slab、endpoint 缓存及共同序号窗口；已有独立 RTS/DONE 时拒绝接管，已有接收侧 GRANT 可以共存。任一绑定存续时 Close 在 Stop 或取消之前返回 InUse。发送协调器只管理自己准入的 RTS/DONE，接收 ingress 继续负责其 GRANT job；宿主不得重复分发共用通知的同一 CQE。

## 后续边界

本阶段的 Mock 回归验证实际 raw 提交、逐 WR 完成与 Slab/import/credit 账务，不代表 UB 真机通过。M6.3.5 的 [接收协调器](rndv-receiver.md) 已提供可信 DONE 到 Grant 终结、只读借用及共同重排归还的适配。当前 DONE 线上没有 WRITE 数量，接收端采用私有无计数入口；GrantHandle 作为独立的借用类型进入重排，span 不冒充 BufferLease。M6.3.6–M6.3.8 已接通能力/Arena 协商、GRANT job、源准入、RTS/GRANT、导入等待、WRITE 和真实完成后的 DONE。M6.3.9 的 Connection 负责通知推进和精确 owner 分发，Runtime 负责有界调度及共享暂停域中的段导入；宿主仍须驱动 Runtime 并保留所有依赖，未启用时不会公告 RNDV 能力。

远端 DONE/应用归还可以早于本地 GRANT CQE；通知取消只隔离仍未完成的发布责任，不抢回 Ready/Borrowed 目标。已退休的旧 Grant 即使被复用，也不会因迟到本地错误而影响新授权。GrantManager 必须存活且保持打开，直到相应通知记录结束。
