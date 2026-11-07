# M5.3.7–8：Peer 授信撤回与 Runtime 调度

[`PeerCreditControl`](../kbsocket/control/peer_credit_control.hpp) 在独立、已认证的非阻塞控制流上交换停止快照与确认，撤回一个 PeerRuntime/接收域/epoch 的物理 RQE 授信关系。M5.3.8 的 [`PeerCreditProgress`](../kbsocket/runtime/peer_credit_progress.hpp) 拥有这些控制器及专用 fd，接入 Runtime 的预算、事件与暂停屏障。宿主仍须显式发起撤回、创建 raw 资源并关闭受影响的逻辑连接。

同一 Peer 的多条逻辑连接共享 `SenderCreditAccount` 与接收端的 `CreditPeerHandle`。本协议停止整个共账关系，不能用于仅关闭其中一条连接；所有消费这份授信的发送者必须使用同一个 SenderCreditAccount，不能绕过账本直接 post。其他 Peer 的正常授信和补投不受这一 Peer 的冻结影响。

## 入口与所有权

| 接口 | 行为 |
| --- | --- |
| `Open(fd, config, now_ns)` | 核验完整双向身份、双方授信 epoch 和实际账本归属；成功才接管 fd、独占绑定账本、Stop 共账发送并 FreezePeer 本端接收授信 |
| `Progress(now_ns)` | 有界推进停止快照、控制 IO、接收责任核对与 ACK；不分配、不调用 provider |
| `stats()` | 报告双方最终 spent、快照/发送/接收/退休/确认阶段；`complete` 仅说明此次双向授信撤回完成 |
| `Close()` | 关闭控制 fd 并解除独占绑定；不恢复发送、不恢复授信、不回收未经核实的责任 |

宿主提供的 fd 必须是专为本协议约定的已连接非阻塞流，并已认证双方完整 `HandshakeIdentity`；`peer_authorized=true` 只记录宿主已完成的授权，不提供认证能力。本组件不从普通 TCP 服务探测协议，也不接管现有 Connection 的 LaneControl fd。成功 Open 后不可再次 Open；失败不接管 fd、不停止发送或冻结账本。

Sender 绑定期间不能 Close/Open 切换 epoch；接收 Peer 绑定期间不能通过普通 `RetirePeer` 提交外部布尔证明，整个接收账本也不能经 `CloseDestroyed` 被提前清退。控制器和账本由同一 owner 操作，借用账本必须活到控制器 Close 成功。旧 Eager TX 流在 Sender 共账重开后会因身份不匹配拒绝投递，不能使用新 epoch 的额度发送旧会话数据。

## 停止快照与退休条件

Open 立即停止新的 Sender `Reserve`。此前已预留的 post 仍须用真实接受前缀 `Resolve`；未决 reservation 存在时，协调器不发送快照。可信部分接受只计入 accepted 前缀；`Resolve(nullopt)` 会把 `accepted_known` 永久置为 false，本 epoch 不能通过保守 spent 数量证明停止完成。

在无未决 reservation 且接受集合已知后，协调器固定两类最终 `spent`，发送 `CREDIT_STOP`。Stop 之后不再允许新 Reserve，因此后续累计 CREDIT 更新不能改变最终 spent。这里不等待 TX CQE，也不把 spent 等同于本地 DMA 已终结。

接收端冻结的是该 Peer 的新 Grant，仍允许它的真实迟到 RX 执行 Consume、Release 和实际 refill。收到 STOP 后分别核对 DATA、CONTROL：

```text
consumed_data    = max_data - base.granted - elastic_granted
consumed_control = max_control - control_granted
```

只有两个 consumed 均等于对端最终 spent、`pending_rx==0` 且域中没有 `unattributed` 责任时，才把该 Peer 未使用的 G 归还 F、保留退休 tombstone，并发送 `CREDIT_STOP_ACK`。Held 和已 Release 但尚未实际补投的 receipt 都会阻止退休；两个类别不能只比较合计数。对端 spent 超过本端已发放上界，或本端已消费数量超过对端 spent，均作为错误保留。控制流可以先于数据 CQE 到达，数量不足时继续等待，不能以一次空 CQ 判定缺失。

累计授信可能尚未送达对端，因此不要求双方看到的授信上界相等。真正的门禁是最终 spent 不超过本端已发放上界，且与实际消费量逐类相等。一个已接受 WR 若以错误结束且没有对应 RX，数量无法对齐，本协议会超时并保留未核实授权；不会用本地错误 CQE 猜测远端没有访问。

## 控制帧与完成范围

在现有控制 wire v3 中新增类型 6 `CREDIT_STOP` 和类型 7 `CREDIT_STOP_ACK`。两种消息都使用固定大端编码，12 B 公共头加 128 B 正文：完整 sender/receiver identity 各 48 B、双方 credit epoch 各 8 B、DATA/CONTROL spent 各 8 B。零流量允许 spent 全零，身份与 epoch 必须完整且非零。ACK 交换身份及 epoch，回显原 STOP 的两类 spent。

初始 ConnectionHandshake 与现有 LaneControl 不接受这两种消息；旧实现或不匹配的通道必须显式拒绝，不能把版本兼容误认为协议能力已协商。协调器拒绝错误身份、错代、重复 STOP/ACK、先于 STOP 的 ACK 及计数不符的确认。双方各先发 STOP 再发 ACK，完整消息边界与短 IO 由 HandshakeChannel 保证。

`complete` 要求本端 STOP 已发送、已收到对端 STOP、本端接收授信已退休、本端 ACK 已发送且已收到对端有效 ACK。收到最后有效 ACK 后只排尽本端待发输出，避免对端正常关闭的 EOF 抢先覆盖已经取得的确认。

这一完成仅证明该 epoch 的授信撤回。它不证明本地 TX DMA/import 引用已结束，不退休 RX 来源，不确认 JFC/async 事件引用，不允许直接删除 Jetty/JFR/MR，也不终结独立 RMA Grant。`stats()` 在 Close 后保留的历史完成状态同样不是新生命周期的销毁证明。

## 宿主推进顺序

1. 在唯一 owner 上停止该 Peer 所有逻辑连接的新业务准入，准备双方已认证的专用 fd 和匹配账本，调用 Open。不要提前 Freeze 整个 EagerReceiveDomain，否则待补投 receipt 无法沿正常路径结账。
2. 对受影响的 Connection 推进 Close，归还已缓冲及未读的 RX Lease；继续分发真实 TX 完成，处理 Close 返回的 Retained。现有连接检查发现 Peer 已冻结会显式失败；这不是保证应用数据发完的优雅关闭协议，未提交数据按现有失败/关闭语义丢弃，不重放。
3. 保留旧来源与原接收 owner，继续可信 RX 消费、Lease Release 和 `EagerReceiveDomain::Refill`，包括路由已撤回后的迟到帧。继续结清任何在 Stop 前预留的 post，按真实 accepted 前缀入账。
4. 使用下述 PeerCreditProgress 时由 Runtime 的 mailbox/timer 预算与 fd readiness 推进；直接使用控制器时由相同 owner 有界调用 Progress。等待 RX/补投或 reservation 时也须继续调度，两种方式均须遵守相关 context 的暂停屏障，不可同时驱动同一控制器。
5. 完成后调用调度器 Release（直接使用时调用协调器 Close）解除绑定，再按独立 TX、来源、事件和 M1 条件推进剩余资源关闭。若仍有其他 Peer，接收域可以继续服务它们；不能因一份 Peer 撤回完成就停掉整个域。

错误或超时锁存首个失败原因并关闭控制 fd，绑定保留至 Close。提前 Close 或失败后的 Close 只解除协议控制权，未退休的接收 Peer 仍冻结、发送账本仍停止，旧 DMA 和授权责任仍由宿主持有。此前已经独立核实并退休的一个方向不会回滚成可发送状态。若正常撤回无法完成，可解除控制器绑定后按已有 M1 真实物理关闭与 M5.3.6 账务清退路径处理；仍须另行取得所需关闭证明，不能把协议失败当作销毁依据。

## Runtime 调度与所有权

`PeerCreditProgress::Open` 在控制路径预分配固定地址的槽、就绪队列及巡检索引，默认容量 4096，上限 65536。每个槽预留完整控制器及其收发缓冲，应按实际同时撤回的 Peer 数配置容量；这不是连接数或吞吐验收结果。它借用发送/接收账本，拥有槽内协调器和成功 Begin 后的专用 fd。动态 fd 由内部 epoll 汇聚为一个固定 Reactor fd，增加或释放 Peer 不改变外层注册集合。

| 接口 | 行为 |
| --- | --- |
| `BeginWithdrawal(fd, config, now)` | 先校验容量及注册 fd、唤醒，再执行不可逆的协议 Open；失败仍由调用方拥有 fd，账本不变；成功返回含 owner/代次的句柄 |
| `Inspect(handle)` | 返回协议阶段、终态及原始协议/调度错误的值快照，不暴露可独立推进的控制器 |
| `Wake(handle)` | 待决 post 结论或 RX/refill 更新后可显式唤醒；只允许 owner 调用，定时巡检也会发现变化 |
| `Release(handle)` | 仅完成或失败后可释放槽、fd 与账本绑定；失败时未证实的授权继续保留 |
| `Abort(handle)` | 显式中止并释放槽；账本仍停止/冻结，不生成任何静默或 DMA 证明 |
| `Close()` | 所有句柄显式 Release/Abort 且已从 Reactor 解绑后关闭；尚占槽时返回 InUse，析构失败保留存储 |

Begin 的 fd 必须由宿主独占，不能复用 Connection/LaneControl fd 或其 dup。同一调度器拒绝相同 fd 数值；账本独占绑定还会拒绝重叠的撤回。句柄在 Release、Abort、槽复用及调度器 Close/Open 后失效，序号耗尽拒绝准入，绝不回绕。完成和失败槽都占用容量直到显式释放，不能因压力淘汰仍绑定的责任。

`PeerCreditProgressConfig.context` 必须非空，并通过 `RawContextBinding.peers` 加入相同 raw context 的第五个成员位置。它是宿主声明的账本归属，账本本身没有可再次核验的 raw context 指针；Runtime 只核验声明与组一致，并拒绝跨组别名或独立 hooks。组暂停期间所有入口、账本访问和外部调用均暂停，其他 context 继续推进。协议超时使用单调时间，暂停不延长期限；Begin 和 Progress 的 `now_ns` 都只是实际 steady_clock 时间的下界，槽内时间不会倒退。

就绪队列每次 mailbox 调用最多访问 budget 个 Peer；timer 每次最多扫描 budget 个占用槽，默认巡检间隔 1 ms。完整一次巡检所需 Tick 数由槽数/预算决定，事件模式还受 Reactor 有限 `max_wait_ms` 影响，1 ms 并非完成或超时回调的实时保证。fd 使用 oneshot；只有待发输出时订阅可写，最终有效 ACK 后停止读订阅。未完成、EAGAIN、待决 reservation 或未归还 receipt 都不持续设置 runnable，等待 fd、显式 Wake 或下一轮巡检。协议推进、通知和巡检均不分配、不调用 provider。

每个完成或失败只报告一次终态回调；回调不能重入调度器。协议错误仅终结该 Peer，其他撤回继续推进；调度源基础 epoll/eventfd 的错误仍作为 Runtime 错误返回。终态移除或停止重装 fd 监听，避免对端 EOF 造成持续唤醒；完整代次拒绝迟到事件。已经由协议关闭的 fd 不按旧数字执行 DEL，避免触碰复用后的描述符。

Release/Abort 解除绑定不代表可以销毁账本背后的 raw 资源。宿主仍须汇总整个域的所有 Peer 关系和独立 TX、来源、Held、通知/async 责任，停止相关进度绑定后在控制路径推进 M1；正常关闭与故障保留分别遵守原证明门禁。RuntimeShard Close 仅解绑 source，不自动 Close/Abort 这些控制器。

M5.3.9 的 [RawDomain](raw-domain.md) 提供单域资源所有权。BeginClose 使用接收账本的 StopAdmission 关闭新 Admit/Grant，同时保持 Consume/Release/真实 refill，供撤回继续收敛。AdvanceClose 要求 `withdrawal_bindings==0` 才能进入物理删除；Peer 已退休不代表控制器已解绑。宿主必须先 Release/Abort，且另行满足 TX、来源、进度及 M1 门禁，不能把账本冻结或协议失败当作静默证明。

## 验证与后续范围

控制帧及协调器已通过 Mock 账本、非阻塞 socketpair 和本地 IPv4 TCP 验证。协调器 30 项测试涵盖停止/计数/迟到 RX/超时/身份、发送背压与最终 ACK/EOF 竞态；与真实 Mock RxBufferPool/EagerReceiveDomain 的组合还验证了 Held 归还、实际 refill 和独立 M1 关闭的顺序。完整回归结果见 [roadmap](roadmap.md)。这些测试不构成 UB 真机观测。

M5.3.8 的调度验证覆盖 fd 接管回滚、容量与代次、定时结账、独立 Peer 失败、终态绑定和暂停屏障；完整结果见 roadmap。M5.3.9–10 已有单域和多 context 资源所有者，跨域关闭须等待全部撤回槽释放；建立/认证专用连接与整个 Peer 生命周期仍由宿主驱动，不实现透明重连、业务重放、RMA Grant 撤销或故障 CQ 在线恢复。
