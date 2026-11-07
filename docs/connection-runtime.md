# M5.3：连接 Runtime 与主动 Lane 来源协商

本阶段将已创建的 Connection、raw 队列、导入缓存和 RX 来源表接入同一个 `RuntimeShard`。owner 通过 `Tick`/`RunOnce` 推进 HELLO/READY、导入、TX/RX、物理补投、累计 Credit 和 FIN。M5.3.2 通过 HELLO 交换主动 Lane，并在 READY 前自动登记 RX 来源；M5.3.3 增加在线 Lane 公告与确认。资源创建、入口认证和应用调用仍由宿主负责；这不是完整的 Runtime 工厂或稳定应用 ABI。

## 组件与暂停范围

| 组件 | 职责 |
| :--- | :--- |
| `ContextProgressGroup` | 将同 context 的 raw、连接和导入源合为一个 Reactor source；任何成员的慢控制任务都暂停整个组 |
| `ImportProgress` | 按 context 指针及 incarnation 过滤缓存操作，通过 Reactor 的唯一控制执行器提交和收割 |
| `ConnectionProgress` | 固定容量连接槽、完整代次 TX 分发、就绪队列、周期巡检、外部 RX 补投和动态 fd 通知 |
| `RxSourceRegistry` | 根据真实 CQE 的本地端点、远端主动发送端点和完整 token 查找已登记的可信会话来源 |
| `HandshakeRxSources` | 把经过宿主授权的对端 HELLO Lane 快照一次性登记到对应 RX pool，保留绑定直至明确静默 |
| `LaneControl` | 在双方 READY 后接管控制 fd；新 Lane 经远端登记并确认后才开放本端投递 |
| `ConnectionIoBinding` | raw 回调到 Connection 的桥接；旧帧物理结账、未知来源冻结、错误记录与 Credit 冻结传播 |

`RawContextBinding.connections` 和 `.control` 分别指向连接源及导入源。设备进度源声明 `exclusive_context()`；RuntimeShard 为每个 context 组装暂停组，拒绝归属不一致的源、跨 context 复用的 companion 以及访问 raw 的独立 hooks。现有 mailbox/内存归还 hooks 可继续独立接入；它们不能在 context 暂停期间操作该 context。无 raw context 时可以只注册 NativeTCP hooks。

每个成员在每个 `ProgressKind` 上拥有独立预算，组内轮换访问顺序。Reactor 提交控制任务前移除整个组的等待 fd 并停止其进度，收割完成并交给原成员后才恢复。`paused(context_index)` 表示整个组；外部应用、回调和借用者也必须遵守这个门禁，不能绕过组直接 post/poll/refill 或准备 raw 连接。

导入缓存新增 `PrepareOperation`、`CommitOperation`、`DiscardOperation`：Prepare 只固定参数并占用 context/并发槽，不调用 provider；执行器接受后 Commit 仅写 owner 元数据，不读取 worker 输出。执行器队列满时保留同一准备任务重试。已提交任务即使被取消也必须收割，未提交任务才可以 Discard。原 `StartOperation` 显式暂停接口继续可用；注册至 RuntimeShard 后禁止额外执行器并行驱动同一缓存/context。

## 装配顺序

1. 在控制路径创建 raw context、JettyPool、AttemptLedger、`TxSender::Open(ledger, true)`、Slab、RX pool、Credit 总账、路由表及导入缓存。初始 RX 全部实际 post 并记账后打开 `EagerReceiveDomain`。
2. 每个 context 打开 `ConnectionProgress`（指定同一个 context）和 `ImportProgress`（指定完整 incarnation）；每个 RX pool 打开独立 `RxSourceRegistry`。用 `ConnectionIoBinding::Open` 绑定三者，同时登记由连接源负责补投的接收域。
3. `RawQueueBinding` 配置 `TxCallback`、`RxCallback`、`ErrorCallback`，cookie 指向对应 `ConnectionIoBinding`，并设置 `external_refill=true`。其余 CQ/async/通知仍由唯一 RawContextProgress 消费。
4. 将连接源和导入源交给 `RawContextBinding`，打开 RuntimeShard。使用 `CaptureTxLanes(pool)` 填充本端 HELLO，声明实际提交器的主动 Lane 快照。Connection 可在此前或运行期间 Attach；Attach 必须早于 PrepareRaw。把返回 `ScheduledConnection.id` 用作 `RawConnectionPreparation.connection_id`。
5. 每条 raw 连接用 `HandshakeRxSources::Open(registry)` 打开独立准入器；Preparing 时完成资源/授信准备，将其作为 `RawConnectionPreparation.sources`。宿主认证并授权对端整份公告后才设置 `peer_lanes_authorized=true`。PrepareRaw 校验本地公告与实际池一致，初始化收发状态后自动一次性登记对端全部 Lane；未完成登记不能发送 READY。
6. 应用每次 Write、Read、Prepare、Shutdown 后调用 owner 侧 `Wake(handle)`。唯一 owner 反复 Tick/RunOnce，禁止同时手动驱动 Connection、导入 executor 或 raw CQ。跨线程业务命令仍需宿主 mailbox 转交 owner，不能跨线程调用 Wake 或 Connection。

初始资源创建和来源登记是控制路径工作，可分配内存。队列推进、fd 收割、TX 分发、RX 查表与补投使用预分配存储，不进行堆分配。当前 Attach/来源登记仍扫描已登记条目做冲突检查，不表示已经满足 40k/s 建链目标。

## 实际来源与旧帧

CQE 不包含 PeerRuntime incarnation、逻辑 session 或 Credit epoch。`RxSourceRegistry::Register` 接收宿主经过可信控制路径确认的完整绑定，而不是从首个包自行学习。键包含本地接收端点 ID/type、CQE 的远端 EID/UASID/主动 Jetty 或 JFS ID，以及完整 slot/generation token。同一物理 Lane 可以登记多个逻辑 session，但不能在旧绑定存续时更换其 PeerRuntime/Credit 归属。

**HELLO 的单个 Jetty 字段仍是被动接收端点。** HELLO 另携带最多 64 条主动 Lane 的 EID/UASID/Jetty ID、pool epoch 和 Lane generation。两种端点不能混用；RAW 不接受空列表、重复物理来源、零代次或主动/被动身份冲突。当前握手版本为 v3，新增在线 UPDATE/ACK；v1/v2 显式拒绝，双端需要同时升级。

`HandshakeRxSources` 只接受匹配 registry 的真实 RX pool、context/incarnation、本地 token/domain/session 及 Peer Credit 身份。`RegisterBatch` 在写入任何条目前检查整批容量、重复键和物理来源归属冲突；失败不留下部分登记。HELLO 是公告载体，不能证明远端有权使用所列 Lane，`peer_lanes_authorized` 必须来自宿主可信控制入口。直接使用低层 `ConnectionHandshake` 的宿主仍须自行完成这一门禁，完整 `Connection` 已强制要求来源准入器。

每次推进 READY 或提交 TX 前，Connection 校验本地实际 Lane 物理身份和代次，并检查已安装来源仍有效。单 Lane 故障而身份不变时由原发送器避开故障 Lane。未启用在线更新时仍采用整份快照失效门禁，换代后以 `kLaneChanged` 显式失败；启用后的流程如下。

Resolve 校验真实 RX pool/Lease、接收方向、CQE 状态/标志、opcode、长度和物理端点，再解码 token 选择已登记会话；后续仍由 ConnectionTable 校验完整路由。旧 token 已撤路由时仍按旧 Peer 扣物理账、释放 Lease 并补投。来源未知或 CQE/帧无效时，桥接仍交付真实事件给接收域，使用 unknown 责任并冻结，不静默丢掉 RQE 消耗。

逻辑连接关闭不立即删除来源。`HandshakeRxSources::Close` 在绑定尚存时返回保留错误；宿主先取得匹配 context incarnation、远端发送停止、已接受流量终结和 RX 事件排空证明，再调用 `Retire` 一次性移除。控制 EOF、本地 Lane 重建和 Connection::Close 成功均不能替代该证明。来源表本身不实现远端关闭协议，借用的 registry/Credit/RX pool 必须覆盖全部旧绑定。

## 在线 Lane 更新

双方在 `RawConnectionPreparation.authorize_lane_update` 提供策略回调，显式启用本连接的在线更新。回调验证既有远端身份对整份新 Lane 列表的使用权，必须在 owner 上有界执行、无阻塞且不得重入。初始 `peer_lanes_authorized` 不能自动授权后续公告；未提供回调时不接纳更新。

1. 同一 owner/控制暂停边界完成本地空闲 Lane 的排空与重建后，Connection 发现代次变化，暂停该连接的后续 raw 投递，保留已排队数据和在途 WR。
2. `LaneControl` 在原 TCP 控制流发送 UPDATE，携带双方完整 handshake identity、单调更新序号和完整 Lane 快照。pool epoch、Lane 数量、被动端点和 context 必须保持不变；相同槽位代次只能增加，同代物理身份不能改变。
3. 接收端核对身份/序号并调用策略回调，`HandshakeRxSources::Update` 原子登记所有新物理来源。成功后才排队 ACK，失败则关闭本逻辑连接，不确认、不删除旧绑定腾位。
4. 发送端消费匹配 ACK 后，再核对实际池是否仍等于这份公告。确认与当前池一致才继续提交；若等待期间再次重建，先公告下一份快照。`local_lane_update_id()` 仅报告已确认序号，`peer_lane_update_id()` 报告已安装序号，初始均为 0。

同一连接最多一份本地待确认 UPDATE 和一份远端待发送 ACK；发送缓冲可用时优先 ACK，支持双向同时更新。控制 IO 每次有固定字节/系统调用预算；Runtime 仅在有待发送控制字节时订阅 EPOLLOUT。`OnControlReady` 报告可读后由 Progress 解码，手动驱动宿主也必须提供 readiness。半帧、ACK 发送背压、重建空槽等待及更新确认均受 `lane_update_timeout_ns` 约束（默认 5 秒），空闲连接不因该参数超时。重复、乱序、错身份或未经请求的 ACK 均显式失败。

ACK 仅证明来源已登记，不证明旧 SEND 已收完、旧 Credit 已撤销或旧 DMA 已终结。每条连接最多保留 256 个不同物理来源；同一 EID/UASID/ID 换代复用时复用原绑定，其余历史来源留到明确静默后统一退休。达到适配器或共享 registry 上限时更新失败，禁止静默淘汰旧项。已失败连接不会因新 Lane 或 ACK 恢复，也不会重放 WR；固定被动端点和导入缓存可保持不变。

## 公平推进与通知

ConnectionProgress 使用有界、去重的 ready 队列。每次 mailbox 预算最多访问相应数量的连接，每个访问最多推进一个已有 TX 批次；有进展且仍有数据的连接排回队尾。等待 Credit/资源时不持续自我唤醒。RX 和补投触发有预算的共账巡检，重复 eventfd 通知合并；周期巡检负责 pending import、握手超时和重排缺口超时。巡检间隔是目标周期，实际检查时延还受 Reactor 最大等待时间、连接数量及预算影响，尚未做大规模延迟验收。

Reactor 提供的批次时间是调度时间下界；每次访问 Connection 时刷新同一单调时钟。RX 回调或应用 Read 可能晚于 Tick 开始，不能再把 Tick 的旧时间传给已观测较新时间的重排窗口，否则会误报时间倒退。独立显式驱动 Connection 的调用方仍须提供单调一致的时间。

动态控制 fd 由内部 epoll 汇聚成一个固定等待 fd，避免直接占用外层 Reactor 的 80 个 fd 配额。握手和 raw Active 均只在控制输出待发送时订阅 EPOLLOUT；raw Active 还消费在线 Lane 消息及控制断连。所有 watch 使用 oneshot，旧 fd 存在 dup/继承别名时也不能持续唤醒已退出的连接；代次失效事件直接丢弃。

NativeTCP 在 `WatchTcp(handle, read, write)` 后报告一次 readiness，不自动读取业务字节。应用按预算 Read/Write，再显式订阅下一次通知；可写监听由需求驱动，避免长期 EPOLLOUT 忙循环。状态变化、RX 驻留变化和 TX 真实退休数通过 `ConnectionProgressEvent` 回调报告。RX 驻留可能只有乱序帧，通知不保证马上可读；Read 仍执行顺序门禁。回调不得重入或销毁调度器。

## 失败与关闭

普通连接错误由连接锁存并通知，调度器继续推进健康连接。TX 只接受完整调度 id、匹配 context 的 Lane 及原 Eager 完成身份；取消后错误代次/重复 CQE 依旧拒绝。status 8 不升级为整个 SQ 故障，status 9 继续由 raw 层隔离排空，均不重放。

raw RX 停止或队列被保留时，ErrorCallback 和补投阶段会将状态传播为物理 Credit 冻结，防止继续广告可用容量。冻结不释放应用 Held Lease 或已接受 DMA。故障 CQ 的在线恢复、丢失 CQE 的资源回收仍未实现。

Connection::Close 返回 Retained 时继续保留注册与全部借用资源。可信 TX 终结即使在 Failed/Closing 状态也报告 `tx_retired`，宿主据此重试 Close；成功后才 Detach。Detach 不关闭 Connection，也不撤销远端 Credit。关闭 RuntimeShard 只解除调度关系，不能替代连接、导入、来源和 raw 资源关闭。

退出时先停止新业务并完成连接收敛，继续调度导入缓存的失效/Unimport；所有任务收割后关闭 RuntimeShard，再关闭 companion 和缓存。来源删除及 raw 域销毁仍要求真实静默/终结证据。冻结 RX 域中的未补投 receipt 尚缺少 DeleteJfr 后统一核销流程，不能把这一阶段当成完整故障关闭协议；无法取得终结证据时保留依赖。

## 验证与剩余范围

新增测试覆盖超过外层 fd 数量的本地 TCP、oneshot 重订阅、旧 fd 别名、完整代次、预算/公平队列、零分配热路径、来源伪造/旧会话、多 context 暂停、控制背压、取消后 DMA 收敛，以及仅由 RuntimeShard 推进的 Mock raw 双端 DATA/Credit/补投/FIN。回归结果见 [roadmap](roadmap.md)。UB 真机和 40k 规模性能未执行。

M5.3.2/3 新增协议边界、原子登记/更新、授权和来源就绪门禁、双主动 Lane 自动来源解析、双向同时更新、ACK 前禁止投递和 ACK 后迟到旧 RX 测试。下一阶段继续补 Runtime 资源所有权与连接入口、完整业务 mailbox、远端关闭/授信撤回、冻结接收责任核销，再收口显式 C ABI 和零拷贝接口。跨 Peer 弹性授信调度与 M6 Grant/RNDV 不在本次交付内。
