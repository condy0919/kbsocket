# M5.3：连接 Runtime 与主动 Lane 来源协商

本阶段将 Connection、raw 队列、导入缓存和 RX 来源表接入同一个 `RuntimeShard`。owner 通过 `Tick`/`RunOnce` 推进 HELLO/READY、导入、TX/RX、物理补投、累计 Credit 和 FIN。M5.3.2 通过 HELLO 交换主动 Lane，并在 READY 前自动登记 RX 来源；M5.3.3 增加在线 Lane 公告与确认；M5.3.4 用 `ConnectionOwner` 统一拥有连接对象及其来源适配器，提供有界建链与读写入口。M5.3.5 增加 `ConnectionMailbox`，把业务线程的读写等命令交给唯一 owner。M5.3.7–8 提供 [Peer 授信撤回协调器及其 Runtime 调度](peer-credit-control.md)。M5.3.9 的 [RawDomain](raw-domain.md) 拥有单接收域资源并协调物理关闭及元数据清退；M5.3.10 的 [RawStack](raw-stack.md) 再汇总 context/会话、共享 Slab/import 和每域连接/调度资源。入口认证与独立静默证明仍由宿主负责，应用 ABI 尚未稳定。

## 组件与暂停范围

| 组件 | 职责 |
| :--- | :--- |
| `ContextProgressGroup` | 将同 context 的 raw、连接、导入、业务 mailbox 和 Peer 撤回合为一个 Reactor source；任何成员的慢控制任务都暂停整个组 |
| `ImportProgress` | 按 context 指针及 incarnation 过滤缓存操作，通过 Reactor 的唯一控制执行器提交和收割 |
| `ConnectionProgress` | 固定容量连接槽、完整代次 TX 分发、就绪队列、周期巡检、外部 RX 补投和动态 fd 通知 |
| `ConnectionOwner` | 拥有固定容量的 Connection 与来源适配器；接管 fd、注入真实调度身份，按关闭与来源退休两道门禁释放槽 |
| `ConnectionCommandQueue` / `ConnectionMailbox` | 固定容量 MPSC 命令与结果槽，复制 I/O 数据，由 owner 有界执行且仅一次，领取结果后才复用槽 |
| `PeerCreditProgress` | 拥有固定容量的 Peer 撤回控制器及专用 fd，按预算/定时器/事件推进，终态显式释放，不代替物理销毁证明 |
| `RxSourceRegistry` | 根据真实 CQE 的本地端点、远端主动发送端点和完整 token 查找已登记的可信会话来源 |
| `HandshakeRxSources` | 把经过宿主授权的对端 HELLO Lane 快照一次性登记到对应 RX pool，保留绑定直至明确静默 |
| `LaneControl` | 在双方 READY 后接管控制 fd；新 Lane 经远端登记并确认后才开放本端投递 |
| `ConnectionIoBinding` | raw 回调到 Connection 的桥接；旧帧物理结账、未知来源冻结、错误记录与 Credit 冻结传播 |
| `RawDomain` | 拥有单域队列/RX/TX/授信/路由/来源/桥接，真实初始 post 后发布视图，独立屏障满足后按 M1 顺序关闭 |

`RawContextBinding.connections`、`.control`、`.mailbox` 和 `.peers` 分别指向连接源、导入源、业务命令源及 Peer 撤回源。设备进度源声明 `exclusive_context()`；RuntimeShard 为每个 context 组装暂停组，拒绝归属不一致的源、跨 context 复用的 companion 以及访问 raw 的独立 hooks。内存归还 hooks 可继续独立接入；它们不能在 context 暂停期间操作该 context。raw mailbox 从 ConnectionOwner 继承 context 身份，必须加入对应暂停组。无 raw context 时，可把 NativeTCP 的 ConnectionProgress 与 ConnectionMailbox 合为一个 ContextProgressGroup 后作为 hooks 接入。

每个成员在每个 `ProgressKind` 上拥有独立预算，组内轮换访问顺序。Reactor 提交控制任务前移除整个组的等待 fd 并停止其进度，收割完成并交给原成员后才恢复。`paused(context_index)` 表示整个组；外部应用、回调和借用者也必须遵守这个门禁，不能绕过组直接 post/poll/refill 或准备 raw 连接。

导入缓存新增 `PrepareOperation`、`CommitOperation`、`DiscardOperation`：Prepare 只固定参数并占用 context/并发槽，不调用 provider；执行器接受后 Commit 仅写 owner 元数据，不读取 worker 输出。执行器队列满时保留同一准备任务重试。已提交任务即使被取消也必须收割，未提交任务才可以 Discard。原 `StartOperation` 显式暂停接口继续可用；注册至 RuntimeShard 后禁止额外执行器并行驱动同一缓存/context。

## 装配顺序

M5.3.9 可用 `RawDomain::Open` 代替以下手工创建的单域队列、RX、TX 账本、接收 Credit、路由及来源表，再以 `Bind(ConnectionProgress&)` 生成第 2–3 步的桥接和 RawQueueBinding。context、Slab、导入、逻辑连接及 Peer 发送账本仍独立装配；具体失败与关闭契约见 [raw 接收域所有权](raw-domain.md)。

1. 在控制路径创建 raw context、JettyPool、AttemptLedger、`TxSender::Open(ledger, true)`、Slab、RX pool、Credit 总账、路由表及导入缓存。初始 RX 全部实际 post 并记账后打开 `EagerReceiveDomain`。
2. 每个 context 打开 `ConnectionProgress`（指定同一个 context）和 `ImportProgress`（指定完整 incarnation）；每个 RX pool 打开独立 `RxSourceRegistry`。用 `ConnectionIoBinding::Open` 绑定三者，同时登记由连接源负责补投的接收域。
3. `RawQueueBinding` 配置 `TxCallback`、`RxCallback`、`ErrorCallback`，cookie 指向对应 `ConnectionIoBinding`，并设置 `external_refill=true`。其余 CQ/async/通知仍由唯一 RawContextProgress 消费。
4. 将连接源、导入源、可选业务 mailbox 及 PeerCreditProgress 交给 `RawContextBinding`，打开 RuntimeShard。使用 `CaptureTxLanes(pool)` 填充本端 HELLO，声明实际提交器的主动 Lane 快照。Connection 可在此前或运行期间 Attach；Attach 必须早于 PrepareRaw。把返回 `ScheduledConnection.id` 用作 `RawConnectionPreparation.connection_id`。
5. 每条 raw 连接用 `HandshakeRxSources::Open(registry)` 打开独立准入器；Preparing 时完成资源/授信准备，将其作为 `RawConnectionPreparation.sources`。宿主认证并授权对端整份公告后才设置 `peer_lanes_authorized=true`。PrepareRaw 校验本地公告与实际池一致，初始化收发状态后自动一次性登记对端全部 Lane；未完成登记不能发送 READY。
6. 应用每次 Write、Read、Prepare、Shutdown 后调用 owner 侧 `Wake(handle)`。唯一 owner 反复 Tick/RunOnce，禁止同时手动驱动 Connection、导入 executor 或 raw CQ。使用 ConnectionOwner 时这些 Wake 已自动完成；跨线程业务通过下述 ConnectionMailbox 提交，不能直接调用 Wake 或 Connection。

初始资源创建和来源登记是控制路径工作，可分配内存。队列推进、fd 收割、TX 分发、RX 查表与补投使用预分配存储，不进行堆分配。当前 Attach/来源登记仍扫描已登记条目做冲突检查，不表示已经满足 40k/s 建链目标。

## 连接对象所有权与入口

`ConnectionOwner::Open(progress, config)` 借用已经打开的 `ConnectionProgress`，一次预分配连接槽，默认容量 256，上限 65536；调度器自身容量也是独立准入门禁。每槽拥有稳定地址的 `Connection` 和按需初始化的 `HandshakeRxSources`。队列、Slab、导入缓存、Peer 共账、路由表和共享来源表仍由宿主装配，必须覆盖占用槽和来源记录的生命周期。

| 入口 | 行为 |
| :--- | :--- |
| `AdoptTcp(fd)` | 接管已连接的非阻塞 TCP，直接使用普通字节流，不发送私有握手 |
| `AdoptProtocol(fd, routes, config, now)` | 接管明确支持本协议的流，沿用 HELLO/READY；宿主提供可信身份、策略和唯一会话 |
| `Peer(handle)` / `PrepareRaw(handle, prep, registry)` | Preparing 时读取经校验的远端公告，完成授权与共账准备；自动填充真实 scheduling id 和槽内来源适配器 |
| `PrepareTcp(handle)` | 协议协商选择 TCP 后完成准备，仍等待双方 READY |
| `Write` / `Read` / `ShutdownWrite` / `Cancel` | 校验完整句柄并自动 Wake，应用无需再手工唤醒对应连接 |
| `Inspect(handle)` / `WatchTcp(handle, read, write)` | 返回状态、失败原因、调度身份和保留来源数；按需求订阅一次性 TCP readiness |

Adopt 先占槽、注册 Idle Connection，最后才 Open；成功才转移 fd 所有权。无效 fd、容量满或握手配置错误均回滚空槽并保留调用方 fd。入口先通过 `ConnectionProgress::HasFd` 检查整个共享调度器，拒绝同一描述符的重复接管；该检查不识别 `dup` 别名，宿主仍须保证独占使用。若异常回滚无法撤销注册，错误中的 `retained_handle` 交回保留槽供调用方处理。

`OwnedConnection` 包含 owner、槽号及完整单调代次；Release 后旧句柄永久失效，序号耗尽停止准入。`Inspect` 返回的 `ScheduledConnection` 可关联既有 Runtime 回调。`Peer` 仅借用 HELLO 到 Release，不授予修改权或绕过认证的权限。普通 TCP 与协议入口可共用同一 owner；raw Prepare 的 context 必须匹配调度器。`prep.connection_id` 和 `prep.sources` 必须留空，来源适配器首次 Open 后固定 registry；授权等可修正的准备错误允许重试，换 registry 则重新建链。发送 Credit 和接收物理总账继续按 Peer/域共享。

ConnectionOwner 的所有接口都由单 owner 调用，遵守 context 暂停屏障；业务线程经下述 ConnectionMailbox 转交。Prepare 是允许分配的控制路径；正常 Read/Write/Inspect 和推进不分配。Read/Write 先 Wake 再接受/交付字节，避免信号错误覆盖已完成的字节前缀。`StopAccepting` 只关闭新 Adopt，不影响已接受连接的准备、收发和排空。

关闭按以下顺序执行：

1. `CloseConnection(handle)` 撤路由并收敛连接责任；返回 Retained 时槽及调度注册继续存活，由原 owner 交付真实 TX 终结后重试。
2. Connection 已 Closed 后，来源仍保留用于迟到 RX 结账。宿主取得独立 `RxSourceRetireProof` 后调用 `RetireSources`；控制 EOF、Lane ACK 或 Close 成功均不替代此证明，也不自动撤回 Peer Credit。
3. `Release(handle)` 要求连接已 Closed 且来源全部退休，随后 Detach 并重构槽。无来源的 TCP/未完成 raw 准备连接可直接 Release。等待证明的槽占用容量，耗尽时拒绝新入口，不淘汰旧责任。
4. 全部槽释放后 `ConnectionOwner::Close` 成功；还有占用槽则返回 InUse。停止 Runtime 调度后再关闭借用的 scheduler、来源表、导入和 raw 资源。错误析构保留整份内部槽存储，宿主仍须保留外部依赖，不提供强制回收。

## 有界业务命令与结果

`ConnectionMailbox::Open(owner, config, callback, cookie)` 借用已打开的 ConnectionOwner，预分配命令及结果环，默认容量 256，上限 65536，实际容量向上取至少 2 的二次幂。每个槽固定保留最多 4096 B 数据，容量预算须包含这份 backing；没有每连接单独分配 mailbox。Open/Close 属于 owner 控制路径，同一个队列成功 Open 后不能重新打开。

| 命令 | 执行及完成结果 |
| :--- | :--- |
| `kWrite` | Submit 时复制输入，owner 调用 Write 一次，返回实际接受字节数或原错误 |
| `kRead` | owner 向槽内缓冲 Read 一次；TakeCompletion 复制已读字节并返回 EOF，不保存调用方输出指针 |
| `kShutdownWrite` / `kCancel` | owner 执行既有半关闭或取消，未完成 DMA 仍沿用原责任账本 |
| `kInspect` / `kWatchTcp` | 返回执行时状态快照，或按需求设置一次性 TCP readiness |

Write/Read 的 `bytes` 必须为 1–4096；Write 输入长度必须一致，其余命令不携带输入。Submit 返回票据只表示命令已复制入队；Write 的完成字节数表示 Connection 接受的前缀，并不证明 raw DMA、远端接收或应用消费已经结束。短读写、WouldBlock 和其他连接错误都作为结果交回；mailbox 不自动重试，也不重放已接受数据。业务可结合 ConnectionProgress 的通知自行决定下一条命令。

队列允许多线程 Submit 与 TakeCompletion；owner 只按预留顺序消费连续已发布前缀，不等待 producer 发布空洞。单个 producer 的顺序保持，多个 producer 之间由预留顺序串行化。同一 Tick 最多执行 `min(budget, 16)` 条命令，预算不足保留 readiness；未轮到的 raw context 或控制暂停期间，命令只占槽、不访问连接。Read 执行时刷新单调时钟，避免旧 Tick 时间早于同轮 RX 更新。

排队、执行中及完成未领取的命令都占容量；环绕遇到未领取槽即返回 `kFull`，即使其他位置已有空槽，也不会越过它重排。调用方须保存每张成功票据，并最终领取。TakeCompletion 通过代次和原子领取保护槽：尚未完成返回 `kPending`，过期/错队列票据拒绝；同一票据并发领取最多一次成功。Read 输出不足返回 `kOutputTooSmall` 并保留原结果，可用更大缓冲再次领取，不重新执行 Read。

入队通过 eventfd 唤醒 owner。可选完成回调只传票据、连接句柄和业务 token，在 owner 上有界执行，不得阻塞、重入或销毁 mailbox/依赖；没有额外完成 fd。结果发布后 collector 可立即领取和复用槽，因此回调使用提前复制的身份，不引用槽内数据。连接句柄的代次由 owner 在执行时核验；已排队命令不能因连接槽复用而作用于新连接。

停止时先 `StopAccepting()`，协调并等待 producer 结束；此前已取得预留资格的并发提交仍可能成功，必须一并处理。owner 继续执行已接受命令，collector 领取全部结果后退出。停止调度并解除 source 绑定，再以 `Close(true)` 显式证明 producer/collector 均已静默，且队列没有任何未领取责任，才释放槽与 fd。空计数本身不构成线程静默证明；未成功 Close 的析构保留存储。宿主仍须保留借用的 owner、scheduler 与 raw 依赖，不能靠保留队列替代外部生命周期管理。

此 mailbox 不执行任意函数，也不把 Adopt/Prepare/CloseConnection/Release 等可能分配、释放或需要控制屏障的操作放入热循环；这些入口继续由宿主在 owner 控制路径串行驱动。`kCancel` 取消连接，不删除队列中后续命令；后续命令仍逐个执行并返回连接状态对应的结果。

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

退出时先按上述顺序停止新命令并领取全部结果，再完成连接收敛，继续调度导入缓存的失效/Unimport；所有任务收割后关闭 RuntimeShard，再关闭 companion 和缓存。来源删除及 raw 域销毁仍要求真实静默/终结证据。相关 Runtime、ConnectionIoBinding 和外部队列访问停用后，宿主在控制路径推进 M1 接收域关闭，不能在 Tick 或业务 mailbox 内执行删除/注销。

M5.3.6 增加 `EagerReceiveDomain::CloseDestroyed(const raw::ReceiveDomainCloser&)`：要求协调器真实 `kClosed`，匹配同一 RxBufferPool 本次成功 Open incarnation 及本层捕获的 Credit receive-domain incarnation，且 Held 为 0、全部 queued receipt 有效且已释放、全局 released 数量匹配。整批检查通过才永久清退 Eager 与接收授信元数据，不 post、不恢复额度、不重授信。冻结或 unknown 的已释放 receipt 可沿此路径结束；没有真实关闭证据时仍保留全部责任。

等待应用归还、DeleteJfr/注销失败及其他 M1 未完成阶段都不能提前核销；残留 RX 数为 0 也需要同样证据。故障 CQ 的 `kRetained` 流程不支持此入口。它不自动调用 RetireSources 或解绑 Runtime，不生成远端关闭/授信撤回证明；来源退休、旧会话隔离和剩余依赖顺序仍由宿主分别完成。每个 Credit 域只允许一个 Eager owner，重复绑定被拒绝；绑定期间不能直接 Credit Close，普通 Eager Close 成功才解绑。Eager 关闭失败后的异常析构保留条目及绑定，外部依赖仍由宿主维持。

M5.3.7 的 `PeerCreditControl` 用另一条已认证的专用非阻塞 fd 处理整个 Peer 共账撤回，不接管 Connection/LaneControl 的控制 fd，也不由 Connection Close 自动启动。成功 Open 后停止新的 Sender Reserve、冻结对应接收 Peer 的 Grant；同 Peer 的所有连接共用这个发送账本，因此都会停止发送。已有 Connection 发现冻结账本会显式失败，宿主必须继续推进 Close 以归还缓冲 Held；这一过程不保证未提交的应用数据发完。其他 Peer 可继续工作。

协议推进必须与账本同 owner，并遵守对应 context 暂停屏障。宿主继续处理真实 TX 终结与迟到 RX、Release 和实际 refill，直到接收端逐类消费计数等于对端固定的最终接受量且全部 receipt 结清；不要提前 Freeze 整个 Eager RX 域。协调器在核实后退休 Peer 并 ACK，双方确认完成也不替代 TX DMA、来源、通知/async 引用或 M1 证明。M5.3.8 的 PeerCreditProgress 已接入 Runtime 调度和专用 fd 管理：BeginWithdrawal 显式发起，终态回调后 Release；主动 Abort 仅解除绑定并保留未证实责任。RawDomain::BeginClose 只停止新准入和授信，保留 RX 收敛路径；其 AdvanceClose 在撤回绑定等责任解除后推进单域关闭。跨域编排仍由宿主完成，接线步骤见 [Peer 授信撤回](peer-credit-control.md#宿主推进顺序)。

控制器绑定期间拒绝直接关闭 Sender、外部 RetirePeer 以及整域 CloseDestroyed。错误或超时保留冻结账本；控制器 Close 解除绑定后才可继续真实 M1 关闭与 M5.3.6 清退，但不会替宿主补足关闭证明。`complete` 与历史统计不能用于删除资源或自动退休来源。

## 验证与剩余范围

新增测试覆盖超过外层 fd 数量的本地 TCP、oneshot 重订阅、旧 fd 别名、完整代次、预算/公平队列、零分配热路径、来源伪造/旧会话、多 context 暂停、控制背压、取消后 DMA 收敛，以及仅由 RuntimeShard 推进的 Mock raw 双端 DATA/Credit/补投/FIN。回归结果见 [roadmap](roadmap.md)。UB 真机和 40k 规模性能未执行。

M5.3.2/3 新增协议边界、原子登记/更新、授权和来源就绪门禁、双主动 Lane 自动来源解析、双向同时更新、ACK 前禁止投递和 ACK 后迟到旧 RX 测试。M5.3.4 增加 fd 接管回滚、重复 fd、带代次句柄复用、自动 Wake、TCP/协议 TCP、Owned raw DATA/Credit/FIN、关闭保留 DMA 和迟到来源的测试。

M5.3.5 增加并发提交/领取、完成背压、有限预算、缓冲复制、陈旧句柄、一次执行与零分配测试；Mock raw 闭环还覆盖 mailbox Read/FIN、Credit 补投以及慢导入期间同 context 暂停、另一 context 继续执行。

M5.3.9 已交付 RawDomain 单域所有者。Runtime 解绑后，`ConnectionIoBinding::Close(true)` 从 ConnectionProgress 的固定补投数组移除本域再清除借用；未停止回调时拒绝，不分配、不调用 provider。ConnectionProgress 可先 Close，但对象必须存活且不得重开，直到桥接解绑。单独使用桥接的宿主也须遵守这一顺序。

M5.3.10 的 RawStack 已补齐最多四 context 顶层装配及 Slab/导入/ConnectionOwner 的关闭门禁。BeginClose 保留 Runtime 控制调度到 unimport 等责任收敛，再逐域关闭。下一阶段收口显式应用 API 和零拷贝接口。宿主仍须组织静默、来源退休与进度解绑，不能通过 ConnectionOwner 析构或授信 ACK 推断物理资源可释放。跨 Peer 弹性授信调度与 M6 Grant/RNDV 不在本次交付内。
