# M5.4.4：raw 应用 Runtime 与建链编排

[`RawStreamRuntime`](../kbsocket/api/raw_stream_runtime.hpp) 将 [RawStack](raw-stack.md)、每个 context 的 [StreamBinding](stream-api.md) 和连接事件转发组合为一个 C++ 宿主入口。应用在建链完成后使用既有 `kbs_stream_table` 进行复制收发、Watch/Poll、关闭和释放。Runtime 拥有本地物理资源与发布表；宿主继续负责 provider 加载、协议 fd 建立、远端认证、Peer 共账和独立生命周期证明。

全部入口由同一 owner 串行调用，不得从进度回调重入。它不自动重连、重放 WR 或切换到 TCP；这里的协议 TCP fd 用于已约定支持 kbsocket 协议的入口，不能向普通 TCP 服务试探发送 HELLO。

## 创建和身份

先加载 `UrmaApi`，再调用 `Open(endpoints, stack_config)`。配置沿用 RawStack 的设备/EID、域、内存预算和 Runtime 参数，支持最多四个 context，包括两个设备、每设备两个 EID。每个 `RawStreamEndpointConfig` 还包括：

| 字段 | 宿主责任 |
| --- | --- |
| `streams` | 发布表的非零 `table_id`、固定容量和 `max_io_bytes`；表身份跨实例、跨生命周期不复用 |
| `endpoint_generation` | 被动接收端点的不可复用代次，不能用可复用的 jetty ID 代替 |
| `authorization` | 本地策略身份，供对端与独立认证结果比对；它不是认证凭证 |
| `credit_epoch` | 本地接收授信代次，与本域的 Peer 接收账本保持一致 |

`runtime_instance`、`receive_domain` 和 `context_incarnation` 仍由域配置提供，并遵守 RawStack 的唯一性及不复用约束。每条连接的 session 由 `AdoptProtocol` 单独提供，不能复用端点代次或 fd 作为 session。

Open 从实际被动接收 jetty 调用 `GetRjetty`，复制有界描述符并配对 `PutRjetty`；保留 provider 扩展并按 RM_CTP 设置传输类型。每次 Adopt 从当前池重新 `CaptureTxLanes`，将真实主动 Lane 来源和代次写入 HELLO。被动导入目标与主动 Lane 列表各有用途，宿主无需手工构造硬件描述符。导出发生在控制路径；HELLO 快照不证明旧 Lane 的 DMA 已终结。

创建失败也可能留下已建立的物理资源。宿主应保留实例，按 `stage()`、`Inspect()` 及错误继续关闭，不能因为 Open 失败就卸载 provider。

## 两阶段建链与 Peer 共账

1. 宿主建立已连接、非阻塞的协议 fd，确认对端支持协议，设置 `RawStreamConnectOptions` 中的 session、预期授权身份、预期 runtime 和绝对单调 deadline，再调用 `AdoptProtocol(context, &fd, options, now_ns)`。`expected_peer_runtime=0` 不固定远端 runtime，仍须在准备阶段按策略认证实际身份。
2. 持续 `Step()`，通过流状态或事件等待 `PREPARING`。`Peer(stream)` 返回远端 HELLO 的副本。宿主独立验证远端身份、描述符 ABI/来源和完整主动 Lane 列表，不能将 HELLO 自报字段当作认证结果。
3. 宿主选取或建立该 Peer 的共享接收与发送账本，保证接收授信足够，再传入 `RawStreamPreparation` 调用 `PrepareRaw`。Runtime 自动绑定本地 context、路由、接收域、TX sender、共享 Slab/import 和来源 registry。准备阶段明确提供 `descriptor_compatible`、`peer_lanes_authorized`，在线 Lane 变更另由 `authorize_lane_update` 授权。
4. 继续 `Step()` 推进导入与双向 READY；只有 `RAW_ACTIVE` 后才可收发业务数据。失败按原状态机报告，不重拨或回退。

`AdoptProtocol` 的 fd 参数是 `int*`：一旦 owner 接管就立即写回 `-1`，即使随后的发布失败也是如此。失败且 fd 仍非负时，fd 仍归调用方。不得在看到错误后无条件再次 close 原 fd；未完成的内部回滚由 Runtime 保留并在关闭时重试。

`HostResources(context)` 为宿主提供 Peer 授信、撤回和 mailbox 编排所需的借用视图。接收 Peer 通过本地 `ReceiveCreditDomain` 管理；发送 `SenderCreditAccount` 由宿主持有并借给连接，其身份分别为：

| 账本 | 身份 |
| --- | --- |
| 本地接收 Peer | `{远端 runtime, 本地 receive_domain, 本地 credit_epoch}` |
| 发往远端的 SenderCreditAccount | `{远端 runtime, 远端 receive_domain, 远端 credit_epoch}` |

同一物理 Peer 授信域上的连接共用账户，不能逐连接新建、清零或重复发放已有额度。`control_credit_target` 是共享 Peer 的固定控制额度目标，各连接保持一致且至少为 4。准备或连接关闭失败不证明已公告授信可以回收；单条流 Release 也不等于 Peer 撤回完成。外部发送账户、Lane authorizer cookie 和宿主回调 cookie 必须覆盖全部借用与保留责任。

## 推进和应用表

`table(context)` 返回借用的 C stream 表，后续使用 `kbs_stream_write/read/watch/poll_events/close/release`。表的存活依赖 Runtime；完整 Close 后已有表入口返回 NOT_READY，实例销毁后不得继续访问。句柄包含表身份与完整代次，不能跨表或在 Release 后复用。

`Step(false)` 执行非阻塞 Tick；`Step(true)` 允许底层 RunOnce 等待，已有待取应用事件时仍只 Tick。事件通过每张表的 `kbs_stream_poll_events` 领取，Watch 为一次性订阅，Poll 不自动重新订阅。Runtime 同步转发连接进度至对应表，并保留配置中的连接、命令和授信撤回回调及其原 cookie；宿主回调不得反向调用 owner 或 Runtime。

控制任务暂停某个 context 时，该 context 的宿主资源和流访问遵守原暂停门禁；准备涉及共享 Slab/import，必须避开全部 context 的暂停窗口。缓存借用指针不豁免此规则。宿主不能独立 Close/Open 栈内成员，也不能绕过发布表另外管理同一批连接。

`RequestReplacement` 转交已有 Lane 更换流程，仍需真实 `JettyLaneQuiescence` 证明；新 Lane 公告不代替旧来源退休。`Inspect()` 用于查看整体资源与关闭阶段，不提供推定硬件静默的快捷条件。

## 关闭与保留

需要优雅半关闭时，先 `shutdown_write` 并持续推进和读取尾部；`BeginClose()` 停止新业务及资源准入，不保证尚未提交的应用数据发送完成。关闭后续顺序为：

1. 停止并收割业务 mailbox 使用者。逐流 `kbs_stream_close`，对尚有真实 TX 责任的流继续推进并重试。
2. 流进入 Closed 后，以独立 `RxSourceRetireProof` 调用 `RetireSources`，证明对应 context incarnation、远端发送停止、已接受流量终结及接收事件排空；再 `kbs_stream_release`。逻辑 Close、控制 EOF、空 CQ 和状态 READY 都不能替代该证明。
3. 完成 Peer 撤回槽的释放或显式保留责任的中止，归还外部内存 Lease，持续推进导入引用清退。账户由宿主管理，Runtime 不擅自关闭或重置。
4. 向 `Close(RawStackCloseProof)` 提交 `clients_stopped` 和按配置顺序覆盖每个 context 的 `ReceiveDomainQuiescence`。这份证明覆盖整个域的远端、事件、外部访问及 CQ 健康，不能只依据某条流的关闭结果。

仍有发布流、来源、命令、撤回、导入或 DMA 责任时关闭保留状态并返回错误，宿主继续完成对应责任后重试。处于 Stopping 时仍可 Step；进入 ClosingDomains 等物理关闭阶段后不再 Step，只完成允许的最后归还并重试 Close。物理删除和注销顺序由 RawStack 执行。

未完整 Closed 的析构保留整个内部 State，包括发布表和回调桥，避免下级对象仍借用已释放的 cookie。保留不是正常释放：宿主也必须继续保留外部账户、cookie 和 provider 代码，不能据此卸载依赖或假定 DMA 安全。

## 交付边界

本阶段提供 C++ raw 生命周期与协议准备入口，复用既有 C copy-stream 表。协议 connect/listen/accept 与认证仍由宿主完成；尚未提供独立 raw C Runtime ABI、监听事件或接收 Lease 借用接口。无 UB 真机时，Mock URMA 与本地协议 fd 的验证只能覆盖装配和软件状态机，不能证明真实硬件行为、吞吐或 40k 规模指标。

## 应用 Buffer Lease 表

M5.4.6 追加可选 `OpenBuffers(BufferBindingConfig)` 和 `buffers()`，将应用缓冲区表接到 RawStack 已注册的共享 Slab；不按连接或 context 新建内存池。每次分配、取视图与归还都经 `RawStack::shared()` 检查全部 context 的暂停状态，拒绝时保留原所有权供重试。

BeginClose 停止新分配/可写视图，允许合法只读/归还；Close 在全部公开 CPU Lease 归还后关闭表，再按原有证明推进物理资源关闭。应用归还不等于 DMA/Grant 终结，底层 Slab 仍独立核对全部责任。详见 [应用 Buffer Lease API](buffer-api.md)。

M5.4.7 的 `kbs_stream_write_lease` 从同一共享 Slab 直接引用应用源，入队即冻结 CPU 视图；扩展身份需要至少两个发送 SGE。使用 `kbs_buffer_inspect` 查询整份源的可复用状态，取消/连接失败与源责任终结分别处理。详见 [Eager Lease 发送](lease-send.md)。
