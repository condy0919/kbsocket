# 建链、端点导入与接收路由（M5.1）

M5.1 将 M4 的身份、乱序窗口和授信原语接入建链准备流程：通过非阻塞协议流交换 HELLO，合并相同端点的导入请求，在接收状态与路由就绪后发送 READY，双方 READY 完成后才开放本端发送准入。

本文描述可显式驱动的 M5.1 建链组件：接管宿主已连接的 fd，借用预建 raw 资源、信用账本和连接对象。M5.2 已通过 [Eager Connection](eager-connection.md) 接通有界 copy-stream 和 `NativeTcpBackend`，自动 Runtime 装配与稳定 ABI 仍待后续。与后续集成的边界见[路线图](roadmap.md) M5，协议及信用原语见[连接协议原语](connection-primitives.md)。

## 1. 组件与调用顺序

| 组件 | 代码 | 职责 |
| :--- | :--- | :--- |
| `HandshakeChannel` | `kbsocket/control/handshake_channel.*` | 固定格式编解码、非阻塞分片读写、整帧边界与截止时间 |
| `EndpointImportCache` | `kbsocket/control/endpoint_import_cache.*` | 有界 Single-Flight、导入句柄引用、失效及异步清理 |
| `ConnectionTable` | `kbsocket/connection/connection_table.*` | 预留完整 token、发布/撤回路由、实际来源与代次校验 |
| `ConnectionHandshake` | `kbsocket/control/connection_handshake.*` | HELLO/READY 状态机、准备门禁、路由先于 READY 发布 |

这些组件均由单 owner 调用，禁止并发或重入。正常 raw 建链按以下顺序推进：

1. 宿主预建本地 context、被动接收端点、JFR/JFC、RX 内存及 `ConnectionTable`，通过已知支持协议的入口获得已连接、非阻塞的流式 fd。
2. `ConnectionHandshake::Open` 从表中预留 token，并将其写入本端 HELLO。Open 成功才接管 fd；任何 Open 失败均由调用方继续持有 fd。
3. 反复调用 `Progress(now_ns, budget)` 交换 HELLO。校验授权策略身份、可选固定远端 runtime、协议版本和共同能力；双方均支持 raw 时优先选择 raw，否则选择共同支持的 TCP。
4. raw 路径由宿主先在本地 `ReceiveCreditDomain` 中建立对应 PeerRuntime 的有效授信，再调用 `PrepareRaw`。该方法验证 ABI 门禁、信用身份与额度，向缓存申请导入引用，并初始化本连接的 `ReorderWindow`；不在 owner 上执行 `ImportJetty`。
5. 宿主通过控制执行器完成缓存导入。协调器确认导入、信用域及接收窗口均可用后，先 `ConnectionTable::Publish`，随后排队发送 READY。每次继续写出 READY 前仍检查已发布路由的 raw 依赖是否有效。
6. 只有收到完整匹配的对端 READY，且本端 READY 全部写出，状态才变为 `kRawActive` 或 `kTcpActive`。

`route_owner` 指向宿主的实际连接对象，必须在 `PrepareRaw` 前完成接收初始化。对端可能先完成双方 READY 并立即发送首包，本端此时仍处于 `kWaitingReady`；已发布路由应允许接收该首包，不能再以“本端尚未 Active”为由丢弃它。Active 门禁约束本端业务发送，路由发布则建立首包接收条件。

## 2. HELLO、READY 与两个方向的身份

控制帧使用固定大端字段，不直接发送 C++ 结构体。12 字节帧头包含 `KBSH`、握手版本、消息类型、保留字节和正文长度。M5.3.2 将握手版本升为 2：HELLO 正文为 104 字节固定字段、每条 32 字节的主动 Lane 列表以及最多 4096 字节的 opaque 端点描述；最多 64 条 Lane，整帧上限 6260 字节。READY 正文仍为 144 字节，REJECT 为 20 字节。旧 v1 显式拒绝，不能混用两版。

每份 `HandshakeIdentity` 包含 runtime 实例、接收域、被动端点代次、授权策略身份、不可复用 session nonce 和完整 32-bit slot/generation。HELLO 另携带数据 wire 版本、能力位、本端接收授信 epoch、被动 EID/Jetty ID、导入 token 及描述符，以及独立的主动 Lane pool epoch、数量和列表。每条 Lane 包含 EID/UASID/Jetty ID/generation。RAW 快照不能为空，不允许零代次、重复物理来源或主动/被动角色冲突；TCP-only 的 raw 字段及 Lane 数量/epoch 必须为零。宿主负责生成不复用的 runtime/domain/session 身份；协调器不会生成随机身份，也不会把自报 `authorization` 当认证凭证。

READY 完整回显发送方和接收方各自的 identity，并携带所选传输、授信 epoch 以及数据、控制、帧序号和驻留字节的累计上界。raw READY 的这些额度必须有效且 epoch 与该发送方 HELLO 一致；TCP READY 不携带 raw 额度。重复 HELLO/READY、身份不一致、意外版本或错误传输选择会使握手显式失败。

假定本端为 A、对端为 B，入站身份与信用的两个方向分别是：

| 用途 | 应采用的身份 |
| :--- | :--- |
| B 发给 A 的数据帧路由 | B 的 runtime；A 的本地接收域、session 和目的 token |
| 该帧消耗 A 的物理 RQE | 本地信用账本中的 `{B.runtime, A.receive_domain, A.credit_epoch}` |
| B 发给 A 的 `CREDIT_UPDATE` | 目的仍为 A 的 session/token；授信 epoch 为 **B.credit_epoch**，描述 A 后续向 B 发送的额度 |
| A 发出的 READY | sender 为 A identity、receiver 为 B identity；公布 A 接收域向 B 承诺的额度 |

两端 epoch 可以不同。路由绑定中的 `credit_epoch` 用于校验对端广告，不可拿来结账本地收到的物理 RX。多个连接共享同一 PeerRuntime/ReceiveDomain 授信时，READY 重复广告同一个累计上界不会产生额外信用；后续发送集成必须共享对应发送账本，不能为每条连接重新建立一份完整额度。

## 3. 非阻塞协议流与失败边界

`HandshakeChannel::Open` 要求已连接的非阻塞 stream fd，以及宿主明确给出的 `compatible_peer`。不能向普通未知 TCP 服务试探发送 HELLO。协议字段的授权身份校验也不能替代入口认证或租户策略检查。

每个方向只保留一个有界帧缓冲。`Progress` 默认共处理最多 16384 字节、8 次系统调用；调用方可缩小预算，系统调用预算上限为 64。短写、短读和 `EAGAIN` 保留当前偏移后立即返回，不使用阻塞 wait/poll。时间采用调用方提供的单调时钟，截止时间贯穿整个握手，不因碎片到达或重复推进而延长。

接收恰好一帧后停止读取，下一帧仍留在内核。协调器收到对端 READY 后关闭握手读取，只继续排空本端输出；因此与 READY 合并到同一 TCP 包中的首个业务字节不会被吞掉或误解码。Active 后 `TakeEstablishedFd` 可转交这个已消费完协议字节的 fd：raw 路径供后续控制通道使用，协商 TCP 路径供后续 TCP 后端使用。

错误、超时和取消关闭已接管的协议 fd，保留首个失败原因，并撤回已发布路由。raw 导入失败不会在同一个脏流上自动变成普通 TCP，也不迁移已经激活的旧字节流；重连及是否选择新 TCP 连接由宿主策略决定。当前只实现初始共同能力选择，尚未实现双 IOdie Primary/Secondary 拓扑决策。

## 4. Single-Flight 缓存与控制任务

`EndpointImportCache` 的键同时包含本地 context 指针及 incarnation、远端 runtime、接收域、端点代次、EID、Jetty ID、授权身份、RM 模式与 CTP 类型。相同键的描述符字节和导入 token 还必须完全一致；不一致直接拒绝，不能覆盖旧缓存。远端重启、端点换代或本地 context 换代均使用新键。

描述符最多 4096 字节，缓存验证 native `urma_rjetty_t` 的身份/模式及用户扩展长度，拒绝 driver 扩展和共享 TP 标志。协议通道只搬运 opaque 字节；宿主仍须确认双端 provider ABI 兼容和导出来源可信。不能由固定 wire 字节序推导 opaque provider 描述符也具备跨 ABI 可移植性。

Open 一次分配所有条目、引用票据和描述符存储。默认上限为 64 个条目、4096 份引用、16 项在途操作；配置硬上限为 4096 个条目和 65536 份引用，在途操作不能超过条目容量。`Acquire` 的每个调用取得独立 `ImportLease`，同键等待者共享一个导入操作；`Get` 在完成收割前返回 Pending，成功后才返回 target。`Retain` 创建另一份独立引用，复制 `ImportLease` 结构本身不会增加引用数。

缓存借用 `ControlExecutor`，不直接调用其 Poll：

1. `NextOperation` 返回待导入或待清理的工作，但尚未提交。
2. 宿主暂停该 context 的全部 post/poll/通知/async 使用者，并排除外部慢控制操作，再以完整 `ImportContextPause` 调用 `StartOperation`。
3. 执行器 worker 调用 Import/Unimport。缓存内同一 context 至多一个任务在途，独立 context 可并行。
4. 唯一执行器 owner 收割完成，根据 `OwnsCompletion` 路由给缓存的 `HandleCompletion`；其他任务仍交给它们各自的所有者。暂停屏障保持至该完成已收割，不能仅因 completion fd 可读就恢复访问。

上述显式控制接口继续保留。M5.3.1 新增 `ImportProgress` 和缓存 Prepare/Commit/Discard，通过 `ContextProgressGroup` 接入 `RuntimeShard` 的完整 context 暂停和任务路由，见 [连接 Runtime](connection-runtime.md)。所有引用、任务参数、本地 context、URMA 函数表及执行器必须维持有效生命周期。

## 5. 引用、取消与关闭

零引用的成功条目作为暖缓存保留，不因连接关闭立即 Unimport。`Invalidate` 阻止新借用和通过旧票据继续 `Get`，但已接受 WR 仍可持有原 target 依赖；最后一份引用归还后，清理操作才可调度。每个实际数据任务必须独立 Retain，直到该任务全部已接受 WR 取得终结证据后 Release，不能只借用握手对象的引用。

全部等待者在提交前取消时可直接撤销未开始的条目；任务一经执行器接受，即使所有等待者取消也仍要收割结果，再异步 Unimport。取消不是阻塞 provider 调用已经撤销的证明。Unimport 失败保留句柄并允许重试，不能清零后报告已经释放。

`ConnectionHandshake::Close` 关闭未转交的 fd、撤回路由，等待乱序/应用持有的接收 Lease 结束后释放本对象的导入引用并退休 token。有未释放接收责任时返回 `kInUse`，宿主继续 Drain、归还真实 Lease 和 receipt 后重试。协调器关闭不会撤销 PeerRuntime 信用，也不执行远端静默、JFR 删除或 RMA Grant 撤权。

缓存 Close 先停止准入并使条目失效；仍有引用、任务或句柄时返回 `kInUse`，宿主继续归还、调度清理并收割。缓存析构不能替代这套流程：无法安全终结时保留内部存储，而外部 context/函数表等依赖仍由宿主保留。

## 6. RX 集成责任与验证范围

`ConnectionTable::Resolve` 为 O(1) 路由查询，要求完整本地接收域和实际接收路径提供的已校验来源，不能把预期绑定冒充观测来源。短 token 与扩展 token 均核对完整代次，撤回后不能重新发布同一 token；槽位复用递增代次，完整 generation 耗尽后永久退出。

无论 Resolve 是否成功，真实 RX 都已经消耗物理 RQE。宿主须先按实际 Peer/域记账；无法可靠归属时使用 `ConsumeUnknown` 并冻结该域。旧 token、晚到帧、协议错误不能绕过 RX 记账，也不能把旧授信重复发给新连接。接收窗口的 receipt、物理信用 receipt 和真实 `RxBufferLease`/Slab Lease 的归还仍是不同责任，只有真实补投被接受后才能恢复物理可授信额度。

当前测试覆盖协议分片与合并、有限 IO 预算、超时/取消、真实本机 TCP 协议交换、READY 前路由发布及首包竞态、相反方向的信用 epoch、1000 等待者单次导入、旧身份拒绝、部分准备失败、导入取消及清理重试。raw provider 由 Mock 替代；本机 TCP 握手测试不代表完整 NativeTCP 后端，也没有形成 UB 真机正确性、吞吐或 40k 建链性能结论。

M5.2 已交付 connect/listener、NativeTCP 后端、有界 Connection copy-stream、RX/补投结账与发送共账接线。M5.3.1/2 已增加 Runtime 公平推进、实际 RX 来源映射和 HELLO 主动 Lane 自动登记；完整 Connection 在来源安装之前禁止 READY，见 [连接 Runtime](connection-runtime.md)。低层 ConnectionHandshake 仍是可独立组合的原语，不替宿主消费 RX 或安装来源。业务 mailbox、在线 Lane 更新、自动资源/拓扑管理、显式 C ABI/零拷贝 API，以及关闭协商与远端静默证明仍待实现。M6 的 WRITE Grant/DONE 生命周期保持独立。
