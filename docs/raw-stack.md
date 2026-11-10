# M5.3.10：多 context 资源装配与关闭

[`RawStack`](../kbsocket/runtime/raw_stack.hpp) 拥有一个进程独占的 URMA 会话，将最多四个 context 接入一个 RuntimeShard。配置按 `device_name + eid_index` 标识端点，允许两个裸设备、每设备两个 EID；每个端点对应一个独立 [RawDomain](raw-domain.md)。它同时拥有连接调度与 ConnectionOwner、业务 mailbox、PeerCreditProgress、ImportProgress，以及共享 Slab、MemoryBudget 和 EndpointImportCache；显式启用 RNDV 时另含共享 TsegImportCache。

宿主先加载 UrmaApi，RawStack 负责 Init、设备目录、context 和下级资源。全部成功关闭后宿主才可 Unload。认证、远端描述符来源、Peer 发送共账、控制 fd 建立、来源退休与独立静默证明继续由宿主负责；这不是透明重连、故障 CQ 恢复或稳定 C ABI。

## 配置与创建

`Open(endpoints, config)` 接受 1–4 个 RawEndpointConfig。一个栈内所有 local_domain 使用相同的 runtime_instance，每个 receive_domain 与 context_incarnation 必须非零且互不重复；宿主还须保证它们跨生命周期不复用。重复端点或身份在 provider 初始化前拒绝。事件模式要求每个域配置 `queues.events=true`。

RawRuntime 从设备目录提供按端点索引的能力快照：同设备不同 EID 共用快照，不额外查询 provider。装配依次完成会话/context、共享 Slab 注册、导入缓存、各域及其四个 companion，最后打开 RuntimeShard；每个 context 的 raw、连接、导入、mailbox、撤回源共用同一个暂停组。全部成功前不发布任何资源视图。

| 配置 | 含义 |
| :--- | :--- |
| `RawEndpointConfig.device/domain` | 设备/EID、完整域身份及队列、授信、路由、来源容量 |
| 每端点 callback/cookie | 连接进度、命令完成和授信撤回通知；回调不得重入，cookie 存活至成功关闭或保留责任结束 |
| `connections_per_context` | 每 context 的连接槽和调度容量 |
| `withdrawals_per_context/commands_per_context` | 每 context 的撤回槽与 mailbox 容量，包含未领取的完成 |
| `slab/memory_budget_bytes` | 一份 backing 在全部 context 注册；预算只扣一次，且只覆盖该 Slab backing，不含域 RX、队列、mailbox 或 provider 元数据 |
| `enable_rndv/segments` | 可选段缓存；与 endpoint 导入共用每 context 暂停屏障，默认关闭，不自动公告 Arena |
| `imports/runtime` | 共用导入缓存、Reactor 公平预算/事件模式和控制执行器配置 |

创建失败保留 `open_error` 和稳定地址的 State，必须 BeginClose/AdvanceClose；下级组件自身回滚失败的证据也保留。成功前没有连接或外部引用，不需要为了回滚临时启动 Runtime。已经投递的 RX 仍需实际 M1 关闭，不能因 Open 失败就释放 context。一个对象只承载一次生命周期，关闭后重建使用新对象与新身份。

## 业务接入与推进

`resources(index)` 返回该端点的 context、RawDomain 借用视图、ConnectionOwner、ConnectionMailbox 和 PeerCreditProgress。`shared()` 返回 Slab 与端点缓存，启用 RNDV 后还返回段缓存。宿主按 [连接 Runtime](connection-runtime.md) 提供已连接 fd、身份及经认证的 HELLO/描述符，通过 ConnectionOwner 准备和操作连接；多个连接继续共享 Peer 发送账本，不能按连接复制额度。

Tick/RunOnce 负责唯一 owner 推进。RequestReplacement 仍须独立 Lane 证明，只在 Ready 阶段允许。业务线程只能使用预先取得的 mailbox 指针，不能直接获取资源、Inspect 或调用 owner；Notify 也由 owner 串行调用。

每个 context 暂停期间 resources(index) 返回 InUse，其他 context 仍可取得视图和推进。shared、Inspect、BeginClose、AdvanceClose 涉及全栈，任一 context 暂停就拒绝，且不部分停止其他域。已缓存的借用指针不豁免这一门禁；共享 Slab/import 也不能在任何相关控制任务运行时操作。外部不得单独 Close/Open 栈内组件或另建 Reactor 驱动它们。

## 两阶段关闭

BeginClose 停止域 TX/授信、新连接、新命令和 Slab Acquire，并令导入条目失效。它保留 RX 消费/归还/补投，**不停止 Runtime 的控制任务准入**：已有 import 结果仍须收割，暖缓存和释放后的引用仍须通过 ImportProgress 执行 unimport。此阶段仍可 Tick/RunOnce，也不保证尚未提交的应用数据发完。

宿主继续处理以下独立责任，再调用 AdvanceClose：

1. 停止 mailbox producer，执行所有已接受命令，由 collector 领取全部结果并退出。提供 `clients_stopped=true`，空 outstanding 不替代线程静默。
2. 逐连接 CloseConnection，等真实 TX 完成后重试保留中的关闭；取得来源证明后 RetireSources，最后 Release 槽。单条连接关闭不撤回整个 Peer 的授信。
3. 在需要终止 Peer 共账时显式启动撤回，继续 RX/refill/TX，终态 Release；无法完成时只能在保留责任的前提下显式 Abort，并独立取得物理关闭证明。完成/失败的撤回槽未释放前也阻止全栈关闭。
4. 归还共享 Slab 的所有 CPU Lease、local DMA 和远端 Grant；释放 import 引用，继续 Tick 至缓存 entries/leases/tasks 全部归零。错误、超时、空 CQ 或控制 EOF 都不代替真实终结证明。
5. 为每个配置 context 提供完整 M1 证明：远端停止、事件确认、外部使用者退出和 CQ 健康。覆盖整个域及外部缓存视图/绑定，不能只用某一 Peer 的 ACK 推断。关闭重试仍须提供有效证明。

AdvanceClose 先无副作用检查所有域的连接槽、调度注册、命令、撤回槽、路由、来源、TX 账本和完整 flush 边界，以及共享 Slab/endpoint/segment 的引用与控制任务责任。任一项未清零返回 InUse，保留整个 Runtime 继续收敛。全部满足后才关闭 Runtime 并收割剩余控制任务，再进入不可逆的物理关闭阶段。

物理关闭按 context 配置逆序进行：解除 companion → RawDomain 的 M1 关闭及 receipt 清退 → 共享 import 元数据 → Slab 逐 context 注销 → context 逆序删除 → URMA Uninit。域内 Held 可跨越 JFR 删除，归还前不释放 backing；任何域未完整关闭，都不会释放共享 Slab 或 context。收到迟到 RX 的调用只结账，下一次重新确认事件屏障后才删除。

进入 ClosingDomains 后不再允许 Tick，借用视图只供最后的责任终结，例如 Held 归还。DeleteJetty/JFR/MR/context 失败保留阶段与 `close_error`，重试不会重复已成功步骤。Uninit 报错时报告原始错误；底层已终止的会话不会再次 Uninit，后续关闭完成仍保留历史错误供诊断。

## 保留与验证边界

不可信 CQ 终结进入 Retained，不提供强制清零或在线恢复。任何未成功 Closed 的析构保留整份 State，包括 RawRuntime、共享内存、回调桥接和控制线程，防止个别成员析构先删除 context。宿主仍须保留外部 provider 代码、cookie、Peer 账本与其他相关依赖；保留不是正常释放。

测试使用 Mock URMA 与本地 TCP，覆盖 2 设备 × 2 EID 装配、部分创建、关闭门禁、缓存 unimport、context 暂停、关闭重试及 DMA 责任。UB 真机和 40k 性能验收未执行；M5.4.1 的 [StreamBinding](stream-api.md) 已把资源门禁接入 Eager/NativeTCP copy-stream C ABI，M5.4.2 已追加统一连接事件；M5.4.3 已交付 [独立 NativeTCP C Runtime](stream-runtime.md)，raw 应用编排、零拷贝接口与 M6 WRITE RNDV 继续后续交付。

M6.3.9 的 [Connection RNDV 装配](rndv-connection.md) 已接通显式预算、段缓存控制进度和真实完成分发；目标 Arena、对端授权和资源限额仍由宿主准备。默认及 RawStreamRuntime 便捷入口继续使用 Eager。
