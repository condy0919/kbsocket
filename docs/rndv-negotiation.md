# M6.3.6：RNDV 能力与 Arena 目录协商

`ConnectionHandshake` 在原有非阻塞 HELLO/READY 流程中交换可选的 WRITE RNDV 能力和最多 8 项 Arena 目录。未启用时 HELLO/READY 保持原有 v3 编码；只有双方声明 RAW 与 WRITE RNDV 后才形成候选限额，双方 READY 完整确认后 `rndv()` 才返回结果。旧二进制会拒绝未知 capability，显式启用 RNDV 不承诺与旧版本透明互通。

本阶段提供协商与导入请求适配，还没有装配 Connection/Runtime 的自动 RTS/GRANT/WRITE/DONE 收发。现有 `Connection::Open` 在接管 fd 前拒绝 RNDV capability，避免 Eager 接收端公告尚不能处理的能力。底层 `ConnectionHandshake` 可供后续装配和显式测试使用。

## 能力与限额

HELLO 的 `kHandshakeCapabilityWriteRndv` 位要求同时有 RAW 能力，并携带 `RndvOffer`。协议版本为 1，当前唯一支持的契约是 `kRndvSuccessfulWriteDone`：发送端只能在全部 WRITE 成功、完整覆盖目标且停止未来写入后发送 DONE。能力声明不是认证、逐 WR 完成证明或关闭证明；接收端仍按 [接收协调器](rndv-receiver.md) 验证每个通知。

Offer 包含单 WR WRITE 大小、整条事务大小、最大 Grant 数量、共同 SN 窗口及总授权字节上限。单 WR 上限为 200 MiB；最大并行 Grant 为 128，共同窗口不超过 4096。整条事务不超过现有 GrantManager 的 UINT32_MAX 字节上限，按单 WR 上限分块也不能超过单引擎的 4096 chunk。实际引擎仍受本地设备能力及全事务池 chunk 总预算限制，宿主不得将能力上限视为全部资源已经分配。

首版在两端所有对应限额中取较小值，得到相同的双向保守上限。双方 READY 必须准确回显该结果；本端准备的重排帧/字节预算，以及对端 READY 的初始预算，都必须容纳协商上限。GrantManager/WRITE pool 的 Grant 槽窗口须采用协商的 `window`，共同 Eager/RNDV 重排窗口可更大。双方只有一端声明 RNDV 时继续使用原 RAW Eager；不能在双方已经声明后因本地准备失败而单方静默禁用。

这些数量是上限，不是已经划拨的内存或当前空闲 Credit。宿主仍要通过共享 Slab/MemoryBudget、GrantManager、通知的未来控制额度和连接授信共同准入，不得为每个空闲连接重复承诺物理容量。

## 目录来源与线格式

`SlabAllocator::ExportSegment` 核验活动注册段的 context、EID/UASID、精确 backing 范围、注册权限及固定属性，返回值快照。CPU-only、停止/关闭状态、未注册 context 或 provider 返回的不一致段均拒绝导出。`ExportRndvArena` 将快照映射到规范字段，并附上宿主分配的非零 Arena 身份和代次；当前 Slab 注册只导出 NONE/零 token。目录不增加 Slab 引用，宿主须保留 Slab/context，并保证身份在旧会话和访问仍可能存在时不复用。

每项包含 Arena id/generation、地址/长度、最大单 Grant 大小、EID、UASID、token id、token、权限及 token policy。目录只支持固定注册内存和 NONE/PLAIN_TEXT token；必须有 WRITE 权限。拒绝地址加长度溢出、无效 UASID/token id、同目录重复 Arena 身份或重叠范围。目录 EID 必须与同一 HELLO 的接收端点一致，每项必须容纳本端声明的最大事务。token 属于访问材料，不应写入日志。

全部多字节字段使用大端，不序列化 `urma_seg_t`、padding 或本地指针。限额编码 40 B，Offer 固定前缀 48 B，每项目录 72 B，最多 624 B；所有保留位必须为零。编码失败不写输出，解码要求精确长度并先校验数量与边界。没有 RNDV 位时不得携带隐藏目录；READY 仅在选中 RNDV 时追加 40 B 限额。

## 信任门禁与段导入

`PrepareRaw` 的 `rndv_peer_authorized` 默认关闭。只有宿主已经独立认证对端及成功通知契约，并允许导入这些规范段描述时才能开启。共同声明 RNDV 而未授权时，在任何缓存引用或重排分配前返回 Unauthorized；网络 authorization 数字和能力位不能自行打开该门禁。

`PrepareRndvImport(grant)` 仅在双方 READY 确认后的 RAW 活跃状态工作。它验证当前 endpoint 缓存及接收域仍可用，再核对完整 Grant 身份、槽位、长度、Arena 代次和范围，最后构造 `TsegImportRequest`：

- A 向 B WRITE 时，GrantBinding 的 sender/receiver runtime 分别为 A/B，domain、session、token 均取 B HELLO；不能误用反向 GRANT 通知的路由身份。
- 本地 context/incarnation 来自本端实际 PrepareRaw，远端 runtime/domain/authorization 来自同一份已验证的 B HELLO，Slab id/generation/EID 来自其中匹配的 Arena。
- 导入描述逐字段转换为本地 URMA 类型，不接受调用者另填一套互相矛盾的缓存身份。

该方法不调用 provider，也不产生导入引用或 WRITE 许可。调用者仍须把 Grant 与原 RTS 配对，通过 [TsegImportCache](tseg-import-cache.md) 和共享 context 暂停域推进导入，并由 [WRITE pool](write-rndv-pool.md) 独立持有实际 WR 的 endpoint/segment pin。目录存在、握手完成、本地 import 成功或控制断链，都不能替代逐事务 Grant 或远端访问终结证明。

## 验证与后续

无硬件测试覆盖规范编码、双方及单方协商、READY 回显、身份/范围拒绝、真实 Mock Slab 导出、缓存失效、fd 所有权及零分配路径，具体结果见 [roadmap](roadmap.md)。UB 真机未执行。

下一阶段接入 Connection/Runtime 的通知与 WRITE 事务调度，以及共同 Eager/RNDV copy-read、Lease 归还和关闭流程；在这些路径完成前，应用流继续默认使用 Eager。
