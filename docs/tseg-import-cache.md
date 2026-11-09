# M6.3.1：远端 WRITE 段导入缓存

[`TsegImportCache`](../kbsocket/control/tseg_import_cache.hpp) 为 WRITE RNDV 提供固定容量的远端段缓存。相同完整身份的请求共享一次 `ImportSeg` 和一个导入句柄，每份借用使用独立票据。首次导入和最终 unimport 均通过 `ControlExecutor` 执行，收发 owner 不直接调用慢 provider 接口。

## 身份与描述验证

缓存键包含本地 context 地址及 incarnation、远端 runtime、接收域、Slab ID 与完整代次、远端 EID、授权身份。宿主负责将 Slab 身份绑定到已认证的 Grant Arena；每事务的 Grant 代次不进入共享键。不同 context、授权范围或远端重启代次不能命中同一条目。

请求携带可信、同 ABI 的固定 `urma_seg_t` 值和 token，不接收任意网络字节。当前范围是固定页、非映射的 raw WRITE：拒绝空地址/长度、地址溢出、非法 UASID/token ID、无 WRITE 权限、LOCAL_ONLY、扩展描述、DSVA、user IOVA、保留位及不支持的 token policy。允许 NONE/PLAIN_TEXT；NONE 的 token 必须为零。导入参数固定为 WRITE、NOMAP、NON_CACHEABLE、`addr=0`。

同一键的描述或 token 改变会返回冲突，不能覆盖旧句柄。描述按字段比较，ABI padding 不参与身份。worker 使用独立入参副本，provider 修改输入不会改变缓存中的原始快照。成功返回的 target 必须属于请求 context，且完整段描述匹配，才会发布给等待者。

缓存不是远端代次认证器：旧条目被清理后不会永久保留所有历史身份。宿主必须在 Acquire 前拒绝失效 session/Arena；不能把重新导入旧描述当作重新取得 Grant 授权。

## 有界引用与 DMA 生命周期

Open 一次预分配 entry、lease 与控制任务并发记录；默认上限分别为 64、4096、16，可配置但不会动态扩容。Acquire、Retain、Get、Release、失效处理和操作准备/收割均不分配。容量不足显式返回背压。缓存查找有固定容量上限，规模性能尚未验收。

- Acquire 在 Pending 状态返回等待票据，同身份请求合并；Get 在收割导入成功前返回 Pending。
- Retain 创建新的独立票据，复制票据不增加引用；重复、外来或已复用的旧票据不能释放其他引用。
- 每份可能依赖该段的 WRITE 必须由有效引用覆盖。应用取消、status 8/9、断链和接受未知不自动解除引用；只有未来提交已停止且对应可能已接受 WR 均有可信终结后，宿主才能 Release。
- Invalidate 禁止新借用及 Get，保留在途引用。保存过的裸指针不能绕过失效门禁；每次新的 WRITE 提交前仍须检查缓存状态。
- Ready 且零引用的条目留作暖缓存；EvictUnused 或失效后才调度 unimport。未知接受保留引用，因此不会因缓存失效、超时或 Close 提前销毁。

缓存 pin、源 Slab 的本地 DMA 引用、远端目标 Grant 是三份不同责任。UnimportSeg 只结束本地导入句柄的生命周期，不产生远端写入终结证明，也不允许目标内存复用。Get 成功也不代替每个 WRITE 的 Grant 身份、独占写权及区间检查。

## 控制执行与 context 暂停

`NextOperation → PrepareOperation → TrySubmit → CommitOperation → HandleCompletion` 使用完整条目代次、reservation 和执行器任务身份。Prepare 保留工作参数和并发容量；执行器队列满时可以重试同一 task，或者在确认从未接受后 Discard。任务一旦接受，即使调用者取消，也必须执行和收割结果，再决定清理。

[`TsegImportProgress`](../kbsocket/runtime/tseg_import_progress.hpp) 将这条流程接入现有反应堆。每个 context 的段导入源必须与 `ImportProgress`、post/poll/refill、事件处理及其他 context 使用者放进同一 `ContextProgressGroup`。缓存自身只序列化自己的操作，不能替代两个缓存之间的屏障。暂停保持到 owner 收割完成，不能看到 worker 返回或 fd 可读就恢复访问。

适配器不读取 provider fd，不接管缓存、context 或执行器。提交前 Close 可以撤销准备；已提交时返回 InUse，继续收割后才能解绑。RawDomain/RawStack 的自动装配尚未接入这个新源，宿主不得将它作为相同 context 的另一个独立反应堆槽注册。

## 错误与关闭

导入失败会通知全部等待者，引用归还后释放无句柄条目；不自动重试或迁移。取消发生在导入执行中时，仍接收实际句柄并安排清理。Unimport 失败保留原句柄和条目，下次控制推进可重试。

若 provider 返回同 context 但描述不匹配的句柄，该句柄不可借用，待引用归还后异步 unimport。若返回错误 context 的句柄，原暂停域无法授权销毁它，缓存保留句柄且 Close 返回 InUse，不尝试访问另一个 context。此项属于 provider 契约违规防御路径。

Close 停止准入并使条目失效，等待所有引用归还、控制任务收割和导入句柄清理。Close 失败时析构保留内部存储；宿主仍须保留 context、provider、执行器及相关外部依赖。

## 验证与下一步

无硬件测试使用真实 ControlExecutor 和 Mock ImportSeg/UnimportSeg，覆盖 Single-Flight、完整身份、输入/输出校验、引用与失效竞态、失败清理及数据路径零分配。反应堆测试使用两个 worker，验证段导入、Jetty 导入和队列访问确实共享暂停屏障，而非依赖单 worker 自然串行。

本阶段交付缓存及调度源；后续 [WriteRndvPool](write-rndv-pool.md) 已接通可复用事务槽与缓存 Lease/WriteRndvEngine 责任绑定。RTS/GRANT/DONE 编解码、统一 SN 重排和应用入口仍待接入。未执行 UB 真机及 WRITE RNDV 端到端回归。
