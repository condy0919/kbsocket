# 应用 Buffer Lease API

本文记录 [roadmap](roadmap.md) 的 M5.4.6：通过有界 C 句柄分配、访问及归还应用缓冲区，并复用已有 Slab 的 CPU/DMA/Grant 责任。公共定义在 [`buffer_lease.h`](../kbsocket/api/buffer_lease.h)，C++ 控制面在 [`BufferBinding`](../kbsocket/api/buffer_binding.hpp)。

本阶段交付缓冲区生命周期接口；`write_lease` / `read_lease`、传输完成通知和 M6 WRITE Grant/RNDV 尚未接入。取得缓冲区或 CPU 视图不会提交 SEND、READ、WRITE，也不代表远端拥有访问权。

## 创建与资源来源

所有入口由唯一 owner 串行调用，不得重入。跨线程调用须先转交 owner；C 表指针仅在其所属绑定或 Runtime 对象存活期间有效。任意失效指针不属于句柄校验能够保护的范围。

| 入口 | 内存及生命周期 |
| --- | --- |
| NativeTCP `kbs_runtime_open_buffers` / `kbs_runtime_buffers` | 可选打开并借用 Runtime 拥有的 CPU-only 池；不改变既有 Runtime 配置布局 |
| `BufferBinding::OpenHost` | 独立拥有 CPU-only Slab 与 backing 预算；控制路径分配、页对齐并预触页 |
| `BufferBinding::OpenSlab` | 借用宿主已打开的 Slab；宿主保持 Slab、注册 context/provider 和预算存活 |
| `BufferBinding::OpenRaw` / `RawStreamRuntime::OpenBuffers` | 复用 RawStack 的共享注册 Slab；每次操作重新检查全部 context 的暂停门禁 |

NativeTCP 池使用 `kbs_buffer_pool_config` 指定 ABI 版本、唯一 `table_id`、容量、每槽字节数及 backing 预算，保留字段必须为零。backing 预算按系统页大小向上取整，不包含发布表、Slab 元数据或应用其他内存。配置容量固定，数据路径不会临时扩容或注册内存。

CPU-only Slab 不具备 URMA DMA/远端 Grant 能力，不能将其当成已注册源。raw 池的注册属性和多 context 句柄继续遵守[内存 Lease 契约](memory-leases.md)。不能用 `OpenSlab` 包装缓存下来的 RawStack Slab 指针来绕过暂停门禁；这种来源必须使用 `OpenRaw`。

`BufferBindingConfig::capacity` 限制公开 CPU Lease 数；共享 Slab 还可能被连接 staging 占用，所以公开表未满时底层 Slab 仍可能耗尽。两种耗尽均显式返回错误，不通过额外分配突破预算。

## 分配、视图和归还

| C 入口 | 行为 |
| --- | --- |
| `kbs_buffer_acquire` | 借出非零且不超过单槽大小的区间，建立唯一 CPU 所有权 |
| `kbs_buffer_writable` | 校验当前身份和责任后返回可写视图，长度等于本次 Acquire 长度 |
| `kbs_buffer_readable` | 校验当前身份和责任后返回只读视图；停止新准入后仍可完成合法消费 |
| `kbs_buffer_release` | 归还 CPU 所有权；成功后全部句柄和视图副本立即失效 |

返回值和 `kbs_stream_error` 沿用 [stream API](stream-api.md) 的稳定错误分类；成功返回零，失败不依赖 `errno`。失败的 Acquire/视图输出清零，避免调用方误用上次成功结果。Release 失败时保留句柄供 owner 重试，不能丢弃尚未归还的责任。

句柄包含表身份、完整 64 位 generation、发布槽号和保留字段。表身份由宿主保证跨表和生命周期不复用；发布槽通过独立元数据映射到真实 Slab Lease，不把内部 staging Lease 直接暴露为应用句柄。旧 generation、已归还、跨表和保留字段非零的句柄均被拒绝，槽位复用不会恢复旧句柄的有效性。

复制句柄、复制视图或多次获取视图均不增加 CPU 引用。调用方必须将同一份所有权至多成功归还一次。接口不主动擦除复用槽的旧内容；应用应初始化本次有效范围后再将数据交给传输。

视图有效期受 CPU 所有权和 Slab 责任共同限制。预留 DMA/Grant、隔离或归还 CPU 后，必须停止使用此前保存的指针；之后需要访问时重新申请视图。已有 DMA/Grant 未终结或已隔离时，新只读和可写视图均被拒绝。API 无法撤销调用方已经保存的裸指针。

## 宿主传输接线与终结证据

`BufferBinding::Resolve` 供宿主后续传输接线取得真实 Lease，仍验证公开句柄、CPU 可访问状态及 raw 暂停门禁，停止新准入后拒绝调用。它不会增加引用或自动提交操作；宿主不得另行 `ReleaseCpu` 或把同一 CPU 所有权重复发布到其他表。

注册 Slab 的传输仍须遵循 `Reserve -> Commit -> CompleteDma/EndGrant`：可信未接受的责任可取消，已接受或接受范围未知的责任须保持到各自真实终结。C API 没有伪造 DMA 完成、远端授权结束或强制回收的入口。

`kbs_buffer_release` 成功只说明应用已归还 CPU 所有权。若仍有本地 DMA 或远端 Grant，底层槽及 MR/backing 继续保留，直到全部责任结清才可复用。应用错误、超时、断链和 CPU 归还均不能替代这些证据。

raw 的共享 Slab 可被多个 context 的控制任务访问，因此任意一个 context 暂停时，本表操作均须等待门禁恢复；这也包括归还。门禁拒绝不能部分修改公开表或 Slab 账本。

已经取得的 raw 视图也不能用于绕过暂停门禁；宿主交出控制任务所有权时必须停止相应共享资源的外部访问，恢复后再取得合法视图。

## 停止与关闭

`BufferBinding::StopAccepting` 停止本表的新分配和可写借用，不停止借用 Slab 的其他使用者。正常只读消费和 CPU 归还仍可继续，但仍受身份、暂停及 DMA/Grant 门禁限制。

Close 停止新准入；存在尚未归还的公开 CPU Lease 时返回 BUSY，保留表、句柄与 backing 供重试，不代替应用归还。绑定自行拥有的 CPU-only Slab 由它关闭；借用的注册 Slab 由宿主按独立生命周期关闭，不能因公开表已空就推断所有传输责任结束。

NativeTCP Runtime Close 将应用缓冲区与已发布流、监听器一起纳入关闭门禁，所有 CPU Lease 归还后才能完成 Close/Destroy。raw Runtime 在 RawStack 物理关闭前关闭应用缓冲区表；共享 Slab 的独立 CPU/DMA/Grant 账本继续阻止资源提前拆除。关闭失败后保留所需对象并继续收敛，不先销毁唯一 owner 或底层依赖。

## 验证范围

无硬件测试覆盖 C ABI 布局/符号、CPU-only 池、注册 Slab 借用、容量及预算、旧身份和槽位复用、视图与 DMA/Grant 责任、停止/关闭重试、raw 暂停门禁及数据路径零分配。具体测试目标和用例数量记录在 [roadmap](roadmap.md)。这些验证不提供新的 UB 真机、零拷贝传输或 40k 性能结论。
