# 预注册 Slab 与 Lease 生命周期

本文记录 [roadmap](roadmap.md) 的 M4.1 实现：固定容量、多 context 注册的内存池，独立 CPU/DMA/Grant 责任，跨线程归还和隔离账本。接口位于 `kbsocket::memory`，供宿主和后续协议层组合；它们不直接提交 WR，也不推断硬件或远端已经停止访问内存。

## 组件与容量

| 组件 | 当前责任与限制 |
| --- | --- |
| [`SlabAllocator`](../kbsocket/memory/slab_allocator.hpp) | 页对齐 backing 切成等长槽位，显式 CPU-only 池，或在 1–4 个不同 context 中分别注册；一个 owner 串行推进全部 Lease 操作 |
| `MemoryBudget` | 多个 Slab 共享 backing 字节硬预算；同一 backing 注册到多个 context 只计一次 |
| [`BufferLease`](../kbsocket/memory/buffer_lease.hpp) | 一次槽位借用及其唯一 CPU 所有权；复制描述符不增加引用 |
| `BufferReference` | 每份本地 DMA 或远端 Grant 责任的独立票据；同一个 Lease 可以持有多份 |
| [`QuarantineLedger`](../kbsocket/memory/quarantine_ledger.hpp) | 每个 Slab 槽位预留一条隔离记录，保留已有责任并阻止新使用 |
| [`LeaseReturnQueue`](../kbsocket/memory/lease_return_queue.hpp) | 有界 MPSC 归还队列，只有 owner 消费；使用自己的 nonblocking eventfd 通知 |
| [`LeaseReturnProgress`](../kbsocket/memory/lease_return_progress.hpp) | 最多绑定 16 个 Slab，将归还队列接入 `RuntimeShard` 的可选 hooks source |

`SlabConfig` 默认 128 个槽位、每槽 4096 字节、512 份责任票据。槽位数和票据数各自不得超过 `1 << 20`，实际使用受各自配置限制。没有空槽或票据时返回 `kExhausted`，数据路径不会扩容。每个槽位可以借出小于槽位大小的有效区间，实际长度随本次 Lease 记录。

backing 的预算量为 `buffer_count * buffer_size` 向系统页大小取整后的字节数；它从分配前预留，直到所有 MR 成功注销并释放 backing 才归还。因此活动、空闲、借出和隔离槽位都已经包含在预算内。该预算不重复计算多 context 注册，也不包含 provider 内部开销、Slab/票据/隔离元数据、归还队列元数据和应用其他内存。元数据由各自固定容量限制，宿主仍须将这些成本纳入总体内存规划。

`Open` 在控制路径分配、预触页并注册，不自动选择 Hugepage、THP 或共享内存。注册权限由 `SlabConfig::access` 选择，当前允许 READ、WRITE、ATOMIC 的非空组合，不接受 `LOCAL_ONLY`；创建远端 Grant 责任要求具备 WRITE 权限。这些权限检查不代表已经实现 Grant 发布协议。

`SlabAllocator::OpenHost` 由 M5.4.6 追加，显式创建 CPU-only 池：沿用 backing 预算、预触页、身份与关闭规则，不调用 MR 注册/注销，拒绝预留任何 DMA/Grant 责任。原 `Open` 仍要求至少一个 context；空 context 不会隐式退化成 CPU 池。应用入口见 [Buffer Lease API](buffer-api.md)。

## 所有权与责任

`Acquire(bytes)` 产生一个 CPU Lease；`Copy(bytes)` 先借出一个可容纳的槽位并复制数据，成功返回时这部分数据已脱离调用方存储。两者只接受非零且不超过槽位大小的长度。

Lease 身份包含 allocator 地址、槽号和单调 generation，责任票据包含 allocator 地址、票据槽号和单调 sequence。计数在同一 allocator 的 `Close`/`Open` 之间不重置，耗尽后拒绝新借用，不回绕。allocator 必须活到所有 Lease、责任和排队归还结束；对象销毁后不能将旧 token 用于相同地址上的新对象。

| 责任 | 建立 | 结束 | 对其他责任的影响 |
| --- | --- | --- | --- |
| CPU | `Acquire` / `Copy` | owner 调用 `ReleaseCpu`，或消费成功入队的归还 token 后调用它 | 不结束 DMA 或 Grant |
| 本地 DMA | `Reserve(lease, kLocalDma)` | 未暴露时 `Cancel`；已提交或可能暴露时，取得可信逐 WR 终结证据后 `CompleteDma` | 不结束 CPU 或 Grant |
| 远端 Grant | `Reserve(lease, kRemoteGrant)` | 未暴露时 `Cancel`；已暴露后，确认禁止后续远端提交且全部访问结束，再调用 `EndGrant` | 不结束 CPU 或本地 DMA |

只有 CPU、全部本地 DMA 和全部远端 Grant 都结清，槽位才回到空闲表。`Reserve` 即开始占用责任票据和计数，不能等 post 成功以后再补记。接口校验票据类型、身份和状态，重复终结、用 `EndGrant` 结束 DMA 等操作均被拒绝。

`Writable` / `Readable` 仅在仍持有 CPU 所有权、没有任何 DMA/Grant 责任且未隔离时返回视图。停止准入后不能取得新的 writable 视图；已有 CPU Lease 在满足上述条件时仍可取得 readonly 视图以完成消费。

**`Reserve`、`Quarantine` 或 CPU 所有权转交后，之前取得的 CPU span 必须停止使用。**需要再次访问时重新调用视图接口检查当前状态。接口无法撤销调用方保存的指针或 span；已返回 SGE 同样不能越过对应责任的终结点。归还队列成功接收 token 后，生产者不得再访问 payload，也不得再次归还其副本。

## post 与 Grant 发布边界

每份责任先处于 Reserved 状态。`Commit(reference)` 表示该责任已经或可能暴露给 provider/远端；提交成功不等于 DMA 已完成。当前接口不主动检查 post 结果，宿主必须将 raw 提交账本中的可信接受证据转成对应操作：

1. 在构造 WR 或发布 Grant 之前调用 `Reserve`。
2. 用 Reserved 票据取得 `Sge(reference, context, offset, bytes)`，并在该责任生命周期内保持描述符有效。
3. post 的可信已接受前缀逐项 `Commit`；明确未接受且从未暴露的后缀逐项 `Cancel`。
4. 接受范围未知时，对所有可能暴露的票据 `Commit` 并隔离相关 Lease，保留完整依赖。不能猜测某条 WR 未接受并 `Cancel`，也不能靠超时制造完成。
5. 按可信逐 WR 终结证据调用 `CompleteDma`；远端授权结束必须由协议层提供独立证明后调用 `EndGrant`。

`Commit` 允许在停止准入或隔离后执行，便于记录已经发生或接受情况未知的暴露；它不会恢复准入。Committed 票据不能再 `Cancel`。已隔离但确定从未暴露的 Reserved 票据仍可取消。

`Sge` 只接受 Reserved 票据，校验具体 context 必须在该 Slab 的注册集合内，使用此 context 的注册句柄；偏移及长度受本次 Lease 的实际长度限制，不能越过有效区间或只按整个 Slab 大小检查。它拒绝零长度、超过 SGE 长度类型范围、停止准入或隔离后的新构造。生成 SGE 本身不提交 WR、不转移所有权，也不保证多条操作之间的顺序。

本地成功或错误 CQE、软件超时、断链、本地 flush 和本地在途计数归零，均不能由此组件自行解释成远端 Grant 结束。`UnregisterSeg` 成功也没有在这里被用作独立远端撤权证明。

## 跨线程归还与 RuntimeShard 接入

生产者只调用 `LeaseReturnQueue::TryPush`，不并发调用 Slab 的状态接口。成功返回才转移唯一 CPU 所有权；满额、停止或参数错误时，调用方继续持有 token，需要安排可靠重试或交回 owner，不能丢弃。队列不解引用 token 中的 allocator 指针，身份验证在 owner 归还时执行。

队列容量向上取至少 2 的二次幂，上限 `1 << 20`。`LeaseReturnProgress::Open` 要求实际容量至少等于所有绑定 Slab 的槽位数之和，且 queue 已打开、为空、仍接受提交，所有 Slab 已打开且仍接受借用。容量保证以每份 CPU 所有权至多提交一次为前提；复制或重复入队不会创造额外的合法所有权。

归还队列预分配槽位，`TryPush`/`Poll` 不分配、不持互斥锁。producer 预留后尚未发布时，owner 不等待这个空洞，由该 producer 完成发布后唤醒。owner 先消费 eventfd 通知，再扫描已发布前缀；预算不足且头部仍有工作时重新保持 fd 可读。`completion_fd()` 只供 readiness 观察，外部不能直接 read、write 或 close；`pending()` 包含预留但未发布的条目，是并发快照，不能证明 producer 已退出。

控制路径按以下顺序接入：

1. 保持 context/provider 和共享 `MemoryBudget` 存活，打开所需 Slab。
2. 按绑定 Slab 的总槽位数打开归还队列。
3. 调用 `LeaseReturnProgress::Open(queue, slabs, callback, cookie)`。
4. 把 progress 作为 `RuntimeShard::Open(..., &progress)` 的 hooks 参数传入，由同一 owner 推进。

progress 只在 `ProgressKind::kMailbox` 类别消费，单次最多处理 `min(budget, 64)` 项，使用反应堆提供的 mailbox 预算，不创建线程。它只向登记过的 Slab 调用 `ReleaseCpu`，未知 allocator、迟到和重复 token 报告归还错误；`released` / `rejected` 分别计数，随后逐项调用 callback。CPU 归还成功不代表该槽位已经没有 DMA/Grant 责任。

progress 借用 queue、Slab 和 callback cookie，不接管它们的生命周期。callback 在 owner 上同步执行，不得阻塞、重入或关闭、销毁 progress、queue、Slab。所有借用对象必须活到 progress 从 reactor 解绑并成功关闭。队列和这些对象的析构保留机制不能替代宿主对线程退出及对象存活的保证。

## 隔离、回滚与关闭

`Quarantine(lease)` 为当前槽位记录隔离状态，重复标记不重复计数。账本在 `Open` 时按全部槽位预留容量，覆盖所有可能借出的 Lease。隔离后不再允许新的视图、SGE 或责任预留，既有责任和 backing 预算继续保留；其他空闲槽位仍可正常准入。没有人工“强制回收”或“假完成”接口：只有各类责任按其原有证据结束，槽位才自动解除隔离并回收。

`quarantined_buffers` / `quarantined_bytes` 反映隔离槽位和对应槽位字节，backing 总预算仍按页取整；隔离记录不会另外复制数据或重复扣 backing 预算。全池没有可用槽位时返回 `kExhausted`，不能用新分配的未注册内存绕过限制。

跨 context 注册期间任何一步失败，`Open` 不发布可用 Lease，逆序注销已经成功的 MR。回滚注销失败时保留尚未注销的句柄、backing、元数据和预算，返回原始注册错误及 cleanup 错误；再次 `Open` 被拒绝，宿主先重试 `Close`。正常关闭也记录已经注销的阶段，重试只处理剩余 MR。

`SlabAllocator::StopAccepting` 阻止新借用和新责任，既有责任仍可终结。`Close` 先停止准入，有未结清槽位时返回 `kInUse`；全部槽位可回收后才逆序注销，任一步失败均保留剩余依赖及完整 backing 预算。只有全部注销成功才释放 backing 并归还预算。

正常关闭由宿主协调以下顺序：

1. 停止新的应用借用、WR/Grant 发布；按协议终结既有 DMA/Grant，保持所需完成推进。Slab 停止准入不等于 producer 已归还 CPU Lease。
2. 让生产者完成归还并确认全部生产者退出，owner 继续推进剩余归还。不要在仍有生产者需要归还时提前关闭队列；`StopAccepting` 之后已经越过准入检查的 push 仍可能成功。
3. 确认必要的归还和控制完成已经收割，再调用 `RuntimeShard::Close()` 解除 hooks/fd 绑定；若仍有已接受控制任务，继续推进并重试。
4. 依次调用 `LeaseReturnProgress::Close(true)`、`LeaseReturnQueue::Close(true)`。`true` 是宿主提供的生产者静默证明；有 pending 条目时关闭被拒绝，必须继续消费后重试。
5. 最后逐个 `SlabAllocator::Close()`，成功后才能释放所借用的 budget、context/provider 等依赖。

关闭失败时保留对象供重试，不先销毁唯一能推进其责任的 owner。无终结证明时继续隔离并保留全部依赖。Slab 异常析构会尝试关闭，失败则保留内部状态；归还队列未显式 `Close(true)` 就析构会保留内部状态和 fd。这些行为避免隐式释放未终结资源，但不接管外部 context、allocator 对象、回调 cookie 或生产者线程。

## 当前范围与验证

M4.1 提供内存责任原语及归还进度源。现有 raw `RxBufferPool` 仍持有自己的 backing 和 `RxBufferLease`，尚未迁移到本组件，也不能把两种 Lease token 混用。M5 的应用 `write`/`writev` 部分接受、分片调度和完整 Socket 接口，以及 M6 的 RTS/GRANT/DONE、远端授权身份校验和 WRITE RNDV 协议均未在这里实现。`Copy` 只完成单个可容纳片段的复制。

相关无硬件测试位于：

- [`slab_allocator_test.cpp`](../kbsocket/memory/slab_allocator_test.cpp)：多 context 注册、预算、回滚/关闭重试、独立责任、隔离、身份及零分配。
- [`lease_return_queue_test.cpp`](../kbsocket/memory/lease_return_queue_test.cpp)：队列容量、多生产者归还、发布/通知、满额与关闭、故障回滚及零分配。
- [`lease_return_progress_test.cpp`](../kbsocket/memory/lease_return_progress_test.cpp)：owner 归还分发、预算、错误回调和反应堆接入。

```bash
bazel test //kbsocket/memory:all --test_output=errors
```

这些测试检查软件身份、计数、通知和清理边界，不提供新的真机 DMA、远端撤权或性能保证。硬件依据继续见[观测记录](hardware-observations.md)与[验证矩阵](hardware-validation-matrix.md)。
