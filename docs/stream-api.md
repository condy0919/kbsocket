# M5.4.1–2：copy-stream C 接口与连接事件

[`stream.h`](../kbsocket/api/stream.h) 提供 Eager 和 NativeTCP 共用的有界复制读写、半关闭、取消、状态查询及关闭/释放接口。头文件可由 C11 独立编译，不暴露 URMA、C++ 类型或底层 fd。`KBS_STREAM_ABI_VERSION=1` 标识本次结构与函数契约；M5.4.2 追加统一连接事件及一次性 readiness，保持已有结构布局不变。M5.4.3 的 [stream_runtime.h](stream-runtime.md) 已提供独立 NativeTCP Runtime 创建、拨号/接收及自动事件转发；M5.4.4 的 [RawStreamRuntime](raw-stream-runtime.md) 提供 raw C++ 应用编排，自动完成本地资源接线和事件转发；M5.4.6 的 [Buffer Lease API](buffer-api.md) 已提供缓冲区生命周期，零拷贝传输接口继续后续。

## 绑定已有运行时

C++ 控制面使用 [`StreamBinding`](../kbsocket/api/stream_binding.hpp)：

- `OpenTcp(owner, config)` 绑定无 raw context 的 ConnectionOwner，允许完全无设备的 NativeTCP 服务。
- `OpenRaw(stack, context, config)` 绑定 RawStack 的指定 context。每次访问连接、发布、退休来源或关闭表前均重新经过 RawStack 的资源门禁，context 被控制任务暂停时返回 `BUSY`。
- 宿主完成 fd 接管及协议准备，用 `Publish(OwnedConnection)` 发布 C 句柄。Negotiating/Preparing 阶段可查询或取消，应用 I/O 直到 Active 才允许；失败或已关闭的连接也可发布以完成诊断/清理。

一个绑定对应一个 owner。Open 预分配发布槽、空闲索引和按 owner 槽索引的反向表，Publish/查找/Release 均为 O(1)，没有全局连接表、锁或隐式扩容。`capacity` 不得大于 owner 容量；反向表按 owner 和 scheduler 容量分配；事件节点内置于发布槽，不另建无界队列。表只支持一次生命周期。创建失败不接管 owner 或连接，配置可以纠正后重试。

`table_id` 必须由宿主保证进程内跨表、跨生命周期不复用。C 句柄同时校验 table_id、完整 64 位发布 generation、slot 和保留字段，generation 耗尽拒绝发布。底层 OwnedConnection 还会检查其 owner 与完整代次；fd 数值、对象地址或单个槽位都不构成身份。

同一逻辑连接只能交给一个发布表。发布后 I/O、关闭和 Release 统一经过该表；宿主仍可执行协议 Prepare，但不能绕过表另行 Release 或把同一连接再次发布到其他表。绑定借用 owner/RawStack，必须在其物理关闭前关闭本表。析构不代办连接关闭或释放 DMA；应用停止访问后才可销毁绑定。已取得的 C 表指针在绑定对象存活期间有效，表 Close 后返回 `NOT_READY`，不得跨对象析构保存。

## 调用与数据语义

所有应用入口由唯一 owner 串行调用，不能在 Runtime 回调中重入，也不接受跨线程直接访问。唯独宿主回调可同步调用 `RecordProgress` 复制原事件快照，不能在其中进行应用 I/O 或查询 owner。业务线程继续通过已有 [ConnectionMailbox](connection-runtime.md) 向 owner 交接。Runtime 进度仍由宿主 Tick/RunOnce；调用 C I/O 本身不轮询 CQ，不等待连接就绪。

| 接口 | 成功含义与边界 |
| --- | --- |
| `kbs_stream_write` | 返回已复制到库缓冲或 TCP 内核的前缀；源内存可立即复用，不代表已提交 WR、远端已收到或消费 |
| `kbs_stream_read` | 返回按连接顺序连续的字节；可短读，`eof` 与零进展 `WOULD_BLOCK` 分开 |
| `kbs_stream_shutdown_write` | 在已接受数据之后发 FIN，读方向继续使用；仍须 Runtime 推进，不提供同步 drain |
| `kbs_stream_cancel` | 显式取消连接，保留已有 DMA/来源责任与句柄，之后仍须关闭和释放 |
| `kbs_stream_inspect` | 查询稳定状态、已锁存错误、Eager 排队及在途数量、RX 驻留数、来源保留数；这些计数不替代终结证明 |
| `kbs_stream_watch` | Eager/NativeTCP 整体一次性读写提示订阅，任一方向满足即消耗读写两方向 |
| `kbs_stream_poll_events` | 非阻塞取出最多 64 条合并事件，不推进 Runtime 或重新 arm |
| `kbs_stream_watch_tcp` | 保留 NativeTCP 专用入口，使用相同事件交付；raw 明确拒绝 |
| `kbs_stream_close` | 有界关闭逻辑连接；可丢弃未提交/未读数据，不承诺优雅发送完毕 |
| `kbs_stream_release` | 连接 Closed 且来源退休后解除调度并释放槽；成功立即使旧 C 句柄失效 |

`max_io_bytes` 在 Open 配置，范围 1 B–1 MiB，默认 64 KiB。超过上限的输入/输出只处理一次有界前缀，其余由应用后续调用。下层 Slab、序列窗口、Credit 和 TCP 预算可能进一步短读写。成功字节前缀不会被后续错误覆盖；后续调用或 Inspect 继续报告已锁存错误，禁止自动重放。

每个函数返回 0 或固定正错误码，不读写 `errno` 作为 API 错误通道。I/O/Inspect 的 out 必填，失败时清零；error 可空，每次调用完整覆盖。输入、输出及诊断结构须为有效且互不重叠的内存。零长度允许 data=nullptr；空 Read 仍校验连接状态，但不探测 EOF，成功返回 bytes=0/eof=0。正长度 Read 在远端 FIN 后才报告 EOF。read 的 now_ns 由 owner 提供，须与 Runtime 使用同源单调时钟。

## 连接事件与接线

宿主在创建 `ConnectionProgress`（TCP）或 `RawEndpointConfig.on_connection/cookie`（RawStack）时安装转发回调，在回调内同步调用对应表的 `RecordProgress(event)`。转发只复制快照，不访问 ConnectionOwner、域资源或 provider，也不执行用户回调；禁止存储旧快照后延迟或乱序回放。返回 `false` 表示未发布或不属于本表的完整 scheduled identity，错误单独返回。表尚未 Open 或已 Close 时宿主停止转发；对象析构前必须停用转发并保证回调不再持有其地址。关闭表本身不替宿主注销 Runtime 回调。

应用主循环先推进 Runtime，再调用 `kbs_stream_poll_events` 处理已发布句柄的提示，随后执行有界 I/O，并按需重新 `kbs_stream_watch`。本次不增加应用事件 fd、独立线程或隐式 Runtime 驱动；如果事件仍在本地队列中，应先继续取完，不能直接进入阻塞等待。

- Publish 自动排初始 STATE 快照；状态/错误、RX 驻留变化、真实 TX 退休自动报告，不依赖 Watch。
- Watch 是整体一次性订阅。读或写任一方向满足后，两方向一起解除；TCP 原生一次 fd 事件同样整体消费订阅。Poll 不重新订阅，`Watch(0,0)` 取消将来的读写提示，但不撤回已排队的历史提示。
- Eager Watch 立即检查已有连续字节、连续 FIN 或已确认 EOF，乱序驻留只可能报告 RX_CHANGED。可写按本地 Slab、记录槽及字节窗口判断；远端 RQE credit 为零仍可先复制到本地缓冲。Runtime 有界巡检也检测共享 Slab 被其他连接归还后的恢复。
- TCP IN/OUT 来自实际 fd，RDHUP/HUP 只报告 PEER_SHUTDOWN 并提示读取尾部；实际 Read 确认后才置 `read_eof`。已确认 EOF 的再次读订阅可立即满足。`write_closed` 表示本地写方向关闭，不证明远端消费。ERROR 可仅来自 fd ERR；此时 `failure.code=0`，应用应进行 I/O/Inspect 获取确定错误，不能猜原生错误码。
- 每个已发布槽最多一个 pending 节点，按首次入队顺序公平取出；重复事件原地合并 flags、更新快照、累加 TX 退休数，计数溢出饱和并置 COUNTER_SATURATED。flags 是历史提示，state/failure/RX/半关闭字段是最新快照，既不是逐次状态历史，也不是每个 WR 的完成列表。应用已耗尽数据后，队列里仍可有 READABLE；下次 I/O 仍可能 WOULD_BLOCK。
- Read/Write/Shutdown/Cancel/Close 直接调用后刷新快照，成功字节前缀不因刷新而被覆盖。Release 仅在底层释放成功后 O(1) 移除 pending 与映射；RETAINED 保留事件责任。完整 scheduler owner/id 与发布 generation 防止迟到回调命中新槽。
- context 暂停期间 Watch/Poll 返回 BUSY 且不消费事件；错误调用 RecordProgress 也会被暂停标志拒绝，不读取暂停域。与其他 owner 的事件分开处理，不跨线程读取表。

C Poll 的 `events`、`count` 必填，capacity 必须大于 0；一次最多返回 `KBS_STREAM_EVENT_BATCH_MAX=64`，空队列成功返回 count=0。失败只清零 count，不承诺覆盖 events 数组；已取出的事件 token 仍须通过完整句柄校验才能访问连接。

## 错误与关闭

外部错误类别固定，内部枚举只保存在 `origin/native_code/detail/underlying` 供本版本诊断。`WOULD_BLOCK` 表示本次零进展；`BUSY` 包含 context 暂停或关闭条件未齐；`RETAINED` 单独表示尚有不能释放的责任，不能当成成功或普通 EAGAIN。provider status 9 等原始完成证据保留，错误映射不会触发重放、恢复 Lane 或跨后端切换。

当前 ConnectionError 已将部分握手通道子类扁平化，不能可靠区分其 timeout/EOF 的来源；这些错误保守映射 `PROTOCOL`，不猜测为确定的 timeout。明确的 Lane 更新超时、TCP 超时、取消及半关闭会映射到对应类别。诊断字段不是跨版本稳定的内部枚举承诺。

关闭顺序为：

1. 停止发布/业务访问；需要优雅发送时先 shutdown_write 并持续推进，close 不承担此保证。
2. `kbs_stream_close` 返回 `RETAINED` 时保留句柄和底层调度，继续分发可信 TX 完成后重试。成功 Close 保持句柄有效，可 Inspect、重复 Close。
3. raw 由宿主以真实独立 `RxSourceRetireProof` 调用 `StreamBinding::RetireSources`；C API 不提供伪造“已安全”的布尔开关。Peer 共账撤回、事件屏障、MR/DMA 生命周期仍各自负责。
4. `kbs_stream_release` 成功后才回收槽；迟到/重复调用被完整身份拒绝。
5. 全部 token 释放、应用退出后 `StreamBinding::Close`，再按 RawStack 或 TCP owner 的原顺序关闭底层资源。表 Close 遇到已发布句柄只停止新发布并返回 BUSY，不吞掉任何连接责任。

## 验证边界

验证使用纯 C 编译/C++ 链接、本地 IPv4 TCP 以及 Mock RawStack。重点覆盖有界前缀、错误与 EOF、旧句柄、关闭保留、来源退休、context 暂停、事件合并/一次性订阅以及无分配 I/O 和事件交付。实际回归数量记录于 [roadmap](roadmap.md)。UB 真机与 40k 性能验收未执行；NativeTCP 独立 C Runtime 已由 M5.4.3 交付，raw C++ 应用入口已由 M5.4.4 交付，raw 独立 C Runtime ABI、跨线程同步 API 和零拷贝传输 ABI 仍未交付。
