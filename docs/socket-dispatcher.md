# M7.2–M7.3.1：Socket 请求调度与 FD 事件桥

[`SocketDispatcher`](../kbsocket/socket/socket_dispatcher.hpp) 接通 FD 身份、跨线程命令与现有 C stream。应用线程只查询绑定、复制提交请求及领取完成；唯一 owner 执行 stream I/O、逻辑关闭、真实资源退休，并可选择启用 Watch/PollEvents 事件桥。当前提供提交/完成及 owner 事件领取接口，尚未实现 POSIX `read/write/close` 替换层或 EpollMux，不能把提交成功解释为实际写入成功。

## 装配与所有权

一个 dispatcher 绑定一个不可变 `owner_id` 和借用的 `SocketStreamBackend`。`CStreamBackend` 将请求转发到既有 `kbs_stream_*`，不创建 Runtime，也不推进网络。宿主先创建 Runtime/stream 表和 backend，再 Open dispatcher；backend、表、实际 owner 及 provider 依赖必须活到 dispatcher Close 成功且外部调用者退出。

`Attach(stream)` 在 owner 控制路径验证完整 stream 句柄，预留独立 `EFD_NONBLOCK | EFD_CLOEXEC` 内核 eventfd，并发布新的 `FdBindingId`。Attach 成功后，绑定 stream 的 I/O、Watch、Close 和 Release 交给 dispatcher，宿主不能绕过它操作相同流；Attach 失败只清理本次公开 FD 预留，stream 继续归宿主管理。Runtime 已接管的 TCP 传输 socket 不被复制，也不会当作公开 fd 再次关闭。

公开 fd **仅预留整数身份**。应用不得直接读写、关闭、dup 或对它期待 socket/epoll readiness；使用完整绑定身份调用本组件。dispatcher 的 `wait_fds()` 返回另一个命令通知 eventfd，供 owner 反应堆等待，不能把这两个描述符混用。后续 SocketAdapter/EpollMux 会提供真正的兼容入口。

Open 时预分配全部目录、绑定节点、读者槽、命令槽/载荷和 stream 记录。记录按 `stream.slot` 直接定位，`stream_capacity` 应覆盖底层 stream 表的槽位范围；每槽同时只容纳一代绑定，旧元数据未退休时不能 Attach 已复用的同一 stream 槽。FD 表预算、按实际二次幂容量及 64 B 步长计算的命令载荷预算、包含 dispatcher State 和记录的 `record_budget_bytes` 分开约束；这些预算不等于实际 Runtime/MR 或 RSS 总量。队列固定元数据另由容量上限约束。

## 请求与完成

应用线程的调用顺序为：

1. 注册线程独占的 `FdReader`，用整数 fd 或完整 `FdBindingId` 查询得到 `FdLease`。异步事件必须使用完整身份。
2. `TrySubmit(lease, command, input)`：成功才移走 Lease；失败保留原 Lease。Write 在返回成功前复制输入，Read 只提交长度。零长度 I/O 合法，单次长度不能超过配置上限。
3. owner 调用 `Progress(kMailbox, budget, now_ns)`。每条命令只尝试一次 Write/Read/ShutdownWrite/Inspect/Close/Watch，保留实际短操作、EOF、WOULD_BLOCK 和完整 `kbs_stream_error`。不自动补发、不重放。Read 使用不早于当前单调时钟的时间，避免同轮 Runtime 已推进后时间倒退。
4. `TakeCompletion(ticket, output)` 领取结果。Read 输出不足时保留完整结果；未完成返回 Pending。成功领取后释放命令槽和绑定 Lease，一个票据至多成功领取一次。原始输入在入队复制后即可复用；实际接受字节数以完成为准。

[`FdCommandQueue`](../kbsocket/socket/fd_command_queue.hpp) 使用预分配 MPSC 环、完整 `queue_id + sequence` 票据及有界 CAS 重试，序号耗尽拒绝回绕。已提交、正在执行和完成未领取共同占槽；满额或竞争过多明确返回错误。生产者发布后通知 owner；连续前缀未处理完保留 readiness，遇到尚未发布的预留槽则由对应生产者再次唤醒。正常提交、执行、领取路径不分配，不持有全局锁。

完整 FD/owner/stream 身份随请求固定，owner 不重新按裸 fd 选择目标。逻辑关闭之前执行的旧请求可以完成；关闭之后才执行或提交的旧 Lease 请求返回 CLOSED。它们不会访问已释放的流，也不会命中复用后的新 fd 或新 stream 代次。

## M7.3.1：订阅与事件交付

`SocketDispatcherConfig.events` 默认关闭，保持已有只提供 I/O 的 backend 行为。开启时要求 backend 支持 Watch/PollEvents，否则 Open 返回 NOT_SUPPORTED；`CStreamBackend` 转发真实 `kbs_stream_watch` 和 `kbs_stream_poll_events`。关闭事件模式时 Watch 命令完成为 NOT_SUPPORTED，dispatcher 不消费后端事件。

开启后，dispatcher **独占后端整张 stream 表的事件消费权**，宿主及其他 dispatcher 不得再直接 Poll 同一张表。尚未 Attach、已经 Detach 或完整 stream 身份不匹配的事件计入 `unmapped_events` 并丢弃；需要交付事件的流应先 Attach，不能依赖 Attach 补发已消费的初始 STATE，应以 Inspect/Watch 建立应用状态。底层 Runtime 仍由宿主显式推进，不由事件桥自动 Step。

应用用 `FdCommandKind::kWatch` 提交 readable/writable；两个标志都为 false 表示取消未来就绪订阅。Watch 不允许携带 I/O 长度或载荷，其完成只确认后端调用结果。底层是**整体一次性订阅**：一个方向就绪可消耗本次读写订阅，继续关注须再次显式 Watch。取消或重新订阅不撤回已经排队的同流提示，包括后端已收割、尚未转换为 C stream 事件的原生 fd 提示。命令 `token` 只关联命令完成，不是事件订阅代次，事件桥不把旧提示标成“新 Watch 触发”。

owner 的调用顺序为：

1. 执行 mailbox 并推进底层 Runtime，再用 `CollectNotifications(budget)` 收集 C stream 事件；每次最多 64 条。
2. 调用 `PollEvents(span<FdEvent>)` 领取本地提示，每次最多 64 条。该操作不推进 Runtime、不调用后端 Poll、不重新 arm；事件数据是副本。
3. 按完整 `event.binding.id` 分发并 Lookup，再提交实际 I/O 或新的 Watch。事件复制后可能立即发生关闭，不能只凭整数 fd 重新路由。

每个绑定记录内嵌一个预分配事件节点，空间计入 `record_budget_bytes`；事件合并、排队和领取不动态分配。首次事件按 FIFO 入队，重复事件保留队列位置，避免活跃流反复抢占队首。`flags` 合并待取期间的历史提示，state/failure/rx_pending/read_eof/write_closed 使用最新快照；`tx_retired` 继续累加，溢出饱和并设置 COUNTER_SATURATED。不会根据 `rx_pending` 合成可读，也不会把 PEER_SHUTDOWN 当作 EOF，或给 `ERROR && failure.code == 0` 编造错误码。READABLE/WRITABLE 仍只是提示，不保证下一次 I/O 有进展。

`CollectNotifications` 在满批收集或本地仍有待交付事件时报告 runnable。宿主必须在每轮领取、分发这些事件，否则同一 reactor 会持续返回而不进入睡眠。独立调用 NativeTCP 的 `kbs_stream_runtime_step(wait=true)` 时，Runtime 看不到已转移到 dispatcher 的待取事件：宿主须先 PollEvents，或以 `events_pending()` 阻止阻塞等待。事件桥没有新建应用通知 fd，公开 FD 仍只保留身份；消费者线程等待和 EpollMux 的 LT/ET/ONESHOT 语义留待 M7.3.2。后端收集失败保留已有本地事件，`stats().event_error` 保存完整错误，并在下一次成功收集时清零；这些统计不构成连接或 DMA 终结证据。

Detach 成功解绑后立即 O(1) 撤销本地待取事件。已经交付的副本不会被追收，后续 Lookup 会拒绝旧身份；后端迟到事件也因退休阶段或 stream 表 ID/slot/generation 不匹配而被过滤。真实 Close/Release 及 CPU 引用退休条件保持如下约束。

## 关闭与退休

Close 命令或 owner 的 `Detach(binding)` 先从 FD 表解绑，再关闭公开预留 fd 一次；即使 Linux close 报错也不重试同一整数，因为它可能已被其他线程复用。Close 命令完成只表示逻辑关闭，传输资源仍按以下阶段收敛：

| 阶段 | owner 动作 | 后续条件 |
| :--- | :--- | :--- |
| `kClose` | 调用真实 stream Close | 成功后进入 Release；失败保留记录及原始错误 |
| `kRelease` | 调用真实 stream Release | 只有成功才确认 FD 节点关闭；raw 来源退休仍需宿主独立证明 |
| `kConfirmed` | 有界扫描 FD 退休队列 | Hazard 消失且最后 CPU Lease 归还后回收记录 |

`RetireStreams(budget)` 在显式 owner 控制路径推进固定退休队列，`Progress(kRetire, budget, ...)` 只做预分配元数据收集，两者每次最多 64 项。既有 stream Close/Release 可能释放连接堆对象，不能从 reactor 的热 Progress 调用；宿主应在 owner 控制阶段定期调用 RetireStreams，不能仅运行数据面 Step 就等待真实流自动释放。预算不足会保留 runnable，完成当前一轮检查；持有引用的队首不能挡住后面的可回收节点。一次检查后仍为 Retained 的流等待后续实际完成或证明，不持续忙转，也不阻塞独立健康流。`InspectBinding` 提供阶段、最后 stream 错误及公开 fd 的 close 错误；`stats()` 是 owner 诊断快照，不能替代终结证明。

完成领取会再次唤醒 owner 进行退休。外部未入队的 Lease 直接 Reset 不发送通知，因此宿主必须保证有界周期的退休推进；现有 `ShardReactor.max_wait_ms` 的有限等待可满足这一要求，不能仅永久等待命令 fd。raw dispatcher 必须以 backend 的真实 context 身份加入同一暂停组；控制任务期间不得在外部单独推进。当前没有自动装配进 RawStreamRuntime，也未替宿主生成来源退休或远端静默证明。

最终关闭顺序为：停止准入；等待应用提交者/领取者退出并领取全部结果；Detach 所有绑定；持续推进 Runtime，在 owner 控制路径调用 RetireStreams，在热路径收集元数据；归还全部 Reader/Lease；最后 `Close(true)`。有绑定、退休节点、命令结果或 Reader 时返回 Retained。成功之后才允许关闭/销毁 backend、表和 Runtime。未完成关闭就析构会保留整个 State，不能据此销毁借用依赖；没有强制删除或自动恢复业务流的路径。

## 验证

- [队列测试](../kbsocket/socket/fd_command_queue_test.cpp)：输入快照、输出不足、满额、票据防复用、Lease 保留、逐分配点失败回滚、零分配、多个生产者及竞争领取。
- [调度测试](../kbsocket/socket/socket_dispatcher_test.cpp)：真实公开 fd 所有权、单次短 I/O、错误诊断、关闭/Release 独立门禁、旧请求与 fd/stream 槽复用、预算为 1 的退休公平性、Retained 无忙转、热 Progress 不执行真实资源释放以及 owner 线程归属。
- [NativeTCP 测试](../kbsocket/socket/socket_dispatcher_tcp_test.cpp)：真实 IPv4 loopback 和 C Runtime，验证复制提交、短写不补发、Read/EOF/半关闭、迟到旧命令、公开 fd 与传输 socket 分离及 Runtime Close/Destroy 门禁。
- [事件桥测试](../kbsocket/socket/socket_dispatcher_events_test.cpp)：Watch 命令、事件合并/饱和、完整身份过滤、Detach 撤销、FIFO 预算、错误保留与热路径零分配。
- [NativeTCP 事件测试](../kbsocket/socket/socket_dispatcher_events_tcp_test.cpp)：真实 C Runtime 的一次性可读/可写通知、已有数据时重新订阅、取消保留历史提示、半关闭和 EOF 分离，以及关闭/复用事件身份。

本轮不覆盖 UB 真机、raw dispatcher 自动装配、内核 epoll 兼容、40k 常驻连接或建链性能。测试计数与下一步见 [路线图](roadmap.md)。
