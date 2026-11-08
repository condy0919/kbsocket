# M5.4.3：独立 NativeTCP C Runtime

[`stream_runtime.h`](../kbsocket/api/stream_runtime.h) 将 M5.4.1–2 的复制流与事件接口组成可独立使用的 NativeTCP Runtime。C 应用可创建实例、非阻塞拨号、监听和接收，随后使用同一套 [`stream.h`](../kbsocket/api/stream.h) 读写、事件、关闭和释放接口。此模式使用透明 TCP 字节流，不发送 HELLO，不做协议探测、DNS、自动重拨或跨后端切换。

本次只提供 NativeTCP 的独立 C 生命周期。raw 已有 `RawStack` 和 `StreamBinding` 接线，继续由 C++ 宿主提供认证、Peer 共账、协议准备、来源退休和独立静默证明；不能用 TCP 的资源释放条件代替 raw DMA 终结条件。raw 的独立应用入口继续下一阶段交付。

## 创建和所有权

先调用 `kbs_runtime_config_init` 填默认配置，再设置非零 `table_id`。该身份由宿主保证进程内跨实例、跨生命周期不复用，不能使用对象地址或 fd 代替。创建要求 ABI 版本与结构大小完全匹配；未知字段不做静默忽略。

| 配置 | 范围及默认值 |
| --- | --- |
| stream_capacity | 1–65536，默认 256，统一约束 owner/scheduler/发布表容量 |
| listener_capacity | 0–65536，默认 16；0 可用于纯客户端 |
| max_io_bytes | 1 B–1 MiB，默认 64 KiB |
| events | 0 或 1，默认 1 |
| progress_budget | 1–64，默认 64，沿用 RuntimeShard 每类进度预算 |
| max_wait_ms | 0–1000，默认 10；只限制 Step(wait=1) 的单次等待 |

Create 一次性建立 ConnectionProgress、ConnectionOwner、StreamBinding、RuntimeShard 和固定容量监听槽。事件回调自动同步转发至发布表，不调用应用代码。Create 失败不交出半初始化实例；Destroy 仅允许已成功 Close 的实例。未释放实例的指针不能被丢弃，调用方须保留直到关闭完成。

`kbs_runtime_streams` 返回借用表指针。它在 Runtime 对象存活期间有效：Close 成功后旧表指针返回 NOT_READY，Destroy 后一切借用指针失效。所有入口由同一 owner 串行调用，不提供跨线程锁或后台业务线程。RuntimeShard 的内部控制线程不改变这个约束。

## 建立连接

- `kbs_runtime_connect_tcp` 接收数值 IPv4/IPv6 `sockaddr` 与绝对单调期限。成功返回流句柄，可能处于 TCP_CONNECTING；Runtime Step 自动推进连接，STATE 事件报告 TCP_ACTIVE 或 FAILED。建立中 Read/Write/ShutdownWrite 返回 NOT_READY；Cancel/Close 仍可使用。失败连接不会自动重拨，完整句柄在显式 Release 前保持有效。
- `kbs_runtime_adopt_tcp` 接管已有的、已连接的非阻塞 IPv4/IPv6 TCP fd，不更改调用方 fd 标志。参数是 `int*`：未接管失败保持原值，一旦 owner 接管立即写回 -1。即使后续发布/回滚出现错误，也以这个写回值判断所有权，不能再次关闭已转移的 fd。库内部保留未完成回滚的槽，拒绝继续准入，Close 可重试清理。
- `kbs_runtime_listen_tcp` 建立固定槽监听器，返回带 runtime identity、slot、完整 generation 的句柄。`listener_address` 用 getsockname 规则查询实际地址，支持端口 0。
- `kbs_runtime_accept_tcp` 每次最多取出一个连接，直接作为 NativeTCP 发布。无连接时返回 WOULD_BLOCK；容量不足在 accept4 前返回 EXHAUSTED，排队连接仍留在内核。内部已接受但未转移的 fd 在错误路径关闭。

监听器由应用显式轮询 Accept。当前 Step 不等待监听 fd，不产生监听事件，也不会自动接收；服务端应在循环中有界调用 Accept，并选择适当的 max_wait_ms，避免将过长的流等待引入新连接接收延迟。后续监听事件汇聚仍需单独接入。

建立连接复用 Connection/Owner/Scheduler 原状态机与身份；没有第二套拨号槽或完成队列。原有状态数值不变，TCP_CONNECTING 追加为 9；同步启动失败直接返回错误，异步错误在后续 Step 中成为连接事件。

## 推进、事件和关闭

`kbs_runtime_step(runtime, 0, …)` 执行非阻塞 Tick；wait=1 使用 RuntimeShard RunOnce，并受 events/max_wait_ms 约束。已存在待取应用事件时只执行 Tick，防止再次睡眠。Step 返回本轮 processed、more_work 与 pending_events；事件仍通过 `kbs_stream_poll_events` 取出。处理提示和有界 I/O 后按需重新 Watch，Poll 不自动重新订阅。监听 Accept、创建和连接准备属于控制路径，正常 Step、事件交付及复制 I/O 不分配内存。

`kbs_runtime_now_ns()` 提供与调度器同源的单调时间，用于 connect 的 deadline 和 stream read 的 now_ns。连接期限由后续 Step 检查；停止推进不会有后台线程自行触发应用完成，批次预算和等待间隔也会影响实际报告时刻。

关闭顺序：

1. 业务需要优雅半关闭时先 shutdown_write，持续 Step/Read 处理尾部和 EOF。
2. 对每条流调用 close，再 release；对每个监听器调用 close_listener。逻辑 close 不等于远端应用已消费。
3. Runtime close 停止新准入。若仍有流或监听器，返回 BUSY，并保留已有 I/O、Step、关闭和 Release；清理后可重试。它不会批量取消应用连接或强行回收句柄。
4. Runtime close 成功后调用 destroy，成功把指针写为 NULL。尚未 Closed 的 Destroy 返回 BUSY 且保持指针；NULL 指针值可重复销毁。

有效 listener 关闭后立即失效，即使 Linux close 返回错误也不重试同一个 fd；复用槽取得新 generation。C 输入/输出及诊断对象必须有效且互不重叠。普通 out 在失败时清零；fd 所有权参数与 getsockname 的地址长度遵守各自规则。错误分类沿用 stream ABI，TCP 建链/监听错误保留原生 errno 诊断，不将未知错误解释为确定的重放或释放许可。

## 验证边界

本次使用真实 IPv4 本机 TCP 与 C11/C++ 链接验证，无 UB 真机。覆盖配置、非阻塞建链、状态与错误、透明字节流、FIN/EOF、fd 接管与拒绝、固定容量、旧监听句柄、BUSY 关闭、待取事件禁止等待和正常推进零分配。回归数量见 [roadmap](roadmap.md)。独立 raw C 生命周期、监听事件、SHM、零拷贝、40k 规模和性能验证仍未交付或执行。
