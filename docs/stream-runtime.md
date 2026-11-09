# M5.4.3 / M5.4.5 / M5.4.6：独立 NativeTCP C Runtime 与监听事件

[`stream_runtime.h`](../kbsocket/api/stream_runtime.h) 将 M5.4.1–2 的复制流与事件接口组成可独立使用的 NativeTCP Runtime。C 应用可创建实例、非阻塞拨号、监听和接收，随后使用同一套 [`stream.h`](../kbsocket/api/stream.h) 读写、事件、关闭和释放接口。M5.4.5 增加有界监听事件与一次性订阅，监听和既有连接共用 Runtime 推进及等待。此模式使用透明 TCP 字节流，不发送 HELLO，不做协议探测、DNS、自动重拨或跨后端切换。

这里提供 NativeTCP 的独立 C 生命周期。M5.4.4 已有 raw C++ [RawStreamRuntime](raw-stream-runtime.md)，由宿主提供认证、Peer 共账、协议准备、来源退休和独立静默证明；本阶段的监听事件不改变这些契约，TCP 的资源释放条件也不能代替 raw DMA 终结条件。

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

Create 一次性建立 ConnectionProgress、ConnectionOwner、StreamBinding、监听通知源、RuntimeShard 和固定容量监听槽。流事件自动同步转发至发布表，不调用应用代码；每个监听槽预留一个待取事件节点。Create 失败不交出半初始化实例；Destroy 仅允许已成功 Close 的实例。未释放实例的指针不能被丢弃，调用方须保留直到关闭完成。

`kbs_runtime_streams` 返回借用表指针。它在 Runtime 对象存活期间有效：Close 成功后旧表指针返回 NOT_READY，Destroy 后一切借用指针失效。所有入口由同一 owner 串行调用，不提供跨线程锁或后台业务线程。RuntimeShard 的内部控制线程不改变这个约束。

## 建立连接

- `kbs_runtime_connect_tcp` 接收数值 IPv4/IPv6 `sockaddr` 与绝对单调期限。成功返回流句柄，可能处于 TCP_CONNECTING；Runtime Step 自动推进连接，STATE 事件报告 TCP_ACTIVE 或 FAILED。建立中 Read/Write/ShutdownWrite 返回 NOT_READY；Cancel/Close 仍可使用。失败连接不会自动重拨，完整句柄在显式 Release 前保持有效。
- `kbs_runtime_adopt_tcp` 接管已有的、已连接的非阻塞 IPv4/IPv6 TCP fd，不更改调用方 fd 标志。参数是 `int*`：未接管失败保持原值，一旦 owner 接管立即写回 -1。即使后续发布/回滚出现错误，也以这个写回值判断所有权，不能再次关闭已转移的 fd。库内部保留未完成回滚的槽，拒绝继续准入，Close 可重试清理。
- `kbs_runtime_listen_tcp` 建立固定槽监听器，返回带 runtime identity、slot、完整 generation 的句柄。`listener_address` 用 getsockname 规则查询实际地址，支持端口 0。
- `kbs_runtime_accept_tcp` 每次最多取出一个连接，直接作为 NativeTCP 发布。无连接时返回 WOULD_BLOCK；容量不足在 accept4 前返回 EXHAUSTED，排队连接仍留在内核。内部已接受但未转移的 fd 在错误路径关闭。

建立连接复用 Connection/Owner/Scheduler 原状态机与身份；没有第二套拨号槽或完成队列。原有状态数值不变，TCP_CONNECTING 追加为 9；同步启动失败直接返回错误，异步错误在后续 Step 中成为连接事件。

## 监听订阅与接收

新监听器默认不订阅通知。调用 `kbs_runtime_watch_listener(runtime, listener, 1, …)` 安装一次性订阅，随后由 Step 收集就绪；事件模式下 RunOnce 也会被已订阅监听器唤醒。即使连接已经排在内核 backlog 中，重新订阅后仍会报告当前就绪。`events=0` 时通过非阻塞 Step 收集相同监听事件，不要求切换到事件等待模式。

`kbs_runtime_poll_listener_events` 从独立于 stream 的有界队列取出 `kbs_listener_event`。输出容量必须为 1–64，每个监听器最多保留一个合并事件；没有事件时成功返回 count=0。事件携带完整监听句柄和以下标志：

| 标志 | 含义 |
| --- | --- |
| `KBS_LISTENER_EVENT_ACCEPTABLE` | 监听 fd 可读，应用可以尝试 Accept |
| `KBS_LISTENER_EVENT_ERROR` | 内核报告错误就绪，由实际操作返回具体错误 |
| `KBS_LISTENER_EVENT_HANGUP` | 内核报告挂断就绪 |

这些标志是提示，不保证下一次 Accept 成功，也不推定已排队连接数或错误 errno。一次就绪整体消耗订阅，Poll、Accept 和 Step 都不会自动 rearm。处理提示后，应用有界调用 Accept，并在需要继续等待时再次 Watch。容量不足时先释放已有 stream；EXHAUSTED 不取走内核排队连接，之后可以重试 Accept 或重新订阅。

Watch 的 enabled 只允许 0 或 1。成功的 Watch(1) 建立新一轮订阅并撤销该监听器尚未领取的旧提示；Watch(0) 取消订阅并清除旧提示。关闭监听器也删除待取提示，复用槽的新句柄不会继承旧事件。应用可以完全不订阅而直接调用 Accept，订阅不改变接收所有权。

## 推进、事件和关闭

`kbs_runtime_step(runtime, 0, …)` 执行非阻塞 Tick；wait=1 使用 RuntimeShard RunOnce，并受 events/max_wait_ms 约束。已存在待取流事件或监听事件时只执行 Tick，防止再次睡眠。Step 返回本轮 processed、more_work 与 pending_events，后两项覆盖两类事件；流事件通过 `kbs_stream_poll_events` 取出，监听事件通过 `kbs_runtime_poll_listener_events` 取出。应用应分别处理两类队列，避免未领取提示让后续 Step 持续不睡眠。

流与监听通知按进度预算推进，监听源每次采集最多 64 条，避免一次扫描全部监听器。处理提示和有界 I/O 后按需重新 Watch，Poll 不自动重新订阅。监听 Accept、创建和连接准备属于控制路径；正常 Step、Watch、事件交付及复制 I/O 不分配内存，也不提供跨线程调用保护。

`kbs_runtime_now_ns()` 提供与调度器同源的单调时间，用于 connect 的 deadline 和 stream read 的 now_ns。连接期限由后续 Step 检查；停止推进不会有后台线程自行触发应用完成，批次预算和等待间隔也会影响实际报告时刻。

关闭顺序：

1. 业务需要优雅半关闭时先 shutdown_write，持续 Step/Read 处理尾部和 EOF。
2. 对每条流调用 close，再 release；对每个监听器调用 close_listener。逻辑 close 不等于远端应用已消费。
3. Runtime close 停止新准入，同时停止监听通知并清除未取监听提示。若仍有流或监听器，返回 BUSY，并保留已有流 I/O、事件、Step、关闭和 Release；监听句柄仍须逐个显式关闭，Watch(1) 已禁止，Watch(0) 仍可取消。清理后可重试，它不会批量取消应用连接或强行回收句柄。
4. Runtime close 成功后调用 destroy，成功把指针写为 NULL。尚未 Closed 的 Destroy 返回 BUSY 且保持指针；NULL 指针值可重复销毁。

`close_listener` 先撤销通知注册，再关闭 socket。撤销失败时保留有效句柄和 socket，返回错误供重试；一旦执行 socket close，句柄立即失效，即使 Linux close 返回错误也不重试同一个 fd。复用槽取得新 generation。C 输入/输出及诊断对象必须有效且互不重叠。普通 out 在失败时清零；fd 所有权参数与 getsockname 的地址长度遵守各自规则。错误分类沿用 stream ABI，TCP 建链/监听错误保留原生 errno 诊断，不将未知错误解释为确定的重放或释放许可。

## 验证边界

验证使用真实 IPv4 本机 TCP 与 C11/C++ 链接，无 UB 真机。既有范围包括配置、非阻塞建链、状态与错误、透明字节流、FIN/EOF、fd 接管与拒绝、固定容量及 BUSY 关闭；监听事件新增一次性订阅、已排队连接重订阅、取消和槽复用、接收容量背压、监听唤醒、预算及无分配推进。实际通过数量见 [roadmap](roadmap.md)。独立 raw C 生命周期、SHM、零拷贝、40k 规模和性能验证仍未交付或执行。

## 可选应用 Buffer Lease 池

M5.4.6 追加 `kbs_runtime_open_buffers` 与 `kbs_runtime_buffers`，按独立配置在控制路径创建固定容量 CPU-only 池，既有 `kbs_runtime_config` 布局不变。未打开时不分配该池；配置失败可纠正重试，成功后不再次 Open。用 [Buffer Lease API](buffer-api.md) 分配/借用/归还，池本身不提交传输。

Runtime Close 同时停止新缓冲区分配与可写借用；仍持有 CPU Lease 时返回 BUSY，保持只读/归还入口。所有应用 Lease 归还后才关闭 backing。Close 后已有表指针在 Destroy 前返回 NOT_READY，Destroy 后不得使用。
