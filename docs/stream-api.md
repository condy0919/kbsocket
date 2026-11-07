# M5.4.1：copy-stream C 接口

[`stream.h`](../kbsocket/api/stream.h) 提供 Eager 和 NativeTCP 共用的有界复制读写、半关闭、取消、状态查询及关闭/释放接口。头文件可由 C11 独立编译，不暴露 URMA、C++ 类型或底层 fd。`KBS_STREAM_ABI_VERSION=1` 标识本次结构与函数契约；完整 Runtime 创建、拨号/接收、统一 readiness 和零拷贝 Lease 接口仍待后续交付。

## 绑定已有运行时

C++ 控制面使用 [`StreamBinding`](../kbsocket/api/stream_binding.hpp)：

- `OpenTcp(owner, config)` 绑定无 raw context 的 ConnectionOwner，允许完全无设备的 NativeTCP 服务。
- `OpenRaw(stack, context, config)` 绑定 RawStack 的指定 context。每次访问连接、发布、退休来源或关闭表前均重新经过 RawStack 的资源门禁，context 被控制任务暂停时返回 `BUSY`。
- 宿主完成 fd 接管及协议准备，用 `Publish(OwnedConnection)` 发布 C 句柄。Negotiating/Preparing 阶段可查询或取消，应用 I/O 直到 Active 才允许；失败或已关闭的连接也可发布以完成诊断/清理。

一个绑定对应一个 owner。Open 预分配发布槽、空闲索引和按 owner 槽索引的反向表，Publish/查找/Release 均为 O(1)，没有全局连接表、锁或隐式扩容。`capacity` 不得大于 owner 容量；反向表按 owner 容量分配。表只支持一次生命周期。创建失败不接管 owner 或连接，配置可以纠正后重试。

`table_id` 必须由宿主保证进程内跨表、跨生命周期不复用。C 句柄同时校验 table_id、完整 64 位发布 generation、slot 和保留字段，generation 耗尽拒绝发布。底层 OwnedConnection 还会检查其 owner 与完整代次；fd 数值、对象地址或单个槽位都不构成身份。

同一逻辑连接只能交给一个发布表。发布后 I/O、关闭和 Release 统一经过该表；宿主仍可执行协议 Prepare，但不能绕过表另行 Release 或把同一连接再次发布到其他表。绑定借用 owner/RawStack，必须在其物理关闭前关闭本表。析构不代办连接关闭或释放 DMA；应用停止访问后才可销毁绑定。已取得的 C 表指针在绑定对象存活期间有效，表 Close 后返回 `NOT_READY`，不得跨对象析构保存。

## 调用与数据语义

所有入口由唯一 owner 串行调用，不能在 Runtime 回调中重入，也不接受跨线程直接访问。业务线程继续通过已有 [ConnectionMailbox](connection-runtime.md) 向 owner 交接。Runtime 进度仍由宿主 Tick/RunOnce；调用 C I/O 本身不轮询 CQ，不等待连接就绪。

| 接口 | 成功含义与边界 |
| --- | --- |
| `kbs_stream_write` | 返回已复制到库缓冲或 TCP 内核的前缀；源内存可立即复用，不代表已提交 WR、远端已收到或消费 |
| `kbs_stream_read` | 返回按连接顺序连续的字节；可短读，`eof` 与零进展 `WOULD_BLOCK` 分开 |
| `kbs_stream_shutdown_write` | 在已接受数据之后发 FIN，读方向继续使用；仍须 Runtime 推进，不提供同步 drain |
| `kbs_stream_cancel` | 显式取消连接，保留已有 DMA/来源责任与句柄，之后仍须关闭和释放 |
| `kbs_stream_inspect` | 查询稳定状态、已锁存错误、Eager 排队及在途数量、RX 驻留数、来源保留数；这些计数不替代终结证明 |
| `kbs_stream_watch_tcp` | 对 NativeTCP 订阅一次 readiness；raw 明确拒绝，完整统一事件 ABI 待后续 |
| `kbs_stream_close` | 有界关闭逻辑连接；可丢弃未提交/未读数据，不承诺优雅发送完毕 |
| `kbs_stream_release` | 连接 Closed 且来源退休后解除调度并释放槽；成功立即使旧 C 句柄失效 |

`max_io_bytes` 在 Open 配置，范围 1 B–1 MiB，默认 64 KiB。超过上限的输入/输出只处理一次有界前缀，其余由应用后续调用。下层 Slab、序列窗口、Credit 和 TCP 预算可能进一步短读写。成功字节前缀不会被后续错误覆盖；后续调用或 Inspect 继续报告已锁存错误，禁止自动重放。

每个函数返回 0 或固定正错误码，不读写 `errno` 作为 API 错误通道。I/O/Inspect 的 out 必填，失败时清零；error 可空，每次调用完整覆盖。输入、输出及诊断结构须为有效且互不重叠的内存。零长度允许 data=nullptr；空 Read 仍校验连接状态，但不探测 EOF，成功返回 bytes=0/eof=0。正长度 Read 在远端 FIN 后才报告 EOF。read 的 now_ns 由 owner 提供，须与 Runtime 使用同源单调时钟。

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

验证使用纯 C 编译/C++ 链接、本地 IPv4 TCP 以及 Mock RawStack。重点覆盖有界前缀、错误与 EOF、旧句柄、关闭保留、来源退休、context 暂停和无分配 I/O。实际回归数量记录于 [roadmap](roadmap.md)。UB 真机与 40k 性能验收未执行；本阶段不宣称完整独立 C Runtime、跨线程同步 API、统一 readiness 或零拷贝 ABI 已交付。
