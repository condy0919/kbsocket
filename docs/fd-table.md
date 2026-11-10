# M7.1：有界 FD 身份表与 CPU 退休

[`FdTable`](../kbsocket/socket/fd_table.hpp) 为后续 Socket/Epoll 兼容层提供整数 fd 到完整 stream 身份的映射。当前只管理绑定元数据：不创建、关闭或代理内核 fd，不执行 stream I/O，不自动延长 ConnectionOwner、Runtime、MR 或 Grant 的生命期。独立使用时宿主仍管理真实目标及其关闭；M7.2 的 [SocketDispatcher](socket-dispatcher.md) 已提供 owner 调度与关闭接线。

## 容量与身份

`Open(FdTableConfig)` 预分配两级目录、绑定池和读者表。目录每页 256 个槽，支持 `[0, fd_limit)`，最大 `fd_limit=1<<20`；宿主按自己的 FD 上限显式配置。第一版一次分配全部目录页，不在查询或发布路径懒分配。绑定池同时容纳当前绑定和未回收旧绑定，容量可以大于实际使用的 fd 数。预算不足在分配前返回 `kBudgetExceeded`，分配失败完整回滚，允许纠正后首次 Open。

`memory_budget_bytes` 覆盖 State、目录指针、页、绑定节点和 Hazard 槽的 `sizeof` 之和；`stats().storage_bytes` 报告相同口径，不包括分配器额外开销、真实 Runtime 或系统 FD 内存。`stats()` 是 owner 上的全池诊断扫描，并发期间引用数仅为快照，不能作为终结证明。

`FdBindingId` 包含不复用的 `table_id`、整数 `fd` 和 32 位 `generation`。宿主保证表 ID 在进程内唯一且不复用；同一对象成功 Open 后不可重新 Open。每个 fd 首次发布从 `initial_generation` 开始，后续只递增，达到上限后永久拒绝该 fd 的新绑定，不回绕到 1。即使绑定池槽已换作另一 fd，旧退休 token 也因完整身份不匹配而失效。

`FdTarget` 保存数值 `owner_id` 和完整 `kbs_stream_handle`，不存放可由应用线程裸解引用的 Runtime/Connection 指针。Publish 前由宿主确认目标有效；FdTable 不能验证另一个组件的内部容量或授权。适配层还须保证内核 fd 的真实分配/关闭与这些映射协调，不能把一个未在内核保留的整数当作已完成的 POSIX socket。

## 查询与引用交接

发布、解绑、确认关闭、收集与诊断由一个 owner 串行执行。应用线程可并发注册 `FdReader`、查询并归还 `FdLease`；同一个 Reader 不可同时由多个线程使用。注册受 `reader_capacity` 限制，读者表每槽按 64 B 对齐；Reader 和 Lease 都是不可复制的可移动 RAII 句柄。

Lookup 的顺序为：

1. 读取 fd 槽指针，将其发布到本线程独占的 Hazard 槽，再重检 fd 槽。
2. 在保护内校验完整绑定代次，取得节点的长期 CPU 引用，再检查停止状态和 fd 槽。
3. 清除 Hazard，返回 `FdLease`。异步命令或回调可接管 Lease，阻塞等待期间不持有 Hazard。

发布、重检、引用交接与回收侧扫描使用顺序一致原子操作。Lookup 的重试次数有界，竞争过多返回 `kContended`，引用计数达到 `max_references` 返回 `kReferenceLimit`。读者槽注册和引用归还不分配、不加锁。

裸 fd 查询取得调用时成功重检的绑定；旧 epoll 事件和已保存请求必须使用带完整 `FdBindingId` 的查询，不能重新按裸 fd 解析。Lookup 成功后，owner 可以立刻解绑；已经取得的 Lease 仍只保存旧目标。`current()` 是瞬时快照，不能作为稍后提交的授权，也不撤销在解绑前已经取得的操作引用。

FdLease **只保留绑定元数据**。跨线程业务必须把完整目标交给 owner，按既有 Mailbox/stream 契约执行；适配层应另行保留 owner registry、命令、回调及实际流的责任，不能因取得 Lease 就裸调单 owner C stream。解绑前已接受的操作可以在旧连接完成或失败，不能改投新代连接。

## 解绑、退休与关闭

`Unbind(id)` 先清除公开槽，再将原绑定放入固定的 owner 退休队列，返回包含完整身份和池槽的 `FdRetireToken`。相同整数 fd 随即可以发布新代；旧节点仍占用绑定容量，与新节点独立存在。

真实目标完成 owner 侧关闭/Release 及外部使用责任后，宿主调用 `ConfirmClosed(token)`。对同一退休 token 的确认幂等，过期或跨表 token 被拒绝。纯元数据回调可以继续持有旧 Lease；实际 I/O 使用者则必须按原 owner 关闭契约结束，不能把确认当作清除业务责任的捷径。

`Collect(output, budget)` 有界轮转退休队列。一个节点必须同时满足关闭已确认、所有 Hazard 均不指向它、长期引用为零，才能输出其身份并归还绑定槽。必须**先扫描 Hazard，再检查引用数**：反过来会漏掉读者从 Hazard 向引用计数交接的中间状态。未确认或被引用的节点移到队尾，不能挡住后续可回收节点；每次最多检查 `budget` 个原有节点，最多输出 `output.size()` 项。

这一回收只涉及 FD 元数据。真实流的关闭继续遵守 TX CQE、RX Lease、来源退休及 RNDV Grant 契约；CPU 引用为零不证明 DMA 或远端授权已终结。

`StopAccepting()` 阻止新的发布、注册和查询，仍允许解绑、确认、收集及已有句柄归还。最终 `Close(true)` 要求外部调用者已停止，全部 Reader 已归还，当前及退休绑定均清空。缺少停用确认返回 `kNotQuiescent`，仍有责任返回 `kRetained`。Open/Close/析构不能与进入门面的线程并发。

未完成 Close 的析构先停止准入，再保留整个 State。存续 Reader/Lease 直接访问保留的稳定节点，迟到 Reset 不通过已销毁的 FdTable；Lease 的身份仍可读而 `current()` 为 false。该路径有意保留内存，不提供后台回收，也不替宿主保活实际 Runtime/provider 依赖。

## 验证与边界

- [基础测试](../kbsocket/socket/fd_table_test.cpp)：页边界与配置预算、各分配点失败回滚、完整身份、代次耗尽、退休确认/CPU 引用独立门禁、Reader/引用容量、移动归还、队列轮转、Open 后零分配和未关闭析构的迟归还。
- [并发测试](../kbsocket/socket/fd_table_concurrency_test.cpp)：四读线程与 owner 执行 4000 代绑定/解绑/回收，检查不可变身份、完整旧代拒绝及全部节点恰好一次回收；另覆盖读者已退出但操作引用仍存续、并发 Reader 满额及归还。ThreadSanitizer 单独运行同一组三个用例通过。
- [NativeTCP 组合测试](../kbsocket/socket/fd_table_stream_test.cpp)：真实 IPv4 loopback 和 C Runtime。应用线程只查询并交接 Lease，owner 将旧请求发给旧 stream、新请求发给新 stream，分别核验收到的字节；真实 Close/Release 后确认退休，复用 stream 槽时旧 stream 句柄和旧 FD 身份均被拒绝。

这里的 M7.1 NativeTCP 用例只模拟 owner dispatch；M7.2 已通过 [SocketDispatcher](socket-dispatcher.md) 接通独立内核 FD 预留、跨线程命令及真实 stream 关闭。POSIX Socket 门面、EpollMux、bRPC 接入和 40k 规模验收仍待后续；未把这些结果当作 UB 或吞吐性能证据。回归计数见 [roadmap](roadmap.md)。
