# kbsocket 架构与实现改进清单（对标 UCX / 基于 RM_CTP 硬件可靠乱序模型）

> **分析前提**：`kbsocket` 当前采用的 UB `RM_CTP` 模式在队列拓扑上类似于 RDMA 的无连接多路径模式（UM），但**硬件自身保证可靠传输（硬件处理重传），软件无需维护重传缓存与重传定时器，仅需处理乱序到达（Out-of-Order）、共享接收队列（Shared JFR）流控、主动发送通道（TX Lanes）复用以及 SEND + WRITE RNDV 协同**。
>
> 在 `ucx` 中，与 `RM_CTP` 硬件语义高度同构的参考实现包括：
> 1. **AWS EFA SRD**（`src/uct/ib/efa/srd/` + `src/ucs/datastruct/frag_list.*`）：硬件负责多路径可靠重传与无连接复用，软件仅做零分配的乱序重组（`ucs_frag_list_t`）与软件 Fence/Flush。
> 2. **MLX5 DC**（`src/uct/ib/mlx5/dc/`）：被动接收端点（`DCT` + `SRQ`，对应 `kbsocket` 的 `rx_target_jetty` + `JFR`）与主动发送通道池（`DCI Pool`，对应 `kbsocket` 的 `tx_lanes`）分离，采用 `DCS` / `DCS_QUOTA` 策略动态绑定发送通道。
> 3. **UCP Stream / RNDV / Worker**（`src/ucp/stream/`、`src/ucp/rndv/`、`src/ucp/core/ucp_worker.c`）：网络层乱序重组与应用层待读字节流队列解耦、多轨流水线 PUT RNDV、以及 $O(1)$ 活跃进度调度。

---

## 总览与优先级矩阵

| 编号 | 模块维度 | `kbsocket` 当前问题摘要 | 对标 `ucx` 设计模式 | 优先级 |
| :--- | :--- | :--- | :--- | :--- |
| **ISSUE-1** | **控制任务与数据面隔离** | 单次 `import_jetty` / `import_seg` / `ModifyJfr` 会将整个 `ContextProgressGroup` 置为 `paused`，导致全 Context 数据面收发与 `epoll` 毫秒级停摆 | 控制操作仅挂起未就绪的单个 `ep` / `req`（`pending_group`），Worker 数据面 CQ 轮询永不中断 | **P0** |
| **ISSUE-2** | **Reactor 连接进度调度** | 每次 `OnRx` / `kRefill` 调用 `RequestSweep()` 触发全量连接线性扫描（`state.dense`），且每连接重复调用 `steady_clock::now()` 及热路径 `eventfd` 系统调用 | 仅调度 `ready` / `ucs_arbiter_t` 活跃队列 + $O(1)$ 时间轮 `ucs_twheel_t` + 时钟采样分摊 | **P0** |
| **ISSUE-3** | **TX Lane 调度与 RX 来源校验** | 逐批 Round-Robin 喷洒人为制造严重乱序；`RxSourceRegistry` 绑定对端每条物理 TX Lane，迫使每连接常驻持有 TCP fd 运行 `LaneControl` 同步 | `DCS_QUOTA` 连接亲和绑定减少乱序；RX 仅校验 `ep_uuid` / Session Token，本地重置 DCI 对端完全无感 | **P1** |
| **ISSUE-4** | **乱序重排窗口 (`ReorderWindow`)** | 乱序窗口与应用未消费数据耦合（`released_prefix` 阻塞窗口推进）；40k 空闲连接预分配 `Entry[W]` 耗费数百 MiB；每包 $O(W)$ 扫描；乱序 RTS 队头阻塞 | `ucs_frag_list_t` 顺序包 0 驻留直入 `ucp_stream` 就绪队列；乱序包原地侵入式链表 $O(\text{holes})$；乱序 RTS 并行准入 | **P1** |
| **ISSUE-5** | **RNDV 并发度与内存分级** | `promised_control != 0` 限制每 `PeerRuntime` 全局仅 1 个 RNDV 在途；Eager 与 RNDV 互斥无法交织；64B 控制帧占用大块 RNDV Slab 槽位 | 入队即分配 SN 支持流水化交织；控制帧使用 `IBV_SEND_INLINE`；定长分片 Pipelined RNDV 与分级内存池 | **P2** |
| **ISSUE-6** | **流控 (`Credit`) 发布机制** | `kDataWithCredit` 捎带更新在发送端未启用；`ReservePublication` 限制每 `PeerRuntime` 全局仅 1 个 `CREDIT_UPDATE` WR 在途 | 累计单调序号天然幂等（无需单在途锁）+ 反向数据帧捎带更新 | **P2** |

---

## 详细问题分析与改进建议

### ISSUE-1 [P0]：控制面任务（`ImportJetty` / `ImportSeg` / `ModifyJfr`）暂停整个 `ContextProgressGroup` 数据面

#### 1. 现状代码定位
- `kbsocket/runtime/raw_stack.cpp:196-220` (`RawStack::Open`)
- `kbsocket/runtime/runtime_shard.cpp:74-91` (`RuntimeShard::Open`)
- `kbsocket/runtime/context_progress_group.cpp:77-224` (`ContextProgressGroup`)
- `kbsocket/runtime/shard_reactor.cpp:244-278` (`ShardReactor::Advance`)

#### 2. 问题描述
- 在 `RuntimeShard::Open` 中，每个 `urma_context_t*` 下的 5 个进度源（`RawContextProgress`、`ConnectionProgress`、`ImportProgress` + `TsegImportProgress`、`ConnectionMailbox`、`PeerCreditProgress`）被打包进同一个 `ContextProgressGroup`，作为 `ShardReactor` 的单个 `SourceSlot` 调度。
- 当组内任意成员产生 `PendingControl()` 任务时（包括建链 `UrmaApi::ImportJetty` / `UnimportJetty` [`endpoint_import_cache.cpp:57-76`]、RNDV `UrmaApi::ImportSeg` / `UnimportSeg` [`tseg_import_cache.cpp:62-80`]、JFR 水线重写 `UrmaApi::ModifyJfr` [`raw_context_progress.cpp:563-598`]）：
  1. `ShardReactor::Advance`（`shard_reactor.cpp:250-255`）立即将该槽位置为 `slot.paused = true`，并调用 `WatchSource(i, false)` 对该组所有监听 fd 执行 `epoll_ctl(EPOLL_CTL_DEL)`（任务完成时再执行一轮 `EPOLL_CTL_ADD`）；
  2. 在 `ControlExecutor` 后台线程执行内核/驱动调用的整个毫秒级窗口内，**该 Context 上所有已有健康连接的 TX/RX CQ 轮询、JFR 补投、业务 Mailbox 读写命令全部 100% 停摆**；
  3. 宿主或 C API 调用 `RawStack::resources()`（`raw_stack.cpp:238`）、`RawStack::shared()`（`raw_stack.cpp:253`）或分配/释放 `BufferBinding` 也会因检查 `paused()` / `AnyPaused()` 直接返回 `kInUse` (`KBS_STREAM_BUSY`)；
  4. `ContextProgressGroup::PendingControl()`（`context_progress_group.cpp:188-204`）每次只串行提交 **1 个**控制任务。在 40k/s 建链或 3000 Peer 冷启动并发导入场景下，数据面将持续处于“冻结—解冻—再冻结”抖动中，严重拉高 P99/P99.9 尾延迟，甚至诱发对端 `ReorderWindow` 触发 1s `kGapTimeout` 断链。

#### 3. `ucx` 对标实现
- 在 `ucx` 中，控制路径操作（如 `uct_srd_ep_t` 异步建立 AH [`src/uct/ib/efa/srd/srd_ep.c:66-90`]、`ucp_ep_rkey_unpack_internal` 解包远端内存段 [`src/ucp/core/ucp_rkey.c:915-1060`]、`ucs_rcache_create_region` 注册内存 [`src/ucs/memory/rcache.c:994-1131`]）仅将尚未就绪的单个 `ep` 或 `req` 挂入 `ep->pending_group`，Worker 线程对共享 CQ/SRQ 的轮询（`ucp_worker_progress` [`src/ucp/core/ucp_worker.c:3325-3345`]）**永不暂停**。

#### 4. 改进建议
1. **移除 `ImportJetty` / `ImportSeg` / `Unimport` / `ModifyJfr` 的全 Context 暂停屏障**：`liburma` 的 `urma_import_jetty`、`urma_import_seg`、`urma_modify_jfr` 属于控制面命令，与单 Owner 线程在用户态无锁队列（`lock_free = 1`）上的 `PostJettySendWr` / `PostJfrWr` / `PollJfc` 正交，无需暂停整个 Context 数据面（仅在 `ReplaceDrainedLane` 替换物理队列时对目标 Lane 做局部状态隔离）。
2. **支持 `ControlExecutor` 批量并行提交与常驻 `epoll` 注册**：允许 `EndpointImportCache` 与 `TsegImportCache` 向 `ControlExecutor` 批量提交多个并发导入任务，执行期间仅对应的 `ConnectionHandshake` 或 `RndvTxCoordinator` 槽位处于 `kPending` 等待态，且不反复调用 `epoll_ctl(DEL/ADD)`。


#### 5. 核实与修复记录（2026-11-11）

- **结论：部分修复。** 全 Context 暂停机制属实；“毫秒级停摆”、P99/P99.9 和 40k 规模影响尚未通过硬件/压测证实。
- **本次修复：** `ControlTask::pause_progress` 默认保留独占屏障，仅 endpoint/tseg 导入及解导入任务关闭屏障。worker 使用独立 `Work` 快照，导入结果只在 owner 收割后发布；解导入仍检查引用。Reactor 与组合源继续数据轮询、通知、Rearm 和 fd 注册，同时保持单源单在途任务及完成身份检查；关闭仍等待在途任务。
- **验证：** `context_progress_group_test`、`import_progress_test`、`tseg_import_progress_test`、`raw_stack_test`、`shard_reactor_test`、`runtime_shard_test` 及两类 `import_cache_test` 通过（146 个独立用例）；连接集成测试已将旧暂停预期改为阻塞导入期间仍能轮询与执行 mailbox。`raw_stack_test` 另验证 `AdvanceClose` 不提前释放后台任务依赖。
- **保留项：** `ModifyJfr`、`BeginDrain`、`ReplaceDrainedLane` 仍有屏障，同 Context 控制操作仍串行。`RawContextProgress::RunControl` 直接访问 `rx->posted()/stopped()`、pool 状态，直接关闭屏障存在竞态；需先拆分 worker 快照和 owner 状态发布，再实现局部隔离及批量并行。UB 真机并发验证未执行。
- **UCX 对比纠正：** 本地 `ucx/src/uct/ib/efa/srd/srd_ep.c` 引用处实际同步调用 `uct_ib_iface_ah_get`，不能据此证明“异步建 AH”或任何控制操作均不阻塞 Worker；`ucp_worker_progress` 的实现也不能支持“永不暂停”的绝对结论。这里修复的依据是本库的具体线程共享关系。

---

### ISSUE-2 [P0]：Reactor 热路径 `RequestSweep()` 全量连接扫描与高频系统调用开销

#### 1. 现状代码定位
- `kbsocket/runtime/connection_io.cpp:64` (`ConnectionIoBinding::OnRx`)
- `kbsocket/runtime/connection_progress.cpp:86-210, 469-549` (`ConnectionProgress`)
- `kbsocket/runtime/connection_command_queue.cpp:209, 223` (`ConnectionCommandQueue`)
- `kbsocket/connection/connection.cpp:757` (`Connection::SubmissionTime`)

#### 2. 问题描述
1. **单包接收触发 $O(\text{All Connections})$ 全量扫描**：
   - `ConnectionIoBinding::OnRx`（`connection_io.cpp:64`）在每收到一个 RX CQE 时，以及 `ConnectionProgress::Progress(kRefill)`（`connection_progress.cpp:476`）在每次补投 JFR 时，都会调用 `RequestSweep()` 将 `state.next_sweep` 置为 `0`；
   - 紧接着在同轮 Tick 的 `kTimer` 阶段（`connection_progress.cpp:479-497`），`ConnectionProgress` 遍历 `state.dense[0..attached-1]`，**将该 Context 下挂载的所有非 TCP 连接全部推入 `ready` 队列逐一调用 `Visit()` -> `Connection::Progress()`**。在单容器 40k 常驻连接（QD=10 稀疏活跃）场景下，单个入站包触发数万次连接遍历会直接打满 CPU。
2. **Busy-Poll 热路径高频系统调用**：
   - `ConnectionCommandQueue::TrySubmit`（`connection_command_queue.cpp:209`）每次提交命令都无条件调用 `::write(state.fd, ...)` 写 `eventfd`；`PollCommands`（`connection_command_queue.cpp:223`）每次轮询都无条件调用 `::read(state.fd, ...)`；
   - `ConnectionProgress::CollectNotifications`（`connection_progress.cpp:521`）在每个 Tick 都调用嵌套的 `::epoll_wait(state_->epoll, ..., 0)`；
   - `ConnectionProgress::State::Visit`（`connection_progress.cpp:169`）和 `Connection::SubmissionTime`（`connection.cpp:757`）在访问每个连接时都调用 `std::chrono::steady_clock::now()`。

#### 3. `ucx` 对标实现
- **紧凑回调数组 `ucs_callbackq_t`**（`src/ucs/datastruct/callbackq.h:207-218`）：`ucp_worker_progress()` 仅在缓存行对齐的 `fast_elems` 数组上执行活跃回调，无锁、无链表跳转。
- **按需调度器 `ucs_arbiter_t` + $O(1)$ 时间轮 `ucs_twheel_t`**（`src/uct/ib/efa/srd/srd_iface.h:181-187`、`src/ucs/time/timer_wheel.c:19-112`）：只有有待发请求或资源等待的 EP 才进入 `ucs_arbiter_t`；超时管理通过 1024 槽位 $O(1)$ 时间轮推进，且时钟读取与时间轮检查每 8~32 次轮询才分摊执行一次（`UCT_UD_SKIP_SWEEP = 8` [`src/uct/ib/ud/base/ud_iface.h:460-471`]、`UCP_WORKER_PROGRESS_TIMER_SKIP_COUNT = 32` [`src/ucp/core/ucp_worker.inl:24`]）。

#### 4. 改进建议
1. **以精准唤醒替代 `RequestSweep()` 全量遍历**：
   - `OnRx` 仅将收到报文的**目标 `Connection` 槽位**推入 `ready` 链表；
   - `kRefill` 补投释放出新可授信额度时，仅唤醒挂在该 `ReceiveCreditDomain` 待授信队列上的连接；
   - 引入 $O(1)$ 时间轮（参考 `ucs_twheel_t`）管理连接级超时，彻底移除对 `state.dense` 的 1ms 周期全表遍历。
2. **消除 Busy-Poll 系统调用**：
   - 在 `ConnectionCommandQueue` 中维护 `std::atomic<bool> sleeping` 状态，仅当 Reactor 准备进入 `epoll_wait` 休眠时才触发 `eventfd` 的 `::write` / `::read`；
   - 在 `ShardReactor::Advance()` 入口采样一次 `now_ns` 并向下透传，移除 `Visit()` 内逐连接的 `steady_clock::now()` 调用。


#### 5. 核实与修复记录（2026-11-11）

- **结论：部分修复。** 已证实普通 RX 无条件 `RequestSweep()`、逐连接时钟采样、命令提交/空轮询的 eventfd 调用。原文“每次 kRefill 都扫描”不准确：实际补投成功才重置巡检时间，空补投不会。
- **本次修复：** 普通 RX 按已验证的路由 O(1) 入队目标连接；共享信用上限变化或整域冻结仍保留巡检，避免漏掉兄弟连接及迟到 CREDIT。命令队列合并突发通知、跳过空轮询 `read`，区分通知写入中/已写入以防丢唤醒；预算耗尽仍保留 readiness。`Visit` 的时钟改为每个非空调度批次采样一次，连接内部仍钳制已观察时间，避免倒退。
- **验证：** `connection_command_queue_test`、`connection_progress_test`、`rx_source_registry_test`、`eager_rx_test` 通过；`connection_runtime_test` 无过滤全目标通过。新增回归验证三条命令仅生成一次 eventfd 通知、多条空闲连接场景下普通 RX 只访问目标，以及批次时间晚于/早于 RX 与应用 Read 时保持单调。
- **保留项：** 成功补投、共享 CREDIT 和 1 ms 周期巡检仍可能扫描全表；精确待授信队列/时间轮、嵌套 epoll、`SubmissionTime()` 和 RNDV 的额外采样未改。通知当前按批合并，尚非仅休眠时写 eventfd，不能宣称热路径已无系统调用。

---

### ISSUE-3 [P1]：主动 TX Lane 逐批 Round-Robin 喷洒加剧乱序，且 `RxSourceRegistry` / `LaneControl` 与对端物理 Lane 过度耦合

#### 1. 现状代码定位
- `kbsocket/transport/raw/jetty_pool.cpp:502-524` (`JettyPool::Reserve`)
- `kbsocket/transport/raw/tx_sender.cpp:120-138` (`TxSender::Send`)
- `kbsocket/runtime/rx_source_registry.cpp:128-236` (`RxSourceRegistry`)
- `kbsocket/control/lane_control.cpp:59-247` (`LaneControl`)
- `kbsocket/control/tx_lane_snapshot.cpp:73-104` (`ValidateLocalTxLanes`)
- `kbsocket/connection/connection.cpp:395-407, 439-445` (`Connection::Progress`)
- `kbsocket/connection/eager_rx.cpp:142-145` (`EagerReceiveDomain::Dispatch`)

#### 2. 问题描述
1. **逐批 Round-Robin 喷洒人为制造乱序**：
   - `EagerTxStream::Post`（每次最多 16 帧）、`CREDIT_UPDATE` 和 `RndvNotificationTx` 调用 `TxSender::Send(requests, std::nullopt)` 时，都会调用 `JettyPool::Reserve()` 轮转递增 `next_lane_`，将**同一条连接的连续报文喷洒到池内多达 64 条独立的物理 JFS（TX Lanes）上**。
   - 不同 JFS 在硬件和网络中被独立调度，导致原本在单 JFS 下小概率乱序的报文变成高频乱序，频繁触发接收端 `ReorderWindow` 暂存与 `RndvRxIngress` 队头阻塞。
2. **`RxSourceRegistry` 笛卡尔积膨胀与 `LaneControl` 40k TCP fd 常驻开销**：
   - 因为一条连接可能使用对端任意一条主动 TX Lane，接收端必须在 `RxSourceRegistry` 中按 `{local_id, is_jetty, remote_id, WireToken}` 四元组为每条连接注册 `L` 条规则（$N \times L$ 条目，且 `RegisterBatch` [`rx_source_registry.cpp:182-186`] 对每条规则做 `for (i = 0; i < capacity; ++i)` 线性扫描）。
   - 建链 `READY` 后，每条连接把 TCP 握手 fd 移交给 `LaneControl` 常驻持有（40k 连接即常驻 40k 个内核 TCP socket），并在每次 `Connection::Progress()` 时调用 `ValidateLocalTxLanes` 扫描本地池内全部 Lane。
   - 一旦某条 TX Lane 因 `status 9` 重建生成新的硬件 `jetty_id`，所有连接立即停止发送（`tx_allowed = false`），必须经由各自的 TCP fd 发送 3104 字节的 `kLaneUpdate` 并等待对端更新 `RxSourceRegistry` 回复 `kLaneAck` 后才能恢复发送。
3. **单个未知来源 CQE 永久冻结整个 `RawDomain`**：
   - 若 `RxSourceRegistry::Resolve` 未命中，[`EagerReceiveDomain::Dispatch`](file:///home/condy/ub/kbsocket/kbsocket/connection/eager_rx.cpp#L142-L145) 调用 `ConsumeUnknown()` + `Freeze()`，直接停止 `RxBufferPool` 并永久废掉整个共享接收域。

#### 3. `ucx` 对标实现
- **`DCS` / `DCS_QUOTA` 发送通道亲和**（`src/uct/ib/mlx5/dc/dc_mlx5_ep.h:628-850`）：Endpoint 发送时绑定一个空闲 DCI（TX Lane），在途 WQE 排空前固定使用同一 DCI，避免跨 DCI 乱序。
- **协议层 `ep_uuid` 鉴权解耦物理发送队列 ID**（`src/uct/ib/efa/srd/srd_iface.c:728-779`、`src/uct/ib/mlx5/dc/dc_mlx5.c:1924-1959`）：接收端仅校验报文头中的 `ep_uuid`，从不校验发送端动态分配的 DCI QPN；发送端 DCI 报错重置（`uct_dc_mlx5_iface_reset_dci`）对接收端完全透明，无需任何跨网络同步。

#### 4. 改进建议
1. **将 `JettyPool` 改为“连接亲和绑定（Connection-Sticky / `DCS_QUOTA`）+ 故障漂移”**：每条连接默认绑定固定的首选 `JettyLane`（包括 Eager、`CREDIT_UPDATE`、RNDV `RTS/GRANT/DONE/WRITE`），仅在该 Lane 满载或故障重建时漂移至其他健康 Lane。
2. **解耦 `RxSourceRegistry` 与对端主动 TX `jetty_id`，移除 `LaneControl`**：
   - 接收端仅校验 `{local_rx_jetty_id, peer_eid, WireToken}`（配合握手中分配的不可预测 `SessionToken`），**不再绑定对端主动 TX Lane 的 `remote_id.id`**；
   - 本地 `ReplaceDrainedLane` 重建 TX Jetty 时对端完全无感，可**彻底删除 `LaneControl` 及 3104B 的 `kLaneUpdate/Ack` 协议**，并在握手 `READY` 后立即关闭或复用每连接的 TCP 握手 fd（节省 40k 个内核 fd）。
3. **未知/迟到 CQE 原位释放补投**：收到迟到或未知帧时直接归还 `RxBufferLease` 并正常进入 `Refill` 补投（物理 RQE 守恒并未破坏），不再调用 `Freeze()` 冻结共享接收域。

---

### ISSUE-4 [P1]：`ReorderWindow` 网络乱序重组与应用接收缓存耦合、静态内存膨胀及乱序 RTS 队头阻塞

#### 1. 现状代码定位
- `kbsocket/connection/reorder_window.hpp:79-167`
- `kbsocket/connection/reorder_window.cpp:50-568`
- `kbsocket/connection/rndv_rx_ingress.cpp:189-208, 270-276`

#### 2. 问题描述
1. **网络乱序窗口与应用未消费数据耦合**：
   - `ReorderWindow::stats()`（`reorder_window.cpp:201`）通告 `max_frame_seq = min(kSequenceEnd, released_prefix + config.window)`。
   - 即使数据包按序到达并已通过 `Poll()` 交付给 `EagerRxStream`，只要应用尚未读完或持有 `ReadLease` 未调用 `Release(receipt)`，`released_prefix` 就无法推进，该槽位依然占用 `W=64` 的重排窗口。单次 256 KiB 的 Eager 写入（64 个 4 KiB 帧）即可填满 `W=64` 导致发送端停顿。
2. **40k 连接下静态预分配 `Entry[W]` 内存开销巨大**：
   - `ReorderWindow::Open`（`reorder_window.cpp:138`）无条件分配 `new Entry[config.window]`（每个 `Entry` 约 144 B）。在 `W=64`（开启 RNDV 时默认 `W=128`）下，40,000 条常驻连接仅空窗就消耗 **368 MiB ~ 736 MiB** 堆内存。
3. **每包到达执行 $O(W)$ 线性扫描**：
   - `ReorderWindow::CheckAdmission`（`reorder_window.cpp:310-323`）和 `CompleteRndv`（`reorder_window.cpp:404-410`）在每次收到包时（含顺序包）都执行 `for (uint32_t i = 0; i < state_->config.window; ++i)` 检查 `SameLease`。
4. **乱序 `RNDV_RTS` 队头阻塞（HoL Blocking）**：
   - 在 `RndvRxIngress::StepPacket`（`rndv_rx_ingress.cpp:189-208`）中，当 `rts.sequence > window.observed_prefix`（例如 `RTS(SN=S)` 比前一个 Eager 帧 `SN=S-1` 早到）时，接收端拒绝调用 `OnRts()` 和回复 `GRANT`；在 `StepJob`（`rndv_rx_ingress.cpp:270-276`）中也强制按序串行 `PrepareGrant()`，白白阻塞整个 RNDV 握手与 RDMA `WRITE` 传输。
5. **`RM_CTP` 下 1s `kGapTimeout` 误杀风险**：
   - `RM_CTP` 硬件保证不丢包，缺口仅代表前序包仍在途或发送端被暂停/背压，`ReorderWindow::ObserveTime`（`reorder_window.cpp:219-221`）在 1s 后直接报 `kGapTimeout` 永久断链过于激进。

#### 3. `ucx` 对标实现
- **`ucs_frag_list_t` 顺序包 0 驻留 + 侵入式两级 Burst 链表**（`src/ucs/datastruct/frag_list.h:174-208`、`src/ucs/datastruct/frag_list.c:263-341`）：
  - 顺序到达包（`sn == head_sn + 1 && elem_count == 0`）仅递增 `head_sn` 并返回 `UCS_FRAG_LIST_INSERT_FAST`，**0 内存开销、不进入重排表**，直接挂入上层 `ucp_stream` 的就绪队列 `match_q`（`src/ucp/stream/stream_recv.c:463-615`）；
  - 乱序包通过内嵌在 RX 描述符 `uct_srd_recv_desc_t` 中的 `ucs_frag_list_elem_t` 原地串联成“连续 Burst 段链表”，空闲连接占 **0 字节**，乱序插入复杂度为 $O(\text{空洞段数 } k)$ 而非 $O(W)$。

#### 4. 改进建议
1. **拆分“网络乱序重组器”与“有序待读流队列”**：帧一旦满足 `sn == rcv_next`，立即移入连接的有序待读链表并释放重排窗口槽位；`max_frame_seq` 锚定 `rcv_next + W`，应用未读驻留量单独由 `max_stream_bytes = consumed_bytes + max_resident_bytes` 约束。
2. **使用侵入式描述符链或按需分配乱序槽**：在 `RxBufferPool` 描述符内嵌链表指针，消除每连接预分配 `Entry[W]` 的百兆级内存开销，并用槽位 `kHeld` 状态位在 $O(1)$ 内替代 $O(W)$ 的 `SameLease` 遍历。
3. **允许乱序到达的 `RNDV_RTS` 立即准入并回复 `GRANT`**：只要 `rts.sequence < max_frame_seq` 且 RNDV 内存预算允许，立即在重排窗口占坑并发回 `GRANT`，让远端 RDMA `WRITE` 与前序在途 Eager 包并行传输，仅在向应用交付时按 `rcv_next` 保序。

---

### ISSUE-5 [P2]：RNDV 每 Peer 单在途事务锁死、Eager/RNDV 互斥与 Slab 内存粒度浪费

#### 1. 现状代码定位
- `kbsocket/connection/rndv_connection.cpp:228-273` (`RndvConnection::Write` / `WriteLease`)
- `kbsocket/connection/rndv_notification_tx.cpp:300, 477` (`RndvNotificationTx`)
- `kbsocket/connection/eager_tx.cpp:373-376, 597-625` (`EagerTxStream`)
- `kbsocket/connection/tx_sequence_window.cpp:48-50` (`TxSequenceWindow::Reserve`)
- `kbsocket/memory/slab_allocator.cpp:272-295` (`SlabAllocator::Acquire`)
- `kbsocket/api/raw_stream_runtime.cpp:63-65`

#### 2. 问题描述
1. **`promised_control != 0` 导致每 `PeerRuntime` 全局最多 1 个 RNDV 在途**：
   - `RndvConnection::WriteLease`（`rndv_connection.cpp:228`）在 `state.credits->stats().promised_control != 0` 时直接返回 `kWouldBlock`。
   - 因为 `SenderCreditAccount` 按 `PeerRuntime` 共享，且 `promised_control` 从 `QueueRts` 一直保持非零直至全部 `WRITE` 完成并激活发送 `DONE`（`rndv_notification_tx.cpp:300, 477`），导致**发往同一对端的所有连接在同一时刻只能串行执行 1 笔 RNDV**，`WriteRndvPool`（`tx_capacity = 16`）无法并发。
2. **Eager 与 RNDV 在发送端严格互斥**：
   - `EagerTxStream::can_start_rndv()`（`eager_tx.cpp:373-376`）要求 `queued == 0 && sequence.pending().empty()` 才允许发起 RNDV；反之 `Connection::Write`（`connection.cpp:482`）在 `rndv_.pending_sequence()` 时拒绝 Eager 写入。
3. **小控制帧浪费大块 RNDV Slab 槽位 & 单次 RNDV 受限于单个 `buffer_size`**：
   - 开启 RNDV 时 `stack.slab.buffer_size` 必须 $\ge \text{rndv\_threshold}$（如 16 KiB~1 MiB），而 48B `CREDIT_UPDATE`、64B `RTS`、104B `GRANT/DONE` 调用 `SlabAllocator::Acquire(bytes)` 时均独占一整块 `buffer_size` 槽位；
   - `GrantManager::Reserve`（`grant_manager.cpp:133`）与 `RndvConnection::Write`（`rndv_connection.cpp:248-273`）均受限于单个 Slab `buffer_size`，不支持跨多个定长 Slice 的分段流水线 RNDV。

#### 3. `ucx` 对标实现
- **短控制帧 `IBV_SEND_INLINE`**（`src/uct/ib/efa/srd/srd_ep.c:390-458`）：控制帧与小消息直接通过 `wr_inl` 内联发送，**0 注册内存槽位占用**。
- **Pipelined RNDV 与分级缓冲池**（`src/ucp/rndv/rndv_ppln.c:262-321`、`src/ucp/rndv/rndv_mtype.inl:100-169`）：将超大请求切分为定长 Fragment 流水化执行 `RTR -> PUT -> ATP`。

#### 4. 改进建议
1. **入队即分配 SN，解除 Eager/RNDV 互斥与 `promised_control` 单事务限制**：在 `Write` / `WriteLease` 入队时即从 `TxSequenceWindow` 分配确定序号；允许 `ReserveControl()` 在控制信用充裕时并发预留多笔 RNDV 的 `DONE` 额度。
2. **控制帧启用硬件 Inline 发送并拆分 Size-Classed Slabs**：对 $\le \text{inline\_bytes}$ 的纯控制帧（`CREDIT_UPDATE`、`RTS`、`GRANT`、`DONE`、`FIN`、`RST`）在 `TxSender` 中使用 `URMA_SEND_INLINE` 发送，免除 Slab 分配与 DMA 引用记账；将 4 KiB Eager Slab 与大块 RNDV Slab 拆池管理，并支持 Pipelined RNDV。

---

### ISSUE-6 [P2]：`kDataWithCredit` 捎带授信未启用，且 `ReservePublication` 限制每 Peer 单在途 `CREDIT_UPDATE`

#### 1. 现状代码定位
- `kbsocket/connection/connection.cpp:278-333` (`Connection::RefreshCredit`)
- `kbsocket/connection/eager_tx.cpp:387-416, 638-652, 855` (`EagerTxStream::QueueCredit` / `Post`)
- `kbsocket/connection/credit_account.cpp:550-622` (`ReceiveCreditDomain::ReservePublication`)

#### 2. 问题描述
1. **反向数据捎带授信（`kDataWithCredit`）在发送端未实现**：
   - 虽然线协议 `wire_frame.hpp:25` 和接收端 `connection.cpp:716` 定义并支持 `kDataWithCredit`，但 `Connection::RefreshCredit` 和 `EagerTxStream` 始终只发送独立的 48 字节 `kCreditUpdate` 控制帧，额外消耗控制信用、SQ WQE 和对端 JFR RQE。
2. **每 `PeerRuntime` 全局仅允许 1 个 `CREDIT_UPDATE` 在途**：
   - `ReceiveCreditDomain::ReservePublication`（`credit_account.cpp:550-574`）强制 `if (peer.publication) return Error(kInUse)`，直到该 `CREDIT_UPDATE` WR 的本地 TX CQE 返回调用 `CompletePublication`（`eager_tx.cpp:855`）才清零。
   - 当同一 Peer 下有多条连接并发通信时，某条连接正在发送 `CREDIT_UPDATE` 会阻塞该 Peer 下其他所有连接发送各自的 `max_frame_seq` / `max_stream_bytes` 窗口更新。

#### 3. 改进建议
1. **移除 `ReservePublication` 的单在途限制**：由于 `max_credit_seq`、`max_control_credit_seq`、`max_frame_seq`、`max_stream_bytes` 均为**单调递增的累计上限（接收端在 `SenderCreditAccount::ApplyUpdate` [`credit_account.cpp:166-206`] 中取单调最大值，天然幂等防乱序）**，无需限制同一时刻只能有 1 个 `CREDIT_UPDATE` WR 在途。
2. **在 `EagerTxStream::Post` 中启用 `kDataWithCredit` 捎带编码**：当连接有待发业务数据帧且存在待发布的信用/窗口更新时，优先编码为 `kDataWithCredit` 捎带发出，在双向 RPC 场景下可消除近 100% 的独立 `CREDIT_UPDATE` 控制帧。


#### 4. 核实与修复记录（2026-11-11）

- **结论：部分修复。** 单 Peer publication 门禁及未发送 `kDataWithCredit` 均已从源码证实；不能仅删除门禁而丢弃每个 WR 的完成责任。
- **本次修复：** `ReceiveCreditDomain` 使用域级预分配 publication 池，每个 WR 独立记录 Peer、槽位、代次和上限快照；多连接窗口更新可同时在途，乱序 CQE 分别确认，累计上限不倒退。池耗尽返回背压，零接受归还票据，未知接受保留责任；Peer 退役/域关闭等待全部在途票据。`EagerTxStream` 继续合并纯共账更新，但连接私有窗口更新不再被兄弟连接的 publication 阻塞。
- **验证：** `USE_BAZEL_VERSION=9.3.0 bazel test //kbsocket/connection:credit_account_test //kbsocket/connection:eager_tx_test --test_output=errors` 通过。覆盖独立快照与乱序完成、池耗尽/复用/旧票据、并发连接窗口、零接受/失败/未知接受及数据路径零分配。
- **原建议纠正：** `wire_frame.cpp` 中现有 `kDataWithCredit` 只附带 8 字节 `max_credit_seq`，不包含控制信用、连接帧/字节窗口和 `credit_epoch`。因此无法直接捎带完整 `CREDIT_UPDATE`，也不能据此推断能消除近 100% 独立控制帧。本次未修改线协议或启用有限的数据信用捎带，该部分保留待设计。
