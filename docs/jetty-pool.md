# JettyPool：共享队列与逐 WR 额度

[文档入口](README.md) · [当前代码流程](raw-transport-flow.md)

实现入口：[jetty_pool.hpp](../kbsocket/transport/raw/jetty_pool.hpp) 与 [jetty_pool.cpp](../kbsocket/transport/raw/jetty_pool.cpp)。

## 资源与创建

`JettyPool` 独占发送 JFC、接收 JFC、一个 JFR 和共享该 JFR 的多个 jetty。这里的共享指 URMA jetty 引用独立创建的 JFR，每池只有一个 JFR；不创建独立 JFS。`Open(ctx, cap, config)` 借用已打开的 context，`cap` 必须来自同一设备的目录快照。先检查 `RM_CTP`、队列深度、SGE 和 inline 限制，再创建资源，全部成功才允许通过 `Reserve()` / `jfr()` 借用。

使用 `//kbsocket/transport/raw:jetty_pool` Bazel 目标。配置默认发送/接收深度各 128，两个 JFC 深度各 256，SGE 各 1；不满足设备限制时显式失败，不自动裁剪。inline 为 0 遵循 URMA 的设备默认值语义。采用无 JFCE 的纯轮询模式，队列配置 `lock_free=1`，`non_blocking` 保持默认值 0；所有投递和轮询必须遵守同一 owner 约束。没有事件唤醒、远端导入或建链；RM_CTP 能力门禁不等于已完成 CTP 连接，远端导入时仍须指定正确的 TP 类型。JFR 暂用默认无 token 策略，后续建链认证需另行配置。

`PollSend` / `PollRecv` 接收调用方预分配的 `std::span<urma_cr_t>`，返回本批完成数量（0 为无完成），不分配内存或加锁。调用方继续检查每条完成的状态并回收对应请求，不能把轮询成功等同于每条 WR 成功。

创建失败自动逆序回滚，`JettyPoolError.failure` 保存原始错误，`cleanup_error` 保存清理失败。关闭顺序为 jetty → JFR → 接收 JFC → 发送 JFC；任一删除失败即停止，保留其依赖资源，再次 Close 只处理尚未释放部分。Open/Close 必须在使用者停止时执行，ready 标志不是并发停止协议。

池当前独立于 RawRuntime，宿主必须在关闭 context 前成功关闭所有借用它的池。退出时若 bRPC worker 仍在访问，整个池、已注册内存及 context 都必须常驻，例如由 NoDestructor 持有上层资源容器；不能仅让 RawRuntime 常驻而让池自动析构。析构只作受控作用域的兜底，失败会记录错误并保留底层依赖资源，不替代显式停止与清理。

## JettyPool 的共享 SQ 与逐 WR 额度

`jetty_count` 控制预创建数量。所有 jetty 共享 TX JFC 和 JFR/RX JFC，容量要求为 `jetty_count * (tx_depth + 1) <= tx_cq_depth`，另一个 CQE 用于每个 jetty 的 FLUSH_ERR_DONE 边界。池不在线扩容、不跨设备或 owner 迁移。

RM_CTP 每个 WR 自行指定 `tjetty`，连接不独占 jetty。`Reserve()` 轮转选择有容量的健康 SQ，为一个 WR 返回 `JettyTicket`；`ReserveOn(lane)` 可以在指定 SQ 上预留后续 WR，用于同队列批量投递。`Get(lane)` 返回物理队列，不代表独占权。同一 SQ 的多个票据可以属于不同连接和目标，同时处于在途状态。

票据遵循 `Reserve → Commit → Complete` 或 `Reserve → Cancel`。批量 post 部分成功时，仅 Commit 已接受前缀，Cancel 未接受后缀。预留已经扣除额度，确认不重复扣减；已提交票据不能 Cancel，未提交票据不能 Complete。票据序号防止迟到或重复完成误释放复用后的额度，lane epoch 是整个池的代次，仅在池成功 Open 时增加；当前没有单个 jetty 的在线重建。票据不跨越池对象销毁重建有效。所有票据槽在 Open 预分配，热路径不分配内存。

池当前不包装 post，不解析 CQE，也不拥有连接或 DMA buffer。调用方须在发送账本中关联票据、ConnId/OpId、目标和 buffer/grant 租约；批量 post、登记和 poll 不得重入。聚合完成须由上层识别实际终结的 WR 并逐条退休，不能按 CQE 条数释放额度，`FLUSH_ERR_DONE` 不对应用户票据。逐 WR 身份与状态校验由池和[AttemptLedger](tx-pipeline.md) 协作完成；账本仅记录逻辑 WR，不暴露 provider 的硬件队列布局。

`MarkFaulted(lane)` 阻止整个 SQ 的新预留及 Get，但允许确认先前已接受的 WR、取消未提交票据及退休终结 WR，不重放请求。只要仍有预留或在途票据，Close 返回 kInUse 并保持轮询可用；若已开始硬件排空，还必须消费 FLUSH_ERR_DONE 并确认软件队列为空，之后才按依赖逆序销毁。关闭连接不销毁共享队列，也不取消其已提交票据；共享 SQ 上其他连接可继续发送。绑定 [RxBufferPool](rx-buffer-pool.md) 后，须先终结全部 RX WR、归还 lease 并关闭 RX 池；仍有绑定时 JettyPool::Close 返回 kInUse。导入对象和发送 DMA 内存的关闭顺序仍由上层管理。

## 通过 gflags 设置队列深度

宿主在创建池前调用 `gflags::ParseCommandLineFlags`，可传入 `--kbsocket_tx_depth=128 --kbsocket_rx_depth=128`，两者默认均为 128。`tx_depth` 是每个 jetty 的发送 WR 深度，rx_depth 是整个池共享 JFR 的深度。

JettyPoolConfig 的 `tx_depth`/`rx_depth` 为可选配置，显式指定优先，否则在 Open 时读取 flags。已创建的池不会随 flag 修改而调整硬件或在途额度。零值、超过设备上限以及不满足 JFC 容量约束的配置，通过 Open 返回错误；库不自行解析参数或终止进程。`tx_cq_depth`/`rx_cq_depth` 保持独立显式配置，增大 WR 深度时须同步保证完成队列容量足够。

TX JFC 的额外容量用于每个 jetty 的 FLUSH_ERR_DONE 边界 CQE；用户 tx_depth、实际 JFS 深度和在途 WR 额度保持不变。加法和乘法使用 64 位计算，避免深度边界值溢出。

## 链路优先级和固定 SGE

`--kbsocket_link_priority=4` 控制创建 jetty 时的 `jfs_cfg.priority`，合法范围为 0–15，在 Open 时读取，已创建的 jetty 不随 flag 变化。非法值在创建任何硬件资源前返回参数错误。实际调度效果依赖 provider 和网络 QoS 配置。

远端 SGE（`jfs_cfg.max_rsge`）与接收 SGE（`jfr_cfg.max_sge`）固定为 1；Open 检查设备至少支持一个，不再暴露 `remote_sge`/`recv_sge` 配置。`rnr_retry=6`、`err_timeout=2` 保持当前配置；min_rnr_timer 使用 `URMA_TYPICAL_MIN_RNR_TIMER`（12），与当前 UMQ 的 19 不同，属于明确的重试等待策略选择，基础 SEND 已通过硬件测试，但 RNR 重试与超时策略尚未专项验证。

## ERROR 状态与排空入口

`error_suspend` 保持默认值 0，采用 error continue。`MarkFaulted` 只禁止本地发送；`BeginDrain` 在隔离后主动 modify 为 ERROR，成功后幂等，失败保留隔离并允许重试。`lane_state` 区分 kReady、kFaulted、kError（等待边界）、kFlushReady、kDrained，只有 kReady 能预留与获取发送句柄。

完成层消费 FLUSH_ERR_DONE 后调用 ObserveFlushDone，再通过 FlushSend 取得软件残留 WR；正常调用方使用封装了身份校验与账本退休的 `TxCompletionProcessor::Flush`。ERROR 状态下 post 可能仍返回成功，因此不得缓存 Get 的裸指针后绕过池状态继续投递。排空只终结在途请求，不恢复 jetty 可用性。

完整顺序、错误语义与验证范围见 [TX 故障排空](tx-pipeline.md#error-continue-与单-sq-故障排空)。
