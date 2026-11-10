# kbsocket 文档

梳理当前代码建议先读 [Raw 传输流程](raw-transport-flow.md)，再沿初始化、资源准备、数据路径逐层阅读。组件文档描述当前实现，观测文档记录证据；roadmap 和长期设计中的目标不等于当前已有能力。

| 文档 | 内容 |
| --- | --- |
| [实施 roadmap](roadmap.md) | M0 契约修订、M1–M7 交付依赖与验收，SHM/观测支线和当前实现缺口 |
| [Raw 传输流程](raw-transport-flow.md) | 所有权、初始化、双端 SEND、退出和当前验证范围 |
| [RuntimeShard 进度器](runtime-shard.md) | 单 owner 四 context、分类预算、通知/async、水线待处理、有界控制执行器与借用资源生命周期 |
| [注册内存与 Lease](memory-leases.md) | 多 context Slab、独立 CPU/DMA/Grant 责任、有界跨线程归还及隔离回收 |
| [连接协议原语](connection-primitives.md) | IMM 短/扩展帧、身份代次、发送序号、乱序交付与应用驻留、数据/控制 Credit 守恒 |
| [copy-stream C 接口](stream-api.md) | owner 上的 Eager/NativeTCP 有界读写、稳定错误、暂停门禁与关闭/释放生命周期 |
| [连接建链与导入缓存](connection-handshake.md) | 非阻塞 HELLO/READY、完整身份路由、Single-Flight 导入、首包与 fd 交接门禁 |
| [WRITE 分块引擎](write-rndv-engine.md) | 有界多 SGE 分块、独立源 DMA 引用、可信逐 WR 完成与 DONE 证据 |
| [WRITE 事务池](write-rndv-pool.md) | 可复用事务槽、导入缓存 pin、排队源 StartPinned 与 SN/Grant 防重放 |
| [RNDV 线格式](rndv-protocol.md) | RTS/GRANT/DONE 身份、实际来源校验、共同 Eager/RNDV 序号与通知证据 |
| [RNDV 通知发送](rndv-notification-tx.md) | 实际 SEND_WITH_IMM、未来 DONE 控制额度与独立通知责任 |
| [RNDV 接收协调](rndv-receiver.md) | 可信 DONE、目标 Grant 终结、共同重排与只读借用归还 |
| [RNDV 能力协商](rndv-negotiation.md) | HELLO/READY 限额、Arena 目录、认证门禁与段导入请求适配 |
| [RNDV 接收分发](rndv-rx-ingress.md) | RTS/GRANT/DONE 分发、RX 背压、GRANT job 与 Eager/RNDV 混合读取 |
| [RNDV 发送事务协调](rndv-tx-coordinator.md) | 源排队 pin、RTS/GRANT、按需导入、WRITE 到真实 DONE 完成及宿主进度边界 |
| [硬件观测记录](hardware-observations.md) | 用户实测的 SEND/RMA、水线重配、错误事件与隔离、资源回收及验证边界 |
| [硬件验证矩阵](hardware-validation-matrix.md) | 残留 RX、provider 边界、进程/链路故障、持续多设备收发、JFC 通知与 READ/WRITE 的完整运行顺序 |
| [URMA 加载与运行时](urma-runtime.md) | 动态加载、gMock 替换、context 校验、会话与进程退出 |
| [DeviceCatalog](device-catalog-reference.md) | 裸设备筛选、EID 快照、查找与指针生命周期 |
| [JettyPool](jetty-pool.md) | 两个 JFC、共享 JFR、多 jetty、配置与逐 WR 额度 |
| [TX 数据路径](tx-pipeline.md) | AttemptLedger、TxSender、TxCompletionProcessor 的配合与单 SQ 故障排空 |
| [RX buffer 池](rx-buffer-pool.md) | 4 KiB 注册内存、共享 JFR 投递、完成 lease 与归还 |
| [日志](logging.md) | 日志封装及 gflags 配置 |
| [device_info 工具](../tools/capabilities/README.md) | 设备能力查看 |
| [双端 SEND 测试](../tools/send_test/README.md) | 单/多 SGE、分散注册的 header 与 payload、IMM、guard 与完成校验 |
| [共享队列双向测试](../tools/shared_queue_test/README.md) | SEND_WITH_IMM、多 jetty 共享 JFR/CQ、逐 WR 切换目标与双向完成归属 |
| [TX 故障排空测试](../tools/tx_drain_test/README.md) | 独立双端工具，验证主动 ERROR、硬件边界、软件 flush 和关闭 |
| [同池 SQ 故障隔离测试](../tools/sq_isolation_test/README.md) | 一条 SQ 排空时，共享 JFR/JFC 的健康 SQ 持续双向收发 |
| [背压与 RNR 续发测试](../tools/backpressure_test/README.md) | 本地 TX 配额、延迟投递 RX、RNR 后同一条 SQ 发送新消息 |
| [同 SQ 多目标观测](../tools/sq_multiplex_test/README.md) | RNR 原始观测，以及 status 8 经正式 TX 完成层后的健康目标隔离与续发 |
| [共享 JFR 关闭观测](../tools/jfr_lifecycle_test/README.md) | 未消费或部分消费 RX 的真实删除、耗时与 watchdog |
| [Provider 提交边界](../tools/provider_boundary_test/README.md) | 容量拒绝、部分接受、非零软件 flush 和 ERROR SQ 原始 post |
| [真实对端故障](../tools/peer_failure_test/README.md) | 进程退出证明、原始 CQE/SQ 状态与异步事件、现场外部故障窗口 |
| [持续收发与事件](../tools/streaming_test/README.md) | 无批间 TCP 屏障、多 context、RX 补投及 JFCE 等待/ack |
| [JFR 水线异步事件](../tools/async_event_test/README.md) | 独立 async fd、事件归属与 Get/Ack、多轮重新使能及未停发时的高/低位重配 |
| [JFC/JFR 错误观测](../tools/async_error_test/README.md) | CQ 溢出候选、外部注入窗口、关联 jetty AE、独立队列隔离和终结证据 |
| [READ/WRITE 测试](../tools/rma_test/README.md) | 串行 READ/WRITE、并发 READ 与同 SQ SEND 混合、双方数据校验 |
| [WRITE 故障隔离测试](../tools/rma_failure_test/README.md) | 确认远端进程退出后，同 SQ 混合写入失效和健康目标、逐 WR 终结与远端数据校验 |

长期架构与协议规划见 [kbsocket 设计 v1](design/kbsocket-design-v1.md)。当前实现范围以本目录的流程和组件文档、对应源码为准。
