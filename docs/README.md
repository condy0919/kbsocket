# kbsocket 文档

梳理当前代码建议先读 [Raw 传输流程](raw-transport-flow.md)，再沿初始化、资源准备、数据路径逐层阅读。下列参考文档描述已实现的行为；长期设计中的目标不等于当前已有能力。

| 文档 | 内容 |
| --- | --- |
| [Raw 传输流程](raw-transport-flow.md) | 所有权、初始化、双端 SEND、退出和当前验证范围 |
| [URMA 加载与运行时](urma-runtime.md) | 动态加载、gMock 替换、context 校验、会话与进程退出 |
| [DeviceCatalog](device-catalog-reference.md) | 裸设备筛选、EID 快照、查找与指针生命周期 |
| [JettyPool](jetty-pool.md) | 两个 JFC、共享 JFR、多 jetty、配置与逐 WR 额度 |
| [TX 数据路径](tx-pipeline.md) | AttemptLedger、TxSender、TxCompletionProcessor 的配合与单 SQ 故障排空 |
| [日志](logging.md) | 日志封装及 gflags 配置 |
| [device_info 工具](../tools/capabilities/README.md) | 设备能力查看 |
| [双端 SEND 测试](../tools/send_test/README.md) | 编译、运行参数、结果判定与故障诊断 |

长期架构与协议规划见 [kbsocket 设计 v1](design/kbsocket-design-v1.md)。当前实现范围以本目录的流程和组件文档、对应源码为准。
