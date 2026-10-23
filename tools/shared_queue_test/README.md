# 共享队列双向 SEND_WITH_IMM 硬件测试

`shared_queue_test` 验证 RM_CTP 下同一 SQ 逐 WR 切换远端 jetty、多个本地 jetty 共用 JFR 和 TX/RX JFC，以及双向收发时的完成归属。每端使用一个裸设备、一个 context、一个 JettyPool；`--server` 只决定 TCP 控制连接的监听角色，**双方都会发送和接收 UB 数据**。

本工具独立于 [基础 SEND](../send_test/README.md) 和 [TX 排空测试](../tx_drain_test/README.md)，使用不同的协议标识及默认端口，不能与其他工具配对。TCP 只交换端点、参数和同步标记，所有测试 payload 都经 URMA 传输。

## 验证流程

1. 每端创建 `jetties` 个本地 jetty，共用一个 JFR、一个 TX JFC 和一个 RX JFC；交换每个 jetty 的 EID 与 ID 并导入全部远端端点。
2. 每轮为每个 source jetty 准备至多 `batch` 条消息，先向共享 JFR 投递足够的接收 buffer，再通过 TCP 同步 READY。
3. 每个 source 的第 `sequence` 条消息发往 `target=(sequence+source)%jetties`，同一个 SQ 因而会轮流向全部目标发送。双方都提交 SEND_WITH_IMM，再交替 poll TX/RX。
4. 每条 TX 必须恰好退休一次。RX 校验接收 opcode、实际远端 EID/jetty ID、目标 jetty、方向、完整序号、长度、正文和 IMM；允许 CQE 乱序，不允许重复消息、错误归属或缺失。
5. 每轮完成后归还全部 RX lease；下一轮重新投递。最后一个不足 `batch` 的尾批只投递实际所需数量，避免正常结束留下未消费 RX WR。
6. 双方确认全部收发完成后分别关闭 session、runtime、动态库，再交换关闭确认，最后才打印 PASS。

IMM 按 `(sequence+source)%8` 依次取以下值，不把逻辑连接号或 SN 正式编码进 IMM：

```text
0x0000000000000000  0xffffffffffffffff
0x8000000000000000  0x0000000000000001
0xaaaaaaaaaaaaaaaa  0x5555555555555555
0x0123456789abcdef  0xfedcba9876543210
```

这些模式覆盖零值、高位、全 1 和交错位，避免仅测试小整数而遗漏高位截断。`imm_mask` 的 bit 0–7 表示实际收到过对应模式；`messages>=8` 时每个 source 都覆盖全部八种模式，两端应显示 `0xff`。更短的冒烟测试可以 PASS，但其 mask 不一定为 `0xff`。

每条 payload 前 32 字节是工具专用的网络字节序测试头，包含 magic、版本、source、target、64 位 sequence、方向和总长度；正文模式依赖这些字段。`--bytes` **包含这 32 字节**，不增加额外的片段头，不定义 kbsocket 正式的连接或分片协议。`bytes=32` 只校验头部，建议同时测试 4096 字节以覆盖正文与整个 RX 槽。

## 编译与运行

在 kbsocket 根目录执行：

```sh
bazel build //tools/shared_queue_test:shared_queue_test
```

先在机器 A 启动 TCP 监听端：

```sh
./bazel-bin/tools/shared_queue_test/shared_queue_test \
  --server --address=0.0.0.0 --port=18517 \
  --device=RAW_DEVICE_A --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --jetties=2 --messages=1001 --batch=32 --bytes=4096 --timeout_ms=60000
```

再在机器 B 启动另一端，替换 TCP IPv4 地址：

```sh
./bazel-bin/tools/shared_queue_test/shared_queue_test \
  --address=192.0.2.10 --port=18517 \
  --device=RAW_DEVICE_B --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --jetties=2 --messages=1001 --batch=32 --bytes=4096 --timeout_ms=60000
```

两端 `jetties/messages/batch/bytes` 必须相同，设备、EID 索引、库路径、超时可各自指定。每次测试重新启动两个进程。

| 参数 | 范围与默认值 |
|------|--------------|
| `jetties` | 1–16，默认 2 |
| `messages` | 每个本地 jetty 发送的消息数，至少为 `jetties`，默认 1024 |
| `batch` | 每轮每个 jetty 的消息数，1–256，默认 32 |
| `bytes` | 每条消息 32–4096 字节，默认 4096，还需符合设备 max_msg_size |
| `timeout_ms` | 1–3600000，默认 10000；分别限制每次控制操作和每轮数据完成等待 |
| `port` | 1–65535，默认 18517 |

设备还需支持以下资源规模；创建前会按真实能力检查，超出能力时降低 `jetties` 或 `batch`：

- 每个 SQ 的 TX 深度为 `batch`。
- 共享 JFR 的 RX 深度与 RX CQ 深度为 `jetties * batch`。
- TX CQ 深度为 `jetties * (batch + 1)`，为每个 SQ 保留 FLUSH_ERR_DONE 槽。
- 每端 RX 注册内存约为 `jetties * batch * 4096` 字节，TX 注册内存约为 `jetties * batch * bytes` 字节，实际按页对齐。

超时不能中断 provider 内同步阻塞的 API 调用。添加 `--urma_debug` 可启用 URMA debug 日志；常规 URMA 日志也会转发到 stderr。

## 建议硬件测试组合

| 组合 | jetties | messages | batch | bytes | 目的 |
|------|---------|----------|-------|-------|------|
| 单 SQ 基线 | 1 | 17 | 1 | 32 | 最小报文、全部 IMM 模式、双向收发 |
| 双 SQ 多目标 | 2 | 1001 | 32 | 4096 | 共享 JFR/CQ、逐 WR 切换目标、短尾批、RX 复用 |
| 四 SQ 多目标 | 4 | 1025 | 32 | 4096 | 全部 16 个 source-target 组合及更多完成交错 |

有余力时将第二组 `bytes` 改为 33、1024，覆盖非对齐和中等长度；在能力允许时增加 `batch=256`，检查大批量投递及跨多次 CQ poll 的处理。这里的“四 SQ”仍属于同一个设备/context，不等于四个裸设备同时运行。

每端总消息数应为 `jetties * messages`，例如第二组：

```text
Shared queue: accepted=2002 retired=2002 received=2002 imm_mask=0xff
Pair counts: source=sender jetty, target=receiver jetty; TX is local->peer, RX is peer->local
  source=0 target=0 tx=501 rx=501
  source=0 target=1 tx=500 rx=500
  source=1 target=0 tx=500 rx=500
  source=1 target=1 tx=501 rx=501
PASS: bidirectional shared queue SEND_WITH_IMM; payload, immediate and completion ownership validated; both endpoints closed resources
```

两端都 PASS 且退出码为 0 才算通过。退出码 2 是参数错误，1 是初始化、协议、收发、校验或清理失败。失败也会尽量输出已完成的计数；接受范围不可信时 `accepted=unknown`。如果不能证明 DMA 与对端访问已终结，会保留队列、注册内存及下层依赖到进程退出，不重放 WR，也不在错误路径强行释放内存。

用户已确认上述三组配置在真实硬件上运行，两端最后均输出 PASS。双 SQ 日志中两端 accepted=retired=received=2002、imm_mask=0xff，四组源—目标计数与示例一致。这是用户提供的运行结果，本地未重复硬件测试；共享队列的故障场景另见 [同池 SQ 故障隔离工具](../sq_isolation_test/README.md)。

异常完成会保留并打印首条 TX/RX CQE 的状态、标志和归属字段；RX 还打印实际 opcode、远端 EID/ID、原始 IMM，以及可解析 payload 的 source/target/sequence/方向和期望 IMM。`raw_imm` 仅在对应接收 opcode 下具有 IMM 语义，其他情况下只作为原始诊断信息。排查高位丢失或路由不匹配时，应一起保存这些行。

建议每次保存两端完整 stdout/stderr、退出码、运行命令、代码提交号及未提交差异，另记录设备名、EID 索引/EID、内核与驱动版本、liburma 路径及版本。之后没有硬件时，这些记录可帮助区分协议假设与实际观察。

## 无硬件测试与边界

```sh
bazel test //tools/shared_queue_test:protocol_test //tools/shared_queue_test:session_test
```

单测使用 socketpair 控制通道与 gMock URMA，覆盖协议、共享队列双向推进及异常完成处理，不依赖本机 liburma.so。

本工具验证的是同一对进程间、各自单 context 的正常数据通路。每轮按实际消息数预投递 RX buffer，**不覆盖接收额度不足后的恢复、provider EAGAIN/部分接受的真实硬件行为、独立 peer 故障及影响范围、四设备并行、RDMA READ/WRITE、JFCE 唤醒、连接重排和分片重组**。固定轮次的 TCP 同步也意味着它不适合衡量吞吐或延迟。相关能力需要独立测试，不能从本工具 PASS 推断。
