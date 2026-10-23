# RM_CTP READ/WRITE、混合 SEND 与 WRITE 通知可见性

本工具验证两类 SEND 测试无法覆盖的行为：注册并导入远端 MR 后，一条 READ/WRITE WR 能否搬运 4KiB 以上的数据；成功 TX CQE 后，目标 CPU 是否能读到完整数据。`--bytes` 是单条 WR 的实际长度，不进行 SEND 分片、不重放 WR，也不依赖生产 `TxSender` 的 SEND 路径。

默认 `--window=1 --mixed_send=false --write_notify=false` 保留串行单 WR 行为，服务端不投递 RX。READ 时它提供源 MR；WRITE 时它提供目标 MR。`--window` 控制每轮独立 RMA WR 数；`--mixed_send` 仅用于 READ，在同一 SQ 上按 `READ0 → SEND0 → READ1 → SEND1 ...` 一次性提交一个链表，每条 SEND_WITH_IMM 为 64 字节。WRITE 默认仍为 `window=1`；新增 `--write_notify` 允许并发 WRITE，等待整批 WRITE 成功完成后再单独提交通知批次。两种 SEND 模式互斥，服务端都通过生产 `RxBufferPool` 接收小包。

每个并发槽都有独立的 RMA payload、前后保护区和（启用 SEND 时）通知源区。客户端按 WR identity 处理乱序 CQE，服务端按 IMM 核对小包身份。WRITE 通知的数据与通知前 16 字节还携带完整轮次、槽位和长度，避免仅用周期性字节 pattern 误判旧轮数据。两端每轮验证全部 RMA payload、保护区与源区未被改写，再复用这些槽。

## 编译和无硬件测试

```bash
bazel build //tools/rma_test:rma_test
bazel test //tools/rma_test:protocol_test //tools/rma_test:session_test --test_output=errors
```

通过 `dlopen` 加载 URMA，本机没有 `liburma.so` 仍可编译和执行 gMock 单测。

## 双端运行

先启动服务端：

```bash
bazel-bin/tools/rma_test/rma_test \
  --server --address=0.0.0.0 --port=8894 \
  --device=udmac0d1e4 --eid_index=38 \
  --operation=read --bytes=4096 --rounds=8
```

再启动客户端：

```bash
bazel-bin/tools/rma_test/rma_test \
  --address=141.62.33.25 --port=8894 \
  --device=udmac0d1e4 --eid_index=60 \
  --operation=read --bytes=4096 --rounds=8
```

将两端的 `operation`、`bytes` 同步修改，依次执行以下八组。每组重新启动两端进程；设备和 EID index 按实际配置替换。

| operation | bytes | 观察点 |
| --- | ---: | --- |
| read | 4096 | READ 基础链路与完成后本地可见性 |
| read | 16384 | 单 WR 跨越 SEND 的 4KiB 尺寸 |
| read | 1048576 | 1MiB READ |
| read | 8388608 | 8MiB READ，设备能力允许时执行 |
| write | 4096 | WRITE 基础链路与远端 CPU 校验 |
| write | 16384 | 16KiB WRITE，无 RX buffer |
| write | 1048576 | 1MiB WRITE |
| write | 8388608 | 8MiB WRITE，设备能力允许时执行 |

## 并发 READ 和混合 SEND

两端必须使用本次编译的协议 v3 程序，且 `operation/bytes/rounds/window/mixed_send/write_notify` 完全一致；旧版本会被拒绝。`window=1..128`，每端 `bytes * window <=256MiB`（保护区、页对齐空间与 RX slab 另计）。最大 READ 混合批次为 128 READ + 128 SEND，共 256 WR。

先验证并发 READ（两端分别执行）：

```bash
./rma_test --server --address=0.0.0.0 --port=8894 \
  --device=udmac0d1e4 --eid_index=39 \
  --operation=read --bytes=16384 --rounds=8 --window=32

./rma_test --address=141.62.33.25 --port=8894 \
  --device=udmac0d1e4 --eid_index=51 \
  --operation=read --bytes=16384 --rounds=8 --window=32
```

再验证同 SQ 的 READ/SEND 混合推进：

```bash
./rma_test --server --address=0.0.0.0 --port=8894 \
  --device=udmac0d1e4 --eid_index=39 \
  --operation=read --bytes=16384 --rounds=8 --window=32 --mixed_send

./rma_test --address=141.62.33.25 --port=8894 \
  --device=udmac0d1e4 --eid_index=51 \
  --operation=read --bytes=16384 --rounds=8 --window=32 --mixed_send
```

然后将两端参数同步替换为下表。每组重新启动双端，EID 按本机配置替换。

| bytes | window | mixed_send | 每轮观察内容 |
| ---: | ---: | --- | --- |
| 16384 | 32 | false | 32 个独立 READ 同批提交 |
| 1048576 | 32 | false | 32MiB READ 批次，跨 slot 数据校验 |
| 16384 | 32 | true | 32 READ 与 32 SEND 同 SQ 交错 |
| 16384 | 128 | true | 256 WR 链及多个 CQ poll 批次 |
| 1048576 | 32 | true | 大 READ 与小 SEND 混合推进 |

以上 READ 参数已有独立的[硬件记录](../../docs/hardware-validation-matrix.md)，不能仅沿用串行 READ 的 PASS。链表同批提交不保证硬件确实同时执行全部 WR；CQE 顺序或小包到达顺序也不证明跨操作顺序、fence 语义或无队头阻塞。`--mixed_send` 的 SEND 是独立流量，不是宣告前面的 READ/WRITE 数据可见的发布消息。

## WRITE 后发送 SEND_WITH_IMM 通知

此模式观察的顺序是：客户端提交本轮 WRITE → 消费全部成功 WRITE CQE → 同一 SQ 提交每槽一个 SEND_WITH_IMM → 服务端消费每条通知 CQE 时立即读取对应目的槽并验证 → 双端完成确认后才复用。服务端不等待 TCP 完成标记再读取目的数据；TCP 的 ready 用于准备注册区和 RX，后续确认仅授权校验后的槽位复用及最终关闭。

```bash
# 服务端
./rma_test --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=39 \
  --operation=write --write_notify --bytes=16384 --window=32 --rounds=8

# 客户端
./rma_test --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=52 \
  --operation=write --write_notify --bytes=16384 --window=32 --rounds=8
```

两端同步替换以下参数；均为待真机观察的新组，不继承已有 WRITE 或 READ/SEND 的通过记录。

| 组 | bytes | window | rounds | 观察点 |
| --- | ---: | ---: | ---: | --- |
| WN1 | 16384 | 1 | 8 | 单槽通知时可见性与跨轮复用 |
| WN2 | 16384 | 32 | 8 | 32 槽独立通知、身份与目的数据校验 |
| WN3 | 16384 | 128 | 8 | 跨 CQ poll 批次的 WRITE 完成与通知 |
| WN4 | 1048576 | 32 | 8 | 大 payload 的远端通知时可见性 |

`--write_notify` 要求 `operation=write`、`bytes>=16`，不能与 `--mixed_send` 同用。通知包括完整轮次、slot、bytes，IMM 为 `round*window+slot+1`；检查来源 jetty/EID、opcode、长度、重复或陈旧身份。每轮目的区先写 poison，收到有效通知后立即校验对应 slot 和 guards；校验失败不会等后续 TCP 消息再重试读到成功。已取得的 RX lease 在失败路径也逐个释放，未决 DMA 依赖仍保留。

只有本轮全部 WRITE 接受且完成成功，才允许发通知；部分接受、未知接受边界或任意错误 CQE（含 status 9）都终止实验，不发送本轮通知或重放请求。通知批次也逐 WR 退休，拒绝、错误、丢失通知均失败。通过仅支持“等待 WRITE 成功完成后再通知，远端消费通知时可读取”的路径；不覆盖不等完成的 WRITE→SEND 链式提交、跨 SQ 顺序或任意平台的内存一致性保证。READ 后通知的完成点在数据消费者本地，不能直接替代这一远端观察。

默认每个控制或 CQ 操作超时 10 秒，整个进程有 `--watchdog_seconds=60` 截止，包括 provider 同步调用。增大 `--rounds` 或等待时间时应同步增大总截止，最大 3600 秒，且必须大于 `timeout_ms`。watchdog 以异步信号安全接口打印失败并 `_exit(124)`，跳过可能提前释放 MR 的 C++ 析构；它不证明内核资源已经完成回收，也无法保证立即打断内核不可中断等待。

## 输出与判据

启动时打印 `max_read_size`、`max_write_size`。工具按对应操作的上限判断，不使用 `max_msg_size` 限制 READ/WRITE；启用 SEND 时另检查设备能发送 64 字节通知。

- 记 `N=rounds*window`。纯 RMA 客户端成功应为 `accepted=retired=verified=N pending=false accepted_known=true`。
- READ 混合模式及 WRITE 通知模式的客户端应为 `accepted=retired=2*N verified=N send_accepted=send_retired=N`；服务端应为 `send_received=N`。
- WRITE 通知模式额外打印 `WRITE visibility`：服务端 `notify_received=visibility_verified=rx_leases_released=N`，客户端 `notify_accepted=notify_retired=N`。客户端没有接收通知，因此其 `visibility_verified=rx_leases_released=0` 正常。
- 服务端没有 TX，故 `accepted=0 retired=0`，各模式都应有 `verified=N`；WRITE 通知模式的该数在通知处理时累计，不在 TCP 完成之后重复计数。
- 成功路径 `partial_batches=0`。provider 部分接受时，已知前缀按 identity 退休后整次测试 FAIL；拒绝的后缀不重发。接受边界未知时 `accepted_known=false pending=true`，已知 accepted 数字不能代表实际接受总数。
- 两端都应最终打印 `PASS`，且确认对端也完成关闭。
- `min_cqe_ns`/`max_cqe_ns` 从 post 调用前计时至软件取到 CQE，包含软件开销，不是硬件传输时延或吞吐 benchmark。
- 明确的设备能力不足、或导出段含不支持的 provider 扩展，打印 `SKIP` 并返回 77。此结果不是已验证通过；应检查两端能力后跳过这一尺寸。由于该检查在公布 MR 前执行，一端 SKIP 时另一端可能等到控制连接超时，应同步终止该组。
- post 返回错误、错误/伪 CQE、数量或身份不匹配、payload/保护区错误及超时均为 `FAIL`，不会被当成“不支持”掩盖。

## MR 和完成协议

1. 两端分配页对齐注册区，payload 前后留保护区，以已在现有工具使用的 `READ | WRITE | ATOMIC` 权限注册。
2. 通过 `GetSegCtx` 获取标准 raw 段描述符，及时 `PutSegCtx`；控制通道逐字段发送 EID、UASID、VA、长度、属性、token ID 和 payload 地址，不发送 C++/C 结构体内存布局。
3. 导入前检查地址加法不回绕、所有并发槽的整个 payload 均位于段内、MR 与 Jetty 的 EID 一致、UASID 在 24 位范围、操作所需权限存在。仅支持 `TOKEN_NONE`，拒绝 LOCAL_ONLY、额外用户信息和地址重映射模式；token 值必须为零。导入权限只请求当前 READ 或 WRITE 所需位。
4. 双端准备当轮所有槽并交换 ready；启用 SEND 时服务端先按 window 精确补投 RX。READ 使用 remote src/local dst，WRITE 使用 local src/remote dst。WRITE 通知模式分成 WRITE 和通知两个提交/退休阶段，其他模式一次 post 整个 WR 链。
5. 默认及 READ 混合模式在整批成功 TX CQE 后通过 TCP 完成标记授权服务端检查目标/源 MR。WRITE 通知模式按上节流程，在每条 SEND_WITH_IMM 接收完成时就检查对应目的 MR，完成标记只在验证之后消费。双方必须等本地 TX 终结和远端验证确认，再改写下一轮内容。
6. 最后一轮双方确认不再发送后，才 unimport 远端对象、删除本地队列、注销 MR、关闭 context。TCP EOF 不代替该确认。

任何无法确认的 DMA 或 peer 状态都保留整个注册内存、队列、context 和动态库依赖链至进程退出，不靠析构释放。这个工具验证的是有协作控制屏障的正向语义；它不证明未经同步的 CPU/DMA 并发访问安全，也不包含远端权限撤销、非法 rkey/token、原子操作、跨 WR 顺序保证或 READ 故障后的软件重放。

本地单测覆盖两种操作 × 四种尺寸的原单 WR 路径、最大 128 槽/256 WR 的逆序 TX/RX 完成、跨 poll 批次、延迟 CQ 前双方不复用 buffer、已接受前缀退休及不重放、同批错误不丢弃后续终结、未知状态与接受边界保留 MR、槽间 guard 损坏、RX 远端身份/IMM/payload 校验，以及协议版本/参数/范围/内存上限。WRITE 通知还检查写完成前不得通知、通知到达时数据未就绪立即失败、旧轮身份拒绝及错误 WRITE 不进入通知阶段。真实尺寸和可见性结论必须来自硬件运行日志。
