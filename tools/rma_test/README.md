# RM_CTP READ/WRITE 单 WR 硬件观察

本工具验证两类 SEND 测试无法覆盖的行为：注册并导入远端 MR 后，一条 READ/WRITE WR 能否搬运 4KiB 以上的数据；成功 TX CQE 后，目标 CPU 是否能读到完整数据。`--bytes` 是单条 WR 的实际长度，不进行 SEND 分片、不重放 WR，也不依赖生产 `TxSender` 的 SEND 路径。

服务端不投递 RX。READ 时它提供源 MR；WRITE 时它提供目标 MR。两端每轮都检查 payload 与前后保护区，且每轮 payload 不同，防止上一轮内容误通过。READ 的源 MR、WRITE 的源 MR 也检查保持不变。

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

默认每个控制或 CQ 操作超时 10 秒，整个进程有 `--watchdog_seconds=60` 截止，包括 provider 同步调用。增大 `--rounds` 或等待时间时应同步增大总截止，最大 3600 秒，且必须大于 `timeout_ms`。watchdog 以异步信号安全接口打印失败并 `_exit(124)`，跳过可能提前释放 MR 的 C++ 析构；它不证明内核资源已经完成回收，也无法保证立即打断内核不可中断等待。

## 输出与判据

启动时打印 `max_read_size`、`max_write_size`。工具按对应操作的上限判断，不使用 `max_msg_size` 限制 READ/WRITE。

- 客户端成功应为 `accepted=rounds retired=rounds verified=rounds pending=false`。
- 服务端没有 TX，故 `accepted=0 retired=0`，但 `verified=rounds`。
- 两端都应最终打印 `PASS`，且确认对端也完成关闭。
- `min_cqe_ns`/`max_cqe_ns` 从 post 调用前计时至软件取到 CQE，包含软件开销，不是硬件传输时延或吞吐 benchmark。
- 明确的设备能力不足、或导出段含不支持的 provider 扩展，打印 `SKIP` 并返回 77。此结果不是已验证通过；应检查两端能力后跳过这一尺寸。由于该检查在公布 MR 前执行，一端 SKIP 时另一端可能等到控制连接超时，应同步终止该组。
- post 返回错误、错误/伪 CQE、数量或身份不匹配、payload/保护区错误及超时均为 `FAIL`，不会被当成“不支持”掩盖。

## MR 和完成协议

1. 两端分配页对齐注册区，payload 前后留保护区，以已在现有工具使用的 `READ | WRITE | ATOMIC` 权限注册。
2. 通过 `GetSegCtx` 获取标准 raw 段描述符，及时 `PutSegCtx`；控制通道逐字段发送 EID、UASID、VA、长度、属性、token ID 和 payload 地址，不发送 C++/C 结构体内存布局。
3. 导入前检查地址加法不回绕、整个 payload 均位于段内、MR 与 Jetty 的 EID 一致、UASID 在 24 位范围、操作所需权限存在。仅支持 `TOKEN_NONE`，拒绝 LOCAL_ONLY、额外用户信息和地址重映射模式；token 值必须为零。导入权限只请求当前 READ 或 WRITE 所需位。
4. 双端准备当轮内存并交换 ready。客户端 post 一条 WR，READ 使用 remote src/local dst，WRITE 使用 local src/remote dst。
5. 客户端取到匹配的成功 TX CQE 并校验后通知服务端。服务端此时才检查目标/源 MR，并确认本轮完成，双方随后才能改写下一轮内容。
6. 最后一轮双方确认不再发送后，才 unimport 远端对象、删除本地队列、注销 MR、关闭 context。TCP EOF 不代替该确认。

任何无法确认的 DMA 或 peer 状态都保留整个注册内存、队列、context 和动态库依赖链至进程退出，不靠析构释放。这个工具验证的是有协作控制屏障的正向语义；它不证明未经同步的 CPU/DMA 并发访问安全，也不包含远端权限撤销、非法 rkey/token、原子操作、跨 WR 顺序保证或 READ 故障后的软件重放。

本地单测覆盖两种操作 × 四种尺寸的单 WR 路径、延迟 CQ 前双方不复用 buffer、协议边界、错误/重复/缺失 CQE、错误接受信息、payload/保护区损坏以及失败后保留 MR。真实尺寸和可见性结论必须来自硬件运行日志。
