# Provider 提交边界观测

这个双端工具补充软件 quota 测试无法覆盖的三个硬件边界：真实 provider 容量拒绝与 `bad_wr` 前缀、主动切 ERROR 后非零 `WR_UNHANDLED`、ERROR SQ 上直接 post 的返回与终结行为。仅实验代码绕过 `JettyPool` 的额度和故障门禁，生产发送不能照搬。

服务端只创建空 JFR，始终不投 RX。客户端连续直接 post、不 poll，达到首个 provider 拒绝或有限 `attempts` 上限后停止。每个 WR 使用唯一 `user_ctx`，共享的只读 4 KiB MR 在全部终结之前保持不变；CQ 深度为 `attempts + 2`，避免把 CQ 溢出误认为 SQ 背压。

随后客户端主动 `ModifyJetty(ERROR)`，先消费硬件完成与 `FLUSH_ERR_DONE`，再调用 `FlushJetty`。每次 flush 数量不超过 `tx_depth`，硬件伪 CQE 的 `user_ctx` 从不用于查账。所有已接受 WR 必须逐条终结一次，软件返回零不替代完整的账本检查。

```sh
USE_BAZEL_VERSION=9.2.0 bazel build //tools/provider_boundary_test:provider_boundary_test
```

先在服务端启动，再在客户端启动。设备/EID 根据本机修改，两端 `tx_depth`、`batch`、`attempts`、`error_post`、`timeout_ms` 必须一致。

```sh
# 服务端
bazel-bin/tools/provider_boundary_test/provider_boundary_test \
  --server --device=udmac0d1e4 --eid_index=38 --address=0.0.0.0 --port=18540 \
  --tx_depth=32 --batch=33 --attempts=1024

# 客户端
bazel-bin/tools/provider_boundary_test/provider_boundary_test \
  --device=udmac0d1e4 --eid_index=60 --address=141.62.33.25 --port=18540 \
  --tx_depth=32 --batch=33 --attempts=1024
```

建议运行矩阵：

| tx_depth | batch | attempts | error_post | 目的 |
|---:|---:|---:|:---:|---|
| 1 | 2 | 64 | false | 小 SQ、最小 flush 上限 |
| 32 | 33 | 1024 | false | 跨越深度边界，尝试触发部分接受 |
| 256 | 255 | 4096 | false | 连续批次填满与较多未处理 WR |
| 32 | 33 | 1024 | true | 正常排空后，额外向 ERROR SQ 直接 post 一次 |

`attempts + 2` 必须不超过设备的 `max_jfc_depth`，不满足时减小 attempts，仍须大于 tx_depth。provider 可能将 SQ 深度取整，batch 大于请求深度不保证部分接受；硬件可能在软件切 ERROR 之前处理完全部请求，因而不保证产生 `WR_UNHANDLED`。工具会分别打印：

- `OBSERVED/NOT_OBSERVED: provider_capacity_rejection`：只计实际 provider `EAGAIN/ENOMEM`，不计软件 quota。
- `OBSERVED/NOT_OBSERVED: provider_partial_acceptance`：以合法 `bad_wr` 确认接受前缀。未知指针、失败却无 bad_wr、SUCCESS 却存在 bad_wr 都是失败并保留资源。
- `OBSERVED/NOT_OBSERVED: nonzero_WR_UNHANDLED`：只有真实软件 flush 返回才算观察到，不能拿普通 `FLUSH_ERR` 替代。
- `OBSERVED: direct_ERROR_post status=... terminal_pending=...`：额外试验只 post 一次，不重放。若 post 返回 SUCCESS 却在 timeout 内没有终结，两端打印 `OBSERVED ... retain resources until process exit`，保留完整依赖链到进程退出，不能打印安全关闭 PASS。这是独立观测结果，退出码为 0；无论 SUCCESS 还是拒绝均按实记录，不预设硬件结论。

正常模式的最终 `PASS` 仅表示已接受 WR 全部终结且双方安全关闭；其中 `NOT_OBSERVED` 项仍未验证。普通排空超时、伪 CQE 处理错误、重复完成、未知接受以及控制通道失败打印 `FAIL` 并保留未决资源。不要把 ERROR post 试验的保留路径用于生产 shutdown。

这些结果不分析 ACK_TIMEOUT 的根因，也不证明链路故障隔离、端到端消息可靠性或硬件严格执行请求深度。

整个实验在加载 URMA 前 fork 出的独立子进程中运行。父进程的 `--watchdog_ms` 覆盖初始化、控制协议、poll/flush 和同步 provider 删除调用；默认 0 表示 `6 * timeout_ms + 10000` 毫秒。超时只向本次子进程发送 SIGKILL，确认回收后退出码为 124 并打印 FAIL，不代表 provider shutdown 成功；若内核不可中断等待导致一秒内仍无法回收，会明确报告进程清理未决。父进程异常退出也会通过 PDEATHSIG 终止子进程。
