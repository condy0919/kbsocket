# 共享 JFR 残留 RX 的关闭观察

本工具补足生产 `RxBufferPool::Close()` 有意拒绝的场景：共享 JFR 上仍有 posted RX 时，先删除两个依赖它的 Jetty，再实际调用 `urma_delete_jfr()`，记录返回码与耗时。只有删除成功，才注销 MR、删除 CQ、关闭 context 和动态库。

这是独立硬件实验，不改变生产 RX 池的关闭约束，也不把 `ModifyJfr(ERROR)`、删除某一个 Jetty 或 TCP 断开当作 DMA 终结证明。成功删除 JFR 不要求为残留的每个 RX 生成可见 CQE；观察的是当前 provider 的资源回收行为，不能单凭一次成功推导所有 provider 的通用 DMA 保证。

## 编译与本地测试

```bash
bazel build //tools/jfr_lifecycle_test:jfr_lifecycle_test
bazel test //tools/jfr_lifecycle_test:session_test --test_output=errors
```

本机没有 `liburma.so` 也能编译、运行 gMock 测试。硬件程序通过 `--library` 动态加载。

## 第一组：全部 RX 尚未使用

只需一端，不需要 TCP 对端或硬件 loopback。本组不向任何对端公布 endpoint。

```bash
bazel-bin/tools/jfr_lifecycle_test/jfr_lifecycle_test \
  --device=udmac0d1e4 --eid_index=38 --rx_depth=1 --consumed=0

bazel-bin/tools/jfr_lifecycle_test/jfr_lifecycle_test \
  --device=udmac0d1e4 --eid_index=38 --rx_depth=32 --consumed=0

bazel-bin/tools/jfr_lifecycle_test/jfr_lifecycle_test \
  --device=udmac0d1e4 --eid_index=38 --rx_depth=256 --consumed=0
```

预期观察 `posted=rx_depth received=0`，然后依次打印两个 `DeleteJetty` 和 `DeleteJfr begin/end`。最终 `PASS` 中的 `residual_rx` 必须等于 `rx_depth`。本工具没有先 flush/poll 完 RX 来绕过残留条件。

## 第二组：部分 RX 已接收，仍有残留

服务端创建共享 JFR 并投递 `rx_depth` 个 4096 字节 buffer；客户端发送 `consumed` 条 SEND。客户端必须收齐成功 TX CQE，服务端必须收齐并校验 payload，双方确认不再发送后才开始关闭。

服务端：

```bash
bazel-bin/tools/jfr_lifecycle_test/jfr_lifecycle_test \
  --server --address=0.0.0.0 --port=8893 \
  --device=udmac0d1e4 --eid_index=38 --rx_depth=32 --consumed=7
```

客户端：

```bash
bazel-bin/tools/jfr_lifecycle_test/jfr_lifecycle_test \
  --address=141.62.33.25 --port=8893 \
  --device=udmac0d1e4 --eid_index=60 --rx_depth=32 --consumed=7
```

两端的 `rx_depth`、`consumed` 必须一致。服务端应显示 `posted=32 received=7 residual=25`；客户端应显示 `tx_retired=7`。两端都完成资源关闭确认后才打印最终 `PASS`。再将两端参数改为 `--rx_depth=256 --consumed=255`，覆盖只剩最后一个 posted RX 的关闭。

## 超时与结果解释

每个硬件进程都先由父进程 `fork()`，随后子进程才加载和初始化 URMA，避免继承 provider 工作线程的状态。默认 `--timeout_ms=10000` 限制单次控制或 CQ 等待，`--watchdog_ms=60000` 限制整个子进程，包括 provider 内部可能阻塞的删除调用。启动双端时，客户端也应在服务端 10 秒的连接超时内启动。

- `PASS`：完成本组前置流量，依赖 Jetty 与残留 JFR 均删除成功，随后才释放 MR/CQ/context；双端模式还确认对端关闭成功。
- `FAIL: delete JFR ...`：记录了实际删除失败；不再次使用或删除这个 JFR，不释放其余 MR/CQ/context，完整链留至子进程退出。
- `DeleteJfr begin` 后无 `end`，最终 watchdog 超时：删除没有在实验总期限内返回。父进程向自己的子进程发送 SIGKILL 并回收它；这不是成功删除的证明。
- SIGKILL 后仍无法在额外 1 秒内回收：可能仍在不可中断内核等待，打印 `kernel cleanup unresolved` 并失败返回，不能宣称资源已清理。
- 流量超时、错误 CQE、部分 post 接受或控制同步失败：未满足观察前提，保留资源并 `FAIL`，不把本轮当作残留 JFR 删除结论。

保留 `DeleteJetty`/`DeleteJfr` 的返回码、耗时、残留数量、完整 `PASS`/`FAIL`，并记录内核、liburma 和 provider 版本。无硬件环境的单测只验证资源顺序、失败保留、双端协议/payload 和 watchdog，不能替代这些实测。
