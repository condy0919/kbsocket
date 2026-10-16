# 单 SQ TX 故障排空硬件测试

`tx_drain_test` 是独立的双端工具，用来验证主动切 ERROR 后的发送门禁、硬件边界、软件 flush 和资源释放。基础收发与 payload 校验仍使用 [send_test](../send_test/README.md)。完整契约见 [TX 故障排空](../../docs/tx-pipeline.md#error-continue-与单-sq-故障排空)。

## 测试方式

两端各创建一个 raw RM_CTP jetty。`--server` 一端提供有效的远端端点，**不投递接收 WR，也不注册接收内存**；发送端提交一批普通、非 inline、全 signal SEND，然后不经 poll，立即调用 BeginDrain 切 ERROR。

不投递 RQ 是有意设置的故障条件：它使发送可能等待接收额度，也避免测试结束时留下数量不确定的 RX WR，把 TX 排空测试变成另一个接收排空问题。这不是正常数据收发验证，不保证出现 SUCCESS，也不证明远端收到 payload；运行前应先通过基础 SEND 测试确认设备与组网配置可用。

工具执行以下检查：

1. 整批 SEND 成功提交，每条 WR 有独立 AttemptId。
2. 切 ERROR 后再次调用 Sender，必须在 SQ 预留阶段返回 kFaulted，不能进入 provider post，也不能改变在途账本。
3. 持续 poll，逐条统计硬件完成，直到恰好一个 FLUSH_ERR_DONE；伪 CQE 的 user_ctx 不参与记账。
4. 在硬件边界后调用软件 Flush，按 WR_UNHANDLED 逐条退休，直到返回 0 且该 SQ 的账本和票据均为空。
5. 每个提交的 AttemptId 恰好退休一次。收到边界后、彻底排空后再次检查发送门禁，共三次；排空不会恢复 jetty 可用性。
6. 对端确认发送端完成排空，然后双方分别关闭 session、runtime 和动态库。双方交换关闭确认后才各自输出 PASS。

TCP 仅用于端点交换和同步，使用与 SEND 工具不同的协议标识与默认端口；不能将两个不同工具配对。测试不要求 FLUSH_ERR、WR_UNHANDLED 都出现，数量取决于切 ERROR 时硬件已经推进到哪里。无接收 WR 时还可能出现 RNR 等普通错误，它们作为合法终结完成分别计数，未单列的错误计入 other_error。

## 编译与运行

在 kbsocket 根目录执行：

```sh
bazel build //tools/tx_drain_test:tx_drain_test
```

先在机器 A 启动对端：

```sh
./bazel-bin/tools/tx_drain_test/tx_drain_test \
  --server --address=0.0.0.0 --port=18516 \
  --device=RAW_DEVICE_A --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --bytes=4096 --batch=32 --timeout_ms=60000
```

在机器 B 启动发送与排空端，替换 TCP IPv4 地址：

```sh
./bazel-bin/tools/tx_drain_test/tx_drain_test \
  --address=192.0.2.10 --port=18516 \
  --device=RAW_DEVICE_B --eid_index=0 \
  --library=/opt/urma/lib/liburma.so \
  --bytes=4096 --batch=32 --timeout_ms=60000
```

两端 bytes/batch 必须一致，设备、EID 索引和库路径各自配置。可依次用 batch=1、32、256 运行；每次重新启动两个进程。发送端 TX 深度为 batch，TX CQ 深度为 batch+1，最大批次要求设备支持至少 257 个 TX CQ 槽。

参数范围：bytes 为 8–1048576，还须满足设备 max_msg_size；batch 为 1–256；timeout_ms 为 1–3600000，默认 10000。timeout_ms 分别限制各控制操作，以及从 modify 成功后开始的整个 poll/软件 flush 循环；同步驱动调用自身不能被这个截止时间中断。添加 `--urma_debug` 可把 URMA debug 日志输出到 stderr，常规 URMA 日志也会转发到 stderr。

## 输出与失败判定

发送端打印实际统计，例如混合完成可能显示：

```text
TX drain: accepted=32 retired=32 success=0 flush_err=20 unhandled=12 loc_access_err=0 remote_access_abort_err=0 ack_timeout_err=0 rnr_retry_cnt_exc_err=0 other_error=0 last_other_status=0 flush_done=1 rejected_sends=3
PASS: all attempts retired once; ERROR SQ rejected new sends; both endpoints closed resources
```

这个数字分布仅作格式示例，不能作为硬件验收要求。判定条件为 accepted=retired=batch、各类终结数量之和等于 retired、flush_done=1、rejected_sends=3，以及双方关闭成功。

四种常见错误独立统计，不再重复计入 other_error：

| 输出字段                | URMA 状态                     |
|-------------------------|-------------------------------|
| loc_access_err          | URMA_CR_LOC_ACCESS_ERR        |
| remote_access_abort_err | URMA_CR_REM_ACCESS_ABORT_ERR  |
| ack_timeout_err         | URMA_CR_ACK_TIMEOUT_ERR       |
| rnr_retry_cnt_exc_err   | URMA_CR_RNR_RETRY_CNT_EXC_ERR |

对端输出：

```text
PASS: peer TX drain confirmed; both endpoints closed resources
```

两端都 PASS 且退出码为 0 才算通过。退出码 2 表示参数错误，1 表示初始化、协议、投递、排空、校验或清理失败。普通 WR 错误状态可以是主动故障测试的预期结果；modify/flush 调用失败、缺失边界、重复或未知 AttemptId、残留记录、对端失联等均不能算通过。接受范围不可信时 accepted 显示 unknown；last_other_status 保存最后一个未单列错误的 URMA 状态码，other_error 为 0 时该字段无意义。other_error 不证明网络健康，需结合基础 SEND 结果与 provider 日志判断。

部分提交、提交范围不可信、异常 CQE 或排空超时会保留会话及注册内存到进程退出，不自动重试或重放。由于无法证明完整排空，这些路径不会先 free buffer、Uninit 或 Unload。正常路径依次解除导入、删除 jetty/JFR/JFC、注销内存、关闭 runtime 和动态库；任何清理失败都不输出 PASS。

## 无硬件验证与当前边界

```sh
bazel test //tools/tx_drain_test:tx_drain_test_test //tools/send_test:send_test_test
```

单测以真实 JettyPool、Ledger、Sender 和 CompletionProcessor 配合 gMock URMA、socketpair 验证控制协议与排空流程。控制通道共用 [tools/common](../common/control_channel.hpp)，基础 SEND 的协议和收发逻辑独立保留。

当前只测试单设备、单 SQ、单 owner 的主动 ERROR。尚未覆盖同池多个 SQ、预置接收队列下的数据交付、异步设备事件、故障重建或吞吐性能。此工具已可用于服务器验证；本地未执行真实 URMA 硬件测试，硬件结果以服务器上的双端运行输出为准。
