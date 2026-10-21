# 真实对端退出与外部故障观测

本工具补齐“目标进程已经退出后，向原有 imported jetty 投递新 WR”的硬件观测。它不会把不投递 RX 的
RNR 实验当成真实进程退出，也不要求错误一定是 ACK_TIMEOUT、RNR 或某个固定状态。

## 流程和边界

`process_exit` 模式：

1. 服务端监督进程在任何 `Load/Initialize` 之前 fork 一个专用 URMA 子进程，监督进程只持有 TCP 控制连接。
2. 子进程创建 endpoint 并投递一条 RX。客户端完成一条真实 SEND；子进程收到并验证 payload。
3. 监督进程向子进程发送 SIGKILL，并通过 `waitpid` 确认因 SIGKILL 退出，之后才发出故障就绪通知。
4. 客户端使用原来 import 的 endpoint 和原 SQ 投递 `batch` 条新 WR，记录 post 返回值、已接受前缀、逐条
   CQE 原始 status、耗时、前后 SQ state 和异步事件。普通错误 CQE 不触发工具主动 modify ERROR。
5. 接受的 WR 全部终结后客户端显式关闭资源。未终结、无可信接受边界或异常 CQE 时保留完整 DMA 依赖链到进程退出。

`process_exit` 是 **exit 后的新投递**，不是“在途 WR 被 kill 中断”测试，也不能证明物理端口/链路失效的行为。
`waitpid` 证明远端进程退出，不证明客户端 accepted WR 已终结；后者仍依赖各自 CQE。
应用主动 DeleteJetty 和异常进程退出的内核回收路径不同，不互相替代。

当前设备的 async 能力仅部分支持。用户提供的现场能力说明是：真实链路/端口故障后的 WR 通常以
ACK_TIMEOUT 终结，但 PORT_DOWN async 明确无法观察；已确认支持 JFS error、JFR error、JFC error 和
JFR 水线相关事件。当前 JFS/相关 jetty 的错误 AE 需先触发它绑定的 JFC error，再联动产生，不能按独立
JFS 故障注入理解。这些是现场能力说明，不是本工具的新事件日志；工具保留实际采集结果，不要求任意故障
都产生 async，也不将 PORT_DOWN 设为必测通过项。已有 `process_exit/inflight_exit` 运行的 async=0 仍是有效结果。

## 构建和运行

```bash
bazel build //tools/peer_failure_test:peer_failure_test
bazel test //tools/peer_failure_test:peer_failure_unit_test --test_output=errors
```

两端使用相同 `bytes/batch/fault/fault_delay_ms`。建议依次运行 batch 1、32、256；每组重新启动两端。
设备、EID、IP 按现场环境替换。

服务端：

```bash
./bazel-bin/tools/peer_failure_test/peer_failure_test \
  --server --address=0.0.0.0 --port=18521 \
  --device=udmac0d1e4 --eid_index=38 --fault=process_exit --batch=32
```

客户端：

```bash
./bazel-bin/tools/peer_failure_test/peer_failure_test \
  --address=141.62.33.25 --port=18521 \
  --device=udmac0d1e4 --eid_index=60 --fault=process_exit --batch=32
```

默认每次控制/数据操作超时为 10 秒，TX 观测之后再观察 async 1 秒。可设置 `--timeout_ms`、
`--async_grace_ms`。整个 worker 的 watchdog 在 Load 之前 fork，并覆盖可能阻塞的 provider 调用；
默认总时限根据操作超时计算，也可通过 `--watchdog_ms` 明确指定。

## 投递后立即退出的窗口

两端改用 `--fault=inflight_exit`，同样建议 batch 1、32、256。该模式改变故障和 post 的先后顺序：

1. baseline 后服务端先 post `batch` 个 RX，再允许客户端投递，避免用 RX 饥饿制造早于退出的 RNR。
2. 客户端 post 一批，最多 poll 一轮 64 个 CQE，输出 `post_status/accepted/accepted_known`、
   `retired_before_exit_request` 和已经看到的各状态；随后发送 `POSTED` 控制 marker。
3. 服务端收到 marker 后 SIGKILL + waitpid，再发退出确认；客户端继续消费剩余 CQE，禁止重放。

这个时序证明 **post 在故障注入之前**，不证明某个 WR 在 SIGKILL 时仍由硬件处理。控制确认期间客户端不 poll，
`software_unretired_at_confirmation` 可能包含已经在 CQ 中排队的完成。工具不会把它标为硬件在途条数。

- 如果 `retired_before_exit_request == accepted`，输出 `NOT_OBSERVED: inflight_at_confirmation`，因为请求退出
  之前已经观测到全部终结。
- 如果还有软件未退休项，也输出硬件在途无法与排队 CQE 区分的 `NOT_OBSERVED`。只能据完整日志判断退出后收到了
  什么状态，不能把所有错误都归因于进程退出；`pre-exit CQE` 尤其不能归因于之后的 SIGKILL。
- 真实设备可能在 TCP marker 到达前完成整个批次，全部 SUCCESS 是合法观测，但这组结果不能记为“验证了硬件
  在途 WR 被中断”。若需要捕获该窗口，应结合现场能独立确认的路径延迟/故障注入手段，不能仅增加软件 poll 延迟。
- 某个 CQE 异常或终结超时时仍保持依赖链到进程退出；即使已收到远端退出确认也不提前释放客户端 MR。

## 手工外部故障

两端改用 `--fault=external --fault_delay_ms=10000`。完成 baseline 后，服务端额外预投递 `batch` 条 RX，
双方打印 `INJECT_NOW`；客户端在 10 秒窗口内只采集 async，窗口结束后向原 endpoint 发新批次。

- 先跑一次 **不注入故障的对照**，应看到全部 SUCCESS；这验证本实验没有借 RX 饥饿制造 RNR。
- 故障组在 `INJECT_NOW` 后手工停止打印的 `endpoint_pid`，或使用现场已有运维方法注入 UB 链路故障。
  工具不会自动改网络、关闭端口或操作无关进程。停止的是专用 URMA 子进程，监督进程应保留。
- 尽量让 TCP 控制面走独立路径。若控制面也中断，客户端仍完成已经开始的 CQ/async 观测，随后将控制确认超时
  单独报告；无法确认远端资源状态时不宣称双方正常收尾。
- 外部模式不能验证操作者是否真的造成了预期链路故障。请保存现场注入时间、方法和设备状态；SUCCESS 也可能说明
  未成功注入、路径未受影响或链路已恢复，不能直接得出“链路故障透明恢复”的结论。
- 对照用户提供的当前现场能力，链路故障通常产生 ACK_TIMEOUT，但工具仍逐项保留实际 CQE 状态；
  PORT_DOWN async 不作为本组的通过条件。受支持队列错误或 JFR 水线事件需要各自的注入条件，不能由断链替代。
- 服务端最后由监督进程回收子进程；未消费的 RX 不在应用层提前 unregister。此工具不负责验证 RX 正常关闭协议。

## 输出判读

- `Probe TX`：`post_status` 是 provider 返回码，`accepted_known=false` 表示无法判断提交边界。
  `retired` 只计入已验证身份的终结 CQE，不根据 TCP EOF、post 返回或条数猜测完成。
- `CQE status=N count=M` 分别保留各原始状态：0 SUCCESS、4 LOC_ACCESS_ERR、8 REM_ACCESS_ABORT_ERR、
  9 ACK_TIMEOUT_ERR、10 RNR_RETRY_CNT_EXC_ERR、11 WR_FLUSH_ERR。测试不调查 status 9 的根因。
- `Local SQ ... state=N` 是两个时点的 query 结果，不代表整个窗口内状态持续不变。1 是 READY；即使 READY，
  也不能只凭状态断言后续发送成功，需要新的独立实验验证。
- `first_cqe_ns/last_cqe_ns` 从调用 post 之前计时，包含软件 poll 开销，不是硬件内部完成时刻。
- async 通过 `poll(ctx->async_fd, timeout=0)` 先检查可读才 Get；每个成功 Get 恰好 Ack 一次。baseline 已可见事件
  先单独消费并输出，然后重置统计。故障窗口输出原始 event type 计数，不解引用事件中的资源指针。
- `NOT_OBSERVED: async event` 只代表该窗口未见事件，不能据此排除真实远端故障。
- 客户端 `PASS` 表示协议与观测完整、接受的 WR 恰好退休一次、资源正常关闭；并不表示 WR 必须成功、故障一定隔离，
  或所有类型的物理故障都已验证。服务端 PASS 只确认自身监督协议和子进程回收。
- 终结超时/无可信 DMA 边界返回非零并保留资源；watchdog 到期返回 124。二者都不能记成“硬件验证通过”。

单测使用 gMock 替换真实 URMA 调用，覆盖协议校验、SIGKILL 后 waitpid、watchdog、async 可读检查与逐事件 Ack、
普通错误收集而不 modify、部分接受不重放、post/poll 分离窗口、无可信接受边界，以及未终结时禁止释放。
真实硬件结果必须单独记录。

本工具只向一个故障目标观察状态；若要确认 status 8 是否影响同一 SQ 上的健康目标，使用
[sq_multiplex_test 的 `--fault=peer_exit` 场景](../sq_multiplex_test/README.md)。该场景先验证故障目标
曾可接收，再确认其进程退出，与存活健康目标混合发送，独立检查 healthy before/mixed/after；其硬件结果
仍待记录，不能从本工具的 READY 查询或现有 RNR 隔离结果推定已通过。
