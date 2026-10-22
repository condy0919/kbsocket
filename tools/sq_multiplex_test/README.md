# 同一 SQ 多目标故障隔离观测

`sq_multiplex_test` 验证一条 RM_CTP SQ 向两个远端目标发送时，一个目标的错误是否影响健康目标的完成进度。`--fault=rnr` 保留原有空 JFR 实验；`--fault=peer_exit` 验证失效远端产生 status 8 时的同 SQ 隔离。两种模式都只有一条客户端发送 SQ，不重建、不重放，也不主动修改为 ERROR。

默认 `--fault=rnr` 时服务端使用两个独立 `JettyPool`，各自拥有 JFR 和 JFC：

- `healthy`：每阶段预先投递 `batch` 个 RX buffer，接收并校验消息。
- `sink`：始终不投递 RX，预期对应发送以 `URMA_CR_RNR_RETRY_CNT_EXC_ERR` 终结。

两个目标必须具有独立 JFR。仅在同一共享 JFR 下创建两个 jetty，无法构造只有一个目标缺少 RX 的实验。

每轮顺序如下：

1. `before`：向 healthy 发送 `batch` 条，建立本轮基线。
2. `mixed`：一次向同一 SQ 提交 `2 * batch` 个 WR，按 `sink[0], healthy[0], sink[1], healthy[1], ...` 排列。
3. `after`：前一批全部终结且结果符合预期后，用同一 SQ 向 healthy 发送 `batch` 条新消息，验证后续可用性。

工具直接消费 TX CQE，并通过 `AttemptLedger` 精确终结已接受的 WR，保持各错误类别的独立硬件观测。它不调用生产 `TxCompletionProcessor`：生产层仍对 RNR、ACK_TIMEOUT 等错误主动排空，合法 status 8 已改为只退休并携原始状态和 record 上报，由上层决定对应连接与操作的失败处理；完成层不自动关闭连接或重试。成功实验和预期错误的处理不修改、不 flush、不重建 SQ，也不重放任何已接受的 WR。若 provider 破坏提交契约、无法确定接受边界或内部记账失败，`TxSender` 可能主动隔离 SQ；此时实验失败并保留未决资源，不能作为普通终结错误的隔离证据。

## 构建与本地测试

在仓库根目录执行：

```sh
USE_BAZEL_VERSION=9.3.0 bazel build //tools/sq_multiplex_test:sq_multiplex_test
USE_BAZEL_VERSION=9.3.0 bazel test //tools/sq_multiplex_test:all --test_output=errors
```

本地单测使用 gMock，不需要 `liburma.so` 或 UB 硬件。双端运行时通过 `dlopen` 加载 URMA，可用 `--library=/path/to/liburma.so` 指定库。

控制协议为版本 3，包含故障模式及独立的 baseline/退出确认。更新工具时必须同时更新两端；版本 1、2 与版本 3 不兼容，模式不匹配也会在开始数据传输前拒绝握手。

## 对端退出后的 status 8 隔离

`--fault=peer_exit` 的服务端在任何 URMA 加载之前 fork 专用故障子进程。该子进程拥有自己的 context、JFR、jetty 和 MR，父 worker 创建健康端点并持有 TCP 控制连接。故障子进程不继承健康端点的 provider 状态，终止它不会同时销毁健康端点。

严格执行以下顺序：

1. 故障子进程预投递一个 baseline RX，再公布端点。客户端通过后续实验使用的同一 SQ 向它发送一条真实消息。
2. 客户端收到 baseline SUCCESS CQE 并检查 SQ READY，通过专用结果握手确认。
3. 服务端命令故障子进程消费并校验 baseline 的源端点、payload 和 IMM；收到成功确认后执行 SIGKILL，再通过 `waitpid` 确认退出。
4. 双方交换退出结果，确认后才开始每轮 `before/mixed/after`。这里的 `before` 是混合目标流量前的健康基线，故障子进程已经退出。
5. mixed 仍按旧故障目标/健康目标交替提交，旧目标预期 `URMA_CR_REM_ACCESS_ABORT_ERR`（status 8），健康目标必须 SUCCESS；after 继续用同一个 SQ 验证健康目标。

以下使用服务端 EID 23、客户端 EID 1；按现场实际设备/EID 修改。第一组 `batch=32`：

```sh
# 服务端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --server --address=0.0.0.0 --port=8889 --device=udmac0d1e4 --eid_index=23 --fault=peer_exit --batch=32 --rounds=8 --bytes=4096
# 客户端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --address=141.62.33.25 --port=8889 --device=udmac0d1e4 --eid_index=1 --fault=peer_exit --batch=32 --rounds=8 --bytes=4096
```

第二组 `batch=128`，mixed 覆盖单次 256 个 WR 的上限：

```sh
# 服务端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --server --address=0.0.0.0 --port=8889 --device=udmac0d1e4 --eid_index=23 --fault=peer_exit --batch=128 --rounds=8 --bytes=4096
# 客户端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --address=141.62.33.25 --port=8889 --device=udmac0d1e4 --eid_index=1 --fault=peer_exit --batch=128 --rounds=8 --bytes=4096
```

预期客户端 `Fault baseline` 为 `accepted=retired=success=1`；三个 `Healthy` 阶段各有 `accepted=retired=success=batch*rounds`，`Sink mixed` 的 `accepted=retired=remote_access_abort_err=batch*rounds`，其余错误计数为零。服务端 `Fault baseline received=1`，三个健康阶段各 `received=batch*rounds`。两端 `Peer exit: confirmed=true`，客户端 `state_checks=2+3*rounds`；逐轮 `sink_remote_access_abort=batch`。baseline 单独计数，不混入三阶段统计。

最终两端必须打印：

```text
PASS: healthy traffic succeeded before, during and after same-SQ REM_ACCESS_ABORT; fault baseline validated before SIGKILL/waitpid; old-target attempts retired once with status 8; same SQ remained READY without modification or recreation; payload validated; both endpoints closed resources
```

2026-10-22 反馈的两组硬件日志均满足上述判据：服务端 `udmac0d1e4:39`、客户端 `udmac0d1e4:61`，`batch=32/128` 各运行 8 轮。三个健康阶段各有 256/1024 条 SUCCESS 且服务端 payload 校验通过，旧目标分别产生 256/1024 条 status 8；各有 26 次同 SQ 状态检查通过，baseline 和退出确认完整，两端均 PASS。详细数据见[硬件观测记录](../../docs/hardware-observations.md)。

这确认了当前设备/provider 配置下的同 SQ 功能隔离，不将它扩展为所有环境或所有错误类型的契约。若健康目标也发生错误，或者旧目标返回 ACK_TIMEOUT/RNR 等其他结果，工具继续收齐当前已接受 WR，再向双方报告 FAIL，不继续 after/下一轮。若仍有未终结 DMA，保留整个依赖链到进程退出。

进程模式有外层 watchdog、子进程 `PDEATHSIG` 和有界 `waitpid`。`--watchdog_ms=0` 根据 `timeout_ms` 与 `rounds` 计算整轮上限，也可以显式指定毫秒数。watchdog 超时返回 124，不能视作 PASS。启动、baseline、退出或控制协议失败时回收故障子进程，不用 TCP EOF 代替退出/静默证明。

本模式不要求 async 事件。已知对端进程退出可能只表现为错误 CQE，本实验不触发 JFC 故障、不以 PORT_DOWN 事件作为通过门槛。

## RNR 双端硬件测试

以下示例沿用设备 `udmac0d1e4`、服务端 EID index `65`、客户端 EID index `61` 和服务端地址 `141.62.33.25`。请按实际环境修改。每组先启动服务端，再启动客户端；上一组结束后再开始下一组。

第一组每个目标 1 条消息，便于观察最小 mixed 批次：

```sh
# 服务端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --server --address=0.0.0.0 --port=8889 --device=udmac0d1e4 --eid_index=65 --batch=1 --rounds=8 --bytes=4096
# 客户端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --address=141.62.33.25 --port=8889 --device=udmac0d1e4 --eid_index=61 --batch=1 --rounds=8 --bytes=4096
```

第二组每个目标 32 条消息，mixed 一次提交 64 个 WR：

```sh
# 服务端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --server --address=0.0.0.0 --port=8889 --device=udmac0d1e4 --eid_index=65 --batch=32 --rounds=8 --bytes=4096
# 客户端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --address=141.62.33.25 --port=8889 --device=udmac0d1e4 --eid_index=61 --batch=32 --rounds=8 --bytes=4096
```

第三组每个目标 128 条消息，mixed 覆盖一次提交 256 个 WR 的上限：

```sh
# 服务端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --server --address=0.0.0.0 --port=8889 --device=udmac0d1e4 --eid_index=65 --batch=128 --rounds=8 --bytes=4096
# 客户端
./bazel-bin/tools/sq_multiplex_test/sq_multiplex_test --address=141.62.33.25 --port=8889 --device=udmac0d1e4 --eid_index=61 --batch=128 --rounds=8 --bytes=4096
```

两端的 `--fault`、`--batch`、`--rounds`、`--bytes` 必须一致。限制为 `batch=1..128`、`rounds=1..1000`、`bytes=32..4096`。`bytes` 包含 32 字节测试头，消息保持在单个 RX buffer 容量内。`--timeout_ms=1..3600000` 是每次控制/数据操作的本地超时，默认 10000 ms；两端可以不同。超时表示本次观测未完成，不代表硬件资源已可安全释放。

## RNR 模式的输出与通过条件

客户端分别打印 `Healthy before`、`Healthy mixed`、`Healthy after` 和 `Sink mixed`：

- 每个 healthy 阶段累计 `accepted=retired=success=batch*rounds`，RNR 和其他错误计数均为 0。
- sink 累计 `accepted=retired=rnr=batch*rounds`，成功和其他错误计数均为 0。
- 服务端对应三个 healthy 阶段的 `received=batch*rounds`，逐条校验消息身份、完整 payload 和 64 位 IMM；sink 不投 RX。
- `Same SQ` 显示同一 jetty ID、`state_checks=1+3*rounds`，每次查询必须为 READY。`requested_rnr_retry` / `requested_err_timeout` 是创建参数，`queried_rnr_retry` / `queried_err_timeout` 是最后查询值；二者的编码可能不同，不能直接转换成重试时间。`last_queried_state` 是最后一次成功 query 返回的状态，未获得该值时显示 `unknown`；`state_checks` 仅累计通过 READY 检查的次数。
- `accepted_known` 必须为 true。`provider_eagain` 仅记录实际 provider 返回的 EAGAIN；本工具不靠重试继续实验，任何发送提交错误都会使测试失败。

错误完成分别显示 `loc_access_err`、`remote_access_abort_err`、`ack_timeout_err` 和 `rnr`；其余合法终结错误计入 `other_error`，`last_other_status` 记录其中最后一个原始状态码。`ACK_TIMEOUT_ERR`（status 9）与 RNR（status 10）是不同结果，前者不能满足 sink 的 RNR 预期。

各阶段汇总行的 `min_cqe_ns` / `max_cqe_ns` 是跨轮次该阶段 CQE 观察耗时的最小值和最大值；它们是相对各自批次 post 的耗时，不是整个测试的起止时刻。

已经开始 mixed 阶段的轮次各有一行 `Mixed round`，未运行轮次不打印：

- `healthy_retired` / `sink_retired`：本轮各目标已有合法终结证据的 WR 数；`healthy_success` / `sink_rnr` 是其中符合该目标预期的数量。
- `first_healthy_ns` / `last_healthy_ns`：本轮首次/最后消费 healthy 合法终结 CQE 的时间，包含成功和错误完成。
- `first_sink_ns` / `last_sink_ns`：本轮首次/最后消费 sink 合法终结 CQE 的时间，包含 RNR 和其他终结状态。
- `first_rnr_ns` / `last_rnr_ns`：本轮首次/最后消费 sink RNR CQE 的时间；应结合 `sink_rnr` 判断是否已观察到 RNR。
- `healthy_before_first_rnr` / `healthy_before_last_rnr`：消费首个/最后一个 sink RNR CQE 前，已消费的 healthy SUCCESS CQE 数量。

时间统一从该批次调用 post 前开始，到软件消费 CQE 为止，包含 provider 调用和轮询开销，不是硬件时间戳，也不能从首个 RNR 的耗时直接反推硬件 timer 编码。健康 SUCCESS CQE 先于 RNR 被观察到，可证明本轮健康发送已有成功完成；健康 CQE 全在 RNR 之后，可能涉及 SQ 执行、完成上报顺序或软件轮询时序，不能单独认定为硬件队头阻塞。`healthy_before_*_rnr=0` 仍可通过测试；本工具记录这一现象，不把某种 CQE 顺序当作正确性契约。

最终两端均应打印：

```text
PASS: healthy traffic succeeded before, during and after same-SQ RNR; sink attempts retired once with RNR; same SQ remained READY without modification or recreation; payload validated; both endpoints closed resources
```

PASS 要求所有阶段计数和状态正确、每个已接受 WR 恰好终结一次、全部 RX buffer 回收，以及双端资源关闭握手完成。未知 CQE、重复 CQE、非预期状态、payload/IMM 错误、超时或无法确认接受数量都会失败。

对于能明确对应已接受 WR 的合法终结 CQE，即使状态不符合预期，工具也先记录失败并继续收集本批完成，直到所有已接受 WR 终结或超时。只有收齐本批完成后才再次查询 SQ 状态；超时或完整性异常时不新增查询，`last_queried_state` 可能仍是出错前的数值，需结合 `state_checks` 和 FAIL 位置判断。完成 RX-ready 握手后，阶段结果通过控制面通知对端，失败后不进入 `after` 或下一轮。例如 sink 返回 ACK_TIMEOUT、healthy 返回 ACK_TIMEOUT 都仍然判 FAIL，不会放宽成允许的成功结果。未知身份、重复 CQE、伪 CQE 或无法建立可靠对应关系的状态会停止后续 poll；当前 poll 已返回的其他 CQE 仍逐条核验，只退休有可信终结证明的 WR。

实验失败不一定意味着资源无法关闭。只有双方确认不再发送且全部已接受 TX 已终结、本地 TX/RX 所有权归零时，工具才正常关闭资源并确认对端完成关闭；此时打印 `FAIL: experiment failed; both endpoints closed resources`，仍以非零退出码结束。不能证明 DMA 终结和对端静默时，工具保留整个资源依赖链到进程退出；控制连接断开不能代替静默确认。

已有 `batch=32` 硬件运行出现 healthy 和 sink 的 ACK_TIMEOUT，旧版本在首个非预期状态后提前停止收集，详细数据见[硬件观测记录](../../docs/hardware-observations.md)。修正后 `batch=32 rounds=8` 的硬件复测已通过：三个健康阶段各 256 条 SUCCESS，sink 256 条 RNR，同 SQ 保持 READY，双方成功关闭。此结论属于 RNR 模式；`peer_exit` 的 batch=32/128 也已独立通过，结果登记在前文，两者都不能解释旧 ACK_TIMEOUT 的根因。

## 观测范围

该实验补充已有 [同一 SQ 在 RNR 后续发](../backpressure_test/README.md) 和 [不同 SQ 故障隔离](../sq_isolation_test/README.md)：前者验证错误后的可用性，后者验证一条 SQ 主动 ERROR 排空时其他 SQ 仍可用，本工具关注同一 SQ 内两个目标之间的影响。

结果仅适用于本次设备、驱动/固件、队列参数和负载。它不测量实际 provider EAGAIN 的部分接受边界、物理链路故障或带剩余 RX 的 JFR 销毁。进程退出是本实验产生 status 8 的手段，不是该远端访问中止错误类别的唯一定义；本工具的通过不能替代生产完成层或上层连接状态机的测试。记录结果时建议保存两端完整输出、设备信息、URMA/provider 与驱动/固件版本。
