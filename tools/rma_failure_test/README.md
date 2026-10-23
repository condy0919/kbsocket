# 同 SQ 的单边 WRITE 故障隔离观测

本工具验证：故障远端进程已经退出之后，向其旧 endpoint/MR 发起单边 WRITE 时，WR 如何终结，以及同一条本地 SQ 对健康远端的 WRITE 能否继续成功。当前固定执行 WRITE，不接受 `--operation`、`--mixed_send` 或 `--write_notify`，也拒绝声明通知模式的对端描述符；不要求故障 CQE 一定是 status 8，也不构造硬件在途 WRITE 期间的退出。

服务端由两个独立 URMA 进程组成：主进程持有健康 WRITE 目的 MR 和 TCP 控制连接，子进程持有故障 WRITE 目的 MR。创建子进程发生在服务端主进程第一次 `UrmaApi::Load()` 之前，避免 fork 继承 provider 状态或健康端点。客户端以同一条 RM_CTP jetty/SQ 访问两个远端；每条 WRITE 使用独立本地源区间和远端目的区间，区间带保护区，每轮数据包含阶段和轮次信息。

故障子进程会被有意 SIGKILL，其 MR、队列和 context 由进程退出路径回收，不执行正常的 URMA 删除流程。文中及 PASS 日志的“双端正常关闭”仅指客户端和持有健康目的 MR 的服务端主进程，不包含这个故障子进程。

这是直接调用 URMA 的硬件观测工具；生产 `TxSender` 尚未因此增加 WRITE 支持。所有端点都不投递 RX buffer，TCP 只传递描述符、阶段同步和远端校验确认，不传测试 payload。WRITE 的数据通过注册段直接写入远端内存；本地 SUCCESS 后仍须取得远端校验确认，才能把该轮记为数据验证成功并复用槽位。

用户已说明当前芯片存在已知缺陷：jetty 一旦出现 status 9（ACK_TIMEOUT），后续不可继续使用，即使查询仍为 READY；大并发 READ 容易触发，用户说明大并发 WRITE 不出现这一现象。因此本工具由 READ 改为 WRITE，任何目标出现 status 9 都停止后续提交，在收集当前已接受批次的终结证据后报告失败，不能把它当作允许继续测试的普通旧目标错误。新版 WRITE 的 WF0/WF3 及 WF2 同命令复测已双端通过，旧目标分别返回 1/256/1024 条 status 8，同 SQ 健康写入和远端校验全部成功；WF2 首次运行的健康基线 status 9 仍保留为未定位失败，复测使用新 jetty；WF1 待执行。完整 WRITE 结果及旧 READ 失败记录见[硬件观测记录](../../docs/hardware-observations.md)。

## 构建与无硬件测试

```sh
bazel build //tools/rma_failure_test:rma_failure_test
bazel test //tools/rma_failure_test:all --test_output=errors
```

可执行文件：`bazel-bin/tools/rma_failure_test/rma_failure_test`。两端使用同一版本；外层 WRITE failure 协议仍为 v2，与旧 READ 版本不兼容；嵌入的通用 RMA 描述符协议已升级为 v3，因此即使不使用通知模式，也必须同时更新两端二进制。编译与单测不需要本机安装 `liburma.so`；实际运行通过 `dlopen` 加载库，可用 `--library=/path/to/liburma.so` 指定。

## 双端运行

先以每条 WRITE 16 KiB、每目标每轮 1 条、共 1 轮验证基本故障行为，再增加批次。根据实际环境调整地址、设备和 EID index。

服务端：

```sh
./rma_failure_test --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=39 \
  --bytes=16384 --batch=1 --rounds=1 --timeout_ms=10000
```

客户端：

```sh
./rma_failure_test --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=52 \
  --bytes=16384 --batch=1 --rounds=1 --timeout_ms=10000
```

随后在两端同步替换以下参数，每组重新启动进程。各组按实际 WRITE 日志记录结果，不继承旧 READ 或 SEND 的结果：

| 组合 | 双端参数 | 每轮混合链 | 主要覆盖与实测状态 |
|---|---|---:|---|
| WF0 | `--bytes=16384 --batch=1 --rounds=1` | 2 条 WRITE | 已通过；旧目标 1 条 status 8，健康三阶段各 1 条成功并经远端校验 |
| WF1 | `--bytes=16384 --batch=32 --rounds=8` | 64 条 WRITE | 待执行；基本故障隔离、逐 WR 退休 |
| WF2 | `--bytes=16384 --batch=128 --rounds=8` | 256 条 WRITE | 同命令复测通过；旧目标 1024 条 status 8，健康三阶段各 1024 条成功并经远端校验；首次健康基线 status 9 原因仍未明 |
| WF3 | `--bytes=1048576 --batch=32 --rounds=8` | 64 条 WRITE | 已通过；旧目标 256 条 status 8，健康三阶段各 256 条成功并经远端校验 |

`batch=1..128` 表示每个目标的 WRITE 数；混合阶段每轮提交 `2 * batch` 条 WR，按 `健康0 → 失效0 → 健康1 → 失效1 …` 交错。这里的 batch 不是 SQ 的请求深度，最小实验无需把 batch 提高到 64。`bytes=1..8MiB`、`rounds=1..256`，并要求 `2 * batch * bytes` 不超过 256 MiB，保护区和对齐空间另计；实际长度还必须满足设备 WRITE 能力。`bytes`、`batch`、`rounds` 必须两端一致。

`--timeout_ms=100..120000` 限制完成等待和控制步骤；基线及退出确认包含远端校验、SIGKILL/waitpid 等复合工作，使用扩展控制等待预算。外层监督进程在任何 URMA 初始化之前创建，覆盖同步 provider 调用。`--watchdog_ms=0` 自动计算总期限；手工设置必须大于 `timeout_ms` 且不超过一小时。超时终止 worker 进程组，故障子进程还受父进程退出信号约束，不能把 watchdog 终止当作通过。

WF2 首次运行的 status 9 来自实际 CQE，不是工具等待超时；增大 `--timeout_ms` 不会修改硬件 ACK 超时配置。baseline 始终只发一条 WRITE，batch 影响初始化时的队列深度和 MR 大小，以及后续阶段的 WR 数量；首次基线失败未进入混合阶段。后续同命令复测已完成最大混合批次，客户端使用新 SQ 25228，首次失败的是 SQ 22394，因此通过结果不表示故障 jetty 恢复或芯片缺陷已修复。保留两次记录，不复用已返回 status 9 的 jetty。

## 流程和判定

1. 服务端创建故障目的 MR 子进程、健康目的 MR 主进程，客户端导入两组 endpoint/MR。
2. 客户端分别 WRITE 两个目的区，检查成功 CQE；健康主进程立即校验基线写入与保护区，故障子进程的基线保留到退出前校验，期间不复用其槽位。
3. 完成 `Healthy before` 阶段的 `rounds * batch` 条健康 WRITE，每轮在 TX 完成后通过 TCP 通知服务端校验，客户端收到确认后才进入下一轮；基线及各阶段使用不同数据，避免把上一轮内容误判为本轮成功。
4. 服务端要求故障子进程校验其目的区基线和保护区，取得确认后才 SIGKILL，并以 `waitpid` 确认退出原因。客户端收到明确退出确认后，才使用旧描述符进行混合 WRITE；其后继续健康目标 WRITE。
5. 健康访问在混合阶段前、中、后分别检查成功 CQE、本地源数据、远端 payload 和保护区，全程检查同一条本地 SQ 的 READY 状态。任何 status 9 都终止后续提交；READY 查询不能覆盖这一失败。
6. 已接受 WR 全部获得可信终结记录、双方确认静默后，客户端和健康服务端主进程分别释放导入对象、队列、MR、context 和动态库，交换关闭标记后才输出 PASS。

故障基线成功不能被故障阶段的错误替代；仅 TCP 断开或 SIGKILL 调用成功也不能替代 `waitpid` 的退出证据。工具不修改 SQ 为 ERROR、不重新创建 SQ，也不重放或重新投递失败 WRITE。状态 9 的生产规避应由软件发送门禁和故障排空负责，观测工具则停止该轮实验并保留不能证明可释放的依赖。

客户端打印六组独立计数：

- `Healthy baseline`、`Fault baseline`：健康目的区和故障目的区各自的成功基线。
- `Healthy before`、`Healthy mixed`、`Healthy after`：三个阶段的健康 WRITE，必须全部成功且取得远端数据校验确认。
- `Fault mixed`：旧目标 WRITE，已接受 WR 必须全部恰好退休一次，实际 CQE 错误状态逐项打印，不预设 status 8；status 9 必须终止实验。

每组 `terminal_status[N]` 是原始 CQE 状态直方图，只打印非零项。`verified` 表示已确认远端目的数据校验通过的成功 WRITE，不能仅凭本地 SUCCESS 增加；故障后旧目标不再可读回校验，其 `verified=0`。错误 WRITE 不表示远端完全未被写入，也不能据此重放请求。

`Same SQ` 打印 SQ ID、状态查询次数与返回码、最后状态、post 返回值、部分接受次数、`accepted_known`、`pending` 和 `peer_exit_confirmed`。正常通过时客户端应满足：

```text
accepted_known=true
pending=false
peer_exit_confirmed=true
last_state=1
```

所有健康 WRITE 的 `accepted=retired=verified`；故障混合 WRITE 的 `accepted=retired`，没有成功 CQE。服务端提供被动 WRITE 目的区，不提交 WRITE，因此 `accepted=retired=0` 是正常现象；服务端的健康 `verified` 反映实际远端内存校验。

只有故障 WRITE 如实错误终结、没有任何 status 9、健康 WRITE 全部成功并由远端验证、同 SQ 检查始终 READY、保护区正确且双方正常关闭资源，才能输出 PASS。

若 post 只接受前缀，工具识别并跟踪已接受 WR，不重试剩余部分；该实验不能按完整成功计。若接受范围未知、CQE 身份无效、完成缺失、健康 WRITE 失败、出现 status 9 或控制确认缺失，返回失败并保留尚未证明可释放的 DMA 依赖直到 worker 退出，禁止猜测完成数量或提前注销 MR。

退出码：`0` 表示上述完整验证通过，`1` 表示实验或 provider/API 失败，`2` 表示参数错误，`124` 表示外层 watchdog 超时。原始错误状态是本轮观测，不作为其他 provider/固件版本的固定承诺。

## 覆盖边界

- 故障严格发生在混合 WRITE 提交之前，不声称终止进程时有 WR 正在硬件中执行。
- 同一 SQ 有多个软件未退休 WR，不代表硬件并行度或乱序程度已经被测量。
- WRITE 使用 TCP 完成确认，不覆盖 WRITE 加 SEND/IMM 数据面通知的跨操作顺序或应用 lease 协议。
- 当前不覆盖 READ、MR 主动注销/权限撤销、真实链路故障、JFC 出错导致完成记录缺失后的资源回收。
- 错误目标的终结状态、健康目标的成功结果和实际资源关闭应一起记录到[硬件验证矩阵](../../docs/hardware-validation-matrix.md)，不能只凭最后一行 PASS 推断错误原因。
