# 同 SQ 的 READ 故障隔离观测

本工具验证：故障远端进程已经退出之后，向其旧 endpoint/MR 发起 READ 时，WR 如何终结，以及同一条本地 SQ 对健康远端的 READ 能否继续成功。当前只覆盖 READ，不要求故障 CQE 一定是 status 8，也不构造硬件在途 READ 期间的退出。

服务端由两个独立 URMA 进程组成：主进程持有健康 READ 源和 TCP 控制连接，子进程持有故障 READ 源。创建子进程发生在服务端主进程第一次 `UrmaApi::Load()` 之前，避免 fork 继承 provider 状态或健康端点。客户端以同一条 RM_CTP jetty/SQ 访问两个远端，各槽独立保存目的数据和保护区；远端源数据在整个测试期间保持不变。

故障子进程会被有意 SIGKILL，其 MR、队列和 context 由进程退出路径回收，不执行正常的 URMA 删除流程。文中及 PASS 日志的“双端正常关闭”仅指客户端和持有健康源的服务端主进程，不包含这个故障子进程。

这是直接调用 URMA 的硬件观测工具；生产 `TxSender` 尚未因此增加 READ 支持。所有端点都不投递 RX buffer，TCP 只传递描述符和同步消息，不传测试 payload。

## 构建与无硬件测试

```sh
bazel build //tools/rma_failure_test:rma_failure_test
bazel test //tools/rma_failure_test:all --test_output=errors
```

可执行文件：`bazel-bin/tools/rma_failure_test/rma_failure_test`。两端使用同一版本。编译与单测不需要本机安装 `liburma.so`；实际运行通过 `dlopen` 加载库，可用 `--library=/path/to/liburma.so` 指定。

## 双端运行

第一组使用每条 READ 16 KiB、每目标每轮 32 条，共 8 轮。根据实际环境调整地址、设备和 EID index。

服务端：

```sh
./rma_failure_test --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=39 \
  --bytes=16384 --batch=32 --rounds=8 --timeout_ms=10000
```

客户端：

```sh
./rma_failure_test --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=52 \
  --bytes=16384 --batch=32 --rounds=8 --timeout_ms=10000
```

随后在两端同步替换以下参数，每组重新启动进程：

| 组合 | 双端参数 | 每轮混合链 | 主要覆盖 |
|---|---|---:|---|
| RF1 | `--bytes=16384 --batch=32 --rounds=8` | 64 条 READ | 基本故障隔离、逐 WR 退休 |
| RF2 | `--bytes=16384 --batch=128 --rounds=8` | 256 条 READ | 最大混合提交批次 |
| RF3 | `--bytes=1048576 --batch=32 --rounds=8` | 64 条 READ | 大目的区、健康数据校验 |

`batch=1..128` 表示每个目标的 READ 数；混合阶段每轮提交 `2 * batch` 条 WR，按 `健康0 → 失效0 → 健康1 → 失效1 …` 交错。`bytes=1..8MiB`、`rounds=1..256`，并要求 `2 * batch * bytes` 不超过 256 MiB，保护区和对齐空间另计；实际长度还必须满足设备 READ 能力。`bytes`、`batch`、`rounds` 必须两端一致。

`--timeout_ms=100..120000` 限制单次完成等待和普通控制步骤；基线确认包含导入及两次 READ，退出确认包含源校验及 SIGKILL/waitpid，这两个复合步骤使用 `3 * timeout_ms` 的等待预算。外层监督进程在任何 URMA 初始化之前创建，覆盖同步 provider 调用。`--watchdog_ms=0` 自动计算总期限，默认参数为 640000 ms；手工设置必须大于 `timeout_ms` 且不超过一小时。超时终止 worker 进程组，故障源还受父进程退出信号约束，不能把 watchdog 终止当作通过。

## 流程和判定

1. 服务端创建故障源子进程、健康源主进程，客户端导入两组 endpoint/MR。
2. 客户端分别 READ 两个源，检查成功 CQE、目的 payload 和保护区，先证明两组远端描述符有效；随后完成 `Healthy before` 阶段的 `rounds * batch` 条健康 READ。
3. 完成基线和 `Healthy before` 后，服务端要求故障子进程检查源数据和保护区仍未改变；得到确认才发出 SIGKILL，并以 `waitpid` 确认退出原因。
4. 客户端收到明确退出确认后，使用旧描述符进行混合 READ。健康访问在混合阶段前、中、后分别检查；全程查询同一条本地 SQ 的 READY 状态。
5. 已接受 WR 全部获得可信终结记录、双方确认静默后，客户端和健康源服务端主进程分别释放导入对象、队列、MR、context 和动态库，交换关闭标记后才输出 PASS。

故障基线成功不能被故障阶段的错误替代；仅 TCP 断开或 SIGKILL 调用成功也不能替代 `waitpid` 的退出证据。工具不修改 SQ 为 ERROR、不重新创建 SQ，也不重放或重新投递失败 READ。

客户端打印六组独立计数：

- `Healthy baseline`、`Fault baseline`：健康源和故障源各自的成功基线。
- `Healthy before`、`Healthy mixed`、`Healthy after`：三个阶段的健康 READ，必须全部成功且数据校验通过。
- `Fault mixed`：旧目标 READ，已接受 WR 必须全部恰好退休一次，实际 CQE 错误状态逐项打印，不预设 status 8。

每组 `terminal_status[N]` 是原始 CQE 状态直方图，只打印非零项。`verified` 表示完成数据校验的成功 READ，包含故障发生前的源基线；错误 READ 的目的 payload 不参与成功数据校验，只检查保护区。失败 READ 可能部分修改目的区，不能将其内容作为有效数据使用。

`Same SQ` 打印 SQ ID、状态查询次数与返回码、最后状态、post 返回值、部分接受次数、`accepted_known`、`pending` 和 `peer_exit_confirmed`。正常通过时客户端应满足：

```text
accepted_known=true
pending=false
peer_exit_confirmed=true
last_state=1
```

所有健康 READ 的 `accepted=retired=verified`；故障混合 READ 的 `accepted=retired`，没有成功 CQE。服务端提供被动 READ 源，因此不提交 READ，其发送计数为零是正常现象。

只有故障 READ 如实错误终结、健康 READ 全部成功、同 SQ 始终 READY、保护区正确且双方正常关闭资源，才输出：

```text
PASS: old-target READs retired once with observed error statuses after confirmed SIGKILL/waitpid; healthy READs and guards validated before/during/after on the same READY SQ; no SQ modification/recreation or replay; both endpoints closed resources
```

若 post 只接受前缀，工具识别并跟踪已接受 WR，不重试剩余部分；该实验不能按完整成功计。若接受范围未知、CQE 身份无效、完成缺失、健康 READ 失败或控制确认缺失，返回失败并保留尚未证明可释放的 DMA 依赖直到 worker 退出，禁止猜测完成数量或提前注销 MR。

退出码：`0` 表示上述完整验证通过，`1` 表示实验或 provider/API 失败，`2` 表示参数错误，`124` 表示外层 watchdog 超时。原始错误状态是本轮观测，不作为其他 provider/固件版本的固定承诺。

## 覆盖边界

- 故障严格发生在混合 READ 提交之前，不声称终止进程时有 WR 正在硬件中执行。
- 同一 SQ 有多个软件未退休 WR，不代表硬件并行度或乱序程度已经被测量。
- 当前不覆盖 WRITE、SEND 与 READ 的跨操作顺序、MR 主动注销/权限撤销、真实链路故障、JFC 出错导致完成记录缺失后的资源回收。
- 错误目标的终结状态、健康目标的成功结果和实际资源关闭应一起记录到[硬件验证矩阵](../../docs/hardware-validation-matrix.md)，不能只凭最后一行 PASS 推断错误原因。
