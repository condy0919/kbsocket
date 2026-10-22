# JFC/JFR 错误、关联事件与共享资源隔离观测

本工具观察指定 RX/TX JFC 或 JFR 的错误 AE、两个关联 jetty 的事件及状态，以及同 context 下另一组独立 JFC/JFR/jetty 是否继续收发。它与只观察 JFR_LIMIT 的 [async_event_test](../async_event_test/README.md) 分开。使用裸设备 RM_CTP、非 inline、逐 WR signal 的 64 字节 SEND_WITH_IMM，不包含 bonding 或软件重放。

## 构建与运行

```bash
USE_BAZEL_VERSION=9.3.0 bazel build //tools/async_error_test:async_error_test
USE_BAZEL_VERSION=9.3.0 bazel test //tools/async_error_test:async_error_unit_test --test_output=errors

# 服务端：持续消费故障 RX CQ，设备/EID 按现场替换。
./async_error_test --server --address=0.0.0.0 --port=8889 \
  --device=udmac0d1e4 --eid_index=39 --fault=tx_jfc_overflow \
  --attempts=512 --cq_depth=64 --healthy_rounds=16 --observe_ms=2000

# 客户端：本次预定故障对象为此端的 fault_tx_jfc。
./async_error_test --address=141.62.33.25 --port=8889 \
  --device=udmac0d1e4 --eid_index=52 --fault=tx_jfc_overflow \
  --attempts=512 --cq_depth=64 --healthy_rounds=16 --observe_ms=2000
```

两端共同选择模式与参数，使用支持 **AEE2 协议**的同版本二进制；旧协议会被拒绝。TCP 只交换端点、参数和阶段结果，不传 payload。`attempts` 是两个故障目标合计的实验 WR 数，另有两条 baseline；必须是 128..4096 的偶数。每条有独立身份和内存，两个发送/接收 jetty 交替配对且共用对应 JFR/JFC。健康组有 `healthy_rounds+2` 条，覆盖之前、窗口内和之后，拥有独立 JFC/JFR。

| 模式 | 注入方式 | 判定所需事件 |
| --- | --- | --- |
| `jfc_overflow` | 请求较小故障 RX JFC，服务端窗口内不 poll 此 CQ；客户端有限量提交真实 SEND。正常收集 async，并独立推进健康队列 | 服务端指定 **RX JFC** 的 JFC_ERR |
| `tx_jfc_overflow` | 仅客户端请求较小故障 TX JFC，窗口内暂停 poll 此 CQ；服务端故障 RX CQ 足量配置且持续 poll，双端独立推进健康队列和 async。无需外部注入工具 | 客户端指定 **TX JFC** 的 JFC_ERR |
| `external_jfc` | 两端 `--observe_ms=30000`，看到服务端 INJECTION_WINDOW 后，使用现场已经确认的注入方式作用于打印的 fault_rx_jfc | 同上 |
| `external_jfr` | 同上，目标改为打印的 fault_jfr | 服务端指定 JFR 的 JFR_ERR |

已有 RX 溢出实验可将两端共同改为 `--fault=jfc_overflow`。外部模式共同替换为 `--fault=external_jfc --observe_ms=30000` 或 `--fault=external_jfr --observe_ms=30000`，待现场有可靠注入工具时再测。工具不执行任意 shell 命令，不包含未经确认的驱动私有注入指令。外部模式在窗口内分批向故障目标继续提交新的 WR，以观察关联 jetty 事件；不是要求操作者在一次极短突发之前完成注入。打印的对象 ID 只对本次进程有效。故障注入方式本身必须由现场确认，不能以编造一个用户态事件代替硬件故障。

自动模式只是 **CQ 溢出候选实验**。provider 可能把深度取整、拒绝配置或采用其他处理，暂停 poll 不保证产生 AE。`requested_tx_cq/requested_rx_cq` 是该端实际请求值，`reported_tx_cq/reported_rx_cq` 来自 URMA 公共对象的配置字段，不是独立测量的硬件物理容量。上述 TX 命令的客户端请求 TX=64、RX=514，服务端请求 TX=516、RX=514；RX 模式则只有服务端的故障 RX CQ 请求为 64。需要时可将 attempts 增为 1024 或 4096；超过设备能力直接失败，不偷偷调整参数。

## 实验步骤与记录

1. 建立两条故障 jetty 和一条健康 jetty，注册独立 buffer/保护区，投递共享 RX；两条故障 baseline 及健康 baseline 均须成功。基线阶段出现 AE 则失败。
2. 打印 INJECTION_WINDOW 中的 `target_role/target_object`、故障/健康 TX/RX JFC 以及 JFR/jetty ID，执行选定模式。TX 溢出模式仅暂停客户端故障 TX CQ 的消费，服务端持续消费故障 RX CQ；RX/外部模式保持服务端故障 RX CQ 暂停消费。双端始终采集 async，并验证独立健康队列的 payload、IMM、来源、长度和保护区。
3. 有界窗口结束后，被测对象所属端先锁定窗口内的故障观察结果，再通过控制消息协调健康 after 流量，随后查询 jetty/JFR 状态。TX 模式由客户端锁定，RX/JFR 模式由服务端锁定。迟到到 after 阶段的事件单独列出，不能追认故障后隔离已成立。查询失败记录返回码和状态 -1；不把未知状态解释为 READY。
4. 到此才恢复此前暂停消费的故障 CQ；TX 模式先收集现有 CQE，避免仅因软件尚未消费完成就主动置 ERROR。客户端仍有未终结 WR 时，封住本工具的新提交并尝试逐 SQ modify ERROR；取得各自 FLUSH_ERR_DONE 后，仅对仍有未终结 WR 的 SQ 调用 flush。主动清理产生的 AE 单独记入 cleanup，不能反过来作为实验注入成功证据。
5. 双端交换终结/观察结果。只有双方都具备完整终结证据才正常删除资源、注销 MR 和关闭 runtime；否则全部依赖保留至 worker 退出。

async 的类型、context 和对象归属在 Ack 前快照；成功 Get 的原事件恰好 Ack 一次。不解引用事件里的对象指针。JFC_ERR 分开统计 fault_rx/fault_tx，不能把别的 CQ 出错记成预定目标。此工具创建的是 jetty，其 SQ 关联事件按 JETTY_ERR 归属；独立 JFS_ERR 仍按原始类型列出，不强制映射成 jetty。其他事件保留计数，不自动推断 PORT_DOWN 或整个设备故障。

TX 溢出模式关注客户端 observation 的 `fault_tx_jfc`、两条 `jetty` 事件及前后状态，服务端 RX 终结数量和双方健康组结果。`fault_poll_calls_in_window` 的客户端应为 0、服务端应大于 0，证明暂停的是发送 CQ、接收侧仍在推进。关联 jetty/JFS AE 仅作为各自的观测结果，不能替代预定 TX JFC_ERR，也不要求二者一定发生。窗口后才恢复客户端故障 TX CQ 的消费；缺失的 TX 完成仍不能由服务端成功接收推定已本地退休。

`cleanup_final` 是关闭前最后一次采集后的累计值，覆盖此前的 `cleanup`，两者不能相加；只有进入正常关闭路径时才有该行。

TX `accepted/retired` 与 RX `posted/rx_retired/received` 分开：错误 RX 可以有可信终结但没有成功 payload。每个终结身份只计一次；硬件伪 CQE 不访问 user_ctx。故障 poll 返回错误会记录并停止重复 poll，无法取得的完成不会被补造。部分 post 拒绝只承认明确接受的 WR，不重放未决数据；未知 bad_wr 保留整个依赖链并失败。

## 输出与退出码

- `OBSERVED`：预定客户端 TX JFC、服务端 RX JFC 或服务端 JFR 的错误事件在观察窗口内实际取得并 Ack；关联 jetty AE 分别列出，不要求每个 jetty 必定都有事件。
- `NOT_OBSERVED`、退出 77：本轮没有建立预定错误的证据。即使健康流量成功，也不是错误行为验证通过。
- `PASS`、退出 0：预定错误已观察到，独立健康组在之前/窗口内/之后正常工作，控制协议完成。必须结合下一项资源结论阅读。
- `CLOSED`：双方取得完整终结证据并正常关闭资源。
- `RETAINED`：缺少终结依据或故障 CQ 已不可用，双方保留注册内存、队列、context 和库至进程退出。它不表示排空或正常资源关闭已通过。不能据进程回收成功推导同步 DMA 终结契约。
- `FAIL`、退出 1：协议、身份、payload、健康流量或 API 检查失败；退出 124 为外部 watchdog 超时，均不是通过。

默认 `timeout_ms=10000`；观察窗口最多 60000 ms。父进程在加载 URMA 前 fork，以 `watchdog_ms` 覆盖同步 provider 调用和删除可能阻塞的情况；0 自动按窗口/轮数/操作截止推导，启动前校验最大一小时。故障资源均由 NoDestructor 保留，失败不运行可能提前释放 DMA 内存的析构。

这只能验证同 context 下独立队列组的隔离，不证明其他 context/物理设备完全独立，也不证明错误 CQ 中所有丢失终结都能通过软件 flush 重建。RX 模式 X1 已有硬件 `PASS + RETAINED` 记录；TX 模式 X4 为新增实验，硬件结果待记录。
