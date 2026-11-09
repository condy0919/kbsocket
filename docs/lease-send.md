# M5.4.7：应用 Lease 的 Eager 零拷贝发送

[`lease_send.h`](../kbsocket/api/lease_send.h) 提供 `kbs_stream_write_lease`，从 [RawStreamRuntime](raw-stream-runtime.md) 的共享注册 Slab 直接引用应用源数据。它复用既有 Eager 顺序、授信、部分接受和逐 WR 完成路径。payload 不复制到发送 staging；扩展身份所需的协议头使用独立注册缓冲。UB 真机验证未执行。

## 调用与接受边界

全部操作由唯一 owner 串行调用。先通过 `OpenBuffers` 打开 [BufferBinding](buffer-api.md)，从 `buffers()` 分配 Lease、取得可写视图并填充数据；流达到 `RAW_ACTIVE` 后调用：

```c
kbs_stream_io sent = {0};
kbs_stream_error error = {0};
int32_t result = kbs_stream_write_lease(streams, stream, buffers, buffer,
                                        offset, length, &sent, &error);
```

`offset/length` 必须完全落在本次 Acquire 的有效范围内，长度非零。先验证完整范围，再按表的 `max_io_bytes`、发送记录容量和流字节窗口接受前缀。成功的 `sent.bytes` 是已进入库发送队列的应用字节数，尚未承诺 provider 已接受或远端已接收；资源不足且没有接受任何字节时返回 WOULD_BLOCK。失败输出清零。

源必须属于该流 RawStack 的同一共享注册 Slab，任何 context 暂停都禁止访问共享源。其他 Slab、过期句柄、越界或正在使用的源均被拒绝。NativeTCP 返回 NOT_SUPPORTED；CPU-only 池也不能作为 raw 源。入口不轮询 CQ，应用继续调用 Runtime `Step()`。

短身份帧直接用源区间作为一个 SGE，每帧最多 4096 B。扩展身份帧使用 8 B 独立头部及源区间两个 SGE，每片 payload 最多 4088 B；宿主须将 JettyPool 的 `send_sge` 配置为至少 2，能力不足时显式拒绝。普通 copy-write 仍使用原有路径，Lease 发送与 copy-write 共享同一字节序列和 FIN 顺序。

源片段及扩展头在入队时备齐责任，并至少保留一个临时责任槽供已排队复制帧和控制帧推进；责任表不足时短写或返回 WOULD_BLOCK。共享 Slab 的 `available_references` 计入 Reserved 与 Committed 占用，不以 provider 是否已接受来估算可用容量。

## 源责任与复用

每个接受入队的片段立即取得独立源责任，覆盖排队和提交等待。应用保留原 CPU 句柄，但从首次成功接受开始必须停止使用整份 Lease 的所有旧视图；即使只接受部分前缀，尚未接受的尾部也不能经旧指针修改或立即再次提交。待整份 Lease 可复用后，重新取得视图或从 `offset + sent.bytes` 提交尾部。

| 情况 | 源责任处理 |
| --- | --- |
| 缺远端授信、排队等待 | 保持源预留，防止应用归还后 backing 被复用 |
| provider 可信部分接受 | 前缀提交责任；未接受后缀继续排队持有预留，稍后仅提交后缀 |
| 取消或连接失败 | 取消明确未提交的排队片段；已提交部分继续等待逐 WR 终结 |
| 已校验的成功或错误 CQE | 释放对应片段本地 DMA 责任；错误仍按原规则报告到连接 |
| 接受范围未知 | 保留可能接受的全部责任并隔离源及协议头，不重放 |
| 应用提前 `kbs_buffer_release` | 仅归还 CPU；排队或已提交责任仍阻止 backing 复用 |

使用 `kbs_buffer_inspect` 查询整份 Lease 的 `source_reusable`。返回成功且值为 1，表示当前没有阻止 CPU 访问的 DMA/Grant 或隔离责任；值为 0 表示仍需等待。暂停、无效句柄或已关闭的表返回错误，不把访问失败伪装成“仍在发送”。停止新准入后查询仍可使用，但重新取得可写视图仍受停止门禁限制。

连接成功/失败与源可复用是独立信息。`TX_RETIRED` 是连接聚合事件，不能用它推算某个 Lease 已可复用；也不能凭取消、超时或 Close 失败释放已提交责任。接受未知后的隔离可能在 DMA 终结后仍使 `source_reusable=0`；应用可归还 CPU 句柄，底层在全部责任结清后自行回收。成功归还后原句柄失效，不再查询或访问。

源可复用不代表远端收到或消费，也不终结未来的 WRITE Grant。M5.4.7 尚未提供接收 Lease 借用、跨线程 Lease 发送 mailbox 或 M6 RNDV。

## 验证

Mock 测试核对实际 provider SGE 地址直接指向应用注册区间，并验证分片与扩展头拼接、可信部分接受和重试、接受未知隔离、提前 CPU 归还、取消、终结错误及无数据路径分配。应用层覆盖 C ABI、范围与身份校验、NativeTCP 拒绝、raw 多 context 暂停和 Runtime 关闭保留。完整回归结果记录在 [roadmap](roadmap.md)，不据此推导 UB 吞吐、延迟或规模结论。
