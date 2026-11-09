# M5.4.8：Eager 接收 Lease

[`lease_receive.h`](../kbsocket/api/lease_receive.h) 提供 raw Eager 接收数据的只读借用与显式归还。视图直接指向已经完成接收的注册 RX 缓冲区，不复制 payload。它与 copy-read 共用有序字节流、当前帧偏移及授信账本，每条流最多借出一个视图。

## 调用与身份

流达到 `RAW_ACTIVE` 后，由唯一 owner 调用：

```c
kbs_receive_view view = {0};
kbs_stream_error error = {0};
int32_t result = kbs_stream_read_lease(streams, stream, max_bytes, now_ns,
                                       &view, &error);
```

`max_bytes` 必须非零。一次借出当前有序帧剩余区间的前缀，长度不超过 `max_bytes` 和绑定表的 `max_io_bytes`，不会跨帧拼接。没有连续数据时返回 WOULD_BLOCK；乱序驻留不代表可读。实际有序 FIN 返回 `eof=1`、空指针和零 Lease，不需要归还。错误时输出清零。接口不推进 CQ，Runtime 仍由应用驱动。

`view.lease` 包含完整 stream 身份和表内不复用的 64 位借用代次。表将公开代次映射到内部 RX token，拒绝跨表、跨流移植、旧 stream、旧借用和重复归还。复制 view 或 token 不增加引用；借用归还成功后，全部描述符副本失效。

此 Lease 属于 RX 池，与 [BufferBinding](buffer-api.md) 分配的可写应用 Slab Lease 不同。不能通过 `kbs_buffer_release` 归还，也不能直接作为 `kbs_stream_write_lease` 的发送源。NativeTCP 显式返回 NOT_SUPPORTED，不临时复制出一个假零拷贝视图。

## 消费与背压

应用只读访问 `view.data[0..view.size)`，消费完毕后调用：

```c
result = kbs_stream_release_read_lease(streams, view.lease, &error);
```

归还消费整个借出前缀，不支持只归还视图的一部分；需要更小消费单位时，在借用时限制 `max_bytes`。持有期间，第二次借用和非空 copy-read 返回 BUSY，不使连接失败；新的 readiness 检查不报告可消费数据。既有事件只是提示，可能已过时。

copy-read 后可借用同一帧未读后缀；部分 Lease 归还后，也可继续 copy-read 或再次借用。偏移只推进一次，先后顺序不变。部分归还不释放物理 RX，不返还该帧重排窗口或物理授信。只有整帧消费完毕，才经 RX Release、物理 receipt 和重排 receipt 分别结账；实际 refill 被 provider 接受后，物理容量才重新可授信。

慢应用会占用已预分配的 RX 和有限重排窗口，背压保留在原账本中，不为保持进度额外分配内存，也不提前覆盖借出的 payload。

## 失败、暂停与关闭

取消、超时或连接失败本身不撤销已借出视图。Drain/Close 不抢回应用持有的 RX；Close 返回保留结果，连接及 Runtime 依赖继续存在。应用在 Failed 或 Closing 状态仍可显式归还，再重试关闭。若归还的是半帧，剩余未读后缀由后续关闭清理。

归还的暂停门禁或身份验证失败，不改变所有权，恢复后可重试。合法归还一旦进入底层清理，旧指针立即失效，即使后续因 RX/授信账务错误返回失败：部分物理责任可能已经释放。此时保留原 token，仅用于再次尝试清理，禁止继续读旧指针；实现记住已推进的偏移及清理阶段，避免重复消费。归还成功才使 token 失效。

所有操作遵守所属 raw context 的暂停门禁；宿主把 context 交给控制任务前也须暂停外部视图访问。借用没有赋予应用跨线程直接调用 owner 的权限。正常销毁前必须归还全部视图；异常析构保留未结清的元数据及依赖，不能用析构替代应用归还或硬件静默证明。

## 验证与边界

Mock 测试核对视图直接指向真实 RX backing，覆盖乱序与 FIN、copy/Lease 交替偏移、完整和部分归还、持有期间无补投/授信增长、取消及关闭保留、代次与跨流拒绝、暂停恢复和零分配。C11 ABI 与 NativeTCP 拒绝也有回归覆盖；完整结果见 [roadmap](roadmap.md)。

本阶段提供单 owner、每流一个只读视图，不包含跨线程接收 Lease 归还、多个并行借用或 WRITE RNDV 目标 Grant；现有通用 Slab 归还队列不接收本 RX token。UB 真机及 40k 性能验证未执行。
