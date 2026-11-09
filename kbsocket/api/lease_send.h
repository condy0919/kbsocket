// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_LEASE_SEND_H_
#define KBSOCKET_API_LEASE_SEND_H_

#include "kbsocket/api/buffer_lease.h"
#include "kbsocket/api/stream.h"

#ifdef __cplusplus
extern "C" {
#endif

// RM_CTP Eager 源 Lease 发送，只接受当前 raw 栈共享注册 Slab 的应用 Lease。
// NativeTCP 返回 NOT_SUPPORTED。offset/length 必须是本次 Lease 的有效非空区间。
// 返回成功表示有界前缀进入发送队列，out.bytes 不是 provider 接受数，也不表示远端已收到。
// 不复制 payload；调用方保留 CPU 句柄，但成功后必须停止访问整份 Lease 的全部旧视图。
// 排队时即预留源责任，直到全部分片可信终结或未提交分片取消才允许再次取视图/提交尾部。
// 用 buffer_inspect 的 source_reusable 查询复用条件；stream 错误/取消本身不证明源可复用。
// 应用也可 release 放弃 CPU 句柄，底层仍等待 DMA 终结；此后不可再查询原句柄。
// 扩展 wire 身份需要配置至少两个发送 SGE；不足时显式失败，不退化为 payload 拷贝。
int32_t kbs_stream_write_lease(kbs_stream_table* table, kbs_stream_handle stream, kbs_buffer_table* buffers,
                               kbs_buffer_handle buffer, size_t offset, size_t length, kbs_stream_io* out,
                               kbs_stream_error* error);

#ifdef __cplusplus
}
#endif
#endif // KBSOCKET_API_LEASE_SEND_H_
