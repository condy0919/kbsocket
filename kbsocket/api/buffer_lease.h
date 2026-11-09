// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_BUFFER_LEASE_H_
#define KBSOCKET_API_BUFFER_LEASE_H_

#include <stddef.h>
#include <stdint.h>

#include "kbsocket/api/stream.h"

#ifdef __cplusplus
extern "C" {
#endif

// 与 stream 共用错误分类；唯一 owner 串行调用。复制句柄不会增加 CPU 所有权。
#define KBS_BUFFER_ABI_VERSION 1u
typedef struct kbs_buffer_table kbs_buffer_table;
typedef struct kbs_buffer_handle {
    uint64_t table_id;
    uint64_t generation;
    uint32_t slot;
    uint32_t reserved;
} kbs_buffer_handle;
typedef struct kbs_buffer_view {
    void* data;
    size_t size;
} kbs_buffer_view;
typedef struct kbs_buffer_const_view {
    const void* data;
    size_t size;
} kbs_buffer_const_view;

// NativeTCP 可选 CPU 内存池；初始化时分配并预触页，不向 URMA 注册或承诺 DMA 能力。
// table_id 由宿主保证跨表/生命周期不复用。预算只计页取整 backing，不含元数据。
typedef struct kbs_buffer_pool_config {
    uint32_t abi_version;
    uint32_t reserved;
    uint64_t table_id;
    uint32_t capacity;
    uint32_t reserved2;
    uint64_t buffer_bytes;
    uint64_t memory_budget_bytes;
} kbs_buffer_pool_config;

// 长度必须非零且能装入一个池槽；成功借出唯一 CPU Lease，不提交任何传输。
int32_t kbs_buffer_acquire(kbs_buffer_table* table, size_t bytes, kbs_buffer_handle* out, kbs_stream_error* error);
// 视图不增加引用，有效范围等于本次 Acquire 长度。Release、传输责任预留或隔离后旧指针失效；
// 调用方必须停止访问，不能靠保存的指针绕过后续检查。DMA/Grant 未终结时拒绝新视图。
int32_t kbs_buffer_writable(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_buffer_view* out,
                            kbs_stream_error* error);
int32_t kbs_buffer_readable(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_buffer_const_view* out,
                            kbs_stream_error* error);
// 成功立即失效全部句柄/视图副本；仅归还 CPU 所有权，不代表 DMA/Grant 已结束或 backing 可复用。
// 失败保留句柄供 owner 重试；跨线程归还须交给 owner，不得并发调用此入口。
int32_t kbs_buffer_release(kbs_buffer_table* table, kbs_buffer_handle buffer, kbs_stream_error* error);

#ifdef __cplusplus
}
#endif
#endif // KBSOCKET_API_BUFFER_LEASE_H_
