// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_STREAM_H_
#define KBSOCKET_API_STREAM_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// V1 copy-stream ABI；表由 C++ 控制面建立，所有入口由唯一 owner 串行调用，不得重入。
// 本接口不初始化设备、不推进 Runtime、不自动重连；跨线程调用须先转交 owner。
#define KBS_STREAM_ABI_VERSION 1u
typedef struct kbs_stream_table kbs_stream_table;
typedef struct kbs_stream_handle {
    uint64_t table_id;
    uint64_t generation;
    uint32_t slot;
    uint32_t reserved;
} kbs_stream_handle;

// 返回 0 表示成功；失败返回下列正值，不依赖 errno，也不返回负字节数。
#define KBS_STREAM_INVALID_ARGUMENT 1
#define KBS_STREAM_NOT_READY 2
#define KBS_STREAM_BUSY 3
#define KBS_STREAM_NO_MEMORY 4
#define KBS_STREAM_EXHAUSTED 5
#define KBS_STREAM_STOPPED 6
#define KBS_STREAM_INVALID_HANDLE 7
#define KBS_STREAM_WOULD_BLOCK 8
#define KBS_STREAM_CLOSED 9
#define KBS_STREAM_CANCELLED 10
#define KBS_STREAM_PEER_RESET 11
#define KBS_STREAM_PROTOCOL 12
#define KBS_STREAM_TRANSPORT 13
#define KBS_STREAM_RETAINED 14
#define KBS_STREAM_TIMEOUT 15

#define KBS_STREAM_ORIGIN_API 0u
#define KBS_STREAM_ORIGIN_OWNER 1u
#define KBS_STREAM_ORIGIN_CONNECTION 2u
#define KBS_STREAM_ORIGIN_PROGRESS 3u
#define KBS_STREAM_ORIGIN_SOURCE 4u
#define KBS_STREAM_ORIGIN_STACK 5u

// code 是稳定分类；其余字段仅诊断，native_code 是本版本对应内部组件枚举值。
// detail/underlying 保留原始状态（例如 provider status 9）；不得据此推测 DMA 已终结。
typedef struct kbs_stream_error {
    int32_t code;
    uint32_t origin;
    int32_t native_code;
    int32_t detail;
    int32_t underlying;
    uint32_t reserved;
} kbs_stream_error;
typedef struct kbs_stream_io {
    uint64_t bytes;
    uint32_t eof;
    uint32_t reserved;
} kbs_stream_io;

#define KBS_STREAM_IDLE 0u
#define KBS_STREAM_NEGOTIATING 1u
#define KBS_STREAM_PREPARING 2u
#define KBS_STREAM_WAITING_READY 3u
#define KBS_STREAM_RAW_ACTIVE 4u
#define KBS_STREAM_TCP_ACTIVE 5u
#define KBS_STREAM_FAILED 6u
#define KBS_STREAM_CLOSING 7u
#define KBS_STREAM_STATE_CLOSED 8u
typedef struct kbs_stream_status {
    uint32_t state;
    uint32_t lane_update_pending;
    kbs_stream_error failure;
    uint64_t tx_pending;
    uint64_t rx_pending;
    uint64_t retained_sources;
} kbs_stream_status;

// out 必须非空；失败时归零，error 可空且每次覆盖。正数前缀不会被后续错误覆盖。
// 单次最多处理绑定表的 max_io_bytes；write 返回的前缀可立即复用，不代表远端已消费。
// 零长度允许 data=nullptr；空 read 不探测 EOF，eof=0。正长度 read 的 EOF 与 EAGAIN 分开。
int32_t kbs_stream_write(kbs_stream_table* table, kbs_stream_handle stream, const void* data, size_t length,
                         kbs_stream_io* out, kbs_stream_error* error);
// now_ns 使用 Runtime 同源的单调时钟，仅由 owner 提供。
int32_t kbs_stream_read(kbs_stream_table* table, kbs_stream_handle stream, void* data, size_t length, uint64_t now_ns,
                        kbs_stream_io* out, kbs_stream_error* error);
int32_t kbs_stream_shutdown_write(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error);
int32_t kbs_stream_cancel(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error);
// NativeTCP 一次性 readiness 订阅；raw 调用明确返回错误，不伪造原生 fd。
int32_t kbs_stream_watch_tcp(kbs_stream_table* table, kbs_stream_handle stream, uint32_t readable, uint32_t writable,
                             kbs_stream_error* error);
int32_t kbs_stream_inspect(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_status* out,
                           kbs_stream_error* error);
// close 可丢弃未提交/未读数据；优雅半关闭先用 shutdown_write 并持续推进。
// Retained 必须继续真实完成收敛后重试；成功也不撤回授信或退休来源，句柄仍有效。
int32_t kbs_stream_close(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error);
// 仅在 Closed 且宿主已独立退休所有来源后成功；成功即失效，旧句柄不得命中新连接。
int32_t kbs_stream_release(kbs_stream_table* table, kbs_stream_handle stream, kbs_stream_error* error);

#ifdef __cplusplus
}
#endif
#endif // KBSOCKET_API_STREAM_H_
