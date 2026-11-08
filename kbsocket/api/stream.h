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

#define KBS_STREAM_EVENT_STATE 1u
#define KBS_STREAM_EVENT_READABLE 2u
#define KBS_STREAM_EVENT_WRITABLE 4u
#define KBS_STREAM_EVENT_RX_CHANGED 8u
#define KBS_STREAM_EVENT_TX_RETIRED 16u
#define KBS_STREAM_EVENT_PEER_SHUTDOWN 32u
#define KBS_STREAM_EVENT_ERROR 64u
#define KBS_STREAM_EVENT_CLOSED 128u
#define KBS_STREAM_EVENT_COUNTER_SATURATED 256u
#define KBS_STREAM_EVENT_BATCH_MAX 64u
// 每个已发布句柄最多一个待取事件。flags 合并曾发生的提示，其余字段为最新快照；
// TX_RETIRED 累加真实退休数量，溢出时饱和并置 COUNTER_SATURATED。不是逐次状态历史。
// READABLE/WRITABLE 不保证下一次 I/O 有进展；rx_pending 可能含乱序数据。
// PEER_SHUTDOWN 是 TCP fd 提示，不等于 EOF；read_eof 只在实际 Read 确认后置位。
// ERROR 也可能仅来自 TCP fd ERR，此时 failure.code=0；用实际 I/O/Inspect 获取确定错误。
// write_closed 仅表示本地写方向关闭，不证明远端已消费 FIN。
typedef struct kbs_stream_event {
    kbs_stream_handle stream;
    uint32_t flags;
    uint32_t state;
    kbs_stream_error failure;
    uint64_t rx_pending;
    uint64_t tx_retired;
    uint32_t read_eof;
    uint32_t write_closed;
} kbs_stream_event;

// 整体一次性订阅：任一方向满足即消耗本次读写订阅，重新订阅必须显式调用 Watch。
// raw 立即检查连续数据/FIN 及本地写缓冲；TCP 由原生 fd readiness 驱动。
// (0,0) 取消后续就绪订阅，不撤回已排队提示。状态、RX 变化和 TX 退休不依赖订阅。
int32_t kbs_stream_watch(kbs_stream_table* table, kbs_stream_handle stream, uint32_t readable, uint32_t writable,
                         kbs_stream_error* error);
// owner 在 Runtime 推进后调用；非阻塞，不推进 Runtime，不重新 arm，无新增事件 fd。
// capacity 必须大于 0；每次至多交付 BATCH_MAX 条，count 必填且失败时清零。
// Release 成功会撤销尚未取出的事件；旧代次事件不得用于新连接。
int32_t kbs_stream_poll_events(kbs_stream_table* table, kbs_stream_event* events, size_t capacity, size_t* count,
                               kbs_stream_error* error);

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
// 兼容的 NativeTCP 专用 Watch；raw 调用明确返回错误，不伪造原生 fd。
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
