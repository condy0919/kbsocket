// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_STREAM_RUNTIME_H_
#define KBSOCKET_API_STREAM_RUNTIME_H_

#include <sys/socket.h>

#include "kbsocket/api/stream.h"

#ifdef __cplusplus
extern "C" {
#endif

// 独立 NativeTCP Runtime；只接受数值 IPv4/IPv6 地址，不做 DNS、协议探测或自动跨后端切换。
// Create/Destroy/建链为控制路径；所有入口由同一 owner 串行调用，不允许回调或跨线程重入。
typedef struct kbs_runtime kbs_runtime;
typedef struct kbs_runtime_config {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t table_id;
    // stream: 1..65536；listener: 0..65536（0 禁用监听）。
    uint32_t stream_capacity;
    uint32_t listener_capacity;
    // I/O: 1..1MiB；events: 0/1；budget: 1..64；wait: 0..1000ms。
    uint32_t max_io_bytes;
    uint32_t events;
    uint32_t progress_budget;
    uint32_t max_wait_ms;
} kbs_runtime_config;

typedef struct kbs_listener_handle {
    uint64_t runtime_id;
    uint64_t generation;
    uint32_t slot;
    uint32_t reserved;
} kbs_listener_handle;
typedef struct kbs_runtime_progress {
    uint64_t processed;
    uint32_t more_work;
    uint32_t pending_events;
} kbs_runtime_progress;

// 初始化默认配置；table_id 默认 0，调用方必须填进程内跨实例不复用的非零身份。
int32_t kbs_runtime_config_init(kbs_runtime_config* config, kbs_stream_error* error);
int32_t kbs_runtime_create(const kbs_runtime_config* config, kbs_runtime** out, kbs_stream_error* error);
// 表借用至 Destroy；Runtime Close 后旧表指针仍可安全返回 NOT_READY，Destroy 后不得访问。
int32_t kbs_runtime_streams(kbs_runtime* runtime, kbs_stream_table** out, kbs_stream_error* error);
// wait=0 为非阻塞 Tick；wait=1 最多等待配置的 max_wait_ms。已有应用事件时绝不再次睡眠。
// 只推进 stream；监听器由应用显式 Accept，不自动接收也不产生监听就绪事件。
int32_t kbs_runtime_step(kbs_runtime* runtime, uint32_t wait, kbs_runtime_progress* out, kbs_stream_error* error);
uint64_t kbs_runtime_now_ns(void);

// fd 必须是已连接的非阻塞 TCP；接管后立即写回 -1。未接管失败保持 fd 原值。
// 极少数发布/回滚错误也可能已接管；以 *fd 判定所有权，不得重复关闭已转移的描述符。
int32_t kbs_runtime_adopt_tcp(kbs_runtime* runtime, int* fd, kbs_stream_handle* out, kbs_stream_error* error);
// 成功返回已发布句柄，可能仍 CONNECTING；用状态事件观察激活/失败，不在后台重拨。
// deadline_ns 与 now_ns 同源且严格晚于当前时刻，超时由后续 Step 推进。
int32_t kbs_runtime_connect_tcp(kbs_runtime* runtime, const struct sockaddr* address, socklen_t length,
                                uint64_t deadline_ns, kbs_stream_handle* out, kbs_stream_error* error);
int32_t kbs_runtime_listen_tcp(kbs_runtime* runtime, const struct sockaddr* address, socklen_t length, int32_t backlog,
                               kbs_listener_handle* out, kbs_stream_error* error);
// 每次最多接收一个连接；WOULD_BLOCK 不分配 stream，容量不足不取走内核排队连接。
int32_t kbs_runtime_accept_tcp(kbs_runtime* runtime, kbs_listener_handle listener, kbs_stream_handle* out,
                               kbs_stream_error* error);
// 与 getsockname 相同的地址容量规则，可取得绑定端口 0 后的实际地址。
int32_t kbs_runtime_listener_address(kbs_runtime* runtime, kbs_listener_handle listener, struct sockaddr* address,
                                     socklen_t* length, kbs_stream_error* error);
// 有效监听句柄一经关闭即失效，即使 Linux close 报错也不重试同一 fd。
int32_t kbs_runtime_close_listener(kbs_runtime* runtime, kbs_listener_handle listener, kbs_stream_error* error);
// 停止新准入；仍有已发布 stream 或 listener 返回 BUSY，保留 Step/关闭/Release 能力。
// 不强制关闭应用连接。全部责任已显式释放后可重试，成功后不允许继续 Step。
int32_t kbs_runtime_close(kbs_runtime* runtime, kbs_stream_error* error);
// 仅 Closed 可销毁；失败保留指针，成功写回 NULL。NULL 指针值允许重复销毁。
int32_t kbs_runtime_destroy(kbs_runtime** runtime, kbs_stream_error* error);

#ifdef __cplusplus
}
#endif
#endif // KBSOCKET_API_STREAM_RUNTIME_H_
