// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_LEASE_RECEIVE_H_
#define KBSOCKET_API_LEASE_RECEIVE_H_

#include <stddef.h>
#include <stdint.h>

#include "kbsocket/api/stream.h"

#ifdef __cplusplus
extern "C" {
#endif

// 接收视图借用的是 RX 池，不是可写 BufferBinding 句柄；复制描述符不会增加所有权。
typedef struct kbs_receive_lease {
    kbs_stream_handle stream;
    uint64_t generation;
} kbs_receive_lease;
typedef struct kbs_receive_view {
    kbs_receive_lease lease;
    const void* data;
    size_t size;
    uint32_t eof;
    uint32_t reserved;
} kbs_receive_view;

// 仅 raw Eager；NativeTCP 显式返回 NOT_SUPPORTED。全部入口由唯一 owner 串行调用。
// max_bytes 非零；返回当前连续帧的有界前缀，不复制 payload、不轮询 CQ。
// 每流至多一个借出视图，归还前其他借用/非空 copy-read 返回 BUSY。
// 无连续数据返回 WOULD_BLOCK；实际有序 FIN 返回 eof=1，data/size/lease 全零，无须归还。
// 应用持有期间不归还该帧 RX/receipt，取消和 Close 不使视图失效；Close 保留到显式归还。
// out 必填，错误输出清零；视图仅限只读，宿主交出 context 控制所有权时暂停外部访问。
int32_t kbs_stream_read_lease(kbs_stream_table* table, kbs_stream_handle stream, size_t max_bytes, uint64_t now_ns,
                              kbs_receive_view* out, kbs_stream_error* error);
// 归还整个借出前缀；完整一帧归还后才释放 RX 与重排责任，实际补投后才恢复物理授信。
// Failed/Closing 仍允许归还；验证身份或暂停门禁失败不改变视图。
// 合法归还开始后旧指针立即失效，即使后续清理返回错误；保留 token 供重试，成功后 token 失效。
int32_t kbs_stream_release_read_lease(kbs_stream_table* table, kbs_receive_lease lease, kbs_stream_error* error);

#ifdef __cplusplus
}
#endif
#endif // KBSOCKET_API_LEASE_RECEIVE_H_
