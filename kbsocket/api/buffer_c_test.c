// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/buffer_lease.h"

// 真正通过 C11 编译并调用全部 C 符号，验证公开结构没有泄漏 C++/URMA 类型。
_Static_assert(KBS_BUFFER_ABI_VERSION == 1u, "buffer ABI");
_Static_assert(sizeof(kbs_buffer_status) == 16, "buffer status layout");
_Static_assert(sizeof(kbs_buffer_handle) == 24, "buffer handle layout");
_Static_assert(offsetof(kbs_buffer_handle, generation) == 8, "full generation layout");
_Static_assert(offsetof(kbs_buffer_handle, slot) == 16, "buffer slot layout");
_Static_assert(sizeof(kbs_buffer_pool_config) == 40, "pool config layout");
_Static_assert(offsetof(kbs_buffer_pool_config, buffer_bytes) == 24, "pool length layout");
_Static_assert(sizeof(kbs_buffer_view) == sizeof(void*) + sizeof(size_t), "mutable view layout");
_Static_assert(sizeof(kbs_buffer_const_view) == sizeof(void*) + sizeof(size_t), "readonly view layout");

int KbsBufferCHeaderTest(void) {
    kbs_buffer_handle handle = {1, 2, 3, 4};
    char sentinel = 0;
    kbs_buffer_view writable = {&sentinel, 77};
    kbs_buffer_const_view readable = {&sentinel, 88};
    kbs_stream_error error = {0};
    kbs_buffer_status status = {99, 1, 1};
    if (kbs_buffer_acquire(NULL, 16, &handle, &error) != KBS_STREAM_INVALID_ARGUMENT || handle.table_id != 0 ||
        handle.generation != 0 || handle.slot != 0 || handle.reserved != 0 ||
        error.code != KBS_STREAM_INVALID_ARGUMENT) {
        return 1;
    }
    if (kbs_buffer_writable(NULL, handle, &writable, NULL) != KBS_STREAM_INVALID_ARGUMENT || writable.data != NULL ||
        writable.size != 0) {
        return 2;
    }
    if (kbs_buffer_readable(NULL, handle, &readable, NULL) != KBS_STREAM_INVALID_ARGUMENT || readable.data != NULL ||
        readable.size != 0) {
        return 3;
    }
    if (kbs_buffer_release(NULL, handle, NULL) != KBS_STREAM_INVALID_ARGUMENT) {
        return 4;
    }
    if (kbs_buffer_inspect(NULL, handle, &status, &error) != KBS_STREAM_INVALID_ARGUMENT || status.bytes != 0 ||
        status.source_reusable != 0 || status.reserved != 0 || error.code != KBS_STREAM_INVALID_ARGUMENT) {
        return 5;
    }
    return 0;
}
