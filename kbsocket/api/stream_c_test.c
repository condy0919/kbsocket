// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream.h"

// 在 C 编译单元校验 ABI 布局并链接实际 C 符号，不依靠 C++ 的兼容解析。
_Static_assert(KBS_STREAM_ABI_VERSION == 1u, "unexpected ABI");
_Static_assert(sizeof(kbs_stream_handle) == 24, "handle layout");
_Static_assert(sizeof(kbs_stream_error) == 24, "error layout");
_Static_assert(sizeof(kbs_stream_io) == 16, "io layout");
_Static_assert(sizeof(kbs_stream_status) == 56, "status layout");
_Static_assert(sizeof(kbs_stream_event) == 80, "event layout");

int KbsStreamCHeaderTest(void) {
    const kbs_stream_handle stream = {0};
    kbs_stream_io io = {99, 1, 1};
    kbs_stream_error error = {0};
    kbs_stream_status status = {0};
    kbs_stream_event event = {0};
    size_t count = 99;
    if (kbs_stream_write(NULL, stream, NULL, 0, &io, &error) != KBS_STREAM_INVALID_ARGUMENT || io.bytes != 0 ||
        io.eof != 0 || error.code != KBS_STREAM_INVALID_ARGUMENT) {
        return 1;
    }
    if (kbs_stream_read(NULL, stream, NULL, 0, 0, &io, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_inspect(NULL, stream, &status, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_shutdown_write(NULL, stream, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_watch_tcp(NULL, stream, 0, 0, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_watch(NULL, stream, 0, 0, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_poll_events(NULL, &event, 1, &count, NULL) != KBS_STREAM_INVALID_ARGUMENT || count != 0 ||
        kbs_stream_cancel(NULL, stream, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_close(NULL, stream, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_stream_release(NULL, stream, NULL) != KBS_STREAM_INVALID_ARGUMENT) {
        return 2;
    }
    return 0;
}
