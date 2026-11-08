// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/stream_runtime.h"

_Static_assert(sizeof(kbs_runtime_config) == 40, "runtime config layout");
_Static_assert(sizeof(kbs_listener_handle) == 24, "listener layout");
_Static_assert(sizeof(kbs_listener_event) == 32, "listener event layout");
_Static_assert(sizeof(kbs_runtime_progress) == 16, "progress layout");

int KbsStreamRuntimeCHeaderTest(void) {
    kbs_runtime_config config = {0};
    kbs_runtime* runtime = NULL;
    kbs_stream_table* table = NULL;
    kbs_stream_handle stream = {99, 99, 99, 99};
    kbs_listener_handle listener = {0};
    kbs_runtime_progress progress = {99, 99, 99};
    kbs_listener_event listener_event = {0};
    size_t count = 99;
    kbs_stream_error error = {0};
    int fd = -1;
    if (kbs_runtime_config_init(&config, &error) != 0 || config.abi_version != KBS_STREAM_ABI_VERSION ||
        config.struct_size != sizeof(config) || config.table_id != 0 || error.code != 0) {
        return 1;
    }
    if (kbs_runtime_config_init(NULL, &error) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_create(NULL, &runtime, &error) != KBS_STREAM_INVALID_ARGUMENT || runtime != NULL ||
        kbs_runtime_create(&config, NULL, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_streams(NULL, &table, NULL) != KBS_STREAM_INVALID_ARGUMENT || table != NULL) {
        return 2;
    }
    if (kbs_runtime_step(NULL, 0, &progress, NULL) != KBS_STREAM_INVALID_ARGUMENT || progress.processed != 0 ||
        progress.more_work != 0 || progress.pending_events != 0 ||
        kbs_runtime_adopt_tcp(NULL, &fd, &stream, NULL) != KBS_STREAM_INVALID_ARGUMENT || stream.generation != 0 ||
        kbs_runtime_connect_tcp(NULL, NULL, 0, 0, &stream, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_listen_tcp(NULL, NULL, 0, 1, &listener, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_accept_tcp(NULL, listener, &stream, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_watch_listener(NULL, listener, 1, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_poll_listener_events(NULL, &listener_event, 1, &count, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        count != 0 || KBS_LISTENER_MAX_EVENT_BATCH != 64 || KBS_LISTENER_EVENT_ACCEPTABLE != 1 ||
        KBS_LISTENER_EVENT_ERROR != 2 || KBS_LISTENER_EVENT_HANGUP != 4) {
        return 3;
    }
    if (kbs_runtime_listener_address(NULL, listener, NULL, NULL, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_close_listener(NULL, listener, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_close(NULL, NULL) != KBS_STREAM_INVALID_ARGUMENT ||
        kbs_runtime_destroy(NULL, NULL) != KBS_STREAM_INVALID_ARGUMENT || kbs_runtime_destroy(&runtime, NULL) != 0 ||
        kbs_runtime_now_ns() == 0) {
        return 4;
    }
    return 0;
}
