// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_STREAM_ERROR_HPP_
#define KBSOCKET_API_STREAM_ERROR_HPP_
#include "kbsocket/api/stream.h"
#include "kbsocket/runtime/connection_owner.hpp"
#include "kbsocket/runtime/raw_stack.hpp"
namespace kbsocket {
namespace api {
kbs_stream_error StreamError(int code) noexcept;
kbs_stream_error MapStreamError(const connection::ConnectionError& error) noexcept;
kbs_stream_error MapStreamError(const runtime::ConnectionOwnerError& error) noexcept;
kbs_stream_error MapStreamError(const runtime::RawStackError& error) noexcept;
kbs_stream_status StreamStatus(const runtime::OwnedConnectionStatus& status) noexcept;
} // namespace api
} // namespace kbsocket
#endif // KBSOCKET_API_STREAM_ERROR_HPP_
