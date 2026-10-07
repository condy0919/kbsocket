// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/urma_context.hpp"

#include <cerrno>
#include <cstring>

#include "kbsocket/base/log.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
UrmaContext::~UrmaContext() {
    if (auto result = Close(); !result) {
        KBSOCKET_LOG_ERROR("context destruction failed: status={}; URMA session must remain alive",
                           result.error().provider_error);
    }
}

std::expected<void, ContextError> UrmaContext::Open(const LocalEndpoint& endpoint) noexcept {
    if (ctx_) {
        return std::unexpected(ContextError{
            .code = ContextErrorCode::kAlreadyOpen,
        });
    }

    const urma_eid_t zero{};
    if (!endpoint.device || std::memcmp(endpoint.eid.raw, zero.raw, sizeof(zero.raw)) == 0) {
        return std::unexpected(ContextError{
            .code = ContextErrorCode::kInvalidArgument,
        });
    }

    errno = 0;
    ctx_ = UrmaApi::CreateContext(endpoint.device, endpoint.eid_index);
    if (!ctx_) {
        return std::unexpected(ContextError{
            .code = ContextErrorCode::kCreateFailed,
            .provider_error = errno,
        });
    }

    if (ctx_->eid_index != endpoint.eid_index ||
        std::memcmp(ctx_->eid.raw, endpoint.eid.raw, sizeof(endpoint.eid.raw)) != 0) {
        ContextError error{
            .code = ContextErrorCode::kEndpointChanged,
        };
        // 校验失败不发布 context；删除失败仍保留所有权，允许外层回滚或显式 Close 重试。
        if (auto cleanup = Close(); !cleanup) {
            error.cleanup_error = cleanup.error().provider_error;
        }
        return std::unexpected(error);
    }

    verified_ = true;
    return {};
}

std::expected<void, ContextError> UrmaContext::Close() noexcept {
    if (!ctx_) {
        return {};
    }

    const auto status = UrmaApi::DeleteContext(ctx_);
    if (status != URMA_SUCCESS) {
        return std::unexpected(ContextError{
            .code = ContextErrorCode::kDeleteFailed,
            .provider_error = static_cast<int>(status),
        });
    }
    ctx_ = nullptr;
    verified_ = false;
    return {};
}
} // namespace raw
} // namespace kbsocket
