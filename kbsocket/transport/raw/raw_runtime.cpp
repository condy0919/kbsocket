// SPDX-License-Identifier: MulanPSL-2.0

#include "kbsocket/transport/raw/raw_runtime.hpp"

#include <atomic>
#include <new>
#include <stdexcept>

#include "kbsocket/base/log.hpp"
#include "kbsocket/base/no_destructor.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace raw {
namespace {
// 进程级独占标志，防止同一进程内并发创建或初始化多个活跃的 RawRuntime 实例。
constinit std::atomic_flag runtime_claimed = ATOMIC_FLAG_INIT;
} // namespace

RawRuntime& GetRawRuntime() noexcept {
    static NoDestructor<RawRuntime> runtime;
    return *runtime;
}

RawRuntime::~RawRuntime() {
    if (auto result = Close(); !result) {
        KBSOCKET_LOG_ERROR("raw runtime close failed: device={} status={}", result.error().device_index,
                           result.error().provider_error);
        // 析构阶段无法向外部返回错误，严禁强行 Uninit 或任由 context 析构函数重复销毁正在使用的硬件资源。
        // 通过 release 放弃 unique_ptr 所有权以泄漏方式保留句柄，由操作系统在进程退出时统一回收；同时保留全局独占标记。
        for (auto& context : contexts_) {
            [[maybe_unused]] auto leaked = context.release();
        }
    }
}

// 初始化失败的统一回滚入口：执行 Close() 清理已分配的部分资源。
// 若回滚过程亦失败，将其保存在 cleanup_error 中，确保原始失败根因不被覆盖。
std::expected<void, RuntimeError> RawRuntime::Rollback(RuntimeError error) noexcept {
    if (auto cleanup = Close(); !cleanup) {
        error.cleanup_error = cleanup.error();
    }
    return std::unexpected(error);
}

std::expected<void, RuntimeError> RawRuntime::Initialize(const std::vector<DeviceConfig>& devices) noexcept {
    if (claimed_) {
        return std::unexpected(RuntimeError{
            .code = RuntimeErrorCode::kInUse,
        });
    }

    if (devices.empty()) {
        return std::unexpected(RuntimeError{
            .code = RuntimeErrorCode::kInvalidArgument,
        });
    }

    // 在调用底层 Init 之前校验白名单参数，避免非法配置触发 provider 初始化产生不可逆副作用。
    for (std::size_t i = 0; i < devices.size(); ++i) {
        const auto& name = devices[i].device_name;
        if (name.empty() || name.size() >= URMA_MAX_NAME || name.find('\0') != std::string::npos) {
            return std::unexpected(RuntimeError{
                .code = RuntimeErrorCode::kInvalidArgument,
                .device_index = i,
            });
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (name == devices[j].device_name) {
                return std::unexpected(RuntimeError{
                    .code = RuntimeErrorCode::kInvalidArgument,
                    .device_index = i,
                });
            }
        }
    }

    // 白名单校验通过后再抢占进程级独占标记，避免非法参数误占全局标记；若已有实例活跃则拒绝。
    if (runtime_claimed.test_and_set(std::memory_order_acquire)) {
        return std::unexpected(RuntimeError{
            .code = RuntimeErrorCode::kInUse,
        });
    }

    claimed_ = true;
    try {
        // 预分配容器内存，避免在底层 Init 成功后因 vector 扩容抛出 bad_alloc 而导致复杂的回滚状态。
        std::vector<std::string> names;
        names.reserve(devices.size());
        contexts_.reserve(devices.size());
        for (const auto& device : devices) {
            names.push_back(device.device_name);
            contexts_.push_back(std::make_unique<UrmaContext>());
        }

        urma_init_attr_t attr{};
        const auto status = UrmaApi::Init(&attr);
        if (status != URMA_SUCCESS) {
            return Rollback({
                .code = RuntimeErrorCode::kInitFailed,
                .provider_error = static_cast<int>(status),
            });
        }

        initialized_ = true;
        catalog_ = std::make_unique<DeviceCatalog>();
        if (auto result = catalog_->Initialize(names); !result) {
            return Rollback({
                .code = RuntimeErrorCode::kCatalogFailed,
                .catalog_error = result.error(),
            });
        }

        // 两阶段建立 context（保证要么全成功，要么干净回滚）：
        // 阶段一：全量解析并校验目标端点，避免后续因某一端点缺失导致前序 context 残留悬挂。
        std::vector<LocalEndpoint> endpoints;
        endpoints.reserve(devices.size());
        for (std::size_t i = 0; i < devices.size(); ++i) {
            auto endpoint = catalog_->Find(devices[i].device_name, devices[i].eid_index);
            if (!endpoint) {
                return Rollback({
                    .code = RuntimeErrorCode::kCatalogFailed,
                    .device_index = i,
                    .catalog_error = endpoint.error(),
                });
            }
            endpoints.push_back(*endpoint);
        }

        // 阶段二：所有端点确认就绪后再依次打开 context；任一打开失败立即中止并触发回滚。
        for (std::size_t i = 0; i < endpoints.size(); ++i) {
            if (auto result = contexts_[i]->Open(endpoints[i]); !result) {
                return Rollback({
                    .code = RuntimeErrorCode::kContextFailed,
                    .device_index = i,
                    .context_error = result.error(),
                });
            }
        }

        ready_ = true;
        return {};
    } catch (const std::bad_alloc&) {
        return Rollback({
            .code = RuntimeErrorCode::kNoMemory,
        });
    } catch (const std::length_error&) {
        return Rollback({
            .code = RuntimeErrorCode::kNoMemory,
        });
    }
}

std::expected<void, RuntimeCloseError> RawRuntime::Close() noexcept {
    // 先置为未就绪状态，阻断外部工作线程后续对 context 的并发获取。
    ready_ = false;

    // 逆序逐一释放 context；若某一 context 删除失败，立即中断后续清理并返回错误，保留剩余句柄供排查与重试。
    for (std::size_t i = contexts_.size(); i > 0; --i) {
        if (auto result = contexts_[i - 1]->Close(); !result) {
            return std::unexpected(RuntimeCloseError{
                .code = RuntimeCloseErrorCode::kContextFailed,
                .device_index = i - 1,
                .provider_error = result.error().provider_error,
            });
        }
    }

    contexts_.clear();
    catalog_.reset();
    auto status = URMA_SUCCESS;
    if (initialized_) {
        // URMA 驱动层即使报告 Uninit 失败，内部也会卸载 provider 并销毁设备，状态不可逆，严禁重复 Uninit。
        status = UrmaApi::Uninit();
        initialized_ = false;
    }

    if (claimed_) {
        claimed_ = false;
        runtime_claimed.clear(std::memory_order_release);
    }

    if (status != URMA_SUCCESS) {
        return std::unexpected(RuntimeCloseError{
            .code = RuntimeCloseErrorCode::kUninitFailed,
            .provider_error = status,
        });
    }
    return {};
}
} // namespace raw
} // namespace kbsocket
