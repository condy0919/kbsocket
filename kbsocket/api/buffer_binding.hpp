// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_BUFFER_BINDING_HPP_
#define KBSOCKET_API_BUFFER_BINDING_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/api/buffer_lease.h"
#include "kbsocket/memory/buffer_lease.hpp"
#include "kbsocket/memory/slab_allocator.hpp"
#include "kbsocket/runtime/raw_stack.hpp"

namespace kbsocket {
namespace api {
struct BufferBindingConfig {
    std::uint64_t table_id = 0;
    std::uint32_t capacity = 256;
};

/// 单 owner 的有界 CPU Lease 发布表；Open 后分配/视图/归还均不动态分配。
/// OpenHost 拥有 CPU-only 池；OpenSlab 借用由宿主串行管理的池；OpenRaw 始终经全部 context 暂停门禁。
/// 不可通过 OpenSlab 绕过 RawStack::shared；raw 必须 OpenRaw。借用 Slab 不得独立 Close/Open。
/// 宿主可 Resolve 后预留传输责任，但不得另行 ReleaseCpu 或把同一 CPU Lease 再发布到其他表。
/// Close 只在全部公开 CPU Lease 归还后成功；不代办 DMA/Grant 终结。依赖须活到所有责任结束。
/// 未关闭析构保留状态；无效 C 指针/调用方保存并越期使用的视图不在身份防护范围内。
class BufferBinding {
public:
    BufferBinding() noexcept;
    ~BufferBinding();
    BufferBinding(const BufferBinding&) = delete;
    BufferBinding& operator=(const BufferBinding&) = delete;
    std::expected<void, kbs_stream_error> OpenHost(const kbs_buffer_pool_config& config) noexcept;
    std::expected<void, kbs_stream_error> OpenSlab(memory::SlabAllocator& slab,
                                                   const BufferBindingConfig& config) noexcept;
    std::expected<void, kbs_stream_error> OpenRaw(runtime::RawStack& stack, const BufferBindingConfig& config) noexcept;
    kbs_buffer_table* table() noexcept;
    std::size_t published() const noexcept;
    void StopAccepting() noexcept;
    std::expected<void, kbs_stream_error> Close() noexcept;
    std::expected<kbs_buffer_handle, kbs_stream_error> Acquire(std::size_t bytes) noexcept;
    std::expected<std::span<std::byte>, kbs_stream_error> Writable(kbs_buffer_handle buffer) noexcept;
    std::expected<std::span<const std::byte>, kbs_stream_error> Readable(kbs_buffer_handle buffer) noexcept;
    std::expected<void, kbs_stream_error> Release(kbs_buffer_handle buffer) noexcept;
    std::expected<kbs_buffer_status, kbs_stream_error> Inspect(kbs_buffer_handle buffer) noexcept;
    // 只供宿主后续传输接线；仍校验公开身份、CPU 可访问状态与 raw 暂停，不创造新引用；停止准入后拒绝。
    std::expected<memory::BufferLease, kbs_stream_error> Resolve(kbs_buffer_handle buffer) noexcept;
    // C 表桥内部使用，完整校验请求范围后再取真实 Lease；不暴露 C 表布局。
    static std::expected<memory::BufferLease, kbs_stream_error>
    Resolve(kbs_buffer_table* table, kbs_buffer_handle buffer, std::size_t offset, std::size_t bytes) noexcept;

private:
    struct State;
    std::expected<void, kbs_stream_error> Open(memory::SlabAllocator* slab, runtime::RawStack* stack,
                                               const BufferBindingConfig& config,
                                               const kbs_buffer_pool_config* host) noexcept;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
};
} // namespace api
} // namespace kbsocket
#endif // KBSOCKET_API_BUFFER_BINDING_HPP_
