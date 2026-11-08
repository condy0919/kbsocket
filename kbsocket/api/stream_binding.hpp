// SPDX-License-Identifier: MulanPSL-2.0
#ifndef KBSOCKET_API_STREAM_BINDING_HPP_
#define KBSOCKET_API_STREAM_BINDING_HPP_

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "kbsocket/api/stream.h"
#include "kbsocket/runtime/connection_owner.hpp"
#include "kbsocket/runtime/connection_progress.hpp"
#include "kbsocket/runtime/raw_stack.hpp"

namespace kbsocket {
namespace api {
struct StreamBindingConfig {
    // 宿主保证进程内跨表/生命周期不复用；0 无效，不能只用对象地址或 fd。
    std::uint64_t table_id = 0;
    std::uint32_t capacity = 256;
    std::size_t max_io_bytes = 64u * 1024u;
};

/// 有界、单 owner 的 C ABI 桥；借用 ConnectionOwner，不再创建第二套连接/调度状态机。
/// raw 入口始终通过 RawStack 的暂停/生命周期门禁；TCP 仅接受无 raw context 的 owner。
/// Open 预分配，Publish 与 I/O 无分配。发布后只经本表操作/Release，Prepare 仍由宿主完成。
/// Close 必须等全部 token Release，且应用不再访问 C 表指针；不关闭借用的 owner/stack。
/// 外部 owner/stack/provider 必须比本表及其所有调用活得更久；任意无效 C 指针不在防护范围内。
class StreamBinding {
public:
    StreamBinding() noexcept;
    ~StreamBinding();
    StreamBinding(const StreamBinding&) = delete;
    StreamBinding& operator=(const StreamBinding&) = delete;
    std::expected<void, kbs_stream_error> OpenTcp(runtime::ConnectionOwner& owner,
                                                  const StreamBindingConfig& config) noexcept;
    std::expected<void, kbs_stream_error> OpenRaw(runtime::RawStack& stack, std::size_t context,
                                                  const StreamBindingConfig& config) noexcept;
    std::expected<kbs_stream_handle, kbs_stream_error> Publish(runtime::OwnedConnection connection) noexcept;
    // C 入口不提供“相信已安全”的布尔开关，来源退休只能由宿主用真实独立证明推进。
    std::expected<void, kbs_stream_error> RetireSources(kbs_stream_handle stream,
                                                        const runtime::RxSourceRetireProof& proof) noexcept;
    void StopAccepting() noexcept;
    std::expected<void, kbs_stream_error> Close() noexcept;
    kbs_stream_table* table() noexcept;
    std::size_t published() const noexcept;
    // 只读本表 owner 元数据，供宿主避免有待交付事件时进入阻塞等待；不访问 raw 域。
    bool events_pending() const noexcept;

    std::expected<std::size_t, kbs_stream_error> Write(kbs_stream_handle stream,
                                                       std::span<const std::byte> input) noexcept;
    std::expected<kbs_stream_io, kbs_stream_error> Read(kbs_stream_handle stream, std::span<std::byte> output,
                                                        std::uint64_t now_ns) noexcept;
    std::expected<void, kbs_stream_error> ShutdownWrite(kbs_stream_handle stream) noexcept;
    std::expected<void, kbs_stream_error> Cancel(kbs_stream_handle stream) noexcept;
    std::expected<void, kbs_stream_error> Watch(kbs_stream_handle stream, bool readable, bool writable) noexcept;
    std::expected<std::size_t, kbs_stream_error> PollEvents(std::span<kbs_stream_event> output) noexcept;
    // 唯一允许从原 ConnectionProgress 回调同步调用的入口：只复制快照，不重入 owner/provider。
    // 不得缓存后乱序回放；未发布/旧 scheduled identity 返回 false。暂停时拒绝并保留队列。
    // 宿主必须先停用转发再析构本对象；Close 不替宿主注销回调。
    std::expected<bool, kbs_stream_error> RecordProgress(const runtime::ConnectionProgressEvent& event) noexcept;
    std::expected<void, kbs_stream_error> WatchTcp(kbs_stream_handle stream, bool readable, bool writable) noexcept;
    std::expected<kbs_stream_status, kbs_stream_error> Inspect(kbs_stream_handle stream) noexcept;
    std::expected<void, kbs_stream_error> CloseStream(kbs_stream_handle stream) noexcept;
    std::expected<void, kbs_stream_error> Release(kbs_stream_handle stream) noexcept;

private:
    struct State;
    std::unique_ptr<State> state_;
    bool opened_once_ = false;
    std::expected<void, kbs_stream_error> Open(runtime::ConnectionOwner& owner, runtime::RawStack* stack,
                                               std::size_t context, const StreamBindingConfig& config) noexcept;
};
} // namespace api
} // namespace kbsocket
#endif // KBSOCKET_API_STREAM_BINDING_HPP_
