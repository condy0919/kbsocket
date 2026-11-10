// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/api/raw_stream_runtime.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>

#include "kbsocket/api/stream_error.hpp"
#include "kbsocket/connection/write_rndv_engine.hpp"
#include "kbsocket/control/rndv_arena.hpp"
#include "kbsocket/control/tx_lane_snapshot.hpp"
#include "kbsocket/transport/raw/urma_api.hpp"

namespace kbsocket {
namespace api {
namespace {
auto Error(int code) noexcept {
    return std::unexpected(StreamError(code));
}
auto HandshakeError(control::ConnectionHandshakeErrorCode code, int underlying = 0) noexcept {
    return std::unexpected(MapStreamError(
        connection::ConnectionError{connection::ConnectionErrorCode::kHandshake, static_cast<int>(code), underlying}));
}
auto CreditError(connection::CreditErrorCode code) noexcept {
    return std::unexpected(
        MapStreamError(connection::ConnectionError{connection::ConnectionErrorCode::kCredit, static_cast<int>(code)}));
}
// 所有乘加在打开 provider 前完成；极大配置也不能回绕成较小准入量。
bool Accumulate(std::uint64_t& total, std::uint64_t count, std::uint64_t size,
                std::uint64_t limit = std::numeric_limits<std::uint64_t>::max()) noexcept {
    if (total > limit || (count && size > (limit - total) / count)) {
        return false;
    }
    total += count * size;
    return true;
}
bool ValidateRndv(std::span<const RawStreamEndpointConfig> endpoints, const runtime::RawStackConfig& stack,
                  const RawStreamRndvBudget& budget) noexcept {
    RawStreamRndvBudget used;
    std::uint64_t targets = 0, controls = 0;
    for (const auto& endpoint : endpoints) {
        const auto capacity = endpoint.streams.capacity;
        const auto owner_capacity = endpoint.rndv
                                        ? capacity
                                        : (endpoint.endpoint.connection_capacity ? endpoint.endpoint.connection_capacity
                                                                                 : stack.connections_per_context);
        if (!Accumulate(controls, owner_capacity, 1)) {
            return false;
        }
        if (!endpoint.rndv) {
            continue;
        }
        const auto& config = endpoint.rndv->connection;
        const auto& limits = endpoint.rndv->limits;
        if (!stack.enable_rndv || !stack.rndv_target_slab || !endpoint.rndv->arena.id ||
            !endpoint.rndv->arena.generation || !connection::RndvConnection::ValidConfig(config) ||
            !control::ValidateRndvLimits(limits) || config.tx_capacity > limits.max_grants ||
            config.receive_capacity < limits.max_grants || config.ingress_packets < limits.max_grants ||
            config.threshold > limits.max_transfer_bytes || config.threshold > stack.slab.buffer_size ||
            stack.slab.buffer_size < 256 || limits.max_transfer_bytes > stack.rndv_target_slab->buffer_size ||
            !(stack.rndv_target_slab->access & URMA_ACCESS_WRITE) ||
            (limits.max_transfer_bytes - 1) / std::min(config.chunk_bytes, limits.max_write_bytes) + 1 >
                config.max_chunks) {
            return false;
        }
        if (!Accumulate(used.connections, capacity, 1, budget.connections) ||
            !Accumulate(used.tx_slots, capacity, config.tx_capacity, budget.tx_slots) ||
            !Accumulate(used.receive_entries, capacity, config.receive_capacity, budget.receive_entries) ||
            !Accumulate(used.write_chunks, capacity, std::uint64_t{config.tx_capacity} * config.max_chunks,
                        budget.write_chunks) ||
            !Accumulate(used.notification_slots, capacity, config.notification_capacity, budget.notification_slots) ||
            !Accumulate(used.ingress_packets, capacity, std::uint64_t{3} * config.ingress_packets,
                        budget.ingress_packets) ||
            !Accumulate(used.grant_bytes, capacity, limits.max_bytes, budget.grant_bytes) ||
            !Accumulate(targets, capacity, limits.max_grants)) {
            return false;
        }
    }
    if (!used.connections) {
        return true;
    }
    const auto& target = *stack.rndv_target_slab;
    std::uint64_t target_bytes = 0;
    // 固定通知与 Eager 控制 buffer 有真实常驻 Lease；另留每 TX 一块拷贝源空间。
    // 应用持有 Lease 或在途 Eager 仍可耗尽共享池，运行时必须正常返回 WOULD_BLOCK。
    return Accumulate(target_bytes, target.buffer_count, target.buffer_size) && used.grant_bytes <= target_bytes &&
           targets <= target.buffer_count && targets <= target.max_references &&
           Accumulate(controls, 1, used.notification_slots, stack.slab.buffer_count) &&
           Accumulate(controls, 1, used.tx_slots, stack.slab.buffer_count) &&
           used.notification_slots <= stack.slab.max_references &&
           used.tx_slots <= stack.slab.max_references - used.notification_slots;
}
// 导出仅发生在 Open 控制路径；保留公共扩展，不把手拼的基础结构冒充 provider 描述符。
std::expected<void, kbs_stream_error> Export(raw::JettyPool& pool, control::HandshakeHello& hello) noexcept {
    const auto id = pool.rx_target_jetty_id();
    if (!id || !pool.rx_target_jetty()) {
        return Error(KBS_STREAM_NOT_READY);
    }
    urma_rjetty_t* descriptor = nullptr;
    std::uint32_t bytes = 0;
    const auto status = raw::UrmaApi::GetRjetty(pool.rx_target_jetty(), &descriptor, &bytes);
    struct Release {
        urma_rjetty_t* value;
        ~Release() {
            if (value) {
                raw::UrmaApi::PutRjetty(value);
            }
        }
    } release{descriptor};
    if (status != URMA_SUCCESS || !descriptor) {
        return std::unexpected(MapStreamError(
            runtime::RawStackError{.code = runtime::RawStackErrorCode::kDomain,
                                   .domain = runtime::RawDomainError{.code = runtime::RawDomainErrorCode::kQueue,
                                                                     .provider_error = static_cast<int>(status)}}));
    }
    if (bytes < sizeof(urma_rjetty_t) || bytes > hello.descriptor.size()) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    urma_rjetty_t base{};
    std::memcpy(&base, descriptor, sizeof(base));
    if (base.jetty_id.id != id->id || base.jetty_id.uasid != id->uasid ||
        std::memcmp(&base.jetty_id.eid, &id->eid, sizeof(id->eid)) || base.type != URMA_JETTY ||
        base.trans_mode != URMA_TM_RM || base.flag.bs.has_drv_ext || base.flag.bs.share_tp || base.flag.bs.reserved ||
        base.flag.bs.token_policy > URMA_TOKEN_ALL_ENCRYPTED) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    if (base.flag.bs.has_user_info) {
        std::uint32_t extension = 0;
        if (bytes < sizeof(base) + sizeof(extension)) {
            return Error(KBS_STREAM_INVALID_ARGUMENT);
        }
        std::memcpy(&extension, reinterpret_cast<const std::byte*>(descriptor) + sizeof(base), sizeof(extension));
        if (extension != bytes - sizeof(base) - sizeof(extension)) {
            return Error(KBS_STREAM_INVALID_ARGUMENT);
        }
    } else if (bytes != sizeof(base)) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    // 当前 provider 导出可能未填 tp_type；本池明确创建为 RM_CTP，只修正本地副本。
    base.tp_type = URMA_CTP;
    std::memcpy(hello.descriptor.data(), descriptor, bytes);
    std::memcpy(hello.descriptor.data(), &base, sizeof(base));
    hello.descriptor_bytes = bytes;
    hello.jetty_id = id->id;
    std::memcpy(hello.eid.data(), &id->eid, hello.eid.size());
    return {};
}
} // namespace

struct RawStreamRuntime::State {
    struct Endpoint {
        StreamBinding binding;
        control::HandshakeHello local;
        std::uint64_t table_id = 0;
        runtime::ConnectionProgressCallback on_connection = nullptr;
        runtime::ConnectionMailboxCallback on_command = nullptr;
        runtime::PeerCreditProgressCallback on_withdrawal = nullptr;
        void* cookie = nullptr;
        std::optional<runtime::OwnedConnection> unpublished;
        std::optional<kbs_stream_error> callback_error;
        std::optional<RawStreamRndvConfig> rndv;

        static void Observe(void* cookie, const runtime::ConnectionProgressEvent& event) noexcept {
            auto& self = *static_cast<Endpoint*>(cookie);
            if (self.binding.table()) {
                const auto recorded = self.binding.RecordProgress(event);
                if (!recorded && !self.callback_error) {
                    self.callback_error = recorded.error();
                }
            }
            if (self.on_connection) {
                self.on_connection(self.cookie, event);
            }
        }
        static void Command(void* cookie, const runtime::ConnectionMailboxNotice& event) noexcept {
            auto& self = *static_cast<Endpoint*>(cookie);
            if (self.on_command) {
                self.on_command(self.cookie, event);
            }
        }
        static void Withdrawal(void* cookie, const runtime::PeerCreditProgressEvent& event) noexcept {
            auto& self = *static_cast<Endpoint*>(cookie);
            if (self.on_withdrawal) {
                self.on_withdrawal(self.cookie, event);
            }
        }
        std::expected<void, kbs_stream_error> CleanUnpublished(runtime::ConnectionOwner& owner) noexcept {
            if (!unpublished) {
                return {};
            }
            (void)owner.Cancel(*unpublished);
            auto result = owner.CloseConnection(*unpublished);
            if (result) {
                result = owner.Release(*unpublished);
            }
            if (!result) {
                auto error = MapStreamError(result.error());
                error.code = KBS_STREAM_RETAINED;
                return std::unexpected(error);
            }
            unpublished.reset();
            return {};
        }
    };
    runtime::RawStack stack;
    BufferBinding buffers;
    std::array<Endpoint, runtime::RawStack::kMaxContexts> endpoints;
    std::size_t count = 0;
    bool ready = false, stopping = false;

    std::expected<std::size_t, kbs_stream_error> Find(kbs_stream_handle stream) const noexcept {
        for (std::size_t i = 0; i < count; ++i) {
            if (endpoints[i].table_id == stream.table_id) {
                return i;
            }
        }
        return Error(KBS_STREAM_INVALID_HANDLE);
    }
};

RawStreamRuntime::RawStreamRuntime() noexcept = default;
RawStreamRuntime::~RawStreamRuntime() {
    // RawStack 自身保留不够：其回调仍引用上面的 Endpoint/StreamBinding。
    if (state_ && state_->stack.stage() != runtime::RawStackStage::kClosed &&
        state_->stack.stage() != runtime::RawStackStage::kEmpty) {
        (void)state_.release();
    }
}
std::expected<void, kbs_stream_error> RawStreamRuntime::Open(std::span<const RawStreamEndpointConfig> endpoints,
                                                             const runtime::RawStackConfig& config,
                                                             const RawStreamRndvBudget& rndv_budget) noexcept {
    if (state_) {
        return Error(KBS_STREAM_BUSY);
    }
    if (endpoints.empty() || endpoints.size() > runtime::RawStack::kMaxContexts) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    for (std::size_t i = 0; i < endpoints.size(); ++i) {
        const auto& e = endpoints[i];
        if (!e.streams.table_id || !e.streams.capacity || e.streams.capacity > config.connections_per_context ||
            !e.streams.max_io_bytes || e.streams.max_io_bytes > 1024u * 1024u || !e.endpoint_generation ||
            !e.authorization || !e.credit_epoch ||
            (e.endpoint.connection_capacity && e.endpoint.connection_capacity < e.streams.capacity)) {
            return Error(KBS_STREAM_INVALID_ARGUMENT);
        }
        for (std::size_t j = 0; j < i; ++j) {
            if (e.streams.table_id == endpoints[j].streams.table_id ||
                (e.rndv && endpoints[j].rndv && e.rndv->arena.id == endpoints[j].rndv->arena.id)) {
                return Error(KBS_STREAM_INVALID_ARGUMENT);
            }
        }
    }
    if (!ValidateRndv(endpoints, config, rndv_budget)) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    std::array<runtime::RawEndpointConfig, runtime::RawStack::kMaxContexts> raw_endpoints;
    try {
        for (std::size_t i = 0; i < endpoints.size(); ++i) {
            raw_endpoints[i] = endpoints[i].endpoint;
        }
        state_ = std::make_unique<State>();
    } catch (const std::bad_alloc&) {
        return Error(KBS_STREAM_NO_MEMORY);
    } catch (const std::length_error&) {
        return Error(KBS_STREAM_NO_MEMORY);
    }
    auto& s = *state_;
    s.count = endpoints.size();
    for (std::size_t i = 0; i < s.count; ++i) {
        const auto& input = endpoints[i];
        auto& e = s.endpoints[i];
        e.table_id = input.streams.table_id;
        e.rndv = input.rndv;
        if (e.rndv) {
            // 使用底层 owner 的真实上限，C API、mailbox 与宿主入口不能绕过占用槽预算。
            raw_endpoints[i].connection_capacity = input.streams.capacity;
        }
        e.on_connection = input.endpoint.on_connection;
        e.on_command = input.endpoint.on_command;
        e.on_withdrawal = input.endpoint.on_withdrawal;
        e.cookie = input.endpoint.cookie;
        e.local.identity = {.runtime = input.endpoint.domain.local_domain.runtime_instance,
                            .receive_domain = input.endpoint.domain.local_domain.receive_domain,
                            .endpoint_generation = input.endpoint_generation,
                            .authorization = input.authorization,
                            .session = 0,
                            .token = {}};
        e.local.credit_epoch = input.credit_epoch;
        raw_endpoints[i].cookie = &e;
        raw_endpoints[i].on_connection = &State::Endpoint::Observe;
        raw_endpoints[i].on_command = &State::Endpoint::Command;
        raw_endpoints[i].on_withdrawal = &State::Endpoint::Withdrawal;
    }
    const auto opened = s.stack.Open(std::span(raw_endpoints).first(s.count), config);
    if (!opened) {
        // 参数校验/State 分配失败尚未接入任何回调或硬件，允许修正后重新 Open。
        if (s.stack.stage() == runtime::RawStackStage::kEmpty) {
            state_.reset();
        }
        return std::unexpected(MapStreamError(opened.error()));
    }
    for (std::size_t i = 0; i < s.count; ++i) {
        const auto resources = s.stack.resources(i);
        if (!resources) {
            return std::unexpected(MapStreamError(resources.error()));
        }
        auto& e = s.endpoints[i];
        if (const auto result = Export(*resources->domain.pool, e.local); !result) {
            return result;
        }
        if (e.rndv) {
            const auto& pool = *resources->domain.pool;
            const auto& rndv = *e.rndv;
            // 对照实际硬件能力，避免公告连首个事务都无法提交的配置。
            const auto references =
                std::uint64_t{rndv.connection.max_chunks} *
                    std::min<std::uint32_t>(connection::WriteRndvEngine::kMaxSources, pool.max_send_sge()) +
                2;
            if (rndv.limits.max_write_bytes > pool.max_write_size() || references > config.slab.max_references) {
                return Error(KBS_STREAM_INVALID_ARGUMENT);
            }
            const auto shared = s.stack.shared();
            if (!shared) {
                return std::unexpected(MapStreamError(shared.error()));
            }
            const auto arena = control::ExportRndvArena(*shared->rndv_target, resources->context, rndv.arena);
            if (!arena) {
                return Error(KBS_STREAM_INVALID_ARGUMENT);
            }
            e.local.rndv.limits = rndv.limits;
            e.local.rndv.arena_count = 1;
            e.local.rndv.arenas[0] = *arena;
            if (!control::ValidateRndvOffer(e.local.rndv)) {
                return Error(KBS_STREAM_INVALID_ARGUMENT);
            }
            e.local.capabilities |= control::kHandshakeCapabilityWriteRndv;
        }
        if (const auto result = e.binding.OpenRaw(s.stack, i, endpoints[i].streams); !result) {
            return result;
        }
    }
    s.ready = true;
    return {};
}
kbs_stream_table* RawStreamRuntime::table(std::size_t context) noexcept {
    return state_ && state_->ready && context < state_->count ? state_->endpoints[context].binding.table() : nullptr;
}
std::expected<void, kbs_stream_error> RawStreamRuntime::OpenBuffers(const BufferBindingConfig& config) noexcept {
    if (!state_ || !state_->ready || stage() == runtime::RawStackStage::kClosed) {
        return Error(KBS_STREAM_NOT_READY);
    }
    if (state_->stopping) {
        return Error(KBS_STREAM_STOPPED);
    }
    return state_->buffers.OpenRaw(state_->stack, config);
}
kbs_buffer_table* RawStreamRuntime::buffers() noexcept {
    return state_ && state_->ready ? state_->buffers.table() : nullptr;
}
runtime::RawStackStage RawStreamRuntime::stage() const noexcept {
    if (!state_) {
        return runtime::RawStackStage::kEmpty;
    }
    const auto stage = state_->stack.stage();
    return stage == runtime::RawStackStage::kReady && !state_->ready ? runtime::RawStackStage::kOpenFailed : stage;
}
bool RawStreamRuntime::paused(std::size_t context) const noexcept {
    return state_ && state_->stack.paused(context);
}
std::expected<runtime::RawStackSnapshot, kbs_stream_error> RawStreamRuntime::Inspect() const noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto result = state_->stack.Inspect();
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    result->stage = stage();
    return *result;
}
std::expected<runtime::RawStackContextResources, kbs_stream_error>
RawStreamRuntime::HostResources(std::size_t context) noexcept {
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto result = state_->stack.resources(context);
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    return *result;
}
std::expected<kbs_stream_handle, kbs_stream_error>
RawStreamRuntime::AdoptProtocol(std::size_t context, int* fd, const RawStreamConnectOptions& options,
                                std::uint64_t now_ns) noexcept {
    if (!fd || *fd < 0 || !options.session) {
        return Error(KBS_STREAM_INVALID_ARGUMENT);
    }
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    auto& s = *state_;
    if (s.stopping) {
        return Error(KBS_STREAM_STOPPED);
    }
    const auto resources = HostResources(context);
    if (!resources) {
        return std::unexpected(resources.error());
    }
    auto& e = s.endpoints[context];
    if (e.unpublished) {
        return Error(KBS_STREAM_RETAINED);
    }
    if (const auto available = e.binding.CanPublish(); !available) {
        return std::unexpected(available.error());
    }
    const auto lanes = control::CaptureTxLanes(*resources->domain.pool);
    if (!lanes) {
        // CaptureTxLanes 使用 channel 枚举，不能把其整数直接解释为 handshake 子类。
        const auto code = lanes.error().code == control::HandshakeErrorCode::kNotReady
                              ? control::ConnectionHandshakeErrorCode::kNotReady
                              : control::ConnectionHandshakeErrorCode::kInvalidArgument;
        return HandshakeError(code, lanes.error().system_error);
    }
    auto hello = e.local;
    hello.identity.session = options.session;
    hello.tx_lanes = *lanes;
    const auto owned =
        resources->connections->AdoptProtocol(*fd, *resources->domain.routes,
                                              {.local = hello,
                                               .expected_peer_authorization = options.expected_peer_authorization,
                                               .expected_peer_runtime = options.expected_peer_runtime,
                                               .compatible_peer = options.compatible_peer,
                                               .deadline_ns = options.deadline_ns},
                                              now_ns, e.rndv ? &e.rndv->connection : nullptr);
    if (!owned) {
        e.unpublished = owned.error().retained_handle;
        if (const auto cleaned = e.CleanUnpublished(*resources->connections); !cleaned) {
            return std::unexpected(cleaned.error());
        }
        return std::unexpected(MapStreamError(owned.error()));
    }
    *fd = -1;
    const auto published = e.binding.Publish(*owned);
    if (!published) {
        e.unpublished = *owned;
        if (const auto cleaned = e.CleanUnpublished(*resources->connections); !cleaned) {
            return std::unexpected(cleaned.error());
        }
    }
    return published;
}
std::expected<control::HandshakeHello, kbs_stream_error> RawStreamRuntime::Peer(kbs_stream_handle stream) noexcept {
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto index = state_->Find(stream);
    if (!index) {
        return std::unexpected(index.error());
    }
    const auto owned = state_->endpoints[*index].binding.Resolve(stream);
    if (!owned) {
        return std::unexpected(owned.error());
    }
    const auto resources = HostResources(*index);
    if (!resources) {
        return std::unexpected(resources.error());
    }
    const auto peer = resources->connections->Peer(*owned);
    if (!peer) {
        return std::unexpected(MapStreamError(peer.error()));
    }
    return **peer;
}
std::expected<runtime::OwnedConnection, kbs_stream_error> RawStreamRuntime::Resolve(kbs_stream_handle stream) noexcept {
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto index = state_->Find(stream);
    if (!index) {
        return std::unexpected(index.error());
    }
    const auto resources = HostResources(*index);
    if (!resources) {
        return std::unexpected(resources.error());
    }
    return state_->endpoints[*index].binding.Resolve(stream);
}
std::expected<void, kbs_stream_error> RawStreamRuntime::PrepareRaw(kbs_stream_handle stream,
                                                                   const RawStreamPreparation& prep) noexcept {
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    if (state_->stopping) {
        return Error(KBS_STREAM_STOPPED);
    }
    // 任何 context 控制任务都可能访问共享 Slab/import；必须先检查全栈门禁。
    const auto shared = state_->stack.shared();
    if (!shared) {
        return std::unexpected(MapStreamError(shared.error()));
    }
    const auto peer = Peer(stream);
    if (!peer) {
        return std::unexpected(peer.error());
    }
    const auto index = state_->Find(stream);
    if (!index) {
        return std::unexpected(index.error());
    }
    auto& endpoint = state_->endpoints[*index];
    const auto owned = endpoint.binding.Resolve(stream);
    if (!owned) {
        return std::unexpected(owned.error());
    }
    const auto resources = HostResources(*index);
    if (!resources) {
        return std::unexpected(resources.error());
    }
    if (!prep.descriptor_compatible || !prep.peer_lanes_authorized) {
        return HandshakeError(control::ConnectionHandshakeErrorCode::kUnauthorized);
    }
    const auto& domain = resources->domain;
    const auto credits = domain.credits->PeerStats(prep.credit_peer);
    if (!credits) {
        return CreditError(credits.error().code);
    }
    const connection::CreditIdentity receive_identity{peer->identity.runtime, endpoint.local.identity.receive_domain,
                                                      endpoint.local.credit_epoch};
    const connection::CreditIdentity send_identity{peer->identity.runtime, peer->identity.receive_domain,
                                                   peer->credit_epoch};
    if (credits->identity != receive_identity || !prep.sender_credits || !prep.sender_credits->ready() ||
        prep.sender_credits->identity() != send_identity) {
        return CreditError(connection::CreditErrorCode::kWrongIdentity);
    }
    if (credits->frozen || credits->retired || prep.sender_credits->stats().stopped) {
        return CreditError(connection::CreditErrorCode::kStopped);
    }
    connection::RndvConnectionPreparation rndv;
    if (endpoint.rndv && (peer->capabilities & control::kHandshakeCapabilityWriteRndv)) {
        if (!prep.rndv_peer_authorized) {
            return HandshakeError(control::ConnectionHandshakeErrorCode::kUnauthorized);
        }
        // 在握手取得资源前拒绝可修正的宿主参数，不把错误 Lane 变成不可重试的部分准备。
        const auto lane = domain.pool->lane_state(prep.rndv_lane);
        const auto& policy = *endpoint.rndv;
        const auto grants = std::min(policy.limits.max_grants, peer->rndv.limits.max_grants);
        const auto transfer = std::min(policy.limits.max_transfer_bytes, peer->rndv.limits.max_transfer_bytes);
        const auto chunk =
            std::min({policy.connection.chunk_bytes, policy.limits.max_write_bytes, peer->rndv.limits.max_write_bytes});
        if (!shared->rndv_target || !shared->segments || prep.rndv_lane.pool != domain.pool || !lane ||
            *lane != raw::JettyLaneState::kReady || peer->rndv.arena_count != 1 ||
            policy.connection.tx_capacity > grants || !transfer || !chunk ||
            (transfer - 1) / chunk + 1 > policy.connection.max_chunks) {
            return Error(KBS_STREAM_INVALID_ARGUMENT);
        }
        rndv = {.target_slab = shared->rndv_target, .segments = shared->segments, .lane = prep.rndv_lane};
    }
    const connection::RawConnectionPreparation bound{
        .handshake = {.imports = shared->imports,
                      .local_context = resources->context,
                      .local_context_incarnation = domain.sources->context_incarnation(),
                      .descriptor_compatible = prep.descriptor_compatible,
                      .credits = domain.credits,
                      .credit_peer = prep.credit_peer,
                      .reorder = prep.reorder,
                      .rndv_peer_authorized = prep.rndv_peer_authorized},
        .receive = domain.receive,
        .sender = domain.sender,
        .slab = shared->slab,
        .sender_credits = prep.sender_credits,
        .tx_capacity = prep.tx_capacity,
        .control_credit_target = prep.control_credit_target,
        .peer_lanes_authorized = prep.peer_lanes_authorized,
        .authorize_lane_update = prep.authorize_lane_update,
        .lane_authorization_cookie = prep.lane_authorization_cookie,
        .lane_update_timeout_ns = prep.lane_update_timeout_ns,
        .rndv = rndv};
    const auto result = resources->connections->PrepareRaw(*owned, bound, *domain.sources);
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    return {};
}
std::expected<void, kbs_stream_error>
RawStreamRuntime::RetireSources(kbs_stream_handle stream, const runtime::RxSourceRetireProof& proof) noexcept {
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto index = state_->Find(stream);
    if (!index) {
        return std::unexpected(index.error());
    }
    return state_->endpoints[*index].binding.RetireSources(stream, proof);
}
std::expected<runtime::ProgressBatch, kbs_stream_error> RawStreamRuntime::Step(bool wait) noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    for (std::size_t i = 0; i < state_->count; ++i) {
        if (state_->endpoints[i].binding.events_pending()) {
            wait = false;
        }
    }
    const auto result = wait ? state_->stack.RunOnce() : state_->stack.Tick();
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    for (std::size_t i = 0; i < state_->count; ++i) {
        if (state_->endpoints[i].callback_error) {
            return std::unexpected(*state_->endpoints[i].callback_error);
        }
    }
    return *result;
}
std::expected<void, kbs_stream_error>
RawStreamRuntime::RequestReplacement(std::size_t context, raw::JettyLane lane,
                                     const raw::JettyLaneQuiescence& proof) noexcept {
    if (!state_ || !state_->ready) {
        return Error(KBS_STREAM_NOT_READY);
    }
    if (state_->stopping) {
        return Error(KBS_STREAM_STOPPED);
    }
    const auto result = state_->stack.RequestReplacement(context, lane, proof);
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    return {};
}
std::expected<void, kbs_stream_error> RawStreamRuntime::BeginClose() noexcept {
    if (!state_) {
        return Error(KBS_STREAM_NOT_READY);
    }
    const auto result = state_->stack.BeginClose();
    // 暂停拒绝无副作用；其余关闭错误可能已停掉部分资源，门面不能继续承诺准入。
    if (!result && (result.error().code == runtime::RawStackErrorCode::kInUse ||
                    result.error().code == runtime::RawStackErrorCode::kNotReady)) {
        return std::unexpected(MapStreamError(result.error()));
    }
    state_->stopping = true;
    state_->buffers.StopAccepting();
    for (std::size_t i = 0; i < state_->count; ++i) {
        state_->endpoints[i].binding.StopAccepting();
    }
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    return {};
}
std::expected<runtime::RawStackSnapshot, kbs_stream_error>
RawStreamRuntime::Close(const runtime::RawStackCloseProof& proof) noexcept {
    if (const auto begun = BeginClose(); !begun) {
        return std::unexpected(begun.error());
    }
    auto& s = *state_;
    if (s.buffers.published()) {
        return Error(KBS_STREAM_BUSY);
    }
    for (std::size_t i = 0; i < s.count; ++i) {
        auto& e = s.endpoints[i];
        if (e.unpublished) {
            const auto resources = s.stack.resources(i);
            if (!resources) {
                return std::unexpected(MapStreamError(resources.error()));
            }
            if (const auto cleaned = e.CleanUnpublished(*resources->connections); !cleaned) {
                return std::unexpected(cleaned.error());
            }
        }
        if (e.binding.published()) {
            return Error(KBS_STREAM_BUSY);
        }
    }
    for (std::size_t i = 0; i < s.count; ++i) {
        if (const auto closed = s.endpoints[i].binding.Close(); !closed) {
            return std::unexpected(closed.error());
        }
    }
    if (const auto closed = s.buffers.Close(); !closed) {
        return std::unexpected(closed.error());
    }
    const auto result = s.stack.AdvanceClose(proof);
    if (!result) {
        return std::unexpected(MapStreamError(result.error()));
    }
    return *result;
}
} // namespace api
} // namespace kbsocket
