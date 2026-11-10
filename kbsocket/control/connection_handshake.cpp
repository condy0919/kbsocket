// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/control/connection_handshake.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <span>

namespace kbsocket {
namespace control {
namespace {
// wire 使用固定语义值；本地 SDK 改变枚举编码时必须显式更新映射，不能静默赋给 provider 位域。
static_assert(kRndvAccessRead == URMA_ACCESS_READ && kRndvAccessWrite == URMA_ACCESS_WRITE &&
              kRndvAccessAtomic == URMA_ACCESS_ATOMIC && kRndvTokenNone == URMA_TOKEN_NONE &&
              kRndvTokenPlainText == URMA_TOKEN_PLAIN_TEXT);
auto Error(ConnectionHandshakeErrorCode code, int detail = 0, int underlying_error = 0) noexcept {
    return std::unexpected(ConnectionHandshakeError{code, detail, underlying_error});
}
ConnectionHandshakeError ChannelError(HandshakeError error) noexcept {
    return {ConnectionHandshakeErrorCode::kChannel, static_cast<int>(error.code), error.system_error};
}
RndvLimits NegotiateRndv(const HandshakeHello& local, const HandshakeHello& peer) noexcept {
    if (!(local.capabilities & peer.capabilities & kHandshakeCapabilityWriteRndv)) {
        return {};
    }
    const auto& left = local.rndv.limits;
    const auto& right = peer.rndv.limits;
    // 首版共用较小的双向上限；实际每连接/每 Arena 分配仍须受共享内存硬预算约束。
    return {.version = left.version,
            .flags = left.flags,
            .max_write_bytes = std::min(left.max_write_bytes, right.max_write_bytes),
            .max_transfer_bytes = std::min(left.max_transfer_bytes, right.max_transfer_bytes),
            .max_grants = std::min(left.max_grants, right.max_grants),
            .window = std::min(left.window, right.window),
            .max_bytes = std::min(left.max_bytes, right.max_bytes)};
}
} // namespace

ConnectionHandshake::~ConnectionHandshake() {
    // 外部域与缓存由宿主持有；Close 失败时不能代替宿主释放仍在用的 DMA/CPU 依赖。
    (void)Close();
}
bool ConnectionHandshake::active() const noexcept {
    return state_ == ConnectionHandshakeState::kRawActive || state_ == ConnectionHandshakeState::kTcpActive;
}
const RndvLimits* ConnectionHandshake::rndv() const noexcept {
    return state_ == ConnectionHandshakeState::kRawActive && rndv_.version ? &rndv_ : nullptr;
}
const RndvLimits* ConnectionHandshake::negotiated_rndv() const noexcept {
    return peer_hello_ && transport_ == HandshakeTransport::kRaw && state_ != ConnectionHandshakeState::kClosed &&
                   rndv_.version
               ? &rndv_
               : nullptr;
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::RetainRndvRoute() noexcept {
    if (rndv_route_retained_) {
        return Error(ConnectionHandshakeErrorCode::kInUse);
    }
    if (!prepared_ || !negotiated_rndv() || failure_ || !table_ || !route_.owner) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    rndv_route_retained_ = true;
    return {};
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::ReleaseRndvRoute() noexcept {
    if (!rndv_route_retained_) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    rndv_route_retained_ = false;
    return {};
}
std::expected<TsegImportRequest, ConnectionHandshakeError>
ConnectionHandshake::PrepareRndvImport(const connection::GrantIdentity& grant) const noexcept {
    if (!rndv()) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    if (!raw_.rndv_peer_authorized) {
        return Error(ConnectionHandshakeErrorCode::kUnauthorized);
    }
    // 接收方向正常 EOF 不撤销反向 WRITE 的已协商权限；失败窗口仍禁止新导入。
    if (!receive_.accepting() && !receive_.eof()) {
        return Error(ConnectionHandshakeErrorCode::kReorder);
    }
    const auto endpoint = raw_.imports->Get(import_);
    if (!endpoint) {
        return Error(ConnectionHandshakeErrorCode::kImport, static_cast<int>(endpoint.error().code),
                     endpoint.error().provider_error);
    }
    const auto credit = raw_.credits->PeerStats(raw_.credit_peer);
    if (!credit || credit->frozen || credit->retired || raw_.credits->stats().frozen) {
        return Error(ConnectionHandshakeErrorCode::kCredit);
    }
    // 当前方向的接收者拥有事务 session/token；不能误用反向 GRANT 通知的路由身份。
    const connection::GrantBinding expected{.sender_runtime = local_.identity.runtime,
                                            .receiver_runtime = peer_.identity.runtime,
                                            .receive_domain = peer_.identity.receive_domain,
                                            .session = peer_.identity.session,
                                            .token = peer_.identity.token};
    if (grant.transfer.binding != expected || !grant.transfer.transfer_id || !grant.transfer.bytes ||
        grant.transfer.bytes > rndv_.max_transfer_bytes || !grant.generation ||
        grant.transfer.seq == std::numeric_limits<std::uint32_t>::max() || grant.slot >= rndv_.window ||
        grant.slot != grant.transfer.seq % rndv_.window) {
        return Error(ConnectionHandshakeErrorCode::kProtocol);
    }
    const RndvArenaDescriptor* arena = nullptr;
    for (std::uint32_t i = 0; i < peer_.rndv.arena_count; ++i) {
        const auto& candidate = peer_.rndv.arenas[i];
        if (candidate.id == grant.arena.id && candidate.generation == grant.arena.generation) {
            arena = &candidate;
            break;
        }
    }
    if (!arena || grant.transfer.bytes > arena->max_grant_bytes || grant.target_address < arena->address ||
        grant.transfer.bytes > arena->length ||
        grant.target_address - arena->address > arena->length - grant.transfer.bytes) {
        return Error(ConnectionHandshakeErrorCode::kProtocol);
    }
    TsegImportRequest request;
    request.key = {.local_context = raw_.local_context,
                   .local_context_incarnation = raw_.local_context_incarnation,
                   .peer_runtime = peer_.identity.runtime,
                   .receive_domain = peer_.identity.receive_domain,
                   .slab_id = arena->id,
                   .slab_generation = arena->generation,
                   .authorization_identity = peer_.identity.authorization};
    for (std::size_t i = 0; i < arena->eid.size(); ++i) {
        const auto byte = std::to_integer<std::uint8_t>(arena->eid[i]);
        request.key.eid[i] = byte;
        request.descriptor.ubva.eid.raw[i] = byte;
    }
    request.descriptor.ubva.uasid = arena->uasid;
    request.descriptor.ubva.va = arena->address;
    request.descriptor.len = arena->length;
    request.descriptor.attr.bs.access = arena->access;
    request.descriptor.attr.bs.token_policy = arena->token_policy;
    request.descriptor.token_id = arena->token_id;
    request.token.token = arena->token;
    return request;
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::Open(int fd, connection::ConnectionTable& table,
                                                                        const ConnectionHandshakeConfig& config,
                                                                        std::uint64_t now_ns) noexcept {
    if (state_ != ConnectionHandshakeState::kIdle) {
        return Error(ConnectionHandshakeErrorCode::kInUse);
    }
    if (!config.expected_peer_authorization || !config.compatible_peer ||
        table.domain().runtime_instance != config.local.identity.runtime ||
        table.domain().receive_domain != config.local.identity.receive_domain) {
        return Error(ConnectionHandshakeErrorCode::kInvalidArgument);
    }
    auto reserved = table.Reserve();
    if (!reserved) {
        return Error(ConnectionHandshakeErrorCode::kRoute, static_cast<int>(reserved.error()));
    }
    HandshakeMessage message;
    message.hello = config.local;
    message.hello.identity.token = reserved->token;
    std::array<std::byte, kHandshakeMaxFrameBytes> encoded{};
    auto validated = EncodeHandshakeMessage(message, encoded);
    if (!validated) {
        (void)table.Retire(*reserved);
        return std::unexpected(ChannelError(validated.error()));
    }
    auto opened = channel_.Open(fd, config.compatible_peer, now_ns, config.deadline_ns);
    if (!opened) {
        (void)table.Retire(*reserved);
        return std::unexpected(ChannelError(opened.error()));
    }
    table_ = &table;
    route_ = *reserved;
    local_ = message.hello;
    expected_authorization_ = config.expected_peer_authorization;
    expected_runtime_ = config.expected_peer_runtime;
    state_ = ConnectionHandshakeState::kNegotiating;
    return {};
}
ConnectionHandshakeError ConnectionHandshake::Fail(ConnectionHandshakeError error) noexcept {
    if (!failure_) {
        failure_ = error;
    }
    state_ = ConnectionHandshakeState::kFailed;
    (void)channel_.Close();
    if (table_ && route_.owner && !rndv_route_retained_) {
        // 不退休 token，保留到 Close 处理完窗口；授信账本独立保留迟到责任。
        (void)table_->Withdraw(route_);
    }
    if (!rndv_route_retained_) {
        published_ = false;
    }
    receive_.Fail();
    return *failure_;
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::Handle(const HandshakeMessage& message) noexcept {
    if (message.kind == HandshakeMessageKind::kHello) {
        if (peer_hello_ || state_ != ConnectionHandshakeState::kNegotiating) {
            return Error(ConnectionHandshakeErrorCode::kProtocol);
        }
        if (message.hello.identity.authorization != expected_authorization_ ||
            (expected_runtime_ && message.hello.identity.runtime != expected_runtime_)) {
            return Error(ConnectionHandshakeErrorCode::kUnauthorized);
        }
        if (message.hello.wire_version != local_.wire_version) {
            return Error(ConnectionHandshakeErrorCode::kProtocol);
        }
        const auto common = local_.capabilities & message.hello.capabilities;
        if (common & kHandshakeCapabilityRaw) {
            transport_ = HandshakeTransport::kRaw;
        } else if (common & kHandshakeCapabilityTcp) {
            transport_ = HandshakeTransport::kTcp;
        } else {
            return Error(ConnectionHandshakeErrorCode::kNoCommonTransport);
        }
        peer_ = message.hello;
        rndv_ = transport_ == HandshakeTransport::kRaw ? NegotiateRndv(local_, peer_) : RndvLimits{};
        peer_hello_ = true;
        state_ = ConnectionHandshakeState::kPreparing;
        return {};
    }
    if (message.kind == HandshakeMessageKind::kReject) {
        if (!peer_hello_ || message.reject.sender_session != peer_.identity.session ||
            message.reject.receiver_session != local_.identity.session) {
            return Error(ConnectionHandshakeErrorCode::kProtocol);
        }
        return Error(ConnectionHandshakeErrorCode::kPeerRejected, static_cast<int>(message.reject.reason));
    }
    if (message.kind != HandshakeMessageKind::kReady || !peer_hello_ || peer_ready_ ||
        message.ready.sender != peer_.identity || message.ready.receiver != local_.identity ||
        message.ready.transport != transport_ || message.ready.rndv != rndv_) {
        return Error(ConnectionHandshakeErrorCode::kProtocol);
    }
    const auto& ready = message.ready;
    if (rndv_.version && (ready.max_frame_seq < rndv_.window || ready.max_stream_bytes < rndv_.max_bytes)) {
        return Error(ConnectionHandshakeErrorCode::kProtocol);
    }
    if (transport_ == HandshakeTransport::kRaw &&
        (ready.credit_epoch != peer_.credit_epoch || !ready.max_credit_seq || !ready.max_control_credit_seq ||
         !ready.max_frame_seq || !ready.max_stream_bytes)) {
        return Error(ConnectionHandshakeErrorCode::kProtocol);
    }
    if (transport_ == HandshakeTransport::kTcp &&
        (ready.credit_epoch || ready.max_credit_seq || ready.max_control_credit_seq || ready.max_frame_seq ||
         ready.max_stream_bytes)) {
        return Error(ConnectionHandshakeErrorCode::kProtocol);
    }
    remote_ready_ = ready;
    peer_ready_ = true;
    return {};
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::CheckRawDependencies() noexcept {
    if (receive_.failure()) {
        return Error(ConnectionHandshakeErrorCode::kReorder, static_cast<int>(receive_.failure()->code));
    }
    auto target = raw_.imports->Get(import_);
    if (!target) {
        return Error(ConnectionHandshakeErrorCode::kImport, static_cast<int>(target.error().code),
                     target.error().provider_error);
    }
    auto peer = raw_.credits->PeerStats(raw_.credit_peer);
    if (!peer || peer->frozen || peer->retired || raw_.credits->stats().frozen) {
        return Error(ConnectionHandshakeErrorCode::kCredit);
    }
    return {};
}
std::expected<void, ConnectionHandshakeError>
ConnectionHandshake::PrepareRaw(const RawHandshakePreparation& preparation) noexcept {
    if (state_ != ConnectionHandshakeState::kPreparing || transport_ != HandshakeTransport::kRaw || prepared_) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    if (!preparation.imports || !preparation.local_context || !preparation.local_context_incarnation ||
        !preparation.descriptor_compatible || !preparation.credits || !preparation.route_owner ||
        preparation.reorder.initial_sequence != 0) {
        return Error(ConnectionHandshakeErrorCode::kInvalidArgument);
    }
    // 在任何 import 引用或重排分配前检查；不能单方静默关闭双方已经声明的 RNDV。
    if (rndv_.version) {
        if (!preparation.rndv_peer_authorized) {
            return Error(ConnectionHandshakeErrorCode::kUnauthorized);
        }
        if (preparation.reorder.window < rndv_.window || preparation.reorder.max_resident_bytes < rndv_.max_bytes) {
            return Error(ConnectionHandshakeErrorCode::kInvalidArgument);
        }
    }
    auto credit = preparation.credits->PeerStats(preparation.credit_peer);
    if (!credit || credit->identity.peer_runtime != peer_.identity.runtime ||
        credit->identity.receive_domain != local_.identity.receive_domain ||
        credit->identity.epoch != local_.credit_epoch || credit->frozen || credit->retired ||
        preparation.credits->stats().frozen || !credit->max_data || !credit->max_control) {
        return Error(ConnectionHandshakeErrorCode::kCredit);
    }
    EndpointImportRequest request;
    request.key = {.local_context = preparation.local_context,
                   .local_context_incarnation = preparation.local_context_incarnation,
                   .peer_runtime = peer_.identity.runtime,
                   .receive_domain = peer_.identity.receive_domain,
                   .endpoint_generation = peer_.identity.endpoint_generation,
                   .jetty_id = peer_.jetty_id,
                   .authorization_identity = peer_.identity.authorization};
    for (std::size_t i = 0; i < request.key.eid.size(); ++i) {
        request.key.eid[i] = std::to_integer<std::uint8_t>(peer_.eid[i]);
    }
    request.descriptor = std::span(peer_.descriptor).first(peer_.descriptor_bytes);
    request.token.token = peer_.import_token;
    auto imported = preparation.imports->Acquire(request);
    if (!imported) {
        return Error(ConnectionHandshakeErrorCode::kImport, static_cast<int>(imported.error().code),
                     imported.error().provider_error);
    }
    auto window = receive_.Open({.peer_runtime = peer_.identity.runtime,
                                 .receive_domain = local_.identity.receive_domain,
                                 .session = local_.identity.session,
                                 .slot = route_.token.slot,
                                 .generation = route_.token.generation},
                                preparation.reorder);
    if (!window) {
        (void)preparation.imports->Release(*imported);
        return Error(ConnectionHandshakeErrorCode::kReorder, static_cast<int>(window.error().code));
    }
    raw_ = preparation;
    import_ = *imported;
    prepared_ = true;
    return {};
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::PublishRaw() noexcept {
    auto checked = CheckRawDependencies();
    if (!checked) {
        return checked;
    }
    auto credit = raw_.credits->PeerStats(raw_.credit_peer);
    if (!credit) {
        return Error(ConnectionHandshakeErrorCode::kCredit);
    }
    const auto window = receive_.stats();
    local_ready_ = {.sender = local_.identity,
                    .receiver = peer_.identity,
                    .transport = transport_,
                    .credit_epoch = credit->identity.epoch,
                    .max_credit_seq = credit->max_data,
                    .max_control_credit_seq = credit->max_control,
                    .max_frame_seq = window.max_frame_seq,
                    .max_stream_bytes = window.max_stream_bytes,
                    .rndv = rndv_};
    connection::WireSessionBinding binding{.context = {.version = local_.wire_version,
                                                       .peer_runtime = peer_.identity.runtime,
                                                       .receive_domain = local_.identity.receive_domain,
                                                       .session = local_.identity.session},
                                           .token = route_.token,
                                           .credit_epoch = peer_.credit_epoch};
    auto published = table_->Publish(route_, binding, raw_.route_owner);
    if (!published) {
        return Error(ConnectionHandshakeErrorCode::kRoute, static_cast<int>(published.error()));
    }
    published_ = true;
    state_ = ConnectionHandshakeState::kWaitingReady;
    return {};
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::PrepareTcp() noexcept {
    if (state_ != ConnectionHandshakeState::kPreparing || transport_ != HandshakeTransport::kTcp || prepared_) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    local_ready_ = {.sender = local_.identity, .receiver = peer_.identity, .transport = transport_};
    prepared_ = true;
    state_ = ConnectionHandshakeState::kWaitingReady;
    return {};
}
std::expected<ConnectionHandshakeState, ConnectionHandshakeError>
ConnectionHandshake::Progress(std::uint64_t now_ns, HandshakeIoBudget budget) noexcept {
    if (failure_) {
        return std::unexpected(*failure_);
    }
    if (active()) {
        return state_;
    }
    if (state_ == ConnectionHandshakeState::kIdle || state_ == ConnectionHandshakeState::kClosed) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    if (!hello_queued_) {
        HandshakeMessage hello;
        hello.hello = local_;
        auto queued = channel_.Queue(hello);
        if (!queued) {
            return std::unexpected(Fail(ChannelError(queued.error())));
        }
        hello_queued_ = true;
    }
    if (published_) {
        // 前一轮仅排队了 READY；若本轮前依赖失效，必须在任何写出前撤回路由。
        auto checked = CheckRawDependencies();
        if (!checked) {
            return std::unexpected(Fail(checked.error()));
        }
    }
    // 对端 READY 之后可能紧接业务字节；仅完成本端剩余握手写出，不再读取。
    budget.receive = budget.receive && !peer_ready_;
    auto progress = channel_.Progress(now_ns, budget);
    if (!progress) {
        return std::unexpected(Fail(ChannelError(progress.error())));
    }
    if (progress->message_ready) {
        auto message = channel_.TakeMessage();
        if (!message) {
            return std::unexpected(Fail(ChannelError(message.error())));
        }
        auto handled = Handle(*message);
        if (!handled) {
            return std::unexpected(Fail(handled.error()));
        }
    }
    if (transport_ == HandshakeTransport::kRaw && prepared_) {
        auto checked = CheckRawDependencies();
        if (!checked) {
            if (checked.error().code == ConnectionHandshakeErrorCode::kImport &&
                checked.error().detail == static_cast<int>(ImportCacheErrorCode::kPending) && !published_) {
                return state_;
            }
            return std::unexpected(Fail(checked.error()));
        }
        if (!published_) {
            auto published = PublishRaw();
            if (!published) {
                return std::unexpected(Fail(published.error()));
            }
        }
    }
    if (state_ == ConnectionHandshakeState::kWaitingReady && !ready_queued_ && channel_.output_flushed()) {
        HandshakeMessage ready;
        ready.kind = HandshakeMessageKind::kReady;
        ready.ready = local_ready_;
        auto queued = channel_.Queue(ready);
        if (!queued) {
            return std::unexpected(Fail(ChannelError(queued.error())));
        }
        ready_queued_ = true;
    }
    if (ready_queued_ && peer_ready_ && channel_.output_flushed()) {
        state_ = transport_ == HandshakeTransport::kRaw ? ConnectionHandshakeState::kRawActive
                                                        : ConnectionHandshakeState::kTcpActive;
    }
    return state_;
}
void ConnectionHandshake::Cancel() noexcept {
    if (state_ != ConnectionHandshakeState::kIdle && state_ != ConnectionHandshakeState::kClosed) {
        (void)Fail({ConnectionHandshakeErrorCode::kCancelled});
    }
}
std::expected<void, ConnectionHandshakeError> ConnectionHandshake::Close() noexcept {
    if (rndv_route_retained_) {
        return Error(ConnectionHandshakeErrorCode::kInUse);
    }
    if (state_ == ConnectionHandshakeState::kClosed) {
        return {};
    }
    (void)channel_.Close();
    if (state_ != ConnectionHandshakeState::kIdle) {
        state_ = ConnectionHandshakeState::kFailed;
    }
    if (table_ && route_.owner) {
        auto withdrawn = table_->Withdraw(route_);
        if (!withdrawn) {
            return Error(ConnectionHandshakeErrorCode::kRoute, static_cast<int>(withdrawn.error()));
        }
    }
    published_ = false;
    auto closed = receive_.Close();
    if (!closed) {
        return Error(ConnectionHandshakeErrorCode::kInUse);
    }
    if (import_.owner) {
        auto released = raw_.imports->Release(import_);
        if (!released) {
            return Error(ConnectionHandshakeErrorCode::kImport, static_cast<int>(released.error().code),
                         released.error().provider_error);
        }
        import_ = {};
    }
    if (table_ && route_.owner) {
        auto retired = table_->Retire(route_);
        if (!retired) {
            return Error(ConnectionHandshakeErrorCode::kRoute, static_cast<int>(retired.error()));
        }
        route_ = {};
    }
    state_ = ConnectionHandshakeState::kClosed;
    return {};
}
std::expected<int, ConnectionHandshakeError> ConnectionHandshake::TakeEstablishedFd() noexcept {
    if (!active()) {
        return Error(ConnectionHandshakeErrorCode::kNotReady);
    }
    auto released = channel_.ReleaseEstablished(true);
    if (!released) {
        return std::unexpected(ChannelError(released.error()));
    }
    return *released;
}

} // namespace control
} // namespace kbsocket
