// SPDX-License-Identifier: MulanPSL-2.0
#include "kbsocket/runtime/rx_source_registry.hpp"

#include <bit>
#include <cstring>
#include <limits>
#include <new>

namespace kbsocket {
namespace runtime {
namespace {
constexpr auto kEnd = std::numeric_limits<std::uint32_t>::max();
auto Error(RxSourceError code) noexcept {
    return std::unexpected(code);
}
bool SameRemote(const urma_jetty_id_t& a, const urma_jetty_id_t& b) noexcept {
    return a.id == b.id && a.uasid == b.uasid && std::memcmp(&a.eid, &b.eid, sizeof(a.eid)) == 0;
}
bool SamePeer(connection::CreditPeerHandle a, connection::CreditPeerHandle b) noexcept {
    return a.domain == b.domain && a.slot == b.slot && a.id == b.id;
}
bool SameKey(const RxSourceRoute& a, const RxSourceRoute& b) noexcept {
    return a.local_id == b.local_id && a.is_jetty == b.is_jetty && SameRemote(a.remote_id, b.remote_id) &&
           a.token == b.token;
}
std::uint64_t Hash(const RxSourceRoute& route) noexcept {
    std::uint64_t hash = 14695981039346656037ull;
    const auto byte = [&hash](std::uint8_t value) { hash = (hash ^ value) * 1099511628211ull; };
    const auto word = [&byte](std::uint32_t value) {
        for (int i = 0; i < 4; ++i) {
            byte(static_cast<std::uint8_t>(value));
            value >>= 8;
        }
    };
    word(route.local_id);
    byte(route.is_jetty);
    const auto* eid = reinterpret_cast<const unsigned char*>(&route.remote_id.eid);
    for (std::size_t i = 0; i < sizeof(route.remote_id.eid); ++i) {
        byte(eid[i]);
    }
    word(route.remote_id.id);
    word(route.remote_id.uasid);
    word(route.token.slot);
    word(route.token.generation);
    return hash;
}
} // namespace

struct RxSourceRegistry::State {
    struct Entry {
        RxSourceRoute route;
        std::uint64_t id = 0;
        std::uint32_t next = kEnd;
        bool live = false;
    };
    bool LivePool() const noexcept {
        return config.buffers->pool() == pool && pool->context() == context;
    }
    bool Local(std::uint32_t id, bool jetty) const noexcept {
        return LivePool() && (jetty ? pool->IsReceiveJetty(id) : pool->jfr() && pool->jfr()->jfr_id.id == id);
    }
    std::uint32_t Bucket(const RxSourceRoute& route) const noexcept {
        return static_cast<std::uint32_t>(Hash(route)) & (bucket_count - 1);
    }
    const Entry* Find(const RxSourceRoute& route) const noexcept {
        for (auto index = buckets[Bucket(route)]; index != kEnd; index = entries[index].next) {
            if (SameKey(entries[index].route, route)) {
                return &entries[index];
            }
        }
        return nullptr;
    }
    RxSourceRegistryConfig config;
    raw::JettyPool* pool = nullptr;
    urma_context_t* context = nullptr;
    std::unique_ptr<Entry[]> entries;
    std::unique_ptr<std::uint32_t[]> buckets;
    std::uint32_t bucket_count = 0, free = kEnd;
    std::size_t live = 0;
};

RxSourceRegistry::RxSourceRegistry() noexcept = default;
RxSourceRegistry::~RxSourceRegistry() = default;

std::expected<void, RxSourceError> RxSourceRegistry::Open(const RxSourceRegistryConfig& config) noexcept {
    if (state_ || opened_once_) {
        return Error(RxSourceError::kInUse);
    }
    if (!config.buffers || !config.buffers->pool() || !config.buffers->pool()->context() ||
        !config.context_incarnation || !config.local_domain.runtime_instance || !config.local_domain.receive_domain ||
        !config.capacity || config.capacity > kMaxCapacity) {
        return Error(RxSourceError::kInvalidArgument);
    }
    auto state = std::unique_ptr<State>(new (std::nothrow) State);
    if (!state) {
        return Error(RxSourceError::kNoMemory);
    }
    state->config = config;
    state->pool = config.buffers->pool();
    state->context = state->pool->context();
    state->bucket_count = std::bit_ceil(config.capacity * 2);
    state->entries.reset(new (std::nothrow) State::Entry[config.capacity]);
    state->buckets.reset(new (std::nothrow) std::uint32_t[state->bucket_count]);
    if (!state->entries || !state->buckets) {
        return Error(RxSourceError::kNoMemory);
    }
    for (std::uint32_t i = 0; i < state->bucket_count; ++i) {
        state->buckets[i] = kEnd;
    }
    for (std::uint32_t i = 0; i < config.capacity; ++i) {
        state->entries[i].next = i + 1 < config.capacity ? i + 1 : kEnd;
    }
    state->free = 0;
    state_ = std::move(state);
    opened_once_ = true;
    return {};
}

std::expected<RxSourceLease, RxSourceError> RxSourceRegistry::Register(const RxSourceRoute& route) noexcept {
    if (!state_) {
        return Error(RxSourceError::kNotReady);
    }
    auto& s = *state_;
    const auto& source = route.source;
    if (!s.Local(route.local_id, route.is_jetty) || !route.token.generation || !source.peer.domain || !source.peer.id ||
        source.local_domain.runtime_instance != s.config.local_domain.runtime_instance ||
        source.local_domain.receive_domain != s.config.local_domain.receive_domain ||
        source.peer_context.version != connection::kWireVersion || !source.peer_context.peer_runtime ||
        source.peer_context.receive_domain != s.config.local_domain.receive_domain || !source.peer_context.session) {
        return Error(RxSourceError::kInvalidArgument);
    }
    const auto peer = source.peer.domain->PeerStats(source.peer);
    if (!peer || peer->retired || peer->identity.peer_runtime != source.peer_context.peer_runtime ||
        peer->identity.receive_domain != source.peer_context.receive_domain) {
        return Error(RxSourceError::kInvalidArgument);
    }
    if (s.Find(route)) {
        return Error(RxSourceError::kDuplicate);
    }
    // 注册在控制路径执行；允许共用物理 Lane 的会话各自有 token/session，禁止重解释旧物理来源。
    for (std::uint32_t i = 0; i < s.config.capacity; ++i) {
        const auto& entry = s.entries[i];
        if (entry.live && entry.route.is_jetty == route.is_jetty &&
            SameRemote(entry.route.remote_id, route.remote_id) &&
            (entry.route.source.peer_context.peer_runtime != source.peer_context.peer_runtime ||
             !SamePeer(entry.route.source.peer, source.peer))) {
            return Error(RxSourceError::kIdentityConflict);
        }
    }
    if (s.free == kEnd) {
        return Error(RxSourceError::kExhausted);
    }
    if (next_id_ == std::numeric_limits<std::uint64_t>::max()) {
        return Error(RxSourceError::kSequenceExhausted);
    }
    const auto slot = s.free;
    auto& entry = s.entries[slot];
    s.free = entry.next;
    const auto bucket = s.Bucket(route);
    entry = {.route = route, .id = ++next_id_, .next = s.buckets[bucket], .live = true};
    s.buckets[bucket] = slot;
    ++s.live;
    return RxSourceLease{this, slot, entry.id};
}

std::expected<connection::EagerReceiveSource, RxSourceError>
RxSourceRegistry::Resolve(const raw::RxBufferEvent& event) const noexcept {
    if (!state_) {
        return Error(RxSourceError::kNotReady);
    }
    const auto& s = *state_;
    const auto& cr = event.completion;
    if (event.error || !event.lease || event.lease->pool != s.config.buffers || !event.lease->id ||
        event.lease->id != cr.user_ctx || cr.status != URMA_CR_SUCCESS || !cr.flag.bs.s_r || cr.flag.bs.reserved ||
        cr.flag.bs.suspend_done || cr.flag.bs.flush_err_done || cr.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
        !s.Local(cr.local_id, cr.flag.bs.jetty) || cr.completion_len != event.payload.size()) {
        return Error(RxSourceError::kInvalidCompletion);
    }
    const auto decoded = connection::DecodeWireFrame(cr.imm_data, event.payload);
    if (!decoded) {
        return Error(RxSourceError::kMalformedFrame);
    }
    RxSourceRoute key;
    key.local_id = cr.local_id;
    key.is_jetty = cr.flag.bs.jetty;
    key.remote_id = cr.remote_id;
    key.token = decoded->frame.token;
    const auto* entry = s.Find(key);
    if (!entry) {
        return Error(RxSourceError::kUnknownSource);
    }
    return entry->route.source;
}

std::expected<void, RxSourceError> RxSourceRegistry::Remove(RxSourceLease lease,
                                                            const RxSourceRetireProof& proof) noexcept {
    if (!state_) {
        return Error(RxSourceError::kNotReady);
    }
    auto& s = *state_;
    if (lease.owner != this || lease.slot >= s.config.capacity || !lease.id || !s.entries[lease.slot].live ||
        s.entries[lease.slot].id != lease.id) {
        return Error(RxSourceError::kInvalidLease);
    }
    if (proof.context_incarnation != s.config.context_incarnation || !proof.remote_sender_stopped ||
        !proof.accepted_traffic_settled || !proof.receive_events_drained) {
        return Error(RxSourceError::kProofRequired);
    }
    auto& entry = s.entries[lease.slot];
    auto* link = &s.buckets[s.Bucket(entry.route)];
    while (*link != lease.slot) {
        link = &s.entries[*link].next;
    }
    *link = entry.next;
    entry.live = false;
    entry.next = s.free;
    s.free = lease.slot;
    --s.live;
    return {};
}

std::expected<void, RxSourceError> RxSourceRegistry::Close() noexcept {
    if (state_ && state_->live) {
        return Error(RxSourceError::kInUse);
    }
    state_.reset();
    return {};
}
std::size_t RxSourceRegistry::size() const noexcept {
    return state_ ? state_->live : 0;
}
raw::RxBufferPool* RxSourceRegistry::buffers() const noexcept {
    return state_ ? state_->config.buffers : nullptr;
}
std::uint64_t RxSourceRegistry::context_incarnation() const noexcept {
    return state_ ? state_->config.context_incarnation : 0;
}

} // namespace runtime
} // namespace kbsocket
