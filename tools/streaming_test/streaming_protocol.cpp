// SPDX-License-Identifier: MulanPSL-2.0
#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <string>
#include <system_error>

#include "tools/streaming_test/streaming_test.hpp"

namespace kbsocket {
namespace tools {
namespace {
void Put32(std::span<std::byte> bytes, std::uint32_t value) {
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<std::byte>(value & 255);
        value >>= 8;
    }
}
std::uint32_t Get32(std::span<const std::byte> bytes) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | std::to_integer<unsigned>(bytes[i]);
    }
    return value;
}
auto Error(const char* text, int code = EINVAL) {
    return std::unexpected(ToolError{text, code});
}
bool ValidGroups(const StreamDeviceGroups& groups, std::size_t count, bool strict) {
    std::array<std::size_t, kStreamMaxContexts> sizes{};
    std::size_t distinct = 0;
    for (std::size_t i = 0; i < groups.size(); ++i) {
        if (i >= count) {
            if (groups[i]) {
                return false;
            }
            continue;
        }
        if (groups[i] > distinct || groups[i] >= sizes.size()) {
            return false;
        }
        if (groups[i] == distinct) {
            ++distinct;
        }
        ++sizes[groups[i]];
    }
    return !strict || (count == 4 && distinct == 2 && sizes[0] == 2 && sizes[1] == 2);
}
} // namespace

std::expected<void, ToolError> ValidateStreamOptions(const StreamOptions& o) noexcept {
    if (o.bytes < 32 || o.bytes > 4096 || !o.window || o.window > kStreamMaxWindow || o.messages < o.window ||
        o.messages > 10000000 || !o.timeout_ms || o.timeout_ms > 3600000 || o.arm_delay_us > 1000000 ||
        (!o.events && o.arm_delay_us)) {
        return Error("invalid streaming options");
    }
    return {};
}

std::expected<std::vector<raw::DeviceConfig>, ToolError> ParseStreamDevices(std::string_view text) {
    std::vector<raw::DeviceConfig> result;
    while (!text.empty()) {
        const auto comma = text.find(',');
        const auto item = text.substr(0, comma);
        const auto colon = item.find(':');
        if (colon == std::string_view::npos || !colon || colon + 1 == item.size()) {
            return Error("devices require name:eid_index pairs");
        }
        std::uint32_t eid = 0;
        const auto number = item.substr(colon + 1);
        const auto parsed = std::from_chars(number.data(), number.data() + number.size(), eid);
        if (parsed.ec != std::errc{} || parsed.ptr != number.data() + number.size()) {
            return Error("invalid device EID index");
        }
        const auto name = item.substr(0, colon);
        if (result.size() == kStreamMaxContexts) {
            return Error("use at most four device/EID pairs");
        }
        if (std::any_of(result.begin(), result.end(),
                        [&](const auto& d) { return d.device_name == name && d.eid_index == eid; })) {
            return Error("duplicate device/EID pair");
        }
        result.push_back({std::string(name), eid});
        if (comma == std::string_view::npos) {
            break;
        }
        text.remove_prefix(comma + 1);
        if (text.empty()) {
            return Error("trailing device separator");
        }
    }
    if (result.empty()) {
        return Error("at least one device required");
    }
    return result;
}

std::expected<StreamDeviceGroups, ToolError> BuildStreamDeviceGroups(std::span<const raw::DeviceConfig> devices,
                                                                     bool two_devices_two_eids) {
    if (devices.empty() || devices.size() > kStreamMaxContexts) {
        return Error("use one to four device/EID pairs");
    }
    StreamDeviceGroups groups{};
    std::uint8_t distinct = 0;
    for (std::size_t i = 0; i < devices.size(); ++i) {
        bool found = false;
        for (std::size_t j = 0; j < i; ++j) {
            if (devices[i].device_name == devices[j].device_name) {
                if (devices[i].eid_index == devices[j].eid_index) {
                    return Error("duplicate device/EID pair");
                }
                groups[i] = groups[j];
                found = true;
            }
        }
        if (!found) {
            groups[i] = distinct++;
        }
    }
    if (!ValidGroups(groups, devices.size(), two_devices_two_eids)) {
        return Error("--two_devices_two_eids requires exactly two devices with two distinct EIDs each");
    }
    return groups;
}

StreamHelloBytes EncodeStreamHello(const StreamHello& hello) noexcept {
    StreamHelloBytes bytes{};
    auto wire = std::span(bytes);
    Put32(wire, 0x4b425354); // KBST
    Put32(wire.subspan(4), 2);
    Put32(wire.subspan(8),
          (hello.server ? 1u : 0u) | (hello.options.events ? 2u : 0u) | (hello.options.two_devices_two_eids ? 4u : 0u));
    Put32(wire.subspan(12), hello.count);
    Put32(wire.subspan(16), hello.options.bytes);
    Put32(wire.subspan(20), hello.options.messages);
    Put32(wire.subspan(24), hello.options.window);
    for (std::size_t i = 0; i < kStreamMaxContexts; ++i) {
        bytes[28 + i] = static_cast<std::byte>(hello.device_groups[i]);
    }
    for (std::size_t i = 0; i < std::min<std::size_t>(hello.count, kStreamMaxContexts); ++i) {
        std::memcpy(bytes.data() + 32 + 20 * i, hello.endpoints[i].eid.raw, 16);
        Put32(wire.subspan(48 + 20 * i), hello.endpoints[i].id);
    }
    return bytes;
}

std::expected<StreamHello, ToolError> DecodeStreamHello(const StreamHelloBytes& bytes) noexcept {
    const auto wire = std::span(bytes);
    if (Get32(wire) != 0x4b425354 || Get32(wire.subspan(4)) != 2 || Get32(wire.subspan(8)) > 7) {
        return Error("stream hello header", EPROTO);
    }
    StreamHello hello;
    hello.server = Get32(wire.subspan(8)) & 1;
    hello.options.events = Get32(wire.subspan(8)) & 2;
    hello.options.two_devices_two_eids = Get32(wire.subspan(8)) & 4;
    hello.count = Get32(wire.subspan(12));
    hello.options.bytes = Get32(wire.subspan(16));
    hello.options.messages = Get32(wire.subspan(20));
    hello.options.window = Get32(wire.subspan(24));
    if (!hello.count || hello.count > kStreamMaxContexts || !ValidateStreamOptions(hello.options)) {
        return Error("stream hello options", EPROTO);
    }
    for (std::size_t i = 0; i < kStreamMaxContexts; ++i) {
        hello.device_groups[i] = std::to_integer<std::uint8_t>(bytes[28 + i]);
    }
    if (!ValidGroups(hello.device_groups, hello.count, hello.options.two_devices_two_eids)) {
        return Error("stream hello device grouping", EPROTO);
    }
    for (std::size_t i = 0; i < hello.count; ++i) {
        std::memcpy(hello.endpoints[i].eid.raw, bytes.data() + 32 + 20 * i, 16);
        hello.endpoints[i].id = Get32(wire.subspan(48 + 20 * i));
        for (std::size_t j = 0; j < i; ++j) {
            if (hello.endpoints[i].id == hello.endpoints[j].id &&
                !std::memcmp(hello.endpoints[i].eid.raw, hello.endpoints[j].eid.raw, 16)) {
                return Error("duplicate stream endpoint", EPROTO);
            }
        }
    }
    for (const auto byte : wire.subspan(32 + hello.count * 20)) {
        if (byte != std::byte{}) {
            return Error("nonzero unused stream endpoint", EPROTO);
        }
    }
    return hello;
}

std::expected<void, ToolError> ValidateStreamPeer(const StreamHello& local, const StreamHello& peer) noexcept {
    if (peer.server == local.server || peer.count != local.count || peer.options.bytes != local.options.bytes ||
        peer.options.window != local.options.window || peer.options.messages != local.options.messages ||
        peer.options.events != local.options.events ||
        peer.options.two_devices_two_eids != local.options.two_devices_two_eids) {
        return Error("stream peer options mismatch", EPROTO);
    }
    if (local.options.two_devices_two_eids && peer.device_groups != local.device_groups) {
        return Error("stream peer device grouping mismatch; pair contexts by matching device groups", EPROTO);
    }
    return {};
}

std::expected<StreamHello, ToolError> ExchangeStreamHello(ControlChannel& channel, const StreamHello& hello) {
    auto local = EncodeStreamHello(hello);
    if (!DecodeStreamHello(local)) {
        return Error("invalid local stream hello");
    }
    auto written = channel.Write(local, hello.options.timeout_ms);
    if (!written) {
        return std::unexpected(written.error());
    }
    StreamHelloBytes incoming{};
    auto read = channel.Read(incoming, hello.options.timeout_ms);
    if (!read) {
        return std::unexpected(read.error());
    }
    auto peer = DecodeStreamHello(incoming);
    if (!peer) {
        return peer;
    }
    if (auto matched = ValidateStreamPeer(hello, *peer); !matched) {
        return std::unexpected(matched.error());
    }
    return peer;
}
} // namespace tools
} // namespace kbsocket
